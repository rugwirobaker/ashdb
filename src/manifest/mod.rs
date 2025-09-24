pub mod edit;
pub mod header;
pub mod meta;

use crate::error::Result;
use crate::Error;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc::{Crc, CRC_32_ISCSI};
use edit::VersionEdit;
use header::{ManifestHeader, HEADER_SIZE};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Mutex, RwLock};

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub struct Manifest {
    file: File,
    writer: Mutex<BufWriter<File>>,
    header: RwLock<ManifestHeader>,
    // path: PathBuf,
    edit_count: AtomicU32,
}

impl Manifest {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let writer = BufWriter::new(file.try_clone()?);

        let header = if file.metadata()?.len() == 0 {
            let h = ManifestHeader::new();
            let header_bytes = h.encode();
            file.write_all(&header_bytes)?;
            file.sync_all()?;
            h
        } else {
            let mut buf = [0u8; HEADER_SIZE];
            file.read_exact(&mut buf)?;
            ManifestHeader::decode(&buf)?
        };

        Ok(Self {
            file,
            writer: Mutex::new(writer),
            header: RwLock::new(header),
            // path,
            edit_count: AtomicU32::new(0),
        })
    }

    pub fn append(&self, edit: VersionEdit) -> Result<()> {
        let edit_bytes = edit.encode();
        let checksum = CRC32.checksum(&edit_bytes);

        let mut writer = self.writer.lock().map_err(|_| Error::MutexPoisoned)?;

        writer.write_u32::<BigEndian>(edit_bytes.len() as u32)?;
        writer.write_all(&edit_bytes)?;
        writer.write_u32::<BigEndian>(checksum)?;

        let seq = edit.seq();
        let snapshot_interval = {
            let mut header = self.header.write().map_err(|_| Error::MutexPoisoned)?;
            header.current_seq = header.current_seq.max(seq);
            header.snapshot_interval
        };

        let count = self.edit_count.fetch_add(1, Ordering::SeqCst) + 1;

        if count >= snapshot_interval {
            self.edit_count.store(0, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.writer
            .lock()
            .map_err(|_| Error::MutexPoisoned)?
            .flush()?;

        let header = self.header.read().map_err(|_| Error::MutexPoisoned)?;
        let header_bytes = header.encode();
        drop(header);

        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        Ok(())
    }

    // TODO: ensure manifest only has one reader a time otherwise they will interfere with each other's reads.
    pub fn iter(&self) -> Result<ManifestIterator> {
        let mut reader = BufReader::new(self.file.try_clone()?);
        reader.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        Ok(ManifestIterator { reader })
    }

    pub fn next_table_id(&self) -> u64 {
        let mut header = self.header.write().unwrap();
        let id = header.next_table_id;
        header.next_table_id += 1;
        id
    }

    pub fn next_seq(&self) -> u64 {
        let mut header = self.header.write().unwrap();
        let seq = header.current_seq;
        header.current_seq += 1;
        seq
    }

    pub fn should_snapshot(&self) -> bool {
        self.edit_count.load(Ordering::SeqCst) >= self.header.read().unwrap().snapshot_interval
    }

    pub fn replay(&self) -> Result<ManifestState> {
        let mut state = ManifestState::new();
        let mut last_snapshot_seq: Option<u64> = None;

        for edit in self.iter()? {
            match edit? {
                VersionEdit::Snapshot {
                    seq,
                    levels,
                    next_table_id,
                    deletable_wals,
                } => {
                    state.levels.clear();
                    for level_meta in levels {
                        while state.levels.len() <= level_meta.level as usize {
                            state.levels.push(LevelMeta {
                                level: state.levels.len() as u32,
                                tables: Vec::new(),
                            });
                        }
                        state.levels[level_meta.level as usize] = level_meta.clone();
                    }
                    state.next_table_id = next_table_id;
                    state.deletable_wals = deletable_wals;
                    last_snapshot_seq = Some(seq);
                }

                VersionEdit::Flush { seq, table, wal_id } => {
                    if let Some(snap_seq) = last_snapshot_seq {
                        if seq <= snap_seq {
                            continue;
                        }
                    }
                    let table_id = table.id;
                    let level = table.level;
                    state.add_table_at_level(table, level);
                    state.deletable_wals.push(wal_id);
                    state.next_table_id = state.next_table_id.max(table_id + 1);
                }

                VersionEdit::BeginCompaction { .. } => {}

                VersionEdit::CommitCompaction {
                    seq,
                    source_level,
                    deleted_tables,
                    target_level,
                    added_tables,
                    ..
                } => {
                    if let Some(snap_seq) = last_snapshot_seq {
                        if seq <= snap_seq {
                            continue;
                        }
                    }
                    state.delete_tables(source_level, &deleted_tables);
                    for table in added_tables {
                        state.next_table_id = state.next_table_id.max(table.id + 1);
                        state.add_table_at_level(table, target_level);
                    }
                }

                VersionEdit::AddSSTable { seq, level, table } => {
                    if let Some(snap_seq) = last_snapshot_seq {
                        if seq <= snap_seq {
                            continue;
                        }
                    }
                    let table_id = table.id;
                    state.add_table_at_level(table, level);
                    state.next_table_id = state.next_table_id.max(table_id + 1);
                }

                VersionEdit::DeleteSSTable {
                    seq,
                    table_id,
                    level,
                } => {
                    if let Some(snap_seq) = last_snapshot_seq {
                        if seq <= snap_seq {
                            continue;
                        }
                    }
                    state.delete_tables(level, &[table_id]);
                }
            }
        }

        Ok(state)
    }
}

use meta::LevelMeta;

#[derive(Debug, Clone)]
pub struct ManifestState {
    pub levels: Vec<LevelMeta>,
    pub next_table_id: u64,
    pub deletable_wals: Vec<u64>,
}

impl Default for ManifestState {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestState {
    pub fn new() -> Self {
        Self {
            levels: Vec::new(),
            next_table_id: 0,
            deletable_wals: Vec::new(),
        }
    }

    fn add_table_at_level(&mut self, table: meta::TableMeta, level: u32) {
        while self.levels.len() <= level as usize {
            self.levels.push(LevelMeta {
                level: self.levels.len() as u32,
                tables: Vec::new(),
            });
        }
        self.levels[level as usize].tables.push(table);
    }

    fn delete_tables(&mut self, level: u32, table_ids: &[u64]) {
        if let Some(level_meta) = self.levels.get_mut(level as usize) {
            level_meta.tables.retain(|t| !table_ids.contains(&t.id));
        }
    }
}

pub struct ManifestIterator {
    reader: BufReader<File>,
}

impl Iterator for ManifestIterator {
    type Item = Result<VersionEdit>;

    fn next(&mut self) -> Option<Self::Item> {
        let edit_len = match self.reader.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e.into())),
        };

        let mut edit_bytes = vec![0u8; edit_len];
        if let Err(e) = self.reader.read_exact(&mut edit_bytes) {
            return Some(Err(e.into()));
        }

        let stored_checksum = match self.reader.read_u32::<BigEndian>() {
            Ok(checksum) => checksum,
            Err(e) => return Some(Err(e.into())),
        };

        let computed_checksum = CRC32.checksum(&edit_bytes);

        if computed_checksum != stored_checksum {
            return Some(Err(Error::ChecksumMismatch));
        }

        Some(VersionEdit::decode(&edit_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tmpfs::TempDir;
    use edit::VersionEdit;
    use meta::{LevelMeta, TableMeta};

    fn create_test_table_meta(id: u64) -> TableMeta {
        TableMeta {
            id,
            level: 0,
            size: 1024,
            entry_count: 100,
            min_key: vec![1, 2, 3],
            max_key: vec![9, 8, 7],
        }
    }

    #[test]
    fn test_manifest_new() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        let header = manifest.header.read().unwrap();
        assert_eq!(header.current_seq, 0);
        assert_eq!(header.next_table_id, 0);

        Ok(())
    }

    #[test]
    fn test_manifest_append_flush() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        let seq = manifest.next_seq();
        let edit = VersionEdit::Flush {
            seq,
            table: create_test_table_meta(0),
            wal_id: 10,
        };

        manifest.append(edit.clone())?;
        manifest.sync()?;

        let edits: Vec<_> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0], edit);

        Ok(())
    }

    #[test]
    fn test_manifest_two_phase_compaction() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        let begin_seq = manifest.next_seq();
        let begin_edit = VersionEdit::BeginCompaction {
            seq: begin_seq,
            job_id: 100,
            source_level: 0,
            target_level: 1,
        };
        manifest.append(begin_edit.clone())?;

        let commit_seq = manifest.next_seq();
        let commit_edit = VersionEdit::CommitCompaction {
            seq: commit_seq,
            job_id: 100,
            source_level: 0,
            deleted_tables: vec![1, 2],
            target_level: 1,
            added_tables: vec![create_test_table_meta(3)],
        };
        manifest.append(commit_edit.clone())?;
        manifest.sync()?;

        let edits: Vec<_> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(edits.len(), 2);
        assert_eq!(edits[0], begin_edit);
        assert_eq!(edits[1], commit_edit);

        Ok(())
    }

    #[test]
    fn test_manifest_snapshot() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        let seq = manifest.next_seq();
        let snapshot = VersionEdit::Snapshot {
            seq,
            levels: vec![LevelMeta {
                level: 0,
                tables: vec![create_test_table_meta(1)],
            }],
            next_table_id: 10,
            deletable_wals: vec![1, 2, 3],
        };

        manifest.append(snapshot.clone())?;
        manifest.sync()?;

        let edits: Vec<_> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(edits.len(), 1);
        assert_eq!(edits[0], snapshot);

        Ok(())
    }

    #[test]
    fn test_manifest_multiple_edits() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        let edits = vec![
            VersionEdit::Flush {
                seq: manifest.next_seq(),
                table: create_test_table_meta(0),
                wal_id: 1,
            },
            VersionEdit::AddSSTable {
                seq: manifest.next_seq(),
                level: 1,
                table: create_test_table_meta(1),
            },
            VersionEdit::DeleteSSTable {
                seq: manifest.next_seq(),
                table_id: 0,
                level: 0,
            },
        ];

        for edit in &edits {
            manifest.append(edit.clone())?;
        }
        manifest.sync()?;

        let read_edits: Vec<_> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_edits, edits);

        Ok(())
    }

    #[test]
    fn test_manifest_recovery() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");

        {
            let manifest = Manifest::new(&manifest_path)?;
            let edit = VersionEdit::Flush {
                seq: manifest.next_seq(),
                table: create_test_table_meta(0),
                wal_id: 5,
            };
            manifest.append(edit)?;
            manifest.sync()?;
        }

        let manifest = Manifest::new(&manifest_path)?;
        let edits: Vec<_> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(edits.len(), 1);

        Ok(())
    }

    #[test]
    fn test_manifest_replay_flush() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        manifest.append(VersionEdit::Flush {
            seq: manifest.next_seq(),
            table: create_test_table_meta(0),
            wal_id: 1,
        })?;
        manifest.append(VersionEdit::Flush {
            seq: manifest.next_seq(),
            table: create_test_table_meta(1),
            wal_id: 2,
        })?;
        manifest.sync()?;

        let state = manifest.replay()?;
        assert_eq!(state.next_table_id, 2);
        assert_eq!(state.deletable_wals, vec![1, 2]);
        assert_eq!(state.levels.len(), 1);
        assert_eq!(state.levels[0].tables.len(), 2);

        Ok(())
    }

    #[test]
    fn test_manifest_replay_compaction() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        manifest.append(VersionEdit::AddSSTable {
            seq: manifest.next_seq(),
            level: 0,
            table: create_test_table_meta(0),
        })?;
        manifest.append(VersionEdit::AddSSTable {
            seq: manifest.next_seq(),
            level: 0,
            table: create_test_table_meta(1),
        })?;

        manifest.append(VersionEdit::BeginCompaction {
            seq: manifest.next_seq(),
            job_id: 100,
            source_level: 0,
            target_level: 1,
        })?;

        manifest.append(VersionEdit::CommitCompaction {
            seq: manifest.next_seq(),
            job_id: 100,
            source_level: 0,
            deleted_tables: vec![0, 1],
            target_level: 1,
            added_tables: vec![create_test_table_meta(2)],
        })?;
        manifest.sync()?;

        let state = manifest.replay()?;
        assert_eq!(state.next_table_id, 3);
        assert_eq!(state.levels[0].tables.len(), 0);
        assert_eq!(state.levels[1].tables.len(), 1);
        assert_eq!(state.levels[1].tables[0].id, 2);

        Ok(())
    }

    #[test]
    fn test_manifest_replay_snapshot() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        manifest.append(VersionEdit::AddSSTable {
            seq: manifest.next_seq(),
            level: 0,
            table: create_test_table_meta(0),
        })?;
        manifest.append(VersionEdit::AddSSTable {
            seq: manifest.next_seq(),
            level: 0,
            table: create_test_table_meta(1),
        })?;

        manifest.append(VersionEdit::Snapshot {
            seq: manifest.next_seq(),
            levels: vec![LevelMeta {
                level: 1,
                tables: vec![create_test_table_meta(5)],
            }],
            next_table_id: 10,
            deletable_wals: vec![3, 4],
        })?;

        manifest.append(VersionEdit::AddSSTable {
            seq: manifest.next_seq(),
            level: 0,
            table: create_test_table_meta(10),
        })?;
        manifest.sync()?;

        let state = manifest.replay()?;
        assert_eq!(state.next_table_id, 11);
        assert_eq!(state.deletable_wals, vec![3, 4]);
        assert_eq!(state.levels[0].tables.len(), 1);
        assert_eq!(state.levels[0].tables[0].id, 10);
        assert_eq!(state.levels[1].tables.len(), 1);
        assert_eq!(state.levels[1].tables[0].id, 5);

        Ok(())
    }

    #[test]
    fn test_manifest_replay_incomplete_compaction() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");
        let manifest = Manifest::new(&manifest_path)?;

        manifest.append(VersionEdit::AddSSTable {
            seq: manifest.next_seq(),
            level: 0,
            table: create_test_table_meta(0),
        })?;

        manifest.append(VersionEdit::BeginCompaction {
            seq: manifest.next_seq(),
            job_id: 100,
            source_level: 0,
            target_level: 1,
        })?;
        manifest.sync()?;

        let state = manifest.replay()?;
        assert_eq!(state.levels[0].tables.len(), 1);
        assert_eq!(state.levels[0].tables[0].id, 0);

        Ok(())
    }
}
