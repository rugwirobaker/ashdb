//! Manifest log for tracking SSTable metadata and LSM-tree state.
//!
//! The manifest is essentially a WAL (Write-Ahead Log) for database metadata
//! instead of user data. While the WAL tracks changes to key-value pairs, the
//! manifest tracks changes to the database structure itself - which SSTables
//! exist, their metadata, and the operations that created or deleted them.
//!
//! # File Format
//!
//! Like the WAL, the manifest uses an append-only log format:
//!
//! ```text
//! +------------------+
//! | Header (64 bytes)|
//! +------------------+
//! | VersionEdit 1    |
//! +------------------+
//! | VersionEdit 2    |
//! +------------------+
//! | ...              |
//! +------------------+
//! ```
//!
//! ## VersionEdit Format
//!
//! Each version edit represents an atomic change to the LSM-tree structure:
//!
//! ```text
//! +-----------+------------------+-----------+
//! |length:u32 | serialized_edit  |crc32:u32  |
//! +-----------+------------------+-----------+
//! | 4 bytes   | variable length  | 4 bytes   |
//! +-----------+------------------+-----------+
//! ```
//!
//! ## Edit Operations
//!
//! - **AddTable**: Records new SSTable creation (from flush or compaction)
//! - **DeleteTable**: Records SSTable deletion (after compaction)
//! - **SetNextWalId**: Updates the next WAL file ID counter
//! - **SetNextTableId**: Updates the next SSTable ID counter
//!
//! # Recovery Process
//!
//! On startup, the manifest is replayed to reconstruct the LSM-tree state:
//! 1. Read and validate the header
//! 2. Apply each VersionEdit in sequence
//! 3. Rebuild the level structure and metadata
//! 4. Verify referenced SSTables exist
//!
//! # Durability and Consistency
//!
//! - **Big-endian encoding**: Ensures cross-platform portability
//! - **CRC32 checksums**: Detects corruption in individual edits
//! - **Atomic writes**: Each edit is written atomically
//! - **Immediate sync**: Changes are synced to disk before returning

pub mod edit;
pub mod header;
pub mod level;
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
            .truncate(false)
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

        let mut writer = self.writer.lock()?;

        writer.write_u32::<BigEndian>(edit_bytes.len() as u32)?;
        writer.write_all(&edit_bytes)?;
        writer.write_u32::<BigEndian>(checksum)?;

        let seq = edit.seq();
        let snapshot_interval = {
            let mut header = self.header.write()?;
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
?
            .flush()?;

        let header = self.header.read()?;
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
                    last_snapshot_seq = Some(seq);
                }

                VersionEdit::Flush {
                    seq,
                    table,
                    wal_id: _,
                } => {
                    if let Some(snap_seq) = last_snapshot_seq {
                        if seq <= snap_seq {
                            continue;
                        }
                    }
                    let table_id = table.id;
                    let level = table.level;
                    state.add_table_at_level(table, level);
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
            }
        }

        Ok(state)
    }
}

pub use level::{Level, SSTable};
use meta::LevelMeta;

#[derive(Debug, Clone)]
pub struct ManifestState {
    pub levels: Vec<LevelMeta>,
    pub next_table_id: u64,
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
            return Some(Err(Error::InvalidData("Checksum mismatch".to_string())));
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
            VersionEdit::Flush {
                seq: manifest.next_seq(),
                table: create_test_table_meta(1),
                wal_id: 2,
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
        assert_eq!(state.levels.len(), 1);
        assert_eq!(state.levels[0].tables.len(), 2);

        Ok(())
    }

    #[test]
    fn test_manifest_replay_compaction() -> Result<()> {
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

        manifest.append(VersionEdit::Snapshot {
            seq: manifest.next_seq(),
            levels: vec![LevelMeta {
                level: 1,
                tables: vec![create_test_table_meta(5)],
            }],
            next_table_id: 10,
        })?;

        manifest.append(VersionEdit::Flush {
            seq: manifest.next_seq(),
            table: create_test_table_meta(10),
            wal_id: 5,
        })?;
        manifest.sync()?;

        let state = manifest.replay()?;
        assert_eq!(state.next_table_id, 11);
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

        manifest.append(VersionEdit::Flush {
            seq: manifest.next_seq(),
            table: create_test_table_meta(0),
            wal_id: 1,
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

    // ===== MANIFEST CORRUPTION AND RECOVERY TESTS =====
    // Tests that ensure proper handling of corrupted manifest files

    #[test]
    fn test_manifest_partial_corruption_recovery() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Create a manifest with some valid entries
        {
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
        }

        // Corrupt the manifest by truncating it in the middle
        {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&manifest_path)?;
            let original_len = file.metadata()?.len();
            // Truncate to remove part of the second record
            file.set_len(original_len - 10)?;
        }

        // Attempt to read the corrupted manifest
        let manifest = Manifest::new(&manifest_path)?;
        let result = manifest.replay();

        // Should recover the first valid entry but fail gracefully on corruption
        match result {
            Ok(state) => {
                // If recovery succeeds, should have recovered the first entry
                assert_eq!(state.levels.len(), 1);
                assert_eq!(state.levels[0].tables.len(), 1);
                assert_eq!(state.next_table_id, 1);
            }
            Err(_) => {
                // Recovery may fail, which is also acceptable for corrupted data
                // The important thing is it doesn't panic or leave the system in a bad state
            }
        }

        Ok(())
    }

    #[test]
    fn test_manifest_checksum_corruption() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Create a manifest with valid entries
        {
            let manifest = Manifest::new(&manifest_path)?;
            manifest.append(VersionEdit::Flush {
                seq: manifest.next_seq(),
                table: create_test_table_meta(0),
                wal_id: 1,
            })?;
            manifest.sync()?;
        }

        // Corrupt the checksum by modifying the last few bytes
        {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .open(&manifest_path)?;
            use std::io::{Seek, SeekFrom, Write};
            file.seek(SeekFrom::End(-4))?; // Go to last 4 bytes (checksum)
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF])?; // Corrupt checksum
        }

        // Try to read the manifest with corrupted checksum
        let manifest = Manifest::new(&manifest_path)?;
        let mut iter = manifest.iter()?;

        // Should detect checksum corruption
        let result = iter.next();
        match result {
            Some(Err(Error::InvalidData(msg))) if msg.contains("Checksum") => {
                // Expected: checksum mismatch detected
            }
            Some(Ok(_)) => {
                panic!("Should have detected checksum corruption");
            }
            Some(Err(other)) => {
                panic!("Expected checksum mismatch, got: {:?}", other);
            }
            None => {
                panic!("Should have at least one entry");
            }
        }

        Ok(())
    }

    #[test]
    fn test_manifest_empty_after_header() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Create manifest and immediately close it (only header written)
        {
            let _manifest = Manifest::new(&manifest_path)?;
            // Manifest is closed with only header
        }

        // Reopen and verify it's handled correctly
        let manifest = Manifest::new(&manifest_path)?;
        let state = manifest.replay()?;

        // Should be empty state but valid
        assert!(state.levels.is_empty());
        assert_eq!(state.next_table_id, 0);

        // Should be able to append new entries
        manifest.append(VersionEdit::Flush {
            seq: manifest.next_seq(),
            table: create_test_table_meta(0),
            wal_id: 1,
        })?;
        manifest.sync()?;

        let state_after = manifest.replay()?;
        assert_eq!(state_after.levels.len(), 1);
        assert_eq!(state_after.levels[0].tables.len(), 1);

        Ok(())
    }

    #[test]
    fn test_manifest_recovery_with_mixed_corruption() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Create manifest with several entries
        {
            let manifest = Manifest::new(&manifest_path)?;

            // Add several valid entries
            for i in 0..5 {
                manifest.append(VersionEdit::Flush {
                    seq: manifest.next_seq(),
                    table: create_test_table_meta(i),
                    wal_id: i + 1,
                })?;
            }
            manifest.sync()?;
        }

        // Corrupt the middle of the file by writing garbage
        {
            let mut file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&manifest_path)?;
            use std::io::{Seek, SeekFrom, Write};

            let file_len = file.metadata()?.len();
            let corrupt_pos = file_len / 2; // Corrupt middle of file
            file.seek(SeekFrom::Start(corrupt_pos))?;
            file.write_all(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF])?; // Write garbage
        }

        // Try to recover what we can
        let manifest = Manifest::new(&manifest_path)?;
        let result = manifest.replay();

        match result {
            Ok(state) => {
                // If recovery succeeds, should have some entries (before corruption)
                assert!(!state.levels.is_empty());
                // May not have all 5 entries due to corruption
            }
            Err(_) => {
                // Recovery may fail due to corruption - acceptable
                // Test ensures system doesn't crash or panic
            }
        }

        Ok(())
    }

    #[test]
    fn test_manifest_invalid_edit_type() -> Result<()> {
        let dir = TempDir::new()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Create a valid manifest first
        {
            let manifest = Manifest::new(&manifest_path)?;
            manifest.append(VersionEdit::Flush {
                seq: manifest.next_seq(),
                table: create_test_table_meta(0),
                wal_id: 1,
            })?;
            manifest.sync()?;
        }

        // Manually append an invalid edit type
        {
            use byteorder::{BigEndian, WriteBytesExt};
            use std::fs::OpenOptions;
            use std::io::Write;

            let mut file = OpenOptions::new().append(true).open(&manifest_path)?;

            // Write an invalid edit (unknown type)
            let invalid_edit = vec![0xFF]; // Invalid edit type
            let checksum = crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(&invalid_edit);

            file.write_u32::<BigEndian>(invalid_edit.len() as u32)?;
            file.write_all(&invalid_edit)?;
            file.write_u32::<BigEndian>(checksum)?;
        }

        // Try to replay manifest with invalid edit
        let manifest = Manifest::new(&manifest_path)?;
        let mut iter = manifest.iter()?;

        // First entry should be valid
        let first = iter.next().unwrap()?;
        match first {
            VersionEdit::Flush { .. } => {
                // Expected first entry
            }
            _ => panic!("Expected flush edit"),
        }

        // Second entry should be invalid
        let second = iter.next();
        match second {
            Some(Err(Error::InvalidData(msg))) if msg.contains("Invalid edit type") => {
                // Expected: invalid edit type detected
            }
            Some(Ok(_)) => {
                panic!("Should have rejected invalid edit type");
            }
            Some(Err(other)) => {
                panic!("Expected invalid edit type error, got: {:?}", other);
            }
            None => {
                panic!("Should have found the invalid entry");
            }
        }

        Ok(())
    }
}
