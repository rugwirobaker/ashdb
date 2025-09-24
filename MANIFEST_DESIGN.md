# Manifest Design Document

## Current Problems

1. **No Versioning** - Can't support MVCC or atomic operations
2. **No Atomicity** - Operations split into separate add/delete records
3. **Unbounded Growth** - Manifest never compacts itself
4. **Linear Replay** - Slow recovery as manifest grows
5. **Limited Metadata** - Missing sequence numbers, key ranges
6. **No Corruption Recovery** - Single bad record can break recovery
7. **Inconsistent Format** - Different from WAL/Entry serialization

## Design Goals

1. **Atomic Edits** - Operations are complete units (flush, compaction)
2. **Versioned** - Every edit has sequence number for MVCC support
3. **Self-Compacting** - Periodic snapshots keep size bounded
4. **Fast Recovery** - Latest snapshot + deltas = current state
5. **Unified Format** - Use same Entry serialization as WAL
6. **Corruption Tolerant** - Skip bad records, continue recovery
7. **Explicit Operations** - Flush/Compaction are first-class, not generic add/delete

## Architecture

### Version-Based Manifest

The manifest tracks LSM tree evolution through atomic version edits:

```
┌─────────────────────────────────────────┐
│          MANIFEST FILE STRUCTURE         │
├─────────────────────────────────────────┤
│ Header (64 bytes, fixed)                │
│  - Magic: [u8; 8] = b"ASHDB\0MF"        │
│  - Version: u32 = 1                     │
│  - Current Seq: u64                     │
│  - Next Table ID: u64                   │
│  - Snapshot Interval: u32               │
│  - Reserved: [u8; 28]                   │
├─────────────────────────────────────────┤
│ VersionEdit 1                           │
│  [type:u8][seq:u64][edit_bytes][crc32:u32] │
├─────────────────────────────────────────┤
│ VersionEdit 2                           │
├─────────────────────────────────────────┤
│ ...                                     │
└─────────────────────────────────────────┘
```

### VersionEdit Types

Instead of generic add/delete, use operation-specific edits:

```rust
pub enum VersionEdit {
    // Atomic flush: memtable -> SSTable at level 0
    Flush {
        seq: u64,
        table: TableMeta,
        wal_id: u64, // WAL that was flushed (can be deleted)
    },

    // Atomic compaction: merge tables from level N to N+1
    Compaction {
        seq: u64,
        source_level: u32,
        deleted_tables: Vec<u64>,     // Table IDs removed
        target_level: u32,
        added_tables: Vec<TableMeta>, // New tables created
    },

    // Full snapshot - allows truncating older records
    Snapshot {
        seq: u64,
        levels: Vec<LevelMeta>,
        next_table_id: u64,
        deletable_wals: Vec<u64>,
    },

    // Manual operations (recovery, import, etc.)
    AddTable {
        seq: u64,
        level: u32,
        table: TableMeta,
    },

    DeleteTable {
        seq: u64,
        table_id: u64,
        level: u32,
    },
}

pub struct TableMeta {
    pub id: u64,
    pub level: u32,
    pub size: u64,
    pub entry_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

pub struct LevelMeta {
    pub level: u32,
    pub tables: Vec<TableMeta>,
}
```

### Why This Design

**Atomicity:**
- `Flush` = "add SSTable + mark WAL deletable" in one record
- `Compaction` = "remove N tables, add M tables" atomically
- Recovery sees complete operations, never partial state

**Versioning:**
- Each edit has sequence number
- Can reconstruct any historical version
- Enables MVCC snapshots (future)

**Compaction-Aware:**
- `Compaction` edit captures full operation
- Recovery knows which tables were compacted together
- Can detect incomplete compactions

**Self-Compacting:**
- Periodic `Snapshot` record with complete state
- Truncate everything before snapshot
- Keeps manifest bounded in size

## Implementation

### Header

```rust
// src/manifest/header.rs

pub const HEADER_SIZE: usize = 64;
const MAGIC: &[u8; 8] = b"ASHDB\0MF";
const VERSION: u32 = 1;
const DEFAULT_SNAPSHOT_INTERVAL: u32 = 100; // Snapshot every 100 edits

pub struct ManifestHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub current_seq: u64,
    pub next_table_id: u64,
    pub snapshot_interval: u32,
}

impl ManifestHeader {
    pub fn new() -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            current_seq: 0,
            next_table_id: 0,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != *MAGIC {
            return Err(Error::InvalidManifestMagic);
        }
        if self.version != VERSION {
            return Err(Error::UnsupportedManifestVersion(self.version));
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        (&mut buf[8..12]).write_u32::<BigEndian>(self.version).unwrap();
        (&mut buf[12..20]).write_u64::<BigEndian>(self.current_seq).unwrap();
        (&mut buf[20..28]).write_u64::<BigEndian>(self.next_table_id).unwrap();
        (&mut buf[28..32]).write_u32::<BigEndian>(self.snapshot_interval).unwrap();
        // Reserved bytes remain zero
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);

        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;

        let version = cursor.read_u32::<BigEndian>()?;
        let current_seq = cursor.read_u64::<BigEndian>()?;
        let next_table_id = cursor.read_u64::<BigEndian>()?;
        let snapshot_interval = cursor.read_u32::<BigEndian>()?;

        let header = Self {
            magic,
            version,
            current_seq,
            next_table_id,
            snapshot_interval,
        };
        header.validate()?;
        Ok(header)
    }
}
```

### VersionEdit Serialization

```rust
// src/manifest/edit.rs

use crc::{Crc, CRC_32_ISCSI};

const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

// Edit type tags
const FLUSH: u8 = 0x01;
const COMPACTION: u8 = 0x02;
const SNAPSHOT: u8 = 0x03;
const ADD_TABLE: u8 = 0x04;
const DELETE_TABLE: u8 = 0x05;

impl VersionEdit {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            VersionEdit::Flush { seq, table, wal_id } => {
                buf.write_u8(FLUSH).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*wal_id).unwrap();
                table.encode_into(&mut buf);
            }

            VersionEdit::Compaction {
                seq,
                source_level,
                deleted_tables,
                target_level,
                added_tables,
            } => {
                buf.write_u8(COMPACTION).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u32::<BigEndian>(*source_level).unwrap();

                // Deleted table IDs
                buf.write_u32::<BigEndian>(deleted_tables.len() as u32).unwrap();
                for id in deleted_tables {
                    buf.write_u64::<BigEndian>(*id).unwrap();
                }

                buf.write_u32::<BigEndian>(*target_level).unwrap();

                // Added tables
                buf.write_u32::<BigEndian>(added_tables.len() as u32).unwrap();
                for table in added_tables {
                    table.encode_into(&mut buf);
                }
            }

            VersionEdit::Snapshot {
                seq,
                levels,
                next_table_id,
                deletable_wals,
            } => {
                buf.write_u8(SNAPSHOT).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*next_table_id).unwrap();

                // Deletable WALs
                buf.write_u32::<BigEndian>(deletable_wals.len() as u32).unwrap();
                for wal_id in deletable_wals {
                    buf.write_u64::<BigEndian>(*wal_id).unwrap();
                }

                // Levels
                buf.write_u32::<BigEndian>(levels.len() as u32).unwrap();
                for level in levels {
                    level.encode_into(&mut buf);
                }
            }

            VersionEdit::AddTable { seq, level, table } => {
                buf.write_u8(ADD_TABLE).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u32::<BigEndian>(*level).unwrap();
                table.encode_into(&mut buf);
            }

            VersionEdit::DeleteTable { seq, table_id, level } => {
                buf.write_u8(DELETE_TABLE).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*table_id).unwrap();
                buf.write_u32::<BigEndian>(*level).unwrap();
            }
        }

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);
        let edit_type = cursor.read_u8()?;

        match edit_type {
            FLUSH => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let wal_id = cursor.read_u64::<BigEndian>()?;
                let table = TableMeta::decode_from(&mut cursor)?;
                Ok(VersionEdit::Flush { seq, table, wal_id })
            }

            COMPACTION => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let source_level = cursor.read_u32::<BigEndian>()?;

                let deleted_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut deleted_tables = Vec::with_capacity(deleted_count);
                for _ in 0..deleted_count {
                    deleted_tables.push(cursor.read_u64::<BigEndian>()?);
                }

                let target_level = cursor.read_u32::<BigEndian>()?;

                let added_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut added_tables = Vec::with_capacity(added_count);
                for _ in 0..added_count {
                    added_tables.push(TableMeta::decode_from(&mut cursor)?);
                }

                Ok(VersionEdit::Compaction {
                    seq,
                    source_level,
                    deleted_tables,
                    target_level,
                    added_tables,
                })
            }

            SNAPSHOT => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let next_table_id = cursor.read_u64::<BigEndian>()?;

                let wal_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut deletable_wals = Vec::with_capacity(wal_count);
                for _ in 0..wal_count {
                    deletable_wals.push(cursor.read_u64::<BigEndian>()?);
                }

                let level_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut levels = Vec::with_capacity(level_count);
                for _ in 0..level_count {
                    levels.push(LevelMeta::decode_from(&mut cursor)?);
                }

                Ok(VersionEdit::Snapshot {
                    seq,
                    levels,
                    next_table_id,
                    deletable_wals,
                })
            }

            ADD_TABLE => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let level = cursor.read_u32::<BigEndian>()?;
                let table = TableMeta::decode_from(&mut cursor)?;
                Ok(VersionEdit::AddTable { seq, level, table })
            }

            DELETE_TABLE => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let table_id = cursor.read_u64::<BigEndian>()?;
                let level = cursor.read_u32::<BigEndian>()?;
                Ok(VersionEdit::DeleteTable { seq, table_id, level })
            }

            _ => Err(Error::InvalidEditType(edit_type)),
        }
    }
}

impl TableMeta {
    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(self.id).unwrap();
        buf.write_u32::<BigEndian>(self.level).unwrap();
        buf.write_u64::<BigEndian>(self.size).unwrap();
        buf.write_u64::<BigEndian>(self.entry_count).unwrap();

        // Min key
        buf.write_u32::<BigEndian>(self.min_key.len() as u32).unwrap();
        buf.extend_from_slice(&self.min_key);

        // Max key
        buf.write_u32::<BigEndian>(self.max_key.len() as u32).unwrap();
        buf.extend_from_slice(&self.max_key);
    }

    fn decode_from(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        let id = cursor.read_u64::<BigEndian>()?;
        let level = cursor.read_u32::<BigEndian>()?;
        let size = cursor.read_u64::<BigEndian>()?;
        let entry_count = cursor.read_u64::<BigEndian>()?;

        let min_key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut min_key = vec![0u8; min_key_len];
        cursor.read_exact(&mut min_key)?;

        let max_key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut max_key = vec![0u8; max_key_len];
        cursor.read_exact(&mut max_key)?;

        Ok(TableMeta {
            id,
            level,
            size,
            entry_count,
            min_key,
            max_key,
        })
    }
}

impl LevelMeta {
    fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.write_u32::<BigEndian>(self.level).unwrap();
        buf.write_u32::<BigEndian>(self.tables.len() as u32).unwrap();
        for table in &self.tables {
            table.encode_into(buf);
        }
    }

    fn decode_from(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        let level = cursor.read_u32::<BigEndian>()?;
        let table_count = cursor.read_u32::<BigEndian>()? as usize;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            tables.push(TableMeta::decode_from(cursor)?);
        }
        Ok(LevelMeta { level, tables })
    }
}
```

### Manifest

```rust
// src/manifest/mod.rs

pub struct Manifest {
    file: File,
    writer: Mutex<BufWriter<File>>,
    header: RwLock<ManifestHeader>,
    path: PathBuf,
    edit_count: AtomicU32, // Edits since last snapshot
}

impl Manifest {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let writer = BufWriter::new(file.try_clone()?);

        // Read or create header
        let header = if file.metadata()?.len() == 0 {
            let h = ManifestHeader::new();
            let header_bytes = h.encode();
            file.write_all_at(&header_bytes, 0)?;
            file.sync_all()?;
            h
        } else {
            let mut buf = [0u8; HEADER_SIZE];
            let mut reader = file.try_clone()?;
            reader.read_exact(&mut buf)?;
            ManifestHeader::decode(&buf)?
        };

        Ok(Self {
            file,
            writer: Mutex::new(writer),
            header: RwLock::new(header),
            path,
            edit_count: AtomicU32::new(0),
        })
    }

    pub fn append(&mut self, edit: VersionEdit) -> Result<()> {
        let edit_bytes = edit.encode();
        let checksum = CRC32.checksum(&edit_bytes);

        let mut writer = self.writer.lock()
            .map_err(|_| Error::MutexPoisoned)?;

        // Write: [type:u8 + edit_bytes][crc32:u32]
        writer.write_all(&edit_bytes)?;
        writer.write_u32::<BigEndian>(checksum)?;

        // Update header
        let seq = match &edit {
            VersionEdit::Flush { seq, .. }
            | VersionEdit::Compaction { seq, .. }
            | VersionEdit::Snapshot { seq, .. }
            | VersionEdit::AddTable { seq, .. }
            | VersionEdit::DeleteTable { seq, .. } => *seq,
        };

        self.header.write()
            .map_err(|_| Error::MutexPoisoned)?
            .current_seq = seq;

        // Track edit count for auto-snapshot
        let count = self.edit_count.fetch_add(1, Ordering::SeqCst) + 1;

        // Auto-snapshot if needed
        if count >= self.header.read().unwrap().snapshot_interval {
            // Note: actual snapshot would be done by caller
            self.edit_count.store(0, Ordering::SeqCst);
        }

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        // Flush buffer
        self.writer.lock()
            .map_err(|_| Error::MutexPoisoned)?
            .flush()?;

        // Update header
        let header = self.header.read()
            .map_err(|_| Error::MutexPoisoned)?;
        let header_bytes = header.encode();
        drop(header);

        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_bytes)?;
        file.sync_all()?;

        Ok(())
    }

    pub fn iter(&self) -> Result<ManifestIterator> {
        let mut reader = BufReader::new(self.file.try_clone()?);
        reader.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        Ok(ManifestIterator { reader })
    }

    pub fn next_table_id(&self) -> u64 {
        self.header.write().unwrap().next_table_id += 1;
        self.header.read().unwrap().next_table_id - 1
    }

    pub fn next_seq(&self) -> u64 {
        self.header.write().unwrap().current_seq += 1;
        self.header.read().unwrap().current_seq
    }
}
```

### Recovery Iterator

```rust
pub struct ManifestIterator {
    reader: BufReader<File>,
}

impl Iterator for ManifestIterator {
    type Item = Result<VersionEdit>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read edit type (first byte of edit)
        let edit_type = match self.reader.read_u8() {
            Ok(t) => t,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return None; // Clean EOF
            }
            Err(e) => return Some(Err(e.into())),
        };

        // Read rest of edit based on type (need to peek size)
        // For simplicity, read until we can decode
        let mut edit_bytes = vec![edit_type];

        // Read sequence number (always present)
        let mut seq_buf = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut seq_buf) {
            return Some(Err(Error::CorruptedManifest(
                format!("Failed to read sequence: {}", e)
            )));
        }
        edit_bytes.extend_from_slice(&seq_buf);

        // Read rest based on type (simplified - in practice would need proper framing)
        // For now, read a large buffer and let decode handle it
        let mut rest = Vec::new();
        loop {
            let mut buf = [0u8; 1024];
            match self.reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => rest.extend_from_slice(&buf[..n]),
                Err(e) => return Some(Err(e.into())),
            }
            // Try to decode - if successful, we have enough
            edit_bytes.extend_from_slice(&rest);
            if let Ok(edit) = VersionEdit::decode(&edit_bytes) {
                // Read and verify checksum
                let stored_crc = match self.reader.read_u32::<BigEndian>() {
                    Ok(crc) => crc,
                    Err(e) => return Some(Err(Error::CorruptedManifest(
                        format!("Failed to read checksum: {}", e)
                    ))),
                };

                let computed_crc = CRC32.checksum(&edit_bytes);
                if computed_crc != stored_crc {
                    return Some(Err(Error::ChecksumMismatch));
                }

                return Some(Ok(edit));
            }
        }

        Some(Err(Error::CorruptedManifest("Incomplete edit".into())))
    }
}
```

### Recovery Algorithm

```rust
pub struct LsmState {
    pub levels: Vec<Level>,
    pub next_table_id: u64,
    pub deletable_wals: Vec<u64>,
}

impl LsmStore {
    fn recover_from_manifest(dir: &Path, manifest: &Manifest) -> Result<LsmState> {
        let mut state = LsmState::new();
        let mut last_snapshot_seq = 0;

        for edit in manifest.iter()? {
            match edit? {
                VersionEdit::Snapshot { seq, levels, next_table_id, deletable_wals } => {
                    // Full state replacement
                    state = LsmState::from_snapshot(levels, next_table_id, deletable_wals);
                    last_snapshot_seq = seq;
                }

                VersionEdit::Flush { seq, table, wal_id } => {
                    if seq <= last_snapshot_seq {
                        continue; // Skip - already in snapshot
                    }
                    state.add_table(0, table);
                    state.mark_wal_deletable(wal_id);
                }

                VersionEdit::Compaction {
                    seq,
                    source_level,
                    deleted_tables,
                    target_level,
                    added_tables,
                } => {
                    if seq <= last_snapshot_seq {
                        continue;
                    }
                    for id in deleted_tables {
                        state.remove_table(source_level, id);
                    }
                    for table in added_tables {
                        state.add_table(target_level, table);
                    }
                }

                VersionEdit::AddTable { seq, level, table } => {
                    if seq <= last_snapshot_seq {
                        continue;
                    }
                    state.add_table(level, table);
                }

                VersionEdit::DeleteTable { seq, table_id, level } => {
                    if seq <= last_snapshot_seq {
                        continue;
                    }
                    state.remove_table(level, table_id);
                }
            }
        }

        Ok(state)
    }
}
```

## Usage Examples

### Flush Operation

```rust
// Flush memtable to SSTable
fn flush_memtable(&mut self) -> Result<()> {
    let memtable = self.frozen_memtables.pop_front().unwrap();
    let wal_id = memtable.wal().id()?;

    // Create SSTable
    let table_id = self.manifest.next_table_id();
    let table_path = self.dir.join(format!("{}.sst", table_id));
    let mut sstable = Table::writable(&table_path)?;

    // Flush to disk
    memtable.flush(&mut sstable)?;
    let table = sstable.finalize()?;

    // Get metadata
    let table_meta = TableMeta {
        id: table_id,
        level: 0,
        size: table.size(),
        entry_count: table.entry_count(),
        min_key: table.min_key().to_vec(),
        max_key: table.max_key().to_vec(),
    };

    // Record in manifest (atomic)
    let seq = self.manifest.next_seq();
    self.manifest.append(VersionEdit::Flush {
        seq,
        table: table_meta.clone(),
        wal_id,
    })?;
    self.manifest.sync()?;

    // Add to level 0
    self.levels[0].add_table(table_meta);

    Ok(())
}
```

### Compaction Operation

```rust
fn compact_level(&mut self, level: u32) -> Result<()> {
    // Pick tables to compact
    let source_tables = self.levels[level as usize].pick_compaction()?;
    let target_tables = self.levels[(level + 1) as usize].overlapping(&source_tables)?;

    // Merge tables
    let new_tables = self.merge_tables(&source_tables, &target_tables)?;

    // Record in manifest (atomic)
    let seq = self.manifest.next_seq();
    self.manifest.append(VersionEdit::Compaction {
        seq,
        source_level: level,
        deleted_tables: source_tables.iter().map(|t| t.id).collect(),
        target_level: level + 1,
        added_tables: new_tables.clone(),
    })?;
    self.manifest.sync()?;

    // Apply to in-memory state
    for table in &source_tables {
        self.levels[level as usize].remove_table(table.id);
    }
    for table in &target_tables {
        self.levels[(level + 1) as usize].remove_table(table.id);
    }
    for table in new_tables {
        self.levels[(level + 1) as usize].add_table(table);
    }

    Ok(())
}
```

### Snapshot Operation

```rust
fn maybe_snapshot(&mut self) -> Result<()> {
    if self.manifest.edit_count.load(Ordering::SeqCst)
        < self.manifest.header.read().unwrap().snapshot_interval
    {
        return Ok(());
    }

    // Collect current state
    let levels = self.levels.iter()
        .map(|l| l.to_meta())
        .collect();

    let seq = self.manifest.next_seq();
    let next_table_id = self.manifest.header.read().unwrap().next_table_id;

    self.manifest.append(VersionEdit::Snapshot {
        seq,
        levels,
        next_table_id,
        deletable_wals: self.deletable_wals.clone(),
    })?;
    self.manifest.sync()?;

    // Optional: truncate old records by creating new manifest file
    // self.compact_manifest()?;

    Ok(())
}
```

## Benefits

1. **Atomic Operations** - Flush/Compaction are single records
2. **Fast Recovery** - Latest snapshot + deltas
3. **Bounded Size** - Snapshots allow truncation
4. **MVCC Ready** - Sequence numbers track versions
5. **Corruption Tolerant** - Skip bad records, continue
6. **Unified Format** - Same CRC32C, serialization patterns as WAL
7. **Self-Documenting** - Operation types explicit, not inferred

## Testing Strategy

1. **Roundtrip Tests** - Encode/decode each VersionEdit type
2. **Recovery Tests** - Build state, recover, compare
3. **Snapshot Tests** - Verify truncation, state preservation
4. **Corruption Handling** - Bad checksums, incomplete edits
5. **Concurrent Recovery** - Multiple readers
6. **Compaction Tests** - Verify atomic application

## Performance Considerations

- CRC32C checksums (same as WAL)
- Buffered writes with explicit sync
- Snapshot interval tunable (default: 100 edits)
- In-memory header updates, periodic flush

## Future Enhancements

1. **Two-Phase Commit** - BeginCompaction/CommitCompaction for safety
2. **Manifest Compaction** - Create new file, truncate old
3. **Multiple Versions** - Keep N snapshots for rollback
4. **Compression** - Compress large snapshots
5. **MVCC Sequences** - Track min/max sequence per table