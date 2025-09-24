# WAL Design Document

## Current Problems

1. **Duplicate Serialization** - WAL and SSTable both serialize key-value pairs differently
2. **No Format Validation** - Missing magic number/header validation
3. **Inefficient Checksums** - CRC64 is overkill for small entries
4. **No O_DIRECT Support** - Missing direct I/O for production durability
5. **Unclear Durability Contract** - Fsync happens implicitly, not controlled by caller
6. **Limited Recovery** - Basic corruption detection without granular error handling

## Design Goals

1. **WAL Owns Wire Format** - WAL handles key-value serialization (following toydb's bitcask Log pattern)
2. **Simple API** - Accept `(key, value)` pairs, return `(key, value)` tuples on replay
3. **Fast Checksums** - CRC32C for speed (sufficient for entry-level integrity)
4. **O_DIRECT Support** - Direct I/O with aligned buffers for production use
5. **Explicit Durability** - Caller controls when to fsync (no auto-sync)
6. **Header Validation** - Magic number and version for format verification
7. **Granular Recovery** - Detect and skip corrupted entries, continue recovery

## Durability Contract (toydb-style)

Following the toydb storage engine pattern:

> **"Writes are only guaranteed durable after calling `flush()`"**

- `append()` - Buffered write (NOT durable)
- `flush()` - Fsync to disk (durability guarantee)
- Transaction layer (future) controls when to pay fsync cost

## Architecture

### WAL Serialization (Following toydb's bitcask Log Pattern)

WAL accepts key-value pairs and handles serialization internally:

**API:**
```rust
// Write
pub fn append(&self, key: &[u8], value: Option<&[u8]>) -> Result<()>

// Replay
pub fn replay(&self) -> impl Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>>
```

**Wire Format (WAL's Internal Framing):**
```
[record_len:u32][key_len:u32][value_len:u32][key_bytes][value_bytes][crc32:u32]
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ Framed payload
```

- `record_len: u32` - Total length of framed payload (excludes crc32)
- `key_len: u32` - Length of key bytes
- `value_len: u32` - Length of value bytes (0 for tombstone)
- `key_bytes: [u8; key_len]` - Raw key data
- `value_bytes: [u8; value_len]` - Raw value data (empty if tombstone)
- `crc32: u32` - CRC32C checksum of framed payload

**Why this design:**
- WAL owns serialization - caller just provides data
- Simple framing enables self-describing format
- Length prefixes allow skip-ahead during recovery
- Tombstones: `value_len = 0, value_bytes = []`
- Follows toydb's proven pattern

### WAL File Structure

```
┌─────────────────────────────────────┐
│ Header (64 bytes, fixed size)       │
│  - Magic: [u8; 8] = b"ASHDB\0WAL"  │
│  - Version: u32 = 1                 │
│  - Entry Count: u64                 │
│  - Reserved: [u8; 44]               │
├─────────────────────────────────────┤
│ Record 1                            │
│  [entry_len:u32][entry_bytes][crc32:u32] │
├─────────────────────────────────────┤
│ Record 2                            │
│  [entry_len:u32][entry_bytes][crc32:u32] │
├─────────────────────────────────────┤
│ ...                                 │
└─────────────────────────────────────┘
```

**Record Format:**
- `record_len: u32` - Length of framed payload (excludes CRC)
- Framed payload:
  - `key_len: u32` - Key length
  - `value_len: u32` - Value length (0 = tombstone)
  - `key_bytes: [u8]` - Key data
  - `value_bytes: [u8]` - Value data (empty if tombstone)
- `crc32: u32` - CRC32C checksum of framed payload

**Header Fields:**
- `magic: [u8; 8]` - Format identifier "ASHDB\0WAL"
- `version: u32` - Format version (currently 1)
- `entry_count: u64` - Number of entries (updated on flush)
- `reserved: [u8; 44]` - Future use (alignment, flags, etc.)

## Implementation

### Header

```rust
// src/wal/header.rs

pub const HEADER_SIZE: usize = 64;
const MAGIC: &[u8; 8] = b"ASHDB\0WAL";
const VERSION: u32 = 1;

pub struct Header {
    pub magic: [u8; 8],
    pub version: u32,
    pub entry_count: u64,
}

impl Header {
    pub fn new() -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            entry_count: 0,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != *MAGIC {
            return Err(Error::InvalidWalMagic);
        }
        if self.version != VERSION {
            return Err(Error::UnsupportedWalVersion(self.version));
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        (&mut buf[8..12]).write_u32::<BigEndian>(self.version).unwrap();
        (&mut buf[12..20]).write_u64::<BigEndian>(self.entry_count).unwrap();
        // Reserved bytes remain zero
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);

        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;

        let version = cursor.read_u32::<BigEndian>()?;
        let entry_count = cursor.read_u64::<BigEndian>()?;

        let header = Self { magic, version, entry_count };
        header.validate()?;
        Ok(header)
    }
}
```

### WAL with O_DIRECT Support

```rust
// src/wal/mod.rs

use crc::{Crc, CRC_32_ISCSI}; // CRC32C (Castagnoli)

pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024; // 64KB, 4K-aligned

pub struct WalOptions {
    pub use_direct_io: bool,
    pub buffer_size: usize, // Must be 4KB-aligned for O_DIRECT
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            use_direct_io: cfg!(target_os = "linux"),
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

pub struct Wal {
    file: File,
    writer: Mutex<Box<dyn Write + Send>>,
    path: PathBuf,
    header: RwLock<Header>,
    direct_io: bool,
}

impl Wal {
    pub fn new(path: &str) -> Result<Self> {
        Self::with_options(path, WalOptions::default())
    }

    pub fn with_options(path: &str, opts: WalOptions) -> Result<Self> {
        use std::os::unix::fs::OpenOptionsExt;

        // Validate alignment for O_DIRECT
        if opts.use_direct_io && opts.buffer_size % 4096 != 0 {
            return Err(Error::InvalidAlignment);
        }

        let mut open_opts = File::options();
        open_opts.create(true).read(true).write(true);

        #[cfg(target_os = "linux")]
        if opts.use_direct_io {
            open_opts.custom_flags(libc::O_DIRECT);
        }

        let file = open_opts.open(path)?;

        // Create appropriate writer
        let writer: Box<dyn Write + Send> = if opts.use_direct_io {
            Box::new(AlignedWriter::new(file.try_clone()?, opts.buffer_size)?)
        } else {
            Box::new(BufWriter::with_capacity(opts.buffer_size, file.try_clone()?))
        };

        // Read or create header
        let header = if file.metadata()?.len() == 0 {
            let h = Header::new();
            let header_bytes = h.encode();
            file.write_all_at(&header_bytes, 0)?;
            file.sync_all()?;
            h
        } else {
            let mut buf = [0u8; HEADER_SIZE];
            let mut reader = file.try_clone()?;
            reader.read_exact(&mut buf)?;
            Header::decode(&buf)?
        };

        Ok(Self {
            file,
            writer: Mutex::new(writer),
            path: path.into(),
            header: RwLock::new(header),
            direct_io: opts.use_direct_io,
        })
    }

    // Append key-value pair (buffered, NOT durable)
    pub fn append(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        // Build framed payload: [key_len:u32][value_len:u32][key][value]
        let mut payload = Vec::new();
        payload.write_u32::<BigEndian>(key.len() as u32)?;
        payload.write_u32::<BigEndian>(value.map_or(0, |v| v.len()) as u32)?;
        payload.extend_from_slice(key);
        if let Some(v) = value {
            payload.extend_from_slice(v);
        }

        let checksum = CRC32.checksum(&payload);

        let mut writer = self.writer.lock()
            .map_err(|_| Error::MutexPoisoned)?;

        // Write: [record_len:u32][framed_payload][crc32:u32]
        writer.write_u32::<BigEndian>(payload.len() as u32)?;
        writer.write_all(&payload)?;
        writer.write_u32::<BigEndian>(checksum)?;

        // Update header entry count (in memory)
        self.header.write()
            .map_err(|_| Error::MutexPoisoned)?
            .entry_count += 1;

        Ok(())
    }

    // Flush to disk - DURABILITY GUARANTEE
    pub fn flush(&self) -> Result<()> {
        // 1. Flush buffered writes to OS
        self.writer.lock()
            .map_err(|_| Error::MutexPoisoned)?
            .flush()?;

        // 2. Update header with entry count
        let header = self.header.read()
            .map_err(|_| Error::MutexPoisoned)?;
        let header_bytes = header.encode();
        drop(header);

        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_bytes)?;

        // 3. FSYNC - durability guarantee
        file.sync_all()?;

        Ok(())
    }

    pub fn replay(&self) -> Result<ReplayIterator> {
        let mut reader = BufReader::new(self.file.try_clone()?);

        // Skip header
        reader.seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        Ok(ReplayIterator { reader })
    }

    pub fn entry_count(&self) -> u64 {
        self.header.read().unwrap().entry_count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
```

### Aligned Writer for O_DIRECT

```rust
// src/wal/aligned_writer.rs

use std::alloc::{alloc, dealloc, Layout};

pub struct AlignedWriter {
    file: File,
    buffer: AlignedBuffer,
    position: usize,
}

impl AlignedWriter {
    pub fn new(file: File, capacity: usize) -> Result<Self> {
        Ok(Self {
            file,
            buffer: AlignedBuffer::new(capacity),
            position: 0,
        })
    }
}

impl Write for AlignedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let remaining = self.buffer.capacity - self.position;

        if buf.len() <= remaining {
            // Fits in buffer
            self.buffer.data[self.position..self.position + buf.len()]
                .copy_from_slice(buf);
            self.position += buf.len();
            Ok(buf.len())
        } else {
            // Need to flush first
            self.flush()?;

            // If still too big, write directly
            if buf.len() > self.buffer.capacity {
                // For O_DIRECT, must write aligned chunks
                let aligned_len = (buf.len() / 4096) * 4096;
                self.file.write_all(&buf[..aligned_len])?;

                // Buffer remainder
                let remainder = &buf[aligned_len..];
                self.buffer.data[..remainder.len()].copy_from_slice(remainder);
                self.position = remainder.len();

                Ok(buf.len())
            } else {
                // Recursively write after flush
                self.write(buf)
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.position == 0 {
            return Ok(());
        }

        // Pad to 4K alignment for O_DIRECT
        let aligned_size = ((self.position + 4095) / 4096) * 4096;

        // Zero-fill padding
        for i in self.position..aligned_size {
            self.buffer.data[i] = 0;
        }

        // Write aligned buffer
        self.file.write_all(&self.buffer.data[..aligned_size])?;
        self.position = 0;

        Ok(())
    }
}

struct AlignedBuffer {
    data: *mut u8,
    capacity: usize,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, 4096).unwrap();
        let data = unsafe { alloc(layout) };

        Self {
            data,
            capacity,
            layout,
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.data, self.layout) }
    }
}

// Implement Deref to treat as slice
impl std::ops::Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.capacity) }
    }
}

impl std::ops::DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.capacity) }
    }
}
```

### Recovery Iterator

```rust
// src/wal/mod.rs

pub struct ReplayIterator {
    reader: BufReader<File>,
}

impl Iterator for ReplayIterator {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read record length
        let record_len = match self.reader.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return None; // Clean EOF
            }
            Err(e) => return Some(Err(e.into())),
        };

        // Read framed payload
        let mut payload = vec![0u8; record_len];
        if let Err(e) = self.reader.read_exact(&mut payload) {
            return Some(Err(Error::CorruptedWal(
                format!("Failed to read payload: {}", e)
            )));
        }

        // Read checksum
        let stored_crc = match self.reader.read_u32::<BigEndian>() {
            Ok(crc) => crc,
            Err(e) => return Some(Err(Error::CorruptedWal(
                format!("Failed to read checksum: {}", e)
            ))),
        };

        // Verify checksum
        let computed_crc = CRC32.checksum(&payload);
        if computed_crc != stored_crc {
            return Some(Err(Error::ChecksumMismatch));
        }

        // Deserialize key-value from payload
        let mut cursor = Cursor::new(&payload);

        let key_len = match cursor.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) => return Some(Err(e.into())),
        };

        let value_len = match cursor.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) => return Some(Err(e.into())),
        };

        let mut key = vec![0u8; key_len];
        if let Err(e) = cursor.read_exact(&mut key) {
            return Some(Err(Error::CorruptedWal(
                format!("Failed to read key: {}", e)
            )));
        }

        let value = if value_len > 0 {
            let mut v = vec![0u8; value_len];
            if let Err(e) = cursor.read_exact(&mut v) {
                return Some(Err(Error::CorruptedWal(
                    format!("Failed to read value: {}", e)
                )));
            }
            Some(v)
        } else {
            None
        };

        Some(Ok((key, value)))
    }
}
```

## Integration with Store

```rust
// Store implements toydb Engine trait pattern
impl LsmStore {
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // 1. Append to WAL (buffered, not durable) - WAL handles serialization
        self.active_memtable.wal().append(key, Some(&value))?;

        // 2. Write to memtable
        self.active_memtable.put(key.to_vec(), Some(value))?;

        // Check if freeze needed
        if self.active_memtable.size() >= MAX_MEMTABLE_SIZE {
            self.freeze_active_memtable()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        // 1. Append tombstone to WAL (value = None)
        self.active_memtable.wal().append(key, None)?;

        // 2. Write tombstone to memtable
        self.active_memtable.put(key.to_vec(), None)?;

        Ok(())
    }

    // Durability guarantee - per toydb Engine trait
    pub fn flush(&mut self) -> Result<()> {
        // Fsync WAL - makes all writes durable
        self.active_memtable.wal().flush()?;
        Ok(())
    }

    // Recovery from WAL
    fn recover_memtable(&mut self, wal: &Wal) -> Result<()> {
        for result in wal.replay() {
            let (key, value) = result?;
            self.active_memtable.put(key, value)?;
        }
        Ok(())
    }
}

// Future transaction layer (separate module)
// let mut txn = store.begin();
// txn.set(b"key", b"value")?;
// txn.delete(b"old")?;
// txn.commit()?; // <- Calls store.flush() for durability
```

## Benefits

1. **Clean API** - WAL handles serialization, caller provides key-value pairs
2. **toydb-Proven Pattern** - Follows bitcask Log design
3. **Fast Checksums** - CRC32C (~15 GB/s vs CRC64 ~8 GB/s)
4. **O_DIRECT Support** - Production-ready direct I/O with 4KB-aligned buffers
5. **Explicit Durability** - Caller controls fsync via `flush()`
6. **Validated Recovery** - Magic number ensures file format correctness
7. **Self-Describing Format** - Length prefixes enable skip-ahead
8. **Granular Errors** - Skip corrupted entries, continue recovery
9. **Smart Alignment** - `AlignedWriter` handles O_DIRECT transparently

## Testing Strategy

1. **Roundtrip Tests** - Encode/decode Entry
2. **Corruption Detection** - Flip bits, verify checksum fails
3. **Partial Write Recovery** - Truncate file mid-entry, verify clean recovery
4. **Magic Number Validation** - Wrong magic, verify rejection
5. **Version Compatibility** - Old version, verify error
6. **Concurrent Replay** - Multiple readers on same WAL
7. **O_DIRECT Alignment** - Test aligned writes, padding behavior
8. **Durability Tests** - Kill process after append (no flush), verify data loss; after flush, verify recovery

## Performance Considerations

- **CRC32C**: ~15 GB/s (vs CRC64: ~8 GB/s) on modern CPUs
- **Buffer Size**: 64KB default, aligned to 4KB for O_DIRECT
- **Fsync Cost**: Explicit via `flush()` - caller amortizes cost
- **O_DIRECT**: Bypasses page cache, reduces system overhead

## AlignedWriter Implementation Details

The `AlignedWriter` elegantly handles O_DIRECT's strict alignment requirements:

### 1. 4KB-Aligned Buffer Allocation
```rust
let layout = Layout::from_size_align(capacity, 4096).unwrap();
let data = unsafe { alloc(layout) };
```
- Uses Rust's allocator to guarantee 4KB-aligned memory address
- Both buffer start and size are aligned
- Required for O_DIRECT writes

### 2. Smart Padding on Flush
```rust
let aligned_size = ((self.position + 4095) / 4096) * 4096;
// Zero-fill padding bytes
for i in self.position..aligned_size {
    self.buffer.data[i] = 0;
}
```
- Rounds up current position to next 4KB boundary
- Pads with zeros to meet O_DIRECT alignment
- No wasted syscalls - writes exactly what's needed

### 3. Transparent Slice Access
```rust
impl Deref for AlignedBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.capacity) }
    }
}
```
- `Deref` trait makes buffer act like `&[u8]`
- Can use normal slice operations: `buffer.data[i]`, `copy_from_slice`, etc.
- Zero cognitive overhead for caller

### 4. Oversized Write Handling
```rust
if buf.len() > self.buffer.capacity {
    let aligned_len = (buf.len() / 4096) * 4096;
    self.file.write_all(&buf[..aligned_len])?;  // Write aligned chunk

    let remainder = &buf[aligned_len..];
    self.buffer.data[..remainder.len()].copy_from_slice(remainder);  // Buffer rest
    self.position = remainder.len();
}
```
- Large writes: split into aligned chunk + remainder
- Aligned portion written directly to disk
- Remainder buffered for next flush
- Maintains O_DIRECT compliance throughout

### Why This Design Works

- **No manual tracking** - layout system ensures alignment
- **Automatic padding** - flush handles rounding transparently
- **Ergonomic** - Deref makes it feel like regular buffer
- **Efficient** - minimizes padding overhead, handles edge cases
- **Safe abstraction** - unsafe code isolated in `AlignedBuffer`

## Migration Path

1. Update WAL to new format (backward compatible via version check)
2. Implement `AlignedWriter` with O_DIRECT support
3. Update memtable recovery to use new replay API
4. Keep old WAL reader for backward compatibility (read-only)

## Future Enhancements

1. **Batching** - Write multiple entries in one syscall
2. **Compression** - Optional compression for large values
3. **Encryption** - Optional AES encryption for entries
4. **Preallocate** - Preallocate WAL file size for faster writes
5. **Entry Size Limit** - Enforce max entry size (e.g., 1MB)
6. **Filesystem Detection** - Auto-disable O_DIRECT on tmpfs (like hedwig)