//! Write-Ahead Log (WAL) implementation for durability and recovery.
//!
//! The WAL provides crash recovery by logging all mutations before applying them
//! to the memtable. This ensures that no committed data is lost, even in the
//! event of sudden process termination or system failure.
//!
//! # File Format
//!
//! Each WAL file follows this binary format:
//!
//! ```text
//! +------------------+
//! | Header (64 bytes)|
//! +------------------+
//! | Entry 1          |
//! +------------------+
//! | Entry 2          |
//! +------------------+
//! | ...              |
//! +------------------+
//! ```
//!
//! ## Entry Format
//!
//! Each entry is variable-length and contains:
//!
//! ```text
//! +-----------+-------+-----------+-------+-----------+
//! |key_len:u32| key   |val_len:u32| value |crc32:u32  |
//! +-----------+-------+-----------+-------+-----------+
//! |  4 bytes  |var len|  4 bytes  |var len| 4 bytes   |
//! +-----------+-------+-----------+-------+-----------+
//! ```
//!
//! - All multi-byte integers use big-endian encoding for portability
//! - CRC32 checksum covers the entire entry (excluding the checksum itself)
//! - Delete operations use `val_len = 0` with no value data
//!
//! # IO Optimizations for Durability
//!
//! ## Direct IO for Write Guarantees
//!
//! The most critical optimization is using direct IO (`O_DIRECT`) to ensure
//! writes reach disk immediately, bypassing kernel page cache:
//!
//! - **Durability certainty**: Writes are guaranteed on disk, not buffered in kernel pages
//! - **No silent data loss**: Eliminates risk of losing "successful" writes during crashes
//! - **Consistent sync behavior**: `sync()` operations have predictable effects
//! - **True write-through**: Data persistence doesn't depend on kernel flush timing
//!
//! This follows patterns from https://transactional.blog/how-to-learn/disk-io for
//! reliable database IO. Future implementations may adopt additional patterns
//! from that resource for even better IO handling.
//!
//! ## Write Throughput Optimization
//!
//! While maintaining durability, several techniques optimize write performance:
//! - **Aligned writes**: Uses `AlignedWriter` to meet direct IO alignment requirements
//! - **Batched operations**: Groups writes to amortize syscall overhead
//! - **Append-only**: Sequential writes optimize for disk performance
//! - **Configurable buffering**: Tunable buffer sizes for workload optimization

pub mod aligned_writer;
pub mod header;
pub mod recovery;

use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use header::Header;
use header::HEADER_SIZE;

use crate::error::Result;
use crate::Error;

use aligned_writer::AlignedWriter;
use crc::{Crc, CRC_32_ISCSI};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::RwLock;

/// Type alias for WAL entry data (key, optional value)
type WalEntry = (Vec<u8>, Option<Vec<u8>>);

pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

pub struct WalOptions {
    pub use_direct_io: bool,
    pub buffer_size: usize,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            use_direct_io: false,
            buffer_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

// Represents a key-value pair or a delete operation in the WAL

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("direct_io", &self.direct_io)
            .finish()
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

        if opts.use_direct_io && opts.buffer_size % 4096 != 0 {
            return Err(Error::InvalidInput(
                "Buffer size must be aligned to 4096 bytes for direct IO".to_string(),
            ));
        }

        let mut open_opts = File::options();
        open_opts.create(true).read(true).write(true);

        #[cfg(target_os = "linux")]
        if opts.use_direct_io {
            open_opts.custom_flags(libc::O_DIRECT);
        }

        let file = open_opts.open(path)?;

        let writer: Box<dyn Write + Send> = if opts.use_direct_io {
            Box::new(AlignedWriter::new(file.try_clone()?, opts.buffer_size)?)
        } else {
            Box::new(BufWriter::with_capacity(
                opts.buffer_size,
                file.try_clone()?,
            ))
        };

        let header = if file.metadata()?.len() == 0 {
            let h = Header::new();
            let header_bytes = h.encode();
            let mut f = file.try_clone()?;
            f.write_all(&header_bytes)?;
            f.sync_all()?;
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

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn remove(self) -> Result<()> {
        let path = self.path.clone();
        // File handles are dropped here
        Ok(std::fs::remove_file(path)?)
    }

    /// Returns the numeric ID of the WAL file, derived from its file name.
    pub fn id(&self) -> Result<u64> {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.split('.').next())
            .and_then(|num| num.parse::<u64>().ok())
            .ok_or_else(|| Error::InvalidData(format!("Invalid WAL file name: {:?}", self.path)))
    }

    /// Returns the current size of the WAL file.
    pub fn size(&self) -> u64 {
        self.file.metadata().map_or(0, |meta| meta.len())
    }

    pub fn entry_count(&self) -> u64 {
        self.header.read().map(|h| h.entry_count).unwrap_or(0)
    }
}

impl Wal {
    pub fn append(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut payload = Vec::new();
        payload.write_u32::<BigEndian>(key.len() as u32)?;
        payload.write_u32::<BigEndian>(value.map_or(0, |v| v.len()) as u32)?;
        payload.extend_from_slice(key);
        if let Some(v) = value {
            payload.extend_from_slice(v);
        }

        let checksum = CRC32.checksum(&payload);

        let mut writer = self.writer.lock()?;

        writer.write_u32::<BigEndian>(payload.len() as u32)?;
        writer.write_all(&payload)?;
        writer.write_u32::<BigEndian>(checksum)?;

        self.header.write()?.entry_count += 1;

        Ok(())
    }

    /// Replays the WAL file and returns a list of key-value pairs.
    pub fn replay(&self) -> Result<ReplayIterator> {
        ReplayIterator::new(&self.path)
    }

    pub fn flush(&self) -> Result<()> {
        self.writer.lock()?.flush()?;

        let header = self.header.read()?;
        let header_bytes = header.encode();
        drop(header);

        let mut file = self.file.try_clone()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header_bytes)?;

        file.sync_all()?;

        Ok(())
    }
}

pub struct ReplayIterator {
    reader: BufReader<File>, // Independent reader for replaying entries
}

impl ReplayIterator {
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.get_mut().seek(SeekFrom::Start(HEADER_SIZE as u64))?;

        Ok(ReplayIterator { reader })
    }
}

impl ReplayIterator {
    fn read<R: Read>(reader: &mut R) -> Result<Option<WalEntry>> {
        let record_len = match reader.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => return Err(e.into()),
        };

        let mut payload = vec![0u8; record_len];
        if let Err(e) = reader.read_exact(&mut payload) {
            return Err(Error::InvalidData(format!("Failed to read payload: {}", e)));
        }

        let stored_crc = match reader.read_u32::<BigEndian>() {
            Ok(crc) => crc,
            Err(e) => {
                return Err(Error::InvalidData(format!(
                    "Failed to read checksum: {}",
                    e
                )))
            }
        };

        let computed_crc = CRC32.checksum(&payload);
        if computed_crc != stored_crc {
            return Err(Error::InvalidData("Checksum mismatch".to_string()));
        }

        let mut cursor = Cursor::new(&payload);

        let key_len = match cursor.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) => return Err(e.into()),
        };

        let value_len = match cursor.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) => return Err(e.into()),
        };

        let mut key = vec![0u8; key_len];
        if let Err(e) = cursor.read_exact(&mut key) {
            return Err(Error::InvalidData(format!("Failed to read key: {}", e)));
        }

        let value = if value_len > 0 {
            let mut v = vec![0u8; value_len];
            if let Err(e) = cursor.read_exact(&mut v) {
                return Err(Error::InvalidData(format!("Failed to read value: {}", e)));
            }
            Some(v)
        } else {
            None
        };

        Ok(Some((key, value)))
    }
}

impl Iterator for ReplayIterator {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match ReplayIterator::read(&mut self.reader) {
            Ok(Some((key, value))) => Some(Ok((key, value))),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::header::HEADER_SIZE;
    use super::Wal;
    use crate::{tmpfs::NamedTempFile, Error};
    use std::io::{Seek, SeekFrom, Write};

    fn create_temp_wal() -> Wal {
        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
        let path = temp_file.path().to_string_lossy().to_string();

        // Create the WAL which will create the actual file
        let wal = Wal::new(&path).expect("Failed to initialize WAL");

        // Don't drop temp_file yet - keep it alive by forgetting it
        // This prevents the file from being deleted
        std::mem::forget(temp_file);

        wal
    }

    #[test]
    fn test_append_and_flush() {
        let wal = create_temp_wal();

        wal.append(b"key1", Some(b"value1"))
            .expect("Failed to append");
        wal.append(b"key2", Some(b"value2"))
            .expect("Failed to append");
        wal.append(b"key3", None)
            .expect("Failed to append (key only)");

        wal.flush().expect("Failed to flush");

        assert_eq!(wal.entry_count(), 3);
    }

    #[test]
    fn test_replay_iterator() {
        let wal = create_temp_wal();

        wal.append(b"key1", Some(b"value1"))
            .expect("Failed to append");
        wal.append(b"key2", Some(b"value2"))
            .expect("Failed to append");
        wal.flush().expect("Failed to flush");

        let replay_iter = wal.replay().expect("Failed to create replay iterator");
        let entries: Vec<_> = replay_iter
            .collect::<Result<Vec<_>, _>>()
            .expect("Replay failed");

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"key1".to_vec(), Some(b"value1".to_vec())));
        assert_eq!(entries[1], (b"key2".to_vec(), Some(b"value2".to_vec())));
    }

    #[test]
    fn test_empty_replay() {
        let wal = create_temp_wal();

        // Replay and ensure no entries exist
        let replay_iter = wal.replay().expect("Failed to create replay iterator");
        assert_eq!(replay_iter.count(), 0);
    }

    #[test]
    fn test_corrupted_wal() {
        let mut wal = create_temp_wal();

        wal.append(b"key1", Some(b"value1"))
            .expect("Failed to append");
        wal.append(b"key2", Some(b"value2"))
            .expect("Failed to append");
        wal.flush().expect("Failed to flush");

        wal.file
            .seek(SeekFrom::Start(HEADER_SIZE as u64 + 5))
            .unwrap();
        wal.file.write_all(b"garbage").unwrap();
        wal.flush().unwrap();

        let replay_iter = wal.replay().expect("Failed to create replay iterator");
        let mut has_corruption = false;

        for entry in replay_iter {
            match entry {
                Err(Error::InvalidData(_)) => {
                    has_corruption = true;
                    break;
                }
                Err(e) => panic!("Unexpected error during replay: {:?}", e),
                Ok(_) => {}
            }
        }
        assert!(has_corruption, "Corruption not detected during replay");
    }

    #[test]
    fn test_key_only_entries() {
        let wal = create_temp_wal();

        wal.append(b"key1", None).expect("Failed to append");
        wal.append(b"key2", None).expect("Failed to append");

        wal.flush().expect("Failed to flush");

        let replay_iter = wal.replay().expect("Failed to create replay iterator");
        let entries: Vec<_> = replay_iter
            .collect::<Result<Vec<_>, _>>()
            .expect("Replay failed");

        assert_eq!(entries.len(), 2, "Unexpected number of entries replayed");
        assert_eq!(entries[0], (b"key1".to_vec(), None));
        assert_eq!(entries[1], (b"key2".to_vec(), None));
    }

    #[test]
    fn test_concurrent_wal_reads() {
        use std::sync::Arc;
        use std::thread;

        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
        let wal = Wal::new(temp_file.path().to_str().unwrap()).expect("Failed to initialize WAL");

        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            wal.append(key.as_bytes(), Some(value.as_bytes()))
                .expect("Failed to write");
        }
        wal.flush().expect("Failed to flush");

        // Create multiple readers through cloning
        let wal = Arc::new(wal);
        let mut handles = vec![];

        // Spawn multiple threads that will read simultaneously
        for thread_id in 0..3 {
            let wal_clone = wal.clone();
            let handle = thread::spawn(move || {
                let replay_iter = wal_clone
                    .replay()
                    .expect("Failed to create replay iterator");
                let mut count = 0;
                for entry in replay_iter {
                    let (key, value) = entry.expect("Failed to read entry");
                    let key_str = String::from_utf8_lossy(&key);
                    let value_str = String::from_utf8_lossy(value.as_ref().unwrap());
                    println!("Thread {} read: {} = {}", thread_id, key_str, value_str);
                    count += 1;
                }
                count
            });
            handles.push(handle);
        }

        // Verify that all threads read all entries
        for handle in handles {
            let count = handle.join().unwrap();
            assert_eq!(count, 100, "Thread did not read all entries");
        }
    }
}
