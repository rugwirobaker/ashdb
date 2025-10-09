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
//! +--------+-----------+-------+-----------+-------+-----------+
//! |type:u8 |key_len:u32| key   |val_len:u32| value |crc32:u32  |
//! +--------+-----------+-------+-----------+-------+-----------+
//! | 1 byte |  4 bytes  |var len|  4 bytes  |var len| 4 bytes   |
//! +--------+-----------+-------+-----------+-------+-----------+
//! ```
//!
//! - All multi-byte integers use big-endian encoding for portability
//! - CRC32 checksum covers the entire entry (excluding the checksum itself)
//! - Type byte: 0x00 = Put operation, 0x01 = Delete operation (tombstone)
//! - Delete operations have `val_len = 0` with no value data
//!
//! # IO Optimizations for Durability
//!
//! ## Buffered IO with Preallocation and fdatasync
//!
//! The WAL uses buffered I/O with kernel page cache for optimal performance:
//!
//! - **Buffered writes**: Uses `BufWriter` to coalesce small writes into larger syscalls
//! - **Preallocation**: Uses `fallocate(FALLOC_FL_KEEP_SIZE)` to reduce metadata updates
//! - **Data-only sync**: Uses `fdatasync()` (sync_data) instead of `fsync()` for faster durability
//! - **Parent dir sync**: Syncs parent directory after create/unlink for crash safety
//! - **Group commit ready**: Architecture prepared for future async I/O with io_uring
//!
//! Direct I/O requires custom I/O scheduling and framed layout to be effective.
//! See: https://www.scylladb.com/2017/10/05/io-access-methods-scylla/
//! We will add proper direct I/O support later with a custom I/O loop.

pub mod header;
pub mod recovery;

use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use header::Header;
use header::HEADER_SIZE;

use crate::error::Result;
use crate::Error;

use crc::{Crc, CRC_32_ISCSI};
use std::fs::File;
use std::io::{BufReader, BufWriter, Cursor, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

pub const CRC32: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
const DEFAULT_BUFFER_SIZE: usize = 64 * 1024;

/// Type alias for WAL entry data (key, optional value)
type WalEntry = (Vec<u8>, Option<Vec<u8>>);

/// Operation type for WAL entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Put = 0x00,
    Delete = 0x01,
}

impl OpType {
    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x00 => Ok(OpType::Put),
            0x01 => Ok(OpType::Delete),
            _ => Err(Error::InvalidData(format!(
                "Unknown operation type: {}",
                byte
            ))),
        }
    }

    fn to_byte(self) -> u8 {
        self as u8
    }
}

pub struct WalOptions {
    pub buffer_size: usize,
    pub prealloc_size: usize,
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            buffer_size: DEFAULT_BUFFER_SIZE,
            prealloc_size: 64 * 1024 * 1024, // 64 MB
        }
    }
}

// Represents a key-value pair or a delete operation in the WAL
pub struct Wal {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
}

impl std::fmt::Debug for Wal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Wal").field("path", &self.path).finish()
    }
}

impl Wal {
    pub fn new(path: &str) -> Result<Self> {
        Self::with_options(path, WalOptions::default())
    }

    pub fn with_options(path: &str, opts: WalOptions) -> Result<Self> {
        let mut open_opts = File::options();
        open_opts.create(true).read(true).write(true);

        let mut file = open_opts.open(path)?;

        // Write or validate immutable header
        if file.metadata()?.len() == 0 {
            // New file: write header once
            file.write_all(&Header::new().encode())?;
            file.sync_data()?; // data-only sync is enough for WAL header
            fsync_parent_dir(Path::new(path))?;
        } else {
            // Existing file: validate header
            let mut buf = [0u8; HEADER_SIZE];
            file.read_exact(&mut buf)?;
            Header::decode(&buf)?;
        }

        // Preallocate the WAL segment to reduce metadata updates
        preallocate(&file, opts.prealloc_size)?;

        let writer = BufWriter::with_capacity(opts.buffer_size, file);

        Ok(Self {
            writer: Mutex::new(writer),
            path: path.into(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn remove(self) -> Result<()> {
        let path = self.path.clone();
        // File handles are dropped here
        std::fs::remove_file(&path)?;
        fsync_parent_dir(&path)?;
        Ok(())
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
        self.writer
            .lock()
            .ok()
            .and_then(|w| w.get_ref().metadata().ok())
            .map_or(0, |meta| meta.len())
    }
}

impl Wal {
    pub fn append(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        // Build one contiguous record: [len:u32][payload...][crc:u32]
        let mut payload = Vec::with_capacity(1 + 4 + 4 + key.len() + value.map_or(0, |v| v.len()));
        let op = if value.is_some() {
            OpType::Put
        } else {
            OpType::Delete
        };
        payload.write_u8(op.to_byte())?;
        payload.write_u32::<BigEndian>(key.len() as u32)?;
        payload.write_u32::<BigEndian>(value.map_or(0, |v| v.len()) as u32)?;
        payload.extend_from_slice(key);
        if let Some(v) = value {
            payload.extend_from_slice(v);
        }

        let crc = CRC32.checksum(&payload);

        let mut record = Vec::with_capacity(4 + payload.len() + 4);
        record.write_u32::<BigEndian>(payload.len() as u32)?;
        record.extend_from_slice(&payload);
        record.write_u32::<BigEndian>(crc)?;

        // Single write into BufWriter (kernel will coalesce further)
        let mut w = self.writer.lock().unwrap();
        w.write_all(&record)?;
        Ok(())
    }

    /// Replays the WAL file and returns a list of key-value pairs.
    pub fn replay(&self) -> Result<ReplayIterator> {
        ReplayIterator::new(&self.path)
    }

    pub fn flush(&self) -> Result<()> {
        let mut w = self.writer.lock()?;
        w.flush()?; // push into kernel
        w.get_ref().sync_data()?; // fdatasync on THE SAME fd the BufWriter owns
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

        let op_type_byte = match cursor.read_u8() {
            Ok(byte) => byte,
            Err(e) => return Err(e.into()),
        };
        let _op_type = OpType::from_byte(op_type_byte)?;

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

/// Preallocate space for a file using fallocate with FALLOC_FL_KEEP_SIZE on Linux.
/// This allocates disk blocks without changing the file size, reducing metadata updates
/// and filesystem fragmentation during writes.
#[cfg(target_os = "linux")]
fn preallocate(file: &File, bytes: usize) -> Result<()> {
    use nix::fcntl::{fallocate, FallocateFlags};
    use std::os::unix::io::AsRawFd;

    if bytes == 0 {
        return Ok(());
    }

    let fd = file.as_raw_fd();
    match fallocate(fd, FallocateFlags::FALLOC_FL_KEEP_SIZE, 0, bytes as i64) {
        Ok(_) => Ok(()),
        // Gracefully handle filesystems that don't support fallocate
        Err(nix::errno::Errno::EOPNOTSUPP | nix::errno::Errno::EINVAL) => Ok(()),
        Err(e) => Err(Error::IO(format!("fallocate failed: {}", e))),
    }
}

#[cfg(not(target_os = "linux"))]
fn preallocate(_file: &File, _bytes: usize) -> Result<()> {
    // No-op on non-Linux platforms
    Ok(())
}

/// Remove a WAL file by path and sync parent directory.
/// Use this when you need to remove a WAL file without having the Wal object.
pub fn remove_wal_by_path(path: &Path) -> Result<()> {
    std::fs::remove_file(path)?;
    fsync_parent_dir(path)?;
    Ok(())
}

/// Sync parent directory to make file creation/deletion durable.
/// This ensures that directory metadata changes (create, unlink, rename) survive crashes.
fn fsync_parent_dir(path: &Path) -> Result<()> {
    use std::fs::OpenOptions;

    if let Some(dir) = path.parent() {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            let dirfd = OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY)
                .open(dir)?;
            dirfd.sync_all()?;
        }

        #[cfg(not(target_os = "linux"))]
        {
            // On non-Linux, just open and sync the directory
            let dirfd = OpenOptions::new().read(true).open(dir)?;
            dirfd.sync_all()?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::header::HEADER_SIZE;
    use super::Wal;
    use crate::{tmpfs::NamedTempFile, Error};
    use std::fs::OpenOptions;
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

        // Verify entries via replay
        let entries: Vec<_> = wal
            .replay()
            .expect("Failed to replay")
            .collect::<Result<Vec<_>, _>>()
            .expect("Replay failed");
        assert_eq!(entries.len(), 3);
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
        let wal = create_temp_wal();

        wal.append(b"key1", Some(b"value1"))
            .expect("Failed to append");
        wal.append(b"key2", Some(b"value2"))
            .expect("Failed to append");
        wal.flush().expect("Failed to flush");

        // Corrupt the file by opening it separately
        {
            let mut file = OpenOptions::new()
                .write(true)
                .open(wal.path())
                .expect("Failed to open WAL for corruption");
            file.seek(SeekFrom::Start(HEADER_SIZE as u64 + 5)).unwrap();
            file.write_all(b"garbage").unwrap();
        }

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
