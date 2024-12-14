pub mod header;

use byteorder::BigEndian;
use byteorder::{ReadBytesExt, WriteBytesExt};
use header::Header;
use header::HEADER_SIZE;

use crate::error::Result;
use crate::Error;
use crate::Hasher;

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::RwLock;

// Represents a key-value pair or a delete operation in the WAL

#[derive(Debug)]
pub struct Wal {
    writer: Mutex<BufWriter<File>>,
    header: RwLock<Header>,
    file: File,
    path: PathBuf,
}

impl Wal {
    pub fn new(path: &str) -> Result<Self> {
        let path = PathBuf::from(path);

        let file = File::options()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let mut writer = BufWriter::new(file.try_clone()?);

        // Use a local BufReader to read the header
        let mut reader = BufReader::new(file.try_clone()?);
        let mut buf = vec![0u8; HEADER_SIZE];
        let header = match reader.read_exact(&mut buf) {
            Ok(_) => Header::try_from(&buf)?, // Successfully read and parse the header
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // If the file is empty or incomplete, create a default header
                let header = Header::new(1); // Default version
                let bytes: Vec<u8> = header.try_into().unwrap();
                writer.write_all(&bytes)?;
                writer.flush()?; // Flush header to disk
                header
            }
            Err(e) => return Err(Error::IoError(e)), // Propagate other errors
        };

        Ok(Self {
            writer: Mutex::new(writer),
            header: RwLock::new(header),
            file,
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn remove(self) -> Result<()> {
        let path = self.path.clone();
        // File handles are dropped here
        std::fs::remove_file(path).map_err(Error::IoError)
    }

    /// Returns the numeric ID of the WAL file, derived from its file name.
    pub fn id(&self) -> Result<u64> {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.split('.').next())
            .and_then(|num| num.parse::<u64>().ok())
            .ok_or_else(|| Error::InvalidWalId(format!("Invalid WAL file name: {:?}", self.path)))
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
    /// Appends a key-value pair to the WAL file.
    pub fn put(&self, key: &[u8], value: Option<&[u8]>) -> Result<()> {
        let mut writer = self.writer.lock().map_err(|_| Error::MutexPoisoned)?;

        // Write key length and key
        writer.write_u32::<BigEndian>(key.len() as u32)?;
        writer.write_all(key)?;

        // Write value length and value if present
        let value_data = value.unwrap_or(&[]);
        writer.write_u32::<BigEndian>(value_data.len() as u32)?;
        writer.write_all(value_data)?;

        // Calculate and write entry checksum
        let mut hasher = Hasher::new();
        hasher.write(key);
        hasher.write(value_data);
        writer.write_u64::<BigEndian>(hasher.checksum())?;

        // Update entry count in header (only once)
        self.header
            .write()
            .map_err(|_| Error::MutexPoisoned)?
            .entry_count += 1;

        Ok(())
    }

    /// Replays the WAL file and returns a list of key-value pairs.
    pub fn replay(&self) -> Result<ReplayIterator> {
        let reader = BufReader::new(self.file.try_clone()?);
        ReplayIterator::new(reader)
    }

    /// Syncs the WAL file to disk.
    pub fn sync(&self) -> Result<()> {
        // Flush the writer to ensure all data is written
        self.writer
            .lock()
            .map_err(|_| Error::MutexPoisoned)?
            .flush()?;

        // Update and persist the checksum in the header
        // Write updated header
        let header = self.header.read().map_err(|_| Error::MutexPoisoned)?;
        let header_bytes: Vec<u8> = header.clone().try_into()?;
        let mut writer = self.writer.lock().map_err(|_| Error::MutexPoisoned)?;
        writer.get_mut().seek(SeekFrom::Start(0))?;
        writer.write_all(&header_bytes)?;
        writer.flush()?;
        Ok(())
    }
}

pub struct ReplayIterator {
    reader: BufReader<File>, // Independent reader for replaying entries
}

impl ReplayIterator {
    pub fn new(mut reader: BufReader<File>) -> Result<Self> {
        reader
            .get_mut()
            .seek(SeekFrom::Start(HEADER_SIZE as u64))
            .map_err(Error::IoError)?;

        Ok(ReplayIterator { reader })
    }
}

impl ReplayIterator {
    fn read<R: Read>(reader: &mut R) -> Result<Option<(Vec<u8>, Option<Vec<u8>>)>> {
        // Read key length
        let key_length = match reader.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None); // Clean EOF
            }
            Err(e) => return Err(Error::IoError(e)),
        };

        // Read key
        let mut key = vec![0u8; key_length];
        match reader.read_exact(&mut key) {
            Ok(_) => (),
            Err(e) => {
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => Err(Error::CorruptedWal(
                        "Unexpected EOF while reading key".to_string(),
                    )),
                    _ => Err(Error::IoError(e)),
                };
            }
        }

        let value_length = match reader.read_u32::<BigEndian>() {
            Ok(len) => len as usize,
            Err(e) => {
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => Err(Error::CorruptedWal(
                        "Unexpected EOF while reading value length".to_string(),
                    )),
                    _ => Err(Error::IoError(e)),
                };
            }
        };

        let value = match value_length {
            0 => None,
            _ => {
                let mut value = vec![0u8; value_length];
                match reader.read_exact(&mut value) {
                    Ok(_) => Some(value),
                    Err(e) => {
                        return match e.kind() {
                            std::io::ErrorKind::UnexpectedEof => Err(Error::CorruptedWal(
                                "Unexpected EOF while reading value".to_string(),
                            )),
                            _ => Err(Error::IoError(e)),
                        };
                    }
                }
            }
        };

        // Read and verify checksum
        let stored_checksum = match reader.read_u64::<BigEndian>() {
            Ok(checksum) => checksum,
            Err(e) => {
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => Err(Error::CorruptedWal(
                        "Unexpected EOF while reading checksum".to_string(),
                    )),
                    _ => Err(Error::IoError(e)),
                };
            }
        };

        let mut hasher = Hasher::new();
        hasher.write(&key);
        hasher.write(value.as_deref().unwrap_or(&[]));
        let computed_checksum = hasher.checksum();

        if computed_checksum != stored_checksum {
            return Err(Error::CorruptedWal(format!(
                "Entry checksum mismatch: stored={}, computed={}",
                stored_checksum, computed_checksum
            )));
        }
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
    use super::Wal;
    use crate::{wal::header::HEADER_SIZE, Error};
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::NamedTempFile;

    fn create_temp_wal() -> Wal {
        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
        Wal::new(temp_file.path().to_str().unwrap()).expect("Failed to initialize WAL")
    }

    #[test]
    fn test_append_and_sync() {
        let wal = create_temp_wal();

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.put(b"key3", None).expect("Failed to append (key only)");

        // Sync to ensure data is flushed and header is updated
        wal.sync().expect("Failed to sync");

        // Check that the header contains the correct entry count
        assert_eq!(wal.entry_count(), 3);
    }

    #[test]
    fn test_replay_iterator() {
        let wal = create_temp_wal();

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.sync().expect("Failed to sync");

        // Replay and verify entries
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

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.sync().expect("Failed to sync");

        // Corrupt the WAL by overwriting part of the valid data
        wal.file
            .seek(SeekFrom::Start(HEADER_SIZE as u64 + 5))
            .unwrap(); // Corrupt middle of first entry
        wal.file.write_all(b"garbage").unwrap();
        wal.sync().unwrap();

        // Replay entries
        let mut replay_iter = wal.replay().expect("Failed to create replay iterator");
        let mut has_corruption = false;

        while let Some(entry) = replay_iter.next() {
            match entry {
                Err(Error::CorruptedWal(msg)) => {
                    println!("Detected corruption: {}", msg);
                    has_corruption = true;
                    break;
                }
                Err(e) => panic!("Unexpected error during replay: {:?}", e),
                Ok(_) => {} // Valid entry, continue
            }
        }
        assert!(has_corruption, "Corruption not detected during replay");
    }

    #[test]
    fn test_key_only_entries() {
        let wal = create_temp_wal();

        // Append key-only entries
        wal.put(b"key1", None).expect("Failed to append");
        wal.put(b"key2", None).expect("Failed to append");

        // Sync the WAL
        wal.sync().expect("Failed to sync");

        // Replay entries
        let replay_iter = wal.replay().expect("Failed to create replay iterator");
        let entries: Vec<_> = replay_iter
            .collect::<Result<Vec<_>, _>>()
            .expect("Replay failed");

        // Verify the entries
        assert_eq!(entries.len(), 2, "Unexpected number of entries replayed");
        assert_eq!(entries[0], (b"key1".to_vec(), None));
        assert_eq!(entries[1], (b"key2".to_vec(), None));
    }

    #[test]
    fn test_concurrent_wal_reads() {
        use std::sync::Arc;
        use std::thread;

        // Create and populate a WAL file
        let temp_file = NamedTempFile::new().expect("Failed to create temporary file");
        let wal = Wal::new(temp_file.path().to_str().unwrap()).expect("Failed to initialize WAL");

        // Write some test data
        for i in 0..100 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            wal.put(key.as_bytes(), Some(value.as_bytes()))
                .expect("Failed to write");
        }
        wal.sync().expect("Failed to sync");

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
