use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::Hasher;

use super::header::{Header, HEADER_SIZE};
use crate::Error;

// Represents a key-value pair or a delete operation in the WAL

#[derive(Debug)]
pub struct Wal {
    file: File,              // Single file handle
    writer: BufWriter<File>, // Buffered writer for appends
    header: Header,          // WAL header
    hasher: Hasher,          // Rolling hasher for incremental checksum
    path: PathBuf,           // For debugging
}

impl Wal {
    pub fn new(path: &str) -> Result<Self, Error> {
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
            file,
            writer,
            header,
            hasher: Hasher::new(),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the numeric ID of the WAL file, derived from its file name.
    pub fn id(&self) -> Result<u64, Error> {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.split('.').next())
            .and_then(|num| num.parse::<u64>().ok())
            .ok_or_else(|| Error::InvalidWalId(format!("Invalid WAL file name: {:?}", self.path)))
    }
}

impl Wal {
    /// Appends a key-value pair to the WAL file.
    pub fn put(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(), Error> {
        let key_length = (key.len() as u32).to_be_bytes();
        let value_data = value.unwrap_or(&[]);
        let value_length = (value_data.len() as u32).to_be_bytes();

        // Write key-value pair
        self.writer.write_all(&key_length)?;
        self.writer.write_all(key)?;
        self.writer.write_all(&value_length)?;
        self.writer.write_all(value_data)?;

        // Update the rolling checksum
        self.hasher.update(key, value_data);

        // Update entry count in the header
        self.header.entry_count += 1;

        Ok(())
    }

    /// Replays the WAL file and returns a list of key-value pairs.
    pub fn replay(&self) -> Result<ReplayIterator, Error> {
        let reader = BufReader::new(self.file.try_clone()?);
        ReplayIterator::new(reader)
    }

    /// Syncs the WAL file to disk.
    pub fn sync(&mut self) -> Result<(), Error> {
        // Flush the writer to ensure all data is written
        self.writer.flush()?;
        self.file.sync_all()?; // Ensure durability on disk

        // Update and persist the checksum in the header
        self.header.checksum = self.hasher.value();
        let header_bytes: Vec<u8> = self.header.try_into()?;
        self.file.seek(SeekFrom::Start(0))?; // Seek to the start
        self.file.write_all(&header_bytes)?;
        self.file.sync_all()?; // Ensure the header is persisted

        Ok(())
    }
    pub fn validate_checksum(&self) -> Result<(), Error> {
        let mut replay = ReplayIterator::new(BufReader::new(self.file.try_clone()?))?;
        while let Some(entry) = replay.next() {
            entry?; // Process entries to compute checksum
        }

        let computed_checksum = replay.checksum();
        if computed_checksum != self.header.checksum {
            Err(Error::CorruptedWal(format!(
                "Checksum mismatch: computed = {}, stored = {}",
                computed_checksum, self.header.checksum
            )))
        } else {
            Ok(())
        }
    }
}

pub struct ReplayIterator {
    reader: BufReader<File>, // Independent reader for replaying entries
    hasher: Hasher,          // Rolling hasher for incremental checksum
}

impl ReplayIterator {
    pub fn new(mut reader: BufReader<File>) -> Result<Self, Error> {
        reader
            .get_mut()
            .seek(SeekFrom::Start(HEADER_SIZE as u64))
            .map_err(|e| Error::IoError(e))?;

        Ok(ReplayIterator {
            reader,
            hasher: Hasher::new(),
        })
    }
}
impl ReplayIterator {
    /// Reads a key-value pair from the underlying WAL file.
    fn read<R: Read>(reader: &mut R) -> Result<Option<(Vec<u8>, Option<Vec<u8>>)>, Error> {
        let mut length_buf = [0u8; 4];

        // Read key length
        match reader.read_exact(&mut length_buf) {
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                // EOF at the start of a new entry is valid (end of WAL)
                return Ok(None);
            }
            Err(e) => return Err(Error::IoError(e)),
        }
        let key_length = u32::from_be_bytes(length_buf) as usize;

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
                }
            }
        }

        // Read value length
        match reader.read_exact(&mut length_buf) {
            Ok(_) => (),
            Err(e) => {
                return match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => Err(Error::CorruptedWal(
                        "Unexpected EOF while reading value length".to_string(),
                    )),
                    _ => Err(Error::IoError(e)),
                }
            }
        }
        let value_length = u32::from_be_bytes(length_buf) as usize;

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
                        }
                    }
                }
            }
        };

        Ok(Some((key, value)))
    }

    pub fn checksum(&self) -> u64 {
        self.hasher.value()
    }
}

impl Iterator for ReplayIterator {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match ReplayIterator::read(&mut self.reader) {
            Ok(Some((key, value))) => {
                println!(
                    "ReplayIterator read entry: key = {:?}, value = {:?}",
                    key, value
                );
                self.hasher.update(&key, value.as_deref().unwrap_or(&[]));
                Some(Ok((key, value)))
            }
            Ok(None) => {
                // println!("ReplayIterator reached EOF");
                None
            }
            Err(e) => {
                println!("ReplayIterator encountered error: {:?}", e);
                Some(Err(e))
            }
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
        let mut wal = create_temp_wal();

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.put(b"key3", None).expect("Failed to append (key only)");

        // Sync to ensure data is flushed and header is updated
        wal.sync().expect("Failed to sync");

        // Check that the header contains the correct entry count
        assert_eq!(wal.header.entry_count, 3);
    }

    #[test]
    fn test_replay_iterator() {
        let mut wal = create_temp_wal();

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.sync().expect("Failed to sync");

        // Validate checksum
        wal.validate_checksum().expect("Checksum validation failed");

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

        // Ensure checksum validation passes for an empty WAL
        wal.validate_checksum()
            .expect("Checksum validation failed for empty WAL");

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
    fn test_checksum_validation() {
        let mut wal = create_temp_wal();

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.sync().expect("Failed to sync");

        // Validate checksum (should pass)
        wal.validate_checksum()
            .expect("Checksum validation failed for valid WAL");

        // Corrupt the first key in the WAL
        wal.file
            .seek(SeekFrom::Start(HEADER_SIZE as u64 + 4))
            .expect("Failed to seek in WAL");
        wal.file
            .write_all(b"corrupt")
            .expect("Failed to corrupt WAL");
        wal.sync().expect("Failed to sync");

        // Validate checksum (should fail)
        assert!(
            wal.validate_checksum().is_err(),
            "Checksum validation passed for corrupted WAL"
        );
    }

    #[test]
    fn test_key_only_entries() {
        let mut wal = create_temp_wal();

        // Append key-only entries
        wal.put(b"key1", None).expect("Failed to append");
        wal.put(b"key2", None).expect("Failed to append");

        // Sync the WAL
        wal.sync().expect("Failed to sync");

        // Validate checksum
        wal.validate_checksum().expect("Checksum validation failed");

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
}
