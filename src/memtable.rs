use std::{
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use crossbeam_skiplist::SkipMap;

use crate::{wal::wal::Wal, Error};

#[derive(Debug)]
pub struct Memtable {
    id: u64,
    data: Arc<SkipMap<Vec<u8>, Option<Vec<u8>>>>, // In-memory key-value store
    wal: Arc<Mutex<Wal>>,                         // Associated Write-Ahead Log
    size: AtomicUsize,                            // Tracks Memtable size
    is_frozen: AtomicBool,                        // Indicates if Memtable is frozen
}

impl Memtable {
    /// Creates a new empty Memtable with a new WAL.
    pub fn new(dir: &str, id: u64) -> Result<Self, Error> {
        let dir_path = Path::new(dir);
        if !dir_path.exists() {
            std::fs::create_dir_all(dir_path).map_err(|e| {
                Error::IoError(std::io::Error::new(
                    e.kind(),
                    format!("Failed to create dir: {}", dir),
                ))
            })?;
        }
        let wal_path = dir_path.join(format!("{:04}.wal", id));
        let wal = Wal::new(wal_path.to_str().ok_or_else(|| {
            Error::InvalidWalId("Failed to convert WAL path to string".to_string())
        })?)?;

        Ok(Self {
            id,
            data: Arc::new(SkipMap::new()),
            wal: Arc::new(Mutex::new(wal)),
            size: AtomicUsize::new(0),
            is_frozen: AtomicBool::new(false),
        })
    }

    pub fn from_wal(wal: Wal) -> Result<Self, Error> {
        let data = Arc::new(SkipMap::new());
        let size = AtomicUsize::new(0);
        let id = wal.id()?;

        // Clone the WAL file for replay
        let replay_iter = wal.replay()?;
        for entry in replay_iter {
            let (key, value) = entry?;
            let entry_size = key.len() + value.as_ref().map_or(0, |v| v.len());
            size.fetch_add(entry_size, Ordering::SeqCst);

            data.insert(key, value);
        }

        Ok(Self {
            data,
            wal: Arc::new(Mutex::new(wal)), // Ensure WAL is still usable elsewhere
            size,
            is_frozen: AtomicBool::new(false),
            id: id,
        })
    }
}

impl Memtable {
    /// Inserts or updates a key-value pair in the Memtable.
    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<(), Error> {
        if self.is_frozen.load(Ordering::SeqCst) {
            return Err(Error::Frozen);
        }
        let key_size = key.len();
        let value_size = value.as_ref().map_or(0, |v| v.len());
        let entry_size = key_size + value_size;

        // Append to WAL for durability
        let mut wal = self.wal.lock().map_err(|_| {
            Error::IoError(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to lock WAL",
            ))
        })?;
        wal.put(&key, value.as_deref())?;
        // Insert into the Memtable
        self.data.insert(key, value);
        // Update Memtable size
        self.size.fetch_add(entry_size, Ordering::SeqCst);

        Ok(())
    }

    /// Retrieves the value for a given key.
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.data.get(key).map(|entry| entry.value().clone())
    }

    /// size returns the size of the Memtable in bytes.
    pub fn size(&self) -> usize {
        self.size.load(Ordering::SeqCst)
    }

    /// id returns the ID of the Memtable.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// freeze prevents further writes to the Memtable.
    pub fn freeze(&self) -> Result<(), Error> {
        if self.is_frozen.swap(true, Ordering::SeqCst) {
            return Err(Error::Frozen);
        }
        Ok(())
    }

    // Scan range of keys
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIter, Error> {
        let start_bound = match range.start_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end_bound = match range.end_bound() {
            Bound::Included(key) => Bound::Included(key.clone()),
            Bound::Excluded(key) => Bound::Excluded(key.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let iterator = self
            .data
            .range((start_bound, end_bound))
            .map(|entry| (entry.key().clone(), entry.value().clone()));

        Ok(ScanIter {
            inner: Box::new(iterator),
        })
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<ScanIter, Error> {
        let start_key = prefix.to_vec();
        let end_key = {
            let mut end = prefix.to_vec();
            for i in (0..end.len()).rev() {
                if end[i] < u8::MAX {
                    end[i] += 1;
                    end.truncate(i + 1);
                    break;
                }
            }
            end
        };

        let iterator = self
            .data
            .range(start_key..end_key)
            .map(|entry| (entry.key().clone(), entry.value().clone()));

        Ok(ScanIter {
            inner: Box::new(iterator),
        })
    }
}

pub struct ScanIter<'a> {
    inner: Box<dyn Iterator<Item = (Vec<u8>, Option<Vec<u8>>)> + 'a>,
}

impl<'a> Iterator for ScanIter<'a> {
    type Item = (Vec<u8>, Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temporary directory")
    }

    fn create_temp_memtable(temp_dir: &TempDir) -> Memtable {
        let wal_path = temp_dir.path().join("0000.wal");
        Memtable::new(wal_path.to_str().expect("Failed to create WAL path"), 0)
            .expect("Failed to initialize Memtable")
    }

    fn create_temp_wal(temp_dir: &TempDir) -> Wal {
        let wal_path = temp_dir.path().join("0000.wal");
        Wal::new(wal_path.to_str().expect("Failed to create WAL path"))
            .expect("Failed to initialize WAL")
    }

    #[test]
    fn test_put_and_get() {
        let temp_dir = create_temp_dir();
        let memtable = create_temp_memtable(&temp_dir);

        // Add key-value pairs
        memtable
            .put(b"key1".to_vec(), Some(b"value1".to_vec()))
            .expect("Put failed");
        memtable
            .put(b"key2".to_vec(), Some(b"value2".to_vec()))
            .expect("Put failed");
        memtable
            .put(b"key3".to_vec(), None)
            .expect("Put failed (key only)");

        // Retrieve and verify
        assert_eq!(memtable.get(b"key1"), Some(Some(b"value1".to_vec())));
        assert_eq!(memtable.get(b"key2"), Some(Some(b"value2".to_vec())));
        assert_eq!(memtable.get(b"key3"), Some(None));
        assert_eq!(memtable.get(b"key4"), None);
    }

    #[test]
    fn test_freeze_twice() {
        let temp_dir = create_temp_dir();
        let memtable = create_temp_memtable(&temp_dir);

        // Freeze the Memtable once
        memtable.freeze().expect("Failed to freeze Memtable");
        // Attempt to freeze it again and verify the error
        assert!(matches!(memtable.freeze(), Err(Error::Frozen)));
    }

    #[test]
    fn test_put_to_frozen_memtable() {
        let temp_dir = create_temp_dir();
        let memtable = create_temp_memtable(&temp_dir);

        // Freeze the Memtable
        memtable.freeze().expect("Failed to freeze Memtable");
        // Attempt to write and verify the error
        assert!(matches!(
            memtable.put(b"key1".to_vec(), Some(b"value1".to_vec())),
            Err(Error::Frozen)
        ));
    }

    #[test]
    fn test_from_wal() {
        let temp_dir = create_temp_dir();
        let mut wal = create_temp_wal(&temp_dir);

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.put(b"key3", None).expect("Failed to append (key only)");
        wal.sync().expect("Failed to sync");

        // Validate checksum to ensure WAL integrity
        wal.validate_checksum().expect("Checksum validation failed");

        // Create a Memtable from the WAL
        let memtable = Memtable::from_wal(wal).expect("Failed to create Memtable from WAL");

        // Verify the reconstructed data
        assert_eq!(memtable.get(b"key1"), Some(Some(b"value1".to_vec())));
        assert_eq!(memtable.get(b"key2"), Some(Some(b"value2".to_vec())));
        assert_eq!(memtable.get(b"key3"), Some(None));
        assert_eq!(memtable.size(), 24);
    }

    #[test]
    fn test_scan() {
        let temp_dir = create_temp_dir();
        println!("TempDir path: {}", temp_dir.path().display());

        let memtable = create_temp_memtable(&temp_dir);

        // Insert key-value pairs
        memtable
            .put(b"key1".to_vec(), Some(b"value1".to_vec()))
            .expect("Put failed");
        memtable
            .put(b"key2".to_vec(), Some(b"value2".to_vec()))
            .expect("Put failed");
        memtable
            .put(b"key3".to_vec(), Some(b"value3".to_vec()))
            .expect("Put failed");

        // Scan a specific range
        let mut scan_iter = memtable
            .scan(b"key1".to_vec()..=b"key2".to_vec())
            .expect("Scan failed");

        assert_eq!(
            scan_iter.next(),
            Some((b"key1".to_vec(), Some(b"value1".to_vec())))
        );
        assert_eq!(
            scan_iter.next(),
            Some((b"key2".to_vec(), Some(b"value2".to_vec())))
        );
        assert_eq!(scan_iter.next(), None);
    }

    #[test]
    fn test_scan_prefix() {
        let temp_dir = create_temp_dir();
        let memtable = create_temp_memtable(&temp_dir);

        // Insert key-value pairs
        memtable
            .put(b"key1".to_vec(), Some(b"value1".to_vec()))
            .expect("Put failed");
        memtable
            .put(b"key2".to_vec(), Some(b"value2".to_vec()))
            .expect("Put failed");
        memtable
            .put(b"other_key".to_vec(), Some(b"value3".to_vec()))
            .expect("Put failed");

        // Scan a prefix
        let entries: Vec<_> = memtable
            .scan_prefix(b"key")
            .expect("Scan prefix failed")
            .collect();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], (b"key1".to_vec(), Some(b"value1".to_vec())));
        assert_eq!(entries[1], (b"key2".to_vec(), Some(b"value2".to_vec())));
    }
}
