use crate::error::Result;
use crate::{wal::wal::Wal, Error};
use crossbeam_skiplist::{map::Entry, SkipMap};
use std::{
    ops::{Bound, RangeBounds},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

// MAX_MEMTABLE_SIZE is the maximum size of the Memtable in bytes.
pub const MAX_MEMTABLE_SIZE: usize = 64 * 1024 * 1024; // 64MB

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
    pub fn new(dir: &str, id: u64) -> Result<Self> {
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

    pub fn from_wal(wal: Wal) -> Result<Self> {
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

impl Clone for Memtable {
    fn clone(&self) -> Self {
        Memtable {
            id: self.id,                                                       // Copy the ID directly
            data: Arc::clone(&self.data), // Clone the Arc for shared data
            wal: Arc::clone(&self.wal),   // Clone the Arc for the WAL
            size: AtomicUsize::new(self.size.load(Ordering::SeqCst)), // Initialize a new AtomicUsize with the current size
            is_frozen: AtomicBool::new(self.is_frozen.load(Ordering::SeqCst)), // Initialize a new AtomicBool with the current frozen state
        }
    }
}

impl Memtable {
    /// Inserts or updates a key-value pair in the Memtable.
    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
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
    pub fn freeze(&self) -> Result<()> {
        if self.is_frozen.swap(true, Ordering::SeqCst) {
            return Err(Error::Frozen);
        }
        Ok(())
    }

    // Scan range of keys
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIter> {
        // Extract start and end bounds from the range
        let start = match range.start_bound() {
            std::ops::Bound::Included(key) => std::ops::Bound::Included(key.clone()),
            std::ops::Bound::Excluded(key) => std::ops::Bound::Excluded(key.clone()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(key) => std::ops::Bound::Included(key.clone()),
            std::ops::Bound::Excluded(key) => std::ops::Bound::Excluded(key.clone()),
            std::ops::Bound::Unbounded => std::ops::Bound::Unbounded,
        };

        // Create a Range with explicit bounds
        let skipmap_range = self.data.range((start, end));
        Ok(ScanIter {
            inner: skipmap_range,
        })
    }
}

type SkipMapRange<'a> = crossbeam_skiplist::map::Range<
    'a,
    Vec<u8>,
    (Bound<Vec<u8>>, Bound<Vec<u8>>),
    Vec<u8>,
    Option<Vec<u8>>,
>;

pub struct ScanIter<'a> {
    inner: SkipMapRange<'a>,
}

impl<'a> ScanIter<'a> {
    /// Maps a SkipMap Entry to the expected output format.
    fn map(entry: Entry<'_, Vec<u8>, Option<Vec<u8>>>) -> <Self as Iterator>::Item {
        let key = entry.key();
        let value = entry.value();
        Ok((key.clone(), value.clone().unwrap_or_default()))
    }
}

impl<'a> Iterator for ScanIter<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Self::map)
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

        // Pull values out and compare them
        let first = scan_iter
            .next()
            .expect("Expected first result")
            .expect("Error in first result");
        assert_eq!(first, (b"key1".to_vec(), b"value1".to_vec()));

        let second = scan_iter
            .next()
            .expect("Expected second result")
            .expect("Error in second result");
        assert_eq!(second, (b"key2".to_vec(), b"value2".to_vec()));

        assert!(scan_iter.next().is_none(), "Expected no more results");
    }

    #[test]
    fn test_memtable_keys_are_sorted() {
        let temp_dir = create_temp_dir();
        let memtable = create_temp_memtable(&temp_dir);

        // Insert keys in unsorted order
        memtable
            .put(b"key3".to_vec(), Some(b"value3".to_vec()))
            .unwrap();
        memtable
            .put(b"key1".to_vec(), Some(b"value1".to_vec()))
            .unwrap();
        memtable
            .put(b"key2".to_vec(), Some(b"value2".to_vec()))
            .unwrap();

        // Scan all keys
        let scanned_keys: Vec<_> = memtable
            .scan(b"key1".to_vec()..=b"key3".to_vec())
            .unwrap()
            .map(|res| res.unwrap().0) // Extract keys
            .collect();

        // Assert keys are in sorted order
        assert_eq!(
            scanned_keys,
            vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()]
        );
    }
}
