use crate::error::Result;
use crate::sstable::{block, table};
use crate::{wal::Wal, Error};
use crossbeam_skiplist::{map::Entry, SkipMap};
use std::sync::Arc;
use std::{
    ops::{Bound, RangeBounds},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

// MAX_MEMTABLE_SIZE is the maximum size of the Memtable in bytes.
pub const MAX_MEMTABLE_SIZE: usize = 64 * 1024 * 1024; // 64MB

#[derive(Debug)]
pub struct Memtable {
    data: Arc<SkipMap<Vec<u8>, Option<Vec<u8>>>>,
    wal: Wal,
    size: AtomicUsize,
    is_frozen: AtomicBool,
}

impl Memtable {
    /// Creates a new empty Memtable with a new WAL.
    pub fn new(wal_path: &str) -> Result<Self> {
        let wal = Wal::new(wal_path)?;
        Ok(Self {
            data: Arc::new(SkipMap::new()),
            wal: wal,
            size: AtomicUsize::new(0),
            is_frozen: AtomicBool::new(false),
        })
    }

    pub fn from_wal(wal: Wal) -> Result<Self> {
        let data = Arc::new(SkipMap::new());
        let size = AtomicUsize::new(0);

        // Replay the WAL file
        let replay_iter = wal.replay()?;
        for entry in replay_iter {
            let (key, value) = entry?;
            let entry_size = key.len() + value.as_ref().map_or(0, |v| v.len());
            size.fetch_add(entry_size, Ordering::SeqCst);
            data.insert(key, value);
        }

        Ok(Self {
            data,
            wal,
            size,
            is_frozen: AtomicBool::new(false),
        })
    }
}

// impl Clone for Memtable {
//     fn clone(&self) -> Self {
//         Memtable {
//             // Copy the ID directly
//             data: self.data.clone(), // Clone the Arc for the SkipMap
//             wal: &self.wal.clone(),  // Clone the Arc for the WAL
//             size: AtomicUsize::new(self.size.load(Ordering::SeqCst)), // Initialize a new AtomicUsize with the current size
//             is_frozen: AtomicBool::new(self.is_frozen.load(Ordering::SeqCst)), // Initialize a new AtomicBool with the current frozen state
//         }
//     }
// }

impl Memtable {
    /// Inserts or updates a key-value pair in the Memtable.
    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        if self.is_frozen.load(Ordering::SeqCst) {
            return Err(Error::Frozen);
        }
        let key_size = key.len();
        let value_size = value.as_ref().map_or(0, |v| v.len());
        let entry_size = key_size + value_size;

        self.wal.put(&key, value.as_deref())?;
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

    // sync synchronizes the Memtable's WAL.
    pub fn sync(&self) -> Result<()> {
        self.wal.sync()
    }
}

impl Memtable {
    pub fn flush(&self, table: &mut table::Table) -> Result<()> {
        let mut builder = block::Builder::new();
        let mut first_key_in_block: Option<Vec<u8>> = None;

        for entry in self.data.iter() {
            let key = entry.key();
            let value = entry.value().clone().unwrap_or_default();

            // If this is the first entry in the block, record the key
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, &value);

            // Check if the block size exceeds the limit (e.g., 4KB)
            if builder.len() >= table::MAX_BLOCK_SIZE {
                // Finish the block and write it to SSTable
                let block_data = builder.finish();
                match first_key_in_block.take() {
                    Some(first_key) => {
                        table.add_block(&block_data, first_key)?;
                    }
                    None => {
                        return Err(Error::InvalidState(
                            "First key in block is missing".to_string(),
                        ));
                    }
                }
                builder = block::Builder::new(); // Start a new block
                first_key_in_block = None; // Reset for the next block
            }
        }

        // Write the last block if any entries are left
        if builder.entry_count() > 0 {
            let block_data = builder.finish();
            match first_key_in_block.take() {
                Some(first_key) => {
                    table.add_block(&block_data, first_key)?;
                }
                None => {
                    return Err(Error::InvalidState(
                        "First key in block is missing".to_string(),
                    ));
                }
            }
        }

        Ok(())
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

    // This helper needs updating
    fn create_temp_memtable(temp_dir: &TempDir) -> Memtable {
        let wal_path = temp_dir.path().join("0000.wal");
        Memtable::new(wal_path.to_str().expect("Failed to create WAL path"))
            .expect("Failed to initialize memtable")
    }

    // This helper also needs updating
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
        let wal = create_temp_wal(&temp_dir);

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.put(b"key3", None).expect("Failed to append (key only)");
        wal.sync().expect("Failed to sync");

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

    #[test]
    fn test_sync() {
        let temp_dir = create_temp_dir();
        let wal = create_temp_wal(&temp_dir);

        // Append some key-value pairs
        wal.put(b"key1", Some(b"value1")).expect("Failed to append");
        wal.put(b"key2", Some(b"value2")).expect("Failed to append");
        wal.put(b"key3", None).expect("Failed to append (key only)");

        // Create a Memtable from the WAL
        let memtable = Memtable::from_wal(wal).expect("Failed to create Memtable from WAL");

        // Sync the Memtable
        memtable.sync().expect("Failed to sync");

        // WAL should contain the same data as the Memtable
        let wal = memtable.wal.replay().unwrap();
        for (i, entry) in wal.enumerate() {
            let (key, value) = entry.unwrap();
            match i {
                0 => {
                    assert_eq!(key, b"key1");
                    assert_eq!(value, Some(b"value1".to_vec()));
                }
                1 => {
                    assert_eq!(key, b"key2");
                    assert_eq!(value, Some(b"value2".to_vec()));
                }
                2 => {
                    assert_eq!(key, b"key3");
                    assert_eq!(value, None);
                }
                _ => panic!("Unexpected entry in WAL"),
            }
        }
    }
}
