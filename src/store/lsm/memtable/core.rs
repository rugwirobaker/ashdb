//! Memtable implementation using a concurrent skip list.
//!
//! This module provides an in-memory sorted data structure for storing key-value
//! pairs before they are written to disk. The memtable serves as the first level
//! in the LSM-tree hierarchy and is optimized for both high write throughput
//! and efficient read access.
//!
//! # Data Structure Choice: Skip List
//!
//! A skip list was chosen over alternatives like B-trees or red-black trees for
//! several key reasons:
//!
//! - **Lock-free concurrency**: Supports multiple concurrent readers without locks
//! - **Ordered traversal**: Naturally maintains lexicographical key ordering
//! - **Memory efficiency**: Lower memory overhead compared to tree structures
//! - **Cache performance**: Better cache locality for sequential scans
//! - **Probabilistic balancing**: Self-balancing without complex rebalancing
//!
//! # Crossbeam SkipMap
//!
//! We specifically use `crossbeam-skiplist::SkipMap` because:
//!
//! - **Memory ordering**: Provides strong consistency guarantees using atomic operations
//! - **Concurrent reads**: Multiple threads can read simultaneously without blocking
//! - **Range queries**: Efficient support for bounded range iteration
//! - **Memory safety**: No unsafe code required for concurrent access
//! - **Performance**: Optimized implementation with minimal contention
//!
//! # Lifecycle Management
//!
//! Memtables progress through these states:
//! 1. **Active**: Accepts new writes and grows until size limit
//! 2. **Frozen**: Read-only, queued for flushing to disk
//! 3. **Flushed**: Converted to SSTable and removed from memory
//!
//! The freeze operation is atomic using `AtomicBool` to ensure consistency
//! during concurrent access.
//!
//! # WAL Integration
//!
//! Each memtable is paired with a Write-Ahead Log (WAL) file for durability:
//! - Writes go to WAL first, then to memtable
//! - WAL enables recovery after crashes
//! - WAL is removed after memtable is flushed to SSTable

use super::super::filter::RangeFilter;
use super::super::sstable::{block, table};
use super::super::wal::Wal;
use crate::error::Result;
use crate::Error;
use crossbeam_skiplist::SkipMap;
use std::sync::{Arc, RwLock};
use std::{
    ops::RangeBounds,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

/// In-memory sorted table using a concurrent skip list.
///
/// Stores key-value pairs in memory with atomic size tracking and freeze state.
/// Each memtable is associated with a WAL file for durability.
#[derive(Debug)]
pub struct Memtable {
    data: Arc<SkipMap<Vec<u8>, Option<Vec<u8>>>>,
    wal: Arc<RwLock<Wal>>,
    wal_id: u64,
    size: AtomicUsize,
    frozen: AtomicBool,
}

impl Clone for Memtable {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            wal: self.wal.clone(),
            wal_id: self.wal_id,
            size: AtomicUsize::new(self.size.load(Ordering::SeqCst)),
            frozen: AtomicBool::new(self.frozen.load(Ordering::SeqCst)),
        }
    }
}

impl Memtable {
    /// Creates a new empty Memtable with a new WAL.
    pub fn new(wal_path: &str, wal_id: u64) -> Result<Self> {
        let wal = Arc::new(RwLock::new(Wal::new(wal_path)?));
        Ok(Self {
            data: Arc::new(SkipMap::new()),
            wal,
            wal_id,
            size: AtomicUsize::new(0),
            frozen: AtomicBool::new(false),
        })
    }

    pub fn from_wal(wal: Wal, wal_id: u64) -> Result<Self> {
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

        let wal = Arc::new(RwLock::new(wal));
        Ok(Self {
            data,
            wal,
            wal_id,
            size,
            frozen: AtomicBool::new(false),
        })
    }

    /// Inserts or updates a key-value pair in the Memtable.
    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        if self.frozen.load(Ordering::SeqCst) {
            return Err(Error::ReadOnly);
        }
        let key_size = key.len();
        let value_size = value.as_ref().map_or(0, |v| v.len());
        let entry_size = key_size + value_size;

        self.wal.write().unwrap().append(&key, value.as_deref())?;
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
        if self.frozen.swap(true, Ordering::SeqCst) {
            return Err(Error::ReadOnly);
        }
        Ok(())
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen.load(Ordering::SeqCst)
    }

    pub fn wal_id(&self) -> u64 {
        self.wal_id
    }

    pub fn wal(&self) -> &Arc<RwLock<Wal>> {
        &self.wal
    }

    // Scan range of keys
    pub fn scan<R>(&self, range: R) -> Result<ScanIterator<R>>
    where
        R: RangeBounds<Vec<u8>> + Clone + Send + Sync,
    {
        let memtable_iter = MemtableIterator::new(Arc::new(self.clone()));
        Ok(RangeFilter::new(memtable_iter, range))
    }

    // sync synchronizes the Memtable's WAL.
    pub fn sync(&self) -> Result<()> {
        self.wal.write().unwrap().flush()
    }

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
                        return Err(Error::InvalidData(
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
                    return Err(Error::InvalidData(
                        "First key in block is missing".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

/// Pure memtable iterator that lazily iterates over all entries (like BlockIterator)
pub struct MemtableIterator {
    memtable: Arc<Memtable>,
    current_key: Option<Vec<u8>>,
    exhausted: bool,
}

impl MemtableIterator {
    pub fn new(memtable: Arc<Memtable>) -> Self {
        Self {
            memtable,
            current_key: None,
            exhausted: false,
        }
    }
}

impl Iterator for MemtableIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        // Create a range starting from current_key
        let range = match &self.current_key {
            Some(key) => {
                use std::ops::Bound;
                (Bound::Excluded(key.clone()), Bound::Unbounded)
            }
            None => {
                use std::ops::Bound;
                (Bound::Unbounded, Bound::Unbounded)
            }
        };

        // Get next entry using a fresh SkipMap range
        let mut range_iter = self.memtable.data.range(range);
        match range_iter.next() {
            Some(entry) => {
                let key = entry.key().clone();
                let value = entry.value().clone().unwrap_or_default();
                self.current_key = Some(key.clone());
                Some(Ok((key, value)))
            }
            None => {
                self.exhausted = true;
                None
            }
        }
    }
}

/// Range-filtered memtable scan iterator (like SSTable's ScanIterator)
pub type ScanIterator<R> = RangeFilter<MemtableIterator, R>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tmpfs::TempDir;

    fn create_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temporary directory")
    }

    fn create_temp_memtable(temp_dir: &TempDir) -> Memtable {
        let wal_path = temp_dir.path().join("0000.wal");
        Memtable::new(wal_path.to_str().expect("Failed to create WAL path"), 0)
            .expect("Failed to initialize memtable")
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
        assert!(matches!(memtable.freeze(), Err(Error::ReadOnly)));
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
            Err(Error::ReadOnly)
        ));
    }

    #[test]
    fn test_from_wal() {
        let temp_dir = create_temp_dir();
        let wal = create_temp_wal(&temp_dir);

        wal.append(b"key1", Some(b"value1"))
            .expect("Failed to append");
        wal.append(b"key2", Some(b"value2"))
            .expect("Failed to append");
        wal.append(b"key3", None)
            .expect("Failed to append (key only)");
        wal.flush().expect("Failed to flush");

        // Create a Memtable from the WAL
        let memtable = Memtable::from_wal(wal, 0).expect("Failed to create Memtable from WAL");

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
        wal.append(b"key1", Some(b"value1"))
            .expect("Failed to append");
        wal.append(b"key2", Some(b"value2"))
            .expect("Failed to append");
        wal.append(b"key3", None)
            .expect("Failed to append (key only)");

        // Create a Memtable from the WAL
        let memtable = Memtable::from_wal(wal, 0).expect("Failed to create Memtable from WAL");

        // Sync the Memtable
        memtable.sync().expect("Failed to sync");

        // WAL should contain the same data as the Memtable
        let wal = memtable.wal.read().unwrap().replay().unwrap();
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
