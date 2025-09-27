// src/store/lsm/sstable/table.rs

//! This module manages the lifecycle of an SSTable file, from creation and
//! writing to opening and reading.
//!
//! ## SSTable File Layout
//!
//! An SSTable file consists of three major sections: a sequence of data blocks,
//! an index block, and a footer containing the offset of the index block.
//!
//! ```text
//! +-------------------+
//! | Data Block 1      |
//! +-------------------+
//! | Data Block 2      |
//! +-------------------+
//! | ...               |
//! +-------------------+
//! | Data Block N      |
//! +-------------------+
//! | Index Block       |
//! +-------------------+
//! | Index Offset (u64)|
//! +-------------------+
//! ```
//!
//! The `Table` enum represents an SSTable and can be in one of two states:
//! `Writable` for building a new table, or `Readable` for querying an existing one.
//!
use super::block::{Block, MultiBlockIterator};
use super::filter::RangeFilter;
use super::index::Index;
use crate::error::Result;
use crate::Error;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;

/// The target maximum size for a data block. In practice, a block may be slightly
/// larger to accommodate the last key-value pair that would have otherwise
/// caused it to exceed this size.
pub const MAX_BLOCK_SIZE: usize = 4096;

/// Represents an SSTable, which can be in either a writable or readable state.
/// This enum acts as a state machine to ensure that operations like `get` cannot
/// be called on a table that is still being written.
pub enum Table {
    Writable(WritableTable),
    Readable(ReadableTable),
}

/// A handle to an SSTable that is currently being written.
pub struct WritableTable {
    /// The file handle for the SSTable.
    file: File,
    /// The path to the SSTable file.
    path: String,
    /// The sparse index, built in memory as blocks are added.
    index: Index,
    /// The current write offset in the file.
    offset: u64,
}

impl Table {
    /// Creates a new `WritableTable` at the given path.
    pub fn writable(path: &str) -> Result<Self> {
        Ok(Self::Writable(WritableTable::new(path)?))
    }

    /// Opens an existing SSTable file for reading.
    pub fn readable(path: &str) -> Result<Self> {
        Ok(Self::Readable(ReadableTable::open(path)?))
    }

    /// Adds a new data block to a `WritableTable`.
    ///
    /// The `first_key` is used to create an entry in the sparse index.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidOperation` error if the table is in a readable state.
    pub fn add_block(&mut self, block_data: &[u8], first_key: Vec<u8>) -> Result<()> {
        match self {
            Table::Writable(writable) => writable.add_block(block_data, first_key),
            Table::Readable(_) => Err(Error::InvalidOperation(
                "Cannot add block to a readable table".to_string(),
            )),
        }
    }

    /// Finalizes a `WritableTable`, making it readable.
    ///
    /// This method writes the index block and the footer to the file, then
    /// transitions the table state from `Writable` to `Readable`.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidOperation` error if the table is already in a readable state.
    pub fn finalize(&mut self) -> Result<Table> {
        match self {
            Table::Writable(writable) => writable.finalize(),
            Table::Readable(_) => Err(Error::InvalidOperation(
                "Cannot finalize a readable table".to_string(),
            )),
        }
    }

    /// Retrieves the value for a given key from a `ReadableTable`.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidOperation` error if the table is in a writable state.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self {
            Table::Readable(readable) => readable.get(key),
            Table::Writable(_) => Err(Error::InvalidOperation(
                "Cannot get from a writable table".to_string(),
            )),
        }
    }

    /// Creates an iterator over a range of key-value pairs in a `ReadableTable`.
    ///
    /// # Errors
    ///
    /// Returns an `InvalidOperation` error if the table is in a writable state.
    pub fn scan<R>(&self, range: R) -> Result<ScanIterator<R>>
    where
        R: RangeBounds<Vec<u8>> + Clone + Send + Sync,
    {
        match self {
            Table::Readable(readable) => readable.scan(range),
            Table::Writable(_) => Err(Error::InvalidOperation(
                "Cannot scan a writable table".to_string(),
            )),
        }
    }
}

impl WritableTable {
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file,
            path: path.to_string(),
            index: Index::new(),
            offset: 0,
        })
    }

    pub fn add_block(&mut self, block_data: &[u8], first_key: Vec<u8>) -> Result<()> {
        let block_size = block_data.len() as u64;
        self.file.write_all(block_data)?;
        self.index.push(first_key, self.offset, block_size);
        self.offset += block_data.len() as u64;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<Table> {
        let index_data: Vec<u8> = self.index.clone().try_into()?;
        let index_offset = self.offset;

        self.file.write_all(&index_data)?;
        self.file.write_u64::<BigEndian>(index_offset)?;
        self.file.flush()?;
        // Create a new `ReadableTable` from the same file path
        let readable = ReadableTable::open(&self.path)?;
        // Return the table in readable mode
        Ok(Table::Readable(readable))
    }
}

pub struct ReadableTable {
    file: File,
    index: Index,
}

impl ReadableTable {
    /// Opens an SSTable file and loads its index into memory.
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        file.seek(SeekFrom::End(-8))?;
        let index_offset = file.read_u64::<BigEndian>()?;

        let mut index_data = vec![0u8; (file_size - 8 - index_offset) as usize];
        file.seek(SeekFrom::Start(index_offset))?;
        file.read_exact(&mut index_data)?;

        let index = Index::try_from(index_data.as_slice())?;

        Ok(Self { file, index })
    }

    /// Retrieves the value for a given key.
    ///
    /// This method first uses the sparse index to find the data block that may
    /// contain the key, then reads that block from disk and searches within it.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(entry) = self.index.find(key) {
            let (block_offset, block_size) = (entry.offset, entry.size);

            let mut block_data = vec![0u8; block_size as usize];
            let mut file_reader = self.file.try_clone()?;
            file_reader.seek(SeekFrom::Start(block_offset))?;
            file_reader.read_exact(&mut block_data)?;

            let block = Block::new(block_data)?;
            return block.get(key);
        }
        Ok(None)
    }

    /// Creates an iterator over a range of key-value pairs.
    pub fn scan<R>(&self, range: R) -> Result<ScanIterator<R>>
    where
        R: RangeBounds<Vec<u8>> + Clone + Send + Sync,
    {
        let blocks = self.index.range(range.clone());
        let reader = self.file.try_clone()?;

        let mut multi_block_iter = MultiBlockIterator::new(reader, blocks)?;

        // If we have a start bound, seek to it in the first block
        if let Some(start_key) = Self::get_start_key(&range) {
            multi_block_iter.seek_first_block(start_key)?;
        }

        Ok(RangeFilter::new(multi_block_iter, range))
    }

    /// Helper to extract the start key from a range for seeking
    fn get_start_key<R>(range: &R) -> Option<&[u8]>
    where
        R: RangeBounds<Vec<u8>>,
    {
        use std::ops::Bound;

        match range.start_bound() {
            Bound::Included(key) | Bound::Excluded(key) => Some(key.as_slice()),
            Bound::Unbounded => None,
        }
    }
}

/// An iterator over a range of key-value pairs in an SSTable.
/// This is a composition of MultiBlockIterator and RangeFilter.
pub type ScanIterator<R> = RangeFilter<MultiBlockIterator, R>;

#[cfg(test)]
mod tests {
    use crate::tmpfs::NamedTempFile;
    use std::sync::Arc;

    use super::*;
    use crate::store::lsm::sstable::block::Builder;

    #[test]
    fn test_write_and_read() {
        // Sample data
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"application".to_vec(), b"software".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
            (b"band".to_vec(), b"music".to_vec()),
            (b"bandana".to_vec(), b"clothing".to_vec()),
        ];

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap();

        // Create a writable table
        let mut table = Table::writable(path).expect("Failed to create writable table");

        // Build blocks and add them to the table
        let mut builder = Builder::new();
        let mut first_key_in_block: Option<Vec<u8>> = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, value);

            if builder.len() >= 2 {
                let block_data = builder.finish();
                let first_key = first_key_in_block.take().expect("Missing first key");
                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                builder = Builder::new();
            }
        }

        // Add the last block if there are any remaining entries
        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block.take().expect("Missing first key");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        // Finalize the table
        let table = table.finalize().expect("Failed to finalize table");

        // Read entries from the table
        if let Table::Readable(readable) = table {
            for (key, value) in entries {
                let result = readable
                    .get(&key)
                    .expect("Error during read")
                    .expect("Key not found");
                assert_eq!(result, value, "Value mismatch for key {:?}", key);
            }
        } else {
            panic!("Expected readable table after finalize");
        }
    }

    // src/store/lsm/sstable/table.rs

    #[test]
    fn test_scan() {
        use crate::tmpfs::NamedTempFile;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Write sample SSTable data
        let mut table = Table::writable(path.to_str().unwrap()).expect("Failed to create table");

        // Generate a larger number of entries to ensure multiple blocks are created
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                let key = format!("key_{:03}", i).into_bytes();
                let value = format!("value_{:03}", i).into_bytes();
                (key, value)
            })
            .collect();

        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in entries.iter() {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, value);

            // Flush block when it reaches a certain size to create multiple blocks
            if builder.len() >= 256 {
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");

                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");

                builder = Builder::new();
            }
        }

        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        let table = match table.finalize().expect("Failed to finalize table") {
            Table::Readable(readable) => readable,
            _ => panic!("Table did not transition to readable mode"),
        };

        // Define the exact range to scan
        let scan_range = b"key_010".to_vec()..=b"key_020".to_vec();
        let mut scan_iter = table
            .scan(scan_range)
            .expect("Failed to create scan iterator");

        // Define the EXACT expected entries for that range
        let expected_entries: Vec<_> = entries[10..=20].to_vec();
        for (expected_key, expected_value) in expected_entries {
            let (key, value) = scan_iter
                .next()
                .expect("Iterator ended prematurely; missing entry")
                .expect("Iterator returned an error");
            assert_eq!(key, expected_key);
            assert_eq!(value, expected_value);
        }

        // Ensure the iterator is now exhausted
        assert!(
            scan_iter.next().is_none(),
            "Iterator returned more entries than expected"
        );
    }

    #[test]
    fn test_get_nonexistent_key() {
        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap();

        // Sample data
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
        ];

        // Create a writable table
        let mut table = Table::writable(path).expect("Failed to create writable table");

        // Build blocks and add them to the table
        let mut builder = Builder::new();
        let mut first_key_in_block: Option<Vec<u8>> = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, value);
        }

        let block_data = builder.finish();
        let first_key = first_key_in_block.take().expect("Missing first key");
        table
            .add_block(&block_data, first_key)
            .expect("Failed to add block");

        // Finalize the table
        let table = table.finalize().expect("Failed to finalize table");

        // Test reading a non-existent key
        if let Table::Readable(readable) = table {
            let non_existent_key = b"nonexistent".to_vec();
            let result = readable.get(&non_existent_key).expect("Error during read");
            assert!(result.is_none(), "Non-existent key unexpectedly found");
        } else {
            panic!("Expected readable table after finalize");
        }
    }

    #[tokio::test]
    async fn test_concurrent_sstable_scans() {
        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Create an SSTable with a large number of entries to ensure multiple blocks
        let mut table = Table::writable(path.to_str().unwrap()).expect("Failed to create table");

        // Generate 1000 entries to ensure we have multiple blocks and good test coverage
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1000)
            .map(|i| {
                let key = format!("key_{:04}", i).into_bytes();
                let value = format!("value_{:04}", i).into_bytes();
                (key, value)
            })
            .collect();

        // Add entries to blocks (similar to existing test pattern)
        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }
            builder.add_entry(key, value);

            // Flush block when it reaches a certain size to create multiple blocks
            if builder.len() >= 256 {
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");
                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                builder = Builder::new();
            }
        }

        // Add the last block if it has data
        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        // Finalize the table
        let table = match table.finalize().expect("Failed to finalize table") {
            Table::Readable(readable) => Arc::new(readable),
            _ => panic!("Table did not transition to readable mode"),
        };

        // Test configuration - with tokio we can handle many more concurrent tasks
        const NUM_TASKS: usize = 20;
        const ENTRIES_PER_TASK: usize = 50; // Each task scans 50 entries

        let mut tasks = Vec::new();

        // Spawn multiple async tasks with non-overlapping ranges to test basic concurrency
        for task_id in 0..NUM_TASKS {
            let table_clone = table.clone();
            let start_idx = task_id * ENTRIES_PER_TASK;
            let end_idx = start_idx + ENTRIES_PER_TASK - 1;

            let task = tokio::spawn(async move {
                // Define the scan range for this task
                let start_key = format!("key_{:04}", start_idx).into_bytes();
                let end_key = format!("key_{:04}", end_idx).into_bytes();
                let scan_range = start_key..=end_key;

                // Perform the scan
                let scan_iter = table_clone
                    .scan(scan_range)
                    .expect("Failed to create scan iterator");

                let mut count = 0;
                let mut last_key = Vec::new();

                // Collect and verify results
                for result in scan_iter {
                    let (key, value) = result.expect("Failed to read entry during concurrent scan");

                    // Verify the key-value pair is correct
                    let expected_value = format!("value_{:04}", start_idx + count).into_bytes();
                    assert_eq!(
                        value,
                        expected_value,
                        "Task {} got incorrect value for key {:?}",
                        task_id,
                        String::from_utf8_lossy(&key)
                    );

                    // Verify keys are in order
                    if !last_key.is_empty() {
                        assert!(
                            key > last_key,
                            "Task {} keys not in order: {:?} <= {:?}",
                            task_id,
                            String::from_utf8_lossy(&key),
                            String::from_utf8_lossy(&last_key)
                        );
                    }

                    last_key = key;
                    count += 1;

                    // Yield control to allow other tasks to run (simulates real async I/O)
                    if count % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                // Return results for verification
                (task_id, count)
            });

            tasks.push(task);
        }

        // Wait for all tasks and verify results
        for task in tasks {
            let (task_id, count) = task.await.expect("Task panicked");
            assert_eq!(
                count, ENTRIES_PER_TASK,
                "Task {} read {} entries, expected {}",
                task_id, count, ENTRIES_PER_TASK
            );
        }
    }

    #[tokio::test]
    async fn test_overlapping_range_concurrent_scans() {
        use std::collections::HashSet;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Create an SSTable with entries
        let mut table = Table::writable(path.to_str().unwrap()).expect("Failed to create table");

        // Generate 500 entries for overlapping range tests
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..500)
            .map(|i| {
                let key = format!("key_{:04}", i).into_bytes();
                let value = format!("value_{:04}", i).into_bytes();
                (key, value)
            })
            .collect();

        // Add entries to blocks
        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }
            builder.add_entry(key, value);

            if builder.len() >= 256 {
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");
                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                builder = Builder::new();
            }
        }

        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        let table = match table.finalize().expect("Failed to finalize table") {
            Table::Readable(readable) => Arc::new(readable),
            _ => panic!("Table did not transition to readable mode"),
        };

        // Test concurrent scans with heavily overlapping ranges
        // Each task scans 200 consecutive entries with significant overlap
        const NUM_TASKS: usize = 10;
        let mut tasks = Vec::new();

        for task_id in 0..NUM_TASKS {
            let table_clone = table.clone();
            let start_idx = task_id * 30; // 30 entry step creates heavy overlap
            let end_idx = std::cmp::min(start_idx + 200, 500);

            let task = tokio::spawn(async move {
                let start_key = format!("key_{:04}", start_idx).into_bytes();
                let end_key = format!("key_{:04}", end_idx - 1).into_bytes();
                let scan_range = start_key..=end_key;

                let scan_iter = table_clone
                    .scan(scan_range)
                    .expect("Failed to create scan iterator");

                let mut results = Vec::new();
                let mut seen_keys = HashSet::new();

                for result in scan_iter {
                    let (key, value) = result.expect("Failed to read entry during concurrent scan");

                    // Verify key is within expected range
                    let start_key_bytes = format!("key_{:04}", start_idx).into_bytes();
                    let end_key_bytes = format!("key_{:04}", end_idx - 1).into_bytes();

                    assert!(
                        key >= start_key_bytes && key <= end_key_bytes,
                        "Task {} key {:?} outside range [{:04}, {:04}]",
                        task_id,
                        String::from_utf8_lossy(&key),
                        start_idx,
                        end_idx - 1
                    );

                    // Check for duplicates within this task's results
                    assert!(
                        seen_keys.insert(key.clone()),
                        "Task {} found duplicate key: {:?}",
                        task_id,
                        String::from_utf8_lossy(&key)
                    );

                    results.push((key, value));

                    // Yield occasionally to allow interleaving
                    if results.len() % 20 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                // Verify we got the correct number of entries
                let expected_count = end_idx - start_idx;
                assert_eq!(
                    results.len(),
                    expected_count,
                    "Task {} expected {} entries but got {}",
                    task_id,
                    expected_count,
                    results.len()
                );

                (task_id, results.len(), start_idx, end_idx)
            });

            tasks.push(task);
        }

        // Wait for all tasks and verify each got correct data
        for task in tasks {
            let (task_id, count, start_idx, end_idx) = task.await.expect("Task panicked");
            let expected_count = end_idx - start_idx;
            assert_eq!(
                count, expected_count,
                "Task {} expected {} entries but got {}",
                task_id, expected_count, count
            );
        }
    }

    #[tokio::test]
    async fn test_mixed_read_workload_concurrent() {
        use std::collections::HashSet;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Create an SSTable with entries
        let mut table = Table::writable(path.to_str().unwrap()).expect("Failed to create table");

        // Generate 800 entries for mixed workload tests
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..800)
            .map(|i| {
                let key = format!("key_{:04}", i).into_bytes();
                let value = format!("value_{:04}", i).into_bytes();
                (key, value)
            })
            .collect();

        // Add entries to blocks
        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }
            builder.add_entry(key, value);

            if builder.len() >= 256 {
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");
                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                builder = Builder::new();
            }
        }

        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        let table = match table.finalize().expect("Failed to finalize table") {
            Table::Readable(readable) => Arc::new(readable),
            _ => panic!("Table did not transition to readable mode"),
        };

        // Mixed workload: some tasks do gets, others do scans
        const NUM_GET_TASKS: usize = 8;
        const NUM_SCAN_TASKS: usize = 8;
        let mut tasks = Vec::new();

        // Spawn get tasks - each does 100 random gets
        for task_id in 0..NUM_GET_TASKS {
            let table_clone = table.clone();

            let task = tokio::spawn(async move {
                let mut successful_gets = 0;
                let mut get_keys = Vec::new();

                // Each task gets keys at regular intervals with some randomness
                for i in 0..100 {
                    let key_idx = (task_id * 100 + i) % 800;
                    let key = format!("key_{:04}", key_idx).into_bytes();
                    get_keys.push(key.clone());

                    match table_clone.get(&key).expect("Get operation failed") {
                        Some(value) => {
                            let expected_value = format!("value_{:04}", key_idx).into_bytes();
                            assert_eq!(
                                value,
                                expected_value,
                                "Get task {} got wrong value for key {:?}",
                                task_id,
                                String::from_utf8_lossy(&key)
                            );
                            successful_gets += 1;
                        }
                        None => panic!(
                            "Get task {} missing key: {:?}",
                            task_id,
                            String::from_utf8_lossy(&key)
                        ),
                    }

                    // Yield periodically to allow scan tasks to run
                    if i % 10 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                ("get", task_id, successful_gets, get_keys.len())
            });

            tasks.push(task);
        }

        // Spawn scan tasks - each scans different ranges
        for task_id in 0..NUM_SCAN_TASKS {
            let table_clone = table.clone();
            let start_idx = task_id * 80; // 80 entry ranges with some overlap
            let end_idx = std::cmp::min(start_idx + 120, 800);

            let task = tokio::spawn(async move {
                let start_key = format!("key_{:04}", start_idx).into_bytes();
                let end_key = format!("key_{:04}", end_idx - 1).into_bytes();
                let scan_range = start_key..=end_key;

                let scan_iter = table_clone
                    .scan(scan_range)
                    .expect("Failed to create scan iterator");

                let mut scanned_entries = 0;
                let mut seen_keys = HashSet::new();

                for result in scan_iter {
                    let (key, value) = result.expect("Scan failed");

                    // Verify correctness
                    let key_str = String::from_utf8_lossy(&key);
                    if let Some(key_num) = key_str.strip_prefix("key_") {
                        if let Ok(key_idx) = key_num.parse::<usize>() {
                            let expected_value = format!("value_{:04}", key_idx).into_bytes();
                            assert_eq!(
                                value, expected_value,
                                "Scan task {} got wrong value for key {:?}",
                                task_id, key_str
                            );
                        }
                    }

                    // Check for duplicates
                    assert!(
                        seen_keys.insert(key.clone()),
                        "Scan task {} found duplicate key: {:?}",
                        task_id,
                        key_str
                    );

                    scanned_entries += 1;

                    // Yield periodically to allow get tasks to run
                    if scanned_entries % 15 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                let expected_count = end_idx - start_idx;
                assert_eq!(
                    scanned_entries, expected_count,
                    "Scan task {} expected {} entries but got {}",
                    task_id, expected_count, scanned_entries
                );

                ("scan", task_id, scanned_entries, expected_count)
            });

            tasks.push(task);
        }

        // Wait for all tasks and verify results
        let mut total_gets = 0;
        let mut total_scans = 0;

        for task in tasks {
            match task.await.expect("Task panicked") {
                ("get", task_id, successful_gets, expected_gets) => {
                    assert_eq!(
                        successful_gets, expected_gets,
                        "Get task {} completed {}/{} gets",
                        task_id, successful_gets, expected_gets
                    );
                    total_gets += successful_gets;
                }
                ("scan", task_id, scanned_entries, expected_count) => {
                    assert_eq!(
                        scanned_entries, expected_count,
                        "Scan task {} scanned {}/{} entries",
                        task_id, scanned_entries, expected_count
                    );
                    total_scans += scanned_entries;
                }
                _ => panic!("Unknown task type"),
            }
        }

        println!(
            "Mixed workload completed: {} gets, {} scan entries",
            total_gets, total_scans
        );
    }

    #[tokio::test]
    async fn test_file_handle_stress_concurrent() {
        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Create an SSTable with enough entries to create multiple blocks
        let mut table = Table::writable(path.to_str().unwrap()).expect("Failed to create table");

        // Generate 1200 entries to ensure we have multiple blocks
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1200)
            .map(|i| {
                let key = format!("key_{:05}", i).into_bytes();
                let value = format!("value_{:05}_{}", i, "x".repeat(50)).into_bytes(); // Larger values
                (key, value)
            })
            .collect();

        // Add entries to blocks
        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }
            builder.add_entry(key, value);

            if builder.len() >= 1024 {
                // Larger blocks to create more file I/O
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");
                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                builder = Builder::new();
            }
        }

        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        let table = match table.finalize().expect("Failed to finalize table") {
            Table::Readable(readable) => Arc::new(readable),
            _ => panic!("Table did not transition to readable mode"),
        };

        // High concurrency stress test specifically targeting file handle cloning
        // This test uses many concurrent tasks to stress File::try_clone()
        const HIGH_CONCURRENCY_TASKS: usize = 50;
        let mut tasks = Vec::new();

        for task_id in 0..HIGH_CONCURRENCY_TASKS {
            let table_clone = table.clone();

            let task = tokio::spawn(async move {
                let mut operation_count = 0;
                let mut successful_operations = 0;

                // Each task performs a mix of operations
                for operation_idx in 0..100 {
                    operation_count += 1;

                    match operation_idx % 3 {
                        0 => {
                            // GET operation - creates file clone for single block read
                            let key_idx = (task_id * 100 + operation_idx) % 1200;
                            let key = format!("key_{:05}", key_idx).into_bytes();

                            match table_clone.get(&key) {
                                Ok(Some(value)) => {
                                    // Verify the value is correct
                                    let expected_value =
                                        format!("value_{:05}_{}", key_idx, "x".repeat(50))
                                            .into_bytes();
                                    assert_eq!(
                                        value, expected_value,
                                        "Task {} get operation got wrong value for key {}",
                                        task_id, key_idx
                                    );
                                    successful_operations += 1;
                                }
                                Ok(None) => {
                                    panic!(
                                        "Task {} get operation missing key {}",
                                        task_id, key_idx
                                    );
                                }
                                Err(e) => {
                                    eprintln!(
                                        "Task {} get operation failed for key {}: {:?}",
                                        task_id, key_idx, e
                                    );
                                    // Don't panic here - let's see if we can detect the file clone issue
                                }
                            }
                        }
                        1 => {
                            // SCAN operation - creates file clone for multi-block iterator
                            let start_idx = (task_id * 50 + operation_idx) % 1000;
                            let end_idx = std::cmp::min(start_idx + 50, 1200);
                            let start_key = format!("key_{:05}", start_idx).into_bytes();
                            let end_key = format!("key_{:05}", end_idx - 1).into_bytes();

                            match table_clone.scan(start_key..=end_key) {
                                Ok(mut scan_iter) => {
                                    let mut scan_count = 0;
                                    let mut scan_error = false;

                                    for result in scan_iter.by_ref() {
                                        match result {
                                            Ok((key, value)) => {
                                                // Basic verification
                                                if let Ok(key_str) = std::str::from_utf8(&key) {
                                                    if let Some(key_num) =
                                                        key_str.strip_prefix("key_")
                                                    {
                                                        if let Ok(key_idx) =
                                                            key_num.parse::<usize>()
                                                        {
                                                            let expected_value = format!(
                                                                "value_{:05}_{}",
                                                                key_idx,
                                                                "x".repeat(50)
                                                            )
                                                            .into_bytes();
                                                            if value != expected_value {
                                                                eprintln!("Task {} scan got wrong value for key {}", task_id, key_idx);
                                                            }
                                                        }
                                                    }
                                                }
                                                scan_count += 1;
                                            }
                                            Err(e) => {
                                                eprintln!("Task {} scan error: {:?}", task_id, e);
                                                scan_error = true;
                                                break;
                                            }
                                        }

                                        // Yield occasionally during long scans
                                        if scan_count % 10 == 0 {
                                            tokio::task::yield_now().await;
                                        }
                                    }

                                    if !scan_error && scan_count > 0 {
                                        successful_operations += 1;
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Task {} scan creation failed: {:?}", task_id, e);
                                }
                            }
                        }
                        _ => {
                            // Another GET operation with different key pattern
                            let key_idx = (task_id * 200 + operation_idx * 3) % 1200;
                            let key = format!("key_{:05}", key_idx).into_bytes();

                            match table_clone.get(&key) {
                                Ok(Some(_)) => successful_operations += 1,
                                Ok(None) => panic!("Task {} missing key {}", task_id, key_idx),
                                Err(e) => {
                                    eprintln!(
                                        "Task {} get operation failed for key {}: {:?}",
                                        task_id, key_idx, e
                                    );
                                }
                            }
                        }
                    }

                    // Brief yield after every few operations to allow other tasks to run
                    if operation_idx % 5 == 0 {
                        tokio::task::yield_now().await;
                    }
                }

                (task_id, successful_operations, operation_count)
            });

            tasks.push(task);
        }

        // Wait for all tasks and analyze results
        let mut total_successful = 0;
        let mut total_operations = 0;
        let mut failed_tasks = 0;

        for task in tasks {
            let (task_id, successful, total) = task.await.expect("Task panicked");
            total_successful += successful;
            total_operations += total;

            // Consider a task failed if it completed less than 90% of operations
            let success_rate = successful as f64 / total as f64;
            if success_rate < 0.9 {
                eprintln!(
                    "Task {} had low success rate: {}/{} ({:.1}%)",
                    task_id,
                    successful,
                    total,
                    success_rate * 100.0
                );
                failed_tasks += 1;
            }
        }

        let overall_success_rate = total_successful as f64 / total_operations as f64;
        println!(
            "File handle stress test: {}/{} operations succeeded ({:.2}%)",
            total_successful,
            total_operations,
            overall_success_rate * 100.0
        );

        // Assert that we have a reasonable success rate
        // If File::try_clone() fails under high concurrency, we should see this
        assert!(
            overall_success_rate >= 0.95,
            "File handle stress test had too many failures: {:.2}% success rate, {} failed tasks",
            overall_success_rate * 100.0,
            failed_tasks
        );
    }
}
