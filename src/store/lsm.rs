use super::{
    level::{Level, SSTable},
    Store,
};

use crate::{
    error::Result,
    flock::FileLock,
    manifest::{
        record::{FileInfo, Operation, Record},
        Manifest,
    },
    memtable::{Memtable, MAX_MEMTABLE_SIZE},
    sstable::table::Table,
    wal::Wal,
};

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, VecDeque},
    fs,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::Arc,
};

const LOCK_FILE: &str = "ashdb.lock";
const MANIFEST_FILE: &str = "manifest.log";

pub struct LsmStore {
    dir: PathBuf,
    lock: Option<FileLock>,
    active_memtable: Arc<Memtable>,
    frozen_memtables: VecDeque<Arc<Memtable>>,
    levels: Vec<Level>, // For now, one level
    manifest: Manifest,
    next_sstable_id: u64,
    next_wal_id: u64,
}

type Memtables = (Arc<Memtable>, VecDeque<Arc<Memtable>>, u64);

impl LsmStore {
    pub fn open(dir: &str) -> Result<Self> {
        let dir = PathBuf::from(dir);
        fs::create_dir_all(&dir)?;

        let lock = FileLock::lock(dir.join(LOCK_FILE))?;

        // Open or create the manifest
        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest = Manifest::new(&manifest_path)?;

        // Recover state from manifest
        let (levels, next_table_id) = Self::replay_manifest(&dir, &manifest)?;

        // Recover memtables from WAL files
        let (active_memtable, frozen_memtables, next_wal_id) = Self::recover_memtables(&dir)?;

        Ok(Self {
            dir,
            lock: Some(lock),
            active_memtable,
            frozen_memtables,
            levels,
            manifest,
            next_sstable_id: next_table_id,
            next_wal_id,
        })
    }
}

impl LsmStore {
    fn replay_manifest(dir: &Path, manifest: &Manifest) -> Result<(Vec<Level>, u64)> {
        let mut levels = Vec::new();
        let mut next_table_id = 0;

        for record in manifest.iter()? {
            match record? {
                Record::AddTable {
                    id,
                    level,
                    info,
                    op_type: _,
                } => {
                    while levels.len() <= level as usize {
                        levels.push(Level::new(levels.len() as u32));
                    }

                    let path = dir.join(format!("{}.sst", id));
                    let table = Table::readable(path.to_str().unwrap())?;

                    levels[level as usize].add_sstable(SSTable {
                        id,
                        table,
                        path,
                        size: info.size,
                        min_key: info.min_key,
                        max_key: info.max_key,
                    });

                    next_table_id = next_table_id.max(id + 1);
                }
                Record::DeleteTable {
                    id,
                    level,
                    op_type: _,
                } => {
                    if let Some(level) = levels.get_mut(level as usize) {
                        level.sstables.retain(|t| t.id != id);
                    }
                }
            }
        }

        Ok((levels, next_table_id))
    }

    fn recover_memtables(dir: &Path) -> Result<Memtables> {
        let wal_dir = dir.join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let mut wal_files: Vec<_> = std::fs::read_dir(&wal_dir)?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension()?.to_str()? == "wal" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect();

        // Sort WAL files by ID
        wal_files.sort_by_key(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0)
        });

        let mut frozen_memtables = VecDeque::new();
        let mut next_wal_id = 0;

        // Process all but the last WAL file into frozen memtables
        for wal_path in wal_files.iter().rev().skip(1) {
            let wal_id = wal_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            next_wal_id = next_wal_id.max(wal_id + 1);

            let wal = Wal::new(wal_path.to_str().unwrap())?;
            let memtable = Memtable::from_wal(wal)?;
            frozen_memtables.push_back(Arc::new(memtable));
        }

        // Process the last WAL file as active memtable
        let active_memtable = match wal_files.last() {
            Some(active_wal_path) => {
                let wal_id = active_wal_path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);

                next_wal_id = next_wal_id.max(wal_id + 1);

                let wal = Wal::new(active_wal_path.to_str().unwrap())?;
                Arc::new(Memtable::from_wal(wal)?)
            }
            None => {
                let wal_id = next_wal_id;
                next_wal_id += 1;
                let wal_path = wal_dir.join(format!("{}.wal", wal_id));
                let wal = Wal::new(wal_path.to_str().unwrap())?;
                Arc::new(Memtable::new(wal))
            }
        };

        Ok((active_memtable, frozen_memtables, next_wal_id))
    }

    fn flush_memtable(&mut self) -> Result<()> {
        // Get frozen memtable from queue
        let memtable = match self.frozen_memtables.pop_front() {
            Some(m) => m,
            None => return Ok(()),
        };

        // Create new SSTable
        let table_id = self.next_sstable_id;
        self.next_sstable_id += 1;

        let sstable_path = self.dir.join(format!("{}.sst", table_id));
        let mut table = Table::writable(sstable_path.to_str().unwrap())?;

        // Flush memtable to SSTable
        match memtable.flush(&mut table) {
            Ok(()) => {
                // Finalize table
                let table = table.finalize()?;

                // Create FileInfo
                let file_info = FileInfo {
                    id: table_id,
                    size: std::fs::metadata(&sstable_path)?.len(),
                    min_key: vec![], // TODO: Get from table
                    max_key: vec![], // TODO: Get from table
                };

                // Record in manifest
                self.manifest.append(Record::AddTable {
                    id: table_id,
                    level: 0,
                    info: file_info.clone(),
                    op_type: Operation::Flush,
                })?;
                self.manifest.sync()?;

                // Add table to level 0
                if self.levels.is_empty() {
                    self.levels.push(Level::new(0));
                }
                self.levels[0].add_sstable(SSTable {
                    id: table_id,
                    table,
                    path: sstable_path,
                    size: file_info.size,
                    min_key: file_info.min_key,
                    max_key: file_info.max_key,
                });
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(())
    }
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        if let Some(lock) = self.lock.take() {
            let _ = lock.unlock();
        }
    }
}

impl LsmStore {
    pub fn freeze_active_memtable(&mut self) -> Result<()> {
        self.active_memtable.freeze()?;
        let frozen = Arc::clone(&self.active_memtable);
        self.frozen_memtables.push_back(frozen);
        let wal_dir = self.dir.join("wal");
        std::fs::create_dir_all(&wal_dir)?;
        let wal_path = wal_dir.join(format!("{}.wal", self.next_wal_id));
        self.next_wal_id += 1;
        let wal = Wal::new(wal_path.to_str().unwrap())?;
        self.active_memtable = Arc::new(Memtable::new(wal));
        Ok(())
    }
}

impl Store for LsmStore {
    // Define the type of iterator returned by the scan method
    type ScanIterator<'a> = MergeIterator<'a>;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.active_memtable.put(key.to_vec(), Some(value))?;
        // Check size and freeze if necessary
        if self.active_memtable.size() >= MAX_MEMTABLE_SIZE {
            self.freeze_active_memtable()?;
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.active_memtable.get(key) {
            return Ok(value);
        }
        for memtable in self.frozen_memtables.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Ok(value);
            }
        }
        Ok(None)
    }

    fn scan<'a>(&'a self, range: impl RangeBounds<Vec<u8>> + 'a + Clone) -> Self::ScanIterator<'a> {
        type BoxedResultIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>;

        let iterators: Vec<BoxedResultIterator<'a>> = std::iter::once(&self.active_memtable)
            .chain(self.frozen_memtables.iter().rev())
            .map(|memtable| Box::new(memtable.scan(range.clone()).unwrap()) as _)
            .collect();
        MergeIterator::new(iterators)
    }
}

// HeapEntry holds entries from iterators for merging
struct HeapEntry<'a> {
    key: Vec<u8>,   // The key from the iterator
    value: Vec<u8>, // The value associated with the key
    source: usize,  // Index indicating the source iterator (lower index means newer data)
    iterator: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>, // The iterator itself
}

impl<'a> std::fmt::Debug for HeapEntry<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapEntry")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("source", &self.source)
            .finish()
    }
}

impl<'a> PartialEq for HeapEntry<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl<'a> Eq for HeapEntry<'a> {}

impl<'a> PartialOrd for HeapEntry<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        {
            Some(self.cmp(other))
        }
    }
}

impl<'a> Ord for HeapEntry<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            // If keys are equal, prioritize entries from newer sources (lower source index)
            Ordering::Equal => other.source.cmp(&self.source),
            // For different keys, order them in ascending order (smaller keys are 'greater' in heap)
            other => other.reverse(),
        }
    }
}

#[derive(Debug)]
pub struct MergeIterator<'a> {
    heap: BinaryHeap<HeapEntry<'a>>, // Max-heap to keep track of the next smallest key
    latest_key: Option<Vec<u8>>,     // The last key that was returned
}

impl<'a> MergeIterator<'a> {
    pub fn new(iterators: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Initialize the heap with the first item from each iterator
        for (source, mut iterator) in iterators.into_iter().enumerate() {
            if let Some(Ok((key, value))) = iterator.next() {
                // Push the initial entry onto the heap
                heap.push(HeapEntry {
                    key,
                    value,
                    source,
                    iterator,
                });
            }
        }

        Self {
            heap,
            latest_key: None, // Initially no key has been processed
        }
    }
}

impl<'a> Iterator for MergeIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Continue until there are no more entries in the heap
        while let Some(mut entry) = self.heap.pop() {
            // Check if the key is a duplicate of the last key returned
            if self.latest_key.as_ref() == Some(&entry.key) {
                // Fetch the next item from the same iterator and push it onto the heap
                if let Some(Ok((key, value))) = entry.iterator.next() {
                    self.heap.push(HeapEntry {
                        key,
                        value,
                        source: entry.source,
                        iterator: entry.iterator,
                    });
                }
                continue; // Skip to the next entry
            }

            self.latest_key = Some(entry.key.clone());

            // Fetch the next item from the same iterator and push it onto the heap
            if let Some(Ok((key, value))) = entry.iterator.next() {
                self.heap.push(HeapEntry {
                    key,
                    value,
                    source: entry.source,
                    iterator: entry.iterator,
                });
            }
            return Some(Ok((entry.key, entry.value)));
        }

        None // No more elements to iterate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_temp_store() -> LsmStore {
        let temp_dir = TempDir::new().expect("Failed to create temporary directory");
        println!("Temp dir: {:?}", temp_dir.path());
        LsmStore::open(temp_dir.path().to_str().unwrap()).expect("Failed to initialize LsmStore")
    }

    #[test]
    fn test_set_and_get() {
        let mut store = create_temp_store();

        // Insert key-value pairs
        store.set(b"key1", b"value1".to_vec()).expect("Set failed");
        store.set(b"key2", b"value2".to_vec()).expect("Set failed");
        store.set(b"key3", b"value3".to_vec()).expect("Set failed");

        // Retrieve and verify
        assert_eq!(
            store.get(b"key1").expect("Get failed"),
            Some(b"value1".to_vec())
        );
        assert_eq!(
            store.get(b"key2").expect("Get failed"),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            store.get(b"key3").expect("Get failed"),
            Some(b"value3".to_vec())
        );
        assert_eq!(store.get(b"key4").expect("Get failed"), None);
    }

    #[test]
    fn test_scan() {
        let mut store = create_temp_store();

        // Insert key-value pairs
        store.set(b"key1", b"value1".to_vec()).expect("Set failed");
        store.set(b"key2", b"value2".to_vec()).expect("Set failed");
        store.set(b"key3", b"value3".to_vec()).expect("Set failed");

        // Scan a specific range
        let mut scan_iter = store.scan(b"key1".to_vec()..=b"key2".to_vec());

        // Verify the scanned range
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
    fn test_scan_prefix() {
        let mut store = create_temp_store();

        // Insert key-value pairs
        store.set(b"key1", b"value1".to_vec()).expect("Set failed");
        store.set(b"key2", b"value2".to_vec()).expect("Set failed");
        store
            .set(b"key_other", b"value_other".to_vec())
            .expect("Set failed");

        // Insert keys that are lexicographically close but should not match
        store
            .set(b"kez1", b"value_kez1".to_vec())
            .expect("Failed to set kez1");
        store
            .set(b"kea", b"value_kea".to_vec())
            .expect("Failed to set kea");

        // Scan by prefix
        let results: Vec<_> = store
            .scan_prefix(b"key")
            .collect::<crate::error::Result<Vec<_>>>()
            .expect("Scan prefix failed");

        // Verify the results
        assert_eq!(results.len(), 3);
        assert_eq!(results[0], (b"key1".to_vec(), b"value1".to_vec()));
        assert_eq!(results[1], (b"key2".to_vec(), b"value2".to_vec()));

        // Assert that lexicographically close but non-matching keys are excluded
        assert!(!results.contains(&(b"kez1".to_vec(), b"value_kez1".to_vec())));
        assert!(!results.contains(&(b"kea".to_vec(), b"value_kea".to_vec())));
    }

    #[test]
    fn test_freeze_on_max_size() {
        let mut store = create_temp_store();

        // Insert enough data to reach the maximum size of the active Memtable
        let large_value = vec![0u8; MAX_MEMTABLE_SIZE / 2];
        store
            .set(b"key1", large_value.clone())
            .expect("Failed to set key1");
        store.set(b"key2", large_value).expect("Failed to set key2");

        // The active Memtable should have been frozen
        assert_eq!(store.frozen_memtables.len(), 1, "Memtable was not frozen");
        assert_eq!(
            store.active_memtable.size(),
            0,
            "New Memtable was not created after freeze"
        );
    }

    #[test]
    fn test_access_frozen_memtables() {
        let mut store = create_temp_store();

        // Insert data into the active Memtable
        store
            .set(b"key1", b"value1".to_vec())
            .expect("Failed to set key1");
        store
            .set(b"key2", b"value2".to_vec())
            .expect("Failed to set key2");

        // Freeze the active Memtable
        store
            .freeze_active_memtable()
            .expect("Failed to freeze Memtable");

        // Insert data into the new active Memtable
        store
            .set(b"key3", b"value3".to_vec())
            .expect("Failed to set key3");

        // Access keys in frozen Memtables
        assert_eq!(
            store.get(b"key1").expect("Failed to get key1"),
            Some(b"value1".to_vec()),
            "Failed to access key1 in frozen Memtable"
        );
        assert_eq!(
            store.get(b"key2").expect("Failed to get key2"),
            Some(b"value2".to_vec()),
            "Failed to access key2 in frozen Memtable"
        );

        // Access keys in the active Memtable
        assert_eq!(
            store.get(b"key3").expect("Failed to get key3"),
            Some(b"value3".to_vec()),
            "Failed to access key3 in active Memtable"
        );
    }

    #[test]
    fn test_scan_across_frozen_and_active_memtables() {
        let mut store = create_temp_store();

        // Insert keys into the active Memtable
        store
            .set(b"key1", b"value1".to_vec())
            .expect("Failed to set key1");
        store
            .set(b"key2", b"value2".to_vec())
            .expect("Failed to set key2");

        // Freeze the active Memtable
        store
            .freeze_active_memtable()
            .expect("Failed to freeze Memtable");

        // Insert keys into the new active Memtable
        store
            .set(b"key3", b"value3".to_vec())
            .expect("Failed to set key3");
        store
            .set(b"key4", b"value4".to_vec())
            .expect("Failed to set key4");

        // Scan across both frozen and active Memtables
        let scanned_keys: Vec<_> = store
            .scan(b"key1".to_vec()..=b"key4".to_vec())
            .collect::<Result<Vec<_>>>()
            .expect("Failed to collect scan results");

        assert_eq!(
            scanned_keys,
            vec![
                (b"key1".to_vec(), b"value1".to_vec()),
                (b"key2".to_vec(), b"value2".to_vec()),
                (b"key3".to_vec(), b"value3".to_vec()),
                (b"key4".to_vec(), b"value4".to_vec())
            ],
            "Scanned keys do not match expected results"
        );
    }

    #[test]
    fn test_scan_duplicate_keys_multiple() {
        let mut store = create_temp_store();

        // Insert the first key ("key1") into the active memtable
        store.set(b"key1", b"value1".to_vec()).expect("Set failed");

        // Insert the second key ("key2") into the active memtable
        store.set(b"key2", b"valueA".to_vec()).expect("Set failed");

        // Freeze the active memtable to move "key1" and "key2" into a frozen memtable
        store
            .freeze_active_memtable()
            .expect("Failed to freeze memtable");

        // Insert the same key ("key1") with a new value into the new active memtable
        store.set(b"key1", b"value2".to_vec()).expect("Set failed");

        // Insert the same key ("key2") with a new value into the new active memtable
        store.set(b"key2", b"valueB".to_vec()).expect("Set failed");

        // Perform a scan that includes "key1" and "key2"
        let results: Vec<_> = store
            .scan(b"key1".to_vec()..=b"key2".to_vec())
            .collect::<Result<Vec<_>>>()
            .expect("Scan failed");

        // Assert that the scan contains exactly two unique keys
        assert_eq!(results.len(), 2, "Unexpected number of keys in scan");

        // Verify the values for "key1" and "key2" are the most recent
        assert_eq!(
            results[0],
            (b"key1".to_vec(), b"value2".to_vec()),
            "Incorrect value for key1"
        );
        assert_eq!(
            results[1],
            (b"key2".to_vec(), b"valueB".to_vec()),
            "Incorrect value for key2"
        );
    }
}
