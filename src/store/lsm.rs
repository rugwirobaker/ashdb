use super::store::Store;
use crate::{
    error::Result,
    memtable::{Memtable, MAX_MEMTABLE_SIZE},
};
use std::{cmp::Ordering, collections::BinaryHeap, ops::RangeBounds, sync::Arc};

pub struct LsmStore {
    dir: String,
    active_memtable: Arc<Memtable>, // Current active Memtable with read/write access
    frozen_memtables: Vec<Arc<Memtable>>, // Frozen Memtables
}

impl LsmStore {
    pub fn new(dir: &str) -> Result<Self> {
        let active_memtable = Memtable::new(dir, 0)?;
        Ok(Self {
            dir: dir.to_string(),
            active_memtable: Arc::new(active_memtable),
            frozen_memtables: Vec::new(),
        })
    }
}

impl LsmStore {
    pub fn freeze_active_memtable(&mut self) -> Result<()> {
        // Freeze the current active Memtable
        self.active_memtable.freeze()?;
        let frozen_memtable = Arc::clone(&self.active_memtable);

        // Push the frozen Memtable to the frozen_memtables list
        self.frozen_memtables.push(frozen_memtable);

        // Create a new active Memtable
        let new_memtable_id = self.frozen_memtables.len() as u64;
        self.active_memtable = Arc::new(Memtable::new(&self.dir, new_memtable_id)?);

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
        Some(self.key.cmp(&other.key).reverse())
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
        LsmStore::new(temp_dir.path().to_str().unwrap()).expect("Failed to initialize LsmStore")
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
