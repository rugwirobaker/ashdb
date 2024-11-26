use super::store::Store;
use crate::{
    error::Result,
    memtable::{Memtable, MAX_MEMTABLE_SIZE},
};
use std::{ops::RangeBounds, sync::Arc};

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
    type ScanIterator<'a> = ScanIterator<'a>;

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
        let iterators: Vec<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>> = self
            .frozen_memtables
            .iter()
            .rev()
            .chain(std::iter::once(&self.active_memtable))
            .map(|memtable| Box::new(memtable.scan(range.clone()).unwrap()) as _)
            .collect();

        ScanIterator {
            inner: Box::new(iterators.into_iter().flatten()),
        }
    }
}

// ScanIterator is a wrapper around the Memtable's ScanIter.
pub struct ScanIterator<'a> {
    inner: Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>,
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
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
}
