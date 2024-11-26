use super::store::Store;
use crate::{error::Result, memtable::Memtable};
use std::ops::RangeBounds;

pub struct LsmStore {
    memtable: Memtable, // Single active Memtable
}

impl LsmStore {
    pub fn new(dir: &str) -> Result<Self> {
        // Initialize the Memtable with ID 0
        let memtable = Memtable::new(dir, 0)?;
        Ok(Self { memtable })
    }
}

impl Store for LsmStore {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.memtable.put(key.to_vec(), Some(value))
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.memtable.get(key).flatten())
    }

    fn scan<'a>(&'a self, range: impl RangeBounds<Vec<u8>> + 'a) -> Self::ScanIterator<'a> {
        let memtable_iter = self.memtable.scan(range).expect("Scan failed");
        ScanIterator {
            inner: Box::new(memtable_iter),
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
}
