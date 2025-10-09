use super::{
    super::Store,
    compaction, flush,
    iterator::LsmIterator,
    memtable::{self},
    recovery, LsmState,
};
use crate::{config::LsmConfig, error::Result, flock::FileLock};

use std::{fs, ops::RangeBounds, path::Path, sync::Arc};

const LOCK_FILE: &str = "ashdb.lock";

/// Construct path for a WAL file by ID.
/// Format: `<dir>/wal/00000000.wal`
pub(super) fn wal_path(dir: &Path, wal_id: u64) -> std::path::PathBuf {
    dir.join("wal").join(format!("{:08}.wal", wal_id))
}

/// Construct path for an SSTable file by ID.
/// Format: `<dir>/sst/00000000.sst`
pub(super) fn sstable_path(dir: &Path, table_id: u64) -> std::path::PathBuf {
    dir.join("sst").join(format!("{:08}.sst", table_id))
}

/// LSM Store with interior mutability
pub struct LsmTree {
    // Immutable configuration (public for background tasks)
    pub(super) config: LsmConfig,
    lock: Option<FileLock>,

    // Shared mutable state (public for background tasks)
    pub(super) state: Arc<LsmState>,
}

impl LsmTree {
    /// Open store with default configuration
    pub fn open(dir: &str) -> Result<Self> {
        let config = LsmConfig::new(dir);
        Self::open_with_config(config)
    }

    /// Open store with custom configuration
    pub fn open_with_config(config: LsmConfig) -> Result<Self> {
        fs::create_dir_all(&config.dir)?;

        // Create subdirectories for organized storage
        fs::create_dir_all(config.dir.join("sst"))?;
        fs::create_dir_all(config.dir.join("wal"))?;

        // Acquire file lock
        let lock = FileLock::lock(config.dir.join(LOCK_FILE))?;

        // Initialize/recover state
        let state = Arc::new(recovery::recover_state(&config)?);

        // Create store instance (no scheduler, no background tasks)
        Ok(Self {
            config,
            lock: Some(lock),
            state,
        })
    }
}

impl Drop for LsmTree {
    fn drop(&mut self) {
        // Release file lock
        if let Some(lock) = self.lock.take() {
            let _ = lock.unlock();
        }
    }
}

impl Store for LsmTree {
    type ScanIterator<'a> = LsmIterator<'a>;

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        loop {
            let active = self.state.active_memtable.load();

            match active.put(key.to_vec(), Some(value.clone())) {
                Ok(_) => {
                    // Check if freeze needed
                    if active.size() >= self.config.max_memtable_size {
                        self.freeze_active_memtable()?;
                    }
                    return Ok(());
                }
                Err(crate::Error::ReadOnly) => {
                    // Retry with new active memtable
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check active memtable
        if let Some(value) = self.state.active_memtable.load().get(key) {
            return Ok(value);
        }

        // Check frozen memtables (newest to oldest)
        let frozen = self.state.frozen_memtables.read().unwrap();
        for memtable in frozen.iter().rev() {
            if let Some(value) = memtable.get(key) {
                return Ok(value);
            }
        }

        // Check SSTables in all levels
        let levels = self.state.levels.read().unwrap();
        for level in levels.iter() {
            if let Some(value) = level.get(key)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn scan<'a>(
        &'a self,
        range: impl RangeBounds<Vec<u8>> + Clone + Send + Sync + 'a,
    ) -> Self::ScanIterator<'a> {
        type BoxedInternalIterator<'a> = Box<
            dyn DoubleEndedIterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>> + Send + Sync + 'a,
        >;

        // Load Arc - no lock needed with ArcSwap
        let active = {
            let active = self.state.active_memtable.load();
            Arc::clone(active.memtable())
        };

        let frozens: Vec<Arc<memtable::Memtable>> = {
            let frozen = self.state.frozen_memtables.read().unwrap();
            frozen
                .iter()
                .rev()
                .map(|m| Arc::clone(m.memtable()))
                .collect()
        };

        // Create iterators for memtables using new scan pattern
        let active_iter = match active.scan(range.clone()) {
            Ok(iter) => iter,
            Err(_) => return LsmIterator::new(vec![]), // Return empty iterator on error
        };

        let frozen_iters: Vec<_> = frozens
            .into_iter()
            .filter_map(|arc| arc.scan(range.clone()).ok())
            .collect();

        let mut iterators: Vec<BoxedInternalIterator> = vec![Box::new(active_iter)];
        for iter in frozen_iters {
            iterators.push(Box::new(iter));
        }

        // Add SSTable iterators from all levels
        {
            let levels = self.state.levels.read().unwrap();
            for level in levels.iter() {
                for sstable in &level.sstables {
                    if let Ok(scan_iter) = sstable.table.scan(range.clone()) {
                        let boxed_iter: BoxedInternalIterator = Box::new(scan_iter);
                        iterators.push(boxed_iter);
                    }
                }
            }
        }

        LsmIterator::new(iterators)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        loop {
            let active = self.state.active_memtable.load();

            match active.delete(key.to_vec()) {
                Ok(_) => {
                    // Check if freeze needed
                    if active.size() >= self.config.max_memtable_size {
                        self.freeze_active_memtable()?;
                    }
                    return Ok(());
                }
                Err(crate::Error::ReadOnly) => {
                    // Retry with new active memtable
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn sync(&self) -> Result<()> {
        let active = self.state.active_memtable.load();
        active.sync()
    }
}

impl LsmTree {
    /// Freeze active memtable (atomic operation)
    pub fn freeze_active_memtable(&self) -> Result<()> {
        // Try to acquire freeze lock
        let _guard = match self.state.try_start_freeze() {
            Some(guard) => guard,
            None => return Ok(()), // Another freeze in progress
        };

        // Create new active memtable
        let (new_wal_id, wal_path) = self.next_wal_path();
        let new_active = Arc::new(memtable::ActiveMemtable::new(
            wal_path.to_str().unwrap(),
            new_wal_id,
        )?);

        // Freeze current active and atomically swap with new one
        let frozen = {
            let old = self.state.active_memtable.load();
            let frozen_memtable = old.freeze()?;
            self.state.active_memtable.swap(new_active);
            frozen_memtable
        };

        // Add to frozen queue
        self.state
            .frozen_memtables
            .write()
            .unwrap()
            .push_back(frozen);

        Ok(())
    }

    /// Check if compaction is needed
    pub fn needs_compaction(&self) -> bool {
        compaction::needs_compaction(&self.state, &self.config.compaction)
    }

    /// Manually flush oldest frozen memtable to SSTable (for testing)
    pub async fn flush_memtable(&self) -> Result<bool> {
        flush::flush_memtable(self).await
    }

    /// Perform tiered compaction if needed
    pub async fn compact(&self) -> Result<()> {
        // Check if it's safe to perform compaction
        if !self.state.is_operation_safe() {
            tracing::info!("Skipping compaction - other operations in progress");
            return Ok(());
        }

        compaction::compact(self).await
    }

    /// Check if flush is needed
    pub fn needs_flush(&self) -> bool {
        self.state.needs_flush()
    }

    /// Generate next WAL ID and construct its path
    pub(super) fn next_wal_path(&self) -> (u64, std::path::PathBuf) {
        let id = self.state.next_wal_id();
        let path = wal_path(&self.config.dir, id);
        (id, path)
    }

    /// Generate next SSTable ID and construct its path
    pub(super) fn next_sstable_path(&self) -> (u64, std::path::PathBuf) {
        let id = self.state.next_sstable_id();
        let path = sstable_path(&self.config.dir, id);
        (id, path)
    }

    /// Construct path for existing WAL by ID
    pub(super) fn wal_path(&self, wal_id: u64) -> std::path::PathBuf {
        wal_path(&self.config.dir, wal_id)
    }

    /// Construct path for existing SSTable by ID
    pub(super) fn sstable_path(&self, table_id: u64) -> std::path::PathBuf {
        sstable_path(&self.config.dir, table_id)
    }

    /// Get current health metrics for monitoring and debugging
    pub fn get_health_metrics(&self) -> super::state::StateMetrics {
        self.state.get_health_metrics()
    }

    /// Comprehensive validation for debug builds and testing
    #[cfg(debug_assertions)]
    pub fn validate_comprehensive(&self) -> Result<()> {
        self.state.validate_comprehensive()
    }

    /// Run periodic status check with validation (renamed from health_check)
    pub fn status(&self) -> Result<super::state::StateMetrics> {
        self.state.status()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{CompactionConfig, LsmConfig},
        store::Store,
        tmpfs::TempDir,
    };

    // Helper function to create a test store with custom config
    fn create_test_store_with_config(temp_dir: &TempDir, config: CompactionConfig) -> LsmTree {
        let lsm_config = LsmConfig::new(temp_dir.path()).compaction(config);
        LsmTree::open_with_config(lsm_config).expect("Failed to create store")
    }

    // Helper function to create a test store with default aggressive compaction settings
    fn create_test_store(temp_dir: &TempDir) -> LsmTree {
        let compaction_config = CompactionConfig::default()
            .level0_compaction_threshold(2) // Lower threshold for easier testing
            .size_ratio_threshold(2) // Lower ratio for easier testing
            .max_tables_per_level(3); // Lower max for easier testing

        create_test_store_with_config(temp_dir, compaction_config)
    }

    #[tokio::test]
    async fn test_store_implementation() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = LsmTree::open(temp_dir.path().to_str().unwrap())?;

        // Test basic set/get operations
        store.set(b"key1", b"value1".to_vec())?;
        store.set(b"key2", b"value2".to_vec())?;
        store.set(b"key3", b"value3".to_vec())?;

        // Test get operations
        assert_eq!(store.get(b"key1")?, Some(b"value1".to_vec()));
        assert_eq!(store.get(b"key2")?, Some(b"value2".to_vec()));
        assert_eq!(store.get(b"key3")?, Some(b"value3".to_vec()));
        assert_eq!(store.get(b"nonexistent")?, None);

        // Test key updates
        store.set(b"key1", b"updated_value1".to_vec())?;
        assert_eq!(store.get(b"key1")?, Some(b"updated_value1".to_vec()));

        // Test scan operations
        let scan_results: Result<Vec<_>> = store.scan(..).collect();
        let results = scan_results?;
        assert_eq!(results.len(), 3);

        // Verify ordering
        assert_eq!(results[0].0, b"key1");
        assert_eq!(results[0].1, b"updated_value1");
        assert_eq!(results[1].0, b"key2");
        assert_eq!(results[1].1, b"value2");
        assert_eq!(results[2].0, b"key3");
        assert_eq!(results[2].1, b"value3");

        // Test delete operations
        store.delete(b"key2")?;

        // Verify deleted key returns None
        assert_eq!(store.get(b"key2")?, None);

        // Verify other keys still exist
        assert_eq!(store.get(b"key1")?, Some(b"updated_value1".to_vec()));
        assert_eq!(store.get(b"key3")?, Some(b"value3".to_vec()));

        // Test scan after delete (should not include deleted key)
        let scan_after_delete: Result<Vec<_>> = store.scan(..).collect();
        let results_after_delete = scan_after_delete?;
        assert_eq!(results_after_delete.len(), 2);
        assert_eq!(results_after_delete[0].0, b"key1");
        assert_eq!(results_after_delete[1].0, b"key3");

        // Test delete non-existent key (should not error)
        store.delete(b"nonexistent")?;

        // Test delete then re-insert same key
        store.delete(b"key1")?;
        assert_eq!(store.get(b"key1")?, None);
        store.set(b"key1", b"new_value1".to_vec())?;
        assert_eq!(store.get(b"key1")?, Some(b"new_value1".to_vec()));

        // Test sync
        store.sync()?;

        Ok(())
    }

    #[tokio::test]
    async fn test_lsm_api_contracts() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // === ACT 1: THE BEGINNING (Fresh Store) ===
        assert!(!store.needs_flush(), "Fresh store should not need flush");
        assert!(
            !store.needs_compaction(),
            "Fresh store should not need compaction"
        );

        // Test empty operations succeed gracefully
        let flushed = store.flush_memtable().await?;
        assert!(!flushed, "Should return false when nothing to flush");
        store.compact().await?; // Should not error

        // === ACT 2: DATA CREATION & BASIC OPERATIONS ===
        // The story begins: Alice and Bob join the system
        store.set(b"user:alice", b"active".to_vec())?;
        store.set(b"user:bob", b"inactive".to_vec())?;

        // Verify initial state
        assert_eq!(
            store.get(b"user:alice")?,
            Some(b"active".to_vec()),
            "Alice should be active initially"
        );
        assert_eq!(
            store.get(b"user:bob")?,
            Some(b"inactive".to_vec()),
            "Bob should be inactive initially"
        );

        // Plot twist: Bob becomes active, Alice leaves the system
        store.set(b"user:bob", b"active".to_vec())?; // Update
        store.delete(b"user:alice")?; // Delete (creates tombstone)

        // Verify immediate changes
        assert_eq!(
            store.get(b"user:alice")?,
            None,
            "Alice should be deleted immediately"
        );
        assert_eq!(
            store.get(b"user:bob")?,
            Some(b"active".to_vec()),
            "Bob should be active after update"
        );

        // Should still not need flush (data in active memtable)
        assert!(
            !store.needs_flush(),
            "Should not need flush with just active memtable"
        );

        // === ACT 3: THE FLUSH JOURNEY ===
        // Force memtable freeze to create flush need
        store.freeze_active_memtable()?;
        assert!(store.needs_flush(), "Should need flush after freezing");

        // The great flush: memtable data moves to SSTables
        let first_flush = store.flush_memtable().await?;
        assert!(first_flush, "First flush should succeed");

        // Verify data survives the flush journey
        assert_eq!(
            store.get(b"user:alice")?,
            None,
            "Alice should still be deleted after flush (tombstone in SSTable)"
        );
        assert_eq!(
            store.get(b"user:bob")?,
            Some(b"active".to_vec()),
            "Bob should still be active after flush"
        );

        // API contracts after flush
        assert!(!store.needs_flush(), "Should not need flush after flushing");
        let second_flush = store.flush_memtable().await?;
        assert!(
            !second_flush,
            "Second flush should return false (nothing to flush)"
        );

        // === ACT 4: THE COMPACTION SAGA ===
        // Add more data to trigger compaction need
        for i in 0..50 {
            let key = format!("batch_key_{:03}", i);
            let value = format!("batch_value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Create another frozen memtable to build up L0 tables
        store.freeze_active_memtable()?;
        store.flush_memtable().await?;

        // Add one more batch and flush to exceed L0 compaction threshold
        for i in 50..100 {
            let key = format!("batch_key_{:03}", i);
            let value = format!("batch_value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }
        store.freeze_active_memtable()?;
        store.flush_memtable().await?;

        // Now should need compaction
        assert!(
            store.needs_compaction(),
            "Should need compaction after creating multiple L0 tables"
        );

        // The great compaction: SSTables merge across levels
        store.compact().await?;

        // Verify our heroes survive the compaction
        assert_eq!(
            store.get(b"user:alice")?,
            None,
            "Alice should still be deleted after compaction (tombstone survives)"
        );
        assert_eq!(
            store.get(b"user:bob")?,
            Some(b"active".to_vec()),
            "Bob should still be active after compaction"
        );

        // Verify batch data survives compaction
        for i in 0..100 {
            let key = format!("batch_key_{:03}", i);
            let expected_value = format!("batch_value_{}", i);
            assert_eq!(
                store.get(key.as_bytes())?,
                Some(expected_value.as_bytes().to_vec()),
                "Batch key {} should survive compaction",
                i
            );
        }

        // === ACT 5: RECOVERY & PERSISTENCE ===
        // The ultimate test: store restart (simulates crash recovery)
        let store_path = temp_dir.path().to_str().unwrap();
        drop(store); // Close the store

        // Reopen the store (recovery scenario)
        let recovered_store = LsmTree::open(store_path)?;

        // Verify our complete story persists after recovery
        assert_eq!(
            recovered_store.get(b"user:alice")?,
            None,
            "Alice should still be deleted after recovery (durability of tombstones)"
        );
        assert_eq!(
            recovered_store.get(b"user:bob")?,
            Some(b"active".to_vec()),
            "Bob should still be active after recovery (durability of updates)"
        );

        // Verify batch data survives recovery
        for i in 0..100 {
            let key = format!("batch_key_{:03}", i);
            let expected_value = format!("batch_value_{}", i);
            assert_eq!(
                recovered_store.get(key.as_bytes())?,
                Some(expected_value.as_bytes().to_vec()),
                "Batch key {} should survive recovery",
                i
            );
        }

        // === ACT 6: COMPLEX MIXED OPERATIONS ===
        // Final test: complex mixed operations survive multiple cycles

        // Add more characters to our story
        recovered_store.set(b"user:charlie", b"pending".to_vec())?;
        recovered_store.set(b"user:diana", b"active".to_vec())?;

        // Update existing characters
        recovered_store.set(b"user:bob", b"admin".to_vec())?; // Bob gets promoted

        // Remove one character
        recovered_store.delete(b"user:diana")?; // Diana leaves

        // Force another complete cycle
        recovered_store.freeze_active_memtable()?;
        recovered_store.flush_memtable().await?;

        if recovered_store.needs_compaction() {
            recovered_store.compact().await?;
        }

        // Verify final state of our story
        assert_eq!(
            recovered_store.get(b"user:alice")?,
            None,
            "Alice should remain deleted through all operations"
        );
        assert_eq!(
            recovered_store.get(b"user:bob")?,
            Some(b"admin".to_vec()),
            "Bob should have final promoted status"
        );
        assert_eq!(
            recovered_store.get(b"user:charlie")?,
            Some(b"pending".to_vec()),
            "Charlie should exist with pending status"
        );
        assert_eq!(
            recovered_store.get(b"user:diana")?,
            None,
            "Diana should be deleted in final state"
        );

        // Verify scan operations work correctly after all operations
        let scan_results: Result<Vec<_>> = recovered_store
            .scan(b"user:".to_vec()..b"user;".to_vec())
            .collect();
        let results = scan_results?;

        // Should have Bob and Charlie, but not Alice or Diana
        assert_eq!(
            results.len(),
            2,
            "Should have exactly 2 users in final state"
        );
        assert!(
            results.iter().any(|(k, _)| k == b"user:bob"),
            "Bob should be present in scan results"
        );
        assert!(
            results.iter().any(|(k, _)| k == b"user:charlie"),
            "Charlie should be present in scan results"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_exclusive_directory_access() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path().to_str().unwrap();

        // First instance should acquire lock successfully
        let store1 = LsmTree::open(store_path)?;

        // Second instance should fail to acquire lock
        let result = LsmTree::open(store_path);
        assert!(
            result.is_err(),
            "Second instance should fail to open the same directory"
        );

        // Add some data to first instance
        store1.set(b"test_key", b"test_value".to_vec())?;

        // Verify the lock error is what we expect
        match result {
            Err(crate::Error::IO(_)) => {
                // Expected - file lock should cause a lock or IO error
            }
            Err(other) => {
                panic!("Expected lock/IO error for file lock, got: {:?}", other);
            }
            Ok(_) => {
                panic!("Should not be able to open same directory twice");
            }
        }

        drop(store1); // Release the lock

        // After first instance is dropped, second should be able to open
        let store2 = LsmTree::open(store_path)?;

        // Verify data from first instance is recovered
        let value = store2.get(b"test_key")?;
        assert_eq!(value, Some(b"test_value".to_vec()));

        Ok(())
    }

    #[test]
    fn test_file_naming_format() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Test WAL path format
        let wal_path_0 = wal_path(temp_dir.path(), 0);
        let wal_path_1 = wal_path(temp_dir.path(), 1);
        let wal_path_999 = wal_path(temp_dir.path(), 999);

        assert_eq!(
            wal_path_0.file_name().unwrap().to_str().unwrap(),
            "00000000.wal"
        );
        assert_eq!(
            wal_path_1.file_name().unwrap().to_str().unwrap(),
            "00000001.wal"
        );
        assert_eq!(
            wal_path_999.file_name().unwrap().to_str().unwrap(),
            "00000999.wal"
        );

        // Test SSTable path format
        let sst_path_0 = sstable_path(temp_dir.path(), 0);
        let sst_path_1 = sstable_path(temp_dir.path(), 1);
        let sst_path_999 = sstable_path(temp_dir.path(), 999);

        assert_eq!(
            sst_path_0.file_name().unwrap().to_str().unwrap(),
            "00000000.sst"
        );
        assert_eq!(
            sst_path_1.file_name().unwrap().to_str().unwrap(),
            "00000001.sst"
        );
        assert_eq!(
            sst_path_999.file_name().unwrap().to_str().unwrap(),
            "00000999.sst"
        );
    }

    #[tokio::test]
    async fn test_empty_store_recovery() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path().to_str().unwrap();

        // Create and immediately close empty store
        {
            let _store = LsmTree::open(store_path)?;
            // Store is empty, just close it
        }

        // Reopen and verify it's still empty but functional
        {
            let store = LsmTree::open(store_path)?;

            // Should be empty
            let scan_results: Result<Vec<_>> = store.scan(..).collect();
            let results = scan_results?;
            assert!(results.is_empty(), "Recovered empty store should be empty");

            // Should still be functional
            store.set(b"test_key", b"test_value".to_vec())?;
            let value = store.get(b"test_key")?;
            assert_eq!(value, Some(b"test_value".to_vec()));
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_status_api() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Test status check
        let initial_metrics = store.status()?;
        assert_eq!(initial_metrics.level_count, 0);
        assert_eq!(initial_metrics.total_sstable_count, 0);
        assert_eq!(initial_metrics.frozen_memtable_count, 0);

        // Add some data and test status again
        for i in 0..20 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        let after_writes = store.status()?;
        assert!(after_writes.active_memtable_size > 0);

        // Test status after operations
        store.freeze_active_memtable()?;
        let after_freeze = store.status()?;
        assert_eq!(after_freeze.frozen_memtable_count, 1);

        while store.flush_memtable().await? {}
        let after_flush = store.status()?;
        assert_eq!(after_flush.frozen_memtable_count, 0);
        assert!(after_flush.level_count >= 1);

        Ok(())
    }
}
