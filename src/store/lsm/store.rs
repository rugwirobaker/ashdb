use super::{
    super::Store,
    compaction, flush,
    iterator::{MergeIterator, OwningMemtableIter},
    memtable::ActiveMemtable,
    metrics, recovery, wal_cleanup, LsmState,
};

use crate::{config::LsmConfig, error::Result, flock::FileLock};

use std::{fs, ops::RangeBounds, sync::Arc};

const LOCK_FILE: &str = "ashdb.lock";

/// LSM Store with interior mutability
pub struct LsmStore {
    // Immutable configuration (public for background tasks)
    pub(crate) config: LsmConfig,
    lock: Option<FileLock>,

    // Shared mutable state (public for background tasks)
    pub(crate) state: Arc<LsmState>,
}

impl LsmStore {
    /// Open store with default configuration
    pub fn open(dir: &str) -> Result<Self> {
        let config = LsmConfig::new(dir);
        Self::open_with_config(config)
    }

    /// Open store with custom configuration
    pub fn open_with_config(config: LsmConfig) -> Result<Self> {
        fs::create_dir_all(&config.dir)?;

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

    /// Freeze active memtable (atomic operation)
    pub fn freeze_active_memtable(&self) -> Result<()> {
        // Try to acquire freeze lock
        let _guard = match self.state.try_start_freeze() {
            Some(guard) => guard,
            None => return Ok(()), // Another freeze in progress
        };

        // Create new active memtable
        let new_wal_id = self.state.next_wal_id();
        let wal_dir = self.config.dir.join("wal");
        fs::create_dir_all(&wal_dir)?;
        let wal_path = wal_dir.join(format!("{}.wal", new_wal_id));
        let new_active = Arc::new(ActiveMemtable::new(wal_path.to_str().unwrap(), new_wal_id)?);

        // Atomic swap
        let old_active = {
            let mut active = self.state.active_memtable.write().unwrap();
            let old = active.freeze()?;
            let _ = std::mem::replace(&mut *active, new_active);
            old
        };

        // Add to frozen queue
        self.state
            .frozen_memtables
            .write()
            .unwrap()
            .push_back(old_active);

        Ok(())
    }

    /// Check if compaction is needed
    pub fn needs_compaction(&self) -> bool {
        compaction::needs_compaction(&self.state, &self.config.compaction)
    }

    /// Manually flush oldest frozen memtable to SSTable (for testing)
    pub async fn flush_memtable(&self) -> Result<bool> {
        flush::flush_memtable(&self.state, &self.config).await
    }

    /// Get list of deletable WAL file IDs from manifest
    pub fn deletable_wals(&self) -> Result<Vec<u64>> {
        wal_cleanup::deletable_wals(&self.state)
    }

    /// Clean up WAL files that are no longer needed
    pub async fn cleanup_wals(&self) -> Result<()> {
        wal_cleanup::cleanup_wals(&self.state, &self.config).await
    }

    /// Perform tiered compaction if needed
    pub async fn compact(&self) -> Result<()> {
        compaction::compact(&self.state, &self.config).await
    }

    /// Collect and log metrics
    pub fn collect_metrics(&self) -> Result<()> {
        metrics::collect_metrics(&self.state)
    }

    /// Check if flush is needed
    pub fn needs_flush(&self) -> bool {
        self.state.needs_flush()
    }

    /// Try to mark flush as pending (returns true if successfully marked)
    pub fn try_mark_flush_pending(&self) -> bool {
        self.state.try_mark_flush_pending()
    }

    /// Mark flush as completed
    pub fn mark_flush_completed(&self) {
        self.state.mark_flush_completed()
    }
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        // Release file lock
        if let Some(lock) = self.lock.take() {
            let _ = lock.unlock();
        }
    }
}

impl Store for LsmStore {
    type ScanIterator<'a> = MergeIterator<'a>;

    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        loop {
            let active = self.state.active_memtable.read().unwrap();

            match active.put(key.to_vec(), Some(value.clone())) {
                Ok(_) => {
                    // Check if freeze needed
                    if active.size() >= self.config.max_memtable_size {
                        drop(active); // Release read lock
                        self.freeze_active_memtable()?;
                    }
                    return Ok(());
                }
                Err(crate::Error::Frozen) => {
                    // Retry with new active memtable
                    drop(active);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check active memtable
        if let Some(value) = self.state.active_memtable.read().unwrap().get(key) {
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

    fn scan<'a>(&'a self, range: impl RangeBounds<Vec<u8>> + 'a + Clone) -> Self::ScanIterator<'a> {
        type BoxedResultIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>;

        // Clone Arc to extend lifetime beyond lock
        let active_arc = {
            let active = self.state.active_memtable.read().unwrap();
            Arc::clone(active.memtable())
        };

        let frozen_arcs: Vec<Arc<super::memtable::Memtable>> = {
            let frozen = self.state.frozen_memtables.read().unwrap();
            frozen
                .iter()
                .rev()
                .map(|m| Arc::clone(m.memtable()))
                .collect()
        };

        // Create owning iterators for memtables
        let active_iter = OwningMemtableIter::new(active_arc, range.clone());
        let frozen_iters: Vec<OwningMemtableIter> = frozen_arcs
            .into_iter()
            .map(|arc| OwningMemtableIter::new(arc, range.clone()))
            .collect();

        let mut iterators: Vec<BoxedResultIterator> = vec![Box::new(active_iter)];
        for iter in frozen_iters {
            iterators.push(Box::new(iter));
        }

        // Add SSTable iterators from all levels
        {
            let levels = self.state.levels.read().unwrap();
            for level in levels.iter() {
                for sstable in &level.sstables {
                    if let Ok(scan_iter) = sstable.table.scan(range.clone()) {
                        let boxed_iter: BoxedResultIterator = Box::new(scan_iter);
                        iterators.push(boxed_iter);
                    }
                }
            }
        }

        MergeIterator::new(iterators)
    }

    fn flush(&self) -> Result<()> {
        let active = self.state.active_memtable.read().unwrap();
        active.sync()
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
    use std::time::Duration;

    // Helper function to create a test store with custom config
    fn create_test_store_with_config(temp_dir: &TempDir, config: CompactionConfig) -> LsmStore {
        let lsm_config = LsmConfig::new(temp_dir.path()).compaction(config);
        LsmStore::open_with_config(lsm_config).expect("Failed to create store")
    }

    // Helper function to create a test store with default aggressive compaction settings
    fn create_test_store(temp_dir: &TempDir) -> LsmStore {
        let compaction_config = CompactionConfig::default()
            .level0_compaction_threshold(2) // Lower threshold for easier testing
            .size_ratio_threshold(2) // Lower ratio for easier testing
            .max_tables_per_level(3); // Lower max for easier testing

        create_test_store_with_config(temp_dir, compaction_config)
    }

    // Helper to populate data that will create multiple SSTables
    async fn populate_multiple_tables(store: &LsmStore, table_count: usize) -> Result<()> {
        let entries_per_table = 100;

        for table_idx in 0..table_count {
            for i in 0..entries_per_table {
                let key = format!("key_{:03}_{:03}", table_idx, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Force freeze of active memtable to create a frozen memtable
            store.freeze_active_memtable()?;

            // Manually flush the frozen memtable to create SSTable
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(())
    }

    // Helper to verify data integrity after compaction
    fn verify_data_integrity(store: &LsmStore, expected_entries: usize) -> Result<()> {
        let mut count = 0;
        let mut last_key = None;

        for entry in store.scan(..) {
            let (key, _value) = entry?;

            // Verify key ordering
            if let Some(ref last) = last_key {
                assert!(
                    key > *last,
                    "Keys not in order: {:?} should be > {:?}",
                    key,
                    last
                );
            }
            last_key = Some(key);
            count += 1;
        }

        assert_eq!(
            count, expected_entries,
            "Expected {} entries, found {}",
            expected_entries, count
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_basic_l0_to_l1_compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create multiple L0 tables (exceeding threshold of 2)
        populate_multiple_tables(&store, 3).await?;

        // Verify we have tables at L0
        {
            let levels = store.state.levels.read().unwrap();
            assert!(!levels.is_empty(), "Expected L0 to have tables");
            assert!(
                levels[0].table_count() >= 3,
                "Expected at least 3 L0 tables"
            );
        }

        // Trigger compaction directly on the store
        store.compact().await?;

        // Verify compaction result
        {
            let levels = store.state.levels.read().unwrap();
            assert!(
                levels.len() >= 2,
                "Expected at least 2 levels after compaction"
            );

            // L0 should be empty or have fewer tables
            assert!(
                levels[0].table_count() <= store.config.compaction.level0_compaction_threshold,
                "L0 should have <= threshold tables after compaction"
            );

            // L1 should have tables
            assert!(
                levels[1].table_count() > 0,
                "L1 should have tables after compaction"
            );
        }

        // Verify data integrity
        verify_data_integrity(&store, 300)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_level_compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create data that will populate multiple levels
        populate_multiple_tables(&store, 8).await?;

        // Run multiple compaction rounds to build up levels
        for _ in 0..5 {
            store.compact().await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Verify we have multiple levels
        {
            let levels = store.state.levels.read().unwrap();
            assert!(levels.len() >= 2, "Expected multiple levels");

            // Should have some distribution across levels
            let total_tables: usize = levels.iter().map(|l| l.table_count()).sum();
            assert!(total_tables > 0, "Should have tables in levels");
        }

        // Verify data integrity
        verify_data_integrity(&store, 800)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_overlapping_keys_compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create overlapping keys across multiple tables (newer should win)
        for table_idx in 0..3 {
            for i in 0..50 {
                let key = format!("key_{:03}", i); // Same keys across tables
                let value = format!("value_{}_{}", table_idx, i); // Different values
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Force flush
            store.state.try_mark_flush_pending();
            tokio::time::sleep(Duration::from_millis(100)).await;
            while store.state.needs_flush() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Compact
        store.compact().await?;

        // Verify only the latest values are present
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let expected_value = format!("value_2_{}", i); // Latest write (table_idx=2)

            if let Some(actual_value) = store.get(key.as_bytes())? {
                assert_eq!(
                    actual_value,
                    expected_value.as_bytes(),
                    "Key {} should have latest value",
                    key
                );
            } else {
                panic!("Key {} should be present", key);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_with_boundary_config() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Test with very aggressive settings
        let aggressive_config = CompactionConfig::default()
            .level0_compaction_threshold(1)
            .size_ratio_threshold(1)
            .max_tables_per_level(1);

        let store = create_test_store_with_config(&temp_dir, aggressive_config);

        // Add some data
        populate_multiple_tables(&store, 2).await?;

        // Should need compaction immediately
        assert!(
            store.needs_compaction(),
            "Should need compaction with aggressive config"
        );

        // Run compaction
        store.compact().await?;

        // Verify data integrity
        verify_data_integrity(&store, 200)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_compaction_prevention() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Start a compaction guard manually to simulate running compaction
        let _guard = store.state.start_compaction();

        // Should not need compaction while one is running
        assert!(
            !store.needs_compaction(),
            "Should not need compaction when one is running"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_tables_compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create tables with minimal data
        for i in 0..3 {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;

            // Force flush to create individual tables
            store.state.try_mark_flush_pending();
            tokio::time::sleep(Duration::from_millis(100)).await;
            while store.state.needs_flush() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Compact
        store.compact().await?;

        // Verify all data is still accessible
        for i in 0..3 {
            let key = format!("key_{}", i);
            assert!(
                store.get(key.as_bytes())?.is_some(),
                "Key {} should be present",
                key
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_no_op_when_not_needed() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Add minimal data (below compaction thresholds)
        store.set(b"key1", b"value1".to_vec())?;

        // Force flush but don't exceed L0 threshold
        store.state.try_mark_flush_pending();
        tokio::time::sleep(Duration::from_millis(100)).await;
        while store.state.needs_flush() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Record initial state
        let initial_table_count = {
            let levels = store.state.levels.read().unwrap();
            levels.first().map(|l| l.table_count()).unwrap_or(0)
        };

        // Attempt compaction - should be no-op
        store.compact().await?;

        // Verify no changes occurred
        {
            let levels = store.state.levels.read().unwrap();
            let current_table_count = levels.first().map(|l| l.table_count()).unwrap_or(0);

            assert_eq!(
                initial_table_count, current_table_count,
                "Table count should not change when compaction is not needed"
            );

            // Should still only have L0
            assert!(
                levels.len() <= 1,
                "Should not create additional levels when compaction not needed"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_end_to_end_compaction_workflow() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Simulate real workflow: write -> flush -> compact -> verify reads
        let entries = vec![
            ("apple", "fruit_1"),
            ("banana", "fruit_2"),
            ("cherry", "fruit_3"),
            ("apple", "fruit_1_updated"), // Update existing key
        ];

        // Write data
        for (key, value) in &entries {
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Create multiple tables by writing and flushing
        populate_multiple_tables(&store, 4).await?;

        // Verify we can read before compaction
        assert!(
            store.get(b"apple")?.is_some(),
            "Should find apple before compaction"
        );

        // Run compaction
        store.compact().await?;

        // Verify we can still read after compaction
        assert!(
            store.get(b"apple")?.is_some(),
            "Should find apple after compaction"
        );
        assert!(
            store.get(b"banana")?.is_some(),
            "Should find banana after compaction"
        );

        // Verify scan still works
        let scan_results: Result<Vec<_>> = store.scan(..).collect();
        let results = scan_results?;
        assert!(
            !results.is_empty(),
            "Scan should return results after compaction"
        );

        // Verify keys are in order
        for i in 1..results.len() {
            assert!(
                results[i - 1].0 < results[i].0,
                "Keys should be in order after compaction"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_manifest_persistence_after_compaction() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");

        // Create and populate store, then compact
        {
            let store = create_test_store(&temp_dir);
            populate_multiple_tables(&store, 3).await?;
            store.compact().await?;
            // Store will be dropped here, forcing manifest to persist
        }

        // Reopen store and verify compaction results are recovered
        let store2 = create_test_store(&temp_dir);

        // Verify the compacted structure was recovered
        {
            let levels = store2.state.levels.read().unwrap();
            assert!(
                levels.len() >= 2,
                "Should have recovered multi-level structure"
            );

            // L1 should have tables from compaction
            if levels.len() >= 2 {
                assert!(
                    levels[1].table_count() > 0,
                    "L1 should have compacted tables after recovery"
                );
            }
        }

        // Verify data is still accessible
        verify_data_integrity(&store2, 300)?;

        Ok(())
    }

    #[tokio::test]
    async fn test_compaction_with_large_keys_and_values() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create data with large keys and values
        for table_idx in 0..3 {
            for i in 0..20 {
                let key = format!(
                    "very_long_key_name_that_exceeds_normal_expectations_{}_{}",
                    table_idx, i
                );
                let value = "x".repeat(1000); // 1KB value
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Force flush
            store.state.try_mark_flush_pending();
            tokio::time::sleep(Duration::from_millis(100)).await;
            while store.state.needs_flush() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        // Compact
        store.compact().await?;

        // Verify large data is handled correctly
        let key = "very_long_key_name_that_exceeds_normal_expectations_0_0";
        if let Some(value) = store.get(key.as_bytes())? {
            assert_eq!(value.len(), 1000, "Large value should be preserved");
        } else {
            panic!("Large key should be accessible after compaction");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_memtable_basic() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Add some data to active memtable
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Freeze the memtable to create a frozen one
        store.freeze_active_memtable()?;

        // Verify we have a frozen memtable
        let frozen_count = {
            let frozen = store.state.frozen_memtables.read().unwrap();
            frozen.len()
        };
        assert_eq!(frozen_count, 1, "Should have one frozen memtable");

        // Flush the memtable
        let flushed = store.flush_memtable().await?;
        assert!(flushed, "Should have flushed a memtable");

        // Verify frozen memtable is gone
        let frozen_count_after = {
            let frozen = store.state.frozen_memtables.read().unwrap();
            frozen.len()
        };
        assert_eq!(
            frozen_count_after, 0,
            "Should have no frozen memtables after flush"
        );

        // Verify data is still accessible
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = store.get(key.as_bytes())?;
            assert!(
                value.is_some(),
                "Key {} should be accessible after flush",
                key
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_memtable_empty() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Try to flush when no frozen memtables exist
        let flushed = store.flush_memtable().await?;
        assert!(!flushed, "Should not flush when no frozen memtables");

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_memtable_multiple() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create multiple frozen memtables
        for batch in 0..3 {
            for i in 0..5 {
                let key = format!("batch_{}_key_{:03}", batch, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }
            store.freeze_active_memtable()?;
        }

        // Verify we have 3 frozen memtables
        let frozen_count = {
            let frozen = store.state.frozen_memtables.read().unwrap();
            frozen.len()
        };
        assert_eq!(frozen_count, 3, "Should have three frozen memtables");

        // Flush all memtables
        let mut flush_count = 0;
        while store.flush_memtable().await? {
            flush_count += 1;
        }
        assert_eq!(flush_count, 3, "Should have flushed 3 memtables");

        // Verify all data is still accessible
        for batch in 0..3 {
            for i in 0..5 {
                let key = format!("batch_{}_key_{:03}", batch, i);
                let value = store.get(key.as_bytes())?;
                assert!(value.is_some(), "Key {} should be accessible", key);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_wal_cleanup_no_deletable() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // With a fresh store, there should be no deletable WALs
        let _deletable = store.deletable_wals()?;
        // Should return a valid list (no assertion needed, if it fails it will panic)

        // Cleanup should be a no-op
        store.cleanup_wals().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_wal_cleanup_after_flush() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Add data and flush to create a scenario where WALs might be deletable
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Freeze and flush
        store.freeze_active_memtable()?;
        let flushed = store.flush_memtable().await?;
        assert!(flushed, "Should have flushed a memtable");

        // Try cleanup - should handle gracefully even if no WALs are deletable yet
        store.cleanup_wals().await?;

        // Verify data is still accessible
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = store.get(key.as_bytes())?;
            assert!(
                value.is_some(),
                "Key {} should be accessible after cleanup",
                key
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_wal_cleanup_missing_file() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Add some data
        for i in 0..5 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Create WAL directory structure
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        // Create a fake WAL file that we can test deletion with
        let test_wal_path = wal_dir.join("999.wal");
        std::fs::write(&test_wal_path, b"test")?;

        // Delete the file to simulate missing file scenario
        std::fs::remove_file(&test_wal_path)?;

        // Cleanup should handle missing files gracefully (tested indirectly through no panic)
        store.cleanup_wals().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_deletable_wals_method() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // The deletable_wals method should work without error
        let _deletable = store.deletable_wals()?;
        // For a fresh store, this should return a valid list (no assertion needed, if it fails it will panic)

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_and_cleanup_integration() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create data that will result in multiple memtables and potential WAL cleanup
        for batch in 0..2 {
            for i in 0..20 {
                let key = format!("integration_{}_{:03}", batch, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Freeze and flush each batch
            store.freeze_active_memtable()?;
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
            }
        }

        // Run cleanup
        store.cleanup_wals().await?;

        // Verify all data is still accessible
        for batch in 0..2 {
            for i in 0..20 {
                let key = format!("integration_{}_{:03}", batch, i);
                let value = store.get(key.as_bytes())?;
                assert!(
                    value.is_some(),
                    "Key {} should be accessible after flush and cleanup",
                    key
                );
            }
        }

        // Verify data integrity with scan
        let scan_results: Result<Vec<_>> = store.scan(..).collect();
        let results = scan_results?;
        assert_eq!(
            results.len(),
            40,
            "Should have 40 entries after integration test"
        );

        // Verify keys are in order
        for i in 1..results.len() {
            assert!(
                results[i - 1].0 < results[i].0,
                "Keys should be in order after flush and cleanup"
            );
        }

        Ok(())
    }
}
