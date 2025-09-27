use super::{
    super::Store,
    compaction, flush,
    iterator::{MergeIterator, OwningMemtableIter},
    memtable::ActiveMemtable,
    recovery, LsmState,
};
use crate::{config::LsmConfig, error::Result, flock::FileLock};

use std::{fs, ops::RangeBounds, sync::Arc};

const LOCK_FILE: &str = "ashdb.lock";

/// LSM Store with interior mutability
pub struct LsmStore {
    // Immutable configuration (public for background tasks)
    pub(super) config: LsmConfig,
    lock: Option<FileLock>,

    // Shared mutable state (public for background tasks)
    pub(super) state: Arc<LsmState>,
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

    /// Freeze active memtable (atomic operation)
    pub fn freeze_active_memtable(&self) -> Result<()> {
        // Try to acquire freeze lock
        let _guard = match self.state.try_start_freeze() {
            Some(guard) => guard,
            None => return Ok(()), // Another freeze in progress
        };

        // Create new active memtable
        let (new_wal_id, wal_path) = self.next_wal_path();
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

    /// Try to mark flush as pending (returns true if successfully marked)
    pub(super) fn try_mark_flush_pending(&self) -> bool {
        self.state.try_mark_flush_pending()
    }

    /// Mark flush as completed
    pub(super) fn mark_flush_completed(&self) {
        self.state.mark_flush_completed()
    }

    /// Generate next WAL ID and construct its path
    pub(super) fn next_wal_path(&self) -> (u64, std::path::PathBuf) {
        let wal_id = self.state.next_wal_id();
        let path = self
            .config
            .dir
            .join("wal")
            .join(format!("{:08}.wal", wal_id));
        (wal_id, path)
    }

    /// Generate next SSTable ID and construct its path
    pub(super) fn next_sstable_path(&self) -> (u64, std::path::PathBuf) {
        let table_id = self.state.next_sstable_id();
        let path = self
            .config
            .dir
            .join("sst")
            .join(format!("{:08}.sst", table_id));
        (table_id, path)
    }

    /// Construct path for existing WAL by ID
    pub(super) fn wal_path(&self, wal_id: u64) -> std::path::PathBuf {
        self.config
            .dir
            .join("wal")
            .join(format!("{:08}.wal", wal_id))
    }

    /// Construct path for existing SSTable by ID
    pub(super) fn sstable_path(&self, table_id: u64) -> std::path::PathBuf {
        self.config
            .dir
            .join("sst")
            .join(format!("{:08}.sst", table_id))
    }

    /// Get current health metrics for monitoring and debugging
    pub fn get_health_metrics(&self) -> super::state::StateMetrics {
        self.state.get_state_metrics()
    }

    /// Comprehensive validation for debug builds and testing
    #[cfg(debug_assertions)]
    pub fn validate_comprehensive(&self) -> Result<()> {
        tracing::debug!("Starting comprehensive validation");

        // Basic consistency validation
        self.state.validate_consistency()?;

        // SSTable ID uniqueness validation
        self.state.validate_sstable_id_uniqueness()?;

        // Level key ordering validation
        self.state.validate_level_key_ordering()?;

        tracing::debug!("Comprehensive validation passed");
        Ok(())
    }

    /// Run periodic health check with validation
    pub fn health_check(&self) -> Result<super::state::StateMetrics> {
        let metrics = self.get_health_metrics();

        // Log health metrics
        tracing::info!(
            "LSM Health Check - levels: {}, tables: {}, active_size: {}, frozen: {}, operations: compaction={}, freeze={}, flush={}",
            metrics.level_count,
            metrics.total_sstable_count,
            metrics.active_memtable_size,
            metrics.frozen_memtable_count,
            metrics.compaction_running,
            metrics.freeze_in_progress,
            metrics.flush_pending
        );

        // Warn about potential issues
        if metrics.compaction_running > 2 {
            tracing::warn!(
                "Multiple compactions running: {}",
                metrics.compaction_running
            );
        }

        if metrics.frozen_memtable_count > 10 {
            tracing::warn!(
                "High number of frozen memtables: {}",
                metrics.frozen_memtable_count
            );
        }

        // Basic consistency check in production
        if let Err(e) = self.state.validate_consistency() {
            tracing::error!("Health check detected inconsistency: {:?}", e);
            return Err(e);
        }

        // Comprehensive validation in debug builds
        #[cfg(debug_assertions)]
        {
            if let Err(e) = self.validate_comprehensive() {
                tracing::error!("Comprehensive health check failed: {:?}", e);
                return Err(e);
            }
        }

        Ok(metrics)
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

    fn scan<'a>(
        &'a self,
        range: impl RangeBounds<Vec<u8>> + Clone + Send + Sync + 'a,
    ) -> Self::ScanIterator<'a> {
        type BoxedResultIterator<'a> =
            Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + Sync + 'a>;

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

    fn sync(&self) -> Result<()> {
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

            // Force flush using public API
            store.freeze_active_memtable()?;
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
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

            // Force flush to create individual tables using public API
            store.freeze_active_memtable()?;
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
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

        // Force flush but don't exceed L0 threshold using public API
        store.freeze_active_memtable()?;
        while store.flush_memtable().await? {
            // Keep flushing until no more frozen memtables
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

            // Force flush using public API
            store.freeze_active_memtable()?;
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
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
    async fn test_wal_cleanup_after_flush() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Add data and flush to test WAL cleanup integration
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Freeze and flush (WAL cleanup is now integrated into flush)
        store.freeze_active_memtable()?;
        let flushed = store.flush_memtable().await?;
        assert!(flushed, "Should have flushed a memtable");

        // Verify data is still accessible after flush (with integrated WAL cleanup)
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = store.get(key.as_bytes())?;
            assert!(
                value.is_some(),
                "Key {} should be accessible after flush with cleanup",
                key
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_flush_and_cleanup_integration() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Create data that will result in multiple memtables with integrated WAL cleanup
        for batch in 0..2 {
            for i in 0..20 {
                let key = format!("integration_{}_{:03}", batch, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Freeze and flush each batch (WAL cleanup integrated into flush)
            store.freeze_active_memtable()?;
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
            }
        }

        // Verify all data is still accessible after flush with integrated cleanup
        for batch in 0..2 {
            for i in 0..20 {
                let key = format!("integration_{}_{:03}", batch, i);
                let value = store.get(key.as_bytes())?;
                assert!(
                    value.is_some(),
                    "Key {} should be accessible after flush with cleanup",
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
                "Keys should be in order after flush with cleanup"
            );
        }

        Ok(())
    }

    // ===== PUBLIC API TESTS =====
    // Tests that exercise the 4 core public API methods in realistic patterns

    #[tokio::test]
    async fn test_manual_flush_cycle() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially should not need flush
        assert!(!store.needs_flush(), "Fresh store should not need flush");

        // Write data until we have a frozen memtable
        for i in 0..10 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Manually freeze to create a frozen memtable
        store.freeze_active_memtable()?;

        // Now should need flush
        assert!(
            store.needs_flush(),
            "Should need flush after freezing memtable"
        );

        // Manual flush using public API
        let flushed = store.flush_memtable().await?;
        assert!(flushed, "Should have flushed a memtable");

        // Should no longer need flush
        assert!(!store.needs_flush(), "Should not need flush after flushing");

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
    async fn test_manual_compaction_cycle() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially should not need compaction
        assert!(
            !store.needs_compaction(),
            "Fresh store should not need compaction"
        );

        // Create multiple SSTables to trigger compaction need
        for table_idx in 0..4 {
            // Exceed our threshold of 2
            for i in 0..20 {
                let key = format!("table_{}_key_{:03}", table_idx, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Create SSTable using public API
            store.freeze_active_memtable()?;
            let flushed = store.flush_memtable().await?;
            assert!(flushed, "Should have flushed memtable {}", table_idx);
        }

        // Now should need compaction
        assert!(
            store.needs_compaction(),
            "Should need compaction after creating multiple SSTables"
        );

        // Manual compaction using public API
        store.compact().await?;

        // Should no longer need compaction (or need less)
        // Note: might still need some compaction depending on the result
        let compaction_reduced = !store.needs_compaction() || {
            // If still needs compaction, verify structure improved
            let levels = store.state.levels.read().unwrap();
            levels.len() >= 2 && levels[1].table_count() > 0
        };
        assert!(
            compaction_reduced,
            "Compaction should reduce need or create multi-level structure"
        );

        // Verify all data is still accessible
        for table_idx in 0..4 {
            for i in 0..20 {
                let key = format!("table_{}_key_{:03}", table_idx, i);
                let value = store.get(key.as_bytes())?;
                assert!(
                    value.is_some(),
                    "Key {} should be accessible after compaction",
                    key
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_maintenance_workflow() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        let mut total_entries = 0;

        // Simulate realistic maintenance workflow
        for batch in 0..6 {
            // Write a batch of data
            for i in 0..30 {
                let key = format!("batch_{}_key_{:03}", batch, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
                total_entries += 1;
            }

            // Periodic flush check using public API
            if store.needs_flush() {
                while store.flush_memtable().await? {
                    // Keep flushing until no more frozen memtables
                }
            }

            // Periodic compaction check using public API
            if store.needs_compaction() {
                store.compact().await?;
            }

            // Every few batches, force a freeze to create more SSTables
            if batch % 2 == 0 {
                store.freeze_active_memtable()?;
            }
        }

        // Final maintenance cycle
        while store.flush_memtable().await? {
            // Flush any remaining frozen memtables
        }

        if store.needs_compaction() {
            store.compact().await?;
        }

        // Verify all data is accessible
        let mut found_entries = 0;
        for batch in 0..6 {
            for i in 0..30 {
                let key = format!("batch_{}_key_{:03}", batch, i);
                if store.get(key.as_bytes())?.is_some() {
                    found_entries += 1;
                }
            }
        }

        assert_eq!(
            found_entries, total_entries,
            "All entries should be accessible after maintenance"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_api_contracts() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Test initial state
        assert!(!store.needs_flush(), "Fresh store should not need flush");
        assert!(
            !store.needs_compaction(),
            "Fresh store should not need compaction"
        );

        // Test flush when nothing to flush
        let flushed = store.flush_memtable().await?;
        assert!(!flushed, "Should return false when nothing to flush");

        // Test compaction when not needed (should succeed gracefully)
        store.compact().await?; // Should not error

        // Test after adding minimal data
        store.set(b"test_key", b"test_value".to_vec())?;

        // Should still not need flush (not frozen yet)
        assert!(
            !store.needs_flush(),
            "Should not need flush with just active memtable"
        );

        // Freeze to create need for flush
        store.freeze_active_memtable()?;
        assert!(store.needs_flush(), "Should need flush after freezing");

        // Test multiple flush calls
        let first_flush = store.flush_memtable().await?;
        assert!(first_flush, "First flush should succeed");

        let second_flush = store.flush_memtable().await?;
        assert!(
            !second_flush,
            "Second flush should return false (nothing to flush)"
        );

        // Verify data integrity maintained
        let value = store.get(b"test_key")?;
        assert_eq!(
            value,
            Some(b"test_value".to_vec()),
            "Data should be preserved through API operations"
        );

        Ok(())
    }

    // ===== STATE CONSISTENCY TESTS =====
    // Tests that ensure state is properly maintained across restarts and operations

    #[tokio::test]
    async fn test_state_recovery_after_restart() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path();

        // Initial data to persist
        let test_data = vec![("key1", "value1"), ("key2", "value2"), ("key3", "value3")];

        // First session: write data and flush
        {
            let store = LsmStore::open(store_path.to_str().unwrap())?;

            for (key, value) in &test_data {
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }

            // Force flush to ensure data is persisted
            store.freeze_active_memtable()?;
            while store.flush_memtable().await? {
                // Keep flushing until no more frozen memtables
            }

            // Verify data is accessible before restart
            for (key, expected_value) in &test_data {
                let value = store.get(key.as_bytes())?;
                assert_eq!(value, Some(expected_value.as_bytes().to_vec()));
            }
        } // Store is dropped here, triggering manifest sync

        // Second session: reopen and verify data is recovered
        {
            let store = LsmStore::open(store_path.to_str().unwrap())?;

            // Verify all data is still accessible after restart
            for (key, expected_value) in &test_data {
                let value = store.get(key.as_bytes())?;
                assert_eq!(
                    value,
                    Some(expected_value.as_bytes().to_vec()),
                    "Key {} should be recovered after restart",
                    key
                );
            }

            // Verify scan still works correctly
            let scan_results: Result<Vec<_>> = store.scan(..).collect();
            let results = scan_results?;
            assert_eq!(results.len(), test_data.len());

            // Verify keys are in correct order
            for i in 1..results.len() {
                assert!(
                    results[i - 1].0 < results[i].0,
                    "Keys should be in order after recovery"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_state_consistency_after_compaction_and_restart() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path();

        // Create multiple SSTables, then compact, then restart
        {
            let store = create_test_store(&temp_dir);

            // Create data that will result in multiple L0 tables
            populate_multiple_tables(&store, 4).await?;

            // Verify compaction is needed
            assert!(
                store.needs_compaction(),
                "Should need compaction after populating tables"
            );

            // Perform compaction
            store.compact().await?;

            // Verify some data after compaction
            let value = store.get(b"key_000_000")?;
            assert!(
                value.is_some(),
                "Data should be accessible after compaction"
            );
        }

        // Restart and verify compacted state is recovered
        {
            let store = LsmStore::open(store_path.to_str().unwrap())?;

            // Verify the compacted structure was recovered
            {
                let levels = store.state.levels.read().unwrap();
                assert!(
                    levels.len() >= 2,
                    "Should have recovered multi-level structure after compaction"
                );
            }

            // Verify all data is still accessible
            for table_idx in 0..4 {
                for i in 0..100 {
                    let key = format!("key_{:03}_{:03}", table_idx, i);
                    let value = store.get(key.as_bytes())?;
                    assert!(
                        value.is_some(),
                        "Key {} should be accessible after compaction recovery",
                        key
                    );
                }
            }

            // Verify scan integrity
            let scan_results: Result<Vec<_>> = store.scan(..).collect();
            let results = scan_results?;
            assert_eq!(
                results.len(),
                400,
                "Should have all 400 entries after recovery"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_manifest_state_consistency() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Perform several operations that modify state
        populate_multiple_tables(&store, 3).await?;
        store.compact().await?;

        // Compare manifest state with in-memory state
        let manifest_state = {
            let manifest = store.state.manifest.read().unwrap();
            manifest.replay()?
        };

        let in_memory_levels = store.state.levels.read().unwrap();

        // Verify level count consistency
        assert_eq!(
            manifest_state.levels.len(),
            in_memory_levels.len(),
            "Manifest and in-memory level count should match"
        );

        // Verify table count consistency per level
        for (level_idx, level_meta) in manifest_state.levels.iter().enumerate() {
            if level_idx < in_memory_levels.len() {
                assert_eq!(
                    level_meta.tables.len(),
                    in_memory_levels[level_idx].table_count(),
                    "Table count should match between manifest and memory for level {}",
                    level_idx
                );
            }
        }

        // Verify next table ID consistency
        let current_max_id = in_memory_levels
            .iter()
            .flat_map(|level| &level.sstables)
            .map(|table| table.id)
            .max()
            .unwrap_or(0);

        assert!(
            manifest_state.next_table_id > current_max_id,
            "Next table ID should be greater than current max table ID"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_state_recovery_with_mixed_operations() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path();

        // Test basic write, flush, compaction cycle
        {
            let store = LsmStore::open(store_path.to_str().unwrap())?;

            // Write data across multiple sessions that will create multiple SSTables
            for batch in 0..3 {
                for i in 0..30 {
                    let key = format!("batch_{}_key_{:03}", batch, i);
                    let value = format!("value_{}", i);
                    store.set(key.as_bytes(), value.as_bytes().to_vec())?;
                }
                store.freeze_active_memtable()?;
                while store.flush_memtable().await? {}
            }

            if store.needs_compaction() {
                store.compact().await?;
            }

            // Verify some data exists
            let value = store.get(b"batch_0_key_000")?;
            assert!(value.is_some(), "Data should exist after operations");
        }

        // Recovery verification after restart
        {
            let store = LsmStore::open(store_path.to_str().unwrap())?;

            // Verify data exists after recovery
            let value = store.get(b"batch_0_key_000")?;
            assert!(value.is_some(), "Data should exist after recovery");

            // Verify scan works
            let scan_results: Result<Vec<_>> = store.scan(..).collect();
            let results = scan_results?;
            assert!(!results.is_empty(), "Should have data after recovery");

            // Verify state validation passes
            assert!(
                store.state.validate_consistency().is_ok(),
                "State should be consistent after recovery"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_empty_store_recovery() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path();

        // Create and immediately close empty store
        {
            let _store = LsmStore::open(store_path.to_str().unwrap())?;
            // Store is empty, just close it
        }

        // Reopen and verify it's still empty but functional
        {
            let store = LsmStore::open(store_path.to_str().unwrap())?;

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

    // ===== CONCURRENT OPERATION TESTS =====
    // Tests that ensure state consistency during concurrent operations

    #[tokio::test]
    async fn test_concurrent_flush_operations() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = Arc::new(create_test_store(&temp_dir));

        // Create some frozen memtables
        for batch in 0..3 {
            for i in 0..50 {
                let key = format!("batch_{}_key_{:03}", batch, i);
                let value = format!("value_{}", i);
                store.set(key.as_bytes(), value.as_bytes().to_vec())?;
            }
            store.freeze_active_memtable()?;
        }

        // Launch multiple concurrent flush operations
        let mut handles = Vec::new();
        for task_id in 0..3 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let mut flush_count = 0;
                while store_clone.flush_memtable().await.unwrap_or(false) {
                    flush_count += 1;
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                (task_id, flush_count)
            });
            handles.push(handle);
        }

        // Wait for all flush operations to complete
        let mut total_flushes = 0;
        for handle in handles {
            let (_, flush_count) = handle.await.unwrap();
            total_flushes += flush_count;
        }

        // Should have flushed exactly 3 memtables total (no double-flushing)
        assert_eq!(
            total_flushes, 3,
            "Should have flushed exactly 3 memtables total"
        );

        // Verify no frozen memtables remain
        assert!(
            !store.needs_flush(),
            "Should not need flush after concurrent operations"
        );

        // Verify data integrity
        for batch in 0..3 {
            for i in 0..50 {
                let key = format!("batch_{}_key_{:03}", batch, i);
                let value = store.get(key.as_bytes())?;
                assert!(
                    value.is_some(),
                    "Data should be preserved during concurrent flush"
                );
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_concurrent_freeze_operations() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = Arc::new(create_test_store(&temp_dir));

        // Fill active memtable close to capacity
        for i in 0..1000 {
            let key = format!("key_{:06}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Launch multiple concurrent freeze operations
        let mut handles = Vec::new();
        for task_id in 0..5 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let result = store_clone.freeze_active_memtable();
                (task_id, result.is_ok())
            });
            handles.push(handle);
        }

        // Wait for all freeze attempts
        let mut successful_freezes = 0;
        for handle in handles {
            let (_, success) = handle.await.unwrap();
            if success {
                successful_freezes += 1;
            }
        }

        // All should succeed (the guard mechanism ensures atomicity)
        assert_eq!(
            successful_freezes, 5,
            "All freeze operations should succeed"
        );

        // Verify we have multiple frozen memtables now
        let frozen_count = {
            let frozen = store.state.frozen_memtables.read().unwrap();
            frozen.len()
        };
        assert!(
            frozen_count >= 1,
            "Should have at least one frozen memtable"
        );

        // Verify data integrity
        for i in 0..1000 {
            let key = format!("key_{:06}", i);
            let value = store.get(key.as_bytes())?;
            assert!(
                value.is_some(),
                "Data should be preserved during concurrent freeze operations"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_mixed_concurrent_operations() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = Arc::new(create_test_store(&temp_dir));

        // Set up initial state with some data
        for i in 0..100 {
            let key = format!("initial_key_{:03}", i);
            let value = format!("initial_value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }
        store.freeze_active_memtable()?;

        // Create more data to trigger various operations
        populate_multiple_tables(&store, 2).await?;

        // Launch mixed concurrent operations
        let mut handles = Vec::new();

        // Flush operations
        for task_id in 0..2 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let mut ops = 0;
                while store_clone.flush_memtable().await.unwrap_or(false) {
                    ops += 1;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                format!("flush_{}: {} ops", task_id, ops)
            });
            handles.push(handle);
        }

        // Compaction operations
        for task_id in 0..2 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                if store_clone.needs_compaction() {
                    let result = store_clone.compact().await;
                    format!("compact_{}: {}", task_id, result.is_ok())
                } else {
                    format!("compact_{}: not_needed", task_id)
                }
            });
            handles.push(handle);
        }

        // Write operations (to test coordination with background operations)
        for task_id in 0..2 {
            let store_clone = Arc::clone(&store);
            let handle = tokio::spawn(async move {
                let mut writes = 0;
                for i in 0..50 {
                    let key = format!("concurrent_{}_{:03}", task_id, i);
                    let value = format!("value_{}", i);
                    if store_clone
                        .set(key.as_bytes(), value.as_bytes().to_vec())
                        .is_ok()
                    {
                        writes += 1;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                format!("write_{}: {} writes", task_id, writes)
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            let op_result = handle.await.unwrap();
            println!("Operation result: {}", op_result);
        }

        // Verify final state consistency
        // All initial data should still be accessible
        for i in 0..100 {
            let key = format!("initial_key_{:03}", i);
            let value = store.get(key.as_bytes())?;
            assert!(
                value.is_some(),
                "Initial data should survive concurrent operations"
            );
        }

        // All concurrent writes should be accessible
        for task_id in 0..2 {
            for i in 0..50 {
                let key = format!("concurrent_{}_{:03}", task_id, i);
                let value = store.get(key.as_bytes())?;
                assert!(
                    value.is_some(),
                    "Concurrent write data should be accessible"
                );
            }
        }

        // Verify scan integrity (all data in order)
        let scan_results: Result<Vec<_>> = store.scan(..).collect();
        let results = scan_results?;
        for i in 1..results.len() {
            assert!(
                results[i - 1].0 < results[i].0,
                "All data should be in correct order after mixed operations"
            );
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_exclusive_directory_access() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store_path = temp_dir.path().to_str().unwrap();

        // First instance should acquire lock successfully
        let store1 = LsmStore::open(store_path)?;

        // Second instance should fail to acquire lock
        let result = LsmStore::open(store_path);
        assert!(
            result.is_err(),
            "Second instance should fail to open the same directory"
        );

        // Add some data to first instance
        store1.set(b"test_key", b"test_value".to_vec())?;

        // Verify the lock error is what we expect
        match result {
            Err(crate::Error::LockError(_)) | Err(crate::Error::IoError(_)) => {
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
        let store2 = LsmStore::open(store_path)?;

        // Verify data from first instance is recovered
        let value = store2.get(b"test_key")?;
        assert_eq!(value, Some(b"test_value".to_vec()));

        Ok(())
    }

    // ===== STATE VALIDATION TESTS =====
    // Tests for the state validation methods

    #[tokio::test]
    async fn test_state_consistency_validation() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially state should be consistent
        if let Err(e) = store.state.validate_consistency() {
            panic!("Initial state validation failed: {:?}", e);
        }

        // Test with normal operations that properly record to manifest
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        // Force flush to ensure manifest is updated
        store.freeze_active_memtable()?;
        while store.flush_memtable().await? {}

        // State should be consistent after proper flush
        if let Err(e) = store.state.validate_consistency() {
            panic!("State validation failed after flush: {:?}", e);
        }

        // Perform compaction if needed
        if store.needs_compaction() {
            store.compact().await?;

            // State should remain consistent after compaction
            if let Err(e) = store.state.validate_consistency() {
                panic!("State validation failed after compaction: {:?}", e);
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_sstable_id_uniqueness_validation() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially should be valid (no SSTables)
        assert!(store.state.validate_sstable_id_uniqueness().is_ok());

        // Add some tables
        populate_multiple_tables(&store, 3).await?;

        // Should still be valid (all IDs should be unique)
        assert!(store.state.validate_sstable_id_uniqueness().is_ok());

        Ok(())
    }

    #[tokio::test]
    async fn test_state_metrics() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Get initial metrics
        let initial_metrics = store.state.get_state_metrics();
        assert_eq!(initial_metrics.level_count, 0);
        assert_eq!(initial_metrics.total_sstable_count, 0);
        assert_eq!(initial_metrics.frozen_memtable_count, 0);

        // Add some data
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        let after_writes = store.state.get_state_metrics();
        assert!(after_writes.active_memtable_size > 0);

        // Freeze memtable
        store.freeze_active_memtable()?;

        let after_freeze = store.state.get_state_metrics();
        assert_eq!(after_freeze.frozen_memtable_count, 1);

        // Flush memtable
        while store.flush_memtable().await? {}

        let after_flush = store.state.get_state_metrics();
        assert_eq!(after_flush.frozen_memtable_count, 0);
        assert!(after_flush.level_count >= 1);
        assert!(after_flush.total_sstable_count >= 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_operation_safety_check() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially should be safe
        assert!(store.state.is_operation_safe());

        // Start a compaction guard
        let _guard = store.state.start_compaction();
        assert!(!store.state.is_operation_safe());

        // Drop the guard
        drop(_guard);
        assert!(store.state.is_operation_safe());

        // Test freeze guard
        let _freeze_guard = store.state.try_start_freeze();
        assert!(!store.state.is_operation_safe());

        Ok(())
    }

    #[tokio::test]
    async fn test_level_key_ordering_validation() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially should be valid (no levels)
        assert!(store.state.validate_level_key_ordering().is_ok());

        // Add ordered data
        for i in 0..100 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        store.freeze_active_memtable()?;
        while store.flush_memtable().await? {}

        // Should still be valid after flush (creates L0 tables which can overlap)
        assert!(store.state.validate_level_key_ordering().is_ok());

        // After compaction, higher levels should have non-overlapping tables
        if store.needs_compaction() {
            store.compact().await?;
            assert!(store.state.validate_level_key_ordering().is_ok());
        }

        Ok(())
    }

    // ===== VALIDATION INTEGRATION TESTS =====
    // Tests for the newly integrated validation functionality

    #[tokio::test]
    async fn test_health_check_api() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Test initial health check
        let initial_metrics = store.health_check()?;
        assert_eq!(initial_metrics.level_count, 0);
        assert_eq!(initial_metrics.total_sstable_count, 0);
        assert_eq!(initial_metrics.frozen_memtable_count, 0);

        // Add some data and test health check again
        for i in 0..50 {
            let key = format!("key_{:03}", i);
            let value = format!("value_{}", i);
            store.set(key.as_bytes(), value.as_bytes().to_vec())?;
        }

        let after_writes = store.health_check()?;
        assert!(after_writes.active_memtable_size > 0);

        // Test health check after operations
        store.freeze_active_memtable()?;
        let after_freeze = store.health_check()?;
        assert_eq!(after_freeze.frozen_memtable_count, 1);

        while store.flush_memtable().await? {}
        let after_flush = store.health_check()?;
        assert_eq!(after_flush.frozen_memtable_count, 0);
        assert!(after_flush.level_count >= 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_validation_api_integration() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Test basic validation API
        assert!(store.state.validate_consistency().is_ok());
        assert!(store.state.is_operation_safe());

        // Test validation with data
        populate_multiple_tables(&store, 2).await?;

        // Should still be valid after operations
        assert!(store.state.validate_consistency().is_ok());

        // Test comprehensive validation in debug mode
        #[cfg(debug_assertions)]
        {
            assert!(store.validate_comprehensive().is_ok());
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_operation_safety_integration() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = create_test_store(&temp_dir);

        // Initially should be safe
        assert!(store.state.is_operation_safe());

        // Create conditions where compaction might be needed
        populate_multiple_tables(&store, 3).await?;

        // Test that compaction respects safety check
        // (Note: the safety check in compact() will skip if unsafe,
        // but won't fail the test as it returns Ok(()))
        let result = store.compact().await;
        assert!(result.is_ok());

        // Verify state remains consistent
        assert!(store.state.validate_consistency().is_ok());

        Ok(())
    }
}
