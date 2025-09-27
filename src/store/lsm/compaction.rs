//! Tiered compaction strategy for LSM-tree space management.
//!
//! This module implements tiered compaction, which organizes SSTables into
//! levels with exponentially increasing size ratios. This strategy was chosen
//! over leveled compaction for its superior performance on write-heavy workloads.
//!
//! # Why Tiered Compaction?
//!
//! Tiered compaction offers several advantages for our use case:
//!
//! - **Write amplification**: Lower write amplification compared to leveled compaction
//! - **Write throughput**: Better performance for write-heavy workloads
//! - **Implementation simplicity**: Easier to implement and reason about
//! - **Level flexibility**: Each level can contain multiple overlapping SSTables
//!
//! According to the RocksDB paper ["RocksDB: Evolution of Development Priorities
//! in a Key-value Store Serving Large-scale Applications"](https://dl.acm.org/doi/pdf/10.1145/3483840),
//! tiered compaction provides better write performance for applications that
//! prioritize write throughput over read latency.
//!
//! # Compaction Decision Algorithm
//!
//! The algorithm uses a three-tier decision hierarchy:
//!
//! 1. **L0 Priority**: Level 0 gets highest priority due to overlapping key ranges
//! 2. **Size Ratio**: Higher levels compact when size ratio exceeds threshold
//! 3. **Table Count**: Final level compacts based on table count limit
//!
//! ## L0 Special Handling
//!
//! Level 0 requires special attention because:
//! - SSTables can have overlapping key ranges (flushed from memtables)
//! - Too many L0 tables slow down reads significantly
//! - L0 compaction merges overlapping tables into non-overlapping L1 tables
//!
//! ## Size Ratio Compaction
//!
//! For levels 1 and above:
//! - Each level should be roughly `size_ratio` times larger than the previous
//! - When a level becomes too large relative to the next, compact it
//! - This maintains the exponential size growth property

use super::{Level, LsmState, SSTable};
use crate::{config::CompactionConfig, error::Result};

use super::iterator::{LSMIterator, LSMScanIterator};

/// Check if compaction is needed
pub fn needs_compaction(state: &LsmState, config: &CompactionConfig) -> bool {
    // Don't run if already running
    if !state.needs_compaction() {
        return false;
    }

    // Use the actual compaction decision logic
    find_compaction_level(state, config).is_some()
}

/// Determines which level, if any, requires compaction based on a tiered strategy.
///
/// Compaction is triggered based on a hierarchy of rules:
/// 1. **L0 Compaction:** The highest priority. Triggered if L0 has too many tables,
///    as these tables can have overlapping key ranges and slow down reads.
/// 2. **Size Ratio Compaction (L1+):** Triggered for a level `N` if its total size is
///    significantly larger (by a configurable ratio) than the next level `N+1`. This
///    keeps the size of levels growing exponentially, which is the goal of tiered compaction.
/// 3. **Last Level Compaction:** If a level is the final one, it's compacted only if it
///    accumulates too many individual table files, as there's no "next level" to compare its size against.
pub fn find_compaction_level(state: &LsmState, config: &CompactionConfig) -> Option<u32> {
    let levels = state.levels.read().unwrap();

    // Rule 1: L0 is the highest priority, triggered by the number of tables.
    if !levels.is_empty() && levels[0].table_count() > config.level0_compaction_threshold {
        return Some(0);
    }

    // Rules 2 & 3: Check higher levels for size-ratio or last-level table count triggers.
    for (level_idx, current_level) in levels.iter().enumerate().skip(1) {
        // First, a level must have a minimum number of tables to be considered for compaction.
        // This avoids compacting levels that are mostly empty or have just been compacted.
        if current_level.table_count() < config.max_tables_per_level {
            continue;
        }

        let level_num = level_idx as u32;

        // Rule 3: Handle the special case for the last level and return immediately if it matches.
        let is_last_level = level_idx == levels.len() - 1;
        if is_last_level {
            return Some(level_num);
        }

        // Rule 2: If we are here, it's an intermediate level. Check the size ratio.
        let next_level = &levels[level_idx + 1];
        let current_size = current_level.size();
        let next_size = next_level.size().max(1);

        let is_oversized = current_size / next_size >= config.size_ratio_threshold as u64;
        if is_oversized {
            return Some(level_num);
        }
    }
    None
}

/// Perform tiered compaction if needed
pub async fn compact(store: &super::LsmStore) -> Result<()> {
    let state = &store.state;
    let config = &store.config;
    let _guard = state.start_compaction();

    // Find which level needs compaction
    let source_level = match find_compaction_level(state, &config.compaction) {
        Some(level) => level,
        None => return Ok(()), // No compaction needed
    };

    let target_level = source_level + 1;

    // Get source tables to compact
    let source_tables = {
        let levels = state.levels.read().unwrap();
        if (source_level as usize) >= levels.len() {
            return Ok(());
        }
        levels[source_level as usize].all_tables()
    };

    tracing::info!(
        source_level = source_level,
        target_level = target_level,
        source_tables = source_tables.len(),
        "Starting tiered compaction"
    );

    // 1. Create iterators for all source tables
    let mut iterators = Vec::new();
    let source_table_ids: Vec<u64> = source_tables.iter().map(|t| t.id).collect();

    {
        let levels = state.levels.read().unwrap();
        for table_meta in &source_tables {
            let sstable = levels[source_level as usize]
                .sstables
                .iter()
                .find(|s| s.id == table_meta.id)
                .ok_or_else(|| crate::Error::InvalidOperation("SSTable not found".to_string()))?;

            let scan_iter = sstable.table.scan(..)?;
            let boxed_iter: LSMIterator = Box::new(scan_iter);
            iterators.push(boxed_iter);
        }
    }

    // 2. Create merge iterator and write to new SSTable
    let merge_iter = LSMScanIterator::new(iterators);

    let (table_id, table_path) = store.next_sstable_path();
    let mut writable_table = super::sstable::table::Table::writable(table_path.to_str().unwrap())?;

    let mut builder = super::sstable::block::Builder::new();
    let mut first_key_in_block: Option<Vec<u8>> = None;
    let mut entry_count = 0;
    let mut min_key: Option<Vec<u8>> = None;
    let mut max_key: Option<Vec<u8>> = None;

    for entry_result in merge_iter {
        let (key, value) = entry_result?;

        // Track min/max keys
        if min_key.is_none() {
            min_key = Some(key.clone());
        }
        max_key = Some(key.clone());

        if first_key_in_block.is_none() {
            first_key_in_block = Some(key.clone());
        }

        builder.add_entry(&key, &value);
        entry_count += 1;

        // Flush block when it gets large enough
        if builder.len() >= super::sstable::table::MAX_BLOCK_SIZE {
            let block_data = builder.finish();
            let first_key = first_key_in_block.take().unwrap();
            writable_table.add_block(&block_data, first_key)?;
            builder = super::sstable::block::Builder::new();
        }
    }

    // Write final block if not empty
    if !builder.is_empty() {
        let block_data = builder.finish();
        let first_key = first_key_in_block.take().unwrap();
        writable_table.add_block(&block_data, first_key)?;
    }

    let table = writable_table.finalize()?;

    // 3. Create table metadata
    let table_meta = super::manifest::meta::TableMeta {
        id: table_id,
        level: target_level,
        size: std::fs::metadata(&table_path)?.len(),
        entry_count,
        min_key: min_key.unwrap_or_default(),
        max_key: max_key.unwrap_or_default(),
    };

    // 4. Update manifest with compaction record
    let job_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    {
        #[allow(clippy::readonly_write_lock)] // next_seq() mutates the manifest
        let manifest = state.manifest.write().unwrap();
        let begin_seq = manifest.next_seq();
        manifest.append(super::manifest::edit::VersionEdit::BeginCompaction {
            seq: begin_seq,
            job_id,
            source_level,
            target_level,
        })?;

        let commit_seq = manifest.next_seq();
        manifest.append(super::manifest::edit::VersionEdit::CommitCompaction {
            seq: commit_seq,
            job_id,
            source_level,
            deleted_tables: source_table_ids.clone(),
            target_level,
            added_tables: vec![table_meta.clone()],
        })?;
        manifest.sync()?;
    }

    // 5. Update levels data structure
    {
        let mut levels = state.levels.write().unwrap();

        // Remove source tables from source level
        if (source_level as usize) < levels.len() {
            levels[source_level as usize]
                .sstables
                .retain(|t| !source_table_ids.contains(&t.id));
        }

        // Ensure target level exists
        while levels.len() <= target_level as usize {
            let level_num = levels.len() as u32;
            levels.push(Level::new(level_num));
        }

        // Add new table to target level
        let sstable = SSTable::new(table_path, table, &table_meta)?;
        levels[target_level as usize].add_sstable(sstable);
    }

    // 6. Delete old SSTable files
    for table_id in &source_table_ids {
        let old_path = store.sstable_path(*table_id);
        if let Err(e) = std::fs::remove_file(&old_path) {
            tracing::warn!(table_id = table_id, error = %e, "Failed to delete old SSTable file");
        }
    }

    tracing::info!(
        source_level = source_level,
        target_level = target_level,
        source_tables = source_table_ids.len(),
        target_table = table_id,
        entries_compacted = entry_count,
        "Completed tiered compaction"
    );

    // Validate state consistency after compaction
    if let Err(e) = store.state.validate_consistency() {
        tracing::warn!("State inconsistency detected after compaction: {:?}", e);
    }

    // Debug-only comprehensive validation
    #[cfg(debug_assertions)]
    {
        if let Err(e) = store.state.validate_sstable_id_uniqueness() {
            tracing::error!("SSTable ID uniqueness violation after compaction: {:?}", e);
        }

        if let Err(e) = store.state.validate_level_key_ordering() {
            tracing::error!("Level key ordering violation after compaction: {:?}", e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{CompactionConfig, LsmConfig},
        error::Result,
        store::Store,
        tmpfs::TempDir,
    };
    use std::time::Duration;

    // Helper function to create a test store with custom config
    fn create_test_store_with_config(
        temp_dir: &TempDir,
        config: CompactionConfig,
    ) -> super::super::LsmStore {
        let lsm_config = LsmConfig::new(temp_dir.path()).compaction(config);
        super::super::LsmStore::open_with_config(lsm_config).expect("Failed to create store")
    }

    // Helper function to create a test store with default aggressive compaction settings
    fn create_test_store(temp_dir: &TempDir) -> super::super::LsmStore {
        let compaction_config = CompactionConfig::default()
            .level0_compaction_threshold(2) // Lower threshold for easier testing
            .size_ratio_threshold(2) // Lower ratio for easier testing
            .max_tables_per_level(3); // Lower max for easier testing

        create_test_store_with_config(temp_dir, compaction_config)
    }

    // Helper to populate data that will create multiple SSTables
    async fn populate_multiple_tables(
        store: &super::super::LsmStore,
        table_count: usize,
    ) -> Result<()> {
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
    fn verify_data_integrity(
        store: &super::super::LsmStore,
        expected_entries: usize,
    ) -> Result<()> {
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
}
