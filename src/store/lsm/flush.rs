use super::{Level, SSTable};
use crate::error::Result;

/// Manually flush oldest frozen memtable to SSTable (for testing)
pub async fn flush_memtable(store: &super::LsmStore) -> Result<bool> {
    // Check if flush needed and try to mark as pending
    if !store.needs_flush() || !store.state.try_mark_flush_pending() {
        return Ok(false);
    }

    let state = &store.state;
    // Get oldest frozen memtable
    let memtable = {
        let mut frozen = state.frozen_memtables.write().unwrap();
        match frozen.pop_front() {
            Some(m) => m,
            None => {
                store.state.mark_flush_completed();
                return Ok(false);
            }
        }
    };

    let wal_id = memtable.wal_id();

    // Create SSTable (I/O - no locks held)
    let (table_id, table_path) = store.next_sstable_path();
    let mut sstable = super::sstable::table::Table::writable(table_path.to_str().unwrap())?;

    // Extract metadata during flush
    let (min_key, max_key, entry_count) = {
        let mut scan_iter = memtable.scan(..)?;
        let mut min_key: Option<Vec<u8>> = None;
        let mut max_key: Option<Vec<u8>> = None;
        let mut count = 0;

        if let Some(Ok((key, _))) = scan_iter.next() {
            min_key = Some(key.clone());
            max_key = Some(key);
            count = 1;

            // Continue iterating to get max key and count
            for entry_result in scan_iter {
                let (key, _) = entry_result?;
                max_key = Some(key);
                count += 1;
            }
        }

        (
            min_key.unwrap_or_default(),
            max_key.unwrap_or_default(),
            count,
        )
    };

    memtable.flush(&mut sstable)?;
    let table = sstable.finalize()?;

    let table_meta = super::manifest::meta::TableMeta {
        id: table_id,
        level: 0,
        size: std::fs::metadata(&table_path)?.len(),
        entry_count,
        min_key,
        max_key,
    };

    // Update manifest (I/O - no locks held)
    {
        #[allow(clippy::readonly_write_lock)] // next_seq() mutates the header
        let manifest = state.manifest.write().unwrap();
        let seq = manifest.next_seq();
        manifest.append(super::manifest::edit::VersionEdit::Flush {
            seq,
            table: table_meta.clone(),
            wal_id,
        })?;
        manifest.sync()?;
    }

    // Add to level 0
    {
        let mut levels = state.levels.write().unwrap();
        if levels.is_empty() {
            levels.push(Level::new(0));
        }
        let sstable = SSTable::new(table_path, table, &table_meta)?;
        levels[0].add_sstable(sstable)
    }

    // Delete WAL file (optional cleanup)
    let wal_path = store.wal_path(wal_id);
    if let Err(e) = std::fs::remove_file(&wal_path) {
        tracing::warn!(wal_id = wal_id, error = %e, "Failed to delete WAL file");
    }

    tracing::info!(
        table_id = table_id,
        wal_id = wal_id,
        "Manually flushed memtable to SSTable"
    );

    // Mark flush as completed
    store.state.mark_flush_completed();

    // Validate state consistency after flush
    if let Err(e) = store.state.validate_consistency() {
        tracing::warn!("State inconsistency detected after flush: {:?}", e);
    }

    // Debug-only comprehensive validation
    #[cfg(debug_assertions)]
    {
        if let Err(e) = store.state.validate_sstable_id_uniqueness() {
            tracing::error!("SSTable ID uniqueness violation after flush: {:?}", e);
        }

        if let Err(e) = store.state.validate_level_key_ordering() {
            tracing::error!("Level key ordering violation after flush: {:?}", e);
        }
    }

    Ok(true)
}

#[cfg(test)]
mod tests {
    use crate::{
        config::{CompactionConfig, LsmConfig},
        error::Result,
        store::Store,
        tmpfs::TempDir,
    };

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
}
