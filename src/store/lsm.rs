use super::{
    level::{Level, SSTable},
    state::LsmState,
    Store,
};

use crate::{
    config::LsmConfig,
    error::Result,
    flock::FileLock,
    manifest::{Manifest, ManifestState},
    memtable::ActiveMemtable,
    sstable::table::Table,
    wal::recovery::recover_memtables,
};

use std::{cmp::Ordering, collections::BinaryHeap, fs, ops::RangeBounds, path::Path, sync::Arc};

/// Type alias for complex iterator used in merge operations
type KvIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>;

const LOCK_FILE: &str = "ashdb.lock";
const MANIFEST_FILE: &str = "manifest.log";

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
        let state = Arc::new(Self::recover_state(&config)?);

        // Create store instance (no scheduler, no background tasks)
        Ok(Self {
            config,
            lock: Some(lock),
            state,
        })
    }

    /// Recover state from manifest and WAL files
    pub(crate) fn recover_state(config: &LsmConfig) -> Result<LsmState> {
        let dir = &config.dir;

        // Open or create manifest
        let manifest_path = dir.join(MANIFEST_FILE);
        let manifest = Manifest::new(&manifest_path)?;

        // Recover levels from manifest
        let manifest_state = manifest.replay()?;
        let levels = Self::levels_from_manifest_state(dir, &manifest_state)?;

        // Recover memtables from WAL
        let (active_memtable, frozen_memtables, next_wal_id) = recover_memtables(dir)?;

        Ok(LsmState::new(
            active_memtable,
            frozen_memtables,
            levels,
            manifest,
            manifest_state.next_table_id,
            next_wal_id,
        ))
    }

    /// Convert manifest state to levels
    fn levels_from_manifest_state(dir: &Path, state: &ManifestState) -> Result<Vec<Level>> {
        let mut levels = Vec::new();

        for level_meta in &state.levels {
            while levels.len() <= level_meta.level as usize {
                levels.push(Level::new(levels.len() as u32));
            }

            for table_meta in &level_meta.tables {
                let path = dir.join(format!("{}.sst", table_meta.id));
                let table = Table::readable(path.to_str().unwrap())?;
                levels[level_meta.level as usize].add_sstable(SSTable {
                    id: table_meta.id,
                    table,
                    path,
                    size: table_meta.size,
                    min_key: table_meta.min_key.clone(),
                    max_key: table_meta.max_key.clone(),
                });
            }
        }

        Ok(levels)
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
        // Don't run if already running
        if !self.state.needs_compaction() {
            return false;
        }

        // Use the actual compaction decision logic
        self.find_compaction_level().is_some()
    }

    /// Manually flush oldest frozen memtable to SSTable (for testing)
    pub async fn flush_memtable(&self) -> Result<bool> {
        // Get oldest frozen memtable
        let memtable = {
            let mut frozen = self.state.frozen_memtables.write().unwrap();
            match frozen.pop_front() {
                Some(m) => m,
                None => return Ok(false), // Nothing to flush
            }
        };

        let wal_id = memtable.wal_id();

        // Create SSTable (I/O - no locks held)
        let table_id = self.state.next_sstable_id();
        let table_path = self.config.dir.join(format!("{}.sst", table_id));
        let mut sstable = crate::sstable::table::Table::writable(table_path.to_str().unwrap())?;

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

        let table_meta = crate::manifest::meta::TableMeta {
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
            let manifest = self.state.manifest.write().unwrap();
            let seq = manifest.next_seq();
            manifest.append(crate::manifest::edit::VersionEdit::Flush {
                seq,
                table: table_meta.clone(),
                wal_id,
            })?;
            manifest.sync()?;
        }

        // Add to level 0
        {
            let mut levels = self.state.levels.write().unwrap();
            if levels.is_empty() {
                levels.push(crate::store::level::Level::new(0));
            }
            levels[0].add_sstable(crate::store::level::SSTable {
                id: table_id,
                table,
                path: table_path,
                size: table_meta.size,
                min_key: table_meta.min_key,
                max_key: table_meta.max_key,
            });
        }

        // Delete WAL file (optional cleanup)
        let wal_path = self.config.dir.join("wal").join(format!("{}.wal", wal_id));
        if let Err(e) = std::fs::remove_file(&wal_path) {
            tracing::warn!(wal_id = wal_id, error = %e, "Failed to delete WAL file");
        }

        tracing::info!(
            table_id = table_id,
            wal_id = wal_id,
            "Manually flushed memtable to SSTable"
        );

        Ok(true)
    }

    /// Check which level needs compaction in tiered strategy
    fn find_compaction_level(&self) -> Option<u32> {
        let levels = self.state.levels.read().unwrap();

        // First priority: L0 compaction
        if !levels.is_empty()
            && levels[0].table_count() > self.config.scheduler.level0_compaction_threshold
        {
            return Some(0);
        }

        // Check higher levels for size-ratio based compaction
        for level_num in 1..levels.len() {
            let current_level = &levels[level_num];

            // Skip if level has fewer than max_tables_per_level tables
            if current_level.table_count() < self.config.scheduler.max_tables_per_level {
                continue;
            }

            // Check size ratio against next level
            if level_num + 1 < levels.len() {
                let next_level = &levels[level_num + 1];
                let current_size = current_level.size();
                let next_size = next_level.size().max(1); // Avoid division by zero
                let size_ratio = current_size / next_size;

                if size_ratio >= self.config.scheduler.size_ratio_threshold as u64 {
                    return Some(level_num as u32);
                }
            } else {
                // If next level doesn't exist, compact when we have too many tables
                return Some(level_num as u32);
            }
        }

        None
    }

    /// Get list of deletable WAL file IDs from manifest
    pub fn deletable_wals(&self) -> Result<Vec<u64>> {
        let manifest = self.state.manifest.read().unwrap();
        let state = manifest.replay()?;
        Ok(state.deletable_wals)
    }

    /// Clean up WAL files that are no longer needed
    pub async fn cleanup_wals(&self) -> Result<()> {
        let deletable = self.deletable_wals()?;

        if deletable.is_empty() {
            return Ok(()); // No WALs to clean up
        }

        tracing::debug!(
            deletable_wals = ?deletable,
            "Found {} WAL files to clean up",
            deletable.len()
        );

        for wal_id in deletable {
            let wal_path = self.config.dir.join("wal").join(format!("{}.wal", wal_id));

            match std::fs::remove_file(&wal_path) {
                Ok(_) => {
                    tracing::info!(wal_id = wal_id, "Deleted WAL file");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already deleted, ignore
                }
                Err(e) => {
                    tracing::warn!(
                        wal_id = wal_id,
                        error = %e,
                        "Failed to delete WAL file"
                    );
                }
            }
        }

        Ok(())
    }

    /// Perform tiered compaction if needed
    pub async fn compact(&self) -> Result<()> {
        let _guard = self.state.start_compaction();

        // Find which level needs compaction
        let source_level = match self.find_compaction_level() {
            Some(level) => level,
            None => return Ok(()), // No compaction needed
        };

        let target_level = source_level + 1;

        // Get source tables to compact
        let source_tables = {
            let levels = self.state.levels.read().unwrap();
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
            let levels = self.state.levels.read().unwrap();
            for table_meta in &source_tables {
                let sstable = levels[source_level as usize]
                    .sstables
                    .iter()
                    .find(|s| s.id == table_meta.id)
                    .ok_or_else(|| {
                        crate::Error::InvalidOperation("SSTable not found".to_string())
                    })?;

                let scan_iter = sstable.table.scan(..)?;
                let boxed_iter: KvIterator = Box::new(scan_iter);
                iterators.push(boxed_iter);
            }
        }

        // 2. Create merge iterator and write to new SSTable
        let merge_iter = MergeIterator::new(iterators);

        let table_id = self.state.next_sstable_id();
        let table_path = self.config.dir.join(format!("{}.sst", table_id));
        let mut writable_table =
            crate::sstable::table::Table::writable(table_path.to_str().unwrap())?;

        let mut builder = crate::sstable::block::Builder::new();
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
            if builder.len() >= crate::sstable::table::MAX_BLOCK_SIZE {
                let block_data = builder.finish();
                let first_key = first_key_in_block.take().unwrap();
                writable_table.add_block(&block_data, first_key)?;
                builder = crate::sstable::block::Builder::new();
            }
        }

        // Write final block if not empty
        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block.take().unwrap();
            writable_table.add_block(&block_data, first_key)?;
        }

        let finalized_table = writable_table.finalize()?;

        // 3. Create table metadata
        let table_meta = crate::manifest::meta::TableMeta {
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
            let manifest = self.state.manifest.write().unwrap();
            let begin_seq = manifest.next_seq();
            manifest.append(crate::manifest::edit::VersionEdit::BeginCompaction {
                seq: begin_seq,
                job_id,
                source_level,
                target_level,
            })?;

            let commit_seq = manifest.next_seq();
            manifest.append(crate::manifest::edit::VersionEdit::CommitCompaction {
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
            let mut levels = self.state.levels.write().unwrap();

            // Remove source tables from source level
            if (source_level as usize) < levels.len() {
                levels[source_level as usize]
                    .sstables
                    .retain(|t| !source_table_ids.contains(&t.id));
            }

            // Ensure target level exists
            while levels.len() <= target_level as usize {
                let level_num = levels.len() as u32;
                levels.push(crate::store::level::Level::new(level_num));
            }

            // Add new table to target level
            let readable_table = match finalized_table {
                crate::sstable::table::Table::Readable(r) => r,
                _ => {
                    return Err(crate::Error::InvalidOperation(
                        "Expected readable table".to_string(),
                    ))
                }
            };

            levels[target_level as usize].add_sstable(crate::store::level::SSTable {
                id: table_id,
                table: crate::sstable::table::Table::Readable(readable_table),
                path: table_path,
                size: table_meta.size,
                min_key: table_meta.min_key.clone(),
                max_key: table_meta.max_key.clone(),
            });
        }

        // 6. Delete old SSTable files
        for table_id in &source_table_ids {
            let old_path = self.config.dir.join(format!("{}.sst", table_id));
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

        Ok(())
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

        let frozen_arcs: Vec<Arc<crate::memtable::Memtable>> = {
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

// Iterator implementations (same as original)
struct OwningMemtableIter {
    _memtable: Arc<crate::memtable::Memtable>,
    iter: crate::memtable::ScanIter<'static>,
}

impl OwningMemtableIter {
    fn new(memtable: Arc<crate::memtable::Memtable>, range: impl RangeBounds<Vec<u8>>) -> Self {
        let iter = unsafe {
            std::mem::transmute::<crate::memtable::ScanIter<'_>, crate::memtable::ScanIter<'static>>(
                memtable.scan(range).unwrap(),
            )
        };

        Self {
            _memtable: memtable,
            iter,
        }
    }
}

impl Iterator for OwningMemtableIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

struct HeapEntry<'a> {
    key: Vec<u8>,
    value: Vec<u8>,
    source: usize,
    iterator: KvIterator<'a>,
}

impl std::fmt::Debug for HeapEntry<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapEntry")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("source", &self.source)
            .finish()
    }
}

impl PartialEq for HeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapEntry<'_> {}

impl PartialOrd for HeapEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => other.source.cmp(&self.source),
            other => other.reverse(),
        }
    }
}

#[derive(Debug)]
pub struct MergeIterator<'a> {
    heap: BinaryHeap<HeapEntry<'a>>,
    latest_key: Option<Vec<u8>>,
}

impl<'a> MergeIterator<'a> {
    pub fn new(iterators: Vec<KvIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (source, mut iterator) in iterators.into_iter().enumerate() {
            if let Some(Ok((key, value))) = iterator.next() {
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
            latest_key: None,
        }
    }
}

impl Iterator for MergeIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut entry) = self.heap.pop() {
            if self.latest_key.as_ref() == Some(&entry.key) {
                if let Some(Ok((key, value))) = entry.iterator.next() {
                    self.heap.push(HeapEntry {
                        key,
                        value,
                        source: entry.source,
                        iterator: entry.iterator,
                    });
                }
                continue;
            }

            self.latest_key = Some(entry.key.clone());

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

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{LsmConfig, SchedulerConfig},
        store::Store,
        tmpfs::TempDir,
    };
    use std::time::Duration;

    // Helper function to create a test store with custom config
    fn create_test_store_with_config(temp_dir: &TempDir, config: SchedulerConfig) -> LsmStore {
        let lsm_config = LsmConfig::new(temp_dir.path()).scheduler(config);
        LsmStore::open_with_config(lsm_config).expect("Failed to create store")
    }

    // Helper function to create a test store with default aggressive compaction settings
    fn create_test_store(temp_dir: &TempDir) -> LsmStore {
        let scheduler_config = SchedulerConfig::default()
            .level0_compaction_threshold(2) // Lower threshold for easier testing
            .size_ratio_threshold(2) // Lower ratio for easier testing
            .max_tables_per_level(3); // Lower max for easier testing

        create_test_store_with_config(temp_dir, scheduler_config)
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
                levels[0].table_count() <= store.config.scheduler.level0_compaction_threshold,
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
        let aggressive_config = SchedulerConfig::default()
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
