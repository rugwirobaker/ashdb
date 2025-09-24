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
    scheduler::Scheduler,
    sstable::table::Table,
    wal::recovery::recover_memtables,
};

use std::{cmp::Ordering, collections::BinaryHeap, fs, ops::RangeBounds, path::Path, sync::Arc};

/// Type alias for complex iterator used in merge operations
type KvIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a>;

const LOCK_FILE: &str = "ashdb.lock";
const MANIFEST_FILE: &str = "manifest.log";

/// LSM Store with background scheduler and interior mutability
pub struct LsmStore {
    // Immutable configuration (public for background tasks)
    pub(crate) config: LsmConfig,
    lock: Option<FileLock>,

    // Shared mutable state (public for background tasks)
    pub(crate) state: Arc<LsmState>,

    // Background task scheduler
    scheduler: Scheduler,
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

        // Create scheduler
        let scheduler = Scheduler::new();

        // Create store instance
        let store = Arc::new(Self {
            config,
            lock: Some(lock),
            state,
            scheduler,
        });

        // Register background tasks
        {
            use crate::store::{
                compaction::CompactionTask, flush::FlushTask, metrics::MetricsTask,
                wal_cleanup::WalCleanupTask,
            };

            store
                .scheduler
                .register(Arc::new(FlushTask::new(store.clone())))
                .register(Arc::new(CompactionTask::new(store.clone())))
                .register(Arc::new(WalCleanupTask::new(store.clone())))
                .register(Arc::new(MetricsTask::new(store.clone())));
        }

        // Return the store (extract from Arc since we need to move it)
        match Arc::try_unwrap(store) {
            Ok(store) => Ok(store),
            Err(_) => panic!("Failed to unwrap store Arc"),
        }
    }

    /// Recover state from manifest and WAL files
    fn recover_state(config: &LsmConfig) -> Result<LsmState> {
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
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        // Graceful shutdown of scheduler
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            // Move scheduler out to own it
            let scheduler = std::mem::take(&mut self.scheduler);
            scheduler.shutdown().await.ok();
        });

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

        // Create owning iterators
        let active_iter = OwningMemtableIter::new(active_arc, range.clone());
        let frozen_iters: Vec<OwningMemtableIter> = frozen_arcs
            .into_iter()
            .map(|arc| OwningMemtableIter::new(arc, range.clone()))
            .collect();

        let mut iterators: Vec<BoxedResultIterator> = vec![Box::new(active_iter)];
        for iter in frozen_iters {
            iterators.push(Box::new(iter));
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
