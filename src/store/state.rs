use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use crate::{
    manifest::Manifest,
    memtable::{ActiveMemtable, FrozenMemtable},
    store::level::Level,
};

/// All mutable state for the LSM store with fine-grained locking
pub struct LsmState {
    // Write path (exclusive access needed)
    pub active_memtable: RwLock<Arc<ActiveMemtable>>,
    pub frozen_memtables: RwLock<VecDeque<FrozenMemtable>>,

    // Read path (concurrent reads)
    pub levels: RwLock<Vec<Level>>,

    // Metadata
    pub manifest: RwLock<Manifest>,
    pub next_sstable_id: AtomicU64,
    pub next_wal_id: AtomicU64,

    // Coordination flags
    pub flush_pending: AtomicBool,
    pub compaction_running: AtomicUsize,
    pub freeze_in_progress: AtomicBool,
}

impl LsmState {
    pub fn new(
        active_memtable: ActiveMemtable,
        frozen_memtables: VecDeque<FrozenMemtable>,
        levels: Vec<Level>,
        manifest: Manifest,
        next_sstable_id: u64,
        next_wal_id: u64,
    ) -> Self {
        Self {
            active_memtable: RwLock::new(Arc::new(active_memtable)),
            frozen_memtables: RwLock::new(frozen_memtables),
            levels: RwLock::new(levels),
            manifest: RwLock::new(manifest),
            next_sstable_id: AtomicU64::new(next_sstable_id),
            next_wal_id: AtomicU64::new(next_wal_id),
            flush_pending: AtomicBool::new(false),
            compaction_running: AtomicUsize::new(0),
            freeze_in_progress: AtomicBool::new(false),
        }
    }

    /// Get next SSTable ID atomically
    pub fn next_sstable_id(&self) -> u64 {
        self.next_sstable_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Get next WAL ID atomically
    pub fn next_wal_id(&self) -> u64 {
        self.next_wal_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Check if flush is needed
    pub fn needs_flush(&self) -> bool {
        let frozen_count = self.frozen_memtables.read().unwrap().len();
        frozen_count > 0 && !self.flush_pending.load(Ordering::SeqCst)
    }

    /// Try to mark flush as pending (returns true if successfully marked)
    pub fn try_mark_flush_pending(&self) -> bool {
        !self.flush_pending.swap(true, Ordering::SeqCst)
    }

    /// Mark flush as completed
    pub fn mark_flush_completed(&self) {
        self.flush_pending.store(false, Ordering::SeqCst);
    }

    /// Check if compaction is needed - simple version for state coordination
    /// The actual compaction decision logic is in LsmStore::find_compaction_level()
    pub fn needs_compaction(&self) -> bool {
        // Don't run compaction if one is already running
        self.compaction_running.load(Ordering::SeqCst) == 0
    }

    /// Increment compaction counter
    pub fn start_compaction(&self) -> CompactionGuard {
        self.compaction_running.fetch_add(1, Ordering::SeqCst);
        CompactionGuard { state: self }
    }

    /// Check if freeze is needed based on active memtable size
    pub fn needs_freeze(&self, max_size: usize) -> bool {
        let active = self.active_memtable.read().unwrap();
        active.size() >= max_size
    }

    /// Try to start freeze operation (returns guard if successful)
    pub fn try_start_freeze(&self) -> Option<FreezeGuard> {
        if !self.freeze_in_progress.swap(true, Ordering::SeqCst) {
            Some(FreezeGuard { state: self })
        } else {
            None
        }
    }
}

/// RAII guard for compaction operations
pub struct CompactionGuard<'a> {
    state: &'a LsmState,
}

impl Drop for CompactionGuard<'_> {
    fn drop(&mut self) {
        self.state.compaction_running.fetch_sub(1, Ordering::SeqCst);
    }
}

/// RAII guard for freeze operations
pub struct FreezeGuard<'a> {
    state: &'a LsmState,
}

impl Drop for FreezeGuard<'_> {
    fn drop(&mut self) {
        self.state.freeze_in_progress.store(false, Ordering::SeqCst);
    }
}
