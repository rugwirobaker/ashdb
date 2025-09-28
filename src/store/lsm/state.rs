use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use super::manifest::{Level, Manifest};
use super::memtable::{ActiveMemtable, FrozenMemtable};
use crate::error::Result;

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

    // ===== STATE VALIDATION METHODS =====

    /// Validate consistency between manifest and in-memory state
    pub fn validate_consistency(&self) -> crate::error::Result<()> {
        // Replay manifest to get expected state
        let manifest_state = {
            let manifest = self.manifest.read().unwrap();
            manifest.replay()?
        };

        let in_memory_levels = self.levels.read().unwrap();

        // Validate level count consistency
        if manifest_state.levels.len() != in_memory_levels.len() {
            return Err(crate::Error::InvalidData(format!(
                "Level count mismatch: manifest has {}, memory has {}",
                manifest_state.levels.len(),
                in_memory_levels.len()
            )));
        }

        // Validate table count per level
        for (level_idx, level_meta) in manifest_state.levels.iter().enumerate() {
            if level_idx < in_memory_levels.len() {
                let memory_table_count = in_memory_levels[level_idx].table_count();
                if level_meta.tables.len() != memory_table_count {
                    return Err(crate::Error::InvalidData(format!(
                        "Table count mismatch at level {}: manifest has {}, memory has {}",
                        level_idx,
                        level_meta.tables.len(),
                        memory_table_count
                    )));
                }
            }
        }

        // Validate next table ID consistency
        let current_max_id = in_memory_levels
            .iter()
            .flat_map(|level| &level.sstables)
            .map(|table| table.id)
            .max()
            .unwrap_or(0);

        // Next table ID should be greater than current max, or equal if no tables exist
        if !in_memory_levels.is_empty() && manifest_state.next_table_id <= current_max_id {
            return Err(crate::Error::InvalidData(format!(
                "Next table ID inconsistency: manifest has {}, but max existing ID is {}",
                manifest_state.next_table_id, current_max_id
            )));
        }

        Ok(())
    }

    /// Check if the state is in a consistent state for operations
    pub fn is_operation_safe(&self) -> bool {
        // Check if any critical operations are in progress
        let compaction_count = self.compaction_running.load(Ordering::SeqCst);
        let freeze_in_progress = self.freeze_in_progress.load(Ordering::SeqCst);
        let flush_pending = self.flush_pending.load(Ordering::SeqCst);

        // State is safe if no conflicting operations are running
        compaction_count == 0 && !freeze_in_progress && !flush_pending
    }

    /// Get a snapshot of current state metrics for debugging
    pub fn get_state_metrics(&self) -> StateMetrics {
        let levels = self.levels.read().unwrap();
        let frozen_memtables = self.frozen_memtables.read().unwrap();
        let active_memtable = self.active_memtable.read().unwrap();

        StateMetrics {
            active_memtable_size: active_memtable.size(),
            frozen_memtable_count: frozen_memtables.len(),
            level_count: levels.len(),
            total_sstable_count: levels.iter().map(|l| l.table_count()).sum(),
            level_sizes: levels.iter().map(|l| l.size()).collect(),
            next_sstable_id: self.next_sstable_id.load(Ordering::SeqCst),
            next_wal_id: self.next_wal_id.load(Ordering::SeqCst),
            compaction_running: self.compaction_running.load(Ordering::SeqCst),
            freeze_in_progress: self.freeze_in_progress.load(Ordering::SeqCst),
            flush_pending: self.flush_pending.load(Ordering::SeqCst),
        }
    }

    /// Validate that all SSTable IDs are unique across levels
    pub fn validate_sstable_id_uniqueness(&self) -> crate::error::Result<()> {
        let levels = self.levels.read().unwrap();
        let mut seen_ids = std::collections::HashSet::new();

        for (level_idx, level) in levels.iter().enumerate() {
            for sstable in &level.sstables {
                if !seen_ids.insert(sstable.id) {
                    return Err(crate::Error::InvalidData(format!(
                        "Duplicate SSTable ID {} found at level {}",
                        sstable.id, level_idx
                    )));
                }
            }
        }

        Ok(())
    }

    /// Validate that keys within each level are properly sorted
    pub fn validate_level_key_ordering(&self) -> crate::error::Result<()> {
        let levels = self.levels.read().unwrap();

        for (level_idx, level) in levels.iter().enumerate() {
            let mut last_max_key: Option<&Vec<u8>> = None;

            for sstable in &level.sstables {
                // Within each SSTable, min_key should be <= max_key
                if sstable.min_key > sstable.max_key {
                    return Err(crate::Error::InvalidData(format!(
                        "SSTable {} at level {} has min_key > max_key",
                        sstable.id, level_idx
                    )));
                }

                // For Level 0, tables can overlap so we don't check ordering
                if level_idx > 0 {
                    // For levels > 0, tables should not overlap
                    if let Some(last_max) = last_max_key {
                        if sstable.min_key <= *last_max {
                            return Err(crate::Error::InvalidData(format!(
                                "SSTable {} at level {} overlaps with previous table",
                                sstable.id, level_idx
                            )));
                        }
                    }
                }

                last_max_key = Some(&sstable.max_key);
            }
        }

        Ok(())
    }

    /// Get current health metrics for monitoring and debugging
    pub fn get_health_metrics(&self) -> StateMetrics {
        self.get_state_metrics()
    }

    /// Comprehensive validation for debug builds and testing
    #[cfg(debug_assertions)]
    pub fn validate_comprehensive(&self) -> Result<()> {
        tracing::debug!("Starting comprehensive validation");

        // Basic consistency validation
        self.validate_consistency()?;

        // SSTable ID uniqueness validation
        self.validate_sstable_id_uniqueness()?;

        // Level key ordering validation
        self.validate_level_key_ordering()?;

        tracing::debug!("Comprehensive validation passed");
        Ok(())
    }

    /// Run periodic health check with validation (renamed from health_check)
    pub fn status(&self) -> Result<StateMetrics> {
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
        if let Err(e) = self.validate_consistency() {
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

/// Metrics snapshot for debugging and monitoring state
#[derive(Debug, Clone)]
pub struct StateMetrics {
    pub active_memtable_size: usize,
    pub frozen_memtable_count: usize,
    pub level_count: usize,
    pub total_sstable_count: usize,
    pub level_sizes: Vec<u64>,
    pub next_sstable_id: u64,
    pub next_wal_id: u64,
    pub compaction_running: usize,
    pub freeze_in_progress: bool,
    pub flush_pending: bool,
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
