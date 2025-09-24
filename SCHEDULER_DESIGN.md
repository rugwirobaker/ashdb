# Scheduler Design Document

## Current Problems

1. **No Background Tasks** - Flush and compaction are manual/blocking
2. **Exclusive Access** - `&mut self` methods prevent concurrent operations
3. **No Interior Mutability** - Can't share state with background tasks
4. **Blocking Writes** - Memtable freeze blocks all writes
5. **No Concurrency** - Single-threaded operation limits throughput
6. **Manual Management** - User must trigger flush/compaction

## Design Goals

1. **Async Background Tasks** - Flush, compaction, cleanup run automatically
2. **Concurrent Access** - Reads/writes don't block each other or background tasks
3. **Interior Mutability** - `&self` methods with `Arc<RwLock<T>>` for shared state
4. **Simple API** - Register periodic tasks, automatic execution
5. **Graceful Shutdown** - Wait for tasks to complete on drop
6. **Pluggable Tasks** - Easy to add new background work

## Architecture

### State Refactoring

Current `LsmStore` uses `&mut self` - incompatible with concurrent access:

```rust
// Current - BLOCKS everything
pub struct LsmStore {
    active_memtable: Arc<Memtable>,  // Shared but not thread-safe writes
    frozen_memtables: VecDeque<Arc<Memtable>>,  // No interior mutability
    levels: Vec<Level>,  // Not shareable
    manifest: Manifest,  // Not shareable
    // ...
}

impl LsmStore {
    pub fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> { ... }
    //        ^^^ Exclusive access required
}
```

**New Design** - Separate immutable config from mutable state:

```rust
// Immutable store handle
pub struct LsmStore {
    dir: PathBuf,
    lock: FileLock,
    state: Arc<LsmState>,  // Shared state
    scheduler: Scheduler,
}

// All mutable state behind RwLocks
pub struct LsmState {
    // Write path (exclusive access needed)
    active_memtable: RwLock<Arc<Memtable>>,
    frozen_memtables: RwLock<VecDeque<Arc<Memtable>>>,

    // Read path (concurrent reads)
    levels: RwLock<Vec<Level>>,

    // Metadata
    manifest: RwLock<Manifest>,
    next_sstable_id: AtomicU64,
    next_wal_id: AtomicU64,

    // Metrics/flags
    flush_pending: AtomicBool,
    compaction_running: AtomicUsize,
}

impl LsmStore {
    // Now uses &self with interior mutability
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        //        ^^^^^ Shared access
        let active = self.state.active_memtable.read().unwrap();
        active.wal().append(key, Some(&value))?;
        active.put(key.to_vec(), Some(value))?;

        // Check freeze threshold
        if active.size() >= MAX_MEMTABLE_SIZE {
            drop(active);
            self.freeze_active_memtable()?;
        }
        Ok(())
    }
}
```

### Lock Granularity

**Fine-grained locks** - one per component:

1. `active_memtable: RwLock` - Many concurrent reads, rare writes (freeze)
2. `frozen_memtables: RwLock` - Reads during scan, writes on freeze/flush
3. `levels: RwLock` - Many reads (get/scan), rare writes (flush/compact)
4. `manifest: RwLock` - Rare reads (recovery), frequent writes (flush/compact)

**Lock Ordering** (prevent deadlocks):
```
Always acquire in this order:
1. active_memtable
2. frozen_memtables
3. levels
4. manifest
```

**Never hold multiple locks** across I/O operations.

### Scheduler Implementation

```rust
// src/scheduler/mod.rs

use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use std::sync::RwLock;

pub struct Scheduler {
    tasks: RwLock<Vec<JoinHandle<()>>>,
    shutdown_tx: broadcast::Sender<()>,
}

#[async_trait]
pub trait BackgroundTask: Send + Sync {
    /// Task name for logging
    fn name(&self) -> &'static str;

    /// How often to run this task
    fn interval(&self) -> Duration;

    /// Execute the task
    async fn execute(&self, ctx: TaskContext) -> Result<()>;
}

pub struct TaskContext {
    pub task_name: &'static str,
    pub run_id: u64,
    pub shutdown: broadcast::Receiver<()>,
}

impl Scheduler {
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            tasks: RwLock::new(Vec::new()),
            shutdown_tx,
        }
    }

    /// Register a periodic background task
    pub fn register<T: BackgroundTask + 'static>(
        &self,
        task: Arc<T>,
    ) -> &Self {
        let handle = self.spawn_timer_loop(task);
        self.tasks.write().unwrap().push(handle);
        self
    }

    fn spawn_timer_loop<T: BackgroundTask + 'static>(
        &self,
        task: Arc<T>,
    ) -> JoinHandle<()> {
        let interval = task.interval();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut run_id = 0u64;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        run_id += 1;
                        let ctx = TaskContext {
                            task_name: task.name(),
                            run_id,
                            shutdown: shutdown_rx.resubscribe(),
                        };

                        if let Err(e) = task.execute(ctx).await {
                            tracing::error!(
                                task = task.name(),
                                error = %e,
                                "Task execution failed"
                            );
                        }
                    }

                    _ = shutdown_rx.recv() => {
                        tracing::info!(task = task.name(), "Task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Spawn one-off task
    pub fn spawn<F>(&self, f: F)
    where
        F: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(e) = f.await {
                tracing::error!(error = %e, "One-off task failed");
            }
        });
    }

    /// Graceful shutdown - wait for all tasks
    pub async fn shutdown(self) -> Result<()> {
        // Signal all tasks to stop
        self.shutdown_tx.send(()).ok();

        // Wait for all tasks to complete
        for task in self.tasks.write().unwrap().drain(..) {
            task.await?;
        }

        Ok(())
    }
}
```

## Background Tasks

### Task 1: Memtable Flush

```rust
// src/scheduler/tasks/flush.rs

pub struct FlushTask {
    state: Arc<LsmState>,
    dir: PathBuf,
}

impl FlushTask {
    pub fn new(state: Arc<LsmState>, dir: PathBuf) -> Self {
        Self { state, dir }
    }

    async fn flush_oldest(&self) -> Result<()> {
        // Get oldest frozen memtable
        let memtable = {
            let mut frozen = self.state.frozen_memtables.write().unwrap();
            match frozen.pop_front() {
                Some(m) => m,
                None => return Ok(()), // Nothing to flush
            }
        };

        let wal_id = memtable.wal().id()?;

        // Create SSTable (I/O - no locks held)
        let table_id = self.state.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let table_path = self.dir.join(format!("{}.sst", table_id));
        let mut sstable = Table::writable(&table_path)?;

        memtable.flush(&mut sstable)?;
        let table = sstable.finalize()?;

        let table_meta = TableMeta {
            id: table_id,
            level: 0,
            size: table.size(),
            entry_count: table.entry_count(),
            min_key: table.min_key().to_vec(),
            max_key: table.max_key().to_vec(),
        };

        // Update manifest (I/O - no locks held)
        {
            let mut manifest = self.state.manifest.write().unwrap();
            let seq = manifest.next_seq();
            manifest.append(VersionEdit::Flush {
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
                levels.push(Level::new(0));
            }
            levels[0].add_table(table_meta);
        }

        // Mark flush complete
        self.state.flush_pending.store(false, Ordering::SeqCst);

        tracing::info!(
            table_id = table_id,
            wal_id = wal_id,
            "Flushed memtable to SSTable"
        );

        Ok(())
    }
}

#[async_trait]
impl BackgroundTask for FlushTask {
    fn name(&self) -> &'static str {
        "memtable-flush"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(1) // Check every second
    }

    async fn execute(&self, ctx: TaskContext) -> Result<()> {
        // Check if flush needed
        let frozen_count = self.state.frozen_memtables.read().unwrap().len();

        if frozen_count == 0 {
            return Ok(());
        }

        // Set flush pending flag
        if self.state.flush_pending.swap(true, Ordering::SeqCst) {
            // Another flush already running
            return Ok(());
        }

        // Flush oldest memtable
        self.flush_oldest().await?;

        Ok(())
    }
}
```

### Task 2: Compaction

```rust
// src/scheduler/tasks/compaction.rs

pub struct CompactionTask {
    state: Arc<LsmState>,
    dir: PathBuf,
}

impl CompactionTask {
    pub fn new(state: Arc<LsmState>, dir: PathBuf) -> Self {
        Self { state, dir }
    }

    fn pick_compaction(&self) -> Result<Option<CompactionJob>> {
        let levels = self.state.levels.read().unwrap();

        // Simple heuristic: compact level 0 if it has > 4 tables
        if levels.get(0).map_or(0, |l| l.table_count()) > 4 {
            let source_tables = levels[0].all_tables();
            let target_tables = if levels.len() > 1 {
                levels[1].overlapping_tables(&source_tables)
            } else {
                vec![]
            };

            return Ok(Some(CompactionJob {
                source_level: 0,
                source_tables,
                target_level: 1,
                target_tables,
            }));
        }

        // TODO: Add more sophisticated compaction picking
        Ok(None)
    }

    async fn compact(&self, job: CompactionJob) -> Result<()> {
        // Mark compaction running
        self.state.compaction_running.fetch_add(1, Ordering::SeqCst);

        // Merge tables (I/O - no locks held)
        let new_tables = self.merge_tables(&job).await?;

        // Update manifest
        {
            let mut manifest = self.state.manifest.write().unwrap();
            let seq = manifest.next_seq();
            manifest.append(VersionEdit::Compaction {
                seq,
                source_level: job.source_level,
                deleted_tables: job.all_table_ids(),
                target_level: job.target_level,
                added_tables: new_tables.clone(),
            })?;
            manifest.sync()?;
        }

        // Update levels
        {
            let mut levels = self.state.levels.write().unwrap();

            // Ensure target level exists
            while levels.len() <= job.target_level as usize {
                levels.push(Level::new(levels.len() as u32));
            }

            // Remove old tables
            for table_id in job.source_table_ids() {
                levels[job.source_level as usize].remove_table(table_id);
            }
            for table_id in job.target_table_ids() {
                levels[job.target_level as usize].remove_table(table_id);
            }

            // Add new tables
            for table in new_tables {
                levels[job.target_level as usize].add_table(table);
            }
        }

        // Delete old SSTable files
        for table_id in job.all_table_ids() {
            let path = self.dir.join(format!("{}.sst", table_id));
            std::fs::remove_file(path).ok();
        }

        self.state.compaction_running.fetch_sub(1, Ordering::SeqCst);

        tracing::info!(
            source_level = job.source_level,
            target_level = job.target_level,
            input_tables = job.all_table_ids().len(),
            output_tables = new_tables.len(),
            "Compaction completed"
        );

        Ok(())
    }

    async fn merge_tables(&self, job: &CompactionJob) -> Result<Vec<TableMeta>> {
        // TODO: Implement table merging logic
        // 1. Create merge iterator over all input tables
        // 2. Write to new SSTables
        // 3. Return metadata for new tables
        todo!()
    }
}

#[async_trait]
impl BackgroundTask for CompactionTask {
    fn name(&self) -> &'static str {
        "compaction"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(10)
    }

    async fn execute(&self, ctx: TaskContext) -> Result<()> {
        // Don't run if already compacting
        if self.state.compaction_running.load(Ordering::SeqCst) > 0 {
            return Ok(());
        }

        // Pick compaction job
        let job = match self.pick_compaction()? {
            Some(j) => j,
            None => return Ok(()), // Nothing to compact
        };

        // Run compaction
        self.compact(job).await?;

        Ok(())
    }
}

struct CompactionJob {
    source_level: u32,
    source_tables: Vec<TableMeta>,
    target_level: u32,
    target_tables: Vec<TableMeta>,
}
```

### Task 3: WAL Cleanup

```rust
// src/scheduler/tasks/wal_cleanup.rs

pub struct WalCleanupTask {
    state: Arc<LsmState>,
    dir: PathBuf,
}

impl WalCleanupTask {
    pub fn new(state: Arc<LsmState>, dir: PathBuf) -> Self {
        Self { state, dir }
    }

    fn deletable_wals(&self) -> Result<Vec<u64>> {
        let manifest = self.state.manifest.read().unwrap();

        // Collect WAL IDs marked as deletable in manifest
        let mut deletable = Vec::new();
        for edit in manifest.iter()? {
            match edit? {
                VersionEdit::Flush { wal_id, .. } => {
                    deletable.push(wal_id);
                }
                VersionEdit::Snapshot { deletable_wals, .. } => {
                    // Snapshot tells us which WALs can be deleted
                    return Ok(deletable_wals);
                }
                _ => {}
            }
        }

        Ok(deletable)
    }
}

#[async_trait]
impl BackgroundTask for WalCleanupTask {
    fn name(&self) -> &'static str {
        "wal-cleanup"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(30)
    }

    async fn execute(&self, ctx: TaskContext) -> Result<()> {
        let deletable = self.deletable_wals()?;

        for wal_id in deletable {
            let wal_path = self.dir.join("wal").join(format!("{}.wal", wal_id));

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
}
```

### Task 4: Metrics Collection

```rust
// src/scheduler/tasks/metrics.rs

pub struct MetricsTask {
    state: Arc<LsmState>,
}

impl MetricsTask {
    pub fn new(state: Arc<LsmState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl BackgroundTask for MetricsTask {
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn interval(&self) -> Duration {
        Duration::from_secs(5)
    }

    async fn execute(&self, ctx: TaskContext) -> Result<()> {
        // Collect metrics
        let active_size = self.state.active_memtable.read().unwrap().size();
        let frozen_count = self.state.frozen_memtables.read().unwrap().len();
        let flush_pending = self.state.flush_pending.load(Ordering::SeqCst);
        let compaction_running = self.state.compaction_running.load(Ordering::SeqCst);

        let level_counts: Vec<_> = self.state.levels.read().unwrap()
            .iter()
            .enumerate()
            .map(|(i, l)| (i, l.table_count()))
            .collect();

        tracing::info!(
            active_memtable_size = active_size,
            frozen_memtables = frozen_count,
            flush_pending = flush_pending,
            compaction_running = compaction_running,
            ?level_counts,
            "LSM metrics"
        );

        Ok(())
    }
}
```

## Store Initialization

```rust
impl LsmStore {
    pub fn open(dir: &str) -> Result<Self> {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir)?;

        let lock = FileLock::lock(dir.join("ashdb.lock"))?;

        // Recover state
        let state = Arc::new(Self::recover(&dir)?);

        // Create scheduler
        let scheduler = Scheduler::new();

        // Register background tasks
        scheduler
            .register(Arc::new(FlushTask::new(
                state.clone(),
                dir.clone(),
            )))
            .register(Arc::new(CompactionTask::new(
                state.clone(),
                dir.clone(),
            )))
            .register(Arc::new(WalCleanupTask::new(
                state.clone(),
                dir.clone(),
            )))
            .register(Arc::new(MetricsTask::new(
                state.clone(),
            )));

        Ok(Self {
            dir,
            lock,
            state,
            scheduler,
        })
    }

    fn recover(dir: &Path) -> Result<LsmState> {
        // Recover from manifest
        let manifest_path = dir.join("manifest.log");
        let manifest = Manifest::new(&manifest_path)?;
        let recovered = Self::recover_from_manifest(dir, &manifest)?;

        // Recover memtables from WAL
        let (active, frozen, next_wal_id) = Self::recover_memtables(dir)?;

        Ok(LsmState {
            active_memtable: RwLock::new(active),
            frozen_memtables: RwLock::new(frozen),
            levels: RwLock::new(recovered.levels),
            manifest: RwLock::new(manifest),
            next_sstable_id: AtomicU64::new(recovered.next_table_id),
            next_wal_id: AtomicU64::new(next_wal_id),
            flush_pending: AtomicBool::new(false),
            compaction_running: AtomicUsize::new(0),
        })
    }
}

impl Drop for LsmStore {
    fn drop(&mut self) {
        // Graceful shutdown
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async {
                self.scheduler.shutdown().await.ok();
            });

        self.lock.unlock().ok();
    }
}
```

## Store Operations with Interior Mutability

```rust
impl LsmStore {
    /// Set key-value (concurrent-safe)
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let active = self.state.active_memtable.read().unwrap();
        active.wal().append(key, Some(&value))?;
        active.put(key.to_vec(), Some(value))?;

        // Check if freeze needed
        if active.size() >= MAX_MEMTABLE_SIZE {
            drop(active); // Release read lock
            self.freeze_active_memtable()?;
        }

        Ok(())
    }

    /// Get value (concurrent-safe)
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check active memtable
        if let Some(val) = self.state.active_memtable.read().unwrap().get(key) {
            return Ok(val);
        }

        // Check frozen memtables
        for memtable in self.state.frozen_memtables.read().unwrap().iter().rev() {
            if let Some(val) = memtable.get(key) {
                return Ok(val);
            }
        }

        // Check levels
        let levels = self.state.levels.read().unwrap();
        for level in levels.iter() {
            if let Some(val) = level.get(key)? {
                return Ok(Some(val));
            }
        }

        Ok(None)
    }

    /// Freeze active memtable (rare write operation)
    fn freeze_active_memtable(&self) -> Result<()> {
        // Acquire locks in order
        let mut active = self.state.active_memtable.write().unwrap();
        let mut frozen = self.state.frozen_memtables.write().unwrap();

        // Freeze current memtable
        active.freeze()?;

        // Move to frozen queue
        let old_active = active.clone();
        frozen.push_back(old_active);

        // Create new active memtable
        let wal_id = self.state.next_wal_id.fetch_add(1, Ordering::SeqCst);
        let wal_path = self.dir.join("wal").join(format!("{}.wal", wal_id));
        *active = Arc::new(Memtable::new(wal_path.to_str().unwrap())?);

        Ok(())
    }

    /// Explicit flush (for transactions)
    pub fn flush(&self) -> Result<()> {
        self.state.active_memtable.read().unwrap().wal().flush()
    }
}
```

## Benefits

1. **Automatic Background Work** - Flush, compaction, cleanup run automatically
2. **Concurrent Operations** - Reads/writes don't block each other
3. **Interior Mutability** - `&self` methods with fine-grained locks
4. **Simple API** - Register tasks, they run periodically
5. **Graceful Shutdown** - Wait for tasks on drop
6. **Pluggable** - Easy to add new tasks
7. **Lock-Free Reads** - Multiple concurrent readers
8. **Minimal Blocking** - Short critical sections

## Testing Strategy

1. **Concurrency Tests** - Concurrent reads/writes, verify correctness
2. **Deadlock Tests** - Stress test lock ordering
3. **Task Tests** - Mock each task, verify behavior
4. **Shutdown Tests** - Verify graceful termination
5. **Performance Tests** - Compare with single-threaded baseline

## Performance Considerations

- RwLock overhead: ~10-20ns for read lock
- Lock contention: rare (different components)
- Task frequency: tunable intervals
- Async runtime: tokio for efficient I/O

## Future Enhancements

1. **Priority Tasks** - High-priority compactions
2. **Adaptive Intervals** - Adjust based on workload
3. **Task Dependencies** - Flush before compaction
4. **Metrics Export** - Prometheus/statsd integration
5. **Manual Triggers** - Force flush/compaction via API