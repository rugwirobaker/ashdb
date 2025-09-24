# Memtable Design Document

## Current Problems

1. **WAL Ownership Prevents Safe Lifecycle Management** - WAL is owned directly by Memtable, cannot be deleted independently
2. **Non-Atomic Freeze Operation** - Multi-step freeze leaves intermediate states vulnerable to failure
3. **Scheduler Incompatibility** - `&mut self` methods prevent concurrent access needed by SCHEDULER_DESIGN.md
4. **Write Path Race Conditions** - Concurrent size checks can trigger duplicate freeze operations
5. **Memory Lifecycle Coupling** - Arc<Memtable> in frozen queue keeps WAL files alive indefinitely

## Design Goals

1. **Independent Lifecycles** - Memtable and WAL can be managed separately
2. **Atomic Operations** - Freeze and swap happen atomically or fail cleanly
3. **Interior Mutability** - `&self` methods compatible with background scheduler
4. **Safe Concurrency** - No race conditions in write path or freeze operations
5. **Clean Deletion** - WAL files deleted after flush, not tied to memtable lifetime

## Current Implementation Analysis

### Memtable Structure

```rust
// Current - PROBLEMATIC
pub struct Memtable {
    data: Arc<SkipMap<Vec<u8>, Option<Vec<u8>>>>,
    wal: Wal,  // ❌ Owned - cannot share or delete independently
    size: AtomicUsize,
    frozen: AtomicBool,
}
```

**Problems:**
- `wal: Wal` is owned exclusively by Memtable
- `Wal::remove(self)` requires consuming ownership
- When `Arc<Memtable>` is in frozen queue, WAL cannot be deleted
- No access to WAL after freezing without keeping entire memtable alive

### Freeze Operation

```rust
// Current - NON-ATOMIC
pub fn freeze_active_memtable(&mut self) -> Result<()> {
    self.active_memtable.freeze()?;           // Step 1: Mark frozen
    let frozen = Arc::clone(&self.active_memtable); // Step 2: Clone Arc
    self.frozen_memtables.push_back(frozen);  // Step 3: Add to queue
    let wal_path = ...;                       // Step 4: Create new WAL path
    self.next_wal_id += 1;                    // Step 5: Increment ID
    self.active_memtable = Arc::new(          // Step 6: Replace active
        Memtable::new(wal_path.to_str().unwrap())?
    );
    Ok(())
}
```

**Problems:**
- 6 separate operations, failure at any point leaves inconsistent state
- Requires `&mut self` on LsmStore - incompatible with scheduler
- No atomicity guarantees
- If new Memtable creation fails, active memtable is frozen but not replaced

### Write Path

```rust
// Current - RACE CONDITION
fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
    self.active_memtable.put(key.to_vec(), Some(value))?;

    // ❌ Race: Thread A and B can both see size >= MAX_MEMTABLE_SIZE
    if self.active_memtable.size() >= MAX_MEMTABLE_SIZE {
        self.freeze_active_memtable()?;  // Both threads freeze!
    }
    Ok(())
}
```

**Problems:**
- Check-then-act race condition
- Multiple threads can trigger freeze simultaneously
- Second freeze will fail (already frozen), but damage is done

### WAL Deletion

```rust
// Current - BLOCKED BY OWNERSHIP
fn flush_memtable(&mut self) -> Result<()> {
    let memtable = self.frozen_memtables.pop_front().unwrap();

    // Flush to SSTable...

    // ❌ Cannot access WAL to delete - it's owned by memtable!
    // Workaround: track WAL ID separately, delete by filename
}
```

**Problems:**
- No way to call `wal.remove()` - owned by Arc<Memtable>
- Must track WAL IDs separately in manifest
- Manual file deletion by path, error-prone

## Proposed Architecture

### Phase 1: Shared WAL Ownership

```rust
pub struct Memtable {
    data: Arc<SkipMap<Vec<u8>, Option<Vec<u8>>>>,
    wal: Arc<RwLock<Wal>>,  // ✅ Shared ownership
    wal_id: u64,            // ✅ Track ID for cleanup
    size: AtomicUsize,
    frozen: AtomicBool,
}

impl Memtable {
    pub fn new(wal_path: &str, wal_id: u64) -> Result<Self> {
        let wal = Arc::new(RwLock::new(Wal::new(wal_path)?));
        Ok(Self {
            data: Arc::new(SkipMap::new()),
            wal,
            wal_id,
            size: AtomicUsize::new(0),
            frozen: AtomicBool::new(false),
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        if self.frozen.load(Ordering::SeqCst) {
            return Err(Error::Frozen);
        }

        let key_size = key.len();
        let value_size = value.as_ref().map_or(0, |v| v.len());
        let entry_size = key_size + value_size;

        // Write to WAL through shared reference
        self.wal.write().unwrap().put(&key, value.as_deref())?;

        self.data.insert(key, value);
        self.size.fetch_add(entry_size, Ordering::SeqCst);

        Ok(())
    }

    pub fn wal_id(&self) -> u64 {
        self.wal_id
    }

    pub fn sync(&self) -> Result<()> {
        self.wal.write().unwrap().sync()
    }
}
```

**Benefits:**
- WAL can be shared across multiple references
- Can be closed/deleted independently
- Memtable keeps reference but not exclusive ownership
- WAL ID tracked for cleanup without needing WAL reference

### Phase 2: Atomic Memtable Handle

```rust
// Wrapper for active memtable with atomic swap capability
pub struct MemtableHandle {
    memtable: Arc<Memtable>,
    wal: Arc<RwLock<Wal>>,  // Same reference as in memtable
    wal_id: u64,
}

impl MemtableHandle {
    pub fn new(wal_path: &str, wal_id: u64) -> Result<Self> {
        let memtable = Arc::new(Memtable::new(wal_path, wal_id)?);
        let wal = memtable.wal.clone();

        Ok(Self {
            memtable,
            wal,
            wal_id,
        })
    }

    pub fn freeze(&self) -> Result<FrozenMemtable> {
        self.memtable.freeze()?;

        Ok(FrozenMemtable {
            memtable: self.memtable.clone(),
            wal_id: self.wal_id,
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        self.memtable.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.memtable.get(key)
    }

    pub fn size(&self) -> usize {
        self.memtable.size()
    }

    pub fn sync(&self) -> Result<()> {
        self.wal.write().unwrap().sync()
    }
}

// Frozen memtable without active WAL reference
pub struct FrozenMemtable {
    memtable: Arc<Memtable>,
    wal_id: u64,  // Track for cleanup, no longer need WAL reference
}

impl FrozenMemtable {
    pub fn wal_id(&self) -> u64 {
        self.wal_id
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.memtable.get(key)
    }

    pub fn flush(&self, table: &mut Table) -> Result<()> {
        self.memtable.flush(table)
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIter> {
        self.memtable.scan(range)
    }
}
```

**Benefits:**
- Clear separation: MemtableHandle (active) vs FrozenMemtable (read-only)
- FrozenMemtable doesn't need WAL reference - just ID for cleanup
- Can delete WAL file using ID after flush completes
- Type system enforces proper usage

### Phase 3: Concurrent-Safe LsmState

```rust
pub struct LsmState {
    // Active memtable with atomic swap capability
    active_memtable: Arc<RwLock<MemtableHandle>>,

    // Queue of frozen memtables waiting for flush
    frozen_memtables: Arc<RwLock<VecDeque<FrozenMemtable>>>,

    // SSTable levels
    levels: Arc<RwLock<Vec<Level>>>,

    // Manifest for metadata
    manifest: Arc<RwLock<Manifest>>,

    // Atomic counters
    next_sstable_id: AtomicU64,
    next_wal_id: AtomicU64,

    // Flags for coordination
    freeze_in_progress: AtomicBool,
}

impl LsmState {
    pub fn new(dir: PathBuf) -> Result<Self> {
        let wal_id = 0;
        let wal_path = dir.join("wal").join(format!("{}.wal", wal_id));
        let active_memtable = Arc::new(RwLock::new(
            MemtableHandle::new(wal_path.to_str().unwrap(), wal_id)?
        ));

        Ok(Self {
            active_memtable,
            frozen_memtables: Arc::new(RwLock::new(VecDeque::new())),
            levels: Arc::new(RwLock::new(Vec::new())),
            manifest: Arc::new(RwLock::new(Manifest::new(&dir.join("manifest.log"))?)),
            next_sstable_id: AtomicU64::new(0),
            next_wal_id: AtomicU64::new(1),
            freeze_in_progress: AtomicBool::new(false),
        })
    }
}
```

**Benefits:**
- All fields have interior mutability
- Can be shared across background tasks
- Compatible with SCHEDULER_DESIGN.md
- Fine-grained locking prevents contention

### Phase 4: Atomic Freeze Operation

```rust
impl LsmStore {
    // Now uses &self with interior mutability
    fn freeze_active_memtable(&self) -> Result<()> {
        // Atomic flag prevents concurrent freezes
        if self.state.freeze_in_progress.swap(true, Ordering::SeqCst) {
            return Ok(()); // Another freeze already in progress
        }

        // Ensure flag is cleared on exit
        let _guard = scopeguard::guard((), |_| {
            self.state.freeze_in_progress.store(false, Ordering::SeqCst);
        });

        // Create new active memtable first (can fail safely)
        let new_wal_id = self.state.next_wal_id.fetch_add(1, Ordering::SeqCst);
        let wal_path = self.dir.join("wal").join(format!("{}.wal", new_wal_id));
        let new_handle = MemtableHandle::new(wal_path.to_str().unwrap(), new_wal_id)?;

        // Atomic swap - all or nothing
        let old_handle = {
            let mut active = self.state.active_memtable.write().unwrap();
            let old = active.freeze()?;
            let swapped = std::mem::replace(&mut *active, new_handle);

            // Verify swap worked correctly
            debug_assert!(swapped.memtable.is_frozen());
            old
        };

        // Add to frozen queue
        self.state.frozen_memtables.write().unwrap().push_back(old_handle);

        Ok(())
    }
}
```

**Benefits:**
- Single atomic swap operation
- Flag prevents concurrent freezes
- scopeguard ensures flag cleanup
- Create new memtable before swap (fail-safe)
- Old memtable verified frozen before adding to queue

### Phase 5: Safe Write Path

```rust
impl Store for LsmStore {
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // Write to active memtable (may succeed multiple times during freeze)
        loop {
            let active = self.state.active_memtable.read().unwrap();

            match active.put(key.to_vec(), Some(value.clone())) {
                Ok(_) => {
                    // Check size threshold
                    if active.size() >= MAX_MEMTABLE_SIZE {
                        drop(active); // Release read lock

                        // Try to freeze (atomic, idempotent)
                        self.freeze_active_memtable()?;
                    }
                    return Ok(());
                }
                Err(Error::Frozen) => {
                    // Memtable was frozen between read and write
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

        // Check SSTables...
        Ok(None)
    }
}
```

**Benefits:**
- Retry loop handles freeze race condition gracefully
- Check-then-freeze still has race, but atomic flag prevents duplicate freezes
- Read path uses shared locks (concurrent)
- No `&mut self` required

### Phase 6: WAL Cleanup

```rust
// In flush task
impl FlushTask {
    async fn flush_oldest(&self) -> Result<()> {
        let frozen = {
            let mut queue = self.state.frozen_memtables.write().unwrap();
            queue.pop_front()
        };

        let frozen = match frozen {
            Some(f) => f,
            None => return Ok(()),
        };

        let wal_id = frozen.wal_id();

        // Create SSTable
        let table_id = self.state.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let table_path = self.dir.join(format!("{}.sst", table_id));
        let mut table = Table::writable(&table_path)?;

        // Flush memtable
        frozen.flush(&mut table)?;
        table.finalize()?;

        // Record in manifest (atomic)
        {
            let mut manifest = self.state.manifest.write().unwrap();
            let seq = manifest.next_seq();
            manifest.append(VersionEdit::Flush {
                seq,
                table: table_meta,
                wal_id,  // Mark WAL as deletable
            })?;
            manifest.sync()?;
        }

        // Add to level 0
        // ...

        Ok(())
    }
}

// In cleanup task
impl WalCleanupTask {
    async fn execute(&self, _ctx: TaskContext) -> Result<()> {
        let deletable_wals = self.collect_deletable_wals()?;

        for wal_id in deletable_wals {
            let wal_path = self.dir.join("wal").join(format!("{}.wal", wal_id));

            // Safe to delete - no references remain
            std::fs::remove_file(&wal_path).ok();

            tracing::info!(wal_id = wal_id, "Deleted WAL file");
        }

        Ok(())
    }

    fn collect_deletable_wals(&self) -> Result<Vec<u64>> {
        let manifest = self.state.manifest.read().unwrap();

        let mut deletable = Vec::new();
        for edit in manifest.iter()? {
            match edit? {
                VersionEdit::Flush { wal_id, .. } => {
                    deletable.push(wal_id);
                }
                VersionEdit::Snapshot { deletable_wals, .. } => {
                    return Ok(deletable_wals);
                }
                _ => {}
            }
        }

        Ok(deletable)
    }
}
```

**Benefits:**
- WAL ID tracked separately from WAL reference
- Safe to delete file after flush recorded in manifest
- Cleanup task runs independently
- No lifetime coupling between memtable and WAL

## Write Path State Machine

```
┌─────────────────────────────────────────────────┐
│                 ACTIVE MEMTABLE                  │
│  - Accepting writes                              │
│  - WAL: Arc<RwLock<Wal>> (shared)               │
│  - Size tracked atomically                       │
│  - frozen: AtomicBool = false                    │
└─────────────────────────────────────────────────┘
                        │
                        │ size >= MAX_MEMTABLE_SIZE
                        ↓
┌─────────────────────────────────────────────────┐
│              FREEZE IN PROGRESS                  │
│  - freeze_in_progress flag set                   │
│  - New memtable created                          │
│  - Atomic swap of handles                        │
│  - frozen: AtomicBool → true                     │
└─────────────────────────────────────────────────┘
                        │
                        │ freeze() succeeds
                        ↓
┌─────────────────────────────────────────────────┐
│              FROZEN MEMTABLE                     │
│  - Read-only                                     │
│  - In frozen queue (FrozenMemtable)             │
│  - WAL ID tracked, WAL still open               │
│  - frozen: AtomicBool = true                     │
└─────────────────────────────────────────────────┘
                        │
                        │ flush task picks up
                        ↓
┌─────────────────────────────────────────────────┐
│                 FLUSHING                         │
│  - Writing to SSTable                            │
│  - Memtable still in queue                       │
│  - WAL still open (for safety)                   │
└─────────────────────────────────────────────────┘
                        │
                        │ flush complete
                        ↓
┌─────────────────────────────────────────────────┐
│                  FLUSHED                         │
│  - SSTable on disk                               │
│  - Manifest records Flush with wal_id            │
│  - FrozenMemtable dropped from queue             │
│  - WAL marked deletable                          │
└─────────────────────────────────────────────────┘
                        │
                        │ cleanup task runs
                        ↓
┌─────────────────────────────────────────────────┐
│              WAL DELETED                         │
│  - WAL file removed from disk                    │
│  - No references remain                          │
│  - Storage reclaimed                             │
└─────────────────────────────────────────────────┘
```

## Read Path Flow

```
┌─────────────┐
│   Get(key)   │
└─────────────┘
       │
       ↓
┌──────────────────────┐
│  Check Active         │
│  Memtable            │
│  (RwLock read)       │
└──────────────────────┘
       │
       │ Not found
       ↓
┌──────────────────────┐
│  Check Frozen         │
│  Memtables           │
│  (newest → oldest)   │
│  (RwLock read)       │
└──────────────────────┘
       │
       │ Not found
       ↓
┌──────────────────────┐
│  Check SSTables      │
│  Level 0 → N         │
└──────────────────────┘
       │
       ↓
┌──────────────────────┐
│  Return Result       │
└──────────────────────┘
```

**Concurrency:**
- Multiple readers can access all memtables concurrently (RwLock)
- Writers use read lock on active memtable (concurrent writes to SkipMap)
- Freeze operation uses write lock (exclusive)
- Short critical sections minimize blocking

## Integration with Other Designs

### WAL_DESIGN.md

**Compatibility:**
- Memtable creates WAL using unified Entry format
- WAL wrapped in Arc<RwLock<>> for shared access
- WAL.flush() called by memtable.sync()
- WAL file lifecycle independent of memtable

**Changes Needed:**
- None - WAL design already supports this

### MANIFEST_DESIGN.md

**Compatibility:**
- Flush operation records wal_id in VersionEdit::Flush
- Manifest tracks deletable WALs
- Snapshot includes deletable_wals list
- Recovery restores WAL cleanup state

**Changes Needed:**
- Ensure VersionEdit::Flush includes wal_id field

### SCHEDULER_DESIGN.md

**Compatibility:**
- LsmState uses RwLock for all mutable state
- LsmStore methods use `&self` (interior mutability)
- FlushTask can access frozen_memtables queue
- WalCleanupTask can delete by wal_id

**Changes Needed:**
- LsmStore refactored to use Arc<LsmState>
- All `&mut self` methods converted to `&self`

## Benefits

1. **Independent Lifecycles** - WAL and Memtable managed separately, no coupling
2. **Atomic Freeze** - Single swap operation, no intermediate states
3. **Concurrent Access** - RwLock enables concurrent reads, exclusive writes
4. **Safe Deletion** - WAL deleted after flush, tracked by ID not reference
5. **Race-Free Writes** - Retry loop + atomic flag prevent duplicate freezes
6. **Scheduler Ready** - Interior mutability enables background tasks
7. **Type Safety** - MemtableHandle vs FrozenMemtable enforced by compiler
8. **Clean Recovery** - Manifest tracks deletable WALs, recovery is consistent

## Implementation Steps

1. **Add wal_id to Memtable**
   - Add `wal_id: u64` field
   - Update constructors to accept wal_id
   - Add `wal_id()` getter

2. **Wrap WAL in Arc<RwLock<>>**
   - Change `wal: Wal` to `wal: Arc<RwLock<Wal>>`
   - Update all WAL accesses to use `.read()` or `.write()`
   - Update Memtable::from_wal() constructor

3. **Create MemtableHandle and FrozenMemtable**
   - Define new types in memtable.rs
   - Implement delegation methods
   - Add conversion methods

4. **Refactor LsmState**
   - Wrap fields in Arc<RwLock<>>
   - Change active_memtable to Arc<RwLock<MemtableHandle>>
   - Change frozen_memtables to Arc<RwLock<VecDeque<FrozenMemtable>>>
   - Add freeze_in_progress: AtomicBool

5. **Implement Atomic Freeze**
   - Add freeze_in_progress flag
   - Create new memtable first
   - Atomic swap with mem::replace
   - Use scopeguard for cleanup

6. **Update Write Path**
   - Change set() to use retry loop
   - Handle Error::Frozen with continue
   - Use atomic freeze operation

7. **Update WAL Cleanup**
   - Use wal_id instead of WAL reference
   - Delete files by path after flush
   - Track deletable WALs in manifest

8. **Update Tests**
   - Test concurrent freezes
   - Test WAL deletion timing
   - Test retry loop in write path
   - Test recovery with deletable WALs

## Testing Strategy

1. **Unit Tests**
   - Memtable freeze atomicity
   - WAL lifecycle independence
   - MemtableHandle vs FrozenMemtable conversion

2. **Concurrency Tests**
   - Concurrent writes during freeze
   - Multiple freeze attempts
   - Concurrent reads during freeze

3. **Integration Tests**
   - Write → freeze → flush → WAL cleanup
   - Recovery with deletable WALs
   - Scheduler integration

4. **Stress Tests**
   - High write throughput
   - Rapid freeze cycles
   - Large frozen queue

## Future Enhancements

1. **Write Batching** - Batch multiple writes before WAL sync
2. **Group Commit** - Coordinate WAL sync across writers
3. **Memory Pressure** - Adaptive freeze based on available memory
4. **Compaction Integration** - Coordinate freeze with compaction schedule
5. **MVCC Support** - Sequence numbers for snapshot isolation