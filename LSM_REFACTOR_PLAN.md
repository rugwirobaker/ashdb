# AshDB LSM Refactor Plan

## Overview
This is a comprehensive refactor to improve AshDB's directory structure, file naming, API clarity, code organization, and documentation. The refactor will be divided into 7 phases to ensure manageable progression.

## Current Issues Addressed
1. **Directory Structure**: Data scattered - SSTables in root, WALs in wal/, need consolidation under ashdb/
2. **File Naming**: Simple numbering (0.sst, 1.wal) causes poor ordering, need padded format
3. **API Confusion**: Store::flush() vs LSM flush_memtable() operations are confusing
4. **Over-exposed API**: Too many implementation details exposed publicly
5. **Incomplete Integration**: Iterator integration needs completion throughout LSM
6. **Code Organization**: Level types should be in manifest package with other metadata
7. **Documentation Gap**: Need comprehensive docs for educational purposes (like sstable module)

## Phase 1: Directory Structure Consolidation
**Goal**: Reorganize data files under a single `ashdb/` directory with proper subdirectories

### Current Structure:
```
{config.dir}/
├── 0.sst, 1.sst, 2.sst...  # SSTables at root level
├── wal/
│   ├── 4.wal, 5.wal...      # WAL files (already good)
├── manifest.log             # Manifest at root (good)
└── ashdb.lock               # Lock file at root (good)
```

### Target Structure:
```
{config.dir}/                # Default: ./ashdb/
├── sst/
│   ├── 00000000.sst         # SSTables in subdirectory with padding
│   ├── 00000001.sst
│   └── ...
├── wal/
│   ├── 00000000.wal         # WAL files with padding
│   ├── 00000001.wal
│   └── ...
├── manifest.log             # Manifest at root
└── ashdb.lock               # Lock file at root
```

### Tasks:
- ✅ Config already defaults to "./ashdb" directory
- Create `sst/` subdirectory in directory creation code
- Update path construction in `recovery.rs` for sst/ subdirectory
- Update path construction in `flush.rs` for sst/ subdirectory
- Update path construction in `compaction.rs` for sst/ subdirectory
- Update tests to use new directory structure

### Files to Modify:
- `src/store/lsm/store.rs:34` - Add sst directory creation
- `src/store/lsm/recovery.rs:48` - Update SSTable path construction
- `src/store/lsm/flush.rs:19` - Update SSTable path construction
- `src/store/lsm/compaction.rs:118,224` - Update SSTable path construction
- Test files throughout

## Phase 2: Enhanced File Naming with 8-Digit Padding
**Goal**: Use 8-digit padded numeric IDs that match the atomically generated entity IDs

### Decision: 8-digit padding (`{:08}`)
- Supports up to 99,999,999 files per type
- Better file system ordering and tooling compatibility
- Future-proof for large databases
- Examples: `00000000.sst`, `00000001.wal`

### Key Integration Points:
- WAL IDs from `LsmState::next_wal_id()` (AtomicU64 counter in state.rs:24)
- SSTable IDs from `LsmState::next_sstable_id()` (AtomicU64 counter in state.rs:23)
- Filenames must use exact same ID values: `{id:08}.sst`, `{id:08}.wal`

### Current Format Locations:
- `compaction.rs:118`: `format!("{}.sst", table_id)` → `format!("{:08}.sst", table_id)`
- `flush.rs:19`: `format!("{}.sst", table_id)` → `format!("{:08}.sst", table_id)`
- `recovery.rs:48`: `format!("{}.sst", table_meta.id)` → `format!("{:08}.sst", table_meta.id)`
- `store.rs:62`: `format!("{}.wal", new_wal_id)` → `format!("{:08}.wal", new_wal_id)`

### Tasks:
- Update all filename generation to use 8-digit padding
- Update ID parsing in recovery to handle padded filenames
- Test migration from old naming (ensure backwards compatibility)
- Update any file listing/iteration code to handle new format

## Phase 3: API Clarification - Store::flush → Store::sync
**Goal**: Rename confusing `flush` method to `sync` to distinguish from LSM memtable flushing

### Problem:
- `Store::flush()` in trait (line 49 in store/mod.rs) syncs WAL to disk
- `LsmStore::flush_memtable()` flushes memtable to SSTable
- Naming is confusing - both called "flush" but do different things

### Solution:
- Rename `Store::flush()` → `Store::sync()` (WAL sync operation)
- Keep `LsmStore::flush_memtable()` (memtable → SSTable operation)

### Tasks:
- Rename `Store::flush()` trait method to `Store::sync()` in `src/store/mod.rs:49`
- Update `LsmStore` implementation in `src/store/lsm/store.rs:254`
- Update all trait usage across the codebase
- Update tests and documentation
- Ensure clear distinction between WAL sync and memtable flush operations

## Phase 4: API Simplification and Encapsulation
**Goal**: Hide implementation details behind main LSM operations

### Current Public API Analysis:
- `compact()` - Keep (main compaction orchestrator)
- `flush_memtable()` - Keep (main memtable flush orchestrator)
- `freeze_active_memtable()` - Consider hiding (implementation detail)
- `cleanup_wals()`, `deletable_wals()` - Consider hiding (should be internal to compact)
- `needs_compaction()`, `needs_flush()` - Keep for external scheduling
- Various coordination methods - Hide as implementation details

### Tasks:
- Make `compact()` orchestrate all compaction-related operations internally
- Make `flush_memtable()` handle all memtable → SSTable conversion aspects
- Consider hiding as `pub(super)` or private:
  - `freeze_active_memtable()` (implementation detail)
  - `cleanup_wals()`, `deletable_wals()` (should be internal to compact)
  - Low-level coordination methods
- Keep public: `compact()`, `flush_memtable()`, `needs_compaction()`, `needs_flush()`
- Update background task interactions with simplified API
- Update tests to use simplified API where possible

## Phase 5: Iterator Integration Completion
**Goal**: Complete iterator integration work throughout the LSM tree

### Current State:
- SSTable iterator integration is already done (in sstable/ module)
- Need to extend to other LSM components where beneficial

### Tasks:
- Review and optimize existing SSTable iterator integration
- Extend iterator patterns to other LSM components where beneficial
- Ensure consistent iterator patterns across memtables, SSTables, and levels
- Optimize iterator performance and memory usage
- Update merge iterator logic for consistency
- Document iterator patterns and usage

## Phase 6: Code Organization - Move Level to Manifest
**Goal**: Move Level types to manifest package since they contain metadata

### Current Structure:
- `src/store/lsm/level.rs` contains `Level` and `SSTable` structs
- These are metadata about the LSM structure
- Manifest package already contains metadata persistence

### Target Structure:
- Move to `src/store/lsm/manifest/level.rs`
- Better organization with other metadata types

### Tasks:
- Move `Level` and `SSTable` structs from `src/store/lsm/level.rs` to `src/store/lsm/manifest/level.rs`
- Update imports in `src/store/lsm/mod.rs` (lines 19-20)
- Update all other imports across the codebase
- Ensure proper module organization within manifest package
- Update tests and ensure no broken dependencies

## Phase 7: Comprehensive Documentation
**Goal**: Document the entire LSM implementation for educational purposes

### Current State:
- `src/store/lsm/sstable/` is well documented (good example to follow)
- Need to extend this documentation quality throughout LSM modules

### Documentation Standards (following sstable/ example):
- Module-level documentation explaining purpose and design decisions
- ASCII diagrams for data structures and file layouts
- Function-level documentation for complex operations
- Data structure documentation with invariants and usage patterns
- Algorithm explanations (compaction strategy, merge logic, etc.)
- Concurrency patterns and safety guarantees
- Recovery and consistency mechanisms
- Performance characteristics and trade-offs
- Examples and usage patterns

### Modules to Document:

#### Core Store Documentation:
- `src/store/mod.rs`: Document Store trait design and rationale
- `src/store/lsm/mod.rs`: Overview of LSM tree architecture and module organization

#### LSM Component Documentation:
- `src/store/lsm/store.rs`: Main LSM store operations and coordination
- `src/store/lsm/state.rs`: State management, atomic operations, and concurrency
- `src/store/lsm/memtable/`: In-memory storage, freezing, and WAL integration
- `src/store/lsm/wal/`: Write-ahead logging format and recovery
- `src/store/lsm/manifest/`: Metadata persistence and crash recovery
- `src/store/lsm/compaction.rs`: Tiered compaction strategy and implementation
- `src/store/lsm/flush.rs`: Memtable to SSTable conversion process
- `src/store/lsm/recovery.rs`: Startup recovery from WAL and manifest
- `src/store/lsm/level.rs` (before moving): Level-based storage organization

## Implementation Strategy

### Testing Approach:
- Run tests after each phase to ensure no regressions
- Use `cargo check`, `cargo clippy`, `cargo fmt` after changes
- Add new tests for changed functionality
- Ensure backwards compatibility where possible

### Migration Strategy:
- Phase 1-2 may require data migration for existing databases
- Consider backwards compatibility for filename parsing
- Document migration procedures

### Dependencies Between Phases:
- Phase 1 → Phase 2 (directory structure before file naming)
- Phase 2 must be complete before Phase 3 (file operations stable)
- Phase 6 can be done in parallel with Phase 4-5
- Phase 7 can proceed throughout (documentation)

## Deliverables:
- Refactored codebase with improved structure
- 8-digit padded file naming using entity IDs for better organization
- Clearer API with Store::sync vs flush_memtable distinction
- Simplified public interface hiding implementation details
- Completed iterator integration across all components
- Better organized code with Level types in manifest package
- Comprehensive educational documentation throughout LSM implementation
- Updated tests ensuring all functionality works
- Migration strategy for filename changes

**Estimated Scope**: Large refactor affecting most LSM-related code with significant documentation effort. Each phase builds on the previous one and can be tested independently. Documentation work can proceed in parallel with implementation phases.

## Progress Tracking

### Phase 1: Directory Structure ⏳
- [ ] Create sst/ subdirectory in directory creation
- [ ] Update recovery.rs path construction
- [ ] Update flush.rs path construction
- [ ] Update compaction.rs path construction
- [ ] Update tests

### Phase 2: File Naming 🔄
- [ ] Implement 8-digit SSTable naming
- [ ] Implement 8-digit WAL naming
- [ ] Update recovery parsing
- [ ] Test backwards compatibility

### Phase 3: API Naming 🔄
- [ ] Rename Store::flush to Store::sync
- [ ] Update LsmStore implementation
- [ ] Update all usages

### Phase 4: API Simplification 🔄
- [ ] Hide implementation details
- [ ] Simplify public interface

### Phase 5: Iterator Integration 🔄
- [ ] Complete iterator patterns

### Phase 6: Code Organization 🔄
- [ ] Move Level to manifest

### Phase 7: Documentation 🔄
- [ ] Document all LSM modules

Legend: ✅ Done | ⏳ In Progress | 🔄 Pending