//! LSM-Tree storage engine implementation.
//!
//! This module implements a Log-Structured Merge-tree (LSM-tree) storage engine
//! that provides efficient write throughput and good read performance for
//! time-series and write-heavy workloads.
//!
//! # LSM-Tree Architecture
//!
//! The LSM-tree organizes data in multiple levels, each containing sorted data:
//!
//! ```text
//! ┌─────────────────┐    ┌─────────────────┐
//! │ Active Memtable │    │ Frozen Memtables│
//! │   (SkipList)    │◄───┤   (SkipLists)   │
//! └─────────────────┘    └─────────────────┘
//!           │                       │
//!           ▼                       ▼
//!      ┌────────┐             ┌─────────────┐
//!      │WAL File│             │  Level 0    │
//!      └────────┘             │ (SSTables)  │
//!                             └─────────────┘
//!                                   │
//!                                   ▼
//!                            ┌─────────────┐
//!                            │  Level 1    │
//!                            │ (SSTables)  │
//!                            └─────────────┘
//!                                   │
//!                                   ▼
//!                                  ...
//! ```
//!
//! # Data Flow
//!
//! ## Write Path
//! 1. **WAL Write**: Data is first written to the Write-Ahead Log for durability
//! 2. **Memtable Insert**: Data is inserted into the active memtable (SkipList)
//! 3. **Freeze**: When memtable reaches size limit, it's frozen and a new active one is created
//! 4. **Flush**: Frozen memtables are flushed to Level 0 SSTables
//! 5. **Compaction**: SSTables are compacted across levels using tiered compaction
//!
//! ## Read Path
//! 1. **Active Memtable**: Check the active memtable first
//! 2. **Frozen Memtables**: Check frozen memtables (newest to oldest)
//! 3. **SSTables**: Check SSTables across all levels
//!
//! # Concurrency and Durability
//!
//! - **File Locking**: Uses `ashdb.lock` to ensure single-process access
//! - **WAL Durability**: Writes are durable after calling `sync()`
//! - **Recovery**: On startup, replays manifest log and WAL files to restore state
//! - **Lock-free Reads**: Memtables support concurrent reads using crossbeam-skiplist
//!
//! # Performance Characteristics
//!
//! - **Writes**: O(log n) for memtable insertion, with high throughput due to WAL batching
//! - **Reads**: O(log n) for recent data (memtables), O(log n * levels) for older data
//! - **Scans**: Efficient range queries with merge-sort across data sources
//! - **Space**: Controlled through compaction policies and level size ratios

pub mod compaction;
pub mod filter;
pub mod flush;
pub mod iterator;
pub mod manifest;
pub mod memtable;
pub mod metrics;
pub mod recovery;
pub mod sstable;
pub mod state;
pub mod store;
pub mod wal;

pub use iterator::{HeapEntry, LSMIterator, LSMScanIterator};
pub use manifest::{Level, SSTable};
pub use memtable::{ActiveMemtable, FrozenMemtable, Memtable, ScanIterator, MAX_MEMTABLE_SIZE};
pub use state::{CompactionGuard, FreezeGuard, LsmState};
pub use store::LsmTree;
