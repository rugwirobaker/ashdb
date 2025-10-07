// src/store/lsm/sstable/mod.rs

//! Sorted String Table (SSTable) implementation for persistent storage.
//!
//! SSTables provide the durable storage layer of the LSM-tree, storing immutable
//! sorted key-value pairs on disk with efficient read access patterns.
//!
//! # Architecture Overview
//!
//! The SSTable design balances storage efficiency with read performance through
//! a multi-level structure optimized for both point lookups and range scans.
//!
//! ## File Format
//!
//! ```text
//! +-------------------+
//! | Data Block 1      |  ← Variable size, ~4KB target
//! +-------------------+
//! | Data Block 2      |  ← Uncompressed key-value entries
//! +-------------------+
//! | ...               |
//! +-------------------+
//! | Data Block N      |
//! +-------------------+
//! | Index Block       |  ← Sparse index for block lookup
//! +-------------------+
//! | Index Offset (u64)|  ← Footer for index location
//! +-------------------+
//! ```
//!
//! # Component Details
//!
//! ## Data Blocks (`block.rs`)
//!
//! Data blocks are the fundamental storage units, typically ~4KB in size:
//! - **Uncompressed keys**: Full keys stored for simplicity and reverse iteration
//! - **Linear scan**: Entries are scanned sequentially for forward iteration
//! - **Lazy offset tracking**: Reverse iteration builds offset index on-demand
//! - **Double-ended**: Supports both `Iterator` and `DoubleEndedIterator`
//!
//! ### Block Entry Format
//! ```text
//! +-----------+-------+-------------+-----+-------+
//! |key_len:u16|op:u8  |value_len:u32| key | value |
//! +-----------+-------+-------------+-----+-------+
//! ```
//!
//! Where `op` is one of:
//! - `0x00`: Put (key-value pair)
//! - `0x01`: Delete (tombstone marker)
//!
//! ## Sparse Index (`index.rs`)
//!
//! The index provides fast block location for any key:
//! - **First key mapping**: Each entry maps a block's first key to its file offset
//! - **Binary search**: O(log n) lookup to find the correct block
//! - **Range optimization**: Efficiently identifies blocks needed for range scans
//!
//! ### Index Entry Format
//! ```text
//! +-------------+-----+----------+----------+
//! |key_len:u16  | key |offset:u64|size:u64  |
//! +-------------+-----+----------+----------+
//! ```
//!
//! ## Table Management (`table.rs`)
//!
//! Handles SSTable lifecycle from creation to querying:
//! - **Typestate pattern**: Enforces write-then-read lifecycle at compile time
//! - **Concurrent access**: Multiple readers via file handle cloning
//! - **Range filtering**: Combines index lookups with iterator chaining
//!
//! # Performance Characteristics
//!
//! - **Point lookups**: O(log blocks + entries_per_block)
//! - **Range scans**: O(log blocks + scan_size)
//! - **Forward iteration**: Zero overhead, no memory allocation
//! - **Reverse iteration**: Lazy offset index built on first `next_back()` call
//! - **Cache friendly**: 4KB blocks align with OS page size
//!
//! # Iteration Model
//!
//! All iterators support both forward and reverse iteration through Rust's
//! `DoubleEndedIterator` trait:
//!
//! - **`BlockIterator`**: Lazily tracks entry offsets for reverse traversal
//! - **`MultiBlockIterator`**: Chains blocks in both directions
//! - **`RangeFilter`**: Wraps iterators with range bounds checking
//!
//! Reverse iteration is optimized for minimal overhead when unused:
//! - Forward-only iteration: O(1) space
//! - Reverse iteration: O(n) space only for visited entries
//!
//! # Future Optimizations
//!
//! ## Bloom Filters
//! - **Block-level filters**: Reduce block reads for non-existent keys
//! - **Table-level filters**: Skip entire SSTables during queries
//! - **Space-time tradeoff**: ~10 bits per key for 1% false positive rate
//!
//! ## Block Caching
//! - **LRU cache**: Keep frequently accessed blocks in memory
//! - **Adaptive sizing**: Adjust cache based on workload patterns
//! - **Prefetching**: Load adjacent blocks for sequential scans
//!
//! ## Async I/O Integration
//! - **Non-blocking operations**: Prevent I/O from blocking other operations
//! - **Batched I/O**: Group multiple block reads for efficiency
//! - **Turso-style approach**: https://github.com/tursodatabase/turso/tree/main/core/io
//! - **Zero-copy**: Direct buffer management for reduced allocations

mod index;

pub mod block;
pub mod table;
