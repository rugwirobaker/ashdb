// src/store/lsm/sstable/mod.rs

//! This module implements the Sorted String Table (SSTable), which is the on-disk
//! storage format for the LSM-tree. SSTables are immutable and contain a sequence of
//! sorted key-value pairs.
//!
//! The SSTable format is designed for efficient reads, especially scans, and is
//! composed of three main parts:
//!
//! 1.  **Data Blocks:** These are variable-sized blocks that store key-value pairs.
//!     To save space, keys are prefix-compressed within each block. Restart points
//!     are used to allow for efficient seeking within a block.
//!
//! 2.  **Index Block:** This is a sparse index that contains an entry for each data
//!     block. The index entry consists of the first key of the block and the block's
//!     offset and size in the file. This allows for a quick lookup of the block that
//!     may contain a given key.
//!
//! 3.  **Footer:** The last 8 bytes of the file store the offset of the index block.
//!
//! ## File Layout
//!
//! ```text
//! +-------------------+
//! | Data Block 1      |
//! +-------------------+
//! | Data Block 2      |
//! +-------------------+
//! | ...               |
//! +-------------------+
//! | Data Block N      |
//! +-------------------+
//! | Index Block       |
//! +-------------------+
//! | Index Offset (u64)|
//! +-------------------+
//! ```

mod index;

pub mod block;
pub mod table;
