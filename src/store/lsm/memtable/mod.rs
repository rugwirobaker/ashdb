pub mod active;
pub mod core;
pub mod frozen;

// MAX_MEMTABLE_SIZE is the maximum size of the Memtable in bytes.
pub const MAX_MEMTABLE_SIZE: usize = 64 * 1024 * 1024; // 64MB

// Re-export all types for easy access
pub use active::ActiveMemtable;
pub use core::{Memtable, ScanIterator};
pub use frozen::FrozenMemtable;
