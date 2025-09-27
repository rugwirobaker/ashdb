//! Storage abstraction layer for AshDB.
//!
//! This module defines the core `Store` trait that provides a key-value storage
//! interface for the database. The trait is designed to support multiple storage
//! backends while maintaining a consistent API.
//!
//! # Storage Model
//!
//! The store operates on byte arrays for both keys and values, allowing for
//! maximum flexibility in data encoding. Keys are maintained in lexicographical
//! order, which enables efficient range scans and prefix queries.
//!
//! # Key Encoding
//!
//! Keys should use the Keycode order-preserving encoding to ensure proper
//! lexicographical ordering. This encoding preserves the natural sort order
//! of various data types when represented as byte arrays.
//!
//! # Core Operations
//!
//! - **Point Operations**: `get()` for retrieving values by key, `set()` for
//!   inserting or updating key-value pairs
//! - **Range Operations**: `scan()` for iterating over key ranges, `scan_prefix()`
//!   for prefix-based iteration
//! - **Durability**: `sync()` for ensuring data persistence
//!
//! # Durability Guarantees
//!
//! Writes are only guaranteed durable after calling [`Store::sync()`]. Until
//! sync is called, writes may be buffered in memory (such as in WAL files)
//! and could be lost on system failure.
//!
//! # Iterator Design
//!
//! The `ScanIterator` trait provides a unified interface for iterating over
//! key-value pairs. Iterators are lazy and can represent data from multiple
//! sources (memtables, SSTables, etc.) that need to be merged while maintaining
//! lexicographical order.
//!
//! # Implementation Notes
//!
//! Storage implementations must handle:
//! - Concurrent access (multiple readers, exclusive writers)
//! - Error recovery and data consistency
//! - Efficient memory usage for large datasets
//! - Ordered iteration across potentially unsorted sources
//! - Proper lexicographical key ordering
//!
//! # Example Usage
//!
//! ```no_run
//! use ashdb::store::{Store, ScanIterator};
//! use ashdb::error::Result;
//!
//! fn example_usage<S: Store>(store: &S) -> Result<()> {
//!     // Insert key-value pairs (keys should use Keycode encoding)
//!     store.set(b"user:1", b"alice".to_vec())?;
//!     store.set(b"user:2", b"bob".to_vec())?;
//!     store.set(b"user:3", b"charlie".to_vec())?;
//!
//!     // Point lookup
//!     let value = store.get(b"user:2")?;
//!     assert_eq!(value, Some(b"bob".to_vec()));
//!
//!     // Range scan (leverages lexicographical ordering)
//!     let results: Result<Vec<_>> = store.scan(b"user:1".to_vec()..=b"user:2".to_vec()).collect();
//!     let pairs = results?;
//!     assert_eq!(pairs.len(), 2);
//!
//!     // Prefix scan
//!     let users: Result<Vec<_>> = store.scan_prefix(b"user:").collect();
//!     let all_users = users?;
//!     assert_eq!(all_users.len(), 3);
//!
//!     // Ensure durability - writes are only guaranteed persistent after sync
//!     store.sync()?;
//!     Ok(())
//! }
//! ```

pub mod lsm;

use crate::error::Result;
use std::ops::RangeBounds;

/// Core storage trait providing ordered key-value operations.
///
/// This trait defines the fundamental operations that any storage backend
/// must implement. It supports both point operations (get/set) and range
/// operations (scan) on lexicographically ordered keys.
pub trait Store: Send + Sync {
    /// The iterator returned by scan().
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a; // omit in trait objects, for dyn compatibility

    /// Sets a key to a value.
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Gets a value for a key, if it exists.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Scans a key range, returning an iterator over key-value pairs.
    fn scan<'a>(
        &'a self,
        range: impl RangeBounds<Vec<u8>> + Clone + Send + Sync + 'a,
    ) -> Self::ScanIterator<'a>
    where
        Self: Sized; // omit in trait objects, for dyn compatibility

    // /// Like scan, but can be used from trait objects (with dynamic dispatch).
    // fn scan_dyn<'a>(&'a self, range: (std::ops::Bound<Vec<u8>>, std::ops::Bound<Vec<u8>>)) -> Box<dyn ScanIterator + 'a>;

    /// Iterates over all key-value pairs starting with the given prefix.
    fn scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> Self::ScanIterator<'a>
    where
        Self: Sized, // omit in trait objects, for dyn compatibility
    {
        let start = std::ops::Bound::Included(prefix.to_vec());
        let end = match prefix.iter().rposition(|b| *b != 0xff) {
            Some(i) => std::ops::Bound::Excluded(
                prefix[..i]
                    .iter()
                    .chain(std::iter::once(&(prefix[i] + 1)))
                    .copied()
                    .collect::<Vec<u8>>(),
            ),
            None => std::ops::Bound::Unbounded,
        };
        self.scan((start, end))
    }

    /// Synchronizes buffered data to disk, ensuring durability.
    fn sync(&self) -> Result<()>;
}

/// Iterator over key-value pairs returned by scan operations.
///
/// This trait is implemented by all iterators that yield key-value pairs
/// from storage scans. The iterator maintains lexicographical key ordering
/// and handles merging data from multiple sources.
pub trait ScanIterator: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

impl<I> ScanIterator for I where I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
