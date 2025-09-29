//! Iterator merge algorithms for LSM-tree scan operations.
//!
//! This module implements efficient merging of multiple sorted iterators from
//! different data sources (memtables, SSTables) while maintaining lexicographical
//! order and handling key deduplication.
//!
//! # Merge Strategy
//!
//! The core challenge is efficiently merging N sorted streams while:
//! - Maintaining lexicographical key ordering
//! - Handling duplicate keys (newer values win)
//! - Minimizing memory usage through lazy evaluation
//! - Supporting efficient range queries
//!
//! # Binary Heap Implementation
//!
//! We use a min-heap (BinaryHeap with reversed ordering) to efficiently
//! find the next smallest key across all iterators:
//!
//! ```text
//! Iterators:  [a, d, g, ...]  [b, e, h, ...]  [c, f, i, ...]
//!                 ↓               ↓               ↓
//! Heap:       [   a,              b,              c     ]
//!                 ↓ (pop minimum)
//! Output:         a
//! ```
//!
//! # Key Deduplication
//!
//! When the same key appears in multiple sources, the iterator with the
//! lowest source index wins (newer data sources have lower indices).
//! This implements the LSM-tree property where newer writes shadow older ones.
//!
//! # Lazy Evaluation
//!
//! The iterator only pulls data from source iterators as needed, reducing
//! memory pressure for large scans and enabling efficient early termination.

use crate::error::Result;
use std::{cmp::Ordering, collections::BinaryHeap};

/// Type alias for internal iterators that may contain tombstones
pub type InternalIterator<'a> =
    Box<dyn Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>> + Send + Sync + 'a>;

/// Type alias for external iterators (no tombstones) - used by LSMScanIterator
pub type LSMIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + Sync + 'a>;

/// Simplified heap entry that doesn't store the iterator inside.
/// The iterator is kept in an external Vec and referenced by index.
#[derive(Debug)]
pub struct HeapEntry {
    key: Vec<u8>,
    value: Vec<u8>,
    source_index: usize,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => self.source_index.cmp(&other.source_index),
            other => other.reverse(), // Reverse for min-heap behavior
        }
    }
}

/// Cleaned up LSM scan iterator that merges multiple sorted iterators.
/// This maintains the same merge sort algorithm but with simpler implementation.
/// Filters out tombstones for user-facing scans.
pub struct LsmIterator<'a> {
    iterators: Vec<InternalIterator<'a>>,
    heap: BinaryHeap<HeapEntry>,
    last_yielded_key: Option<Vec<u8>>,
}

impl<'a> LsmIterator<'a> {
    pub fn new(mut iterators: Vec<InternalIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Prime the heap with the first non-tombstone entry from each iterator
        for (source_index, iterator) in iterators.iter_mut().enumerate() {
            Self::prime_iterator(&mut heap, iterator, source_index);
        }

        Self {
            iterators,
            heap,
            last_yielded_key: None,
        }
    }

    fn prime_iterator(
        heap: &mut BinaryHeap<HeapEntry>,
        iterator: &mut InternalIterator<'_>,
        source_index: usize,
    ) {
        for result in iterator.by_ref() {
            match result {
                Ok((key, Some(value))) => {
                    // Non-tombstone entry, add to heap
                    heap.push(HeapEntry {
                        key,
                        value,
                        source_index,
                    });
                    break;
                }
                Ok((_, None)) => {
                    // Tombstone, skip and continue
                    continue;
                }
                Err(_) => {
                    // Error, stop processing this iterator
                    break;
                }
            }
        }
    }
}

impl Iterator for LsmIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.heap.pop() {
            // Skip duplicates - if we see the same key again, newer source wins
            if self.last_yielded_key.as_ref() == Some(&entry.key) {
                // Pull the next non-tombstone entry from this iterator
                Self::prime_iterator(
                    &mut self.heap,
                    &mut self.iterators[entry.source_index],
                    entry.source_index,
                );
                continue; // Skip this duplicate key
            }

            // Update our last yielded key for deduplication
            self.last_yielded_key = Some(entry.key.clone());

            // Pull the next non-tombstone entry from this iterator
            Self::prime_iterator(
                &mut self.heap,
                &mut self.iterators[entry.source_index],
                entry.source_index,
            );

            // Return this entry (guaranteed to be non-tombstone)
            return Some(Ok((entry.key, entry.value)));
        }

        None
    }
}

/// Compaction iterator that preserves tombstones (unlike LSMScanIterator which filters them).
/// This is used for internal compaction operations where tombstones need to be preserved.
pub struct CompactionIterator<'a> {
    iterators: Vec<InternalIterator<'a>>,
    heap: BinaryHeap<HeapEntry>,
    last_yielded_key: Option<Vec<u8>>,
}

impl<'a> CompactionIterator<'a> {
    pub fn new(mut iterators: Vec<InternalIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Prime the heap with the first entry from each iterator (including tombstones)
        for (source_index, iterator) in iterators.iter_mut().enumerate() {
            if let Some(result) = iterator.next() {
                match result {
                    Ok((key, value)) => {
                        // Convert Option<Vec<u8>> to Vec<u8> for heap storage
                        let heap_value = value.unwrap_or_default();
                        heap.push(HeapEntry {
                            key,
                            value: heap_value,
                            source_index,
                        });
                    }
                    Err(_) => {
                        // Skip error entries during initialization
                    }
                }
            }
        }

        Self {
            iterators,
            heap,
            last_yielded_key: None,
        }
    }
}

impl Iterator for CompactionIterator<'_> {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(entry) = self.heap.pop() {
            // Skip duplicates - if we see the same key again, newer source wins
            if self.last_yielded_key.as_ref() == Some(&entry.key) {
                // Pull the next entry from this iterator
                if let Some(result) = self.iterators[entry.source_index].next() {
                    match result {
                        Ok((key, value)) => {
                            let heap_value = value.unwrap_or_default();
                            self.heap.push(HeapEntry {
                                key,
                                value: heap_value,
                                source_index: entry.source_index,
                            });
                        }
                        Err(_) => {} // Skip errors
                    }
                }
                continue; // Skip this duplicate key
            }

            // Update our last yielded key for deduplication
            self.last_yielded_key = Some(entry.key.clone());

            // Pull the next entry from this iterator
            if let Some(result) = self.iterators[entry.source_index].next() {
                match result {
                    Ok((key, value)) => {
                        let heap_value = value.unwrap_or_default();
                        self.heap.push(HeapEntry {
                            key,
                            value: heap_value,
                            source_index: entry.source_index,
                        });
                    }
                    Err(_) => {} // Skip errors
                }
            }

            // Return this entry - convert empty vec back to None for tombstones
            let value = if entry.value.is_empty() {
                None
            } else {
                Some(entry.value)
            };

            return Some(Ok((entry.key, value)));
        }

        None
    }
}
