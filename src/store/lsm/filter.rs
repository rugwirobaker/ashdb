//! Iterator filtering utilities for LSM tree components.
//!
//! This module provides generic iterator adapters that can filter and transform
//! streams of key-value pairs based on various criteria. These utilities are shared
//! across all LSM components including memtables, SSTables, and merge iterators.

use crate::error::Result;
use std::ops::{Bound, RangeBounds};

/// A generic iterator adapter that filters entries by a key range.
/// This can wrap any iterator that produces `Result<(Vec<u8>, Vec<u8>)>` items.
///
/// # Examples
///
/// ```ignore
/// let filtered = RangeFilter::new(some_iterator, b"key_010".to_vec()..=b"key_020".to_vec());
/// for result in filtered {
///     // Only entries with keys in the range will be yielded
/// }
/// ```
pub struct RangeFilter<I, R>
where
    I: Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>>,
    R: RangeBounds<Vec<u8>> + Send + Sync,
{
    inner: I,
    range: R,
}

impl<I, R> RangeFilter<I, R>
where
    I: Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>>,
    R: RangeBounds<Vec<u8>> + Send + Sync,
{
    /// Create a new range filter that wraps the given iterator.
    pub fn new(inner: I, range: R) -> Self {
        Self { inner, range }
    }

    /// Check if a key is within the filter's range.
    fn is_key_in_range(&self, key: &[u8]) -> bool {
        // Check start bound
        match self.range.start_bound() {
            Bound::Included(start) => {
                if key < start.as_slice() {
                    return false;
                }
            }
            Bound::Excluded(start) => {
                if key <= start.as_slice() {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        // Check end bound
        match self.range.end_bound() {
            Bound::Included(end) => {
                if key > end.as_slice() {
                    return false;
                }
            }
            Bound::Excluded(end) => {
                if key >= end.as_slice() {
                    return false;
                }
            }
            Bound::Unbounded => {}
        }

        true
    }

    /// Check if a key is beyond the end bound (used for early termination in forward iteration).
    fn is_key_beyond_end(&self, key: &[u8]) -> bool {
        match self.range.end_bound() {
            Bound::Included(end) => key > end.as_slice(),
            Bound::Excluded(end) => key >= end.as_slice(),
            Bound::Unbounded => false,
        }
    }

    /// Check if a key is before the start bound (used for early termination in reverse iteration).
    fn is_key_before_start(&self, key: &[u8]) -> bool {
        match self.range.start_bound() {
            Bound::Included(start) => key < start.as_slice(),
            Bound::Excluded(start) => key <= start.as_slice(),
            Bound::Unbounded => false,
        }
    }
}

impl<I, R> Iterator for RangeFilter<I, R>
where
    I: Iterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>>,
    R: RangeBounds<Vec<u8>> + Send + Sync,
{
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next()? {
                Ok((key, value)) => {
                    if self.is_key_in_range(&key) {
                        return Some(Ok((key, value)));
                    }
                    // If key is beyond the end bound, we're done
                    if self.is_key_beyond_end(&key) {
                        return None;
                    }
                    // Otherwise, continue to the next entry
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl<I, R> DoubleEndedIterator for RangeFilter<I, R>
where
    I: DoubleEndedIterator<Item = Result<(Vec<u8>, Option<Vec<u8>>)>>,
    R: RangeBounds<Vec<u8>> + Send + Sync,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next_back()? {
                Ok((key, value)) => {
                    if self.is_key_in_range(&key) {
                        return Some(Ok((key, value)));
                    }
                    // If key is before the start bound, we're done (early termination)
                    if self.is_key_before_start(&key) {
                        return None;
                    }
                    // Otherwise, continue to the previous entry
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_filter_inclusive() {
        let data = vec![
            Ok((b"key_005".to_vec(), Some(b"value_005".to_vec()))),
            Ok((b"key_010".to_vec(), Some(b"value_010".to_vec()))),
            Ok((b"key_015".to_vec(), Some(b"value_015".to_vec()))),
            Ok((b"key_020".to_vec(), Some(b"value_020".to_vec()))),
            Ok((b"key_025".to_vec(), Some(b"value_025".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_010".to_vec()..=b"key_020".to_vec();
        let filtered: Vec<_> = RangeFilter::new(iter, range)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(filtered.len(), 3);
        assert_eq!(filtered[0].0, b"key_010");
        assert_eq!(filtered[1].0, b"key_015");
        assert_eq!(filtered[2].0, b"key_020");
    }

    #[test]
    fn test_range_filter_exclusive() {
        let data = vec![
            Ok((b"key_005".to_vec(), Some(b"value_005".to_vec()))),
            Ok((b"key_010".to_vec(), Some(b"value_010".to_vec()))),
            Ok((b"key_015".to_vec(), Some(b"value_015".to_vec()))),
            Ok((b"key_020".to_vec(), Some(b"value_020".to_vec()))),
            Ok((b"key_025".to_vec(), Some(b"value_025".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_010".to_vec()..b"key_020".to_vec();
        let filtered: Vec<_> = RangeFilter::new(iter, range)
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].0, b"key_010");
        assert_eq!(filtered[1].0, b"key_015");
    }

    #[test]
    fn test_range_filter_error_propagation() {
        use crate::Error;

        let data = vec![
            Ok((b"key_005".to_vec(), Some(b"value_005".to_vec()))),
            Err(Error::InvalidData("test error".to_string())),
            Ok((b"key_015".to_vec(), Some(b"value_015".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_000".to_vec()..=b"key_020".to_vec();
        let mut filtered = RangeFilter::new(iter, range);

        // First item should be Ok
        assert!(filtered.next().unwrap().is_ok());

        // Second item should be the error
        assert!(filtered.next().unwrap().is_err());
    }

    #[test]
    fn test_range_filter_reverse_inclusive() {
        let data = vec![
            Ok((b"key_005".to_vec(), Some(b"value_005".to_vec()))),
            Ok((b"key_010".to_vec(), Some(b"value_010".to_vec()))),
            Ok((b"key_015".to_vec(), Some(b"value_015".to_vec()))),
            Ok((b"key_020".to_vec(), Some(b"value_020".to_vec()))),
            Ok((b"key_025".to_vec(), Some(b"value_025".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_010".to_vec()..=b"key_020".to_vec();
        let mut filtered = RangeFilter::new(iter, range);

        // Iterate backward
        assert_eq!(
            filtered.next_back().unwrap().unwrap().0,
            b"key_020".to_vec()
        );
        assert_eq!(
            filtered.next_back().unwrap().unwrap().0,
            b"key_015".to_vec()
        );
        assert_eq!(
            filtered.next_back().unwrap().unwrap().0,
            b"key_010".to_vec()
        );
        assert!(filtered.next_back().is_none());
    }

    #[test]
    fn test_range_filter_reverse_exclusive() {
        let data = vec![
            Ok((b"key_005".to_vec(), Some(b"value_005".to_vec()))),
            Ok((b"key_010".to_vec(), Some(b"value_010".to_vec()))),
            Ok((b"key_015".to_vec(), Some(b"value_015".to_vec()))),
            Ok((b"key_020".to_vec(), Some(b"value_020".to_vec()))),
            Ok((b"key_025".to_vec(), Some(b"value_025".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_010".to_vec()..b"key_020".to_vec();
        let mut filtered = RangeFilter::new(iter, range);

        // Iterate backward
        assert_eq!(
            filtered.next_back().unwrap().unwrap().0,
            b"key_015".to_vec()
        );
        assert_eq!(
            filtered.next_back().unwrap().unwrap().0,
            b"key_010".to_vec()
        );
        assert!(filtered.next_back().is_none());
    }

    #[test]
    fn test_range_filter_mixed_iteration() {
        let data = vec![
            Ok((b"key_005".to_vec(), Some(b"value_005".to_vec()))),
            Ok((b"key_010".to_vec(), Some(b"value_010".to_vec()))),
            Ok((b"key_015".to_vec(), Some(b"value_015".to_vec()))),
            Ok((b"key_020".to_vec(), Some(b"value_020".to_vec()))),
            Ok((b"key_025".to_vec(), Some(b"value_025".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_010".to_vec()..=b"key_020".to_vec();
        let mut filtered = RangeFilter::new(iter, range);

        // Mix forward and backward iteration
        assert_eq!(filtered.next().unwrap().unwrap().0, b"key_010".to_vec());
        assert_eq!(
            filtered.next_back().unwrap().unwrap().0,
            b"key_020".to_vec()
        );
        assert_eq!(filtered.next().unwrap().unwrap().0, b"key_015".to_vec());
        assert!(filtered.next().is_none());
        assert!(filtered.next_back().is_none());
    }

    #[test]
    fn test_range_filter_reverse_with_tombstones() {
        let data = vec![
            Ok((b"key_010".to_vec(), Some(b"value_010".to_vec()))),
            Ok((b"key_015".to_vec(), None)), // Tombstone
            Ok((b"key_020".to_vec(), Some(b"value_020".to_vec()))),
        ];

        let iter = data.into_iter();
        let range = b"key_000".to_vec()..=b"key_030".to_vec();
        let mut filtered = RangeFilter::new(iter, range);

        // Should see tombstone in reverse iteration
        assert_eq!(
            filtered.next_back().unwrap().unwrap(),
            (b"key_020".to_vec(), Some(b"value_020".to_vec()))
        );
        assert_eq!(
            filtered.next_back().unwrap().unwrap(),
            (b"key_015".to_vec(), None)
        );
        assert_eq!(
            filtered.next_back().unwrap().unwrap(),
            (b"key_010".to_vec(), Some(b"value_010".to_vec()))
        );
        assert!(filtered.next_back().is_none());
    }
}
