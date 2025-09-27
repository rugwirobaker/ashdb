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
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    R: RangeBounds<Vec<u8>> + Send + Sync,
{
    inner: I,
    range: R,
}

impl<I, R> RangeFilter<I, R>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
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

    /// Check if a key is beyond the end bound (used for early termination).
    fn is_key_beyond_end(&self, key: &[u8]) -> bool {
        match self.range.end_bound() {
            Bound::Included(end) => key > end.as_slice(),
            Bound::Excluded(end) => key >= end.as_slice(),
            Bound::Unbounded => false,
        }
    }
}

impl<I, R> Iterator for RangeFilter<I, R>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    R: RangeBounds<Vec<u8>> + Send + Sync,
{
    type Item = Result<(Vec<u8>, Vec<u8>)>;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_filter_inclusive() {
        let data = vec![
            Ok((b"key_005".to_vec(), b"value_005".to_vec())),
            Ok((b"key_010".to_vec(), b"value_010".to_vec())),
            Ok((b"key_015".to_vec(), b"value_015".to_vec())),
            Ok((b"key_020".to_vec(), b"value_020".to_vec())),
            Ok((b"key_025".to_vec(), b"value_025".to_vec())),
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
            Ok((b"key_005".to_vec(), b"value_005".to_vec())),
            Ok((b"key_010".to_vec(), b"value_010".to_vec())),
            Ok((b"key_015".to_vec(), b"value_015".to_vec())),
            Ok((b"key_020".to_vec(), b"value_020".to_vec())),
            Ok((b"key_025".to_vec(), b"value_025".to_vec())),
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
            Ok((b"key_005".to_vec(), b"value_005".to_vec())),
            Err(Error::Decode(
                "test error",
                std::io::Error::new(std::io::ErrorKind::Other, "test"),
            )),
            Ok((b"key_015".to_vec(), b"value_015".to_vec())),
        ];

        let iter = data.into_iter();
        let range = b"key_000".to_vec()..=b"key_020".to_vec();
        let mut filtered = RangeFilter::new(iter, range);

        // First item should be Ok
        assert!(filtered.next().unwrap().is_ok());

        // Second item should be the error
        assert!(filtered.next().unwrap().is_err());
    }
}
