pub mod lsm;

use crate::error::Result;
use std::ops::RangeBounds;

pub trait Store {
    /// The iterator returned by scan().
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: 'a;

    /// Inserts or updates a key-value pair.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Retrieves the value for a given key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key-value pairs.
    fn scan<'a>(&'a self, range: impl RangeBounds<Vec<u8>> + 'a + Clone) -> Self::ScanIterator<'a>;

    /// Iterates over all key-value pairs starting with the given prefix.
    fn scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> Self::ScanIterator<'a> {
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
}

pub trait ScanIterator: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

impl<I> ScanIterator for I where I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
