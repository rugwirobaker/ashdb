pub mod lsm;

use crate::error::Result;
use std::ops::RangeBounds;

pub trait Store: Send + Sync {
    /// The iterator returned by scan().
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: Sized + 'a; // omit in trait objects, for dyn compatibility

    /// Inserts or updates a key-value pair.
    fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Retrieves the value for a given key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key-value pairs.
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

    /// Synchronizes buffered WAL writes to disk. Writes (set operations) are not
    /// guaranteed to be durable until this is called.
    fn sync(&self) -> Result<()>;
}

pub trait ScanIterator: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}

impl<I> ScanIterator for I where I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> {}
