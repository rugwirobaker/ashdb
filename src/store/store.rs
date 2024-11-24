use crate::Error;

pub trait Store: Send {
    /// The iterator type returned by scan and scan_prefix methods.
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: 'a;

    /// Inserts or updates a key-value pair.
    fn put(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error>;

    /// Retrieves the value for a given key.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;

    /// Scans key-value pairs within the given range.
    fn scan<'a>(
        &'a self,
        range: impl std::ops::RangeBounds<&'a [u8]> + 'a,
    ) -> Result<Self::ScanIterator<'a>, Error>;

    /// Scans all key-value pairs with the given prefix.
    fn scan_prefix<'a>(&'a self, prefix: &'a [u8]) -> Result<Self::ScanIterator<'a>, Error>;
}

pub trait ScanIterator: Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> {}

impl<I> ScanIterator for I where I: Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> {}
