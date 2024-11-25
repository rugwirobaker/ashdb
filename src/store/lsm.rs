// use std::ops::RangeBounds;

// use crate::{memtable::Memtable, Error};

// use super::store::{ScanIterator, Store};

// pub struct LsmStore {
//     memtable: Memtable, // Single active Memtable
// }

// impl LsmStore {
//     pub fn new(dir: &str) -> Result<Self, Error> {
//         // Initialize the Memtable with ID 0
//         let memtable = Memtable::new(dir, 0)?;
//         Ok(Self { memtable })
//     }
// }

// impl Store for LsmStore {
//     type ScanIterator<'a> = impl ScanIterator + 'a;

//     fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<(), Error> {
//         self.memtable.put(key.to_vec(), Some(value))
//     }

//     fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
//         Ok(self.memtable.get(key).flatten())
//     }

//     fn scan<'a>(&'a self, range: impl RangeBounds<Vec<u8>> + 'a) -> Self::ScanIterator<'a> {
//         self.memtable.scan(range).expect("Scan failed") // Expect is fine for now
//     }
// }
