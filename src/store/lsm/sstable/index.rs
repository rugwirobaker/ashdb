//! Sparse index implementation for efficient SSTable block lookup.

use std::convert::TryFrom;
use std::io::{Read, Write};
use std::ops::RangeBounds;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::Result;
use crate::Error;

/// An entry in the sparse index, representing a single data block.
#[derive(Debug)]
pub struct Entry<'a> {
    pub index: usize,  // The index of the entry in the sparse index
    pub key: &'a [u8], // The key associated with this entry
    pub offset: u64,   // The offset associated with this entry
    pub size: u64,     // Size of the block
}

/// The sparse index for an SSTable. It contains an ordered list of entries,
/// where each entry corresponds to a data block.
#[derive(Debug, Clone)]
pub struct Index {
    /// The entries in the index, sorted by key. Each entry is a tuple of
    /// `(key, block_offset, block_size)`.
    entries: Vec<(Vec<u8>, u64, u64)>,
}

impl Index {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl Index {
    /// push inserts a new entry into the index
    pub fn push(&mut self, key: Vec<u8>, offset: u64, size: u64) {
        self.entries.push((key, offset, size));
    }

    /// Finds the data block that may contain the given key.
    ///
    /// This method performs a binary search on the index entries. If an exact match
    /// is found, it returns the corresponding entry. Otherwise, it returns the
    /// entry for the block that *precedes* the block where the key would be, which
    /// is the correct block to search.
    pub fn find(&self, key: &[u8]) -> Option<Entry> {
        let mut low = 0;
        let mut high = self.entries.len();

        // Perform binary search
        while low < high {
            let mid = (low + high) / 2;

            match self.entries[mid].0.as_slice().cmp(key) {
                std::cmp::Ordering::Less => low = mid + 1, // Narrow search to right half
                std::cmp::Ordering::Greater => {
                    if mid == 0 {
                        break; // Exit if the key is smaller than all entries
                    }
                    high = mid; // Narrow search to left half
                }
                std::cmp::Ordering::Equal => {
                    // Return the exact match if found
                    return Some(Entry {
                        index: mid,
                        key: &self.entries[mid].0,
                        offset: self.entries[mid].1,
                        size: self.entries[mid].2,
                    });
                }
            }
        }
        // If no exact match, return the closest preceding entry
        if low > 0 {
            let idx = low - 1;
            Some(Entry {
                index: idx,
                key: &self.entries[idx].0,
                offset: self.entries[idx].1,
                size: self.entries[idx].2, // Include block
            })
        } else {
            None
        }
    }

    /// Gets the offset for a given entry index
    pub fn get(&self, index: usize) -> Option<(u64, u64)> {
        self.entries
            .get(index)
            .map(|(_, offset, size)| (*offset, *size))
    }

    /// Returns the number of entries in the index
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn first_key(&self) -> Option<&[u8]> {
        self.entries.first().map(|(key, _, _)| key.as_slice())
    }

    /// Returns the handle (offset and size) for the last block in the SSTable.
    pub fn last_block_handle(&self) -> Option<(u64, u64)> {
        self.entries
            .last()
            .map(|(_, offset, size)| (*offset, *size))
    }

    // /// Gets the key for a given entry index
    // pub fn key(&self, index: usize) -> Option<&[u8]> {
    //     self.entries.get(index).map(|(key, _)| key.as_slice())
    // }

    // /// Checks if the index is empty
    // pub fn is_empty(&self) -> bool {
    //     self.entries.is_empty()
    // }
}

impl Index {
    // In src/store/lsm/sstable/index.rs

    pub fn range(&self, range: impl RangeBounds<Vec<u8>>) -> Vec<(u64, u64)> {
        let start_index = match range.start_bound() {
            std::ops::Bound::Included(start) => {
                // Find the first block that could contain the start key
                match self
                    .entries
                    .binary_search_by(|entry| entry.0.as_slice().cmp(start))
                {
                    Ok(idx) => idx, // Key found exactly, start with this block
                    Err(idx) => {
                        // Key not found, idx is the insertion point
                        // We want the block that could contain this key, which is the previous block
                        idx.saturating_sub(1)
                    }
                }
            }
            std::ops::Bound::Excluded(start) => {
                // Find the first block that could contain keys > start
                match self
                    .entries
                    .binary_search_by(|entry| entry.0.as_slice().cmp(start))
                {
                    Ok(idx) => {
                        // Exact match found. For exclusion, we want blocks that may contain keys > start
                        // This could be the same block (if it has keys > start) or the next block
                        idx
                    }
                    Err(idx) => {
                        // Key not found, idx is the insertion point
                        // We want the block that could contain this key, which is the previous block
                        idx.saturating_sub(1)
                    }
                }
            }
            std::ops::Bound::Unbounded => 0,
        };

        let end_index = match range.end_bound() {
            std::ops::Bound::Included(end) => {
                match self
                    .entries
                    .binary_search_by(|entry| entry.0.as_slice().cmp(end))
                {
                    Ok(idx) => idx + 1, // Key found, include it by specifying the next index as the end.
                    Err(idx) => idx,    // Key not found, the insertion point is the exclusive end.
                }
            }
            std::ops::Bound::Excluded(end) => {
                match self
                    .entries
                    .binary_search_by(|entry| entry.0.as_slice().cmp(end))
                {
                    Ok(idx) => idx,  // Key found, exclude it.
                    Err(idx) => idx, // Key not found, the insertion point is the correct end.
                }
            }
            std::ops::Bound::Unbounded => self.entries.len(),
        };

        if start_index >= end_index {
            return Vec::new();
        }

        self.entries[start_index..end_index]
            .iter()
            .map(|(_, offset, size)| (*offset, *size))
            .collect()
    }
}

impl Default for Index {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<&[u8]> for Index {
    type Error = Error;

    fn try_from(buffer: &[u8]) -> Result<Self> {
        let mut cursor = std::io::Cursor::new(buffer);
        let mut entries = Vec::new();

        while (cursor.position() as usize) < buffer.len() {
            // Read key length (u16)
            let key_len = cursor
                .read_u16::<BigEndian>()
                .map_err(|e| Error::ReadError("key length in index block", e))?
                as usize;

            // Read key bytes
            let mut key = vec![0u8; key_len];
            cursor
                .read_exact(&mut key)
                .map_err(|e| Error::ReadError("key in index block", e))?;

            let block_offset = cursor
                .read_u64::<BigEndian>()
                .map_err(|e| Error::ReadError("block offset in index block", e))?;

            let block_size = cursor
                .read_u64::<BigEndian>()
                .map_err(|e| Error::ReadError("block size in index block", e))?;

            entries.push((key, block_offset, block_size));
        }
        Ok(Self { entries })
    }
}

impl TryFrom<&Index> for Vec<u8> {
    type Error = Error;

    fn try_from(index: &Index) -> Result<Vec<u8>> {
        index.try_into()
    }
}

// it might be better to serialize directly to a Writer instead of try_into
impl TryInto<Vec<u8>> for Index {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        for (key, offset, size) in self.entries {
            buffer.write_u16::<BigEndian>(key.len() as u16)?; // Write key length
            buffer.write_all(&key)?; // Write key bytes
            buffer.write_u64::<BigEndian>(offset)?; // Write block offset
            buffer.write_u64::<BigEndian>(size)?; // Write block size
        }
        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    fn create_test_index() -> Index {
        let mut index = Index::new();
        index.push(b"apple".to_vec(), 0, 100);
        index.push(b"banana".to_vec(), 100, 100);
        index.push(b"cherry".to_vec(), 200, 100);
        index
    }

    #[test]
    fn test_find_exact_match() {
        let index = create_test_index();
        let entry = index.find(b"banana").unwrap();
        assert_eq!(entry.key, b"banana");
        assert_eq!(entry.offset, 100);
    }

    #[test]
    fn test_find_between_entries() {
        let index = create_test_index();
        let entry = index.find(b"apricot").unwrap();
        assert_eq!(entry.key, b"apple");
        assert_eq!(entry.offset, 0);
    }

    #[test]
    fn test_find_smaller_than_all() {
        let index = create_test_index();
        assert!(index.find(b"ant").is_none());
    }

    #[test]
    fn test_find_larger_than_all() {
        let index = create_test_index();
        let entry = index.find(b"date").unwrap();
        assert_eq!(entry.key, b"cherry");
        assert_eq!(entry.offset, 200);
    }

    #[test]
    fn test_find_empty_index() {
        let index = Index::new();
        assert!(index.find(b"any").is_none());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let original_index = create_test_index();
        let buffer: Vec<u8> = original_index.clone().try_into().unwrap();
        let deserialized_index = Index::try_from(buffer.as_slice()).unwrap();

        assert_eq!(original_index.entries, deserialized_index.entries);
    }

    #[test]
    fn test_empty_serialization_roundtrip() {
        let original_index = Index::new();
        let buffer: Vec<u8> = original_index.clone().try_into().unwrap();
        let deserialized_index = Index::try_from(buffer.as_slice()).unwrap();

        assert_eq!(original_index.entries, deserialized_index.entries);
    }

    #[test]
    fn test_range_full() {
        let index = create_test_index();
        let range = index.range(..);
        assert_eq!(range.len(), 3);
        assert_eq!(range[0], (0, 100));
        assert_eq!(range[1], (100, 100));
        assert_eq!(range[2], (200, 100));
    }

    #[test]
    fn test_range_partial_inclusive() {
        let index = create_test_index();
        // Range includes "banana" and "cherry"
        let range = index.range(b"banana".to_vec()..=b"cherry".to_vec());
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], (100, 100)); // banana
        assert_eq!(range[1], (200, 100)); // cherry
    }

    #[test]
    fn test_range_partial_exclusive() {
        let index = create_test_index();
        // Range includes "apple" and "banana", but excludes "cherry"
        let range = index.range(b"apple".to_vec()..b"cherry".to_vec());
        assert_eq!(range.len(), 2);
        assert_eq!(range[0], (0, 100)); // apple
        assert_eq!(range[1], (100, 100)); // banana
    }
}
