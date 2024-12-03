use std::convert::TryFrom;
use std::io::{Read, Write};
use std::ops::RangeBounds;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::Result;
use crate::Error;

#[derive(Debug)]
pub struct Entry<'a> {
    pub index: usize,  // The index of the entry in the sparse index
    pub key: &'a [u8], // The key associated with this entry
    pub offset: u64,   // The offset associated with this entry
    pub size: u64,     // Size of the block
}

#[derive(Debug, Clone)]
pub struct Index {
    entries: Vec<(Vec<u8>, u64, u64)>, // Key, block offset, block size
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

    /// Finds the closest entry that matches or precedes the given key
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
    pub fn range(&self, range: impl RangeBounds<Vec<u8>>) -> Vec<(u64, u64)> {
        let mut start_index = 0;
        let mut end_index = self.entries.len();

        if let std::ops::Bound::Included(start) = range.start_bound() {
            start_index = self
                .entries
                .binary_search_by(|entry| entry.0.as_slice().cmp(start))
                .unwrap_or_else(|idx| idx);
        } else if let std::ops::Bound::Excluded(start) = range.start_bound() {
            start_index = self
                .entries
                .binary_search_by(|entry| entry.0.as_slice().cmp(start))
                .unwrap_or_else(|idx| idx);
            if start_index < self.entries.len() {
                start_index += 1;
            }
        }

        if let std::ops::Bound::Included(end) = range.end_bound() {
            end_index = self
                .entries
                .binary_search_by(|entry| entry.0.as_slice().cmp(end))
                .unwrap_or_else(|idx| idx + 1);
        } else if let std::ops::Bound::Excluded(end) = range.end_bound() {
            end_index = self
                .entries
                .binary_search_by(|entry| entry.0.as_slice().cmp(end))
                .unwrap_or_else(|idx| idx);
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
