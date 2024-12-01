use std::convert::TryFrom;
use std::io::{Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::Result;
use crate::Error;

#[derive(Debug)]
pub struct Entry<'a> {
    pub index: usize,  // The index of the entry in the sparse index
    pub key: &'a [u8], // The key associated with this entry
    pub offset: u64,   // The offset associated with this entry
}

#[derive(Debug, Clone)]
pub struct Index {
    entries: Vec<(Vec<u8>, u64)>, // Key and block offset
}

impl Index {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
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

            entries.push((key, block_offset));
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

        for (key, offset) in self.entries {
            // Write key length (u16)
            buffer.write_u16::<BigEndian>(key.len() as u16)?;
            // Write key bytes
            buffer.write_all(&key)?;
            // Write block offset (u64)
            buffer.write_u64::<BigEndian>(offset)?;
        }
        Ok(buffer)
    }
}

impl Index {
    /// push inserts a new entry into the index
    pub fn push(&mut self, key: Vec<u8>, offset: u64) {
        self.entries.push((key, offset));
    }

    /// Finds the closest entry that matches or precedes the given key
    pub fn find(&self, key: &[u8]) -> Option<Entry> {
        let mut left = 0;
        let mut right = self.entries.len();

        while left < right {
            let mid = (left + right) / 2;
            let cmp = self.entries[mid].0.as_slice().cmp(key);

            if cmp == std::cmp::Ordering::Less {
                left = mid + 1;
            } else if cmp == std::cmp::Ordering::Greater {
                if mid == 0 {
                    break;
                }
                right = mid;
            } else {
                return Some(Entry {
                    index: mid,
                    key: &self.entries[mid].0,
                    offset: self.entries[mid].1,
                });
            }
        }
        if left > 0 {
            let idx = left - 1;
            Some(Entry {
                index: idx,
                key: &self.entries[idx].0,
                offset: self.entries[idx].1,
            })
        } else {
            None
        }
    }

    /// Gets the offset for a given entry index
    pub fn get(&self, index: usize) -> Option<u64> {
        self.entries.get(index).map(|(_, offset)| *offset)
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
