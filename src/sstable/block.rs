use std::io;

use crate::{error::Result, Error};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const RESTART_INTERVAL: usize = 16;

pub struct Builder {
    buffer: Vec<u8>,             // Buffer to hold block data
    restart_positions: Vec<u32>, // Offsets of restart points
    entry_count: usize,          // Number of entries added
    last_key: Vec<u8>,           // Last key added
}

impl Builder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            restart_positions: Vec::new(),
            entry_count: 0,
            last_key: Vec::new(),
        }
    }

    pub fn add_entry(&mut self, key: &[u8], value: &[u8]) {
        let mut shared_prefix_len = 0;

        if self.entry_count % RESTART_INTERVAL == 0 {
            self.restart_positions.push(self.buffer.len() as u32);
        } else {
            shared_prefix_len = self.shared_prefix_length(&self.last_key, key);
        }

        let unshared_key_len = key.len() - shared_prefix_len;

        let mut entry_buf = Vec::with_capacity(2 + 2 + 4 + unshared_key_len + value.len());
        entry_buf
            .write_u16::<BigEndian>(shared_prefix_len as u16)
            .unwrap();
        entry_buf
            .write_u16::<BigEndian>(unshared_key_len as u16)
            .unwrap();
        entry_buf
            .write_u32::<BigEndian>(value.len() as u32)
            .unwrap();
        entry_buf.extend_from_slice(&key[shared_prefix_len..]);
        entry_buf.extend_from_slice(value);

        self.buffer.extend_from_slice(&entry_buf);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);

        self.entry_count += 1;
    }

    pub fn finish(mut self) -> Vec<u8> {
        // Write restart positions
        for pos in &self.restart_positions {
            self.buffer.write_u32::<BigEndian>(*pos).unwrap();
        }
        self.buffer
            .write_u32::<BigEndian>(self.restart_positions.len() as u32)
            .unwrap();
        self.buffer
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn entry_count(&self) -> usize {
        self.entry_count
    }

    fn shared_prefix_length(&self, a: &[u8], b: &[u8]) -> usize {
        let min_len = a.len().min(b.len());
        for i in 0..min_len {
            if a[i] != b[i] {
                return i;
            }
        }
        min_len
    }
}

pub struct Block {
    data: Vec<u8>,
    restart_positions: Vec<u32>,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        use std::io::Cursor;

        let data_len = data.len();

        if data_len < 4 {
            return Err(Error::Decode(
                "block data",
                io::Error::new(io::ErrorKind::UnexpectedEof, "Data too short"),
            ));
        }

        // Read number of restarts (last 4 bytes)
        let num_restarts_offset = data_len - 4;
        let mut cursor = Cursor::new(&data[num_restarts_offset..]);
        let num_restarts = cursor.read_u32::<BigEndian>()? as usize;

        // Calculate the size of the restart positions array
        let restart_array_size = num_restarts * 4;
        if num_restarts_offset < restart_array_size {
            return Err(Error::Decode(
                "block data",
                io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Data too short for restart positions",
                ),
            ));
        }

        // Read restart positions
        let restart_array_offset = num_restarts_offset - restart_array_size;
        let mut cursor = Cursor::new(&data[restart_array_offset..num_restarts_offset]);
        let mut restart_positions = Vec::with_capacity(num_restarts);
        for _ in 0..num_restarts {
            let pos = cursor.read_u32::<BigEndian>()?;
            restart_positions.push(pos);
        }

        Ok(Self {
            data: data[..restart_array_offset].to_vec(),
            restart_positions,
        })
    }

    /// Iterates over the block entries, and returns the value for the given key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = self.iter();
        if let Some((found_key, found_value)) = iter.seek(key) {
            if found_key == key {
                return Ok(Some(found_value));
            }
        }
        Ok(None)
    }

    pub fn iter(&self) -> BlockIterator {
        BlockIterator::new(&self.data, &self.restart_positions)
    }
}

pub struct BlockIterator<'a> {
    data: &'a [u8],
    restart_positions: &'a [u32],
    current_restart: usize,
    current_offset: usize,
    last_key: Vec<u8>,
}

impl<'a> BlockIterator<'a> {
    fn new(data: &'a [u8], restart_positions: &'a [u32]) -> Self {
        Self {
            data,
            restart_positions,
            current_restart: 0,
            current_offset: restart_positions[0] as usize,
            last_key: Vec::new(),
        }
    }

    pub fn seek(&mut self, target: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        // Binary search over restart points
        let mut left = 0;
        let mut right = self.restart_positions.len() - 1;

        while left <= right {
            let mid = (left + right) / 2;
            let offset = self.restart_positions[mid] as usize;
            let (key, _) = self.read_entry_at(offset)?;
            let cmp = key.as_slice().cmp(target);

            if cmp == std::cmp::Ordering::Less {
                left = mid + 1;
            } else if cmp == std::cmp::Ordering::Greater {
                if mid == 0 {
                    break;
                }
                right = mid - 1;
            } else {
                // Exact match
                self.current_restart = mid;
                self.current_offset = offset;
                self.last_key = key.clone();
                return Some((key, self.read_value_at(offset)?));
            }
        }

        // Start linear scan from the last restart point before the target
        if left > 0 {
            self.current_restart = left - 1;
        } else {
            self.current_restart = 0;
        }
        self.current_offset = self.restart_positions[self.current_restart] as usize;
        self.last_key.clear();

        while self.current_offset < self.data.len() {
            // let entry_offset = self.current_offset;
            let (key, value) = self.read_entry()?;
            let cmp = key.as_slice().cmp(target);

            if cmp == std::cmp::Ordering::Greater {
                // Key not found
                return None;
            } else if cmp == std::cmp::Ordering::Equal {
                // Key found
                return Some((key, value));
            }
        }

        None
    }

    fn read_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let result = self.read_entry_at(self.current_offset);
        if let Some((key, _value)) = &result {
            self.last_key = key.clone();
        }
        result
    }

    fn read_entry_at(&mut self, offset: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        let mut pos = offset;

        if pos + 8 > self.data.len() {
            return None;
        }

        // Read shared key length
        let shared_len = (&self.data[pos..]).read_u16::<BigEndian>().ok()? as usize;
        pos += 2;

        // Read unshared key length
        let unshared_len = (&self.data[pos..]).read_u16::<BigEndian>().ok()? as usize;
        pos += 2;

        // Read value length
        let value_len = (&self.data[pos..]).read_u32::<BigEndian>().ok()? as usize;
        pos += 4;

        if pos + unshared_len + value_len > self.data.len() {
            return None;
        }

        // Reconstruct key
        let mut key = Vec::with_capacity(shared_len + unshared_len);
        key.extend_from_slice(&self.last_key[..shared_len]);
        key.extend_from_slice(&self.data[pos..pos + unshared_len]);
        pos += unshared_len;

        // Read value
        let value = self.data[pos..pos + value_len].to_vec();
        pos += value_len;

        self.current_offset = pos;
        self.last_key = key.clone();

        Some((key, value))
    }

    fn read_value_at(&self, offset: usize) -> Option<Vec<u8>> {
        let mut pos = offset;

        if pos + 8 > self.data.len() {
            return None;
        }

        // Read shared key length
        let _shared_len = (&self.data[pos..]).read_u16::<BigEndian>().ok()? as usize;
        pos += 2;

        // Read unshared key length
        let unshared_len = (&self.data[pos..]).read_u16::<BigEndian>().ok()? as usize;
        pos += 2;

        // Read value length
        let value_len = (&self.data[pos..]).read_u32::<BigEndian>().ok()? as usize;
        pos += 4;

        // Skip key suffix
        pos += unshared_len;

        if pos + value_len > self.data.len() {
            return None;
        }

        // Read value
        Some(self.data[pos..pos + value_len].to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder_and_reader() {
        // Create a BlockBuilder
        let mut builder = Builder::new();

        // Sample entries
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"application".to_vec(), b"software".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
            (b"band".to_vec(), b"music".to_vec()),
            (b"bandana".to_vec(), b"clothing".to_vec()),
        ];

        // Add entries to the block
        for (key, value) in &entries {
            builder.add_entry(key, value);
        }

        // Finish the block
        let block_data = builder.finish();

        // Create a Block from the block data
        let block = Block::new(block_data).expect("Failed to create block");

        // Create a BlockIterator
        let mut iter = block.iter();

        // Test seeking to keys
        for (key, value) in entries {
            if let Some((found_key, found_value)) = iter.seek(&key) {
                assert_eq!(found_key, key);
                assert_eq!(found_value, value);
            } else {
                panic!("Key {:?} not found in block", String::from_utf8_lossy(&key));
            }
        }

        // Test seeking to a non-existent key
        let non_existent_key = b"unknown";
        if let Some((_found_key, _found_value)) = iter.seek(non_existent_key) {
            panic!(
                "Non-existent key {:?} unexpectedly found in block",
                String::from_utf8_lossy(non_existent_key)
            );
        }
    }
}
