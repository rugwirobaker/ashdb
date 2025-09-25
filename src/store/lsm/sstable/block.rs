use std::{cmp::Ordering, io, sync::Arc};

use crate::{error::Result, Error};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

const RESTART_INTERVAL: usize = 16;

// --- Builder (No changes needed) ---
pub struct Builder {
    buffer: Vec<u8>,
    restart_positions: Vec<u32>,
    entry_count: usize,
    last_key: Vec<u8>,
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
        let shared_prefix_len = if self.entry_count % RESTART_INTERVAL == 0 {
            self.restart_positions.push(self.buffer.len() as u32);
            0
        } else {
            self.shared_prefix_length(&self.last_key, key)
        };

        let unshared_key_len = key.len() - shared_prefix_len;

        let mut entry_buf = Vec::with_capacity(8 + unshared_key_len + value.len());
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
        a.iter().zip(b.iter()).take_while(|&(a, b)| a == b).count()
    }
}

// --- Block (Simplified `get` method) ---
#[derive(Clone)]
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

        let num_restarts_offset = data_len - 4;
        let mut cursor = Cursor::new(&data[num_restarts_offset..]);
        let num_restarts = cursor.read_u32::<BigEndian>()? as usize;

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

        let restart_array_offset = num_restarts_offset - restart_array_size;
        let mut cursor = Cursor::new(&data[restart_array_offset..num_restarts_offset]);
        let mut restart_positions = Vec::with_capacity(num_restarts);
        for _ in 0..num_restarts {
            restart_positions.push(cursor.read_u32::<BigEndian>()?);
        }

        Ok(Self {
            data: data[..restart_array_offset].to_vec(),
            restart_positions,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = Arc::new(self.clone()).iter();
        iter.seek(key)?;
        match iter.next() {
            Some(Ok((found_key, value))) if found_key == key => Ok(Some(value)),
            _ => Ok(None),
        }
    }

    pub fn iter(self: Arc<Self>) -> BlockIterator {
        BlockIterator::new(self)
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

// --- BlockIterator (Refactored `seek` and helpers) ---
pub struct BlockIterator {
    block: Arc<Block>,
    current_offset: usize,
    last_key: Vec<u8>,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        let initial_offset = block.restart_positions.first().map_or(0, |&p| p as usize);
        Self {
            block,
            current_offset: initial_offset,
            last_key: Vec::new(),
        }
    }

    /// Helper to read just the key at a given offset for comparisons.
    /// It does not modify the iterator's state.
    fn read_key_at(&self, offset: usize) -> Result<Vec<u8>> {
        let mut pos = offset;
        let data = &self.block.data;

        // A restart point key has no shared prefix.
        let shared_len = (&data[pos..]).read_u16::<BigEndian>()? as usize;
        pos += 2;
        let unshared_len = (&data[pos..]).read_u16::<BigEndian>()? as usize;
        pos += 2;
        let value_len = (&data[pos..]).read_u32::<BigEndian>()? as usize;
        pos += 4;

        if shared_len != 0 {
            return Err(Error::IndexCorruption(
                "Restart point key has a shared prefix".into(),
            ));
        }
        if pos + unshared_len + value_len > data.len() {
            return Err(Error::IndexCorruption("Entry out of bounds".into()));
        }

        Ok(data[pos..pos + unshared_len].to_vec())
    }

    /// Seeks the iterator to the first key that is >= `target`.
    /// Positions the iterator so that the next call to `next()` will return
    /// the first key >= target, or None if no such key exists.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Store all positions and keys from the current restart point to target
        let mut entries_to_target = Vec::new();

        // 1. Binary search restart points to find the right region.
        let mut left = 0;
        let mut right = self.block.restart_positions.len();

        while left < right {
            let mid = left + (right - left) / 2;
            match self.read_key_at(self.block.restart_positions[mid] as usize) {
                Ok(key) => match key.as_slice().cmp(target) {
                    Ordering::Less => left = mid + 1,
                    _ => right = mid,
                },
                Err(e) => return Err(e),
            }
        }

        // Declare and initialize restart_index just before it's needed.
        let restart_index = left.saturating_sub(1);

        // 2. Position the iterator at the found restart point.
        self.current_offset = self.block.restart_positions[restart_index] as usize;
        self.last_key.clear();

        // 3. Scan from restart point, collecting entries until we find target
        while let Some(result) = self.next() {
            match result {
                Ok((key, value)) => {
                    if key.as_slice() >= target {
                        // Found the target! Now we need to position iterator
                        // to return this entry on the next `next()` call.

                        // Reset to restart point
                        self.current_offset = self.block.restart_positions[restart_index] as usize;
                        self.last_key.clear();

                        // Advance through all the entries before our target
                        for _ in &entries_to_target {
                            self.next(); // Consume the entries before target
                        }

                        return Ok(());
                    }
                    // Store this entry and continue
                    entries_to_target.push((key, value));
                }
                Err(e) => return Err(e),
            }
        }

        // If we get here, no key >= target was found
        // Iterator is already positioned at end, so next() will return None
        Ok(())
    }
}

/// An iterator that chains multiple blocks together without range filtering.
/// This handles the low-level block loading and iteration across multiple blocks.
pub struct MultiBlockIterator {
    reader: std::fs::File,
    blocks: Vec<(u64, u64)>, // Block offset and size
    current_block_iter: Option<BlockIterator>,
    current_block_index: usize,
}

impl MultiBlockIterator {
    pub fn new(reader: std::fs::File, blocks: Vec<(u64, u64)>) -> Result<Self> {
        Ok(Self {
            reader,
            blocks,
            current_block_iter: None,
            current_block_index: 0,
        })
    }

    /// Seek to the first key >= target in the first block
    pub fn seek_first_block(&mut self, target: &[u8]) -> Result<()> {
        if !self.blocks.is_empty() && self.current_block_index == 0 {
            self.load_current_block()?;
            if let Some(iter) = &mut self.current_block_iter {
                iter.seek(target)?;
            }
        }
        Ok(())
    }

    fn load_current_block(&mut self) -> Result<()> {
        if self.current_block_index >= self.blocks.len() {
            self.current_block_iter = None;
            return Ok(());
        }

        use std::io::{Read, Seek, SeekFrom};

        // Get the current block's offset and size
        let (offset, size) = self.blocks[self.current_block_index];

        // Read the block
        let mut block_data = vec![0u8; size as usize];
        self.reader.seek(SeekFrom::Start(offset))?;
        self.reader.read_exact(&mut block_data)?;

        let block = std::sync::Arc::new(Block::new(block_data)?);
        self.current_block_iter = Some(block.iter());
        self.current_block_index += 1;

        Ok(())
    }
}

impl Iterator for MultiBlockIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get the next entry from the current block iterator
            if let Some(iter) = &mut self.current_block_iter {
                if let Some(entry) = iter.next() {
                    return Some(entry);
                }
            }

            // Current block iterator is exhausted, load the next block
            if let Err(e) = self.load_current_block() {
                return Some(Err(e));
            }

            // If no more blocks, we're done
            self.current_block_iter.as_ref()?;
        }
    }
}

impl Iterator for BlockIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.block.data.len() {
            return None;
        }

        let mut pos = self.current_offset;
        let data = &self.block.data;

        macro_rules! try_read {
            ($expr:expr, $field:expr) => {
                match $expr {
                    Ok(val) => val,
                    Err(e) => return Some(Err(Error::Decode($field, e))),
                }
            };
        }

        let shared_len = try_read!((&data[pos..]).read_u16::<BigEndian>(), "shared_len") as usize;
        pos += 2;
        let unshared_len =
            try_read!((&data[pos..]).read_u16::<BigEndian>(), "unshared_len") as usize;
        pos += 2;
        let value_len = try_read!((&data[pos..]).read_u32::<BigEndian>(), "value_len") as usize;
        pos += 4;

        if shared_len > self.last_key.len() || pos + unshared_len + value_len > data.len() {
            return Some(Err(Error::IndexCorruption(
                "Block entry out of bounds".into(),
            )));
        }

        let mut key = Vec::with_capacity(shared_len + unshared_len);
        key.extend_from_slice(&self.last_key[..shared_len]);
        key.extend_from_slice(&data[pos..pos + unshared_len]);
        pos += unshared_len;

        let value = data[pos..pos + value_len].to_vec();
        pos += value_len;

        self.current_offset = pos;
        self.last_key = key.clone();

        Some(Ok((key, value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder_and_reader() {
        let mut builder = Builder::new();
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"application".to_vec(), b"software".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
            (b"band".to_vec(), b"music".to_vec()),
            (b"bandana".to_vec(), b"clothing".to_vec()),
        ];

        for (key, value) in &entries {
            builder.add_entry(key, value);
        }
        let block_data = builder.finish();
        let block = Arc::new(Block::new(block_data).expect("Failed to create block"));

        // Test `get` for exact matches
        for (key, value) in &entries {
            let result = block.get(key).unwrap().unwrap();
            assert_eq!(&result, value);
        }

        // Test `get` for non-existent key
        assert!(block.get(b"unknown").unwrap().is_none());

        // Test `seek`
        let mut iter = block.iter();

        // Seek to an exact match
        iter.seek(b"band").unwrap();
        let (found_key, _) = iter.next().unwrap().unwrap();
        assert_eq!(found_key, b"band");

        // Seek to a key that doesn't exist, should find the next one
        iter.seek(b"bana").unwrap();
        let (found_key, _) = iter.next().unwrap().unwrap();
        assert_eq!(found_key, b"banana");
    }
}
