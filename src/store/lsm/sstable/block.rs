use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;

use crate::{error::Result, Error};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

/// Operation type for SSTable entries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Put = 0x00,
    Delete = 0x01,
}

impl OpType {
    fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x00 => Ok(OpType::Put),
            0x01 => Ok(OpType::Delete),
            _ => Err(Error::InvalidData(format!(
                "Unknown operation type: {}",
                byte
            ))),
        }
    }

    fn to_byte(self) -> u8 {
        self as u8
    }
}

pub struct Builder {
    buffer: Vec<u8>,
    entry_count: usize,
}

impl Builder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            entry_count: 0,
        }
    }

    /// Add an entry to the block.
    /// Format: [key_len:u16][op:u8][value_len:u32][key][value]
    pub fn add_entry(&mut self, key: &[u8], value: Option<&[u8]>) {
        let key_len = key.len();
        let op_type = if value.is_some() {
            OpType::Put
        } else {
            OpType::Delete
        };
        let value_len = value.map_or(0, |v| v.len());

        let mut entry_buf = Vec::with_capacity(7 + key_len + value_len);
        entry_buf.write_u16::<BigEndian>(key_len as u16).unwrap();
        entry_buf.write_u8(op_type.to_byte()).unwrap();
        entry_buf.write_u32::<BigEndian>(value_len as u32).unwrap();
        entry_buf.extend_from_slice(key);
        if let Some(v) = value {
            entry_buf.extend_from_slice(v);
        }

        self.buffer.extend_from_slice(&entry_buf);
        self.entry_count += 1;
    }

    pub fn finish(self) -> Vec<u8> {
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
}

// --- Block ---
#[derive(Clone)]
pub struct Block {
    data: Vec<u8>,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        Ok(Self { data })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut iter = Arc::new(self.clone()).iter();
        iter.seek(key)?;
        match iter.next() {
            Some(Ok((found_key, value))) if found_key == key => Ok(value),
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

// --- BlockIterator ---
pub struct BlockIterator {
    block: Arc<Block>,
    current_offset: usize,
    // Lazy offset tracking for reverse iteration
    entry_offsets: Vec<usize>,
    backward_index: Option<usize>,
    backward_initialized: bool,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            current_offset: 0,
            entry_offsets: vec![0], // Start with first entry at offset 0
            backward_index: None,
            backward_initialized: false,
        }
    }

    /// Helper to read just the key at a given offset for comparisons.
    /// Format: [key_len:u16][op:u8][value_len:u32][key][value]
    fn read_key_at(&self, offset: usize) -> Result<Vec<u8>> {
        let mut pos = offset;
        let data = &self.block.data;

        let key_len = (&data[pos..]).read_u16::<BigEndian>()? as usize;
        pos += 2;
        let _op_type = OpType::from_byte((&data[pos..]).read_u8()?)?;
        pos += 1;
        let value_len = (&data[pos..]).read_u32::<BigEndian>()? as usize;
        pos += 4;

        if pos + key_len + value_len > data.len() {
            return Err(Error::InvalidData("Entry out of bounds".into()));
        }

        Ok(data[pos..pos + key_len].to_vec())
    }

    /// Seeks the iterator to the first key that is >= `target`.
    /// Positions the iterator so that the next call to `next()` will return
    /// the first key >= target, or None if no such key exists.
    pub fn seek(&mut self, target: &[u8]) -> Result<()> {
        // Reset to beginning
        self.current_offset = 0;

        // Linear scan to find first key >= target
        let mut temp_offset = 0;
        while temp_offset < self.block.data.len() {
            match self.read_key_at(temp_offset) {
                Ok(key) => {
                    if key.as_slice() >= target {
                        // Found it, position iterator here
                        self.current_offset = temp_offset;
                        return Ok(());
                    }
                    // Skip to next entry
                    temp_offset += self.entry_size_at(temp_offset)?;
                }
                Err(_) => {
                    // Reached end or error
                    self.current_offset = self.block.data.len();
                    return Ok(());
                }
            }
        }

        // No key >= target found
        self.current_offset = self.block.data.len();
        Ok(())
    }

    /// Calculate the size of an entry at a given offset
    fn entry_size_at(&self, offset: usize) -> Result<usize> {
        let data = &self.block.data;
        let mut pos = offset;

        let key_len = (&data[pos..]).read_u16::<BigEndian>()? as usize;
        pos += 2;
        let _op_type = (&data[pos..]).read_u8()?;
        pos += 1;
        let value_len = (&data[pos..]).read_u32::<BigEndian>()? as usize;

        Ok(7 + key_len + value_len)
    }

    /// Helper to parse a complete entry at the given offset.
    /// Returns (key, value).
    fn read_entry_at(&self, offset: usize) -> Result<(Vec<u8>, Option<Vec<u8>>)> {
        let mut pos = offset;
        let data = &self.block.data;

        let key_len = (&data[pos..]).read_u16::<BigEndian>()? as usize;
        pos += 2;
        let op_type = OpType::from_byte((&data[pos..]).read_u8()?)?;
        pos += 1;
        let value_len = (&data[pos..]).read_u32::<BigEndian>()? as usize;
        pos += 4;

        if pos + key_len + value_len > data.len() {
            return Err(Error::InvalidData("Entry out of bounds".into()));
        }

        let key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        let value = match op_type {
            OpType::Delete => None,
            OpType::Put => Some(data[pos..pos + value_len].to_vec()),
        };

        Ok((key, value))
    }
}

/// An iterator that chains multiple blocks together without range filtering.
/// This handles the low-level block loading and iteration across multiple blocks.
pub struct MultiBlockIterator {
    reader: std::fs::File,
    blocks: Vec<(u64, u64)>, // Block offset and size

    // Forward iteration state
    current_block_iter: Option<BlockIterator>,
    current_block_index: usize,

    // Backward iteration state (lazy initialization)
    backward_block_iter: Option<BlockIterator>,
    backward_block_index: Option<usize>,
    backward_initialized: bool,
}

impl MultiBlockIterator {
    pub fn new(reader: std::fs::File, blocks: Vec<(u64, u64)>) -> Result<Self> {
        Ok(Self {
            reader,
            blocks,
            current_block_iter: None,
            current_block_index: 0,
            backward_block_iter: None,
            backward_block_index: None,
            backward_initialized: false,
        })
    }

    /// Seek to the first key >= target in the first block
    pub fn seek_first_block(&mut self, target: &[u8]) -> Result<()> {
        if !self.blocks.is_empty() && self.current_block_index == 0 {
            self.load_forward_block()?;
            if let Some(iter) = &mut self.current_block_iter {
                iter.seek(target)?;
            }
        }
        Ok(())
    }

    fn load_forward_block(&mut self) -> Result<()> {
        if self.current_block_index >= self.blocks.len() {
            self.current_block_iter = None;
            return Ok(());
        }

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

    fn load_backward_block(&mut self) -> Result<()> {
        // Get the backward block index (already decremented by caller)
        let idx = match self.backward_block_index {
            Some(i) => i,
            None => {
                self.backward_block_iter = None;
                return Ok(());
            }
        };

        if idx >= self.blocks.len() {
            self.backward_block_iter = None;
            return Ok(());
        }

        // Get the block's offset and size
        let (offset, size) = self.blocks[idx];

        // Read the block using a cloned file handle
        let mut reader = self.reader.try_clone()?;
        let mut block_data = vec![0u8; size as usize];
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut block_data)?;

        let block = std::sync::Arc::new(Block::new(block_data)?);
        self.backward_block_iter = Some(block.iter());

        Ok(())
    }
}

impl Iterator for MultiBlockIterator {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Try to get the next entry from the current block iterator
            if let Some(iter) = &mut self.current_block_iter {
                if let Some(entry) = iter.next() {
                    return Some(entry);
                }
            }

            // Current block iterator is exhausted, load the next block
            if let Err(e) = self.load_forward_block() {
                return Some(Err(e));
            }

            // If no more blocks, we're done
            self.current_block_iter.as_ref()?;
        }
    }
}

impl DoubleEndedIterator for MultiBlockIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Lazy initialization: set backward block index on first call
        if !self.backward_initialized {
            self.backward_initialized = true;
            if self.blocks.is_empty() {
                return None;
            }
            // Start from the last block
            self.backward_block_index = Some(self.blocks.len() - 1);
            // Load the last block
            if let Err(e) = self.load_backward_block() {
                return Some(Err(e));
            }
        }

        loop {
            // Try to get the previous entry from the current backward block iterator
            if let Some(iter) = &mut self.backward_block_iter {
                if let Some(entry) = iter.next_back() {
                    return Some(entry);
                }
            }

            // Current block iterator is exhausted, move to previous block
            match self.backward_block_index {
                Some(0) => {
                    // We're at the first block and it's exhausted
                    self.backward_block_index = None;
                    return None;
                }
                Some(idx) => {
                    // Move to previous block
                    self.backward_block_index = Some(idx - 1);
                    if let Err(e) = self.load_backward_block() {
                        return Some(Err(e));
                    }
                }
                None => {
                    // Already exhausted
                    return None;
                }
            }
        }
    }
}

impl Iterator for BlockIterator {
    type Item = Result<(Vec<u8>, Option<Vec<u8>>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.block.data.len() {
            return None;
        }

        // Read entry at current offset
        let entry = match self.read_entry_at(self.current_offset) {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };

        // Calculate next offset
        let entry_size = match self.entry_size_at(self.current_offset) {
            Ok(s) => s,
            Err(e) => return Some(Err(e)),
        };

        let next_offset = self.current_offset + entry_size;

        // Track offset for reverse iteration
        if next_offset < self.block.data.len() {
            self.entry_offsets.push(next_offset);
        }

        self.current_offset = next_offset;

        Some(Ok(entry))
    }
}

impl DoubleEndedIterator for BlockIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Lazy initialization: scan block to build full offset index on first call
        if !self.backward_initialized {
            self.backward_initialized = true;

            // Build offset index by scanning through block metadata only
            let mut offset = self.current_offset;
            while offset < self.block.data.len() {
                let size = match self.entry_size_at(offset) {
                    Ok(s) => s,
                    Err(e) => return Some(Err(e)),
                };
                offset += size;
                if offset < self.block.data.len() {
                    self.entry_offsets.push(offset);
                }
            }

            // Position backward_index at last entry
            if self.entry_offsets.len() > 1 {
                self.backward_index = Some(self.entry_offsets.len() - 1);
            } else {
                return None; // Empty block
            }
        }

        // Get current backward index
        let idx = self.backward_index?;

        // Read entry at this offset using existing helper
        let offset = self.entry_offsets[idx];
        let entry = match self.read_entry_at(offset) {
            Ok(e) => e,
            Err(e) => return Some(Err(e)),
        };

        // Move backward or mark exhausted
        if idx == 0 {
            self.backward_index = None; // Exhausted after this
        } else {
            self.backward_index = Some(idx - 1);
        }

        Some(Ok(entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

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
            builder.add_entry(key, Some(value));
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

    #[test]
    fn test_block_reverse_iteration() {
        let mut builder = Builder::new();
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
        ];

        for (key, value) in &entries {
            builder.add_entry(key, Some(value));
        }
        let block_data = builder.finish();
        let block = Arc::new(Block::new(block_data).expect("Failed to create block"));

        // Test reverse iteration
        let mut iter = block.iter();

        // Read from back
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"d".to_vec(), Some(b"4".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"c".to_vec(), Some(b"3".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"b".to_vec(), Some(b"2".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_block_mixed_iteration() {
        let mut builder = Builder::new();
        let entries = vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"3".to_vec()),
            (b"d".to_vec(), b"4".to_vec()),
            (b"e".to_vec(), b"5".to_vec()),
        ];

        for (key, value) in &entries {
            builder.add_entry(key, Some(value));
        }
        let block_data = builder.finish();
        let block = Arc::new(Block::new(block_data).expect("Failed to create block"));

        let mut iter = block.iter();

        // Forward
        assert_eq!(
            iter.next().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );
        assert_eq!(
            iter.next().unwrap().unwrap(),
            (b"b".to_vec(), Some(b"2".to_vec()))
        );

        // Backward (should scan to end first, then return from back)
        // Note: Calling next_back() scans the ENTIRE block, so all entries are available
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"e".to_vec(), Some(b"5".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"d".to_vec(), Some(b"4".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"c".to_vec(), Some(b"3".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"b".to_vec(), Some(b"2".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );

        // Now exhausted
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_multiblock_reverse_iteration() {
        use crate::tmpfs::NamedTempFile;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Create multiple blocks with entries
        let mut file = File::create(path).expect("Failed to create file");
        let mut blocks = Vec::new();

        // Block 1: keys a, b, c
        let mut builder1 = Builder::new();
        builder1.add_entry(b"a", Some(b"1"));
        builder1.add_entry(b"b", Some(b"2"));
        builder1.add_entry(b"c", Some(b"3"));
        let block1_data = builder1.finish();
        let block1_offset = 0u64;
        let block1_size = block1_data.len() as u64;
        file.write_all(&block1_data)
            .expect("Failed to write block 1");
        blocks.push((block1_offset, block1_size));

        // Block 2: keys d, e, f
        let mut builder2 = Builder::new();
        builder2.add_entry(b"d", Some(b"4"));
        builder2.add_entry(b"e", Some(b"5"));
        builder2.add_entry(b"f", Some(b"6"));
        let block2_data = builder2.finish();
        let block2_offset = block1_size;
        let block2_size = block2_data.len() as u64;
        file.write_all(&block2_data)
            .expect("Failed to write block 2");
        blocks.push((block2_offset, block2_size));

        // Block 3: keys g, h, i
        let mut builder3 = Builder::new();
        builder3.add_entry(b"g", Some(b"7"));
        builder3.add_entry(b"h", Some(b"8"));
        builder3.add_entry(b"i", Some(b"9"));
        let block3_data = builder3.finish();
        let block3_offset = block2_offset + block2_size;
        let block3_size = block3_data.len() as u64;
        file.write_all(&block3_data)
            .expect("Failed to write block 3");
        blocks.push((block3_offset, block3_size));

        drop(file);

        // Open file for reading
        let reader = File::open(path).expect("Failed to open file");
        let mut iter = MultiBlockIterator::new(reader, blocks).expect("Failed to create iterator");

        // Test pure reverse iteration
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"i".to_vec(), Some(b"9".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"h".to_vec(), Some(b"8".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"g".to_vec(), Some(b"7".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"f".to_vec(), Some(b"6".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"e".to_vec(), Some(b"5".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"d".to_vec(), Some(b"4".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"c".to_vec(), Some(b"3".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"b".to_vec(), Some(b"2".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );
        assert!(iter.next_back().is_none());
    }

    #[test]
    fn test_multiblock_mixed_iteration() {
        use crate::tmpfs::NamedTempFile;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Create multiple blocks with entries
        let mut file = File::create(path).expect("Failed to create file");
        let mut blocks = Vec::new();

        // Block 1: keys a, b
        let mut builder1 = Builder::new();
        builder1.add_entry(b"a", Some(b"1"));
        builder1.add_entry(b"b", Some(b"2"));
        let block1_data = builder1.finish();
        let block1_offset = 0u64;
        let block1_size = block1_data.len() as u64;
        file.write_all(&block1_data)
            .expect("Failed to write block 1");
        blocks.push((block1_offset, block1_size));

        // Block 2: keys c, d
        let mut builder2 = Builder::new();
        builder2.add_entry(b"c", Some(b"3"));
        builder2.add_entry(b"d", Some(b"4"));
        let block2_data = builder2.finish();
        let block2_offset = block1_size;
        let block2_size = block2_data.len() as u64;
        file.write_all(&block2_data)
            .expect("Failed to write block 2");
        blocks.push((block2_offset, block2_size));

        // Block 3: keys e, f
        let mut builder3 = Builder::new();
        builder3.add_entry(b"e", Some(b"5"));
        builder3.add_entry(b"f", Some(b"6"));
        let block3_data = builder3.finish();
        let block3_offset = block2_offset + block2_size;
        let block3_size = block3_data.len() as u64;
        file.write_all(&block3_data)
            .expect("Failed to write block 3");
        blocks.push((block3_offset, block3_size));

        drop(file);

        // Open file for reading
        let reader = File::open(path).expect("Failed to open file");
        let mut iter = MultiBlockIterator::new(reader, blocks).expect("Failed to create iterator");

        // Forward first
        assert_eq!(
            iter.next().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );
        assert_eq!(
            iter.next().unwrap().unwrap(),
            (b"b".to_vec(), Some(b"2".to_vec()))
        );

        // Then backward (should scan from current position to end, then iterate back)
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"f".to_vec(), Some(b"6".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"e".to_vec(), Some(b"5".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"d".to_vec(), Some(b"4".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"c".to_vec(), Some(b"3".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"b".to_vec(), Some(b"2".to_vec()))
        );
        assert_eq!(
            iter.next_back().unwrap().unwrap(),
            (b"a".to_vec(), Some(b"1".to_vec()))
        );
        assert!(iter.next_back().is_none());
    }
}
