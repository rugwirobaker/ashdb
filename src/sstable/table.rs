use super::block::{Block, BlockIterator};
use super::index::Index;
// use crate::cache::Cache;
use crate::error::Result;
use crate::Error;
// use crate::Error;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::sync::Arc;

pub const MAX_BLOCK_SIZE: usize = 4096;

pub enum Table {
    Writable(WritableTable),
    Readable(ReadableTable),
}

pub struct WritableTable {
    file: File,
    path: String,
    index: Index, // Sparse index
    offset: u64,  // Current file offset
}

impl Table {
    pub fn writable(path: &str) -> Result<Self> {
        Ok(Self::Writable(WritableTable::new(path)?))
    }

    pub fn readable(path: &str) -> Result<Self> {
        Ok(Self::Readable(ReadableTable::open(path)?))
    }

    pub fn add_block(&mut self, block_data: &[u8], first_key: Vec<u8>) -> Result<()> {
        match self {
            Table::Writable(writable) => writable.add_block(block_data, first_key),
            Table::Readable(_) => Err(Error::InvalidOperation(
                "Cannot add block to a readable table".to_string(),
            )),
        }
    }

    pub fn finalize(&mut self) -> Result<Table> {
        match self {
            Table::Writable(writable) => writable.finalize(),
            Table::Readable(_) => Err(Error::InvalidOperation(
                "Cannot finalize a readable table".to_string(),
            )),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self {
            Table::Readable(readable) => readable.get(key),
            Table::Writable(_) => Err(Error::InvalidOperation(
                "Cannot get from a writable table".to_string(),
            )),
        }
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIterator> {
        match self {
            Table::Readable(readable) => readable.scan(range),
            Table::Writable(_) => Err(Error::InvalidOperation(
                "Cannot scan a writable table".to_string(),
            )),
        }
    }

    pub fn min_key(&self) -> &[u8] {
        match self {
            Table::Readable(readable) => readable.min_key(),
            Table::Writable(_) => unimplemented!(),
        }
    }

    pub fn max_key(&self) -> &[u8] {
        match self {
            Table::Readable(readable) => readable.max_key(),
            Table::Writable(_) => unimplemented!(),
        }
    }
}

impl WritableTable {
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file,
            path: path.to_string(),
            index: Index::new(),
            offset: 0,
        })
    }

    pub fn add_block(&mut self, block_data: &[u8], first_key: Vec<u8>) -> Result<()> {
        let block_size = block_data.len() as u64;
        self.file.write_all(block_data)?;
        self.index.push(first_key, self.offset, block_size);
        self.offset += block_data.len() as u64;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<Table> {
        let index_data: Vec<u8> = self.index.clone().try_into()?;
        let index_offset = self.offset;

        self.file.write_all(&index_data)?;
        self.file.write_u64::<BigEndian>(index_offset)?;
        self.file.flush()?;
        // Create a new `ReadableTable` from the same file path
        let readable = ReadableTable::open(&self.path)?;
        // Return the table in readable mode
        Ok(Table::Readable(readable))
    }
}

pub struct ReadableTable {
    file: File,
    index: Index,
}

impl ReadableTable {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_size = file.metadata()?.len();

        file.seek(SeekFrom::End(-8))?;
        let index_offset = file.read_u64::<BigEndian>()?;

        let mut index_data = vec![0u8; (file_size - 8 - index_offset) as usize];
        file.seek(SeekFrom::Start(index_offset))?;
        file.read_exact(&mut index_data)?;

        let index = Index::try_from(index_data.as_slice())?;

        Ok(Self { file, index })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(entry) = self.index.find(key) {
            let (block_offset, block_size) = (entry.offset, entry.size);

            let mut block_data = vec![0u8; block_size as usize];
            let mut file_reader = self.file.try_clone()?;
            file_reader.seek(SeekFrom::Start(block_offset))?;
            file_reader.read_exact(&mut block_data)?;

            let block = Block::new(block_data)?;
            return block.get(key);
        }
        Ok(None)
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIterator> {
        let blocks = self.index.range(range);
        let reader = self.file.try_clone()?;
        ScanIterator::new(reader, blocks)
    }

    pub fn min_key(&self) -> &[u8] {
        // self.index.min_key()
        unimplemented!()
    }

    pub fn max_key(&self) -> &[u8] {
        unimplemented!()
    }
}

pub struct ScanIterator {
    reader: File,
    blocks: Vec<(u64, u64)>, // Block offset and size
    current_block: Option<Arc<Block>>,
    current_block_iter: Option<BlockIterator>,
    current_block_index: usize,
}

impl ScanIterator {
    pub fn new(reader: File, blocks: Vec<(u64, u64)>) -> Result<Self> {
        Ok(Self {
            reader,
            blocks,
            current_block: None,
            current_block_iter: None,
            current_block_index: 0,
        })
    }

    fn next_block(&mut self) -> Result<()> {
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

        let block = Arc::new(Block::new(block_data)?);

        // Update state
        self.current_block = Some(block.clone());
        self.current_block_iter = Some(block.iter());
        self.current_block_index += 1;

        Ok(())
    }
}

impl Iterator for ScanIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(iter) = &mut self.current_block_iter {
                if let Some(entry) = iter.peek() {
                    return Some(Ok(entry));
                }
            }

            // Load the next block
            if let Err(e) = self.next_block() {
                return Some(Err(e));
            }
            self.current_block_iter.as_ref()?;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tmpfs::NamedTempFile;

    use super::*;
    use crate::sstable::block::Builder;

    #[test]
    fn test_write_and_read() {
        // Sample data
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"application".to_vec(), b"software".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
            (b"band".to_vec(), b"music".to_vec()),
            (b"bandana".to_vec(), b"clothing".to_vec()),
        ];

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap();

        // Create a writable table
        let mut table = Table::writable(path).expect("Failed to create writable table");

        // Build blocks and add them to the table
        let mut builder = Builder::new();
        let mut first_key_in_block: Option<Vec<u8>> = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, value);

            if builder.len() >= 2 {
                let block_data = builder.finish();
                let first_key = first_key_in_block.take().expect("Missing first key");
                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                builder = Builder::new();
            }
        }

        // Add the last block if there are any remaining entries
        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block.take().expect("Missing first key");
            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        // Finalize the table
        let table = table.finalize().expect("Failed to finalize table");

        // Read entries from the table
        if let Table::Readable(readable) = table {
            for (key, value) in entries {
                let result = readable
                    .get(&key)
                    .expect("Error during read")
                    .expect("Key not found");
                assert_eq!(result, value, "Value mismatch for key {:?}", key);
            }
        } else {
            panic!("Expected readable table after finalize");
        }
    }

    #[test]
    fn test_scan() {
        use crate::tmpfs::NamedTempFile;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Write sample SSTable data
        let mut table = Table::writable(path.to_str().unwrap()).expect("Failed to create table");

        // Generate a larger number of entries to ensure multiple blocks are created
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                let key = format!("key_{:03}", i).into_bytes();
                let value = format!("value_{:03}", i).into_bytes();
                (key, value)
            })
            .collect();

        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in entries.iter() {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, value);

            // Flush block when it reaches or exceeds 4KB
            if builder.len() >= 4 * 1024 {
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");

                table
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");

                // Reset the builder for the next block
                builder = Builder::new();
            }
        }

        // Write the final block if there are remaining entries
        if !builder.is_empty() {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");

            table
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        // Open the table in readable mode and scan its contents
        let table = match table.finalize().expect("Failed to finalize table") {
            Table::Readable(readable) => readable,
            _ => panic!("Table did not transition to readable mode"),
        };

        let mut scan_iter = table
            .scan(b"application".to_vec()..=b"bandana".to_vec())
            .expect("Failed to create scan iterator");

        for (key, value) in entries {
            let result = scan_iter
                .next()
                .expect("Missing entry")
                .expect("Failed entry");
            assert_eq!((key, value), result);
        }

        // Ensure no more entries exist
        assert!(scan_iter.next().is_none());
    }

    #[test]
    fn test_get_nonexistent_key() {
        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path().to_str().unwrap();

        // Sample data
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
        ];

        // Create a writable table
        let mut table = Table::writable(path).expect("Failed to create writable table");

        // Build blocks and add them to the table
        let mut builder = Builder::new();
        let mut first_key_in_block: Option<Vec<u8>> = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            builder.add_entry(key, value);
        }

        let block_data = builder.finish();
        let first_key = first_key_in_block.take().expect("Missing first key");
        table
            .add_block(&block_data, first_key)
            .expect("Failed to add block");

        // Finalize the table
        let table = table.finalize().expect("Failed to finalize table");

        // Test reading a non-existent key
        if let Table::Readable(readable) = table {
            let non_existent_key = b"nonexistent".to_vec();
            let result = readable.get(&non_existent_key).expect("Error during read");
            assert!(result.is_none(), "Non-existent key unexpectedly found");
        } else {
            panic!("Expected readable table after finalize");
        }
    }
}
