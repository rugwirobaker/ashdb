use super::block::{Block, BlockIterator};
use super::index::Index;
use crate::cache::Cache;
use crate::error::Result;
use crate::Error;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use std::io::{Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::{fs::File, io::Read};

pub const MAX_BLOCK_SIZE: usize = 4096;

pub struct Writer {
    file: File,
    index: Index, // Sparse index
    offset: u64,  // Current file offset
}

impl Writer {
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file,
            index: Index::new(),
            offset: 0,
        })
    }

    pub fn add_block(&mut self, block_data: &[u8], first_key: Vec<u8>) -> Result<()> {
        // Write block data to file
        self.file.write_all(block_data)?;
        self.file.flush()?;

        // Record the offset and first key
        self.index.push(first_key, self.offset);

        // Update offset
        self.offset += block_data.len() as u64;

        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        let index_data: Vec<u8> = self.index.try_into()?;

        let index_offset = self.offset;
        self.file.write_all(&index_data)?;

        self.file
            .write_u64::<BigEndian>(index_offset)
            .map_err(|e| Error::WriteError("footer (index offset)", e))?;
        self.file.flush().map_err(Error::IoError)?;

        Ok(())
    }
}

pub struct Reader {
    file: File,
    index: Index,
    index_offset: u64,
    block_cache: Arc<Mutex<Cache<u64, Arc<Block>>>>,
}

impl Reader {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path).map_err(Error::IoError)?;
        let file_size = file.metadata()?.len();

        // Read footer to get the index offset
        file.seek(SeekFrom::End(-8)).map_err(Error::IoError)?;

        let index_offset = file.read_u64::<BigEndian>().map_err(Error::IoError)?;

        // Read index block into a buffer
        let index_block_size = file_size - 8 - index_offset;
        let mut index_block = vec![0u8; index_block_size as usize];
        file.seek(SeekFrom::Start(index_offset))
            .map_err(|e| Error::ReadError("seek to index block", e))?;
        file.read_exact(&mut index_block)
            .map_err(|e| Error::ReadError("read index block", e))?;

        // Use TryFrom to parse the SparseIndex
        let index = Index::try_from(index_block.as_slice())?;

        let block_cache = Arc::new(Mutex::new(Cache::new(1000, None)));

        Ok(Self {
            file,
            index,
            index_offset,
            block_cache,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Use the SparseIndex to find the closest entry
        if let Some(entry) = self.index.find(key) {
            // Determine the size of the block
            let block_size = if entry.index + 1 < self.index.len() {
                self.index.get(entry.index + 1).unwrap() - entry.offset
            } else {
                self.index_offset - entry.offset // Use index offset for last block
            };

            // Read the block
            let block = self.read_block(entry.offset, block_size)?;

            return block.get(key);
        }
        Ok(None)
    }

    pub fn scan(&mut self) -> ScanIterator<'_> {
        ScanIterator::new(self)
    }

    fn read_block(&mut self, offset: u64, size: u64) -> Result<Arc<Block>> {
        if let Some(block_arc) = self.block_cache.lock().unwrap().get(&offset) {
            return Ok(block_arc.clone());
        }
        // Read block data
        self.file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| Error::ReadError("seek to block", e))?;
        let mut block_data = vec![0u8; size as usize];
        self.file
            .read_exact(&mut block_data)
            .map_err(|e| Error::ReadError("read block data", e))?;

        // Initialize BlockReader
        let block = Arc::new(Block::new(block_data)?);

        // Insert into cache
        self.block_cache
            .lock()
            .unwrap()
            .insert(offset, block.clone());
        Ok(block)
    }
}

pub struct ScanIterator<'a> {
    reader: &'a mut Reader,
    current_block: Option<Arc<Block>>,
    current_block_iter: Option<BlockIterator>,
    current_block_index: usize,
}

impl<'a> ScanIterator<'a> {
    fn new(reader: &'a mut Reader) -> Self {
        Self {
            reader,
            current_block: None,
            current_block_iter: None,
            current_block_index: 0,
        }
    }

    /// Load the next block and initialize its iterator
    fn next_block(&mut self) -> Result<()> {
        if self.current_block_index >= self.reader.index.len() {
            self.current_block_iter = None; // No more blocks to load
            return Ok(());
        }

        // Get the current block's offset and size
        let block_offset = self.reader.index.get(self.current_block_index).unwrap();
        let block_size = if self.current_block_index + 1 < self.reader.index.len() {
            self.reader.index.get(self.current_block_index + 1).unwrap() - block_offset
        } else {
            self.reader.index_offset - block_offset // Last block
        };

        // Read the block
        let block = self.reader.read_block(block_offset, block_size)?;

        // Store the block and initialize its iterator
        self.current_block = Some(block.clone()); // Keep the block alive
        self.current_block_iter = Some(block.iter());
        self.current_block_index += 1;

        Ok(())
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // If there's a current block iterator, try to get the next entry
            if let Some(iter) = &mut self.current_block_iter {
                if let Some(entry) = iter.peek() {
                    return Some(Ok(entry));
                }
            }

            // Load the next block if possible
            if let Err(e) = self.next_block() {
                return Some(Err(e));
            }

            // If no more blocks are available, terminate
            if self.current_block_iter.is_none() {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::sstable::block::{self, Builder};
    use std::fs;

    fn create_temp_dir() -> TempDir {
        TempDir::new().expect("Failed to create temporary directory")
    }

    #[test]
    fn test_sstable_writer_and_reader() {
        // Create sample data
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"application".to_vec(), b"software".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
            (b"band".to_vec(), b"music".to_vec()),
            (b"bandana".to_vec(), b"clothing".to_vec()),
        ];

        let temp_dir = create_temp_dir();
        let sstable_path = temp_dir.path().join("test_sstable.dat");

        let mut sstable_writer =
            Writer::new(sstable_path.to_str().unwrap()).expect("Failed to create SSTable writer");

        // Use a BlockBuilder to create blocks
        let mut block_builder = block::Builder::new();
        let mut first_key_in_block: Option<Vec<u8>> = None;

        for (key, value) in &entries {
            // Record the first key in the block
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            block_builder.add_entry(key, value);

            // For testing, write a block after every two entries
            if block_builder.entry_count() >= 2 {
                let block_data = block_builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("First key in block is missing");
                sstable_writer
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");
                block_builder = block::Builder::new();
                first_key_in_block = None;
            }
        }

        // Write the last block if any entries are left
        if block_builder.entry_count() > 0 {
            let block_data = block_builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("First key in block is missing");
            sstable_writer
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        // Finish writing the SSTable
        sstable_writer
            .finish()
            .expect("Failed to finish writing SSTable");

        // Read from the SSTable
        let mut sstable_reader =
            Reader::open(sstable_path.to_str().unwrap()).expect("failed to open SSTable reader");

        // Test reading keys
        for (key, value) in entries {
            match sstable_reader.get(&key) {
                Ok(Some(found_value)) => {
                    assert_eq!(
                        found_value,
                        value,
                        "Value for key '{}' does not match",
                        String::from_utf8_lossy(&key)
                    );
                }
                Ok(None) => {
                    panic!(
                        "Key '{}' not found in SSTable",
                        String::from_utf8_lossy(&key)
                    );
                }
                Err(e) => {
                    panic!(
                        "Error reading key '{}': {}",
                        String::from_utf8_lossy(&key),
                        e
                    );
                }
            }
        }

        // Test reading a non-existent key
        let non_existent_key = b"unknown";
        match sstable_reader.get(non_existent_key) {
            Ok(Some(_)) => {
                panic!(
                    "Non-existent key '{}' unexpectedly found in SSTable",
                    String::from_utf8_lossy(non_existent_key)
                );
            }
            Ok(None) => {
                // Expected outcome
            }
            Err(e) => {
                panic!(
                    "Error reading non-existent key '{}': {}",
                    String::from_utf8_lossy(non_existent_key),
                    e
                );
            }
        }

        // Clean up the test SSTable file
        fs::remove_file(sstable_path).expect("Failed to remove test SSTable file");
    }

    #[test]
    fn test_sstable_exact_match() {
        // Remove any existing test file
        let _ = fs::remove_file("test_sstable_exact.dat");

        // Create the SSTable
        let sstable_path = "test_sstable_exact.dat";
        let mut sstable_writer =
            Writer::new(sstable_path).expect("Failed to create SSTable writer");

        // Prepare the keys and values
        let entries = vec![
            (b"apple".to_vec(), b"fruit1".to_vec()),
            (b"apricot".to_vec(), b"fruit2".to_vec()),
        ];

        // Build the block
        let mut block_builder = block::Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in &entries {
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }
            block_builder.add_entry(&key, &value);
        }

        // Finish the block
        let block_data = block_builder.finish();
        let first_key = first_key_in_block
            .take()
            .expect("First key in block is missing");
        sstable_writer
            .add_block(&block_data, first_key)
            .expect("Failed to add block");

        // Finish the SSTable
        sstable_writer.finish().expect("Failed to finish SSTable");

        // Read back the SSTable file
        let actual_bytes = fs::read(sstable_path).expect("Failed to read SSTable file");

        // Expected bytes (we've already defined this)
        let expected_bytes = get_expected_sstable_bytes();

        // Compare the actual bytes with the expected bytes
        assert_eq!(
            actual_bytes, expected_bytes,
            "The SSTable file contents do not match the expected bytes."
        );

        // Now, read back the SSTable and verify the entries
        let mut sstable_reader = Reader::open(sstable_path).expect("Failed to open SSTable reader");

        // Iterate over the entries and collect them
        let mut read_entries = Vec::new();

        for (key, _) in &entries {
            let value = sstable_reader
                .get(key)
                .expect("Failed to get value")
                .expect("Key not found");
            read_entries.push((key.clone(), value));
        }

        // Compare the read entries with the original entries
        for ((expected_key, expected_value), (read_key, read_value)) in
            entries.into_iter().zip(read_entries)
        {
            assert_eq!(expected_key, read_key, "Keys do not match");
            assert_eq!(expected_value, read_value, "Values do not match");
        }

        // Optionally, remove the test file after the test
        let _ = fs::remove_file(sstable_path);
    }

    #[test]
    fn test_sstable_scan() {
        use tempfile::NamedTempFile;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Write sample SSTable data
        let mut writer = Writer::new(path.to_str().unwrap()).expect("Failed to create writer");

        // Create a large number of entries to ensure multiple blocks are created
        let entries = vec![
            (b"apple".to_vec(), b"fruit".to_vec()),
            (b"application".to_vec(), b"software".to_vec()),
            (b"banana".to_vec(), b"fruit".to_vec()),
            (b"band".to_vec(), b"music".to_vec()),
            (b"bandana".to_vec(), b"clothing".to_vec()),
            (b"cherry".to_vec(), b"fruit".to_vec()),
            (b"date".to_vec(), b"fruit".to_vec()),
            (b"durian".to_vec(), b"fruit".to_vec()),
        ];

        let mut builder = Builder::new();
        let mut first_key_in_block = None;

        for (key, value) in entries.iter() {
            // Track the first key in the block
            if first_key_in_block.is_none() {
                first_key_in_block = Some(key.clone());
            }

            // Add the key-value pair to the block
            builder.add_entry(key, value);

            // Simulate production-like behavior: flush the block if it grows too large
            if builder.len() >= 4 * 1024 {
                let block_data = builder.finish();
                let first_key = first_key_in_block
                    .take()
                    .expect("Missing first key in block");

                writer
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");

                // Reset the builder for the next block
                builder = Builder::new();
            }
        }

        // Write the final block if there are remaining entries
        if builder.len() > 0 {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");

            writer
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        writer.finish().expect("Failed to finish SSTable");

        // Read and scan the SSTable
        let mut reader = Reader::open(path.to_str().unwrap()).expect("Failed to open reader");
        let mut scan_iter = reader.scan();

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
    fn test_sstable_scan_with_large_blocks() {
        use tempfile::NamedTempFile;

        // Create a temporary file for the SSTable
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        let path = temp_file.path();

        // Write sample SSTable data
        let mut writer = Writer::new(path.to_str().unwrap()).expect("Failed to create writer");

        // Generate large entries to simulate real-world behavior
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| {
                let key = format!("key_{:05}", i).repeat(50).into_bytes(); // Large key
                let value = format!("value_{:05}", i).repeat(100).into_bytes(); // Large value
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

                writer
                    .add_block(&block_data, first_key)
                    .expect("Failed to add block");

                // Reset the builder for the next block
                builder = Builder::new();
            }
        }

        // Write the final block if there are remaining entries
        if builder.len() > 0 {
            let block_data = builder.finish();
            let first_key = first_key_in_block
                .take()
                .expect("Missing first key in block");

            writer
                .add_block(&block_data, first_key)
                .expect("Failed to add block");
        }

        writer.finish().expect("Failed to finish SSTable");

        // Read and scan the SSTable
        let mut reader = Reader::open(path.to_str().unwrap()).expect("Failed to open reader");
        let mut scan_iter = reader.scan();

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

    fn get_expected_sstable_bytes() -> Vec<u8> {
        let mut expected_bytes = Vec::new();

        // Data Block

        // Entry 1
        expected_bytes.extend_from_slice(&[0x00, 0x00]); // Shared Key Length
        expected_bytes.extend_from_slice(&[0x00, 0x05]); // Unshared Key Length
        expected_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x06]); // Value Length
        expected_bytes.extend_from_slice(b"apple"); // Unshared Key Bytes
        expected_bytes.extend_from_slice(b"fruit1"); // Value Bytes

        // Entry 2
        expected_bytes.extend_from_slice(&[0x00, 0x02]); // Shared Key Length
        expected_bytes.extend_from_slice(&[0x00, 0x05]); // Unshared Key Length
        expected_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x06]); // Value Length
        expected_bytes.extend_from_slice(b"ricot"); // Unshared Key Bytes
        expected_bytes.extend_from_slice(b"fruit2"); // Value Bytes

        // Restart Positions
        expected_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Restart Position at offset 0

        // Number of Restarts
        expected_bytes.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // Number of restarts: 1

        // Index Block
        let mut index_block = Vec::new();

        // Key Length
        index_block.extend_from_slice(&[0x00, 0x05]); // Key Length
        index_block.extend_from_slice(b"apple"); // Key Bytes

        // Block Offset (8 bytes)
        let block_offset = 0u64;
        index_block.extend_from_slice(&block_offset.to_be_bytes());

        // Append the index block to expected_bytes
        expected_bytes.extend_from_slice(&index_block);

        // Footer

        // Index Offset (offset where index block starts)
        let index_offset = expected_bytes.len() as u64 - index_block.len() as u64;
        expected_bytes.extend_from_slice(&index_offset.to_be_bytes());

        expected_bytes
    }
}
