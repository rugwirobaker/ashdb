use super::block::Block;
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
    index: Vec<(Vec<u8>, u64)>, // Pairs of first key and block offset
    offset: u64,                // Current file offset
}

impl Writer {
    pub fn new(path: &str) -> Result<Self> {
        let file = File::create(path)?;
        Ok(Self {
            file,
            index: Vec::new(),
            offset: 0,
        })
    }

    pub fn add_block(&mut self, block_data: &[u8], first_key: Vec<u8>) -> Result<()> {
        // Write block data to file
        self.file.write_all(block_data)?;
        self.file.flush()?;

        // Record the offset and first key
        self.index.push((first_key, self.offset));

        // Update offset
        self.offset += block_data.len() as u64;

        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        // Write index block
        let index_offset = self.offset;
        for (key, block_offset) in &self.index {
            // Write key length (u16) in big-endian
            self.file
                .write_u16::<BigEndian>(key.len() as u16)
                .map_err(|e| Error::WriteError("key length in index block", e))?;
            // Write key bytes
            self.file
                .write_all(key)
                .map_err(|e| Error::WriteError("key in index block", e))?;
            // Write block offset (u64) in big-endian
            self.file
                .write_u64::<BigEndian>(*block_offset)
                .map_err(|e| Error::WriteError("block offset in index block", e))?;
        }
        self.file.flush().map_err(Error::IoError)?;

        // Write footer (index offset)
        self.file
            .write_u64::<BigEndian>(index_offset)
            .map_err(|e| Error::WriteError("footer (index offset)", e))?;
        self.file.flush().map_err(Error::IoError)?;

        Ok(())
    }
}

pub struct Reader {
    file: File,
    index: Vec<(Vec<u8>, u64)>, // Pairs of first key and block offset
    index_offset: u64,
    block_cache: Arc<Mutex<Cache<u64, Arc<Block>>>>,
}

impl Reader {
    pub fn open(path: &str) -> Result<Self> {
        let mut file = File::open(path).map_err(Error::IoError)?;

        // Get file size
        let file_size = file.metadata().map_err(Error::IoError)?.len();

        // Read footer (index offset)
        file.seek(SeekFrom::End(-8))
            .map_err(|e| Error::ReadError("footer (index offset)", e))?;
        let index_offset = file
            .read_u64::<BigEndian>()
            .map_err(|e| Error::ReadError("index offset", e))?;

        // Calculate index block size
        let index_block_size = file_size - 8 - index_offset;

        // Read index block into a buffer
        file.seek(SeekFrom::Start(index_offset))
            .map_err(|e| Error::ReadError("index block start", e))?;
        let mut index_block = vec![0u8; index_block_size as usize];
        file.read_exact(&mut index_block)
            .map_err(|e| Error::ReadError("index block", e))?;

        // Parse index entries from buffer
        let mut index = Vec::new();
        let mut cursor = std::io::Cursor::new(&index_block);

        while (cursor.position() as usize) < index_block.len() {
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

            // Read block offset (u64)
            let block_offset = cursor
                .read_u64::<BigEndian>()
                .map_err(|e| Error::ReadError("block offset in index block", e))?;

            index.push((key, block_offset));
        }

        let block_cache = Arc::new(Mutex::new(Cache::new(1000, None))); // Adjust capacity as needed

        Ok(Self {
            file,
            index,
            index_offset,
            block_cache,
        })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Binary search over the index
        let mut left = 0;
        let mut right = self.index.len();

            // Read the block
            let block = self.read_block(entry.offset, block_size)?;

            if cmp == std::cmp::Ordering::Less {
                left = mid + 1;
            } else if cmp == std::cmp::Ordering::Greater {
                if mid == 0 {
                    break;
                }
                right = mid;
            } else {
                // Exact match
                let block_offset = self.index[mid].1;
                let block_size = self.get_block_size(mid);
                return self.read_from_block(block_offset, block_size, key);
            }
        }

        // Key might be in the block at index left - 1
        if left > 0 {
            let block_index = left - 1;
            let block_offset = self.index[block_index].1;
            let block_size = self.get_block_size(block_index);
            return self.read_from_block(block_offset, block_size, key);
        } else {
            Ok(None)
        }
    }

    fn read_block(&mut self, offset: u64, size: u64) -> Result<Arc<Block>> {
        // Check cache first
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sstable::block;
    use std::fs;

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

        // Create an SSTableWriter
        let sstable_path = "test_sstable.dat";
        let mut sstable_writer =
            Writer::new(sstable_path).expect("Failed to create sstable writer");

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
        let mut sstable_reader = Reader::open(sstable_path).expect("Failed to open SSTable");

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
