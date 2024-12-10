use std::io::{Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::Result;
use crate::Error;

#[derive(Debug, Copy, Clone)]
#[repr(u8)]
pub enum RecordType {
    AddTable = 1,
    DeleteTable = 2,
    BeginCompaction = 3,
    EndCompaction = 4,
    BeginFlush = 5,
    EndFlush = 6,
}

impl TryFrom<u8> for RecordType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(RecordType::AddTable),
            2 => Ok(RecordType::DeleteTable),
            3 => Ok(RecordType::BeginCompaction),
            4 => Ok(RecordType::EndCompaction),
            5 => Ok(RecordType::BeginFlush),
            6 => Ok(RecordType::EndFlush),
            _ => Err(Error::InvalidRecordType(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    AddTable {
        id: u64,
        level: u32,
        info: FileInfo,
    },
    DeleteTable {
        id: u64,
        level: u32,
    },
    BeginCompaction {
        job_id: u64,
        input_files: Vec<FileInfo>,
        source_level: u32,
        target_level: u32,
    },
    EndCompaction {
        job_id: u64,
        output_files: Vec<FileInfo>,
        success: bool,
    },
    BeginFlush {
        memtable_id: u64,
        wal_id: u64,
    },
    EndFlush {
        memtable_id: u64,
        output_files: Vec<FileInfo>,
        success: bool,
    },
}

impl TryInto<Vec<u8>> for Record {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        match self {
            Record::AddTable { id, level, info } => {
                buf.write_u8(RecordType::AddTable as u8)?;
                buf.write_u64::<BigEndian>(id)?;
                buf.write_u32::<BigEndian>(level)?;
                let meta_bytes: Vec<u8> = info.try_into()?;
                buf.write_u32::<BigEndian>(meta_bytes.len() as u32)?;
                buf.write_all(&meta_bytes)?;
            }
            Record::DeleteTable { id, level } => {
                buf.write_u8(RecordType::DeleteTable as u8)?;
                buf.write_u64::<BigEndian>(id)?;
                buf.write_u32::<BigEndian>(level)?;
            }
            Record::BeginCompaction {
                job_id,
                input_files,
                source_level,
                target_level,
            } => {
                buf.write_u8(RecordType::BeginCompaction as u8)?;
                buf.write_u64::<BigEndian>(job_id)?;
                buf.write_u32::<BigEndian>(source_level)?;
                buf.write_u32::<BigEndian>(target_level)?;

                buf.write_u32::<BigEndian>(input_files.len() as u32)?;
                for meta in input_files {
                    let meta_bytes: Vec<u8> = meta.try_into()?;
                    buf.write_u32::<BigEndian>(meta_bytes.len() as u32)?;
                    buf.write_all(&meta_bytes)?;
                }
            }
            Record::EndCompaction {
                job_id,
                output_files,
                success,
            } => {
                buf.write_u8(RecordType::EndCompaction as u8)?;
                buf.write_u64::<BigEndian>(job_id)?;
                buf.write_u8(if success { 1 } else { 0 })?;

                buf.write_u32::<BigEndian>(output_files.len() as u32)?;
                for meta in output_files {
                    let meta_bytes: Vec<u8> = meta.try_into()?;
                    buf.write_u32::<BigEndian>(meta_bytes.len() as u32)?;
                    buf.write_all(&meta_bytes)?;
                }
            }
            Record::BeginFlush {
                memtable_id,
                wal_id,
            } => {
                buf.write_u8(RecordType::BeginFlush as u8)?;
                buf.write_u64::<BigEndian>(memtable_id)?;
                buf.write_u64::<BigEndian>(wal_id)?;
            }
            Record::EndFlush {
                memtable_id,
                output_files,
                success,
            } => {
                buf.write_u8(RecordType::EndFlush as u8)?;
                buf.write_u64::<BigEndian>(memtable_id)?;
                buf.write_u8(if success { 1 } else { 0 })?;

                buf.write_u32::<BigEndian>(output_files.len() as u32)?;
                for meta in output_files {
                    let meta_bytes: Vec<u8> = meta.try_into()?;
                    buf.write_u32::<BigEndian>(meta_bytes.len() as u32)?;
                    buf.write_all(&meta_bytes)?;
                }
            }
        }
        Ok(buf)
    }
}

impl TryFrom<&[u8]> for Record {
    type Error = Error;

    fn try_from(buf: &[u8]) -> Result<Self> {
        let mut reader = std::io::Cursor::new(buf);
        let record_type = RecordType::try_from(reader.read_u8()?)?;

        match record_type {
            RecordType::AddTable => {
                let id = reader.read_u64::<BigEndian>()?;
                let level = reader.read_u32::<BigEndian>()?;
                let info_len = reader.read_u32::<BigEndian>()? as usize;
                let mut info_buf = vec![0; info_len];
                reader.read_exact(&mut info_buf)?;
                let info = FileInfo::try_from(info_buf.as_slice())?;

                Ok(Record::AddTable { id, level, info })
            }
            RecordType::DeleteTable => {
                let id = reader.read_u64::<BigEndian>()?;
                let level = reader.read_u32::<BigEndian>()?;

                Ok(Record::DeleteTable { id, level })
            }
            RecordType::BeginCompaction => {
                let job_id = reader.read_u64::<BigEndian>()?;
                let source_level = reader.read_u32::<BigEndian>()?;
                let target_level = reader.read_u32::<BigEndian>()?;

                let file_count = reader.read_u32::<BigEndian>()? as usize;
                let mut input_files = Vec::with_capacity(file_count);
                for _ in 0..file_count {
                    let meta_len = reader.read_u32::<BigEndian>()? as usize;
                    let mut meta_buf = vec![0; meta_len];
                    reader.read_exact(&mut meta_buf)?;
                    let meta = FileInfo::try_from(meta_buf.as_slice())?;
                    input_files.push(meta);
                }

                Ok(Record::BeginCompaction {
                    job_id,
                    input_files,
                    source_level,
                    target_level,
                })
            }
            RecordType::EndCompaction => {
                let job_id = reader.read_u64::<BigEndian>()?;
                let success = reader.read_u8()? != 0;

                let file_count = reader.read_u32::<BigEndian>()? as usize;
                let mut output_files = Vec::with_capacity(file_count);
                for _ in 0..file_count {
                    let meta_len = reader.read_u32::<BigEndian>()? as usize;
                    let mut meta_buf = vec![0; meta_len];
                    reader.read_exact(&mut meta_buf)?;
                    let meta = FileInfo::try_from(meta_buf.as_slice())?;
                    output_files.push(meta);
                }

                Ok(Record::EndCompaction {
                    job_id,
                    output_files,
                    success,
                })
            }
            RecordType::BeginFlush => {
                let memtable_id = reader.read_u64::<BigEndian>()?;
                let wal_id = reader.read_u64::<BigEndian>()?;

                Ok(Record::BeginFlush {
                    memtable_id,
                    wal_id,
                })
            }
            RecordType::EndFlush => {
                let memtable_id = reader.read_u64::<BigEndian>()?;
                let success = reader.read_u8()? != 0;

                let file_count = reader.read_u32::<BigEndian>()? as usize;
                let mut output_files = Vec::with_capacity(file_count);
                for _ in 0..file_count {
                    let meta_len = reader.read_u32::<BigEndian>()? as usize;
                    let mut meta_buf = vec![0; meta_len];
                    reader.read_exact(&mut meta_buf)?;
                    let meta = FileInfo::try_from(meta_buf.as_slice())?;
                    output_files.push(meta);
                }

                Ok(Record::EndFlush {
                    memtable_id,
                    output_files,
                    success,
                })
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq)]
pub struct FileInfo {
    pub id: u64,
    pub size: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

impl TryInto<Vec<u8>> for FileInfo {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        // Write fixed-size fields
        buf.write_u64::<BigEndian>(self.id)?;
        buf.write_u64::<BigEndian>(self.size)?;

        // Write min_key with length prefix
        buf.write_u32::<BigEndian>(self.min_key.len() as u32)?;
        buf.write_all(&self.min_key)?;

        // Write max_key with length prefix
        buf.write_u32::<BigEndian>(self.max_key.len() as u32)?;
        buf.write_all(&self.max_key)?;

        Ok(buf)
    }
}

impl TryFrom<&[u8]> for FileInfo {
    type Error = Error;

    fn try_from(buf: &[u8]) -> Result<Self> {
        let mut reader = std::io::Cursor::new(buf);

        // Read fixed-size fields
        let id = reader.read_u64::<BigEndian>()?;
        let size = reader.read_u64::<BigEndian>()?;

        // Read min_key
        let min_key_len = reader.read_u32::<BigEndian>()? as usize;
        let mut min_key = vec![0; min_key_len];
        reader.read_exact(&mut min_key)?;

        // Read max_key
        let max_key_len = reader.read_u32::<BigEndian>()? as usize;
        let mut max_key = vec![0; max_key_len];
        reader.read_exact(&mut max_key)?;

        Ok(FileInfo {
            id,
            size,
            min_key,
            max_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create test FileInfo
    fn create_test_file_info() -> FileInfo {
        FileInfo {
            id: 42,
            size: 1024,
            min_key: vec![1, 2, 3],
            max_key: vec![9, 8, 7],
        }
    }

    #[test]
    fn test_record_add_table_roundtrip() {
        let original = Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(),
        };

        let encoded: Vec<u8> = original.try_into().expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        match decoded {
            Record::AddTable { id, level, info } => {
                assert_eq!(id, 1);
                assert_eq!(level, 0);
                assert_eq!(info.id, 42);
                assert_eq!(info.size, 1024);
                assert_eq!(info.min_key, vec![1, 2, 3]);
                assert_eq!(info.max_key, vec![9, 8, 7]);
            }
            _ => panic!("Decoded to wrong variant"),
        }
    }

    #[test]
    fn test_record_begin_compaction_roundtrip() {
        let original = Record::BeginCompaction {
            job_id: 123,
            input_files: vec![create_test_file_info(), create_test_file_info()],
            source_level: 1,
            target_level: 2,
        };

        let encoded: Vec<u8> = original.try_into().expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        match decoded {
            Record::BeginCompaction {
                job_id,
                input_files,
                source_level,
                target_level,
            } => {
                assert_eq!(job_id, 123);
                assert_eq!(input_files.len(), 2);
                assert_eq!(source_level, 1);
                assert_eq!(target_level, 2);
                for info in input_files {
                    assert_eq!(info.id, 42);
                    assert_eq!(info.size, 1024);
                }
            }
            _ => panic!("Decoded to wrong variant"),
        }
    }

    #[test]
    fn test_record_end_flush_roundtrip() {
        let original = Record::EndFlush {
            memtable_id: 456,
            output_files: vec![create_test_file_info()],
            success: true,
        };

        let encoded: Vec<u8> = original.try_into().expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        match decoded {
            Record::EndFlush {
                memtable_id,
                output_files,
                success,
            } => {
                assert_eq!(memtable_id, 456);
                assert_eq!(output_files.len(), 1);
                assert!(success);
                assert_eq!(output_files[0].id, 42);
            }
            _ => panic!("Decoded to wrong variant"),
        }
    }

    #[test]
    fn test_record_invalid_type() {
        let mut invalid_data = vec![255]; // Invalid record type
        invalid_data.extend_from_slice(&[0; 10]); // Add some padding
        let result = Record::try_from(invalid_data.as_slice());
        assert!(matches!(result, Err(Error::InvalidRecordType(255))));
    }

    #[test]
    fn test_record_truncated_data() {
        let record = Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(),
        };
        let mut encoded: Vec<u8> = record.try_into().expect("Failed to encode Record");
        encoded.truncate(encoded.len() - 1); // Remove last byte

        let result = Record::try_from(encoded.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn test_file_info_roundtrip() {
        let original = create_test_file_info();
        let encoded: Vec<u8> = original
            .clone()
            .try_into()
            .expect("Failed to encode FileInfo");
        let decoded = FileInfo::try_from(encoded.as_slice()).expect("Failed to decode FileInfo");

        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.size, original.size);
        assert_eq!(decoded.min_key, original.min_key);
        assert_eq!(decoded.max_key, original.max_key);
    }

    #[test]
    fn test_file_info_empty_keys() {
        let original = FileInfo {
            id: 42,
            size: 1024,
            min_key: vec![],
            max_key: vec![],
        };

        let encoded: Vec<u8> = original
            .clone()
            .try_into()
            .expect("Failed to encode FileInfo");
        let decoded = FileInfo::try_from(encoded.as_slice()).expect("Failed to decode FileInfo");

        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.size, original.size);
        assert!(decoded.min_key.is_empty());
        assert!(decoded.max_key.is_empty());
    }
}
