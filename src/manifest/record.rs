use std::io::{Read, Write};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::Result;
use crate::Error;

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Flush,
    Compaction { job_id: u64 },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Record {
    AddTable {
        id: u64,
        level: u32,
        info: FileInfo,
        op_type: Operation,
    },
    DeleteTable {
        id: u64,
        level: u32,
        op_type: Operation,
    },
}

impl Record {
    fn record_type(&self) -> u8 {
        match self {
            Record::AddTable { .. } => 0x01,
            Record::DeleteTable { .. } => 0x02,
        }
    }
}

impl TryInto<Vec<u8>> for Record {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.write_u8(self.record_type())?;

        match self {
            Record::AddTable {
                id,
                level,
                info,
                op_type,
            } => {
                buf.write_u64::<BigEndian>(id)?;
                buf.write_u32::<BigEndian>(level)?;

                // Encode operation type
                match op_type {
                    Operation::Flush => {
                        buf.write_u8(0x01)?; // Changed from 0 to 0x01
                    }
                    Operation::Compaction { job_id } => {
                        buf.write_u8(0x02)?; // Changed from 1 to 0x02
                        buf.write_u64::<BigEndian>(job_id)?;
                    }
                }

                let info_bytes: Vec<u8> = info.try_into()?;
                buf.write_u32::<BigEndian>(info_bytes.len() as u32)?;
                buf.write_all(&info_bytes)?;
            }
            Record::DeleteTable { id, level, op_type } => {
                buf.write_u64::<BigEndian>(id)?;
                buf.write_u32::<BigEndian>(level)?;

                match op_type {
                    Operation::Flush => {
                        buf.write_u8(0x01)?; // Changed from 0 to 0x01
                    }
                    Operation::Compaction { job_id } => {
                        buf.write_u8(0x02)?; // Changed from 1 to 0x02
                        buf.write_u64::<BigEndian>(job_id)?;
                    }
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
        match reader.read_u8()? {
            0x01 => {
                let id = reader.read_u64::<BigEndian>()?;
                let level = reader.read_u32::<BigEndian>()?;

                // Decode operation type
                let op_type = match reader.read_u8()? {
                    0x01 => Operation::Flush,
                    0x02 => Operation::Compaction {
                        job_id: reader.read_u64::<BigEndian>()?,
                    },
                    n => return Err(Error::InvalidOperationType(n)),
                };

                let info_len = reader.read_u32::<BigEndian>()? as usize;
                let mut info_buf = vec![0; info_len];
                reader.read_exact(&mut info_buf)?;
                let info = FileInfo::try_from(info_buf.as_slice())?;

                Ok(Record::AddTable {
                    id,
                    level,
                    info,
                    op_type,
                })
            }
            0x02 => {
                let id = reader.read_u64::<BigEndian>()?;
                let level = reader.read_u32::<BigEndian>()?;

                // Decode operation type
                let op_type = match reader.read_u8()? {
                    0x01 => Operation::Flush,
                    0x02 => Operation::Compaction {
                        job_id: reader.read_u64::<BigEndian>()?,
                    },
                    n => return Err(Error::InvalidOperationType(n)),
                };

                Ok(Record::DeleteTable { id, level, op_type })
            }
            n => Err(Error::InvalidRecordType(n)),
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

    fn create_test_file_info() -> FileInfo {
        FileInfo {
            id: 42,
            size: 1024,
            min_key: vec![1, 2, 3],
            max_key: vec![9, 8, 7],
        }
    }

    #[test]
    fn test_record_add_table_flush_roundtrip() {
        let original = Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(),
            op_type: Operation::Flush,
        };

        let encoded: Vec<u8> = original
            .clone()
            .try_into()
            .expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_record_add_table_compaction_roundtrip() {
        let original = Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(),
            op_type: Operation::Compaction { job_id: 123 },
        };

        let encoded: Vec<u8> = original
            .clone()
            .try_into()
            .expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_record_delete_table_flush_roundtrip() {
        let original = Record::DeleteTable {
            id: 1,
            level: 0,
            op_type: Operation::Flush,
        };

        let encoded: Vec<u8> = original
            .clone()
            .try_into()
            .expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_record_delete_table_compaction_roundtrip() {
        let original = Record::DeleteTable {
            id: 1,
            level: 0,
            op_type: Operation::Compaction { job_id: 123 },
        };

        let encoded: Vec<u8> = original
            .clone()
            .try_into()
            .expect("Failed to encode Record");
        let decoded = Record::try_from(encoded.as_slice()).expect("Failed to decode Record");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_record_invalid_type() {
        let mut invalid_data = vec![255]; // Invalid record type
        invalid_data.extend_from_slice(&[0; 10]); // Add some padding
        let result = Record::try_from(invalid_data.as_slice());
        assert!(matches!(result, Err(Error::InvalidRecordType(255))));
    }

    #[test]
    fn test_record_invalid_operation_type() {
        let data = vec![
            0x01, //
            0, 0, 0, 0, 0, 0, 0, 1, // id
            0, 0, 0, 0,    // level
            0xFF, // Invalid operation type
        ];
        let result = Record::try_from(data.as_slice());
        assert!(matches!(result, Err(Error::InvalidOperationType(0xFF))));
    }

    #[test]
    fn test_record_truncated_data() {
        let record = Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(),
            op_type: Operation::Flush,
        };
        let mut encoded: Vec<u8> = record.try_into().expect("Failed to encode Record");
        encoded.truncate(encoded.len() - 1); // Remove last byte

        let result = Record::try_from(encoded.as_slice());
        assert!(result.is_err());
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
