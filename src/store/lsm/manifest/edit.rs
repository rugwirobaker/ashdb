use super::meta::{LevelMeta, TableMeta};
use crate::error::Result;
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

const FLUSH: u8 = 0x01;
const BEGIN_COMPACTION: u8 = 0x02;
const COMMIT_COMPACTION: u8 = 0x03;
const SNAPSHOT: u8 = 0x04;

#[derive(Debug, Clone, PartialEq)]
pub enum VersionEdit {
    Flush {
        seq: u64,
        table: TableMeta,
        wal_id: u64,
    },

    BeginCompaction {
        seq: u64,
        job_id: u64,
        source_level: u32,
        target_level: u32,
    },

    CommitCompaction {
        seq: u64,
        job_id: u64,
        source_level: u32,
        deleted_tables: Vec<u64>,
        target_level: u32,
        added_tables: Vec<TableMeta>,
    },

    Snapshot {
        seq: u64,
        levels: Vec<LevelMeta>,
        next_table_id: u64,
    },
}

impl VersionEdit {
    pub fn seq(&self) -> u64 {
        match self {
            VersionEdit::Flush { seq, .. }
            | VersionEdit::BeginCompaction { seq, .. }
            | VersionEdit::CommitCompaction { seq, .. }
            | VersionEdit::Snapshot { seq, .. } => *seq,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        match self {
            VersionEdit::Flush { seq, table, wal_id } => {
                buf.write_u8(FLUSH).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*wal_id).unwrap();
                table.encode_into(&mut buf);
            }

            VersionEdit::BeginCompaction {
                seq,
                job_id,
                source_level,
                target_level,
            } => {
                buf.write_u8(BEGIN_COMPACTION).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*job_id).unwrap();
                buf.write_u32::<BigEndian>(*source_level).unwrap();
                buf.write_u32::<BigEndian>(*target_level).unwrap();
            }

            VersionEdit::CommitCompaction {
                seq,
                job_id,
                source_level,
                deleted_tables,
                target_level,
                added_tables,
            } => {
                buf.write_u8(COMMIT_COMPACTION).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*job_id).unwrap();
                buf.write_u32::<BigEndian>(*source_level).unwrap();

                buf.write_u32::<BigEndian>(deleted_tables.len() as u32)
                    .unwrap();
                for id in deleted_tables {
                    buf.write_u64::<BigEndian>(*id).unwrap();
                }

                buf.write_u32::<BigEndian>(*target_level).unwrap();

                buf.write_u32::<BigEndian>(added_tables.len() as u32)
                    .unwrap();
                for table in added_tables {
                    table.encode_into(&mut buf);
                }
            }

            VersionEdit::Snapshot {
                seq,
                levels,
                next_table_id,
            } => {
                buf.write_u8(SNAPSHOT).unwrap();
                buf.write_u64::<BigEndian>(*seq).unwrap();
                buf.write_u64::<BigEndian>(*next_table_id).unwrap();

                buf.write_u32::<BigEndian>(levels.len() as u32).unwrap();
                for level in levels {
                    level.encode_into(&mut buf);
                }
            }
        }

        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);
        let edit_type = cursor.read_u8()?;

        match edit_type {
            FLUSH => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let wal_id = cursor.read_u64::<BigEndian>()?;
                let table = TableMeta::decode_from(&mut cursor)?;
                Ok(VersionEdit::Flush { seq, table, wal_id })
            }

            BEGIN_COMPACTION => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let job_id = cursor.read_u64::<BigEndian>()?;
                let source_level = cursor.read_u32::<BigEndian>()?;
                let target_level = cursor.read_u32::<BigEndian>()?;
                Ok(VersionEdit::BeginCompaction {
                    seq,
                    job_id,
                    source_level,
                    target_level,
                })
            }

            COMMIT_COMPACTION => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let job_id = cursor.read_u64::<BigEndian>()?;
                let source_level = cursor.read_u32::<BigEndian>()?;

                let deleted_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut deleted_tables = Vec::with_capacity(deleted_count);
                for _ in 0..deleted_count {
                    deleted_tables.push(cursor.read_u64::<BigEndian>()?);
                }

                let target_level = cursor.read_u32::<BigEndian>()?;

                let added_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut added_tables = Vec::with_capacity(added_count);
                for _ in 0..added_count {
                    added_tables.push(TableMeta::decode_from(&mut cursor)?);
                }

                Ok(VersionEdit::CommitCompaction {
                    seq,
                    job_id,
                    source_level,
                    deleted_tables,
                    target_level,
                    added_tables,
                })
            }

            SNAPSHOT => {
                let seq = cursor.read_u64::<BigEndian>()?;
                let next_table_id = cursor.read_u64::<BigEndian>()?;

                let level_count = cursor.read_u32::<BigEndian>()? as usize;
                let mut levels = Vec::with_capacity(level_count);
                for _ in 0..level_count {
                    levels.push(LevelMeta::decode_from(&mut cursor)?);
                }

                Ok(VersionEdit::Snapshot {
                    seq,
                    levels,
                    next_table_id,
                })
            }

            _ => Err(Error::InvalidData(format!(
                "Invalid edit type: {}",
                edit_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_table_meta() -> TableMeta {
        TableMeta {
            id: 42,
            level: 0,
            size: 1024,
            entry_count: 100,
            min_key: vec![1, 2, 3],
            max_key: vec![9, 8, 7],
        }
    }

    #[test]
    fn test_flush_roundtrip() {
        let original = VersionEdit::Flush {
            seq: 1,
            table: create_test_table_meta(),
            wal_id: 10,
        };

        let encoded = original.encode();
        let decoded = VersionEdit::decode(&encoded).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_begin_compaction_roundtrip() {
        let original = VersionEdit::BeginCompaction {
            seq: 5,
            job_id: 100,
            source_level: 0,
            target_level: 1,
        };

        let encoded = original.encode();
        let decoded = VersionEdit::decode(&encoded).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_commit_compaction_roundtrip() {
        let original = VersionEdit::CommitCompaction {
            seq: 6,
            job_id: 100,
            source_level: 0,
            deleted_tables: vec![1, 2, 3],
            target_level: 1,
            added_tables: vec![create_test_table_meta()],
        };

        let encoded = original.encode();
        let decoded = VersionEdit::decode(&encoded).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_snapshot_roundtrip() {
        let original = VersionEdit::Snapshot {
            seq: 10,
            levels: vec![LevelMeta {
                level: 0,
                tables: vec![create_test_table_meta()],
            }],
            next_table_id: 50,
        };

        let encoded = original.encode();
        let decoded = VersionEdit::decode(&encoded).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_invalid_edit_type() {
        let invalid_data = vec![0xFF, 0, 0, 0, 0, 0, 0, 0, 0];
        let result = VersionEdit::decode(&invalid_data);
        assert!(matches!(result, Err(Error::InvalidData(_))));
    }
}
