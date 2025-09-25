use crate::error::Result;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read};

#[derive(Debug, Clone, PartialEq)]
pub struct TableMeta {
    pub id: u64,
    pub level: u32,
    pub size: u64,
    pub entry_count: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

impl TableMeta {
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(self.id).unwrap();
        buf.write_u32::<BigEndian>(self.level).unwrap();
        buf.write_u64::<BigEndian>(self.size).unwrap();
        buf.write_u64::<BigEndian>(self.entry_count).unwrap();

        buf.write_u32::<BigEndian>(self.min_key.len() as u32)
            .unwrap();
        buf.extend_from_slice(&self.min_key);

        buf.write_u32::<BigEndian>(self.max_key.len() as u32)
            .unwrap();
        buf.extend_from_slice(&self.max_key);
    }

    pub fn decode_from(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        let id = cursor.read_u64::<BigEndian>()?;
        let level = cursor.read_u32::<BigEndian>()?;
        let size = cursor.read_u64::<BigEndian>()?;
        let entry_count = cursor.read_u64::<BigEndian>()?;

        let min_key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut min_key = vec![0u8; min_key_len];
        cursor.read_exact(&mut min_key)?;

        let max_key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut max_key = vec![0u8; max_key_len];
        cursor.read_exact(&mut max_key)?;

        Ok(TableMeta {
            id,
            level,
            size,
            entry_count,
            min_key,
            max_key,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LevelMeta {
    pub level: u32,
    pub tables: Vec<TableMeta>,
}

impl LevelMeta {
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.write_u32::<BigEndian>(self.level).unwrap();
        buf.write_u32::<BigEndian>(self.tables.len() as u32)
            .unwrap();
        for table in &self.tables {
            table.encode_into(buf);
        }
    }

    pub fn decode_from(cursor: &mut Cursor<&[u8]>) -> Result<Self> {
        let level = cursor.read_u32::<BigEndian>()?;
        let table_count = cursor.read_u32::<BigEndian>()? as usize;
        let mut tables = Vec::with_capacity(table_count);
        for _ in 0..table_count {
            tables.push(TableMeta::decode_from(cursor)?);
        }
        Ok(LevelMeta { level, tables })
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
    fn test_table_meta_roundtrip() {
        let original = create_test_table_meta();
        let mut buf = Vec::new();
        original.encode_into(&mut buf);

        let mut cursor = Cursor::new(buf.as_slice());
        let decoded = TableMeta::decode_from(&mut cursor).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_table_meta_empty_keys() {
        let original = TableMeta {
            id: 1,
            level: 0,
            size: 512,
            entry_count: 50,
            min_key: vec![],
            max_key: vec![],
        };

        let mut buf = Vec::new();
        original.encode_into(&mut buf);

        let mut cursor = Cursor::new(buf.as_slice());
        let decoded = TableMeta::decode_from(&mut cursor).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_level_meta_roundtrip() {
        let original = LevelMeta {
            level: 1,
            tables: vec![
                create_test_table_meta(),
                TableMeta {
                    id: 43,
                    level: 1,
                    size: 2048,
                    entry_count: 200,
                    min_key: vec![10, 20],
                    max_key: vec![90, 80],
                },
            ],
        };

        let mut buf = Vec::new();
        original.encode_into(&mut buf);

        let mut cursor = Cursor::new(buf.as_slice());
        let decoded = LevelMeta::decode_from(&mut cursor).expect("Failed to decode");

        assert_eq!(decoded, original);
    }

    #[test]
    fn test_level_meta_empty() {
        let original = LevelMeta {
            level: 2,
            tables: vec![],
        };

        let mut buf = Vec::new();
        original.encode_into(&mut buf);

        let mut cursor = Cursor::new(buf.as_slice());
        let decoded = LevelMeta::decode_from(&mut cursor).expect("Failed to decode");

        assert_eq!(decoded, original);
    }
}
