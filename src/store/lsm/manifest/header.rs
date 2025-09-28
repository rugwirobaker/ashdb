use crate::error::Result;
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read};

pub const HEADER_SIZE: usize = 64;
const MAGIC: &[u8; 8] = b"ASHDB\0MF";
const VERSION: u32 = 1;
const DEFAULT_SNAPSHOT_INTERVAL: u32 = 100;

#[derive(Debug, Clone)]
pub struct ManifestHeader {
    pub magic: [u8; 8],
    pub version: u32,
    pub current_seq: u64,
    pub next_table_id: u64,
    pub snapshot_interval: u32,
}

impl Default for ManifestHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl ManifestHeader {
    pub fn new() -> Self {
        Self {
            magic: *MAGIC,
            version: VERSION,
            current_seq: 0,
            next_table_id: 0,
            snapshot_interval: DEFAULT_SNAPSHOT_INTERVAL,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != *MAGIC {
            return Err(Error::InvalidData(
                "Invalid manifest magic number".to_string(),
            ));
        }
        if self.version != VERSION {
            return Err(Error::InvalidData(format!(
                "Unsupported manifest version: {}",
                self.version
            )));
        }
        Ok(())
    }

    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        (&mut buf[8..12])
            .write_u32::<BigEndian>(self.version)
            .unwrap();
        (&mut buf[12..20])
            .write_u64::<BigEndian>(self.current_seq)
            .unwrap();
        (&mut buf[20..28])
            .write_u64::<BigEndian>(self.next_table_id)
            .unwrap();
        (&mut buf[28..32])
            .write_u32::<BigEndian>(self.snapshot_interval)
            .unwrap();
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let mut cursor = Cursor::new(buf);

        let mut magic = [0u8; 8];
        cursor.read_exact(&mut magic)?;

        let version = cursor.read_u32::<BigEndian>()?;
        let current_seq = cursor.read_u64::<BigEndian>()?;
        let next_table_id = cursor.read_u64::<BigEndian>()?;
        let snapshot_interval = cursor.read_u32::<BigEndian>()?;

        let header = Self {
            magic,
            version,
            current_seq,
            next_table_id,
            snapshot_interval,
        };
        header.validate()?;
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = ManifestHeader::new();
        let encoded = header.encode();
        let decoded = ManifestHeader::decode(&encoded).expect("Failed to decode header");

        assert_eq!(decoded.magic, *MAGIC);
        assert_eq!(decoded.version, VERSION);
        assert_eq!(decoded.current_seq, 0);
        assert_eq!(decoded.next_table_id, 0);
        assert_eq!(decoded.snapshot_interval, DEFAULT_SNAPSHOT_INTERVAL);
    }

    #[test]
    fn test_header_with_values() {
        let mut header = ManifestHeader::new();
        header.current_seq = 42;
        header.next_table_id = 100;
        header.snapshot_interval = 200;

        let encoded = header.encode();
        let decoded = ManifestHeader::decode(&encoded).expect("Failed to decode header");

        assert_eq!(decoded.current_seq, 42);
        assert_eq!(decoded.next_table_id, 100);
        assert_eq!(decoded.snapshot_interval, 200);
    }

    #[test]
    fn test_invalid_magic() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(b"INVALID!");

        let result = ManifestHeader::decode(&buf);
        assert!(matches!(result, Err(Error::InvalidData(_))));
    }

    #[test]
    fn test_unsupported_version() {
        let mut header = ManifestHeader::new();
        header.version = 999;
        let encoded = header.encode();

        let result = ManifestHeader::decode(&encoded);
        assert!(matches!(result, Err(Error::InvalidData(_))));
    }
}
