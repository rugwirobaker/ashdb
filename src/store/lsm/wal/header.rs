use crate::error::Result;
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

pub const HEADER_SIZE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Header {
    pub magic: [u8; 8],
    pub version: u32,
    pub entry_count: u64,
}

const MAGIC: &[u8; 8] = b"ASHDB\x00WL";
const VERSION: u32 = 1;

impl Header {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Header {
            magic: *MAGIC,
            version: VERSION,
            entry_count: 0,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.magic != *MAGIC {
            return Err(Error::InvalidWalMagic);
        }
        if self.version != VERSION {
            return Err(Error::UnsupportedWalVersion(self.version));
        }
        Ok(())
    }
}

// Header encoding
impl TryInto<Vec<u8>> for Header {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        Ok(self.encode().to_vec())
    }
}

impl TryInto<Vec<u8>> for &Header {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        Ok(self.encode().to_vec())
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::InvalidHeader);
        }
        Header::decode(&bytes[..HEADER_SIZE].try_into().unwrap())
    }
}

impl TryFrom<&Vec<u8>> for Header {
    type Error = Error;

    fn try_from(value: &Vec<u8>) -> Result<Self> {
        Header::try_from(value.as_slice())
    }
}

impl Header {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(&self.magic);
        (&mut buf[8..12])
            .write_u32::<BigEndian>(self.version)
            .unwrap();
        (&mut buf[12..20])
            .write_u64::<BigEndian>(self.entry_count)
            .unwrap();
        buf
    }

    pub fn decode(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);

        let version = (&buf[8..12]).read_u32::<BigEndian>()?;
        let entry_count = (&buf[12..20]).read_u64::<BigEndian>()?;

        let header = Self {
            magic,
            version,
            entry_count,
        };
        header.validate()?;
        Ok(header)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encoding_decoding() {
        let mut header = Header::new();
        header.entry_count = 42;

        let encoded = header.encode();
        assert_eq!(encoded.len(), HEADER_SIZE);

        let decoded = Header::decode(&encoded).expect("Failed to decode Header");
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_header_magic_validation() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(b"INVALID!");
        (&mut buf[8..12]).write_u32::<BigEndian>(1).unwrap();
        (&mut buf[12..20]).write_u64::<BigEndian>(0).unwrap();

        let result = Header::decode(&buf);
        assert!(matches!(result, Err(Error::InvalidWalMagic)));
    }

    #[test]
    fn test_header_version_validation() {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..8].copy_from_slice(b"ASHDB\x00WL");
        (&mut buf[8..12]).write_u32::<BigEndian>(999).unwrap();
        (&mut buf[12..20]).write_u64::<BigEndian>(0).unwrap();

        let result = Header::decode(&buf);
        assert!(matches!(result, Err(Error::UnsupportedWalVersion(999))));
    }

    #[test]
    fn test_header_decoding_invalid_length() {
        let invalid_data = [0u8; HEADER_SIZE - 2];
        let result = Header::try_from(&invalid_data[..]);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidHeader) => {}
            _ => panic!("Expected InvalidHeader error"),
        }
    }
}
