use crate::error::Result;
use crate::Error;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

// 14 bytes
pub const HEADER_SIZE: usize = 22;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Header {
    pub magic_number: u32,
    pub version: u16,
    pub entry_count: u64,
    pub checksum: u64,
}

impl Header {
    pub fn new(version: u16) -> Self {
        Header {
            magic_number: 0x57_41_4C, // ASCII "WAL"
            version: version,
            entry_count: 0,
            checksum: 0,
        }
    }
}

// Header encoding
impl TryInto<Vec<u8>> for Header {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        let mut encoder = HeaderEncoder::new(&mut buf);
        encoder.encode(&self)?;
        Ok(buf)
    }
}

impl TryInto<Vec<u8>> for &Header {
    type Error = Error;

    fn try_into(self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(HEADER_SIZE);
        let mut encoder = HeaderEncoder::new(&mut buf);
        encoder.encode(self)?;
        Ok(buf)
    }
}

impl TryFrom<&[u8]> for Header {
    type Error = Error;

    fn try_from(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < HEADER_SIZE {
            return Err(Error::InvalidHeader);
        }

        let mut decoder = HeaderDecoder::new(&bytes[..]);
        decoder.decode()
    }
}

impl TryFrom<&Vec<u8>> for Header {
    type Error = Error;

    fn try_from(value: &Vec<u8>) -> Result<Self> {
        Header::try_from(value.as_slice())
    }
}

// Header decoder
pub struct HeaderDecoder<R: Read> {
    reader: R,
}

impl<R: Read> HeaderDecoder<R> {
    pub fn new(reader: R) -> Self {
        HeaderDecoder { reader }
    }

    pub fn decode(&mut self) -> Result<Header> {
        let magic_number = self
            .reader
            .read_u32::<BigEndian>()
            .map_err(|e| Error::Decode("magic_number", e))?;

        let version = self
            .reader
            .read_u16::<BigEndian>()
            .map_err(|e| Error::Decode("version", e))?;

        let entry_count = self
            .reader
            .read_u64::<BigEndian>()
            .map_err(|e| Error::Decode("entry_count", e))?;

        let checksum = self
            .reader
            .read_u64::<BigEndian>()
            .map_err(|e| Error::Decode("checksum", e))?;

        Ok(Header {
            magic_number,
            version,
            entry_count,
            checksum,
        })
    }
}

// Header encoder
pub struct HeaderEncoder<W: Write> {
    writer: W,
}

impl<W: Write> HeaderEncoder<W> {
    pub fn new(writer: W) -> Self {
        HeaderEncoder { writer }
    }

    pub fn encode(&mut self, header: &Header) -> Result<()> {
        self.writer
            .write_u32::<BigEndian>(header.magic_number)
            .map_err(|e| Error::Encode("magic_number", e))?;

        self.writer
            .write_u16::<BigEndian>(header.version)
            .map_err(|e| Error::Encode("version", e))?;

        self.writer
            .write_u64::<BigEndian>(header.entry_count)
            .map_err(|e| Error::Encode("entry_count", e))?;

        self.writer
            .write_u64::<BigEndian>(header.checksum)
            .map_err(|e| Error::Encode("checksum", e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_encoding_decoding() {
        // Create a sample Header
        let header = Header {
            magic_number: 0x57_41_4C,
            version: 1,
            entry_count: 42,
            checksum: 12345,
        };

        // Encode the Header into bytes
        let encoded: Vec<u8> = header.try_into().expect("Failed to encode Header");

        // Ensure the encoded length matches HEADER_SIZE
        assert_eq!(encoded.len(), HEADER_SIZE);

        // Decode the bytes back into a Header
        let decoded = Header::try_from(&encoded[..]).expect("Failed to decode Header");

        // Verify the decoded Header matches the original
        assert_eq!(header, decoded);
    }

    #[test]
    fn test_header_encoding_error() {
        // Simulate a write failure by using a writer that errors
        struct FailingWriter;

        impl Write for FailingWriter {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Write failure",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let header = Header {
            magic_number: 0x57_41_4C,
            version: 1,
            entry_count: 42,
            checksum: 12345,
        };

        let mut writer = FailingWriter;
        let mut encoder = HeaderEncoder::new(&mut writer);

        // Ensure encoding fails
        let result = encoder.encode(&header);
        assert!(result.is_err());
    }

    #[test]
    fn test_header_decoding_invalid_length() {
        // Provide fewer bytes than required for HEADER_SIZE
        let invalid_data = vec![0u8; HEADER_SIZE - 2];

        // Attempt decoding and ensure it fails
        let result = Header::try_from(&invalid_data[..]);
        assert!(result.is_err());
        match result {
            Err(Error::InvalidHeader) => {} // Expected error
            _ => panic!("Expected InvalidHeader error"),
        }
    }
}
