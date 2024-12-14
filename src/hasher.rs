use std::fmt;

use crc::{Algorithm, Crc};

pub const CRC_64_ECMA: Algorithm<u64> = crc::CRC_64_ECMA_182; // Use the desired algorithm

#[derive(Clone)]
pub struct Hasher {
    crc64: Crc<u64>,
    buffer: Vec<u8>, // Add a buffer to store data
}

impl fmt::Debug for Hasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hasher")
    }
}

impl Hasher {
    pub fn new() -> Self {
        Self {
            crc64: Crc::<u64>::new(&CRC_64_ECMA),
            buffer: Vec::new(),
        }
    }

    pub fn write(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    pub fn checksum(&self) -> u64 {
        self.crc64.checksum(&self.buffer)
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
    }
}

impl Default for Hasher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_incremental_checksum() {
        let mut hasher1 = Hasher::new();
        hasher1.write(b"hello ");
        hasher1.write(b"world");
        let checksum1 = hasher1.checksum();

        let mut hasher2 = Hasher::new();
        hasher2.write(b"hello world");
        let checksum2 = hasher2.checksum();

        assert_eq!(
            checksum1, checksum2,
            "Incremental and single-write checksums should match"
        );
    }

    #[test]
    fn test_reset_hasher() {
        let mut hasher = Hasher::new();
        hasher.write(b"hello");
        let first_checksum = hasher.checksum();

        hasher.reset();
        hasher.write(b"hello");
        let second_checksum = hasher.checksum();

        assert_eq!(
            first_checksum, second_checksum,
            "Checksums after reset should match for same input"
        );
    }

    #[test]
    fn test_different_data_different_checksums() {
        let mut hasher = Hasher::new();
        hasher.write(b"hello");
        let checksum1 = hasher.checksum();

        let mut hasher = Hasher::new();
        hasher.write(b"world");
        let checksum2 = hasher.checksum();

        assert_ne!(
            checksum1, checksum2,
            "Different data should have different checksums"
        );
    }
}
