use std::fmt;

use crc::{Algorithm, Crc};

pub const CRC_64_ECMA: Algorithm<u64> = crc::CRC_64_ECMA_182; // Use the desired algorithm
pub struct Hasher {
    rolling_checksum: u64, // Current rolling checksum
    crc64: Crc<u64>,       // CRC64 instance
}

impl fmt::Debug for Hasher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Hasher")
            .field("rolling_checksum", &self.rolling_checksum)
            .finish()
    }
}

impl Hasher {
    /// Creates a new `Hasher` with an initial rolling checksum.
    pub fn new() -> Self {
        Self {
            rolling_checksum: 0,
            crc64: Crc::<u64>::new(&CRC_64_ECMA),
        }
    }

    /// Updates the checksum with a new key-value pair.
    pub fn update(&mut self, key: &[u8], value: &[u8]) {
        // Compute the checksum for the key
        let key_checksum = self.crc64.checksum(key);

        // Compute the checksum for the value
        let value_checksum = self.crc64.checksum(value);

        // XOR the combined key + value checksum into the rolling checksum
        self.rolling_checksum ^= key_checksum ^ value_checksum;
    }

    /// Returns the current rolling checksum.
    pub fn value(&self) -> u64 {
        self.rolling_checksum
    }

    /// Resets the rolling checksum to its initial state.
    pub fn reset(&mut self) {
        self.rolling_checksum = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rolling_checksum() {
        let mut hasher = Hasher::new();

        // Add a key-value pair in a single step
        hasher.update(b"key1", b"value1");
        let single_step_checksum = hasher.value();

        // Reset and add the same key-value pair in separate steps
        hasher.reset();
        hasher.update(b"key1", b""); // Add key only
        let partial_checksum = hasher.value();
        hasher.update(b"", b"value1"); // Add value only
        let multi_step_checksum = hasher.value();

        // The rolling checksum should match whether added in a single step or multiple steps
        assert_eq!(single_step_checksum, multi_step_checksum);

        // Ensure the intermediate partial checksum is different
        assert_ne!(single_step_checksum, partial_checksum);
    }
}
