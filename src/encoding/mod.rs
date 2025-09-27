pub mod bincode;
pub mod keycode;

use crate::error::Result;

/// Trait for encoding keys with order preservation.
///
/// Keys must maintain lexicographic ordering after encoding to support
/// efficient range scans. This is critical for the SCAN command in the CLI.
pub trait Key {
    /// Encode the key to bytes while preserving sort order
    fn encode(&self) -> Vec<u8>;

    /// Decode bytes back to the original key type
    fn decode(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// Trait for encoding values for storage.
///
/// Values don't need to preserve ordering, so we can use more efficient
/// serialization methods.
pub trait Value {
    /// Encode the value to bytes
    fn encode(&self) -> Vec<u8>;

    /// Decode bytes back to the original value type
    fn decode(bytes: &[u8]) -> Result<Self>
    where
        Self: Sized;
}

/// Error type for encoding operations
#[derive(Debug)]
pub enum EncodingError {
    InvalidFormat(String),
    UnsupportedType,
    TruncatedData,
    InvalidUtf8,
}

impl std::fmt::Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingError::InvalidFormat(msg) => write!(f, "Invalid encoding format: {}", msg),
            EncodingError::UnsupportedType => write!(f, "Unsupported data type"),
            EncodingError::TruncatedData => write!(f, "Truncated data"),
            EncodingError::InvalidUtf8 => write!(f, "Invalid UTF-8 sequence"),
        }
    }
}

impl std::error::Error for EncodingError {}

impl From<EncodingError> for crate::Error {
    fn from(err: EncodingError) -> Self {
        crate::Error::InvalidOperation(err.to_string())
    }
}
