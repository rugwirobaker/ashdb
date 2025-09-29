pub mod bincode;
pub mod format;
pub mod keycode;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::Result;

/// Trait for encoding keys with order preservation using keycode.
///
/// Keys must maintain lexicographic ordering after encoding to support
/// efficient range scans. This trait uses the keycode module for
/// order-preserving serialization.
pub trait Key: Serialize + DeserializeOwned {
    /// Encode the key to bytes while preserving sort order
    fn encode(&self) -> Vec<u8> {
        keycode::serialize(self)
    }

    /// Decode bytes back to the original key type
    fn decode(bytes: &[u8]) -> Result<Self> {
        keycode::deserialize(bytes)
    }
}

/// Trait for encoding values for storage using bincode.
///
/// Values don't need to preserve ordering, so we can use more efficient
/// serialization methods like bincode.
pub trait Value: Serialize + DeserializeOwned {
    /// Encode the value to bytes
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self)
    }

    /// Decode bytes back to the original value type
    fn decode(bytes: &[u8]) -> Result<Self> {
        bincode::deserialize(bytes)
    }
}

// Blanket implementations for common types that can be used as keys
impl Key for bool {}
impl Key for i64 {}
impl Key for u64 {}
impl Key for f64 {}
impl Key for String {}
impl Key for Vec<u8> {}

// Blanket implementations for common types that can be used as values
impl<T> Value for T where T: Serialize + DeserializeOwned {}

// Additional key implementations for tuples (commonly used in composite keys)
impl<A: Serialize + DeserializeOwned> Key for (A,) {}
impl<A: Serialize + DeserializeOwned, B: Serialize + DeserializeOwned> Key for (A, B) {}
impl<
        A: Serialize + DeserializeOwned,
        B: Serialize + DeserializeOwned,
        C: Serialize + DeserializeOwned,
    > Key for (A, B, C)
{
}
impl<
        A: Serialize + DeserializeOwned,
        B: Serialize + DeserializeOwned,
        C: Serialize + DeserializeOwned,
        D: Serialize + DeserializeOwned,
    > Key for (A, B, C, D)
{
}
