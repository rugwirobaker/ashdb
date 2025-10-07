pub mod bincode;
pub mod format;
pub mod keycode;

use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// Trait for encoding keys with order preservation using keycode.
///
/// Keys must maintain lexicographic ordering after encoding to support
/// efficient range scans. This trait uses the keycode module for
/// order-preserving serialization.
pub trait Key<'de>: Serialize + Deserialize<'de> {
    /// Encode the key to bytes while preserving sort order
    fn encode(&self) -> Vec<u8> {
        keycode::serialize(self)
    }

    /// Decode bytes back to the original key type
    fn decode(bytes: &'de [u8]) -> Result<Self> {
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
impl<V: Value> Value for Option<V> {}
impl<V: Value> Value for Result<V> {}
impl<V: Value> Value for Vec<V> {}
impl<V1: Value, V2: Value> Value for (V1, V2) {}
impl<V: Value + Eq + Hash> Value for HashSet<V> {}
impl<V: Value + Eq + Ord + Hash> Value for BTreeSet<V> {}
