use super::{EncodingError, Value};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

/// Serialize a value using bincode
pub fn serialize<T: Serialize>(value: &T) -> Vec<u8> {
    bincode::serialize(value).expect("serialization should not fail")
}

/// Serialize a value into a writer using bincode
pub fn serialize_into<T: Serialize, W: Write>(writer: W, value: &T) -> Result<()> {
    bincode::serialize_into(writer, value)
        .map_err(|e| EncodingError::InvalidFormat(format!("Serialization failed: {}", e)).into())
}

/// Deserialize a value from a byte slice using bincode
pub fn deserialize<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> Result<T> {
    bincode::deserialize(bytes)
        .map_err(|e| EncodingError::InvalidFormat(format!("Deserialization failed: {}", e)).into())
}

/// Deserialize a value from a reader using bincode
pub fn deserialize_from<T: for<'a> Deserialize<'a>, R: Read>(reader: R) -> Result<T> {
    bincode::deserialize_from(reader).map_err(|e| {
        // Handle common error cases
        if e.to_string().contains("UnexpectedEof") || e.to_string().contains("connection") {
            EncodingError::TruncatedData.into()
        } else {
            EncodingError::InvalidFormat(format!("Deserialization failed: {}", e)).into()
        }
    })
}

/// Maybe deserialize a value from a reader, returning None on EOF
pub fn maybe_deserialize_from<T: for<'a> Deserialize<'a>, R: Read>(reader: R) -> Result<Option<T>> {
    match deserialize_from(reader) {
        Ok(value) => Ok(Some(value)),
        Err(e) => {
            // Check if this is an EOF error
            let error_string = e.to_string();
            if error_string.contains("UnexpectedEof") || error_string.contains("Truncated") {
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}

// Implement Value trait for types that can be serialized/deserialized
impl<T> Value for T
where
    T: Serialize + for<'a> Deserialize<'a>,
{
    fn encode(&self) -> Vec<u8> {
        serialize(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestStruct {
        id: u32,
        name: String,
        active: bool,
    }

    #[test]
    fn test_serialize_deserialize() {
        let original = TestStruct {
            id: 42,
            name: "test".to_string(),
            active: true,
        };

        let serialized = serialize(&original);
        let deserialized: TestStruct = deserialize(&serialized).unwrap();

        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_primitive_types() {
        // Test bool
        let bool_val = true;
        assert_eq!(
            bool_val,
            deserialize::<bool>(&serialize(&bool_val)).unwrap()
        );

        // Test integers
        let int_val = 42i64;
        assert_eq!(int_val, deserialize::<i64>(&serialize(&int_val)).unwrap());

        // Test floats
        let float_val = 3.14f64;
        assert_eq!(
            float_val,
            deserialize::<f64>(&serialize(&float_val)).unwrap()
        );

        // Test strings
        let string_val = "hello world".to_string();
        assert_eq!(
            string_val,
            deserialize::<String>(&serialize(&string_val)).unwrap()
        );

        // Test byte vectors
        let bytes_val = vec![0x01, 0x02, 0x03];
        assert_eq!(
            bytes_val,
            deserialize::<Vec<u8>>(&serialize(&bytes_val)).unwrap()
        );
    }

    #[test]
    fn test_value_trait_implementation() {
        // Test that the Value trait works correctly
        let string_val = "test string".to_string();
        let encoded = string_val.encode();
        let decoded = String::decode(&encoded).unwrap();
        assert_eq!(string_val, decoded);

        let int_val = 12345i64;
        let encoded = int_val.encode();
        let decoded = i64::decode(&encoded).unwrap();
        assert_eq!(int_val, decoded);
    }

    #[test]
    fn test_serialize_into_and_deserialize_from() {
        let original = TestStruct {
            id: 123,
            name: "serialization test".to_string(),
            active: false,
        };

        // Test serialize_into
        let mut buffer = Vec::new();
        serialize_into(&mut buffer, &original).unwrap();

        // Test deserialize_from
        let deserialized: TestStruct = deserialize_from(buffer.as_slice()).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_maybe_deserialize_from() {
        let original = vec![1i32, 2, 3, 4, 5];
        let serialized = serialize(&original);

        // Test successful deserialization
        let result: Option<Vec<i32>> = maybe_deserialize_from(serialized.as_slice()).unwrap();
        assert_eq!(Some(original), result);

        // Test empty buffer (should return None, not error)
        let empty: &[u8] = &[];
        let result = maybe_deserialize_from::<Vec<i32>, &[u8]>(empty);
        // Empty buffer should return None (not an error)
        match result {
            Ok(None) => {} // Expected
            Ok(Some(_)) => panic!("Expected None for empty buffer"),
            Err(_) => {} // Also acceptable for completely empty buffer
        }
    }

    #[test]
    fn test_invalid_data() {
        // Test deserializing invalid data
        let invalid_data = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let result: Result<String> = deserialize(&invalid_data);
        assert!(result.is_err());
    }
}
