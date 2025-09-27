use super::{EncodingError, Key};
use crate::error::Result;

// Type prefixes for order-preserving encoding
// Order matters: these determine cross-type ordering
const TYPE_BOOLEAN: u8 = 0x01;
const TYPE_INTEGER: u8 = 0x02;
const TYPE_FLOAT: u8 = 0x03;
const TYPE_STRING: u8 = 0x04;
const TYPE_BYTES: u8 = 0x05;

/// Encode a boolean with order preservation
pub fn encode_boolean(b: bool) -> Vec<u8> {
    vec![TYPE_BOOLEAN, if b { 0x01 } else { 0x00 }]
}

/// Decode a boolean from bytes
pub fn decode_boolean(bytes: &[u8]) -> Result<bool> {
    if bytes.is_empty() {
        return Err(EncodingError::TruncatedData.into());
    }

    if bytes[0] != TYPE_BOOLEAN {
        return Err(EncodingError::InvalidFormat("Not a boolean type".to_string()).into());
    }

    if bytes.len() != 2 {
        return Err(
            EncodingError::InvalidFormat("Boolean must be 2 bytes total".to_string()).into(),
        );
    }

    match bytes[1] {
        0x00 => Ok(false),
        0x01 => Ok(true),
        _ => Err(EncodingError::InvalidFormat("Invalid boolean value".to_string()).into()),
    }
}

/// Encode an integer with order preservation
pub fn encode_integer(i: i64) -> Vec<u8> {
    // Flip the sign bit to ensure negative numbers sort before positive
    let unsigned = (i as u64) ^ (1u64 << 63);
    let mut result = vec![TYPE_INTEGER];
    result.extend_from_slice(&unsigned.to_be_bytes());
    result
}

/// Decode an integer from bytes
pub fn decode_integer(bytes: &[u8]) -> Result<i64> {
    if bytes.is_empty() {
        return Err(EncodingError::TruncatedData.into());
    }

    if bytes[0] != TYPE_INTEGER {
        return Err(EncodingError::InvalidFormat("Not an integer type".to_string()).into());
    }

    if bytes.len() != 9 {
        return Err(
            EncodingError::InvalidFormat("Integer must be 9 bytes total".to_string()).into(),
        );
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[1..9]);
    let unsigned = u64::from_be_bytes(buf);

    // Flip the sign bit back
    let signed = (unsigned ^ (1u64 << 63)) as i64;
    Ok(signed)
}

/// Encode a float with order preservation
pub fn encode_float(f: f64) -> Vec<u8> {
    let bits = f.to_bits();

    // Handle IEEE 754 ordering:
    // - If negative, flip all bits
    // - If positive, flip only the sign bit
    let ordered_bits = if bits & (1u64 << 63) != 0 {
        // Negative: flip all bits
        !bits
    } else {
        // Positive: flip sign bit to 1
        bits | (1u64 << 63)
    };

    let mut result = vec![TYPE_FLOAT];
    result.extend_from_slice(&ordered_bits.to_be_bytes());
    result
}

/// Decode a float from bytes
pub fn decode_float(bytes: &[u8]) -> Result<f64> {
    if bytes.is_empty() {
        return Err(EncodingError::TruncatedData.into());
    }

    if bytes[0] != TYPE_FLOAT {
        return Err(EncodingError::InvalidFormat("Not a float type".to_string()).into());
    }

    if bytes.len() != 9 {
        return Err(EncodingError::InvalidFormat("Float must be 9 bytes total".to_string()).into());
    }

    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[1..9]);
    let ordered_bits = u64::from_be_bytes(buf);

    // Reverse the ordering transformation
    let original_bits = if ordered_bits & (1u64 << 63) != 0 {
        if ordered_bits == (1u64 << 63) {
            // This was a positive zero, restore it
            0
        } else {
            // This was positive, remove the flipped sign bit
            ordered_bits & !(1u64 << 63)
        }
    } else {
        // This was negative, flip all bits back
        !ordered_bits
    };

    let f = f64::from_bits(original_bits);
    Ok(f)
}

/// Encode a string with order preservation
pub fn encode_string(s: &str) -> Vec<u8> {
    let mut result = vec![TYPE_STRING];
    result.extend_from_slice(s.as_bytes());
    // Null terminator to handle prefix scans correctly
    result.push(0x00);
    result
}

/// Decode a string from bytes
pub fn decode_string(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() {
        return Err(EncodingError::TruncatedData.into());
    }

    if bytes[0] != TYPE_STRING {
        return Err(EncodingError::InvalidFormat("Not a string type".to_string()).into());
    }

    let string_bytes = &bytes[1..];

    // Remove null terminator if present
    let string_bytes = if string_bytes.last() == Some(&0x00) {
        &string_bytes[..string_bytes.len() - 1]
    } else {
        string_bytes
    };

    let s = std::str::from_utf8(string_bytes).map_err(|_| EncodingError::InvalidUtf8)?;

    Ok(s.to_string())
}

/// Encode raw bytes with order preservation
pub fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut result = vec![TYPE_BYTES];

    // Escape null bytes to preserve ordering
    for &byte in bytes {
        if byte == 0x00 {
            result.extend_from_slice(&[0x00, 0xFF]);
        } else {
            result.push(byte);
        }
    }

    // Final null terminator
    result.extend_from_slice(&[0x00, 0x00]);
    result
}

/// Decode raw bytes from encoded form
pub fn decode_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty() {
        return Err(EncodingError::TruncatedData.into());
    }

    if bytes[0] != TYPE_BYTES {
        return Err(EncodingError::InvalidFormat("Not a bytes type".to_string()).into());
    }

    let encoded_bytes = &bytes[1..];
    let mut result = Vec::new();
    let mut i = 0;

    while i < encoded_bytes.len() {
        if encoded_bytes[i] == 0x00 {
            if i + 1 < encoded_bytes.len() {
                match encoded_bytes[i + 1] {
                    0xFF => {
                        // Escaped null byte
                        result.push(0x00);
                        i += 2;
                    }
                    0x00 => {
                        // End marker
                        break;
                    }
                    _ => {
                        return Err(EncodingError::InvalidFormat(
                            "Invalid null byte escape".to_string(),
                        )
                        .into());
                    }
                }
            } else {
                return Err(EncodingError::TruncatedData.into());
            }
        } else {
            result.push(encoded_bytes[i]);
            i += 1;
        }
    }

    Ok(result)
}

/// Get the type prefix from encoded bytes
pub fn get_type_prefix(bytes: &[u8]) -> Option<u8> {
    bytes.first().copied()
}

// Implement Key trait for common Rust types used in keys
impl Key for bool {
    fn encode(&self) -> Vec<u8> {
        encode_boolean(*self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_boolean(bytes)
    }
}

impl Key for i64 {
    fn encode(&self) -> Vec<u8> {
        encode_integer(*self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_integer(bytes)
    }
}

impl Key for f64 {
    fn encode(&self) -> Vec<u8> {
        encode_float(*self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_float(bytes)
    }
}

impl Key for String {
    fn encode(&self) -> Vec<u8> {
        encode_string(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_string(bytes)
    }
}

impl Key for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        encode_bytes(self)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        decode_bytes(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_boolean_ordering() {
        let false_encoded = false.encode();
        let true_encoded = true.encode();

        // false should sort before true
        assert!(false_encoded < true_encoded);

        // Test round-trip
        assert!(!bool::decode(&false_encoded).unwrap());
        assert!(bool::decode(&true_encoded).unwrap());
    }

    #[test]
    fn test_integer_ordering() {
        let values = [-100i64, -1, 0, 1, 100];
        let encoded: Vec<_> = values.iter().map(|v| v.encode()).collect();

        // Test that encoded values maintain order
        for i in 1..encoded.len() {
            assert!(encoded[i - 1] < encoded[i]);
        }

        // Test round-trip
        for (original, encoded) in values.iter().zip(encoded.iter()) {
            assert_eq!(&i64::decode(encoded).unwrap(), original);
        }
    }

    #[test]
    fn test_float_ordering() {
        let values = [-100.5, -1.0, 0.0, 1.0, 100.5];
        let encoded: Vec<_> = values.iter().map(|v| v.encode()).collect();

        // Test that encoded values maintain order
        for i in 1..encoded.len() {
            assert!(encoded[i - 1] < encoded[i]);
        }

        // Test round-trip
        for (original, encoded) in values.iter().zip(encoded.iter()) {
            assert_eq!(&f64::decode(encoded).unwrap(), original);
        }
    }

    #[test]
    fn test_string_ordering() {
        let values = ["apple", "banana", "cherry"];
        let strings: Vec<String> = values.iter().map(|s| s.to_string()).collect();
        let encoded: Vec<_> = strings.iter().map(|v| v.encode()).collect();

        // Test that encoded values maintain order
        for i in 1..encoded.len() {
            assert!(encoded[i - 1] < encoded[i]);
        }

        // Test round-trip
        for (original, encoded) in strings.iter().zip(encoded.iter()) {
            assert_eq!(&String::decode(encoded).unwrap(), original);
        }
    }

    #[test]
    fn test_bytes_encoding() {
        let test_cases = vec![
            vec![],
            vec![0x01, 0x02, 0x03],
            vec![0x00],       // null byte
            vec![0x00, 0xFF], // null and 0xFF
            vec![0xFF, 0x00], // 0xFF and null
        ];

        for original in test_cases {
            let encoded = original.encode();
            let decoded = Vec::<u8>::decode(&encoded).unwrap();
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_mixed_type_ordering() {
        // Test that types sort in the correct order: bool < i64 < f64 < String < Vec<u8>
        let bool_encoded = true.encode();
        let int_encoded = 42i64.encode();
        let float_encoded = std::f64::consts::PI.encode();
        let string_encoded = "hello".to_string().encode();
        let bytes_encoded = vec![0x01, 0x02].encode();

        assert!(bool_encoded < int_encoded);
        assert!(int_encoded < float_encoded);
        assert!(float_encoded < string_encoded);
        assert!(string_encoded < bytes_encoded);
    }

    #[test]
    fn test_type_prefix_detection() {
        assert_eq!(get_type_prefix(&true.encode()), Some(TYPE_BOOLEAN));
        assert_eq!(get_type_prefix(&42i64.encode()), Some(TYPE_INTEGER));
        assert_eq!(
            get_type_prefix(&std::f64::consts::PI.encode()),
            Some(TYPE_FLOAT)
        );
        assert_eq!(
            get_type_prefix(&"hello".to_string().encode()),
            Some(TYPE_STRING)
        );
        assert_eq!(get_type_prefix(&vec![0x01].encode()), Some(TYPE_BYTES));
    }
}
