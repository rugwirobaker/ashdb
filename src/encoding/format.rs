//! Formats raw keys and values, recursively where necessary. Handles both both
//! Raft, MVCC, SQL, and raw binary data.

use itertools::Itertools as _;

//use crate::storage::mvcc;

/// Formats encoded keys and values.
pub trait Formatter {
    /// Formats a key.
    fn key(key: &[u8]) -> String;

    /// Formats a value. Also takes the key to determine the kind of value.
    fn value(key: &[u8], value: &[u8]) -> String;

    /// Formats a key/value pair.
    fn key_value(key: &[u8], value: &[u8]) -> String {
        Self::key_maybe_value(key, Some(value))
    }

    /// Formats a key/value pair, where the value may not exist.
    fn key_maybe_value(key: &[u8], value: Option<&[u8]>) -> String {
        let fmtkey = Self::key(key);
        let fmtvalue = value.map_or("None".to_string(), |v| Self::value(key, v));
        format!("{fmtkey} → {fmtvalue}")
    }
}

/// Formats raw byte slices without any decoding.
pub struct Raw;

impl Raw {
    /// Formats raw bytes as escaped ASCII strings.
    pub fn bytes(bytes: &[u8]) -> String {
        let escaped = bytes
            .iter()
            .copied()
            .flat_map(std::ascii::escape_default)
            .collect_vec();
        format!("\"{}\"", String::from_utf8_lossy(&escaped))
    }
}

impl Formatter for Raw {
    fn key(key: &[u8]) -> String {
        Self::bytes(key)
    }

    fn value(_key: &[u8], value: &[u8]) -> String {
        Self::bytes(value)
    }
}

// /// Formats MVCC keys/values. Dispatches to F to format the inner key/value.
// pub struct MVCC<F: Formatter>(PhantomData<F>);

// impl<F: Formatter> Formatter for MVCC<F> {
//     fn key(key: &[u8]) -> String {
//         let Ok(key) = mvcc::Key::decode(key) else {
//             return Raw::key(key); // invalid key
//         };
//         match key {
//             mvcc::Key::TxnWrite(version, innerkey) => {
//                 format!("mvcc:TxnWrite({version}, {})", F::key(&innerkey))
//             }
//             mvcc::Key::Version(innerkey, version) => {
//                 format!("mvcc:Version({}, {version})", F::key(&innerkey))
//             }
//             mvcc::Key::Unversioned(innerkey) => {
//                 format!("mvcc:Unversioned({})", F::key(&innerkey))
//             }
//             mvcc::Key::NextVersion | mvcc::Key::TxnActive(_) | mvcc::Key::TxnActiveSnapshot(_) => {
//                 format!("mvcc:{key:?}")
//             }
//         }
//     }

//     fn value(key: &[u8], value: &[u8]) -> String {
//         let Ok(key) = mvcc::Key::decode(key) else {
//             return Raw::bytes(value); // invalid key
//         };
//         match key {
//             mvcc::Key::NextVersion => {
//                 let Ok(version) = bincode::deserialize::<mvcc::Version>(value) else {
//                     return Raw::bytes(value);
//                 };
//                 version.to_string()
//             }
//             mvcc::Key::TxnActiveSnapshot(_) => {
//                 let Ok(active) = bincode::deserialize::<BTreeSet<u64>>(value) else {
//                     return Raw::bytes(value);
//                 };
//                 format!("{{{}}}", active.iter().join(","))
//             }
//             mvcc::Key::TxnActive(_) | mvcc::Key::TxnWrite(_, _) => Raw::bytes(value),
//             mvcc::Key::Version(userkey, _) => match bincode::deserialize(value) {
//                 Ok(Some(value)) => F::value(&userkey, value),
//                 Ok(None) => "None".to_string(),
//                 Err(_) => Raw::bytes(value),
//             },
//             mvcc::Key::Unversioned(userkey) => F::value(&userkey, value),
//         }
//     }
// }
