pub mod header;
pub mod wal;

use std::io;

#[derive(Debug)]
pub enum Error {
    IoError(io::Error),
    InvalidHeader,
    Decode(&'static str, io::Error),
    Encode(&'static str, io::Error),
    CorruptedWal(String),
    InvalidChecksum,
    ChecksumMismatch,
    MutexPoisoned, // From WAL-related concurrency handling
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IoError(err)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IoError(err) => write!(f, "I/O error: {}", err),
            Error::InvalidHeader => write!(f, "Invalid header"),
            Error::Decode(field, err) => write!(f, "Failed to decode {}: {}", field, err),
            Error::Encode(field, err) => write!(f, "Failed to encode {}: {}", field, err),
            Error::CorruptedWal(msg) => write!(f, "Corrupted WAL: {}", msg),
            Error::InvalidChecksum => write!(f, "Invalid checksum"),
            Error::ChecksumMismatch => write!(f, "Checksum mismatch"),
            Error::MutexPoisoned => write!(f, "Mutex was poisoned"),
        }
    }
}

impl std::error::Error for Error {}
