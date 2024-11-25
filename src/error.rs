use std::io;

pub type Result<T> = std::result::Result<T, Error>;

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
    KeyNotFound,   // From LSM store
    Frozen,
    InvalidWalId(String),
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
            Error::Frozen => write!(f, "Memtable is frozen"),
            Error::KeyNotFound => write!(f, "Key not found"),
            Error::InvalidWalId(msg) => write!(f, "Invalid WAL ID: {}", msg),
        }
    }
}

impl std::error::Error for Error {}
