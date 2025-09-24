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
    InvalidState(String),
    // New variants
    ReadError(&'static str, io::Error),
    WriteError(&'static str, io::Error),
    IndexCorruption(String),
    LockError(io::Error),
    InvalidOperation(String),
    InvalidRecordType(u8),
    InvalidOperationType(u8),
    InvalidWalMagic,
    UnsupportedWalVersion(u32),
    InvalidAlignment,
    InvalidManifestMagic,
    UnsupportedManifestVersion(u32),
    InvalidEditType(u8),
    CorruptedManifest(String),
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
            Error::InvalidState(msg) => write!(f, "Invalid state: {}", msg),
            Error::ReadError(context, err) => write!(f, "Failed to read {}: {}", context, err),
            Error::WriteError(context, err) => write!(f, "Failed to write {}: {}", context, err),
            Error::IndexCorruption(msg) => write!(f, "Index corruption: {}", msg),
            Error::LockError(err) => write!(f, "Lock error: {}", err),
            Error::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            Error::InvalidRecordType(t) => write!(f, "Invalid record type: {}", t),
            Error::InvalidOperationType(t) => write!(f, "Invalid operation type: {}", t),
            Error::InvalidWalMagic => write!(f, "Invalid WAL magic number"),
            Error::UnsupportedWalVersion(v) => write!(f, "Unsupported WAL version: {}", v),
            Error::InvalidAlignment => write!(f, "Invalid alignment for O_DIRECT"),
            Error::InvalidManifestMagic => write!(f, "Invalid manifest magic number"),
            Error::UnsupportedManifestVersion(v) => {
                write!(f, "Unsupported manifest version: {}", v)
            }
            Error::InvalidEditType(t) => write!(f, "Invalid edit type: {}", t),
            Error::CorruptedManifest(msg) => write!(f, "Corrupted manifest: {}", msg),
        }
    }
}

impl std::error::Error for Error {}
