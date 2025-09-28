use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// AshDB errors.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Error {
    /// The operation was aborted and must be retried. This typically happens
    /// with system state changes that require operation retry.
    Abort,
    /// Invalid data, typically decoding errors, corruption, or unexpected internal values.
    InvalidData(String),
    /// Invalid user input, typically parser or query errors.
    InvalidInput(String),
    /// An IO error.
    IO(String),
    /// A write was attempted on a read-only structure (frozen memtable, SSTable, etc.).
    ReadOnly,
    /// A write transaction conflicted with a different writer and lost. The
    /// transaction must be retried.
    Serialization,
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Abort => write!(f, "operation aborted"),
            Error::InvalidData(msg) => write!(f, "invalid data: {msg}"),
            Error::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Error::IO(msg) => write!(f, "io error: {msg}"),
            Error::ReadOnly => write!(f, "write attempted on read-only structure"),
            Error::Serialization => write!(f, "serialization failure, retry transaction"),
        }
    }
}

/// Constructs an Error::InvalidData for the given format string.
#[macro_export]
macro_rules! errdata {
    ($($args:tt)*) => { $crate::error::Error::InvalidData(format!($($args)*)).into() };
}

/// Constructs an Error::InvalidInput for the given format string.
#[macro_export]
macro_rules! errinput {
    ($($args:tt)*) => { $crate::error::Error::InvalidInput(format!($($args)*)).into() };
}

/// An AshDB Result returning Error.
pub type Result<T> = std::result::Result<T, Error>;

impl<T> From<Error> for Result<T> {
    fn from(error: Error) -> Self {
        Err(error)
    }
}

impl serde::de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl serde::ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::InvalidData(msg.to_string())
    }
}

impl From<Box<bincode::ErrorKind>> for Error {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        Error::InvalidData(err.to_string())
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err.to_string())
    }
}

impl<T> From<std::sync::PoisonError<T>> for Error {
    fn from(err: std::sync::PoisonError<T>) -> Self {
        Error::IO(err.to_string())
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Error::IO(err.to_string())
    }
}
