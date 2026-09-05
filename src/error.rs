//! Error and Result types for Keystone.

use std::fmt;

/// The unified error type returned across the engine.
#[derive(Debug)]
pub enum Error {
    /// An underlying I/O failure.
    Io(std::io::Error),
    /// On-disk data failed an integrity or structural check.
    Corruption(String),
    /// An argument or state precondition was violated.
    Invalid(String),
}

/// Convenient result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io error: {e}"),
            Error::Corruption(m) => write!(f, "corruption: {m}"),
            Error::Invalid(m) => write!(f, "invalid: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl Error {
    /// Build a corruption error from anything displayable.
    pub fn corruption(msg: impl Into<String>) -> Self {
        Error::Corruption(msg.into())
    }

    /// Build an invalid-argument error from anything displayable.
    pub fn invalid(msg: impl Into<String>) -> Self {
        Error::Invalid(msg.into())
    }
}
