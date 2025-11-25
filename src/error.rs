//! Error
//!
//! This module provides the errors needed in [`crate::KvStore`]
//! with [`thiserror`]
//!
use std::io;
use thiserror::Error;

/// Result use the [`KvsError`] as error.
pub type Result<T> = std::result::Result<T, KvsError>;

/// KvsError is the specific error for [`crate::KvStore`]
#[derive(Error, Debug)]
pub enum KvsError {
    #[error("io error {0}")]
    /// IO relevant errors
    IOError(#[from] io::Error),
    #[error("serde error {0}")]
    /// Serialized or Deserialized errors
    SerdeError(#[from] serde_json::Error),

    /// Remove a non existent key
    #[error("the key {0} is not existent")]
    NonExistentKey(String),

    /// Deserializing Error
    #[error("error when deserialize from files")]
    DeserializeError,
}
