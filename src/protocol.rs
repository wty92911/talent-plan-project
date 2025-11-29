//! Client-server communication protocol definitions.
//!
//! This module defines the message types used for communication between
//! the key-value store client and server over TCP connections.

use serde::{Deserialize, Serialize};

/// Client request message.
///
/// Represents operations that clients can request from the server.
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Set a key-value pair in the store.
    Set {
        /// The key to set.
        key: String,
        /// The value to associate with the key.
        value: String,
    },
    /// Get the value associated with a key.
    Get {
        /// The key to retrieve.
        key: String,
    },
    /// Remove a key-value pair from the store.
    Remove {
        /// The key to remove.
        key: String,
    },
}

/// Server response message.
///
/// Represents the server's response to client requests.
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Operation completed successfully.
    Ok,
    /// Retrieved value, `None` if key doesn't exist.
    Value(Option<String>),
    /// Operation failed with error message.
    Err(String),
}
