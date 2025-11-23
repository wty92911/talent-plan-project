#![deny(missing_docs)]
//! # KvStore
//!
//! This crate provides a key-value storage utilites as memory-database
//!
//! ## Example Usage
//!
//! ```rust
//! use kvs::KvStore;
//!
//! let mut kvs = KvStore::new();
//!
//! kvs.get("key1");
//! kvs.set("key1", "value1");
//! kvs.remove("key1");
//! ```
use std::collections::HashMap;

/// The KvStore structures.
///
/// This struct stores the key-value mappings as database in memory.
///
///  ## Example Usage
/// ```rust
/// use kvs::KvStore;
///
/// let mut kvs = KvStore::new();
///
/// kvs.get("key1");
/// ```
pub struct KvStore {
    inner: HashMap<String, String>,
}

impl KvStore {
    /// Creates a new `KvStore` with empty contents.
    pub fn new() -> KvStore {
        KvStore {
            inner: HashMap::new(),
        }
    }

    /// Set a pair of **key-value**
    pub fn set(&mut self, key: String, value: String) {
        self.inner.insert(key, value);
    }

    /// Get the `value` for `key`
    pub fn get(&self, key: String) -> Option<String> {
        self.inner.get(&key).cloned()
    }

    /// Remove the `key`.
    pub fn remove(&mut self, key: String) {
        self.inner.remove(&key);
    }
}
impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
