//! A module for engine.
//!
//!

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use crate::error::{KvsError, Result};

/// A trait for key-value store engine.
pub trait KvsEngine: Clone + Send + 'static {
    /// Set a key-value pair.
    fn set(&self, key: String, value: String) -> Result<()>;

    /// Get a value by key.
    fn get(&self, key: String) -> Result<Option<String>>;

    /// Remove a key-value pair.
    fn remove(&self, key: String) -> Result<()>;
}
/// A key-value store engine.
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<Mutex<crate::kv_store::KvStore>>,
}

impl KvStore {
    /// Create a new kvs store engine at the given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let db = crate::kv_store::KvStore::open(path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner.lock().unwrap().set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.inner.lock().unwrap().get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.inner.lock().unwrap().remove(key)
    }
}
/// A sled engine.
#[derive(Clone)]
pub struct SledEngine {
    inner: Arc<Mutex<sled::Db>>,
}

impl SledEngine {
    /// Create a new sled engine at the given path.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let db = sled::open(path).map_err(|e| {
            KvsError::IOError(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("sled error: {}", e),
            ))
        })?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }
}

impl KvsEngine for SledEngine {
    /// Set a key-value pair.
    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner
            .lock()
            .unwrap()
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| KvsError::IOError(e.into()))?;
        self.inner
            .lock()
            .unwrap()
            .flush()
            .map_err(|e| KvsError::IOError(e.into()))?;
        Ok(())
    }

    /// Get a value by key.
    fn get(&self, key: String) -> Result<Option<String>> {
        match self
            .inner
            .lock()
            .unwrap()
            .get(key.as_bytes())
            .map_err(|e| KvsError::IOError(e.into()))?
        {
            Some(value) => {
                let value = String::from_utf8(value.to_vec()).map_err(|e| {
                    KvsError::IOError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Invalid UTF-8: {}", e),
                    ))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Remove a key-value pair.
    fn remove(&self, key: String) -> Result<()> {
        let result = self
            .inner
            .lock()
            .unwrap()
            .remove(key.as_bytes())
            .map_err(|e| KvsError::IOError(e.into()))?;
        if result.is_none() {
            return Err(KvsError::NonExistentKey(key));
        }
        self.inner
            .lock()
            .unwrap()
            .flush()
            .map_err(|e| KvsError::IOError(e.into()))?;
        Ok(())
    }
}
