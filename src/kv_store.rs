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
//! let mut kvs = KvStore::open("./").unwrap();
//!
//! kvs.get("key1".into()).unwrap();
//! kvs.set("key1".into(), "value1".into()).unwrap();
//! kvs.remove("key1".into()).unwrap();
//! ```
use std::fs::{self, File, OpenOptions};
use std::{collections::HashMap, path::PathBuf};

pub use crate::error::{KvsError, Result};
use walkdir::WalkDir;

use crate::log_helper::{FileIndex, LogHelper, Record};

const MAX_LOG_SIZE: u64 = 1 << 20;
const MAX_UNCOMPACTED_SIZE: u64 = 1 << 10;

/// The KvStore structures.
///
/// This struct stores the key-value mapping database.
///
///  ## Example Usage
/// ```rust
/// use kvs::KvStore;
///
/// let mut kvs = KvStore::open("./").unwrap();
///
/// kvs.get("key1".into());
/// ```
///
pub(crate) struct KvStore {
    log_dir: PathBuf,
    file_count: i32,
    cur_file: File,
    cur_path: PathBuf,

    idx: HashMap<String, FileIndex>,
    uncompacted: u64,
}

impl KvStore {
    /// Open the [`KvStore`] at a given dir path, and return it.
    /// Here we assume that there are only logs files like **1.log, 2.log** in the path dir.
    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        // Find the maximum log file number
        let mut file_count = 0;
        for entry in WalkDir::new(&path).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                if let Some(name) = entry.file_name().to_str() {
                    if let Some(num_str) = name.strip_suffix(".log") {
                        if let Ok(num) = num_str.parse::<i32>() {
                            if num > file_count {
                                file_count = num;
                            }
                        }
                    }
                }
            }
        }

        let (cur_file, cur_path) = {
            if file_count < 1 {
                file_count = 1;
            }
            KvStore::open_file(&path, file_count)?
        };
        let mut idx = HashMap::new();
        let mut uncompacted = 0;
        for num in 1..=file_count {
            let file_path = path.join(format!("{num}.log"));
            if file_path.exists() {
                for record in LogHelper::read_all(file_path)? {
                    let (record, file_index) = record;
                    match record {
                        Record::Set(key, _) => {
                            if let Some(_) = idx.insert(key, file_index) {
                                uncompacted += 1;
                            }
                        }
                        Record::Remove(key) => {
                            uncompacted += 1;
                            idx.remove(&key);
                        }
                    }
                }
            }
        }
        Ok(Self {
            log_dir: path,
            file_count,
            cur_file,
            cur_path,
            idx,
            uncompacted,
        })
    }

    /// Set a pair of **key-value**
    pub(crate) fn set(&mut self, key: String, value: String) -> Result<()> {
        self.check_if_new_file()?;
        let idx = LogHelper::write(
            &mut self.cur_file,
            self.cur_path.clone(),
            &Record::Set(key.clone(), value),
        )?;
        if let Some(_) = self.idx.insert(key, idx) {
            self.uncompacted += 1;
            self.record_uncompact()?;
        }

        Ok(())
    }

    /// Get the `value` for `key`
    pub(crate) fn get(&self, key: String) -> Result<Option<String>> {
        let idx = self.idx.get(&key);
        match idx {
            Some(idx) => {
                let record = LogHelper::read(idx)?;
                if let Record::Set(_, value) = record {
                    Ok(Some(value))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Remove the `key`.
    pub(crate) fn remove(&mut self, key: String) -> Result<()> {
        let value = self.idx.get(&key);
        if value.is_none() {
            Err(KvsError::NonExistentKey(key))
        } else {
            self.idx.remove(&key);
            self.check_if_new_file()?;
            LogHelper::write(
                &mut self.cur_file,
                self.cur_path.clone(),
                &Record::Remove(key),
            )?;
            self.record_uncompact()?;
            Ok(())
        }
    }
}

impl KvStore {
    pub(crate) fn open_file(log_dir: &PathBuf, file_count: i32) -> Result<(File, PathBuf)> {
        let file_path = log_dir.join(format!("{}.log", file_count));
        Ok((
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(file_path.clone())?,
            file_path,
        ))
    }

    fn new_file(&mut self) -> Result<()> {
        self.file_count += 1;
        (self.cur_file, self.cur_path) = KvStore::open_file(&self.log_dir, self.file_count)?;
        Ok(())
    }
    fn check_if_new_file(&mut self) -> Result<()> {
        if self.cur_file.metadata()?.len() > MAX_LOG_SIZE {
            self.new_file()?;
        }
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        self.uncompacted = 0;
        let old_file_count = self.file_count;
        self.new_file()?;

        for (_, v) in self.idx.iter_mut() {
            let record = LogHelper::read(v)?;
            let new_v = LogHelper::write(&mut self.cur_file, self.cur_path.clone(), &record)?;
            *v = new_v;
        }

        for num in 1..=old_file_count {
            let path = self.log_dir.join(format!("{num}.log"));
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }

    fn record_uncompact(&mut self) -> Result<()> {
        self.uncompacted += 1;
        if self.uncompacted >= MAX_UNCOMPACTED_SIZE {
            self.compact()?;
        }
        Ok(())
    }
}
