use crate::KvsError;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum Record {
    Set(String, String),
    Remove(String),
}

#[derive(Debug)]
pub(crate) struct FileIndex {
    path: PathBuf,
    offset: u64,
}
pub struct LogHelper {}

impl LogHelper {
    pub(crate) fn read(idx: &FileIndex) -> Result<Record> {
        let mut file = File::open(idx.path.clone())?;
        file.seek(SeekFrom::Start(idx.offset))?;
        let mut reader = BufReader::new(file);
        let mut buf = String::new();
        reader.read_line(&mut buf)?;
        LogHelper::deserialize(&buf)
    }

    pub(crate) fn read_all(path: PathBuf) -> Result<Vec<(Record, FileIndex)>> {
        let file = File::open(path.clone())?;
        let mut records = Vec::new();
        let mut reader = BufReader::new(file);
        let mut offset = 0;

        loop {
            let mut buf = Vec::new();
            let n = reader.read_until(b'\n', &mut buf)?;
            if n == 0 {
                break;
            }

            let line_str = String::from_utf8_lossy(&buf);

            records.push((
                LogHelper::deserialize(&line_str)?,
                FileIndex {
                    path: path.clone(),
                    offset,
                },
            ));

            offset += n as u64; // 精准，因为 n 包含 '\n'
        }

        Ok(records)
    }
    pub(crate) fn write(file: &mut File, path: PathBuf, record: &Record) -> Result<FileIndex> {
        let serialized_record = LogHelper::serialize(record)?;
        let offset = file.metadata()?.len();
        file.write(serialized_record.as_bytes())?;
        Ok(FileIndex { path, offset })
    }

    fn serialize(record: &Record) -> Result<String> {
        match record {
            Record::Set(key, value) => Ok(format!("set {key} {value}\n")),
            Record::Remove(key) => Ok(format!("rm {key}\n")),
        }
    }

    fn deserialize(buf: &str) -> Result<Record> {
        let tokens: Vec<&str> = buf.trim().split(' ').collect();
        if tokens.is_empty() {
            Err(KvsError::DeserializeError)
        } else {
            match tokens[0] {
                "set" if tokens.len() == 3 => {
                    Ok(Record::Set(tokens[1].to_string(), tokens[2].to_string()))
                }
                "rm" if tokens.len() == 2 => Ok(Record::Remove(tokens[1].to_string())),
                _ => Err(KvsError::DeserializeError),
            }
        }
    }
}
