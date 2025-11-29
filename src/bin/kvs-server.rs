use std::{
    fs,
    io::{BufReader, BufWriter, Write},
    net::TcpListener,
    path::Path,
};

use anyhow::{Error, Result};
use clap::Parser;
use kvs::{
    KvStore,
    protocol::{Request, Response},
};
use serde_json::Deserializer;
use sled::Tree;
#[derive(Parser)]
#[command(author, version)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[arg(short, long, default_value = "kvs")]
    engine: String,
}
trait Engine {
    fn set(&mut self, key: String, value: String) -> Result<()>;
    fn get(&self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

/// 检查数据目录中之前使用的引擎
fn detect_previous_engine(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    let mut has_kvs = false;
    let mut has_sled = false;

    // 遍历目录中的所有条目（文件和目录）
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let name_str = file_name.to_str().unwrap_or("");

        // 检查 kvs 引擎的 .log 文件
        if name_str.ends_with(".log") {
            has_kvs = true;
        }

        // 检查 sled 引擎的特定文件/目录
        // sled 会在目录中创建 "db" 目录或 "_sled" 开头的文件
        if name_str == "db" || name_str.starts_with("_sled") {
            has_sled = true;
        }
    }

    match (has_kvs, has_sled) {
        (true, false) => Ok(Some("kvs".to_string())),
        (false, true) => Ok(Some("sled".to_string())),
        (false, false) => Ok(None), // 新目录，没有之前的引擎
        (true, true) => Err(Error::msg("Both kvs and sled data detected")), // 不应该发生
    }
}

impl Engine for sled::Db {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        Tree::insert(self, key.as_str(), value.as_str())?;
        self.flush()?;
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        match Tree::get(self, key) {
            Ok(Some(v)) => Ok(Some(String::from_utf8(v.to_vec())?)),
            Ok(None) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let result = Tree::remove(self, key)?;
        if result.is_none() {
            Err(Error::msg("Key not found"))
        } else {
            self.flush()?;
            Ok(())
        }
    }
}

impl Engine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.set(key, value)?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.get(key).map_err(|e| e.into())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.remove(key)?;
        Ok(())
    }
}

fn main() -> Result<()> {
    eprintln!("CARGO_PKG_VERSION: {}", env!("CARGO_PKG_VERSION"));
    let args = Args::parse();
    eprintln!(
        "Starting server on {}, and using engine {}",
        args.addr, args.engine
    );

    let data_dir = Path::new("./");

    // 检查之前使用的引擎
    if let Some(previous_engine) = detect_previous_engine(data_dir)? {
        if previous_engine != args.engine {
            return Err(Error::msg(format!(
                "Wrong engine! Previous: {}, current: {}",
                previous_engine, args.engine
            )));
        }
    }

    let mut engine: Box<dyn Engine> = match args.engine.as_str() {
        "kvs" => Box::new(KvStore::open("./")?),
        "sled" => Box::new(sled::open("./")?),
        _ => return Err(Error::msg("Unknown engine")),
    };

    let listener = TcpListener::bind(args.addr)?;
    loop {
        let (stream, _) = listener.accept()?;
        let mut buf_reader = BufReader::new(stream.try_clone()?);
        let mut buf_writer = BufWriter::new(stream.try_clone()?);
        let stream = Deserializer::from_reader(&mut buf_reader).into_iter::<Request>();
        for request in stream {
            let request = request?;
            eprintln!("Received request: {:?}", request);
            match request {
                Request::Set { key, value } => match engine.set(key, value) {
                    Ok(_) => {
                        let response = Response::Ok;
                        serde_json::to_writer(&mut buf_writer, &response)?;
                        eprintln!("Sent response: {:?}", response);
                    }
                    Err(e) => {
                        let response = Response::Err(e.to_string());
                        serde_json::to_writer(&mut buf_writer, &response)?;
                        eprintln!("Error setting key: {:?}", e);
                    }
                },
                Request::Get { key } => match engine.get(key) {
                    Ok(value) => {
                        let response = Response::Value(value);
                        serde_json::to_writer(&mut buf_writer, &response)?;
                        eprintln!("Sent response: {:?}", response);
                    }
                    Err(e) => {
                        let response = Response::Err(e.to_string());
                        serde_json::to_writer(&mut buf_writer, &response)?;
                        eprintln!("Error getting key: {:?}", e);
                    }
                },
                Request::Remove { key } => match engine.remove(key) {
                    Ok(_) => {
                        let response = Response::Ok;
                        serde_json::to_writer(&mut buf_writer, &response)?;
                        eprintln!("Sent response: {:?}", response);
                    }
                    Err(e) => {
                        let response = Response::Err(e.to_string());
                        serde_json::to_writer(&mut buf_writer, &response)?;
                        eprintln!("Error removing key: {:?}", e);
                    }
                },
            }
            buf_writer.flush().unwrap();
        }
    }
}
