use std::{
    fs,
    io::{BufReader, BufWriter, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Error, Result};
use clap::Parser;
use kvs::{
    KvStore, SledEngine,
    engine::KvsEngine,
    protocol::{Request, Response},
    thread_pool::{NaiveThreadPool, ThreadPool},
};
use serde_json::Deserializer;
#[derive(Parser)]
#[command(author, version)]
struct Args {
    #[arg(short, long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[arg(short, long, default_value = "kvs")]
    engine: String,
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

    match args.engine.as_str() {
        "kvs" => {
            let mut server = KvsServer::new(args.addr, KvStore::open("./")?)?;
            server.run()?;
        }
        "sled" => {
            let mut server = KvsServer::new(args.addr, SledEngine::open("./")?)?;
            server.run()?;
        }
        _ => return Err(Error::msg("Unknown engine")),
    };

    Ok(())
}

/// KVS 服务器
pub struct KvsServer<E: KvsEngine> {
    listener: TcpListener,
    thread_pool: NaiveThreadPool,
    engine: E,
    shutdown: Arc<AtomicBool>,
}

impl<E: KvsEngine> KvsServer<E> {
    /// 创建新的 KVS 服务器
    pub fn new(addr: String, engine: E) -> Result<Self> {
        let cpus = num_cpus::get();
        let thread_pool = NaiveThreadPool::new(cpus as u32)?;
        let listener = TcpListener::bind(addr)?;
        // 设置非阻塞模式以便能够检查关闭标志
        listener.set_nonblocking(true)?;

        Ok(Self {
            listener,
            thread_pool,
            engine,
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    /// 运行服务器
    pub fn run(&mut self) -> Result<()> {
        eprintln!("Server started, waiting for connections...");

        loop {
            // 检查是否收到关闭信号
            if self.shutdown.load(Ordering::Relaxed) {
                eprintln!("Shutdown signal received, stopping server...");
                break;
            }

            // 尝试接受新连接（非阻塞）
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let engine = self.engine.clone();
                    let shutdown = self.shutdown.clone();
                    self.thread_pool.spawn(move || {
                        // 在处理流时也检查关闭标志
                        if !shutdown.load(Ordering::Relaxed) {
                            if let Err(e) = handle_stream(stream, engine) {
                                eprintln!("Error handling stream: {:?}", e);
                            }
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // 没有新连接，继续循环检查关闭标志
                    std::thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(e) => {
                    // 其他错误
                    if !self.shutdown.load(Ordering::Relaxed) {
                        return Err(Error::from(e));
                    }
                    break;
                }
            }
        }

        eprintln!(
            "Server stopped accepting new connections, waiting for active connections to finish..."
        );
        // 线程池会在 Drop 时等待所有任务完成
        Ok(())
    }

    /// 关闭服务器
    pub fn shutdown(&self) {
        eprintln!("Shutting down server...");
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

fn handle_stream(stream: TcpStream, engine: impl KvsEngine) -> Result<()> {
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
    Ok(())
}
