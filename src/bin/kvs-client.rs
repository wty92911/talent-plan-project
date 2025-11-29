use std::{
    io::{BufReader, BufWriter, Write},
    net::TcpStream,
};

use clap::{Parser, Subcommand};
use kvs::protocol::{Request, Response};
use serde_json::Deserializer;

#[derive(Parser, Debug)]
#[command(author, version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Parser, Debug)]
struct CommandOpts {
    #[arg(short, long, default_value = "127.0.0.1:4000")]
    addr: String,
}

#[derive(Subcommand, Debug)]

enum Commands {
    Get {
        key: String,
        #[command(flatten)]
        opts: CommandOpts,
    },
    Set {
        key: String,
        value: String,
        #[command(flatten)]
        opts: CommandOpts,
    },
    #[command(name = "rm")]
    Remove {
        key: String,
        #[command(flatten)]
        opts: CommandOpts,
    },
}

/// 发送请求并接收响应
fn send_request_and_get_response(
    request: Request,
    buf_writer: &mut BufWriter<TcpStream>,
    buf_reader: &mut BufReader<TcpStream>,
) -> kvs::Result<Response> {
    serde_json::to_writer(&mut *buf_writer, &request)?;
    buf_writer.flush()?;
    let deserializer = Deserializer::from_reader(buf_reader);
    let response = deserializer.into_iter::<Response>().next().unwrap()?;
    Ok(response)
}

fn main() -> kvs::Result<()> {
    let cli = Cli::parse();

    // 从命令中提取地址
    let addr = match &cli.command {
        Commands::Get { opts, .. } => opts.addr.clone(),
        Commands::Set { opts, .. } => opts.addr.clone(),
        Commands::Remove { opts, .. } => opts.addr.clone(),
    };

    let stream = TcpStream::connect(&addr)?;
    let mut buf_reader = BufReader::new(stream.try_clone()?);
    let mut buf_writer = BufWriter::new(stream);

    // 构建请求
    let request = match cli.command {
        Commands::Get { key, .. } => Request::Get { key },
        Commands::Set { key, value, .. } => Request::Set {
            key: key.clone(),
            value: value.clone(),
        },
        Commands::Remove { key, .. } => Request::Remove { key },
    };

    // 发送请求并获取响应
    let response = send_request_and_get_response(request, &mut buf_writer, &mut buf_reader)?;

    // 处理响应
    match response {
        Response::Value(value) => {
            if let Some(value) = value {
                println!("{value}");
            } else {
                println!("Key not found");
            }
        }
        Response::Ok => {
            // Set 和 Remove 操作成功，无需输出
        }
        Response::Err(e) => {
            return Err(kvs::KvsError::ResponseError(e));
        }
    }
    Ok(())
}
