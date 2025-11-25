use std::env::temp_dir;

use clap::{Parser, Subcommand};
use kvs::KvStore;

#[derive(Parser, Debug)]
#[command(author, version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]

enum Commands {
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    #[command(name = "rm")]
    Remove {
        key: String,
    },
}
fn main() -> kvs::Result<()> {
    let cli = Cli::parse();
    let mut kvs = KvStore::open("./")?;
    match cli.command {
        Commands::Get { key } => {
            if let Some(value) = kvs.get(key)? {
                println!("{value}");
            } else {
                println!("Key not found");
            }
        }
        Commands::Set { key, value } => {
            kvs.set(key, value)?;
        }
        Commands::Remove { key } => {
            let result = kvs.remove(key);
            if result.is_err() {
                println!("Key not found");
                return result;
            }
        }
    }
    Ok(())
}
