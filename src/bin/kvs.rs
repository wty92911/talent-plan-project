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
fn main() {
    let cli = Cli::parse();
    let mut kvs = KvStore::new();
    match cli.command {
        Commands::Get { key } => {
            eprintln!("unimplemented!");
            unimplemented!()
            // println!("{:?}", kvs.get(key));
        }
        Commands::Set { key, value } => {
            eprintln!("unimplemented!");
            unimplemented!()
            // kvs.set(key, value);
        }
        Commands::Remove { key } => {
            eprintln!("unimplemented!");
            unimplemented!();
            // kvs.remove(key);
        }
    }
}
