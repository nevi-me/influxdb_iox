//! This module implements the `partition` CLI command
use influxdb_iox_client::{
    connection::Builder,
    management::{
        self,
        //ListPartitionsError
    },
};
//use std::convert::TryFrom;
use structopt::StructOpt;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    // #[error("Error listing partitions: {0}")]
    // ListPartitionError(#[from] ListPartitionsError),

    // #[error("Error interpreting server response: {0}")]
    // ConvertingResponse(#[from] data_types::partition::Error),

    // #[error("Error rendering response as JSON: {0}")]
    // WritingJson(#[from] serde_json::Error),
    #[error("Error connecting to IOx: {0}")]
    ConnectionError(#[from] influxdb_iox_client::connection::Error),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Manage IOx partitions
#[derive(Debug, StructOpt)]
pub struct Config {
    #[structopt(subcommand)]
    command: Command,
}

/// List all known partition keys for a database
#[derive(Debug, StructOpt)]
struct List {
    /// The name of the database
    db_name: String,
}

/// Get details of a specific partition in JSON format (TODO)
#[derive(Debug, StructOpt)]
struct Get {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,
}

/// Loads the specified chunk into the read buffer
#[derive(Debug, StructOpt)]
struct LoadRbChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The chunk id
    chunk_id: String,
}

/// Drop the specified chunk from the mutable buffer
#[derive(Debug, StructOpt)]
struct DropMbChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The chunk id
    chunk_id: String,
}

/// Drop the specified chunk from the read buffer
#[derive(Debug, StructOpt)]
struct DropRbChunk {
    /// The name of the database
    db_name: String,

    /// The partition key
    partition_key: String,

    /// The chunk id
    chunk_id: String,
}

/// All possible subcommands for partition
#[derive(Debug, StructOpt)]
enum Command {
    // List partitions
    List(List),
    // Get details about a particular partition
    Get(Get),
    // Loads a chunk into the read buffer
    LoadRbChunk(LoadRbChunk),
    // Drop a chunk from the mutable buffer
    DropMbChunk(DropMbChunk),
    // Drop a chunk from the read buffer
    DropRbChunk(DropRbChunk),
}

pub async fn command(url: String, config: Config) -> Result<()> {
    let connection = Builder::default().build(url).await?;

    match config.command {
        Command::Get(get) => {
            let Get {
                db_name,
                partition_key,
            } = get;
            println!(
                "getting detail for database {} partition {}",
                db_name, partition_key
            );

            // let mut client = management::Client::new(connection);

            // let partitions = client
            //     .list_partitions(db_name)
            //     .await
            //     .map_err(Error::ListPartitionError)?;

            // let partitions = partitions
            //     .into_iter()
            //     .map(|c|
            // PartitionSummary::try_from(c).map_err(Error::ConvertingResponse))
            //     .collect::<Result<Vec<_>>>()?;

            // serde_json::to_writer_pretty(std::io::stdout(),
            // &partitions).map_err(Error::WritingJson)?;
        }
        Command::List(list) => {
            let List { db_name } = list;
            println!("Listing partitions for database {}", db_name);
        }
        Command::LoadRbChunk(load_rb_chunk) => {
            let LoadRbChunk {
                db_name,
                partition_key,
                chunk_id,
            } = load_rb_chunk;
            println!(
                "Loading Rb chunk for database {} partition {} chunk {}",
                db_name, partition_key, chunk_id
            );
        }
        Command::DropMbChunk(drop_mb_chunk) => {
            let DropMbChunk {
                db_name,
                partition_key,
                chunk_id,
            } = drop_mb_chunk;
            println!(
                "Dropping mb chunk for database {} partition {} chunk {}",
                db_name, partition_key, chunk_id
            );
        }
        Command::DropRbChunk(drop_rb_chunk) => {
            let DropRbChunk {
                db_name,
                partition_key,
                chunk_id,
            } = drop_rb_chunk;
            println!(
                "Dropping rb chunk for database {} partition {} chunk {}",
                db_name, partition_key, chunk_id
            );
        }
    }

    Ok(())
}
