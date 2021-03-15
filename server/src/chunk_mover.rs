//! This module contains code for ChunkMover
use crate::{db, ConnectionManagerImpl as ConnectionManager, Db, Server as AppServer};
use mutable_buffer::chunk::ChunkState;
use query::DatabaseStore;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use tracing::debug;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Joining execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },

    #[snafu(display("Cannot advance chunk: {}", source))]
    AdvanceChunk { source: db::Error },

    #[snafu(display("Cannot get table Data. {}", source))]
    TableData { source: db::Error },

    #[snafu(display("Cannot get table for chunk. {}", source))]
    ChunkTables { source: db::Error },

    #[snafu(display("Cannot get table for read buffer chunk. {}", source))]
    ReadBufferChunkTables { source: db::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Default, Debug)]
pub struct ChunkMover {}

impl ChunkMover {
    pub fn new() -> Self {
        Self {}
    }

    /**
    ChunkMover: a background service (or a background task) that is responsible
    for moving eligible chunks from mutable buffer to read buffer. This is a
    repeating process that ensures all chunks are moved successfully
    or move them again if they fail or are stuck for too long for some reasons.
    Chunks that are already moved also need to get dropped.
    The whole process is done through 3 independent sub-services, each will be
    independently correct. To do so, we need to keep track of a state for each
    chunk of the mutable buffer.

    # State's values:
      * `open`: this chunk is still accepting ingesting data and should not be
        moved.
      * `closing`: this chunk is in a closing process which might accept more
        writes or be merged with other chunks or be split into many chunks.
      * `closed`: this chunk is closed and becomes immutable which means it is
        eligible to move to read buffer.
      * `moving`: this chunk is in the moving-process to read buffer. All tables
        of the chunk will be moved in parallel in different tasks.

    # Sub-services
    For each DB, four major sub-services will be spawned, each repeat their
    below duty cycle after some sleep.
      * `move_chunks`: to move "closed" chunks (eligible chunks) of mutable
        buffer to read buffer. Before the process starts, the "closed" chunk
        will be advanced to "moving".
      * `move_moving_chunks`: to move chunks that have been moved & marked
        "moving" but either failed or still running after a while.
      * `drop_successful_moving_chunks`: to drop chunks whose tables have been
        moved into read buffer.
    */
    pub async fn run_chunk_movers(server: Arc<AppServer<ConnectionManager>>) {
        // TODO
        // (1). DBs must be read inside each service below to make sure new DB and
        // dropped DBs are included (2).Instead of calling server.db_names_sorted()
        // there should probably be a server.dbs() method added to the DatabaseStore
        // trait. I imagine it would look something like:      async fn dbs(&self)
        // -> Vec<Arc<Self::Database>>;

        let database_names = server.db_names_sorted();
        for name in database_names {
            //let db = match server.db_or_create(&name).await {
            let db = <AppServer<ConnectionManager> as DatabaseStore>::db(&server, &name).unwrap();

            // Move "closed" chunks
            let service_db = Arc::clone(&db);
            tokio::task::spawn(async move { Self::move_chunks(service_db).await });

            // Move "moving" chunks
            // TODO: need to discuss this further with Edd and see if we can use
            // anything from Raphael's task trackers & cancel a long-running task
            // let service_db = Arc::clone(&db);
            // tokio::task::spawn(async move { service_db.move_moving_chunks().await } );

            // Advance "moving" chunks to "moved"
            let service_db = Arc::clone(&db);
            tokio::task::spawn(
                async move { Self::drop_successful_moving_chunks(service_db).await },
            );
        }
    }

    /// Move "closed" chunks (eligible chunks) of mutable buffer to read buffer.
    /// Before the process starts, the "closed" chunk will be advanced to
    /// "moving".
    pub async fn move_chunks(db: Arc<Db>) -> Result<(), Error> {
        //let service_db = Arc::clone(&db);
        let db_name = "Database"; // TODO: get actual name service_db.rules.name;
        Self::capture_start_service("move_chunks", db_name);

        let service_db = Arc::clone(&db);
        let mut interval = tokio::time::interval(service_db.rules.chunk_mover_duration);
        interval.tick().await;

        // TODO: need to decide whether to handle error inside the loop and continue or
        // exit the loop loop {
        //     ...
        //     fn body()  -> Result<()> {
        //       ///
        //     }
        //     if let Err(e) = body() {
        //        ....
        //   }

        //   https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=1e93a917933d96dfa06983fb135e9df0

        loop {
            // Sleep and wait
            interval.tick().await;

            Self::capture_start_service_cycle("move_chunks", db_name);
            let now = std::time::Instant::now();

            // Collect closed chunks
            let service_db = Arc::clone(&db);
            let partition_chunks = service_db.collect_chunks(ChunkState::Closed);

            // Advance the chunks' state from "closed" to "moving"
            for (partition_key, chunk_id) in &partition_chunks {
                db.advance_chunk_state(partition_key.as_str(), *chunk_id, ChunkState::Moving)
                    .context(AdvanceChunk)?;
            }

            // Get Data for tables of each chunk
            let batches = db
                .table_data_of_chunks(&partition_chunks)
                .context(TableData)?;

            // Now spawn tasks for the each table data batch
            for (partition_key, chunk_id, table_name, data) in batches {
                let read_buff = Arc::clone(&db.read_buffer);
                tokio::task::spawn(async move {
                    read_buff.upsert_partition(
                        partition_key.as_str(),
                        chunk_id,
                        table_name.as_str(),
                        data,
                    )
                })
                .await // TODO: collect handle and wait afterward
                .context(JoinError)?;
            }

            Self::capture_end_service_cycle("move_chunks", db_name, now.elapsed());
        }
    }

    pub fn capture_start_service(service: &str, db_name: &str) {
        debug!("Background {} for Database {} starts", service, db_name);
    }
    pub fn capture_start_service_cycle(service: &str, db_name: &str) {
        debug!(
            "Background {} for Database {} starts a new cycle",
            service, db_name
        );
    }
    pub fn capture_end_service_cycle(service: &str, db_name: &str, elapse: std::time::Duration) {
        debug!(
            "Background {} for Database {} finished checking a cycle in {:?}",
            service, db_name, elapse
        );
    }

    /// Move chunks that have been moved & marked "moving" but either failed or
    /// still running after a while.
    pub async fn move_moving_chunks(&self, db: Arc<Db>) {
        let db_name = "Database";
        Self::capture_start_service("move_moving_chunks", db_name);
        let mut interval = tokio::time::interval(db.rules.chunk_mover_duration);
        interval.tick().await;
        loop {
            // Sleep and wait
            // TODO: let see if we need different sleep time to move "moving" chunks
            interval.tick().await;
            Self::capture_start_service_cycle("move_moving_chunks", db_name);
            let now = std::time::Instant::now();

            // Move the tables of "moving" chunks that have ot been successfully moved by
            // previous cycle Collect moving chunks
            let _partition_chunks = db.collect_chunks(ChunkState::Moving);
            // TODO
            // Get Data for tables of each chunk that have not been in read buffer yet
            // (which means they have not been successfully moved in last cycle)
            // let batches =
            // self.get_remaining_table_data_of_chunks(partition_chunks).unwrap(); // TODO
            // // Now spawn tasks for each table data batch
            // for (partition_key, chunk_id, table_name, data) in batches {  // TODO
            //     // tokio::task::spawn(|...|
            //     //     self.read_buffer
            //     //        .upsert_partition(partition_key, chunk_id, table_name, data);
            //     //     chunk_num_tables(p_key, c_id)--
            // }

            Self::capture_end_service_cycle("move_moving_chunks", db_name, now.elapsed());
        }
    }

    /// Advance "moving" chunks whose all tables are in read buffer to "moved"
    pub async fn drop_successful_moving_chunks(db: Arc<Db>) -> Result<()> {
        let db_name = "Database";
        Self::capture_start_service("drop_successful_moving_chunks", db_name);
        let mut interval = tokio::time::interval(db.rules.chunk_mover_duration);
        interval.tick().await;
        loop {
            // Sleep and wait
            interval.tick().await;
            Self::capture_start_service_cycle("drop_successful_moving_chunks", db_name);
            let now = std::time::Instant::now();

            // Collect all "moving" chunks
            let partition_chunks = db.collect_chunks(ChunkState::Moving);

            // Go over each chunk to see if it is ready to advance to "moved"
            for (partition_key, chunk_id) in partition_chunks {
                // Get all tables of this chunk
                let tables = db
                    .chunk_tables(partition_key.as_str(), chunk_id)
                    .context(ChunkTables)?;

                // Check if all tables of the chunk are in read buffer
                if db.all_tables_in_read_buffer(partition_key.as_str(), chunk_id, &tables) {
                    db.drop_mutable_buffer_chunk(partition_key.as_str(), chunk_id)
                        .await
                        .context(ReadBufferChunkTables)?;
                }
            }

            Self::capture_end_service_cycle(
                "drop_successful_moving_chunks",
                db_name,
                now.elapsed(),
            );
        }
    }
}
