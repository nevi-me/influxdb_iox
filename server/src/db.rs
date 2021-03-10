//! This module contains the main IOx Database object which has the
//! instances of the mutable buffer, read buffer, and object store

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use arrow_deps::arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use data_types::{data::ReplicatedWrite, database_rules::DatabaseRules, selection::Selection};
use mutable_buffer::{chunk::Chunk, chunk::ChunkState, MutableBufferDb};
use parking_lot::Mutex;
use query::{Database, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};

use crate::buffer::Buffer;

use tracing::{debug, info};

mod chunk;
pub(crate) use chunk::DBChunk;
pub mod pred;
mod streams;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display(
        "Too many RecordBatches in a Mutable Buffer partition {}, chunk {}, table {}",
        partition_key,
        chunk_id,
        table_name
    ))]
    MutableBufferChunkBatch {
        partition_key: String,
        chunk_id: u32,
        table_name: String,
    },

    #[snafu(display(
        "Error while upserting a table batch: partition {}, chunk {}, table {}",
        partition_key,
        chunk_id,
        table_name
    ))]
    ReadBufferChunkTable {
        partition_key: String,
        chunk_id: u32,
        table_name: String,
    },

    #[snafu(display("Joining execution task: {}", source))]
    JoinError { source: tokio::task::JoinError },

    #[snafu(display("Unknown Mutable Buffer Chunk {}", chunk_id))]
    UnknownMutableBufferChunk { chunk_id: u32 },

    #[snafu(display("Cannot write to this database: no mutable buffer configured"))]
    DatatbaseNotWriteable {},

    #[snafu(display("Cannot read to this database: no mutable buffer configured"))]
    DatabaseNotReadable {},

    #[snafu(display("Error dropping data from mutable buffer: {}", source))]
    MutableBufferDrop {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error rolling partition: {}", source))]
    RollingPartition {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error querying mutable buffer: {}", source))]
    MutableBufferRead {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error writing to mutable buffer: {}", source))]
    MutableBufferWrite {
        source: mutable_buffer::database::Error,
    },

    #[snafu(display("Error dropping data from read buffer: {}", source))]
    ReadBufferDrop { source: read_buffer::Error },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

const STARTING_SEQUENCE: u64 = 1;

#[derive(Debug, Serialize, Deserialize)]
/// This is the main IOx Database object. It is the root object of any
/// specific InfluxDB IOx instance
pub struct Db {
    #[serde(flatten)]
    pub rules: DatabaseRules,

    #[serde(skip)]
    /// The (optional) mutable buffer stores incoming writes. If a
    /// database does not have a mutable buffer it can not accept
    /// writes (it is a read replica)
    pub mutable_buffer: Option<MutableBufferDb>,

    #[serde(skip)]
    /// The read buffer holds chunk data in an in-memory optimized
    /// format.
    pub read_buffer: Arc<ReadBufferDb>,

    #[serde(skip)]
    /// The wal buffer holds replicated writes in an append in-memory
    /// buffer. This buffer is used for sending data to subscribers
    /// and to persist segments in object storage for recovery.
    pub wal_buffer: Option<Mutex<Buffer>>,

    #[serde(skip)]
    sequence: AtomicU64,
}

impl Db {
    pub fn new(
        rules: DatabaseRules,
        mutable_buffer: Option<MutableBufferDb>,
        read_buffer: ReadBufferDb,
        wal_buffer: Option<Buffer>,
    ) -> Self {
        let wal_buffer = wal_buffer.map(Mutex::new);
        let read_buffer = Arc::new(read_buffer);
        Self {
            rules,
            mutable_buffer,
            read_buffer,
            wal_buffer,
            sequence: AtomicU64::new(STARTING_SEQUENCE),
        }
    }

    /// Move "closed" chunks (eligible chunks) of mutable buffer to read buffer.
    /// Before the process starts, the "closed" chunk will be advanced to
    /// "moving".
    pub async fn move_chunks(&self) -> Result<()> {
        self.capture_start_service("move_chunks");

        loop {
            // Sleep and wait
            // TODO: let see if we need different sleep time for different sub-service
            tokio::time::sleep(self.rules.chunk_mover_duration).await;
            self.capture_start_service_cycle("move_chunks");
            let now = std::time::Instant::now();

            // Collect closed chunks
            let partition_chunks = self.collect_chunks(ChunkState::Closed).unwrap();

            // Advance the chunks' state from "closed" to "moving"
            for (partition_key, chunk_id) in &partition_chunks {
                self.advance_chunk_state(partition_key.as_str(), *chunk_id)?;
            }

            // Get Data for tables of each chunk
            let batches = self.get_table_data_of_chunks(partition_chunks).unwrap();

            // Now spawn tasks for the each table data batch
            for (partition_key, chunk_id, table_name, data) in batches {
                let read_buff = Arc::clone(&self.read_buffer);
                tokio::task::spawn(async move {
                    read_buff.upsert_partition(
                        partition_key.as_str(),
                        chunk_id,
                        table_name.as_str(),
                        data,
                    )
                })
                .await
                .context(JoinError)?;
            }

            self.capture_end_service_cycle("move_chunks", now.elapsed());
        }
    }

    pub fn capture_start_service(&self, service: &str) {
        debug!(
            "Background {} for Database {} starts",
            service, self.rules.name
        );
    }
    pub fn capture_start_service_cycle(&self, service: &str) {
        debug!(
            "Background {} for Database {} starts a new cycle",
            service, self.rules.name
        );
    }
    pub fn capture_end_service_cycle(&self, service: &str, elapse: std::time::Duration) {
        debug!(
            "Background {} for Database {} finished checking a cycle in {:?}",
            service, self.rules.name, elapse
        );
    }

    // Move chunks that have been moved & marked "moving" but either failed or
    /// still running after a while.
    pub async fn move_moving_chunks(&self) {
        self.capture_start_service("move_moving_chunks");
        loop {
            // Sleep and wait
            // TODO: let see if we need different sleep time to move "moving" chunks
            tokio::time::sleep(self.rules.chunk_mover_duration).await;
            self.capture_start_service_cycle("move_moving_chunks");
            let now = std::time::Instant::now();

            // Move the tables of "moving" chunks that have ot been successfully moved by
            // previous cycle Collect moving chunks
            let _partition_chunks = self.collect_chunks(ChunkState::Moving).unwrap();
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

            self.capture_end_service_cycle("move_moving_chunks", now.elapsed());
        }
    }

    /// Advance "moving" chunks whose all tables are in read buffer to "moved"
    pub async fn advance_successful_moving_chunks(&self) -> Result<()> {
        self.capture_start_service("advance_successful_moving_chunks");
        loop {
            // Sleep and wait
            tokio::time::sleep(self.rules.chunk_mover_duration).await;
            self.capture_start_service_cycle("advance_successful_moving_chunks");
            let now = std::time::Instant::now();

            // Collect all "moving" chunks
            let partition_chunks = self.collect_chunks(ChunkState::Moving).unwrap();

            // Go over each chunk to see if it is ready to advance to "moved"
            for (partition_key, chunk_id) in partition_chunks {
                // Get all tables of this chunk
                let tables = self.get_chunk_tables(partition_key.as_str(), chunk_id);
                // No tables found
                if let Err(e) = tables {
                    return Err(e);
                }

                // Check if all tables of the chunk are in read buffer
                if self.all_tables_in_read_buffer(partition_key.as_str(), chunk_id, tables.unwrap())
                {
                    self.advance_chunk_state(partition_key.as_str(), chunk_id)?;
                }
            }

            self.capture_end_service_cycle("advance_successful_moving_chunks", now.elapsed());
        }
    }

    /// Drops "moved" chunks from Mutable Buffer
    pub async fn drop_chunks(&self) -> Result<()> {
        self.capture_start_service("drop_chunks");
        loop {
            // Sleep and wait
            tokio::time::sleep(self.rules.chunk_mover_duration).await;
            self.capture_start_service_cycle("drop_chunks");
            let now = std::time::Instant::now();

            // Collect all "moved" chunks
            let partition_chunks = self.collect_chunks(ChunkState::Moved).unwrap();
            for (partition_key, chunk_id) in partition_chunks {
                self.drop_mutable_buffer_chunk(partition_key.as_str(), chunk_id)
                    .await?;
            }

            self.capture_end_service_cycle("drop_chunks", now.elapsed());
        }
    }

    /// Return all mutable buffer chunks of a given state
    pub fn collect_chunks(&self, chunk_state: ChunkState) -> Result<Vec<(String, u32)>> {
        // Get all partitions
        let partition_keys = self.partition_keys().unwrap();

        // Return a vector of (partition_key, chunk_id)
        let mut partition_chunks: Vec<(String, u32)> = vec![];
        for partition_key in partition_keys {
            // Get all mutable buffer chunks of this partition_key of the given chunk_state
            let chunks =
                self.mutable_buffer_state_specified_chunks(partition_key.as_str(), chunk_state);
            let mut part_chunks: Vec<(String, u32)> = chunks
                .iter()
                .map(|chunk| (partition_key.clone(), chunk.id()))
                .collect();

            partition_chunks.append(&mut part_chunks);
        }

        Ok(partition_chunks)
    }

    /// Get the mutable buffer chunk of a given partition key and chunk id
    pub fn get_mutable_buffer_chunk(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<Chunk>> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .get_chunk(partition_key, chunk_id)
            .context(UnknownMutableBufferChunk { chunk_id })
    }

    /// Advance the state of a given chunk
    pub fn advance_chunk_state(&self, partition_key: &str, chunk_id: u32) -> Result<()> {
        // Get mutable buffer chunks of the (partition_key, chunk_id)
        let mb_chunk = self.get_mutable_buffer_chunk(partition_key, chunk_id);

        match mb_chunk {
            Ok(chunk) => {
                chunk.advance_state();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Get all table names of a given chunk
    pub fn get_chunk_tables(&self, partition_key: &str, chunk_id: u32) -> Result<Vec<String>> {
        let mb_chunk = self.get_mutable_buffer_chunk(partition_key, chunk_id);

        match mb_chunk {
            Ok(chunk) => {
                let tables = chunk
                    .table_stats()
                    .unwrap()
                    .iter()
                    .map(|stats| stats.name.clone())
                    .collect();

                Ok(tables)
            }
            Err(e) => Err(e),
        }
    }

    /// Check if all given tables are in the given chunk of the read buffer
    pub fn all_tables_in_read_buffer(
        &self,
        partition_key: &str,
        chunk_id: u32,
        table_names: Vec<String>,
    ) -> bool {
        let chunk_ids = [chunk_id];

        for table_name in table_names {
            if !self
                .read_buffer
                .has_table(partition_key, table_name.as_str(), &chunk_ids)
            {
                return false;
            }
        }

        true
    }

    /// Return data of tables of a list of chunks
    pub fn get_table_data_of_chunks(
        &self,
        chunks: Vec<(String, u32)>,
    ) -> Result<Vec<(String, u32, String, RecordBatch)>> {
        let mut chunk_table_data = Vec::new();

        for (partition_key, chunk_id) in chunks {
            let mut chk_table_data = self
                .get_table_data_of_chunk(partition_key, chunk_id)
                .unwrap();
            chunk_table_data.append(&mut chk_table_data);
        }

        Ok(chunk_table_data)
    }

    //TODO: this will be needed when finishing up move_moving_chunks
    // pub fn get_remaining_table_data_of_chunks(partition_chunks) {
    // }

    /// Get Table data of a chunk
    pub fn get_table_data_of_chunk(
        &self,
        partition_key: String,
        chunk_id: u32,
    ) -> Result<Vec<(String, u32, String, RecordBatch)>> {
        let mut chunk_table_data = Vec::new();

        let mb_chunk = self
            .get_mutable_buffer_chunk(partition_key.as_str(), chunk_id)
            .unwrap();

        // Get data for each table of the chunk
        for stats in mb_chunk.table_stats().unwrap() {
            let mut batches = Vec::new();
            mb_chunk
                .table_to_arrow(&mut batches, &stats.name, Selection::All)
                .unwrap();

            // Make sure there is only one batch in the batches
            if batches.len() != 1 {
                panic!("Each table in a partition should have one RecordBatch. Partition {}, chunk {}, table {}", partition_key, chunk_id, stats.name);
                // TODO: not sure how to do this yet
                // Err(MutableBufferChunkBatch(partition_key, chunk_id,
                // stats.name))
            } else {
                let batch = batches.pop().unwrap();
                chunk_table_data.push((partition_key.clone(), chunk_id, stats.name.clone(), batch));
            }
        }

        Ok(chunk_table_data)
    }

    /// Rolls over the active chunk in the database's specified partition
    pub async fn rollover_partition(&self, partition_key: &str) -> Result<Arc<DBChunk>> {
        if let Some(local_store) = self.mutable_buffer.as_ref() {
            local_store
                .rollover_partition(partition_key)
                .context(RollingPartition)
                .map(DBChunk::new_mb)
        } else {
            DatatbaseNotWriteable {}.fail()
        }
    }

    pub fn mutable_buffer_state_specified_chunks(
        &self,
        partition_key: &str,
        chunk_state: ChunkState,
    ) -> Vec<Arc<DBChunk>> {
        let chunks = if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            mutable_buffer
                .state_specified_chunks(partition_key, chunk_state)
                .into_iter()
                .map(DBChunk::new_mb)
                .collect()
        } else {
            vec![]
        };
        chunks
    }

    // TODO (nga): this function is currently used for many purpose so I keep it
    // intact. I will write a different function for getting chunks eligible to
    // move to read buffer. After ChunkMover is done, this function will be
    // either rewritten or removed

    // Return a list of all chunks in the mutable_buffer (that can
    // potentially be migrated into the read buffer or object store: this statement
    // no longer appropriate)
    pub fn mutable_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        let chunks = if let Some(mutable_buffer) = self.mutable_buffer.as_ref() {
            mutable_buffer
                .chunks(partition_key)
                .into_iter()
                .map(DBChunk::new_mb)
                .collect()
        } else {
            vec![]
        };
        chunks
    }

    /// List chunks that are currently in the read buffer
    pub fn read_buffer_chunks(&self, partition_key: &str) -> Vec<Arc<DBChunk>> {
        self.read_buffer
            .chunk_ids(partition_key)
            .into_iter()
            .map(|chunk_id| DBChunk::new_rb(Arc::clone(&self.read_buffer), partition_key, chunk_id))
            .collect()
    }

    /// Drops the specified chunk from the mutable buffer, returning
    /// the dropped chunk.
    pub async fn drop_mutable_buffer_chunk(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .drop_chunk(partition_key, chunk_id)
            .map(DBChunk::new_mb)
            .context(MutableBufferDrop)
    }

    /// Drops the specified chunk from the read buffer, returning
    /// the dropped chunk.
    pub async fn drop_read_buffer_chunk(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        self.read_buffer
            .drop_chunk(partition_key, chunk_id)
            .context(ReadBufferDrop)?;

        Ok(DBChunk::new_rb(
            Arc::clone(&self.read_buffer),
            partition_key,
            chunk_id,
        ))
    }

    // TODO (nga): remove this function
    /// Loads a chunk into the ReadBuffer.
    ///
    /// If the chunk is present in the mutable_buffer then it is
    /// loaded from there. Otherwise, the chunk must be fetched from the
    /// object store (Not yet implemented)
    ///
    /// Also uncontemplated as of yet is ensuring the read buffer does
    /// not exceed a memory limit)
    ///
    /// This (async) function returns when this process is complete,
    /// but the process may take a long time
    ///
    /// Returns a reference to the newly loaded chunk in the read buffer
    pub async fn load_chunk_to_read_buffer(
        &self,
        partition_key: &str,
        chunk_id: u32,
    ) -> Result<Arc<DBChunk>> {
        let mb_chunk = self
            .mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .get_chunk(partition_key, chunk_id)
            .context(UnknownMutableBufferChunk { chunk_id })?;

        let mut batches = Vec::new();
        for stats in mb_chunk.table_stats().unwrap() {
            mb_chunk
                .table_to_arrow(&mut batches, &stats.name, Selection::All)
                .unwrap();
            for batch in batches.drain(..) {
                // As implemented now, taking this write lock will wait
                // until all reads to the read buffer to complete and
                // then will block all reads while the insert is occuring
                self.read_buffer
                    .upsert_partition(partition_key, mb_chunk.id(), &stats.name, batch)
            }
        }

        Ok(DBChunk::new_rb(
            Arc::clone(&self.read_buffer),
            partition_key,
            mb_chunk.id,
        ))
    }

    /// Returns the next write sequence number
    pub fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst)
    }

    /// Drops partitions from the mutable buffer if it is over size
    pub fn check_size_and_drop_partitions(&self) -> Result<()> {
        if let (Some(db), Some(config)) = (&self.mutable_buffer, &self.rules.mutable_buffer_config)
        {
            let mut size = db.size();
            if size > config.buffer_size {
                let mut partitions = db.partitions_sorted_by(&config.partition_drop_order);
                while let Some(p) = partitions.pop() {
                    let p = p.read().expect("mutex poisoned");
                    let partition_size = p.size();
                    size -= partition_size;
                    let key = p.key();
                    db.drop_partition(key);
                    info!(
                        partition_key = key,
                        partition_size, "dropped partition from mutable buffer",
                    );
                    if size < config.buffer_size {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}

impl PartialEq for Db {
    fn eq(&self, other: &Self) -> bool {
        self.rules == other.rules
    }
}
impl Eq for Db {}

#[async_trait]
impl Database for Db {
    type Error = Error;
    type Chunk = DBChunk;

    /// Return a covering set of chunks for a particular partition
    fn chunks(&self, partition_key: &str) -> Vec<Arc<Self::Chunk>> {
        // return a coverting set of chunks. TODO include read buffer
        // chunks and take them preferentially from the read buffer.
        // returns a coverting set of chunks -- aka take chunks from read buffer
        // preferentially
        let mutable_chunk_iter = self.mutable_buffer_chunks(partition_key).into_iter();

        let read_buffer_chunk_iter = self.read_buffer_chunks(partition_key).into_iter();

        let chunks: BTreeMap<_, _> = mutable_chunk_iter
            .chain(read_buffer_chunk_iter)
            .map(|chunk| (chunk.id(), chunk))
            .collect();

        // inserting into the map will have removed any dupes
        chunks.into_iter().map(|(_id, chunk)| chunk).collect()
    }

    // Note that most of the functions below will eventually be removed from
    // this trait. For now, pass them directly on to the local store

    async fn store_replicated_write(&self, write: &ReplicatedWrite) -> Result<(), Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatatbaseNotWriteable)?
            .store_replicated_write(write)
            .await
            .context(MutableBufferWrite)
    }

    fn partition_keys(&self) -> Result<Vec<String>, Self::Error> {
        self.mutable_buffer
            .as_ref()
            .context(DatabaseNotReadable)?
            .partition_keys()
            .context(MutableBufferRead)
    }
}

#[cfg(test)]
mod tests {
    use crate::query_tests::utils::make_db;

    use super::*;

    use arrow_deps::{
        arrow::record_batch::RecordBatch, assert_table_eq, datafusion::physical_plan::collect,
    };
    use data_types::database_rules::{
        MutableBufferConfig, Order, PartitionSort, PartitionSortRules,
    };
    use query::{
        exec::Executor, frontend::sql::SQLQueryPlanner, test::TestLPWriter, PartitionChunk,
    };
    use test_helpers::assert_contains;

    #[tokio::test]
    async fn write_no_mutable_buffer() {
        // Validate that writes are rejected if there is no mutable buffer
        let mutable_buffer = None;
        let db = make_db();
        let db = Db {
            mutable_buffer,
            ..db
        };

        let mut writer = TestLPWriter::default();
        let res = writer.write_lp_string(&db, "cpu bar=1 10").await;
        assert_contains!(
            res.unwrap_err().to_string(),
            "Cannot write to this database: no mutable buffer configured"
        );
    }

    #[tokio::test]
    async fn read_write() {
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();

        let batches = run_query(&db, "select * from cpu").await;

        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn write_with_rollover() {
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        assert_eq!(vec!["1970-01-01T00"], db.partition_keys().unwrap());

        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(mb_chunk.id(), 0);

        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(expected, &batches);

        // add new data
        writer.write_lp_string(&db, "cpu bar=2 20").await.unwrap();
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);

        // And expect that we still get the same thing when data is rolled over again
        let chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(chunk.id(), 1);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);
    }

    #[tokio::test]
    async fn read_from_read_buffer() {
        // Test that data can be loaded into the ReadBuffer
        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        writer.write_lp_string(&db, "cpu bar=2 20").await.unwrap();

        let partition_key = "1970-01-01T00";
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        let rb_chunk = db
            .load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();

        // it should be the same chunk!
        assert_eq!(mb_chunk.id(), rb_chunk.id());

        // we should have chunks in both the mutable buffer and read buffer
        // (Note the currently open chunk is not listed)
        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);

        // data should be readable
        let expected = vec![
            "+-----+------+",
            "| bar | time |",
            "+-----+------+",
            "| 1   | 10   |",
            "| 2   | 20   |",
            "+-----+------+",
        ];
        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);

        // now, drop the mutable buffer chunk and results should still be the same
        db.drop_mutable_buffer_chunk(partition_key, mb_chunk.id())
            .await
            .unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![1]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![0]);

        let batches = run_query(&db, "select * from cpu").await;
        assert_table_eq!(&expected, &batches);

        // drop, the chunk from the read buffer
        db.drop_read_buffer_chunk(partition_key, mb_chunk.id())
            .await
            .unwrap();
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
            vec![] as Vec<u32>
        );

        // Currently this doesn't work (as we need to teach the stores how to
        // purge tables after data bas beend dropped println!("running
        // query after all data dropped!"); let expected = vec![] as
        // Vec<&str>; let batches = run_query(&db, "select * from
        // cpu").await; assert_table_eq!(expected, &batches);
    }

    #[tokio::test]
    async fn chunk_id_listing() {
        // Test that chunk id listing is hooked up
        let db = make_db();
        let partition_key = "1970-01-01T00";
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, "cpu bar=1 10").await.unwrap();
        writer.write_lp_string(&db, "cpu bar=1 20").await.unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0]);
        assert_eq!(
            read_buffer_chunk_ids(&db, partition_key),
            vec![] as Vec<u32>
        );

        let partition_key = "1970-01-01T00";
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        assert_eq!(mb_chunk.id(), 0);

        // add a new chunk in mutable buffer, and move chunk1 (but
        // not chunk 0) to read buffer
        writer.write_lp_string(&db, "cpu bar=1 30").await.unwrap();
        let mb_chunk = db.rollover_partition("1970-01-01T00").await.unwrap();
        db.load_chunk_to_read_buffer(partition_key, mb_chunk.id())
            .await
            .unwrap();

        writer.write_lp_string(&db, "cpu bar=1 40").await.unwrap();

        assert_eq!(mutable_chunk_ids(&db, partition_key), vec![0, 1, 2]);
        assert_eq!(read_buffer_chunk_ids(&db, partition_key), vec![1]);
    }

    #[tokio::test]
    async fn check_size_and_drop_partitions() {
        let mut mbconf = MutableBufferConfig {
            buffer_size: 300,
            ..Default::default()
        };
        let rules = DatabaseRules {
            mutable_buffer_config: Some(mbconf.clone()),
            ..Default::default()
        };

        let mut db = Db::new(
            rules,
            Some(MutableBufferDb::new("foo")),
            read_buffer::Database::new(),
            None, // wal buffer
        );

        let mut writer = TestLPWriter::default();

        writer
            .write_lp_to_partition(&db, "cpu,adsf=jkl,foo=bar val=1 1", "p1")
            .await;
        writer
            .write_lp_to_partition(&db, "cpu,foo=bar val=1 1", "p2")
            .await;
        writer
            .write_lp_to_partition(&db, "cpu,foo=bar val=1 1", "p3")
            .await;

        assert!(db.mutable_buffer.as_ref().unwrap().size() > 300);
        db.check_size_and_drop_partitions().unwrap();
        assert!(db.mutable_buffer.as_ref().unwrap().size() < 300);

        let mut partitions = db
            .mutable_buffer
            .as_ref()
            .unwrap()
            .partition_keys()
            .unwrap();
        partitions.sort();
        assert_eq!(&partitions[0], "p2");
        assert_eq!(&partitions[1], "p3");

        writer
            .write_lp_to_partition(&db, "cpu,foo=bar val=1 1", "p4")
            .await;
        mbconf.buffer_size = db.mutable_buffer.as_ref().unwrap().size();
        mbconf.partition_drop_order = PartitionSortRules {
            order: Order::Desc,
            sort: PartitionSort::LastWriteTime,
        };
        db.rules.mutable_buffer_config = Some(mbconf);
    }

    // run a sql query against the database, returning the results as record batches
    async fn run_query(db: &Db, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();

        let physical_plan = planner.query(db, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }

    fn mutable_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .mutable_buffer_chunks(partition_key)
            .iter()
            .map(|chunk| chunk.id())
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }

    fn read_buffer_chunk_ids(db: &Db, partition_key: &str) -> Vec<u32> {
        let mut chunk_ids: Vec<u32> = db
            .read_buffer_chunks(partition_key)
            .iter()
            .map(|chunk| chunk.id())
            .collect();
        chunk_ids.sort_unstable();
        chunk_ids
    }
}
