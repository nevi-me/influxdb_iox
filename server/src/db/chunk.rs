use arrow_deps::datafusion::physical_plan::SendableRecordBatchStream;
use data_types::chunk::{ChunkStorage, ChunkSummary};
use internal_types::{schema::Schema, selection::Selection};
use mutable_buffer::chunk::Chunk as MBChunk;
use query::{exec::stringset::StringSet, predicate::Predicate, PartitionChunk};
use read_buffer::Database as ReadBufferDb;
use snafu::{ResultExt, Snafu};
use tracing::debug;

use std::sync::Arc;

use super::{
    pred::{to_mutable_buffer_predicate, to_read_buffer_predicate},
    streams::{MutableBufferChunkStream, ReadFilterResultsStream},
};

use async_trait::async_trait;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },

    #[snafu(display("Read Buffer Error in chunk {}: {}", chunk_id, source))]
    ReadBufferChunk {
        source: read_buffer::Error,
        chunk_id: u32,
    },

    #[snafu(display("Internal error restricting schema: {}", source))]
    InternalSelectingSchema {
        source: internal_types::schema::Error,
    },

    #[snafu(display("Predicate conversion error: {}", source))]
    PredicateConversion { source: super::pred::Error },

    #[snafu(display(
        "Internal error: mutable buffer does not support predicate pushdown, but got: {:?}",
        predicate
    ))]
    InternalPredicateNotSupported { predicate: Predicate },

    #[snafu(display("internal error creating plan: {}", source))]
    InternalPlanCreation {
        source: arrow_deps::datafusion::error::DataFusionError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowConversion {
        source: arrow_deps::arrow::error::ArrowError,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DBChunk {
    MutableBuffer {
        chunk: Arc<MBChunk>,
        partition_key: Arc<String>,
        /// is this chunk open for writing?
        open: bool,
    },
    ReadBuffer {
        db: Arc<ReadBufferDb>,
        partition_key: Arc<String>,
        chunk_id: u32,
    },
    ParquetFile, // TODO add appropriate type here
}

impl DBChunk {
    /// Create a new mutable buffer chunk
    pub fn new_mb(
        chunk: Arc<mutable_buffer::chunk::Chunk>,
        partition_key: impl Into<String>,
        open: bool,
    ) -> Arc<Self> {
        let partition_key = Arc::new(partition_key.into());
        Arc::new(Self::MutableBuffer {
            chunk,
            partition_key,
            open,
        })
    }

    /// create a new read buffer chunk
    pub fn new_rb(
        db: Arc<ReadBufferDb>,
        partition_key: impl Into<String>,
        chunk_id: u32,
    ) -> Arc<Self> {
        let partition_key = Arc::new(partition_key.into());
        Arc::new(Self::ReadBuffer {
            db,
            chunk_id,
            partition_key,
        })
    }

    pub fn summary(&self) -> ChunkSummary {
        match self {
            Self::MutableBuffer {
                chunk,
                partition_key,
                open,
            } => {
                let storage = if *open {
                    ChunkStorage::OpenMutableBuffer
                } else {
                    ChunkStorage::ClosedMutableBuffer
                };
                ChunkSummary {
                    partition_key: Arc::clone(partition_key),
                    id: chunk.id(),
                    storage,
                    estimated_bytes: chunk.size(),
                }
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let estimated_bytes = db
                    .chunks_size(partition_key.as_ref(), &[*chunk_id])
                    .unwrap_or(0) as usize;

                ChunkSummary {
                    partition_key: Arc::clone(&partition_key),
                    id: *chunk_id,
                    storage: ChunkStorage::ReadBuffer,
                    estimated_bytes,
                }
            }
            Self::ParquetFile => {
                unimplemented!("parquet file summary not implemented")
            }
        }
    }
}

#[async_trait]
impl PartitionChunk for DBChunk {
    type Error = Error;

    fn id(&self) -> u32 {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.id(),
            Self::ReadBuffer { chunk_id, .. } => *chunk_id,
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_stats(
        &self,
    ) -> Result<Vec<data_types::partition_metadata::TableSummary>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.table_stats().context(MutableBufferChunk),
            Self::ReadBuffer { .. } => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    async fn table_names(
        &self,
        predicate: &Predicate,
        _known_tables: &StringSet,
    ) -> Result<Option<StringSet>, Self::Error> {
        let names = match self {
            Self::MutableBuffer { chunk, .. } => {
                if chunk.is_empty() {
                    Some(StringSet::new())
                } else {
                    let chunk_predicate = match to_mutable_buffer_predicate(chunk, predicate) {
                        Ok(chunk_predicate) => chunk_predicate,
                        Err(e) => {
                            debug!(?predicate, %e, "mutable buffer predicate not supported for table_names, falling back");
                            return Ok(None);
                        }
                    };

                    // we don't support arbitrary expressions in chunk predicate yet
                    if !chunk_predicate.chunk_exprs.is_empty() {
                        None
                    } else {
                        let names = chunk
                            .table_names(&chunk_predicate)
                            .context(MutableBufferChunk)?
                            .into_iter()
                            .map(|s| s.to_string())
                            .collect::<StringSet>();

                        Some(names)
                    }
                }
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;

                // If not supported, ReadBuffer can't answer with
                // metadata only
                let rb_predicate = match to_read_buffer_predicate(&predicate) {
                    Ok(rb_predicate) => rb_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "read buffer predicate not supported for table_names, falling back");
                        return Ok(None);
                    }
                };

                let names = db
                    .table_names(partition_key, &[chunk_id], rb_predicate)
                    .context(ReadBufferChunk { chunk_id })?;

                Some(names)
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented")
            }
        };

        // Prune out tables that should not be
        // present (based on additional table restrictions of the Predicate)
        //
        // This is needed because at time of writing, the ReadBuffer's
        // table_names implementation doesn't include any way to
        // further restrict the tables to a known set of tables
        let names = names.map(|names| {
            names
                .into_iter()
                .filter(|table_name| predicate.should_include_table(table_name))
                .collect()
        });
        Ok(names)
    }

    async fn table_schema(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<Schema, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk
                .table_schema(table_name, selection)
                .context(MutableBufferChunk),
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;

                // TODO: Andrew -- I think technically this reordering
                // should be happening inside the read buffer, but
                // we'll see when we get to read_filter as the same
                // issue will appear when actually reading columns
                // back
                let needs_sort = matches!(selection, Selection::All);

                // For now, since read_filter is evaluated lazily,
                // "run" a query with no predicates simply to get back the
                // schema
                let predicate = read_buffer::Predicate::default();
                let mut schema = db
                    .read_filter(partition_key, table_name, &[chunk_id], predicate, selection)
                    .context(ReadBufferChunk { chunk_id })?
                    .schema()
                    .context(ReadBufferChunk { chunk_id })?;

                // Ensure the order of the output columns is as
                // specified
                if needs_sort {
                    schema = schema.sort_fields_by_name()
                }

                Ok(schema)
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented for table schema")
            }
        }
    }

    fn has_table(&self, table_name: &str) -> bool {
        match self {
            Self::MutableBuffer { chunk, .. } => chunk.has_table(table_name),
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;
                db.has_table(partition_key, table_name, &[chunk_id])
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented for has_table")
            }
        }
    }

    async fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => {
                // Note MutableBuffer doesn't support predicate
                // pushdown (other than pruning out the entire chunk
                // via `might_pass_predicate)
                if !predicate.is_empty() {
                    return InternalPredicateNotSupported {
                        predicate: predicate.clone(),
                    }
                    .fail();
                }
                let schema: Schema = self.table_schema(table_name, selection).await?;

                Ok(Box::pin(MutableBufferChunkStream::new(
                    Arc::clone(&chunk),
                    schema.as_arrow(),
                    table_name,
                )))
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;
                // Error converting to a rb_predicate needs to fail
                let rb_predicate =
                    to_read_buffer_predicate(&predicate).context(PredicateConversion)?;

                let chunk_ids = &[chunk_id];

                let read_results = db
                    .read_filter(
                        partition_key,
                        table_name,
                        chunk_ids,
                        rb_predicate,
                        selection,
                    )
                    .context(ReadBufferChunk { chunk_id })?;

                let schema = read_results
                    .schema()
                    .context(ReadBufferChunk { chunk_id })?
                    .into();

                Ok(Box::pin(ReadFilterResultsStream::new(read_results, schema)))
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented for scan_data")
            }
        }
    }

    fn could_pass_predicate(&self, _predicate: &Predicate) -> Result<bool> {
        match self {
            Self::MutableBuffer { .. } => {
                // For now, we might get an error if we try and
                // compile a chunk predicate (e.g. for tables that
                // don't exist in the chunk) which could signal the
                // chunk can't pass the predicate.

                // However, we can also get an error if there is some
                // unsupported operation and we need a way to
                // distinguish the two cases.
                Ok(true)
            }
            Self::ReadBuffer { .. } => {
                // TODO: ask Edd how he wants this wired up in the read buffer
                Ok(true)
            }
            Self::ParquetFile => {
                // TODO proper filtering for parquet files
                Ok(true)
            }
        }
    }

    async fn column_names(
        &self,
        table_name: &str,
        predicate: &Predicate,
        columns: Selection<'_>,
    ) -> Result<Option<StringSet>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => {
                let chunk_predicate = match to_mutable_buffer_predicate(chunk, predicate) {
                    Ok(chunk_predicate) => chunk_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "mutable buffer predicate not supported for column_names, falling back");
                        return Ok(None);
                    }
                };

                chunk
                    .column_names(table_name, &chunk_predicate, columns)
                    .context(MutableBufferChunk)
            }
            Self::ReadBuffer {
                db,
                partition_key,
                chunk_id,
            } => {
                let chunk_id = *chunk_id;
                let rb_predicate = match to_read_buffer_predicate(&predicate) {
                    Ok(rb_predicate) => rb_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "read buffer predicate not supported for column_names, falling back");
                        return Ok(None);
                    }
                };

                let chunk_ids = &[chunk_id];

                let names = db
                    .column_names(partition_key, table_name, chunk_ids, rb_predicate, columns)
                    .context(ReadBufferChunk { chunk_id })?;

                Ok(names)
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented for column_names")
            }
        }
    }

    async fn column_values(
        &self,
        table_name: &str,
        column_name: &str,
        predicate: &Predicate,
    ) -> Result<Option<StringSet>, Self::Error> {
        match self {
            Self::MutableBuffer { chunk, .. } => {
                use mutable_buffer::chunk::Error::UnsupportedColumnTypeForListingValues;

                let chunk_predicate = match to_mutable_buffer_predicate(chunk, predicate) {
                    Ok(chunk_predicate) => chunk_predicate,
                    Err(e) => {
                        debug!(?predicate, %e, "mutable buffer predicate not supported for column_values, falling back");
                        return Ok(None);
                    }
                };

                let values = chunk.tag_column_values(table_name, column_name, &chunk_predicate);

                // if the mutable buffer doesn't support getting
                // values for this kind of column, report back None
                if let Err(UnsupportedColumnTypeForListingValues { .. }) = values {
                    Ok(None)
                } else {
                    values.context(MutableBufferChunk)
                }
            }
            Self::ReadBuffer { .. } => {
                // TODO hook up read buffer API here when ready. Until
                // now, fallback to using a full plan
                // https://github.com/influxdata/influxdb_iox/issues/857
                Ok(None)
            }
            Self::ParquetFile => {
                unimplemented!("parquet file not implemented for column_values")
            }
        }
    }
}
