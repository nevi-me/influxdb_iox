//! This module contains the HTTP api for InfluxDB IOx, including a
//! partial implementation of the /v2 HTTP api routes from InfluxDB
//! for compatibility.
//!
//! Note that these routes are designed to be just helpers for now,
//! and "close enough" to the real /v2 api to be able to test InfluxDB IOx
//! without needing to create and manage a mapping layer from name -->
//! id (this is done by other services in the influx cloud)
//!
//! Long term, we expect to create IOx specific api in terms of
//! database names and may remove this quasi /v2 API.

// Influx crates
use arrow_deps::datafusion::physical_plan::collect;
use data_types::{
    http::WalMetadataQuery,
    names::{org_and_bucket_to_database, OrgBucketMappingError},
    DatabaseName,
};
use influxdb_iox_client::format::QueryOutputFormat;
use influxdb_line_protocol::parse_lines;
use object_store::ObjectStoreApi;
use query::{frontend::sql::SQLQueryPlanner, Database, DatabaseStore};
use server::{ConnectionManager, Server as AppServer};

// External crates
use bytes::{Bytes, BytesMut};
use futures::{self, StreamExt};
use http::header::{CONTENT_ENCODING, CONTENT_TYPE};
use hyper::{Body, Method, Request, Response, StatusCode};
use routerify::{prelude::*, Middleware, RequestInfo, Router, RouterError, RouterService};
use serde::Deserialize;
use snafu::{OptionExt, ResultExt, Snafu};
use tracing::{debug, error, info};

use data_types::http::WalMetadataResponse;
use hyper::server::conn::AddrIncoming;
use std::{
    fmt::Debug,
    str::{self, FromStr},
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

/// Constants used in API error codes.
///
/// Expressing this as a enum prevents reuse of discriminants, and as they're
/// effectively consts this uses UPPER_SNAKE_CASE.
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
pub enum ApiErrorCode {
    /// An unknown/unhandled error
    UNKNOWN = 100,

    /// The database name in the request is invalid.
    DB_INVALID_NAME = 101,

    /// The database referenced already exists.
    DB_ALREADY_EXISTS = 102,

    /// The database referenced does not exist.
    DB_NOT_FOUND = 103,
}

impl From<ApiErrorCode> for u32 {
    fn from(v: ApiErrorCode) -> Self {
        v as Self
    }
}

#[derive(Debug, Snafu)]
pub enum ApplicationError {
    // Internal (unexpected) errors
    #[snafu(display(
        "Internal error accessing org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    BucketByName {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Internal error mapping org & bucket: {}", source))]
    BucketMappingError { source: OrgBucketMappingError },

    #[snafu(display(
        "Internal error writing points into org {}, bucket {}:  {}",
        org,
        bucket_name,
        source
    ))]
    WritingPoints {
        org: String,
        bucket_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error planning query {}: {}", query, source))]
    PlanningSQLQuery {
        query: String,
        source: query::frontend::sql::Error,
    },

    #[snafu(display("Internal error reading points from database {}:  {}", db_name, source))]
    Query {
        db_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // Application level errors
    #[snafu(display("Bucket {} not found in org {}", bucket, org))]
    BucketNotFound { org: String, bucket: String },

    #[snafu(display("Body exceeds limit of {} bytes", max_body_size))]
    RequestSizeExceeded { max_body_size: usize },

    #[snafu(display("Expected query string in request, but none was provided"))]
    ExpectedQueryString {},

    /// Error for when we could not parse the http query uri (e.g.
    /// `?foo=bar&bar=baz)`
    #[snafu(display("Invalid query string in HTTP URI '{}': {}", query_string, source))]
    InvalidQueryString {
        query_string: String,
        source: serde_urlencoded::de::Error,
    },

    #[snafu(display("Query error: {}", source))]
    QueryError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Invalid request body: {}", source))]
    InvalidRequestBody { source: serde_json::error::Error },

    #[snafu(display("Invalid response body: {}", source))]
    InternalSerializationError { source: serde_json::error::Error },

    #[snafu(display("Invalid content encoding: {}", content_encoding))]
    InvalidContentEncoding { content_encoding: String },

    #[snafu(display("Error reading request header '{}' as Utf8: {}", header_name, source))]
    ReadingHeaderAsUtf8 {
        header_name: String,
        source: hyper::header::ToStrError,
    },

    #[snafu(display("Error reading request body: {}", source))]
    ReadingBody { source: hyper::Error },

    #[snafu(display("Error reading request body as utf8: {}", source))]
    ReadingBodyAsUtf8 { source: std::str::Utf8Error },

    #[snafu(display("Error parsing line protocol: {}", source))]
    ParsingLineProtocol {
        source: influxdb_line_protocol::Error,
    },

    #[snafu(display("Error decompressing body as gzip: {}", source))]
    ReadingBodyAsGzip { source: std::io::Error },

    #[snafu(display("No handler for {:?} {}", method, path))]
    RouteNotFound { method: Method, path: String },

    #[snafu(display("Internal error from database {}: {}", database, source))]
    DatabaseError {
        database: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error generating json response: {}", source))]
    JsonGenerationError { source: serde_json::Error },

    #[snafu(display("Error creating database: {}", source))]
    ErrorCreatingDatabase { source: server::Error },

    #[snafu(display("Invalid database name: {}", source))]
    DatabaseNameError {
        source: data_types::DatabaseNameError,
    },

    #[snafu(display("Database {} not found", name))]
    DatabaseNotFound { name: String },

    #[snafu(display("Database {} does not have a WAL", name))]
    WALNotFound { name: String },

    #[snafu(display("Internal error creating HTTP response:  {}", source))]
    CreatingResponse { source: http::Error },

    #[snafu(display("Invalid format '{}': : {}", format, source))]
    ParsingFormat {
        format: String,
        source: influxdb_iox_client::format::Error,
    },

    #[snafu(display(
        "Error formatting results of SQL query '{}' using '{:?}': {}",
        q,
        format,
        source
    ))]
    FormattingResult {
        q: String,
        format: QueryOutputFormat,
        source: influxdb_iox_client::format::Error,
    },
}

impl ApplicationError {
    pub fn response(&self) -> Response<Body> {
        match self {
            Self::BucketByName { .. } => self.internal_error(),
            Self::BucketMappingError { .. } => self.internal_error(),
            Self::WritingPoints { .. } => self.internal_error(),
            Self::PlanningSQLQuery { .. } => self.bad_request(),
            Self::Query { .. } => self.internal_error(),
            Self::QueryError { .. } => self.bad_request(),
            Self::BucketNotFound { .. } => self.not_found(),
            Self::RequestSizeExceeded { .. } => self.bad_request(),
            Self::ExpectedQueryString { .. } => self.bad_request(),
            Self::InvalidQueryString { .. } => self.bad_request(),
            Self::InvalidRequestBody { .. } => self.bad_request(),
            Self::InternalSerializationError { .. } => self.internal_error(),
            Self::InvalidContentEncoding { .. } => self.bad_request(),
            Self::ReadingHeaderAsUtf8 { .. } => self.bad_request(),
            Self::ReadingBody { .. } => self.bad_request(),
            Self::ReadingBodyAsUtf8 { .. } => self.bad_request(),
            Self::ParsingLineProtocol { .. } => self.bad_request(),
            Self::ReadingBodyAsGzip { .. } => self.bad_request(),
            Self::RouteNotFound { .. } => self.not_found(),
            Self::DatabaseError { .. } => self.internal_error(),
            Self::JsonGenerationError { .. } => self.internal_error(),
            Self::ErrorCreatingDatabase { .. } => self.bad_request(),
            Self::DatabaseNameError { .. } => self.bad_request(),
            Self::DatabaseNotFound { .. } => self.not_found(),
            Self::WALNotFound { .. } => self.not_found(),
            Self::CreatingResponse { .. } => self.internal_error(),
            Self::FormattingResult { .. } => self.internal_error(),
            Self::ParsingFormat { .. } => self.bad_request(),
        }
    }

    fn bad_request(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(self.body())
            .unwrap()
    }

    fn internal_error(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(self.body())
            .unwrap()
    }

    fn not_found(&self) -> Response<Body> {
        Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap()
    }

    fn body(&self) -> Body {
        let json =
            serde_json::json!({"error": self.to_string(), "error_code": self.api_error_code()})
                .to_string();
        Body::from(json)
    }

    /// Map the error type into an API error code.
    fn api_error_code(&self) -> u32 {
        match self {
            Self::DatabaseNameError { .. } => ApiErrorCode::DB_INVALID_NAME,
            Self::DatabaseNotFound { .. } => ApiErrorCode::DB_NOT_FOUND,

            // Some errors are wrapped
            Self::ErrorCreatingDatabase {
                source: server::Error::InvalidDatabaseName { .. },
            } => ApiErrorCode::DB_INVALID_NAME,

            Self::ErrorCreatingDatabase {
                source: server::Error::DatabaseNotFound { .. },
            } => ApiErrorCode::DB_NOT_FOUND,

            Self::ErrorCreatingDatabase {
                source: server::Error::DatabaseAlreadyExists { .. },
            } => ApiErrorCode::DB_ALREADY_EXISTS,

            // A "catch all" error code
            _ => ApiErrorCode::UNKNOWN,
        }
        .into()
    }
}

const MAX_SIZE: usize = 10_485_760; // max write request size of 10MB

fn router<M>(server: Arc<AppServer<M>>) -> Router<Body, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    // Create a router and specify the the handlers.
    Router::builder()
        .data(server)
        .middleware(Middleware::pre(|req| async move {
            info!(request = ?req, "Processing request");
            Ok(req)
        }))
        .middleware(Middleware::post(|res| async move {
            info!(response = ?res, "Successfully processed request");
            Ok(res)
        })) // this endpoint is for API backward compatibility with InfluxDB 2.x
        .post("/api/v2/write", write::<M>)
        .get("/health", health)
        .get("/iox/api/v1/databases/:name/query", query::<M>)
        .get("/iox/api/v1/databases/:name/wal/meta", get_wal_meta::<M>)
        .get("/api/v1/partitions", list_partitions::<M>)
        .post("/api/v1/snapshot", snapshot_partition::<M>)
        // Specify the error handler to handle any errors caused by
        // a route or any middleware.
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}

// The API-global error handler, handles ApplicationErrors originating from
// individual routes and middlewares, along with errors from the router itself
async fn error_handler(err: RouterError<ApplicationError>, req: RequestInfo) -> Response<Body> {
    match err {
        RouterError::HandleRequest(e, _)
        | RouterError::HandlePreMiddlewareRequest(e)
        | RouterError::HandlePostMiddlewareWithInfoRequest(e)
        | RouterError::HandlePostMiddlewareWithoutInfoRequest(e) => {
            error!(error = ?e, error_message = ?e.to_string(), "Error while handling request");
            e.response()
        }
        _ => {
            let method = req.method().clone();
            let uri = req.uri().clone();
            error!(error = ?err, error_message = ?err.to_string(), method = ?method, uri = ?uri, "Error while handling request");

            let json = serde_json::json!({"error": err.to_string()}).to_string();
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(json))
                .unwrap()
        }
    }
}

#[derive(Debug, Deserialize)]
/// Body of the request to the /write endpoint
struct WriteInfo {
    org: String,
    bucket: String,
}

/// Parse the request's body into raw bytes, applying size limits and
/// content encoding as needed.
async fn parse_body(req: hyper::Request<Body>) -> Result<Bytes, ApplicationError> {
    // clippy says the const needs to be assigned to a local variable:
    // error: a `const` item with interior mutability should not be borrowed
    let header_name = CONTENT_ENCODING;
    let ungzip = match req.headers().get(&header_name) {
        None => false,
        Some(content_encoding) => {
            let content_encoding = content_encoding.to_str().context(ReadingHeaderAsUtf8 {
                header_name: header_name.as_str(),
            })?;
            match content_encoding {
                "gzip" => true,
                _ => InvalidContentEncoding { content_encoding }.fail()?,
            }
        }
    };

    let mut payload = req.into_body();

    let mut body = BytesMut::new();
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.expect("Should have been able to read the next chunk");
        // limit max size of in-memory payload
        if (body.len() + chunk.len()) > MAX_SIZE {
            return Err(ApplicationError::RequestSizeExceeded {
                max_body_size: MAX_SIZE,
            });
        }
        body.extend_from_slice(&chunk);
    }
    let body = body.freeze();

    // apply any content encoding needed
    if ungzip {
        use std::io::Read;
        let decoder = flate2::read::GzDecoder::new(&body[..]);

        // Read at most MAX_SIZE bytes to prevent a decompression bomb based
        // DoS.
        let mut decoder = decoder.take(MAX_SIZE as u64);
        let mut decoded_data = Vec::new();
        decoder
            .read_to_end(&mut decoded_data)
            .context(ReadingBodyAsGzip)?;
        Ok(decoded_data.into())
    } else {
        Ok(body)
    }
}

#[tracing::instrument(level = "debug")]
async fn write<M>(req: Request<Body>) -> Result<Response<Body>, ApplicationError>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let server = Arc::clone(&req.data::<Arc<AppServer<M>>>().expect("server state"));

    let query = req.uri().query().context(ExpectedQueryString)?;

    let write_info: WriteInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: String::from(query),
    })?;

    let db_name = org_and_bucket_to_database(&write_info.org, &write_info.bucket)
        .context(BucketMappingError)?;

    let body = parse_body(req).await?;

    let body = str::from_utf8(&body).context(ReadingBodyAsUtf8)?;

    let lines = parse_lines(body)
        .collect::<Result<Vec<_>, influxdb_line_protocol::Error>>()
        .context(ParsingLineProtocol)?;

    debug!(
        "Inserting {} lines into database {} (org {} bucket {})",
        lines.len(),
        db_name,
        write_info.org,
        write_info.bucket
    );

    server
        .write_lines(&db_name, &lines)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(WritingPoints {
            org: write_info.org.clone(),
            bucket_name: write_info.bucket.clone(),
        })?;

    Ok(Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap())
}

#[derive(Deserialize, Debug, PartialEq)]
/// Parsed URI Parameters of the request to the .../query endpoint
struct QueryParams {
    q: String,
    #[serde(default = "default_format")]
    format: String,
}

fn default_format() -> String {
    QueryOutputFormat::default().to_string()
}

#[tracing::instrument(level = "debug")]
async fn query<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = Arc::clone(&req.data::<Arc<AppServer<M>>>().expect("server state"));

    let uri_query = req.uri().query().context(ExpectedQueryString {})?;

    let QueryParams { q, format } =
        serde_urlencoded::from_str(uri_query).context(InvalidQueryString {
            query_string: uri_query,
        })?;

    let format = QueryOutputFormat::from_str(&format).context(ParsingFormat { format })?;

    let db_name_str = req
        .param("name")
        .expect("db name must have been set by routerify")
        .clone();

    let db_name = DatabaseName::new(&db_name_str).context(DatabaseNameError)?;
    debug!(uri = ?req.uri(), %q, ?format, %db_name, "running SQL query");

    let db = server
        .db(&db_name)
        .context(DatabaseNotFound { name: &db_name_str })?;

    let planner = SQLQueryPlanner::default();
    let executor = server.executor();

    let physical_plan = planner
        .query(db.as_ref(), &q, executor.as_ref())
        .await
        .context(PlanningSQLQuery { query: &q })?;

    // TODO: stream read results out rather than rendering the
    // whole thing in mem
    let batches = collect(physical_plan)
        .await
        .map_err(|e| Box::new(e) as _)
        .context(Query { db_name })?;

    let results = format
        .format(&batches)
        .context(FormattingResult { q, format })?;

    let body = Body::from(results.into_bytes());

    let response = Response::builder()
        .header(CONTENT_TYPE, format.content_type())
        .body(body)
        .context(CreatingResponse)?;

    Ok(response)
}

#[tracing::instrument(level = "debug")]
async fn get_wal_meta<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = Arc::clone(&req.data::<Arc<AppServer<M>>>().expect("server state"));

    let db_name_str = req
        .param("name")
        .expect("db name must have been set")
        .clone();

    let query: WalMetadataQuery = req
        .uri()
        .query()
        .map(|query| {
            serde_urlencoded::from_str(query).context(InvalidQueryString {
                query_string: query,
            })
        })
        .transpose()?
        .unwrap_or_default();

    let db_name = DatabaseName::new(&db_name_str).context(DatabaseNameError)?;

    let db = server
        .db(&db_name)
        .context(DatabaseNotFound { name: &db_name_str })?;

    let wal = db
        .wal_buffer
        .as_ref()
        .context(WALNotFound { name: &db_name_str })?;
    let wal_buffer = wal.lock();

    let segments = wal_buffer
        .segments(query.offset)
        .take(query.limit.unwrap_or(10))
        .take_while(|x| {
            query
                .newer_than
                .map(|newer_than| x.created_at > newer_than)
                .unwrap_or(true)
        })
        .collect();

    let response = Response::builder()
        .status(StatusCode::OK)
        .body(Body::from(
            serde_json::to_string(&WalMetadataResponse { segments })
                .expect("json encoding should not fail"),
        ))
        .expect("builder should be successful");

    Ok(response)
}

#[tracing::instrument(level = "debug")]
async fn health(_: Request<Body>) -> Result<Response<Body>, ApplicationError> {
    let response_body = "OK";
    Ok(Response::new(Body::from(response_body.to_string())))
}

#[derive(Deserialize, Debug)]
/// Arguments in the query string of the request to /partitions
struct DatabaseInfo {
    org: String,
    bucket: String,
}

#[tracing::instrument(level = "debug")]
async fn list_partitions<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    let server = Arc::clone(&req.data::<Arc<AppServer<M>>>().expect("server state"));
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let info: DatabaseInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db_name =
        org_and_bucket_to_database(&info.org, &info.bucket).context(BucketMappingError)?;

    let db = server.db(&db_name).context(BucketNotFound {
        org: &info.org,
        bucket: &info.bucket,
    })?;

    let partition_keys =
        db.partition_keys()
            .map_err(|e| Box::new(e) as _)
            .context(BucketByName {
                org: &info.org,
                bucket_name: &info.bucket,
            })?;

    let result = serde_json::to_string(&partition_keys).context(JsonGenerationError)?;

    Ok(Response::new(Body::from(result)))
}

#[derive(Deserialize, Debug)]
/// Arguments in the query string of the request to /snapshot
struct SnapshotInfo {
    org: String,
    bucket: String,
    partition: String,
}

#[tracing::instrument(level = "debug")]
async fn snapshot_partition<M: ConnectionManager + Send + Sync + Debug + 'static>(
    req: Request<Body>,
) -> Result<Response<Body>, ApplicationError> {
    use object_store::path::ObjectStorePath;

    let server = Arc::clone(&req.data::<Arc<AppServer<M>>>().expect("server state"));
    let query = req.uri().query().context(ExpectedQueryString {})?;

    let snapshot: SnapshotInfo = serde_urlencoded::from_str(query).context(InvalidQueryString {
        query_string: query,
    })?;

    let db_name =
        org_and_bucket_to_database(&snapshot.org, &snapshot.bucket).context(BucketMappingError)?;

    // TODO: refactor the rest of this out of the http route and into the server
    // crate.
    let db = server.db(&db_name).context(BucketNotFound {
        org: &snapshot.org,
        bucket: &snapshot.bucket,
    })?;

    let store = Arc::clone(&server.store);

    let mut metadata_path = store.new_path();
    metadata_path.push_dir(&db_name.to_string());
    let mut data_path = metadata_path.clone();
    metadata_path.push_dir("meta");
    data_path.push_all_dirs(&["data", &snapshot.partition]);

    let partition_key = &snapshot.partition;
    let chunk = db.rollover_partition(partition_key).await.unwrap();
    let snapshot = server::snapshot::snapshot_chunk(
        metadata_path,
        data_path,
        store,
        partition_key,
        chunk,
        None,
    )
    .unwrap();

    let ret = format!("{}", snapshot.id);
    Ok(Response::new(Body::from(ret)))
}

pub async fn serve<M>(
    addr: AddrIncoming,
    server: Arc<AppServer<M>>,
    shutdown: CancellationToken,
) -> Result<(), hyper::Error>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    let router = router(server);
    let service = RouterService::new(router).unwrap();

    hyper::Server::builder(addr)
        .serve(service)
        .with_graceful_shutdown(shutdown.cancelled())
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use arrow_deps::{arrow::record_batch::RecordBatch, assert_table_eq};
    use query::exec::Executor;
    use reqwest::{Client, Response};

    use data_types::{
        database_rules::{DatabaseRules, WalBufferConfig, WalBufferRollover},
        wal::WriterSummary,
        DatabaseName,
    };
    use object_store::{memory::InMemory, ObjectStore};
    use serde::de::DeserializeOwned;
    use server::{db::Db, ConnectionManagerImpl};

    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = Error> = std::result::Result<T, E>;

    #[tokio::test]
    async fn test_health() -> Result<()> {
        let test_storage = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        let server_url = test_server(Arc::clone(&test_storage));

        let client = Client::new();
        let response = client.get(&format!("{}/health", server_url)).send().await;

        // Print the response so if the test fails, we have a log of what went wrong
        check_response("health", response, StatusCode::OK, "OK").await;
        Ok(())
    }

    #[tokio::test]
    async fn test_write() -> Result<()> {
        let test_storage = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        test_storage.set_id(1);
        test_storage
            .create_database("MyOrg_MyBucket", DatabaseRules::new())
            .await
            .unwrap();
        let server_url = test_server(Arc::clone(&test_storage));

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, "").await;

        // Check that the data got into the right bucket
        let test_db = test_storage
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .expect("Database exists");

        let batches = run_query(test_db.as_ref(), "select * from h2o_temperature").await;
        let expected = vec![
            "+----------------+--------------+-------+-----------------+------------+",
            "| bottom_degrees | location     | state | surface_degrees | time       |",
            "+----------------+--------------+-------+-----------------+------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 1568756160 |",
            "+----------------+--------------+-------+-----------------+------------+",
        ];
        assert_table_eq!(expected, &batches);

        Ok(())
    }

    /// Sets up a test database with some data for testing the query endpoint
    /// returns a client for communicting with the server, and the server
    /// endpoint
    async fn setup_test_data() -> (Client, String) {
        let test_storage: Arc<AppServer<ConnectionManagerImpl>> = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        test_storage.set_id(1);
        test_storage
            .create_database("MyOrg_MyBucket", DatabaseRules::new())
            .await
            .unwrap();
        let server_url = test_server(Arc::clone(&test_storage));

        let client = Client::new();

        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, "").await;
        (client, server_url)
    }

    #[tokio::test]
    async fn test_query_pretty() -> Result<()> {
        let (client, server_url) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "text/plain");

        let res = "+----------------+--------------+-------+-----------------+------------+\n\
                   | bottom_degrees | location     | state | surface_degrees | time       |\n\
                   +----------------+--------------+-------+-----------------+------------+\n\
                   | 50.4           | santa_monica | CA    | 65.2            | 1568756160 |\n\
                   +----------------+--------------+-------+-----------------+------------+\n";
        check_response("query", response, StatusCode::OK, res).await;

        // same response is expected if we explicitly request 'format=pretty'
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}&format=pretty",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;
        assert_eq!(get_content_type(&response), "text/plain");

        check_response("query", response, StatusCode::OK, res).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_query_csv() -> Result<()> {
        let (client, server_url) = setup_test_data().await;

        // send query data
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}&format=csv",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "text/csv");

        let res = "bottom_degrees,location,state,surface_degrees,time\n\
                   50.4,santa_monica,CA,65.2,1568756160\n";
        check_response("query", response, StatusCode::OK, res).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_query_json() -> Result<()> {
        let (client, server_url) = setup_test_data().await;

        // send a second line of data to demontrate how that works
        let lp_data = "h2o_temperature,location=Boston,state=MA surface_degrees=50.2 1568756160";

        // send write data
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .body(lp_data)
            .send()
            .await;

        check_response("write", response, StatusCode::NO_CONTENT, "").await;

        // send query data
        let response = client
            .get(&format!(
                "{}/iox/api/v1/databases/MyOrg_MyBucket/query?q={}&format=json",
                server_url, "select%20*%20from%20h2o_temperature"
            ))
            .send()
            .await;

        assert_eq!(get_content_type(&response), "application/json");

        // Note two json records: one record on each line
        let res = r#"[{"bottom_degrees":50.4,"location":"santa_monica","state":"CA","surface_degrees":65.2,"time":1568756160},{"location":"Boston","state":"MA","surface_degrees":50.2,"time":1568756160}]"#;
        check_response("query", response, StatusCode::OK, res).await;

        Ok(())
    }

    fn gzip_str(s: &str) -> Vec<u8> {
        use flate2::{write::GzEncoder, Compression};
        use std::io::Write;
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        write!(encoder, "{}", s).expect("writing into encoder");
        encoder.finish().expect("successfully encoding gzip data")
    }

    #[tokio::test]
    async fn test_gzip_write() -> Result<()> {
        let test_storage = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        test_storage.set_id(1);
        test_storage
            .create_database("MyOrg_MyBucket", DatabaseRules::new())
            .await
            .unwrap();
        let server_url = test_server(Arc::clone(&test_storage));

        let client = Client::new();
        let lp_data = "h2o_temperature,location=santa_monica,state=CA surface_degrees=65.2,bottom_degrees=50.4 1568756160";

        // send write data encoded with gzip
        let bucket_name = "MyBucket";
        let org_name = "MyOrg";
        let response = client
            .post(&format!(
                "{}/api/v2/write?bucket={}&org={}",
                server_url, bucket_name, org_name
            ))
            .header(CONTENT_ENCODING, "gzip")
            .body(gzip_str(lp_data))
            .send()
            .await;

        check_response("gzip_write", response, StatusCode::NO_CONTENT, "").await;

        // Check that the data got into the right bucket
        let test_db = test_storage
            .db(&DatabaseName::new("MyOrg_MyBucket").unwrap())
            .expect("Database exists");

        let batches = run_query(test_db.as_ref(), "select * from h2o_temperature").await;

        let expected = vec![
            "+----------------+--------------+-------+-----------------+------------+",
            "| bottom_degrees | location     | state | surface_degrees | time       |",
            "+----------------+--------------+-------+-----------------+------------+",
            "| 50.4           | santa_monica | CA    | 65.2            | 1568756160 |",
            "+----------------+--------------+-------+-----------------+------------+",
        ];
        assert_table_eq!(expected, &batches);

        Ok(())
    }

    #[tokio::test]
    async fn get_wal_meta() {
        let server = Arc::new(AppServer::new(
            ConnectionManagerImpl {},
            Arc::new(ObjectStore::new_in_memory(InMemory::new())),
        ));
        server.set_id(1);
        let server_url = test_server(Arc::clone(&server));

        let database_name = "foo_bar";
        let rules = DatabaseRules {
            name: database_name.to_owned(),
            wal_buffer_config: Some(WalBufferConfig {
                buffer_size: 500,
                segment_size: 10,
                buffer_rollover: WalBufferRollover::ReturnError,
                store_segments: true,
                close_segment_after: None,
            }),
            ..Default::default()
        };

        server.create_database(database_name, rules).await.unwrap();

        let base_url = format!(
            "{}/iox/api/v1/databases/{}/wal/meta",
            server_url, database_name
        );

        let client = Client::new();

        let r1: WalMetadataResponse = check_json_response(&client, &base_url, StatusCode::OK).await;

        let lines: std::result::Result<Vec<_>, _> = influxdb_line_protocol::parse_lines(
            "cpu,host=A,region=west usage_system=64i 1590488773254420000",
        )
        .collect();

        server
            .write_lines(database_name, &lines.unwrap())
            .await
            .unwrap();

        let r2: WalMetadataResponse = check_json_response(&client, &base_url, StatusCode::OK).await;

        let limit_1 = serde_urlencoded::to_string(&WalMetadataQuery {
            limit: Some(1),
            newer_than: None,
            offset: None,
        })
        .unwrap();
        let limit_url = format!("{}?{}", base_url, limit_1);

        let r3: WalMetadataResponse =
            check_json_response(&client, &limit_url, StatusCode::OK).await;

        let limit_future = serde_urlencoded::to_string(&WalMetadataQuery {
            limit: None,
            offset: None,
            newer_than: Some(chrono::Utc::now() + chrono::Duration::seconds(5)),
        })
        .unwrap();
        let future_url = format!("{}?{}", base_url, limit_future);

        let r4: WalMetadataResponse =
            check_json_response(&client, &future_url, StatusCode::OK).await;

        // No data written yet - expect no results
        assert_eq!(r1.segments.len(), 1);
        assert_eq!(r1.segments[0].size, 0);
        assert_eq!(r1.segments[0].writers.len(), 0);

        // The WAL segment size is less than the line size
        // We therefore expect an open and a closed segment in that order
        // With the closed segment containing the written data
        // And the open segment containing no data
        assert_eq!(r2.segments.len(), 2);
        assert_eq!(r2.segments[0].size, 0);
        assert!(r2.segments[0].created_at >= r2.segments[1].created_at);

        assert!(r2.segments[1].persisted.is_none());
        assert_eq!(r2.segments[1].size, 368);
        assert_eq!(r2.segments[1].writers.len(), 1);
        assert_eq!(
            r2.segments[1].writers.values().next().unwrap(),
            &WriterSummary {
                start_sequence: 1,
                end_sequence: 1,
                missing_sequence: false
            }
        );

        // Query limited to a single segment - expect only the most recent segment
        assert_eq!(r3.segments.len(), 1);
        assert_eq!(r3.segments[0], r2.segments[0]);

        // Requesting segments from future - expect no results
        assert_eq!(r4.segments.len(), 0);
    }

    fn get_content_type(response: &Result<Response, reqwest::Error>) -> String {
        if let Ok(response) = response {
            response
                .headers()
                .get(CONTENT_TYPE)
                .map(|v| v.to_str().unwrap())
                .unwrap_or("")
                .to_string()
        } else {
            "".to_string()
        }
    }

    /// checks a http response against expected results
    async fn check_response(
        description: &str,
        response: Result<Response, reqwest::Error>,
        expected_status: StatusCode,
        expected_body: &str,
    ) {
        // Print the response so if the test fails, we have a log of
        // what went wrong
        println!("{} response: {:?}", description, response);

        if let Ok(response) = response {
            let status = response.status();
            let body = response
                .text()
                .await
                .expect("Converting request body to string");

            assert_eq!(status, expected_status);
            assert_eq!(body, expected_body);
        } else {
            panic!("Unexpected error response: {:?}", response);
        }
    }

    async fn check_json_response<T: DeserializeOwned + Eq + Debug>(
        client: &Client,
        url: &str,
        expected_status: StatusCode,
    ) -> T {
        let response = client.get(url).send().await;

        // Print the response so if the test fails, we have a log of
        // what went wrong
        println!("{} response: {:?}", url, response);

        if let Ok(response) = response {
            let status = response.status();
            let body: T = response
                .json()
                .await
                .expect("Converting request body to string");

            assert_eq!(status, expected_status);
            body
        } else {
            panic!("Unexpected error response: {:?}", response);
        }
    }

    /// creates an instance of the http service backed by a in-memory
    /// testable database.  Returns the url of the server
    fn test_server(server: Arc<AppServer<ConnectionManagerImpl>>) -> String {
        // NB: specify port 0 to let the OS pick the port.
        let bind_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        let addr = AddrIncoming::bind(&bind_addr).expect("failed to bind server");
        let server_url = format!("http://{}", addr.local_addr());

        tokio::task::spawn(serve(addr, server, CancellationToken::new()));
        println!("Started server at {}", server_url);
        server_url
    }

    /// Run the specified SQL query and return formatted results as a string
    async fn run_query(db: &Db, query: &str) -> Vec<RecordBatch> {
        let planner = SQLQueryPlanner::default();
        let executor = Executor::new();
        let physical_plan = planner.query(db, query, &executor).await.unwrap();

        collect(physical_plan).await.unwrap()
    }
}
