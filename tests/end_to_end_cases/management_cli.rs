use assert_cmd::Command;
use data_types::job::{Job, Operation};
use predicates::prelude::*;
use test_helpers::make_temp_file;

use crate::common::server_fixture::ServerFixture;

use super::scenario::{create_readable_database, rand_name};

#[tokio::test]
async fn test_writer_id() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("writer")
        .arg("set")
        .arg("32")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("writer")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("32"));
}

#[tokio::test]
async fn test_create_database() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();
    let db = &db_name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    // Listing the databases includes the newly created database
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(db));

    // Retrieving the database includes the name and a mutable buffer configuration
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(
            predicate::str::contains(db)
                .and(predicate::str::contains(format!("name: \"{}\"", db)))
                // validate the defaults have been set reasonably
                .and(predicate::str::contains("%Y-%m-%d %H:00:00"))
                .and(predicate::str::contains("buffer_size: 104857600"))
                .and(predicate::str::contains("MutableBufferConfig")),
        );
}

#[tokio::test]
async fn test_create_database_size() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();
    let db = &db_name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("-m")
        .arg("1000")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(
            predicate::str::contains("buffer_size: 1000")
                .and(predicate::str::contains("MutableBufferConfig")),
        );
}

#[tokio::test]
async fn test_create_database_zero_size() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();
    let db = &db_name;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("create")
        .arg(db)
        .arg("-m")
        .arg("0")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Ok"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg(db)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        // Should not have a mutable buffer
        .stdout(predicate::str::contains("MutableBufferConfig").not());
}

#[tokio::test]
async fn test_get_chunks() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
    ];

    load_lp(addr, &db_name, lp_data);

    let expected = r#"[
  {
    "partition_key": "cpu",
    "id": 0,
    "storage": "OpenMutableBuffer",
    "estimated_bytes": 145
  }
]"#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("chunk")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

#[tokio::test]
async fn test_list_chunks_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    // note don't make the database, expect error

    // list the chunks
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("chunk")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("Some requested entity was not found: Resource database")
                .and(predicate::str::contains(&db_name)),
        );
}

#[tokio::test]
async fn test_remotes() {
    let server_fixture = ServerFixture::create_single_use().await;
    let addr = server_fixture.grpc_base();
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("no remotes configured"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("set")
        .arg("1")
        .arg("http://1.2.3.4:1234")
        .arg("--host")
        .arg(addr)
        .assert()
        .success();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("http://1.2.3.4:1234"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("remove")
        .arg("1")
        .arg("--host")
        .arg(addr)
        .assert()
        .success();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("server")
        .arg("remote")
        .arg("list")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("no remotes configured"));
}

#[tokio::test]
async fn test_list_partitions() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "mem,region=west free=100000 150",
    ];
    load_lp(addr, &db_name, lp_data);

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("cpu").and(predicate::str::contains("mem")));
}

#[tokio::test]
async fn test_list_partitions_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list")
        .arg("non_existent_database")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

#[tokio::test]
async fn test_get_partition() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "mem,region=west free=100000 150",
    ];
    load_lp(addr, &db_name, lp_data);

    let expected = r#""key": "cpu""#;

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("get")
        .arg(&db_name)
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

#[tokio::test]
async fn test_get_partition_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("get")
        .arg("cpu")
        .arg("non_existent_database")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

#[tokio::test]
async fn test_list_partition_chunks() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec![
        "cpu,region=west user=23.2 100",
        "cpu2,region=west user=21.0 150",
    ];

    load_lp(addr, &db_name, lp_data);

    let expected = r#"
    "partition_key": "cpu",
    "id": 0,
    "storage": "OpenMutableBuffer",
"#;

    let partition_key = "cpu";
    // should not contain anything related to cpu2 partition
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list-chunks")
        .arg(&db_name)
        .arg(&partition_key)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected).and(predicate::str::contains("cpu2").not()));
}

#[tokio::test]
async fn test_list_partition_chunks_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    // note don't make the database, expect error

    // list the chunks
    let partition_key = "cpu";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("list-chunks")
        .arg(&db_name)
        .arg(&partition_key)
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(
            predicate::str::contains("Some requested entity was not found: Resource database")
                .and(predicate::str::contains(&db_name)),
        );
}

#[tokio::test]
async fn test_new_partition_chunk() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec!["cpu,region=west user=23.2 100"];
    load_lp(addr, &db_name, lp_data);

    let expected = "Ok";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("new-chunk")
        .arg(&db_name)
        .arg("cpu")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));

    let expected = "ClosedMutableBuffer";
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("chunk")
        .arg("list")
        .arg(&db_name)
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(expected));
}

#[tokio::test]
async fn test_new_partition_chunk_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("new-chunk")
        .arg("non_existent_database")
        .arg("non_existent_partition")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

#[tokio::test]
async fn test_close_partition_chunk() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();
    let db_name = rand_name();

    create_readable_database(&db_name, server_fixture.grpc_channel()).await;

    let lp_data = vec!["cpu,region=west user=23.2 100"];
    load_lp(addr, &db_name, lp_data);

    let stdout: Operation = serde_json::from_slice(
        &Command::cargo_bin("influxdb_iox")
            .unwrap()
            .arg("database")
            .arg("partition")
            .arg("close-chunk")
            .arg(&db_name)
            .arg("cpu")
            .arg("0")
            .arg("--host")
            .arg(addr)
            .assert()
            .success()
            .get_output()
            .stdout,
    )
    .expect("Expected JSON output");

    let expected_job = Job::CloseChunk {
        db_name,
        partition_key: "cpu".into(),
        chunk_id: 0,
    };

    assert_eq!(
        Some(expected_job),
        stdout.job,
        "operation was {:#?}",
        stdout
    );
}

#[tokio::test]
async fn test_close_partition_chunk_error() {
    let server_fixture = ServerFixture::create_shared().await;
    let addr = server_fixture.grpc_base();

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("partition")
        .arg("close-chunk")
        .arg("non_existent_database")
        .arg("non_existent_partition")
        .arg("0")
        .arg("--host")
        .arg(addr)
        .assert()
        .failure()
        .stderr(predicate::str::contains("Database not found"));
}

/// Loads the specified lines into the named database
fn load_lp(addr: &str, db_name: &str, lp_data: Vec<&str>) {
    let lp_data_file = make_temp_file(lp_data.join("\n"));

    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("write")
        .arg(&db_name)
        .arg(lp_data_file.as_ref())
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains("Lines OK"));
}
