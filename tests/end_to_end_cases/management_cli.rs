use assert_cmd::Command;
use predicates::prelude::*;
use test_helpers::make_temp_file;

use crate::common::server_fixture::ServerFixture;

use super::util::{create_readable_database, rand_name};

#[tokio::test]
async fn test_writer_id() {
    let server_fixture = ServerFixture::create_single_use().await;
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

    // Listing the database includes the name
    Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("database")
        .arg("get")
        .arg("--host")
        .arg(addr)
        .assert()
        .success()
        .stdout(predicate::str::contains(db));

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
            predicate::str::contains(format!("name: \"{}\"", db)).and(
                // validate the defaults have been set reasonably
                predicate::str::contains("%Y-%m-%d %H")
                    .and(predicate::str::contains("buffer_size: 104857600")),
            ),
        );
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
        .stdout(predicate::str::contains("2 Lines OK"));

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
        .arg("chunk")
        .arg("get")
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
        .arg("chunk")
        .arg("get")
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
