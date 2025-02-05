use std::num::NonZeroU32;

use generated_types::{
    google::protobuf::{Duration, Empty},
    influxdata::iox::management::v1::*,
};
use influxdb_iox_client::management::CreateDatabaseError;
use test_helpers::assert_contains;

use super::{
    operations_api::get_operation_metadata,
    scenario::{
        create_readable_database, create_two_partition_database, create_unreadable_database,
        rand_name,
    },
};
use crate::common::server_fixture::ServerFixture;

#[tokio::test]
async fn test_list_update_remotes() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    const TEST_REMOTE_ID_1: u32 = 42;
    const TEST_REMOTE_ADDR_1: &str = "1.2.3.4:1234";
    const TEST_REMOTE_ID_2: u32 = 84;
    const TEST_REMOTE_ADDR_2: &str = "4.3.2.1:4321";
    const TEST_REMOTE_ADDR_2_UPDATED: &str = "40.30.20.10:4321";

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 0);

    client
        .update_remote(TEST_REMOTE_ID_1, TEST_REMOTE_ADDR_1)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);

    client
        .update_remote(TEST_REMOTE_ID_2, TEST_REMOTE_ADDR_2)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 2);
    assert_eq!(res[0].id, TEST_REMOTE_ID_1);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_1);
    assert_eq!(res[1].id, TEST_REMOTE_ID_2);
    assert_eq!(res[1].connection_string, TEST_REMOTE_ADDR_2);

    client
        .delete_remote(TEST_REMOTE_ID_1)
        .await
        .expect("delete failed");

    client
        .delete_remote(TEST_REMOTE_ID_1)
        .await
        .expect_err("expected delete to fail");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].id, TEST_REMOTE_ID_2);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_2);

    client
        .update_remote(TEST_REMOTE_ID_2, TEST_REMOTE_ADDR_2_UPDATED)
        .await
        .expect("update failed");

    let res = client.list_remotes().await.expect("list remotes failed");
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].id, TEST_REMOTE_ID_2);
    assert_eq!(res[0].connection_string, TEST_REMOTE_ADDR_2_UPDATED);
}

#[tokio::test]
async fn test_set_get_writer_id() {
    let server_fixture = ServerFixture::create_single_use().await;
    let mut client = server_fixture.management_client();

    const TEST_ID: u32 = 42;

    client
        .update_writer_id(NonZeroU32::new(TEST_ID).unwrap())
        .await
        .expect("set ID failed");

    let got = client.get_writer_id().await.expect("get ID failed");

    assert_eq!(got.get(), TEST_ID);
}

#[tokio::test]
async fn test_create_database_duplicate_name() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();

    client
        .create_database(DatabaseRules {
            name: db_name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let err = client
        .create_database(DatabaseRules {
            name: db_name,
            ..Default::default()
        })
        .await
        .expect_err("create database failed");

    assert!(matches!(
        dbg!(err),
        CreateDatabaseError::DatabaseAlreadyExists
    ))
}

#[tokio::test]
async fn test_create_database_invalid_name() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let err = client
        .create_database(DatabaseRules {
            name: "my_example\ndb".to_string(),
            ..Default::default()
        })
        .await
        .expect_err("expected request to fail");

    assert!(matches!(dbg!(err), CreateDatabaseError::InvalidArgument(_)));
}

#[tokio::test]
async fn test_list_databases() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let name = rand_name();
    client
        .create_database(DatabaseRules {
            name: name.clone(),
            ..Default::default()
        })
        .await
        .expect("create database failed");

    let names = client
        .list_databases()
        .await
        .expect("list databases failed");
    assert!(names.contains(&name));
}

#[tokio::test]
async fn test_create_get_database() {
    let server_fixture = ServerFixture::create_shared().await;
    let mut client = server_fixture.management_client();

    let db_name = rand_name();

    // Specify everything to allow direct comparison between request and response
    // Otherwise would expect difference due to server-side defaulting
    let rules = DatabaseRules {
        name: db_name.clone(),
        partition_template: Some(PartitionTemplate {
            parts: vec![partition_template::Part {
                part: Some(partition_template::part::Part::Table(Empty {})),
            }],
        }),
        wal_buffer_config: Some(WalBufferConfig {
            buffer_size: 24,
            segment_size: 2,
            buffer_rollover: wal_buffer_config::Rollover::DropIncoming as _,
            persist_segments: true,
            close_segment_after: Some(Duration {
                seconds: 324,
                nanos: 2,
            }),
        }),
        mutable_buffer_config: Some(MutableBufferConfig {
            buffer_size: 553,
            reject_if_not_persisted: true,
            partition_drop_order: Some(mutable_buffer_config::PartitionDropOrder {
                order: Order::Asc as _,
                sort: Some(
                    mutable_buffer_config::partition_drop_order::Sort::CreatedAtTime(Empty {}),
                ),
            }),
            persist_after_cold_seconds: 34,
        }),
    };

    client
        .create_database(rules.clone())
        .await
        .expect("create database failed");

    let response = client
        .get_database(db_name)
        .await
        .expect("get database failed");

    assert_eq!(response, rules);
}

#[tokio::test]
async fn test_chunk_get() {
    use generated_types::influxdata::iox::management::v1::{Chunk, ChunkStorage};

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let mut chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    // ensure the output order is consistent
    chunks.sort_by(|c1, c2| c1.partition_key.cmp(&c2.partition_key));

    let expected: Vec<Chunk> = vec![
        Chunk {
            partition_key: "cpu".into(),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer as i32,
            estimated_bytes: 145,
        },
        Chunk {
            partition_key: "disk".into(),
            id: 0,
            storage: ChunkStorage::OpenMutableBuffer as i32,
            estimated_bytes: 107,
        },
    ];
    assert_eq!(
        expected, chunks,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, chunks
    );
}

#[tokio::test]
async fn test_chunk_get_errors() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let db_name = rand_name();

    let err = management_client
        .list_chunks(&db_name)
        .await
        .expect_err("no db had been created");

    assert_contains!(
        err.to_string(),
        "Some requested entity was not found: Resource database"
    );

    create_unreadable_database(&db_name, fixture.grpc_channel()).await;

    let err = management_client
        .list_chunks(&db_name)
        .await
        .expect_err("db can't be read");

    assert_contains!(
        err.to_string(),
        "Precondition violation influxdata.com/iox - database"
    );
    assert_contains!(
        err.to_string(),
        "Cannot read from database: no mutable buffer configured"
    );
}

#[tokio::test]
async fn test_partition_list() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let mut partitions = management_client
        .list_partitions(&db_name)
        .await
        .expect("listing partition");

    // ensure the output order is consistent
    partitions.sort_by(|p1, p2| p1.key.cmp(&p2.key));

    let expected = vec![
        Partition {
            key: "cpu".to_string(),
        },
        Partition {
            key: "mem".to_string(),
        },
    ];

    assert_eq!(
        expected, partitions,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, partitions
    );
}

#[tokio::test]
async fn test_partition_list_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .list_partitions("this database does not exist")
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");
}

#[tokio::test]
async fn test_partition_get() {
    use generated_types::influxdata::iox::management::v1::Partition;

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let db_name = rand_name();
    create_two_partition_database(&db_name, fixture.grpc_channel()).await;

    let partition_key = "cpu";
    let partition = management_client
        .get_partition(&db_name, partition_key)
        .await
        .expect("getting partition");

    let expected = Partition { key: "cpu".into() };

    assert_eq!(
        expected, partition,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, partition
    );
}

#[tokio::test]
async fn test_partition_get_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let err = management_client
        .list_partitions("this database does not exist")
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines =
        vec!["processes,host=foo running=4i,sleeping=514i,total=519i 1591894310000000000"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let err = management_client
        .get_partition(&db_name, "non existent partition")
        .await
        .expect_err("exepcted error getting partition");

    assert_contains!(err.to_string(), "Partition not found");
}

#[tokio::test]
async fn test_list_partition_chunks() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec![
        "cpu,region=west user=23.2 100",
        "cpu,region=west user=21.0 150",
        "disk,region=east bytes=99i 200",
    ];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let partition_key = "cpu";
    let chunks = management_client
        .list_partition_chunks(&db_name, partition_key)
        .await
        .expect("getting partition chunks");

    let expected: Vec<Chunk> = vec![Chunk {
        partition_key: "cpu".into(),
        id: 0,
        storage: ChunkStorage::OpenMutableBuffer as i32,
        estimated_bytes: 145,
    }];

    assert_eq!(
        expected, chunks,
        "expected:\n\n{:#?}\n\nactual:{:#?}",
        expected, chunks
    );
}

#[tokio::test]
async fn test_list_partition_chunk_errors() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let db_name = rand_name();

    let err = management_client
        .list_partition_chunks(&db_name, "cpu")
        .await
        .expect_err("no db had been created");

    assert_contains!(
        err.to_string(),
        "Some requested entity was not found: Resource database"
    );
}

#[tokio::test]
async fn test_new_partition_chunk() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    let partition_key = "cpu";

    // Rollover the a second chunk
    management_client
        .new_partition_chunk(&db_name, partition_key)
        .await
        .expect("new partition chunk");

    // Load some more data and now expect that we have a second chunk

    let lp_lines = vec!["cpu,region=west user=21.0 150"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 2, "Chunks: {:#?}", chunks);

    // Made all chunks in the same partition
    assert_eq!(
        chunks.iter().filter(|c| c.partition_key == "cpu").count(),
        2,
        "Chunks: {:#?}",
        chunks
    );

    // Rollover a (currently non existent) partition which is OK
    management_client
        .new_partition_chunk(&db_name, "non_existent_partition")
        .await
        .expect("new partition chunk");

    assert_eq!(chunks.len(), 2, "Chunks: {:#?}", chunks);
    assert_eq!(
        chunks.iter().filter(|c| c.partition_key == "cpu").count(),
        2,
        "Chunks: {:#?}",
        chunks
    );
    assert_eq!(
        chunks
            .iter()
            .filter(|c| c.partition_key == "non_existent_partition")
            .count(),
        0,
        "Chunks: {:#?}",
        chunks
    );
}

#[tokio::test]
async fn test_new_partition_chunk_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .new_partition_chunk("this database does not exist", "nor_does_this_partition")
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");
}

#[tokio::test]
async fn test_close_partition_chunk() {
    use influxdb_iox_client::management::generated_types::operation_metadata::Job;
    use influxdb_iox_client::management::generated_types::ChunkStorage;

    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();
    let mut write_client = fixture.write_client();
    let mut operations_client = fixture.operations_client();

    let db_name = rand_name();
    create_readable_database(&db_name, fixture.grpc_channel()).await;

    let partition_key = "cpu";
    let lp_lines = vec!["cpu,region=west user=23.2 100"];

    write_client
        .write(&db_name, lp_lines.join("\n"))
        .await
        .expect("write succeded");

    let chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");

    assert_eq!(chunks.len(), 1, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, 0);
    assert_eq!(chunks[0].storage, ChunkStorage::OpenMutableBuffer as i32);

    // Move the chunk to read buffer
    let operation = management_client
        .close_partition_chunk(&db_name, partition_key, 0)
        .await
        .expect("new partition chunk");

    println!("Operation response is {:?}", operation);
    let operation_id = operation.name.parse().expect("not an integer");

    let meta = get_operation_metadata(operation.metadata);

    // ensure we got a legit job description back
    if let Some(Job::CloseChunk(close_chunk)) = meta.job {
        assert_eq!(close_chunk.db_name, db_name);
        assert_eq!(close_chunk.partition_key, partition_key);
        assert_eq!(close_chunk.chunk_id, 0);
    } else {
        panic!("unexpected job returned")
    };

    // wait for the job to be done
    operations_client
        .wait_operation(operation_id, Some(std::time::Duration::from_secs(1)))
        .await
        .expect("failed to wait operation");

    // And now the chunk  should be good
    let mut chunks = management_client
        .list_chunks(&db_name)
        .await
        .expect("listing chunks");
    chunks.sort_by(|c1, c2| c1.id.cmp(&c2.id));

    assert_eq!(chunks.len(), 2, "Chunks: {:#?}", chunks);
    assert_eq!(chunks[0].id, 0);
    assert_eq!(chunks[0].storage, ChunkStorage::ReadBuffer as i32);
    assert_eq!(chunks[1].id, 1);
    assert_eq!(chunks[1].storage, ChunkStorage::OpenMutableBuffer as i32);
}

#[tokio::test]
async fn test_close_partition_chunk_error() {
    let fixture = ServerFixture::create_shared().await;
    let mut management_client = fixture.management_client();

    let err = management_client
        .close_partition_chunk("this database does not exist", "nor_does_this_partition", 0)
        .await
        .expect_err("expected error");

    assert_contains!(err.to_string(), "Database not found");
}
