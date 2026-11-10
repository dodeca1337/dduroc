use dduroc::prelude::*;
use dduroc::read::{Cancellation, Query, ReadLimits, Reader};
use dduroc_graphql::{SchemaLimits, Service, ServiceConfig, build_schema};
use std::sync::Arc;

dduroc::schema! {
    name: statistics_fixture, version: 1, languages: [en],
    events { Tick = 1 { level: Info, en: "tick {value}", value: u32 } }
    metrics { Reading = 1 { type: u64 } }
    spans { Operation = 1 }
}
fn graph(reader: Arc<Reader>) -> dduroc_graphql::LogSchema {
    build_schema(
        Service::new(reader, ServiceConfig::default()).unwrap(),
        SchemaLimits::default(),
    )
    .unwrap()
}
#[tokio::test]
async fn statistics_count_retained_wire_records_not_process_writes_or_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let records;
    {
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        let namespace = store
            .namespace("source", statistics_fixture::SCHEMA)
            .unwrap();
        for value in 0..3 {
            namespace
                .try_log(statistics_fixture::events::Tick { value })
                .unwrap();
        }
        namespace
            .series(statistics_fixture::metrics::Reading)
            .unwrap()
            .sample(42);
        {
            let _span = namespace.span(statistics_fixture::spans::Operation);
        }
        store.sync().unwrap();
        let reader = Arc::new(store.reader());
        records = reader.query(&Query::new()).unwrap().entries.len() as u64;
        let snapshot = reader
            .statistics_controlled(ReadLimits::default(), Cancellation::new(), true)
            .unwrap();
        assert_eq!(snapshot.retained_records, Some(records));
        assert!(snapshot.retained_blocks.unwrap() > 0);
        assert!(snapshot.logical_retained_bytes > 0);
        assert!(snapshot.runtime.is_some());
        assert!(snapshot.roots.as_ref().unwrap()[0].allocated_bytes > 0);
        let response = graph(reader).execute("{statistics {countingState retainedRecords retainedBlocks logicalRetainedBytes channels classes { class logicalRetainedBytes limitBytes } runtime { recordsWritten dropped } roots { index allocatedBytes logBytes rewriteBytes metadataBytes physicalLimitBytes } }}").await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        let data = response.data.into_json().unwrap();
        assert_eq!(data["statistics"]["countingState"], "COMPLETE");
        assert_eq!(data["statistics"]["retainedRecords"], records.to_string());
        assert!(data["statistics"]["runtime"].is_object());
    }
    let offline = Arc::new(Reader::open_dump([dir.path()], &[statistics_fixture::SCHEMA]).unwrap());
    let snapshot = offline
        .statistics_controlled(ReadLimits::default(), Cancellation::new(), true)
        .unwrap();
    assert_eq!(snapshot.retained_records, Some(records));
    assert!(snapshot.runtime.is_none());
    assert!(
        snapshot
            .classes
            .iter()
            .all(|class| class.limit_bytes.is_none())
    );
    let response = graph(offline).execute("{statistics {retainedRecords runtime { recordsWritten } roots { physicalLimitBytes } }}").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    let data = response.data.into_json().unwrap();
    assert_eq!(data["statistics"]["runtime"], serde_json::Value::Null);
    assert_eq!(
        data["statistics"]["roots"][0]["physicalLimitBytes"],
        serde_json::Value::Null
    );
}

#[tokio::test]
async fn missing_index_is_backfilled_by_bounded_graphql_steps_without_changing_records() {
    let dir = tempfile::tempdir().unwrap();
    let before;
    {
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        let namespace = store
            .namespace("old-index", statistics_fixture::SCHEMA)
            .unwrap();
        for value in 0..3 {
            namespace
                .try_log(statistics_fixture::events::Tick { value })
                .unwrap();
            store.sync().unwrap();
        }
        before = store
            .reader()
            .query(&Query::new())
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.at)
            .collect::<Vec<_>>();
    }
    // Disposable fixture only: remove the count index as if it predated this
    // feature. A fresh thread isolates raft-engine's thread-local block cache.
    let path = dir.path().join("raft-engine");
    std::thread::spawn(move || {
        let config = raft_engine::Config {
            dir: path.to_str().unwrap().into(),
            recovery_mode: raft_engine::RecoveryMode::AbsoluteConsistency,
            batch_compression_threshold: raft_engine::ReadableSize(0),
            target_file_size: raft_engine::ReadableSize(4 * 1024 * 1024),
            purge_threshold: raft_engine::ReadableSize(1024 * 1024 * 1024),
            purge_rewrite_threshold: Some(raft_engine::ReadableSize(8 * 1024 * 1024)),
            enable_log_recycle: false,
            prefill_for_recycle: false,
            ..Default::default()
        };
        let engine = raft_engine::Engine::open(config).unwrap();
        let mut batch = raft_engine::LogBatch::default();
        for id in engine.raft_groups().into_iter().filter(|id| *id != 0) {
            batch.delete(id, b"dduroc.counts.v1".to_vec());
        }
        engine.write(&mut batch, true).unwrap();
    })
    .join()
    .unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let reader = Arc::new(store.reader());
    let same = reader
        .query(&Query::new())
        .unwrap()
        .entries
        .into_iter()
        .map(|entry| entry.at)
        .collect::<Vec<_>>();
    assert_eq!(same, before);
    let schema = graph(reader.clone());
    let initial = schema
        .execute("{statistics {countingState retainedRecords retainedBlocks}}")
        .await;
    assert!(initial.errors.is_empty(), "{:?}", initial.errors);
    let data = initial.data.into_json().unwrap();
    assert_eq!(data["statistics"]["countingState"], "BACKFILL_REQUIRED");
    assert_eq!(
        data["statistics"]["retainedRecords"],
        serde_json::Value::Null
    );
    let mut steps = 0;
    loop {
        let response = schema
            .execute("mutation {backfillStatistics(limits:{blocks:\"1\"}) {more}}")
            .await;
        assert!(response.errors.is_empty(), "{:?}", response.errors);
        steps += 1;
        if !response.data.into_json().unwrap()["backfillStatistics"]["more"]
            .as_bool()
            .unwrap()
        {
            break;
        }
        assert!(steps < 20);
    }
    assert!(steps >= 3, "each mutation scans at most one block");
    let done = schema
        .execute("{statistics {countingState retainedRecords retainedBlocks}}")
        .await;
    assert!(done.errors.is_empty(), "{:?}", done.errors);
    let data = done.data.into_json().unwrap();
    assert_eq!(data["statistics"]["countingState"], "COMPLETE");
    assert_eq!(data["statistics"]["retainedRecords"], "3");
    assert_eq!(data["statistics"]["retainedBlocks"], "3");
    assert_eq!(
        reader
            .query(&Query::new())
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.at)
            .collect::<Vec<_>>(),
        before
    );
}
