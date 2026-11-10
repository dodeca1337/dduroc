//! Independent retained-vs-runtime statistics through the public Rust and GraphQL APIs.
use dduroc::prelude::*;
use dduroc::read::{Cancellation, Query, ReadLimits, Reader};
use dduroc_graphql::{LogSchema, SchemaLimits, Service, ServiceConfig, build_schema};
use serde_json::Value;
use std::sync::Arc;

dduroc::schema! {
    name: statistics_acceptance, version: 1, languages: [en],
    events {
        Reading = 1 { level: Info, en: "reading {value}", value: u32 },
        Alert = 2 { level: Error, store: critical, en: "alert" },
    }
    metrics { Mode = 1 { states: [Off = 0, On = 1], store: telemetry } }
    spans { Work = 1, }
}

fn graph(reader: Arc<Reader>) -> LogSchema {
    build_schema(
        Service::new(reader, ServiceConfig::default()).unwrap(),
        SchemaLimits::default(),
    )
    .unwrap()
}
async fn statistics(schema: &LogSchema) -> Value {
    let response = schema.execute("{statistics{countingState retainedRecords retainedBlocks logicalRetainedBytes channels classes{class logicalRetainedBytes limitBytes} runtime{recordsWritten dropped rejected} roots{index allocatedBytes logBytes rewriteBytes metadataBytes physicalLimitBytes}}}").await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()["statistics"].clone()
}

#[tokio::test]
async fn counts_retained_wire_records_not_blocks_and_preserves_closed_namespaces_on_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let before;
    {
        let store = Store::open(StoreConfig::new(directory.path())).unwrap();
        {
            let first = store
                .namespace("closed", statistics_acceptance::SCHEMA)
                .unwrap();
            for value in 0..7 {
                first
                    .try_log(statistics_acceptance::events::Reading { value })
                    .unwrap();
            }
            first
                .series(statistics_acceptance::metrics::Mode)
                .unwrap()
                .try_sample(statistics_acceptance::metrics::Mode::On)
                .unwrap();
            first.try_log(statistics_acceptance::events::Alert).unwrap();
            first
                .try_log_text(Level::Info, "test", "reader-visible text", None)
                .unwrap();
            {
                let _span = first.try_span(statistics_acceptance::spans::Work).unwrap();
            }
            first.sync().unwrap();
        }
        let second = store
            .namespace("active", statistics_acceptance::SCHEMA)
            .unwrap();
        second
            .try_log(statistics_acceptance::events::Reading { value: 9 })
            .unwrap();
        // Count bookkeeping must not invoke a typed decoder. This is a valid
        // stored Message frame with intentionally invalid schema payload.
        second
            .try_log_raw(dduroc::EventId(1), &[0xff], None)
            .unwrap();
        store.sync().unwrap();
        let reader = Arc::new(store.reader());
        let native = reader
            .statistics_controlled(ReadLimits::default(), Cancellation::new(), true)
            .unwrap();
        assert_eq!(native.retained_records, Some(14));
        assert!(native.retained_blocks.unwrap() < 14);
        assert_eq!(reader.query(&Query::new()).unwrap().entries.len(), 14);
        let schema = graph(Arc::clone(&reader));
        before = statistics(&schema).await;
        assert_eq!(before["countingState"], "COMPLETE");
        assert_eq!(before["retainedRecords"], "14");
        assert_eq!(
            before["retainedBlocks"],
            native.retained_blocks.unwrap().to_string()
        );
        assert_eq!(before["runtime"]["recordsWritten"], "14");
        assert!(
            before["classes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|class| class["limitBytes"].is_string())
        );
        let root = &before["roots"][0];
        let allocated: u64 = root["allocatedBytes"].as_str().unwrap().parse().unwrap();
        let logical: u64 = before["logicalRetainedBytes"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(
            allocated > logical,
            "filesystem allocation must not be mislabeled as logical retained bytes"
        );
        assert!(root["physicalLimitBytes"].is_string());
        let cancelled = Cancellation::new();
        cancelled.cancel();
        assert!(
            reader
                .statistics_controlled(ReadLimits::default(), cancelled, false)
                .is_err()
        );
    }
    {
        let reader = Arc::new(
            Reader::open_dump([directory.path()], &[statistics_acceptance::SCHEMA]).unwrap(),
        );
        let after = statistics(&graph(reader)).await;
        for key in [
            "retainedRecords",
            "retainedBlocks",
            "logicalRetainedBytes",
            "channels",
        ] {
            assert_eq!(after[key], before[key], "{key}");
        }
        assert!(after["runtime"].is_null());
        assert!(
            after["classes"]
                .as_array()
                .unwrap()
                .iter()
                .all(|class| class["limitBytes"].is_null())
        );
        assert!(after["roots"][0]["physicalLimitBytes"].is_null());
    }
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let stats = store
        .reader()
        .statistics_controlled(ReadLimits::default(), Cancellation::new(), false)
        .unwrap();
    assert_eq!(stats.retained_records, Some(14));
    assert_eq!(
        stats.runtime.unwrap().records_written,
        0,
        "process counters restart independently of retained history"
    );
    assert!(stats.roots.is_none());
}

#[tokio::test]
async fn empty_store_zero_is_distinct_from_unindexed_and_offline_backfill_is_refused() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = Store::open(StoreConfig::new(directory.path())).unwrap();
        store.sync().unwrap();
    }
    let reader =
        Arc::new(Reader::open_dump([directory.path()], &[statistics_acceptance::SCHEMA]).unwrap());
    let schema = graph(reader);
    let stats = statistics(&schema).await;
    assert_eq!(stats["countingState"], "COMPLETE");
    assert_eq!(stats["retainedRecords"], "0");
    assert_eq!(stats["retainedBlocks"], "0");
    let result = schema.execute("mutation{backfillStatistics{more}}").await;
    assert_eq!(result.errors.len(), 1);
}
