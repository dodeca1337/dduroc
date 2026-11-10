//! Independent public acceptance of clock identity and RFC3339 boundaries.
use async_graphql::{Request, Variables};
use dduroc::prelude::*;
use dduroc::read::{Query, Reader};
use dduroc_graphql::{LogSchema, SchemaLimits, Service, ServiceConfig, build_schema};
use serde_json::{Value, json};
use std::sync::Arc;

dduroc::schema! {
    name: clock_acceptance, version: 1, languages: [en],
    events { Tick = 1 { level: Info, en: "tick {value}", value: u32 } }
    metrics { Mode = 1 { states: [Off = 0, On = 1], store: telemetry } }
}

fn graph(reader: Arc<Reader>) -> LogSchema {
    build_schema(
        Service::new(reader, ServiceConfig::default()).unwrap(),
        SchemaLimits::default(),
    )
    .unwrap()
}

async fn read(schema: &LogSchema, query: Value) -> Value {
    let response = schema
        .execute(
            Request::new(
                "query($query:QueryInput!){read(query:$query){entries{namespace at{boot micros} utc} seeds{at{boot micros} utc sample{stateName}} unanchored exhausted}}",
            )
            .variables(Variables::from_json(json!({"query": query}))),
        )
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()["read"].clone()
}

#[tokio::test]
async fn live_anchor_refresh_offsets_and_dump_preserve_the_same_record_identity() {
    let directory = tempfile::tempdir().unwrap();
    let anchored;
    let instant;
    {
        let store = Store::open(StoreConfig::new(directory.path())).unwrap();
        let namespace = store.namespace("clock", clock_acceptance::SCHEMA).unwrap();
        namespace
            .try_log(clock_acceptance::events::Tick { value: 7 })
            .unwrap();
        store.sync().unwrap();
        let reader = Arc::new(store.reader());
        let schema = graph(Arc::clone(&reader));
        let before = read(&schema, json!({})).await;
        assert_eq!(before["entries"].as_array().unwrap().len(), 1);
        assert!(before["entries"][0]["utc"].is_null());
        let missing = read(&schema, json!({"from":{"utc":"2020-01-01T04:00:00+04:00"}})).await;
        assert!(missing["entries"].as_array().unwrap().is_empty());
        assert_eq!(
            missing["unanchored"],
            json!([store.boot_counter().to_string()])
        );

        assert!(store.record_sync(Utc::now(), SyncSource::User).unwrap());
        anchored = read(&schema, json!({})).await;
        // A reader created before synchronization must pick up the new anchor,
        // without changing the stable boot/microsecond identity of the event.
        assert_eq!(anchored["entries"][0]["at"], before["entries"][0]["at"]);
        let native = reader.query(&Query::new()).unwrap();
        instant = native.entries[0].utc.unwrap();
        assert_eq!(store.to_utc(native.entries[0].at), Some(instant));
        assert_eq!(anchored["entries"][0]["utc"], instant.to_rfc3339());
        assert_eq!(instant.timestamp_subsec_nanos() % 1_000, 0);

        let offset = instant.with_timezone(&dduroc::chrono::FixedOffset::east_opt(14_400).unwrap());
        let point = json!({"from":{"utc":offset.to_rfc3339()},"to":{"utc":instant.to_rfc3339()}});
        assert_eq!(read(&schema, point).await["entries"], anchored["entries"]);
        let before_tick = instant - dduroc::chrono::Duration::nanoseconds(1);
        let excluded = read(&schema, json!({"to":{"utc":before_tick.to_rfc3339()}})).await;
        assert!(excluded["entries"].as_array().unwrap().is_empty());
        let after_tick = instant + dduroc::chrono::Duration::nanoseconds(1);
        let excluded = read(&schema, json!({"from":{"utc":after_tick.to_rfc3339()}})).await;
        assert!(excluded["entries"].as_array().unwrap().is_empty());
    }
    let reader =
        Arc::new(Reader::open_dump([directory.path()], &[clock_acceptance::SCHEMA]).unwrap());
    let schema = graph(reader);
    let restored = read(
        &schema,
        json!({"from":{"utc":instant.to_rfc3339()},"to":{"utc":instant.to_rfc3339()}}),
    )
    .await;
    assert_eq!(restored["entries"], anchored["entries"]);
    assert_eq!(restored["unanchored"], json!([]));
}

#[tokio::test]
async fn fractional_utc_window_keeps_the_last_state_strictly_before_its_start() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let namespace = store.namespace("clock", clock_acceptance::SCHEMA).unwrap();
    namespace
        .series(clock_acceptance::metrics::Mode)
        .unwrap()
        .sample(clock_acceptance::metrics::Mode::On);
    store.sync().unwrap();
    assert!(store.record_sync(Utc::now(), SyncSource::User).unwrap());
    let reader = Arc::new(store.reader());
    let all = reader.query(&Query::new()).unwrap();
    assert_eq!(all.entries.len(), 1);
    let instant = all.entries[0].utc.unwrap();
    let from = instant + dduroc::chrono::Duration::nanoseconds(1);
    let native = reader
        .query(&Query::new().since(from).with_state_seed())
        .unwrap();
    assert!(native.entries.is_empty());
    assert_eq!(native.seeds.len(), 1);
    assert_eq!(native.seeds[0].at, all.entries[0].at);
    let schema = graph(reader);
    let response = read(
        &schema,
        json!({"from":{"utc":from.to_rfc3339()},"seedStates":true}),
    )
    .await;
    assert!(response["entries"].as_array().unwrap().is_empty());
    assert_eq!(response["seeds"].as_array().unwrap().len(), 1);
    assert_eq!(response["seeds"][0]["sample"]["stateName"], "On");
    assert_eq!(response["seeds"][0]["utc"], instant.to_rfc3339());
}

#[tokio::test]
async fn leap_second_bounds_follow_datetime_order_without_aliasing_the_next_second() {
    use dduroc::chrono::{DateTime, Duration, Timelike};
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let namespace = store.namespace("clock", clock_acceptance::SCHEMA).unwrap();
    namespace
        .try_log(clock_acceptance::events::Tick { value: 1 })
        .unwrap();
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let at = reader.query(&Query::new()).unwrap().entries[0].at;
    let next_second = DateTime::parse_from_rfc3339("2026-11-10T00:00:00Z")
        .unwrap()
        .to_utc();
    // Account for setup time before explicitly anchoring the already written
    // record near the middle of the first normal second after the leap bound.
    let age = store.now().at.0.checked_sub(at.at.0).unwrap();
    let synchronized_now = next_second
        + Duration::milliseconds(500)
        + Duration::microseconds(i64::try_from(age).unwrap());
    assert!(
        store
            .record_sync(synchronized_now, SyncSource::User)
            .unwrap()
    );
    let instant = reader.query(&Query::new()).unwrap().entries[0].utc.unwrap();
    assert!(
        instant >= next_second && instant < next_second + Duration::seconds(1),
        "fixture synchronization delayed beyond the one-second window: {instant}"
    );
    let leap = DateTime::parse_from_rfc3339("2026-11-09T23:59:60.999999999Z")
        .unwrap()
        .to_utc();
    assert!(leap.nanosecond() >= 1_000_000_000);
    assert!(leap < instant);
    assert!(
        reader
            .query(&Query::new().until(leap))
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        reader
            .query(&Query::new().since(leap))
            .unwrap()
            .entries
            .len(),
        1
    );
    let schema = graph(reader);
    let before = read(&schema, json!({"to":{"utc":leap.to_rfc3339()}})).await;
    assert!(before["entries"].as_array().unwrap().is_empty());
    let after = read(&schema, json!({"from":{"utc":leap.to_rfc3339()}})).await;
    assert_eq!(after["entries"].as_array().unwrap().len(), 1);
    assert_eq!(after["entries"][0]["utc"], instant.to_rfc3339());
}
