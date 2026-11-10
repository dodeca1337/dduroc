use dduroc::prelude::*;
use dduroc::read::{Order, Query, Reader};
use dduroc_graphql::{SchemaLimits, Service, ServiceConfig, build_schema};
use std::sync::Arc;

dduroc::schema! {
    name: utc_fixture, version: 1, languages: [en],
    events { Tick = 1 { level: Info, en: "tick {value}", value: u32 } }
}
fn graph(reader: Arc<Reader>) -> dduroc_graphql::LogSchema {
    build_schema(
        Service::new(reader, ServiceConfig::default()).unwrap(),
        SchemaLimits::default(),
    )
    .unwrap()
}
async fn read(schema: &dduroc_graphql::LogSchema, from: &str, to: &str) -> serde_json::Value {
    let query = format!(
        r#"{{ read(query:{{order:OLDEST,from:{{utc:"{from}"}},to:{{utc:"{to}"}}}},page:{{maxRecords:"10"}}) {{ entries {{ at {{ boot micros }} utc }} unanchored }} }}"#
    );
    let response = schema.execute(query).await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()
}
#[tokio::test]
async fn persisted_anchor_and_submicrosecond_ranges_agree_across_native_and_graphql() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        assert!(
            store
                .record_sync(dduroc::chrono::Utc::now(), dduroc::SyncSource::User)
                .unwrap()
        );
        let namespace = store.namespace("radio", utc_fixture::SCHEMA).unwrap();
        namespace
            .try_log(utc_fixture::events::Tick { value: 42 })
            .unwrap();
        store.sync().unwrap();
    }
    let reader = Arc::new(Reader::open_dump([dir.path()], &[utc_fixture::SCHEMA]).unwrap());
    let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert_eq!(all.entries.len(), 1);
    let when = all.entries[0].utc.expect("persisted anchor");
    let sub_tick = when + dduroc::chrono::Duration::nanoseconds(1);
    let end = when + dduroc::chrono::Duration::seconds(1);
    assert_eq!(
        reader
            .query(&Query::new().time_window(when, when))
            .unwrap()
            .entries
            .len(),
        1
    );
    assert!(
        reader
            .query(&Query::new().time_window(sub_tick, end))
            .unwrap()
            .entries
            .is_empty()
    );
    let schema = graph(reader);
    let exact = read(&schema, &when.to_rfc3339(), &when.to_rfc3339()).await;
    assert_eq!(exact["read"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(exact["read"]["entries"][0]["utc"], when.to_rfc3339());
    assert!(exact["read"]["unanchored"].as_array().unwrap().is_empty());
    let after = read(&schema, &sub_tick.to_rfc3339(), &end.to_rfc3339()).await;
    assert!(after["read"]["entries"].as_array().unwrap().is_empty());
    let inverted = format!(
        r#"{{ read(query:{{from:{{utc:"{}"}},to:{{utc:"{}"}}}}) {{ exhausted }} }}"#,
        end.to_rfc3339(),
        when.to_rfc3339()
    );
    assert!(!schema.execute(inverted).await.errors.is_empty());
}
#[tokio::test]
async fn unanchored_utc_range_reports_missing_anchor() {
    let dir = tempfile::tempdir().unwrap();
    {
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        let namespace = store.namespace("radio", utc_fixture::SCHEMA).unwrap();
        namespace
            .try_log(utc_fixture::events::Tick { value: 1 })
            .unwrap();
        store.sync().unwrap();
    }
    let reader = Arc::new(Reader::open_dump([dir.path()], &[utc_fixture::SCHEMA]).unwrap());
    let from = dduroc::chrono::Utc::now() - dduroc::chrono::Duration::days(1);
    let to = from + dduroc::chrono::Duration::days(2);
    let native = reader.query(&Query::new().time_window(from, to)).unwrap();
    assert!(native.entries.is_empty());
    assert_eq!(native.unanchored.len(), 1);
    let result = read(&graph(reader), &from.to_rfc3339(), &to.to_rfc3339()).await;
    assert!(result["read"]["entries"].as_array().unwrap().is_empty());
    assert_eq!(result["read"]["unanchored"].as_array().unwrap().len(), 1);
}
