//! Independent acceptance of exact filtered totals and their frozen pages.
use async_graphql::{Request, Variables};
use dduroc::prelude::*;
use dduroc::read::{
    Cancellation, KindFilter, NsSelect, Order, Query, ReadError, ReadLimits, ReadResource, Reader,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Instant;

dduroc::schema! {
    name: counted_fixture, version: 1, languages: [en, ru],
    events {
        Tick = 1 { level: Info, tags: [rf], en: "tick {value}", ru: "отсчёт {value}", value: u32 },
        Fault = 2 { level: Error, tags: [fault], store: critical, en: "fault", ru: "авария" },
    }
    metrics { Mode = 1 { states: [Off = 0, On = 1], tags: [rf], store: telemetry } }
    spans { Work = 1, }
}
fn store(path: &std::path::Path) -> Arc<Store> {
    Store::open(StoreConfig::new(path).with_budget_per_class(16 << 20)).unwrap()
}
fn counted(
    reader: &Arc<Reader>,
    query: Query,
    records: usize,
    bytes: usize,
) -> (dduroc::read::QuerySession, dduroc::read::QueryTotals) {
    reader
        .open_counted_session(
            query,
            ReadLimits::default(),
            Cancellation::new(),
            dduroc::read::PageLimits::new(records, bytes).unwrap(),
        )
        .unwrap()
}
#[test]
fn exact_counts_apply_content_namespace_class_and_time_filters_in_both_orders() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    for name in ["orc-a", "orc-b", "apt-a"] {
        let ns = store.namespace(name, counted_fixture::SCHEMA).unwrap();
        for value in 0..3 {
            ns.try_log(counted_fixture::events::Tick { value }).unwrap();
        }
        ns.try_log(counted_fixture::events::Fault).unwrap();
        ns.try_log_text(Level::Warn, "test", "literal", None)
            .unwrap();
        ns.series(counted_fixture::metrics::Mode)
            .unwrap()
            .try_sample(counted_fixture::metrics::Mode::On)
            .unwrap();
        {
            let _span = ns.try_span(counted_fixture::spans::Work).unwrap();
        }
    }
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let cases = [
        (Query::new(), 24_u64),
        (
            Query::new().namespaces(NsSelect::Names(vec!["orc-a".into()])),
            8,
        ),
        (Query::new().namespaces(NsSelect::Group("orc-".into())), 16),
        (
            Query {
                channels: vec![dduroc::StorageClass::Critical],
                ..Query::new()
            },
            3,
        ),
        (Query::new().min_level(Level::Error), 12),
        (Query::new().any_tag("rf"), 12),
        (Query::new().event(dduroc::EventId(1)), 9),
        (Query::new().event_name("Fault"), 3),
        (Query::new().kinds(KindFilter::LOGS), 15),
        (Query::new().kinds(KindFilter::TELEMETRY), 3),
        (Query::new().kinds(KindFilter::SPANS), 6),
        (
            Query::new()
                .namespaces(NsSelect::Group("orc-".into()))
                .event_name("Tick")
                .any_tag("rf")
                .min_level(Level::Info),
            6,
        ),
        (
            Query::new().namespaces(NsSelect::Names(vec!["absent".into()])),
            0,
        ),
        (Query::new().boot(store.boot_counter()), 24),
        (Query::new().boot(store.boot_counter() + 1), 0),
    ];
    for (query, expected) in cases {
        for order in [Order::Oldest, Order::Newest] {
            let query = query.clone().order(order);
            let reference = reader.query(&query).unwrap();
            assert_eq!(
                reference.entries.len() as u64,
                expected,
                "fixture: {query:?}"
            );
            let (mut session, totals) = counted(&reader, query, 3, 65536);
            assert_eq!(
                (totals.total_matches, totals.total_seeds, totals.total_pages),
                (expected, 0, expected.div_ceil(3).max(1))
            );
            let mut actual = Vec::new();
            for target in 1..=totals.total_pages {
                let page = session
                    .seek_page(target, dduroc::read::PageLimits::new(3, 65536).unwrap(), 8)
                    .unwrap()
                    .page
                    .unwrap();
                actual.extend(page.entries.into_iter().map(|e| (e.namespace, e.at)));
            }
            assert_eq!(
                actual,
                reference
                    .entries
                    .into_iter()
                    .map(|e| (e.namespace, e.at))
                    .collect::<Vec<_>>()
            );
        }
    }
    assert!(store.record_sync(Utc::now(), SyncSource::User).unwrap());
    let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    let selected = &all.entries[5];
    for query in [
        Query::new().time_window(selected.at, selected.at),
        Query::new().time_window(selected.utc.unwrap(), selected.utc.unwrap()),
    ] {
        let expected = reader.query(&query).unwrap().entries.len() as u64;
        assert!(expected > 0);
        assert_eq!(counted(&reader, query, 2, 65536).1.total_matches, expected);
    }
}
#[test]
fn byte_limited_pages_and_seed_only_pages_are_counted_as_traversed() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("bytes", counted_fixture::SCHEMA).unwrap();
    for _ in 0..3 {
        ns.try_log_text(Level::Info, "test", "x".repeat(1000), None)
            .unwrap();
    }
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let (mut session, totals) = counted(&reader, Query::new(), 64, 2000);
    assert_eq!(
        (totals.total_matches, totals.total_seeds, totals.total_pages),
        (3, 0, 3)
    );
    for page in 1..=3 {
        assert_eq!(
            session
                .seek_page(page, dduroc::read::PageLimits::new(64, 2000).unwrap(), 8)
                .unwrap()
                .page
                .unwrap()
                .entries
                .len(),
            1
        );
    }
    assert!(
        session
            .seek_page(4, dduroc::read::PageLimits::new(64, 2000).unwrap(), 8)
            .unwrap()
            .exhausted
    );
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(64, 2001).unwrap(), 8),
        Err(ReadError::InvalidNavigation)
    ));
    for name in ["state-a", "state-b", "state-c"] {
        store
            .namespace(name, counted_fixture::SCHEMA)
            .unwrap()
            .series(counted_fixture::metrics::Mode)
            .unwrap()
            .try_sample(counted_fixture::metrics::Mode::On)
            .unwrap();
    }
    store.sync().unwrap();
    let all = reader.query(&Query::new()).unwrap();
    let after = all.entries.iter().map(|e| e.at).max().unwrap();
    let from = dduroc::BootTime::new(after.boot, dduroc::Micros(after.at.0 + 1));
    let (mut session, totals) = counted(
        &reader,
        Query::new().since(from).with_state_seed(),
        2,
        65536,
    );
    assert_eq!(
        (totals.total_matches, totals.total_seeds, totals.total_pages),
        (0, 3, 2)
    );
    let first = session
        .seek_page(1, dduroc::read::PageLimits::new(2, 65536).unwrap(), 8)
        .unwrap()
        .page
        .unwrap();
    let last = session
        .seek_page(2, dduroc::read::PageLimits::new(2, 65536).unwrap(), 8)
        .unwrap()
        .page
        .unwrap();
    assert_eq!((first.seeds.len(), last.seeds.len()), (2, 1));
    assert!(first.entries.is_empty() && last.entries.is_empty() && last.exhausted);
}
#[test]
fn empty_zero_and_truncation_never_publish_misleading_exact_totals() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let reader = Arc::new(store.reader());
    let (mut empty, totals) = counted(&reader, Query::new().limit(0), 64, 65536);
    assert_eq!((totals.total_matches, totals.total_pages), (0, 1));
    assert!(
        empty
            .seek_page(1, dduroc::read::PageLimits::new(64, 65536).unwrap(), 8)
            .unwrap()
            .exhausted
    );
    let ns = store.namespace("limit", counted_fixture::SCHEMA).unwrap();
    for value in 0..4 {
        ns.try_log(counted_fixture::events::Tick { value }).unwrap();
    }
    store.sync().unwrap();
    for limit in [0, 1, 3] {
        assert!(matches!(
            reader.open_counted_session(
                Query::new().limit(limit),
                ReadLimits::default(),
                Cancellation::new(),
                dduroc::read::PageLimits::new(2, 65536).unwrap()
            ),
            Err(ReadError::IncompleteCount)
        ));
    }
    for limit in [4, 5] {
        assert_eq!(
            counted(&reader, Query::new().limit(limit), 2, 65536)
                .1
                .total_matches,
            4
        );
    }
    assert!(matches!(
        reader.open_counted_session(
            Query::new().since(Utc::now()),
            ReadLimits::default(),
            Cancellation::new(),
            dduroc::read::PageLimits::new(2, 65536).unwrap()
        ),
        Err(ReadError::IncompleteCount)
    ));
}
#[test]
fn counting_consumes_scan_budget_but_not_output_budget_and_honors_cancellation() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("budget", counted_fixture::SCHEMA).unwrap();
    for value in 0..8 {
        ns.try_log(counted_fixture::events::Tick { value }).unwrap();
    }
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let (mut session, totals) = reader
        .open_counted_session(
            Query::new(),
            ReadLimits {
                returned_bytes: 0,
                ..ReadLimits::default()
            },
            Cancellation::new(),
            dduroc::read::PageLimits::new(2, 65536).unwrap(),
        )
        .unwrap();
    assert_eq!(totals.total_matches, 8);
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(2, 65536).unwrap(), 8),
        Err(ReadError::LimitExceeded(ReadResource::ReturnedBytes))
    ));
    for limits in [
        ReadLimits {
            examined_records: 3,
            ..ReadLimits::default()
        },
        ReadLimits {
            blocks: 0,
            ..ReadLimits::default()
        },
    ] {
        assert!(matches!(
            reader.open_counted_session(
                Query::new(),
                limits,
                Cancellation::new(),
                dduroc::read::PageLimits::new(2, 65536).unwrap()
            ),
            Err(ReadError::LimitExceeded(_))
        ));
    }
    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        reader.open_counted_session(
            Query::new(),
            ReadLimits::default(),
            cancelled,
            dduroc::read::PageLimits::new(2, 65536).unwrap()
        ),
        Err(ReadError::Cancelled)
    ));
    assert!(matches!(
        reader.open_counted_session(
            Query::new(),
            ReadLimits {
                deadline: Some(Instant::now()),
                ..ReadLimits::default()
            },
            Cancellation::new(),
            dduroc::read::PageLimits::new(2, 65536).unwrap()
        ),
        Err(ReadError::Deadline)
    ));
}
#[test]
fn unknown_schema_count_matches_existing_filter_semantics() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = store(directory.path());
        let ns = store.namespace("foreign", counted_fixture::SCHEMA).unwrap();
        ns.try_log(counted_fixture::events::Tick { value: 1 })
            .unwrap();
        ns.try_log_text(Level::Warn, "test", "literal", None)
            .unwrap();
        store.sync().unwrap();
        store.shutdown();
    }
    let reader = Arc::new(Reader::open_dump([directory.path()], &[]).unwrap());
    for query in [
        Query::new(),
        Query::new().event(dduroc::EventId(1)),
        Query::new().event_name("Tick"),
        Query::new().any_tag("rf"),
        Query::new().min_level(Level::Error),
    ] {
        let expected = reader.query(&query).unwrap().entries.len() as u64;
        assert_eq!(counted(&reader, query, 64, 65536).1.total_matches, expected);
    }
}
fn graph(reader: Arc<Reader>) -> dduroc_graphql::LogSchema {
    dduroc_graphql::build_schema(
        dduroc_graphql::Service::new(reader, Default::default()).unwrap(),
        Default::default(),
    )
    .unwrap()
}
async fn execute(schema: &dduroc_graphql::LogSchema, document: &str, variables: Value) -> Value {
    let response = schema
        .execute(Request::new(document).variables(Variables::from_json(variables)))
        .await;
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()
}
const OPEN: &str = "mutation($query:QueryInput!){openCountedReadSession(query:$query,page:{maxRecords:\"64\",maxBytes:\"65536\"}){id revision totalMatches totalSeeds totalPages}}";
const SEEK: &str = "mutation($id:String!,$revision:UInt64!,$target:UInt64!,$language:String!){seekReadSession(id:$id,expectedRevision:$revision,targetPage:$target,page:{maxRecords:\"64\",maxBytes:\"65536\"},presentation:{languages:[$language]}){revision reachedPage targetReached exhausted page{entries{namespace at{boot micros} utc message{rendered}} seeds{namespace} exhausted}}}";
#[tokio::test]
async fn default_count_and_all_79_pages_are_usable_after_5000_records() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store
        .namespace("original", counted_fixture::SCHEMA)
        .unwrap();
    for value in 0..5000 {
        ns.try_log(counted_fixture::events::Tick { value }).unwrap();
    }
    store.sync().unwrap();
    let schema = graph(Arc::new(store.reader()));
    let opened = execute(&schema, OPEN, json!({"query":{"order":"OLDEST"}})).await;
    let opened = &opened["openCountedReadSession"];
    assert_eq!(
        (
            opened["totalMatches"].as_str(),
            opened["totalSeeds"].as_str(),
            opened["totalPages"].as_str()
        ),
        (Some("5000"), Some("0"), Some("79"))
    );
    let id = opened["id"].as_str().unwrap();
    let mut revision = opened["revision"].as_str().unwrap().to_owned();
    ns.try_log(counted_fixture::events::Tick { value: 9000 })
        .unwrap();
    store
        .namespace("later", counted_fixture::SCHEMA)
        .unwrap()
        .try_log(counted_fixture::events::Tick { value: 9001 })
        .unwrap();
    store.sync().unwrap();
    assert!(store.record_sync(Utc::now(), SyncSource::User).unwrap());
    let mut seen = 0;
    for target in 1..=79 {
        let response = execute(
            &schema,
            SEEK,
            json!({"id":id,"revision":revision,"target":target.to_string(),"language":"en"}),
        )
        .await;
        let reached = &response["seekReadSession"];
        revision = reached["revision"].as_str().unwrap().into();
        assert_eq!(reached["targetReached"], true);
        let entries = reached["page"]["entries"].as_array().unwrap();
        assert_eq!(entries.len(), if target == 79 { 8 } else { 64 });
        assert!(
            entries
                .iter()
                .all(|e| e["namespace"] == "original" && e["utc"].is_null())
        );
        seen += entries.len();
    }
    assert_eq!(seen, 5000);
    let russian = execute(
        &schema,
        SEEK,
        json!({"id":id,"revision":revision,"target":"79","language":"ru"}),
    )
    .await;
    let reached = &russian["seekReadSession"];
    assert_eq!(
        reached["page"]["entries"][0]["message"]["rendered"],
        "отсчёт 4992"
    );
    assert_eq!(reached["exhausted"], true);
    let filtered = execute(&schema, OPEN, json!({"query":{"namespaces":["later"]}})).await;
    assert_eq!(filtered["openCountedReadSession"]["totalMatches"], "1");
}

mod old_read {
    dduroc::schema! {
        name: counted_fixture, version: 1, languages: [en],
        events { Tick = 1 { level: Info, en: "tick {value}", value: u32 } }
    }
}

mod future {
    dduroc::schema! {
        name: counted_fixture, version: 2, languages: [en],
        events { Tick = 1 { level: Info, en: "tick {value}", value: u32 } }
        history { 1 { events { Tick = 1 { value: u32 } } } }
        migrations { 1 => { v1::Tick: |old| events::Tick { value: old.value }, }, }
    }
}

#[test]
fn incompatible_blocks_cannot_be_reported_as_an_exact_empty_result() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = store(directory.path());
        store
            .namespace("future", future::counted_fixture::SCHEMA)
            .unwrap()
            .try_log(future::counted_fixture::events::Tick { value: 1 })
            .unwrap();
        store.sync().unwrap();
    }
    let reader = Arc::new(
        Reader::open_dump([directory.path()], &[old_read::counted_fixture::SCHEMA]).unwrap(),
    );
    let ordinary = reader.query(&Query::new()).unwrap();
    assert!(!ordinary.damaged.is_empty());
    assert!(ordinary.entries.is_empty());
    assert!(matches!(
        reader.open_counted_session(
            Query::new(),
            ReadLimits::default(),
            Cancellation::new(),
            dduroc::read::PageLimits::new(64, 65536).unwrap()
        ),
        Err(ReadError::IncompleteCount)
    ));
}

#[test]
fn counted_snapshot_reports_retention_loss_instead_of_revising_exact_totals() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path()).channel(
        dduroc::StorageClass::Default,
        dduroc::ChannelConfig {
            segment_bytes: 64 << 10,
            block_max_bytes: 8 << 10,
            compression: dduroc::Compression::None,
            ..dduroc::ChannelConfig::new(128 << 10)
        },
    ))
    .unwrap();
    let victim = store.namespace("victim", counted_fixture::SCHEMA).unwrap();
    for _ in 0..2 {
        victim
            .try_log_text(Level::Info, "test", "x".repeat(40 << 10), None)
            .unwrap();
        victim.sync().unwrap();
    }
    let reader = Arc::new(store.reader());
    let (mut session, totals) = counted(
        &reader,
        Query::new().namespaces(NsSelect::Names(vec!["victim".into()])),
        1,
        65536,
    );
    assert_eq!((totals.total_matches, totals.total_pages), (2, 2));
    let pressure = store
        .namespace("pressure", counted_fixture::SCHEMA)
        .unwrap();
    for _ in 0..5 {
        pressure
            .try_log_text(Level::Info, "test", "y".repeat(40 << 10), None)
            .unwrap();
        pressure.sync().unwrap();
    }
    assert!(
        store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .any(|row| row.key.namespace == "victim" && row.first_index.is_none())
    );
    for _ in 0..2 {
        assert!(matches!(
            session.seek_page(1, dduroc::read::PageLimits::new(1, 65536).unwrap(), 8),
            Err(ReadError::RetentionGap)
        ));
    }
}

#[test]
fn counted_seed_preparation_is_charged_exactly_once_before_first_output() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store
        .namespace("seed-budget", counted_fixture::SCHEMA)
        .unwrap();
    ns.series(counted_fixture::metrics::Mode)
        .unwrap()
        .try_sample(counted_fixture::metrics::Mode::On)
        .unwrap();
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let entry = reader.query(&Query::new()).unwrap().entries.remove(0);
    let query = Query::new()
        .since(dduroc::BootTime::new(
            entry.at.boot,
            dduroc::Micros(entry.at.at.0 + 1),
        ))
        .with_state_seed();
    assert!(matches!(
        reader.open_counted_session(
            query.clone(),
            ReadLimits {
                returned_bytes: 0,
                ..ReadLimits::default()
            },
            Cancellation::new(),
            dduroc::read::PageLimits::new(64, 65536).unwrap()
        ),
        Err(ReadError::LimitExceeded(ReadResource::ReturnedBytes))
    ));
    let one_seed = (std::mem::size_of::<dduroc::read::Entry>() + entry.namespace.len()) as u64;
    let (mut session, totals) = reader
        .open_counted_session(
            query,
            ReadLimits {
                returned_bytes: one_seed,
                ..ReadLimits::default()
            },
            Cancellation::new(),
            dduroc::read::PageLimits::new(64, 65536).unwrap(),
        )
        .unwrap();
    assert_eq!((totals.total_matches, totals.total_seeds), (0, 1));
    assert_eq!(
        session
            .seek_page(1, dduroc::read::PageLimits::new(64, 65536).unwrap(), 8)
            .unwrap()
            .page
            .unwrap()
            .seeds
            .len(),
        1
    );
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(64, 65536).unwrap(), 8),
        Err(ReadError::LimitExceeded(ReadResource::ReturnedBytes))
    ));
}
