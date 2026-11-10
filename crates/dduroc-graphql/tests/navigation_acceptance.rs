//! Independent fixed-snapshot navigation and presentation acceptance.
use async_graphql::{Request, Response, Variables};
use dduroc::StoreConfig;
use dduroc::prelude::*;
use serde_json::{Value, json};
use std::sync::Arc;

mod bilingual {
    dduroc::schema! {
        name: navigation_fixture, version: 1, languages: [en, ru],
        events { Tick = 1 { level: Info, en: "tick {value}", ru: "отсчёт {value}", value: u32 }, }
    }
}
fn ok(response: Response) -> Value {
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()
}
fn code(response: &Response) -> &str {
    match response
        .errors
        .first()
        .unwrap()
        .extensions
        .as_ref()
        .unwrap()
        .get("code")
    {
        Some(async_graphql::Value::String(code)) => code,
        other => panic!("missing error code: {other:?}"),
    }
}
fn request(document: &str, variables: Value) -> Request {
    Request::new(document).variables(Variables::from_json(variables))
}
fn app(store: &Arc<Store>) -> dduroc_graphql::LogSchema {
    dduroc_graphql::build_schema(
        dduroc_graphql::Service::new(Arc::new(store.reader()), Default::default()).unwrap(),
        Default::default(),
    )
    .unwrap()
}
async fn open(schema: &dduroc_graphql::LogSchema, order: &str) -> (String, String) {
    let data = ok(schema
        .execute(format!(
            "mutation{{openReadSession(query:{{order:{order}}}){{id revision}}}}"
        ))
        .await);
    (
        data["openReadSession"]["id"].as_str().unwrap().into(),
        data["openReadSession"]["revision"].as_str().unwrap().into(),
    )
}
const SEEK: &str = "mutation($id:String!,$revision:UInt64!,$target:UInt64!,$records:UInt64!,$presentation:PresentationInput){seekReadSession(id:$id,expectedRevision:$revision,targetPage:$target,page:{maxRecords:$records,maxBytes:\"65536\"},presentation:$presentation){revision reachedPage targetReached exhausted page{entries{namespace at{boot micros} text{text} message{rendered language languagePreferenceIndex}} seeds{namespace} exhausted truncated}}}";
async fn seek(
    schema: &dduroc_graphql::LogSchema,
    id: &str,
    revision: &str,
    target: u64,
    records: u64,
    language: &str,
) -> Response {
    schema.execute(request(SEEK, json!({"id":id,"revision":revision,"target":target.to_string(),"records":records.to_string(),"presentation":{"languages":[language]}}))).await
}
async fn reach(
    schema: &dduroc_graphql::LogSchema,
    id: &str,
    revision: &mut String,
    target: u64,
    language: &str,
) -> Value {
    for _ in 0..100 {
        let data = ok(seek(schema, id, revision, target, 2, language).await);
        let navigation = &data["seekReadSession"];
        *revision = navigation["revision"].as_str().unwrap().into();
        if navigation["targetReached"] == true || navigation["exhausted"] == true {
            return navigation.clone();
        }
        assert!(
            navigation["page"].is_null(),
            "progress must not publish an intermediate target page"
        );
    }
    panic!("navigation failed to make bounded progress");
}
fn texts(value: &Value) -> Vec<String> {
    value["page"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            entry["message"]["rendered"]
                .as_str()
                .or_else(|| entry["text"]["text"].as_str())
                .unwrap()
                .into()
        })
        .collect()
}

#[tokio::test]
async fn direct_navigation_replays_original_snapshot_after_appends_in_both_orders() {
    for order in ["OLDEST", "NEWEST"] {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(directory.path()).with_budget_per_class(16 << 20))
            .unwrap();
        let ns = store
            .namespace("original", bilingual::navigation_fixture::SCHEMA)
            .unwrap();
        for value in 0..60 {
            ns.try_log(bilingual::navigation_fixture::events::Tick { value })
                .unwrap();
        }
        store.sync().unwrap();
        let schema = app(&store);
        let (id, mut revision) = open(&schema, order).await;
        let first = reach(&schema, &id, &mut revision, 1, "en").await;
        ns.try_log(bilingual::navigation_fixture::events::Tick { value: 999 })
            .unwrap();
        let later = store
            .namespace("added-after-open", bilingual::navigation_fixture::SCHEMA)
            .unwrap();
        later
            .try_log(bilingual::navigation_fixture::events::Tick { value: 1000 })
            .unwrap();
        store.sync().unwrap();
        let progress = ok(seek(&schema, &id, &revision, 25, 2, "en").await);
        let progress = &progress["seekReadSession"];
        revision = progress["revision"].as_str().unwrap().into();
        assert_eq!(
            progress["targetReached"], false,
            "far seek must return bounded progress"
        );
        assert!(progress["page"].is_null());
        let reached = reach(&schema, &id, &mut revision, 25, "en").await;
        assert_eq!(reached["reachedPage"], "25");
        assert_eq!(
            texts(&reached),
            if order == "OLDEST" {
                vec!["tick 48", "tick 49"]
            } else {
                vec!["tick 11", "tick 10"]
            }
        );
        let back = reach(&schema, &id, &mut revision, 1, "en").await;
        assert_eq!(back["page"], first["page"]);
        let end = reach(&schema, &id, &mut revision, 100, "en").await;
        assert_eq!(end["reachedPage"], "30");
        assert_eq!(end["targetReached"], false);
        assert_eq!(end["exhausted"], true);
        assert_eq!(
            texts(&end),
            if order == "OLDEST" {
                vec!["tick 58", "tick 59"]
            } else {
                vec!["tick 1", "tick 0"]
            }
        );
        assert_eq!(
            reach(&schema, &id, &mut revision, 1, "en").await["page"],
            first["page"]
        );
    }
}

#[tokio::test]
async fn language_representations_share_cas_and_keep_raw_text_and_page_identity() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let ns = store
        .namespace("original", bilingual::navigation_fixture::SCHEMA)
        .unwrap();
    ns.try_log(bilingual::navigation_fixture::events::Tick { value: 7 })
        .unwrap();
    ns.try_log_text(Level::Info, "literal", "raw Русский / English", None)
        .unwrap();
    store.sync().unwrap();
    let schema = app(&store);
    let (id, mut revision) = open(&schema, "OLDEST").await;
    let english = reach(&schema, &id, &mut revision, 1, "en").await;
    let stale = revision.clone();
    let russian = reach(&schema, &id, &mut revision, 1, "ru").await;
    assert_eq!(texts(&russian), vec!["отсчёт 7", "raw Русский / English"]);
    assert_eq!(
        russian["page"]["entries"][0]["at"],
        english["page"]["entries"][0]["at"]
    );
    assert_eq!(russian["page"]["entries"][0]["message"]["language"], "ru");
    assert_eq!(
        code(&seek(&schema, &id, &stale, 1, 2, "en").await),
        "REVISION_CONFLICT"
    );
    assert_eq!(
        reach(&schema, &id, &mut revision, 1, "en").await["page"],
        english["page"]
    );
    assert_eq!(
        code(&seek(&schema, &id, &revision, 1, 3, "en").await),
        "INVALID_INPUT"
    );
}

#[test]
fn native_rewind_retention_error_remains_terminal_on_every_retry() {
    use dduroc::read::{Cancellation, NsSelect, Query, ReadError, ReadLimits};
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
    let victim = store
        .namespace("victim", bilingual::navigation_fixture::SCHEMA)
        .unwrap();
    let pressure = store
        .namespace("pressure", bilingual::navigation_fixture::SCHEMA)
        .unwrap();
    for _ in 0..2 {
        victim
            .try_log_text(Level::Info, "fixture", "x".repeat(40 << 10), None)
            .unwrap();
        victim.sync().unwrap();
    }
    let reader = Arc::new(store.reader());
    let mut session = reader
        .open_session(
            Query::new().namespaces(NsSelect::Names(vec!["victim".into()])),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    let end = session
        .seek_page(2, dduroc::read::PageLimits::new(1, 65536).unwrap(), 8)
        .unwrap();
    assert_eq!(end.reached_page, 2);
    assert!(end.exhausted);
    for _ in 0..5 {
        pressure
            .try_log_text(Level::Info, "fixture", "y".repeat(40 << 10), None)
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
    for _ in 0..3 {
        assert!(matches!(
            session.seek_page(1, dduroc::read::PageLimits::new(1, 65536).unwrap(), 8),
            Err(ReadError::RetentionGap)
        ));
        assert!(
            session
                .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
                .is_err()
        );
    }
}

#[tokio::test]
async fn exact_full_pages_have_no_phantom_empty_page_and_invalid_modes_keep_revision() {
    for count in [64, 128] {
        let directory = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(directory.path())).unwrap();
        let ns = store
            .namespace("exact", bilingual::navigation_fixture::SCHEMA)
            .unwrap();
        for value in 0..count {
            ns.try_log(bilingual::navigation_fixture::events::Tick { value })
                .unwrap();
        }
        store.sync().unwrap();
        let schema = app(&store);
        let (id, mut revision) = open(&schema, "OLDEST").await;
        let first = ok(seek(&schema, &id, &revision, 1, 64, "en").await);
        revision = first["seekReadSession"]["revision"]
            .as_str()
            .unwrap()
            .into();
        assert_eq!(
            first["seekReadSession"]["page"]["entries"]
                .as_array()
                .unwrap()
                .len(),
            64
        );
        let legacy = schema.execute(request(
            "mutation($id:String!,$revision:UInt64!){readSession(id:$id,expectedRevision:$revision){revision}}",
            json!({"id":id,"revision":revision}),
        )).await;
        assert_eq!(code(&legacy), "INVALID_INPUT");
        assert_eq!(
            code(&seek(&schema, &id, &revision, 1, 63, "en").await),
            "INVALID_INPUT"
        );
        let end = ok(seek(&schema, &id, &revision, 100, 64, "en").await);
        let end = &end["seekReadSession"];
        revision = end["revision"].as_str().unwrap().into();
        assert_eq!(end["reachedPage"], (count / 64).to_string());
        assert_eq!(end["exhausted"], true);
        assert_eq!(end["targetReached"], false);
        assert_eq!(end["page"]["entries"].as_array().unwrap().len(), 64);
        let back = ok(seek(&schema, &id, &revision, 1, 64, "en").await);
        assert_eq!(
            back["seekReadSession"]["page"],
            first["seekReadSession"]["page"]
        );
    }
}

#[tokio::test]
async fn indexed_too_small_page_closes_capability_without_reporting_eof() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    store
        .namespace("size", bilingual::navigation_fixture::SCHEMA)
        .unwrap()
        .try_log(bilingual::navigation_fixture::events::Tick { value: 1 })
        .unwrap();
    store.sync().unwrap();
    let schema = app(&store);
    let (id, revision) = open(&schema, "OLDEST").await;
    let too_small = schema.execute(request(
        "mutation($id:String!,$revision:UInt64!){seekReadSession(id:$id,expectedRevision:$revision,targetPage:\"1\",page:{maxRecords:\"64\",maxBytes:\"1\"}){revision reachedPage exhausted page{entries{namespace}}}}",
        json!({"id":id,"revision":revision}),
    )).await;
    assert_eq!(code(&too_small), "PAGE_TOO_SMALL");
    assert_eq!(
        code(&seek(&schema, &id, &revision, 1, 64, "en").await),
        "SESSION_NOT_FOUND"
    );
}

#[test]
fn same_page_representations_reuse_raw_page_but_rewind_keeps_cumulative_scan_budget() {
    use dduroc::read::{Cancellation, Query, ReadError, ReadLimits, ReadResource};
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let ns = store
        .namespace("bounded", bilingual::navigation_fixture::SCHEMA)
        .unwrap();
    for value in 0..8 {
        ns.try_log(bilingual::navigation_fixture::events::Tick { value })
            .unwrap();
    }
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let mut session = reader
        .open_session(
            Query::new(),
            ReadLimits {
                blocks: 1,
                ..ReadLimits::default()
            },
            Cancellation::new(),
        )
        .unwrap();
    let first = session
        .seek_page(1, dduroc::read::PageLimits::new(2, 65536).unwrap(), 1)
        .unwrap();
    for _ in 0..20 {
        let represented = session
            .seek_page(1, dduroc::read::PageLimits::new(2, 65536).unwrap(), 1)
            .unwrap();
        assert_eq!(
            represented.page.unwrap().entries[0].at,
            first.page.as_ref().unwrap().entries[0].at
        );
    }
    session
        .seek_page(2, dduroc::read::PageLimits::new(2, 65536).unwrap(), 1)
        .unwrap();
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(2, 65536).unwrap(), 1),
        Err(ReadError::LimitExceeded(ReadResource::Blocks))
    ));
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(2, 65536).unwrap(), 1),
        Err(ReadError::LimitExceeded(ReadResource::Blocks))
    ));
}

#[test]
fn native_indexed_too_small_page_stays_terminal_even_with_larger_dimensions() {
    use dduroc::read::{Cancellation, Query, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    store
        .namespace("size", bilingual::navigation_fixture::SCHEMA)
        .unwrap()
        .try_log(bilingual::navigation_fixture::events::Tick { value: 1 })
        .unwrap();
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let mut session = reader
        .open_session(Query::new(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(1, 1).unwrap(), 8),
        Err(ReadError::PageTooSmall)
    ));
    assert!(matches!(
        session.seek_page(1, dduroc::read::PageLimits::new(1, 65536).unwrap(), 8),
        Err(ReadError::PageTooSmall)
    ));
    assert!(matches!(
        session.seek_page(2, dduroc::read::PageLimits::new(1, 1).unwrap(), 8),
        Err(ReadError::PageTooSmall)
    ));
}

#[tokio::test]
async fn filtered_tail_after_full_page_does_not_create_empty_last_page() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let ns = store
        .namespace("filtered", bilingual::navigation_fixture::SCHEMA)
        .unwrap();
    for _ in 0..64 {
        ns.try_log_text(
            Level::Info,
            "fixture",
            "matching",
            Some(dduroc::SpanId(123)),
        )
        .unwrap();
    }
    for _ in 0..10 {
        ns.try_log_text(Level::Info, "fixture", "excluded tail", None)
            .unwrap();
    }
    store.sync().unwrap();
    let schema = app(&store);
    let opened = ok(schema.execute("mutation{openReadSession(query:{order:OLDEST,filter:{spanIds:[\"123\"]}}){id revision}}").await);
    let id = opened["openReadSession"]["id"].as_str().unwrap();
    let revision = opened["openReadSession"]["revision"].as_str().unwrap();
    let first = ok(seek(&schema, id, revision, 1, 64, "en").await);
    let revision = first["seekReadSession"]["revision"].as_str().unwrap();
    assert_eq!(
        first["seekReadSession"]["page"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        64
    );
    let end = ok(seek(&schema, id, revision, 2, 64, "en").await);
    assert_eq!(end["seekReadSession"]["reachedPage"], "1");
    assert_eq!(end["seekReadSession"]["exhausted"], true);
    assert_eq!(end["seekReadSession"]["targetReached"], false);
    assert_eq!(
        end["seekReadSession"]["page"]["entries"],
        first["seekReadSession"]["page"]["entries"]
    );
}

#[test]
fn repeated_cached_native_output_respects_cumulative_returned_byte_limit() {
    use dduroc::read::{
        Cancellation, Entry, EntryKind, Query, ReadError, ReadLimits, ReadResource,
    };
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    store
        .namespace("output", bilingual::navigation_fixture::SCHEMA)
        .unwrap()
        .try_log(bilingual::navigation_fixture::events::Tick { value: 1 })
        .unwrap();
    store.sync().unwrap();
    let reader = Arc::new(store.reader());
    let mut probe = reader
        .open_session(Query::new(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    let first = probe
        .seek_page(1, dduroc::read::PageLimits::new(1, 65536).unwrap(), 1)
        .unwrap();
    let entry = &first.page.as_ref().unwrap().entries[0];
    let EntryKind::Message { payload, .. } = &entry.kind else {
        panic!("message fixture");
    };
    let one_output = (std::mem::size_of::<Entry>() + entry.namespace.len() + payload.len()) as u64;
    for target in [1, 100] {
        let mut session = reader
            .open_session(
                Query::new(),
                ReadLimits {
                    blocks: 1,
                    returned_bytes: one_output,
                    ..ReadLimits::default()
                },
                Cancellation::new(),
            )
            .unwrap();
        assert!(
            session
                .seek_page(1, dduroc::read::PageLimits::new(1, 65536).unwrap(), 1)
                .unwrap()
                .exhausted
        );
        assert!(
            matches!(
                session.seek_page(target, dduroc::read::PageLimits::new(1, 65536).unwrap(), 1),
                Err(ReadError::LimitExceeded(ReadResource::ReturnedBytes))
            ),
            "cached page output must consume the cumulative returned-byte allowance, including a beyond-EOF request"
        );
    }
}
