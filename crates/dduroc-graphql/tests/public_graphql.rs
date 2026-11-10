//! Independent transport-neutral GraphQL acceptance through public execution.

use async_graphql::{Request, Response};
use dduroc::prelude::*;
use dduroc::{StoreConfig, serde};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;

// Public service return types must be nameable by downstream Rust callers.
#[test]
fn seek_result_types_are_public() {
    fn result_type(_: Option<dduroc_graphql::SessionSeekPage>) {}
    fn page_type(_: Option<dduroc_graphql::SessionSeekResult>) {}
    fn counted_totals(value: dduroc_graphql::CountedSessionOpened) -> dduroc::read::QueryTotals {
        value.totals
    }
    result_type(None);
    page_type(None);
    let _ = counted_totals;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Nested {
    label: String,
    values: Vec<Option<u128>>,
}

mod alpha {
    dduroc::schema! {
        name: alpha, version: 1, languages: [en],
        events {
            Wide = 1 { level: Info, en: "wide {unsigned}",
                unsigned: u64, signed: i64, unsigned128: u128, signed128: i128,
                float: f64, text: String, bytes: Vec<u8>, nested: crate::Nested },
            Tick = 2 { level: Info, en: "tick {value}", value: u32 },
        }
        metrics {
            Unsigned = 1 { type: u64, store: telemetry },
            Signed = 2 { type: i64, store: telemetry },
            Float = 3 { type: f64, store: telemetry },
            Blob = 4 { type: blob, store: telemetry },
            Mode = 5 { states: [Off = 0, On = 1], store: telemetry },
        }
    }
}
mod beta {
    dduroc::schema! {
        name: beta, version: 1, languages: [en],
        events { Wide = 1 { level: Error, en: "beta {text}", text: String }, }
    }
}
mod old_alpha {
    dduroc::schema! {
        name: alpha, version: 1, languages: [en],
        events { Tick = 2 { level: Info, en: "tick {value}", value: u32 }, }
    }
}
mod future_alpha {
    dduroc::schema! {
        name: alpha, version: 2, languages: [en],
        events { Tick = 2 { level: Info, en: "tick {value}", value: u32 }, }
        history { 1 { events { Tick = 2 { value: u32 } } } }
        migrations { 1 => { v1::Tick: |old| events::Tick { value: old.value }, }, }
    }
}

fn store(path: &std::path::Path) -> Arc<Store> {
    Store::open(StoreConfig::new(path).with_budget_per_class(16 << 20)).unwrap()
}

fn ok(response: Response) -> Value {
    assert!(
        response.errors.is_empty(),
        "GraphQL errors: {:?}",
        response.errors
    );
    response.data.into_json().unwrap()
}

fn error_code(response: &Response) -> &str {
    assert!(
        !response.errors.is_empty(),
        "expected GraphQL error, got {:?}",
        response.data
    );
    match response.errors[0].extensions.as_ref().unwrap().get("code") {
        Some(async_graphql::Value::String(code)) => code,
        other => panic!("missing string error code: {other:?}"),
    }
}

#[tokio::test]
async fn invalid_service_configuration_returns_errors_without_panicking() {
    use dduroc_graphql::{Service, ServiceConfig};
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let reader = Arc::new(store.reader());
    let invalid = [
        ServiceConfig {
            max_seek_pages_per_call: 0,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            max_jobs: usize::MAX,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            max_sessions: 0,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            max_subscriptions: ServiceConfig::default().max_jobs,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            subscription_queue_bytes: usize::MAX,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            subscription_queue_items: usize::MAX,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            idle_ttl: Duration::ZERO,
            ..ServiceConfig::default()
        },
        ServiceConfig {
            session_lifetime: Duration::MAX,
            ..ServiceConfig::default()
        },
    ];
    for config in invalid {
        assert!(Service::new(Arc::clone(&reader), config).is_err());
    }
    assert!(
        Service::new(
            reader,
            ServiceConfig {
                max_jobs: 1,
                max_subscriptions: 0,
                ..ServiceConfig::default()
            }
        )
        .is_ok()
    );
}

#[derive(Default)]
struct GateState {
    calls: usize,
    entered: bool,
    released: bool,
}
struct BlockingGate {
    state: std::sync::Mutex<GateState>,
    wake: std::sync::Condvar,
    block_on_call: usize,
}
impl BlockingGate {
    fn new(block_on_call: usize) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(GateState::default()),
            wake: std::sync::Condvar::new(),
            block_on_call,
        })
    }
    fn call(&self) {
        let mut state = self.state.lock().unwrap();
        state.calls += 1;
        if state.calls != self.block_on_call {
            return;
        }
        state.entered = true;
        self.wake.notify_all();
        while !state.released {
            state = self.wake.wait(state).unwrap();
        }
    }
    fn release(&self) {
        self.state.lock().unwrap().released = true;
        self.wake.notify_all();
    }
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.state.lock().unwrap().entered {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("native worker did not reach controlled fixture gate");
    }
}
struct ReleaseGate(Arc<BlockingGate>);
impl Drop for ReleaseGate {
    fn drop(&mut self) {
        self.0.release();
    }
}
static GATE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static NATIVE_GATE: std::sync::Mutex<Option<Arc<BlockingGate>>> = std::sync::Mutex::new(None);
fn block_native_migration() {
    let gate = NATIVE_GATE.lock().unwrap().clone();
    if let Some(gate) = gate {
        gate.call();
    }
}
mod blocking_alpha {
    dduroc::schema! {
        name: alpha, version: 2, languages: [en],
        events { Tick = 2 { level: Info, en: "tick {value}", value: u32 }, }
        history { 1 { events { Tick = 2 { value: u32 } } } }
        migrations { 1 => { v1::Tick: |old| {
            crate::block_native_migration();
            events::Tick { value: old.value }
        }, }, }
    }
}

fn blocked_fixture() -> (tempfile::TempDir, Arc<Store>, dduroc::Namespace) {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = store(directory.path());
        let namespace = store.namespace("blocked", alpha::alpha::SCHEMA).unwrap();
        for value in 0..3 {
            namespace
                .try_log(alpha::alpha::events::Tick { value })
                .unwrap();
            namespace.sync().unwrap();
        }
    }
    let store = store(directory.path());
    let namespace = store
        .namespace("blocked", blocking_alpha::alpha::SCHEMA)
        .unwrap();
    (directory, store, namespace)
}

fn app(
    reader: Arc<dduroc::read::Reader>,
    config: dduroc_graphql::ServiceConfig,
) -> dduroc_graphql::LogSchema {
    let service = dduroc_graphql::Service::new(reader, config).unwrap();
    dduroc_graphql::build_schema(service, dduroc_graphql::SchemaLimits::default()).unwrap()
}
fn request(document: &str, variables: Value) -> Request {
    Request::new(document).variables(async_graphql::Variables::from_json(variables))
}
async fn open_read(schema: &dduroc_graphql::LogSchema) -> (String, String) {
    let data = ok(schema
        .execute("mutation { openReadSession(query:{order:OLDEST}) { id revision } }")
        .await);
    (
        data["openReadSession"]["id"].as_str().unwrap().to_owned(),
        data["openReadSession"]["revision"]
            .as_str()
            .unwrap()
            .to_owned(),
    )
}
async fn page(
    schema: &dduroc_graphql::LogSchema,
    id: &str,
    revision: &str,
    records: usize,
    bytes: usize,
) -> Response {
    schema.execute(request(
        "mutation($id:String!,$revision:UInt64!,$records:UInt64!,$bytes:UInt64!){readSession(id:$id,expectedRevision:$revision,page:{maxRecords:$records,maxBytes:$bytes}){revision page{entries{namespace class at{boot micros} kind message{eventId name rendered}} seeds{namespace} exhausted truncated damaged{offset message} unanchored}}}",
        json!({"id":id,"revision":revision,"records":records.to_string(),"bytes":bytes.to_string()})
    )).await
}
async fn close_read(schema: &dduroc_graphql::LogSchema, id: &str, revision: &str) -> Response {
    schema.execute(request("mutation($id:String!,$revision:UInt64!){closeReadSession(id:$id,expectedRevision:$revision)}", json!({"id":id,"revision":revision}))).await
}

#[tokio::test]
async fn live_catalog_and_revisioned_pages_preserve_multiple_schemas() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let a = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    let b = store.namespace("b", beta::beta::SCHEMA).unwrap();
    for value in 0..5 {
        a.try_log(alpha::alpha::events::Tick { value }).unwrap();
    }
    b.try_log(beta::beta::events::Wide {
        text: "other schema".into(),
    })
    .unwrap();
    store.sync().unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let catalog = ok(schema.execute("{apiVersion catalog {apiVersion namespaces{namespace class schemaName protocolVersion retainedBytes} schemas{name version events{id name fields{name typeName}}}}}").await);
    assert_eq!(catalog["apiVersion"], "1");
    let names: std::collections::BTreeSet<_> = catalog["catalog"]["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, std::collections::BTreeSet::from(["alpha", "beta"]));
    let (id, mut revision) = open_read(&schema).await;
    let mut entries = Vec::new();
    for _ in 0..10 {
        let data = ok(page(&schema, &id, &revision, 2, 65536).await);
        let next_revision = data["readSession"]["revision"].as_str().unwrap().to_owned();
        assert_eq!(
            next_revision.parse::<u64>().unwrap(),
            revision.parse::<u64>().unwrap() + 1
        );
        let result = &data["readSession"]["page"];
        assert!(result["damaged"].as_array().unwrap().is_empty());
        entries.extend(result["entries"].as_array().unwrap().clone());
        revision = next_revision;
        if result["exhausted"] == true {
            break;
        }
    }
    assert_eq!(entries.len(), 6);
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry["namespace"] == "a")
            .count(),
        5
    );
    assert_eq!(
        entries
            .iter()
            .find(|entry| entry["namespace"] == "b")
            .unwrap()["message"]["rendered"],
        "beta other schema"
    );
    assert_eq!(
        error_code(&page(&schema, &id, "0", 2, 65536).await),
        "REVISION_CONFLICT"
    );
    assert_eq!(
        ok(close_read(&schema, &id, &revision).await)["closeReadSession"],
        true
    );
    assert_eq!(
        error_code(&page(&schema, &id, &revision, 2, 65536).await),
        "SESSION_NOT_FOUND"
    );
}

#[tokio::test]
async fn too_small_page_is_retryable_with_the_same_revision_and_no_record_loss() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.try_log(alpha::alpha::events::Tick { value: 7 }).unwrap();
    ns.sync().unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let (id, revision) = open_read(&schema).await;
    assert_eq!(
        error_code(&page(&schema, &id, &revision, 1, 1).await),
        "PAGE_TOO_SMALL"
    );
    let data = ok(page(&schema, &id, &revision, 1, 65536).await);
    assert_eq!(data["readSession"]["revision"], "1");
    assert_eq!(
        data["readSession"]["page"]["entries"][0]["message"]["rendered"],
        "tick 7"
    );
}

#[tokio::test]
async fn offline_reader_keeps_unknown_payloads_and_reports_incompatible_blocks_as_damage() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = store(directory.path());
        let ns = store
            .namespace("future", future_alpha::alpha::SCHEMA)
            .unwrap();
        ns.try_log(future_alpha::alpha::events::Tick { value: 99 })
            .unwrap();
        ns.sync().unwrap();
    }
    {
        let reader = Arc::new(dduroc::read::Reader::open_dump([directory.path()], &[]).unwrap());
        let schema = app(reader, dduroc_graphql::ServiceConfig::default());
        let result=ok(schema.execute("{read{entries{namespace message{fieldsStatus fields{kind} payload{base64 bytes}}} damaged{message} exhausted}}").await);
        let message = &result["read"]["entries"][0]["message"];
        assert_eq!(message["fieldsStatus"], "UNKNOWN_SCHEMA");
        assert!(message["fields"].is_null());
        assert!(!message["payload"]["base64"].as_str().unwrap().is_empty());
    }
    let reader = Arc::new(
        dduroc::read::Reader::open_dump([directory.path()], &[old_alpha::alpha::SCHEMA]).unwrap(),
    );
    let schema = app(reader, dduroc_graphql::ServiceConfig::default());
    let result = ok(schema
        .execute("{read{entries{namespace} damaged{offset message} exhausted}}")
        .await);
    assert!(result["read"]["entries"].as_array().unwrap().is_empty());
    assert!(!result["read"]["damaged"].as_array().unwrap().is_empty());
    assert!(
        result
            .to_string()
            .find(directory.path().to_str().unwrap())
            .is_none()
    );
}

#[tokio::test]
async fn query_seeds_unanchored_and_truncation_are_exposed_without_false_completeness() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.series(alpha::alpha::metrics::Mode)
        .unwrap()
        .sample(alpha::alpha::metrics::Mode::On);
    ns.sync().unwrap();
    let edge = store.now();
    for value in 0..3 {
        ns.try_log(alpha::alpha::events::Tick { value }).unwrap();
    }
    ns.sync().unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let result=ok(schema.execute(request(r#"query($boot:UInt64!,$micros:UInt64!){read(query:{from:{boot:$boot,micros:$micros},seedStates:true,order:OLDEST,limit:"1"}){entries{kind} seeds{sample{stateName}} exhausted truncated}}"#,json!({"boot":edge.boot.0.to_string(),"micros":(edge.at.0+1).to_string()}))).await);
    assert_eq!(result["read"]["seeds"][0]["sample"]["stateName"], "On");
    assert_eq!(result["read"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(result["read"]["truncated"], true);
    let result=ok(schema.execute(r#"{read(query:{from:{utc:"2020-01-01T00:00:00Z"}}){entries{namespace} unanchored exhausted}}"#).await);
    assert!(result["read"]["entries"].as_array().unwrap().is_empty());
    assert_eq!(
        result["read"]["unanchored"],
        json!([store.boot_counter().to_string()])
    );
}

#[tokio::test]
async fn widest_integers_nested_values_float_bits_and_blobs_are_lossless_on_the_wire() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("values", alpha::alpha::SCHEMA).unwrap();
    let nan_bits = 0x7ff8_0000_0000_0042u64;
    ns.try_log(alpha::alpha::events::Wide {
        unsigned: u64::MAX,
        signed: i64::MIN,
        unsigned128: u128::MAX,
        signed128: i128::MIN,
        float: f64::from_bits(nan_bits),
        text: "text with quotes \" and Unicode λ".into(),
        bytes: vec![0, 1, 254, 255],
        nested: Nested {
            label: "nested".into(),
            values: vec![Some(u128::MAX), None],
        },
    })
    .unwrap();
    ns.series(alpha::alpha::metrics::Unsigned)
        .unwrap()
        .sample(u64::MAX);
    ns.series(alpha::alpha::metrics::Signed)
        .unwrap()
        .sample(i64::MIN);
    let float = ns.series(alpha::alpha::metrics::Float).unwrap();
    for value in [
        f64::from_bits(nan_bits),
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.0,
    ] {
        float.sample(value);
    }
    ns.series(alpha::alpha::metrics::Blob)
        .unwrap()
        .sample(&[0u8, 1, 254, 255][..]);
    ns.sync().unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let data = ok(schema.execute(r#"{read(query:{order:OLDEST}){entries{kind message{fieldsStatus fields{kind pairs{key{kind text} value{kind text float{width class decimal bits} items{kind text} pairs{key{kind text} value{kind text items{kind text}}}}}}} sample{metricId value{kind text float{width class decimal bits} bytes{base64 bytes}}}}}}"#).await);
    let entries = data["read"]["entries"].as_array().unwrap();
    let message = entries
        .iter()
        .find(|entry| entry["kind"] == "MESSAGE")
        .unwrap();
    assert_eq!(message["message"]["fieldsStatus"], "DECODED");
    let pairs = message["message"]["fields"]["pairs"].as_array().unwrap();
    let field = |name: &str| -> &Value {
        &pairs
            .iter()
            .find(|pair| pair["key"]["text"] == name)
            .unwrap()["value"]
    };
    assert_eq!(field("unsigned")["text"], u64::MAX.to_string());
    assert_eq!(field("signed")["text"], i64::MIN.to_string());
    assert_eq!(field("unsigned128")["text"], u128::MAX.to_string());
    assert_eq!(field("signed128")["text"], i128::MIN.to_string());
    assert_eq!(field("float")["float"]["class"], "NAN");
    assert_eq!(field("float")["float"]["bits"], format!("{nan_bits:016x}"));
    assert!(field("nested").to_string().contains(&u128::MAX.to_string()));
    let samples: Vec<_> = entries
        .iter()
        .filter(|entry| entry["kind"] == "SAMPLE")
        .map(|entry| &entry["sample"])
        .collect();
    assert_eq!(
        samples
            .iter()
            .find(|sample| sample["metricId"] == "1")
            .unwrap()["value"]["text"],
        u64::MAX.to_string()
    );
    assert_eq!(
        samples
            .iter()
            .find(|sample| sample["metricId"] == "2")
            .unwrap()["value"]["text"],
        i64::MIN.to_string()
    );
    let floats: Vec<_> = samples
        .iter()
        .filter(|sample| sample["metricId"] == "3")
        .map(|sample| sample["value"]["float"]["bits"].as_str().unwrap())
        .collect();
    assert_eq!(
        floats,
        vec![
            "7ff8000000000042",
            "7ff0000000000000",
            "fff0000000000000",
            "8000000000000000"
        ]
    );
    let blob = &samples
        .iter()
        .find(|sample| sample["metricId"] == "4")
        .unwrap()["value"]["bytes"];
    assert_eq!(blob["base64"], "AAH+/w==");
    assert_eq!(blob["bytes"], "4");
}

#[tokio::test]
async fn malformed_inputs_are_rejected_and_native_scan_limits_have_typed_errors() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.try_log(alpha::alpha::events::Tick { value: 1 }).unwrap();
    ns.sync().unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    for query in [
        r#"{read(query:{boot:1}){exhausted}}"#,
        r#"{read(query:{boot:"01"}){exhausted}}"#,
        r#"{read(query:{boot:"-1"}){exhausted}}"#,
        r#"{read(query:{boot:"18446744073709551616"}){exhausted}}"#,
        r#"{read(query:{boot:"4294967296"}){exhausted}}"#,
        r#"{read(query:{namespaces:["a"],namespacePrefix:"a"}){exhausted}}"#,
        r#"{read(query:{from:{boot:"1"}}){exhausted}}"#,
        r#"{read(query:{from:{utc:"garbage"}}){exhausted}}"#,
        r#"{read(query:{from:{boot:"1",micros:"2"},to:{boot:"1",micros:"1"}}){exhausted}}"#,
        r#"{read(query:{filter:{eventIds:["65536"]}}){exhausted}}"#,
        r#"{read(page:{maxRecords:"0"}){exhausted}}"#,
    ] {
        assert!(
            !schema.execute(query).await.errors.is_empty(),
            "input was accepted: {query}"
        );
    }
    let response = schema
        .execute(r#"{read(limits:{blocks:"0"}){exhausted}}"#)
        .await;
    assert_eq!(error_code(&response), "READ_LIMIT_EXCEEDED");
    assert_eq!(
        response.errors[0]
            .extensions
            .as_ref()
            .unwrap()
            .get("resource"),
        Some(&async_graphql::Value::String("Blocks".into()))
    );
    assert_eq!(
        ok(schema.execute("{read{entries{namespace}}}").await)["read"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn unselected_expensive_fields_are_not_decoded_rendered_or_base64_encoded() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    // Valid journal framing with a deliberately invalid postcard event payload.
    ns.try_log_raw(dduroc::EventId(2), &[0xff], None).unwrap();
    ns.series(alpha::alpha::metrics::Blob)
        .unwrap()
        .sample(&vec![7u8; 1024][..]);
    ns.sync().unwrap();
    let mut config = dduroc_graphql::ServiceConfig::default();
    config.conversion_limits.max_blob_bytes = 8;
    let schema = app(Arc::new(store.reader()), config);
    let data=ok(schema.execute(r#"query { read { entries { ...Metadata } } } fragment Metadata on EntryView { namespace kind message { alias:eventId name level } sample { name metricId } }"#).await);
    assert_eq!(data["read"]["entries"].as_array().unwrap().len(), 2);
    let skipped = ok(schema.execute(request(r#"query($skip:Boolean!,$include:Boolean!){read{entries{namespace message{eventId fields @skip(if:$skip){kind} rendered @include(if:$include)} sample{metricId value @skip(if:true){bytes{base64}}}}}}"#, json!({"skip":true,"include":false}))).await);
    assert_eq!(skipped["read"]["entries"].as_array().unwrap().len(), 2);
    let data=ok(schema.execute(r#"{read(query:{filter:{kinds:{messages:true,text:false,samples:false,spans:false}}}){entries{message{payload{base64 bytes}}}}}"#).await);
    assert_eq!(
        data["read"]["entries"][0]["message"]["payload"]["base64"],
        "/w=="
    );
    for query in [
        r#"{read{entries{message{fields{kind}}}}}"#,
        r#"query {read{entries{message{...Requested}}}} fragment Requested on MessageView { rendered }"#,
        r#"{read{entries{sample{value{bytes{base64}}}}}}"#,
    ] {
        assert_eq!(
            error_code(&schema.execute(query).await),
            "CONVERSION_FAILED"
        );
    }
}

#[tokio::test]
async fn idle_ttl_cannot_be_revived_before_janitor_and_cleanup_needs_no_new_request() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let _ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    let config = dduroc_graphql::ServiceConfig {
        max_sessions: 1,
        idle_ttl: Duration::from_millis(10),
        ..Default::default()
    };
    let schema = app(Arc::new(store.reader()), config);
    let (id, revision) = open_read(&schema).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    let response = page(&schema, &id, &revision, 1, 65536).await;
    assert!(matches!(
        error_code(&response),
        "SESSION_EXPIRED" | "SESSION_NOT_FOUND"
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (_second, _) = open_read(&schema).await;
    // No operation addresses the second capability: the autonomous janitor
    // must return its capacity, without traffic driving cleanup.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (_third, _) = open_read(&schema).await;
}

async fn eventually_open(schema: &dduroc_graphql::LogSchema) -> (String, String) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let response = schema
                .execute("mutation{openReadSession(query:{order:OLDEST}){id revision}}")
                .await;
            if response.errors.is_empty() {
                let data = ok(response);
                return (
                    data["openReadSession"]["id"].as_str().unwrap().to_owned(),
                    data["openReadSession"]["revision"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            }
            assert_eq!(error_code(&response), "CAPACITY_EXCEEDED");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("completed native worker failed to return admission capacity")
}

#[tokio::test]
async fn pending_open_slots_and_actual_worker_permits_survive_abort_and_timeout() {
    let _exclusive_gate = GATE_TEST_LOCK.lock().await;
    for (jobs, sessions, timeout) in [(1, 2, false), (2, 1, false), (1, 2, true)] {
        let (_directory, store, _ns) = blocked_fixture();
        let gate = BlockingGate::new(1);
        let _release = ReleaseGate(Arc::clone(&gate));
        *NATIVE_GATE.lock().unwrap() = Some(Arc::clone(&gate));
        let config = dduroc_graphql::ServiceConfig {
            max_jobs: jobs,
            max_sessions: sessions,
            max_subscriptions: 0,
            operation_timeout: if timeout {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(5)
            },
            ..Default::default()
        };
        let schema = app(Arc::new(store.reader()), config);
        let worker_schema = schema.clone();
        let worker = tokio::spawn(async move {
            worker_schema
                .execute("mutation{openReadSession(query:{order:OLDEST}){id revision}}")
                .await
        });
        gate.entered().await;
        assert_eq!(
            ok(schema.execute("{apiVersion}").await)["apiVersion"],
            "1",
            "blocking native work must not occupy async executor"
        );
        assert_eq!(
            error_code(&schema.execute("mutation{openReadSession{id}}").await),
            "CAPACITY_EXCEEDED"
        );
        if timeout {
            assert_eq!(error_code(&worker.await.unwrap()), "DEADLINE_EXCEEDED");
        } else {
            worker.abort();
            assert!(worker.await.unwrap_err().is_cancelled());
        }
        // The request is gone, but the native callback is deliberately still
        // blocked: neither the job nor the pending-session lease may be reused.
        assert_eq!(
            error_code(&schema.execute("mutation{openReadSession{id}}").await),
            "CAPACITY_EXCEEDED"
        );
        gate.release();
        let (id, revision) = eventually_open(&schema).await;
        ok(close_read(&schema, &id, &revision).await);
        *NATIVE_GATE.lock().unwrap() = None;
    }
}

#[tokio::test]
async fn concurrent_page_is_busy_and_old_revision_cannot_read_or_close_advanced_session() {
    let _exclusive_gate = GATE_TEST_LOCK.lock().await;
    let (_directory, store, _ns) = blocked_fixture();
    let gate = BlockingGate::new(2);
    let _release = ReleaseGate(Arc::clone(&gate));
    // This fixture is independent of the blocked-open test's callback. Serialize
    // access to the global callback installation with the test gate below.
    *NATIVE_GATE.lock().unwrap() = Some(Arc::clone(&gate));
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let (id, revision) = open_read(&schema).await;
    let worker_schema = schema.clone();
    let worker_id = id.clone();
    let worker_revision = revision.clone();
    let worker =
        tokio::spawn(
            async move { page(&worker_schema, &worker_id, &worker_revision, 1, 65536).await },
        );
    gate.entered().await;
    assert_eq!(
        error_code(&page(&schema, &id, "999", 1, 65536).await),
        "SESSION_BUSY"
    );
    assert_eq!(
        error_code(&close_read(&schema, &id, &revision).await),
        "SESSION_BUSY"
    );
    gate.release();
    let data = ok(worker.await.unwrap());
    assert_eq!(data["readSession"]["revision"], "1");
    assert_eq!(
        error_code(&close_read(&schema, &id, "0").await),
        "REVISION_CONFLICT"
    );
    assert_eq!(
        error_code(&page(&schema, &id, "0", 1, 65536).await),
        "REVISION_CONFLICT"
    );
    ok(close_read(&schema, &id, "1").await);
    *NATIVE_GATE.lock().unwrap() = None;
}

async fn next_stream<S: futures_util::Stream<Item = Response> + Unpin>(stream: &mut S) -> Response {
    tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .expect("subscription did not produce its expected item")
        .expect("subscription ended early")
}

#[tokio::test]
async fn subscription_seeds_are_distinct_and_drop_returns_capacity_after_worker_drain() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.series(alpha::alpha::metrics::Mode)
        .unwrap()
        .sample(alpha::alpha::metrics::Mode::On);
    ns.sync().unwrap();
    let edge = store.now();
    ns.try_log(alpha::alpha::events::Tick { value: 7 }).unwrap();
    ns.sync().unwrap();
    let config = dduroc_graphql::ServiceConfig {
        max_subscriptions: 1,
        max_jobs: 2,
        ..Default::default()
    };
    let schema = app(Arc::new(store.reader()), config);
    let mut stream=Box::pin(schema.execute_stream(request("subscription($boot:UInt64!,$micros:UInt64!){follow(query:{from:{boot:$boot,micros:$micros},seedStates:true}){entries{message{rendered}} seeds{sample{stateName}} damaged{message} unanchored ended}}",json!({"boot":edge.boot.0.to_string(),"micros":(edge.at.0+1).to_string()}))));
    let first = ok(next_stream(&mut stream).await);
    assert_eq!(first["follow"]["seeds"][0]["sample"]["stateName"], "On");
    assert!(first["follow"]["entries"].as_array().unwrap().is_empty());
    let second = ok(next_stream(&mut stream).await);
    assert_eq!(
        second["follow"]["entries"][0]["message"]["rendered"],
        "tick 7"
    );
    assert!(second["follow"]["seeds"].as_array().unwrap().is_empty());
    let mut denied = Box::pin(schema.execute_stream("subscription{follow{ended}}"));
    assert_eq!(
        error_code(&next_stream(&mut denied).await),
        "CAPACITY_EXCEEDED"
    );
    drop(denied);
    drop(stream);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let mut retry =
                Box::pin(schema.execute_stream("subscription{follow{entries{kind} ended}}"));
            let result = next_stream(&mut retry).await;
            if result.errors.is_empty() {
                break;
            }
            assert_eq!(error_code(&result), "CAPACITY_EXCEEDED");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn maximum_default_subscriptions_leave_room_for_queries() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.try_log(alpha::alpha::events::Tick { value: 1 }).unwrap();
    ns.sync().unwrap();
    let config = dduroc_graphql::ServiceConfig::default();
    let count = config.max_subscriptions;
    let schema = app(Arc::new(store.reader()), config);
    let mut streams = Vec::new();
    for _ in 0..count {
        let mut stream =
            Box::pin(schema.execute_stream("subscription{follow{entries{kind} ended}}"));
        ok(next_stream(&mut stream).await);
        streams.push(stream);
    }
    assert_eq!(
        ok(schema.execute("{read{entries{namespace}}}").await)["read"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let mut over = Box::pin(schema.execute_stream("subscription{follow{ended}}"));
    assert_eq!(
        error_code(&next_stream(&mut over).await),
        "CAPACITY_EXCEEDED"
    );
    drop(streams);
}

#[tokio::test]
async fn slow_consumer_is_terminal_and_its_unpolled_queue_keeps_subscription_admission() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    for value in 0..20 {
        ns.try_log(alpha::alpha::events::Tick { value }).unwrap();
    }
    ns.sync().unwrap();
    let config = dduroc_graphql::ServiceConfig {
        max_jobs: 2,
        max_subscriptions: 1,
        subscription_queue_items: 1,
        slow_consumer_timeout: Duration::from_millis(30),
        ..Default::default()
    };
    let schema = app(Arc::new(store.reader()), config);
    let mut slow =
        Box::pin(schema.execute_stream("subscription{follow{entries{message{rendered}} ended}}"));
    ok(next_stream(&mut slow).await);
    tokio::time::sleep(Duration::from_millis(150)).await;
    // The native worker may already have stopped, but an undrained queued
    // response remains resident and must still consume the subscription slot.
    let mut over = Box::pin(schema.execute_stream("subscription{follow{ended}}"));
    assert_eq!(
        error_code(&next_stream(&mut over).await),
        "CAPACITY_EXCEEDED"
    );
    drop(over);
    let mut queued = 0;
    let mut terminal = false;
    for _ in 0..4 {
        let result = next_stream(&mut slow).await;
        if result.errors.is_empty() {
            queued += 1;
        } else {
            assert_eq!(error_code(&result), "SLOW_CONSUMER");
            terminal = true;
            break;
        }
    }
    assert!(terminal);
    assert!(
        queued <= 1,
        "producer accumulated more than the configured queue"
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(1), slow.next())
            .await
            .unwrap()
            .is_none()
    );
    drop(slow);
    let mut renewed = Box::pin(schema.execute_stream("subscription{follow{entries{kind}}}"));
    ok(next_stream(&mut renewed).await);
}

#[tokio::test]
async fn operation_caps_cover_documents_variables_aliases_and_nested_list_expansion() {
    use dduroc_graphql::{SchemaLimits, Service, ServiceConfig, build_schema};
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.try_log(alpha::alpha::events::Wide {
        unsigned: 1,
        signed: 1,
        unsigned128: 1,
        signed128: 1,
        float: 0.0,
        text: "x".repeat(100),
        bytes: vec![],
        nested: Nested {
            label: "nested".into(),
            values: vec![Some(7); 64],
        },
    })
    .unwrap();
    ns.sync().unwrap();
    let service = Service::new(Arc::new(store.reader()), ServiceConfig::default()).unwrap();
    let schema = build_schema(
        service.clone(),
        SchemaLimits {
            max_document_bytes: 64,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(
            &schema
                .execute(format!("{}{{apiVersion}}", " ".repeat(100)))
                .await
        ),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let schema = build_schema(
        service.clone(),
        SchemaLimits {
            max_variable_bytes: 32,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(
            &schema
                .execute(request("{apiVersion}", json!({"unused":"x".repeat(100)})))
                .await
        ),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let schema = build_schema(
        service.clone(),
        SchemaLimits {
            max_variable_nodes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(
            &schema
                .execute(request("{apiVersion}", json!({"unused":[1,2,3,4,5]})))
                .await
        ),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let schema = build_schema(
        service.clone(),
        SchemaLimits {
            max_aliases: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(&schema.execute("{a:apiVersion b:apiVersion}").await),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let conversion = service.config().conversion_limits.max_output_bytes;
    let schema = build_schema(
        service.clone(),
        SchemaLimits {
            max_conversion_bytes: conversion,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(
            &schema
                .execute("{a:read{exhausted} b:read{exhausted}}")
                .await
        ),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let schema = build_schema(
        service,
        SchemaLimits {
            max_resolved_fields: 100,
            ..Default::default()
        },
    )
    .unwrap();
    let amplified = r#"{read{entries{message{fields{pairs{key{text} value{a:pairs{key{text} value{items{a:text b:text c:kind}}} b:pairs{key{text} value{items{a:text b:text c:kind}}}}}}}}}}"#;
    assert_eq!(
        error_code(&schema.execute(amplified).await),
        "OPERATION_LIMIT_EXCEEDED"
    );
    assert_eq!(ok(schema.execute("{apiVersion}").await)["apiVersion"], "1");
    assert_eq!(
        ok(schema.execute("{read{entries{namespace}}}").await)["read"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn whole_operation_failure_retires_previously_opened_capabilities() {
    use dduroc_graphql::{SchemaLimits, Service, ServiceConfig, build_schema};
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let _ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    let service = Service::new(
        Arc::new(store.reader()),
        ServiceConfig {
            max_sessions: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let normal = build_schema(service.clone(), SchemaLimits::default()).unwrap();
    let roots = build_schema(
        service.clone(),
        SchemaLimits {
            max_root_calls: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(
            &roots
                .execute("mutation{first:openReadSession{id} second:openReadSession{id}}")
                .await
        ),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let (id, revision) = open_read(&normal).await;
    ok(close_read(&normal, &id, &revision).await);
    let scalar = build_schema(
        service,
        SchemaLimits {
            max_response_bytes: 200,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(
            &scalar
                .execute("mutation{openReadSession{id revision}}")
                .await
        ),
        "OPERATION_LIMIT_EXCEEDED"
    );
    let (id, revision) = open_read(&normal).await;
    ok(close_read(&normal, &id, &revision).await);
}

#[tokio::test]
async fn abort_after_an_earlier_mutation_succeeded_retires_its_undelivered_capability() {
    let _exclusive_gate = GATE_TEST_LOCK.lock().await;
    let (_directory, store, _ns) = blocked_fixture();
    let gate = BlockingGate::new(1);
    let _release = ReleaseGate(Arc::clone(&gate));
    *NATIVE_GATE.lock().unwrap() = Some(Arc::clone(&gate));
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig {
            max_sessions: 2,
            max_jobs: 2,
            max_subscriptions: 0,
            ..Default::default()
        },
    );
    let worker_schema = schema.clone();
    let worker = tokio::spawn(async move {
        worker_schema.execute(r#"mutation{first:openReadSession(query:{namespaces:["absent"]}){id} second:openReadSession(query:{namespaces:["blocked"]}){id}}"#).await
    });
    gate.entered().await;
    worker.abort();
    assert!(worker.await.unwrap_err().is_cancelled());
    // One slot is still held by the blocked native worker. The first published
    // but undelivered capability must have released the other slot immediately.
    let data = ok(schema
        .execute(r#"mutation{openReadSession(query:{namespaces:["absent"]}){id revision}}"#)
        .await);
    let id = data["openReadSession"]["id"].as_str().unwrap();
    let revision = data["openReadSession"]["revision"].as_str().unwrap();
    ok(close_read(&schema, id, revision).await);
    gate.release();
    *NATIVE_GATE.lock().unwrap() = None;
}

static UNIT_DECODES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct CountedUnit;
impl<'de> serde::Deserialize<'de> for CountedUnit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <() as serde::Deserialize>::deserialize(deserializer)?;
        UNIT_DECODES.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Self)
    }
}
mod counted {
    dduroc::schema! {
        name: counted,version:1,languages:[en],
        events { Units=1{level:Info,en:"units",units:Vec<crate::CountedUnit>}, }
    }
}
#[tokio::test]
async fn field_node_limit_stops_deserialization_before_zero_sized_sequence_expansion() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", counted::counted::SCHEMA).unwrap();
    ns.try_log(counted::counted::events::Units {
        units: vec![CountedUnit; 2000],
    })
    .unwrap();
    ns.sync().unwrap();
    let mut config = dduroc_graphql::ServiceConfig::default();
    config.conversion_limits.max_value_nodes = 16;
    let schema = app(Arc::new(store.reader()), config);
    UNIT_DECODES.store(0, std::sync::atomic::Ordering::SeqCst);
    ok(schema
        .execute("{read{entries{namespace message{eventId name}}}}")
        .await);
    assert_eq!(
        UNIT_DECODES.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "metadata projection invoked the event decoder"
    );
    let response = schema
        .execute("{read{entries{message{fields{kind}}}}}")
        .await;
    assert_eq!(error_code(&response), "CONVERSION_FAILED");
    assert!(
        UNIT_DECODES.load(std::sync::atomic::Ordering::SeqCst) < 32,
        "decoder traversed the whole sequence before applying its node budget"
    );
}

#[tokio::test]
async fn rewrite_expiry_is_a_typed_terminal_error_and_is_not_normal_eof() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = store(directory.path());
        let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
        ns.try_log(alpha::alpha::events::Tick { value: 7 }).unwrap();
        ns.sync().unwrap();
    }
    let store = store(directory.path());
    let ns = store.namespace("a", future_alpha::alpha::SCHEMA).unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let (id, revision) = open_read(&schema).await;
    ns.migrate().unwrap();
    assert_eq!(
        error_code(&page(&schema, &id, &revision, 10, 65536).await),
        "SNAPSHOT_EXPIRED"
    );
    assert_eq!(
        error_code(&page(&schema, &id, &revision, 10, 65536).await),
        "SESSION_NOT_FOUND"
    );
    let (id, revision) = open_read(&schema).await;
    let data = ok(page(&schema, &id, &revision, 10, 65536).await);
    assert_eq!(data["readSession"]["page"]["exhausted"], true);
    let revision = data["readSession"]["revision"].as_str().unwrap();
    let data = ok(page(&schema, &id, revision, 10, 65536).await);
    assert_eq!(data["readSession"]["page"]["exhausted"], true);
    assert!(
        data["readSession"]["page"]["entries"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn retention_gap_is_a_typed_error_instead_of_an_empty_successful_page() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(directory.path())
            .with_budget_per_class(16 << 20)
            .channel(
                dduroc::StorageClass::Default,
                dduroc::ChannelConfig {
                    segment_bytes: 64 << 10,
                    block_max_bytes: 8 << 10,
                    compression: dduroc::Compression::None,
                    ..dduroc::ChannelConfig::new(128 << 10)
                },
            ),
    )
    .unwrap();
    let victim = store.namespace("victim", alpha::alpha::SCHEMA).unwrap();
    let pressure = store.namespace("pressure", alpha::alpha::SCHEMA).unwrap();
    victim
        .try_log_text(dduroc::Level::Info, "fixture", "x".repeat(40 << 10), None)
        .unwrap();
    victim.sync().unwrap();
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let data = ok(schema
        .execute(r#"mutation{openReadSession(query:{namespaces:["victim"]}){id revision}}"#)
        .await);
    let id = data["openReadSession"]["id"].as_str().unwrap();
    let revision = data["openReadSession"]["revision"].as_str().unwrap();
    for _ in 0..5 {
        pressure
            .try_log_text(dduroc::Level::Info, "fixture", "y".repeat(40 << 10), None)
            .unwrap();
        pressure.sync().unwrap();
    }
    assert!(
        store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .any(|row| row.key.namespace == "victim"
                && row.key.class == dduroc::StorageClass::Default
                && row.first_index.is_none())
    );
    assert_eq!(
        error_code(&page(&schema, id, revision, 10, 65536).await),
        "RETENTION_GAP"
    );
}

#[tokio::test]
async fn explicit_cancel_closes_a_busy_session_without_waiting_for_blocking_io() {
    let _exclusive_gate = GATE_TEST_LOCK.lock().await;
    let (_directory, store, _ns) = blocked_fixture();
    let gate = BlockingGate::new(2);
    let _release = ReleaseGate(Arc::clone(&gate));
    *NATIVE_GATE.lock().unwrap() = Some(Arc::clone(&gate));
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let (id, revision) = open_read(&schema).await;
    let worker_schema = schema.clone();
    let worker_id = id.clone();
    let worker =
        tokio::spawn(async move { page(&worker_schema, &worker_id, &revision, 1, 65536).await });
    gate.entered().await;
    let response = tokio::time::timeout(
        Duration::from_secs(1),
        schema.execute(request(
            "mutation($id:String!){cancelReadSession(id:$id)}",
            json!({"id":id}),
        )),
    )
    .await
    .unwrap();
    assert_eq!(ok(response)["cancelReadSession"], true);
    assert_eq!(
        error_code(&page(&schema, &id, "0", 1, 65536).await),
        "SESSION_NOT_FOUND"
    );
    gate.release();
    let response = worker.await.unwrap();
    assert!(matches!(
        error_code(&response),
        "SESSION_EXPIRED" | "CANCELLED"
    ));
    *NATIVE_GATE.lock().unwrap() = None;
}

static LEGACY_CALLBACK_USED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
fn legacy_render(_: &[u8], _: usize) -> Result<String, dduroc::DecodeError> {
    LEGACY_CALLBACK_USED.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok("unbounded legacy rendering".into())
}
fn legacy_json(_: &[u8]) -> Result<String, dduroc::DecodeError> {
    LEGACY_CALLBACK_USED.store(true, std::sync::atomic::Ordering::SeqCst);
    Ok("{}".into())
}
#[tokio::test]
async fn manual_schema_reports_unsupported_bounded_decoders_without_calling_legacy_callbacks() {
    let mut descriptor = *alpha::alpha::SCHEMA.event(dduroc::EventId(2)).unwrap();
    descriptor.decoders = Some(dduroc::EventDecoders {
        render: legacy_render,
        json: legacy_json,
        fields: None,
        render_bounded: None,
    });
    let mut manual = alpha::alpha::SCHEMA;
    manual.name = "manual";
    manual.identity = None;
    manual.events = Box::leak(vec![descriptor].into_boxed_slice());
    manual.metrics = &[];
    manual.spans = &[];
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", manual).unwrap();
    let payload = dduroc::postcard::to_allocvec(&alpha::alpha::events::Tick { value: 7 }).unwrap();
    ns.try_log_raw(dduroc::EventId(2), &payload, None).unwrap();
    ns.sync().unwrap();
    LEGACY_CALLBACK_USED.store(false, std::sync::atomic::Ordering::SeqCst);
    let schema = app(
        Arc::new(store.reader()),
        dduroc_graphql::ServiceConfig::default(),
    );
    let data=ok(schema.execute("{read{entries{message{fieldsStatus fields{kind} renderStatus rendered payload{base64 bytes}}}}}").await);
    let message = &data["read"]["entries"][0]["message"];
    assert_eq!(message["fieldsStatus"], "UNSUPPORTED_DECODER");
    assert!(message["fields"].is_null());
    assert_eq!(message["renderStatus"], "UNSUPPORTED_DECODER");
    assert!(message["rendered"].is_null());
    assert_eq!(message["payload"]["bytes"], payload.len().to_string());
    assert!(!LEGACY_CALLBACK_USED.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn operation_timeout_does_not_shorten_session_lifetime_but_client_deadline_does() {
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let ns = store.namespace("a", alpha::alpha::SCHEMA).unwrap();
    ns.try_log(alpha::alpha::events::Tick { value: 1 }).unwrap();
    ns.sync().unwrap();
    let config = dduroc_graphql::ServiceConfig {
        operation_timeout: Duration::from_millis(50),
        idle_ttl: Duration::from_secs(1),
        session_lifetime: Duration::from_millis(250),
        ..Default::default()
    };
    let schema = app(Arc::new(store.reader()), config);
    let (id, revision) = open_read(&schema).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let data = ok(page(&schema, &id, &revision, 1, 65536).await);
    assert_eq!(
        data["readSession"]["page"]["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let revision = data["readSession"]["revision"].as_str().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(matches!(
        error_code(&page(&schema, &id, revision, 1, 65536).await),
        "SESSION_EXPIRED" | "SESSION_NOT_FOUND"
    ));
    let data = ok(schema
        .execute(r#"mutation{openReadSession(limits:{timeoutMillis:"30"}){id revision}}"#)
        .await);
    let id = data["openReadSession"]["id"].as_str().unwrap();
    let revision = data["openReadSession"]["revision"].as_str().unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        error_code(&page(&schema, id, revision, 1, 65536).await),
        "DEADLINE_EXCEEDED"
    );
}

#[tokio::test]
async fn response_budget_counts_lists_of_scalar_values() {
    use dduroc_graphql::{SchemaLimits, Service, ServiceConfig, build_schema};
    let directory = tempfile::tempdir().unwrap();
    let store = store(directory.path());
    let codes = (0..8).map(|index| format!("x{index}{}", "a".repeat(60)));
    let preferences = dduroc::read::LanguagePreferences::new(codes).unwrap();
    let reader = Arc::new(store.reader().with_language_preferences(preferences));
    let service = Service::new(reader, ServiceConfig::default()).unwrap();
    let schema = build_schema(
        service,
        SchemaLimits {
            max_response_bytes: 300,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        error_code(&schema.execute("{catalog{defaultLanguages}}").await),
        "OPERATION_LIMIT_EXCEEDED"
    );
}
