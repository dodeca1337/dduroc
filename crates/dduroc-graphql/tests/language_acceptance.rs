//! Independent acceptance of ordered message languages through public APIs.
use async_graphql::{Request, Response, Variables};
use dduroc::prelude::*;
use dduroc::{
    StoreConfig,
    read::{EntryKind, LanguagePreferences, Query, Reader},
};
use dduroc_engine::schema::{Language, Schema};
use futures_util::StreamExt;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};

mod bilingual {
    dduroc::schema! {
        name: bilingual, version: 1, languages: [en, ru],
        events { Tick = 1 { level: Info, en: "tick {value}", ru: "отсчёт {value}", value: u32 }, }
    }
}
mod english {
    dduroc::schema! {
        name: english, version: 1, languages: [en],
        events { Tick = 1 { level: Info, en: "english {value}", value: u32 }, }
    }
}
fn preferences(codes: &[&str]) -> LanguagePreferences {
    LanguagePreferences::new(codes.iter().copied()).unwrap()
}
fn store(path: &std::path::Path) -> Arc<Store> {
    Store::open(StoreConfig::new(path).with_budget_per_class(16 << 20)).unwrap()
}
fn app(reader: Arc<Reader>) -> dduroc_graphql::LogSchema {
    let service = dduroc_graphql::Service::new(reader, Default::default()).unwrap();
    dduroc_graphql::build_schema(service, Default::default()).unwrap()
}
fn ok(response: Response) -> Value {
    assert!(response.errors.is_empty(), "{:?}", response.errors);
    response.data.into_json().unwrap()
}
fn request(document: &str, variables: Value) -> Request {
    Request::new(document).variables(Variables::from_json(variables))
}
fn populated(path: &std::path::Path) -> Arc<Store> {
    let store = store(path);
    let ns = store.namespace("bi", bilingual::bilingual::SCHEMA).unwrap();
    ns.try_log(bilingual::bilingual::events::Tick { value: 7 })
        .unwrap();
    ns.try_log_text(Level::Info, "literal", "raw Русский / English", None)
        .unwrap();
    let en = store.namespace("en", english::english::SCHEMA).unwrap();
    en.try_log(english::english::events::Tick { value: 8 })
        .unwrap();
    store.sync().unwrap();
    store
}
const READ: &str = "query($presentation:PresentationInput){read(query:{order:OLDEST},presentation:$presentation){entries{namespace text{text} message{rendered renderStatus language languagePreferenceIndex}}}}";
fn message<'a>(data: &'a Value, namespace: &str) -> &'a Value {
    &data["read"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["namespace"] == namespace && !entry["message"].is_null())
        .unwrap()["message"]
}
fn assert_render(message: &Value, text: &str, language: &str, preference: Option<usize>) {
    assert_eq!(message["rendered"], text);
    assert_eq!(message["renderStatus"], "RENDERED");
    assert_eq!(message["language"], language);
    assert_eq!(message["languagePreferenceIndex"], json!(preference));
}

#[test]
fn language_selection_is_ordered_exact_then_equivalent_without_region_inference() {
    let schema = Schema {
        languages: &[
            Language("en"),
            Language("RU_ru"),
            Language("ru-RU"),
            Language("ru"),
        ],
        ..bilingual::bilingual::SCHEMA
    };
    for (codes, language, preference) in [
        (vec!["zz", "ru-RU"], 2, Some(1)),
        (vec!["Ru_rU", "en"], 1, Some(0)),
        (vec!["ru-RU", "en"], 2, Some(0)),
        (vec!["ru-CA", "en"], 0, Some(1)),
        (vec!["ru-CA"], 0, None),
        (vec!["RU", "en"], 3, Some(0)),
    ] {
        let selected = preferences(&codes).select(&schema).unwrap();
        assert_eq!(
            (selected.language_index, selected.preference_index),
            (language, preference),
            "{codes:?}"
        );
    }
    assert!(
        preferences(&["en"])
            .select(&Schema {
                languages: &[],
                ..schema
            })
            .is_none()
    );
    assert_eq!(LanguagePreferences::default().codes(), &["en"]);
    assert!(LanguagePreferences::new(["x".repeat(64)]).is_ok());
    assert!(LanguagePreferences::new((0..8).map(|i| format!("x{i}"))).is_ok());
    for invalid in [
        vec![],
        vec!["".into()],
        vec!["en US".into()],
        vec!["язык".into()],
        vec!["x".repeat(65)],
        (0..9).map(|i| format!("x{i}")).collect(),
        vec!["en-US".into(), "EN_us".into()],
    ] {
        assert!(LanguagePreferences::new(invalid).is_err());
    }
}

#[test]
fn native_readers_are_independent_and_refuse_wrong_schema_identity() {
    let directory = tempfile::tempdir().unwrap();
    let store = populated(directory.path());
    let ru = store
        .reader()
        .with_language_preferences(preferences(&["ru", "en"]));
    let en = store.reader();
    let entries = ru.query(&Query::default()).unwrap().entries;
    let entry = entries
        .iter()
        .find(|entry| {
            entry.namespace.as_ref() == "bi" && matches!(entry.kind, EntryKind::Message { .. })
        })
        .unwrap();
    let rendered = ru.render_preferred(entry).unwrap();
    assert_eq!(
        (
            rendered.text.as_str(),
            rendered.language.as_deref(),
            rendered.preference_index
        ),
        ("отсчёт 7", Some("ru"), Some(0))
    );
    assert_eq!(en.render_preferred(entry).unwrap().text, "tick 7");
    assert_eq!(
        ru.render_with_preferences(entry, &preferences(&["en"]))
            .unwrap()
            .text,
        "tick 7"
    );
    assert_eq!(ru.render_preferred(entry).unwrap().text, "отсчёт 7");
    assert_eq!(en.language_preferences().codes(), &["en"]);
    assert_eq!(
        bilingual::bilingual::SCHEMA.languages,
        &[Language("en"), Language("ru")]
    );
    let mut wrong = entry.clone();
    wrong.schema_identity = english::english::SCHEMA.identity;
    assert!(ru.render_preferred(&wrong).is_none());
    wrong.schema_identity = None;
    assert!(ru.render_preferred(&wrong).is_none());
    let raw = entries
        .iter()
        .find(|entry| matches!(entry.kind, EntryKind::Text { .. }))
        .unwrap();
    let rendered = ru.render_preferred(raw).unwrap();
    assert_eq!(rendered.text, "raw Русский / English");
    assert_eq!((rendered.language, rendered.preference_index), (None, None));
}

#[tokio::test]
async fn graphql_inherits_reader_defaults_and_overrides_do_not_leak_between_clients() {
    let directory = tempfile::tempdir().unwrap();
    let store = populated(directory.path());
    let schema = app(Arc::new(
        store
            .reader()
            .with_language_preferences(preferences(&["ru", "en"])),
    ));
    let catalog = ok(schema
        .execute("{catalog{defaultLanguages schemas{languages}}}")
        .await);
    assert_eq!(catalog["catalog"]["defaultLanguages"], json!(["ru", "en"]));
    for presentation in [Value::Null, json!({}), json!({"languages":null})] {
        let data = ok(schema
            .execute(request(READ, json!({"presentation":presentation})))
            .await);
        assert_render(message(&data, "bi"), "отсчёт 7", "ru", Some(0));
        assert_render(message(&data, "en"), "english 8", "en", Some(1));
        assert!(
            data["read"]["entries"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["text"]["text"] == "raw Русский / English")
        );
    }
    let (a, b) = tokio::join!(
        schema.execute(request(
            READ,
            json!({"presentation":{"languages":["zz","RU"]}})
        )),
        schema.execute(request(READ, json!({"presentation":{"language":"en"}})))
    );
    assert_render(message(&ok(a), "bi"), "отсчёт 7", "ru", Some(1));
    assert_render(message(&ok(b), "bi"), "tick 7", "en", Some(0));
    let fallback = ok(schema
        .execute(request(
            READ,
            json!({"presentation":{"languages":["ru-RU"]}}),
        ))
        .await);
    assert_render(message(&fallback, "bi"), "tick 7", "en", None);
    let inherited = ok(schema.execute(request(READ, json!({}))).await);
    assert_render(message(&inherited, "bi"), "отсчёт 7", "ru", Some(0));
    for invalid in [
        json!({"languages":[]}),
        json!({"languages":["en"],"language":"ru"}),
        json!({"languages":["en US"]}),
        json!({"languages":["x".repeat(65)]}),
        json!({"languages":(0..9).map(|i|format!("x{i}")).collect::<Vec<_>>()}),
    ] {
        let response = schema
            .execute(request(READ, json!({"presentation":invalid})))
            .await;
        assert!(!response.errors.is_empty(), "accepted invalid preference");
        assert_eq!(
            response.errors[0].extensions.as_ref().unwrap().get("code"),
            Some(&async_graphql::Value::String("INVALID_INPUT".into()))
        );
    }
}

#[tokio::test]
async fn sessions_and_follow_pin_the_same_policy_as_direct_queries() {
    let directory = tempfile::tempdir().unwrap();
    let store = populated(directory.path());
    let schema = app(Arc::new(
        store
            .reader()
            .with_language_preferences(preferences(&["ru", "en"])),
    ));
    for presentation in [Value::Null, json!({"languages":["zz","en"]})] {
        let opened = ok(schema.execute(request("mutation($p:PresentationInput){openReadSession(query:{namespaces:[\"bi\"],order:OLDEST},presentation:$p){id revision}}",json!({"p":presentation}))).await);
        let opened = &opened["openReadSession"];
        let page = ok(schema.execute(request("mutation($id:String!,$rev:UInt64!){readSession(id:$id,expectedRevision:$rev){revision page{entries{message{rendered renderStatus language languagePreferenceIndex}}}}}",json!({"id":opened["id"],"rev":opened["revision"]}))).await);
        let entry = page["readSession"]["page"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| !e["message"].is_null())
            .unwrap();
        let expected = if presentation.is_null() {
            ("отсчёт 7", "ru", Some(0))
        } else {
            ("tick 7", "en", Some(1))
        };
        assert_render(&entry["message"], expected.0, expected.1, expected.2);
        ok(schema.execute(request("mutation($id:String!,$rev:UInt64!){closeReadSession(id:$id,expectedRevision:$rev)}",json!({"id":opened["id"],"rev":page["readSession"]["revision"]}))).await);
        let mut follow = Box::pin(schema.execute_stream(request("subscription($p:PresentationInput){follow(query:{namespaces:[\"bi\"]},presentation:$p){entries{message{rendered renderStatus language languagePreferenceIndex}} ended}}",json!({"p":presentation}))));
        let batch = tokio::time::timeout(Duration::from_secs(5), follow.next())
            .await
            .unwrap()
            .unwrap();
        let data = ok(batch);
        let entry = data["follow"]["entries"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| !e["message"].is_null())
            .unwrap();
        assert_render(&entry["message"], expected.0, expected.1, expected.2);
    }
}

#[tokio::test]
async fn unknown_schema_keeps_payload_but_never_claims_a_rendered_language() {
    let directory = tempfile::tempdir().unwrap();
    drop(populated(directory.path()));
    let reader = Reader::open_dump([directory.path()], &[])
        .unwrap()
        .with_language_preferences(preferences(&["ru", "en"]));
    let entries = reader.query(&Query::default()).unwrap().entries;
    for entry in entries
        .iter()
        .filter(|entry| matches!(entry.kind, EntryKind::Message { .. }))
    {
        assert!(reader.render_preferred(entry).is_none());
    }
    let schema = app(Arc::new(reader));
    let data = ok(schema.execute("{read{entries{message{rendered renderStatus language languagePreferenceIndex payload{base64}}}}}").await);
    let messages = data["read"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|entry| entry["message"].as_object());
    let mut count = 0;
    for message in messages {
        count += 1;
        assert_eq!(message["renderStatus"], "UNKNOWN_SCHEMA");
        for field in ["rendered", "language", "languagePreferenceIndex"] {
            assert!(message[field].is_null());
        }
        assert!(!message["payload"]["base64"].as_str().unwrap().is_empty());
    }
    assert_eq!(count, 2);
}

#[tokio::test]
async fn public_rust_service_default_presentation_inherits_reader() {
    let directory = tempfile::tempdir().unwrap();
    let store = populated(directory.path());
    let service = dduroc_graphql::Service::new(
        Arc::new(
            store
                .reader()
                .with_language_preferences(preferences(&["ru", "en"])),
        ),
        Default::default(),
    )
    .unwrap();
    let (default_page, override_page) = tokio::join!(
        service.query(
            Query::default(),
            Default::default(),
            dduroc::read::PageLimits::new(100, 65536).unwrap(),
            Default::default()
        ),
        service.query(
            Query::default(),
            Default::default(),
            dduroc::read::PageLimits::new(100, 65536).unwrap(),
            dduroc_graphql::Presentation {
                languages: Some(preferences(&["zz", "en"])),
                ..Default::default()
            }
        )
    );
    for (page, expected, language, index) in [
        (default_page.unwrap(), "отсчёт 7", "ru", 0),
        (override_page.unwrap(), "tick 7", "en", 1),
    ] {
        let message = page
            .entries
            .iter()
            .find(|entry| entry.namespace == "bi" && entry.message.is_some())
            .unwrap()
            .message
            .as_ref()
            .unwrap();
        assert_eq!(message.rendered.as_deref(), Some(expected));
        assert_eq!(message.language.as_deref(), Some(language));
        assert_eq!(message.language_preference_index, Some(index));
    }
}

#[test]
fn unknown_entry_cannot_be_rendered_by_an_unidentified_manual_decoder() {
    let directory = tempfile::tempdir().unwrap();
    drop(populated(directory.path()));
    let unknown = Reader::open_dump([directory.path()], &[]).unwrap();
    let entry = unknown
        .query(&Query::default())
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| {
            entry.namespace.as_ref() == "bi" && matches!(entry.kind, EntryKind::Message { .. })
        })
        .unwrap();
    assert!(entry.schema_identity.is_none());
    drop(unknown);
    let unrelated = Schema {
        identity: None,
        name: "bilingual",
        ..english::english::SCHEMA
    };
    let reader = Reader::open_dump([directory.path()], &[unrelated]).unwrap();
    assert!(
        reader.render_preferred(&entry).is_none(),
        "unknown payload was interpreted by unrelated manual decoder: {:?}",
        reader.render_preferred(&entry)
    );
    assert!(
        reader
            .render_with_preferences(&entry, &preferences(&["en"]))
            .is_none()
    );
}
