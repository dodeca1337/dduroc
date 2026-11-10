//! Independent acceptance of the native public API contracts.
#![cfg(feature = "read")]

use dduroc::prelude::*;
use dduroc::read::{EntryKind, Order, Query, ReadResource};
use dduroc::{EventId, MetricLimits, SchemaIdentity, StoreConfig};
use std::sync::atomic::{AtomicBool, Ordering};

mod primary {
    dduroc::schema! {
        name: twins, version: 1, languages: [en],
        events { Twin = 1 { level: Info, en: "primary {value}", value: u32 }, }
        metrics {
            Reading = 1 { type: f32, warn: ..=10.0, alarm: ..=20.0 },
            Mode = 2 { states: [Off = 0, On = 1] },
        }
        spans { Work = 1, }
    }
}
mod foreign {
    // Deliberately identical schema/event/type spelling and field layout.
    // Textual labels and a successful postcard parse cannot prove identity.
    dduroc::schema! {
        name: twins, version: 1, languages: [en],
        events { Twin = 1 { level: Info, en: "foreign {value}", value: u32 }, }
        metrics {
            Reading = 1 { type: f32, warn: ..=10.0, alarm: ..=20.0 },
            Mode = 2 { states: [Off = 0, On = 1] },
        }
        spans { Work = 1, }
    }
}
mod upgraded {
    dduroc::schema! {
        name: twins, version: 2, languages: [en],
        events { Twin = 1 { level: Info, en: "upgraded {value}", value: u32 }, }
        history { 1 { events { Twin = 1 { value: u32 } } } }
        migrations { 1 => { v1::Twin: |old| events::Twin { value: old.value + 1 }, }, }
    }
}

fn open(path: &std::path::Path) -> std::sync::Arc<Store> {
    Store::open(StoreConfig::new(path).with_budget_per_class(16 << 20)).unwrap()
}

static SERIALIZED: AtomicBool = AtomicBool::new(false);
struct ForeignSerializationProbe;
impl dduroc::serde::Serialize for ForeignSerializationProbe {
    fn serialize<S: dduroc::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        SERIALIZED.store(true, Ordering::SeqCst);
        serializer.serialize_u32(7)
    }
}
impl dduroc::Event for ForeignSerializationProbe {
    const SCHEMA: SchemaIdentity = <foreign::twins::events::Twin as dduroc::Event>::SCHEMA;
    const ID: EventId = EventId(1);
    const LEVEL: dduroc::Level = dduroc::Level::Info;
    const NAME: &'static str = "Twin";
}

#[test]
fn identical_foreign_event_is_rejected_before_serialization_or_queue_admission() {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
    SERIALIZED.store(false, Ordering::SeqCst);
    assert!(namespace.try_log(ForeignSerializationProbe).is_err());
    assert!(!SERIALIZED.load(Ordering::SeqCst));
    assert!(
        namespace
            .try_log(foreign::twins::events::Twin { value: 7 })
            .is_err()
    );
    namespace.log(foreign::twins::events::Twin { value: 8 });
    namespace
        .try_log(primary::twins::events::Twin { value: 9 })
        .unwrap();
    namespace.sync().unwrap();
    let reader = store.reader();
    let answer = reader.query(&Query::new()).unwrap();
    let events: Vec<_> = answer
        .entries
        .iter()
        .filter(|entry| matches!(entry.kind, EntryKind::Message { .. }))
        .collect();
    assert_eq!(events.len(), 1);
    assert_eq!(
        reader
            .decode::<primary::twins::events::Twin>(events[0])
            .unwrap()
            .unwrap()
            .value,
        9
    );
    assert!(
        reader
            .decode::<foreign::twins::events::Twin>(events[0])
            .is_none()
    );
}

#[test]
fn foreign_metric_and_limit_descriptors_cannot_change_matching_numeric_ids() {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
    namespace
        .set_thresholds(primary::twins::metrics::Reading, ..=4.0, ..=8.0)
        .unwrap();
    let expected = namespace.limits(primary::twins::metrics::Reading).unwrap();
    assert!(namespace.series(foreign::twins::metrics::Reading).is_err());
    assert!(namespace.series(foreign::twins::metrics::Mode).is_err());
    assert!(
        namespace
            .set_thresholds(foreign::twins::metrics::Reading, ..=100.0, ..=200.0)
            .is_err()
    );
    assert!(
        namespace
            .set_limits(foreign::twins::metrics::Reading, MetricLimits::default())
            .is_err()
    );
    assert!(
        namespace
            .clear_limits(foreign::twins::metrics::Reading)
            .is_err()
    );
    assert!(
        namespace
            .set_severity_fn(foreign::twins::metrics::Reading, |_| {
                dduroc::Severity::Normal
            })
            .is_err()
    );
    assert!(
        namespace
            .clear_severity_fn(foreign::twins::metrics::Reading)
            .is_err()
    );
    assert!(namespace.limits(foreign::twins::metrics::Reading).is_err());
    assert!(
        namespace
            .severity_of(foreign::twins::metrics::Reading, 7.0)
            .is_err()
    );
    assert_eq!(
        namespace.limits(primary::twins::metrics::Reading).unwrap(),
        expected
    );
    assert_eq!(
        namespace
            .severity_of(primary::twins::metrics::Reading, 7.0)
            .unwrap(),
        dduroc::Severity::Warn
    );
}

#[test]
fn quiet_foreign_span_and_its_children_leave_no_start_end_or_attached_event() {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
    assert!(namespace.try_span(foreign::twins::spans::Work).is_err());
    {
        let rejected = namespace.span(foreign::twins::spans::Work);
        assert!(
            rejected
                .try_log(primary::twins::events::Twin { value: 1 })
                .is_err()
        );
        assert!(
            namespace
                .try_log_in(&rejected, primary::twins::events::Twin { value: 2 })
                .is_err()
        );
        assert!(rejected.try_child(primary::twins::spans::Work).is_err());
        let _child = rejected.child(primary::twins::spans::Work);
        assert!(rejected.close().is_err());
    }
    namespace.sync().unwrap();
    let answer = store.reader().query(&Query::new()).unwrap();
    assert!(answer.entries.iter().all(|entry| !matches!(
        entry.kind,
        EntryKind::Message { .. } | EntryKind::SpanStart { .. } | EntryKind::SpanEnd { .. }
    )));
}

#[test]
fn spans_attach_across_namespaces_of_one_store_but_never_a_foreign_store() {
    let directory = tempfile::tempdir().unwrap();
    let other_directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let other = open(other_directory.path());
    let origin = store.namespace("origin", primary::twins::SCHEMA).unwrap();
    let peer = store.namespace("peer", primary::twins::SCHEMA).unwrap();
    let foreign_namespace = other.namespace("peer", primary::twins::SCHEMA).unwrap();
    let span = origin.try_span(primary::twins::spans::Work).unwrap();
    let id = span.id();
    peer.try_log_in(&span, primary::twins::events::Twin { value: 42 })
        .unwrap();
    assert!(
        foreign_namespace
            .try_log_in(&span, primary::twins::events::Twin { value: 99 })
            .is_err()
    );
    let child = peer
        .try_span_with_parent(primary::twins::spans::Work, Some(&span))
        .unwrap();
    assert!(
        foreign_namespace
            .try_span_with_parent(primary::twins::spans::Work, Some(&span))
            .is_err()
    );
    drop(child);
    drop(span);
    store.sync().unwrap();
    other.sync().unwrap();
    let answer = store
        .reader()
        .query(&Query::new().order(Order::Oldest))
        .unwrap();
    assert!(
        answer
            .entries
            .iter()
            .any(|entry| entry.namespace.as_ref() == "peer"
                && entry.span == Some(id)
                && matches!(entry.kind, EntryKind::Message { .. }))
    );
    assert!(
        other
            .reader()
            .query(&Query::new())
            .unwrap()
            .entries
            .is_empty()
    );
}

#[test]
fn a_foreign_reader_cannot_reinterpret_an_entry_with_identical_namespace_and_layout() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let a = open(a_dir.path());
    let b = open(b_dir.path());
    let a_ns = a.namespace("same", primary::twins::SCHEMA).unwrap();
    let b_ns = b.namespace("same", foreign::twins::SCHEMA).unwrap();
    a_ns.try_log(primary::twins::events::Twin { value: 11 })
        .unwrap();
    b_ns.try_log(foreign::twins::events::Twin { value: 12 })
        .unwrap();
    a.sync().unwrap();
    b.sync().unwrap();
    let a_reader = a.reader();
    let b_reader = b.reader();
    let answer = a_reader.query(&Query::new()).unwrap();
    let entry = answer
        .entries
        .iter()
        .find(|entry| matches!(entry.kind, EntryKind::Message { .. }))
        .unwrap();
    assert_eq!(
        a_reader
            .decode::<primary::twins::events::Twin>(entry)
            .unwrap()
            .unwrap()
            .value,
        11
    );
    assert!(
        b_reader
            .decode::<foreign::twins::events::Twin>(entry)
            .is_none()
    );
    assert_ne!(b_reader.render(entry, "en").as_deref(), Some("foreign 11"));
}

#[test]
fn upgraded_namespace_rejects_old_typed_scope_and_decodes_migrated_payload_in_new_scope() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = open(directory.path());
        let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
        namespace
            .try_log(primary::twins::events::Twin { value: 20 })
            .unwrap();
        namespace.sync().unwrap();
    }
    let store = open(directory.path());
    let namespace = store.namespace("service", upgraded::twins::SCHEMA).unwrap();
    assert!(
        namespace
            .try_log(primary::twins::events::Twin { value: 30 })
            .is_err()
    );
    let reader = store.reader();
    let answer = reader.query(&Query::new()).unwrap();
    let entry = answer
        .entries
        .iter()
        .find(|entry| matches!(entry.kind, EntryKind::Message { .. }))
        .unwrap();
    assert_eq!(
        reader
            .decode::<upgraded::twins::events::Twin>(entry)
            .unwrap()
            .unwrap()
            .value,
        21
    );
    assert!(
        reader
            .decode::<primary::twins::events::Twin>(entry)
            .is_none()
    );
}

mod paging {
    dduroc::schema! {
        name: pages, version: 1, languages: [en],
        events {
            Normal = 1 { level: Info, en: "normal {value}", value: u32 },
            Urgent = 2 { level: Error, store: critical, en: "urgent {value}", value: u32 },
            Rare = 3 { level: Info, en: "rare {value}", value: u32 },
            Large = 4 { level: Error, store: critical, en: "{payload}", payload: String },
        }
        metrics { Mode = 1 { store: telemetry, states: [Off = 0, On = 1] }, Blob = 2 { store: telemetry, type: blob }, }
    }
}

fn pages_fixture() -> (
    tempfile::TempDir,
    std::sync::Arc<Store>,
    dduroc::Namespace,
    dduroc::Namespace,
) {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let a = store.namespace("a", paging::pages::SCHEMA).unwrap();
    let b = store.namespace("b", paging::pages::SCHEMA).unwrap();
    for value in 0..12 {
        a.try_log(paging::pages::events::Normal { value }).unwrap();
        b.try_log(paging::pages::events::Normal { value: value + 100 })
            .unwrap();
        if value % 3 == 0 {
            a.try_log(paging::pages::events::Urgent { value: value + 200 })
                .unwrap();
            b.try_log(paging::pages::events::Urgent { value: value + 300 })
                .unwrap();
        }
    }
    store.sync().unwrap();
    (directory, store, a, b)
}

#[test]
fn owned_pages_match_unpaged_order_and_exclude_later_roster_and_channel_appends() {
    use dduroc::read::{Cancellation, ReadLimits};
    for order in [Order::Oldest, Order::Newest] {
        let (_directory, store, a, b) = pages_fixture();
        let reader = std::sync::Arc::new(store.reader());
        let query = Query::new().order(order);
        let expected = reader.query(&query).unwrap().entries;
        let mut session = reader
            .open_session(query, ReadLimits::default(), Cancellation::new())
            .unwrap();
        a.try_log(paging::pages::events::Normal { value: 999 })
            .unwrap();
        // This channel existed but was empty when the session captured it.
        b.series(paging::pages::metrics::Mode)
            .unwrap()
            .sample(paging::pages::metrics::Mode::On);
        let late = store
            .namespace("new-service", paging::pages::SCHEMA)
            .unwrap();
        late.try_log(paging::pages::events::Normal { value: 1000 })
            .unwrap();
        store.sync().unwrap();
        drop(reader); // Session owns the reader instead of borrowing this binding.
        let mut actual = Vec::new();
        for _ in 0..100 {
            let page = session
                .next_page(dduroc::read::PageLimits::new(3, 65536).unwrap())
                .unwrap();
            assert!(page.damaged.is_empty());
            assert!(!page.truncated);
            actual.extend(page.entries);
            if page.exhausted {
                break;
            }
        }
        assert_eq!(actual, expected);
        assert!(
            session
                .next_page(dduroc::read::PageLimits::new(3, 65536).unwrap())
                .unwrap()
                .exhausted
        );
    }
}

#[test]
fn too_small_and_partial_byte_pages_preserve_the_pending_record_exactly_once() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let (_directory, store, _a, _b) = pages_fixture();
    let reader = std::sync::Arc::new(store.reader());
    let query = Query::new().order(Order::Oldest);
    let expected = reader.query(&query).unwrap().entries;
    let mut session = reader
        .open_session(query, ReadLimits::default(), Cancellation::new())
        .unwrap();
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(20, 1).unwrap()),
        Err(ReadError::PageTooSmall)
    ));
    let first = session
        .next_page(dduroc::read::PageLimits::new(20, 1024).unwrap())
        .unwrap();
    assert!(!first.entries.is_empty());
    assert!(first.entries.len() < expected.len());
    let mut actual = first.entries;
    for _ in 0..10 {
        let page = session
            .next_page(dduroc::read::PageLimits::new(100, 65536).unwrap())
            .unwrap();
        actual.extend(page.entries);
        if page.exhausted {
            break;
        }
    }
    assert_eq!(actual, expected);
}

#[test]
fn session_freezes_epochs_and_distinguishes_cancel_deadline_and_budget_from_eof() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let (_directory, store, _a, _b) = pages_fixture();
    let reader = std::sync::Arc::new(store.reader());
    let query = Query::new().order(Order::Oldest);
    let mut frozen = reader
        .open_session(query.clone(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    store
        .record_sync(
            chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap(),
            SyncSource::Gps,
        )
        .unwrap();
    assert!(
        reader
            .query(&query)
            .unwrap()
            .entries
            .iter()
            .all(|entry| entry.utc.is_some())
    );
    assert!(
        frozen
            .next_page(dduroc::read::PageLimits::new(100, 65536).unwrap())
            .unwrap()
            .entries
            .iter()
            .all(|entry| entry.utc.is_none())
    );

    let cancelled = Cancellation::new();
    cancelled.cancel();
    assert!(matches!(
        reader.open_session(query.clone(), ReadLimits::default(), cancelled),
        Err(ReadError::Cancelled)
    ));
    let cancellation = Cancellation::new();
    let mut session = reader
        .open_session(query.clone(), ReadLimits::default(), cancellation.clone())
        .unwrap();
    assert!(
        !session
            .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
            .unwrap()
            .entries
            .is_empty()
    );
    cancellation.cancel();
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(10, 65536).unwrap()),
        Err(ReadError::Cancelled)
    ));
    let limits = ReadLimits {
        deadline: Some(std::time::Instant::now()),
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(query.clone(), limits, Cancellation::new()),
        Err(ReadError::Deadline)
    ));
    let limits = ReadLimits {
        returned_bytes: 1,
        ..ReadLimits::default()
    };
    let mut session = reader
        .open_session(query, limits, Cancellation::new())
        .unwrap();
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(10, 65536).unwrap()),
        Err(ReadError::LimitExceeded(ReadResource::ReturnedBytes))
    ));
}

#[test]
fn initialization_limits_roster_and_channels_before_reading_or_decoding_blocks() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let (_directory, store, _a, _b) = pages_fixture();
    let reader = std::sync::Arc::new(store.reader());
    let limits = ReadLimits {
        roster_entries: 0,
        channels: 0,
        blocks: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::RosterEntries))
    ));
    let limits = ReadLimits {
        channels: 0,
        blocks: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::Channels))
    ));
    let limits = ReadLimits {
        blocks: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::Blocks))
    ));
    let limits = ReadLimits {
        compressed_bytes: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::CompressedBytes))
    ));
    let limits = ReadLimits {
        decoded_bytes: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::DecodedBytes))
    ));
}

#[test]
fn a_rare_filter_is_charged_for_rejected_records_instead_of_reporting_false_eof() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("service", paging::pages::SCHEMA).unwrap();
    for value in 0..100 {
        namespace
            .try_log(paging::pages::events::Normal { value })
            .unwrap();
    }
    namespace
        .try_log(paging::pages::events::Rare { value: 101 })
        .unwrap();
    store.sync().unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let query = Query::new().order(Order::Oldest).event(EventId(3));
    let limits = ReadLimits {
        examined_records: 7,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(query, limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::ExaminedRecords))
    ));
}

#[test]
fn compressed_input_and_decoded_bytes_have_separate_limits() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("service", paging::pages::SCHEMA).unwrap();
    namespace
        .try_log_text(dduroc::Level::Info, "test", "x".repeat(200_000), None)
        .unwrap();
    namespace.sync().unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let limits = ReadLimits {
        compressed_bytes: 64 << 10,
        decoded_bytes: 64 << 10,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::DecodedBytes))
    ));
}

#[test]
fn retention_of_every_unread_block_is_a_gap_even_when_the_channel_becomes_empty() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let cfg = StoreConfig::new(directory.path())
        .with_budget_per_class(16 << 20)
        .channel(
            dduroc::StorageClass::Default,
            dduroc::ChannelConfig {
                segment_bytes: 64 << 10,
                block_max_bytes: 8 << 10,
                compression: dduroc::Compression::None,
                ..dduroc::ChannelConfig::new(128 << 10)
            },
        );
    let store = Store::open(cfg).unwrap();
    let victim = store.namespace("victim", paging::pages::SCHEMA).unwrap();
    for value in 0..3 {
        victim
            .try_log_text(
                dduroc::Level::Info,
                "test",
                format!("{value}{}", "x".repeat(40 << 10)),
                None,
            )
            .unwrap();
        victim.sync().unwrap();
    }
    let reader = std::sync::Arc::new(store.reader());
    let mut session = reader
        .open_session(
            Query::new().order(Order::Oldest),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    let pressure = store.namespace("pressure", paging::pages::SCHEMA).unwrap();
    for _ in 0..5 {
        pressure
            .try_log_text(dduroc::Level::Info, "test", "y".repeat(40 << 10), None)
            .unwrap();
        pressure.sync().unwrap();
    }
    assert!(
        store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .any(|channel| channel.key.namespace == "victim"
                && channel.key.class == dduroc::StorageClass::Default
                && channel.first_index.is_none()
                && channel.last_index.is_none())
    );
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(100, 1 << 20).unwrap()),
        Err(ReadError::RetentionGap)
    ));
}

#[test]
fn schema_rewrite_expires_both_unread_and_already_exhausted_sessions() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    {
        let store = open(directory.path());
        let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
        namespace
            .try_log(primary::twins::events::Twin { value: 1 })
            .unwrap();
        namespace.sync().unwrap();
    }
    let store = open(directory.path());
    let namespace = store.namespace("service", upgraded::twins::SCHEMA).unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let mut unread = reader
        .open_session(Query::new(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    let mut exhausted = reader
        .open_session(Query::new(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    let page = exhausted
        .next_page(dduroc::read::PageLimits::new(100, 65536).unwrap())
        .unwrap();
    assert_eq!(page.entries.len(), 1);
    assert!(page.exhausted);
    namespace.migrate().unwrap();
    assert!(matches!(
        unread.next_page(dduroc::read::PageLimits::new(100, 65536).unwrap()),
        Err(ReadError::SnapshotExpired)
    ));
    assert!(matches!(
        exhausted.next_page(dduroc::read::PageLimits::new(100, 65536).unwrap()),
        Err(ReadError::SnapshotExpired)
    ));
}

#[test]
fn owned_dump_session_holds_its_original_store_lock_until_dropped() {
    use dduroc::read::{Cancellation, ReadLimits, Reader};
    let directory = tempfile::tempdir().unwrap();
    {
        let store = open(directory.path());
        let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
        namespace
            .try_log(primary::twins::events::Twin { value: 77 })
            .unwrap();
        namespace.sync().unwrap();
    }
    let reader = std::sync::Arc::new(
        Reader::open_dump([directory.path()], &[primary::twins::SCHEMA]).unwrap(),
    );
    let mut session = reader
        .open_session(Query::new(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    drop(reader);
    assert!(
        Store::open(StoreConfig::new(directory.path()).with_budget_per_class(16 << 20)).is_err()
    );
    assert_eq!(
        session
            .next_page(dduroc::read::PageLimits::new(100, 65536).unwrap())
            .unwrap()
            .entries
            .len(),
        1
    );
    drop(session);
    let reopened = open(directory.path());
    let namespace = reopened
        .namespace("service", primary::twins::SCHEMA)
        .unwrap();
    namespace
        .try_log(primary::twins::events::Twin { value: 78 })
        .unwrap();
    namespace.sync().unwrap();
    assert_eq!(
        reopened
            .reader()
            .query(&Query::new())
            .unwrap()
            .entries
            .len(),
        2
    );
}

static MIGRATION_CANCELLATION: std::sync::Mutex<Option<dduroc::read::Cancellation>> =
    std::sync::Mutex::new(None);
static MIGRATION_CALLS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

fn cancel_during_migration() {
    MIGRATION_CALLS.fetch_add(1, Ordering::SeqCst);
    if let Some(cancellation) = MIGRATION_CANCELLATION.lock().unwrap().as_ref() {
        cancellation.cancel();
    }
}

mod cancellable_upgrade {
    dduroc::schema! {
        name: twins, version: 2, languages: [en],
        events { Twin = 1 { level: Info, en: "current {value}", value: u32 }, }
        metrics { Mode = 2 { states: [Off = 0, On = 1] }, }
        history { 1 { events { Twin = 1 { value: u32 } } } }
        migrations { 1 => { v1::Twin: |old| {
            crate::cancel_during_migration();
            events::Twin { value: old.value }
        }, }, }
    }
}

#[test]
fn cancellation_during_initial_scan_stops_before_migrating_the_next_record() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    {
        let store = open(directory.path());
        let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
        for value in 0..100 {
            namespace
                .try_log(primary::twins::events::Twin { value })
                .unwrap();
        }
        namespace.sync().unwrap();
    }
    let store = open(directory.path());
    let _namespace = store
        .namespace("service", cancellable_upgrade::twins::SCHEMA)
        .unwrap();
    let reader = std::sync::Arc::new(store.reader());
    for query in [
        Query::new(),
        Query::new().since(store.now()).with_state_seed(),
    ] {
        let cancellation = Cancellation::new();
        MIGRATION_CALLS.store(0, Ordering::SeqCst);
        *MIGRATION_CANCELLATION.lock().unwrap() = Some(cancellation.clone());
        let result = reader.open_session(query.clone(), ReadLimits::default(), cancellation);
        *MIGRATION_CANCELLATION.lock().unwrap() = None;
        assert!(matches!(result, Err(ReadError::Cancelled)));
        assert_eq!(MIGRATION_CALLS.load(Ordering::SeqCst), 1);
        let cancellation = Cancellation::new();
        MIGRATION_CALLS.store(0, Ordering::SeqCst);
        *MIGRATION_CANCELLATION.lock().unwrap() = Some(cancellation.clone());
        let result = reader.follow_controlled(
            query.order(Order::Oldest),
            ReadLimits::default(),
            cancellation,
        );
        *MIGRATION_CANCELLATION.lock().unwrap() = None;
        assert!(matches!(result, Err(ReadError::Cancelled)));
        assert_eq!(MIGRATION_CALLS.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn live_store_rejects_same_textual_schema_with_foreign_or_new_version_identity() {
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("first", primary::twins::SCHEMA).unwrap();
    assert!(store.namespace("foreign", foreign::twins::SCHEMA).is_err());
    assert!(store.namespace("upgrade", upgraded::twins::SCHEMA).is_err());
    assert!(store.namespace("first", foreign::twins::SCHEMA).is_err());
    let peer = store.namespace("peer", primary::twins::SCHEMA).unwrap();
    namespace
        .try_log(primary::twins::events::Twin { value: 3 })
        .unwrap();
    peer.try_log(primary::twins::events::Twin { value: 4 })
        .unwrap();
    store.sync().unwrap();
    let reader = store.reader();
    let answer = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    let values: Vec<_> = answer
        .entries
        .iter()
        .map(|entry| {
            reader
                .decode::<primary::twins::events::Twin>(entry)
                .unwrap()
                .unwrap()
                .value
        })
        .collect();
    assert_eq!(values, [3, 4]);
}

#[test]
fn epoch_catalog_limits_are_checked_before_snapshot_initialization() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    let (_directory, store, _a, _b) = pages_fixture();
    let reader = std::sync::Arc::new(store.reader());
    let limits = ReadLimits {
        epoch_runs: 0,
        blocks: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::EpochRuns))
    ));
    let limits = ReadLimits {
        hardware_boots: 0,
        blocks: 0,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(Query::new(), limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::HardwareBoots))
    ));
}

#[test]
fn old_sparse_states_are_found_beyond_legacy_seed_cutoff_and_page_without_loss() {
    use dduroc::read::{BootTime, Cancellation, Micros, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let a = store.namespace("a", paging::pages::SCHEMA).unwrap();
    let b = store.namespace("b", paging::pages::SCHEMA).unwrap();
    a.series(paging::pages::metrics::Mode)
        .unwrap()
        .sample(paging::pages::metrics::Mode::On);
    b.series(paging::pages::metrics::Mode)
        .unwrap()
        .sample(paging::pages::metrics::Mode::Off);
    store.sync().unwrap();
    let blob = a.series(paging::pages::metrics::Blob).unwrap();
    for value in 0..270u16 {
        blob.sample(&value.to_le_bytes()[..]);
        a.sync().unwrap();
    }
    let now = store.now();
    let query = Query::new()
        .since(BootTime::new(now.boot, Micros(now.at.0 + 1)))
        .with_state_seed();
    let reader = std::sync::Arc::new(store.reader());
    let mut session = reader
        .open_session(query.clone(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(1, 1).unwrap()),
        Err(ReadError::PageTooSmall)
    ));
    let first = session
        .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
        .unwrap();
    assert_eq!(first.seeds.len(), 1);
    assert!(first.entries.is_empty());
    assert!(!first.exhausted);
    let second = session
        .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
        .unwrap();
    assert_eq!(second.seeds.len(), 1);
    assert!(second.entries.is_empty());
    assert!(second.exhausted);
    let mut namespaces = [
        first.seeds[0].namespace.clone(),
        second.seeds[0].namespace.clone(),
    ];
    namespaces.sort();
    assert_eq!(
        namespaces.iter().map(|n| n.as_ref()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert!(
        session
            .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
            .unwrap()
            .seeds
            .is_empty()
    );
    let limits = ReadLimits {
        blocks: 10,
        ..ReadLimits::default()
    };
    assert!(matches!(
        reader.open_session(query, limits, Cancellation::new()),
        Err(ReadError::LimitExceeded(ReadResource::Blocks))
    ));
}

#[test]
fn offline_schema_registry_rejects_ambiguous_names_instead_of_last_wins() {
    use dduroc::read::Reader;
    let directory = tempfile::tempdir().unwrap();
    {
        let store = open(directory.path());
        let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
        namespace
            .try_log(primary::twins::events::Twin { value: 7 })
            .unwrap();
        namespace.sync().unwrap();
    }
    for schemas in [
        [primary::twins::SCHEMA, foreign::twins::SCHEMA],
        [foreign::twins::SCHEMA, primary::twins::SCHEMA],
        [primary::twins::SCHEMA, upgraded::twins::SCHEMA],
    ] {
        assert!(Reader::open_dump([directory.path()], &schemas).is_err());
    }
    assert!(
        Reader::open_dump(
            [directory.path()],
            &[primary::twins::SCHEMA, primary::twins::SCHEMA]
        )
        .is_ok()
    );
}

#[test]
fn controlled_follow_sees_appends_and_new_namespaces_without_false_end() {
    use dduroc::read::{Cancellation, ReadLimits, Tail};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let a = store.namespace("a", paging::pages::SCHEMA).unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let mut follow = reader
        .follow_controlled(
            Query::new().order(Order::Oldest),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(follow.next(Duration::ZERO).unwrap(), Tail::Idle));
    for (name, value) in [("a", 1), ("b", 2)] {
        let added;
        let namespace = if name == "a" {
            &a
        } else {
            added = store.namespace(name, paging::pages::SCHEMA).unwrap();
            &added
        };
        namespace
            .try_log(paging::pages::events::Normal { value })
            .unwrap();
        namespace.sync().unwrap();
        let Tail::Entry(entry) = follow.next(Duration::from_secs(2)).unwrap() else {
            panic!("committed append was not followed");
        };
        assert_eq!(entry.namespace.as_ref(), name);
        assert_eq!(
            reader
                .decode::<paging::pages::events::Normal>(&entry)
                .unwrap()
                .unwrap()
                .value,
            value
        );
    }
    assert!(matches!(follow.next(Duration::ZERO).unwrap(), Tail::Idle));
}

#[test]
fn controlled_follow_long_wait_observes_cancellation_and_absolute_deadline() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    use std::time::{Duration, Instant};
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let _namespace = store.namespace("service", paging::pages::SCHEMA).unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let cancel = Cancellation::new();
    let mut follow = reader
        .follow_controlled(
            Query::new().order(Order::Oldest),
            ReadLimits::default(),
            cancel.clone(),
        )
        .unwrap();
    let start = Instant::now();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(20));
            cancel.cancel();
        });
        assert!(matches!(
            follow.next(Duration::from_secs(5)),
            Err(ReadError::Cancelled)
        ));
    });
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "cancel must interrupt the requested five-second wait"
    );
    assert!(matches!(
        follow.next(Duration::ZERO),
        Err(ReadError::Cancelled)
    ));
    let limits = ReadLimits {
        deadline: Some(Instant::now() + Duration::from_millis(30)),
        ..ReadLimits::default()
    };
    let mut follow = reader
        .follow_controlled(
            Query::new().order(Order::Oldest),
            limits,
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(
        follow.next(Duration::from_secs(5)),
        Err(ReadError::Deadline)
    ));
}

#[test]
fn controlled_follow_cumulative_scan_and_output_limits_cover_new_data() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let store = open(directory.path());
    let namespace = store.namespace("service", paging::pages::SCHEMA).unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let limits = ReadLimits {
        examined_records: 7,
        ..ReadLimits::default()
    };
    let mut follow = reader
        .follow_controlled(
            Query::new().order(Order::Oldest).event(EventId(3)),
            limits,
            Cancellation::new(),
        )
        .unwrap();
    for value in 0..100 {
        namespace
            .try_log(paging::pages::events::Normal { value })
            .unwrap();
    }
    namespace
        .try_log(paging::pages::events::Rare { value: 999 })
        .unwrap();
    namespace.sync().unwrap();
    assert!(matches!(
        follow.next(Duration::from_secs(2)),
        Err(ReadError::LimitExceeded(ReadResource::ExaminedRecords))
    ));
    assert!(matches!(
        follow.next(Duration::ZERO),
        Err(ReadError::LimitExceeded(ReadResource::ExaminedRecords))
    ));
    let limits = ReadLimits {
        returned_bytes: 1,
        ..ReadLimits::default()
    };
    let mut follow = reader
        .follow_controlled(
            Query::new().order(Order::Oldest),
            limits,
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(
        follow.next(Duration::ZERO),
        Err(ReadError::LimitExceeded(ReadResource::ReturnedBytes))
    ));
}

#[test]
fn a_zero_record_session_does_not_scan_blocks() {
    use dduroc::read::{Cancellation, ReadLimits};
    let (_directory, store, _a, _b) = pages_fixture();
    let reader = std::sync::Arc::new(store.reader());
    let limits = ReadLimits {
        blocks: 0,
        examined_records: 0,
        ..ReadLimits::default()
    };
    let mut session = reader
        .open_session(Query::new().limit(0), limits, Cancellation::new())
        .unwrap();
    let page = session
        .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
        .unwrap();
    assert!(page.entries.is_empty());
    assert!(page.exhausted);
    assert!(page.truncated, "retained data was deliberately not scanned");
    let limits = ReadLimits {
        blocks: 0,
        examined_records: 0,
        ..ReadLimits::default()
    };
    let mut empty = reader
        .open_session(
            Query::new()
                .limit(0)
                .namespaces(dduroc::read::NsSelect::Names(vec!["absent".into()])),
            limits,
            Cancellation::new(),
        )
        .unwrap();
    let page = empty
        .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
        .unwrap();
    assert!(page.exhausted);
    assert!(!page.truncated);
}

#[test]
fn controlled_follow_detects_all_evicted_appends_to_a_previously_empty_channel() {
    use dduroc::read::{Cancellation, NsSelect, ReadError, ReadLimits, Tail};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(directory.path())
            .with_budget_per_class(16 << 20)
            .channel(
                dduroc::StorageClass::Critical,
                dduroc::ChannelConfig {
                    segment_bytes: 64 << 10,
                    block_max_bytes: 8 << 10,
                    compression: dduroc::Compression::None,
                    ..dduroc::ChannelConfig::critical(128 << 10)
                },
            ),
    )
    .unwrap();
    let victim = store.namespace("victim", paging::pages::SCHEMA).unwrap();
    let pressure = store.namespace("pressure", paging::pages::SCHEMA).unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let mut follow = reader
        .follow_controlled(
            Query::new()
                .order(Order::Oldest)
                .namespaces(NsSelect::Names(vec!["victim".into()])),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(follow.next(Duration::ZERO).unwrap(), Tail::Idle));
    victim
        .try_log(paging::pages::events::Large {
            payload: "x".repeat(40 << 10),
        })
        .unwrap();
    for _ in 0..5 {
        pressure
            .try_log(paging::pages::events::Large {
                payload: "y".repeat(40 << 10),
            })
            .unwrap();
    }
    assert!(
        store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .any(|c| c.key.namespace == "victim"
                && c.key.class == dduroc::StorageClass::Critical
                && c.first_index.is_none()
                && c.last_index.is_none())
    );
    assert!(matches!(
        follow.next(Duration::from_secs(2)),
        Err(ReadError::RetentionGap)
    ));
    assert!(matches!(
        follow.next(Duration::ZERO),
        Err(ReadError::RetentionGap)
    ));
}

#[test]
fn controlled_follow_never_jumps_over_evicted_unread_blocks_to_later_survivors() {
    use dduroc::read::{Cancellation, NsSelect, ReadError, ReadLimits, Tail};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(directory.path())
            .with_budget_per_class(16 << 20)
            .channel(
                dduroc::StorageClass::Critical,
                dduroc::ChannelConfig {
                    segment_bytes: 64 << 10,
                    block_max_bytes: 8 << 10,
                    compression: dduroc::Compression::None,
                    ..dduroc::ChannelConfig::critical(128 << 10)
                },
            ),
    )
    .unwrap();
    let victim = store.namespace("victim", paging::pages::SCHEMA).unwrap();
    let pressure = store.namespace("pressure", paging::pages::SCHEMA).unwrap();
    victim
        .try_log(paging::pages::events::Large {
            payload: "initial".into(),
        })
        .unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let mut follow = reader
        .follow_controlled(
            Query::new()
                .order(Order::Oldest)
                .namespaces(NsSelect::Names(vec!["victim".into()])),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(
        follow.next(Duration::ZERO).unwrap(),
        Tail::Entry(_)
    ));
    for _ in 0..3 {
        victim
            .try_log(paging::pages::events::Large {
                payload: "x".repeat(40 << 10),
            })
            .unwrap();
    }
    for _ in 0..5 {
        pressure
            .try_log(paging::pages::events::Large {
                payload: "y".repeat(40 << 10),
            })
            .unwrap();
    }
    victim
        .try_log(paging::pages::events::Large {
            payload: "survivor".into(),
        })
        .unwrap();
    assert!(matches!(
        follow.next(Duration::from_secs(2)),
        Err(ReadError::RetentionGap)
    ));
}

#[test]
fn controlled_follow_rewrite_expires_cached_entries_and_previously_ended_streams() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits, Tail};
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    {
        let store = open(directory.path());
        let namespace = store.namespace("service", primary::twins::SCHEMA).unwrap();
        namespace
            .try_log(primary::twins::events::Twin { value: 1 })
            .unwrap();
        namespace.sync().unwrap();
    }
    let store = open(directory.path());
    let namespace = store.namespace("service", upgraded::twins::SCHEMA).unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let mut cached = reader
        .follow_controlled(
            Query::new().order(Order::Oldest),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    let mut ended = reader
        .follow_controlled(
            Query::new().order(Order::Oldest).limit(1),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(
        ended.next(Duration::ZERO).unwrap(),
        Tail::Entry(_)
    ));
    assert!(matches!(ended.next(Duration::ZERO).unwrap(), Tail::Ended));
    namespace.migrate().unwrap();
    assert!(matches!(
        cached.next(Duration::ZERO),
        Err(ReadError::SnapshotExpired)
    ));
    assert!(matches!(
        ended.next(Duration::ZERO),
        Err(ReadError::SnapshotExpired)
    ));
}

#[test]
fn controlled_follow_admission_limits_apply_to_later_namespace_discovery() {
    use dduroc::read::{Cancellation, ReadError, ReadLimits, Tail};
    use std::time::Duration;
    for resource in [ReadResource::Channels, ReadResource::RosterEntries] {
        let directory = tempfile::tempdir().unwrap();
        let store = open(directory.path());
        let _a = store.namespace("a", paging::pages::SCHEMA).unwrap();
        let initial = store.read_backend().catalog().unwrap().len();
        let reader = std::sync::Arc::new(store.reader());
        let mut limits = ReadLimits::default();
        if resource == ReadResource::Channels {
            limits.channels = initial;
        } else {
            limits.roster_entries = initial;
        }
        let mut follow = reader
            .follow_controlled(
                Query::new().order(Order::Oldest),
                limits,
                Cancellation::new(),
            )
            .unwrap();
        assert!(matches!(follow.next(Duration::ZERO).unwrap(), Tail::Idle));
        let b = store.namespace("b", paging::pages::SCHEMA).unwrap();
        b.try_log(paging::pages::events::Normal { value: 5 })
            .unwrap();
        b.sync().unwrap();
        assert!(
            matches!(follow.next(Duration::from_secs(2)), Err(ReadError::LimitExceeded(found)) if found == resource)
        );
    }
}

#[test]
fn last_pending_page_record_keeps_its_retention_watermark_until_delivery() {
    use dduroc::read::{Cancellation, NsSelect, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(directory.path())
            .with_budget_per_class(16 << 20)
            .channel(
                dduroc::StorageClass::Critical,
                dduroc::ChannelConfig {
                    segment_bytes: 64 << 10,
                    block_max_bytes: 8 << 10,
                    compression: dduroc::Compression::None,
                    ..dduroc::ChannelConfig::critical(128 << 10)
                },
            ),
    )
    .unwrap();
    let victim = store.namespace("victim", paging::pages::SCHEMA).unwrap();
    let pressure = store.namespace("pressure", paging::pages::SCHEMA).unwrap();
    victim
        .try_log(paging::pages::events::Large {
            payload: "x".repeat(40 << 10),
        })
        .unwrap();
    let reader = std::sync::Arc::new(store.reader());
    let mut session = reader
        .open_session(
            Query::new().namespaces(NsSelect::Names(vec!["victim".into()])),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(1, 1).unwrap()),
        Err(ReadError::PageTooSmall)
    ));
    for _ in 0..5 {
        pressure
            .try_log(paging::pages::events::Large {
                payload: "y".repeat(40 << 10),
            })
            .unwrap();
    }
    assert!(
        store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .any(|c| c.key.namespace == "victim"
                && c.key.class == dduroc::StorageClass::Critical
                && c.first_index.is_none())
    );
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(1, 1 << 20).unwrap()),
        Err(ReadError::RetentionGap)
    ));
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(1, 1 << 20).unwrap()),
        Err(ReadError::RetentionGap)
    ));
}

#[test]
fn buffered_state_seeds_cannot_outlive_retention_of_their_source_snapshot() {
    use dduroc::read::{BootTime, Cancellation, Micros, NsSelect, ReadError, ReadLimits};
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(directory.path())
            .with_budget_per_class(16 << 20)
            .channel(
                dduroc::StorageClass::Telemetry,
                dduroc::ChannelConfig {
                    segment_bytes: 64 << 10,
                    block_max_bytes: 8 << 10,
                    compression: dduroc::Compression::None,
                    ..dduroc::ChannelConfig::new(128 << 10)
                },
            ),
    )
    .unwrap();
    let victim = store.namespace("victim", paging::pages::SCHEMA).unwrap();
    let pressure = store.namespace("pressure", paging::pages::SCHEMA).unwrap();
    victim
        .series(paging::pages::metrics::Mode)
        .unwrap()
        .sample(paging::pages::metrics::Mode::On);
    victim.sync().unwrap();
    let at = store.now();
    let query = Query::new()
        .namespaces(NsSelect::Names(vec!["victim".into()]))
        .since(BootTime::new(at.boot, Micros(at.at.0 + 1)))
        .with_state_seed();
    let reader = std::sync::Arc::new(store.reader());
    let mut session = reader
        .open_session(query.clone(), ReadLimits::default(), Cancellation::new())
        .unwrap();
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(1, 1).unwrap()),
        Err(ReadError::PageTooSmall)
    ));
    let mut follow = reader
        .follow_controlled(
            query.order(Order::Oldest),
            ReadLimits::default(),
            Cancellation::new(),
        )
        .unwrap();
    let series = pressure.series(paging::pages::metrics::Blob).unwrap();
    for _ in 0..5 {
        series.sample(&vec![1u8; 40 << 10][..]);
        pressure.sync().unwrap();
    }
    assert!(
        store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .any(|c| c.key.namespace == "victim"
                && c.key.class == dduroc::StorageClass::Telemetry
                && c.first_index.is_none())
    );
    assert!(matches!(
        session.next_page(dduroc::read::PageLimits::new(1, 65536).unwrap()),
        Err(ReadError::RetentionGap)
    ));
    assert!(matches!(
        follow.next(std::time::Duration::ZERO),
        Err(ReadError::RetentionGap)
    ));
}

// Test-only persisted fixture: public Store writes complete single-record blocks;
// a fresh thread rewrites their timestamps without changing payloads, indexes,
// boot/version/retention envelope, or metadata. The separate thread lifetime also
// prevents the pinned raft-engine thread-local block cache from crossing Engines.
fn force_single_record_fixture_timestamps(root: std::path::PathBuf) {
    std::thread::spawn(move || {
        use dduroc_format::Micros;
        use dduroc_format::block::{Block, BlockBuilder, BlockHeader, Compression};
        use protobuf::well_known_types::BytesValue;
        use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
        struct FixtureEntry;
        impl MessageExt for FixtureEntry {
            type Entry = BytesValue;
            fn index(entry: &BytesValue) -> u64 {
                u64::from_le_bytes(entry.value[..8].try_into().unwrap())
            }
        }
        // SPEC: the dduroc raft v1 block envelope is index:u64, boot:u32,
        // protocol_version:u16, global_sequence:u64, followed by format block.
        const ENVELOPE: usize = 22;
        let engine = Engine::open(Config {
            dir: root.join("raft-engine").to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::mb(1),
            purge_threshold: ReadableSize::mb(16),
            enable_log_recycle: false,
            recovery_mode: RecoveryMode::AbsoluteConsistency,
            ..Config::default()
        })
        .unwrap();
        let sequence = engine.get(0, b"dduroc.sequence");
        let mut rewritten_count = 0;
        for group in engine.raft_groups() {
            let metadata = engine.get(group, b"dduroc.channel.v1");
            let Some(first) = engine.first_index(group) else {
                continue;
            };
            let last = engine.last_index(group).unwrap();
            let mut entries = Vec::new();
            for index in first..=last {
                let mut entry = engine
                    .get_entry::<FixtureEntry>(group, index)
                    .unwrap()
                    .unwrap();
                assert_eq!(FixtureEntry::index(&entry), index);
                let prefix = entry.value[..ENVELOPE].to_vec();
                let rewritten = {
                    let original = &entry.value[ENVELOPE..];
                    let block = Block::parse(original).unwrap().unwrap();
                    assert_eq!(block.header.count, 1);
                    assert_eq!(block.header.compression, Compression::None);
                    let mut records = block.records();
                    let (_, record) = records.next().unwrap().unwrap();
                    assert!(records.next().is_none());
                    let mut builder = BlockBuilder::new();
                    builder.push(Micros(123), &record).unwrap();
                    let mut bytes = Vec::new();
                    builder
                        .finish(block.header.seq, Compression::None, &mut bytes)
                        .unwrap();
                    assert_eq!(bytes.len(), original.len());
                    assert_eq!(&bytes[BlockHeader::SIZE..], &original[BlockHeader::SIZE..]);
                    bytes
                };
                entry.value.truncate(ENVELOPE);
                entry.value.extend_from_slice(&rewritten);
                assert_eq!(&entry.value[..ENVELOPE], prefix.as_slice());
                entries.push(entry);
                rewritten_count += 1;
            }
            let mut batch = LogBatch::default();
            batch.add_entries::<FixtureEntry>(group, &entries).unwrap();
            engine.write(&mut batch, true).unwrap();
            assert_eq!(engine.get(group, b"dduroc.channel.v1"), metadata);
        }
        assert_eq!(rewritten_count, 6);
        assert_eq!(engine.get(0, b"dduroc.sequence"), sequence);
    })
    .join()
    .unwrap();
}

#[test]
fn genuine_equal_timestamps_page_stably_without_duplicates_in_both_directions() {
    use dduroc::read::{Cancellation, ReadLimits, Reader};
    let directory = tempfile::tempdir().unwrap();
    {
        let store = Store::open(
            StoreConfig::new(directory.path())
                .with_budget_per_class(16 << 20)
                .channel(
                    dduroc::StorageClass::Critical,
                    dduroc::ChannelConfig {
                        compression: dduroc::Compression::None,
                        ..dduroc::ChannelConfig::critical(16 << 20)
                    },
                ),
        )
        .unwrap();
        let a = store.namespace("a", paging::pages::SCHEMA).unwrap();
        let b = store.namespace("b", paging::pages::SCHEMA).unwrap();
        for value in 0..3 {
            a.try_log(paging::pages::events::Urgent { value }).unwrap();
            b.try_log(paging::pages::events::Urgent { value: value + 10 })
                .unwrap();
        }
    }
    force_single_record_fixture_timestamps(directory.path().to_path_buf());
    let reader = std::sync::Arc::new(
        Reader::open_dump([directory.path()], &[paging::pages::SCHEMA]).unwrap(),
    );
    for (order, expected) in [
        (
            Order::Oldest,
            vec![
                ("a", 0),
                ("a", 1),
                ("a", 2),
                ("b", 10),
                ("b", 11),
                ("b", 12),
            ],
        ),
        (
            Order::Newest,
            vec![
                ("a", 2),
                ("a", 1),
                ("a", 0),
                ("b", 12),
                ("b", 11),
                ("b", 10),
            ],
        ),
    ] {
        let mut session = reader
            .open_session(
                Query::new().order(order),
                ReadLimits::default(),
                Cancellation::new(),
            )
            .unwrap();
        let mut got = Vec::new();
        let mut timestamp = None;
        for _ in 0..10 {
            let page = session
                .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
                .unwrap();
            assert!(page.damaged.is_empty());
            for entry in page.entries {
                assert_eq!(entry.at.at.0, 123);
                assert_eq!(*timestamp.get_or_insert(entry.at), entry.at);
                let value = reader
                    .decode::<paging::pages::events::Urgent>(&entry)
                    .unwrap()
                    .unwrap()
                    .value;
                got.push((entry.namespace.to_string(), value));
            }
            if page.exhausted {
                break;
            }
        }
        assert_eq!(
            got,
            expected
                .into_iter()
                .map(|(ns, value)| (ns.to_owned(), value))
                .collect::<Vec<_>>()
        );
        assert!(
            session
                .next_page(dduroc::read::PageLimits::new(1, 65536).unwrap())
                .unwrap()
                .exhausted
        );
    }
}
