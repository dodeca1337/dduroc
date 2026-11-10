//! Independent regressions for release audit findings, using public engine APIs.
use dduroc_engine::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};
use dduroc_engine::{EpochStore, EventDesc, Language, Schema, StorageClass, Store, StoreConfig};
use dduroc_format::{EventId, Level, ProtocolVersion};

const EVENTS: &[EventDesc] = &[EventDesc {
    id: EventId(1),
    name: "Critical",
    level: Level::Error,
    class: StorageClass::Critical,
    tags: &[],
    templates: &["critical"],
    fields: &[],
    decoders: None,
}];
fn reject_first_record(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
    Err(DecodeError)
}
const MIGRATIONS: &[Migration] = &[Migration {
    from: 1,
    touches_all: true,
    events: &[],
    metrics: &[],
    spans: &[],
    migrate: reject_first_record,
}];
fn schema(version: u16) -> Schema {
    Schema {
        identity: None,
        name: "core-release-audit",
        version: ProtocolVersion(version),
        languages: &[Language("en")],
        events: EVENTS,
        metrics: &[],
        spans: &[],
        migrations: if version == 1 { &[] } else { MIGRATIONS },
    }
}

#[test]
fn failed_first_migration_record_does_not_poison_later_namespace_registration() {
    let directory = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(directory.path());
    {
        let store = Store::open(config.clone()).unwrap();
        let namespace = store.namespace("original", schema(1)).unwrap();
        namespace
            .try_log_raw(EventId(1), b"retained", None)
            .unwrap();
        store.sync().unwrap();
    }
    {
        let store = Store::open(config.clone()).unwrap();
        let namespace = store.namespace("original", schema(2)).unwrap();
        assert!(
            namespace.migrate().is_err(),
            "the callback deliberately refuses"
        );
        let another = store.namespace("another", schema(2)).unwrap();
        another
            .try_log_raw(EventId(1), b"also-retained", None)
            .unwrap();
        store.sync().unwrap();
    }
    let reopened = Store::open(config);
    assert!(
        reopened.is_ok(),
        "a refused migration must not poison a subsequent open: {reopened:?}"
    );
}

#[test]
fn exhausted_run_counter_is_refused_without_reusing_a_retained_run() {
    let directory = tempfile::tempdir().unwrap();
    let result = EpochStore::open_and_register(directory.path(), 1, &|| Ok(Some(u32::MAX)));
    assert!(
        result.is_err(),
        "there is no fresh run identity above u32::MAX"
    );
    assert!(!directory.path().join("epochs.bin").exists());
}

#[test]
fn clean_shutdown_closes_the_public_pulse() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let namespace = store.namespace("original", schema(1)).unwrap();
    namespace
        .try_log_raw(EventId(1), b"retained", None)
        .unwrap();
    store.shutdown();
    assert!(
        store.pulse().beat().closed,
        "followers need a final close sweep"
    );
}

#[test]
fn parseable_empty_epochs_never_reuses_a_retained_run() {
    let directory = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(directory.path());
    let prior;
    {
        let store = Store::open(config.clone()).unwrap();
        let namespace = store.namespace("original", schema(1)).unwrap();
        namespace
            .try_log_raw(EventId(1), b"retained", None)
            .unwrap();
        prior = store.boot_counter();
    }
    std::fs::write(
        directory.path().join("epochs.bin"),
        postcard::to_allocvec(&dduroc_engine::Epochs::default()).unwrap(),
    )
    .unwrap();
    if let Ok(reopened) = Store::open(config) {
        assert!(
            reopened.boot_counter() > prior,
            "retained run must never be reused even if epochs decodes"
        );
    }
}

#[test]
fn runtime_thresholds_reject_inverted_nan_and_blob_bounds() {
    use dduroc_engine::{MetricDesc, MetricKind, MetricLimits, Range, Thresholds};
    use dduroc_format::{MetricId, ValueType};
    const METRICS: &[MetricDesc] = &[
        MetricDesc {
            id: MetricId(1),
            name: "numeric",
            value_type: ValueType::F64,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: Thresholds::NONE,
            warn_if: None,
            alarm_if: None,
        },
        MetricDesc {
            id: MetricId(2),
            name: "binary",
            value_type: ValueType::Blob,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: Thresholds::NONE,
            warn_if: None,
            alarm_if: None,
        },
    ];
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(directory.path())).unwrap();
    let namespace = store
        .namespace(
            "limits",
            Schema {
                metrics: METRICS,
                ..schema(1)
            },
        )
        .unwrap();
    for invalid in [
        Thresholds {
            warn: Range {
                min: Some(10.0),
                max: Some(0.0),
            },
            alarm: Range::NONE,
        },
        Thresholds {
            warn: Range {
                min: Some(f64::NAN),
                max: None,
            },
            alarm: Range::NONE,
        },
    ] {
        assert!(
            namespace
                .set_limits_raw(MetricId(1), MetricLimits::numeric(invalid))
                .is_err(),
            "a malformed runtime threshold must be refused: {invalid:?}"
        );
    }
    assert!(
        namespace
            .set_limits_raw(
                MetricId(2),
                MetricLimits::numeric(Thresholds::new(..=10.0, ..=20.0))
            )
            .is_err(),
        "a binary payload cannot be diagnosed against numeric bounds"
    );
}

#[test]
fn impossible_block_size_is_a_configuration_error_not_a_panic() {
    let directory = tempfile::tempdir().unwrap();
    let config = StoreConfig::new(directory.path()).channel(
        StorageClass::Default,
        dduroc_engine::ChannelConfig {
            block_max_bytes: usize::MAX,
            ..dduroc_engine::ChannelConfig::new(64 << 20)
        },
    );
    let result = std::panic::catch_unwind(|| Store::open(config));
    assert!(
        matches!(result, Ok(Err(_))),
        "invalid user configuration must be refused without unwinding"
    );
}
