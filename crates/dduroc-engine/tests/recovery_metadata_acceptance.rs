#![cfg(feature = "recovery")]
//! Strict recovery must expose damaged interpretation metadata, without fixing evidence.
use dduroc_engine::recovery::{RecoveryOptions, recover_copy};
use dduroc_engine::{EventDesc, Language, Schema, StorageClass, Store, StoreConfig};
use dduroc_format::{EventId, Level, ProtocolVersion};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

const SCHEMA: Schema = Schema {
    identity: None,
    name: "recovery_metadata",
    version: ProtocolVersion(1),
    languages: &[Language("en")],
    events: &[EventDesc {
        id: EventId(1),
        name: "Ack",
        level: Level::Error,
        class: StorageClass::Critical,
        tags: &[],
        templates: &["ack"],
        fields: &[],
        decoders: None,
    }],
    metrics: &[],
    spans: &[],
    migrations: &[],
};
fn fixture(root: &Path) -> StoreConfig {
    let config = StoreConfig::new(root);
    let store = Store::open(config.clone()).unwrap();
    let namespace = store.namespace("radio", SCHEMA).unwrap();
    namespace.try_log_raw(EventId(1), b"ack", None).unwrap();
    drop(namespace);
    drop(store);
    config
}
fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut result = BTreeMap::new();
    let mut queue = vec![root.to_owned()];
    while let Some(dir) = queue.pop() {
        for item in std::fs::read_dir(dir).unwrap() {
            let path = item.unwrap().path();
            if path.is_dir() {
                queue.push(path);
            } else {
                result.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    result
}
fn damaged(change: impl FnOnce(&Path)) {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(&temp.path().join("source"));
    change(&config.root);
    let before = files(&config.root);
    let destination = temp.path().join("copy");
    let report = recover_copy(
        &config,
        &destination,
        &RecoveryOptions::default(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(report.evidence_complete && report.complete);
    assert!(
        !report.strict_inspection_succeeded,
        "damaged interpretation metadata must not pass strict inspection"
    );
    assert!(report.inspection_error.is_some());
    assert!(!report.repair_attempted);
    assert_eq!(files(&config.root), before);
    assert_eq!(files(&destination.join("evidence/root-0")), before);
}
#[test]
fn corrupt_epochs_cannot_be_silently_replaced_with_empty_anchor_metadata() {
    damaged(|root| std::fs::write(root.join("epochs.bin"), [0xff]).unwrap());
}
#[test]
fn missing_epochs_are_reported_even_though_normal_readers_allow_boot_time_fallback() {
    damaged(|root| std::fs::remove_file(root.join("epochs.bin")).unwrap());
}
#[test]
fn valid_empty_epochs_cannot_hide_a_retained_records_missing_run() {
    damaged(|root| {
        std::fs::write(
            root.join("epochs.bin"),
            postcard::to_allocvec(&dduroc_engine::Epochs::default()).unwrap(),
        )
        .unwrap()
    });
}
#[test]
fn malformed_namespace_metadata_is_not_a_successful_recovery() {
    damaged(|root| std::fs::write(root.join("radio/ns-meta"), [0xff]).unwrap());
}
#[test]
fn missing_namespace_metadata_is_not_a_successful_recovery() {
    damaged(|root| std::fs::remove_file(root.join("radio/ns-meta")).unwrap());
}
#[test]
fn foreign_namespace_schema_name_is_rejected_against_channel_catalog() {
    damaged(|root| {
        let meta = dduroc_engine::namespace::NsMeta {
            schema_name: "other_schema".into(),
            protocol_version: 1,
        };
        std::fs::write(
            root.join("radio/ns-meta"),
            postcard::to_allocvec(&meta).unwrap(),
        )
        .unwrap();
    });
}
#[test]
fn genuinely_unanchored_but_intact_epochs_still_pass_strict_inspection() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(&temp.path().join("source"));
    let before = files(&config.root);
    let report = recover_copy(
        &config,
        &temp.path().join("copy"),
        &RecoveryOptions::default(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(
        report.strict_inspection_succeeded,
        "{:?}",
        report.inspection_error
    );
    assert_eq!(files(&config.root), before);
}

#[test]
fn pending_and_completed_migration_keep_valid_older_closed_class_metadata() {
    const NEW: Schema = Schema {
        version: ProtocolVersion(2),
        events: &[EventDesc {
            id: EventId(1),
            name: "Ack",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &[],
            templates: &["ack"],
            fields: &[],
            decoders: None,
        }],
        migrations: &[dduroc_engine::schema::Migration {
            from: 1,
            touches_all: false,
            events: &[],
            metrics: &[],
            spans: &[],
            migrate: |_| panic!("unchanged records must not be transformed"),
        }],
        ..SCHEMA
    };
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(&temp.path().join("source"));
    for (phase, complete) in [("pending", false), ("completed", true)] {
        {
            let store = Store::open(config.clone()).unwrap();
            let namespace = store.namespace("radio", NEW).unwrap();
            if complete {
                namespace.migrate().unwrap();
            }
        }
        let meta: dduroc_engine::namespace::NsMeta =
            postcard::from_bytes(&std::fs::read(config.root.join("radio/ns-meta")).unwrap())
                .unwrap();
        assert_eq!(meta.protocol_version, if complete { 2 } else { 1 });
        let before = files(&config.root);
        let report = recover_copy(
            &config,
            &temp.path().join(phase),
            &RecoveryOptions::default(),
            &AtomicBool::new(false),
        )
        .unwrap();
        assert!(
            report.strict_inspection_succeeded,
            "{phase}: {:?}",
            report.inspection_error
        );
        assert_eq!(files(&config.root), before);
    }
}

#[test]
fn duplicate_kernel_identity_fails_strict_inspection() {
    damaged(|root| {
        let path = root.join("epochs.bin");
        let mut epochs: dduroc_engine::Epochs =
            postcard::from_bytes(&std::fs::read(&path).unwrap()).unwrap();
        let mut duplicate = epochs.hw_boots[0];
        duplicate.hw_boot_id = duplicate.hw_boot_id.checked_add(1).unwrap();
        epochs.hw_boots.push(duplicate);
        std::fs::write(path, postcard::to_allocvec(&epochs).unwrap()).unwrap();
    });
}

#[test]
fn invalid_utc_anchor_fails_strict_inspection() {
    damaged(|root| {
        let path = root.join("epochs.bin");
        let mut epochs: dduroc_engine::Epochs =
            postcard::from_bytes(&std::fs::read(&path).unwrap()).unwrap();
        epochs.hw_boots[0].utc_anchor_ms = Some(i64::MAX);
        std::fs::write(path, postcard::to_allocvec(&epochs).unwrap()).unwrap();
    });
}
