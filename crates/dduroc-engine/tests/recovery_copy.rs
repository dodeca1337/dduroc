#![cfg(feature = "recovery")]
//! Disposable host-file acceptance; no physical power-loss claim.
use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use dduroc_engine::recovery::{
    ManifestEntry, RecoveryError, RecoveryMode, RecoveryOptions, recover_copy,
};
use dduroc_engine::{ChannelConfig, EventDesc, Language, Schema, StorageClass, Store, StoreConfig};
use dduroc_format::{EventId, Level, ProtocolVersion};
use sha2::{Digest, Sha256};

fn fixture(base: &Path, custom: bool) -> StoreConfig {
    let mut config = StoreConfig::new(base.join("source")).with_budget_per_class(16 << 20);
    if custom {
        config = config.channel(
            StorageClass::Critical,
            ChannelConfig {
                custom_root: Some(base.join("critical")),
                ..ChannelConfig::critical(16 << 20)
            },
        );
    }
    let store = Store::open(config.clone()).unwrap();
    let namespace = store
        .namespace(
            "radio",
            Schema {
                identity: None,
                name: "recovery",
                version: ProtocolVersion(1),
                languages: &[Language("en")],
                events: &[EventDesc {
                    id: EventId(1),
                    name: "Reading",
                    level: Level::Error,
                    class: StorageClass::Critical,
                    tags: &[],
                    templates: &["reading"],
                    fields: &[],
                    decoders: None,
                }],
                metrics: &[],
                spans: &[],
                migrations: &[],
            },
        )
        .unwrap();
    namespace
        .try_log_raw(EventId(1), b"first ack", None)
        .unwrap();
    namespace
        .try_log_raw(EventId(1), b"second ack", None)
        .unwrap();
    store.sync().unwrap();
    drop(namespace);
    store.shutdown();
    drop(store);
    config
}
fn files(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for item in fs::read_dir(dir).unwrap() {
            let item = item.unwrap();
            if item.file_type().unwrap().is_dir() {
                pending.push(item.path());
            } else if item.file_type().unwrap().is_file() {
                out.insert(
                    item.path().strip_prefix(root).unwrap().to_path_buf(),
                    fs::read(item.path()).unwrap(),
                );
            }
        }
    }
    out
}
fn recover(
    config: &StoreConfig,
    target: &Path,
) -> Result<dduroc_engine::recovery::RecoveryReport, RecoveryError> {
    recover_copy(
        config,
        target,
        &RecoveryOptions::default(),
        &AtomicBool::new(false),
    )
}
fn assert_evidence(target: &Path, roots: &[BTreeMap<PathBuf, Vec<u8>>]) {
    let manifest: Vec<ManifestEntry> =
        serde_json::from_slice(&fs::read(target.join("evidence-manifest.json")).unwrap()).unwrap();
    assert_eq!(
        manifest.len(),
        roots.iter().map(BTreeMap::len).sum::<usize>()
    );
    for entry in manifest {
        let bytes = &roots[entry.root][&entry.path];
        assert_eq!(entry.bytes, bytes.len() as u64);
        assert_eq!(entry.sha256, format!("{:x}", Sha256::digest(bytes)));
        assert_eq!(
            fs::read(
                target
                    .join(format!("evidence/root-{}", entry.root))
                    .join(&entry.path)
            )
            .unwrap(),
            *bytes
        );
    }
}
#[test]
fn strict_copy_checks_records_and_preserves_every_source_byte() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    let before = files(&config.root);
    let target = temp.path().join("recovered");
    let report = recover(&config, &target).unwrap();
    assert!(report.complete && report.evidence_complete && report.strict_inspection_succeeded);
    assert!(!report.loss_possible);
    assert!(report.initial_strict_error.is_none());
    assert!(!report.repair_attempted);
    assert!(report.inspected_blocks > 0);
    assert_eq!(files(&config.root), before);
    assert_evidence(&target, &[before]);
    let json = fs::read_to_string(target.join("report.json")).unwrap();
    assert!(!json.contains(config.root.to_str().unwrap()));
}
#[test]
fn custom_critical_root_mapping_and_identity_are_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), true);
    let vault = config.channels[&StorageClass::Critical]
        .custom_root
        .as_ref()
        .unwrap();
    let before = [files(&config.root), files(vault)];
    let target = temp.path().join("recovered");
    let report = recover(&config, &target).unwrap();
    assert!(
        report.strict_inspection_succeeded,
        "{:?}",
        report.inspection_error
    );
    assert_eq!(report.roots.len(), 2);
    assert_eq!(report.roots[1].classes, ["critical"]);
    assert_eq!(files(&config.root), before[0]);
    assert_eq!(files(vault), before[1]);
    assert_evidence(&target, &before);
}
#[test]
fn active_writer_and_custom_root_lock_prevent_copy() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), true);
    let active = Store::open(config.clone()).unwrap();
    let target = temp.path().join("recovered");
    let before = files(&config.root);
    assert!(matches!(
        recover(&config, &target),
        Err(RecoveryError::Busy(0))
    ));
    assert!(!target.exists());
    assert_eq!(files(&config.root), before);
    active.shutdown();
    drop(active);
    let vault = config.channels[&StorageClass::Critical]
        .custom_root
        .as_ref()
        .unwrap();
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(vault.join("raft-engine/LOCK"))
        .unwrap();
    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    assert!(matches!(
        recover(&config, &target),
        Err(RecoveryError::Busy(1))
    ));
    assert!(!target.exists());
}
#[test]
fn missing_source_lock_is_never_recreated() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    fs::remove_file(config.root.join(".lock")).unwrap();
    let before = files(&config.root);
    assert!(recover(&config, &temp.path().join("copy")).is_err());
    assert_eq!(files(&config.root), before);
    assert!(!config.root.join(".lock").exists());
}
#[test]
fn overlap_existing_destination_symlinks_and_special_files_are_refused() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    assert!(recover(&config, &config.root.join("copy")).is_err());
    assert!(!config.root.join("copy").exists());
    assert!(recover(&config, temp.path()).is_err());
    let target = temp.path().join("copy");
    symlink(&config.root, config.root.join("link")).unwrap();
    assert!(recover(&config, &target).is_err());
    assert!(!target.exists());
    fs::remove_file(config.root.join("link")).unwrap();
    let socket = std::os::unix::net::UnixListener::bind(config.root.join("socket")).unwrap();
    assert!(recover(&config, &target).is_err());
    assert!(!target.exists());
    drop(socket);
}
#[test]
fn bounds_cancellation_and_expired_deadline_precede_destination_creation() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    let target = temp.path().join("copy");
    for options in [
        RecoveryOptions {
            max_source_bytes: 1,
            ..Default::default()
        },
        RecoveryOptions {
            max_entries: 1,
            ..Default::default()
        },
        RecoveryOptions {
            deadline: Some(Instant::now()),
            ..Default::default()
        },
    ] {
        assert!(recover_copy(&config, &target, &options, &AtomicBool::new(false)).is_err());
        assert!(!target.exists());
    }
    assert!(matches!(
        recover_copy(
            &config,
            &target,
            &RecoveryOptions::default(),
            &AtomicBool::new(true)
        ),
        Err(RecoveryError::Cancelled)
    ));
    assert!(!target.exists());
}
#[test]
fn corrupt_tail_is_strictly_refused_and_only_explicit_candidate_can_be_repaired() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    let mut logs: Vec<_> = fs::read_dir(config.root.join("raft-engine"))
        .unwrap()
        .map(|i| i.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "raftlog"))
        .collect();
    logs.sort();
    let tail = fs::OpenOptions::new()
        .write(true)
        .open(logs.last().unwrap())
        .unwrap();
    tail.set_len(tail.metadata().unwrap().len() - 3).unwrap();
    tail.sync_all().unwrap();
    drop(tail);
    let before = files(&config.root);
    let strict_target = temp.path().join("strict");
    let strict = recover(&config, &strict_target).unwrap();
    assert!(strict.complete && strict.evidence_complete);
    assert!(!strict.strict_inspection_succeeded && !strict.loss_possible);
    assert!(strict.inspection_error.is_some());
    assert_eq!(files(&config.root), before);
    assert_evidence(&strict_target, std::slice::from_ref(&before));
    let repair_target = temp.path().join("repair");
    let repaired = recover_copy(
        &config,
        &repair_target,
        &RecoveryOptions {
            mode: RecoveryMode::RepairCopyAllowDataLoss,
            ..Default::default()
        },
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(repaired.complete && repaired.loss_possible && repaired.repair_attempted);
    assert!(repaired.initial_strict_error.is_some());
    assert!(!repaired.changed_candidate_files.is_empty());
    assert_eq!(files(&config.root), before);
    assert_evidence(&repair_target, &[before]);
}
#[test]
fn foreign_custom_root_is_reported_without_mutating_either_source() {
    let temp = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let mut config = fixture(temp.path(), false);
    let foreign = fixture(other.path(), false);
    config = config.channel(
        StorageClass::Critical,
        ChannelConfig {
            custom_root: Some(foreign.root.clone()),
            ..ChannelConfig::critical(16 << 20)
        },
    );
    let before = [files(&config.root), files(&foreign.root)];
    let target = temp.path().join("copy");
    let report = recover(&config, &target).unwrap();
    assert!(!report.strict_inspection_succeeded);
    assert!(report.inspection_error.unwrap().contains("foreign root"));
    assert_eq!(files(&config.root), before[0]);
    assert_eq!(files(&foreign.root), before[1]);
    assert_evidence(&target, &before);
}

#[test]
fn cancellation_during_copy_preserves_partial_artifacts_with_incomplete_report() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    // Plenty of chunk boundaries for cooperative cancellation, without a
    // dependency on a particular storage throughput or arbitrary sleep.
    let extra = fs::File::create(config.root.join("large-evidence")).unwrap();
    extra.set_len(64 * 1024 * 1024).unwrap();
    drop(extra);
    let target = temp.path().join("copy");
    let cancel = Arc::new(AtomicBool::new(false));
    let observer = Arc::clone(&cancel);
    let report_path = target.join("report.json");
    let worker = std::thread::spawn(move || {
        let until = Instant::now() + Duration::from_secs(10);
        while !report_path.exists() {
            assert!(Instant::now() < until, "copy never created initial report");
            std::thread::yield_now();
        }
        observer.store(true, Ordering::Relaxed);
    });
    let result = recover_copy(&config, &target, &RecoveryOptions::default(), &cancel);
    worker.join().unwrap();
    assert!(matches!(result, Err(RecoveryError::Cancelled)));
    assert!(target.exists());
    let report: dduroc_engine::recovery::RecoveryReport =
        serde_json::from_slice(&fs::read(target.join("report.json")).unwrap()).unwrap();
    assert!(!report.complete);
    assert!(report.inspection_error.unwrap().contains("cancelled"));
    assert_eq!(
        fs::metadata(config.root.join("large-evidence"))
            .unwrap()
            .len(),
        64 * 1024 * 1024
    );
}

#[test]
fn repair_opt_in_on_clean_input_does_not_run_tolerant_recovery() {
    let temp = tempfile::tempdir().unwrap();
    let config = fixture(temp.path(), false);
    let report = recover_copy(
        &config,
        &temp.path().join("copy"),
        &RecoveryOptions {
            mode: RecoveryMode::RepairCopyAllowDataLoss,
            ..Default::default()
        },
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(report.strict_inspection_succeeded && report.loss_possible);
    assert!(!report.repair_attempted);
    assert!(report.initial_strict_error.is_none());
}
