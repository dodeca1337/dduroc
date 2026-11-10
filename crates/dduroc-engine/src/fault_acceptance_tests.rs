//! Public Store acceptance with injected syscall errors, not physical power loss.
use crate::fsutil::{FaultStage, fail_next};
use crate::{EventDesc, Language, Schema, StorageClass, Store, StoreConfig, SyncSource};
use chrono::{Duration as ChronoDuration, Utc};
use dduroc_format::{EventId, Level, ProtocolVersion};
use std::time::Duration;

const SCHEMA: Schema = Schema {
    identity: None,
    name: "fault_acceptance",
    version: ProtocolVersion(1),
    languages: &[Language("en")],
    events: &[EventDesc {
        id: EventId(1),
        name: "Critical",
        level: Level::Error,
        class: StorageClass::Critical,
        tags: &[],
        templates: &["critical"],
        fields: &[],
        decoders: None,
    }],
    metrics: &[],
    spans: &[],
    migrations: &[],
};

fn anchor_failure(stage: FaultStage, errno: i32) {
    // A lost worker reply must fail this test, rather than hang the entire
    // suite. This timeout is test supervision, not a production I/O timeout.
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let directory = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(directory.path());
        let store = Store::open(config.clone()).unwrap();
        let namespace = store.namespace("radio", SCHEMA).unwrap();
        namespace
            .try_log_raw(EventId(1), b"prior-ack", None)
            .unwrap();
        let at = store.now();
        let first = Utc::now();
        store.record_sync(first, SyncSource::User).unwrap();
        let original = store.to_utc(at).unwrap();
        let epochs_path = directory.path().join("epochs.bin");
        let before = std::fs::read(&epochs_path).unwrap();
        {
            let fault = fail_next(stage, errno);
            assert!(
                store
                    .record_sync(first + ChronoDuration::days(1), SyncSource::Gps)
                    .is_err()
            );
            assert!(
                fault.consumed(),
                "the intended syscall stage must be reached"
            );
        }
        // No publication of an unconfirmed anchor, even after a successful
        // rename followed by a refused directory fsync.
        assert_eq!(store.to_utc(at), Some(original));
        assert_eq!(store.epochs().to_utc(at), Some(original));
        let after = std::fs::read(&epochs_path).unwrap();
        if matches!(stage, FaultStage::DirectorySync) {
            assert_ne!(
                after, before,
                "rename occurred before injected directory-sync failure"
            );
        } else {
            assert_eq!(
                after, before,
                "failure before rename must preserve the old authority"
            );
        }
        assert!(
            store.record_sync(first, SyncSource::User).is_err(),
            "failure is sticky"
        );
        assert!(
            store.sync().is_err(),
            "a later barrier must not report success"
        );
        assert!(
            namespace
                .try_log_raw(EventId(1), b"unconfirmed", None)
                .is_err(),
            "no critical ACK after fencing"
        );
        drop(namespace);
        drop(store);
        // Reopen follows the actual authoritative file. After DirectorySync
        // failure the new file is visible on this running OS; this assertion
        // deliberately makes no claim about which version survives power loss.
        let disk_epochs = crate::EpochStore::open_read_only(directory.path()).unwrap();
        let recovered = Store::open(config).unwrap();
        assert_eq!(recovered.to_utc(at), disk_epochs.to_utc(at));
        if !matches!(stage, FaultStage::DirectorySync) {
            assert_eq!(recovered.to_utc(at), Some(original));
        }
        let backend = recovered.read_backend();
        let channels = backend.snapshot_channels(|_| Ok(true)).unwrap();
        let mut payloads = Vec::new();
        for snapshot in channels {
            let info = snapshot.info;
            let (Some(first), Some(last)) = (info.first_index, info.last_index) else {
                continue;
            };
            let blocks = backend
                .read_blocks(&info.key, first, last + 1, 64, 8 * 1024 * 1024, false)
                .unwrap();
            for bytes in blocks.blocks {
                let block = dduroc_format::Block::parse(&bytes.bytes).unwrap().unwrap();
                for record in block.records() {
                    let (_, record) = record.unwrap();
                    let dduroc_format::Record::Message(message) = record else {
                        panic!("unexpected non-message record")
                    };
                    payloads.push(message.payload.to_vec());
                }
            }
        }
        assert_eq!(
            payloads,
            vec![b"prior-ack".to_vec()],
            "only the previously acknowledged record is retained"
        );
        drop(backend);
        drop(recovered);
        send.send(()).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(10))
        .expect("faulted Store operations or Drop did not finish");
    worker.join().unwrap();
}

macro_rules! case {
    ($name:ident, $stage:ident, $errno:expr) => {
        #[test]
        fn $name() {
            anchor_failure(FaultStage::$stage, $errno);
        }
    };
}
case!(enospc_create_preserves_ack_and_fences, Create, 28);
case!(enospc_write_preserves_ack_and_fences, Write, 28);
case!(enospc_file_sync_preserves_ack_and_fences, FileSync, 28);
case!(enospc_rename_preserves_ack_and_fences, Rename, 28);
case!(
    enospc_directory_sync_is_unconfirmed_and_fences,
    DirectorySync,
    28
);
case!(eio_create_preserves_ack_and_fences, Create, 5);
case!(eio_write_preserves_ack_and_fences, Write, 5);
case!(eio_file_sync_preserves_ack_and_fences, FileSync, 5);
case!(eio_rename_preserves_ack_and_fences, Rename, 5);
case!(
    eio_directory_sync_is_unconfirmed_and_fences,
    DirectorySync,
    5
);

fn critical_commit_failure(fault: crate::backend::CommitFault, committed: bool) {
    let (send, receive) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let directory = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(directory.path());
        let store = Store::open(config.clone()).unwrap();
        let namespace = store.namespace("radio", SCHEMA).unwrap();
        namespace
            .try_log_raw(EventId(1), b"prior-ack", None)
            .unwrap();
        let backend = store.read_backend();
        backend.inject_commit_fault(fault);
        // This seam models an adapter error before write or after successful
        // sync but before ACK. It does not inject a real Raft syscall failure.
        assert!(
            namespace
                .try_log_raw(EventId(1), b"uncertain", None)
                .is_err()
        );
        assert!(
            namespace
                .try_log_raw(EventId(1), b"must-not-write", None)
                .is_err()
        );
        assert!(store.sync().is_err());
        assert!(
            backend.cached_statistics().is_err(),
            "fenced backend cannot present stale COMPLETE retained counts"
        );
        drop(backend);
        drop(namespace);
        drop(store);
        let reopened = Store::open(config).unwrap();
        let backend = reopened.read_backend();
        let mut payloads = Vec::new();
        for snapshot in backend.snapshot_channels(|_| Ok(true)).unwrap() {
            let info = snapshot.info;
            let (Some(first), Some(last)) = (info.first_index, info.last_index) else {
                continue;
            };
            let page = backend
                .read_blocks(&info.key, first, last + 1, 64, 8 * 1024 * 1024, false)
                .unwrap();
            for bytes in page.blocks {
                let block = dduroc_format::Block::parse(&bytes.bytes).unwrap().unwrap();
                for record in block.records() {
                    let (_, dduroc_format::Record::Message(message)) = record.unwrap() else {
                        panic!("unexpected record")
                    };
                    payloads.push(message.payload.to_vec());
                }
            }
        }
        let mut expected = vec![b"prior-ack".to_vec()];
        if committed {
            expected.push(b"uncertain".to_vec());
        }
        assert_eq!(
            payloads, expected,
            "an error must not cause an automatic replay or lose a prior ACK"
        );
        drop(backend);
        drop(reopened);
        send.send(()).unwrap();
    });
    receive
        .recv_timeout(Duration::from_secs(10))
        .expect("failed commit or Store Drop did not finish");
    worker.join().unwrap();
}
#[test]
fn critical_before_eio_has_no_ack_no_retry_and_keeps_prior_ack() {
    critical_commit_failure(crate::backend::CommitFault::BeforeEio, false);
}
#[test]
fn critical_before_enospc_has_no_ack_no_retry_and_keeps_prior_ack() {
    critical_commit_failure(crate::backend::CommitFault::BeforeEnospc, false);
}
#[test]
fn critical_after_eio_is_unconfirmed_despite_persisted_record() {
    critical_commit_failure(crate::backend::CommitFault::AfterEio, true);
}
#[test]
fn critical_after_enospc_is_unconfirmed_despite_persisted_record() {
    critical_commit_failure(crate::backend::CommitFault::AfterEnospc, true);
}

#[test]
fn busy_custom_root_is_refused_before_its_writer_compatibility_marker_changes() {
    let directory = tempfile::tempdir().unwrap();
    let custom = directory.path().join("critical");
    let config = StoreConfig::new(directory.path().join("main")).channel(
        StorageClass::Critical,
        crate::ChannelConfig {
            custom_root: Some(custom.clone()),
            ..crate::ChannelConfig::critical(16 << 20)
        },
    );
    {
        let store = Store::open(config.clone()).unwrap();
        let namespace = store.namespace("radio", SCHEMA).unwrap();
        namespace
            .try_log_raw(EventId(1), b"prior-ack", None)
            .unwrap();
    }
    let marker = custom.join("dduroc-raft-v1");
    std::fs::write(&marker, b"dduroc raft store v1\n").unwrap();
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(custom.join("raft-engine/LOCK"))
        .unwrap();
    rustix::fs::flock(&lock, rustix::fs::FlockOperation::NonBlockingLockExclusive).unwrap();
    assert!(Store::open(config).is_err());
    assert_eq!(
        std::fs::read(marker).unwrap(),
        b"dduroc raft store v1\n",
        "a refused open must not upgrade a root owned by another process"
    );
}

#[test]
fn inactive_foreign_custom_root_is_refused_before_compatibility_upgrade() {
    let directory = tempfile::tempdir().unwrap();
    let main = directory.path().join("main");
    let foreign = directory.path().join("foreign");
    for root in [&main, &foreign] {
        let store = Store::open(StoreConfig::new(root)).unwrap();
        let namespace = store.namespace("radio", SCHEMA).unwrap();
        namespace
            .try_log_raw(EventId(1), b"prior-ack", None)
            .unwrap();
    }
    let marker = foreign.join("dduroc-raft-v1");
    std::fs::write(&marker, b"dduroc raft store v1\n").unwrap();
    let metadata = std::fs::read(foreign.join("store-meta")).unwrap();
    let config = StoreConfig::new(main).channel(
        StorageClass::Critical,
        crate::ChannelConfig {
            custom_root: Some(foreign.clone()),
            ..crate::ChannelConfig::critical(16 << 20)
        },
    );
    assert!(Store::open(config).is_err());
    assert_eq!(
        std::fs::read(marker).unwrap(),
        b"dduroc raft store v1\n",
        "foreign store must not be marked upgraded by a rejected opener"
    );
    assert_eq!(std::fs::read(foreign.join("store-meta")).unwrap(), metadata);
}
