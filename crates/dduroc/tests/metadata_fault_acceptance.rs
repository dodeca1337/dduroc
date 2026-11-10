//! Public acceptance of metadata persistence failures in a disposable root.
#![cfg(feature = "read")]

use dduroc::prelude::*;
use dduroc::read::{Query, Reader};

dduroc::schema! {
    name: metadata_fault_fixture, version: 1, languages: [en],
    events {
        Tick = 1 { level: Info, en: "tick {value}", value: u32 },
        Critical = 2 { level: Error, store: critical, en: "critical" },
    }
}

#[test]
fn failed_anchor_persistence_does_not_publish_a_live_only_clock_mapping() {
    let directory = tempfile::tempdir().unwrap();
    {
        let store = Store::open(StoreConfig::new(directory.path())).unwrap();
        let namespace = store
            .namespace("clock", metadata_fault_fixture::SCHEMA)
            .unwrap();
        namespace
            .try_log(metadata_fault_fixture::events::Tick { value: 7 })
            .unwrap();
        store.sync().unwrap();
        let reader = store.reader();
        let original = reader.query(&Query::new()).unwrap();
        assert_eq!(original.entries.len(), 1);
        let at = original.entries[0].at;
        assert_eq!(store.to_utc(at), None);
        let original_bytes = std::fs::read(directory.path().join("epochs.bin")).unwrap();

        // A directory at the temporary-file path deterministically refuses
        // file creation before rename. This is an injected filesystem refusal,
        // not evidence of an actual full disk or physical power interruption.
        let temporary = directory.path().join("epochs.bin.tmp");
        std::fs::create_dir(&temporary).unwrap();
        assert!(store.record_sync(Utc::now(), SyncSource::User).is_err());
        assert_eq!(
            std::fs::read(directory.path().join("epochs.bin")).unwrap(),
            original_bytes
        );
        assert_eq!(
            store.to_utc(at),
            None,
            "a refused anchor must not become visible only in RAM"
        );
        let live = reader.query(&Query::new()).unwrap();
        assert_eq!(live.entries[0].at, at);
        assert_eq!(live.entries[0].utc, None);
        std::fs::remove_dir(temporary).unwrap();
        assert!(
            store.record_sync(Utc::now(), SyncSource::User).is_err(),
            "the uncertain backend must remain fenced even after the obstruction is removed"
        );
        assert!(store.sync().is_err());
        assert!(
            namespace
                .try_log(metadata_fault_fixture::events::Critical)
                .is_err(),
            "no critical ACK after the metadata failure"
        );
    }
    let offline = Reader::open_dump([directory.path()], &[metadata_fault_fixture::SCHEMA]).unwrap();
    let recovered = offline.query(&Query::new()).unwrap();
    assert_eq!(recovered.entries.len(), 1);
    assert_eq!(recovered.entries[0].utc, None);
}
