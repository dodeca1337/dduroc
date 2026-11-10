//! EXP-0043: bounded host characterization, deliberately excluded from routine gates.
use dduroc_engine::store::NsQuota;
use dduroc_engine::{ChannelConfig, EventDesc, Language, Schema, StorageClass, Store, StoreConfig};
use dduroc_format::{Block, Compression, EventId, Level, ProtocolVersion, Record};
use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

const MIB: u64 = 1 << 20;
const COLD: usize = 48;
const HOT: usize = 96;
const PAYLOAD: usize = 512 << 10;

fn schema() -> Schema {
    Schema {
        identity: None,
        name: "rewrite-headroom",
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
    }
}

// File identities prevent counting both a removed old generation and its
// replacement as if they coexisted. Raft files only grow while identity stays
// fixed. The first pass is therefore a lower bound during a stable-identity
// interval; invalidated scans are discarded, never promoted to peak evidence.
fn inventory(root: &Path) -> std::io::Result<BTreeMap<PathBuf, (u64, u64, u64)>> {
    let mut files = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        let meta = std::fs::symlink_metadata(&path)?;
        if meta.is_dir() {
            for entry in std::fs::read_dir(path)? {
                pending.push(entry?.path());
            }
        } else {
            files.insert(path, (meta.dev(), meta.ino(), meta.blocks() * 512));
        }
        assert!(
            files.len() + pending.len() < 4096,
            "fixture file count exceeded"
        );
    }
    Ok(files)
}

fn sample(root: &Path) -> Option<u64> {
    let before = inventory(root).ok()?;
    let after = inventory(root).ok()?;
    if before.len() != after.len()
        || before.iter().any(|(path, (dev, ino, bytes))| {
            !after
                .get(path)
                .is_some_and(|(d, i, n)| (d, i) == (dev, ino) && n >= bytes)
        })
    {
        return None;
    }
    Some(before.values().map(|(_, _, bytes)| bytes).sum())
}

fn verify_cold(store: &Store) {
    let backend = store.read_backend();
    let key = dduroc_engine::backend::ChannelKey {
        namespace: "cold".into(),
        class: StorageClass::Critical,
    };
    let catalog = backend.catalog().unwrap();
    let info = catalog.iter().find(|info| info.key == key).unwrap();
    assert_eq!(info.retained_records, Some(COLD as u64));
    let page = backend
        .read_blocks(
            &key,
            info.first_index.unwrap(),
            info.last_index.unwrap() + 1,
            128,
            32 << 20,
            false,
        )
        .unwrap();
    assert!(page.next_index.is_none());
    let mut count = 0;
    for stored in page.blocks {
        let block = Block::parse(&stored.bytes).unwrap().unwrap();
        for record in block.records() {
            let (_, Record::Message(message)) = record.unwrap() else {
                panic!("unexpected record")
            };
            assert_eq!(message.event, EventId(1));
            assert_eq!(message.payload.len(), PAYLOAD);
            assert!(
                message.payload.iter().all(|&byte| byte == count as u8),
                "changed cold payload {count}"
            );
            count += 1;
        }
    }
    assert_eq!(count, COLD);
    let hot = catalog
        .iter()
        .find(|info| info.key.namespace == "hot" && info.key.class == StorageClass::Critical)
        .unwrap();
    assert!(hot.retained_bytes <= 8 * MIB);
}

#[test]
#[ignore = "EXP-0043 host disk characterization; run explicitly with --nocapture"]
fn cold_hot_rewrite_peak_and_exact_reopen() {
    let disk_root = std::env::var_os("DDUROC_TEST_DISK_ROOT")
        .expect("set DDUROC_TEST_DISK_ROOT to a disk-backed disposable parent");
    let filesystem = std::process::Command::new("findmnt")
        .args(["-no", "FSTYPE", "-T"])
        .arg(&disk_root)
        .output()
        .unwrap();
    assert!(filesystem.status.success());
    let filesystem = String::from_utf8(filesystem.stdout)
        .unwrap()
        .trim()
        .to_owned();
    assert!(!matches!(filesystem.as_str(), "tmpfs" | "ramfs"));
    let main = tempfile::tempdir_in(&disk_root).unwrap();
    let critical = tempfile::tempdir_in(&disk_root).unwrap();
    let config = StoreConfig::new(main.path())
        .with_budget_per_class(16 * MIB)
        .channel(
            StorageClass::Critical,
            ChannelConfig {
                segment_bytes: MIB,
                block_max_bytes: PAYLOAD,
                compression: Compression::None,
                custom_root: Some(critical.path().into()),
                ..ChannelConfig::critical(32 * MIB)
            },
        );
    let store = Store::open(config.clone()).unwrap();
    let cold = store.namespace("cold", schema()).unwrap();
    let hot = store
        .namespace_with_quota(
            "hot",
            schema(),
            NsQuota::new().limit_bytes(StorageClass::Critical, 8 * MIB),
        )
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let sampler_stop = Arc::clone(&stop);
    let sampled_root = critical.path().to_owned();
    let started = Instant::now();
    let sampler = std::thread::spawn(move || {
        let mut peak = 0;
        let mut accepted = 0;
        let mut discarded = 0;
        while !sampler_stop.load(Ordering::Acquire) && started.elapsed() < Duration::from_secs(60) {
            if let Some(bytes) = sample(&sampled_root) {
                peak = peak.max(bytes);
                accepted += 1;
            } else {
                discarded += 1;
            }
            std::thread::yield_now();
        }
        (peak, accepted, discarded)
    });
    for n in 0..COLD {
        assert!(started.elapsed() < Duration::from_secs(60));
        cold.try_log_raw(EventId(1), &vec![n as u8; PAYLOAD], None)
            .unwrap();
    }
    let mut hot_acknowledged = 0;
    let mut write_error = None;
    for n in 0..HOT {
        assert!(started.elapsed() < Duration::from_secs(60));
        match hot.try_log_raw(EventId(1), &vec![(n + 128) as u8; PAYLOAD], None) {
            Ok(()) => hot_acknowledged += 1,
            Err(error) => {
                write_error = Some(error.to_string());
                break;
            }
        }
    }
    if write_error.is_none() {
        store.sync().unwrap();
    }
    stop.store(true, Ordering::Release);
    let (peak, samples, discarded) = sampler.join().unwrap();
    let usage = store
        .read_backend()
        .usage()
        .unwrap()
        .into_iter()
        .find(|u| u.root == critical.path())
        .unwrap();
    let elapsed_ms = started.elapsed().as_millis();
    println!(
        "pre_reopen {}",
        serde_json::json!({"peak_file_allocated_bytes": peak,
        "hot_acknowledged": hot_acknowledged, "write_error": write_error,
        "final_root_allocated_bytes":usage.allocated_bytes,
        "physical_admission_ceiling_bytes":usage.physical_limit_bytes})
    );
    verify_cold(&store);
    drop(cold);
    drop(hot);
    drop(store);
    let reopened = Store::open(config).unwrap();
    verify_cold(&reopened);
    println!(
        "{}",
        serde_json::json!({"experiment":"EXP-0043", "filesystem":filesystem, "cold_records":COLD, "hot_write_limit":HOT, "hot_acknowledged":hot_acknowledged, "write_error":write_error,
        "payload_bytes":PAYLOAD, "peak_file_allocated_bytes":peak, "sample_count":samples,
        "discarded_samples":discarded, "final_root_allocated_bytes":usage.allocated_bytes,
        "final_logical_retained_bytes":usage.logical_retained_bytes, "physical_admission_ceiling_bytes":usage.physical_limit_bytes,
        "elapsed_ms":elapsed_ms, "cold_exact_reopen":true,
        "peak_above_admission_ceiling":peak > usage.physical_limit_bytes})
    );
}
