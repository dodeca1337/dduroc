//! Independent public Store durability/retention acceptance. Host crash tests only.
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use dduroc_engine::backend::{ChannelKey, ReadBackend};
use dduroc_engine::{ChannelConfig, EventDesc, Language, Schema, StorageClass, Store, StoreConfig};
use dduroc_format::{Block, EventId, Level, ProtocolVersion, Record};

const CHILD: &str = "DDUROC_PUBLIC_ACK_CHILD";
const MODE: &str = "DDUROC_PUBLIC_ACK_MODE";
const ROUND: &str = "DDUROC_PUBLIC_ACK_ROUND";
const EVENTS: &[EventDesc] = &[
    EventDesc {
        id: EventId(1),
        name: "Critical",
        level: Level::Error,
        class: StorageClass::Critical,
        tags: &[],
        templates: &["critical"],
        fields: &[],
        decoders: None,
    },
    EventDesc {
        id: EventId(2),
        name: "Ordinary",
        level: Level::Info,
        class: StorageClass::Default,
        tags: &[],
        templates: &["ordinary"],
        fields: &[],
        decoders: None,
    },
];
fn schema() -> Schema {
    Schema {
        identity: None,
        name: "acceptance",
        version: ProtocolVersion(1),
        languages: &[Language("en")],
        events: EVENTS,
        metrics: &[],
        spans: &[],
        migrations: &[],
    }
}
fn config(path: &Path) -> StoreConfig {
    StoreConfig::new(path)
        .with_budget_per_class(16 << 20)
        .channel(
            StorageClass::Default,
            ChannelConfig {
                flush_interval: Duration::from_millis(10),
                sync_interval: Duration::from_millis(60),
                segment_bytes: 256 << 10,
                ..ChannelConfig::new(16 << 20)
            },
        )
}
fn payloads(backend: &ReadBackend, namespace: &str, class: StorageClass) -> Vec<Vec<u8>> {
    let key = ChannelKey {
        namespace: namespace.into(),
        class,
    };
    let page = backend
        .read_blocks(&key, 0, u64::MAX, 1024, 16 << 20, false)
        .unwrap();
    assert!(page.next_index.is_none());
    let mut result = Vec::new();
    for stored in page.blocks {
        let block = Block::parse(&stored.bytes).unwrap().unwrap();
        for record in block.records() {
            if let (_, Record::Message(message)) = record.unwrap() {
                result.push(message.payload.to_vec());
            }
        }
    }
    result
}

#[test]
fn public_ack_child() {
    let Some(path) = std::env::var_os(CHILD) else {
        return;
    };
    let store = Store::open(config(Path::new(&path))).unwrap();
    let ns = store.namespace("service", schema()).unwrap();
    let mode = std::env::var(MODE).unwrap();
    let round: u32 = std::env::var(ROUND).unwrap().parse().unwrap();
    let before = store.stats().syncs;
    for index in 0u32..16 {
        let value = (round * 16 + index).to_le_bytes();
        ns.try_log_raw(
            EventId(if mode == "critical" { 1 } else { 2 }),
            &value,
            None,
        )
        .unwrap();
    }
    if mode == "manual" {
        ns.sync().unwrap();
    } else if mode == "periodic" {
        let until = Instant::now() + Duration::from_secs(5);
        while store.stats().syncs == before {
            assert!(Instant::now() < until, "periodic barrier never ran");
            std::thread::sleep(Duration::from_millis(5));
        }
    }
    // Acknowledgement is emitted only after the selected public durability
    // boundary. Hold all handles alive so shutdown/drop cannot mask a bug.
    println!("PUBLIC_DURABLE_ACK");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

fn crash_after_ack(mode: &str) {
    let directory = tempfile::tempdir().unwrap();
    for round in 0u32..3 {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "public_ack_child", "--nocapture"])
            .env(CHILD, directory.path())
            .env(MODE, mode)
            .env(ROUND, round.to_string())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                if line.unwrap().contains("PUBLIC_DURABLE_ACK") {
                    let _ = send.send(());
                    return;
                }
            }
        });
        let acknowledged = receive.recv_timeout(Duration::from_secs(15));
        child.kill().unwrap(); // SIGKILL on Unix: no Store/Namespace destructors.
        let status = child.wait().unwrap();
        assert!(acknowledged.is_ok(), "child failed before ACK: {status}");
        assert!(!status.success());
        let backend = ReadBackend::open_dump(&[directory.path().to_path_buf()]).unwrap();
        let class = if mode == "critical" {
            StorageClass::Critical
        } else {
            StorageClass::Default
        };
        let expected: Vec<_> = (0..(round + 1) * 16)
            .map(|n| n.to_le_bytes().to_vec())
            .collect();
        assert_eq!(payloads(&backend, "service", class), expected);
    }
}

#[test]
fn critical_public_ack_survives_three_sigkill_restarts() {
    crash_after_ack("critical");
}
#[test]
fn manual_public_barrier_survives_three_sigkill_restarts() {
    crash_after_ack("manual");
}
#[test]
fn ordinary_periodic_barrier_survives_three_sigkill_restarts() {
    crash_after_ack("periodic");
}

#[test]
fn live_backend_and_namespace_drop_preserve_bounded_closed_history() {
    let directory = tempfile::tempdir().unwrap();
    let mut cfg = StoreConfig::new(directory.path()).with_budget_per_class(256 << 10);
    for class in StorageClass::ALL {
        let channel = if class == StorageClass::Critical {
            ChannelConfig::critical(256 << 10)
        } else {
            ChannelConfig::new(256 << 10)
        };
        cfg = cfg.channel(
            class,
            ChannelConfig {
                segment_bytes: 64 << 10,
                block_max_bytes: 8 << 10,
                ..channel
            },
        );
    }
    let store = Store::open(cfg.clone()).unwrap();
    let live = store.read_backend();
    let mut physical = Vec::new();
    // Each namespace is dropped before the next is registered: retained
    // history must remain in quota accounting without a live Namespace.
    for index in 0u32..160 {
        let name = format!("service-{index}");
        let ns = store.namespace(&name, schema()).unwrap();
        let mut data = vec![0u8; 40 << 10];
        let mut state = index as u64 + 1;
        for byte in &mut data {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state as u8;
        }
        ns.try_log_raw(EventId(1), &data, None).unwrap();
        assert_eq!(payloads(&live, &name, StorageClass::Critical), vec![data]);
        drop(ns);
        if index % 20 == 19 {
            store.sync().unwrap();
            let usage = live.usage().unwrap();
            assert!(usage.iter().all(|u| u.logical_retained_bytes <= 256 << 10));
            assert!(
                usage
                    .iter()
                    .all(|u| u.allocated_bytes <= u.physical_limit_bytes)
            );
            physical.push(usage.iter().map(|u| u.allocated_bytes).sum::<u64>());
        }
    }
    assert!(
        physical.last().unwrap() < &(4 << 20),
        "unbounded physical history: {physical:?}"
    );
    drop(live);
    drop(store);
    let reopened = Store::open(cfg).unwrap();
    let backend = reopened.read_backend();
    assert!(
        backend
            .usage()
            .unwrap()
            .iter()
            .all(|u| u.logical_retained_bytes <= 256 << 10)
    );
    assert!(payloads(&backend, "service-0", StorageClass::Critical).is_empty());
    assert_eq!(
        payloads(&backend, "service-159", StorageClass::Critical).len(),
        1
    );
}

fn resident_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix("VmRSS:")
                .map(|value| value.split_whitespace().next().unwrap().parse().unwrap())
        })
        .unwrap()
}

#[test]
fn repeated_namespace_churn_child() {
    let Some(path) = std::env::var_os("DDUROC_CHURN_CHILD") else {
        return;
    };
    let store = Store::open(config(Path::new(&path))).unwrap();
    let mut samples = Vec::new();
    for cycle in 0..600 {
        let namespace = store.namespace("same-service", schema()).unwrap();
        namespace
            .try_log_raw(EventId(1), &[42; 1024], None)
            .unwrap();
        drop(namespace);
        if cycle % 100 == 99 {
            store.sync().unwrap();
            samples.push(resident_kib());
        }
    }
    let warmed = samples[0];
    let peak_after_warmup = *samples[1..].iter().max().unwrap();
    assert!(
        peak_after_warmup <= warmed + 8192,
        "same-namespace churn retained unbounded runtime allocations: {samples:?}"
    );
    let backend = store.read_backend();
    assert_eq!(backend.catalog().unwrap().len(), 2);
    assert_eq!(
        payloads(&backend, "same-service", StorageClass::Critical).len(),
        600
    );
    println!("NAMESPACE_CHURN_RSS_KIB={samples:?}");
}

#[test]
fn repeated_namespace_open_drop_has_bounded_runtime_memory() {
    let directory = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "repeated_namespace_churn_child", "--nocapture"])
        .env("DDUROC_CHURN_CHILD", directory.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("NAMESPACE_CHURN_RSS_KIB="));
    println!("{stdout}");
}

#[test]
fn public_reads_isolate_equal_geometry_stores_and_fresh_reopens() {
    let directory = tempfile::tempdir().unwrap();
    let roots = [
        directory.path().join("first"),
        directory.path().join("second"),
    ];
    let first = Store::open(config(&roots[0])).unwrap();
    let second = Store::open(config(&roots[1])).unwrap();
    let a = first.namespace("same-service", schema()).unwrap();
    let b = second.namespace("same-service", schema()).unwrap();
    a.try_log_raw(EventId(1), &[b'A'; 32], None).unwrap();
    b.try_log_raw(EventId(1), &[b'B'; 32], None).unwrap();
    let backends = [first.read_backend(), second.read_backend()];
    for _ in 0..8 {
        for (backend, byte) in backends.iter().zip(*b"AB") {
            assert_eq!(
                payloads(backend, "same-service", StorageClass::Critical),
                vec![vec![byte; 32]]
            );
        }
    }
    drop(backends);
    drop(a);
    drop(b);
    drop(first);
    drop(second);
    for _ in 0..6 {
        for (root, byte) in roots.iter().zip(*b"AB") {
            let backend = ReadBackend::open_dump(std::slice::from_ref(root)).unwrap();
            assert_eq!(
                payloads(&backend, "same-service", StorageClass::Critical),
                vec![vec![byte; 32]]
            );
        }
    }
}

fn bounded_config(path: &Path) -> StoreConfig {
    let mut cfg = StoreConfig::new(path).with_budget_per_class(16 << 20);
    for class in StorageClass::ALL {
        let channel = if class == StorageClass::Critical {
            ChannelConfig::critical(16 << 20)
        } else {
            ChannelConfig::new(16 << 20)
        };
        cfg = cfg.channel(
            class,
            ChannelConfig {
                segment_bytes: 64 << 10,
                block_max_bytes: 8 << 10,
                compression: dduroc_format::Compression::None,
                ..channel
            },
        );
    }
    cfg
}

#[test]
fn different_namespace_quotas_survive_closed_handles_and_store_reopen() {
    use dduroc_engine::store::NsQuota;
    let directory = tempfile::tempdir().unwrap();
    let cfg = bounded_config(directory.path());
    let store = Store::open(cfg.clone()).unwrap();
    let ns_a = store
        .namespace_with_quota(
            "small",
            schema(),
            NsQuota::new().limit_bytes(StorageClass::Critical, 128 << 10),
        )
        .unwrap();
    let ns_b = store
        .namespace_with_quota(
            "large",
            schema(),
            NsQuota::new().limit_bytes(StorageClass::Critical, 256 << 10),
        )
        .unwrap();
    for index in 0u8..12 {
        ns_a.try_log_raw(EventId(1), &vec![index; 40 << 10], None)
            .unwrap();
        ns_b.try_log_raw(EventId(1), &vec![index; 40 << 10], None)
            .unwrap();
    }
    drop(ns_a);
    drop(ns_b);
    store.sync().unwrap();
    drop(store);
    let store = Store::open(cfg).unwrap();
    let backend = store.read_backend();
    for (name, quota, expected) in [
        ("small", 128u64 << 10, 3usize),
        ("large", 256u64 << 10, 6usize),
    ] {
        let info = backend
            .catalog()
            .unwrap()
            .into_iter()
            .find(|item| item.key.namespace == name && item.key.class == StorageClass::Critical)
            .unwrap();
        assert!(info.retained_bytes <= quota);
        let records = payloads(&backend, name, StorageClass::Critical);
        assert_eq!(records.len(), expected);
        assert_eq!(records.last().unwrap()[0], 11);
    }
}

fn percentile_us(values: &mut [u128], percent: usize) -> u128 {
    values.sort_unstable();
    values[(values.len() - 1) * percent / 100]
}

#[test]
fn public_store_characterization_child() {
    use dduroc_engine::store::NsQuota;
    let Some(path) = std::env::var_os("DDUROC_CHARACTERIZE_CHILD") else {
        return;
    };
    let count: usize = std::env::var("DDUROC_CHARACTERIZE_NAMESPACES")
        .unwrap()
        .parse()
        .unwrap();
    let store = Store::open(bounded_config(Path::new(&path))).unwrap();
    for index in 0..count {
        let namespace = store
            .namespace(&format!("dormant-{index:04}"), schema())
            .unwrap();
        drop(namespace);
    }
    let namespace = store
        .namespace_with_quota(
            "active",
            schema(),
            NsQuota::new()
                .limit_bytes(StorageClass::Critical, 128 << 10)
                .limit_bytes(StorageClass::Default, 128 << 10),
        )
        .unwrap();
    let data = vec![42u8; 2048];
    for _ in 0..64 {
        namespace.try_log_raw(EventId(1), &data, None).unwrap();
    }
    store.sync().unwrap();
    let baseline_rss = resident_kib();
    let baseline_disk = store.read_backend().usage().unwrap()[0].allocated_bytes;
    let started = Instant::now();
    let mut critical_latency = Vec::new();
    for _ in 0..256 {
        let at = Instant::now();
        namespace.try_log_raw(EventId(1), &data, None).unwrap();
        critical_latency.push(at.elapsed().as_micros());
    }
    let critical_elapsed = started.elapsed();
    let normal_started = Instant::now();
    for _ in 0..128 {
        for _ in 0..4 {
            namespace.try_log_raw(EventId(2), &data, None).unwrap();
        }
        namespace.sync().unwrap();
    }
    let normal_elapsed = normal_started.elapsed();
    let usage = store.read_backend().usage().unwrap();
    assert!(
        usage
            .iter()
            .all(|root| root.allocated_bytes <= root.physical_limit_bytes)
    );
    assert!(
        usage
            .iter()
            .all(|root| root.logical_retained_bytes <= 256 << 10)
    );
    let end_rss = resident_kib();
    assert!(end_rss <= baseline_rss + 8192);
    println!(
        "PUBLIC_STORE_METRICS namespaces={count} payload_bytes=2048 critical_records=256 critical_elapsed_ms={} sync_p50_us={} sync_p95_us={} sync_p99_us={} normal_records=512 normal_batches=128 normal_elapsed_ms={} rss_baseline_kib={baseline_rss} rss_end_kib={end_rss} allocated_baseline_bytes={baseline_disk} allocated_end_bytes={} retained_bytes={}",
        critical_elapsed.as_millis(),
        percentile_us(&mut critical_latency, 50),
        percentile_us(&mut critical_latency, 95),
        percentile_us(&mut critical_latency, 99),
        normal_elapsed.as_millis(),
        usage[0].allocated_bytes,
        usage[0].logical_retained_bytes
    );
}

#[test]
fn migrated_public_store_resource_characterization() {
    for count in [100, 1000] {
        let temporary_root = std::env::var_os("DDUROC_TEST_DISK_ROOT")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let directory = tempfile::tempdir_in(temporary_root).unwrap();
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "public_store_characterization_child",
                "--nocapture",
            ])
            .env("DDUROC_CHARACTERIZE_CHILD", directory.path())
            .env("DDUROC_CHARACTERIZE_NAMESPACES", count.to_string())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("PUBLIC_STORE_METRICS"));
        println!("{stdout}");
    }
}
