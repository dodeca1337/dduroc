//! End-to-end checks under load: the shared class budget, personal quotas,
//! back pressure on the critical queue, multi-threaded writing and reading a
//! live store.
//!
//! All of this used to rest on unit tests and review. Rotation was checked on
//! an inventory of files stuffed with zeros — no test made the writer actually
//! exceed its budget; back pressure was never exercised; there were no threads
//! in the tests at all, although writing from many threads is the library's
//! main mode of operation.

use dduroc::prelude::*;
use dduroc::{
    ChannelConfig, ChannelOverride, GroupPolicy, NsQuota, QueueSizes, StorageClass, StoreConfig,
};
use dduroc_read::{EntryKind, Order, Query, Reader};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

dduroc::schema! {
    name: pressure,
    version: 1,
    languages: [en],

    events {
        Mark = 0x01 { level: Info, en: "mark" },
        Alarm = 0x02 { level: Error, store: critical, en: "alarm" },
    }

    metrics {
        Seq = 0x01 { type: u64 },
        Bulk = 0x02 { type: blob },
    }
}

/// Incompressible bytes: LZ4 must not reduce the volume to nothing, or the
/// segments will not run out and there will be nothing to check.
fn noise(n: usize) -> Vec<u8> {
    let mut s: u64 = 0x2545_F491_4F6C_DD1D;
    (0..n)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            s as u8
        })
        .collect()
}

/// The total size of the segments in a directory tree.
fn bytes_under(root: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(root) else {
        return 0;
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| {
            let path = e.path();
            if path.is_dir() {
                bytes_under(&path)
            } else if path.extension().is_some_and(|x| x == "seg") {
                e.metadata().map(|m| m.len()).unwrap_or(0)
            } else {
                0
            }
        })
        .sum()
}

/// The numbers that reached the disk, in read order.
fn sequence(root: &Path) -> Vec<u64> {
    let reader = Reader::open_dump([root], &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "damage: {:?}", result.damaged);
    result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Sample {
                metric,
                value: dduroc_read::OwnedSampleValue::U64(v),
                ..
            } if *metric == dduroc::MetricId(1) => Some(*v),
            _ => None,
        })
        .collect()
}

#[test]
fn rotation_drops_the_oldest_and_keeps_the_class_inside_its_budget() {
    // The budget is declared on a class; here the class is represented by one
    // namespace — and this is exactly where the reserve, the size accounting
    // after sealing and the protection of the active segment meet.
    const BUDGET: u64 = 32 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(BUDGET)).unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();
    let seq = ns.series(pressure::metrics::Seq).unwrap();
    let bulk = ns.series(pressure::metrics::Bulk).unwrap();

    // We write twice the budget: every batch is marked with its own number.
    let chunk = noise(64 << 10);
    let rounds = (BUDGET / chunk.len() as u64) * 2;
    for i in 0..rounds {
        seq.sample(i);
        bulk.sample(&chunk[..]);
        if i % 32 == 0 {
            ns.sync().unwrap();
        }
    }
    ns.sync().unwrap();
    store.shutdown();

    let stats = store.stats();
    assert_eq!(stats.dropped, 0, "the queue kept up: {stats:?}");
    assert!(
        stats.segments_rotated > 0,
        "otherwise the test is not about rotation: {stats:?}"
    );

    let occupied = bytes_under(dir.path());
    assert!(
        occupied <= BUDGET,
        "the channel takes {occupied} on a budget of {BUDGET}"
    );

    // And most of all — rotation ate the BEGINNING of the history, not its end.
    // The reverse would mean the device throws away fresh data and keeps
    // ancient.
    let seen = sequence(dir.path());
    assert!(!seen.is_empty(), "something had to survive");
    assert_eq!(
        *seen.last().unwrap(),
        rounds - 1,
        "the last record has to survive"
    );
    assert!(
        seen[0] > 0,
        "otherwise nothing was evicted and the budget was met by accident"
    );
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "the tail of the history has to stay unbroken and ordered"
    );
}

#[test]
fn the_class_budget_is_shared_across_namespaces() {
    // The budget is a property of a class rather than of a namespace: "all logs
    // get this much". Two namespaces write into one class and together have to
    // fit its budget; the class's oldest is evicted whoever's directory it lies
    // in.
    const CEILING: u64 = 12 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).channel(
        StorageClass::Default,
        ChannelConfig {
            // A smaller segment: two active segments are supposed to fit the
            // budget with room for history.
            segment_bytes: 4 << 20,
            ..ChannelConfig::new(CEILING)
        },
    ))
    .unwrap();

    let chunk = noise(64 << 10);
    let a = store.namespace("orc-0", pressure::SCHEMA).unwrap();
    let b = store.namespace("orc-1", pressure::SCHEMA).unwrap();
    for (i, ns) in [&a, &b].into_iter().cycle().take(320).enumerate() {
        ns.series(pressure::metrics::Bulk)
            .unwrap()
            .sample(&chunk[..]);
        if i % 32 == 0 {
            store.sync().unwrap();
        }
    }
    store.sync().unwrap();
    store.shutdown();

    let occupied = bytes_under(dir.path());
    assert!(
        occupied > 0 && occupied <= CEILING,
        "the class takes {occupied} on a budget of {CEILING}"
    );
    assert_eq!(
        store.stats().budget_overruns,
        0,
        "the class budget is meetable: there are only two active segments"
    );
    // Both namespaces took part — eviction went by age rather than by "who
    // wrote last".
    for name in ["orc-0", "orc-1"] {
        assert!(
            bytes_under(&dir.path().join(name)) > 0,
            "{name} was evicted entirely: eviction has to go by age"
        );
    }
}

#[test]
fn a_group_hands_its_namespaces_their_own_segments_and_quota() {
    // Channel settings are given for the whole store, and that holds exactly as
    // long as the namespaces are uniform. A group is the way to say "the
    // orchestrators' telemetry is its own" once rather than at every namespace
    // open. What is checked is that what was said reaches the medium: both the
    // occupancy limit and the segment size.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(64 << 20)
            .group(
                "orc-",
                GroupPolicy::new()
                    .channel(
                        StorageClass::Default,
                        ChannelOverride::new()
                            .segment_bytes(64 << 10)
                            .block_max_bytes(4 << 10),
                    )
                    .limit_bytes(StorageClass::Default, 256 << 10),
            ),
    )
    .unwrap();

    let grouped = store.namespace("orc-radio-0", pressure::SCHEMA).unwrap();
    let outsider = store.namespace("diag-0", pressure::SCHEMA).unwrap();
    for ns in [&grouped, &outsider] {
        let bulk = ns.series(pressure::metrics::Bulk).unwrap();
        for _ in 0..40 {
            bulk.sample(noise(16 << 10));
        }
        ns.sync().unwrap();
    }

    let channel = |name: &str| dir.path().join(name).join("default");
    let occupied = bytes_under(&channel("orc-radio-0"));
    let outside = bytes_under(&channel("diag-0"));

    // A group's quota holds its own: the limit plus the active segment, which
    // cannot be evicted.
    assert!(
        occupied <= (256 << 10) + (64 << 10),
        "the group quota did not hold: {occupied} B"
    );
    // An outsider inherited no quota and draws on the class's shared budget.
    assert!(
        outside > occupied,
        "a namespace outside the group must not obey its quota: {outside} B against {occupied} B"
    );

    // The segment size is the group's too: an outsider gets the shared
    // eight-megabyte one, and all of its volume fitted one file.
    let files = |name: &str| {
        std::fs::read_dir(channel(name))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .is_ok_and(|e| e.path().extension().is_some_and(|x| x == "seg"))
            })
            .count()
    };
    assert!(files("orc-radio-0") > 1, "the group's small segments");
    assert_eq!(
        files("diag-0"),
        1,
        "the shared segment holds everything at once"
    );
}

#[test]
fn a_full_critical_queue_makes_the_caller_wait_and_loses_nothing() {
    // The critical queue's promise is "not lost": on overflow the caller waits
    // for room rather than getting a hole. The waiting path was exercised by no
    // test, which is to say the promise rested on reading the code.
    const N: u64 = 400;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(16 << 20)
            // A queue of one record: waiting stops being a rarity and becomes
            // the rule — and that is what is checked.
            .with_queues(QueueSizes {
                normal: 1,
                critical: 1,
            }),
    )
    .unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();

    for _ in 0..N {
        ns.try_log(pressure::events::Alarm)
            .expect("a critical write has no right to be refused");
    }
    ns.sync().unwrap();
    store.shutdown();

    let stats = store.stats();
    assert!(
        stats.backpressure_waits > 0,
        "otherwise the wait was never exercised and the test is empty: {stats:?}"
    );
    assert_eq!(stats.dropped, 0, "a critical write is not lost: {stats:?}");

    let reader = Reader::open_dump([dir.path()], &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "damage: {:?}", result.damaged);
    let alarms = result
        .entries
        .iter()
        .filter(|e| matches!(&e.kind, EntryKind::Message { event, .. } if event.0 == 2))
        .count();
    assert_eq!(
        alarms as u64, N,
        "we waited for room, so all of them arrived"
    );
    assert!(
        result
            .entries
            .iter()
            .all(|e| e.channel == StorageClass::Critical),
        "critical events have to lie in their own channel"
    );
}

#[test]
fn many_threads_write_one_namespace_without_losing_or_duplicating() {
    // Writing from many threads is the library's main mode of operation, and no
    // test did it: the queue, the monotonicity of time within a channel and the
    // stable sorting of a batch were checked one at a time and in the abstract.
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 500;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 << 20)).unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let seq = ns.series(pressure::metrics::Seq).unwrap();
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    // The queue overflows — we wait for room rather than lose:
                    // this test is about preservation, not about behaviour on
                    // failure.
                    while seq.try_sample(t * PER_THREAD + i).is_err() {
                        std::thread::yield_now();
                    }
                }
            });
        }
    });

    ns.sync().unwrap();
    store.shutdown();
    assert_eq!(store.stats().dropped, 0, "nothing was discarded");

    let mut seen = sequence(dir.path());
    assert_eq!(
        seen.len() as u64,
        THREADS * PER_THREAD,
        "neither losses nor duplicates"
    );
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen.len() as u64,
        THREADS * PER_THREAD,
        "every number has to appear exactly once"
    );
}

#[test]
fn reading_a_live_store_never_takes_back_what_it_already_showed() {
    // The reader works on the device itself, in parallel with writing. There
    // was not one test of that, and the property matters more than
    // completeness: an unsealed segment is read to the end by scanning, and a
    // block cut off mid-word is ordinary. Completeness cannot be demanded of
    // live reading — but taking back what it has already shown is what it may
    // not do.
    const N: u64 = 3_000;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 << 20)).unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();

    let done = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicU64::new(0));
    let queries = std::thread::scope(|s| {
        {
            let (done, written) = (Arc::clone(&done), Arc::clone(&written));
            let seq = ns.series(pressure::metrics::Seq).unwrap();
            s.spawn(move || {
                for i in 0..N {
                    while seq.try_sample(i).is_err() {
                        std::thread::yield_now();
                    }
                    written.store(i + 1, Ordering::Release);
                }
                done.store(true, Ordering::Release);
            });
        }

        let mut queries = 0u32;
        let mut floor = 0usize;
        while !done.load(Ordering::Acquire) {
            let reader = Reader::open_dump([dir.path()], &[pressure::SCHEMA]).unwrap();
            let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
            let count = result
                .entries
                .iter()
                .filter(|e| matches!(e.kind, EntryKind::Sample { .. }))
                .count();
            assert!(
                count >= floor,
                "the read handed out {count} records after {floor}: what has been shown \
                 has no right to disappear"
            );
            floor = count;
            queries += 1;
        }
        queries
    });

    assert!(queries > 0, "otherwise the read never overlapped the write");
    ns.sync().unwrap();
    store.shutdown();

    // And after the stop, completeness: everything accepted has to be there.
    let seen = sequence(dir.path());
    assert_eq!(
        seen.len() as u64,
        N,
        "after the stop the answer is complete"
    );
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "and ordered");
}

#[test]
fn a_class_budget_below_two_segments_is_refused_at_open() {
    // A class budget below a pair of segments is unmeetable by construction:
    // the active segment cannot be evicted. Learning that at open time is the
    // only moment when it is still fixable.
    let dir = tempfile::tempdir().unwrap();
    let segment = ChannelConfig::new(0).segment_bytes;
    let err = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(segment))
        .expect_err("an unmeetable budget must be refused");
    assert!(matches!(err, dduroc::Error::BadChannel { .. }), "{err}");

    Store::open(StoreConfig::new(dir.path()).with_budget_per_class(segment * 2))
        .expect("two segments are already enough");
}

#[test]
fn a_namespace_quota_rotates_inside_the_shared_class_budget() {
    // A personal quota is an optional limit INSIDE the class's shared budget: a
    // greedy service rotates within it without waiting for the class to hit its
    // budget — and does not eat its neighbours.
    const QUOTA: u64 = 16 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(256 << 20)).unwrap();

    let hog = store
        .namespace_with_quota(
            "orc-hog",
            pressure::SCHEMA,
            NsQuota::new().limit_bytes(StorageClass::Default, QUOTA),
        )
        .unwrap();
    let bulk = hog.series(pressure::metrics::Bulk).unwrap();
    let chunk = noise(64 << 10);
    let rounds = (QUOTA / chunk.len() as u64) * 3;
    for i in 0..rounds {
        bulk.sample(&chunk[..]);
        if i % 32 == 0 {
            hog.sync().unwrap();
        }
    }
    hog.sync().unwrap();
    store.shutdown();
    let stats = store.stats();
    // The lock on the root is released with the last handle: a series and a
    // namespace keep the store alive.
    drop(bulk);
    drop(hog);
    drop(store);

    let occupied = bytes_under(&dir.path().join("orc-hog"));
    assert!(
        occupied > 0 && occupied <= QUOTA,
        "the namespace takes {occupied} on a quota of {QUOTA}"
    );
    assert!(
        stats.segments_rotated > 0,
        "the quota had to fire: {stats:?}"
    );

    // A quota smaller than two segments is meaningless and is refused at open
    // time.
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(256 << 20)).unwrap();
    let err = store
        .namespace_with_quota(
            "orc-tiny",
            pressure::SCHEMA,
            NsQuota::new().limit_bytes(StorageClass::Default, 1 << 20),
        )
        .expect_err("a quota of one segment is meaningless");
    assert!(matches!(err, dduroc::Error::BadChannel { .. }), "{err}");
    store.shutdown();
}

#[test]
fn a_class_can_live_on_its_own_root() {
    // Critical data has to be able to live on a medium of its own (a protected
    // partition such as jffs2): a class is given its own root, and the layout
    // inside it is the same — `<root>/<namespace>/<class>/`.
    let main = tempfile::tempdir().unwrap();
    let vault = tempfile::tempdir().unwrap();
    {
        let store = Store::open(
            StoreConfig::new(main.path())
                .with_budget_per_class(16 << 20)
                .channel(
                    StorageClass::Critical,
                    ChannelConfig {
                        custom_root: Some(vault.path().to_path_buf()),
                        ..ChannelConfig::critical(16 << 20)
                    },
                ),
        )
        .unwrap();
        let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();
        ns.log(pressure::events::Mark);
        ns.log(pressure::events::Alarm);
        ns.sync().unwrap();
        assert!(store.stats().is_clean(), "{:?}", store.stats());
        store.shutdown();
    }

    // The segments landed on their own media.
    assert!(
        bytes_under(&vault.path().join("orc-0").join("critical")) > 0,
        "the critical channel lives on its own partition"
    );
    assert!(
        !main.path().join("orc-0").join("critical").exists(),
        "there is no critical directory in the main root"
    );
    assert!(
        bytes_under(&main.path().join("orc-0").join("default")) > 0,
        "the ordinary channel stayed in the main root"
    );

    // The reader gathers both trees: a dump is opened with every root at once.
    let reader = Reader::open_dump([main.path(), vault.path()], &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "{:?}", result.damaged);
    let kinds: Vec<u16> = result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Message { event, .. } => Some(event.0),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        [1, 2],
        "both the ordinary and the critical are visible"
    );
    let listing = reader.namespaces().unwrap();
    assert_eq!(
        listing.namespaces[0].channels,
        [StorageClass::Default, StorageClass::Critical],
        "the listing merges the channels of both roots"
    );

    // A dump without the second tree is not a "short answer" but a refusal:
    // completeness is checked against the schema at open time, and the critical
    // part must not be lost silently.
    let e = Reader::open_dump([main.path()], &[pressure::SCHEMA]).unwrap_err();
    assert!(
        matches!(
            &e,
            dduroc_read::ReadError::IncompleteDump { namespace, class }
                if namespace == "orc-0" && *class == StorageClass::Critical
        ),
        "{e}"
    );
}

/// How many of the process's descriptors point inside a tree.
fn open_fds_under(dir: &Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("procfs is mounted")
        .filter_map(|e| e.ok())
        .filter_map(|e| std::fs::read_link(e.path()).ok())
        .filter(|target| target.starts_with(dir))
        .count()
}

#[test]
fn a_query_does_not_hold_a_descriptor_per_channel() {
    // Merging by time needs a head from EVERY cursor, and a cursor is created
    // per (namespace, channel) pair. While a cursor held an open segment, one
    // query cost a descriptor per channel: at the twenty-four thousand
    // namespaces claimed that is tens of thousands of open files on top of what
    // the writer holds — that is, a `ulimit` failure out of nowhere.
    //
    // A hundred namespaces is enough to tell "per channel" from "per read": the
    // difference between a hundred and a handful drowns in no noise.
    const NAMESPACES: usize = 100;

    let dir = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 * 1024 * 1024)).unwrap();

    let mut handles = Vec::with_capacity(NAMESPACES);
    for i in 0..NAMESPACES {
        let ns = store
            .namespace(&format!("svc-{i:04}"), pressure::SCHEMA)
            .unwrap();
        ns.log(pressure::events::Mark);
        handles.push(ns);
    }
    store.sync().unwrap();

    let before = open_fds_under(dir.path());
    let reader = store.reader();
    let mut stream = reader.stream(&Query::new().order(Order::Oldest)).unwrap();
    // The first record means the cursors are loaded: every one was asked for a
    // head.
    let first = stream.next().expect("there are records");
    let held = open_fds_under(dir.path()).saturating_sub(before);
    assert_eq!(first.namespace.as_ref(), "svc-0000");
    assert!(
        held <= 2,
        "the stream holds {held} descriptors for {NAMESPACES} namespaces — that is, \
         one per channel"
    );

    // And it reads everything meanwhile: saving descriptors has no right to
    // cost records.
    let seen = 1 + stream.by_ref().count();
    assert_eq!(seen, NAMESPACES, "every namespace was read");
    assert!(stream.damaged().is_empty(), "{:?}", stream.damaged());

    drop(stream);
    drop(handles);
    store.shutdown();
}
