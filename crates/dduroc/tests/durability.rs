//! End-to-end checks of durability: what happens to a store after a crash
//! stop, when a record is larger than a segment, and when a namespace is
//! brought up again.
//!
//! All three scenarios used to lose data or capacity and were caught by none of
//! the existing tests: they need either a real death of the process or
//! knowingly awkward sizes.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass, StoreConfig};
use dduroc_read::{Order, Query, Reader};
use std::path::Path;

dduroc::schema! {
    name: durability,
    version: 1,
    languages: [en],

    events {
        Tick = 0x01 { level: Info, en: "tick" },
    }

    metrics {
        Spectrum = 0x01 { type: blob },
    }

    spans {
        Work = 0x01,
    }
}

/// The names and sizes of a channel's segments.
fn segments(dir: &Path) -> Vec<(String, u64)> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<_> = entries
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
        .map(|e| {
            (
                e.file_name().to_string_lossy().into_owned(),
                e.metadata().map(|m| m.len()).unwrap_or(0),
            )
        })
        .collect();
    out.sort();
    out
}

/// Incompressible bytes: xorshift, so that LZ4 cannot squeeze them and the
/// block really does end up larger than a segment.
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

/// The environment variable by which the test asks a child process to play a
/// device whose power was cut.
const CRASH_ROOT: &str = "DDUROC_TEST_CRASH_ROOT";

#[test]
fn crash_does_not_cost_a_whole_segment_of_budget() {
    // The child's role: write, sync and die without sealing the segment.
    if let Ok(root) = std::env::var(CRASH_ROOT) {
        let store = Store::open(StoreConfig::new(&root).with_budget_per_class(16 << 20)).unwrap();
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        for _ in 0..3 {
            ns.log(durability::events::Tick);
        }
        ns.sync().unwrap();
        std::process::abort();
    }

    let dir = tempfile::tempdir().unwrap();
    let channel = dir
        .path()
        .join("orc-0")
        .join(StorageClass::Default.as_str());
    let segment_bytes = ChannelConfig::new(0).segment_bytes;

    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_does_not_cost_a_whole_segment_of_budget",
            "--exact",
            "--nocapture",
        ])
        .env(CRASH_ROOT, dir.path())
        .output()
        .expect("the child process starts");
    assert!(!out.status.success(), "the child process must die");

    // After the crash the segment lies unsealed and takes its reserve window —
    // not the whole segment: space is reserved as a window, and a crash costs
    // one window.
    let crashed = segments(&channel);
    assert_eq!(crashed.len(), 1);
    assert!(
        crashed[0].1 < segment_bytes / 8,
        "the crash cost {} bytes on a segment of {segment_bytes}: the reserve was \
         taken whole rather than as a window",
        crashed[0].1
    );

    // A restart has to give that window back too: an unsealed segment is
    // counted in the channel's budget together with its unwritten tail, and
    // such tails piling up from crash to crash would eat the budget with
    // emptiness — rotation would start on live history.
    {
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        ns.log(durability::events::Tick);
        ns.sync().unwrap();
        assert_eq!(
            store.stats().truncated_tails,
            0,
            "the tail was intact: the fdatasync went through before the death"
        );
        store.shutdown();
    }

    let after = segments(&channel);
    let total: u64 = after.iter().map(|(_, s)| s).sum();
    assert_eq!(
        after.len(),
        2,
        "the previous run's segment and the new one: {after:?}"
    );
    assert!(
        after[0].1 < crashed[0].1,
        "the previous run's segment took {} bytes, after recovery {}: the \
         window's tail was not given back",
        crashed[0].1,
        after[0].1
    );
    assert!(
        total < segment_bytes / 8,
        "after recovery the channel takes {total} bytes on a segment of \
         {segment_bytes}: the reserve was not given back"
    );

    // And most of all — every record is there and nothing is damaged.
    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "damage: {:?}", result.damaged);
    assert_eq!(
        result.entries.len(),
        4,
        "three before the crash and one after"
    );
}

#[test]
fn crashed_segment_gets_a_footer_so_migration_can_see_it() {
    // Recovery assembles the footer in the same walk it uses to find the end of
    // the data. Without the type sets in it a migration would walk past the
    // segment and a reader would look for blocks by scanning.
    if let Ok(root) = std::env::var(CRASH_ROOT) {
        let store = Store::open(StoreConfig::new(&root).with_budget_per_class(16 << 20)).unwrap();
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        ns.log(durability::events::Tick);
        ns.series(durability::metrics::Spectrum)
            .unwrap()
            .sample(&[1u8, 2, 3][..]);
        ns.sync().unwrap();
        std::process::abort();
    }

    let dir = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crashed_segment_gets_a_footer_so_migration_can_see_it",
            "--exact",
            "--nocapture",
        ])
        .env(CRASH_ROOT, dir.path())
        .output()
        .unwrap();
    assert!(!out.status.success());

    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    drop(store.namespace("orc-0", durability::SCHEMA).unwrap());
    store.shutdown();

    let channel = dir
        .path()
        .join("orc-0")
        .join(StorageClass::Default.as_str());
    let path = channel.join(&segments(&channel)[0].0);
    let reader = dduroc_engine::segment::SegmentReader::open(&path).unwrap();
    assert!(reader.is_sealed(), "the torn segment is sealed");
    let footer = reader.footer().expect("the footer reads");
    assert_eq!(footer.events, vec![dduroc::EventId(1)]);
    assert_eq!(
        footer.metrics,
        vec![dduroc::MetricId(1)],
        "a migration must see that the segment is affected"
    );
}

#[test]
fn record_larger_than_a_segment_is_refused_not_written_past_it() {
    // Space is reserved for one guarantee: ENOSPC arrives once, when a segment
    // is created, rather than in the middle of writing a critical event.
    // Writing past its boundary cancels that guarantee, so a block that does
    // not fit even a fresh segment is discarded — and declared lost.
    let dir = tempfile::tempdir().unwrap();
    let segment_bytes = ChannelConfig::new(0).segment_bytes;
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 << 20)).unwrap();
    let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();

    ns.series(durability::metrics::Spectrum)
        .unwrap()
        .sample(&noise(segment_bytes as usize * 2)[..]);
    ns.log(durability::events::Tick);
    ns.sync().unwrap();
    store.shutdown();

    let channel = dir
        .path()
        .join("orc-0")
        .join(StorageClass::Default.as_str());
    for (name, size) in segments(&channel) {
        assert!(
            size <= segment_bytes,
            "segment {name} grew to {size} against a limit of {segment_bytes}: \
             the write went past the reserve"
        );
    }

    let stats = store.stats();
    assert!(
        stats.dropped >= 1,
        "the loss must be accounted for: {stats:?}"
    );
    assert_eq!(
        stats.io_errors, 0,
        "this is not a failure of the medium: {stats:?}"
    );

    // The hole is announced in the stream itself, not only in a counter.
    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    let announced = result.entries.iter().any(|e| {
        matches!(&e.kind, dduroc_read::EntryKind::Text { text, .. }
                 if text.starts_with("records lost"))
    });
    assert!(
        announced,
        "the loss must be visible in the stream: {result:?}"
    );
}

#[test]
fn reopened_namespace_does_not_leave_a_second_state_on_the_directory() {
    // The namespace handle is released — the writer has to seal its segments
    // and free the slot. Otherwise one directory would have two channel states
    // with inventories of their own, and one's rotation would delete a segment
    // the other had open: writing would go on into a file with no name and all
    // of it would be lost.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let channel = dir
        .path()
        .join("orc-0")
        .join(StorageClass::Default.as_str());

    for round in 0..4 {
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        for _ in 0..50 {
            ns.log(durability::events::Tick);
        }
        ns.sync().unwrap();
        drop(ns);
        // The release goes without a reply — a `Drop` must not wait for the
        // medium — but the commands are handled in order, so any following
        // command serves as a barrier.
        store.sync().unwrap();

        // After the release the name is available again and the segment is
        // already sealed: nothing unsealed should be left behind.
        let unsealed = segments(&channel)
            .into_iter()
            .filter(|(name, _)| {
                let path = channel.join(name);
                dduroc_engine::segment::SegmentReader::open(&path)
                    .map(|r| !r.is_sealed())
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(unsealed, 0, "round {round}: an unsealed segment was left");
    }
    store.shutdown();

    // Not one record was lost on the way.
    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "damage: {:?}", result.damaged);
    assert_eq!(result.entries.len(), 200, "four rounds of fifty");
    assert_eq!(store.stats().dropped, 0);
}

#[test]
fn records_enqueued_before_the_handle_is_dropped_still_land() {
    // Releasing a namespace drains the queues dry: what `log()` already
    // answered `Ok` to has to reach the disk, even if the handle was released
    // on the very next line.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();

    let mut accepted = 0u64;
    {
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        for _ in 0..2_000 {
            while ns.try_log(durability::events::Tick).is_err() {
                std::thread::yield_now();
            }
            accepted += 1;
        }
    } // the handle was released without a sync

    // Bringing it up again is handled after the release — the command queue
    // preserves the order.
    drop(store.namespace("orc-0", durability::SCHEMA).unwrap());
    store.shutdown();

    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(
        result.entries.len() as u64 >= accepted,
        "{accepted} accepted, {} read",
        result.entries.len()
    );
}
