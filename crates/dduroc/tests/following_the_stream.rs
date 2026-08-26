//! Subscribing to the record stream.
//!
//! Before it a live reader was left with polling on a timer, and the choice was
//! between frequent (trips to the medium for nothing, and on a device flash
//! wear besides) and rare (a chart lagging by the polling period). A
//! subscription removes the choice: the reader sleeps while there is nothing to
//! write and wakes on the very first block.
//!
//! What is checked here is not "the data arrives" — a query could do that too —
//! but that a subscription does not lose it at the seams: an unfinished tail, a
//! change of segment, a namespace brought up after it.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass, StoreConfig, StoreExt};
use dduroc_read::{Follow, Order, Query, Reader, Tail};
use std::time::{Duration, Instant};

dduroc::schema! {
    name: probe,
    version: 1,
    languages: [en],

    events {
        Tick = 0x01 { level: Info, en: "tick {n}", n: u32 },
        Fault = 0x02 { level: Error, store: critical, en: "fault {code}", code: u8 },
    }
}

dduroc::schema! {
    name: latecomer,
    version: 1,
    languages: [en],

    events {
        Hello = 0x01 { level: Info, en: "came up" },
    }
}

/// How long to wait for a record before calling the subscription broken.
///
/// A test has no right to hang: `Tail::Idle` is a legitimate answer, and a loop
/// over it without an overall deadline would spin forever on any breakage.
const PATIENCE: Duration = Duration::from_secs(20);

/// Take exactly `want` records from a subscription.
///
/// It panics on the overall deadline: a subscription that has gone silent
/// forever has to fail the test rather than hang the run.
fn take(tail: &mut Follow<'_>, want: usize) -> Vec<dduroc_read::Entry> {
    let deadline = Instant::now() + PATIENCE;
    let mut got = Vec::with_capacity(want);
    while got.len() < want {
        assert!(
            Instant::now() < deadline,
            "the subscription handed out {} of {want} records and went silent",
            got.len()
        );
        match tail.next(Duration::from_millis(50)) {
            Tail::Entry(e) => got.push(*e),
            Tail::Idle => {}
            Tail::Ended => panic!("the store stopped in the middle of the test"),
        }
    }
    got
}

#[test]
fn records_reach_a_subscription_without_asking_for_them_again() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();

    let reader = store.reader();
    let mut tail = reader
        .follow(&Query::new().order(Order::Oldest))
        .expect("a subscription to a live store");

    // The first batch — before anyone asked.
    for n in 0..3 {
        ns.log(probe::events::Tick { n });
    }
    ns.sync().unwrap();
    let first = take(&mut tail, 3);
    assert_eq!(
        first
            .iter()
            .filter_map(|e| reader.render(e, "ru"))
            .collect::<Vec<_>>(),
        vec!["tick 0", "tick 1", "tick 2"]
    );

    // The second, to the same subscription, without recreating the reader once.
    for n in 3..6 {
        ns.log(probe::events::Tick { n });
    }
    ns.sync().unwrap();
    let second = take(&mut tail, 3);
    assert_eq!(
        second
            .iter()
            .filter_map(|e| reader.render(e, "ru"))
            .collect::<Vec<_>>(),
        vec!["tick 3", "tick 4", "tick 5"]
    );
    assert!(tail.take_damage().is_empty());
}

#[test]
fn a_namespace_raised_later_joins_the_subscription() {
    // The viewer started before the service — the ordinary order on a device. A
    // subscription to a group that showed only those that came up before it
    // would look like "the service did not start".
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let first = store.namespace("orc-a", probe::SCHEMA).unwrap();

    let reader = store.reader();
    let mut tail = reader
        .follow(&Query::new().group("orc-").order(Order::Oldest))
        .unwrap();
    first.log(probe::events::Tick { n: 1 });
    first.sync().unwrap();
    assert_eq!(take(&mut tail, 1).len(), 1);

    // The second service comes up with the subscription already running.
    let late = store.namespace("orc-b", latecomer::SCHEMA).unwrap();
    late.log(latecomer::events::Hello);
    late.sync().unwrap();

    let got = take(&mut tail, 1);
    assert_eq!(
        &*got[0].namespace, "orc-b",
        "the late namespace was picked up"
    );
    assert_eq!(reader.render(&got[0], "en").as_deref(), Some("came up"));

    // And the next right behind it. The root walk is rate limited (it lists the
    // whole store), so this one is knowably deferred: the debt has to be paid
    // rather than lost. Otherwise a service that came up close behind another
    // would never appear.
    let later = store.namespace("orc-c", latecomer::SCHEMA).unwrap();
    later.log(latecomer::events::Hello);
    later.sync().unwrap();
    let got = take(&mut tail, 1);
    assert_eq!(&*got[0].namespace, "orc-c", "the deferred walk happened");
}

#[test]
fn rotation_under_a_subscription_loses_nothing_and_looks_like_nothing() {
    // The segment under a subscription changes silently: it is reading a file
    // the writer is sealing at that moment, and it has to move to the next one
    // at once. The budget is knowably larger than what is written — there is
    // nothing to evict, so EVERYTHING has to arrive: coming up short here means
    // losing at the seam between segments.
    const COUNT: u32 = 3000;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(64 << 20)
            // Small segments and blocks with compression off: rotation has to
            // happen many times, and with compression these three thousand
            // identical records would have fitted one segment.
            .channel(
                StorageClass::Default,
                ChannelConfig {
                    segment_bytes: 8 << 10,
                    block_max_bytes: 512,
                    compression: dduroc::Compression::None,
                    flush_interval: Duration::from_millis(5),
                    ..ChannelConfig::new(64 << 20)
                },
            ),
    )
    .unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();

    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();

    let writer = std::thread::spawn({
        let ns = ns.clone();
        move || {
            for n in 0..COUNT {
                ns.log(probe::events::Tick { n });
            }
            ns.sync().unwrap();
        }
    });

    let got = take(&mut tail, COUNT as usize);
    writer.join().unwrap();

    let numbers: Vec<u32> = got
        .iter()
        .filter_map(|e| reader.decode::<probe::events::Tick>(e))
        .map(|t| t.expect("the payload parses").n)
        .collect();
    assert_eq!(
        numbers,
        (0..COUNT).collect::<Vec<_>>(),
        "order and completeness"
    );
    assert!(
        tail.take_damage().is_empty(),
        "rotation under reading is not damage: {:?}",
        tail.take_damage()
    );

    // The test has to prove it checked the seam between segments: fitting into
    // one file it would have checked something else entirely and still been
    // green.
    let segments = std::fs::read_dir(dir.path().join("dev-0").join("default"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .is_ok_and(|e| e.file_name().to_string_lossy().ends_with(".seg"))
        })
        .count();
    assert!(segments > 2, "there was no rotation: {segments} segments");
}

#[test]
fn a_lasting_damage_is_named_once_not_at_every_rotation() {
    // A subscription walks the roots anew on every change of the store's shape,
    // and an unreadable directory does not go anywhere. Without a memory of
    // what it has said, it would announce one and the same damage on every
    // rotation — and the list, which is handed out as a difference, would stop
    // being one.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    // A directory that is the channel of no class: the cause is permanent.
    std::fs::create_dir(dir.path().join("dev-0").join("attic")).unwrap();

    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();
    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    take(&mut tail, 1);

    let first = tail.take_damage();
    assert_eq!(first.len(), 1, "the foreign directory is named: {first:?}");

    // Any number of further turns — we stay silent about the same thing.
    for n in 1..4 {
        ns.log(probe::events::Tick { n });
        ns.sync().unwrap();
        take(&mut tail, 1);
    }
    assert!(
        tail.take_damage().is_empty(),
        "the same damage was reported a second time"
    );
}

#[test]
fn a_stopped_store_ends_the_subscription_instead_of_leaving_it_waiting() {
    // For a subscription the store stopping is not silence but the end: without
    // an explicit answer it would wait for new records from someone there is
    // nobody left to write for, and that would look exactly like a device gone
    // quiet.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();

    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    assert_eq!(take(&mut tail, 1).len(), 1);

    // What was written just before the stop has to reach the subscription: the
    // end of the stream is the end of the DATA, not a break at the last batch.
    ns.log(probe::events::Tick { n: 1 });
    store.shutdown();

    let deadline = Instant::now() + PATIENCE;
    let mut after = Vec::new();
    let ended = loop {
        assert!(
            Instant::now() < deadline,
            "the subscription did not notice the stop"
        );
        match tail.next(Duration::from_millis(50)) {
            Tail::Entry(e) => after.push(*e),
            Tail::Idle => {}
            Tail::Ended => break true,
        }
    };
    assert!(ended);
    assert_eq!(
        after.len(),
        1,
        "the last record reached the end of the stream"
    );
    assert!(
        tail.take_damage().is_empty(),
        "stopping is not damage: {:?}",
        tail.take_damage()
    );
    assert!(
        matches!(tail.next(Duration::from_millis(10)), Tail::Ended),
        "the end is irreversible"
    );
}

#[test]
fn a_newborn_segment_is_waited_for_not_walked_past() {
    // A segment caught at birth (the file created, the header not yet written)
    // is passed over by a one-off query: the next query will show it. A
    // subscription has no next query — passing it by, it would lose the whole
    // segment. It is checked the same way as everything else: rotation under
    // continuous writing, which is where such moments happen.
    const COUNT: u32 = 2000;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(64 << 20)
            .channel(
                StorageClass::Default,
                ChannelConfig {
                    segment_bytes: 8 << 10,
                    block_max_bytes: 512,
                    compression: dduroc::Compression::None,
                    flush_interval: Duration::from_millis(1),
                    ..ChannelConfig::new(64 << 20)
                },
            ),
    )
    .unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();

    // The writer does not stop: the subscription reads alongside it, not after
    // it.
    let writer = std::thread::spawn({
        let ns = ns.clone();
        move || {
            for n in 0..COUNT {
                ns.log(probe::events::Tick { n });
                if n % 64 == 0 {
                    std::thread::yield_now();
                }
            }
            ns.sync().unwrap();
        }
    });
    let got = take(&mut tail, COUNT as usize);
    writer.join().unwrap();

    let numbers: Vec<u32> = got
        .iter()
        .filter_map(|e| reader.decode::<probe::events::Tick>(e))
        .map(|t| t.expect("the payload parses").n)
        .collect();
    assert_eq!(numbers, (0..COUNT).collect::<Vec<_>>(), "not one gap");
    assert!(tail.take_damage().is_empty());
}

#[test]
fn a_run_without_an_anchor_is_named_even_if_it_appears_after_the_start() {
    // A device with no RTC and no synchronization: the run has no anchor, and
    // there is nothing to apply a wall-clock window to it with. Such a run
    // drops out of the selection — and has to be named, or the subscription's
    // silence is indistinguishable from "the device wrote nothing in those
    // hours". There are no segments yet when the subscription opens, so it can
    // only learn of the run by walking the directories later.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    let boot = dduroc::BootCounter(store.boot_counter());

    let reader = store.reader();
    let mut tail = reader
        .follow(
            &Query::new()
                .since(Utc::now() - dduroc::chrono::TimeDelta::hours(1))
                .order(Order::Oldest),
        )
        .unwrap();
    assert!(tail.unanchored().is_empty(), "writing has not started yet");

    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();

    // There will be no records: the run does not fit the wall-clock window. But
    // there must be no silence either — a subscription is polled until it names
    // the reason.
    let deadline = Instant::now() + PATIENCE;
    while tail.unanchored().is_empty() {
        assert!(
            Instant::now() < deadline,
            "a run with no anchor was never named — silence is indistinguishable from emptiness"
        );
        assert!(
            !matches!(tail.next(Duration::from_millis(50)), Tail::Entry(_)),
            "a record with no anchor has no right to reach a wall-clock window"
        );
    }
    assert_eq!(
        tail.unanchored(),
        vec![boot],
        "it is this run that is named"
    );
}

#[test]
fn asking_to_wait_forever_answers_instead_of_panicking() {
    // A `Duration::MAX` deadline is a panic on the addition with the clock. A
    // panic instead of a wait would be the worst reading of such a request.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();

    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    // What was written is already on the medium — there will be no waiting, but
    // the deadline still has to be added up.
    assert!(matches!(tail.next(Duration::MAX), Tail::Entry(_)));
}

#[test]
fn a_subscription_refuses_what_it_cannot_promise() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    let reader = store.reader();

    // Reverse order: "the last hundred" of a stream that has no last record
    // mean nothing.
    let e = reader
        .follow(&Query::new().order(Order::Newest))
        .unwrap_err();
    assert!(matches!(e, dduroc_read::ReadError::NotFollowable(_)), "{e}");

    // An upper window bound: a subscription reads what is not there yet.
    let e = reader
        .follow(&Query::new().order(Order::Oldest).until(ns.now()))
        .unwrap_err();
    assert!(matches!(e, dduroc_read::ReadError::NotFollowable(_)), "{e}");

    // Nobody appends to a dump.
    let dump = Reader::open_dump([dir.path()], &[probe::SCHEMA]).unwrap();
    let e = dump.follow(&Query::new().order(Order::Oldest)).unwrap_err();
    assert!(matches!(e, dduroc_read::ReadError::NotFollowable(_)), "{e}");
}

#[test]
fn a_rotation_is_not_announced_as_a_new_namespace() {
    // Answering "the segment changed" and "a namespace came up" cost different
    // things: the first is listing one channel's directory, the second walking
    // the root, reading `ns-meta` in every matching directory and opening
    // cursors. While this was one mark, a subscription did the second on every
    // rotation, that is, constantly and for nothing: at the twenty-four
    // thousand namespaces claimed, a walk of the whole store every half second
    // for the sake of a segment that changed in one channel.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(64 << 20)
            .channel(
                StorageClass::Default,
                ChannelConfig {
                    segment_bytes: 8 << 10,
                    block_max_bytes: 512,
                    compression: dduroc::Compression::None,
                    flush_interval: Duration::from_millis(5),
                    ..ChannelConfig::new(64 << 20)
                },
            ),
    )
    .unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();

    let base = store.pulse().wait(Default::default(), PATIENCE);
    assert!(base.shape > 0, "the first segment is already created");

    // Many rotations and not one new namespace.
    for n in 1..2000u32 {
        ns.log(probe::events::Tick { n });
    }
    ns.sync().unwrap();
    let rotated = store.pulse().wait(base, PATIENCE);
    assert_ne!(rotated.shape, base.shape, "the segments changed");
    assert_eq!(
        rotated.roster, base.roster,
        "the store roster did not change — a subscription has no reason to walk the root"
    );

    // And a namespace coming up is the opposite: it alone triggers a walk.
    let _late = store.namespace("dev-1", latecomer::SCHEMA).unwrap();
    let deadline = Instant::now() + PATIENCE;
    let mut now = rotated;
    while now.roster == rotated.roster {
        assert!(
            Instant::now() < deadline,
            "a namespace coming up was not announced"
        );
        now = store.pulse().wait(now, Duration::from_millis(50));
    }

    store.shutdown();
}
