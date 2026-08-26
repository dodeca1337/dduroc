//! Operating it: class policy, the shared class budget, a medium of its own for
//! critical data, accounting for losses.
//!
//! Run: `cargo run -p dduroc --example 04_operations`
//!
//! A device's journal lives for years unattended, so the engine has no state of
//! "out of space, nothing more to be done": the history rotates within its
//! budgets, losses are counted and announced, and a violation of the schema
//! contract is told apart from the disk lagging. This example shows those
//! mechanisms one at a time.

use dduroc::prelude::*;
use dduroc::read::{EntryKind, KindFilter, Order, OwnedSampleValue, Query, Reader};
use dduroc::{
    ChannelConfig, ChannelOverride, Compression, EventId, GroupPolicy, QueueSizes, StorageClass,
};

dduroc::schema! {
    name: probe,
    version: 1,
    languages: [en],

    events {
        Ping = 0x01 { level: Debug, en: "ping {seq}", seq: u32 },
        // Critical data goes into its own channel: an fdatasync right after the
        // write (group commit), no compression, and a queue in which on
        // overflow the caller WAITS for room rather than losing the record.
        Fault = 0x02 { level: Error, store: critical, en: "fault {code}", code: u8 },
    }

    metrics {
        // Binary snapshots are the heaviest stream; they get the telemetry
        // channel.
        Chunk = 0x01 { type: blob, store: telemetry },
    }
}

/// A snapshot with its number in the first bytes — it shows exactly what
/// survived.
fn chunk(index: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 4096];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    bytes
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::temp_dir().join("dduroc-examples").join("04");
    let _ = std::fs::remove_dir_all(&base);

    rotation(&base.join("rotation"))?;
    ceiling(&base.join("ceiling"))?;
    losses(&base.join("losses"))?;
    vault(&base.join("vault"), &base.join("vault-critical"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 1. Rotation within a class budget. The budget answers "how much history to
// keep for ALL telemetry": the class writes in a circle, the oldest segment
// goes whole, and fresh data is never refused.
// ---------------------------------------------------------------------------
fn rotation(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("— rotation within a class budget —");
    let store = Store::open(StoreConfig::new(root).channel(
        StorageClass::Telemetry,
        ChannelConfig {
            // The snapshots are dense already — compression would only burn
            // CPU.
            compression: Compression::None,
            // Telemetry tolerates a minute unwritten — fewer fdatasyncs.
            sync_interval: std::time::Duration::from_secs(60),
            // 12 MiB of budget with a 4 MiB segment: three segments live.
            segment_bytes: 4 << 20,
            ..ChannelConfig::new(12 << 20)
        },
    ))?;
    let ns = store.namespace("orc-probe-0", probe::SCHEMA)?;
    let chunks = ns.series(probe::metrics::Chunk)?;

    // 16 MiB of snapshots into 12 MiB of budget: the head of the history has to
    // go. A periodic sync lets the disk catch up — a real device's telemetry
    // arrives at the sensor's rate rather than in a while loop.
    for i in 0..4096u64 {
        chunks.sample(chunk(i));
        if i % 512 == 511 {
            ns.sync()?;
        }
    }
    ns.sync()?;

    let stats = store.stats();
    println!(
        "  segments created {}, sealed {}, rotated {}; records lost {}",
        stats.segments_created, stats.segments_sealed, stats.segments_rotated, stats.dropped
    );
    assert!(stats.segments_rotated > 0, "{stats:?}");
    store.shutdown();

    // What survived: the oldest available snapshot is no longer number zero.
    let reader = Reader::open_dump([root], &[probe::SCHEMA])?;
    let oldest = reader.query(
        &Query::new()
            .kinds(KindFilter::TELEMETRY)
            .order(Order::Oldest)
            .limit(1),
    )?;
    if let Some(EntryKind::Sample {
        value: OwnedSampleValue::Blob(bytes),
        ..
    }) = oldest.entries.first().map(|e| &e.kind)
    {
        let index = u64::from_le_bytes(bytes[..8].try_into()?);
        println!("  the oldest surviving snapshot: no. {index} of 4096 — the head was evicted\n");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. A class budget is shared by every namespace. The number of services grows
// while the budget of "all telemetry" does not: the channels draw on it
// together, and when it is exceeded the CLASS's oldest segment is evicted
// whoever's namespace it lies in — a quiet service does not hold space a noisy
// one lacks. (A personal limit for a greedy service is
// `store.namespace_with_quota` plus NsQuota, and one for a whole group at once
// is `StoreConfig::group`, as below.)
// ---------------------------------------------------------------------------
fn ceiling(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("— one class budget shared by every namespace —");
    let store = Store::open(
        StoreConfig::new(root)
            .channel(
                StorageClass::Telemetry,
                ChannelConfig {
                    compression: Compression::None,
                    segment_bytes: 4 << 20,
                    ..ChannelConfig::new(12 << 20)
                },
            )
            // Class settings are shared by the store — exactly as long as the
            // namespaces are uniform. A group (a name prefix, the same one
            // `Query::group` selects by) speaks about every orchestrator at
            // once instead of repeating at each open. The budget and the medium
            // are not available to it: they belong to the CLASS and are shared
            // by the store.
            .group(
                "orc-",
                GroupPolicy::new().channel(
                    StorageClass::Telemetry,
                    ChannelOverride::new().segment_bytes(2 << 20),
                ),
            )
            // A ceiling on the RAM held for block buffers — not a budget: a
            // budget is about space on the medium. Optional and absent by
            // default; it is needed where "only a handful write" stops being
            // true.
            .with_buffer_ceiling(4 << 20),
    )?;

    // The quiet service wrote 6 MiB and fell silent.
    let quiet = store.namespace("orc-quiet", probe::SCHEMA)?;
    let series = quiet.series(probe::metrics::Chunk)?;
    for i in 0..1536u64 {
        series.sample(chunk(i));
        if i % 512 == 511 {
            quiet.sync()?;
        }
    }
    quiet.sync()?;

    // The noisy one writes 10 MiB: together they do not fit the ceiling.
    let noisy = store.namespace("orc-noisy", probe::SCHEMA)?;
    let series = noisy.series(probe::metrics::Chunk)?;
    for i in 0..2560u64 {
        series.sample(chunk(i));
        if i % 512 == 511 {
            noisy.sync()?;
        }
    }
    noisy.sync()?;

    // Along the way: cleaning the epoch registry. Entries for runs of which not
    // one segment is left are cleared out (automatically when the store comes
    // up).
    let removed = store.compact_epochs()?;
    println!("  epochs cleaned out: {removed}");

    let stats = store.stats();
    store.shutdown();

    let reader = Reader::open_dump([root], &[probe::SCHEMA])?;
    let listing = reader.namespaces()?;
    for ns in &listing.namespaces {
        println!("  {}: {} KiB taken", ns.name, ns.total_bytes >> 10);
    }
    println!(
        "  the quiet one wrote 6 MiB — its head was evicted by another stream of the \
         same class; neither ceiling was ever exceeded (space: {}, memory: {})\n",
        stats.budget_overruns, stats.buffer_overruns
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Losses and the contract. Writing does not return a Result not because the
// outcome does not matter but because there is nothing to do with it at the
// call site. The outcome does not disappear: losses are counted and ANNOUNCED
// in the record stream itself, and records foreign to the schema are counted
// separately — that is a build defect, not the disk lagging.
// ---------------------------------------------------------------------------
fn losses(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("— losses are counted and announced —");
    // Tiny queues make the overflow reproducible in an example.
    let store = Store::open(
        StoreConfig::new(root)
            .with_budget_per_class(16 << 20)
            .with_queues(QueueSizes {
                normal: 4,
                critical: 4,
            }),
    )?;
    let ns = store.namespace("orc-probe-0", probe::SCHEMA)?;

    // The ordinary channel: the queue is full, the record is lost, the refusal
    // counted.
    let mut refused = 0u64;
    for seq in 0..20_000u32 {
        if let Err(e) = ns.try_log(probe::events::Ping { seq }) {
            assert!(e.loses_record());
            refused += 1;
        }
    }

    // The critical channel: the queue is full and the caller waits for room.
    // Slower, but the alarm will not be lost.
    for code in 0..2_000u32 {
        ns.try_log(probe::events::Fault { code: code as u8 })
            .expect("a critical write is not lost");
    }

    // Writing a type that is not in the schema is a contract violation, a build
    // defect. `try_*` gives the verdict to the caller and washes its hands:
    let contract = ns
        .try_log_raw(EventId(0xEE), &[], None)
        .expect_err("an id not from the schema");
    assert!(contract.breaks_contract() && !contract.loses_record());
    // …while the "quiet" path accounts for the failure itself: the `rejected`
    // counter plus a one-off announcement in the journal — the defect gets
    // found rather than searched for as the cause of silence.
    ns.log_raw(EventId(0xEE), &[], None);

    store.shutdown();
    let stats = store.stats();
    println!(
        "  refusals on the ordinary queue: {refused}; losses counted: {}; \
         waits on the critical one: {}; contract violations: {}",
        stats.dropped, stats.backpressure_waits, stats.rejected
    );
    assert_eq!(stats.dropped, refused);

    // A hole nobody mentions is indistinguishable from silence: the losses are
    // announced by notices in the stream itself, and their sum equals the
    // counter. A notice is parsed by `Entry::dropped_records` — the format of
    // its text belongs to the engine, and application code need not parse the
    // prose.
    let reader = Reader::open_dump([root], &[probe::SCHEMA])?;
    let mut announced = 0u64;
    let mut marks = 0usize;
    for e in reader.stream(&Query::new().kinds(KindFilter {
        text: true,
        ..KindFilter::TELEMETRY
    }))? {
        if let Some(count) = e.dropped_records() {
            marks += 1;
            announced += count;
        } else if let EntryKind::Text { text, .. } = &e.kind {
            println!("  the announcement in the journal: \"{text}\"");
        }
    }
    println!("  announced in the stream: {announced} (notices: {marks}) — it matches the count");
    assert_eq!(announced, stats.dropped);
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. A class with a medium of its own. Critical data is written to a protected
// partition (jffs2 and the like): the class is given its own root, and the
// layout inside it is the same — `<root>/<namespace>/<class>/`. A dump of such
// a store is two trees, and the viewer is told both.
// ---------------------------------------------------------------------------
fn vault(
    root: &std::path::Path,
    vault: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("— critical data on its own partition —");
    let store = Store::open(
        StoreConfig::new(root)
            .with_budget_per_class(16 << 20)
            .channel(
                StorageClass::Critical,
                ChannelConfig {
                    custom_root: Some(vault.to_path_buf()),
                    ..ChannelConfig::critical(16 << 20)
                },
            ),
    )?;
    let ns = store.namespace("orc-probe-0", probe::SCHEMA)?;
    ns.log(probe::events::Ping { seq: 1 });
    ns.log(probe::events::Fault { code: 3 });
    ns.sync()?;
    println!(
        "  the main root:          {:?}\n  the critical partition: {:?}",
        std::fs::read_dir(root.join("orc-probe-0"))?
            .filter_map(|e| e
                .ok()
                .map(|e| e.file_name().into_string().unwrap_or_default()))
            .collect::<Vec<_>>(),
        std::fs::read_dir(vault.join("orc-probe-0"))?
            .filter_map(|e| e
                .ok()
                .map(|e| e.file_name().into_string().unwrap_or_default()))
            .collect::<Vec<_>>(),
    );

    // One's own store is read by itself: it already has the roots (both!) and
    // the schemas of the namespaces that came up. Naming the critical partition
    // a second time is only possible by forgetting — and then the history would
    // silently come out shorter.
    let reader = store.reader();
    let read = reader.query(&Query::new().kinds(KindFilter::LOGS).order(Order::Oldest))?;
    for e in &read.entries {
        println!(
            "  [{}] {}",
            e.channel.as_str(),
            reader.render(e, "en").unwrap_or_default()
        );
    }

    // A foreign dump is another matter: a `Store` must not be opened there (it
    // takes a lock on the root and sweeps temporary files), so the roots and
    // schemas are named by hand.
    store.shutdown();
    let offline = Reader::open_dump([root, vault], &[probe::SCHEMA])?;
    println!(
        "  the same dump through a viewer: {} records",
        offline
            .query(&Query::new().kinds(KindFilter::LOGS))?
            .entries
            .len()
    );
    Ok(())
}
