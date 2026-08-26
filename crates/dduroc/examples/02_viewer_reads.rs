//! The journal is read: queries, time windows, restoring text and states.
//!
//! Run: `cargo run -p dduroc --example 02_viewer_reads`
//!
//! The reader is one and the same on a device and in an offline viewer: it
//! needs the store's directory and the application's schemas — without a schema
//! the records stay identifiers with binary fields. The example first writes
//! the history of two runs (a run = opening a Store), then takes it apart with
//! queries.

use dduroc::prelude::*;
use dduroc::read::{EntryKind, KindFilter, Order, Query, Reader, Tail};
use dduroc::{BootCounter, Micros};

dduroc::schema! {
    name: radio,
    version: 1,
    languages: [en, ru],

    events {
        Started = 0x01 { level: Debug, en: "radio started", ru: "радио запущено" },
        PowerSet = 0x02 {
            level: Info,
            tags: [rf],
            en: "power set to {dbm} dBm",
            ru: "мощность {dbm} дБм",
            dbm: f32,
        },
        Overheat = 0x03 {
            level: Error,
            store: critical,
            tags: [thermal],
            en: "overheat: {t:.1} °C",
            ru: "перегрев: {t:.1} °C",
            t: f32,
        },
    }

    metrics {
        TempPa = 0x01 { type: f32, unit: "°C", tags: [thermal],
                        warn: ..=70.0, alarm: ..=85.0 },
        LinkState = 0x02 { states: [alarm Los = 0, warn Sync = 1, Lock = 2] },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("dduroc-examples").join("02");
    let _ = std::fs::remove_dir_all(&root);
    let cfg = StoreConfig::new(&root).with_budget_per_class(16 << 20);

    // -----------------------------------------------------------------------
    // The history. Run 1: the link came up — the only change of state in the
    // whole history (useful below, in carrying states through).
    // -----------------------------------------------------------------------
    let boot_first;
    {
        let store = Store::open(cfg.clone())?;
        boot_first = store.boot_counter();
        let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
        ns.log(radio::events::Started);
        ns.log(radio::events::PowerSet { dbm: 27.5 });
        ns.series(radio::metrics::TempPa)?.sample(36.6);
        ns.series(radio::metrics::LinkState)?
            .sample(radio::metrics::LinkState::Lock);
        ns.sync()?;
        store.shutdown();
    }

    // Run 2: a time synchronization arrived and an overheat happened.
    let boot_second;
    let sync_at;
    {
        let store = Store::open(cfg)?;
        boot_second = store.boot_counter();
        let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
        ns.log(radio::events::Started);

        // The anchor is retroactive across the whole hardware boot: the records
        // of the FIRST run get a wall-clock time too — the process restarted,
        // the device did not.
        sync_at = Utc::now();
        store.record_sync(sync_at, SyncSource::Ntp)?;

        ns.log(radio::events::PowerSet { dbm: 30.0 });
        let temp = ns.series(radio::metrics::TempPa)?;
        temp.sample(52.0);
        temp.sample(88.5);
        ns.log(radio::events::Overheat { t: 88.5 });
        ns.log_text(Level::Warn, "app", "power dropped to a safe level", None);
        ns.log(radio::events::PowerSet { dbm: 5.0 });
        ns.sync()?;

        // -------------------------------------------------------------------
        // Live reading: the store is being WRITTEN right now and is read by the
        // same process — `store.reader()` is parallel to writing by construction.
        // It is created once and asks the store for the truth (the roots, the
        // schemas, the time anchors) on every query; rotation and a segment's
        // growing tail are ordinary events for it rather than damage. What is
        // visible is what is on the medium — which is what the `sync()` a line
        // above was for.
        // -------------------------------------------------------------------
        let live = store.reader();
        let now = live.query(&Query::new().kinds(KindFilter::LOGS).limit(3))?;
        println!("— live reading, the writer never stopped —");
        for e in &now.entries {
            println!("  {}", live.render(e, "en").unwrap_or_default());
        }
        assert!(now.damaged.is_empty());

        // -------------------------------------------------------------------
        // A subscription: the same window, but the stream does not end at the end
        // of the data — it waits for more. The reader sleeps while there is nothing
        // to write and wakes on the very first block that lands in a file, so no
        // polling on a timer is needed. `Idle` is silence, not the end; the end is
        // announced only by `Ended`.
        // -------------------------------------------------------------------
        let mut tail = live.follow(&Query::new().since(ns.now()).order(Order::Oldest))?;
        ns.log(radio::events::PowerSet { dbm: 12.0 });
        ns.log(radio::events::Started);
        ns.sync()?;

        println!("— a subscription to the stream —");
        let mut seen = 0;
        while seen < 2 {
            match tail.next(std::time::Duration::from_millis(200)) {
                Tail::Entry(e) => {
                    seen += 1;
                    println!("  {}", live.render(&e, "en").unwrap_or_default());
                }
                // In this example there is nothing to wait for: everything
                // written is already on the medium.
                Tail::Idle => break,
                Tail::Ended => break,
            }
        }
        assert_eq!(seen, 2, "the subscription handed out both records");
        assert!(tail.take_damage().is_empty());

        store.shutdown();
    }

    // -----------------------------------------------------------------------
    // From here on it is the offline viewer: the store is closed and its dump is
    // taken apart. A `Store` is not opened for a dump (it takes a lock on the root
    // and sweeps temporary files) — ALL of the dump's roots and schemas are named
    // at once; for a dump from another device there is `allow_foreign_segments`.
    // Completeness is checked at open time: a dump missing some class's tree is an
    // error rather than a silently shortened history. A dump's Reader changes
    // nothing and freezes a snapshot at the moment it opens.
    // -----------------------------------------------------------------------
    let reader = Reader::open_dump([&root], &[radio::SCHEMA])?;

    println!("— what is in the store —");
    let listing = reader.namespaces()?;
    for ns in &listing.namespaces {
        println!(
            "  {}: schema {} v{}, channels {:?}, {} B taken",
            ns.name, ns.schema_name, ns.protocol_version, ns.channels, ns.total_bytes
        );
    }
    // An unreadable namespace would reach `listing.damaged` rather than drop
    // out silently.
    assert!(listing.is_complete());

    // -----------------------------------------------------------------------
    // "The latest events" is the default query: newest to oldest. A record's text
    // was not on disk — it is assembled from the schema's template in the chosen
    // language. Every record has a relative time (a run plus microseconds), and a
    // wall-clock one only thanks to an anchor.
    // -----------------------------------------------------------------------
    println!("\n— the last 5 journal records —");
    let last = reader.query(&Query::new().kinds(KindFilter::LOGS).limit(5))?;
    for e in &last.entries {
        println!("  {}", line(&reader, e));
    }
    println!(
        "  (the answer was cut short by limit: {}, damaged: {})",
        last.truncated,
        last.damaged.len()
    );

    // -----------------------------------------------------------------------
    // Filters are computed from the schema BEFORE any scanning: levels and tags
    // are properties of types, and for such a selection the disk is read only
    // where those types are.
    // -----------------------------------------------------------------------
    println!("\n— Warn and above only —");
    for e in &reader
        .query(
            &Query::new()
                .kinds(KindFilter::LOGS)
                .min_level(Level::Warn)
                .order(Order::Oldest),
        )?
        .entries
    {
        println!("  {}", line(&reader, e));
    }

    println!("\n— everything with the thermal tag —");
    // Messages and samples carry tags (metrics have tags of their own); free
    // text and spans do not and cannot pass such a filter — they are excluded
    // rather than slipping through.
    for e in &reader
        .query(&Query::new().any_tag("thermal").order(Order::Oldest))?
        .entries
    {
        println!("  {}", line(&reader, e));
    }

    // -----------------------------------------------------------------------
    // Typed parsing: `render` gives the text, `reader.decode::<E>` the FIELDS,
    // with the same type the event was written with. The type is checked against
    // the schema of the record's namespace: a matching id from a foreign schema is
    // not a matching type. `None` means the record is not an event E; `Some(Err)`
    // that it declares itself an E but the fields did not parse (corruption that
    // must not be passed over).
    // -----------------------------------------------------------------------
    println!("\n— the fields of events, not the text —");
    for e in &reader
        .query(
            &Query::new()
                .event(radio::events::Overheat::ID)
                .order(Order::Oldest),
        )?
        .entries
    {
        if let Some(Ok(radio::events::Overheat { t })) = reader.decode(e) {
            println!("  the overheat as data: t = {t} — it can be computed with, not just read");
        }
    }

    // -----------------------------------------------------------------------
    // Telemetry. The metric's name, its unit, the state's label and the severity
    // are restored from the schema — an identifier and a value lay on disk.
    // -----------------------------------------------------------------------
    println!("\n— telemetry, oldest to newest —");
    for e in &reader
        .query(
            &Query::new()
                .kinds(KindFilter::TELEMETRY)
                .order(Order::Oldest),
        )?
        .entries
    {
        if let EntryKind::Sample {
            metric_name,
            unit,
            state_name,
            severity,
            value,
            ..
        } = &e.kind
        {
            println!(
                "  {} {} = {:?}{} [{:?}]{}",
                e.at,
                metric_name.unwrap_or("?"),
                value,
                unit.filter(|u| !u.is_empty())
                    .map(|u| format!(" {u}"))
                    .unwrap_or_default(),
                severity.unwrap_or_default(),
                state_name
                    .map(|s| format!(" state: {s}"))
                    .unwrap_or_default(),
            );
        }
    }

    // -----------------------------------------------------------------------
    // Time windows. A bound is either a BootTime (always present) or a UTC
    // (comparable with records only through an anchor).
    // -----------------------------------------------------------------------
    println!("\n— the first run's journal (boot {boot_first}) —");
    for e in &reader
        .query(
            &Query::new()
                .boot(boot_first)
                .kinds(KindFilter::LOGS)
                .order(Order::Oldest),
        )?
        .entries
    {
        println!("  {}", line(&reader, e));
    }

    println!("\n— a wall-clock window: the minute around the synchronization —");
    let minute = reader.query(
        &Query::new()
            .time_window(
                sync_at - chrono::TimeDelta::seconds(30),
                sync_at + chrono::TimeDelta::seconds(30),
            )
            .order(Order::Oldest),
    )?;
    println!(
        "  records: {} (both runs are in the window: one boot shares its anchor), \
         runs dropped: {}",
        minute.entries.len(),
        minute.unanchored.len()
    );

    // -----------------------------------------------------------------------
    // Carrying states through. States are written ON CHANGE; the window "the
    // second run" holds not one LinkState sample — but the state band on a chart
    // must not be empty: the link was in Lock the whole time. `with_state_seed`
    // puts the last sample of every state series before the window into `seeds` —
    // separately, without breaking "everything in entries lies inside the window".
    // -----------------------------------------------------------------------
    println!("\n— the second run plus states carried to the window's left edge —");
    let seeded = reader.query(
        &Query::new()
            .since(BootTime::new(BootCounter(boot_second), Micros(0)))
            .kinds(KindFilter::TELEMETRY)
            .with_state_seed(),
    )?;
    println!("  samples in the window: {}", seeded.entries.len());
    for seed in &seeded.seeds {
        if let EntryKind::Sample {
            metric_name,
            state_name,
            ..
        } = &seed.kind
        {
            println!(
                "  seed: {} = {} (a sample from the past, {})",
                metric_name.unwrap_or("?"),
                state_name.unwrap_or("?"),
                seed.at
            );
        }
    }

    // -----------------------------------------------------------------------
    // A stream instead of an assembled answer. `query` gathers everything in
    // memory; on a large store the only way to read a lot is `stream`: the
    // channels are merged by time, the records are handed out one at a time, and
    // the walk can be abandoned at any moment.
    // -----------------------------------------------------------------------
    println!("\n— the stream: the first 3 records, then we stop reading —");
    let mut stream = reader.stream(&Query::new().kinds(KindFilter::LOGS).order(Order::Oldest))?;
    for e in stream.by_ref().take(3) {
        println!("  {}", line(&reader, &e));
    }
    println!("  handed out: {}", stream.yielded());

    // -----------------------------------------------------------------------
    // An honest answer. A damaged fragment reaches `damaged` with its name and
    // the reason — the answer does not pretend to be complete. The other kind of
    // incompleteness is `unanchored`: the window is in wall-clock time while the
    // boot has no anchor, and there is nothing to compare its records with a clock
    // with. A store with not one synchronization answers a wall-clock window with
    // emptiness — but names the runs that dropped out instead of "the device wrote
    // nothing".
    // -----------------------------------------------------------------------
    let lone = std::env::temp_dir()
        .join("dduroc-examples")
        .join("02-no-sync");
    let _ = std::fs::remove_dir_all(&lone);
    {
        let store = Store::open(StoreConfig::new(&lone).with_budget_per_class(16 << 20))?;
        let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
        ns.log(radio::events::Started);
        ns.sync()?;
        store.shutdown(); // record_sync was never called: there is no anchor
    }
    let no_clock = Reader::open_dump([&lone], &[radio::SCHEMA])?;
    let asked = no_clock
        .query(&Query::new().time_window(sync_at - chrono::TimeDelta::hours(1), sync_at))?;
    println!(
        "\n— a wall-clock window with no anchor: records {}, runs dropped {:?} —",
        asked.entries.len(),
        asked.unanchored
    );

    Ok(())
}

/// A journal line: the time, the level, the text.
fn line(reader: &Reader, e: &dduroc::read::Entry) -> String {
    let clock = e
        .utc
        .map(|t| t.format("%H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "--:--:--".to_owned());
    let what = match &e.kind {
        EntryKind::Sample {
            metric_name, value, ..
        } => format!("{} = {value:?}", metric_name.unwrap_or("?")),
        _ => reader
            .render(e, "en")
            .unwrap_or_else(|| format!("{:?}", e.kind)),
    };
    format!(
        "{} | {clock} | {:9} | {what}",
        e.at,
        e.level().map(|l| l.as_str()).unwrap_or(""),
    )
}
