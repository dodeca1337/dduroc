//! The firmware writes: a schema, events, telemetry, spans, free text.
//!
//! An end-to-end example of the writing side — what a microservice on a device
//! does. Run: `cargo run -p dduroc --example 01_device_writes`
//!
//! Nothing that can be recovered from the firmware's code is kept on disk:
//! neither text, nor field names, nor levels. A record is a type identifier, a
//! time delta and binary fields; everything else is substituted by a reader
//! from this same schema (example 02).

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass};

// ---------------------------------------------------------------------------
// The schema is the single source of truth about what this service writes.
//
// The macro generates the module `radio`: `radio::SCHEMA`, the event types
// (`radio::events::PowerSet`), the metric constants (`radio::metrics::TempPa`),
// the span kinds (`radio::spans::Calibration`) and the decoders for reading.
// ---------------------------------------------------------------------------
dduroc::schema! {
    name: radio,
    version: 1,
    // The order of the languages determines their numbers in the decoders; a
    // reader asks for one by name ("ru"). The templates are kept in the
    // firmware, not on disk.
    languages: [en, ru],

    events {
        // The level and the tags are static properties of the type: they are
        // not written to disk, and filtering by them reduces, for a reader, to
        // a set of identifiers.
        PowerSet = 0x01 {
            level: Info,
            tags: [rf],
            en: "power set to {dbm} dBm",
            ru: "мощность {dbm} дБм",
            dbm: f32,
        },
        // `store: critical` sends the type into the critical channel: its own
        // directory, an fdatasync at once (group commit), no compression, and a
        // separate queue where on overflow the caller waits rather than loses.
        Overheat = 0x02 {
            level: Error,
            store: critical,
            tags: [thermal],
            // Rust's format specifiers work inside a template.
            en: "overheat: {t:.1} °C on sensor {sensor}",
            ru: "перегрев: {t:.1} °C на датчике {sensor}",
            t: f32,
            sensor: u8,
        },
        Started = 0x03 {
            level: Debug,
            en: "radio started",
            ru: "радио запущено",
        },
    }

    metrics {
        // A continuous quantity. `warn:`/`alarm:` are the ranges of what is
        // NORMAL (data: the bands on a chart are drawn from them and they can
        // be overridden at runtime): outside `warn` is a warning, outside
        // `alarm` a fault.
        TempPa = 0x01 { type: f32, unit: "°C", tags: [thermal],
                        warn: ..=70.0, alarm: ..=85.0 },
        // A shape a range cannot express is a TRIGGER predicate (`v` is the
        // value, the polarity is the opposite, hence a different key): VSWR is
        // critical both above and below one, and alarming only above.
        Vswr = 0x04 { type: f32, warn_if: v > 1.5,
                      alarm_if: v > 3.0 || v < 1.0 },
        // A state machine as a time series: only the code goes to disk, while
        // the state's name and its severity a reader takes from the schema. The
        // codes are explicit: positional numbering would shift on an insertion.
        LinkState = 0x02 {
            states: [alarm Los = 0, warn Sync = 1, Lock = 2],
            tags: [rf],
        },
        // A binary snapshot (a spectrum, a register dump) goes into the
        // telemetry channel.
        Spectrum = 0x03 { type: blob, store: telemetry },
    }

    spans {
        Calibration = 0x01,
        PowerRamp = 0x02,
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("dduroc-examples").join("01");
    let _ = std::fs::remove_dir_all(&root);

    // -----------------------------------------------------------------------
    // The store. One Store per process: it owns the writer thread and the run
    // counter. Budgets are mandatory in spirit: without them the journal will eat
    // the medium.
    // -----------------------------------------------------------------------
    let store = Store::open(
        StoreConfig::new(&root)
            // The budget of a CLASS across the whole store: "all logs get this
            // much". The channels of every namespace of the class draw on the
            // shared budget; the sum of the class budgets is the medium's
            // occupancy ceiling.
            .with_budget_per_class(16 << 20)
            // A class's policy can be overridden whole: its own budget, its own
            // sync rhythm, its own medium (`root:` — critical data on a
            // protected partition, say). The directory name is the class
            // itself.
            .channel(
                StorageClass::Telemetry,
                ChannelConfig {
                    // Telemetry tolerates a minute unwritten — fewer
                    // fdatasyncs.
                    sync_interval: std::time::Duration::from_secs(60),
                    ..ChannelConfig::new(64 << 20)
                },
            ),
    )?;

    // A namespace is an instance of a service: "orc-radio-0" and "orc-radio-1"
    // have their own history and their own directories, and may share a schema.
    let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;

    // -----------------------------------------------------------------------
    // Events. `log` returns nothing — logging does not influence control flow:
    // there is nothing to do with a failure at the call site, and a loss is
    // counted in `store.stats().dropped` and announced by a notice in the record
    // stream itself.
    // -----------------------------------------------------------------------
    ns.log(radio::events::Started);
    ns.log(radio::events::PowerSet { dbm: 27.5 });

    // Whoever needs a verdict on the spot has the paired `try_*`. Errors come
    // in two kinds: `loses_record` (the disk is behind, the record is lost) and
    // `breaks_contract` (the type is not from this schema — a build defect).
    if let Err(e) = ns.try_log(radio::events::Overheat { t: 91.0, sensor: 1 }) {
        eprintln!(
            "the write did not go through: {e} (a loss: {})",
            e.loses_record()
        );
    }

    // -----------------------------------------------------------------------
    // Telemetry. A series is opened once, and a sample after that looks nothing
    // up. The value type is baked into the metric constant: `sample` on TempPa
    // will not take an integer, and on LinkState not a state of another metric.
    // -----------------------------------------------------------------------
    let temp = ns.series(radio::metrics::TempPa)?;
    for value in [36.6, 52.0, 71.5] {
        temp.sample(value);
    }

    let link = ns.series(radio::metrics::LinkState)?;
    link.sample(radio::metrics::LinkState::Sync);
    link.sample(radio::metrics::LinkState::Lock);

    ns.series(radio::metrics::Spectrum)?
        .sample(vec![0x42u8; 512]);

    // Limits known only at runtime (the hardware model was determined at
    // start): they override the schema's and are never written to disk — they
    // are a property of the installation, not of the measurement.
    ns.set_thresholds(radio::metrics::TempPa, ..=60.0, ..=75.0)?;

    // The engine raises no events of its own: what to do about a bound being
    // crossed is the application's decision. A value's severity can be asked
    // for at any moment. 65 °C is normal by the schema's limits (warn up to 70)
    // and already a warning by the effective ones (warn up to 60). The value is
    // the same one a sample would take: there is no need to assemble it by
    // hand, and the type comes from the same metric constant that opens a
    // series.
    println!(
        "65.0 °C by the effective limits: {:?} (by the schema's it would be Normal)",
        ns.severity_of(radio::metrics::TempPa, 65.0)
    );
    if temp.severity_of(65.0) >= Severity::Warn {
        ns.log(radio::events::Overheat { t: 65.0, sensor: 0 });
    }

    // A series already knows its metric — it can be asked directly, and the
    // states are then named rather than given as codes.
    println!(
        "state {:?}: {:?}",
        radio::metrics::LinkState::Los,
        link.severity_of(radio::metrics::LinkState::Los)
    );

    // The schema's predicates work everywhere severity is asked for — here and
    // in a reader of a dump alike.
    println!(
        "VSWR 0.5 (a broken feeder): {:?} — a range of what is normal cannot express that",
        ns.severity_of(radio::metrics::Vswr, 0.5)
    );

    // The extreme form of a limit is a closure with captured context: it takes
    // the diagnosis over entirely (beating both the schema and set_thresholds)
    // until it is removed. A latch, for instance: after the first overheat, a
    // warning until reset, whatever the temperature is now.
    let tripped = std::sync::atomic::AtomicBool::new(false);
    ns.set_severity_fn(radio::metrics::TempPa, move |v| {
        use std::sync::atomic::Ordering;
        if v > 60.0 {
            tripped.store(true, Ordering::Relaxed);
        }
        if tripped.load(Ordering::Relaxed) {
            Severity::Alarm
        } else {
            Severity::Normal
        }
    })?;
    let _ = ns.severity_of(radio::metrics::TempPa, 65.0); // latched
    println!(
        "36.6 °C after the overheat: {:?} — the closure remembers the context",
        ns.severity_of(radio::metrics::TempPa, 36.6)
    );
    ns.clear_severity_fn(radio::metrics::TempPa)?; // the data applies again

    // -----------------------------------------------------------------------
    // Spans are stretches of work. The end is written when the guard is dropped,
    // during a panic included: a reader shows an unclosed span as cut short.
    // -----------------------------------------------------------------------
    {
        let cal = ns.span(radio::spans::Calibration);
        cal.log(radio::events::PowerSet { dbm: 10.0 }); // attached to the span

        {
            let ramp = cal.child(radio::spans::PowerRamp); // nesting
            ns.log_in(&ramp, radio::events::PowerSet { dbm: 20.0 });
        } // PowerRamp ends here

        cal.log(radio::events::PowerSet { dbm: 30.0 });
    } // Calibration ends here

    // -----------------------------------------------------------------------
    // Free text is for what a schema cannot describe in advance: the bridge from
    // tracing/log, a panic handler. It costs more than an event (the text lies on
    // disk as it is) — the firmware's working path should be schema-based.
    // -----------------------------------------------------------------------
    ns.log_text(
        Level::Warn,
        "app",
        "the amplifier configuration is out of date, the default profile was taken",
        None,
    );

    // -----------------------------------------------------------------------
    // Time. The device has no battery-backed clock: every record always has only
    // a BootTime — the run number plus microseconds since it started. Wall-clock
    // time appears after the fact: the anchor is retroactive, and one
    // synchronization gives a UTC to every record of that hardware boot — those
    // made before it included.
    // -----------------------------------------------------------------------
    let early = store.now();
    println!(
        "before the synchronization: to_utc(now) = {:?}",
        store.to_utc(early)
    );

    let accepted = store.record_sync(Utc::now(), SyncSource::Ntp)?;
    println!(
        "the NTP anchor was accepted: {accepted}; the same early stamp is now: {:?}",
        store.to_utc(early)
    );
    // A less trustworthy source does not override a more trustworthy one: the
    // order is User < Ntp < Gps.
    let overwritten = store.record_sync(Utc::now(), SyncSource::User)?;
    println!("a manual entry over NTP accepted: {overwritten}");

    // -----------------------------------------------------------------------
    // Finishing. `sync` waits for the medium; `shutdown` writes out, seals the
    // segments and stops the writer.
    // -----------------------------------------------------------------------
    ns.sync()?;

    let stats = store.stats();
    println!(
        "\nstats: records {}, blocks {}, bytes {}, lost {}, refused {}",
        stats.records_written,
        stats.blocks_written,
        stats.bytes_written,
        stats.dropped,
        stats.rejected,
    );
    assert!(
        stats.is_clean(),
        "there must be no losses in this example: {stats:?}"
    );

    store.shutdown();

    // What landed on disk: a directory per namespace, inside it a directory per
    // channel (the storage classes from the schema), and `.seg` segments in
    // those.
    println!("\nthe store in {}:", root.display());
    print_tree(&root, 0)?;
    Ok(())
}

fn print_tree(dir: &std::path::Path, depth: usize) -> std::io::Result<()> {
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let meta = entry.metadata()?;
        println!(
            "{:indent$}{}{}",
            "",
            entry.file_name().to_string_lossy(),
            if meta.is_dir() {
                "/".to_owned()
            } else {
                format!("  ({} B)", meta.len())
            },
            indent = depth * 2
        );
        if meta.is_dir() {
            print_tree(&entry.path(), depth + 1)?;
        }
    }
    Ok(())
}
