//! The schema grew: history, migration rules, reading through the steps,
//! migrate().
//!
//! Run: `cargo run -p dduroc --example 03_schema_grows`
//!
//! The firmware was updated while the history on the medium stayed in the old
//! layout — and the danger is not "unreadability" but the opposite: postcard is
//! not self-describing, and the old bytes would parse silently under the new
//! decoders into plausible garbage (an i8 in an f32 layout is a number, just
//! not the right one).
//!
//! How it works:
//!  - reading is ALWAYS correct: a segment of an earlier version goes through
//!    the migration steps as it is read, with no need to wait for a physical run;
//!  - `ns.migrate()` is the explicit physical run: it rewrites the old segments
//!    into the current layout. When to call it is the application's decision —
//!    a run burns flash wear (after an update, in a quiet hour);
//!  - a run changes the medium, not the answer: reading before and after gives
//!    the same thing.

use dduroc::MigrationReport;
use dduroc::prelude::*;
use dduroc::read::{EntryKind, KindFilter, Order, Query, Reader};

// ---------------------------------------------------------------------------
// Yesterday's firmware, version 1. In reality this module is not in the new
// build; here it exists so that the example can write the "old" history itself.
// ---------------------------------------------------------------------------
mod yesterday {
    dduroc::schema! {
        name: radio,
        version: 1,
        languages: [en],

        events {
            // Power was kept as an integer dBm — decided to be a mistake.
            PowerSet = 0x01 { level: Info, en: "power {dbm} dBm", dbm: i8 },
            // A type the new schema does not need at all.
            LegacyPing = 0x05 { level: Debug, en: "ping" },
            // A type the migration does not touch.
            Note = 0x03 { level: Info, en: "note {n}", n: u32 },
        }

        metrics {
            // Temperature was renamed and moved to a new id.
            Temp = 0x07 { type: f32, unit: "°C" },
        }
    }
}

// ---------------------------------------------------------------------------
// Today's firmware, version 2. Three changes:
//   dbm: i8 → f32; LegacyPing removed; Temp moved to id 0x08.
// ---------------------------------------------------------------------------
mod today {
    dduroc::schema! {
        name: radio,
        version: 2,
        languages: [en],

        events {
            PowerSet = 0x01 { level: Info, en: "power {dbm} dBm", dbm: f32 },
            Note = 0x03 { level: Info, en: "note {n}", n: u32 },
        }

        metrics {
            TempPa = 0x08 { type: f32, unit: "°C" },
        }

        // The layouts of past versions — the ones the steps consume. Only the
        // types that changed are listed, and only their fields. The macro
        // generates `v1::PowerSet` with Deserialize (the step decodes the old
        // bytes with it) and Serialize (tests write fixtures of the old version
        // with the same type).
        history {
            1 { events { PowerSet = 0x01 { dbm: i8 } } }
        }

        // The step "from version 1". A rule's key says WHAT to change, its action HOW:
        //   v1::Type: |old| ...   — decode the old layout, the closure, encode;
        //   event(0xID): drop     — the type is gone, its name is no longer in the schema;
        //   metric(0xID): metrics::Name — an id remap, the values are not touched.
        // The affected sets are inferred from the keys: a segment where those types
        // are absent (Note and only Note) is not rewritten and spends no flash.
        // What the rules do not touch (Note, text, spans) passes through as it is.
        //
        // For changes the rules cannot express there is still the raw step:
        //   `1 => migrate_v1,` — an fn(MigrationInput) with full access.
        migrations {
            1 => {
                v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) },
                event(0x05): drop,
                metric(0x07): metrics::TempPa,
            },
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("dduroc-examples").join("03");
    let _ = std::fs::remove_dir_all(&root);
    let cfg = StoreConfig::new(&root).with_budget_per_class(16 << 20);

    // Yesterday: firmware v1 writes its history.
    {
        let store = Store::open(cfg.clone())?;
        let ns = store.namespace("orc-radio-0", yesterday::radio::SCHEMA)?;
        ns.log(yesterday::radio::events::PowerSet { dbm: -3 });
        ns.log(yesterday::radio::events::LegacyPing);
        ns.log(yesterday::radio::events::Note { n: 7 });
        ns.series(yesterday::radio::metrics::Temp)?.sample(36.5);
        ns.sync()?;
        store.shutdown();
    }

    // Today: firmware v2 opens the same directory.
    let store = Store::open(cfg)?;
    let ns = store.namespace("orc-radio-0", today::radio::SCHEMA)?;

    // The debt is visible at once: the medium holds segments in a layout older
    // than the schema. That is not an error but the normal state after an
    // update.
    println!("unfinished migration: {:?}", ns.pending_migration());

    // -----------------------------------------------------------------------
    // Reading BEFORE the run. The steps are applied on the fly: -3i8 became
    // -3.0f32 and renders with the v2 template, LegacyPing is already absent, and
    // Temp answers under its new identifier. Not one record is shown in the old
    // layout.
    // -----------------------------------------------------------------------
    let before = read_all(&root)?;
    println!("\nbefore the run:");
    print_entries(&before);

    // -----------------------------------------------------------------------
    // The physical run. The heavy work happens on the calling thread; writing
    // does not stop meanwhile. A power loss anywhere is safe: every segment stays
    // either as it was or already rewritten, and a repeat call continues with the
    // rest.
    // -----------------------------------------------------------------------
    let report = ns.migrate()?;
    println!("\nthe run's report:");
    println!("  segments rewritten:    {}", report.rewritten);
    println!("  already current:       {}", report.already_current);
    println!("  untouched by the steps: {}", report.skipped_untouched);
    println!("  records rewritten:     {}", report.records_rewritten);
    println!("  records deleted:       {}", report.records_dropped);
    println!("the debt after the run: {:?}", ns.pending_migration());

    // A run changes the medium, not the answer: reading gives the same thing.
    let after = read_all(&root)?;
    println!("\nafter the run:");
    print_entries(&after);
    assert_eq!(before, after, "the reader's answer has no right to change");

    // A repeat call is an honest no-op: there is no debt and no flash is spent.
    assert_eq!(ns.migrate()?, MigrationReport::default());

    store.shutdown();
    Ok(())
}

/// Read the whole journal and telemetry through the eyes of schema v2.
fn read_all(root: &std::path::Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let reader = Reader::open_dump([root], &[today::radio::SCHEMA])?;
    let result = reader.query(
        &Query::new()
            .kinds(KindFilter {
                text: false,
                ..KindFilter::default()
            })
            .order(Order::Oldest),
    )?;
    assert!(result.is_complete(), "damage: {:?}", result.damaged);

    Ok(result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Message { event, .. } => Some(format!(
                "{} (id 0x{:02x})",
                reader.render(e, "en").unwrap_or_else(|| "?".to_owned()),
                event.0
            )),
            EntryKind::Sample {
                metric,
                metric_name,
                value,
                ..
            } => Some(format!(
                "{} = {value:?} (id 0x{:02x})",
                metric_name.unwrap_or("?"),
                metric.0
            )),
            _ => None,
        })
        .collect())
}

fn print_entries(lines: &[String]) {
    for l in lines {
        println!("  {l}");
    }
}
