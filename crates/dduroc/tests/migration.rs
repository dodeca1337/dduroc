//! The end-to-end migration cycle through the macro: firmware v1 writes,
//! firmware v2 reads the old history through the steps and then brings it up
//! physically.
//!
//! Exactly the scenario migrations exist for: a field changed type (i8 → f32 —
//! postcard would have read the old bytes silently and wrongly), a type was
//! removed from the schema, a metric changed its identifier. Two `schema!`
//! declarations of one name in different modules are "yesterday's" and
//! "today's" builds.

use dduroc::prelude::*;
use dduroc::{EventId, MetricId, StorageClass, StoreConfig};
use dduroc_read::{EntryKind, Order, OwnedSampleValue, Query, Reader};

/// Yesterday's firmware schema.
mod was {
    dduroc::schema! {
        name: probe,
        version: 1,
        languages: [en],

        events {
            PowerSet = 0x01 { level: Info, en: "power {dbm} dBm", dbm: i8 },
            Legacy = 0x05 { level: Info, en: "legacy" },
            Note = 0x03 { level: Info, en: "note {n}", n: u32 },
        }

        metrics {
            Temp = 0x07 { type: f32 },
        }
    }
}

/// Today's: `dbm` became an f32, `Legacy` was removed, `Temp` moved to a new
/// identifier.
mod now {
    dduroc::schema! {
        name: probe,
        version: 2,
        languages: [en],

        events {
            PowerSet = 0x01 { level: Info, en: "power {dbm} dBm", dbm: f32 },
            Note = 0x03 { level: Info, en: "note {n}", n: u32 },
        }

        metrics {
            TempPa = 0x08 { type: f32 },
        }

        history {
            1 { events { PowerSet = 0x01 { dbm: i8 } } }
        }

        migrations {
            1 => {
                v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) },
                event(0x05): drop,
                metric(0x07): metrics::TempPa,
            },
        }
    }
}

/// A reader's answer reduced to a checkable form.
#[derive(Debug, PartialEq)]
enum Seen {
    Power(f32),
    Note(u32),
    Sample(MetricId, f32),
    Other(EventId),
}

fn read_all(root: &std::path::Path) -> Vec<Seen> {
    let reader = Reader::open_dump([root], &[now::probe::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "damage: {:?}", result.damaged);
    result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Message { event, payload, .. } => match event.0 {
                0x01 => {
                    let p: now::probe::events::PowerSet = dduroc::postcard::from_bytes(payload)
                        .expect("the layout is the current one");
                    Some(Seen::Power(p.dbm))
                }
                0x03 => {
                    let p: now::probe::events::Note =
                        dduroc::postcard::from_bytes(payload).unwrap();
                    Some(Seen::Note(p.n))
                }
                _ => Some(Seen::Other(*event)),
            },
            EntryKind::Sample {
                metric,
                value: OwnedSampleValue::F32(v),
                ..
            } => Some(Seen::Sample(*metric, *v)),
            _ => None,
        })
        .collect()
}

#[test]
fn yesterdays_history_reads_the_same_before_and_after_the_physical_run() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 << 20);

    // Yesterday: firmware v1 writes its history.
    {
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-0", was::probe::SCHEMA).unwrap();
        ns.log(was::probe::events::PowerSet { dbm: -3 });
        ns.log(was::probe::events::Legacy);
        ns.log(was::probe::events::Note { n: 7 });
        ns.series(was::probe::metrics::Temp).unwrap().sample(36.5);
        ns.sync().unwrap();
        store.shutdown();
    }

    // Today: firmware v2. Nothing has been migrated physically yet.
    let store = Store::open(cfg).unwrap();
    let ns = store.namespace("orc-0", now::probe::SCHEMA).unwrap();
    assert_eq!(ns.pending_migration(), Some((1, 2)), "the debt is named");

    let expected = vec![
        Seen::Power(-3.0),
        Seen::Note(7),
        Seen::Sample(MetricId(0x08), 36.5),
    ];

    // Reading is correct BEFORE the run: the steps are applied on the fly. The
    // byte -3i8 in the f32 layout would otherwise parse silently and wrongly,
    // Legacy would show as an "unknown type", and Temp would stay under its old
    // number.
    let before = read_all(dir.path());
    assert_eq!(before, expected, "reading through the steps");

    // And rendering works from the current template over the migrated payload.
    {
        let reader = Reader::open_dump([dir.path()], &[now::probe::SCHEMA]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let power = result
            .entries
            .iter()
            .find(|e| matches!(&e.kind, EntryKind::Message { event, .. } if event.0 == 1))
            .expect("PowerSet in the answer");
        assert_eq!(
            reader.render(power, "en").as_deref(),
            Some("power -3 dBm"),
            "the v2 template applies because the payload has already been brought up"
        );
    }

    // The physical run.
    let report = ns.migrate().expect("the run goes through");
    assert_eq!(report.rewritten, 1, "{report:?}");
    assert_eq!(
        report.records_rewritten, 3,
        "PowerSet, Note and the sample survived the step: {report:?}"
    );
    assert_eq!(report.records_dropped, 1, "Legacy was deleted: {report:?}");
    assert_eq!(ns.pending_migration(), None, "the debt is paid");

    // Reading AFTER the run answers exactly the same — which is the whole
    // point: a run changes the medium, not the answer.
    assert_eq!(read_all(dir.path()), expected, "the answer did not change");

    // The segment headers are at the current version only.
    let channel = dir
        .path()
        .join("orc-0")
        .join(StorageClass::Default.as_str());
    for entry in std::fs::read_dir(&channel).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|x| x == "seg") {
            let r = dduroc_engine::segment::SegmentReader::open(&path).unwrap();
            assert_eq!(r.header().protocol_version.0, 2, "{path:?}");
        }
    }

    // A second run is an honest no-op.
    assert_eq!(ns.migrate().unwrap(), dduroc::MigrationReport::default());
    store.shutdown();
}

#[test]
fn old_fixtures_are_written_with_the_generated_history_types() {
    // The history types get Serialize for exactly this: a fixture of the old
    // version is assembled with the same type the step decodes with — the
    // layout is declared once, and the test does not duplicate it in bytes.
    let bytes = dduroc::postcard::to_allocvec(&now::probe::v1::PowerSet { dbm: -3 }).unwrap();
    let old: now::probe::v1::PowerSet = dduroc::postcard::from_bytes(&bytes).unwrap();
    assert_eq!(old, now::probe::v1::PowerSet { dbm: -3 });
    assert_eq!(
        <now::probe::v1::PowerSet as dduroc::EventShape>::SHAPE_ID,
        EventId(0x01),
        "the layout's id is the old one, from history"
    );
}

// ════════════════════════════════════════════════════════════════════════════
// Spans and sample values
// ════════════════════════════════════════════════════════════════════════════

/// Yesterday's firmware: the level was written in tenths as an integer, and
/// the span kind was called something else.
mod tenths {
    dduroc::schema! {
        name: gauge,
        version: 1,
        languages: [en],

        events { Mark = 0x01 { level: Info, en: "mark" } }
        metrics { Level = 0x02 { type: u64 } }
        spans { Calib = 0x01 }
    }
}

/// Today's: the level became a float in its own units and the span kind was
/// renamed. The earlier name is no longer in the schema — the key is a bare id.
mod units {
    dduroc::schema! {
        name: gauge,
        version: 2,
        languages: [en],

        events { Mark = 0x01 { level: Info, en: "mark" } }
        metrics { Level = 0x02 { type: f32, unit: "dB" } }
        spans { Calibration = 0x02 }

        migrations {
            1 => {
                metrics::Level: |v: u64| v as f32 / 10.0,
                span(0x01): spans::Calibration,
            },
        }
    }
}

/// The samples and span kinds in read order.
fn levels_and_spans(root: &std::path::Path) -> (Vec<f32>, Vec<Option<&'static str>>) {
    let reader = Reader::open_dump([root], &[units::gauge::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "damage: {:?}", result.damaged);
    let mut levels = Vec::new();
    let mut kinds = Vec::new();
    for e in &result.entries {
        match &e.kind {
            EntryKind::Sample {
                value: OwnedSampleValue::F32(v),
                ..
            } => levels.push(*v),
            EntryKind::SpanStart { kind_name, .. } => kinds.push(*kind_name),
            _ => {}
        }
    }
    (levels, kinds)
}

#[test]
fn a_span_kind_and_a_sample_value_migrate_like_everything_else() {
    // Events have had migrations for a long time, samples only a change of
    // identifier, spans nothing at all. Yet "the quantity is now written in its
    // own units" and "the span kind was renamed" are ordinary schema edits, and
    // without them the history would stay in the earlier layout forever.
    let dir = tempfile::tempdir().unwrap();
    let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 << 20);

    {
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-0", tenths::gauge::SCHEMA).unwrap();
        let cal = ns.span(tenths::gauge::spans::Calib);
        let level = ns.series(tenths::gauge::metrics::Level).unwrap();
        level.sample(365);
        level.sample(700);
        cal.close().unwrap();
        ns.sync().unwrap();
        store.shutdown();
    }

    let store = Store::open(cfg).unwrap();
    let ns = store.namespace("orc-0", units::gauge::SCHEMA).unwrap();
    assert_eq!(ns.pending_migration(), Some((1, 2)));

    // Reading through the steps — before any run.
    let expected = (vec![36.5f32, 70.0], vec![Some("Calibration")]);
    assert_eq!(
        levels_and_spans(dir.path()),
        expected,
        "reading through the steps"
    );

    // The physical run. A segment with spans is always rewritten: there is no
    // set of span kinds in the footer, and there is nothing to say "this
    // segment holds no such spans" with.
    let report = ns.migrate().expect("the run goes through");
    assert_eq!(report.rewritten, 1, "{report:?}");
    assert_eq!(
        report.records_dropped, 0,
        "spans are not deleted: {report:?}"
    );
    assert_eq!(ns.pending_migration(), None);

    // And the same answer after the run: a run changes the medium, not the
    // answer.
    assert_eq!(
        levels_and_spans(dir.path()),
        expected,
        "the answer did not change"
    );
    store.shutdown();
}
