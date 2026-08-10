//! Сквозной цикл миграции через макрос: прошивка v1 пишет, прошивка v2
//! читает старую историю через шаги, потом приводит её физически.
//!
//! Ровно тот сценарий, ради которого миграции существуют: поле сменило тип
//! (i8 → f32 — postcard прочитал бы старые байты молча и неверно), тип
//! удалён из схемы, метрика сменила идентификатор. Два `schema!` с одним
//! именем в разных модулях — это «вчерашний» и «сегодняшний» билды.

use dduroc::prelude::*;
use dduroc::{EventId, MetricId, StoreConfig};
use dduroc_read::{EntryKind, Order, OwnedSampleValue, Query, Reader};

/// Схема вчерашней прошивки.
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
            Temp = 0x07 { vtype: f32 },
        }
    }
}

/// Схема сегодняшней: `dbm` стал f32, `Legacy` удалён, `Temp` переехал на
/// новый идентификатор.
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
            TempPa = 0x08 { vtype: f32 },
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

/// Ответ читателя, сведённый к проверяемому виду.
#[derive(Debug, PartialEq)]
enum Seen {
    Power(f32),
    Note(u32),
    Sample(MetricId, f32),
    Other(EventId),
}

fn read_all(root: &std::path::Path) -> Vec<Seen> {
    let reader = Reader::open(root, &[now::probe::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);
    result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Message { event, payload, .. } => match event.0 {
                0x01 => {
                    let p: now::probe::events::PowerSet =
                        dduroc::postcard::from_bytes(payload).expect("раскладка текущая");
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
    let cfg = StoreConfig::new(dir.path()).with_budget(16 << 20);

    // Вчера: прошивка v1 пишет свою историю.
    {
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-0", was::probe::SCHEMA).unwrap();
        ns.log(was::probe::events::PowerSet { dbm: -3 });
        ns.log(was::probe::events::Legacy {});
        ns.log(was::probe::events::Note { n: 7 });
        ns.series(was::probe::metrics::Temp).unwrap().sample(36.5);
        ns.sync().unwrap();
        store.shutdown();
    }

    // Сегодня: прошивка v2. Ничего ещё не мигрировано физически.
    let store = Store::open(cfg).unwrap();
    let ns = store.namespace("orc-0", now::probe::SCHEMA).unwrap();
    assert_eq!(ns.pending_migration(), Some((1, 2)), "долг назван");

    let expected = vec![
        Seen::Power(-3.0),
        Seen::Note(7),
        Seen::Sample(MetricId(0x08), 36.5),
    ];

    // Чтение корректно ДО прогона: шаги применяются на лету. Байт -3i8 в
    // раскладке f32 иначе разобрался бы молча и неверно, Legacy показался бы
    // «неизвестным типом», а Temp остался бы под старым номером.
    let before = read_all(dir.path());
    assert_eq!(before, expected, "чтение через шаги");

    // И рендер работает по текущему шаблону поверх мигрированного payload'а.
    {
        let reader = Reader::open(dir.path(), &[now::probe::SCHEMA]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let power = result
            .entries
            .iter()
            .find(|e| matches!(&e.kind, EntryKind::Message { event, .. } if event.0 == 1))
            .expect("PowerSet в ответе");
        assert_eq!(
            reader.render(power, "en").as_deref(),
            Some("power -3 dBm"),
            "шаблон v2 применим, потому что payload уже приведён"
        );
    }

    // Физический прогон.
    let report = ns.migrate().expect("прогон проходит");
    assert_eq!(report.rewritten, 1, "{report:?}");
    assert_eq!(
        report.records_rewritten, 3,
        "PowerSet, Note и отсчёт пережили шаг: {report:?}"
    );
    assert_eq!(report.records_dropped, 1, "Legacy удалён: {report:?}");
    assert_eq!(ns.pending_migration(), None, "долг погашен");

    // Чтение ПОСЛЕ прогона отвечает то же самое — в этом весь смысл: прогон
    // меняет носитель, а не ответ.
    assert_eq!(read_all(dir.path()), expected, "ответ не изменился");

    // Заголовки сегментов — только текущей версии.
    let channel = dir.path().join("orc-0").join("default");
    for entry in std::fs::read_dir(&channel).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|x| x == "seg") {
            let r = dduroc_engine::segment::SegmentReader::open(&path).unwrap();
            assert_eq!(r.header().protocol_version.0, 2, "{path:?}");
        }
    }

    // Повторный прогон — честный no-op.
    assert_eq!(ns.migrate().unwrap(), dduroc::MigrationReport::default());
    store.shutdown();
}

#[test]
fn old_fixtures_are_written_with_the_generated_history_types() {
    // history-типы получают Serialize именно ради этого: фикстура старой
    // версии собирается тем же типом, который декодирует шаг, — раскладка
    // объявлена один раз, и тест не дублирует её байтами.
    let bytes = dduroc::postcard::to_allocvec(&now::probe::v1::PowerSet { dbm: -3 }).unwrap();
    let old: now::probe::v1::PowerSet = dduroc::postcard::from_bytes(&bytes).unwrap();
    assert_eq!(old, now::probe::v1::PowerSet { dbm: -3 });
    assert_eq!(
        <now::probe::v1::PowerSet as dduroc::EventShape>::SHAPE_ID,
        EventId(0x01),
        "id раскладки — старый, из history"
    );
}
