//! Сквозные проверки под нагрузкой: ротация по бюджету, потолок хранилища,
//! обратное давление критической очереди, многопоточная запись и чтение
//! живого хранилища.
//!
//! Всё это до сих пор держалось на юнит-тестах и ревью. Ротация проверялась
//! на инвентаре из файлов, набитых нулями, — ни один тест не заставлял writer
//! действительно выйти за бюджет; обратное давление не исполнялось ни разу;
//! потоков в тестах не было вовсе, хотя писать из многих потоков — основной
//! режим работы библиотеки.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, QueueSizes, StoreConfig};
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
        Seq = 0x01 { vtype: u64 },
        Bulk = 0x02 { vtype: blob },
    }
}

/// Несжимаемые байты: LZ4 не должен свести объём к нулю, иначе сегменты не
/// кончатся и проверять будет нечего.
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

/// Суммарный размер сегментов в дереве каталогов.
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

/// Номера, дошедшие до диска, в порядке чтения.
fn sequence(root: &Path) -> Vec<u64> {
    let reader = Reader::open(root, &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);
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
fn rotation_drops_the_oldest_and_keeps_the_channel_inside_its_budget() {
    // Ротация проверялась только на инвентаре: файлы из нулей, `enforce_budget`
    // вызывался тестом напрямую. Заставить writer действительно выйти за
    // бюджет не пробовал никто — а именно там сходятся преаллокация, учёт
    // размера после запечатывания и защита активного сегмента.
    const BUDGET: u64 = 16 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget(BUDGET)).unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();
    let seq = ns.series(pressure::metrics::Seq).unwrap();
    let bulk = ns.series(pressure::metrics::Bulk).unwrap();

    // Пишем вдвое больше бюджета: каждая порция помечена своим номером.
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
    assert_eq!(stats.dropped, 0, "очередь успевала: {stats:?}");
    assert!(
        stats.segments_rotated > 0,
        "иначе тест не про ротацию: {stats:?}"
    );

    let occupied = bytes_under(dir.path());
    assert!(
        occupied <= BUDGET,
        "канал занимает {occupied} при бюджете {BUDGET}"
    );

    // И главное — ротация съела НАЧАЛО истории, а не её конец. Обратное
    // означало бы, что прибор выбрасывает свежие данные, оставляя древние.
    let seen = sequence(dir.path());
    assert!(!seen.is_empty(), "что-то обязано было уцелеть");
    assert_eq!(
        *seen.last().unwrap(),
        rounds - 1,
        "последняя запись обязана уцелеть"
    );
    assert!(
        seen[0] > 0,
        "иначе ничего не вытеснено и бюджет соблюдён случайно"
    );
    assert!(
        seen.windows(2).all(|w| w[0] < w[1]),
        "хвост истории обязан остаться непрерывным и упорядоченным"
    );
}

#[test]
fn the_store_ceiling_holds_when_channel_budgets_do_not_add_up() {
    // Поканальный бюджет — не бюджет носителя: неймспейсов на приборе тысячи,
    // и сумма их бюджетов кратно больше любой карты. Здесь два неймспейса, у
    // каждого бюджет во весь потолок хранилища: по отдельности ротация в них
    // не сработает почти никогда, а вместе они обязаны уложиться в потолок.
    const CEILING: u64 = 12 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget(CEILING)
            .with_total_budget(CEILING),
    )
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
        "хранилище занимает {occupied} при потолке {CEILING}"
    );
    assert_eq!(
        store.stats().over_budget,
        0,
        "потолок выполним: активных сегментов всего два"
    );
    // Оба неймспейса участвовали — вытеснение шло по возрасту, а не по
    // принципу «кто последний писал».
    for name in ["orc-0", "orc-1"] {
        assert!(
            bytes_under(&dir.path().join(name)) > 0,
            "{name} вытеснен целиком: вытеснение обязано идти по возрасту"
        );
    }
}

#[test]
fn a_full_critical_queue_makes_the_caller_wait_and_loses_nothing() {
    // Обещание критической очереди — «не потеряно»: при переполнении
    // вызывающий ждёт места, а не получает дыру. Путь ожидания не исполнялся
    // ни одним тестом, то есть обещание держалось на чтении кода.
    const N: u64 = 400;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget(16 << 20)
            // Очередь на одну запись: ожидание становится не редкостью, а
            // правилом — и проверяется именно оно.
            .with_queues(QueueSizes {
                normal: 1,
                critical: 1,
            }),
    )
    .unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();

    for _ in 0..N {
        ns.try_log(pressure::events::Alarm {})
            .expect("критическая запись не имеет права быть отвергнутой");
    }
    ns.sync().unwrap();
    store.shutdown();

    let stats = store.stats();
    assert!(
        stats.backpressure_waits > 0,
        "иначе ожидание не исполнялось и тест пуст: {stats:?}"
    );
    assert_eq!(
        stats.dropped, 0,
        "критическая запись не теряется: {stats:?}"
    );

    let reader = Reader::open(dir.path(), &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);
    let alarms = result
        .entries
        .iter()
        .filter(|e| matches!(&e.kind, EntryKind::Message { event, .. } if event.0 == 2))
        .count();
    assert_eq!(alarms as u64, N, "ждали места — значит все дошли");
    assert!(
        result.entries.iter().all(|e| &*e.channel == "critical"),
        "критические события обязаны лежать в своём канале"
    );
}

#[test]
fn many_threads_write_one_namespace_without_losing_or_duplicating() {
    // Писать из многих потоков — основной режим работы библиотеки, и ни один
    // тест этого не делал: очередь, монотонность времени внутри канала и
    // устойчивая сортировка батча проверялись поодиночке и умозрительно.
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 500;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget(64 << 20)).unwrap();
    let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let seq = ns.series(pressure::metrics::Seq).unwrap();
            s.spawn(move || {
                for i in 0..PER_THREAD {
                    // Очередь переполняется — ждём места, а не теряем:
                    // тест про сохранность, а не про поведение при отказе.
                    while seq.try_sample(t * PER_THREAD + i).is_err() {
                        std::thread::yield_now();
                    }
                }
            });
        }
    });

    ns.sync().unwrap();
    store.shutdown();
    assert_eq!(store.stats().dropped, 0, "ничего не отброшено");

    let mut seen = sequence(dir.path());
    assert_eq!(
        seen.len() as u64,
        THREADS * PER_THREAD,
        "ни потерь, ни дублей"
    );
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(
        seen.len() as u64,
        THREADS * PER_THREAD,
        "каждый номер обязан встретиться ровно раз"
    );
}

#[test]
fn reading_a_live_store_never_takes_back_what_it_already_showed() {
    // Читатель работает и на самом приборе, параллельно записи. Такого теста
    // не было ни одного, а свойство важнее полноты: незапечатанный сегмент
    // дочитывается сканом, и оборванный на полуслове блок — обычное дело.
    // Требовать от живого чтения полноты нельзя, а вот отдать назад уже
    // показанное — нельзя ему.
    const N: u64 = 3_000;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget(64 << 20)).unwrap();
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
            let reader = Reader::open(dir.path(), &[pressure::SCHEMA]).unwrap();
            let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
            let count = result
                .entries
                .iter()
                .filter(|e| matches!(e.kind, EntryKind::Sample { .. }))
                .count();
            assert!(
                count >= floor,
                "чтение отдало {count} записей после {floor}: уже показанное \
                 не имеет права исчезать"
            );
            floor = count;
            queries += 1;
        }
        queries
    });

    assert!(queries > 0, "иначе чтение не пересеклось с записью");
    ns.sync().unwrap();
    store.shutdown();

    // А после остановки — уже полнота: всё принятое обязано быть на месте.
    let seen = sequence(dir.path());
    assert_eq!(seen.len() as u64, N, "после остановки ответ полон");
    assert!(seen.windows(2).all(|w| w[0] < w[1]), "и упорядочен");
}

#[test]
fn a_ceiling_below_a_single_segment_is_refused_at_open() {
    // Потолок ниже пары сегментов невыполним по построению: активный сегмент
    // вытеснить нельзя. Узнать об этом при открытии — единственный момент,
    // когда это ещё поправимо.
    let dir = tempfile::tempdir().unwrap();
    let segment = ChannelConfig::segment_size_for(64 << 20);
    let err = Store::open(
        StoreConfig::new(dir.path())
            .with_budget(64 << 20)
            .with_total_budget(segment),
    )
    .expect_err("невыполнимый потолок обязан быть отвергнут");
    assert!(matches!(err, dduroc::Error::BadChannel { .. }), "{err}");

    Store::open(
        StoreConfig::new(dir.path())
            .with_budget(64 << 20)
            .with_total_budget(segment * 2),
    )
    .expect("двух сегментов уже достаточно");
}
