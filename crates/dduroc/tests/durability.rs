//! Сквозные проверки долговечности: что происходит с хранилищем после
//! аварийной остановки, при записи крупнее сегмента и при переподъёме
//! неймспейса.
//!
//! Все три сценария роняли данные или ёмкость и ни одним из существующих
//! тестов не ловились: они требуют либо настоящей гибели процесса, либо
//! заведомо неудобных размеров.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StoreConfig};
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

/// Имена и размеры сегментов канала.
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

/// Несжимаемые байты: xorshift, чтобы LZ4 не смог их ужать и блок
/// действительно оказался больше сегмента.
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

/// Переменная окружения, которой тест просит дочерний процесс сыграть роль
/// прибора, у которого отключили питание.
const CRASH_ROOT: &str = "DDUROC_TEST_CRASH_ROOT";

#[test]
fn crash_does_not_cost_a_whole_segment_of_budget() {
    // Дочерняя роль: пишем, синхронизируем и гибнем, не запечатав сегмент.
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
    let channel = dir.path().join("orc-0").join("default");
    let segment_bytes = ChannelConfig::new(0).segment_bytes;

    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "crash_does_not_cost_a_whole_segment_of_budget",
            "--exact",
            "--nocapture",
        ])
        .env(CRASH_ROOT, dir.path())
        .output()
        .expect("дочерний процесс запускается");
    assert!(!out.status.success(), "дочерний процесс обязан упасть");

    // После аварии сегмент лежит незапечатанным и занимает всю преаллокацию.
    let crashed = segments(&channel);
    assert_eq!(crashed.len(), 1);
    assert_eq!(crashed[0].1, segment_bytes, "преаллокация на месте");

    // Перезапуск обязан вернуть её: незапечатанный сегмент числится в бюджете
    // канала целиком, и четыре аварии подряд при бюджете 16 МиБ съели бы его
    // весь — ротация принялась бы за живую историю.
    {
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        ns.log(durability::events::Tick);
        ns.sync().unwrap();
        assert_eq!(
            store.stats().truncated_tails,
            0,
            "хвост был целым: fdatasync прошёл до гибели"
        );
        store.shutdown();
    }

    let after = segments(&channel);
    let total: u64 = after.iter().map(|(_, s)| s).sum();
    assert_eq!(
        after.len(),
        2,
        "сегмент прошлого запуска и новый: {after:?}"
    );
    assert!(
        total < segment_bytes / 8,
        "после восстановления канал занимает {total} байт при сегменте \
         {segment_bytes}: преаллокация не возвращена"
    );

    // И главное — все записи на месте, повреждений нет.
    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);
    assert_eq!(result.entries.len(), 4, "три до аварии и одна после");
}

#[test]
fn crashed_segment_gets_a_footer_so_migration_can_see_it() {
    // Восстановление собирает footer тем же обходом, которым ищет конец
    // данных. Без множества типов в нём миграция прошла бы мимо сегмента, а
    // читатель искал бы блоки сканом.
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

    let channel = dir.path().join("orc-0").join("default");
    let path = channel.join(&segments(&channel)[0].0);
    let reader = dduroc_engine::segment::SegmentReader::open(&path).unwrap();
    assert!(reader.is_sealed(), "оборванный сегмент запечатан");
    let footer = reader.footer().expect("footer читается");
    assert_eq!(footer.events, vec![dduroc::EventId(1)]);
    assert_eq!(
        footer.metrics,
        vec![dduroc::MetricId(1)],
        "миграция обязана увидеть, что сегмент затронут"
    );
}

#[test]
fn record_larger_than_a_segment_is_refused_not_written_past_it() {
    // Преаллокация делается ради одной гарантии: ENOSPC приходит один раз,
    // при создании сегмента, а не посреди записи критического события. Запись
    // за её границу эту гарантию отменяет, поэтому блок, который не помещается
    // даже в свежий сегмент, отбрасывается — и объявляется потерянным.
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

    let channel = dir.path().join("orc-0").join("default");
    for (name, size) in segments(&channel) {
        assert!(
            size <= segment_bytes,
            "сегмент {name} вырос до {size} при пределе {segment_bytes}: \
             запись ушла за преаллокацию"
        );
    }

    let stats = store.stats();
    assert!(stats.dropped >= 1, "потеря обязана быть учтена: {stats:?}");
    assert_eq!(stats.io_errors, 0, "это не отказ носителя: {stats:?}");

    // Дыра объявлена в самом потоке, а не только в счётчике.
    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    let announced = result.entries.iter().any(|e| {
        matches!(&e.kind, dduroc_read::EntryKind::Text { text, .. }
                 if text.starts_with("потеряно записей"))
    });
    assert!(announced, "потеря обязана быть видна в потоке: {result:?}");
}

#[test]
fn reopened_namespace_does_not_leave_a_second_state_on_the_directory() {
    // Ручка неймспейса отпускается — writer обязан запечатать его сегменты и
    // освободить слот. Иначе на один каталог пришлось бы два состояния канала
    // со своими инвентарями, и ротация одного удалила бы сегмент, открытый
    // другим: запись продолжалась бы в файл без имени и пропала бы вся.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let channel = dir.path().join("orc-0").join("default");

    for round in 0..4 {
        let ns = store.namespace("orc-0", durability::SCHEMA).unwrap();
        for _ in 0..50 {
            ns.log(durability::events::Tick);
        }
        ns.sync().unwrap();
        drop(ns);
        // Освобождение идёт без ответа — в `Drop` ждать носитель нельзя, —
        // но команды обрабатываются по очереди, поэтому любая следующая
        // команда служит барьером.
        store.sync().unwrap();

        // После освобождения имя снова доступно, а сегмент уже запечатан:
        // незапечатанных за спиной оставаться не должно.
        let unsealed = segments(&channel)
            .into_iter()
            .filter(|(name, _)| {
                let path = channel.join(name);
                dduroc_engine::segment::SegmentReader::open(&path)
                    .map(|r| !r.is_sealed())
                    .unwrap_or(false)
            })
            .count();
        assert_eq!(unsealed, 0, "круг {round}: остался незапечатанный сегмент");
    }
    store.shutdown();

    // Ни одна запись не потерялась по дороге.
    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);
    assert_eq!(result.entries.len(), 200, "четыре круга по пятьдесят");
    assert_eq!(store.stats().dropped, 0);
}

#[test]
fn records_enqueued_before_the_handle_is_dropped_still_land() {
    // Освобождение неймспейса вычерпывает очереди досуха: то, на что `log()`
    // уже ответил `Ok`, обязано лечь на диск, даже если ручку отпустили
    // следующей строкой.
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
    } // ручка отпущена без sync

    // Повторный подъём обслуживается после освобождения — очередь команд
    // сохраняет порядок.
    drop(store.namespace("orc-0", durability::SCHEMA).unwrap());
    store.shutdown();

    let reader = Reader::open_dump([dir.path()], &[durability::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(
        result.entries.len() as u64 >= accepted,
        "принято {accepted}, прочитано {}",
        result.entries.len()
    );
}
