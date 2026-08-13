//! Сквозные проверки под нагрузкой: общий бюджет класса, личные квоты,
//! обратное давление критической очереди, многопоточная запись и чтение
//! живого хранилища.
//!
//! Всё это до сих пор держалось на юнит-тестах и ревью. Ротация проверялась
//! на инвентаре из файлов, набитых нулями, — ни один тест не заставлял writer
//! действительно выйти за бюджет; обратное давление не исполнялось ни разу;
//! потоков в тестах не было вовсе, хотя писать из многих потоков — основной
//! режим работы библиотеки.

use dduroc::prelude::*;
use dduroc::{
    ChannelConfig, ChannelOverride, GroupPolicy, NsQuota, QueueSizes, StorageClass, StoreConfig,
};
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
        Seq = 0x01 { type: u64 },
        Bulk = 0x02 { type: blob },
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
    let reader = Reader::open_dump([root], &[pressure::SCHEMA]).unwrap();
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
fn rotation_drops_the_oldest_and_keeps_the_class_inside_its_budget() {
    // Бюджет объявлен на класс; здесь класс представлен одним неймспейсом —
    // и именно тут сходятся преаллокация, учёт размера после запечатывания
    // и защита активного сегмента.
    const BUDGET: u64 = 32 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(BUDGET)).unwrap();
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
fn the_class_budget_is_shared_across_namespaces() {
    // Бюджет — свойство класса, а не неймспейса: «все логи — столько-то».
    // Два неймспейса пишут в один класс и вместе обязаны уложиться в его
    // бюджет; вытесняется старейшее по классу, в чьём бы каталоге оно ни
    // лежало.
    const CEILING: u64 = 12 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).channel(
        StorageClass::Default,
        ChannelConfig {
            // Сегмент поменьше: двум активным сегментам положено влезать
            // в бюджет с запасом на историю.
            segment_bytes: 4 << 20,
            ..ChannelConfig::new(CEILING)
        },
    ))
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
        "класс занимает {occupied} при бюджете {CEILING}"
    );
    assert_eq!(
        store.stats().budget_overruns,
        0,
        "бюджет класса выполним: активных сегментов всего два"
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
fn a_group_hands_its_namespaces_their_own_segments_and_quota() {
    // Настройки каналов задаются на всё хранилище, и это верно ровно до тех
    // пор, пока неймспейсы однородны. Группа — способ сказать «у
    // оркестраторов телеметрия своя» один раз, а не при каждом открытии
    // неймспейса. Проверяется, что сказанное доезжает до носителя: и предел
    // занятости, и размер сегмента.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(64 << 20)
            .group(
                "orc-",
                GroupPolicy::new()
                    .channel(
                        StorageClass::Default,
                        ChannelOverride::new()
                            .segment_bytes(64 << 10)
                            .block_max_bytes(4 << 10),
                    )
                    .limit_bytes(StorageClass::Default, 256 << 10),
            ),
    )
    .unwrap();

    let grouped = store.namespace("orc-radio-0", pressure::SCHEMA).unwrap();
    let outsider = store.namespace("diag-0", pressure::SCHEMA).unwrap();
    for ns in [&grouped, &outsider] {
        let bulk = ns.series(pressure::metrics::Bulk).unwrap();
        for _ in 0..40 {
            bulk.sample(noise(16 << 10));
        }
        ns.sync().unwrap();
    }

    let channel = |name: &str| dir.path().join(name).join("default");
    let occupied = bytes_under(&channel("orc-radio-0"));
    let outside = bytes_under(&channel("diag-0"));

    // Квота группы держит своих: предел плюс активный сегмент, который
    // вытеснить нельзя, — его преаллокация и есть гарантия ENOSPC.
    assert!(
        occupied <= (256 << 10) + (64 << 10),
        "квота группы не удержала: {occupied} Б"
    );
    // Чужой квоты не унаследовал и черпает из общего бюджета класса.
    assert!(
        outside > occupied,
        "неймспейс вне группы не должен подчиняться её квоте: {outside} Б против {occupied} Б"
    );

    // Размер сегмента тоже групповой: у чужого он общий, восьмимегабайтный,
    // и весь его объём улёгся в один файл.
    let files = |name: &str| {
        std::fs::read_dir(channel(name))
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .is_ok_and(|e| e.path().extension().is_some_and(|x| x == "seg"))
            })
            .count()
    };
    assert!(files("orc-radio-0") > 1, "мелкие сегменты группы");
    assert_eq!(files("diag-0"), 1, "общий сегмент вмещает всё разом");
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
            .with_budget_per_class(16 << 20)
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
        ns.try_log(pressure::events::Alarm)
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

    let reader = Reader::open_dump([dir.path()], &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);
    let alarms = result
        .entries
        .iter()
        .filter(|e| matches!(&e.kind, EntryKind::Message { event, .. } if event.0 == 2))
        .count();
    assert_eq!(alarms as u64, N, "ждали места — значит все дошли");
    assert!(
        result
            .entries
            .iter()
            .all(|e| e.channel == StorageClass::Critical),
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
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 << 20)).unwrap();
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
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 << 20)).unwrap();
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
            let reader = Reader::open_dump([dir.path()], &[pressure::SCHEMA]).unwrap();
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
fn a_class_budget_below_two_segments_is_refused_at_open() {
    // Бюджет класса ниже пары сегментов невыполним по построению: активный
    // сегмент вытеснить нельзя. Узнать об этом при открытии — единственный
    // момент, когда это ещё поправимо.
    let dir = tempfile::tempdir().unwrap();
    let segment = ChannelConfig::new(0).segment_bytes;
    let err = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(segment))
        .expect_err("невыполнимый бюджет обязан быть отвергнут");
    assert!(matches!(err, dduroc::Error::BadChannel { .. }), "{err}");

    Store::open(StoreConfig::new(dir.path()).with_budget_per_class(segment * 2))
        .expect("двух сегментов уже достаточно");
}

#[test]
fn a_namespace_quota_rotates_inside_the_shared_class_budget() {
    // Личная квота — необязательный предел ВНУТРИ общего бюджета класса:
    // прожорливый сервис ротируется в её рамках, не дожидаясь, пока класс
    // упрётся в свой бюджет, — и не выедает соседей.
    const QUOTA: u64 = 16 << 20;
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(256 << 20)).unwrap();

    let hog = store
        .namespace_with_quota(
            "orc-hog",
            pressure::SCHEMA,
            NsQuota::new().limit_bytes(StorageClass::Default, QUOTA),
        )
        .unwrap();
    let bulk = hog.series(pressure::metrics::Bulk).unwrap();
    let chunk = noise(64 << 10);
    let rounds = (QUOTA / chunk.len() as u64) * 3;
    for i in 0..rounds {
        bulk.sample(&chunk[..]);
        if i % 32 == 0 {
            hog.sync().unwrap();
        }
    }
    hog.sync().unwrap();
    store.shutdown();
    let stats = store.stats();
    // Блокировка корня снимается вместе с последней ручкой: ряд и неймспейс
    // держат хранилище живым.
    drop(bulk);
    drop(hog);
    drop(store);

    let occupied = bytes_under(&dir.path().join("orc-hog"));
    assert!(
        occupied > 0 && occupied <= QUOTA,
        "неймспейс занимает {occupied} при квоте {QUOTA}"
    );
    assert!(
        stats.segments_rotated > 0,
        "квота обязана была сработать: {stats:?}"
    );

    // Квота меньше двух сегментов бессмысленна и отвергается при открытии.
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(256 << 20)).unwrap();
    let err = store
        .namespace_with_quota(
            "orc-tiny",
            pressure::SCHEMA,
            NsQuota::new().limit_bytes(StorageClass::Default, 1 << 20),
        )
        .expect_err("квота в один сегмент бессмысленна");
    assert!(matches!(err, dduroc::Error::BadChannel { .. }), "{err}");
    store.shutdown();
}

#[test]
fn a_class_can_live_on_its_own_root() {
    // Критические данные должны уметь жить на своём носителе (защищённый
    // раздел вроде jffs2): классу задаётся собственный корень, раскладка
    // внутри него та же — `<корень>/<неймспейс>/<класс>/`.
    let main = tempfile::tempdir().unwrap();
    let vault = tempfile::tempdir().unwrap();
    {
        let store = Store::open(
            StoreConfig::new(main.path())
                .with_budget_per_class(16 << 20)
                .channel(
                    StorageClass::Critical,
                    ChannelConfig {
                        custom_root: Some(vault.path().to_path_buf()),
                        ..ChannelConfig::critical(16 << 20)
                    },
                ),
        )
        .unwrap();
        let ns = store.namespace("orc-0", pressure::SCHEMA).unwrap();
        ns.log(pressure::events::Mark);
        ns.log(pressure::events::Alarm);
        ns.sync().unwrap();
        assert!(store.stats().is_clean(), "{:?}", store.stats());
        store.shutdown();
    }

    // Сегменты легли по своим носителям.
    assert!(
        bytes_under(&vault.path().join("orc-0").join("critical")) > 0,
        "критический канал живёт на своём разделе"
    );
    assert!(
        !main.path().join("orc-0").join("critical").exists(),
        "в основном корне критического каталога нет"
    );
    assert!(
        bytes_under(&main.path().join("orc-0").join("default")) > 0,
        "обычный канал остался в основном корне"
    );

    // Читатель собирает оба дерева: дамп открывается всеми корнями разом.
    let reader = Reader::open_dump([main.path(), vault.path()], &[pressure::SCHEMA]).unwrap();
    let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
    assert!(result.is_complete(), "{:?}", result.damaged);
    let kinds: Vec<u16> = result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Message { event, .. } => Some(event.0),
            _ => None,
        })
        .collect();
    assert_eq!(kinds, [1, 2], "видны и обычное, и критическое");
    let listing = reader.namespaces().unwrap();
    assert_eq!(
        listing.namespaces[0].channels,
        [StorageClass::Default, StorageClass::Critical],
        "перечисление сливает каналы обоих корней"
    );

    // Дамп без второго дерева — не «короткий ответ», а отказ: полнота
    // проверяется по схеме при открытии, и потерять критику молча нельзя.
    let e = Reader::open_dump([main.path()], &[pressure::SCHEMA]).unwrap_err();
    assert!(
        matches!(
            &e,
            dduroc_read::ReadError::IncompleteDump { namespace, class }
                if namespace == "orc-0" && *class == StorageClass::Critical
        ),
        "{e}"
    );
}

/// Сколько дескрипторов процесса указывает внутрь дерева.
fn open_fds_under(dir: &Path) -> usize {
    std::fs::read_dir("/proc/self/fd")
        .expect("procfs смонтирована")
        .filter_map(|e| e.ok())
        .filter_map(|e| std::fs::read_link(e.path()).ok())
        .filter(|target| target.starts_with(dir))
        .count()
}

#[test]
fn a_query_does_not_hold_a_descriptor_per_channel() {
    // Слияние по времени требует головы от КАЖДОГО курсора, а курсор заводится
    // на каждую пару (неймспейс, канал). Пока курсор держал открытый сегмент,
    // один запрос стоил дескриптора на канал: на заявленных двадцати четырёх
    // тысячах неймспейсов это десятки тысяч открытых файлов сверх тех, что
    // держит writer, — то есть отказ по `ulimit` на ровном месте.
    //
    // Ста неймспейсов хватает, чтобы отличить «на канал» от «на чтение»:
    // разница между сотней и единицами не тонет ни в каком шуме.
    const NAMESPACES: usize = 100;

    let dir = tempfile::tempdir().unwrap();
    let store =
        Store::open(StoreConfig::new(dir.path()).with_budget_per_class(64 * 1024 * 1024)).unwrap();

    let mut handles = Vec::with_capacity(NAMESPACES);
    for i in 0..NAMESPACES {
        let ns = store
            .namespace(&format!("svc-{i:04}"), pressure::SCHEMA)
            .unwrap();
        ns.log(pressure::events::Mark);
        handles.push(ns);
    }
    store.sync().unwrap();

    let before = open_fds_under(dir.path());
    let reader = store.reader();
    let mut stream = reader.stream(&Query::new().order(Order::Oldest)).unwrap();
    // Первая запись означает, что курсоры заряжены: голову спросили у всех.
    let first = stream.next().expect("записи есть");
    let held = open_fds_under(dir.path()).saturating_sub(before);
    assert_eq!(first.namespace.as_ref(), "svc-0000");
    assert!(
        held <= 2,
        "поток держит {held} дескрипторов на {NAMESPACES} неймспейсов — \
         значит, по одному на канал"
    );

    // И читает при этом всё: экономия дескрипторов не имеет права стоить
    // записей.
    let seen = 1 + stream.by_ref().count();
    assert_eq!(seen, NAMESPACES, "прочитаны все неймспейсы");
    assert!(stream.damaged().is_empty(), "{:?}", stream.damaged());

    drop(stream);
    drop(handles);
    store.shutdown();
}
