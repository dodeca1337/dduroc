//! Подписка на поток записей.
//!
//! До неё живому читателю оставался опрос по таймеру, и выбирать приходилось
//! между частым (обращения к носителю впустую, а на приборе это ещё и износ
//! флеша) и редким (график отстаёт на период опроса). Подписка снимает выбор:
//! читатель спит, пока писать нечего, и просыпается на первом же блоке.
//!
//! Проверяется здесь не «данные доходят» — это умел и запрос, — а то, что
//! подписка не теряет их на стыках: недописанный хвост, смена сегмента,
//! неймспейс, поднятый позже неё.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass, StoreConfig, StoreExt};
use dduroc_read::{Follow, Order, Query, Reader, Tail};
use std::time::{Duration, Instant};

dduroc::schema! {
    name: probe,
    version: 1,
    languages: [ru],

    events {
        Tick = 0x01 { level: Info, ru: "тик {n}", n: u32 },
        Fault = 0x02 { level: Error, store: critical, ru: "отказ {code}", code: u8 },
    }
}

dduroc::schema! {
    name: latecomer,
    version: 1,
    languages: [ru],

    events {
        Hello = 0x01 { level: Info, ru: "поднялся" },
    }
}

/// Сколько ждать записи, прежде чем считать подписку сломанной.
///
/// Тест не имеет права висеть: `Tail::Idle` — законный ответ, и цикл по нему
/// без общего срока крутился бы вечно на любой поломке.
const PATIENCE: Duration = Duration::from_secs(20);

/// Забрать из подписки ровно `want` записей.
///
/// Паникует по общему сроку: подписка, замолчавшая навсегда, обязана валить
/// тест, а не подвешивать прогон.
fn take(tail: &mut Follow<'_>, want: usize) -> Vec<dduroc_read::Entry> {
    let deadline = Instant::now() + PATIENCE;
    let mut got = Vec::with_capacity(want);
    while got.len() < want {
        assert!(
            Instant::now() < deadline,
            "подписка отдала {} из {want} записей и замолчала",
            got.len()
        );
        match tail.next(Duration::from_millis(50)) {
            Tail::Entry(e) => got.push(*e),
            Tail::Idle => {}
            Tail::Ended => panic!("хранилище остановилось посреди теста"),
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
        .expect("подписка на живое хранилище");

    // Первая порция — до того, как кто-то спросил.
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
        vec!["тик 0", "тик 1", "тик 2"]
    );

    // Вторая — той же подписке, без единого пересоздания читателя.
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
        vec!["тик 3", "тик 4", "тик 5"]
    );
    assert!(tail.take_damage().is_empty());
}

#[test]
fn a_namespace_raised_later_joins_the_subscription() {
    // Вьюер стартовал раньше сервиса — обычный порядок на приборе. Подписка на
    // группу, которая показывает только тех, кто успел подняться до неё,
    // выглядела бы как «сервис не взлетел».
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

    // Второй сервис поднимается уже под работающей подпиской.
    let late = store.namespace("orc-b", latecomer::SCHEMA).unwrap();
    late.log(latecomer::events::Hello);
    late.sync().unwrap();

    let got = take(&mut tail, 1);
    assert_eq!(&*got[0].namespace, "orc-b", "подхвачен поздний неймспейс");
    assert_eq!(reader.render(&got[0], "ru").as_deref(), Some("поднялся"));

    // И следующий — сразу за ним. Обход корней ограничен по частоте (он
    // перечисляет всё хранилище), поэтому этот заведомо попадает в отложенные:
    // долг обязан быть отдан, а не потерян. Иначе сервис, поднявшийся
    // вплотную за другим, не появился бы никогда.
    let later = store.namespace("orc-c", latecomer::SCHEMA).unwrap();
    later.log(latecomer::events::Hello);
    later.sync().unwrap();
    let got = take(&mut tail, 1);
    assert_eq!(&*got[0].namespace, "orc-c", "отложенный обход состоялся");
}

#[test]
fn rotation_under_a_subscription_loses_nothing_and_looks_like_nothing() {
    // Сегмент под подпиской сменяется молча: она читает файл, который writer
    // в этот момент запечатывает, и тут же обязана перейти на следующий.
    // Бюджет заведомо больше написанного — вытеснять нечего, поэтому дойти
    // обязано ВСЁ: недосчитаться здесь значит потерять на стыке сегментов.
    const COUNT: u32 = 3000;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(64 << 20)
            // Мелкие сегменты и блоки, сжатие выключено: ротация должна
            // случиться много раз, а со сжатием эти три тысячи одинаковых
            // записей улеглись бы в один сегмент.
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
        .map(|t| t.expect("payload разбирается").n)
        .collect();
    assert_eq!(numbers, (0..COUNT).collect::<Vec<_>>(), "порядок и полнота");
    assert!(
        tail.take_damage().is_empty(),
        "ротация под чтением — не порча: {:?}",
        tail.take_damage()
    );

    // Тест обязан доказать, что проверял именно стык сегментов: уместившись в
    // один файл, он проверил бы совсем другое и всё равно был бы зелёным.
    let segments = std::fs::read_dir(dir.path().join("dev-0").join("default"))
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .is_ok_and(|e| e.file_name().to_string_lossy().ends_with(".seg"))
        })
        .count();
    assert!(segments > 2, "ротации не было: сегментов {segments}");
}

#[test]
fn a_lasting_damage_is_named_once_not_at_every_rotation() {
    // Подписка обходит корни заново на каждую смену устройства хранилища, а
    // нечитаемый каталог никуда не девается. Без памяти о сказанном она
    // объявляла бы одно и то же повреждение при каждой ротации — и список,
    // который отдаётся разницей, перестал бы быть разницей.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    // Каталог, не являющийся каналом ни одного класса: причина постоянная.
    std::fs::create_dir(dir.path().join("dev-0").join("attic")).unwrap();

    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();
    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    take(&mut tail, 1);

    let first = tail.take_damage();
    assert_eq!(first.len(), 1, "чужой каталог назван: {first:?}");

    // Ещё сколько угодно оборотов — о том же молчим.
    for n in 1..4 {
        ns.log(probe::events::Tick { n });
        ns.sync().unwrap();
        take(&mut tail, 1);
    }
    assert!(
        tail.take_damage().is_empty(),
        "о том же повреждении сказано второй раз"
    );
}

#[test]
fn a_stopped_store_ends_the_subscription_instead_of_leaving_it_waiting() {
    // Остановка хранилища для подписки — не тишина, а конец: без явного
    // ответа она ждала бы новых записей от того, кому их уже некому писать, и
    // выглядело бы это ровно как молчащий прибор.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();

    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    assert_eq!(take(&mut tail, 1).len(), 1);

    // Записанное перед самой остановкой обязано дойти до подписки: конец
    // потока — это конец ДАННЫХ, а не обрыв на последней порции.
    ns.log(probe::events::Tick { n: 1 });
    store.shutdown();

    let deadline = Instant::now() + PATIENCE;
    let mut after = Vec::new();
    let ended = loop {
        assert!(Instant::now() < deadline, "подписка не заметила остановки");
        match tail.next(Duration::from_millis(50)) {
            Tail::Entry(e) => after.push(*e),
            Tail::Idle => {}
            Tail::Ended => break true,
        }
    };
    assert!(ended);
    assert_eq!(after.len(), 1, "последняя запись дошла до конца потока");
    assert!(
        tail.take_damage().is_empty(),
        "остановка — не порча: {:?}",
        tail.take_damage()
    );
    assert!(
        matches!(tail.next(Duration::from_millis(10)), Tail::Ended),
        "конец необратим"
    );
}

#[test]
fn a_newborn_segment_is_waited_for_not_walked_past() {
    // Сегмент, застигнутый в момент рождения (файл создан, заголовок ещё не
    // дописан), разовый запрос пропускает: его покажет следующий запрос. У
    // подписки следующего запроса нет — пройдя мимо, она потеряла бы весь
    // сегмент. Проверяется тем же способом, что и всё остальное: ротация под
    // непрерывной записью, где такие моменты и случаются.
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

    // Писатель не останавливается: подписка читает вместе с ним, а не после.
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
        .map(|t| t.expect("payload разбирается").n)
        .collect();
    assert_eq!(
        numbers,
        (0..COUNT).collect::<Vec<_>>(),
        "ни одного пропуска"
    );
    assert!(tail.take_damage().is_empty());
}

#[test]
fn a_run_without_an_anchor_is_named_even_if_it_appears_after_the_start() {
    // Прибор без RTC и без синхронизации: у запуска нет якоря, и настенное
    // окно приложить к нему нечем. Такой запуск выпадает из выборки — и
    // обязан быть назван, иначе молчание подписки неотличимо от «в эти часы
    // прибор ничего не писал». Сегментов при открытии подписки ещё нет, так
    // что узнать о запуске она может только обойдя каталоги позже.
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
    assert!(tail.unanchored().is_empty(), "писать ещё не начинали");

    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();

    // Записей не будет: запуск в настенное окно не укладывается. Но и тишины
    // быть не должно — подписку опрашивают, пока она не назовёт причину.
    let deadline = Instant::now() + PATIENCE;
    while tail.unanchored().is_empty() {
        assert!(
            Instant::now() < deadline,
            "запуск без якоря так и не назван — молчание неотличимо от пустоты"
        );
        assert!(
            !matches!(tail.next(Duration::from_millis(50)), Tail::Entry(_)),
            "запись без якоря не имеет права попасть в настенное окно"
        );
    }
    assert_eq!(tail.unanchored(), vec![boot], "назван именно этот запуск");
}

#[test]
fn asking_to_wait_forever_answers_instead_of_panicking() {
    // `Duration::MAX` в сроке ожидания — это паника на сложении с часами.
    // Паника вместо ожидания была бы худшим прочтением такой просьбы.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path())).unwrap();
    let ns = store.namespace("dev-0", probe::SCHEMA).unwrap();
    let reader = store.reader();
    let mut tail = reader.follow(&Query::new().order(Order::Oldest)).unwrap();

    ns.log(probe::events::Tick { n: 0 });
    ns.sync().unwrap();
    // Записанное уже на носителе — ждать не придётся, а вот сложить срок
    // потребуется.
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

    // Обратный порядок: «последние сто» у потока, у которого нет последней
    // записи, ничего не значат.
    let e = reader
        .follow(&Query::new().order(Order::Newest))
        .unwrap_err();
    assert!(matches!(e, dduroc_read::ReadError::NotFollowable(_)), "{e}");

    // Верхняя граница окна: подписка читает то, чего ещё нет.
    let e = reader
        .follow(&Query::new().order(Order::Oldest).until(ns.now()))
        .unwrap_err();
    assert!(matches!(e, dduroc_read::ReadError::NotFollowable(_)), "{e}");

    // Дамп никто не дописывает.
    let dump = Reader::open_dump([dir.path()], &[probe::SCHEMA]).unwrap();
    let e = dump.follow(&Query::new().order(Order::Oldest)).unwrap_err();
    assert!(matches!(e, dduroc_read::ReadError::NotFollowable(_)), "{e}");
}
