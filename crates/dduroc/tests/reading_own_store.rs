//! Читатель, построенный по своему же хранилищу.
//!
//! До этого связь между `Store` и `Reader` держалась на памяти вызывающего, и
//! у неё был молчаливый исход: класс, вынесенный на свой носитель, — это
//! второе дерево, и читатель, которому назвали только основной корень,
//! показывал историю без него. Без ошибки, без отметки о повреждении, без
//! единого признака пропажи — просто короче.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass, StoreConfig, StoreExt};
use dduroc_read::{KindFilter, Order, Query, Reader};

dduroc::schema! {
    name: split,
    version: 1,
    languages: [ru],

    events {
        Ping = 0x01 { level: Info, ru: "пинг {seq}", seq: u32 },
        Fault = 0x02 { level: Error, store: critical, ru: "отказ {code}", code: u8 },
    }
}

/// Тексты записей журнала в порядке от старых к новым.
fn lines(reader: &Reader) -> Vec<String> {
    let q = Query::new().kinds(KindFilter::LOGS).order(Order::Oldest);
    let result = reader.query(&q).expect("запрос");
    assert!(result.damaged.is_empty(), "{:?}", result.damaged);
    result
        .entries
        .iter()
        .filter_map(|e| reader.render(e, "ru"))
        .collect()
}

#[test]
fn a_reader_of_the_store_sees_the_class_that_lives_on_another_medium() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let vault = dir.path().join("vault");

    let store = Store::open(
        StoreConfig::new(&root)
            .with_budget_per_class(16 << 20)
            .channel(
                StorageClass::Critical,
                ChannelConfig {
                    custom_root: Some(vault.clone()),
                    ..ChannelConfig::critical(16 << 20)
                },
            ),
    )
    .unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.log(split::events::Fault { code: 3 });
    ns.sync().unwrap();

    // Хранилище знает оба своих корня и схему поднятого неймспейса — называть
    // их по второму разу неоткуда и незачем.
    assert_eq!(store.roots().len(), 2, "{:?}", store.roots());
    let reader = store.reader();
    assert_eq!(lines(&reader), ["пинг 1", "отказ 3"]);

    // А вот и то, ради чего мост нужен: читателю, которому назвали только
    // основной корень, критики не видно, и молчит он об этом совершенно.
    let blind = Reader::open(&root, &[split::SCHEMA]).unwrap();
    assert_eq!(
        lines(&blind),
        ["пинг 1"],
        "проверка потеряла смысл: без второго корня критика не может быть видна"
    );

    store.shutdown();
}

#[test]
fn schemas_outlive_the_namespace_handle() {
    // Ручку неймспейса отпускают, как только сервис отработал; расшифровывать
    // его записи процесс от этого не разучивается. Иначе читатель, взятый у
    // хранилища позже, показывал бы голые идентификаторы.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    {
        let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
        ns.log(split::events::Ping { seq: 7 });
        ns.sync().unwrap();
    }

    assert_eq!(store.schemas().len(), 1);
    assert_eq!(lines(&store.reader()), ["пинг 7"]);
    store.shutdown();
}

#[test]
fn a_live_reader_stays_current_without_being_rebuilt() {
    // Живой читатель создаётся один раз при старте и живёт параллельно с
    // записью. Всё, что появляется в хранилище после его создания, он обязан
    // видеть без пересоздания: правда спрашивается у хранилища на каждый
    // запрос, а не замораживается в момент создания.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let reader = store.reader(); // до неймспейса, записей и якоря времени

    assert!(lines(&reader).is_empty(), "хранилище ещё пусто");

    // Неймспейс поднят ПОСЛЕ создания читателя: схема обязана найтись.
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.sync().unwrap();
    let first = reader.query(&Query::new().kinds(KindFilter::LOGS)).unwrap();
    assert_eq!(
        reader.render(&first.entries[0], "ru").as_deref(),
        Some("пинг 1"),
        "схема сервиса, стартовавшего после создания читателя, видна"
    );
    assert!(
        first.entries[0].utc.is_none(),
        "якоря ещё нет — настенного времени взять неоткуда"
    );

    // Синхронизация времени ПОСЛЕ создания читателя ретроактивна, и тот же
    // читатель обязан увидеть её следующим же запросом.
    store.record_sync(Utc::now(), SyncSource::Ntp).unwrap();
    let second = reader.query(&Query::new().kinds(KindFilter::LOGS)).unwrap();
    assert!(
        second.entries[0].utc.is_some(),
        "якорь ретроактивен: запись, сделанная до синхронизации, получила UTC"
    );
    store.shutdown();
}

#[test]
fn a_torn_active_tail_is_data_not_yet_for_live_and_damage_for_dump() {
    // Writer кладёт блок одним write, но читатель видит страницы без
    // гарантии целиком: у активного сегмента может найтись хвост, который
    // ещё не долетел. Для живого читателя это «данные ещё не готовы», для
    // дампа — честная порча: дамп никто не дописывает.
    use dduroc_engine::segment::SegmentReader;
    use std::os::unix::fs::FileExt;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.sync().unwrap();

    // Единственный сегмент канала — активный. Дописываем в него мусор там,
    // где writer продолжил бы писать: так выглядит блок, чей write ещё не
    // стал виден целиком.
    let ch_dir = dir.path().join("orc-probe-0").join("default");
    let seg_path = std::fs::read_dir(&ch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "seg"))
        .expect("активный сегмент");
    let seg = SegmentReader::open(&seg_path).unwrap();
    let (offsets, stopped) = seg.scan_block_offsets();
    assert!(stopped.is_none(), "до вмешательства сегмент цел");
    let mut buf = Vec::new();
    let end = seg
        .read_block_at(*offsets.last().unwrap(), &mut buf)
        .unwrap()
        .expect("последний блок цел");
    drop(seg);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&seg_path)
        .unwrap()
        .write_all_at(&[0xEE; 16], end)
        .unwrap();

    let live = store
        .reader()
        .query(&Query::new().kinds(KindFilter::LOGS))
        .unwrap();
    assert_eq!(live.entries.len(), 1, "целые блоки читаются");
    assert!(
        live.damaged.is_empty(),
        "недописанный хвост — не порча: {:?}",
        live.damaged
    );

    let dump = Reader::open(dir.path(), &[split::SCHEMA])
        .unwrap()
        .query(&Query::new().kinds(KindFilter::LOGS))
        .unwrap();
    assert_eq!(
        dump.damaged.len(),
        1,
        "у дампа тот же хвост — повреждение, молчать о нём нельзя"
    );
    store.shutdown();
}

#[test]
fn rotation_and_writes_under_a_live_reader_never_look_like_damage() {
    // Дымовая проверка настоящей параллельности: писатель молотит записи
    // сквозь тесную квоту (постоянная ротация), читатель непрерывно
    // спрашивает. Ни один ответ не имеет права объявить порчу: вытеснение и
    // дописывание — штатная жизнь хранилища, а не повреждения.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(16 << 20)
            .channel(StorageClass::Default, {
                let mut c = ChannelConfig::new(16 << 20);
                c.segment_bytes = 128 * 1024;
                c.block_max_bytes = 8 * 1024;
                c
            }),
    )
    .unwrap();
    let ns = store
        .namespace_with_quota(
            "orc-probe-0",
            split::SCHEMA,
            dduroc::NsQuota::new().limit_bytes(StorageClass::Default, 256 * 1024),
        )
        .unwrap();
    let reader = store.reader();

    let writer = std::thread::spawn(move || {
        for i in 0..4000u32 {
            ns.log(split::events::Ping { seq: i });
            if i % 256 == 0 {
                let _ = ns.sync();
            }
        }
        let _ = ns.sync();
    });

    // Не меньше трёх запросов в любом случае: на быстрой машине писатель
    // может успеть раньше, чем главный поток дойдёт до первого запроса.
    let mut total_queries = 0;
    while !writer.is_finished() || total_queries < 3 {
        let got = reader
            .query(&Query::new().kinds(KindFilter::LOGS).limit(64))
            .unwrap();
        assert!(
            got.damaged.is_empty(),
            "живое чтение объявило порчу: {:?}",
            got.damaged
        );
        total_queries += 1;
    }
    writer.join().unwrap();
    let last = reader.query(&Query::new().kinds(KindFilter::LOGS)).unwrap();
    assert!(last.damaged.is_empty(), "{:?}", last.damaged);
    assert!(
        !last.entries.is_empty() && total_queries > 0,
        "проверка обязана была застать и записи, и запросы"
    );
    store.shutdown();
}

#[test]
fn a_foreign_namespace_needs_its_schema_named() {
    // В хранилище лежит неймспейс чужого сервиса: его схемы у этого билда нет,
    // и записи остаются идентификаторами — пока схему не назовут.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 2 });
    ns.sync().unwrap();
    store.shutdown();
    drop(ns);
    drop(store);

    // Второй процесс о схеме не знает.
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    assert!(store.schemas().is_empty());
    assert!(
        lines(&store.reader()).is_empty(),
        "без схемы рендерить нечем — и выдумывать текст читатель не станет"
    );
    assert_eq!(
        lines(&store.reader().with_schema(split::SCHEMA)),
        ["пинг 2"]
    );
    store.shutdown();
}
