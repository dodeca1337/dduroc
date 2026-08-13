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
    let reader = store.reader().unwrap();
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
    assert_eq!(lines(&store.reader().unwrap()), ["пинг 7"]);
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
        lines(&store.reader().unwrap()).is_empty(),
        "без схемы рендерить нечем — и выдумывать текст читатель не станет"
    );
    assert_eq!(
        lines(&store.reader().unwrap().with_schema(split::SCHEMA)),
        ["пинг 2"]
    );
    store.shutdown();
}
