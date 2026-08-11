//! Журнал читают: запросы, окна времени, восстановление текста и состояний.
//!
//! Запуск: `cargo run -p dduroc --example 02_viewer_reads`
//!
//! Читатель один и тот же на приборе и в офлайн-вьюере: ему нужны каталог
//! хранилища и схемы приложения — без схемы записи остаются идентификаторами
//! с бинарными полями. Пример сначала пишет историю двух запусков (запуск =
//! открытие Store), затем разбирает её запросами.

use dduroc::prelude::*;
use dduroc::read::{EntryKind, KindFilter, Order, Query, Reader};
use dduroc::{BootCounter, Micros};

dduroc::schema! {
    name: radio,
    version: 1,
    languages: [en, ru],

    events {
        Started = 0x01 { level: Debug, en: "radio started", ru: "радио запущено" },
        PowerSet = 0x02 {
            level: Info,
            tags: [rf],
            en: "power set to {dbm} dBm",
            ru: "мощность {dbm} дБм",
            dbm: f32,
        },
        Overheat = 0x03 {
            level: Error,
            store: critical,
            tags: [thermal],
            en: "overheat: {t:.1} °C",
            ru: "перегрев: {t:.1} °C",
            t: f32,
        },
    }

    metrics {
        TempPa = 0x01 { type: f32, unit: "°C", tags: [thermal],
                        warn: ..=70.0, alarm: ..=85.0 },
        LinkState = 0x02 { states: [alarm Los = 0, warn Sync = 1, Lock = 2] },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("dduroc-examples").join("02");
    let _ = std::fs::remove_dir_all(&root);
    let cfg = StoreConfig::new(&root).with_budget_per_class(16 << 20);

    // -----------------------------------------------------------------------
    // История. Запуск 1: связь поднялась — единственное изменение состояния
    // за всю историю (пригодится ниже, в подтяжке состояний).
    // -----------------------------------------------------------------------
    let boot_first;
    {
        let store = Store::open(cfg.clone())?;
        boot_first = store.boot_counter();
        let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
        ns.log(radio::events::Started {});
        ns.log(radio::events::PowerSet { dbm: 27.5 });
        ns.series(radio::metrics::TempPa)?.sample(36.6);
        ns.series(radio::metrics::LinkState)?
            .sample(radio::metrics::LinkState::Lock);
        ns.sync()?;
        store.shutdown();
    }

    // Запуск 2: пришла синхронизация времени, случился перегрев.
    let boot_second;
    let sync_at;
    {
        let store = Store::open(cfg)?;
        boot_second = store.boot_counter();
        let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
        ns.log(radio::events::Started {});

        // Якорь ретроактивен на всю загрузку железа: настенное время получат
        // и записи ПЕРВОГО запуска — процесс перезапускался, прибор нет.
        sync_at = Utc::now();
        store.record_sync(sync_at, SyncSource::Ntp)?;

        ns.log(radio::events::PowerSet { dbm: 30.0 });
        let temp = ns.series(radio::metrics::TempPa)?;
        temp.sample(52.0);
        temp.sample(88.5);
        ns.log(radio::events::Overheat { t: 88.5 });
        ns.log_text(Level::Warn, "app", "мощность сброшена до безопасной", None);
        ns.log(radio::events::PowerSet { dbm: 5.0 });
        ns.sync()?;
        store.shutdown();
    }

    // -----------------------------------------------------------------------
    // Чтение. Reader не изменяет хранилище — вьюеру можно дать чужой дамп
    // (для дампа с другого прибора есть `allow_foreign_segments`).
    // -----------------------------------------------------------------------
    let reader = Reader::open(&root, &[radio::SCHEMA])?;

    println!("— что есть в хранилище —");
    let listing = reader.namespaces()?;
    for ns in &listing.namespaces {
        println!(
            "  {}: схема {} v{}, каналы {:?}, занято {} Б",
            ns.name, ns.schema_name, ns.protocol_version, ns.channels, ns.total_bytes
        );
    }
    // Нечитаемый неймспейс попал бы в `listing.damaged`, а не выпал молча.
    assert!(listing.is_complete());

    // -----------------------------------------------------------------------
    // «Последние события» — запрос по умолчанию: от нового к старому.
    // Текст записи на диске не лежал — он собран из шаблона схемы на
    // выбранном языке. У каждой записи есть относительное время (запуск +
    // микросекунды), настенное — только благодаря якорю.
    // -----------------------------------------------------------------------
    println!("\n— последние 5 записей журнала (ru) —");
    let last = reader.query(&Query::new().kinds(KindFilter::LOGS).limit(5))?;
    for e in &last.entries {
        println!("  {}", line(&reader, e));
    }
    println!(
        "  (ответ обрезан по limit: {}, повреждений: {})",
        last.truncated,
        last.damaged.len()
    );

    // -----------------------------------------------------------------------
    // Фильтры считаются по схеме ДО сканирования: уровень и тэги — свойства
    // типов, диск для такого отбора читается только там, где эти типы есть.
    // -----------------------------------------------------------------------
    println!("\n— только Warn и выше —");
    for e in &reader
        .query(
            &Query::new()
                .kinds(KindFilter::LOGS)
                .min_level(Level::Warn)
                .order(Order::Oldest),
        )?
        .entries
    {
        println!("  {}", line(&reader, e));
    }

    println!("\n— всё с тэгом thermal —");
    // Тэг несут сообщения и отсчёты (у метрик — свои тэги); свободный текст
    // и спаны не несут и пройти такой фильтр не могут — они исключаются, а
    // не просачиваются.
    for e in &reader
        .query(&Query::new().any_tag("thermal").order(Order::Oldest))?
        .entries
    {
        println!("  {}", line(&reader, e));
    }

    // -----------------------------------------------------------------------
    // Телеметрия. Имя метрики, единица, подпись состояния и важность
    // восстановлены по схеме — на диске лежали идентификатор и значение.
    // -----------------------------------------------------------------------
    println!("\n— телеметрия, от старого к новому —");
    for e in &reader
        .query(
            &Query::new()
                .kinds(KindFilter::TELEMETRY)
                .order(Order::Oldest),
        )?
        .entries
    {
        if let EntryKind::Sample {
            metric_name,
            unit,
            state_name,
            severity,
            value,
            ..
        } = &e.kind
        {
            println!(
                "  {} {} = {:?}{} [{:?}]{}",
                e.at,
                metric_name.unwrap_or("?"),
                value,
                unit.filter(|u| !u.is_empty())
                    .map(|u| format!(" {u}"))
                    .unwrap_or_default(),
                severity.unwrap_or_default(),
                state_name
                    .map(|s| format!(" состояние: {s}"))
                    .unwrap_or_default(),
            );
        }
    }

    // -----------------------------------------------------------------------
    // Окна времени. Граница — либо BootTime (есть всегда), либо UTC
    // (сравнима с записями только через якорь).
    // -----------------------------------------------------------------------
    println!("\n— журнал первого запуска (boot {boot_first}) —");
    for e in &reader
        .query(
            &Query::new()
                .boot(boot_first)
                .kinds(KindFilter::LOGS)
                .order(Order::Oldest),
        )?
        .entries
    {
        println!("  {}", line(&reader, e));
    }

    println!("\n— окно по настенным часам: минута вокруг синхронизации —");
    let minute = reader.query(
        &Query::new()
            .range(
                sync_at - chrono::TimeDelta::seconds(30),
                sync_at + chrono::TimeDelta::seconds(30),
            )
            .order(Order::Oldest),
    )?;
    println!(
        "  записей: {} (в окне оба запуска: якорь одной загрузки общий), \
         выпавших запусков: {}",
        minute.entries.len(),
        minute.unanchored.len()
    );

    // -----------------------------------------------------------------------
    // Подтяжка состояний. Состояния пишутся ПО ИЗМЕНЕНИЮ; окно «второй
    // запуск» не содержит ни одного отсчёта LinkState — но полоса состояний
    // на графике не должна быть пустой: связь всё это время была в Lock.
    // `with_state_seed` кладёт последний отсчёт каждого ряда-состояний до
    // окна в `seeds` — отдельно, не нарушая «всё в entries лежит в окне».
    // -----------------------------------------------------------------------
    println!("\n— второй запуск + состояния на левый край окна —");
    let seeded = reader.query(
        &Query::new()
            .since(BootTime::new(BootCounter(boot_second), Micros(0)))
            .kinds(KindFilter::TELEMETRY)
            .with_state_seed(),
    )?;
    println!("  отсчётов в окне: {}", seeded.entries.len());
    for seed in &seeded.seeds {
        if let EntryKind::Sample {
            metric_name,
            state_name,
            ..
        } = &seed.kind
        {
            println!(
                "  затравка: {} = {} (отсчёт из прошлого, {})",
                metric_name.unwrap_or("?"),
                state_name.unwrap_or("?"),
                seed.at
            );
        }
    }

    // -----------------------------------------------------------------------
    // Поток вместо собранного ответа. `query` собирает всё в память;
    // на большом хранилище единственный способ читать много — `stream`:
    // слияние каналов по времени, записи выдаются по одной, обход можно
    // бросить в любой момент.
    // -----------------------------------------------------------------------
    println!("\n— поток: первые 3 записи, дальше не читаем —");
    let mut stream = reader.stream(&Query::new().kinds(KindFilter::LOGS).order(Order::Oldest))?;
    for e in stream.by_ref().take(3) {
        println!("  {}", line(&reader, &e));
    }
    println!("  выдано: {}", stream.yielded());

    // -----------------------------------------------------------------------
    // Честность ответа. Повреждённый фрагмент попадает в `damaged` с именем
    // и причиной — ответ не притворяется полным. Другой сорт неполноты —
    // `unanchored`: окно задано настенным временем, а у загрузки нет якоря,
    // и сравнить её записи с часами нечем. Хранилище без единой
    // синхронизации отвечает на настенное окно пустотой — но называет
    // запуски, которые выпали, вместо «прибор ничего не писал».
    // -----------------------------------------------------------------------
    let lone = std::env::temp_dir()
        .join("dduroc-examples")
        .join("02-no-sync");
    let _ = std::fs::remove_dir_all(&lone);
    {
        let store = Store::open(StoreConfig::new(&lone).with_budget_per_class(16 << 20))?;
        let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
        ns.log(radio::events::Started {});
        ns.sync()?;
        store.shutdown(); // record_sync не звали: якоря нет
    }
    let no_clock = Reader::open(&lone, &[radio::SCHEMA])?;
    let asked =
        no_clock.query(&Query::new().range(sync_at - chrono::TimeDelta::hours(1), sync_at))?;
    println!(
        "\n— настенное окно без якоря: записей {}, выпали запуски {:?} —",
        asked.entries.len(),
        asked.unanchored
    );

    Ok(())
}

/// Строка журнала: время, уровень, текст на русском.
fn line(reader: &Reader, e: &dduroc::read::Entry) -> String {
    let clock = e
        .utc
        .map(|t| t.format("%H:%M:%S%.3f UTC").to_string())
        .unwrap_or_else(|| "--:--:--".to_owned());
    let what = match &e.kind {
        EntryKind::Sample {
            metric_name, value, ..
        } => format!("{} = {value:?}", metric_name.unwrap_or("?")),
        _ => reader
            .render(e, "ru")
            .unwrap_or_else(|| format!("{:?}", e.kind)),
    };
    format!(
        "{} | {clock} | {:9} | {what}",
        e.at,
        e.level().map(|l| l.as_str()).unwrap_or(""),
    )
}
