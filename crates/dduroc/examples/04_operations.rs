//! Эксплуатация: политика классов, общий бюджет класса, свой носитель у
//! критики, учёт потерь.
//!
//! Запуск: `cargo run -p dduroc --example 04_operations`
//!
//! Журнал на приборе живёт годами без присмотра, поэтому у движка нет
//! состояния «место кончилось, дальше никак»: история ротируется в рамках
//! бюджетов, потери учитываются и объявляются, нарушение контракта схемы
//! отличается от отставания диска. Пример показывает эти механизмы по одному.

use dduroc::prelude::*;
use dduroc::read::{EntryKind, KindFilter, Order, OwnedSampleValue, Query, Reader};
use dduroc::{
    ChannelConfig, ChannelOverride, Compression, EventId, GroupPolicy, QueueSizes, StorageClass,
};

dduroc::schema! {
    name: probe,
    version: 1,
    languages: [ru],

    events {
        Ping = 0x01 { level: Debug, ru: "пинг {seq}", seq: u32 },
        // Критическое — в свой канал: fdatasync сразу после записи (group
        // commit), без сжатия, и очередь, в которой при переполнении
        // вызывающий ЖДЁТ места, а не теряет запись.
        Fault = 0x02 { level: Error, store: critical, ru: "авария {code}", code: u8 },
    }

    metrics {
        // Бинарные слепки — самый тяжёлый поток, ему канал телеметрии.
        Chunk = 0x01 { type: blob, store: telemetry },
    }
}

/// Слепок с номером в первых байтах — по нему видно, что именно выжило.
fn chunk(index: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 4096];
    bytes[..8].copy_from_slice(&index.to_le_bytes());
    bytes
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::temp_dir().join("dduroc-examples").join("04");
    let _ = std::fs::remove_dir_all(&base);

    rotation(&base.join("rotation"))?;
    ceiling(&base.join("ceiling"))?;
    losses(&base.join("losses"))?;
    vault(&base.join("vault"), &base.join("vault-critical"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// 1. Ротация в бюджете класса. Бюджет — ответ на вопрос «сколько истории
// хранить у ВСЕЙ телеметрии»: класс пишет по кругу, старейший сегмент уходит
// целиком, свежие данные не отвергаются никогда.
// ---------------------------------------------------------------------------
fn rotation(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("— ротация в бюджете класса —");
    let store = Store::open(StoreConfig::new(root).channel(
        StorageClass::Telemetry,
        ChannelConfig {
            // Слепки уже плотные — сжатие только жгло бы CPU.
            compression: Compression::None,
            // Телеметрия терпит минуту незаписанного — реже fdatasync.
            sync_interval: std::time::Duration::from_secs(60),
            // 12 МиБ бюджета при сегменте 4 МиБ: живут три сегмента.
            segment_bytes: 4 << 20,
            ..ChannelConfig::new(12 << 20)
        },
    ))?;
    let ns = store.namespace("orc-probe-0", probe::SCHEMA)?;
    let chunks = ns.series(probe::metrics::Chunk)?;

    // 16 МиБ слепков в 12 МиБ бюджета: голова истории обязана уйти.
    // Периодический sync даёт диску догнать — телеметрия реального прибора
    // приходит с частотой датчика, а не циклом while.
    for i in 0..4096u64 {
        chunks.sample(chunk(i));
        if i % 512 == 511 {
            ns.sync()?;
        }
    }
    ns.sync()?;

    let stats = store.stats();
    println!(
        "  сегментов создано {}, запечатано {}, ротировано {}; потеряно записей {}",
        stats.segments_created, stats.segments_sealed, stats.segments_rotated, stats.dropped
    );
    assert!(stats.segments_rotated > 0, "{stats:?}");
    store.shutdown();

    // Что выжило: самый старый доступный слепок — уже не нулевой.
    let reader = Reader::open_dump([root], &[probe::SCHEMA])?;
    let oldest = reader.query(
        &Query::new()
            .kinds(KindFilter::TELEMETRY)
            .order(Order::Oldest)
            .limit(1),
    )?;
    if let Some(EntryKind::Sample {
        value: OwnedSampleValue::Blob(bytes),
        ..
    }) = oldest.entries.first().map(|e| &e.kind)
    {
        let index = u64::from_le_bytes(bytes[..8].try_into()?);
        println!("  старейший выживший слепок: №{index} из 4096 — голова вытеснена\n");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 2. Бюджет класса — общий на все неймспейсы. Число сервисов растёт, а
// бюджет «всей телеметрии» — нет: каналы черпают из него вместе, и при
// превышении вытесняется старейший сегмент КЛАССА, в чьём бы неймспейсе он
// ни лежал — молчащий сервис не держит место, которого не хватает шумному.
// (Личный предел прожорливому сервису — `store.namespace_with_quota` + NsQuota,
// а сразу всей группе — `StoreConfig::group`, как ниже.)
// ---------------------------------------------------------------------------
fn ceiling(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("— общий бюджет класса на все неймспейсы —");
    let store = Store::open(
        StoreConfig::new(root)
            .channel(
                StorageClass::Telemetry,
                ChannelConfig {
                    compression: Compression::None,
                    segment_bytes: 4 << 20,
                    ..ChannelConfig::new(12 << 20)
                },
            )
            // Настройки класса общие на хранилище — ровно до тех пор, пока
            // неймспейсы однородны. Группа (префикс имени, тот же, которым
            // отбирает `Query::group`) говорит про всех оркестраторов разом,
            // вместо повторения при каждом открытии. Бюджет и носитель ей
            // недоступны: они принадлежат КЛАССУ и общие на всё хранилище.
            .group(
                "orc-",
                GroupPolicy::new().channel(
                    StorageClass::Telemetry,
                    ChannelOverride::new().segment_bytes(2 << 20),
                ),
            )
            // Потолок оперативной памяти под буферы блоков — не бюджет:
            // бюджет про место на носителе. Необязателен и по умолчанию
            // отсутствует; нужен там, где «пишущих единицы» перестаёт быть
            // правдой.
            .with_buffer_ceiling(4 << 20),
    )?;

    // Тихий сервис записал 6 МиБ и замолчал.
    let quiet = store.namespace("orc-quiet", probe::SCHEMA)?;
    let series = quiet.series(probe::metrics::Chunk)?;
    for i in 0..1536u64 {
        series.sample(chunk(i));
        if i % 512 == 511 {
            quiet.sync()?;
        }
    }
    quiet.sync()?;

    // Шумный пишет 10 МиБ: вдвоём в потолок они не помещаются.
    let noisy = store.namespace("orc-noisy", probe::SCHEMA)?;
    let series = noisy.series(probe::metrics::Chunk)?;
    for i in 0..2560u64 {
        series.sample(chunk(i));
        if i % 512 == 511 {
            noisy.sync()?;
        }
    }
    noisy.sync()?;

    // Заодно: уборка реестра эпох. Записи о запусках, от которых не осталось
    // ни одного сегмента, вычищаются (при подъёме хранилища — автоматически).
    let removed = store.compact_epochs()?;
    println!("  эпох вычищено: {removed}");

    let stats = store.stats();
    store.shutdown();

    let reader = Reader::open_dump([root], &[probe::SCHEMA])?;
    let listing = reader.namespaces()?;
    for ns in &listing.namespaces {
        println!("  {}: занято {} КиБ", ns.name, ns.total_bytes >> 10);
    }
    println!(
        "  тихий писал 6 МиБ — его голову вытеснил чужой поток того же класса; \
         поверх потолков не вылезли ни разу (место: {}, память: {})\n",
        stats.budget_overruns, stats.buffer_overruns
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// 3. Потери и контракт. Запись не возвращает Result не потому, что исход
// не важен, а потому, что с ним нечего делать на месте вызова. Исход не
// исчезает: потери считаются и ОБЪЯВЛЯЮТСЯ в самом потоке записей, чужие
// схеме записи считаются отдельно — это дефект сборки, а не отставание диска.
// ---------------------------------------------------------------------------
fn losses(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    println!("— потери учтены и объявлены —");
    // Крошечные очереди делают переполнение воспроизводимым в примере.
    let store = Store::open(
        StoreConfig::new(root)
            .with_budget_per_class(16 << 20)
            .with_queues(QueueSizes {
                normal: 4,
                critical: 4,
            }),
    )?;
    let ns = store.namespace("orc-probe-0", probe::SCHEMA)?;

    // Обычный канал: очередь полна — запись теряется, отказ считается.
    let mut refused = 0u64;
    for seq in 0..20_000u32 {
        if let Err(e) = ns.try_log(probe::events::Ping { seq }) {
            assert!(e.loses_record());
            refused += 1;
        }
    }

    // Критический канал: очередь полна — вызывающий ждёт места. Медленнее,
    // зато авария не потеряется.
    for code in 0..2_000u32 {
        ns.try_log(probe::events::Fault { code: code as u8 })
            .expect("критическая запись не теряется");
    }

    // Запись типа, которого нет в схеме, — нарушение контракта, дефект
    // сборки. `try_*` отдаёт вердикт вызывающему и на этом умывает руки:
    let contract = ns
        .try_log_raw(EventId(0xEE), &[], None)
        .expect_err("id не из схемы");
    assert!(contract.breaks_contract() && !contract.loses_record());
    // …а «тихий» путь учитывает отказ сам: счётчик `rejected` плюс
    // однократное объявление в журнале — дефект найдут, а не будут искать
    // причину тишины.
    ns.log_raw(EventId(0xEE), &[], None);

    store.shutdown();
    let stats = store.stats();
    println!(
        "  отказов на обычной очереди: {refused}; учтено потерь: {}; \
         ожиданий на критической: {}; нарушений контракта: {}",
        stats.dropped, stats.backpressure_waits, stats.rejected
    );
    assert_eq!(stats.dropped, refused);

    // Дыра, о которой нигде не сказано, неотличима от тишины: потери
    // объявлены отметками в самом потоке, и их сумма равна счётчику.
    // Отметку разбирает `Entry::dropped_records` — формат её текста
    // принадлежит движку, парсить прозу прикладному коду не нужно.
    let reader = Reader::open_dump([root], &[probe::SCHEMA])?;
    let mut announced = 0u64;
    let mut marks = 0usize;
    for e in reader.stream(&Query::new().kinds(KindFilter {
        text: true,
        ..KindFilter::TELEMETRY
    }))? {
        if let Some(count) = e.dropped_records() {
            marks += 1;
            announced += count;
        } else if let EntryKind::Text { text, .. } = &e.kind {
            println!("  объявление в журнале: «{text}»");
        }
    }
    println!("  объявлено в потоке: {announced} (отметок: {marks}) — сходится с учётом");
    assert_eq!(announced, stats.dropped);
    Ok(())
}

// ---------------------------------------------------------------------------
// 4. Свой носитель у класса. Критические данные пишутся на защищённый раздел
// (jffs2 и подобное): классу задаётся собственный корень, раскладка внутри
// та же — `<корень>/<неймспейс>/<класс>/`. Дамп такого хранилища — два
// дерева, и вьюеру называют оба.
// ---------------------------------------------------------------------------
fn vault(
    root: &std::path::Path,
    vault: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("— критика на своём разделе —");
    let store = Store::open(
        StoreConfig::new(root)
            .with_budget_per_class(16 << 20)
            .channel(
                StorageClass::Critical,
                ChannelConfig {
                    custom_root: Some(vault.to_path_buf()),
                    ..ChannelConfig::critical(16 << 20)
                },
            ),
    )?;
    let ns = store.namespace("orc-probe-0", probe::SCHEMA)?;
    ns.log(probe::events::Ping { seq: 1 });
    ns.log(probe::events::Fault { code: 3 });
    ns.sync()?;
    println!(
        "  основной корень: {:?}\n  раздел критики:  {:?}",
        std::fs::read_dir(root.join("orc-probe-0"))?
            .filter_map(|e| e
                .ok()
                .map(|e| e.file_name().into_string().unwrap_or_default()))
            .collect::<Vec<_>>(),
        std::fs::read_dir(vault.join("orc-probe-0"))?
            .filter_map(|e| e
                .ok()
                .map(|e| e.file_name().into_string().unwrap_or_default()))
            .collect::<Vec<_>>(),
    );

    // Своё хранилище читается им самим: корни (оба!) и схемы поднятых
    // неймспейсов у него уже есть. Назвать раздел критики второй раз можно
    // только забыв — и тогда история молча оказалась бы короче.
    let reader = store.reader();
    let read = reader.query(&Query::new().kinds(KindFilter::LOGS).order(Order::Oldest))?;
    for e in &read.entries {
        println!(
            "  [{}] {}",
            e.channel.as_str(),
            reader.render(e, "ru").unwrap_or_default()
        );
    }

    // Чужой дамп — другое дело: `Store` там открывать нельзя (он берёт
    // блокировку корня и подметает временные файлы), поэтому корни и схемы
    // называются руками.
    store.shutdown();
    let offline = Reader::open_dump([root, vault], &[probe::SCHEMA])?;
    println!(
        "  тот же дамп вьюером: {} записей",
        offline
            .query(&Query::new().kinds(KindFilter::LOGS))?
            .entries
            .len()
    );
    Ok(())
}
