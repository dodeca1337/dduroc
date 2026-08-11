//! Схема выросла: history, правила миграции, чтение сквозь шаги, migrate().
//!
//! Запуск: `cargo run -p dduroc --example 03_schema_grows`
//!
//! Прошивку обновили, а история на носителе осталась в старой раскладке —
//! и это опасно не «нечитаемостью», а обратным: postcard не самоописываем,
//! и старые байты молча разобрались бы новыми декодерами в правдоподобный
//! мусор (i8 в раскладке f32 — число, просто не то).
//!
//! Как это устроено:
//!  - чтение ВСЕГДА правильное: сегмент прежней версии проходит через шаги
//!    миграции прямо при чтении, физического прогона ждать не нужно;
//!  - `ns.migrate()` — явный физический прогон: перезаписывает старые
//!    сегменты в текущую раскладку. Когда его звать, решает приложение —
//!    прогон жжёт ресурс флеша (после обновления, в тихий час);
//!  - прогон меняет носитель, а не ответ: чтение до и после — одно и то же.

use dduroc::MigrationReport;
use dduroc::prelude::*;
use dduroc::read::{EntryKind, KindFilter, Order, Query, Reader};

// ---------------------------------------------------------------------------
// Вчерашняя прошивка, версия 1. В реальности этого модуля в новом билде нет —
// здесь он существует, чтобы пример сам записал «старую» историю.
// ---------------------------------------------------------------------------
mod yesterday {
    dduroc::schema! {
        name: radio,
        version: 1,
        languages: [ru],

        events {
            // Мощность хранили целым дБм — решили, что зря.
            PowerSet = 0x01 { level: Info, ru: "мощность {dbm} дБм", dbm: i8 },
            // Тип, который в новой схеме не нужен вовсе.
            LegacyPing = 0x05 { level: Debug, ru: "пинг" },
            // Тип, которого миграция не касается.
            Note = 0x03 { level: Info, ru: "заметка {n}", n: u32 },
        }

        metrics {
            // Температуру решили переименовать и перевесить на новый id.
            Temp = 0x07 { type: f32, unit: "°C" },
        }
    }
}

// ---------------------------------------------------------------------------
// Сегодняшняя прошивка, версия 2. Три перемены:
//   dbm: i8 → f32; LegacyPing удалён; Temp переехал на id 0x08.
// ---------------------------------------------------------------------------
mod today {
    dduroc::schema! {
        name: radio,
        version: 2,
        languages: [ru],

        events {
            PowerSet = 0x01 { level: Info, ru: "мощность {dbm} дБм", dbm: f32 },
            Note = 0x03 { level: Info, ru: "заметка {n}", n: u32 },
        }

        metrics {
            TempPa = 0x08 { type: f32, unit: "°C" },
        }

        // Раскладки прошлых версий — те, что потребляют шаги. Перечисляются
        // только изменившиеся типы и только их поля. Макрос генерирует
        // `v1::PowerSet` с Deserialize (шаг декодирует старые байты им)
        // и Serialize (тесты пишут фикстуры старой версии тем же типом).
        history {
            1 { events { PowerSet = 0x01 { dbm: i8 } } }
        }

        // Шаг «из версии 1». Ключ правила говорит, ЧТО менять, действие — КАК:
        //   v1::Тип: |old| ...   — декод старой раскладки, замыкание, энкод;
        //   event(0xID): drop    — тип удалён, имени в схеме больше нет;
        //   metric(0xID): metrics::Имя — ремап id, значения не трогаются.
        // Затронутые множества выводятся из ключей: сегмент, где этих типов
        // нет (Note и только Note), не перепишется и не потратит флеш.
        // Незатронутое правилами (Note, текст, спаны) проходит как есть.
        //
        // Для перемен, которые правилами не выразить, остаётся сырой шаг:
        //   `1 => migrate_v1,` — fn(MigrationInput) с полным доступом.
        migrations {
            1 => {
                v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) },
                event(0x05): drop,
                metric(0x07): metrics::TempPa,
            },
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::temp_dir().join("dduroc-examples").join("03");
    let _ = std::fs::remove_dir_all(&root);
    let cfg = StoreConfig::new(&root).with_budget_per_class(16 << 20);

    // Вчера: прошивка v1 пишет свою историю.
    {
        let store = Store::open(cfg.clone())?;
        let ns = store.namespace("orc-radio-0", yesterday::radio::SCHEMA)?;
        ns.log(yesterday::radio::events::PowerSet { dbm: -3 });
        ns.log(yesterday::radio::events::LegacyPing {});
        ns.log(yesterday::radio::events::Note { n: 7 });
        ns.series(yesterday::radio::metrics::Temp)?.sample(36.5);
        ns.sync()?;
        store.shutdown();
    }

    // Сегодня: тот же каталог открывает прошивка v2.
    let store = Store::open(cfg)?;
    let ns = store.namespace("orc-radio-0", today::radio::SCHEMA)?;

    // Долг виден сразу: на носителе есть сегменты раскладки старше схемы.
    // Это не ошибка, а нормальное состояние после обновления прошивки.
    println!("незавершённая миграция: {:?}", ns.pending_migration());

    // -----------------------------------------------------------------------
    // Чтение ДО прогона. Шаги применяются на лету: -3i8 стал -3.0f32 и
    // рендерится шаблоном v2, LegacyPing уже отсутствует, Temp отвечает под
    // новым идентификатором. Ни одна запись не показана в старой раскладке.
    // -----------------------------------------------------------------------
    let before = read_all(&root)?;
    println!("\nдо прогона:");
    print_entries(&before);

    // -----------------------------------------------------------------------
    // Физический прогон. Тяжёлая работа на потоке вызывающего; запись при
    // этом не останавливается. Обрыв питания в любом месте безопасен:
    // каждый сегмент остаётся либо прежним, либо уже переписанным, повторный
    // вызов продолжает с оставшихся.
    // -----------------------------------------------------------------------
    let report = ns.migrate()?;
    println!("\nотчёт прогона:");
    println!("  переписано сегментов:  {}", report.rewritten);
    println!("  уже текущей версии:    {}", report.already_current);
    println!("  не затронуто шагами:   {}", report.skipped_untouched);
    println!("  записей переписано:    {}", report.records_rewritten);
    println!("  записей удалено:       {}", report.records_dropped);
    println!("долг после прогона: {:?}", ns.pending_migration());

    // Прогон меняет носитель, а не ответ: чтение отвечает то же самое.
    let after = read_all(&root)?;
    println!("\nпосле прогона:");
    print_entries(&after);
    assert_eq!(before, after, "ответ читателя не имеет права измениться");

    // Повторный вызов — честный no-op: долга нет, флеш не тратится.
    assert_eq!(ns.migrate()?, MigrationReport::default());

    store.shutdown();
    Ok(())
}

/// Прочитать весь журнал и телеметрию глазами схемы v2.
fn read_all(root: &std::path::Path) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let reader = Reader::open(root, &[today::radio::SCHEMA])?;
    let result = reader.query(
        &Query::new()
            .kinds(KindFilter {
                text: false,
                ..KindFilter::default()
            })
            .order(Order::Oldest),
    )?;
    assert!(result.is_complete(), "повреждения: {:?}", result.damaged);

    Ok(result
        .entries
        .iter()
        .filter_map(|e| match &e.kind {
            EntryKind::Message { event, .. } => Some(format!(
                "{} (id 0x{:02x})",
                reader.render(e, "ru").unwrap_or_else(|| "?".to_owned()),
                event.0
            )),
            EntryKind::Sample {
                metric,
                metric_name,
                value,
                ..
            } => Some(format!(
                "{} = {value:?} (id 0x{:02x})",
                metric_name.unwrap_or("?"),
                metric.0
            )),
            _ => None,
        })
        .collect())
}

fn print_entries(lines: &[String]) {
    for l in lines {
        println!("  {l}");
    }
}
