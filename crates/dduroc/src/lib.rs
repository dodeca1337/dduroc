//! # dduroc
//!
//! Логирование и телеметрия для встроенных систем на Linux.
//!
//! Хранит **минимум**: на диске лежат только динамические данные — тип
//! события, время дельтой от соседней записи и бинарные поля. Уровни,
//! шаблоны текста, тэги и имена живут в схеме прошивки и подставляются при
//! чтении. Времени по абсолютным часам у оборудования нет, поэтому метка —
//! это [`BootTime`] (запуск плюс микросекунды от его старта), а настенное
//! время появляется задним числом, когда приходит синхронизация: якорь
//! ретроактивен, и одна синхронизация даёт UTC всем записям этой загрузки,
//! включая сделанные до неё.
//!
//! ```no_run
//! dduroc::schema! {
//!     name: radio,
//!     version: 1,
//!     languages: [en, ru],
//!
//!     events {
//!         PowerSet = 0x01 {
//!             level: Info,
//!             tags: [rf],
//!             en: "power set to {dbm} dBm",
//!             ru: "мощность {dbm} дБм",
//!             dbm: f32,
//!         },
//!         Overheat = 0x02 {
//!             level: Error,
//!             store: critical,
//!             en: "overheat: {t} °C",
//!             ru: "перегрев: {t} °C",
//!             t: f32,
//!         },
//!     }
//!
//!     metrics {
//!         // Непрерывная величина с пределами: вне `warn` — тревога,
//!         // вне `alarm` — авария. Верхняя граница включительная,
//!         // поэтому `..=`.
//!         TempPa = 0x01 { vtype: f32, unit: "°C", tags: [thermal],
//!                         warn: ..=70.0, alarm: ..=85.0 },
//!         // Второй датчик — отдельная метрика, а не размерность первой:
//!         // рантайм-тэгов нет, и различающий признак не занимает места
//!         // в каждом отсчёте.
//!         TempLna = 0x02 { vtype: f32, unit: "°C", tags: [thermal] },
//!         // Конечный автомат как временной ряд. Коды явные: позиционная
//!         // нумерация сдвинулась бы при вставке состояния в середину.
//!         LinkState = 0x03 {
//!             states: [Los = 0: alarm, Sync = 1: warn, Lock = 2],
//!             tags: [rf],
//!         },
//!     }
//!
//!     spans {
//!         Calibration = 0x01,
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use dduroc::prelude::*;
//!
//! let store = dduroc::Store::open(
//!     dduroc::StoreConfig::new("/data/logs")
//!         // Сколько истории хранить у КАЖДОГО канала КАЖДОГО неймспейса...
//!         .with_budget(256 << 20)
//!         // ...и сколько всего занять на носителе. Второе — не сумма
//!         // первых: неймспейсов на приборе тысячи, и их бюджеты кратно
//!         // превышают любую карту. Без потолка прибор упирается в
//!         // кончившееся место раньше, чем срабатывает хоть одна ротация.
//!         .with_total_budget(20 << 30),
//! )?;
//!
//! // Экземпляр микросервиса поднимает свой неймспейс.
//! let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
//!
//! // Запись ничего не возвращает: логирование не влияет на управление.
//! // Потери учтены в `store.stats()` и объявлены в самом потоке.
//! ns.log(radio::events::PowerSet { dbm: 27.5 });
//!
//! // Тип отсчёта — из константы метрики: `Metric<f32>` не примет целое.
//! let temp = ns.series(radio::metrics::TempPa)?;
//! temp.sample(36.6);
//!
//! // Состояние — то же `sample`: на диск уходит код, имя и важность
//! // берутся из схемы. Состояние чужой метрики не пройдёт проверку типов.
//! let link = ns.series(radio::metrics::LinkState)?;
//! link.sample(radio::metrics::LinkState::Lock);
//!
//! // Границы, известные только в рантайме (модель железа определена
//! // внешней системой) — теми же range-выражениями, что и в схеме.
//! // На диск не пишутся.
//! ns.set_thresholds(radio::metrics::TempPa, ..=60.0, ..=75.0)?;
//!
//! {
//!     let cal = ns.span(radio::spans::Calibration);
//!     cal.log(radio::events::PowerSet { dbm: 30.0 });
//! } // конец спана записывается здесь
//!
//! // Пришёл GPS: якорь ретроактивен, UTC получают и записи выше.
//! store.record_sync(Utc::now(), SyncSource::Gps)?;
//!
//! // Кому нужен вердикт на месте вызова — парный `try_*`.
//! if let Err(e) = ns.try_log(radio::events::Overheat { t: 91.0 }) {
//!     assert!(e.loses_record() || e.breaks_contract());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Примеры
//!
//! Развёрнутые сценарии — в `examples/` крейта, каждый запускается сам по
//! себе и печатает, что сделал:
//!
//! - `01_device_writes` — пишущая сторона: схема целиком, события, телеметрия,
//!   спаны, свободный текст, синхронизация времени;
//! - `02_viewer_reads` — читающая: запросы, фильтры, окна в двух шкалах
//!   времени, восстановление текста и состояний, честность неполного ответа;
//! - `03_schema_grows` — миграции: `history {}`, правила, чтение сквозь шаги
//!   и явный физический прогон `Namespace::migrate`;
//! - `04_operations` — эксплуатация: политика каналов, ротация, потолок
//!   хранилища, учёт и объявление потерь.
//!
//! `cargo run -p dduroc --example 01_device_writes`

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

// Макрос генерирует пути вида `::dduroc::...`, поэтому крейт должен быть
// виден сам себе — иначе `schema!` не собирался бы внутри собственных тестов
// и примеров документации.
extern crate self as dduroc;

/// Объявить схему неймспейса.
pub use dduroc_macros::schema;

/// Настенное время — [`chrono`], чтобы не заводить свой тип даты и не
/// заставлять пользователя добавлять зависимость ради одного вызова.
pub use chrono;

/// Чтение хранилища: тот же слой на устройстве и в офлайн-вьюере.
///
/// Отдельной зависимости не нужно — за одним исключением: прошивке, которая
/// только пишет, чтение можно выключить (`default-features = false`).
#[cfg(feature = "read")]
pub use dduroc_read as read;

// Реэкспорты для кода, генерируемого макросом: пользователю не нужно
// добавлять serde и postcard в свои зависимости.
#[doc(hidden)]
pub use postcard;
#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use serde_json;

pub use dduroc_engine::MigrationReport;
pub use dduroc_engine::channel::{ChannelConfig, Durability};
pub use dduroc_engine::epochs::SyncSource;
pub use dduroc_engine::limits::{EffectiveLimits, MetricLimits, StateStatus};
pub use dduroc_engine::metric::{Blob, Metric, MetricState, MetricValue, Untyped};
pub use dduroc_engine::namespace::{Namespace, Series, SpanGuard};
pub use dduroc_engine::schema::{
    DecodeError, EventDecoders, EventDesc, FieldDesc, Language, MetricDesc, MetricKind,
    MigratedRecord, Migration, OwnedRecord, Range, Schema, Severity, SpanDesc, StateDesc,
    StorageClass, Thresholds,
};
pub use dduroc_engine::staged::{OwnedValue, Payload};
pub use dduroc_engine::stats::Stats;
pub use dduroc_engine::store::{Store, StoreConfig};
pub use dduroc_engine::writer::QueueSizes;
pub use dduroc_engine::{Clock, Error, Result};
pub use dduroc_format::{
    BootCounter, BootTime, Compression, EventId, Level, MetricId, Micros, ProtocolVersion, SpanId,
    SpanKindId, ValueType,
};

/// Всё, что нужно для записи: типы и трейты одной строкой.
pub mod prelude {
    pub use crate::{
        BootTime, Event, Level, MetricLimits, MetricState, NamespaceExt, OwnedValue, Severity,
        SpanExt, Store, StoreConfig, SyncSource, Thresholds,
    };
    /// Настенное время нужно ровно для [`crate::Store::record_sync`], но нужно
    /// почти всегда: без синхронизации у записей есть только относительное
    /// время.
    pub use chrono::{DateTime, Utc};
}

/// Тип события, объявленный макросом [`schema!`].
///
/// Реализуется генерируемым кодом; вручную реализовывать не нужно.
pub trait Event: serde::Serialize {
    /// Идентификатор в пределах схемы.
    const ID: EventId;
    /// Уровень — статическое свойство типа, на диск не пишется.
    const LEVEL: Level;
    /// Имя типа для интерфейсов.
    const NAME: &'static str;
}

/// Payload события какой-то версии схемы — то, что умеет лечь в запись.
///
/// Реализуется генерируемым кодом для событий **текущей** версии (через
/// [`Event`]) и для типов из `history {}` — раскладок прежних версий. Шаг
/// миграции возвращает любой из них: обычно текущий тип, а в длинной цепочке
/// — раскладку следующей версии, которую доведёт до текущей следующий шаг.
pub trait EventShape: serde::Serialize {
    /// Идентификатор события в той версии, которой принадлежит раскладка.
    const SHAPE_ID: EventId;
}

impl<E: Event> EventShape for E {
    const SHAPE_ID: EventId = E::ID;
}

/// Исход типизированного правила миграции.
///
/// Замыкание правила возвращает либо payload ([`EventShape`] — запись
/// перекодируется), либо `Option` от него (`None` — запись удаляется).
/// Трейт существует, чтобы генерируемый код принимал и то, и другое, не
/// заставляя каждое правило писать `Some(...)`.
pub trait MigrateOutcome {
    /// Превратить исход в запись миграции.
    fn into_outcome(self) -> std::result::Result<Option<OwnedRecord>, DecodeError>;
}

impl<E: EventShape> MigrateOutcome for E {
    fn into_outcome(self) -> std::result::Result<Option<OwnedRecord>, DecodeError> {
        Ok(Some(OwnedRecord::Message {
            event: E::SHAPE_ID,
            payload: postcard::to_allocvec(&self).map_err(|_| DecodeError)?,
        }))
    }
}

impl<E: EventShape> MigrateOutcome for Option<E> {
    fn into_outcome(self) -> std::result::Result<Option<OwnedRecord>, DecodeError> {
        match self {
            Some(e) => e.into_outcome(),
            None => Ok(None),
        }
    }
}

/// Вызвать преобразование типизированного правила. Только для макроса.
///
/// Существует из-за порядка вывода типов у замыканий: `(|old| old.dbm)(x)`
/// не компилируется — тело проверяется раньше, чем немедленный вызов успел
/// подсказать тип параметра. Ожидание `impl FnOnce(T)` сообщает его до
/// проверки тела, и правило пишется без аннотации: `|old| ...`.
#[doc(hidden)]
pub fn __migrate_map<T, O: MigrateOutcome>(
    map: impl FnOnce(T) -> O,
    old: T,
) -> std::result::Result<Option<OwnedRecord>, DecodeError> {
    map(old).into_outcome()
}

/// Расширение [`Namespace`] типизированной записью.
///
/// Записывающие методы **ничего не возвращают**: логирование не должно влиять
/// на управление в прикладном коде, а сделать с отказом на месте вызова нечего
/// — повтор при отстающем диске только усугубляет. Потери учтены в
/// [`Stats::dropped`] и объявлены в самом потоке записей; кому нужен ответ на
/// месте, у каждого метода есть парный `try_*`.
pub trait NamespaceExt {
    /// Записать событие: поля сериализуются в компактный бинарный вид.
    fn log<E: Event>(&self, event: E);

    /// Записать событие, привязав его к спану.
    fn log_in<E: Event>(&self, span: &SpanGuard, event: E);

    /// То же с ответом об исходе. `Err` различается двумя предикатами:
    /// [`Error::loses_record`] — запись не дошла до носителя,
    /// [`Error::breaks_contract`] — событие не из этой схемы.
    fn try_log<E: Event>(&self, event: E) -> Result<()>;

    /// То же для события в спане.
    fn try_log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()>;
}

impl NamespaceExt for Namespace {
    #[inline]
    fn log<E: Event>(&self, event: E) {
        self.report_here(self.try_log(event));
    }

    #[inline]
    fn log_in<E: Event>(&self, span: &SpanGuard, event: E) {
        self.report_here(self.try_log_in(span, event));
    }

    #[inline]
    fn try_log<E: Event>(&self, event: E) -> Result<()> {
        self.try_log_payload(E::ID, encode(&event)?, None)
    }

    #[inline]
    fn try_log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()> {
        self.try_log_payload(E::ID, encode(&event)?, Some(span.id()))
    }
}

/// Расширение [`SpanGuard`] типизированной записью.
pub trait SpanExt {
    /// Записать событие внутри спана.
    fn log<E: Event>(&self, event: E);

    /// То же с ответом об исходе.
    fn try_log<E: Event>(&self, event: E) -> Result<()>;
}

impl SpanExt for SpanGuard {
    #[inline]
    fn log<E: Event>(&self, event: E) {
        let outcome = self.try_log(event);
        self.namespace().report_here(outcome);
    }

    #[inline]
    fn try_log<E: Event>(&self, event: E) -> Result<()> {
        self.try_log_payload(E::ID, encode(&event)?)
    }
}

/// Учесть отказ сериализации там же, где движок учитывает свои.
trait ReportHere {
    fn report_here(&self, outcome: Result<()>);
}

impl ReportHere for Namespace {
    #[inline]
    fn report_here(&self, outcome: Result<()>) {
        if let Err(e) = outcome {
            self.note_failure(e);
        }
    }
}

/// Сериализовать поля события прямо в буфер записи.
///
/// Именно сюда упирается стоимость логирования, поэтому промежуточного
/// `Vec` нет: postcard пишет в тот самый буфер, который уйдёт в очередь.
/// При типичном размере полей он остаётся inline, и обращений к куче на
/// событие не происходит вовсе.
#[inline]
fn encode<E: Event>(event: &E) -> Result<Payload> {
    postcard::to_extend(event, Payload::new()).map_err(|_| Error::EncodeFailed { event: E::NAME })
}

#[cfg(test)]
mod tests {
    use super::*;

    schema! {
        name: testing,
        version: 1,
        languages: [en, ru],

        events {
            PowerSet = 0x01 {
                level: Info,
                tags: [rf],
                en: "power set to {dbm} dBm",
                ru: "мощность {dbm} дБм",
                dbm: f32,
            },
            Overheat = 0x02 {
                level: Error,
                store: critical,
                en: "overheat: {t:.1} °C on {sensor}",
                ru: "перегрев: {t:.1} °C на {sensor}",
                t: f32,
                sensor: u8,
            },
            Started = 0x10 {
                level: Debug,
                en: "started",
                ru: "запущено",
            },
        }

        metrics {
            Temp = 0x01 { vtype: f32, unit: "°C", tags: [thermal],
                          warn: ..=70.0, alarm: ..=85.0 },
            Spectrum = 0x02 { vtype: blob, store: telemetry },
            LinkState = 0x03 {
                states: [Los = 0: alarm, Sync = 1: warn, Lock = 2],
                tags: [rf],
            },
            Locked = 0x04 {
                vtype: bool,
                states: [Unlocked = 0: alarm, Locked = 1],
            },
        }

        spans {
            Calibration = 0x01,
            PowerRamp = 0x02,
        }
    }

    /// Шаги миграции живут рядом с объявлением схемы; макрос раскрывается в
    /// отдельный модуль, но имена из этой области там видны.
    fn migrate_v1(_: MigratedRecord<'_>) -> std::result::Result<Option<OwnedRecord>, DecodeError> {
        Ok(Some(OwnedRecord::AsIs))
    }
    fn migrate_v2(_: MigratedRecord<'_>) -> std::result::Result<Option<OwnedRecord>, DecodeError> {
        Ok(Some(OwnedRecord::AsIs))
    }

    schema! {
        name: migrating,
        version: 3,
        languages: [en],

        events {
            Renamed = 0x01 { level: Info, en: "renamed" },
            Untouched = 0x02 { level: Info, en: "untouched" },
        }

        metrics {
            Temp = 0x01 { vtype: f32 },
        }

        // Ключ — версия, ИЗ которой мигрируем: цепочка обязана быть
        // непрерывной от 1 до текущей.
        migrations {
            // Затронутые типы названы: сегменты без них не переписываются.
            1 => migrate_v1 { events: [Renamed], metrics: [Temp] },
            // Не названы — значит затрагивает всё.
            2 => migrate_v2,
        }
    }

    #[test]
    fn migration_touches_everything_unless_told_otherwise() {
        // Множества типов в footer'е решают, переписывать ли сегмент. Пустые
        // списки означали «не затрагивает ничего», и шаг, у которого их забыли
        // объявить, молча обходил бы всю историю стороной — а обнаружилось бы
        // это только на нечитаемых логах через месяц.
        use dduroc_format::{FooterBuilder, MetricId};

        migrating::SCHEMA.validate().expect("схема корректна");

        // Типы попадают в множества сегмента вместе с блоком, в котором
        // встретились: блок, не поместившийся в сегмент, уезжает в следующий,
        // и его типы обязаны уехать с ним. Поэтому footer собирается так же,
        // как его собирает writer, — записи, потом блок.
        let footer_of = |events: &[EventId], metrics: &[MetricId]| {
            use dduroc_format::block::{BlockHeader, Compression};
            let mut b = FooterBuilder::new();
            for e in events {
                b.add_event(*e);
            }
            for m in metrics {
                b.add_metric(*m);
            }
            b.add_block(
                32,
                &BlockHeader {
                    body_len: 10,
                    raw_len: 10,
                    seq: 0,
                    base: dduroc_format::Micros(0),
                    count: 1,
                    compression: Compression::None,
                    crc: 0,
                },
                dduroc_format::Micros(1),
            );
            let bytes = b.build();
            dduroc_format::Footer::parse(&bytes).unwrap().unwrap()
        };

        let narrow = migrating::SCHEMA.migration(1).expect("шаг объявлен");
        assert!(!narrow.touches_all, "затронутые типы названы явно");
        assert_eq!(narrow.events, &[EventId(1)]);
        assert_eq!(narrow.metrics, &[MetricId(1)]);
        assert!(
            narrow.touches(&footer_of(&[EventId(1)], &[])),
            "сегмент с затронутым событием переписывается"
        );
        assert!(
            !narrow.touches(&footer_of(&[EventId(2)], &[])),
            "сегмент без затронутых типов не тратит цикла записи флеша"
        );

        let wide = migrating::SCHEMA.migration(2).expect("шаг объявлен");
        assert!(
            wide.touches_all,
            "списки не объявлены — шаг обязан считаться затрагивающим всё"
        );
        assert!(
            wide.touches(&footer_of(&[EventId(2)], &[])),
            "молча пропустить сегмент хуже, чем переписать лишний"
        );
        assert!(wide.touches(&footer_of(&[], &[])), "и пустой тоже");
    }

    #[test]
    fn channel_policy_is_configurable_through_the_facade_alone() {
        // Фасад — единственная зависимость приложения, поэтому всё, что
        // фигурирует в сигнатурах его же реэкспортов, обязано быть названо им
        // самим. `Compression` не был реэкспортирован, и настроить сжатие
        // канала, не добавив в проект `dduroc-engine` вручную, было нельзя —
        // при том, что поле в `ChannelConfig` публичное.
        // Заодно проверяется, что имя канала берётся из класса, а не из
        // конфига: два источника одного имени расходятся, и расхождение здесь
        // увело бы записи класса в чужой каталог с чужой политикой. Имя в
        // конфиге намеренно неверное.
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path())
            .with_budget(16 * 1024 * 1024)
            .channel(
                StorageClass::TELEMETRY,
                ChannelConfig {
                    compression: Compression::None,
                    durability: Durability::Relaxed,
                    ..ChannelConfig::new("opechatka", 16 * 1024 * 1024)
                },
            );
        {
            let store = Store::open(config).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();
            ns.series(testing::metrics::Spectrum)
                .unwrap()
                .sample(vec![1u8, 2, 3]);
            ns.sync().unwrap();
            assert!(store.stats().is_clean(), "{:?}", store.stats());
            store.shutdown();
        }

        let channels: Vec<String> = std::fs::read_dir(dir.path().join("orc-radio-0"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        assert!(
            channels.iter().any(|c| c == "telemetry"),
            "каталог назван классом: {channels:?}"
        );
        assert!(
            !channels.iter().any(|c| c == "opechatka"),
            "и только им: {channels:?}"
        );
    }

    #[test]
    fn an_invalid_channel_config_is_refused_at_open() {
        // Настройки канала приходят снаружи и до сих пор не проверялись
        // нигде: бюджет меньше двух сегментов доезжал до writer'а как есть, и
        // ротация съедала единственный сегмент сразу после запечатывания.
        let dir = tempfile::tempdir().unwrap();
        let broken = StoreConfig::new(dir.path()).channel(
            StorageClass::DEFAULT,
            ChannelConfig {
                budget_bytes: 4 * 1024 * 1024,
                segment_bytes: 4 * 1024 * 1024,
                ..ChannelConfig::new("default", 4 * 1024 * 1024)
            },
        );
        let err = Store::open(broken).expect_err("бюджет в один сегмент бессмыслен");
        assert!(
            matches!(err, Error::BadChannel { .. }),
            "отказ пришёл именно от проверки настроек канала: {err}"
        );
    }

    #[test]
    fn generated_schema_is_valid() {
        testing::SCHEMA.validate().expect("схема корректна");
        assert_eq!(testing::SCHEMA.name, "testing");
        assert_eq!(testing::SCHEMA.version, ProtocolVersion(1));
        assert_eq!(testing::SCHEMA.events.len(), 3);
        assert_eq!(testing::SCHEMA.metrics.len(), 4);
        assert_eq!(testing::SCHEMA.spans.len(), 2);
    }

    #[test]
    fn storage_class_comes_from_declaration() {
        let overheat = testing::SCHEMA.event(EventId(2)).unwrap();
        assert_eq!(overheat.class, StorageClass::CRITICAL);
        assert_eq!(overheat.level, Level::Error);

        let power = testing::SCHEMA.event(EventId(1)).unwrap();
        assert_eq!(power.class, StorageClass::DEFAULT);
        assert_eq!(power.tags, &["rf"]);

        let spectrum = testing::SCHEMA.metric(MetricId(2)).unwrap();
        assert_eq!(spectrum.class, StorageClass::TELEMETRY);
        assert_eq!(spectrum.value_type, ValueType::Blob);
    }

    #[test]
    fn decoders_render_and_serialize() {
        let event = testing::events::Overheat {
            t: 87.25,
            sensor: 3,
        };
        let payload = encode(&event).unwrap();

        let desc = testing::SCHEMA.event(EventId(2)).unwrap();
        let decoders = desc.decoders.expect("макрос сгенерировал декодеры");

        // Язык 0 — en, язык 1 — ru: порядок из `languages:`.
        assert_eq!(
            (decoders.render)(&payload, 0).unwrap(),
            "overheat: 87.2 °C on 3"
        );
        assert_eq!(
            (decoders.render)(&payload, 1).unwrap(),
            "перегрев: 87.2 °C на 3"
        );
        // Неизвестный язык не паникует.
        assert!((decoders.render)(&payload, 99).is_ok());

        let json = (decoders.json)(&payload).unwrap();
        assert!(json.contains("\"t\":87.25"), "получено {json}");
        assert!(json.contains("\"sensor\":3"));

        // Мусорный payload — ошибка, а не паника.
        assert!((decoders.render)(&[0xFF, 0xFF, 0xFF, 0xFF], 0).is_err());
    }

    #[test]
    fn event_without_fields_renders_template_as_is() {
        let payload = encode(&testing::events::Started {}).unwrap();
        assert!(payload.is_empty(), "у события без полей payload пуст");
        let d = testing::SCHEMA
            .event(EventId(0x10))
            .unwrap()
            .decoders
            .unwrap();
        assert_eq!((d.render)(&payload, 0).unwrap(), "started");
        assert_eq!((d.render)(&payload, 1).unwrap(), "запущено");
    }

    #[test]
    fn payload_is_compact() {
        // f32 + u8 = 5 байт: ни имён полей, ни типов на диске нет.
        let payload = encode(&testing::events::Overheat { t: 1.0, sensor: 2 }).unwrap();
        assert_eq!(payload.len(), 5, "получено {payload:?}");
    }

    #[test]
    fn end_to_end_write_and_read() {
        use dduroc_read::{KindFilter, Order, Query, Reader};

        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path()).with_budget(8 * 1024 * 1024);
        {
            let store = Store::open(config.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

            ns.log(testing::events::PowerSet { dbm: 27.5 });
            ns.log(testing::events::Overheat { t: 91.0, sensor: 1 });

            let temp = ns.series(testing::metrics::Temp).unwrap();
            temp.sample(36.6);

            {
                let cal = ns.span(testing::spans::Calibration);
                cal.log(testing::events::PowerSet { dbm: 30.0 });
            }

            ns.sync().unwrap();
            assert!(store.stats().is_clean(), "{:?}", store.stats());
            store.shutdown();
        }

        let reader = Reader::open(dir.path(), &[testing::SCHEMA]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();
        assert!(result.is_complete());
        assert_eq!(result.entries.len(), 3);

        // Текст восстановлен из шаблона схемы: на диске его не было.
        let rendered: Vec<String> = result
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                dduroc_read::EntryKind::Message { event, payload, .. } => {
                    dduroc_read::reader_render(&testing::SCHEMA, *event, payload, "ru")
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "мощность 27.5 дБм",
                "перегрев: 91.0 °C на 1",
                "мощность 30 дБм"
            ]
        );

        // Событие внутри спана привязано к нему.
        let in_span = result.entries.iter().filter(|e| e.span.is_some()).count();
        assert_eq!(in_span, 1);
    }

    #[test]
    fn macro_generates_typed_states_and_limits() {
        // Проверка того, что даёт макрос: константа метрики и Rust-тип
        // состояний с одним именем (у значений и типов разные пространства
        // имён), подписи и важность в схеме, пределы из диапазонов.
        testing::SCHEMA.validate().expect("схема корректна");

        let link = testing::SCHEMA.metric(MetricId(3)).unwrap();
        assert_eq!(link.kind, MetricKind::State);
        assert_eq!(link.value_type, ValueType::U64, "код состояния — целое");
        assert_eq!(link.tags, &["rf"]);
        assert_eq!(link.states.len(), 3);
        assert_eq!(link.state(0).unwrap().name, "Los");
        assert_eq!(link.state(0).unwrap().severity, Severity::Alarm);
        assert_eq!(link.state(1).unwrap().severity, Severity::Warn);
        assert_eq!(link.state(2).unwrap().severity, Severity::Normal);

        // Одно имя, два пространства имён: константа метрики и тип её
        // состояний.
        let id: MetricId = testing::metrics::LinkState.into();
        assert_eq!(id, MetricId(3));
        assert_eq!(
            <testing::metrics::LinkState as MetricState>::metric(),
            MetricId(3)
        );
        assert_eq!(testing::metrics::LinkState::Lock.code(), 2);
        assert_eq!(testing::metrics::LinkState::Los.name(), "Los");

        // bool как два состояния.
        let locked = testing::SCHEMA.metric(MetricId(4)).unwrap();
        assert_eq!(locked.value_type, ValueType::Bool);
        assert_eq!(locked.kind, MetricKind::State);
        assert_eq!(locked.state(0).unwrap().severity, Severity::Alarm);

        // Пределы из диапазонов: верхняя граница включительная.
        let temp = testing::SCHEMA.metric(MetricId(1)).unwrap();
        assert_eq!(temp.thresholds.warn.max, Some(70.0));
        assert_eq!(temp.thresholds.warn.min, None);
        assert_eq!(temp.thresholds.alarm.max, Some(85.0));
        assert_eq!(
            temp.severity_of(&dduroc_format::Value::F32(70.0)),
            Severity::Normal
        );
        assert_eq!(
            temp.severity_of(&dduroc_format::Value::F32(71.0)),
            Severity::Warn
        );
        assert_eq!(
            temp.severity_of(&dduroc_format::Value::F32(90.0)),
            Severity::Alarm
        );

        // Метрика без пределов и без состояний.
        let spec = testing::SCHEMA.metric(MetricId(2)).unwrap();
        assert!(spec.thresholds.is_unset());
        assert!(spec.states.is_empty());
        assert_eq!(spec.kind, MetricKind::Gauge);
    }

    #[test]
    fn states_roundtrip_through_disk_as_bare_codes() {
        use dduroc_read::{EntryKind, KindFilter, Order, Query, Reader};

        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path()).with_budget(8 * 1024 * 1024);
        {
            let store = Store::open(config.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

            let link = ns.series(testing::metrics::LinkState).unwrap();
            for st in [
                testing::metrics::LinkState::Los,
                testing::metrics::LinkState::Sync,
                testing::metrics::LinkState::Lock,
            ] {
                link.sample(st);
            }
            let locked = ns.series(testing::metrics::Locked).unwrap();
            locked.sample(testing::metrics::Locked::Locked);

            ns.sync().unwrap();
            assert!(store.stats().is_clean(), "{:?}", store.stats());
            assert_eq!(
                store.stats().records_written,
                4,
                "ни одной служебной записи: идентичность ряда — сама метрика"
            );
            store.shutdown();
        }

        let reader = Reader::open(dir.path(), &[testing::SCHEMA]).unwrap();
        let result = reader
            .query(
                &Query::new()
                    .order(Order::Oldest)
                    .kinds(KindFilter::TELEMETRY),
            )
            .unwrap();
        assert!(result.is_complete());

        let seen: Vec<(&str, Severity)> = result
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                EntryKind::Sample {
                    state: Some(name),
                    severity: Some(sev),
                    ..
                } => Some((*name, *sev)),
                _ => None,
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                ("Los", Severity::Alarm),
                ("Sync", Severity::Warn),
                ("Lock", Severity::Normal),
                ("Locked", Severity::Normal),
            ],
            "подписи и важность восстановлены по схеме — на диске их не было"
        );
    }

    #[test]
    fn every_lost_record_is_accounted_for_in_the_stream() {
        // Дыра, о которой нигде не сказано, неотличима от тишины. Отметки о
        // потерях выталкиваются по таймеру, поэтому потери, случившиеся между
        // последним таймером и остановкой, попадают в поток только если их
        // выталкивают ещё и при завершении, — а именно тогда очередь
        // переполнена чаще всего: процесс останавливают под нагрузкой.
        //
        // Останавливаемся без `sync`: проверяется путь остановки как таковой.
        use dduroc_read::{EntryKind, KindFilter, Order, Query, Reader};

        let dir = tempfile::tempdir().unwrap();
        // Крошечная очередь делает переполнение воспроизводимым, а не
        // зависящим от того, чем ещё занята машина.
        let config = StoreConfig::new(dir.path())
            .with_budget(32 * 1024 * 1024)
            .with_queues(QueueSizes {
                normal: 4,
                critical: 4,
            });
        let refused = {
            let store = Store::open(config.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

            let mut refused = 0u64;
            for _ in 0..20_000 {
                if let Err(e) = ns.try_log(testing::events::PowerSet { dbm: 27.5 }) {
                    assert!(e.loses_record(), "потеря, а не дефект вызова: {e}");
                    refused += 1;
                }
            }
            store.shutdown();
            assert_eq!(
                store.stats().dropped,
                refused,
                "счётчик обязан совпасть с числом отказов"
            );
            refused
        };
        assert!(refused > 0, "тест бессмыслен без переполнения очереди");

        let reader = Reader::open(dir.path(), &[testing::SCHEMA]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter {
                text: true,
                ..KindFilter::LOGS
            }))
            .unwrap();

        let announced: u64 = result
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                EntryKind::Text { text, .. } => text
                    .strip_prefix("потеряно записей: ")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|n| n.parse::<u64>().ok()),
                _ => None,
            })
            .sum();

        assert_eq!(
            announced, refused,
            "в потоке объявлено {announced} потерь при {refused} реальных: \
             остаток не выталкивается при остановке"
        );
    }
}
