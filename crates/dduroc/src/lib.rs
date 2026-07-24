//! # dduroc
//!
//! Логирование и телеметрия для встроенных систем на Linux.
//!
//! Хранит **минимум**: на диске лежат только динамические данные — тип
//! события, время дельтой от соседней записи и бинарные поля. Уровни,
//! шаблоны текста, тэги и имена живут в схеме прошивки и подставляются при
//! чтении. Времени по абсолютным часам у оборудования нет, поэтому метка —
//! это `(номер запуска, микросекунды от старта)`, а UTC появляется задним
//! числом, когда приходит синхронизация.
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
//!         Temp = 0x01 { vtype: f32, unit: "°C", tags: [sensor] },
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
//! let config = dduroc::StoreConfig::new("/data/logs").with_budget(20 << 30);
//! let store = dduroc::Store::open(config.clone())?;
//!
//! // Экземпляр микросервиса поднимает свой неймспейс.
//! let ns = store.namespace("orc-radio-0", radio::SCHEMA, &config)?;
//!
//! ns.log(radio::events::PowerSet { dbm: 27.5 })?;
//!
//! let temp = ns.series(radio::metrics::Temp, &[("sensor", "pa")])?;
//! temp.sample_f32(36.6)?;
//!
//! {
//!     let cal = ns.span(radio::spans::Calibration)?;
//!     cal.log(radio::events::PowerSet { dbm: 30.0 })?;
//! } // конец спана записывается здесь
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

// Макрос генерирует пути вида `::dduroc::...`, поэтому крейт должен быть
// виден сам себе — иначе `schema!` не собирался бы внутри собственных тестов
// и примеров документации.
extern crate self as dduroc;

/// Объявить схему неймспейса.
pub use dduroc_macros::schema;

// Реэкспорты для кода, генерируемого макросом: пользователю не нужно
// добавлять serde и postcard в свои зависимости.
#[doc(hidden)]
pub use postcard;
#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use serde_json;

pub use dduroc_engine::channel::{ChannelConfig, Durability};
pub use dduroc_engine::epochs::SyncSource;
pub use dduroc_engine::namespace::{Namespace, Series, SpanGuard};
pub use dduroc_engine::schema::{
    DecodeError, EventDecoders, EventDesc, FieldDesc, Language, MetricDesc, MigratedRecord,
    Migration, OwnedRecord, Schema, SpanDesc, StorageClass,
};
pub use dduroc_engine::staged::{OwnedValue, Payload};
pub use dduroc_engine::stats::Stats;
pub use dduroc_engine::store::{Store, StoreConfig};
pub use dduroc_engine::writer::QueueSizes;
pub use dduroc_engine::{Clock, Error, Result};
pub use dduroc_format::{
    EventId, Level, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId, ValueType,
};

/// Всё, что нужно для записи: типы и трейты одной строкой.
pub mod prelude {
    pub use crate::{
        Event, Level, NamespaceExt, OwnedValue, SpanExt, Store, StoreConfig, SyncSource,
    };
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

/// Расширение [`Namespace`] типизированной записью.
pub trait NamespaceExt {
    /// Записать событие: поля сериализуются в компактный бинарный вид.
    fn log<E: Event>(&self, event: E) -> Result<()>;

    /// Записать событие, привязав его к спану.
    fn log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()>;
}

impl NamespaceExt for Namespace {
    fn log<E: Event>(&self, event: E) -> Result<()> {
        self.log_payload(E::ID, encode(&event)?, None)
    }

    fn log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()> {
        self.log_payload(E::ID, encode(&event)?, Some(span.id()))
    }
}

/// Расширение [`SpanGuard`] типизированной записью.
pub trait SpanExt {
    /// Записать событие внутри спана.
    fn log<E: Event>(&self, event: E) -> Result<()>;
}

impl SpanExt for SpanGuard {
    fn log<E: Event>(&self, event: E) -> Result<()> {
        self.log_payload(E::ID, encode(&event)?)
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
    postcard::to_extend(event, Payload::new()).map_err(|_| Error::BadNamespace {
        name: String::new(),
        reason: "поля события не сериализуются",
    })
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
            Temp = 0x01 { vtype: f32, unit: "°C", tags: [sensor] },
            Spectrum = 0x02 { vtype: blob, store: telemetry },
        }

        spans {
            Calibration = 0x01,
            PowerRamp = 0x02,
        }
    }

    #[test]
    fn generated_schema_is_valid() {
        testing::SCHEMA.validate().expect("схема корректна");
        assert_eq!(testing::SCHEMA.name, "testing");
        assert_eq!(testing::SCHEMA.version, ProtocolVersion(1));
        assert_eq!(testing::SCHEMA.events.len(), 3);
        assert_eq!(testing::SCHEMA.metrics.len(), 2);
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
            let ns = store
                .namespace("orc-radio-0", testing::SCHEMA, &config)
                .unwrap();

            ns.log(testing::events::PowerSet { dbm: 27.5 }).unwrap();
            ns.log(testing::events::Overheat { t: 91.0, sensor: 1 })
                .unwrap();

            let temp = ns
                .series(testing::metrics::Temp, &[("sensor", "pa")])
                .unwrap();
            temp.sample_f32(36.6).unwrap();

            {
                let cal = ns.span(testing::spans::Calibration).unwrap();
                cal.log(testing::events::PowerSet { dbm: 30.0 }).unwrap();
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
            let ns = store
                .namespace("orc-radio-0", testing::SCHEMA, &config)
                .unwrap();

            let mut refused = 0u64;
            for _ in 0..20_000 {
                if ns.log(testing::events::PowerSet { dbm: 27.5 }).is_err() {
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
