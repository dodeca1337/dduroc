//! Запись в пути от вызывающего потока к writer'у.
//!
//! Через очередь нельзя передать [`dduroc_format::Record`] — он заимствует
//! байты. Поэтому запись «стажируется»: payload копируется в
//! [`SmallVec`][smallvec::SmallVec] с inline-ёмкостью, которой хватает
//! типичному событию, и обращения к куче на горячем пути нет.

use dduroc_format::{
    EventId, Level, MetricId, Micros, Record, SpanId, SpanKindId, Value, ValueType,
};
use smallvec::SmallVec;
use std::sync::Arc;

/// Inline-ёмкость payload'а. Событие с парой чисел укладывается целиком;
/// более крупные уходят в кучу.
pub const INLINE_PAYLOAD: usize = 32;

pub type Payload = SmallVec<[u8; INLINE_PAYLOAD]>;

/// Идентификатор неймспейса внутри процесса.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NsId(pub u32);

/// Индекс канала внутри неймспейса.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelIdx(pub u16);

/// Значение сэмпла во владеющей форме.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedValue {
    F32(f32),
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Blob(Payload),
}

impl OwnedValue {
    pub fn value_type(&self) -> ValueType {
        match self {
            OwnedValue::F32(_) => ValueType::F32,
            OwnedValue::F64(_) => ValueType::F64,
            OwnedValue::I64(_) => ValueType::I64,
            OwnedValue::U64(_) => ValueType::U64,
            OwnedValue::Bool(_) => ValueType::Bool,
            OwnedValue::Blob(_) => ValueType::Blob,
        }
    }

    pub fn as_value(&self) -> Value<'_> {
        match self {
            OwnedValue::F32(v) => Value::F32(*v),
            OwnedValue::F64(v) => Value::F64(*v),
            OwnedValue::I64(v) => Value::I64(*v),
            OwnedValue::U64(v) => Value::U64(*v),
            OwnedValue::Bool(v) => Value::Bool(*v),
            OwnedValue::Blob(b) => Value::Blob(b),
        }
    }
}

/// Тело записи во владеющей форме.
#[derive(Debug, Clone, PartialEq)]
pub enum StagedRecord {
    Message {
        event: EventId,
        span: Option<SpanId>,
        payload: Payload,
    },
    SpanStart {
        span: SpanId,
        kind: SpanKindId,
        parent: Option<SpanId>,
    },
    SpanEnd {
        span: SpanId,
    },
    Sample {
        metric: MetricId,
        value: OwnedValue,
    },
    Text {
        level: Level,
        span: Option<SpanId>,
        /// Источник сообщения. `Arc`, потому что у моста из `tracing` он
        /// один и тот же на миллионы записей.
        target: Arc<str>,
        text: Box<str>,
    },
}

/// Запись вместе с адресом назначения.
#[derive(Debug, Clone, PartialEq)]
pub struct Staged {
    pub ns: NsId,
    pub channel: ChannelIdx,
    pub at: Micros,
    pub record: StagedRecord,
}

impl Staged {
    /// Идентификатор типа для учёта в footer'е.
    ///
    /// Множества встреченных типов нужны миграции (сегмент без затронутых
    /// типов не переписывается) и читателю — узнать, что вообще есть в
    /// сегменте, не читая блоки.
    pub fn footer_ids(&self) -> (Option<EventId>, Option<MetricId>) {
        match self.record {
            StagedRecord::Message { event, .. } => (Some(event), None),
            StagedRecord::Sample { metric, .. } => (None, Some(metric)),
            _ => (None, None),
        }
    }
}

impl StagedRecord {
    /// Одолжить как [`Record`] формата.
    ///
    /// Разрешать нечего: сэмпл несёт метрику, а метрика — это и есть
    /// идентичность ряда.
    pub fn as_record(&self) -> Record<'_> {
        match self {
            StagedRecord::Message {
                event,
                span,
                payload,
            } => Record::Message(dduroc_format::Message {
                event: *event,
                span: *span,
                payload,
            }),
            StagedRecord::SpanStart { span, kind, parent } => {
                Record::SpanStart(dduroc_format::SpanStart {
                    span: *span,
                    kind: *kind,
                    parent: *parent,
                })
            }
            StagedRecord::SpanEnd { span } => Record::SpanEnd { span: *span },
            StagedRecord::Sample { metric, value } => Record::Sample(dduroc_format::Sample {
                metric: *metric,
                value: value.as_value(),
            }),
            StagedRecord::Text {
                level,
                span,
                target,
                text,
            } => Record::Text(dduroc_format::Text {
                level: *level,
                span: *span,
                target,
                text,
            }),
        }
    }
}

/// Счётчики потерь по каналам неймспейса.
///
/// Учёт потерь лежит **на горячем пути ровно тогда, когда система под
/// давлением** — в худший из возможных моментов. Поэтому это массив атомиков,
/// индексируемый номером канала, а не разделяемая таблица под мьютексом:
/// последняя добавляла бы к каждой потерянной записи захват блокировки и
/// работу с хеш-таблицей.
#[derive(Debug)]
pub struct DropCounters {
    per_channel: Vec<std::sync::atomic::AtomicU64>,
}

impl DropCounters {
    pub fn new(channels: usize) -> Self {
        Self {
            per_channel: (0..channels)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect(),
        }
    }

    /// Отметить потерю в канале.
    #[inline]
    pub fn record(&self, channel: ChannelIdx) {
        if let Some(c) = self.per_channel.get(channel.0 as usize) {
            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Забрать накопленное, обнулив счётчик.
    pub fn take(&self, channel: ChannelIdx) -> u64 {
        self.per_channel
            .get(channel.0 as usize)
            .map_or(0, |c| c.swap(0, std::sync::atomic::Ordering::Relaxed))
    }

    pub fn channels(&self) -> usize {
        self.per_channel.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typical_payload_stays_inline() {
        // Событие с пятью f32 — 20 байт postcard: обращения к куче быть не
        // должно, иначе горячий путь логирования аллоцирует на каждый вызов.
        let payload: Payload = smallvec::smallvec![0u8; 20];
        assert!(!payload.spilled(), "типичный payload обязан быть inline");

        let big: Payload = smallvec::smallvec![0u8; INLINE_PAYLOAD + 1];
        assert!(big.spilled(), "крупный payload уходит в кучу");
    }

    #[test]
    fn owned_value_roundtrips_through_format() {
        for v in [
            OwnedValue::F32(1.5),
            OwnedValue::F64(-2.5),
            OwnedValue::I64(-7),
            OwnedValue::U64(9),
            OwnedValue::Bool(true),
            OwnedValue::Blob(smallvec::smallvec![1, 2, 3]),
        ] {
            assert_eq!(v.as_value().value_type(), v.value_type());
        }
    }

    #[test]
    fn sample_needs_nothing_resolved() {
        // Метрика — это и есть идентичность ряда, поэтому запись собирается
        // без обращения к какому-либо реестру. Раньше здесь требовался
        // сегментно-локальный номер серии, известный только writer'у.
        let rec = StagedRecord::Sample {
            metric: MetricId(7),
            value: OwnedValue::F32(1.0),
        };
        match rec.as_record() {
            Record::Sample(s) => {
                assert_eq!(s.metric, MetricId(7));
                assert_eq!(s.value, Value::F32(1.0));
            }
            other => panic!("ожидался сэмпл: {other:?}"),
        }
        assert_eq!(
            Staged {
                ns: NsId(0),
                channel: ChannelIdx(0),
                at: Micros(0),
                record: rec,
            }
            .footer_ids(),
            (None, Some(MetricId(7))),
            "метрика обязана попасть в множество footer'а"
        );
    }

    #[test]
    fn message_borrows_payload_without_copy() {
        let rec = StagedRecord::Message {
            event: EventId(1),
            span: None,
            payload: smallvec::smallvec![9, 8, 7],
        };
        match rec.as_record() {
            Record::Message(m) => assert_eq!(m.payload, &[9, 8, 7]),
            other => panic!("ожидалось сообщение: {other:?}"),
        }
    }
}
