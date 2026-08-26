//! A record on its way from the calling thread to the writer.
//!
//! A [`dduroc_format::Record`] cannot travel through a queue — it borrows its
//! bytes. So a record is "staged": the payload is copied into a
//! [`smallvec::SmallVec`] whose inline capacity is enough for a typical event,
//! and the hot path never touches the heap.

use dduroc_format::{
    EventId, Level, MetricId, Micros, Record, SpanId, SpanKindId, Value, ValueType,
};
use smallvec::SmallVec;
use std::sync::Arc;

/// The payload's inline capacity. An event with a couple of numbers fits
/// entirely; larger ones go to the heap.
pub const INLINE_PAYLOAD: usize = 32;

pub type Payload = SmallVec<[u8; INLINE_PAYLOAD]>;

/// A namespace identifier within the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NsId(pub u32);

/// A channel index within a namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ChannelIdx(pub u16);

/// A sample value in owning form.
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

/// A record body in owning form.
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
        /// The message's source. An `Arc`, because the bridge from `tracing`
        /// has one and the same for millions of records.
        target: Arc<str>,
        text: Box<str>,
    },
}

/// A record together with its destination address.
#[derive(Debug, Clone, PartialEq)]
pub struct Staged {
    pub ns: NsId,
    pub channel: ChannelIdx,
    pub at: Micros,
    pub record: StagedRecord,
}

impl Staged {
    /// The type identifier, for accounting in the footer.
    ///
    /// The sets of types seen are what migrations need (a segment holding none
    /// of the affected types is not rewritten) and what a reader needs to learn
    /// what is in a segment at all without reading its blocks.
    pub fn footer_ids(&self) -> (Option<EventId>, Option<MetricId>) {
        match self.record {
            StagedRecord::Message { event, .. } => (Some(event), None),
            StagedRecord::Sample { metric, .. } => (None, Some(metric)),
            _ => (None, None),
        }
    }
}

impl StagedRecord {
    /// Borrow as a format [`Record`].
    ///
    /// There is nothing to resolve: a sample carries its metric, and the metric
    /// is what identifies the series.
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

/// Per-channel loss counters of a namespace.
///
/// Loss accounting sits **on the hot path at exactly the moment the system is
/// under pressure** — the worst moment there is. So this is an array of
/// atomics indexed by channel number rather than a shared table under a mutex:
/// the latter would add a lock acquisition and hash-table work to every record
/// lost.
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

    /// Note a loss in a channel.
    #[inline]
    pub fn record(&self, channel: ChannelIdx) {
        self.record_n(channel, 1);
    }

    /// Note several losses at once: a block is lost whole, and announcing one
    /// record instead of a hundred would lie about the size of the hole.
    #[inline]
    pub fn record_n(&self, channel: ChannelIdx, n: u64) {
        if let Some(c) = self.per_channel.get(channel.0 as usize) {
            c.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Take what has accumulated, zeroing the counter.
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
        // An event with five f32s is 20 postcard bytes: there must be no trip
        // to the heap, or the hot logging path allocates on every call.
        let payload: Payload = smallvec::smallvec![0u8; 20];
        assert!(!payload.spilled(), "a typical payload must stay inline");

        let big: Payload = smallvec::smallvec![0u8; INLINE_PAYLOAD + 1];
        assert!(big.spilled(), "a large payload goes to the heap");
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
        // The metric is what identifies the series, so a record is assembled
        // without consulting any registry. This used to require a segment-local
        // series number known only to the writer.
        let rec = StagedRecord::Sample {
            metric: MetricId(7),
            value: OwnedValue::F32(1.0),
        };
        match rec.as_record() {
            Record::Sample(s) => {
                assert_eq!(s.metric, MetricId(7));
                assert_eq!(s.value, Value::F32(1.0));
            }
            other => panic!("expected a sample: {other:?}"),
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
            "the metric must reach the footer set"
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
            other => panic!("expected a message: {other:?}"),
        }
    }
}
