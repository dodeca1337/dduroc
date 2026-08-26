//! Records inside a block.
//!
//! The frame: `[b0: kind(4 bits) | flags(4 bits)] [Δt varint] [fields by
//! kind]`.
//!
//! `Δt` is microseconds since the **previous record in the block** (for the
//! first one, since the block header's `base_micros`, so usually 0). A delta
//! from the neighbour rather than from the base: smaller numbers, shorter
//! varints.
//!
//! Only what varies reaches the disk. The level, text templates, message tags,
//! names and units are static properties of the type; they live in the
//! binary's schema and are resolved at read time. Belonging to a namespace and
//! a channel is implicit in the file path, and `boot_counter` comes from the
//! segment header.

use crate::cursor::{Cursor, write_str};
use crate::error::{Error, Result};
use crate::ids::{EventId, MetricId, SpanId, SpanKindId};
use crate::level::Level;
use crate::value::{Value, ValueType};
use crate::varint;

/// The record kind — the high nibble of the first byte.
///
/// An enum rather than bare constants: a non-existent kind is unrepresentable
/// and a `match` on the kind has to be exhaustive. The number behind a variant
/// is the on-disk format, pinned by the discriminant precisely so that the
/// name and the wire have a single source.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    Message = 0x0,
    SpanStart = 0x1,
    SpanEnd = 0x2,
    Sample = 0x4,
    Text = 0x5,
    /// An extension: `len varint` plus bytes. The only way to add a new kind
    /// of record without breaking old readers — they can skip it by length.
    Ext = 0xF,
}

/// Taken in container version 1 by the `SeriesDef` record, which defined a
/// series as `(metric, runtime tags)`. Runtime tags are gone, a series is
/// identified by its metric, and a sample carries `metric_id` directly. The
/// code is not reused: that costs nothing and makes the two versions
/// impossible to confuse when parsing. [`RecordKind`] has no variant for it —
/// this is NOT a record kind of the current format but a distinguishable
/// diagnosis in [`RecordKind::from_u8`].
const RETIRED_SERIES_DEF: u8 = 0x3;

impl RecordKind {
    /// The kind from the nibble — the inverse of the discriminant.
    ///
    /// The code of the earlier version yields a distinguishable diagnosis:
    /// "unknown kind" would send someone hunting for medium corruption where a
    /// version 1 segment is in fact being read.
    pub const fn from_u8(raw: u8) -> Result<Self> {
        match raw {
            0x0 => Ok(RecordKind::Message),
            0x1 => Ok(RecordKind::SpanStart),
            0x2 => Ok(RecordKind::SpanEnd),
            0x4 => Ok(RecordKind::Sample),
            0x5 => Ok(RecordKind::Text),
            0xF => Ok(RecordKind::Ext),
            RETIRED_SERIES_DEF => Err(Error::RetiredRecordKind(RETIRED_SERIES_DEF)),
            other => Err(Error::UnknownRecordKind(other)),
        }
    }
}

/// Flag marking the presence of `span` in a record (messages and text).
const FLAG_SPAN: u8 = 0b0001;
/// Mask of the value type in the flags of a `Sample` record.
const SAMPLE_VTYPE_MASK: u8 = 0b0111;

// ════════════════════════════════════════════════════════════════════════════
// Records
// ════════════════════════════════════════════════════════════════════════════

/// A schema message: a type plus binary fields (postcard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Message<'a> {
    pub event: EventId,
    /// The span the message is attached to (from the runtime context).
    pub span: Option<SpanId>,
    /// The serialized fields of the event. The length is stored explicitly even
    /// though postcard is self-describing given the schema: without the schema,
    /// records of unknown types could not be skipped (a foreign build, or a
    /// state from before a migration).
    pub payload: &'a [u8],
}

/// The start of a span.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanStart {
    pub span: SpanId,
    pub kind: SpanKindId,
    /// The parent; `None` means a root span.
    pub parent: Option<SpanId>,
}

/// A telemetry sample.
///
/// A series is identified by its metric and by nothing else: there are no
/// runtime dimensions in the system, and the name, the unit, the state labels
/// and the limits are schema statics. So a sample carries `metric_id`
/// directly, with no intermediate interning and no series-definition record:
/// one varint instead of a whole mechanism that on top of everything had to be
/// reconstructed when reading from the middle or in reverse.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample<'a> {
    pub metric: MetricId,
    pub value: Value<'a>,
}

/// Free text without a schema: the bridge from `tracing`/`log`, a panic
/// handler. The level is stored in the record — there is no schema to resolve
/// it from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Text<'a> {
    pub level: Level,
    pub span: Option<SpanId>,
    /// The source: a tracing target, a module name and the like.
    pub target: &'a str,
    pub text: &'a str,
}

/// One record of a block.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Record<'a> {
    Message(Message<'a>),
    SpanStart(SpanStart),
    SpanEnd {
        span: SpanId,
    },
    Sample(Sample<'a>),
    Text(Text<'a>),
    /// An unrecognized extension: kept whole, skipped by length.
    Ext {
        bytes: &'a [u8],
    },
}

/// A record together with its time delta.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Framed<'a> {
    /// Microseconds since the previous record in the block.
    pub dt: u64,
    pub record: Record<'a>,
}

impl Record<'_> {
    /// The record kind.
    pub const fn kind(&self) -> RecordKind {
        match self {
            Record::Message(_) => RecordKind::Message,
            Record::SpanStart(_) => RecordKind::SpanStart,
            Record::SpanEnd { .. } => RecordKind::SpanEnd,
            Record::Sample(_) => RecordKind::Sample,
            Record::Text(_) => RecordKind::Text,
            Record::Ext { .. } => RecordKind::Ext,
        }
    }

    /// The record's span, if it is attached to one.
    pub const fn span(&self) -> Option<SpanId> {
        match self {
            Record::Message(m) => m.span,
            Record::Text(t) => t.span,
            Record::SpanStart(s) => Some(s.span),
            Record::SpanEnd { span } => Some(*span),
            _ => None,
        }
    }
}

/// Write a span identifier, rejecting the reserved zero.
///
/// Zero means "no span" and is not a valid [`SpanId`]. It is checked on
/// **encoding**, not only on parsing: the decoder rejects such a byte, and a
/// codec has no right to produce what it will not read back. Letting a zero
/// through silently would give the writer a block that the reader loses
/// entirely at the first such record — not one record, but the whole rest of
/// the body.
#[inline]
fn write_span(out: &mut Vec<u8>, span: SpanId) -> Result<()> {
    if span.0 == SpanId::NONE_RAW {
        return Err(Error::ReservedValue);
    }
    varint::write_u64(out, u64::from(span.0));
    Ok(())
}

/// Encode a record with delta `dt`. Returns the number of bytes appended.
pub fn encode(record: &Record<'_>, dt: u64, out: &mut Vec<u8>) -> Result<usize> {
    let start = out.len();

    let flags = match record {
        Record::Message(m) if m.span.is_some() => FLAG_SPAN,
        Record::Text(t) if t.span.is_some() => FLAG_SPAN,
        Record::Sample(s) => s.value.value_type() as u8,
        _ => 0,
    };
    out.push(((record.kind() as u8) << 4) | flags);
    varint::write_u64(out, dt);

    // The checks live inside the parsing rather than in a separate pass before
    // it: a sample is the most frequent record there is, and an extra variant
    // dispatch on it shows up in measurements. The price is rewinding the
    // buffer on failure, but failure here means a defect in the caller and
    // happens zero times in a process's life.
    if let Err(e) = encode_body(record, out) {
        out.truncate(start);
        return Err(e);
    }
    Ok(out.len() - start)
}

fn encode_body(record: &Record<'_>, out: &mut Vec<u8>) -> Result<()> {
    match record {
        Record::Message(m) => {
            varint::write_u64(out, u64::from(m.event.0));
            if let Some(span) = m.span {
                write_span(out, span)?;
            }
            varint::write_u64(out, m.payload.len() as u64);
            out.extend_from_slice(m.payload);
        }
        Record::SpanStart(s) => {
            write_span(out, s.span)?;
            varint::write_u64(out, u64::from(s.kind.0));
            // A `None` parent is encoded as zero by design; an explicit
            // `Some(0)` is the same reserved value under another name.
            match s.parent {
                Some(parent) => write_span(out, parent)?,
                None => {
                    varint::write_u64(out, u64::from(SpanId::NONE_RAW));
                }
            }
        }
        Record::SpanEnd { span } => write_span(out, *span)?,
        Record::Sample(s) => {
            varint::write_u64(out, u64::from(s.metric.0));
            s.value.encode(out);
        }
        Record::Text(t) => {
            out.push(t.level as u8);
            if let Some(span) = t.span {
                write_span(out, span)?;
            }
            write_str(out, t.target);
            write_str(out, t.text);
        }
        Record::Ext { bytes } => {
            varint::write_u64(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
        }
    }
    Ok(())
}

/// Decode one record from the start of `input`. Returns the record and the
/// number of bytes consumed.
pub fn decode(input: &[u8]) -> Result<(Framed<'_>, usize)> {
    let mut c = Cursor::new(input);
    let b0 = c.u8()?;
    let (kind, flags) = (b0 >> 4, b0 & 0x0F);
    let dt = c.varint()?;

    let record = match RecordKind::from_u8(kind)? {
        RecordKind::Message => {
            let event = EventId(c.varint_u16("event_id")?);
            let span = read_flagged_span(&mut c, flags)?;
            let len = c.varint_len("payload_len")?;
            Record::Message(Message {
                event,
                span,
                payload: c.take(len)?,
            })
        }
        RecordKind::SpanStart => {
            reject_flags(flags)?;
            let span = read_span(&mut c)?;
            let span_kind = SpanKindId(c.varint_u16("span_kind_id")?);
            let parent = SpanId::from_raw(c.varint_u32("parent")?);
            Record::SpanStart(SpanStart {
                span,
                kind: span_kind,
                parent,
            })
        }
        RecordKind::SpanEnd => {
            reject_flags(flags)?;
            Record::SpanEnd {
                span: read_span(&mut c)?,
            }
        }
        RecordKind::Sample => {
            // The value type is duplicated in the flags: without it there is
            // nowhere to get the value's length from — a sample, unlike a
            // message or an extension, has no length prefix.
            let ty = ValueType::from_u8(flags & SAMPLE_VTYPE_MASK)?;
            if flags & !SAMPLE_VTYPE_MASK != 0 {
                return Err(Error::ReservedValue);
            }
            let metric = MetricId(c.varint_u16("metric_id")?);
            Record::Sample(Sample {
                metric,
                value: c.value(ty)?,
            })
        }
        RecordKind::Text => {
            let level = Level::from_u8(c.u8()?)?;
            let span = read_flagged_span(&mut c, flags)?;
            let target = c.str("target")?;
            let text = c.str("text")?;
            Record::Text(Text {
                level,
                span,
                target,
                text,
            })
        }
        RecordKind::Ext => {
            reject_flags(flags)?;
            let len = c.varint_len("ext_len")?;
            Record::Ext {
                bytes: c.take(len)?,
            }
        }
    };

    Ok((Framed { dt, record }, c.pos()))
}

/// An iterator over a block's records. It stops at the end of the data and
/// returns an error as an item, leaving the caller to decide whether to trim
/// the tail or call the block corrupt.
pub fn iter(body: &[u8]) -> RecordIter<'_> {
    RecordIter { body, pos: 0 }
}

#[derive(Debug)]
pub struct RecordIter<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> RecordIter<'a> {
    /// Offset of the next unread record within the block body.
    pub fn offset(&self) -> usize {
        self.pos
    }
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = Result<Framed<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.body.len() {
            return None;
        }
        match decode(&self.body[self.pos..]) {
            Ok((framed, n)) => {
                self.pos += n;
                Some(Ok(framed))
            }
            Err(e) => {
                // Stop here, or the iterator would spin forever on the error.
                self.pos = self.body.len();
                Some(Err(e))
            }
        }
    }
}

#[inline]
fn read_span(c: &mut Cursor<'_>) -> Result<SpanId> {
    let raw = c.varint_u32("span")?;
    SpanId::from_raw(raw).ok_or(Error::ReservedValue)
}

#[inline]
fn read_flagged_span(c: &mut Cursor<'_>, flags: u8) -> Result<Option<SpanId>> {
    if flags & FLAG_SPAN == 0 {
        // The remaining bits are not defined yet for these kinds.
        if flags != 0 {
            return Err(Error::ReservedValue);
        }
        return Ok(None);
    }
    if flags & !FLAG_SPAN != 0 {
        return Err(Error::ReservedValue);
    }
    Ok(Some(read_span(c)?))
}

#[inline]
fn reject_flags(flags: u8) -> Result<()> {
    if flags != 0 {
        return Err(Error::ReservedValue);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(rec: Record<'_>, dt: u64) -> usize {
        let mut buf = Vec::new();
        let written = encode(&rec, dt, &mut buf).expect("encoding");
        assert_eq!(written, buf.len());
        let (framed, read) = decode(&buf).expect("decoding");
        assert_eq!(read, buf.len(), "not everything was consumed");
        assert_eq!(framed.dt, dt);
        assert_eq!(framed.record, rec);
        buf.len()
    }

    #[test]
    fn message_sizes() {
        // The size claimed in SPEC.md: kind+flags(1) + dt(1) + event(1) +
        // payload_len(1) + payload(4) = 8 bytes.
        let size = roundtrip(
            Record::Message(Message {
                event: EventId(1),
                span: None,
                payload: &[0xAA; 4],
            }),
            0,
        );
        assert_eq!(size, 8, "a typical message must take 8 bytes");

        // With a span, plus a varint span_id.
        let size = roundtrip(
            Record::Message(Message {
                event: EventId(1),
                span: Some(SpanId(5)),
                payload: &[0xAA; 4],
            }),
            0,
        );
        assert_eq!(size, 9);
    }

    #[test]
    fn sample_sizes() {
        // f32: kind+flags(1) + dt(1) + metric_id(1) + value(4) = 7 bytes.
        let size = roundtrip(
            Record::Sample(Sample {
                metric: MetricId(1),
                value: Value::F32(36.6),
            }),
            0,
        );
        assert_eq!(size, 7, "an f32 sample is 7 bytes");

        // A small u64: 1 + 1 + 1 + 1 = 4 bytes. An enum metric's state code
        // takes the same: on disk it is an ordinary u64.
        let size = roundtrip(
            Record::Sample(Sample {
                metric: MetricId(3),
                value: Value::U64(42),
            }),
            0,
        );
        assert_eq!(size, 4, "a small u64 sample is 4 bytes");
    }

    #[test]
    fn dropping_series_interning_did_not_cost_bytes() {
        // A sample used to refer to a segment-local series number counted from
        // zero, and a separate SeriesDef record tied it to a metric. Now the
        // metric sits in the sample itself. The size has to stay the same for
        // metrics that fit in one varint byte, that is for every id below 128 —
        // and the schema budget is 150 metrics.
        for id in [0u16, 1, 42, 127] {
            let size = roundtrip(
                Record::Sample(Sample {
                    metric: MetricId(id),
                    value: Value::F32(1.0),
                }),
                0,
            );
            assert_eq!(size, 7, "metric_id {id} must cost one byte");
        }
        // Only ids from 128 up cost a second byte, and that is the price of
        // losing the series-definition record, which cost tens of bytes per
        // segment.
        let size = roundtrip(
            Record::Sample(Sample {
                metric: MetricId(128),
                value: Value::F32(1.0),
            }),
            0,
        );
        assert_eq!(size, 8);
    }

    #[test]
    fn retired_series_def_kind_is_not_decodable() {
        // Code 0x3 was taken by the SeriesDef record of container version 1. It
        // is not reused, and it can only turn up in a file of a foreign version
        // — a reader has to refuse honestly rather than parse the bytes as
        // something else.
        let mut buf = vec![RETIRED_SERIES_DEF << 4];
        varint::write_u64(&mut buf, 0);
        assert_eq!(
            decode(&buf),
            Err(Error::RetiredRecordKind(RETIRED_SERIES_DEF)),
            "the diagnosis must point at the format version, not at corruption"
        );
    }

    #[test]
    fn all_kinds_roundtrip() {
        roundtrip(
            Record::Message(Message {
                event: EventId(0xFFFF),
                span: Some(SpanId(u32::MAX)),
                payload: &[],
            }),
            u64::MAX,
        );
        roundtrip(
            Record::SpanStart(SpanStart {
                span: SpanId(1),
                kind: SpanKindId(7),
                parent: None,
            }),
            10,
        );
        roundtrip(
            Record::SpanStart(SpanStart {
                span: SpanId(2),
                kind: SpanKindId(7),
                parent: Some(SpanId(1)),
            }),
            10,
        );
        roundtrip(Record::SpanEnd { span: SpanId(9) }, 1_000_000);
        roundtrip(
            Record::Sample(Sample {
                metric: MetricId(1),
                value: Value::F32(36.6),
            }),
            0,
        );
        roundtrip(
            Record::Sample(Sample {
                metric: MetricId(u16::MAX),
                value: Value::Blob(&[1, 2, 3]),
            }),
            5,
        );
        roundtrip(
            Record::Text(Text {
                level: Level::Warn,
                span: None,
                target: "fjall::journal",
                text: "recovering",
            }),
            42,
        );
        roundtrip(
            Record::Text(Text {
                level: Level::Error,
                span: Some(SpanId(3)),
                target: "panic",
                text: "panic in a thread",
            }),
            0,
        );
        roundtrip(Record::Ext { bytes: &[9, 8, 7] }, 0);
    }

    #[test]
    fn sample_carries_no_dimensions_beyond_the_metric() {
        // A check of the model's key property: on disk a sample holds nothing
        // but a time, a metric and a value. Any dimension one might want to add
        // at runtime has to become a metric of the schema — otherwise it starts
        // taking room in every single sample.
        let mut buf = Vec::new();
        encode(
            &Record::Sample(Sample {
                metric: MetricId(0x2a),
                value: Value::U64(2),
            }),
            0,
            &mut buf,
        )
        .unwrap();
        assert_eq!(
            buf,
            vec![
                ((RecordKind::Sample as u8) << 4) | ValueType::U64 as u8,
                0,    // dt
                0x2a, // metric_id
                2,    // the value
            ]
        );
    }

    #[test]
    fn iterates_multiple_records() {
        let mut body = Vec::new();
        for i in 0..5u16 {
            encode(
                &Record::Message(Message {
                    event: EventId(i),
                    span: None,
                    payload: &[i as u8],
                }),
                u64::from(i) * 100,
                &mut body,
            )
            .unwrap();
        }

        let got: Vec<_> = iter(&body).map(|r| r.unwrap()).collect();
        assert_eq!(got.len(), 5);
        for (i, f) in got.iter().enumerate() {
            assert_eq!(f.dt, i as u64 * 100);
            match f.record {
                Record::Message(m) => assert_eq!(m.event, EventId(i as u16)),
                ref other => panic!("expected a message, got {other:?}"),
            }
        }
    }

    #[test]
    fn iterator_stops_on_error() {
        let mut body = Vec::new();
        encode(
            &Record::Message(Message {
                event: EventId(1),
                span: None,
                payload: &[1, 2],
            }),
            0,
            &mut body,
        )
        .unwrap();
        // A truncated second record — as after a power loss.
        body.push((RecordKind::Message as u8) << 4);

        let results: Vec<_> = iter(&body).collect();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        let err = results[1].as_ref().unwrap_err();
        assert!(err.is_torn_tail(), "a torn tail: {err}");
    }

    #[test]
    fn zero_span_id_rejected() {
        // span_id = 0 with the flag set: the reserved value.
        let mut buf = vec![((RecordKind::Message as u8) << 4) | FLAG_SPAN];
        varint::write_u64(&mut buf, 0); // dt
        varint::write_u64(&mut buf, 1); // event
        varint::write_u64(&mut buf, 0); // span = 0 is not allowed
        varint::write_u64(&mut buf, 0); // payload_len
        assert_eq!(decode(&buf), Err(Error::ReservedValue));
    }

    #[test]
    fn encoder_refuses_to_produce_what_it_cannot_read() {
        // The codec has to be symmetric: `SpanId(0)` is the reserved value and
        // the decoder rejects it. Letting a zero through on write would put a
        // record in the block that the reader stops at — losing the whole rest
        // of the block, not one record.
        let mut buf = Vec::new();
        for rec in [
            Record::Message(Message {
                event: EventId(1),
                span: Some(SpanId(0)),
                payload: &[],
            }),
            Record::Text(Text {
                level: Level::Info,
                span: Some(SpanId(0)),
                target: "t",
                text: "x",
            }),
            Record::SpanStart(SpanStart {
                span: SpanId(0),
                kind: SpanKindId(1),
                parent: None,
            }),
            Record::SpanStart(SpanStart {
                span: SpanId(1),
                kind: SpanKindId(1),
                parent: Some(SpanId(0)),
            }),
            Record::SpanEnd { span: SpanId(0) },
        ] {
            assert_eq!(encode(&rec, 0, &mut buf), Err(Error::ReservedValue));
            assert!(
                buf.is_empty(),
                "a refusal must not leave a fragment behind: {buf:?}"
            );
        }

        // A root span is still encoded as zero in the `parent` field.
        let n = encode(
            &Record::SpanStart(SpanStart {
                span: SpanId(1),
                kind: SpanKindId(1),
                parent: None,
            }),
            0,
            &mut buf,
        )
        .expect("a root span is legal");
        assert_eq!(n, buf.len());
    }

    #[test]
    fn unknown_kind_and_reserved_flags_rejected() {
        // Kind 0x6 is undefined — a reader has to report an error rather than
        // guess the length.
        let mut buf = vec![0x6 << 4];
        varint::write_u64(&mut buf, 0);
        assert_eq!(decode(&buf), Err(Error::UnknownRecordKind(0x6)));

        // Non-zero reserved flags on a SpanEnd.
        let mut buf = vec![((RecordKind::SpanEnd as u8) << 4) | 0b0010];
        varint::write_u64(&mut buf, 0);
        varint::write_u64(&mut buf, 1);
        assert_eq!(decode(&buf), Err(Error::ReservedValue));

        // An unknown vtype in a sample's flags.
        let mut buf = vec![((RecordKind::Sample as u8) << 4) | 0b0110];
        varint::write_u64(&mut buf, 0);
        varint::write_u64(&mut buf, 0);
        assert_eq!(decode(&buf), Err(Error::UnknownValueType(6)));
    }

    #[test]
    fn span_accessor() {
        assert_eq!(
            Record::Message(Message {
                event: EventId(0),
                span: Some(SpanId(4)),
                payload: &[]
            })
            .span(),
            Some(SpanId(4))
        );
        assert_eq!(
            Record::Sample(Sample {
                metric: MetricId(0),
                value: Value::Bool(true)
            })
            .span(),
            None
        );
    }
}
