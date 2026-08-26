//! Property tests for the format.
//!
//! Two classes of property are checked:
//! 1. **Roundtrip** — what was written is what is read, byte for byte in
//!    meaning.
//! 2. **Resilience to garbage** — the decoder either parses arbitrary bytes or
//!    refuses them honestly with an error, but never panics and never spins.
//!    That is critical: a segment after a power loss holds exactly such an
//!    arbitrary tail, and the reader has to survive it.

use dduroc_format::block::{Block, BlockBuilder, BlockHeader, Compression};
use dduroc_format::footer::{Footer, FooterBuilder, Trailer};
use dduroc_format::record::{self, Message, Record, Sample, SpanStart, Text};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{
    BootCounter, EventId, Level, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId, Value,
    varint,
};
use proptest::prelude::*;

// ════════════════════════════════════════════════════════════════════════════
// Strategies
// ════════════════════════════════════════════════════════════════════════════

fn any_span() -> impl Strategy<Value = Option<SpanId>> {
    prop_oneof![
        1 => Just(None),
        3 => (1u32..u32::MAX).prop_map(|v| Some(SpanId(v))),
    ]
}

fn any_value() -> impl Strategy<Value = Value<'static>> {
    prop_oneof![
        any::<f32>().prop_map(Value::F32),
        any::<f64>().prop_map(Value::F64),
        any::<i64>().prop_map(Value::I64),
        any::<u64>().prop_map(Value::U64),
        any::<bool>().prop_map(Value::Bool),
        // Blobs in property tests come from statics: Value borrows the bytes.
        Just(Value::Blob(&[])),
        Just(Value::Blob(&[0u8, 1, 2, 3, 255])),
    ]
}

/// An owning description of a record: `Record` borrows, so the strategies
/// generate the owning form and the test borrows out of it.
#[derive(Debug, Clone)]
enum OwnedRecord {
    Message {
        event: u16,
        span: Option<SpanId>,
        payload: Vec<u8>,
    },
    SpanStart {
        span: u32,
        kind: u16,
        parent: Option<SpanId>,
    },
    SpanEnd {
        span: u32,
    },
    Sample {
        metric: u16,
        value: Value<'static>,
    },
    Text {
        level: u8,
        span: Option<SpanId>,
        target: String,
        text: String,
    },
    Ext {
        bytes: Vec<u8>,
    },
}

impl OwnedRecord {
    fn as_record(&self) -> Record<'_> {
        match self {
            OwnedRecord::Message {
                event,
                span,
                payload,
            } => Record::Message(Message {
                event: EventId(*event),
                span: *span,
                payload,
            }),
            OwnedRecord::SpanStart { span, kind, parent } => Record::SpanStart(SpanStart {
                span: SpanId(*span),
                kind: SpanKindId(*kind),
                parent: *parent,
            }),
            OwnedRecord::SpanEnd { span } => Record::SpanEnd {
                span: SpanId(*span),
            },
            OwnedRecord::Sample { metric, value } => Record::Sample(Sample {
                metric: MetricId(*metric),
                value: *value,
            }),
            OwnedRecord::Text {
                level,
                span,
                target,
                text,
            } => Record::Text(Text {
                level: Level::from_u8(*level).unwrap(),
                span: *span,
                target,
                text,
            }),
            OwnedRecord::Ext { bytes } => Record::Ext { bytes },
        }
    }
}

fn any_record() -> impl Strategy<Value = OwnedRecord> {
    prop_oneof![
        (
            any::<u16>(),
            any_span(),
            prop::collection::vec(any::<u8>(), 0..64)
        )
            .prop_map(|(event, span, payload)| OwnedRecord::Message {
                event,
                span,
                payload
            }),
        (1u32..u32::MAX, any::<u16>(), any_span())
            .prop_map(|(span, kind, parent)| { OwnedRecord::SpanStart { span, kind, parent } }),
        (1u32..u32::MAX).prop_map(|span| OwnedRecord::SpanEnd { span }),
        (any::<u16>(), any_value())
            .prop_map(|(metric, value)| OwnedRecord::Sample { metric, value }),
        (0u8..6, any_span(), ".{0,20}", ".{0,40}").prop_map(|(level, span, target, text)| {
            OwnedRecord::Text {
                level,
                span,
                target,
                text,
            }
        }),
        prop::collection::vec(any::<u8>(), 0..32).prop_map(|bytes| OwnedRecord::Ext { bytes }),
    ]
}

/// f32/f64 values are compared bitwise: NaN != NaN would break the roundtrip.
fn records_eq(a: &Record<'_>, b: &Record<'_>) -> bool {
    match (a, b) {
        (Record::Sample(x), Record::Sample(y)) => {
            x.metric == y.metric
                && match (x.value, y.value) {
                    (Value::F32(p), Value::F32(q)) => p.to_bits() == q.to_bits(),
                    (Value::F64(p), Value::F64(q)) => p.to_bits() == q.to_bits(),
                    (p, q) => p == q,
                }
        }
        _ => a == b,
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Properties
// ════════════════════════════════════════════════════════════════════════════

proptest! {
    #[test]
    fn varint_roundtrips(v in any::<u64>()) {
        let mut buf = Vec::new();
        let n = varint::write_u64(&mut buf, v);
        prop_assert_eq!(n, varint::len_u64(v));
        let (got, read) = varint::read_u64(&buf)?;
        prop_assert_eq!(got, v);
        prop_assert_eq!(read, n);
    }

    #[test]
    fn varint_never_panics_on_garbage(bytes in prop::collection::vec(any::<u8>(), 0..24)) {
        // The result does not matter — what matters is that there is no panic
        // and the consumed length stays within the input.
        if let Ok((_, n)) = varint::read_u64(&bytes) {
            prop_assert!(n <= bytes.len());
        }
    }

    #[test]
    fn record_roundtrips(owned in any_record(), dt in any::<u64>()) {
        let rec = owned.as_record();

        let mut buf = Vec::new();
        let written = record::encode(&rec, dt, &mut buf).unwrap();
        prop_assert_eq!(written, buf.len());

        let (framed, read) = record::decode(&buf)?;
        prop_assert_eq!(read, buf.len(), "the decoder must consume exactly the record");
        prop_assert_eq!(framed.dt, dt);
        prop_assert!(records_eq(&framed.record, &rec), "{:?} != {:?}", framed.record, rec);
    }

    #[test]
    fn record_decode_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        if let Ok((_, n)) = record::decode(&bytes) {
            prop_assert!(n <= bytes.len());
        }
    }

    #[test]
    fn record_iter_terminates_on_garbage(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        // The iterator has to terminate on any input: spinning on a corrupt
        // tail would hang the reading of a segment after a power loss.
        let count = record::iter(&bytes).take(1024).count();
        prop_assert!(count <= 1024);
    }

    #[test]
    fn block_roundtrips(
        records in prop::collection::vec(any_record(), 1..40),
        times in prop::collection::vec(0u64..1_000_000_000, 1..40),
        compress in any::<bool>(),
    ) {
        let n = records.len().min(times.len());
        let mut times: Vec<u64> = times[..n].to_vec();
        times.sort_unstable(); // the writer feeds records in time order

        let mut builder = BlockBuilder::new();
        for (i, owned) in records[..n].iter().enumerate() {
            builder.push(Micros(times[i]), &owned.as_record()).unwrap();
        }

        let compression = if compress { Compression::Lz4 } else { Compression::None };
        let mut out = Vec::new();
        let header = builder.finish(0, compression, &mut out).unwrap();
        prop_assert_eq!(header.count as usize, n);
        prop_assert_eq!(out.len() as u64, header.total_len());

        let block = Block::parse(&out)?.expect("the block must be read");
        let decoded: Vec<_> = block.records().collect::<Result<Vec<_>, _>>()?;
        prop_assert_eq!(decoded.len(), n);
        for (i, (at, rec)) in decoded.iter().enumerate() {
            prop_assert_eq!(at.0, times[i], "the time of record {} was restored wrongly", i);
            prop_assert!(records_eq(rec, &records[i].as_record()));
        }
    }

    #[test]
    fn block_parse_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = Block::parse(&bytes);
        let _ = BlockHeader::parse(&bytes);
    }

    #[test]
    fn corrupting_any_byte_is_detected(
        payload in prop::collection::vec(any::<u8>(), 1..64),
        idx in any::<prop::sample::Index>(),
        xor in 1u8..=255,
    ) {
        // Any single corruption of a block has to be noticed — otherwise the
        // CRC is not doing its job.
        let mut builder = BlockBuilder::new();
        builder.push(Micros(1000), &Record::Message(Message {
            event: EventId(7),
            span: None,
            payload: &payload,
        })).unwrap();
        let mut out = Vec::new();
        builder.finish(0, Compression::None, &mut out).unwrap();

        let i = idx.index(out.len());
        out[i] ^= xor;

        match Block::parse(&out) {
            Err(_) => {}                    // the corruption was noticed
            Ok(None) => {}                  // body_len was wiped, so "end of data"
            Ok(Some(block)) => {
                // A surviving CRC with a changed byte is impossible; the only
                // legitimate case is corruption of reserved bytes outside the
                // CRC, and the block has none. So the content has to differ.
                let recs: Vec<_> = block.records().collect();
                let same = recs.len() == 1 && matches!(
                    recs.first(),
                    Some(Ok((_, Record::Message(m)))) if m.payload == payload.as_slice()
                );
                prop_assert!(!same, "corruption of byte {} went unnoticed", i);
            }
        }
    }

    #[test]
    fn segment_header_roundtrips(
        protocol in any::<u16>(),
        boot in any::<u32>(),
        base in any::<u64>(),
    ) {
        let h = SegmentHeader {
            protocol_version: ProtocolVersion(protocol),
            boot: BootCounter(boot),
            base: Micros(base),
            store_id: 0,
        };
        let bytes = h.to_bytes();
        prop_assert_eq!(SegmentHeader::parse(&bytes)?, h);

        // A file name always parses back.
        let name = h.file_name();
        prop_assert_eq!(SegmentName::parse(&name.to_string()), Some(name));
    }

    #[test]
    fn segment_name_order_matches_time_order(
        a in (any::<u32>(), any::<u64>()),
        b in (any::<u32>(), any::<u64>()),
    ) {
        let na = SegmentName::new(BootCounter(a.0), Micros(a.1));
        let nb = SegmentName::new(BootCounter(b.0), Micros(b.1));
        // Lexicographic order of names has to match (boot, time): selecting
        // segments by range without reading files rests on it.
        prop_assert_eq!(
            na.to_string().cmp(&nb.to_string()),
            (a.0, a.1).cmp(&(b.0, b.1))
        );
    }

    #[test]
    fn segment_header_parse_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let _ = SegmentHeader::parse(&bytes);
    }

    #[test]
    fn footer_roundtrips(
        blocks in prop::collection::vec((0u64..1_000_000, 0u16..1000), 0..20),
        events in prop::collection::vec(any::<u16>(), 0..30),
        metrics in prop::collection::vec(any::<u16>(), 0..30),
    ) {
        let mut builder = FooterBuilder::new();
        let mut offset = SegmentHeader::SIZE as u64;
        let mut base = 0u64;
        let mut expected_blocks = Vec::new();
        // Types are noted before the block: they arrive with its records and
        // are pinned to a segment together with it. Sets declared after the
        // last block belong to a block that is still being assembled and do not
        // reach this footer — that is what the separate stage is for.
        for e in &events {
            builder.add_event(EventId(*e));
        }
        for m in &metrics {
            builder.add_metric(MetricId(*m));
        }
        for (delta, count) in &blocks {
            base += delta;
            let header = BlockHeader {
                body_len: 16,
                raw_len: 16,
                seq: 0,
                base: Micros(base),
                count: *count,
                compression: Compression::None,
                crc: 0,
            };
            builder.add_block(offset, &header, Micros(base));
            expected_blocks.push((offset, base, *count));
            offset += 24 + 16;
        }

        let bytes = builder.build();
        let trailer = Trailer::parse(&bytes)?.expect("the signature is there");
        prop_assert_eq!(trailer.total_len(), bytes.len() as u64);

        let footer = Footer::parse(&bytes)?.expect("the footer reads");
        prop_assert_eq!(footer.blocks.len(), expected_blocks.len());
        for (got, (offset, base, count)) in footer.blocks.iter().zip(&expected_blocks) {
            prop_assert_eq!(got.offset, *offset);
            prop_assert_eq!(got.base.0, *base);
            prop_assert_eq!(got.count, *count);
        }

        // The sets are sorted and free of duplicates: both the migration and
        // the question "what telemetry is in here" binary-search them.
        //
        // A segment without a single block holds no records either, so it holds
        // no types: the ones noted belong to a block that is still being
        // assembled and will travel to whichever segment it lands in.
        let sealed = !blocks.is_empty();
        for (declared, got) in [
            (&events, footer.events.iter().map(|e| e.0).collect::<Vec<u16>>()),
            (&metrics, footer.metrics.iter().map(|m| m.0).collect::<Vec<u16>>()),
        ] {
            let mut expected: Vec<u16> = if sealed { declared.clone() } else { Vec::new() };
            expected.sort_unstable();
            expected.dedup();
            prop_assert_eq!(got, expected);
        }
    }

    #[test]
    fn footer_parse_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = Footer::parse(&bytes);
        let _ = Trailer::parse(&bytes);
    }

    #[test]
    fn block_lookup_is_consistent(
        deltas in prop::collection::vec(1u64..10_000, 1..30),
        probe in any::<u64>(),
    ) {
        let mut builder = FooterBuilder::new();
        let mut base = 0u64;
        let mut bases = Vec::new();
        for (i, d) in deltas.iter().enumerate() {
            base += d;
            bases.push(base);
            let header = BlockHeader {
                body_len: 8,
                raw_len: 8,
                seq: 0,
                base: Micros(base),
                count: 1,
                compression: Compression::None,
                crc: 0,
            };
            builder.add_block(SegmentHeader::SIZE as u64 + i as u64 * 32, &header, Micros(base));
        }
        let bytes = builder.build();
        let footer = Footer::parse(&bytes)?.unwrap();

        match footer.block_for_time(Micros(probe)) {
            None => prop_assert!(probe < bases[0], "None only before the first block"),
            Some(i) => {
                prop_assert!(bases[i] <= probe, "the block found starts later than probe");
                if let Some(next) = bases.get(i + 1) {
                    prop_assert!(*next > probe, "a better-fitting block exists");
                }
            }
        }
    }
}
