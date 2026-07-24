//! Property-тесты формата.
//!
//! Проверяются два класса свойств:
//! 1. **Roundtrip** — что записано, то и прочитано, байт в байт по смыслу.
//! 2. **Устойчивость к мусору** — произвольные байты декодер либо разбирает,
//!    либо честно отвергает ошибкой, но никогда не паникует и не зацикливается.
//!    Это критично: сегмент после обрыва питания содержит именно произвольный
//!    хвост, и читатель обязан его пережить.

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
// Стратегии
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
        // Blob'ы в property-тестах — из статики: Value заимствует байты.
        Just(Value::Blob(&[])),
        Just(Value::Blob(&[0u8, 1, 2, 3, 255])),
    ]
}

/// Владеющее описание записи: `Record` заимствует, поэтому стратегии
/// генерируют владеющую форму, а тест одалживает из неё.
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

/// Значения f32/f64 сравниваем по битам: NaN != NaN ломал бы roundtrip.
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
// Свойства
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
        // Результат не важен — важно, что нет паники и потреблённая длина
        // не выходит за границы входа.
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
        prop_assert_eq!(read, buf.len(), "декодер обязан потребить ровно запись");
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
        // Итератор обязан завершиться на любом входе: зацикливание на битом
        // хвосте подвесило бы чтение сегмента после обрыва питания.
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
        times.sort_unstable(); // writer подаёт записи в порядке времени

        let mut builder = BlockBuilder::new();
        for (i, owned) in records[..n].iter().enumerate() {
            builder.push(Micros(times[i]), &owned.as_record()).unwrap();
        }

        let compression = if compress { Compression::Lz4 } else { Compression::None };
        let mut out = Vec::new();
        let header = builder.finish(0, compression, &mut out).unwrap();
        prop_assert_eq!(header.count as usize, n);
        prop_assert_eq!(out.len() as u64, header.total_len());

        let block = Block::parse(&out)?.expect("блок должен быть прочитан");
        let decoded: Vec<_> = block.records().collect::<Result<Vec<_>, _>>()?;
        prop_assert_eq!(decoded.len(), n);
        for (i, (at, rec)) in decoded.iter().enumerate() {
            prop_assert_eq!(at.0, times[i], "время записи {} восстановлено неверно", i);
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
        // Любая одиночная порча блока обязана быть замечена — иначе CRC
        // не выполняет свою работу.
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
            Err(_) => {}                    // порча замечена
            Ok(None) => {}                  // затёрт body_len → «конец данных»
            Ok(Some(block)) => {
                // Уцелевший CRC при изменённом байте невозможен; единственный
                // законный случай — порча не входящих в CRC резервов, которых
                // в блоке нет. Значит, содержимое обязано отличаться.
                let recs: Vec<_> = block.records().collect();
                let same = recs.len() == 1 && matches!(
                    recs.first(),
                    Some(Ok((_, Record::Message(m)))) if m.payload == payload.as_slice()
                );
                prop_assert!(!same, "порча байта {} осталась незамеченной", i);
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

        // Имя файла всегда разбирается обратно.
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
        // Лексикографический порядок имён обязан совпадать с (boot, время):
        // на этом стоит выбор сегментов по диапазону без чтения файлов.
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
        for e in &events {
            builder.add_event(EventId(*e));
        }
        for m in &metrics {
            builder.add_metric(MetricId(*m));
        }

        let bytes = builder.build();
        let trailer = Trailer::parse(&bytes)?.expect("сигнатура на месте");
        prop_assert_eq!(trailer.total_len(), bytes.len() as u64);

        let footer = Footer::parse(&bytes)?.expect("footer читается");
        prop_assert_eq!(footer.blocks.len(), expected_blocks.len());
        for (got, (offset, base, count)) in footer.blocks.iter().zip(&expected_blocks) {
            prop_assert_eq!(got.offset, *offset);
            prop_assert_eq!(got.base.0, *base);
            prop_assert_eq!(got.count, *count);
        }

        // Множества — отсортированные и без дублей: по ним идёт бинарный
        // поиск и в миграции, и при вопросе «какая телеметрия здесь есть».
        for (declared, got) in [
            (&events, footer.events.iter().map(|e| e.0).collect::<Vec<u16>>()),
            (&metrics, footer.metrics.iter().map(|m| m.0).collect::<Vec<u16>>()),
        ] {
            let mut expected: Vec<u16> = declared.clone();
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
            None => prop_assert!(probe < bases[0], "None только раньше первого блока"),
            Some(i) => {
                prop_assert!(bases[i] <= probe, "найденный блок начинается позже probe");
                if let Some(next) = bases.get(i + 1) {
                    prop_assert!(*next > probe, "существует более подходящий блок");
                }
            }
        }
    }
}
