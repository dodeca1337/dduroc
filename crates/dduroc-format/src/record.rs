//! Записи внутри блока.
//!
//! Каркас: `[b0: kind(4 бита) | flags(4 бита)] [Δt varint] [поля по kind]`.
//!
//! `Δt` — микросекунды от **предыдущей записи блока** (у первой — от
//! `base_micros` заголовка блока, т.е. обычно 0). Дельта от соседа, а не от
//! базы: числа меньше, varint короче.
//!
//! На диск попадает только динамика. Уровень, шаблоны текста, тэги сообщений,
//! имена и единицы измерения — статические свойства типа, живут в схеме
//! бинарника и резолвятся при чтении. Принадлежность неймспейсу и каналу
//! имплицитна из пути файла, `boot_counter` — из заголовка сегмента.

use crate::cursor::{Cursor, write_str};
use crate::error::{Error, Result};
use crate::ids::{EventId, MetricId, SeriesLocal, SpanId, SpanKindId};
use crate::level::Level;
use crate::value::{Value, ValueType};
use crate::varint;

/// Тип записи — старший полубайт первого байта.
pub mod kind {
    pub const MESSAGE: u8 = 0x0;
    pub const SPAN_START: u8 = 0x1;
    pub const SPAN_END: u8 = 0x2;
    pub const SERIES_DEF: u8 = 0x3;
    pub const SAMPLE: u8 = 0x4;
    pub const TEXT: u8 = 0x5;
    /// Расширение: `len varint` + байты. Единственный способ добавить
    /// новую разновидность записи, не ломая старых читателей — они умеют
    /// пропустить её по длине.
    pub const EXT: u8 = 0xF;
}

/// Флаг наличия `span` в записи (сообщения и текст).
const FLAG_SPAN: u8 = 0b0001;
/// Маска типа значения в флагах записи `Sample`.
const SAMPLE_VTYPE_MASK: u8 = 0b0111;

// ════════════════════════════════════════════════════════════════════════════
// Тэги серий
// ════════════════════════════════════════════════════════════════════════════

/// Тэги серии телеметрии — часть её идентичности (`sensor="pa"`).
///
/// Два представления с общим API: [`Tags::Slice`] при записи, [`Tags::Raw`]
/// при чтении (заимствует байты блока, не аллоцирует — важно на armv7).
#[derive(Debug, Clone, Copy)]
pub enum Tags<'a> {
    Slice(&'a [(&'a str, &'a str)]),
    Raw { count: u32, bytes: &'a [u8] },
}

impl<'a> Tags<'a> {
    pub const EMPTY: Tags<'static> = Tags::Slice(&[]);

    pub fn len(&self) -> u32 {
        match self {
            Tags::Slice(s) => s.len() as u32,
            Tags::Raw { count, .. } => *count,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Итератор пар `(ключ, значение)`. Для [`Tags::Raw`] элементы
    /// декодируются лениво, поэтому возможна ошибка формата.
    pub fn iter(&self) -> TagIter<'a> {
        TagIter(match *self {
            Tags::Slice(s) => TagIterInner::Slice(s.iter()),
            Tags::Raw { count, bytes } => TagIterInner::Raw {
                remaining: count,
                cursor: Cursor::new(bytes),
            },
        })
    }

    fn encode(&self, out: &mut Vec<u8>) -> Result<()> {
        varint::write_u64(out, u64::from(self.len()));
        for pair in self.iter() {
            let (k, v) = pair?;
            write_str(out, k);
            write_str(out, v);
        }
        Ok(())
    }
}

/// Итератор тэгов. Для сырого представления возвращает `Result`, так как
/// разбор идёт по ходу.
#[derive(Debug)]
pub struct TagIter<'a>(TagIterInner<'a>);

#[derive(Debug)]
enum TagIterInner<'a> {
    Slice(core::slice::Iter<'a, (&'a str, &'a str)>),
    Raw { remaining: u32, cursor: Cursor<'a> },
}

impl<'a> Iterator for TagIter<'a> {
    type Item = Result<(&'a str, &'a str)>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.0 {
            TagIterInner::Slice(it) => it.next().map(|&(k, v)| Ok((k, v))),
            TagIterInner::Raw { remaining, cursor } => {
                if *remaining == 0 {
                    return None;
                }
                *remaining -= 1;
                let pair = (|| {
                    let k = cursor.str("tag_key")?;
                    let v = cursor.str("tag_value")?;
                    Ok((k, v))
                })();
                Some(pair)
            }
        }
    }
}

impl PartialEq for Tags<'_> {
    /// Сравнение логическое (по парам), а не по представлению: `Slice` и
    /// `Raw` с одинаковым содержимым равны — на это опираются roundtrip-тесты.
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        self.iter().zip(other.iter()).all(|(a, b)| match (a, b) {
            (Ok(a), Ok(b)) => a == b,
            _ => false,
        })
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Записи
// ════════════════════════════════════════════════════════════════════════════

/// Схемное сообщение: тип + бинарные поля (postcard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Message<'a> {
    pub event: EventId,
    /// Спан, к которому привязано сообщение (из runtime-контекста).
    pub span: Option<SpanId>,
    /// Сериализованные поля события. Длина хранится явно, хотя postcard
    /// самоописуем при известной схеме: без неё записи неизвестных типов
    /// нельзя было бы пропустить (чужой билд, состояние до миграции).
    pub payload: &'a [u8],
}

/// Начало спана.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpanStart {
    pub span: SpanId,
    pub kind: SpanKindId,
    /// Родитель; `None` — корневой спан.
    pub parent: Option<SpanId>,
}

/// Определение серии телеметрии: интернирование `(метрика, тэги)` в
/// сегментно-локальный `SeriesLocal`. Пишется при первом сэмпле серии
/// в сегменте и дублируется в footer'е.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SeriesDef<'a> {
    pub series: SeriesLocal,
    pub metric: MetricId,
    pub value_type: ValueType,
    pub tags: Tags<'a>,
}

/// Отсчёт телеметрии.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample<'a> {
    pub series: SeriesLocal,
    pub value: Value<'a>,
}

/// Свободный текст без схемы: мост из `tracing`/`log`, panic-handler.
/// Уровень хранится в записи — резолвить его по схеме не через что.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Text<'a> {
    pub level: Level,
    pub span: Option<SpanId>,
    /// Источник: target из tracing, имя модуля и т.п.
    pub target: &'a str,
    pub text: &'a str,
}

/// Одна запись блока.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Record<'a> {
    Message(Message<'a>),
    SpanStart(SpanStart),
    SpanEnd {
        span: SpanId,
    },
    SeriesDef(SeriesDef<'a>),
    Sample(Sample<'a>),
    Text(Text<'a>),
    /// Нераспознанное расширение: сохранено целиком, пропускается по длине.
    Ext {
        bytes: &'a [u8],
    },
}

/// Запись вместе с её временной дельтой.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Framed<'a> {
    /// Микросекунды от предыдущей записи блока.
    pub dt: u64,
    pub record: Record<'a>,
}

impl Record<'_> {
    /// Полубайт типа.
    pub const fn kind(&self) -> u8 {
        match self {
            Record::Message(_) => kind::MESSAGE,
            Record::SpanStart(_) => kind::SPAN_START,
            Record::SpanEnd { .. } => kind::SPAN_END,
            Record::SeriesDef(_) => kind::SERIES_DEF,
            Record::Sample(_) => kind::SAMPLE,
            Record::Text(_) => kind::TEXT,
            Record::Ext { .. } => kind::EXT,
        }
    }

    /// Спан записи, если она к нему привязана.
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

/// Закодировать запись с дельтой `dt`. Возвращает число дописанных байт.
pub fn encode(record: &Record<'_>, dt: u64, out: &mut Vec<u8>) -> Result<usize> {
    let start = out.len();

    let flags = match record {
        Record::Message(m) if m.span.is_some() => FLAG_SPAN,
        Record::Text(t) if t.span.is_some() => FLAG_SPAN,
        Record::Sample(s) => s.value.value_type() as u8,
        _ => 0,
    };
    out.push((record.kind() << 4) | flags);
    varint::write_u64(out, dt);

    match record {
        Record::Message(m) => {
            varint::write_u64(out, u64::from(m.event.0));
            if let Some(span) = m.span {
                varint::write_u64(out, u64::from(span.0));
            }
            varint::write_u64(out, m.payload.len() as u64);
            out.extend_from_slice(m.payload);
        }
        Record::SpanStart(s) => {
            varint::write_u64(out, u64::from(s.span.0));
            varint::write_u64(out, u64::from(s.kind.0));
            varint::write_u64(out, u64::from(SpanId::raw_or_none(s.parent)));
        }
        Record::SpanEnd { span } => {
            varint::write_u64(out, u64::from(span.0));
        }
        Record::SeriesDef(d) => {
            varint::write_u64(out, u64::from(d.series.0));
            varint::write_u64(out, u64::from(d.metric.0));
            out.push(d.value_type as u8);
            d.tags.encode(out)?;
        }
        Record::Sample(s) => {
            varint::write_u64(out, u64::from(s.series.0));
            s.value.encode(out);
        }
        Record::Text(t) => {
            out.push(t.level as u8);
            if let Some(span) = t.span {
                varint::write_u64(out, u64::from(span.0));
            }
            write_str(out, t.target);
            write_str(out, t.text);
        }
        Record::Ext { bytes } => {
            varint::write_u64(out, bytes.len() as u64);
            out.extend_from_slice(bytes);
        }
    }

    Ok(out.len() - start)
}

/// Раскодировать одну запись из начала `input`.
/// Возвращает запись и число потреблённых байт.
pub fn decode(input: &[u8]) -> Result<(Framed<'_>, usize)> {
    let mut c = Cursor::new(input);
    let b0 = c.u8()?;
    let (kind, flags) = (b0 >> 4, b0 & 0x0F);
    let dt = c.varint()?;

    let record = match kind {
        kind::MESSAGE => {
            let event = EventId(c.varint_u16("event_id")?);
            let span = read_flagged_span(&mut c, flags)?;
            let len = c.varint_len("payload_len")?;
            Record::Message(Message {
                event,
                span,
                payload: c.take(len)?,
            })
        }
        kind::SPAN_START => {
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
        kind::SPAN_END => {
            reject_flags(flags)?;
            Record::SpanEnd {
                span: read_span(&mut c)?,
            }
        }
        kind::SERIES_DEF => {
            reject_flags(flags)?;
            let series = SeriesLocal(c.varint_u32("series_local")?);
            let metric = MetricId(c.varint_u16("metric_id")?);
            let value_type = ValueType::from_u8(c.u8()?)?;
            let count = c.varint_u32("n_tags")?;
            // Границы каждого тэга проверяются при ленивом разборе; здесь
            // нужно лишь потребить их байты, чтобы найти конец записи.
            let tags_start = c.pos();
            for _ in 0..count {
                let _ = c.str("tag_key")?;
                let _ = c.str("tag_value")?;
            }
            let bytes = &input[tags_start..c.pos()];
            Record::SeriesDef(SeriesDef {
                series,
                metric,
                value_type,
                tags: Tags::Raw { count, bytes },
            })
        }
        kind::SAMPLE => {
            // Тип значения дублирован во флагах: без него сэмпл нельзя
            // пропустить, не зная его серию.
            let ty = ValueType::from_u8(flags & SAMPLE_VTYPE_MASK)?;
            if flags & !SAMPLE_VTYPE_MASK != 0 {
                return Err(Error::ReservedNotZero);
            }
            let series = SeriesLocal(c.varint_u32("series_local")?);
            Record::Sample(Sample {
                series,
                value: c.value(ty)?,
            })
        }
        kind::TEXT => {
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
        kind::EXT => {
            reject_flags(flags)?;
            let len = c.varint_len("ext_len")?;
            Record::Ext {
                bytes: c.take(len)?,
            }
        }
        other => return Err(Error::UnknownRecordKind(other)),
    };

    Ok((Framed { dt, record }, c.pos()))
}

/// Итератор по записям блока. Останавливается на конце данных; ошибку
/// возвращает элементом (вызывающий решает, обрезать хвост или считать
/// блок битым).
pub fn iter(body: &[u8]) -> RecordIter<'_> {
    RecordIter { body, pos: 0 }
}

#[derive(Debug)]
pub struct RecordIter<'a> {
    body: &'a [u8],
    pos: usize,
}

impl<'a> RecordIter<'a> {
    /// Смещение следующей нечитанной записи в теле блока.
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
                // Останавливаемся, иначе итератор зациклился бы на ошибке.
                self.pos = self.body.len();
                Some(Err(e))
            }
        }
    }
}

#[inline]
fn read_span(c: &mut Cursor<'_>) -> Result<SpanId> {
    let raw = c.varint_u32("span")?;
    SpanId::from_raw(raw).ok_or(Error::ReservedNotZero)
}

#[inline]
fn read_flagged_span(c: &mut Cursor<'_>, flags: u8) -> Result<Option<SpanId>> {
    if flags & FLAG_SPAN == 0 {
        // Прочие биты у этих типов пока не определены.
        if flags != 0 {
            return Err(Error::ReservedNotZero);
        }
        return Ok(None);
    }
    if flags & !FLAG_SPAN != 0 {
        return Err(Error::ReservedNotZero);
    }
    Ok(Some(read_span(c)?))
}

#[inline]
fn reject_flags(flags: u8) -> Result<()> {
    if flags != 0 {
        return Err(Error::ReservedNotZero);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(rec: Record<'_>, dt: u64) -> usize {
        let mut buf = Vec::new();
        let written = encode(&rec, dt, &mut buf).expect("кодирование");
        assert_eq!(written, buf.len());
        let (framed, read) = decode(&buf).expect("декодирование");
        assert_eq!(read, buf.len(), "потреблено не всё");
        assert_eq!(framed.dt, dt);
        assert_eq!(framed.record, rec);
        buf.len()
    }

    #[test]
    fn message_sizes() {
        // Заявленный в SPEC.md размер: kind+flags(1) + dt(1) + event(1)
        // + payload_len(1) + payload(4) = 8 байт.
        let size = roundtrip(
            Record::Message(Message {
                event: EventId(1),
                span: None,
                payload: &[0xAA; 4],
            }),
            0,
        );
        assert_eq!(size, 8, "типичное сообщение должно занимать 8 байт");

        // Со спаном — плюс varint span_id.
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
        // f32: 1 + 1 + 1 + 4 = 7 байт.
        let size = roundtrip(
            Record::Sample(Sample {
                series: SeriesLocal(0),
                value: Value::F32(36.6),
            }),
            0,
        );
        assert_eq!(size, 7, "сэмпл f32 — 7 байт");

        // Малое u64: 1 + 1 + 1 + 1 = 4 байта.
        let size = roundtrip(
            Record::Sample(Sample {
                series: SeriesLocal(3),
                value: Value::U64(42),
            }),
            0,
        );
        assert_eq!(size, 4, "сэмпл малого u64 — 4 байта");
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
            Record::SeriesDef(SeriesDef {
                series: SeriesLocal(0),
                metric: MetricId(1),
                value_type: ValueType::F32,
                tags: Tags::Slice(&[("sensor", "pa"), ("канал", "3")]),
            }),
            0,
        );
        roundtrip(
            Record::SeriesDef(SeriesDef {
                series: SeriesLocal(1),
                metric: MetricId(2),
                value_type: ValueType::Blob,
                tags: Tags::EMPTY,
            }),
            0,
        );
        roundtrip(
            Record::Sample(Sample {
                series: SeriesLocal(1),
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
                text: "паника в потоке",
            }),
            0,
        );
        roundtrip(Record::Ext { bytes: &[9, 8, 7] }, 0);
    }

    #[test]
    fn tags_decoded_lazily_and_compare_logically() {
        let tags: &[(&str, &str)] = &[("a", "1"), ("b", "2")];
        let mut buf = Vec::new();
        encode(
            &Record::SeriesDef(SeriesDef {
                series: SeriesLocal(0),
                metric: MetricId(0),
                value_type: ValueType::I64,
                tags: Tags::Slice(tags),
            }),
            0,
            &mut buf,
        )
        .unwrap();

        let (framed, _) = decode(&buf).unwrap();
        let Record::SeriesDef(def) = framed.record else {
            panic!("ожидался SeriesDef");
        };
        assert_eq!(def.tags.len(), 2);
        let pairs: Vec<_> = def.tags.iter().map(|r| r.unwrap()).collect();
        assert_eq!(pairs, tags);
        assert_eq!(def.tags, Tags::Slice(tags), "Raw == Slice по содержимому");
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
                ref other => panic!("ожидалось сообщение, получено {other:?}"),
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
        // Обрезанная вторая запись — как после обрыва питания.
        body.push(kind::MESSAGE << 4);

        let results: Vec<_> = iter(&body).collect();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_ok());
        let err = results[1].as_ref().unwrap_err();
        assert!(err.is_torn_tail(), "обрыв хвоста: {err}");
    }

    #[test]
    fn zero_span_id_rejected() {
        // span_id = 0 при выставленном флаге — зарезервированное значение.
        let mut buf = vec![(kind::MESSAGE << 4) | FLAG_SPAN];
        varint::write_u64(&mut buf, 0); // dt
        varint::write_u64(&mut buf, 1); // event
        varint::write_u64(&mut buf, 0); // span = 0 — недопустимо
        varint::write_u64(&mut buf, 0); // payload_len
        assert_eq!(decode(&buf), Err(Error::ReservedNotZero));
    }

    #[test]
    fn unknown_kind_and_reserved_flags_rejected() {
        // kind 0x6 не определён — читатель обязан сообщить об ошибке,
        // а не угадывать длину.
        let mut buf = vec![0x6 << 4];
        varint::write_u64(&mut buf, 0);
        assert_eq!(decode(&buf), Err(Error::UnknownRecordKind(0x6)));

        // Ненулевые зарезервированные флаги у SpanEnd.
        let mut buf = vec![(kind::SPAN_END << 4) | 0b0010];
        varint::write_u64(&mut buf, 0);
        varint::write_u64(&mut buf, 1);
        assert_eq!(decode(&buf), Err(Error::ReservedNotZero));

        // Неизвестный vtype в флагах сэмпла.
        let mut buf = vec![(kind::SAMPLE << 4) | 0b0110];
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
                series: SeriesLocal(0),
                value: Value::Bool(true)
            })
            .span(),
            None
        );
    }
}
