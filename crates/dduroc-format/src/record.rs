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
use crate::ids::{EventId, MetricId, SpanId, SpanKindId};
use crate::level::Level;
use crate::value::{Value, ValueType};
use crate::varint;

/// Тип записи — старший полубайт первого байта.
pub mod kind {
    pub const MESSAGE: u8 = 0x0;
    pub const SPAN_START: u8 = 0x1;
    pub const SPAN_END: u8 = 0x2;
    /// Занят в контейнере версии 1 записью `SeriesDef`, определявшей серию
    /// как `(метрика, рантайм-тэги)`. Рантайм-тэгов больше нет, идентичность
    /// серии равна метрике, и сэмпл несёт `metric_id` напрямую. Код не
    /// переиспользуется: цена — ничего, а спутать разбор версий нельзя.
    pub const RETIRED_SERIES_DEF: u8 = 0x3;
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

/// Отсчёт телеметрии.
///
/// Идентичность ряда — это метрика, и ничего кроме: рантайм-размерностей в
/// системе нет, а имя, единица, подписи состояний и пределы — статика схемы.
/// Поэтому сэмпл несёт `metric_id` напрямую, без промежуточного
/// интернирования и без записи-определения серии: одна varint-величина
/// вместо целого механизма, который к тому же приходилось восстанавливать
/// при чтении с середины и в обратном порядке.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample<'a> {
    pub metric: MetricId,
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

/// Записать идентификатор спана, отвергнув зарезервированный ноль.
///
/// Ноль означает «спана нет» и допустимым [`SpanId`] не является. Проверяется
/// на **кодировании**, а не только на разборе: декодер такой байт отвергает, и
/// кодек не имеет права порождать то, чего сам же не прочитает. Молча
/// пропустив ноль, писатель получил бы блок, теряемый читателем целиком на
/// первой же такой записи, — не одна запись, а весь остаток тела.
#[inline]
fn write_span(out: &mut Vec<u8>, span: SpanId) -> Result<()> {
    if span.0 == SpanId::NONE_RAW {
        return Err(Error::ReservedNotZero);
    }
    varint::write_u64(out, u64::from(span.0));
    Ok(())
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

    // Проверки живут внутри разбора, а не отдельным проходом перед ним:
    // сэмпл — самая частая запись, и лишний разбор варианта на ней виден
    // в замерах. Цена — откат буфера при отказе, но отказ здесь означает
    // дефект вызывающего и случается ноль раз за жизнь процесса.
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
            // `None` у родителя кодируется нулём штатно; явный `Some(0)` —
            // то же зарезервированное значение под другим именем.
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
        kind::SAMPLE => {
            // Тип значения дублирован во флагах: без него длину значения
            // взять негде — у сэмпла, в отличие от сообщения и расширения,
            // нет префикса длины.
            let ty = ValueType::from_u8(flags & SAMPLE_VTYPE_MASK)?;
            if flags & !SAMPLE_VTYPE_MASK != 0 {
                return Err(Error::ReservedNotZero);
            }
            let metric = MetricId(c.varint_u16("metric_id")?);
            Record::Sample(Sample {
                metric,
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
        // Отличимый диагноз для кода, занятого прежней версией: «неизвестный
        // тип» отправил бы искать порчу носителя там, где на самом деле
        // читается сегмент версии 1.
        kind::RETIRED_SERIES_DEF => {
            return Err(Error::RetiredRecordKind(kind::RETIRED_SERIES_DEF));
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
        // f32: kind+flags(1) + dt(1) + metric_id(1) + значение(4) = 7 байт.
        let size = roundtrip(
            Record::Sample(Sample {
                metric: MetricId(1),
                value: Value::F32(36.6),
            }),
            0,
        );
        assert_eq!(size, 7, "сэмпл f32 — 7 байт");

        // Малое u64: 1 + 1 + 1 + 1 = 4 байта. Столько же занимает и код
        // состояния метрики-перечисления: на диске это обычный u64.
        let size = roundtrip(
            Record::Sample(Sample {
                metric: MetricId(3),
                value: Value::U64(42),
            }),
            0,
        );
        assert_eq!(size, 4, "сэмпл малого u64 — 4 байта");
    }

    #[test]
    fn dropping_series_interning_did_not_cost_bytes() {
        // Раньше сэмпл ссылался на сегментно-локальный номер серии, который
        // нумеровался с нуля, и отдельная запись SeriesDef связывала его с
        // метрикой. Теперь метрика лежит в самом сэмпле. Размер обязан
        // остаться прежним для метрик, укладывающихся в один байт varint,
        // то есть для всех id меньше 128 — а бюджет схемы это 150 метрик.
        for id in [0u16, 1, 42, 127] {
            let size = roundtrip(
                Record::Sample(Sample {
                    metric: MetricId(id),
                    value: Value::F32(1.0),
                }),
                0,
            );
            assert_eq!(size, 7, "metric_id {id} обязан стоить один байт");
        }
        // Только id от 128 стоит второй байт — и это плата за исчезновение
        // записи-определения серии, которая стоила десятки байт на сегмент.
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
        // Код 0x3 занимала запись SeriesDef контейнера версии 1. Он не
        // переиспользуется, и встретить его можно только в файле чужой
        // версии — читатель обязан честно отказаться, а не разобрать
        // байты как что-то другое.
        let mut buf = vec![kind::RETIRED_SERIES_DEF << 4];
        varint::write_u64(&mut buf, 0);
        assert_eq!(
            decode(&buf),
            Err(Error::RetiredRecordKind(kind::RETIRED_SERIES_DEF)),
            "диагноз обязан указывать на версию формата, а не на порчу"
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
                text: "паника в потоке",
            }),
            0,
        );
        roundtrip(Record::Ext { bytes: &[9, 8, 7] }, 0);
    }

    #[test]
    fn sample_carries_no_dimensions_beyond_the_metric() {
        // Проверка ключевого свойства модели: на диске у сэмпла нет ничего,
        // кроме времени, метрики и значения. Любая размерность, которую
        // захотелось бы добавить в рантайме, обязана стать отдельной
        // метрикой схемы — иначе она начнёт занимать место в каждом отсчёте.
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
                (kind::SAMPLE << 4) | ValueType::U64 as u8,
                0,    // dt
                0x2a, // metric_id
                2,    // значение
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
    fn encoder_refuses_to_produce_what_it_cannot_read() {
        // Кодек обязан быть симметричен: `SpanId(0)` — зарезервированное
        // значение, декодер его отвергает. Пропустив ноль на записи, писатель
        // положил бы в блок запись, на которой читатель останавливается, —
        // и потерял бы весь остаток блока, а не одну запись.
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
            assert_eq!(encode(&rec, 0, &mut buf), Err(Error::ReservedNotZero));
            assert!(buf.is_empty(), "отказ не должен оставлять обрывок: {buf:?}");
        }

        // Корневой спан по-прежнему кодируется нулём в поле `parent`.
        let n = encode(
            &Record::SpanStart(SpanStart {
                span: SpanId(1),
                kind: SpanKindId(1),
                parent: None,
            }),
            0,
            &mut buf,
        )
        .expect("корневой спан законен");
        assert_eq!(n, buf.len());
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
                metric: MetricId(0),
                value: Value::Bool(true)
            })
            .span(),
            None
        );
    }
}
