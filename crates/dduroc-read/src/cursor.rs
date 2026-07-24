//! Курсоры чтения: записи сегмента, сегменты канала.
//!
//! Память ограничена одним распакованным блоком на курсор — сегменты бывают
//! сотнями мегабайт, а читать их целиком на armv7 нельзя.
//!
//! # Повреждения не заметаются под ковёр
//!
//! Битый блок **не обрывает** чтение сегмента: следующий блок находится по
//! footer-индексу, а о пропуске сообщается вызывающему. Молчаливое
//! прекращение выдало бы неполный ответ за полный — худший из возможных
//! исходов для диагностики.

use crate::error::{ReadError, Result};
use dduroc_engine::segment::{SegmentReader, parse_block};
use dduroc_format::segment::SegmentName;
use dduroc_format::{Micros, Record};
use std::path::{Path, PathBuf};

/// Одна прочитанная запись с восстановленным абсолютным временем.
#[derive(Debug, Clone)]
pub struct RawEntry {
    pub at: Micros,
    pub boot: u32,
    pub record: OwnedRecord,
}

/// Владеющая копия записи: курсор переиспользует буфер блока, поэтому
/// заимствовать наружу нечего.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedRecord {
    Message {
        event: dduroc_format::EventId,
        span: Option<dduroc_format::SpanId>,
        payload: Vec<u8>,
    },
    SpanStart {
        span: dduroc_format::SpanId,
        kind: dduroc_format::SpanKindId,
        parent: Option<dduroc_format::SpanId>,
    },
    SpanEnd {
        span: dduroc_format::SpanId,
    },
    Sample {
        series: dduroc_format::SeriesLocal,
        value: OwnedSampleValue,
    },
    SeriesDef {
        series: dduroc_format::SeriesLocal,
        metric: dduroc_format::MetricId,
        value_type: dduroc_format::ValueType,
        tags: Vec<(String, String)>,
    },
    Text {
        level: dduroc_format::Level,
        span: Option<dduroc_format::SpanId>,
        target: String,
        text: String,
    },
    Ext {
        bytes: Vec<u8>,
    },
}

/// Значение сэмпла во владеющей форме.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedSampleValue {
    F32(f32),
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Blob(Vec<u8>),
}

impl OwnedSampleValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            OwnedSampleValue::F32(v) => Some(f64::from(*v)),
            OwnedSampleValue::F64(v) => Some(*v),
            OwnedSampleValue::I64(v) => Some(*v as f64),
            OwnedSampleValue::U64(v) => Some(*v as f64),
            OwnedSampleValue::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            OwnedSampleValue::Blob(_) => None,
        }
    }
}

fn own(record: &Record<'_>) -> OwnedRecord {
    match record {
        Record::Message(m) => OwnedRecord::Message {
            event: m.event,
            span: m.span,
            payload: m.payload.to_vec(),
        },
        Record::SpanStart(s) => OwnedRecord::SpanStart {
            span: s.span,
            kind: s.kind,
            parent: s.parent,
        },
        Record::SpanEnd { span } => OwnedRecord::SpanEnd { span: *span },
        Record::Sample(s) => OwnedRecord::Sample {
            series: s.series,
            value: match s.value {
                dduroc_format::Value::F32(v) => OwnedSampleValue::F32(v),
                dduroc_format::Value::F64(v) => OwnedSampleValue::F64(v),
                dduroc_format::Value::I64(v) => OwnedSampleValue::I64(v),
                dduroc_format::Value::U64(v) => OwnedSampleValue::U64(v),
                dduroc_format::Value::Bool(v) => OwnedSampleValue::Bool(v),
                dduroc_format::Value::Blob(b) => OwnedSampleValue::Blob(b.to_vec()),
            },
        },
        Record::SeriesDef(d) => OwnedRecord::SeriesDef {
            series: d.series,
            metric: d.metric,
            value_type: d.value_type,
            tags: d
                .tags
                .iter()
                .filter_map(|r| r.ok())
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect(),
        },
        Record::Text(t) => OwnedRecord::Text {
            level: t.level,
            span: t.span,
            target: t.target.to_owned(),
            text: t.text.to_owned(),
        },
        Record::Ext { bytes } => OwnedRecord::Ext {
            bytes: bytes.to_vec(),
        },
    }
}

/// Курсор по записям одного сегмента.
#[derive(Debug)]
pub struct SegmentCursor {
    reader: SegmentReader,
    path: PathBuf,
    /// Смещения блоков в порядке возрастания времени.
    offsets: Vec<u64>,
    /// Индекс следующего блока.
    next_block: usize,
    /// Распакованные записи текущего блока.
    buffered: Vec<RawEntry>,
    /// Позиция в `buffered`.
    pos: usize,
    /// Обратный порядок.
    reverse: bool,
    /// Блоки, которые не удалось прочитать.
    damaged: Vec<Damage>,
}

/// Сведения о пропущенном фрагменте.
#[derive(Debug, Clone)]
pub struct Damage {
    pub path: PathBuf,
    pub offset: u64,
    pub reason: String,
}

impl SegmentCursor {
    pub fn open(path: &Path, reverse: bool, expect_store: Option<u64>) -> Result<Self> {
        let reader = SegmentReader::open(path).map_err(ReadError::Engine)?;
        if let Some(id) = expect_store
            && reader.header().store_id != id
        {
            return Err(ReadError::ForeignStore {
                path: path.to_owned(),
                expected: id,
                found: reader.header().store_id,
            });
        }
        let mut offsets = reader.block_offsets().map_err(ReadError::Engine)?;
        if reverse {
            offsets.reverse();
        }
        Ok(Self {
            reader,
            path: path.to_owned(),
            offsets,
            next_block: 0,
            buffered: Vec::new(),
            pos: 0,
            reverse,
            damaged: Vec::new(),
        })
    }

    pub fn boot(&self) -> u32 {
        self.reader.header().boot.0
    }

    pub fn protocol_version(&self) -> u16 {
        self.reader.header().protocol_version.0
    }

    pub fn name(&self) -> SegmentName {
        self.reader.header().file_name()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Пропущенные фрагменты, накопленные к этому моменту.
    pub fn damaged(&self) -> &[Damage] {
        &self.damaged
    }

    /// Заглянуть в следующую запись, не потребляя её.
    pub fn peek(&mut self) -> Option<&RawEntry> {
        if self.pos >= self.buffered.len() && !self.fill() {
            return None;
        }
        self.buffered.get(self.pos)
    }

    /// Взять следующую запись.
    pub fn next_entry(&mut self) -> Option<RawEntry> {
        if self.pos >= self.buffered.len() && !self.fill() {
            return None;
        }
        let item = self.buffered.get(self.pos).cloned();
        self.pos += 1;
        item
    }

    /// Пропустить блоки, целиком лежащие раньше `from`.
    ///
    /// Границы блока известны из footer'а, поэтому отбрасывание идёт без
    /// чтения тел — ради этого footer и существует.
    pub fn seek_from(&mut self, from: Micros) {
        if self.reverse {
            return;
        }
        let Some(footer) = self.reader.footer() else {
            return;
        };
        // Ищем последний блок с базой <= from: записи с нужным временем
        // могут начинаться внутри него.
        let start = footer.block_for_time(from).unwrap_or(0);
        self.next_block = self.next_block.max(start);
    }

    /// Загрузить следующий блок. `false` — блоков больше нет.
    fn fill(&mut self) -> bool {
        let mut buf = Vec::new();
        while self.next_block < self.offsets.len() {
            let offset = self.offsets[self.next_block];
            self.next_block += 1;

            match self.reader.read_block_at(offset, &mut buf) {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(e) => {
                    // Битый блок не обрывает сегмент: остальные блоки
                    // адресуются независимо, и терять их незачем.
                    self.damaged.push(Damage {
                        path: self.path.clone(),
                        offset,
                        reason: e.to_string(),
                    });
                    continue;
                }
            }

            let block = match parse_block(&buf) {
                Ok(Some(b)) => b,
                Ok(None) => continue,
                Err(e) => {
                    self.damaged.push(Damage {
                        path: self.path.clone(),
                        offset,
                        reason: e.to_string(),
                    });
                    continue;
                }
            };

            let boot = self.reader.header().boot.0;
            self.buffered.clear();
            self.pos = 0;
            let mut broken = None;
            for item in block.records() {
                match item {
                    Ok((at, record)) => self.buffered.push(RawEntry {
                        at,
                        boot,
                        record: own(&record),
                    }),
                    Err(e) => {
                        broken = Some(e.to_string());
                        break;
                    }
                }
            }
            if let Some(reason) = broken {
                self.damaged.push(Damage {
                    path: self.path.clone(),
                    offset,
                    reason,
                });
            }
            if self.reverse {
                self.buffered.reverse();
            }
            if !self.buffered.is_empty() {
                return true;
            }
        }
        false
    }
}

/// Курсор по сегментам одного канала.
#[derive(Debug)]
pub struct ChannelCursor {
    dir: PathBuf,
    /// Имена сегментов в порядке обхода.
    segments: Vec<SegmentName>,
    next: usize,
    current: Option<SegmentCursor>,
    reverse: bool,
    from: Option<Micros>,
    expect_store: Option<u64>,
    damaged: Vec<Damage>,
    /// Неймспейс и канал — для маркировки выдаваемых записей.
    pub namespace: String,
    pub channel: String,
}

/// Параметры открытия канала.
#[derive(Debug, Clone, Default)]
pub struct ChannelScope {
    pub from: Option<Micros>,
    pub to: Option<Micros>,
    pub boot: Option<u32>,
    pub reverse: bool,
    pub expect_store: Option<u64>,
}

impl ChannelCursor {
    /// Открыть канал, отобрав сегменты по диапазону времени.
    pub fn open(
        dir: &Path,
        namespace: String,
        channel: String,
        scope: &ChannelScope,
    ) -> Result<Self> {
        let ChannelScope {
            from,
            to,
            boot,
            reverse,
            expect_store,
        } = *scope;
        let inventory = dduroc_engine::rotation::Inventory::scan(dir).map_err(ReadError::Engine)?;
        let all: Vec<SegmentName> = inventory.iter().map(|e| e.name).collect();

        // Сегмент начинается со времени в имени, поэтому отбор по верхней
        // границе точен: сегмент, начавшийся позже `to`, заведомо не нужен.
        // Нижнюю границу так отсечь нельзя — сегмент мог начаться раньше и
        // содержать нужные записи, поэтому отбрасывается только тот, за
        // которым идёт сегмент, тоже начинающийся раньше `from`.
        let mut segments: Vec<SegmentName> = Vec::new();
        for (i, name) in all.iter().enumerate() {
            if let Some(b) = boot
                && name.boot.0 != b
            {
                continue;
            }
            if let Some(to) = to
                && name.base > to
            {
                continue;
            }
            if let Some(from) = from
                && let Some(next) = all.get(i + 1)
                && next.base <= from
                && next.boot == name.boot
            {
                continue;
            }
            segments.push(*name);
        }
        if reverse {
            segments.reverse();
        }

        Ok(Self {
            dir: dir.to_owned(),
            segments,
            next: 0,
            current: None,
            reverse,
            from,
            expect_store,
            damaged: Vec::new(),
            namespace,
            channel,
        })
    }

    pub fn damaged(&self) -> &[Damage] {
        &self.damaged
    }

    pub fn peek(&mut self) -> Option<&RawEntry> {
        loop {
            if self.current.is_none() && !self.advance() {
                return None;
            }
            let has = self.current.as_mut().and_then(|c| c.peek()).is_some();
            if has {
                return self.current.as_mut().and_then(|c| c.peek());
            }
            self.finish_current();
        }
    }

    pub fn next_entry(&mut self) -> Option<RawEntry> {
        loop {
            if self.current.is_none() && !self.advance() {
                return None;
            }
            if let Some(item) = self.current.as_mut().and_then(|c| c.next_entry()) {
                return Some(item);
            }
            self.finish_current();
        }
    }

    /// Версия протокола текущего сегмента: миграции применяются при чтении,
    /// поэтому знать её нужно на каждую запись.
    pub fn current_protocol_version(&self) -> Option<u16> {
        self.current.as_ref().map(|c| c.protocol_version())
    }

    /// Определения серий текущего сегмента.
    pub fn current_segment_path(&self) -> Option<&Path> {
        self.current.as_ref().map(|c| c.path())
    }

    fn finish_current(&mut self) {
        if let Some(c) = self.current.take() {
            self.damaged.extend_from_slice(c.damaged());
        }
    }

    fn advance(&mut self) -> bool {
        while self.next < self.segments.len() {
            let name = self.segments[self.next];
            self.next += 1;
            let path = self.dir.join(name.to_string());
            match SegmentCursor::open(&path, self.reverse, self.expect_store) {
                Ok(mut c) => {
                    if let Some(from) = self.from {
                        c.seek_from(from);
                    }
                    self.current = Some(c);
                    return true;
                }
                Err(e) => {
                    // Сегмент, который не открылся, не должен прекращать
                    // обход канала: остальные читаются независимо.
                    self.damaged.push(Damage {
                        path,
                        offset: 0,
                        reason: e.to_string(),
                    });
                }
            }
        }
        false
    }
}
