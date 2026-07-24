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
use std::sync::Arc;

/// Предикат отбора, применяемый **до** материализации записи.
///
/// Владеющая копия записи стоит аллокации payload'а, поэтому запрос вроде
/// «только ошибки» не должен её платить за каждую из сотен тысяч
/// отфильтрованных записей. Определения серий пропускаются всегда: без них
/// не восстановить идентичность сэмплов.
pub type Prefilter = Arc<dyn Fn(&Record<'_>) -> bool + Send + Sync>;

/// Одна прочитанная запись с восстановленным абсолютным временем.
#[derive(Debug, Clone)]
pub struct RawEntry {
    pub at: Micros,
    pub boot: u32,
    pub record: OwnedRecord,
    /// Идентичность серии у сэмплов, разрешённая **внутри сегмента**.
    ///
    /// Номер серии сегментно-локален и переиспользуется с нуля в каждом
    /// сегменте, поэтому восстанавливать идентичность на уровне канала
    /// нельзя: сэмпл напряжения из нового сегмента унаследовал бы
    /// определение температуры из предыдущего.
    pub series: Option<SeriesDefinition>,
}

/// Идентичность серии телеметрии.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesDefinition {
    pub metric: dduroc_format::MetricId,
    pub value_type: dduroc_format::ValueType,
    pub tags: Vec<(String, String)>,
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

/// Запомнить определение серии в таблице сегмента.
fn remember_series(table: &mut Vec<Option<SeriesDefinition>>, def: &dduroc_format::SeriesDef<'_>) {
    let idx = def.series.0 as usize;
    // Номер серии приходит из файла: расширяем таблицу только в разумных
    // пределах, иначе повреждённая запись задала бы размер аллокации.
    const MAX_SERIES: usize = 64 * 1024;
    if idx >= MAX_SERIES {
        return;
    }
    if idx >= table.len() {
        table.resize(idx + 1, None);
    }
    table[idx] = Some(SeriesDefinition {
        metric: def.metric,
        value_type: def.value_type,
        tags: def
            .tags
            .iter()
            .filter_map(|r| r.ok())
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect(),
    });
}

/// Прямой проход по сегменту, собирающий только определения серий.
fn collect_series(
    reader: &SegmentReader,
    offsets: &[u64],
    table: &mut Vec<Option<SeriesDefinition>>,
) {
    let mut buf = Vec::new();
    for &offset in offsets {
        if reader.read_block_at(offset, &mut buf).is_err() {
            continue;
        }
        let Ok(Some(block)) = parse_block(&buf) else {
            continue;
        };
        for item in block.records() {
            if let Ok((_, Record::SeriesDef(def))) = item {
                remember_series(table, &def);
            }
        }
    }
}

/// Курсор по записям одного сегмента.
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
    /// Отбор до материализации.
    prefilter: Option<Prefilter>,
    /// Таблица серий сегмента: индекс — сегментно-локальный номер.
    ///
    /// У запечатанного сегмента берётся из footer'а, куда она продублирована
    /// ровно для этого: определение серии пишется в тело один раз, и при
    /// чтении с середины, в обратном порядке или после битого блока его
    /// в потоке уже не встретить.
    series: Vec<Option<SeriesDefinition>>,
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
    pub fn open(
        path: &Path,
        reverse: bool,
        expect_store: Option<u64>,
        prefilter: Option<Prefilter>,
    ) -> Result<Self> {
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
        let mut damaged = Vec::new();
        let mut offsets = match reader.footer() {
            Some(footer) => footer.blocks.iter().map(|b| b.offset).collect(),
            None => {
                // Незапечатанный сегмент сканируется; обрыв скана — обычное
                // следствие потери питания. Уже найденные блоки остаются в
                // выборке, а о месте обрыва сообщается явно.
                let (offsets, stopped) = reader.scan_block_offsets();
                if let Some((offset, reason)) = stopped {
                    damaged.push(Damage {
                        path: path.to_owned(),
                        offset,
                        reason,
                    });
                }
                offsets
            }
        };

        // Таблица серий: из footer'а, если сегмент запечатан.
        let mut series: Vec<Option<SeriesDefinition>> = match reader.footer() {
            Some(footer) => footer
                .series
                .iter()
                .map(|s| {
                    Some(SeriesDefinition {
                        metric: s.metric,
                        value_type: s.value_type,
                        tags: s
                            .tags
                            .iter()
                            .filter_map(|r| r.ok())
                            .map(|(k, v)| (k.to_owned(), v.to_owned()))
                            .collect(),
                    })
                })
                .collect(),
            None => Vec::new(),
        };

        // Незапечатанный сегмент читается с конца: определения серий лежат
        // в теле перед своими сэмплами, то есть в обратном обходе — уже
        // позади. Собираем их предварительным прямым проходом.
        //
        // Условие именно «не запечатан», а не «таблица пуста»: у сегмента
        // без телеметрии таблица пуста законно, и лишний полный проход по
        // нему обошёлся бы дороже самого запроса.
        if reverse && !reader.is_sealed() {
            collect_series(&reader, &offsets, &mut series);
        }

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
            prefilter,
            series,
            damaged,
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
                    Ok((at, record)) => {
                        // Определение серии — служебная запись: она наполняет
                        // таблицу сегмента и наружу не выдаётся.
                        if let Record::SeriesDef(def) = &record {
                            remember_series(&mut self.series, def);
                            continue;
                        }
                        // Отбор до владеющей копии: отброшенная запись не
                        // должна стоить аллокации своего payload'а.
                        if let Some(f) = &self.prefilter
                            && !f(&record)
                        {
                            continue;
                        }
                        let series = match &record {
                            Record::Sample(s) => {
                                self.series.get(s.series.0 as usize).and_then(|d| d.clone())
                            }
                            _ => None,
                        };
                        self.buffered.push(RawEntry {
                            at,
                            boot,
                            record: own(&record),
                            series,
                        });
                    }
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
pub struct ChannelCursor {
    dir: PathBuf,
    /// Имена сегментов в порядке обхода.
    segments: Vec<SegmentName>,
    next: usize,
    current: Option<SegmentCursor>,
    reverse: bool,
    from: Option<Micros>,
    expect_store: Option<u64>,
    prefilter: Option<Prefilter>,
    damaged: Vec<Damage>,
    /// Неймспейс и канал — для маркировки выдаваемых записей.
    ///
    /// `Arc<str>`, а не `String`: имя копируется в каждую выдаваемую запись,
    /// и на сотне тысяч записей это была бы сотня тысяч аллокаций.
    pub namespace: Arc<str>,
    pub channel: Arc<str>,
}

/// Параметры открытия канала.
#[derive(Clone, Default)]
pub struct ChannelScope {
    pub from: Option<Micros>,
    pub to: Option<Micros>,
    pub boot: Option<u32>,
    pub reverse: bool,
    pub expect_store: Option<u64>,
    pub prefilter: Option<Prefilter>,
}

impl ChannelCursor {
    /// Открыть канал, отобрав сегменты по диапазону времени.
    pub fn open(
        dir: &Path,
        namespace: Arc<str>,
        channel: Arc<str>,
        scope: &ChannelScope,
    ) -> Result<Self> {
        let (from, to, boot, reverse, expect_store) = (
            scope.from,
            scope.to,
            scope.boot,
            scope.reverse,
            scope.expect_store,
        );
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
            prefilter: scope.prefilter.clone(),
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
            match SegmentCursor::open(
                &path,
                self.reverse,
                self.expect_store,
                self.prefilter.clone(),
            ) {
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

impl std::fmt::Debug for ChannelScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelScope")
            .field("from", &self.from)
            .field("to", &self.to)
            .field("boot", &self.boot)
            .field("reverse", &self.reverse)
            .field("expect_store", &self.expect_store)
            .field("prefilter", &self.prefilter.is_some())
            .finish()
    }
}

impl std::fmt::Debug for SegmentCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCursor")
            .field("path", &self.path)
            .field("blocks", &self.offsets.len())
            .field("next_block", &self.next_block)
            .field("reverse", &self.reverse)
            .field("damaged", &self.damaged.len())
            .finish()
    }
}

impl std::fmt::Debug for ChannelCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelCursor")
            .field("namespace", &self.namespace)
            .field("channel", &self.channel)
            .field("segments", &self.segments.len())
            .field("next", &self.next)
            .field("damaged", &self.damaged.len())
            .finish()
    }
}
