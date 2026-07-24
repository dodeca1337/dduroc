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
        metric: dduroc_format::MetricId,
        value: OwnedSampleValue,
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
            metric: s.metric,
            value: match s.value {
                dduroc_format::Value::F32(v) => OwnedSampleValue::F32(v),
                dduroc_format::Value::F64(v) => OwnedSampleValue::F64(v),
                dduroc_format::Value::I64(v) => OwnedSampleValue::I64(v),
                dduroc_format::Value::U64(v) => OwnedSampleValue::U64(v),
                dduroc_format::Value::Bool(v) => OwnedSampleValue::Bool(v),
                dduroc_format::Value::Blob(b) => OwnedSampleValue::Blob(b.to_vec()),
            },
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

/// Пройти незапечатанный сегмент по заголовкам блоков.
///
/// Тела не разбираются вовсе: идентичность ряда лежит в самом сэмпле, и
/// восстанавливать её больше нечем и незачем. Раньше здесь был второй проход
/// по всему сегменту — он собирал определения серий, которые при обратном
/// обходе оказывались позади своих сэмплов.
fn scan_offsets(reader: &SegmentReader) -> (Vec<u64>, Option<(u64, String)>) {
    let mut offsets = Vec::new();
    let mut buf = Vec::new();
    let mut offset = SegmentReader::first_block_offset();
    loop {
        match reader.read_block_at(offset, &mut buf) {
            Ok(Some(next)) => {
                offsets.push(offset);
                offset = next;
            }
            Ok(None) => return (offsets, None),
            Err(e) => return (offsets, Some((offset, e.to_string()))),
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
        // Запечатанный сегмент отдаёт смещения блоков из footer'а; иначе —
        // скан заголовков. Обрыв скана — обычное следствие потери питания:
        // уже найденные блоки остаются в выборке, о месте обрыва сообщается
        // явно.
        //
        // Обратный обход больше не требует предварительного прохода: раньше
        // сэмпл ссылался на локальный номер серии, определение которого лежало
        // в потоке ПЕРЕД ним, то есть при чтении с конца — уже позади.
        let mut offsets: Vec<u64> = match reader.footer() {
            Some(footer) => footer.blocks.iter().map(|b| b.offset).collect(),
            None => {
                let (offsets, stopped) = scan_offsets(&reader);
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

    /// Есть ли в сегменте хоть одна из указанных метрик.
    ///
    /// `None` — сегмент не запечатан, множества метрик нет, и ответить, не
    /// читая блоки, нельзя. Ради этого вопроса множество и лежит в footer'е:
    /// поиск последнего состояния перед окном иначе читал бы историю целиком.
    pub fn contains_any_metric(
        &self,
        wanted: &std::collections::HashSet<dduroc_format::MetricId>,
    ) -> Option<bool> {
        let footer = self.reader.footer()?;
        Some(
            wanted
                .iter()
                .any(|m| footer.metrics.binary_search(m).is_ok()),
        )
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
                        // Служебных записей в блоке больше нет: всё, что
                        // прочитано, выдаётся наружу как есть.
                        //
                        // Отбор до владеющей копии: отброшенная запись не
                        // должна стоить аллокации своего payload'а.
                        if let Some(f) = &self.prefilter
                            && !f(&record)
                        {
                            continue;
                        }
                        self.buffered.push(RawEntry {
                            at,
                            boot,
                            record: own(&record),
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
    require_metrics: Option<Arc<std::collections::HashSet<dduroc_format::MetricId>>>,
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
    /// Сколько сегментов максимум просмотреть (в порядке обхода).
    ///
    /// Нужно поиску «что было до окна»: без границы он мог бы уйти в историю
    /// на всю глубину хранения, читая мегабайты ради одного значения.
    pub max_segments: Option<usize>,
    /// Пропускать запечатанные сегменты, в которых нет ни одной из этих
    /// метрик. Проверка идёт по множеству из footer'а, без чтения блоков.
    pub require_metrics: Option<Arc<std::collections::HashSet<dduroc_format::MetricId>>>,
}

/// Отобрать сегменты, которые могут содержать записи из диапазона.
///
/// Имя сегмента несёт время его **первой** записи, поэтому верхняя граница
/// отсекается точно: сегмент, начавшийся позже `to`, не нужен заведомо.
///
/// Нижнюю границу так отсечь нельзя: сегмент мог начаться раньше `from` и
/// содержать нужные записи. Отбрасывается только тот, за которым идёт
/// сегмент того же run'а, начинающийся **строго раньше** `from`, — тогда
/// все записи первого лежат до начала второго, то есть до `from`.
///
/// Сравнение именно строгое. При `next.base == from` последняя запись
/// текущего сегмента может иметь время ровно `from`: часы монотонны, но не
/// строго возрастают, и во всплеске два соседних события получают одну и ту
/// же микросекунду. Нестрогое сравнение выбрасывало бы такую запись из
/// выборки, которая её включает.
fn select_segments(
    all: &[SegmentName],
    from: Option<Micros>,
    to: Option<Micros>,
    boot: Option<u32>,
) -> Vec<SegmentName> {
    let mut segments = Vec::new();
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
            && next.base < from
            && next.boot == name.boot
        {
            continue;
        }
        segments.push(*name);
    }
    segments
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

        let mut segments = select_segments(&all, from, to, boot);
        if reverse {
            segments.reverse();
        }
        // Граница просмотра применяется ПОСЛЕ разворота: смысл её — «столько
        // сегментов от начала обхода», а обход у обратного порядка идёт от
        // свежих к старым.
        if let Some(k) = scope.max_segments {
            segments.truncate(k);
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
            require_metrics: scope.require_metrics.clone(),
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
                    // Сегмент, в котором заведомо нет нужных метрик, не
                    // читается вовсе: множество идентификаторов лежит в
                    // footer'е, и ответ получается без единого чтения блока.
                    if let Some(wanted) = &self.require_metrics
                        && c.contains_any_metric(wanted) == Some(false)
                    {
                        continue;
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::BootCounter;

    fn seg(boot: u32, base: u64) -> SegmentName {
        SegmentName::new(BootCounter(boot), Micros(base))
    }

    fn bases(v: &[SegmentName]) -> Vec<u64> {
        v.iter().map(|n| n.base.0).collect()
    }

    #[test]
    fn lower_bound_keeps_the_segment_that_may_hold_it() {
        // Три сегмента одного запуска: [0..100), [100..200), [200..).
        let all = [seg(0, 0), seg(0, 100), seg(0, 200)];

        // from ровно на границе. Последняя запись первого сегмента может
        // иметь время ровно 100: часы монотонны, но не строго возрастают, и
        // во всплеске два соседних события получают одну микросекунду.
        // Отбросить первый сегмент значило бы потерять эту запись.
        assert_eq!(
            bases(&select_segments(&all, Some(Micros(100)), None, None)),
            vec![0, 100, 200],
            "сегмент, чья последняя запись может лежать ровно на границе, нужен"
        );

        // from строго внутри второго: первый заведомо весь позади.
        assert_eq!(
            bases(&select_segments(&all, Some(Micros(101)), None, None)),
            vec![100, 200]
        );
        assert_eq!(
            bases(&select_segments(&all, Some(Micros(250)), None, None)),
            vec![200]
        );
        // Позже всех данных: последний сегмент всё равно проверяется — он
        // открыт и мог получить записи после составления инвентаря.
        assert_eq!(
            bases(&select_segments(&all, Some(Micros(9_999)), None, None)),
            vec![200]
        );
    }

    #[test]
    fn upper_bound_is_exact() {
        let all = [seg(0, 0), seg(0, 100), seg(0, 200)];
        // Имя несёт время первой записи, поэтому сегмент, начавшийся позже
        // `to`, не нужен заведомо. Начавшийся ровно на `to` — нужен.
        assert_eq!(
            bases(&select_segments(&all, None, Some(Micros(100)), None)),
            vec![0, 100]
        );
        assert_eq!(
            bases(&select_segments(&all, None, Some(Micros(99)), None)),
            vec![0]
        );
        assert!(select_segments(&all, None, Some(Micros(0)), None).len() == 1);
    }

    #[test]
    fn boot_filter_and_run_boundary() {
        // Нижняя граница не должна отбрасывать сегмент через границу запуска:
        // время у разных run'ов своё, и «следующий начался раньше» там
        // ничего не означает.
        let all = [seg(0, 500), seg(1, 10), seg(1, 900)];
        assert_eq!(
            bases(&select_segments(&all, Some(Micros(400)), None, None)),
            vec![500, 10, 900],
            "сегмент run'а 0 не отбрасывается по времени run'а 1"
        );

        assert_eq!(
            bases(&select_segments(&all, None, None, Some(1))),
            vec![10, 900]
        );
        assert!(select_segments(&all, None, None, Some(7)).is_empty());
    }

    #[test]
    fn empty_and_single() {
        assert!(select_segments(&[], Some(Micros(5)), Some(Micros(9)), None).is_empty());
        let one = [seg(0, 100)];
        // Единственный сегмент не отбрасывается никогда: за ним ничего нет,
        // и его верхняя граница неизвестна без чтения.
        assert_eq!(
            bases(&select_segments(&one, Some(Micros(u64::MAX)), None, None)),
            vec![100]
        );
    }
}
