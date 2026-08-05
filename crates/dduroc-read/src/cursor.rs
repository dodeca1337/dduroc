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
use crate::query::{Bounds, Fit};
use dduroc_engine::segment::{SegmentReader, parse_block};
use dduroc_format::segment::SegmentName;
use dduroc_format::{BootCounter, BootTime, Micros, Record};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Предикат отбора, применяемый **до** материализации записи.
///
/// Владеющая копия записи стоит аллокации payload'а, поэтому запрос вроде
/// «только ошибки» не должен её платить за каждую из сотен тысяч
/// отфильтрованных записей. Определения серий пропускаются всегда: без них
/// не восстановить идентичность сэмплов.
pub type Prefilter = Arc<dyn Fn(&Record<'_>) -> bool + Send + Sync>;

/// Одна прочитанная запись.
///
/// Время — целиком: микросекунды пришли из записи, запуск — из заголовка
/// сегмента, и порознь они не сравнимы.
#[derive(Debug, Clone)]
pub struct RawEntry {
    pub at: BootTime,
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
                // Скан заодно ловит разрыв нумерации блоков: тела разбирать
                // для этого не нужно, номер лежит в заголовке.
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

    pub fn boot(&self) -> BootCounter {
        self.reader.header().boot
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

            let boot = self.reader.header().boot;
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
                            at: BootTime::new(boot, at),
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
    bounds: Bounds,
    expect_store: Option<u64>,
    prefilter: Option<Prefilter>,
    require_metrics: Option<Arc<std::collections::HashSet<dduroc_format::MetricId>>>,
    damaged: Vec<Damage>,
    /// Запуски, чьи сегменты пришлось пропустить: окно настенное, якоря нет.
    unanchored: Vec<BootCounter>,
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
    /// Окно, уже приведённое к относительной шкале запусков: настенные
    /// границы переводить по якорям — дело запроса, а не курсора.
    pub bounds: Bounds,
    pub boot: Option<BootCounter>,
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

/// Отобрать сегменты, которые могут содержать записи из окна.
///
/// Границы берутся отдельно на каждый запуск: сравнивать микросекунды разных
/// запусков нельзя, а запуск, которого в окне нет вовсе, отбрасывается целиком
/// — без открытия его файлов.
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
    bounds: &Bounds,
    boot: Option<BootCounter>,
    unanchored: &mut Vec<BootCounter>,
) -> Vec<SegmentName> {
    let mut segments = Vec::new();
    for (i, name) in all.iter().enumerate() {
        if let Some(b) = boot
            && name.boot != b
        {
            continue;
        }
        let run = match bounds.fit(name.boot) {
            Fit::In(run) => run,
            Fit::Outside => continue,
            // Данные есть, но приложить их к настенному окну нечем. Молчание
            // здесь выглядело бы как «в эти часы прибор ничего не писал».
            Fit::Unanchored => {
                if !unanchored.contains(&name.boot) {
                    unanchored.push(name.boot);
                }
                continue;
            }
        };
        if let Some(to) = run.to
            && name.base > to
        {
            continue;
        }
        if let Some(from) = run.from
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
        let (boot, reverse, expect_store) = (scope.boot, scope.reverse, scope.expect_store);
        // Только имена: размеры сегментов стоят `stat` на файл, а отбор по
        // окну идёт по именам — время первой записи в них и лежит.
        let all = dduroc_engine::rotation::Inventory::scan_names(dir).map_err(ReadError::Engine)?;

        let mut unanchored = Vec::new();
        let mut segments = select_segments(&all, &scope.bounds, boot, &mut unanchored);
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
            bounds: scope.bounds.clone(),
            expect_store,
            prefilter: scope.prefilter.clone(),
            require_metrics: scope.require_metrics.clone(),
            damaged: Vec::new(),
            unanchored,
            namespace,
            channel,
        })
    }

    /// Фрагменты канала, которые не удалось прочитать.
    ///
    /// Включает и повреждения сегмента, который **сейчас читается**. Без
    /// этого они всплывали бы только в `finish_current`, то есть когда
    /// сегмент дочитан до конца, — а обход обрывается по `limit` и по
    /// выходу из `stream` посреди сегмента. Пропущенный блок исчезал бы из
    /// отчёта, и `QueryResult::is_complete()` объявлял бы полным ответ, из
    /// которого выпали данные.
    pub fn damaged(&self) -> Vec<Damage> {
        let mut out = self.damaged.clone();
        if let Some(c) = &self.current {
            out.extend_from_slice(c.damaged());
        }
        out
    }

    /// Запуски, чьи сегменты лежат в этом канале, но в выборку не попали:
    /// окно задано настенным временем, а якоря у них нет.
    pub fn unanchored(&self) -> &[BootCounter] {
        &self.unanchored
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
                    // Нижняя граница — в шкале того запуска, которому
                    // принадлежит сегмент.
                    if let Some(from) = self.bounds.for_boot(c.boot()).and_then(|b| b.from) {
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
            .field("bounds", &self.bounds)
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
    use crate::query::Query;
    use dduroc_engine::epochs::Epochs;

    fn seg(boot: u32, base: u64) -> SegmentName {
        SegmentName::new(BootCounter(boot), Micros(base))
    }

    fn bases(v: &[SegmentName]) -> Vec<u64> {
        v.iter().map(|n| n.base.0).collect()
    }

    /// Отбор без интереса к выпавшим запускам — их проверяет отдельный тест.
    fn select(all: &[SegmentName], bounds: &Bounds, boot: Option<BootCounter>) -> Vec<SegmentName> {
        select_segments(all, bounds, boot, &mut Vec::new())
    }

    /// Границы одного запуска — так, как их построит запрос.
    fn within(boot: u32, from: Option<u64>, to: Option<u64>) -> Bounds {
        let mut q = Query::new();
        q.from = from.map(|m| BootTime::from_raw(boot, m).into());
        q.to = to.map(|m| BootTime::from_raw(boot, m).into());
        q.resolve(&Epochs::default()).bounds
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
            bases(&select(&all, &within(0, Some(100), None), None)),
            vec![0, 100, 200],
            "сегмент, чья последняя запись может лежать ровно на границе, нужен"
        );

        // from строго внутри второго: первый заведомо весь позади.
        assert_eq!(
            bases(&select(&all, &within(0, Some(101), None), None)),
            vec![100, 200]
        );
        assert_eq!(
            bases(&select(&all, &within(0, Some(250), None), None)),
            vec![200]
        );
        // Позже всех данных: последний сегмент всё равно проверяется — он
        // открыт и мог получить записи после составления инвентаря.
        assert_eq!(
            bases(&select(&all, &within(0, Some(9_999), None), None)),
            vec![200]
        );
    }

    #[test]
    fn upper_bound_is_exact() {
        let all = [seg(0, 0), seg(0, 100), seg(0, 200)];
        // Имя несёт время первой записи, поэтому сегмент, начавшийся позже
        // `to`, не нужен заведомо. Начавшийся ровно на `to` — нужен.
        assert_eq!(
            bases(&select(&all, &within(0, None, Some(100)), None)),
            vec![0, 100]
        );
        assert_eq!(
            bases(&select(&all, &within(0, None, Some(99)), None)),
            vec![0]
        );
        assert_eq!(select(&all, &within(0, None, Some(0)), None).len(), 1);
    }

    #[test]
    fn bounds_of_one_run_do_not_touch_another() {
        // Время у разных запусков своё, поэтому «следующий начался раньше»
        // через границу запуска ничего не означает.
        let all = [seg(0, 500), seg(1, 10), seg(1, 900)];
        assert_eq!(
            bases(&select(&all, &within(0, Some(400), None), None)),
            vec![500, 10, 900],
            "сегмент запуска 0 не отбрасывается по времени запуска 1"
        );

        // А вот граница в шкале запуска 1 отбрасывает запуск 0 целиком: он
        // весь позади. Раньше это было невыразимо — микросекунды без запуска
        // прикладывались к каждой шкале.
        assert_eq!(
            bases(&select(&all, &within(1, Some(400), None), None)),
            vec![10, 900]
        );
        assert_eq!(
            bases(&select(&all, &within(0, None, Some(600)), None)),
            vec![500],
            "верхняя граница запуска 0 отсекает весь запуск 1"
        );
        // Та же граница ниже старта единственного сегмента запуска 0 не
        // оставляет ничего: 500 > 400, а запуск 1 весь позже.
        assert!(select(&all, &within(0, None, Some(400)), None).is_empty());
    }

    #[test]
    fn boot_filter_selects_one_run() {
        let all = [seg(0, 500), seg(1, 10), seg(1, 900)];
        assert_eq!(
            bases(&select(&all, &Bounds::All, Some(BootCounter(1)))),
            vec![10, 900]
        );
        assert!(select(&all, &Bounds::All, Some(BootCounter(7))).is_empty());
    }

    #[test]
    fn segments_of_unanchored_runs_are_named_not_just_skipped() {
        // Настенное окно и пустой реестр эпох — дамп скопировали без
        // `epochs.bin`. Сегменты на диске есть, но приложить их к настенным
        // часам нечем; перечислить такие запуски можно только по каталогу.
        let all = [seg(0, 100), seg(0, 900), seg(3, 50)];
        let utc = chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let bounds = Query::new().since(utc).resolve(&Epochs::default()).bounds;

        let mut unanchored = Vec::new();
        let picked = select_segments(&all, &bounds, None, &mut unanchored);
        assert!(picked.is_empty(), "сопоставить нечем");
        assert_eq!(
            unanchored,
            vec![BootCounter(0), BootCounter(3)],
            "каждый запуск назван по разу"
        );

        // Относительное окно от эпох не зависит и ничего не теряет.
        let mut unanchored = Vec::new();
        let bounds = within(0, Some(0), None);
        assert_eq!(
            bases(&select_segments(&all, &bounds, None, &mut unanchored)),
            vec![100, 900, 50]
        );
        assert!(unanchored.is_empty());
    }

    #[test]
    fn empty_and_single() {
        assert!(select(&[], &within(0, Some(5), Some(9)), None).is_empty());
        let one = [seg(0, 100)];
        // Единственный сегмент не отбрасывается никогда: за ним ничего нет,
        // и его верхняя граница неизвестна без чтения.
        assert_eq!(
            bases(&select(&one, &within(0, Some(u64::MAX), None), None)),
            vec![100]
        );
    }
}
