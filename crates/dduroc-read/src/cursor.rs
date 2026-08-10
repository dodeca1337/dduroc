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
use dduroc_engine::migrate::{self, Chained};
use dduroc_engine::schema::{Migration, Schema};
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

/// Версия схемы и её шаги — всё, что нужно курсору, чтобы привести записи
/// старых сегментов к текущей раскладке.
///
/// Не целая [`Schema`]: курсору не нужны ни декодеры, ни имена — только
/// версия и цепочка, и оба поля `'static`, так что контекст копируется.
#[derive(Debug, Clone, Copy)]
pub struct MigrationCtx {
    pub current: u16,
    pub steps: &'static [Migration],
}

impl MigrationCtx {
    pub fn of(schema: &Schema) -> Self {
        Self {
            current: schema.version.0,
            steps: schema.migrations,
        }
    }
}

/// Что курсор делает с записями этого сегмента.
enum MigrationState {
    /// Ничего: сегмент текущей версии либо не затронут ни одним шагом.
    None,
    /// Прогонять каждую запись через цепочку.
    Chain(Vec<&'static Migration>),
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
    /// Приведение записей к текущей версии схемы.
    migration: MigrationState,
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
        migrations: Option<MigrationCtx>,
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

        // Сегмент старой версии читается через цепочку шагов миграции: без
        // неё текущие декодеры разобрали бы старую раскладку молча и неверно
        // (postcard не самоописуем — i8 читается как хвост f32, без ошибки).
        // Сегмент, который цепочка не затрагивает (по footer'у), идёт как
        // есть; сегмент версии НОВЕЕ схемы не читается вовсе — раскладку из
        // будущего разбирать нечем, и молчание здесь выглядело бы как «прибор
        // ничего не писал».
        let seg_version = reader.header().protocol_version.0;
        let mut migration = MigrationState::None;
        if let Some(ctx) = migrations {
            if seg_version > ctx.current {
                damaged.push(Damage {
                    path: path.to_owned(),
                    offset: 0,
                    reason: format!(
                        "версия протокола {seg_version} новее схемы ({}): записи этой \
                         раскладки этому билду не разобрать",
                        ctx.current
                    ),
                });
                return Ok(Self::empty(reader, path, reverse, damaged));
            }
            if seg_version < ctx.current {
                match migrate::chain_between(ctx.steps, seg_version, ctx.current) {
                    Ok(steps) => {
                        let untouched = reader
                            .footer()
                            .is_some_and(|f| !migrate::chain_touches(&steps, &f));
                        if !untouched {
                            migration = MigrationState::Chain(steps);
                        }
                    }
                    Err(e) => {
                        // Дыра в цепочке: читать старую раскладку текущими
                        // декодерами нельзя, а привести её нечем.
                        damaged.push(Damage {
                            path: path.to_owned(),
                            offset: 0,
                            reason: format!("сегмент версии {seg_version} не прочитать: {e}"),
                        });
                        return Ok(Self::empty(reader, path, reverse, damaged));
                    }
                }
            }
        }
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
            migration,
            damaged,
        })
    }

    /// Курсор, который не отдаст ни записи, — для сегментов, которые нельзя
    /// разбирать. Повреждения при этом доносятся как обычно.
    fn empty(reader: SegmentReader, path: &Path, reverse: bool, damaged: Vec<Damage>) -> Self {
        Self {
            reader,
            path: path.to_owned(),
            offsets: Vec::new(),
            next_block: 0,
            buffered: Vec::new(),
            pos: 0,
            reverse,
            prefilter: None,
            migration: MigrationState::None,
            damaged,
        }
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
        // Сегмент, который переписывает цепочка миграций, по footer'у не
        // судится: множества описывают ДОмиграционные идентификаторы, а
        // `wanted` — текущие. Ремапнутая метрика лежала бы в сегменте под
        // старым номером, и ответ «нет» отбросил бы сегмент вместе с ней.
        if matches!(self.migration, MigrationState::Chain(_)) {
            return None;
        }
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

    /// Отбросить блоки, целиком лежащие вне окна.
    ///
    /// Границы блоков известны из footer'а, поэтому отбор идёт без чтения тел
    /// — ради этого footer и существует.
    ///
    /// Обе границы и оба направления. Прежде пропуск работал только по нижней
    /// границе и только в прямом порядке, а порядок по умолчанию у запроса —
    /// [`crate::Order::Newest`]: индекс блоков не работал в самом частом
    /// сценарии вовсе, и «последние сто записей» читали сегмент целиком.
    ///
    /// Вызывается один раз, до начала обхода: окно вырезается из списка
    /// смещений, а не запоминается отдельным состоянием.
    pub fn seek_bounds(&mut self, from: Option<Micros>, to: Option<Micros>) {
        debug_assert_eq!(self.next_block, 0, "окно вырезается до начала обхода");
        if from.is_none() && to.is_none() {
            return;
        }
        let Some(footer) = self.reader.footer() else {
            return;
        };
        let total = footer.blocks.len();
        debug_assert_eq!(total, self.offsets.len(), "смещения взяты из footer'а");

        // Нижняя граница: блок, который МОЖЕТ содержать `from`, — последний с
        // базой не позже него; всё, что перед ним, целиком в прошлом.
        let lo = from.map_or(0, |t| footer.block_for_time(t).unwrap_or(0));
        // Верхняя: база блока — время его первой записи, поэтому блок,
        // начавшийся позже `to`, состоит из записей ещё более поздних.
        let hi = to.map_or(total, |t| footer.blocks.partition_point(|b| b.base <= t));

        if lo >= hi {
            self.offsets.clear();
            return;
        }
        // При обратном обходе `offsets` уже развёрнут: окно [lo, hi) прямого
        // порядка — это [total - hi, total - lo) в порядке обхода.
        let (head, tail) = if self.reverse {
            (total - hi, total - lo)
        } else {
            (lo, hi)
        };
        self.offsets.truncate(tail);
        self.offsets.drain(..head);
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
            // Отказы миграции копятся на блок, а не сыплются по записи:
            // они систематические (весь блок одной раскладки), и тысяча
            // одинаковых записей о повреждении хуже одной со счётчиком.
            let mut unmigrated: u32 = 0;
            let mut first_chain_error = None;
            for item in block.records() {
                match item {
                    Ok((at, record)) => {
                        // Запись старого сегмента сперва приводится к текущей
                        // раскладке: фильтры и владеющая копия обязаны видеть
                        // то, что увидит вызывающий, а не сырьё с диска.
                        let migrated = match &self.migration {
                            MigrationState::None => Chained::Same(record),
                            MigrationState::Chain(steps) => match migrate::apply(steps, record) {
                                Ok(m) => m,
                                Err(e) => {
                                    unmigrated += 1;
                                    first_chain_error.get_or_insert(e);
                                    continue;
                                }
                            },
                        };
                        let Some(record) = migrated.record() else {
                            // Запись удалена шагом — это решение схемы, а не
                            // потеря: в отчёте о повреждениях ей не место.
                            continue;
                        };
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
            if unmigrated > 0 {
                let e = first_chain_error.expect("счётчик растёт вместе с ошибкой");
                self.damaged.push(Damage {
                    path: self.path.clone(),
                    offset,
                    reason: format!("{e}: записей не приведено к текущей версии: {unmigrated}"),
                });
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
    migrations: Option<MigrationCtx>,
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
    /// Версия схемы и шаги миграции: сегменты прежних версий читаются через
    /// цепочку. `None` — схема неизвестна, записи идут как есть.
    pub migrations: Option<MigrationCtx>,
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
            migrations: scope.migrations,
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
                self.migrations,
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
                    // Границы — в шкале того запуска, которому принадлежит
                    // сегмент: микросекунды разных запусков не сравнимы.
                    if let Some(run) = self.bounds.for_boot(c.boot()) {
                        c.seek_bounds(run.from, run.to);
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

    /// Запечатанный сегмент, в котором каждый блок — одна запись с указанным
    /// временем. Блоки нужны поштучно: проверяется именно отбор блоков.
    /// Запечатанный сегмент указанной версии из готовых записей: одна запись
    /// — один блок, footer собирается как у writer'а (типы вместе с блоком).
    fn sealed_versioned(dir: &Path, version: u16, records: &[(u64, Record<'_>)]) -> PathBuf {
        use dduroc_engine::segment::SegmentWriter;
        use dduroc_format::block::BlockBuilder;
        use dduroc_format::segment::SegmentHeader;
        use dduroc_format::{Compression, FooterBuilder, ProtocolVersion};

        let base = Micros(records[0].0);
        let header = SegmentHeader {
            protocol_version: ProtocolVersion(version),
            boot: BootCounter(0),
            base,
            store_id: 0,
        };
        let mut seg = SegmentWriter::create(dir, header, 1 << 20).unwrap();
        let mut footer = FooterBuilder::new();
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        for (t, rec) in records {
            match rec {
                Record::Message(m) => footer.add_event(m.event),
                Record::Sample(s) => footer.add_metric(s.metric),
                _ => {}
            }
            builder.push(Micros(*t), rec).unwrap();
            out.clear();
            let h = builder
                .finish(seg.next_seq(), Compression::None, &mut out)
                .unwrap();
            let offset = seg.append_block(&out).unwrap();
            footer.add_block(offset, &h, Micros(*t));
        }
        seg.seal(&footer.build()).unwrap();
        dir.join(SegmentName::new(BootCounter(0), base).to_string())
    }

    fn sealed_segment(dir: &Path, times: &[u64]) -> PathBuf {
        use dduroc_engine::segment::SegmentWriter;
        use dduroc_format::block::BlockBuilder;
        use dduroc_format::record::Message;
        use dduroc_format::segment::SegmentHeader;
        use dduroc_format::{Compression, EventId, FooterBuilder, ProtocolVersion, Record};

        let base = Micros(times[0]);
        let header = SegmentHeader {
            protocol_version: ProtocolVersion(1),
            boot: BootCounter(0),
            base,
            store_id: 0,
        };
        let mut seg = SegmentWriter::create(dir, header, 1 << 20).unwrap();
        let mut footer = FooterBuilder::new();
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        for &t in times {
            builder
                .push(
                    Micros(t),
                    &Record::Message(Message {
                        event: EventId(1),
                        span: None,
                        payload: &[],
                    }),
                )
                .unwrap();
            out.clear();
            let h = builder
                .finish(seg.next_seq(), Compression::None, &mut out)
                .unwrap();
            let offset = seg.append_block(&out).unwrap();
            footer.add_block(offset, &h, Micros(t));
        }
        seg.seal(&footer.build()).unwrap();
        dir.join(SegmentName::new(BootCounter(0), base).to_string())
    }

    #[test]
    fn an_old_segment_is_read_through_the_migration_chain() {
        // Сегмент v1 при схеме v2: изменённый тип перекодируется, удалённый
        // выпадает, ремапнутая метрика меняет номер, нетронутый тип идёт
        // байт в байт. Без цепочки текущие декодеры разобрали бы старую
        // раскладку молча и неверно — postcard не самоописуем.
        use dduroc_engine::schema::{DecodeError, MigratedRecord, OwnedRecord as Out};
        use dduroc_format::record::{Message, Sample};
        use dduroc_format::{EventId, MetricId, Value};

        fn step1(r: MigratedRecord<'_>) -> std::result::Result<Option<Out>, DecodeError> {
            match (r.event_id(), r.metric_id()) {
                (Some(EventId(1)), _) => {
                    let old: (u8,) = r.decode()?;
                    Ok(Some(Out::Message {
                        event: EventId(1),
                        payload: postcard::to_allocvec(&(u16::from(old.0) * 2,)).unwrap(),
                    }))
                }
                (Some(EventId(2)), _) => Ok(None),
                (_, Some(MetricId(0x10))) => Ok(Some(Out::SampleMetric(MetricId(0x20)))),
                _ => Ok(Some(Out::AsIs)),
            }
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            events: &[EventId(1), EventId(2)],
            metrics: &[MetricId(0x10)],
            migrate: step1,
        }];
        let ctx = MigrationCtx {
            current: 2,
            steps: STEPS,
        };

        let dir = tempfile::tempdir().unwrap();
        let changed = postcard::to_allocvec(&(21u8,)).unwrap();
        let path = sealed_versioned(
            dir.path(),
            1,
            &[
                (
                    100,
                    Record::Message(Message {
                        event: EventId(1),
                        span: None,
                        payload: &changed,
                    }),
                ),
                (
                    200,
                    Record::Message(Message {
                        event: EventId(2),
                        span: None,
                        payload: &[7],
                    }),
                ),
                (
                    300,
                    Record::Message(Message {
                        event: EventId(3),
                        span: None,
                        payload: &[9],
                    }),
                ),
                (
                    400,
                    Record::Sample(Sample {
                        metric: MetricId(0x10),
                        value: Value::U64(555),
                    }),
                ),
            ],
        );

        let mut c = SegmentCursor::open(&path, false, None, None, Some(ctx)).unwrap();
        let got: Vec<RawEntry> = std::iter::from_fn(|| c.next_entry()).collect();
        assert!(c.damaged().is_empty(), "{:?}", c.damaged());

        assert_eq!(got.len(), 3, "удалённый тип выпал: {got:?}");
        match &got[0].record {
            OwnedRecord::Message { event, payload, .. } => {
                assert_eq!(*event, EventId(1));
                let v: (u16,) = postcard::from_bytes(payload).unwrap();
                assert_eq!(v.0, 42, "payload перекодирован из старой раскладки");
            }
            other => panic!("{other:?}"),
        }
        match &got[1].record {
            OwnedRecord::Message { event, payload, .. } => {
                assert_eq!(*event, EventId(3));
                assert_eq!(payload, &[9], "нетронутый тип — байт в байт");
            }
            other => panic!("{other:?}"),
        }
        match &got[2].record {
            OwnedRecord::Sample { metric, value } => {
                assert_eq!(*metric, MetricId(0x20), "метрика ремапнута");
                assert_eq!(*value, OwnedSampleValue::U64(555), "значение исходное");
            }
            other => panic!("{other:?}"),
        }

        // Тот же сегмент, записанный текущей версией, цепочку не проходит:
        // payload события 1 остаётся в НОВОЙ раскладке как есть.
        let dir2 = tempfile::tempdir().unwrap();
        let fresh = postcard::to_allocvec(&(42u16,)).unwrap();
        let path = sealed_versioned(
            dir2.path(),
            2,
            &[(
                100,
                Record::Message(Message {
                    event: EventId(1),
                    span: None,
                    payload: &fresh,
                }),
            )],
        );
        let mut c = SegmentCursor::open(&path, false, None, None, Some(ctx)).unwrap();
        match &c.next_entry().unwrap().record {
            OwnedRecord::Message { payload, .. } => {
                assert_eq!(payload, &fresh, "текущая версия не трогается");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_segment_from_the_future_is_named_not_misread() {
        // Дамп с прибора с новой прошивкой в старом вьюере: раскладку из
        // будущего разбирать нечем, и прочитать её текущими декодерами
        // значило бы показать мусор за данные. Сегмент выпадает целиком — с
        // объявлением, а не молча.
        use dduroc_format::EventId;
        use dduroc_format::record::Message;

        let dir = tempfile::tempdir().unwrap();
        let path = sealed_versioned(
            dir.path(),
            5,
            &[(
                100,
                Record::Message(Message {
                    event: EventId(1),
                    span: None,
                    payload: &[1],
                }),
            )],
        );

        let ctx = MigrationCtx {
            current: 2,
            steps: &[],
        };
        let mut c = SegmentCursor::open(&path, false, None, None, Some(ctx)).unwrap();
        assert!(c.next_entry().is_none(), "записей из будущего не выдаётся");
        let damaged = c.damaged();
        assert_eq!(damaged.len(), 1);
        assert!(
            damaged[0].reason.contains("новее схемы"),
            "причина названа: {}",
            damaged[0].reason
        );

        // Без контекста миграций (схемы нет вовсе) поведение прежнее: записи
        // выдаются как есть — их разбирает тот, у кого схема есть.
        let mut c = SegmentCursor::open(&path, false, None, None, None).unwrap();
        assert!(c.next_entry().is_some());
    }

    #[test]
    fn a_step_that_cannot_decode_reports_one_damage_per_block() {
        // Отказ шага — систематический: весь блок одной раскладки. Тысяча
        // одинаковых записей о повреждении хуже одной со счётчиком — но и
        // молчание недопустимо: запись выпала из ответа.
        use dduroc_engine::schema::{DecodeError, MigratedRecord, OwnedRecord as Out};
        use dduroc_format::EventId;
        use dduroc_format::record::Message;

        fn broken(r: MigratedRecord<'_>) -> std::result::Result<Option<Out>, DecodeError> {
            let _: (u64, u64, u64, u64) = r.decode()?;
            Ok(Some(Out::AsIs))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            events: &[EventId(1)],
            metrics: &[],
            migrate: broken,
        }];

        let dir = tempfile::tempdir().unwrap();
        let path = sealed_versioned(
            dir.path(),
            1,
            &[
                (
                    100,
                    Record::Message(Message {
                        event: EventId(1),
                        span: None,
                        payload: &[1],
                    }),
                ),
                (
                    200,
                    Record::Message(Message {
                        event: EventId(9),
                        span: None,
                        payload: &[2],
                    }),
                ),
            ],
        );
        let ctx = MigrationCtx {
            current: 2,
            steps: STEPS,
        };
        let mut c = SegmentCursor::open(&path, false, None, None, Some(ctx)).unwrap();
        let got: Vec<RawEntry> = std::iter::from_fn(|| c.next_entry()).collect();
        assert_eq!(got.len(), 1, "уцелевшая запись читается");
        let damaged = c.damaged();
        assert_eq!(damaged.len(), 1, "одна запись о повреждении: {damaged:?}");
        assert!(
            damaged[0].reason.contains("шаг миграции 1 → 2") && damaged[0].reason.contains(": 1"),
            "виновник и счётчик названы: {}",
            damaged[0].reason
        );
    }

    #[test]
    fn the_block_index_works_in_both_directions_and_on_both_bounds() {
        // Порядок по умолчанию у запроса — `Order::Newest`, а пропуск блоков
        // по footer'у работал только по нижней границе и только в прямом
        // обходе. То есть индекс, ради которого footer и существует, в самом
        // частом сценарии не работал вовсе: «последние сто записей» читали
        // сегмент целиком, блок за блоком.
        let dir = tempfile::tempdir().unwrap();
        let times: Vec<u64> = (0..20).map(|i| 100 + i * 10).collect();
        let path = sealed_segment(dir.path(), &times);

        let read = |reverse: bool, from: Option<u64>, to: Option<u64>| -> Vec<u64> {
            let mut c = SegmentCursor::open(&path, reverse, None, None, None).unwrap();
            assert_eq!(c.offsets.len(), times.len(), "иначе тест не про отбор");
            c.seek_bounds(from.map(Micros), to.map(Micros));
            std::iter::from_fn(|| c.next_entry())
                .map(|e| e.at.at.0)
                .collect()
        };

        // Окно [150, 200] — шесть блоков из двадцати, и ни одного лишнего.
        // Курсор не фильтрует записи по времени: всё, что он отдал, он и
        // прочитал с диска.
        assert_eq!(
            read(false, Some(150), Some(200)),
            vec![150, 160, 170, 180, 190, 200]
        );
        assert_eq!(
            read(true, Some(150), Some(200)),
            vec![200, 190, 180, 170, 160, 150]
        );

        // Одна граница из двух — тоже граница.
        assert_eq!(read(true, None, Some(120)), vec![120, 110, 100]);
        assert_eq!(read(true, Some(270), None), vec![290, 280, 270]);

        // Окно внутри одного блока оставляет ровно его: база — время ПЕРВОЙ
        // записи блока, и что лежит дальше внутри него, без чтения не узнать.
        // Отбросить его значило бы потерять записи, а не сэкономить чтение.
        assert_eq!(read(true, Some(151), Some(159)), vec![150]);
        assert_eq!(read(false, Some(1_000), None), vec![290]);
        // А вот окно, кончающееся раньше первого блока, не оставляет ничего.
        assert!(read(true, None, Some(50)).is_empty());
        // Без границ обход полный.
        assert_eq!(read(true, None, None).len(), times.len());
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
