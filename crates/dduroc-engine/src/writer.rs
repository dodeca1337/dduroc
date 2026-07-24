//! Writer: единственный поток, который пишет в файлы.
//!
//! # Почему один поток
//!
//! Сегмент — append-only файл: два писателя в него означали бы блокировку на
//! каждую запись. Один поток снимает вопрос вовсе, а батчинг превращает
//! всплеск событий в один блок и один `fdatasync`.
//!
//! # Две очереди, а не одна
//!
//! Критические и обычные записи разведены по разным очередям. С общей
//! очередью поток телеметрии в тысячи сэмплов в секунду вставал бы перед
//! аварийным сообщением — классическая инверсия приоритетов, при которой
//! «критический» канал критичен только на бумаге.
//!
//! - обычная очередь: при переполнении запись **отбрасывается**, счётчик
//!   растёт, а в поток попадает отметка о потере — дыра не должна быть
//!   неотличима от тишины;
//! - критическая: вызывающий **ждёт** места (с таймаутом). Критические
//!   события редки, поэтому ожидание практически недостижимо, но обещание
//!   «не потеряно» становится честным.
//!
//! # Чего writer не делает
//!
//! Он **не логирует через публичный API**. Освободить очередь может только он
//! сам, поэтому запись из его собственного потока — гарантированный
//! самоблок при заполненной очереди. Вся диагностика — атомарные счётчики
//! ([`crate::stats`]) и отметки, вставляемые прямо в поток записей.

use crate::channel::{ChannelConfig, Durability};
use crate::error::{Error, Result};
use crate::rotation::{Inventory, SegmentEntry};
use crate::segment::SegmentWriter;
use crate::staged::{ChannelIdx, NsId, SeriesEntry, SeriesId, Staged, StagedRecord};
use crate::stats::Counters;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use dduroc_format::block::{BlockBuilder, BlockHeader};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{
    BootCounter, FooterBuilder, Level, MetricId, Micros, ProtocolVersion, SeriesLocal,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Ёмкость очередей по умолчанию.
const NORMAL_QUEUE: usize = 8192;
const CRITICAL_QUEUE: usize = 1024;

/// Сколько записей writer забирает за один заход перед тем, как заняться
/// таймерами. Ограничение нужно, чтобы поток телеметрии не заморозил
/// обслуживание flush- и sync-дедлайнов.
const DRAIN_LIMIT: usize = 4096;

/// Максимальное ожидание места в критической очереди.
const BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(5);

/// Источник отметок о служебных событиях в потоке записей.
const DIAG_TARGET: &str = "dduroc";

// ════════════════════════════════════════════════════════════════════════════
// Команды
// ════════════════════════════════════════════════════════════════════════════

/// Управляющая команда. Идёт по той же очереди, что и записи соответствующего
/// приоритета, поэтому не может обогнать данные, находящиеся в полёте.
#[derive(Debug)]
enum Control {
    /// Зарегистрировать неймспейс.
    Register(Box<NsSetup>, Sender<Result<NsId>>),
    /// Вытолкнуть и синхронизировать всё накопленное неймспейсом.
    Sync(Option<NsId>, Sender<Result<()>>),
    /// Запечатать активные сегменты и завершить работу.
    Shutdown(Sender<()>),
}

/// Параметры регистрации неймспейса.
#[derive(Debug)]
pub struct NsSetup {
    pub name: String,
    pub dir: PathBuf,
    pub protocol_version: ProtocolVersion,
    pub store_id: u64,
    pub boot: BootCounter,
    pub channels: Vec<ChannelConfig>,
    pub series: Arc<RwLock<Vec<SeriesEntry>>>,
}

// ════════════════════════════════════════════════════════════════════════════
// Ручка
// ════════════════════════════════════════════════════════════════════════════

/// Ручка writer'а: то, чем пользуются прикладные потоки.
#[derive(Debug)]
pub struct Writer {
    normal: Sender<Staged>,
    critical: Sender<Staged>,
    control: Sender<Control>,
    /// Потери, о которых ещё не сделана отметка в потоке.
    pending_drops: Arc<Mutex<HashMap<(NsId, ChannelIdx), u64>>>,
    counters: Arc<Counters>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Writer {
    /// Запустить writer-поток.
    pub fn spawn(counters: Arc<Counters>) -> Result<Arc<Self>> {
        let (normal_tx, normal_rx) = crossbeam_channel::bounded(NORMAL_QUEUE);
        let (critical_tx, critical_rx) = crossbeam_channel::bounded(CRITICAL_QUEUE);
        let (control_tx, control_rx) = crossbeam_channel::bounded(64);
        let pending_drops = Arc::new(Mutex::new(HashMap::new()));

        let loop_state = WriterLoop {
            namespaces: Vec::new(),
            counters: Arc::clone(&counters),
            pending_drops: Arc::clone(&pending_drops),
            batch: Vec::new(),
        };

        let handle = std::thread::Builder::new()
            .name("dduroc-writer".to_owned())
            .spawn(move || loop_state.run(normal_rx, critical_rx, control_rx))
            .map_err(|source| Error::Io {
                context: "запуск writer-потока".to_owned(),
                source,
            })?;

        Ok(Arc::new(Self {
            normal: normal_tx,
            critical: critical_tx,
            control: control_tx,
            pending_drops,
            counters,
            handle: Mutex::new(Some(handle)),
        }))
    }

    /// Зарегистрировать неймспейс, получив его идентификатор.
    pub fn register(&self, setup: NsSetup) -> Result<NsId> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Register(Box::new(setup), tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }

    /// Поставить запись в очередь.
    ///
    /// `critical` выбирает поведение при переполнении: ожидание вместо потери.
    pub fn write(&self, item: Staged, critical: bool) -> Result<()> {
        if critical {
            self.write_critical(item)
        } else {
            self.write_normal(item)
        }
    }

    fn write_normal(&self, item: Staged) -> Result<()> {
        match self.normal.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                // Потеря учитывается адресно: отметка о ней должна попасть в
                // тот же канал, где образовалась дыра.
                Counters::bump(&self.counters.dropped);
                if let Ok(mut map) = self.pending_drops.lock() {
                    *map.entry((item.ns, item.channel)).or_insert(0) += 1;
                }
                Err(Error::QueueFull)
            }
            Err(TrySendError::Disconnected(_)) => Err(Error::WriterDead),
        }
    }

    fn write_critical(&self, item: Staged) -> Result<()> {
        match self.critical.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                Counters::bump(&self.counters.backpressure_waits);
                // Ждём с таймаутом: вечная блокировка на отказавшем носителе
                // подвесила бы прикладные потоки навсегда.
                match self.critical.send_timeout(item, BACKPRESSURE_TIMEOUT) {
                    Ok(()) => Ok(()),
                    Err(crossbeam_channel::SendTimeoutError::Timeout(item)) => {
                        Counters::bump(&self.counters.dropped);
                        if let Ok(mut map) = self.pending_drops.lock() {
                            *map.entry((item.ns, item.channel)).or_insert(0) += 1;
                        }
                        Err(Error::QueueFull)
                    }
                    Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                        Err(Error::WriterDead)
                    }
                }
            }
            Err(TrySendError::Disconnected(_)) => Err(Error::WriterDead),
        }
    }

    /// Вытолкнуть накопленное и дождаться `fdatasync`.
    ///
    /// `None` — все неймспейсы.
    pub fn sync(&self, ns: Option<NsId>) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Sync(ns, tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }

    /// Завершить работу: дописать, запечатать, дождаться потока.
    pub fn shutdown(&self) {
        let (tx, rx) = crossbeam_channel::bounded(1);
        if self.control.send(Control::Shutdown(tx)).is_ok() {
            let _ = rx.recv();
        }
        if let Ok(mut guard) = self.handle.lock()
            && let Some(h) = guard.take()
        {
            let _ = h.join();
        }
    }

    /// Жив ли writer-поток.
    pub fn is_alive(&self) -> bool {
        !self.control.is_full() || !self.normal.is_full()
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Состояние канала
// ════════════════════════════════════════════════════════════════════════════

struct ChannelState {
    config: ChannelConfig,
    dir: PathBuf,
    inventory: Inventory,
    /// Открытый сегмент. `None` — канал ещё ничего не писал либо закрыт
    /// по бездействию: пустой неймспейс не должен занимать ни файлового
    /// дескриптора, ни преаллоцированных байт.
    segment: Option<SegmentWriter>,
    builder: BlockBuilder,
    footer: FooterBuilder,
    /// Соответствие серий реестра сегментно-локальным номерам.
    /// Сбрасывается вместе со сменой сегмента — определения серий живут
    /// в том же сегменте, что и их сэмплы.
    series_map: HashMap<SeriesId, SeriesLocal>,
    /// Максимальное записанное время: время, ушедшее назад, подтягивается
    /// вперёд, чтобы индекс блоков оставался сортированным.
    last_time: Micros,
    block_opened: Option<Instant>,
    last_sync: Instant,
    last_activity: Instant,
    dirty_since_sync: bool,
}

impl ChannelState {
    fn new(dir: PathBuf, config: ChannelConfig) -> Result<Self> {
        let inventory = Inventory::scan(&dir)?;
        let now = Instant::now();
        Ok(Self {
            builder: BlockBuilder::with_capacity(config.block_max_bytes.min(64 * 1024)),
            config,
            dir,
            inventory,
            segment: None,
            footer: FooterBuilder::new(),
            series_map: HashMap::new(),
            last_time: Micros(0),
            block_opened: None,
            last_sync: now,
            last_activity: now,
            dirty_since_sync: false,
        })
    }

    /// Пора ли вытолкнуть неполный блок.
    fn flush_deadline(&self) -> Option<Instant> {
        self.block_opened.map(|t| t + self.config.flush_interval)
    }

    /// Пора ли синхронизировать.
    fn sync_deadline(&self) -> Option<Instant> {
        if !self.dirty_since_sync {
            return None;
        }
        self.config
            .durability
            .min_interval()
            .map(|d| self.last_sync + d)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Состояние неймспейса
// ════════════════════════════════════════════════════════════════════════════

struct NsState {
    #[allow(dead_code)]
    name: String,
    protocol_version: ProtocolVersion,
    store_id: u64,
    boot: BootCounter,
    channels: Vec<ChannelState>,
    series: Arc<RwLock<Vec<SeriesEntry>>>,
}

// ════════════════════════════════════════════════════════════════════════════
// Цикл writer'а
// ════════════════════════════════════════════════════════════════════════════

struct WriterLoop {
    namespaces: Vec<NsState>,
    counters: Arc<Counters>,
    pending_drops: Arc<Mutex<HashMap<(NsId, ChannelIdx), u64>>>,
    /// Переиспользуемый буфер батча.
    batch: Vec<Staged>,
}

impl WriterLoop {
    fn run(
        mut self,
        normal: Receiver<Staged>,
        critical: Receiver<Staged>,
        control: Receiver<Control>,
    ) {
        loop {
            // Критические записи забираются первыми и целиком: обычный поток
            // не имеет права задерживать аварийные сообщения.
            let mut got = self.drain(&critical);
            got += self.drain(&normal);

            if got > 0 {
                self.apply_batch();
            }

            match self.poll_control(&control) {
                ControlOutcome::Continue => {}
                ControlOutcome::Stop => break,
            }

            if got == 0 {
                // Ждём либо новую запись, либо ближайший дедлайн.
                let timeout = self.next_deadline().unwrap_or(Duration::from_millis(250));
                crossbeam_channel::select! {
                    recv(critical) -> item => if let Ok(item) = item { self.batch.push(item); self.apply_batch(); },
                    recv(normal) -> item => if let Ok(item) = item { self.batch.push(item); self.apply_batch(); },
                    recv(control) -> cmd => match cmd {
                        Ok(cmd) => if matches!(self.handle_control(cmd), ControlOutcome::Stop) { break },
                        Err(_) => break,
                    },
                    default(timeout) => {}
                }
            }

            self.tick();
        }

        self.finish();
    }

    /// Забрать из очереди сколько получится, не превышая лимит.
    fn drain(&mut self, rx: &Receiver<Staged>) -> usize {
        let mut n = 0;
        while self.batch.len() < DRAIN_LIMIT {
            match rx.try_recv() {
                Ok(item) => {
                    self.batch.push(item);
                    n += 1;
                }
                Err(_) => break,
            }
        }
        n
    }

    /// Разложить батч по каналам.
    fn apply_batch(&mut self) {
        // Сортировка по времени внутри канала: записи от разных потоков
        // приходят переупорядоченными (поток взял метку и был вытеснен),
        // а блок и индекс должны остаться монотонными. Сортировка
        // устойчивая — SpanStart не обгонит одновременный SpanEnd.
        self.batch.sort_by_key(|s| (s.ns.0, s.channel.0, s.at.0));

        let batch = std::mem::take(&mut self.batch);
        for item in &batch {
            if let Err(e) = self.push(item) {
                Counters::bump(&self.counters.io_errors);
                // Логировать нельзя — очередь наша собственная. Считаем и
                // продолжаем: отказ одного канала не должен ронять остальные.
                debug_assert!(false, "ошибка записи: {e}");
                let _ = e;
            }
        }
        self.batch = batch;
        self.batch.clear();
    }

    fn push(&mut self, item: &Staged) -> Result<()> {
        let ns_idx = item.ns.0 as usize;
        let ch_idx = item.channel.0 as usize;
        if ns_idx >= self.namespaces.len() || ch_idx >= self.namespaces[ns_idx].channels.len() {
            return Ok(()); // неизвестный адрес — молча игнорируем
        }

        // Серию нужно разрешить до заимствования канала: справочник лежит
        // в неймспейсе, а канал берётся изменяемой ссылкой.
        let series_entry = match &item.record {
            StagedRecord::Sample { series, .. } => {
                let ns = &self.namespaces[ns_idx];
                let guard = ns.series.read().map_err(|_| Error::WriterDead)?;
                Some((*series, guard.get(series.0 as usize).cloned()))
            }
            _ => None,
        };

        let ns = &mut self.namespaces[ns_idx];
        let (protocol_version, store_id, boot) = (ns.protocol_version, ns.store_id, ns.boot);
        let ch = &mut ns.channels[ch_idx];

        // Блок открывается против конкретного сегмента: решать, куда он
        // ляжет, в момент выталкивания нельзя — определения серий уже
        // посчитаны против текущего сегмента и уехали бы в чужой.
        if ch.builder.is_empty() {
            Self::ensure_room(ch, protocol_version, store_id, boot, &self.counters)?;
            ch.block_opened = Some(Instant::now());
        }

        // Монотонность внутри канала: время из прошлого подтягивается вперёд.
        let at = Micros(item.at.0.max(ch.last_time.0));
        ch.last_time = at;

        let series_local = match series_entry {
            Some((series, Some(entry))) => Some(Self::intern_series(ch, series, &entry, at)?),
            Some((_, None)) => return Ok(()), // серия не зарегистрирована
            None => None,
        };

        let Some(record) = item.record.as_record(series_local) else {
            return Ok(());
        };
        ch.builder.push(at, &record)?;
        Counters::bump(&self.counters.records_written);
        if let Some(id) = item.event_id() {
            ch.footer.add_event(id);
        }
        ch.last_activity = Instant::now();
        ch.dirty_since_sync = true;

        if ch.builder.body_len() >= ch.config.block_max_bytes {
            Self::flush_block(ch, &self.counters)?;
        }
        // Критический канал синхронизирует немедленно: group commit забирает
        // всё, что успело накопиться, поэтому всплеск стоит одного fdatasync.
        if ch.config.durability == Durability::Immediate {
            Self::flush_block(ch, &self.counters)?;
            Self::sync_channel(ch, &self.counters)?;
        }
        Ok(())
    }

    /// Выдать серии сегментно-локальный номер, при необходимости записав
    /// её определение в текущий блок.
    fn intern_series(
        ch: &mut ChannelState,
        series: SeriesId,
        entry: &SeriesEntry,
        at: Micros,
    ) -> Result<SeriesLocal> {
        if let Some(local) = ch.series_map.get(&series) {
            return Ok(*local);
        }
        let local = SeriesLocal(ch.series_map.len() as u32);
        let tags = entry.tag_refs();
        let def = dduroc_format::Record::SeriesDef(dduroc_format::SeriesDef {
            series: local,
            metric: entry.metric,
            value_type: entry.value_type,
            tags: dduroc_format::Tags::Slice(&tags),
        });
        ch.builder.push(at, &def)?;
        ch.footer.add_series(entry.metric, entry.value_type, &tags);
        ch.series_map.insert(series, local);
        Ok(local)
    }

    /// Убедиться, что в активном сегменте хватит места на целый блок.
    fn ensure_room(
        ch: &mut ChannelState,
        protocol_version: ProtocolVersion,
        store_id: u64,
        boot: BootCounter,
        counters: &Counters,
    ) -> Result<()> {
        let need = ch.config.block_max_bytes as u64 + BlockHeader::SIZE as u64 * 2;

        if let Some(seg) = &ch.segment
            && seg.fits(need)
        {
            return Ok(());
        }
        if ch.segment.is_some() {
            Self::seal_segment(ch, counters)?;
        }
        Self::open_segment(ch, protocol_version, store_id, boot, counters)
    }

    fn open_segment(
        ch: &mut ChannelState,
        protocol_version: ProtocolVersion,
        store_id: u64,
        boot: BootCounter,
        counters: &Counters,
    ) -> Result<()> {
        // Имя сегмента — (boot, время его первой записи). Совпадение имён
        // возможно только при регрессе времени; сдвигаем на микросекунду,
        // чтобы не затереть существующий файл.
        let mut base = ch.last_time;
        for attempt in 0..64 {
            let header = SegmentHeader {
                protocol_version,
                boot,
                base,
                store_id,
            };
            match SegmentWriter::create(&ch.dir, header, ch.config.segment_bytes) {
                Ok(seg) => {
                    ch.inventory.push_newest(SegmentEntry {
                        name: SegmentName::new(boot, base),
                        size: ch.config.segment_bytes,
                    });
                    ch.segment = Some(seg);
                    ch.series_map.clear();
                    ch.footer.reset();
                    Counters::bump(&counters.segments_created);
                    // Новый сегмент занял место — освобождаем старые.
                    Self::rotate(ch, counters)?;
                    return Ok(());
                }
                Err(Error::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    base = Micros(base.0.saturating_add(1));
                    ch.last_time = base;
                }
                Err(e) if e.is_no_space() => {
                    // Место кончилось: удаляем самый старый сегмент и пробуем
                    // ещё раз. Без этого канал замер бы навсегда, хотя
                    // освободить место — его собственная задача.
                    if attempt > 8 {
                        return Err(e);
                    }
                    let freed = Self::rotate_one(ch, counters)?;
                    if !freed {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::Corrupt {
            path: ch.dir.clone(),
            reason: "не удалось подобрать имя для нового сегмента".to_owned(),
        })
    }

    fn flush_block(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        if ch.builder.is_empty() {
            return Ok(());
        }
        let Some(seg) = ch.segment.as_mut() else {
            return Ok(());
        };

        let mut out = Vec::new();
        let last = ch.builder.last().unwrap_or(ch.last_time);
        let header = ch
            .builder
            .finish(seg.next_seq(), ch.config.compression, &mut out)?;
        let offset = seg.append_block(&out)?;
        ch.footer.add_block(offset, &header, last);
        ch.block_opened = None;
        Counters::bump(&counters.blocks_written);
        Counters::add(&counters.bytes_written, out.len() as u64);
        Ok(())
    }

    fn sync_channel(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        if let Some(seg) = ch.segment.as_mut()
            && seg.is_dirty()
        {
            seg.sync()?;
            Counters::bump(&counters.syncs);
        }
        ch.last_sync = Instant::now();
        ch.dirty_since_sync = false;
        Ok(())
    }

    fn seal_segment(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        Self::flush_block(ch, counters)?;
        let Some(seg) = ch.segment.take() else {
            return Ok(());
        };
        let name = SegmentName::new(seg.header().boot, seg.header().base);
        let data_end = seg.data_end();
        let footer = ch.footer.build();
        let footer_len = footer.len() as u64;
        seg.seal(&footer)?;
        // Запечатанный файл обрезан до фактических данных — бюджет обязан
        // это учесть, иначе ротация считала бы преаллокацию вечной.
        ch.inventory.update_size(name, data_end + footer_len);
        ch.footer.reset();
        ch.series_map.clear();
        ch.dirty_since_sync = false;
        Counters::bump(&counters.segments_sealed);
        Ok(())
    }

    /// Удалить старые сегменты, пока канал не влезет в бюджет.
    fn rotate(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        let active = ch
            .segment
            .as_ref()
            .map(|s| SegmentName::new(s.header().boot, s.header().base));
        let removed = ch
            .inventory
            .enforce_budget(&ch.dir, ch.config.budget_bytes, active)?;
        Counters::add(&counters.segments_rotated, removed as u64);
        Ok(())
    }

    /// Удалить ровно один самый старый сегмент. Используется, когда на
    /// носителе кончилось место.
    fn rotate_one(ch: &mut ChannelState, counters: &Counters) -> Result<bool> {
        let active = ch
            .segment
            .as_ref()
            .map(|s| SegmentName::new(s.header().boot, s.header().base));
        let Some(oldest) = ch.inventory.oldest().cloned() else {
            return Ok(false);
        };
        if Some(oldest.name) == active {
            return Ok(false);
        }
        crate::fsutil::remove_synced(&oldest.path(&ch.dir))?;
        ch.inventory.remove(oldest.name);
        Counters::bump(&counters.segments_rotated);
        Ok(true)
    }

    /// Ближайший дедлайн среди всех каналов.
    fn next_deadline(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut best: Option<Instant> = None;
        for ns in &self.namespaces {
            for ch in &ns.channels {
                for d in [ch.flush_deadline(), ch.sync_deadline()]
                    .into_iter()
                    .flatten()
                {
                    best = Some(best.map_or(d, |b: Instant| b.min(d)));
                }
            }
        }
        best.map(|d| d.saturating_duration_since(now))
    }

    /// Обслужить дедлайны и отметки о потерях.
    fn tick(&mut self) {
        self.emit_drop_notices();

        let now = Instant::now();
        for ns_idx in 0..self.namespaces.len() {
            for ch_idx in 0..self.namespaces[ns_idx].channels.len() {
                let counters = Arc::clone(&self.counters);
                let ch = &mut self.namespaces[ns_idx].channels[ch_idx];

                let flush_due = ch.flush_deadline().is_some_and(|d| d <= now);
                if flush_due && let Err(_e) = Self::flush_block(ch, &counters) {
                    Counters::bump(&counters.io_errors);
                }
                let sync_due = ch.sync_deadline().is_some_and(|d| d <= now);
                if sync_due && let Err(_e) = Self::sync_channel(ch, &counters) {
                    Counters::bump(&counters.io_errors);
                }
            }
        }
    }

    /// Вставить в поток отметки о потерянных записях.
    ///
    /// Дыра, о которой нигде не сказано, неотличима от тишины — ровно тот
    /// дефект прототипа, ради которого здесь ведётся учёт.
    fn emit_drop_notices(&mut self) {
        let drops: Vec<((NsId, ChannelIdx), u64)> = {
            let Ok(mut map) = self.pending_drops.lock() else {
                return;
            };
            if map.is_empty() {
                return;
            }
            map.drain().collect()
        };

        for ((ns, channel), count) in drops {
            let at = self
                .namespaces
                .get(ns.0 as usize)
                .and_then(|n| n.channels.get(channel.0 as usize))
                .map_or(Micros(0), |c| c.last_time);
            let item = Staged {
                ns,
                channel,
                at,
                record: StagedRecord::Text {
                    level: Level::Error,
                    span: None,
                    target: Arc::from(DIAG_TARGET),
                    text: format!("потеряно записей: {count} (очередь переполнена)")
                        .into_boxed_str(),
                },
            };
            if self.push(&item).is_err() {
                Counters::bump(&self.counters.io_errors);
            }
        }
    }

    fn poll_control(&mut self, control: &Receiver<Control>) -> ControlOutcome {
        while let Ok(cmd) = control.try_recv() {
            if matches!(self.handle_control(cmd), ControlOutcome::Stop) {
                return ControlOutcome::Stop;
            }
        }
        ControlOutcome::Continue
    }

    fn handle_control(&mut self, cmd: Control) -> ControlOutcome {
        match cmd {
            Control::Register(setup, reply) => {
                let _ = reply.send(self.register(*setup));
                ControlOutcome::Continue
            }
            Control::Sync(ns, reply) => {
                let _ = reply.send(self.sync_all(ns));
                ControlOutcome::Continue
            }
            Control::Shutdown(reply) => {
                self.finish();
                let _ = reply.send(());
                ControlOutcome::Stop
            }
        }
    }

    fn register(&mut self, setup: NsSetup) -> Result<NsId> {
        let mut channels = Vec::with_capacity(setup.channels.len());
        for cfg in setup.channels {
            crate::fsutil::create_dir_all_synced(&setup.dir.join(&cfg.name))?;
            channels.push(ChannelState::new(setup.dir.join(&cfg.name), cfg)?);
        }
        let id = NsId(self.namespaces.len() as u32);
        self.namespaces.push(NsState {
            name: setup.name,
            protocol_version: setup.protocol_version,
            store_id: setup.store_id,
            boot: setup.boot,
            channels,
            series: setup.series,
        });
        Ok(id)
    }

    fn sync_all(&mut self, only: Option<NsId>) -> Result<()> {
        let counters = Arc::clone(&self.counters);
        let mut first_error = None;
        for (idx, ns) in self.namespaces.iter_mut().enumerate() {
            if let Some(NsId(want)) = only
                && want as usize != idx
            {
                continue;
            }
            for ch in &mut ns.channels {
                let r = Self::flush_block(ch, &counters)
                    .and_then(|()| Self::sync_channel(ch, &counters));
                if let Err(e) = r {
                    Counters::bump(&counters.io_errors);
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// Финальное закрытие: дописать и запечатать всё.
    fn finish(&mut self) {
        let counters = Arc::clone(&self.counters);
        for ns in &mut self.namespaces {
            for ch in &mut ns.channels {
                if Self::seal_segment(ch, &counters).is_err() {
                    Counters::bump(&counters.io_errors);
                }
            }
        }
    }
}

enum ControlOutcome {
    Continue,
    Stop,
}

/// Операции над реестром серий неймспейса.
///
/// Реестр — просто разделяемый вектор: серия регистрируется один раз при
/// открытии, дальше сэмпл ссылается на неё номером.
#[derive(Debug)]
pub struct SeriesRegistry;

impl SeriesRegistry {
    /// Создать пустой реестр.
    pub fn new_shared() -> Arc<RwLock<Vec<SeriesEntry>>> {
        Arc::new(RwLock::new(Vec::new()))
    }

    /// Найти или создать серию. Идентификаторы выдаются по порядку и не
    /// переиспользуются.
    pub fn intern(
        registry: &RwLock<Vec<SeriesEntry>>,
        metric: MetricId,
        value_type: dduroc_format::ValueType,
        tags: &[(&str, &str)],
    ) -> SeriesId {
        let matches = |e: &SeriesEntry| {
            e.metric == metric
                && e.tags.len() == tags.len()
                && e.tags
                    .iter()
                    .zip(tags)
                    .all(|((k, v), (tk, tv))| k == tk && v == tv)
        };

        if let Ok(guard) = registry.read()
            && let Some(pos) = guard.iter().position(matches)
        {
            return SeriesId(pos as u32);
        }

        let mut guard = match registry.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        // Повторная проверка: между освобождением read и взятием write
        // серию мог зарегистрировать другой поток.
        if let Some(pos) = guard.iter().position(matches) {
            return SeriesId(pos as u32);
        }
        guard.push(SeriesEntry {
            metric,
            value_type,
            tags: tags
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        });
        SeriesId((guard.len() - 1) as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::ValueType;

    #[test]
    fn series_registry_dedups_and_is_namespace_local() {
        let reg = SeriesRegistry::new_shared();
        let a = SeriesRegistry::intern(&reg, MetricId(1), ValueType::F32, &[("sensor", "pa")]);
        let b = SeriesRegistry::intern(&reg, MetricId(1), ValueType::F32, &[("sensor", "pa")]);
        assert_eq!(a, b, "та же серия — тот же идентификатор");

        let c = SeriesRegistry::intern(&reg, MetricId(1), ValueType::F32, &[("sensor", "lna")]);
        assert_ne!(a, c, "другие тэги — другая серия");
        let d = SeriesRegistry::intern(&reg, MetricId(2), ValueType::F32, &[("sensor", "pa")]);
        assert_ne!(a, d, "другая метрика — другая серия");

        // Реестр другого неймспейса нумеруется независимо: общий на процесс
        // склеил бы серии разных экземпляров сервиса.
        let other = SeriesRegistry::new_shared();
        let e = SeriesRegistry::intern(&other, MetricId(9), ValueType::F64, &[]);
        assert_eq!(e, SeriesId(0));
    }

    #[test]
    fn series_registry_is_thread_safe() {
        let reg = SeriesRegistry::new_shared();
        let mut handles = Vec::new();
        for t in 0..4 {
            let reg = Arc::clone(&reg);
            handles.push(std::thread::spawn(move || {
                let mut ids = Vec::new();
                for i in 0..50u16 {
                    let tag = format!("s{}", i % 10);
                    ids.push(SeriesRegistry::intern(
                        &reg,
                        MetricId(i % 5),
                        ValueType::F32,
                        &[("sensor", tag.as_str())],
                    ));
                }
                let _ = t;
                ids
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        // 5 метрик × 10 тэгов, но пары (метрика, тэг) повторяются циклами
        // по 10: уникальных комбинаций ровно 10.
        let count = reg.read().unwrap().len();
        assert_eq!(count, 10, "дубликаты не создаются при гонке");
    }
}
