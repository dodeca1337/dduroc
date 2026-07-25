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
use crate::staged::{ChannelIdx, DropCounters, NsId, Staged, StagedRecord};
use crate::stats::Counters;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use dduroc_format::block::{BlockBuilder, BlockHeader};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{BootCounter, FooterBuilder, Level, Micros, ProtocolVersion};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Ёмкости очередей.
///
/// Очередь выделяется целиком при открытии хранилища: crossbeam резервирует
/// весь буфер сразу. При 32-байтовом inline-payload'е запись занимает под
/// сотню байт, то есть значения по умолчанию стоят около трёх четвертей
/// мегабайта на процесс — на armv7 это заметно, и прибор, который пишет
/// редко, вправе выбрать очередь поменьше.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSizes {
    /// Обычная очередь: при переполнении запись теряется.
    pub normal: usize,
    /// Критическая: при переполнении вызывающий ждёт места.
    pub critical: usize,
}

impl Default for QueueSizes {
    fn default() -> Self {
        Self {
            normal: 8192,
            critical: 1024,
        }
    }
}

impl QueueSizes {
    /// Нулевая ёмкость означала бы рандеву на каждой записи: писать стало бы
    /// можно только в темпе writer'а, и обычный канал перестал бы отличаться
    /// от критического.
    fn sanitized(self) -> Self {
        Self {
            normal: self.normal.max(1),
            critical: self.critical.max(1),
        }
    }
}

/// Сколько записей writer забирает за один заход перед тем, как заняться
/// таймерами. Ограничение нужно, чтобы поток телеметрии не заморозил
/// обслуживание flush- и sync-дедлайнов.
const DRAIN_LIMIT: usize = 4096;

/// Максимальное ожидание места в критической очереди.
const BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(5);

/// Сон, когда обслуживать нечего.
const IDLE_TIMEOUT: Duration = Duration::from_millis(250);

/// Нижняя граница ожидания в цикле: без неё просроченный дедлайн даёт
/// нулевой таймаут и вырождается в busy-wait на целое ядро.
const MIN_TIMEOUT: Duration = Duration::from_millis(1);

/// Потолок числа проходов при вычерпывании очередей перед `sync`/`shutdown`.
///
/// Без него поток, пишущий быстрее, чем успевает writer, не дал бы
/// завершиться ни `sync`, ни `shutdown` — процесс не смог бы остановиться.
const DRAIN_ROUNDS: usize = 64;

/// Источник отметок о служебных событиях в потоке записей.
const DIAG_TARGET: &str = "dduroc";

// ════════════════════════════════════════════════════════════════════════════
// Команды
// ════════════════════════════════════════════════════════════════════════════

/// Управляющая команда.
///
/// Команды идут отдельной очередью, поэтому сами по себе **обгоняют** данные,
/// находящиеся в полёте. Чтобы `sync` не отчитался об успехе, пока часть
/// записей ещё лежит в очереди, а `shutdown` не запечатал сегменты поверх
/// недописанного, обе команды сперва вычерпывают очереди данных досуха
/// (см. [`WriterLoop::drain_pending`]).
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
    pub drops: Arc<DropCounters>,
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
    counters: Arc<Counters>,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Writer {
    /// Запустить writer-поток.
    pub fn spawn(counters: Arc<Counters>, queues: QueueSizes) -> Result<Arc<Self>> {
        let queues = queues.sanitized();
        let (normal_tx, normal_rx) = crossbeam_channel::bounded(queues.normal);
        let (critical_tx, critical_rx) = crossbeam_channel::bounded(queues.critical);
        let (control_tx, control_rx) = crossbeam_channel::bounded(64);

        let loop_state = WriterLoop {
            namespaces: Vec::new(),
            counters: Arc::clone(&counters),
            batch: Vec::new(),
            active: Vec::new(),
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
    /// `drops` — счётчики канала: потеря должна быть отмечена там, где
    /// образовалась дыра.
    #[inline]
    pub fn write(&self, item: Staged, critical: bool, drops: &DropCounters) -> Result<()> {
        if critical {
            self.write_critical(item, drops)
        } else {
            self.write_normal(item, drops)
        }
    }

    /// Поставить запись, ни при каких условиях не блокируя вызывающего.
    ///
    /// Очередь выбирается та же, что и обычно, — порядок критических записей
    /// между собой сохраняется; отличается только реакция на переполнение.
    /// Нужно там, где ожидание недопустимо в принципе: `Drop` стража спана
    /// вызывается в том числе при развёртке стека после паники, и пятисекундное
    /// ожидание места превратило бы аварийное завершение в зависание.
    #[inline]
    pub fn write_no_wait(&self, item: Staged, critical: bool, drops: &DropCounters) -> Result<()> {
        let queue = if critical {
            &self.critical
        } else {
            &self.normal
        };
        match queue.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                Counters::bump(&self.counters.dropped);
                drops.record(item.channel);
                Err(Error::QueueFull)
            }
            Err(TrySendError::Disconnected(item)) => Err(self.writer_died(item)),
        }
    }

    #[inline]
    fn write_normal(&self, item: Staged, drops: &DropCounters) -> Result<()> {
        self.write_no_wait(item, false, drops)
    }

    fn write_critical(&self, item: Staged, drops: &DropCounters) -> Result<()> {
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
                        drops.record(item.channel);
                        Err(Error::QueueFull)
                    }
                    Err(crossbeam_channel::SendTimeoutError::Disconnected(item)) => {
                        Err(self.writer_died(item))
                    }
                }
            }
            Err(TrySendError::Disconnected(item)) => Err(self.writer_died(item)),
        }
    }

    /// Учесть запись, потерянную из-за смерти writer'а.
    ///
    /// Отказ очереди из-за отсутствия потребителя — такая же потеря, как
    /// переполнение, и обязан быть виден в `Stats`: иначе `is_clean()`
    /// отчитался бы о благополучии на хранилище, в которое давно ничего
    /// не пишется.
    #[cold]
    fn writer_died(&self, _item: Staged) -> Error {
        Counters::bump(&self.counters.dropped);
        Error::WriterDead
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

    /// Счётчики, общие с хранилищем: путь записи учитывает в них и то, о чём
    /// вызывающему уже не сообщает.
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Жив ли writer-поток.
    ///
    /// Спрашивается у самого потока, а не у очередей: заполненная очередь
    /// означает лишь отставание диска, а пустая — что писать нечего. Ни то,
    /// ни другое не говорит, работает ли потребитель.
    pub fn is_alive(&self) -> bool {
        match self.handle.lock() {
            // `None` — поток уже присоединён в `shutdown`.
            Ok(guard) => guard.as_ref().is_some_and(|h| !h.is_finished()),
            // Мьютекс отравлен паникой в `shutdown`; сам поток при этом
            // жив-здоров, а очередь на приём работает.
            Err(poisoned) => poisoned
                .into_inner()
                .as_ref()
                .is_some_and(|h| !h.is_finished()),
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Состояние канала
// ════════════════════════════════════════════════════════════════════════════

struct ChannelState {
    config: ChannelConfig,
    dir: PathBuf,
    /// Неизменные атрибуты, которые уходят в заголовок каждого сегмента.
    protocol_version: ProtocolVersion,
    store_id: u64,
    boot: BootCounter,
    inventory: Inventory,
    /// Открытый сегмент. `None` — канал ещё ничего не писал либо закрыт
    /// по бездействию: пустой неймспейс не должен занимать ни файлового
    /// дескриптора, ни преаллоцированных байт.
    segment: Option<SegmentWriter>,
    builder: BlockBuilder,
    footer: FooterBuilder,
    /// Буфер сериализации блока. Переиспользуется: аллокация и рост на
    /// каждый flush — лишняя работа на пути, который выполняется тысячи раз
    /// в секунду.
    scratch: Vec<u8>,
    /// Максимальное записанное время: время, ушедшее назад, подтягивается
    /// вперёд, чтобы индекс блоков оставался сортированным.
    last_time: Micros,
    block_opened: Option<Instant>,
    last_sync: Instant,
    dirty_since_sync: bool,
}

impl ChannelState {
    fn new(
        dir: PathBuf,
        config: ChannelConfig,
        protocol_version: ProtocolVersion,
        store_id: u64,
        boot: BootCounter,
    ) -> Result<Self> {
        let inventory = Inventory::scan(&dir)?;
        let now = Instant::now();
        Ok(Self {
            // Буфер растёт при первой записи: неймспейс, который ничего не
            // пишет, не должен занимать памяти. При двадцати четырёх тысячах
            // неймспейсов предварительное выделение по 64 КиБ на канал стоило
            // бы гигабайты на 32-битной цели.
            builder: BlockBuilder::new(),
            config,
            dir,
            protocol_version,
            store_id,
            boot,
            inventory,
            segment: None,
            footer: FooterBuilder::new(),
            scratch: Vec::new(),
            last_time: Micros(0),
            block_opened: None,
            last_sync: now,
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
    channels: Vec<ChannelState>,
    drops: Arc<DropCounters>,
}

// ════════════════════════════════════════════════════════════════════════════
// Цикл writer'а
// ════════════════════════════════════════════════════════════════════════════

struct WriterLoop {
    namespaces: Vec<NsState>,
    counters: Arc<Counters>,
    /// Переиспользуемый буфер батча.
    batch: Vec<Staged>,
    /// Каналы, которым есть что обслуживать: открытый блок, несинхронизованные
    /// данные или незапечатанный сегмент.
    ///
    /// Обходить все каналы подряд нельзя: при заявленных двадцати четырёх
    /// тысячах неймспейсов их десятки тысяч, и полный проход на каждом
    /// обороте цикла съел бы процессор впустую. Пишущих в любой момент —
    /// единицы.
    active: Vec<(usize, usize)>,
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

            match self.poll_control(&control, &normal, &critical) {
                ControlOutcome::Continue => {}
                ControlOutcome::Stop => break,
            }

            if got == 0 {
                // Ждём либо новую запись, либо ближайший дедлайн.
                // Просроченный дедлайн даёт нулевой таймаут, а нулевой
                // таймаут в `select!` — мгновенный возврат: цикл сжёг бы
                // целое ядро. Нижняя граница делает такой оборот безвредным.
                let timeout = self
                    .next_deadline()
                    .unwrap_or(IDLE_TIMEOUT)
                    .max(MIN_TIMEOUT);
                let mut stop = false;
                crossbeam_channel::select! {
                    recv(critical) -> item => if let Ok(item) = item { self.batch.push(item); self.apply_batch(); },
                    recv(normal) -> item => if let Ok(item) = item { self.batch.push(item); self.apply_batch(); },
                    recv(control) -> cmd => match cmd {
                        Ok(cmd) => {
                            stop = matches!(
                                self.handle_control(cmd, &normal, &critical),
                                ControlOutcome::Stop
                            );
                        }
                        Err(_) => stop = true,
                    },
                    default(timeout) => {}
                }
                if stop {
                    break;
                }
            }

            self.tick();
        }

        self.finish();
    }

    /// Вычерпать обе очереди данных досуха.
    ///
    /// Вызывается перед `sync` и `shutdown`: команды идут отдельной очередью
    /// и без этого обгоняли бы записи, уже стоящие в очереди данных. Тогда
    /// `sync` отчитывался бы об успехе, не записав их, а `shutdown` запечатывал
    /// сегменты поверх недописанного — записи исчезали бы, хотя `log()`
    /// вернул `Ok`.
    fn drain_pending(&mut self, normal: &Receiver<Staged>, critical: &Receiver<Staged>) {
        for _ in 0..DRAIN_ROUNDS {
            let got = self.drain(critical) + self.drain(normal);
            if got == 0 {
                return;
            }
            self.apply_batch();
        }
        // Очередь всё ещё пополняется быстрее, чем вычерпывается. Дальше
        // ждать нельзя: остановка процесса не должна зависеть от того,
        // перестанут ли прикладные потоки писать.
        let leftover = normal.len() + critical.len();
        if leftover > 0 {
            Counters::add(&self.counters.dropped, leftover as u64);
        }
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
        //
        // Проверка «уже отсортировано» не украшение: устойчивая сортировка
        // выделяет временный буфер на половину батча, то есть до полутора
        // сотен килобайт на каждый заход. Батч приходит упорядоченным почти
        // всегда — очередь FIFO, а время монотонно, — и линейная проверка
        // избавляет от этой аллокации в общем случае.
        let key = |s: &Staged| (s.ns.0, s.channel.0, s.at.0);
        if !self.batch.is_sorted_by_key(key) {
            self.batch.sort_by_key(key);
        }

        let batch = std::mem::take(&mut self.batch);
        for item in &batch {
            if self.push(item).is_err() {
                // Логировать нельзя — очередь наша собственная; падать тоже:
                // отказ носителя не должен уносить с собой весь механизм
                // логирования, включая остальные каналы. Считаем и идём
                // дальше, ошибка видна через `Stats::io_errors`.
                Counters::bump(&self.counters.io_errors);
            }
        }
        self.batch = batch;
        self.batch.clear();

        // Group commit: критический канал синхронизируется ОДИН раз на батч.
        // Синхронизация на каждую запись, как было поначалу, превращала
        // всплеск из пятисот аварийных сообщений в пятьсот блоков и пятьсот
        // fdatasync — секунды записи и лишний износ флеша там, где хватает
        // одного обращения к носителю.
        let counters = Arc::clone(&self.counters);
        for &(ns_idx, ch_idx) in &self.active {
            let ch = &mut self.namespaces[ns_idx].channels[ch_idx];
            if ch.config.durability == Durability::Immediate && ch.dirty_since_sync {
                let done = Self::flush_block(ch, &counters)
                    .and_then(|()| Self::sync_channel(ch, &counters));
                if done.is_err() {
                    Counters::bump(&counters.io_errors);
                }
            }
        }
    }

    fn push(&mut self, item: &Staged) -> Result<()> {
        let ns_idx = item.ns.0 as usize;
        let ch_idx = item.channel.0 as usize;
        if ns_idx >= self.namespaces.len() || ch_idx >= self.namespaces[ns_idx].channels.len() {
            // Адрес не существует — записать некуда. Молчать нельзя: это
            // ошибка вызывающего, и она обязана быть видна в счётчиках.
            Counters::bump(&self.counters.dropped);
            return Ok(());
        }

        let ch = &mut self.namespaces[ns_idx].channels[ch_idx];

        // Монотонность внутри канала: время из прошлого подтягивается вперёд.
        // Считается ДО открытия блока: именем и базой нового сегмента должно
        // стать время его первой записи, как требует формат, а не время
        // предыдущей (у нового канала — ноль).
        let at = Micros(item.at.0.max(ch.last_time.0));
        ch.last_time = at;

        if ch.builder.is_empty() {
            Self::ensure_room(ch, at, &self.counters)?;
            ch.block_opened = Some(Instant::now());
        }

        ch.builder.push(at, &item.record.as_record())?;
        Counters::bump(&self.counters.records_written);
        // Множества типов в footer'е: миграция по ним решает, переписывать ли
        // сегмент, а читатель — что в сегменте вообще есть.
        let (event, metric) = item.footer_ids();
        if let Some(id) = event {
            ch.footer.add_event(id);
        }
        if let Some(id) = metric {
            ch.footer.add_metric(id);
        }
        ch.dirty_since_sync = true;
        if !self.active.contains(&(ns_idx, ch_idx)) {
            self.active.push((ns_idx, ch_idx));
        }

        if ch.builder.body_len() >= ch.config.block_max_bytes {
            Self::flush_block(ch, &self.counters)?;
        }
        Ok(())
    }

    /// Убедиться, что в активном сегменте хватит места на целый блок.
    fn ensure_room(ch: &mut ChannelState, at: Micros, counters: &Counters) -> Result<()> {
        let need = ch.config.block_max_bytes as u64 + BlockHeader::SIZE as u64 * 2;

        if let Some(seg) = &ch.segment
            && seg.fits(need)
        {
            return Ok(());
        }
        if ch.segment.is_some() {
            Self::seal_segment(ch, counters)?;
        }
        Self::open_segment(ch, at, counters)
    }

    fn open_segment(ch: &mut ChannelState, at: Micros, counters: &Counters) -> Result<()> {
        let (protocol_version, store_id, boot) = (ch.protocol_version, ch.store_id, ch.boot);
        // Имя сегмента — (boot, время его первой записи). Совпадение имён
        // возможно только при регрессе времени; сдвигаем на микросекунду,
        // чтобы не затереть существующий файл.
        let mut base = at;
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
        // Отметка о незакрытом блоке снимается ПЕРВЫМ делом, до любых ранних
        // выходов и до возможной ошибки записи. Иначе просроченный дедлайн
        // остаётся навсегда, `next_deadline` возвращает нулевой таймаут, и
        // цикл writer'а превращается в busy-loop на целое ядро.
        ch.block_opened = None;

        if ch.builder.is_empty() {
            return Ok(());
        }
        let Some(seg) = ch.segment.as_mut() else {
            // Собранный блок некуда положить: сегмент не открыт. Такого быть
            // не должно (его открывает `ensure_room`), но потерю всё равно
            // нужно показать, а не отдать `Ok`.
            Counters::add(&counters.dropped, u64::from(ch.builder.count()));
            ch.builder.reset();
            return Ok(());
        };

        let mut out = std::mem::take(&mut ch.scratch);
        out.clear();
        let last = ch.builder.last().unwrap_or(ch.last_time);
        let seq = seg.next_seq();
        let header = match ch.builder.finish(seq, ch.config.compression, &mut out) {
            Ok(h) => h,
            Err(e) => {
                ch.scratch = out;
                return Err(e.into());
            }
        };

        // Резерв в `ensure_room` рассчитан по `block_max_bytes`, но одна
        // крупная запись могла перевалить порог: писать за границу
        // преаллокации нельзя — там нет зарезервированного на носителе места,
        // и запись упёрлась бы в ENOSPC уже посреди блока.
        if !seg.fits(out.len() as u64) {
            Self::seal_segment(ch, counters)?;
            Self::open_segment(ch, header.base, counters)?;
            let seg = ch.segment.as_mut().ok_or(Error::WriterDead)?;
            // Нумерация блоков в новом сегменте начинается заново.
            dduroc_format::restamp_seq(&mut out, seg.next_seq())?;
        }

        let result = (|| {
            let seg = ch.segment.as_mut().ok_or(Error::WriterDead)?;
            let offset = seg.append_block(&out)?;
            Ok::<u64, Error>(offset)
        })();
        let written = out.len() as u64;
        ch.scratch = out;

        let offset = result?;
        ch.footer.add_block(offset, &header, last);
        Counters::bump(&counters.blocks_written);
        Counters::add(&counters.bytes_written, written);
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
        let counters = Arc::clone(&self.counters);
        let active = std::mem::take(&mut self.active);
        for &(ns_idx, ch_idx) in &active {
            let ch = &mut self.namespaces[ns_idx].channels[ch_idx];

            if ch.flush_deadline().is_some_and(|d| d <= now)
                && Self::flush_block(ch, &counters).is_err()
            {
                Counters::bump(&counters.io_errors);
            }
            if ch.sync_deadline().is_some_and(|d| d <= now)
                && Self::sync_channel(ch, &counters).is_err()
            {
                Counters::bump(&counters.io_errors);
            }

            // Канал, которому больше нечего обслуживать, покидает список:
            // иначе он оставался бы в нём до конца жизни процесса.
            if ch.block_opened.is_some() || ch.dirty_since_sync {
                self.active.push((ns_idx, ch_idx));
            }
        }
    }

    /// Вставить в поток отметки о потерянных записях.
    ///
    /// Дыра, о которой нигде не сказано, неотличима от тишины — ровно тот
    /// дефект прототипа, ради которого здесь ведётся учёт.
    fn emit_drop_notices(&mut self) {
        let mut notices: Vec<(NsId, ChannelIdx, u64, Micros)> = Vec::new();
        for (ns_idx, ns) in self.namespaces.iter().enumerate() {
            for ch_idx in 0..ns.channels.len() {
                let channel = ChannelIdx(ch_idx as u16);
                let count = ns.drops.take(channel);
                if count > 0 {
                    notices.push((
                        NsId(ns_idx as u32),
                        channel,
                        count,
                        ns.channels[ch_idx].last_time,
                    ));
                }
            }
        }

        for (ns, channel, count, at) in notices {
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

    fn poll_control(
        &mut self,
        control: &Receiver<Control>,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
    ) -> ControlOutcome {
        while let Ok(cmd) = control.try_recv() {
            if matches!(
                self.handle_control(cmd, normal, critical),
                ControlOutcome::Stop
            ) {
                return ControlOutcome::Stop;
            }
        }
        ControlOutcome::Continue
    }

    fn handle_control(
        &mut self,
        cmd: Control,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
    ) -> ControlOutcome {
        match cmd {
            Control::Register(setup, reply) => {
                let _ = reply.send(self.register(*setup));
                ControlOutcome::Continue
            }
            Control::Sync(ns, reply) => {
                // Сначала записи, потом отчёт: иначе `sync` подтвердил бы
                // сохранность того, что ещё стоит в очереди.
                self.drain_pending(normal, critical);
                let _ = reply.send(self.sync_all(ns));
                ControlOutcome::Continue
            }
            Control::Shutdown(reply) => {
                self.drain_pending(normal, critical);
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
            channels.push(ChannelState::new(
                setup.dir.join(&cfg.name),
                cfg,
                setup.protocol_version,
                setup.store_id,
                setup.boot,
            )?);
        }
        let id = NsId(self.namespaces.len() as u32);
        self.namespaces.push(NsState {
            name: setup.name,
            channels,
            drops: setup.drops,
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
        // Отметки о потерях выталкиваются ДО запечатывания. Иначе дыра,
        // образовавшаяся между последним `tick` и остановкой, не попадала бы
        // в поток вовсе — а это ровно тот момент, когда очередь переполнена
        // чаще всего: процесс завершается под нагрузкой.
        self.emit_drop_notices();

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staged::{OwnedValue, StagedRecord as SR};
    use dduroc_format::MetricId;

    #[test]
    fn queue_sizes_never_degenerate_to_rendezvous() {
        // Нулевая ёмкость превратила бы постановку в очередь в рандеву с
        // writer'ом: обычный канал перестал бы отличаться от критического, а
        // прикладной поток стал бы ждать диск на каждой записи.
        let q = QueueSizes {
            normal: 0,
            critical: 0,
        }
        .sanitized();
        assert_eq!(q.normal, 1);
        assert_eq!(q.critical, 1);

        let d = QueueSizes::default();
        assert_eq!(d.sanitized(), d, "разумные значения не искажаются");
    }

    #[test]
    fn footer_ids_split_events_from_metrics() {
        // Множества типов в footer'е ведутся раздельно: миграция спрашивает
        // про события и метрики по отдельности.
        let sample = Staged {
            ns: NsId(0),
            channel: ChannelIdx(0),
            at: Micros(0),
            record: SR::Sample {
                metric: MetricId(4),
                value: OwnedValue::F32(1.0),
            },
        };
        assert_eq!(sample.footer_ids(), (None, Some(MetricId(4))));

        let span = Staged {
            record: SR::SpanEnd {
                span: dduroc_format::SpanId(1),
            },
            ..sample
        };
        assert_eq!(span.footer_ids(), (None, None), "спан не тип данных");
    }
}
