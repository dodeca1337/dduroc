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
use std::path::{Path, PathBuf};
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

/// Сколько канал должен простоять без дела, прежде чем отдаст буферы.
///
/// Мгновенный возврат неверен: канал с [`Durability::Immediate`] оказывается
/// «без дела» после **каждой** групповой фиксации — блок вытолкнут,
/// синхронизировать нечего, — и отдавал бы буфер блока со scratch'ем на
/// каждом батче, чтобы тут же выделить их снова. Это ровно горячий путь
/// критических записей. Пауза его не касается, а настоящее бездействие от
/// неё не убежит: держать пик лишние секунды дешевле, чем платить парой
/// аллокаций за каждую аварийную запись.
const RELEASE_AFTER: Duration = Duration::from_secs(2);

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
    /// Отпустить неймспейс: запечатать его сегменты и освободить слот.
    ///
    /// Идёт той же очередью, что и `Register`, поэтому повторный подъём того
    /// же имени гарантированно обрабатывается **после** освобождения. Иначе на
    /// один каталог пришлось бы два состояния канала со своими инвентарями, и
    /// ротация одного удаляла бы сегмент, открытый другим, — записи уходили бы
    /// в файл без имени и пропадали при закрытии.
    Release(NsId),
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
    /// Началась остановка: очередь ещё принимает, но разбирать её уже некому.
    ///
    /// Между вычерпыванием очереди в `shutdown` и выходом потока очередь жива,
    /// и `try_send` отвечал бы `Ok` записям, которые затем гибнут в
    /// деструкторе канала — без счётчика, без отметки, без ответа вызывающему.
    /// Отказ здесь честнее: [`Error::ShuttingDown`] и без того объявлен
    /// потерей ([`Error::loses_record`]), просто до сих пор его никто не
    /// порождал.
    ///
    /// Цена — одно расслабленное чтение на запись; предсказуемое ветвление,
    /// на armv7 неразличимое на фоне самой постановки в очередь.
    stopping: std::sync::atomic::AtomicBool,
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
            diag_target: Arc::from(DIAG_TARGET),
            batch: Vec::new(),
            drops_seen: 0,
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
            stopping: std::sync::atomic::AtomicBool::new(false),
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
        // Идёт остановка — очередь ещё принимает, но разбирать её уже некому:
        // всё, что ляжет в неё сейчас, умрёт в деструкторе канала. Отказать
        // честнее, чем ответить `Ok` записи, которой не будет на носителе.
        if self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
            drops.record(item.channel);
            Counters::publish(&self.counters.dropped);
            return Err(Error::ShuttingDown);
        }
        let queue = if critical {
            &self.critical
        } else {
            &self.normal
        };
        match queue.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                // Порядок обязателен: сначала поканальный счётчик, потом
                // общий — и общий с публикацией. По его изменению writer
                // решает, обходить ли каналы за отметками о потерях, и с
                // обратным порядком он мог бы застать обход пустым, счесть
                // отметку выданной и оставить дыру необъявленной.
                drops.record(item.channel);
                Counters::publish(&self.counters.dropped);
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
        // См. `write_no_wait`: ждать места в очереди, которую уже никто не
        // разбирает, значило бы ждать пять секунд ради гарантированной потери.
        if self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
            drops.record(item.channel);
            Counters::publish(&self.counters.dropped);
            return Err(Error::ShuttingDown);
        }
        match self.critical.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                Counters::bump(&self.counters.backpressure_waits);
                // Ждём с таймаутом: вечная блокировка на отказавшем носителе
                // подвесила бы прикладные потоки навсегда.
                match self.critical.send_timeout(item, BACKPRESSURE_TIMEOUT) {
                    Ok(()) => Ok(()),
                    Err(crossbeam_channel::SendTimeoutError::Timeout(item)) => {
                        // Порядок тот же и по той же причине, что в
                        // `write_no_wait`.
                        drops.record(item.channel);
                        Counters::publish(&self.counters.dropped);
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

    /// Отпустить неймспейс: writer запечатает его сегменты и освободит слот.
    ///
    /// Вызывается при уничтожении последней ручки. Без ответа: вызывающий —
    /// `Drop`, и ждать носитель в нём нельзя. Порядок с последующим
    /// `register` сохраняет сама очередь команд.
    pub fn release(&self, ns: NsId) {
        let _ = self.control.send(Control::Release(ns));
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
        // Флаг ставится ДО команды: между вычерпыванием очереди в потоке и
        // его выходом очередь остаётся живой, и без флага `try_send` отвечал
        // бы `Ok` записям, которые затем гибнут вместе с каналом.
        self.stopping
            .store(true, std::sync::atomic::Ordering::Relaxed);
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

/// Неизменные атрибуты, которые уходят в заголовок каждого сегмента канала.
///
/// Одним типом, а не тремя полями рядом: они всегда передаются вместе и
/// всегда попадают в один и тот же заголовок.
#[derive(Debug, Clone, Copy)]
struct SegmentIdentity {
    protocol_version: ProtocolVersion,
    store_id: u64,
    boot: BootCounter,
}

struct ChannelState {
    config: ChannelConfig,
    dir: PathBuf,
    /// Свой номер и счётчики потерь неймспейса: потерю, обнаруженную уже в
    /// writer'е, надо отметить там же, где образовалась дыра, — иначе она не
    /// попадёт в поток отдельной записью.
    index: ChannelIdx,
    drops: Arc<DropCounters>,
    identity: SegmentIdentity,
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
    /// Числится ли канал в списке `active` writer'а.
    ///
    /// Флаг, а не `active.contains(..)`: проверка выполняется на **каждую**
    /// запись, и линейный поиск по списку в десятки тысяч пар превращал бы
    /// раскладку батча в квадрат от числа пишущих каналов.
    in_active: bool,
    /// С какого момента каналу нечего обслуживать. `None` — есть.
    ///
    /// Отсрочка возврата буферов, см. [`RELEASE_AFTER`].
    idle_since: Option<Instant>,
}

impl ChannelState {
    fn new(
        dir: PathBuf,
        config: ChannelConfig,
        identity: SegmentIdentity,
        index: ChannelIdx,
        drops: Arc<DropCounters>,
        counters: &Counters,
    ) -> Result<Self> {
        let mut inventory = Inventory::scan(&dir)?;
        Self::recover_orphan(&dir, &mut inventory, identity, counters);
        let now = Instant::now();
        Ok(Self {
            // Буфер растёт при первой записи: неймспейс, который ничего не
            // пишет, не должен занимать памяти. При двадцати четырёх тысячах
            // неймспейсов предварительное выделение по 64 КиБ на канал стоило
            // бы гигабайты на 32-битной цели.
            builder: BlockBuilder::new(),
            config,
            dir,
            index,
            drops,
            identity,
            inventory,
            segment: None,
            footer: FooterBuilder::new(),
            scratch: Vec::new(),
            last_time: Micros(0),
            block_opened: None,
            last_sync: now,
            dirty_since_sync: false,
            in_active: false,
            idle_since: None,
        })
    }

    /// Вернуть память бездействующего канала аллокатору.
    ///
    /// Буферы канала растут до крупнейшего блока, прошедшего через них, и без
    /// возврата остаются такими навсегда: один мегабайтный blob закреплял бы
    /// ~2× своего размера за каналом до конца жизни процесса, а стационарные
    /// 64–128 КиБ на канал при заявленных десятках тысяч каналов складывались
    /// бы в гигабайты. Поэтому при уходе в бездействие память отдаётся
    /// **целиком**, а не сжимается до порога: пишущих в любой момент единицы,
    /// и держать пустые буферы за молчащими нечем оправдать.
    ///
    /// Цена возврата — реаллокация при следующем пробуждении, но канал уходит
    /// в бездействие не раньше, чем выполнит sync, то есть не чаще периода
    /// синхронизации: одна аллокация в несколько секунд на канал не видна
    /// даже на armv7.
    fn release_buffers(&mut self) {
        self.builder.shrink_to(0);
        self.scratch = Vec::new();
        self.footer.shrink_to_fit();
    }

    /// Запечатать сегмент, оборванный прошлым запуском.
    ///
    /// Смысл — вернуть преаллокацию: незапечатанный сегмент числится в бюджете
    /// канала целиком, и несколько аварийных остановок подряд выедают бюджет
    /// пустотой, после чего ротация принимается за живую историю.
    ///
    /// Трогается **только** сегмент чужого запуска. Сегмент текущего может
    /// быть открыт живым состоянием этого же процесса (неймспейс подняли
    /// повторно), и обрезать его под ним значило бы потерять данные. Смены
    /// запуска для этого достаточно: сегмент по формату не пересекает её
    /// границу, поэтому в чужой уже никто не пишет.
    ///
    /// Отказ восстановления не фатален: сегмент останется незапечатанным и
    /// будет читаться сканом — ровно так, как было до этой правки.
    fn recover_orphan(
        dir: &Path,
        inventory: &mut Inventory,
        identity: SegmentIdentity,
        counters: &Counters,
    ) {
        let Some(newest) = inventory.newest().cloned() else {
            return;
        };
        if newest.name.boot == identity.boot {
            return;
        }
        match crate::segment::seal_orphan(&newest.path(dir), Some(identity.store_id)) {
            Ok(Some(recovered)) => {
                inventory.update_size(recovered.name, recovered.size);
                Counters::bump(&counters.segments_sealed);
                if recovered.truncated {
                    Counters::bump(&counters.recovered_tails);
                }
            }
            Ok(None) => {}
            // Сегмент, принесённый с другого прибора, — не отказ носителя, а
            // законное состояние каталога: кто-то положил сюда чужой дамп.
            // Трогать его нельзя, а объявлять хранилище неисправным — тем
            // более: об этих файлах честно сообщает читатель.
            Err(Error::ForeignSegment { .. }) => {}
            Err(_) => Counters::bump(&counters.io_errors),
        }
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
    /// Слоты неймспейсов. `None` — отпущенный: [`NsId`] это индекс, поэтому
    /// освобождённый слот обнуляется и переиспользуется, а не удаляется —
    /// сдвиг индексов увёл бы записи в полёте в чужой неймспейс.
    namespaces: Vec<Option<NsState>>,
    counters: Arc<Counters>,
    /// Источник отметок о потерях — один на всю жизнь writer'а: `Arc::from`
    /// аллоцирует, а отметки появляются ровно тогда, когда система под
    /// давлением.
    diag_target: Arc<str>,
    /// Переиспользуемый буфер батча.
    batch: Vec<Staged>,
    /// Значение общего счётчика потерь на момент последнего обхода отметок.
    ///
    /// Сторож полного прохода по флоту — см. [`WriterLoop::emit_drop_notices`].
    drops_seen: u64,
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
    fn drain_pending(
        &mut self,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
        leftovers: Leftovers,
    ) -> bool {
        self.drain_pending_rounds(normal, critical, leftovers, DRAIN_ROUNDS)
    }

    /// То же с явным числом проходов: воспроизвести исчерпание отведённых
    /// проходов подбором нагрузки нельзя — оно зависит от того, кому
    /// планировщик дал ход, а поведение на этой границе как раз и решает,
    /// сохранятся записи или будут уничтожены.
    fn drain_pending_rounds(
        &mut self,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
        leftovers: Leftovers,
        rounds: usize,
    ) -> bool {
        for _ in 0..rounds {
            let got = self.drain(critical) + self.drain(normal);
            if got == 0 {
                return true;
            }
            self.apply_batch();
        }
        // Очередь всё ещё пополняется быстрее, чем вычерпывается. Дальше
        // ждать нельзя: остановка процесса не должна зависеть от того,
        // перестанут ли прикладные потоки писать.
        if leftovers == Leftovers::Keep {
            // Записывать их ещё будет кому — обычным ходом цикла. Выбросить
            // их значило бы уничтожить принятое ради операции, которая
            // ничего от этого не выигрывает: очередь и так продолжит
            // разбираться. Вызывающему сообщается, что обещание выполнено
            // не до конца.
            return false;
        }
        // Остаток забирается поимённо, а не просто пересчитывается: потеря
        // обязана быть отмечена в канале, где образовалась дыра, — иначе она
        // не попадёт в поток отдельной записью и станет неотличима от тишины.
        // Число проходов ограничено снимком длины: производитель, пишущий
        // быстрее, не должен удерживать остановку.
        let mut leftover = 0u64;
        for rx in [critical, normal] {
            for _ in 0..rx.len() {
                let Ok(item) = rx.try_recv() else { break };
                if let Some(ns) = self
                    .namespaces
                    .get(item.ns.0 as usize)
                    .and_then(|n| n.as_ref())
                {
                    ns.drops.record(item.channel);
                }
                leftover += 1;
            }
        }
        if leftover > 0 {
            Counters::add(&self.counters.dropped, leftover);
        }
        false
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
            let Some(ch) = self
                .namespaces
                .get_mut(ns_idx)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch_idx))
            else {
                continue;
            };
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
        let exists = self
            .namespaces
            .get(ns_idx)
            .and_then(|n| n.as_ref())
            .is_some_and(|n| ch_idx < n.channels.len());
        if !exists {
            // Адрес не существует либо неймспейс уже отпущен — записать
            // некуда. Молчать нельзя: это ошибка вызывающего, и она обязана
            // быть видна в счётчиках.
            Counters::bump(&self.counters.dropped);
            return Ok(());
        }

        let ch = &mut self.namespaces[ns_idx]
            .as_mut()
            .expect("наличие проверено выше")
            .channels[ch_idx];

        // Монотонность внутри канала: время из прошлого подтягивается вперёд.
        // Считается ДО открытия блока: именем и базой нового сегмента должно
        // стать время его первой записи, как требует формат, а не время
        // предыдущей (у нового канала — ноль).
        let at = Micros(item.at.0.max(ch.last_time.0));
        ch.last_time = at;

        // Запись, не дошедшая до накопителя (нет места под сегмент, не
        // закодировалась), — потеряна, и учесть её надо там, где образовалась
        // дыра: `io_errors` скажет «что-то сломалось», но не «сколько записей
        // пропало», и отметка в поток без поканального счётчика не попадёт.
        if ch.builder.is_empty() {
            if let Err(e) = Self::ensure_room(ch, at, &self.counters) {
                Counters::bump(&self.counters.dropped);
                ch.drops.record(ch.index);
                return Err(e);
            }
            ch.block_opened = Some(Instant::now());
        }

        if let Err(e) = ch.builder.push(at, &item.record.as_record()) {
            Counters::bump(&self.counters.dropped);
            ch.drops.record(ch.index);
            return Err(e.into());
        }
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
        // Канал снова при деле: отсчёт бездействия начнётся заново, когда
        // ему опять станет нечего обслуживать.
        ch.idle_since = None;
        if !ch.in_active {
            ch.in_active = true;
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
        let SegmentIdentity {
            protocol_version,
            store_id,
            boot,
        } = ch.identity;
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
            // нужно показать, а не отдать `Ok` — и показать там же, где она
            // образовалась, чтобы отметка попала в поток этого канала.
            let lost = u64::from(ch.builder.count());
            Counters::add(&counters.dropped, lost);
            ch.drops.record_n(ch.index, lost);
            ch.builder.reset();
            ch.footer.discard_pending();
            return Ok(());
        };

        let mut out = std::mem::take(&mut ch.scratch);
        out.clear();
        let last = ch.builder.last().unwrap_or(ch.last_time);
        let seq = seg.next_seq();
        // Число записей снимается ДО `finish`: на превышении потолка тела он
        // сбрасывает накопитель, и посчитать потерю потом будет нечем.
        let pending = ch.builder.count();
        let header = match ch.builder.finish(seq, ch.config.compression, &mut out) {
            Ok(h) => h,
            Err(e) => {
                ch.scratch = out;
                // Блок не собрался — его записи потеряны, и это обязано быть
                // видно там же, где образовалась дыра. Один `io_errors`
                // сказал бы «что-то сломалось», но не «сколько пропало», и
                // отметка в поток без поканального счётчика не попадёт.
                let lost = u64::from(pending);
                Counters::add(&counters.dropped, lost);
                ch.drops.record_n(ch.index, lost);
                // Накопитель сбрасывается и здесь: `finish` делает это только
                // на превышении потолка, а заряженный накопитель заклинил бы
                // канал на той же ошибке при каждом следующем flush'е.
                ch.builder.reset();
                ch.footer.discard_pending();
                return Err(e.into());
            }
        };

        // Дальше любой исход проходит через одну точку возврата буфера:
        // раньше четыре ранних `?` роняли `out` на пол — вместе с ёмкостью,
        // которую следующий flush реаллоцировал бы, — а записи блока исчезали
        // с одним лишь `io_errors`, без учёта в потерях и без отметки в
        // потоке канала.
        let placed = Self::place_block(ch, counters, &mut out, &header);
        let written = out.len() as u64;
        ch.scratch = out;

        match placed {
            Ok(Placement::At(offset)) => {
                ch.footer.add_block(offset, &header, last);
                Counters::bump(&counters.blocks_written);
                Counters::add(&counters.bytes_written, written);
                Ok(())
            }
            Ok(Placement::Dropped) => {
                // Блока не будет ни в одном сегменте — его типы не должны
                // осесть в множествах ни того, ни другого.
                ch.footer.discard_pending();
                // Негабаритный блок раздул буферы до размеров, которых канал
                // больше не увидит, — держать их за ним незачем.
                ch.release_buffers();
                Ok(())
            }
            Err(e) => {
                let lost = u64::from(header.count);
                Counters::add(&counters.dropped, lost);
                ch.drops.record_n(ch.index, lost);
                ch.footer.discard_pending();
                Err(e)
            }
        }
    }

    /// Положить собранный блок в сегмент, при необходимости сменив сегмент.
    ///
    /// Ошибка означает, что блок потерян: учёт потерь и возврат буфера —
    /// забота вызывающего, у которого буфер и остаётся.
    fn place_block(
        ch: &mut ChannelState,
        counters: &Counters,
        out: &mut [u8],
        header: &BlockHeader,
    ) -> Result<Placement> {
        // Резерв в `ensure_room` рассчитан по `block_max_bytes`, но одна
        // крупная запись могла перевалить порог: писать за границу
        // преаллокации нельзя — там нет зарезервированного на носителе места,
        // и запись упёрлась бы в ENOSPC уже посреди блока.
        let fits = ch
            .segment
            .as_ref()
            .is_some_and(|seg| seg.fits(out.len() as u64));
        if !fits {
            Self::seal_segment(ch, counters)?;
            Self::open_segment(ch, header.base, counters)?;
            let next_seq = {
                let seg = ch.segment.as_ref().ok_or(Error::WriterDead)?;
                // Свежий сегмент тоже может не вместить блок: одна запись
                // бывает крупнее целого сегмента (несжимаемый blob). Писать её
                // всё равно значило бы выйти за преаллокацию, то есть
                // отказаться от единственной гарантии, ради которой она
                // делается: ENOSPC приходит один раз, при создании сегмента,
                // а не посреди записи критического события. Блок
                // отбрасывается, потеря объявляется отметкой в потоке.
                if !seg.fits(out.len() as u64) {
                    let lost = u64::from(header.count);
                    Counters::add(&counters.dropped, lost);
                    ch.drops.record_n(ch.index, lost);
                    return Ok(Placement::Dropped);
                }
                seg.next_seq()
            };
            // Нумерация блоков в новом сегменте начинается заново.
            dduroc_format::restamp_seq(out, next_seq)?;
        }
        let seg = ch.segment.as_mut().ok_or(Error::WriterDead)?;
        Ok(Placement::At(seg.append_block(out)?))
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

    /// Ближайший дедлайн — только по каналам, которым есть что обслуживать.
    ///
    /// Обходить все каналы подряд нельзя: при заявленных двадцати четырёх
    /// тысячах неймспейсов их десятки тысяч, а вызывается это на **каждом**
    /// холостом обороте цикла, то есть до четырёх раз в секунду. Дедлайн
    /// бывает только у канала с открытым блоком или несинхронизованными
    /// данными — то есть ровно у того, кто уже лежит в `active`.
    fn next_deadline(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut best: Option<Instant> = None;
        for &(ns_idx, ch_idx) in &self.active {
            let Some(ch) = self
                .namespaces
                .get(ns_idx)
                .and_then(|n| n.as_ref())
                .and_then(|n| n.channels.get(ch_idx))
            else {
                continue;
            };
            for d in [ch.flush_deadline(), ch.sync_deadline()]
                .into_iter()
                .flatten()
            {
                best = Some(best.map_or(d, |b: Instant| b.min(d)));
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
            let Some(ch) = self
                .namespaces
                .get_mut(ns_idx)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch_idx))
            else {
                continue;
            };

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

            // Канал остаётся в списке, только пока ему есть что обслуживать:
            // открытый блок или дедлайн синхронизации. Проверять сырой
            // `dirty_since_sync` нельзя — у Relaxed-канала он не сбрасывается
            // до самого seal, и канал жил бы в списке вечно, возвращая полный
            // обход, ради устранения которого список заведён.
            if ch.block_opened.is_some() || ch.sync_deadline().is_some() {
                ch.idle_since = None;
                self.active.push((ns_idx, ch_idx));
            } else {
                // Уходящий в бездействие канал отдаёт буферы: их ёмкость —
                // след самого крупного блока, и держать её за молчащим
                // каналом значит закрепить пик навсегда. Но не сразу:
                // Immediate-канал попадает сюда после каждой групповой
                // фиксации (см. `RELEASE_AFTER`), и мгновенный возврат стоил
                // бы пары аллокаций на каждую аварийную запись.
                let idle_since = *ch.idle_since.get_or_insert(now);
                if now.duration_since(idle_since) >= RELEASE_AFTER {
                    ch.in_active = false;
                    ch.idle_since = None;
                    ch.release_buffers();
                } else {
                    self.active.push((ns_idx, ch_idx));
                }
            }
        }
    }

    /// Вставить в поток отметки о потерянных записях.
    ///
    /// Дыра, о которой нигде не сказано, неотличима от тишины — ровно тот
    /// дефект прототипа, ради которого здесь ведётся учёт.
    fn emit_drop_notices(&mut self) {
        // Обход всех каналов всех неймспейсов — это десятки тысяч атомарных
        // обменов при заявленном масштабе, а зовётся он на каждом обороте
        // цикла, то есть до четырёх раз в секунду даже в полном простое.
        // Ровно тот полный проход, ради устранения которого заведён список
        // `active`.
        //
        // Каждая поканальная потеря сопровождается инкрементом общего
        // счётчика — иначе она не попала бы в `Stats`, — поэтому
        // неизменившийся общий счётчик доказывает, что обходить нечего.
        // Одно атомарное чтение вместо прохода по флоту.
        // Чтение с захватом: поканальные счётчики растут на прикладных
        // потоках ДО публикации общего ([`Counters::publish`]), и увидеть
        // новый общий счётчик значит увидеть и их. С `Relaxed` обход мог бы
        // застать поканальный счётчик ещё нулевым, счесть отметку выданной и
        // оставить дыру необъявленной до следующей потери.
        let total = self
            .counters
            .dropped
            .load(std::sync::atomic::Ordering::Acquire);
        if total == self.drops_seen {
            return;
        }
        self.drops_seen = total;

        let mut notices: Vec<(NsId, ChannelIdx, u64, Micros)> = Vec::new();
        for (ns_idx, slot) in self.namespaces.iter().enumerate() {
            let Some(ns) = slot.as_ref() else { continue };
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
                    target: Arc::clone(&self.diag_target),
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
            Control::Release(ns) => {
                // Сначала записи, потом освобождение: то, что прикладной поток
                // успел поставить в очередь до уничтожения ручки, обязано лечь
                // на диск — `log()` на него уже ответил `Ok`.
                //
                // Остаток очереди не трогаем. Очередь общая на процесс, и в
                // ней лежат записи ЧУЖИХ, живых неймспейсов: отпустить один
                // неймспейс не значит уничтожить всё, что успели написать
                // остальные. Записи самого отпускаемого писать уже некуда —
                // их учтёт `push` по несуществующему адресу.
                self.drain_pending(normal, critical, Leftovers::Keep);
                self.release(ns);
                ControlOutcome::Continue
            }
            Control::Sync(ns, reply) => {
                // Сначала записи, потом отчёт: иначе `sync` подтвердил бы
                // сохранность того, что ещё стоит в очереди.
                let drained = self.drain_pending(normal, critical, Leftovers::Keep);
                let mut outcome = self.sync_all(ns);
                if outcome.is_ok() && !drained {
                    // Забранное лежит на носителе, но очередь пополняется
                    // быстрее, чем разбирается. Записи не потеряны — они
                    // по-прежнему в очереди, — а вот обещание «всё
                    // накопленное на носителе» в этот раз не выполнено, и
                    // отдать `Ok` значило бы соврать.
                    outcome = Err(Error::SyncIncomplete);
                }
                let _ = reply.send(outcome);
                ControlOutcome::Continue
            }
            Control::Shutdown(reply) => {
                // Единственный случай, когда остаток очереди отбрасывается:
                // после остановки записывать его будет некому, и держать
                // процесс живым до молчания пишущих потоков нельзя.
                self.drain_pending(normal, critical, Leftovers::Discard);
                self.finish();
                let _ = reply.send(());
                ControlOutcome::Stop
            }
        }
    }

    fn register(&mut self, setup: NsSetup) -> Result<NsId> {
        let identity = SegmentIdentity {
            protocol_version: setup.protocol_version,
            store_id: setup.store_id,
            boot: setup.boot,
        };
        let mut channels = Vec::with_capacity(setup.channels.len());
        for (i, cfg) in setup.channels.into_iter().enumerate() {
            crate::fsutil::create_dir_all_synced(&setup.dir.join(&cfg.name))?;
            channels.push(ChannelState::new(
                setup.dir.join(&cfg.name),
                cfg,
                identity,
                ChannelIdx(i as u16),
                Arc::clone(&setup.drops),
                &self.counters,
            )?);
        }
        let state = NsState {
            name: setup.name,
            channels,
            drops: setup.drops,
        };
        // Слот отпущенного неймспейса переиспользуется: иначе повторный
        // подъём того же имени (переподключение сервиса) растил бы таблицу
        // до конца жизни процесса.
        match self.namespaces.iter().position(Option::is_none) {
            Some(i) => {
                self.namespaces[i] = Some(state);
                Ok(NsId(i as u32))
            }
            None => {
                self.namespaces.push(Some(state));
                Ok(NsId((self.namespaces.len() - 1) as u32))
            }
        }
    }

    /// Отпустить неймспейс: дописать, запечатать сегменты, освободить слот.
    ///
    /// Без этого состояние канала жило бы до конца процесса, и повторный
    /// подъём того же имени дал бы **два** состояния на один каталог со своими
    /// инвентарями. Ротация одного не знала бы об активном сегменте другого и
    /// удалила бы его: запись продолжалась бы в файл, у которого больше нет
    /// имени, и всё записанное после этого исчезло бы при закрытии.
    fn release(&mut self, ns: NsId) {
        // Отметки о потерях выталкиваются ДО запечатывания — иначе дыра,
        // образовавшаяся перед закрытием, не попала бы в поток вовсе.
        self.emit_drop_notices();

        let idx = ns.0 as usize;
        let counters = Arc::clone(&self.counters);
        if let Some(slot) = self.namespaces.get_mut(idx)
            && let Some(state) = slot.as_mut()
        {
            for ch in &mut state.channels {
                if Self::seal_segment(ch, &counters).is_err() {
                    Counters::bump(&counters.io_errors);
                }
            }
            *slot = None;
        }
        self.active.retain(|&(n, _)| n != idx);
    }

    fn sync_all(&mut self, only: Option<NsId>) -> Result<()> {
        let counters = Arc::clone(&self.counters);
        let mut first_error = None;
        for (idx, slot) in self.namespaces.iter_mut().enumerate() {
            if let Some(NsId(want)) = only
                && want as usize != idx
            {
                continue;
            }
            let Some(ns) = slot.as_mut() else { continue };
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
        for ns in self.namespaces.iter_mut().flatten() {
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

/// Что делать с записями, оставшимися в очереди после отведённых проходов.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leftovers {
    /// Оставить в очереди: их запишет обычный ход цикла.
    Keep,
    /// Отбросить и учесть как потерю: писать их больше некому.
    Discard,
}

/// Куда лёг собранный блок.
enum Placement {
    /// Записан по смещению.
    At(u64),
    /// Отброшен: не помещается даже в свежий сегмент. Потеря уже учтена.
    Dropped,
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
    fn idle_channel_gives_its_buffers_back() {
        // Буферы канала растут до крупнейшего блока и без возврата остаются
        // такими навсегда: RSS-замер показывал +16 МиБ после ОДНОГО blob'а
        // на 8 МиБ — буфер блока плюс scratch, оба по размеру блока.
        // При уходе канала в бездействие память обязана вернуться аллокатору.
        use dduroc_format::record::Sample;
        use dduroc_format::{MetricId, Record, Value};

        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::default();
        let drops = Arc::new(DropCounters::new(1));
        let mut ch = ChannelState::new(
            dir.path().to_path_buf(),
            ChannelConfig::new("default", 64 * 1024 * 1024),
            SegmentIdentity {
                protocol_version: ProtocolVersion(1),
                store_id: 0,
                boot: BootCounter(0),
            },
            ChannelIdx(0),
            drops,
            &counters,
        )
        .unwrap();

        // Мегабайтный несжимаемый blob — блок заведомо крупнее block_max.
        let noise: Vec<u8> = {
            let mut s: u64 = 0x2545_F491_4F6C_DD1D;
            (0..1 << 20)
                .map(|_| {
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    s as u8
                })
                .collect()
        };
        WriterLoop::ensure_room(&mut ch, Micros(0), &counters).unwrap();
        ch.builder
            .push(
                Micros(0),
                &Record::Sample(Sample {
                    metric: MetricId(1),
                    value: Value::Blob(&noise),
                }),
            )
            .unwrap();
        WriterLoop::flush_block(&mut ch, &counters).unwrap();

        let held = ch.builder.capacity() + ch.scratch.capacity();
        assert!(
            held >= 2 << 20,
            "после blob'а буферы обязаны быть раздуты (иначе тест пуст): {held}"
        );

        // То, что делает tick с каналом, покинувшим active.
        ch.release_buffers();
        assert_eq!(
            ch.builder.capacity() + ch.scratch.capacity(),
            0,
            "бездействующий канал не имеет права держать пик"
        );

        // Канал остаётся рабочим: следующая запись переоткрывает буферы.
        ch.builder
            .push(
                Micros(10),
                &Record::Sample(Sample {
                    metric: MetricId(1),
                    value: Value::U64(7),
                }),
            )
            .unwrap();
        WriterLoop::flush_block(&mut ch, &counters).unwrap();
        assert_eq!(counters.snapshot().dropped, 0);
        assert_eq!(counters.snapshot().blocks_written, 2);
    }

    fn loop_with_one_channel(dir: &Path, config: ChannelConfig) -> (WriterLoop, NsId) {
        let counters = Arc::new(Counters::default());
        let mut w = WriterLoop {
            namespaces: Vec::new(),
            counters,
            diag_target: Arc::from(DIAG_TARGET),
            batch: Vec::new(),
            drops_seen: 0,
            active: Vec::new(),
        };
        let ns = w
            .register(NsSetup {
                name: "ns".to_owned(),
                dir: dir.to_path_buf(),
                protocol_version: ProtocolVersion(1),
                store_id: 0,
                boot: BootCounter(0),
                channels: vec![config],
                drops: Arc::new(DropCounters::new(1)),
            })
            .unwrap();
        (w, ns)
    }

    fn held_bytes(w: &WriterLoop) -> usize {
        let ch = &w.namespaces[0].as_ref().unwrap().channels[0];
        ch.builder.capacity() + ch.scratch.capacity()
    }

    #[test]
    fn an_immediate_channel_keeps_its_buffers_between_batches() {
        // Канал с немедленной долговечностью после КАЖДОЙ групповой фиксации
        // оказывается «без дела»: блок вытолкнут, синхронизировать нечего.
        // Возврат буферов на этом основании означал бы освобождение и
        // повторное выделение буфера блока со scratch'ем на каждой аварийной
        // записи — ровно на том пути, ради скорости которого канал и заведён.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) = loop_with_one_channel(
            dir.path(),
            ChannelConfig::critical("critical", 16 * 1024 * 1024),
        );

        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(7),
            },
        });
        w.apply_batch();

        let held = held_bytes(&w);
        assert!(held > 0, "буферы выделены под записанный блок");
        assert_eq!(
            w.counters.snapshot().syncs,
            1,
            "групповая фиксация состоялась"
        );

        w.tick();
        assert_eq!(
            held_bytes(&w),
            held,
            "буферы обязаны пережить обслуженный батч"
        );
        assert_eq!(
            w.active.len(),
            1,
            "канал остаётся под наблюдением — иначе некому будет отдать буферы"
        );

        // Но настоящее бездействие их всё-таки забирает: отматываем начало
        // простоя за отведённую паузу.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.idle_since = Instant::now().checked_sub(RELEASE_AFTER * 2);
            assert!(ch.idle_since.is_some(), "часы монотонны и уже идут");
        }
        w.tick();
        assert_eq!(held_bytes(&w), 0, "простоявший канал возвращает память");
        assert!(w.active.is_empty(), "и уходит из списка обслуживаемых");
    }

    #[test]
    fn sync_never_throws_away_what_it_could_not_keep_up_with() {
        // Вычерпывание очереди ограничено числом проходов: иначе `sync` не
        // вернулся бы, пока пишущие потоки не замолчат. Но остаток при этом
        // НЕ выбрасывается — писать его по-прежнему есть кому, обычным ходом
        // цикла. Отбросить принятое ради операции, которая от этого ничего не
        // выигрывает, значит уничтожить данные, на которые `log()` ответил
        // `Ok`. Отбрасывает только `shutdown`, и только потому, что после
        // него записывать некому.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) =
            loop_with_one_channel(dir.path(), ChannelConfig::new("default", 16 * 1024 * 1024));

        // Один проход забирает не больше DRAIN_LIMIT записей, поэтому очередь
        // на одну запись длиннее заведомо не разбирается за отведённый
        // единственный проход — без всякой зависимости от планировщика.
        let n = DRAIN_LIMIT + 1;
        let (tx, rx) = crossbeam_channel::bounded::<Staged>(n);
        let (_ctx, crx) = crossbeam_channel::bounded::<Staged>(1);
        let item = Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(1),
            },
        };
        for _ in 0..n {
            tx.send(item.clone()).unwrap();
        }

        let drained = w.drain_pending_rounds(&rx, &crx, Leftovers::Keep, 1);
        assert!(!drained, "проход отведён один, записей больше");
        assert_eq!(rx.len(), 1, "остаток остался в очереди, а не выброшен");
        assert_eq!(
            w.counters.snapshot().dropped,
            0,
            "ничего не потеряно: писать остаток по-прежнему есть кому"
        );
        assert_eq!(
            w.counters.snapshot().records_written,
            DRAIN_LIMIT as u64,
            "забранное записано"
        );

        // Для остановки — наоборот: писать остаток будет некому, поэтому он
        // отбрасывается, но не молча, а с учётом потери. Проходов снова
        // ровно один, чтобы отказ вычерпывания был настоящим, а не следствием
        // нулевого числа попыток.
        for _ in 0..DRAIN_LIMIT {
            tx.send(item.clone()).unwrap();
        }
        let before = w.counters.snapshot().dropped;
        let drained = w.drain_pending_rounds(&rx, &crx, Leftovers::Discard, 1);
        assert!(!drained, "проход отведён один, записей больше");
        assert_eq!(rx.len(), 0, "очередь опустошена");
        assert_eq!(
            w.counters.snapshot().dropped - before,
            1,
            "отброшенная запись обязана быть учтена"
        );
    }

    #[test]
    fn a_stopping_store_refuses_instead_of_swallowing() {
        // Между вычерпыванием очереди в `shutdown` и выходом потока очередь
        // остаётся живой. Без признака остановки `try_send` отвечал бы `Ok`
        // записям, которые тут же гибнут в деструкторе канала: ни счётчика,
        // ни отметки, ни ответа вызывающему — то есть ровно та неотличимая от
        // тишины дыра, против которой заведён весь учёт потерь.
        let counters = Arc::new(Counters::default());
        let writer = Writer::spawn(Arc::clone(&counters), QueueSizes::default()).unwrap();
        let drops = DropCounters::new(1);
        let item = Staged {
            ns: NsId(0),
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(1),
            },
        };

        writer.shutdown();

        let e = writer
            .write(item, false, &drops)
            .expect_err("после остановки записывать некуда");
        assert!(
            matches!(e, Error::ShuttingDown),
            "причина названа своим именем: {e}"
        );
        assert!(e.loses_record(), "и это потеря, а не дефект вызова");
        assert_eq!(
            counters.snapshot().dropped,
            1,
            "потеря учтена, а не проглочена"
        );
        assert_eq!(drops.take(ChannelIdx(0)), 1, "и отмечена в своём канале");
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
