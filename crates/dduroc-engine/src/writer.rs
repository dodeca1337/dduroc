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

use crate::channel::ChannelConfig;
use crate::error::{Error, IoContext, Result};
use crate::rotation::{Inventory, SegmentEntry};
use crate::segment::SegmentWriter;
use crate::staged::{ChannelIdx, DropCounters, NsId, Staged, StagedRecord};
use crate::stats::Counters;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use dduroc_format::block::{BlockBuilder, BlockHeader};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{BootCounter, FooterBuilder, Level, Micros, ProtocolVersion};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
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
/// Мгновенный возврат неверен: канал с немедленной синхронизацией
/// (`sync_interval == 0` — критический) оказывается «без дела» после
/// **каждой** групповой фиксации — блок вытолкнут,
/// синхронизировать нечего, — и отдавал бы буфер блока со scratch'ем на
/// каждом батче, чтобы тут же выделить их снова. Это ровно горячий путь
/// критических записей. Пауза его не касается, а настоящее бездействие от
/// неё не убежит: держать пик лишние секунды дешевле, чем платить парой
/// аллокаций за каждую аварийную запись.
const RELEASE_AFTER: Duration = Duration::from_secs(2);

/// Сколько канал должен простоять, прежде чем отдаст **сегмент**.
///
/// Открытый сегмент стоит дескриптора и целиком числится в бюджете: файл
/// преаллоцирован на полный размер, и записанного в нём может быть сто байт.
/// При заявленных десятках тысяч каналов это десятки тысяч дескрипторов и
/// сотни гигабайт зарезервированного места за каналами, которые молчат.
///
/// Отпускание возвращает и то, и другое: файл обрезается до фактических
/// данных и закрывается. Но **не запечатывается** — следующая запись
/// продолжит его, а не заведёт новый (см. [`WriterLoop::unpark`]). Разница
/// принципиальна: канал, пишущий раз в час, при запечатывании оставлял бы по
/// файлу на запись — тысячи крохотных сегментов в год на канал, которых
/// байтовая ротация не тронет, потому что байт в них почти нет.
///
/// Пауза заметно длиннее [`RELEASE_AFTER`], потому что цена другая: буферы
/// стоят пары аллокаций, а сегмент — `fdatasync` и `ftruncate`, и обхода
/// файла при возвращении.
const PARK_AFTER: Duration = Duration::from_secs(300);

/// Источник отметок о служебных событиях в потоке записей.
const DIAG_TARGET: &str = crate::diag::TARGET;

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
    /// Подменить сегмент результатом миграции (или удалить опустевший).
    ///
    /// Тяжёлая работа миграции — чтение, преобразование, запись временного
    /// файла — идёт на потоке вызывающего; сюда приходит только **фиксация**:
    /// rename поверх старого имени и правка инвентаря. Делать её мимо writer'а
    /// нельзя — он единственный владелец инвентаря и ротации, и внешняя
    /// подмена файла гонялась бы с удалением этого же файла ротацией.
    CommitMigration {
        ns: NsId,
        channel: ChannelIdx,
        name: SegmentName,
        commit: MigrationCommit,
        /// `Ok(false)` — сегмента больше нет (ротирован), фиксировать нечего.
        reply: Sender<Result<bool>>,
    },
    /// Запечатать активные сегменты и завершить работу.
    Shutdown(Sender<()>),
}

/// Чем закончилась миграция одного сегмента.
#[derive(Debug)]
pub(crate) enum MigrationCommit {
    /// Подменить файл сегмента временным (он уже записан и синхронизирован).
    Replace {
        tmp: PathBuf,
        /// Размер нового файла — инвентарь обязан узнать его сразу, а не при
        /// следующем скане: по суммам ходит ротация и потолок хранилища.
        size: u64,
    },
    /// Удалить сегмент: все его записи удалены шагами миграции.
    Remove,
}

/// Параметры регистрации неймспейса.
#[derive(Debug)]
pub struct NsSetup {
    pub name: String,
    pub protocol_version: ProtocolVersion,
    pub store_id: u64,
    pub boot: BootCounter,
    pub channels: Vec<ChannelSpec>,
    pub drops: Arc<DropCounters>,
}

/// Один канал неймспейса глазами writer'а. Классов хранения writer не знает:
/// его дело — каталог, политика, группа бюджета и личная квота.
#[derive(Debug)]
pub struct ChannelSpec {
    /// Полный путь каталога канала — уже с учётом корня класса.
    pub dir: PathBuf,
    /// Бюджетная группа (индекс в списке групп writer'а).
    pub group: usize,
    /// Личная квота неймспейса в группе. `None` — канал черпает из общего
    /// бюджета группы без индивидуального предела.
    pub quota_bytes: Option<u64>,
    pub config: ChannelConfig,
}

/// Бюджетная группа: общий предел суммарного размера сегментов её каналов.
#[derive(Debug, Clone, Copy)]
pub struct GroupBudget {
    pub budget_bytes: u64,
    /// Ключ носителя: группы с одним `root_key` живут на одном разделе и
    /// делят давление ENOSPC.
    pub root_key: u8,
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
    ///
    /// `groups` — бюджетные группы (по одной на класс хранения): writer не
    /// знает классов, ему достаточно «каналы такой-то группы делят такой-то
    /// бюджет на таком-то носителе».
    pub fn spawn(
        counters: Arc<Counters>,
        queues: QueueSizes,
        groups: Vec<GroupBudget>,
        buffer_ceiling: Option<u64>,
        pulse: Arc<crate::pulse::Pulse>,
    ) -> Result<Arc<Self>> {
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
            groups,
            occupancy_seen: 0,
            pressured_roots: 0,
            buffer_ceiling,
            pulse,
            pulsed_blocks: 0,
            pulsed_shape: 0,
            roster_changed: false,
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

    /// Зафиксировать миграцию сегмента. `Ok(false)` — сегмент уже ротирован.
    pub(crate) fn commit_migration(
        &self,
        ns: NsId,
        channel: ChannelIdx,
        name: SegmentName,
        commit: MigrationCommit,
    ) -> Result<bool> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::CommitMigration {
                ns,
                channel,
                name,
                commit,
                reply: tx,
            })
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
    /// Бюджетная группа канала (класс хранения глазами writer'а).
    group: usize,
    /// Ключ носителя группы — копия [`GroupBudget::root_key`], чтобы путь
    /// ошибки не ходил по таблице групп.
    root_key: u8,
    /// Личная квота неймспейса, байт. `None` — только общий бюджет группы.
    quota_bytes: Option<u64>,
    /// Свой номер и счётчики потерь неймспейса: потерю, обнаруженную уже в
    /// writer'е, надо отметить там же, где образовалась дыра, — иначе она не
    /// попадёт в поток отдельной записью.
    index: ChannelIdx,
    drops: Arc<DropCounters>,
    identity: SegmentIdentity,
    inventory: Inventory,
    /// Открытый сегмент. `None` — канал ещё ничего не писал либо отпустил
    /// сегмент по бездействию ([`PARK_AFTER`]): молчащий неймспейс не должен
    /// занимать ни файлового дескриптора, ни преаллоцированных байт.
    segment: Option<SegmentWriter>,
    /// Отпущенный по бездействию сегмент: файл обрезан до данных и закрыт, но
    /// **не запечатан** — следующая запись продолжит именно его.
    ///
    /// Без этого молчащий канал заводил бы новый файл на каждое пробуждение:
    /// раз в час — восемь тысяч крохотных сегментов в год, и байтовая ротация
    /// не убрала бы ни одного, потому что байт в них почти нет. Файлы
    /// кончились бы раньше места.
    parked_segment: Option<SegmentName>,
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
    is_in_active_list: bool,
    /// С какого момента каналу нечего обслуживать. `None` — есть.
    ///
    /// Отсрочка возврата ресурсов: сперва буферы ([`RELEASE_AFTER`]), затем
    /// сегмент ([`PARK_AFTER`]).
    idle_since: Option<Instant>,
    /// Отданы ли уже буферы этого простоя.
    ///
    /// Без отметки возврат повторялся бы на каждом обороте цикла всю паузу до
    /// отпускания сегмента — четыре раза в секунду, без всякой пользы.
    buffers_released: bool,
}

impl ChannelState {
    fn new(
        spec: ChannelSpec,
        root_key: u8,
        identity: SegmentIdentity,
        index: ChannelIdx,
        drops: Arc<DropCounters>,
        counters: &Counters,
    ) -> Result<Self> {
        let ChannelSpec {
            dir,
            group,
            quota_bytes,
            config,
        } = spec;
        // След прерванной миграции: обрыв до rename оставляет `*.tmp`, чьё
        // содержимое уже никем не адресуется. Подмести обязан тот, кто первым
        // приходит в каталог, — то есть регистрация канала.
        crate::fsutil::sweep_tmp(&dir)?;
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
            group,
            root_key,
            quota_bytes,
            index,
            drops,
            identity,
            inventory,
            segment: None,
            parked_segment: None,
            footer: FooterBuilder::new(),
            scratch: Vec::new(),
            last_time: Micros(0),
            block_opened: None,
            last_sync: now,
            dirty_since_sync: false,
            is_in_active_list: false,
            idle_since: None,
            buffers_released: true,
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
    ///
    /// Индекс блоков в footer'е сюда не входит: он описывает **открытый**
    /// сегмент и будет расти дальше. Его ёмкость возвращается после
    /// запечатывания, когда индекс уже не нужен.
    fn release_buffers(&mut self) {
        self.builder.shrink_to(0);
        self.scratch = Vec::new();
    }

    /// Байты, которые канал держит в памяти под блоки.
    ///
    /// Считается ёмкость, а не занятое: буферы растут до крупнейшего
    /// прошедшего блока и остаются такими до возврата — держит канал именно
    /// ёмкость. `scratch` входит наравне с накопителем: он хранит целый
    /// сериализованный блок, и без него счёт занижался бы примерно на блок.
    ///
    /// Индекс блоков footer'а сюда не входит намеренно: он описывает открытый
    /// сегмент, живыми записями возвращён быть не может и уходит только с
    /// запечатыванием. Потолок же — про то, что можно отдать.
    fn held_bytes(&self) -> u64 {
        (self.builder.capacity() + self.scratch.capacity()) as u64
    }

    /// Запечатать сегмент, оборванный прошлым запуском.
    ///
    /// Смысл — вернуть хвост окна резерва: незапечатанный сегмент числится в
    /// бюджете канала вместе с ним, и несколько аварийных остановок подряд
    /// выедают бюджет пустотой, после чего ротация принимается за живую
    /// историю.
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
                inventory.update_size_bytes(recovered.name, recovered.size_bytes);
                Counters::bump(&counters.segments_sealed);
                if recovered.truncated {
                    Counters::bump(&counters.truncated_tails);
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
        Some(self.last_sync + self.config.sync_interval)
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
    /// Бюджетные группы: по одной на класс хранения. Индекс группы несут
    /// каналы ([`ChannelState::group`]); писателю не нужны имена классов.
    groups: Vec<GroupBudget>,
    /// Значение [`Counters::occupancy_raised`] на момент последней проверки
    /// бюджетов групп.
    ///
    /// Сторож полного прохода по флоту, как `drops_seen` у отметок о потерях:
    /// суммарная занятость растёт **только** когда канал берёт сегмент в
    /// работу или раздвигает окно резерва; запечатывание, отпускание и
    /// вытеснение её уменьшают. Поэтому неизменившийся счётчик доказывает,
    /// что считать нечего.
    occupancy_seen: u64,
    /// Потолок суммарных байт, которые пишущие каналы держат под блоки.
    /// `None` — потолка нет.
    buffer_ceiling: Option<u64>,
    /// Отметки для подписки читателя. Поднимаются раз в оборот цикла, а не на
    /// каждый блок: за один оборот writer успевает записать целый батч, и
    /// будить читателя чаще значило бы будить его на ту же порцию данных.
    pulse: Arc<crate::pulse::Pulse>,
    /// Значение [`Counters::blocks_written`] на момент последней отметки.
    pulsed_blocks: u64,
    /// Сумма счётчиков событий с сегментами на момент последней отметки.
    pulsed_shape: u64,
    /// В этом обороте поднялся неймспейс: в хранилище появились каталоги, о
    /// которых читатель не знает, а счётчиками сегментов это не видно — пустой
    /// канал файлов ещё не завёл.
    roster_changed: bool,
    /// Битовая маска корней ([`GroupBudget::root_key`]), отказавших по месту
    /// с прошлого обхода.
    ///
    /// Освобождать надо там, где место занято, а не там, где оно
    /// понадобилось: ротация внутри канала, упёршегося в ENOSPC, оставляет
    /// молчащему каналу его преаллокацию нетронутой. И именно на **том же
    /// носителе**: классы могут жить на разных разделах, и вытеснение на
    /// одном не освобождает байт на другом.
    pressured_roots: u8,
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

            // Перед сном — объявить сделанное. Команда, пришедшая на пустых
            // очередях (например `sync`), выталкивает блок прямо здесь, а
            // отметка иначе дождалась бы конца оборота, то есть истечения
            // таймаута сна: подписка узнавала бы о синхронизации через
            // секунды после неё.
            self.publish_pulse();

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
            self.publish_pulse();
        }

        self.finish();
        // Дописанное на остановке — тоже данные, и объявить о них надо до
        // закрытия: подписка обязана дочитать поток до конца, а не оборваться
        // на последней порции.
        self.publish_pulse();
        // Ожидающих больше некому будить: без этого подписка на остановленном
        // хранилище висела бы до таймаута и выглядела бы как молчащий прибор.
        self.pulse.close();
    }

    /// Объявить читателям то, что случилось за оборот цикла.
    ///
    /// Считается по уже существующим счётчикам: отдельный учёт в `flush_block`
    /// пришлось бы протаскивать через все шесть его вызывающих, а разница
    /// между «блоков стало больше» и «блок лёг вот сюда» подписке не нужна —
    /// она всё равно дочитывает сегмент с последнего своего места.
    fn publish_pulse(&mut self) {
        let blocks = self.counters.blocks_written.load(Ordering::Relaxed);
        if blocks != self.pulsed_blocks {
            self.pulsed_blocks = blocks;
            self.pulse.data_written();
        }
        // Устройство каналов меняют ровно четыре события, и все они уже
        // считаются: сегмент создан, взят в работу, запечатан, вытеснен.
        let shape = self.counters.segments_created.load(Ordering::Relaxed)
            + self.counters.segments_opened.load(Ordering::Relaxed)
            + self.counters.segments_sealed.load(Ordering::Relaxed)
            + self.counters.segments_rotated.load(Ordering::Relaxed);
        if shape != self.pulsed_shape {
            self.pulsed_shape = shape;
            self.pulse.shape_changed();
        }
        // Состав хранилища — отдельная отметка и по отдельной причине: ответ
        // на неё пропорционален всему хранилищу, а поднимается она раз за
        // жизнь сервиса. Смешать её с ротацией значило бы заставить подписку
        // обходить двадцать четыре тысячи каталогов каждые полсекунды.
        if self.roster_changed {
            self.roster_changed = false;
            self.pulse.roster_changed();
        }
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
            if let Err(e) = self.push(item) {
                // Логировать нельзя — очередь наша собственная; падать тоже:
                // отказ носителя не должен уносить с собой весь механизм
                // логирования, включая остальные каналы. Считаем и идём
                // дальше, ошибка видна через `Stats::io_errors`.
                Counters::bump(&self.counters.io_errors);
                // Кончилось место: канал уже попробовал освободить его у себя
                // и не смог. Дальше нужен взгляд на весь носитель, а он есть
                // только у обхода — см. `enforce_limits`.
                if e.is_no_space() {
                    self.note_pressure(item.ns.0 as usize, item.channel.0 as usize);
                }
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
        let mut pressured = 0u8;
        for &(ns_idx, ch_idx) in &self.active {
            let Some(ch) = self
                .namespaces
                .get_mut(ns_idx)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch_idx))
            else {
                continue;
            };
            if ch.config.sync_interval == Duration::ZERO && ch.dirty_since_sync {
                let done = Self::flush_block(ch, &counters)
                    .and_then(|()| Self::sync_channel(ch, &counters));
                if let Err(e) = done {
                    Counters::bump(&counters.io_errors);
                    if e.is_no_space() {
                        pressured |= 1 << ch.root_key.min(7);
                    }
                }
            }
        }
        self.pressured_roots |= pressured;
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
        ch.buffers_released = false;
        if !ch.is_in_active_list {
            ch.is_in_active_list = true;
            self.active.push((ns_idx, ch_idx));
        }

        if ch.builder.raw_len() >= ch.config.block_max_bytes {
            Self::flush_block(ch, &self.counters)?;
        }
        Ok(())
    }

    /// Отпустить сегмент бездействующего канала, сохранив возможность
    /// продолжить его.
    ///
    /// Файл обрезается до фактических данных (`ftruncate` после `fdatasync`)
    /// и закрывается — возвращаются и дескриптор, и непрописанный хвост
    /// преаллокации. Footer **не** пишется: индекс блоков остаётся в памяти и
    /// достроится, когда канал проснётся. Если не проснётся до остановки
    /// процесса, сегмент останется незапечатанным — он читается сканом, а
    /// footer ему допишет `recover_orphan` при следующем запуске.
    fn park_segment(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        // Накопленное — на диск, прежде чем отпускать файл. Штатный путь сюда
        // приходит с уже вытолкнутым блоком, но опираться на это незачем:
        // пустой накопитель делает вызов бесплатным.
        Self::flush_block(ch, counters)?;
        let Some(seg) = ch.segment.take() else {
            return Ok(());
        };
        let name = SegmentName::new(seg.header().boot, seg.header().base);
        let data_end = seg.data_end();
        seg.close_unsealed()?;
        // Бюджет обязан увидеть возврат: незапечатанный сегмент числился в
        // нём вместе с нерасписанным хвостом окна.
        ch.inventory.update_size_bytes(name, data_end);
        ch.parked_segment = Some(name);
        Ok(())
    }

    /// Вернуть отпущенный сегмент в работу. `false` — вернуть не вышло.
    ///
    /// Обход файла при открытии не лишний: между отпусканием и возвратом за
    /// файлом никто не следил, а восстановление позиции по обходу — уже
    /// написанный и проверенный путь. Платится он ровно раз на пробуждение и
    /// только теми каналами, которые молчали, — то есть теми, у кого сегмент
    /// мал. Места возврат не просит: окно резерва раздвинет первая запись.
    fn unpark(ch: &mut ChannelState, counters: &Counters) -> bool {
        let Some(name) = ch.parked_segment.take() else {
            return false;
        };
        let path = ch.dir.join(name.to_string());
        match SegmentWriter::reopen(
            &path,
            Some(ch.identity.store_id),
            Some(ch.config.segment_bytes),
        ) {
            Ok(seg) => {
                // Места открытие не просило: файл так и остался обрезанным до
                // конца данных, окно восстановит первая запись. Но в бюджете
                // сегмент числился обрезанным, и сверить его с носителем
                // всё равно надо — восстановление могло отбросить хвост.
                ch.inventory.update_size_bytes(name, seg.capacity());
                ch.segment = Some(seg);
                Counters::bump(&counters.segments_opened);
                true
            }
            Err(_) => {
                // Файл исчез (вытеснен, убран снаружи) или не открылся.
                // Не беда: заведём новый, а индекс прежнего больше ни к чему.
                Counters::bump(&counters.io_errors);
                ch.inventory.remove(name);
                ch.footer.reset();
                ch.footer.discard_pending();
                false
            }
        }
    }

    /// Убедиться, что в активном сегменте хватит места на целый блок.
    fn ensure_room(ch: &mut ChannelState, at: Micros, counters: &Counters) -> Result<()> {
        let need = ch.config.block_max_bytes as u64 + BlockHeader::SIZE as u64 * 2;

        if ch.segment.is_none() && ch.parked_segment.is_some() {
            Self::unpark(ch, counters);
        }
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
                    // В бюджете сегмент числится тем, что занял на носителе, —
                    // первым окном резерва, а не пределом роста. Числить предел
                    // значило бы вытеснять чужую историю ради пустоты, а на
                    // флоте — упереть практический предел одновременно пишущих
                    // каналов в `budget_bytes / segment_bytes`.
                    ch.inventory.push_newest(SegmentEntry {
                        name: SegmentName::new(boot, base),
                        size_bytes: seg.capacity(),
                    });
                    ch.segment = Some(seg);
                    ch.footer.reset();
                    Counters::bump(&counters.segments_created);
                    Counters::bump(&counters.segments_opened);
                    // Занятость выросла на первое окно резерва — бюджет
                    // обязан пересчитаться. Возвращение отпущенного сегмента
                    // (`unpark`), наоборот, места не просит: файл остаётся
                    // обрезанным, и числится он тем же, чем числился.
                    Counters::bump(&counters.occupancy_raised);
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
        // Проверка в `ensure_room` рассчитана по `block_max_bytes`, но одна
        // крупная запись могла перевалить порог: писать за предел роста
        // нельзя — это граница ротации, за ней начинается следующий сегмент.
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
                // бывает крупнее целого сегмента (несжимаемый blob). Растить
                // окно за предел значило бы отменить границу ротации: сегмент
                // рос бы под одну запись сколько угодно, а бюджет класса
                // вытесняет только целыми сегментами. Блок отбрасывается,
                // потеря объявляется отметкой в потоке.
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
        let before = ch.segment.as_ref().ok_or(Error::WriterDead)?.capacity();
        let mut placed = ch
            .segment
            .as_mut()
            .ok_or(Error::WriterDead)?
            .append_block(out);
        // Окно не раздвинулось — на носителе кончилось место. Сперва канал
        // освобождает своё, как и при создании сегмента: неудача на
        // расширении окна ничем не отличается от неудачи на создании файла,
        // и отвечать на неё потерей блока, не попробовав отдать собственную
        // историю, было бы непоследовательно.
        //
        // Попытка ровно одна: вытеснение отдаёт целый сегмент, а просят —
        // одну восьмую его. Не хватило места после этого — дело не в канале,
        // и дальше нужен взгляд на весь носитель (`enforce_limits`).
        //
        // Ошибка самого вытеснения проглатывается намеренно: наверх обязана
        // уйти исходная причина — кончившееся место, — а не то, что вдобавок
        // не удалось удалить файл. Она уже сосчитана в `io_errors`.
        if placed.as_ref().err().is_some_and(Error::is_no_space)
            && Self::rotate_one(ch, counters).unwrap_or(false)
        {
            placed = ch
                .segment
                .as_mut()
                .ok_or(Error::WriterDead)?
                .append_block(out);
        }
        let offset = placed?;
        let seg = ch.segment.as_mut().ok_or(Error::WriterDead)?;
        // Окно резерва могло раздвинуться под этот блок — тогда занятость
        // носителя выросла, и бюджет обязан увидеть рост там же, где он
        // случился. Сверка вместо безусловной записи не бережливость, а
        // условие: `update_size_bytes` ищет сегмент по имени, а зовётся это
        // на каждый блок.
        if seg.capacity() != before {
            let name = SegmentName::new(seg.header().boot, seg.header().base);
            let grown = seg.capacity();
            ch.inventory.update_size_bytes(name, grown);
            Counters::bump(&counters.occupancy_raised);
            // Личная квота неймспейса — предел по занятому, и проверять её
            // надо там, где занятое растёт. Одного создания сегмента для
            // этого мало: между двумя созданиями окно доходит от одного блока
            // до целого сегмента, и всё это время канал был бы над квотой.
            //
            // Ошибка ротации не означает потери блока — он уже лёг на
            // носитель, — поэтому наверх она не идёт: вернуть её значило бы
            // объявить потерянными записи, которые записаны.
            if Self::rotate(ch, counters).is_err() {
                Counters::bump(&counters.io_errors);
            }
        }
        Ok(Placement::At(offset))
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
            // Отпущенный сегмент так и останется незапечатанным. Открывать его
            // ради footer'а незачем: footer — оптимизация чтения, сегмент и
            // без него читается сканом, а при следующем запуске его запечатает
            // `recover_orphan` тем же обходом, которым ищет конец данных.
            // Иначе остановка стоила бы открытия и обхода каждого молчащего
            // канала — десятков тысяч при заявленном масштабе.
            return Ok(());
        };
        let name = SegmentName::new(seg.header().boot, seg.header().base);
        let data_end = seg.data_end();
        let footer = ch.footer.build();
        let footer_bytes = footer.len() as u64;
        seg.seal(&footer)?;
        // Запечатанный файл обрезан до фактических данных — бюджет обязан
        // это учесть, иначе ротация считала бы преаллокацию вечной.
        ch.inventory
            .update_size_bytes(name, data_end + footer_bytes);
        ch.footer.reset();
        ch.dirty_since_sync = false;
        Counters::bump(&counters.segments_sealed);
        Ok(())
    }

    /// Сегмент, в который канал пишет прямо сейчас или продолжит писать:
    /// вытеснять его нельзя ни ротацией, ни потолком хранилища.
    fn live_segment(ch: &ChannelState) -> Option<SegmentName> {
        ch.segment
            .as_ref()
            .map(|s| SegmentName::new(s.header().boot, s.header().base))
            .or(ch.parked_segment)
    }

    /// Удержать канал в личной квоте неймспейса, если она задана.
    ///
    /// Без квоты поканальной ротации нет вовсе: предел держит бюджет группы,
    /// и старейшее вытесняется по всему классу (`enforce_groups`).
    ///
    /// Зовётся в обеих точках роста занятости: при создании сегмента и при
    /// расширении окна резерва.
    fn rotate(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        let Some(quota_bytes) = ch.quota_bytes else {
            return Ok(());
        };
        let live = Self::live_segment(ch);
        let removed = ch.inventory.enforce_budget(&ch.dir, quota_bytes, live)?;
        Counters::add(&counters.segments_rotated, removed as u64);
        Ok(())
    }

    /// Удалить ровно один самый старый сегмент. Используется, когда на
    /// носителе кончилось место.
    fn rotate_one(ch: &mut ChannelState, counters: &Counters) -> Result<bool> {
        let live = Self::live_segment(ch);
        let Some(oldest) = ch.inventory.oldest().cloned() else {
            return Ok(false);
        };
        if Some(oldest.name) == live {
            return Ok(false);
        }
        crate::fsutil::remove_synced(&oldest.path(&ch.dir))?;
        ch.inventory.remove(oldest.name);
        Counters::bump(&counters.segments_rotated);
        Ok(true)
    }

    /// Занятость каждой группы: один проход по флоту.
    ///
    /// Полный проход дорог при десятках тысяч каналов — поэтому вызывается
    /// только когда сумма действительно могла вырасти (см. `occupancy_seen`),
    /// а не на каждом обороте цикла.
    fn group_totals(&self) -> Vec<u64> {
        let mut totals = vec![0u64; self.groups.len()];
        for ch in self
            .namespaces
            .iter()
            .flatten()
            .flat_map(|ns| ns.channels.iter())
        {
            if let Some(t) = totals.get_mut(ch.group) {
                *t += ch.inventory.total_bytes();
            }
        }
        totals
    }

    /// Самый крупный сегмент среди каналов носителя `root`.
    ///
    /// Мера того, сколько надо освободить, чтобы попытка записи имела шанс.
    /// Меньше целого сегмента брать незачем: канал под давлением по месту уже
    /// испробовал свою ротацию, и освобождать надо с запасом на его рост, а не
    /// на один блок — иначе каждый оборот стоил бы ещё одной потери.
    fn max_segment_bytes_on(&self, root: u8) -> u64 {
        self.namespaces
            .iter()
            .flatten()
            .flat_map(|ns| ns.channels.iter())
            .filter(|ch| ch.root_key == root)
            .map(|ch| ch.config.segment_bytes)
            .max()
            .unwrap_or(0)
    }

    /// Отметить отказ носителя по месту у канала `(ns, канал)`.
    fn note_pressure(&mut self, ns_idx: usize, ch_idx: usize) {
        if let Some(ch) = self
            .namespaces
            .get(ns_idx)
            .and_then(|n| n.as_ref())
            .and_then(|n| n.channels.get(ch_idx))
        {
            self.pressured_roots |= 1 << ch.root_key.min(7);
        }
    }

    /// Удалить самый старый сегмент среди каналов, прошедших отбор `pick`.
    /// Возвращает освободившиеся байты; `None` — вытеснять нечего.
    ///
    /// Порядок глобальный по построению: имя сегмента — это пара
    /// `(номер запуска, микросекунды от его старта)`, а номер запуска один на
    /// всё хранилище. Поэтому «самый старый» имеет смысл и через границу
    /// канала, и через границу неймспейса.
    ///
    /// Живой сегмент неприкосновенен — и открытый, и отпущенный по
    /// бездействию: в первый пишут, второй продолжат.
    fn evict_oldest_where(&mut self, pick: impl Fn(&ChannelState) -> bool) -> Option<u64> {
        let mut victim: Option<(usize, usize, SegmentEntry)> = None;
        for (ns_idx, slot) in self.namespaces.iter().enumerate() {
            let Some(ns) = slot.as_ref() else { continue };
            for (ch_idx, ch) in ns.channels.iter().enumerate() {
                if !pick(ch) {
                    continue;
                }
                let Some(oldest) = ch.inventory.oldest() else {
                    continue;
                };
                if Some(oldest.name) == Self::live_segment(ch) {
                    continue;
                }
                if victim.as_ref().is_none_or(|(_, _, b)| oldest.name < b.name) {
                    victim = Some((ns_idx, ch_idx, oldest.clone()));
                }
            }
        }

        let (ns_idx, ch_idx, entry) = victim?;
        let ch = &mut self.namespaces[ns_idx].as_mut()?.channels[ch_idx];
        if crate::fsutil::remove_synced(&entry.path(&ch.dir)).is_err() {
            // Не смогли удалить — вытеснять этот сегмент бессмысленно и
            // дальше: следующий вызов выбрал бы его снова и зациклился.
            Counters::bump(&self.counters.io_errors);
            return None;
        }
        ch.inventory.remove(entry.name);
        Counters::bump(&self.counters.segments_rotated);
        Some(entry.size_bytes)
    }

    /// Отпустить сегменты каналов, молчащих не меньше `quiet_for`, досрочно.
    ///
    /// Возвращает нерасписанный хвост окна резерва: файл обрезается до
    /// фактических данных. Это и есть ответ на «тихий канал держит место,
    /// пока шумный голодает» — занятое, но неиспользованное отдаётся первым,
    /// до того как вытеснение примется за чью-то историю.
    ///
    /// Полный проход по флоту, поэтому зовётся только под давлением: когда
    /// место кончилось или бюджет группы не выдержан.
    fn park_idle(&mut self, quiet_for: Duration) -> usize {
        let now = Instant::now();
        let counters = Arc::clone(&self.counters);
        let mut parked = 0;
        for ns in self.namespaces.iter_mut().flatten() {
            for ch in &mut ns.channels {
                if ch.segment.is_none()
                    || !ch
                        .idle_since
                        .is_some_and(|t| now.duration_since(t) >= quiet_for)
                {
                    continue;
                }
                if Self::park_segment(ch, &counters).is_err() {
                    Counters::bump(&counters.io_errors);
                    continue;
                }
                ch.footer.shrink_to_fit();
                // Канал, отпустивший сегмент, тем более не нуждается в буферах
                // блока. Раньше сюда попадали только под давлением по месту, и
                // память при этом не возвращалась вовсе: до обычного оборота
                // цикла с его лестницей бездействия ещё надо дожить.
                ch.release_buffers();
                ch.buffers_released = true;
                parked += 1;
            }
        }
        parked
    }

    /// Удержать хранилище в объявленных пределах.
    ///
    /// Обе причины сюда попадать редки, и обе требуют взгляда шире одного
    /// канала: поканальная квота по построению не видит, что место занято
    /// соседом.
    fn enforce_limits(&mut self) {
        let pressured = std::mem::take(&mut self.pressured_roots);
        if pressured != 0 {
            // Место на носителе кончилось. Ротация внутри канала, который в
            // него упёрся, уже испробована и не помогла — освобождаем там, где
            // место действительно занято, и на ТОМ ЖЕ носителе: сперва
            // нерасписанные хвосты окон у молчащих каналов, потом старейшая
            // история.
            //
            // Освобождать надо не «что-нибудь», а хотя бы целый сегмент:
            // меньшего не хватит на следующую попытку, и каждый оборот стоил
            // бы ещё одной потерянной записи.
            self.park_idle(RELEASE_AFTER);
            for root in 0..8u8 {
                if pressured & (1 << root) == 0 {
                    continue;
                }
                let need = self.max_segment_bytes_on(root);
                let mut freed = 0;
                while freed < need {
                    let Some(f) = self.evict_oldest_where(|ch| ch.root_key == root) else {
                        break;
                    };
                    freed += f;
                }
            }
        }

        // Сумма растёт только когда канал берёт сегмент в работу или
        // раздвигает окно резерва; пока счётчик не сдвинулся, обходить флот
        // незачем.
        let raised = self
            .counters
            .occupancy_raised
            .load(std::sync::atomic::Ordering::Relaxed);
        if raised == self.occupancy_seen && pressured == 0 {
            return;
        }
        self.occupancy_seen = raised;
        self.enforce_groups();
    }

    /// Вернуть каждую группу под её бюджет — без сторожа, безусловно.
    fn enforce_groups(&mut self) {
        let mut totals = self.group_totals();
        let mut parked = false;
        for g in 0..self.groups.len() {
            let budget_bytes = self.groups[g].budget_bytes;
            totals[g] = self.evict_group_down_to(g, budget_bytes, totals[g]);
            if totals[g] > budget_bytes && !parked {
                // Вытеснять больше нечего, но остались нерасписанные хвосты
                // окон у молчащих каналов — отдать их дешевле, чем объявлять
                // бюджет невыполнимым.
                parked = true;
                if self.park_idle(RELEASE_AFTER) > 0 {
                    totals = self.group_totals();
                    totals[g] = self.evict_group_down_to(g, budget_bytes, totals[g]);
                }
            }
            if totals[g] > budget_bytes {
                // Всё, что осталось, — живые сегменты: вытеснить их нельзя, и
                // бюджет группы в этой конфигурации попросту невыполним.
                Counters::bump(&self.counters.budget_overruns);
            }
        }
    }

    /// Вытеснять старейшее в группе, пока её сумма не влезет в бюджет.
    fn evict_group_down_to(&mut self, group: usize, budget_bytes: u64, mut total: u64) -> u64 {
        while total > budget_bytes {
            let Some(freed) = self.evict_oldest_where(|ch| ch.group == group) else {
                break;
            };
            total = total.saturating_sub(freed);
        }
        total
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
        let mut pressured = 0u8;
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
                && let Err(e) = Self::flush_block(ch, &counters)
            {
                Counters::bump(&counters.io_errors);
                if e.is_no_space() {
                    pressured |= 1 << ch.root_key.min(7);
                }
            }
            if ch.sync_deadline().is_some_and(|d| d <= now)
                && let Err(e) = Self::sync_channel(ch, &counters)
            {
                Counters::bump(&counters.io_errors);
                if e.is_no_space() {
                    pressured |= 1 << ch.root_key.min(7);
                }
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
                // Бездействие отдаётся в два приёма, по разной цене.
                //
                // Сперва буферы: их ёмкость — след самого крупного блока, и
                // держать её за молчащим каналом значит закрепить пик
                // навсегда. Но не мгновенно: Immediate-канал попадает сюда
                // после каждой групповой фиксации (см. `RELEASE_AFTER`), и
                // возврат на месте стоил бы пары аллокаций на каждую
                // аварийную запись.
                //
                // Потом сегмент: он стоит дескриптора и числится в бюджете
                // вместе с нерасписанным хвостом окна, хотя записанного в нём
                // может быть сто байт (см. `PARK_AFTER`). Отпускание обрезает
                // файл до фактических данных и закрывает дескриптор; из списка
                // обслуживаемых канал уходит только здесь — иначе отдавать
                // сегмент стало бы некому.
                let idle_since = *ch.idle_since.get_or_insert(now);
                let idle = now.duration_since(idle_since);
                if idle >= RELEASE_AFTER && !ch.buffers_released {
                    ch.release_buffers();
                    ch.buffers_released = true;
                }
                if idle >= PARK_AFTER {
                    if Self::park_segment(ch, &counters).is_err() {
                        Counters::bump(&counters.io_errors);
                    }
                    ch.footer.shrink_to_fit();
                    ch.is_in_active_list = false;
                    ch.idle_since = None;
                } else {
                    self.active.push((ns_idx, ch_idx));
                }
            }
        }

        self.pressured_roots |= pressured;
        // В самом конце: и обслуживание дедлайнов, и запечатывание по
        // бездействию только что меняли занятость носителя.
        self.enforce_limits();
        // После лестницы бездействия: она могла всё уже освободить, и потолок
        // не должен отбирать буферы у тех, кто и так их сейчас отдаёт.
        self.enforce_memory();
    }

    /// Байты, которые каналы держат в памяти под блоки.
    ///
    /// Обходится только `active` — и этого достаточно: канал уходит из списка
    /// обслуживаемых лишь при запечатывании по бездействию, а буферы к тому
    /// моменту уже возвращены. Полный проход по флоту здесь означал бы десятки
    /// тысяч каналов на каждом обороте цикла — ровно то, ради устранения чего
    /// этот список и заведён.
    fn held_bytes(&self) -> u64 {
        self.active
            .iter()
            .filter_map(|&(ns, ch)| {
                self.namespaces
                    .get(ns)?
                    .as_ref()?
                    .channels
                    .get(ch)
                    .map(ChannelState::held_bytes)
            })
            .sum()
    }

    /// Вернуть буферы блоков в объявленный потолок памяти.
    ///
    /// Потолок соблюдается **между оборотами цикла**, а не на каждой записи, и
    /// это не послабление, а единственный честный вариант. Порог закрытия
    /// блока проверяется ПОСЛЕ добавления записи, поэтому одна запись способна
    /// раздуть буфер сверх любого потолка (несжимаемый blob крупнее сегмента —
    /// уже известный случай), а отбросить её ради бухгалтерии по памяти
    /// значило бы потерять данные там, где память всего лишь неудобна.
    /// Невыполнимый потолок объявляется счётчиком — как и невыполнимый бюджет.
    ///
    /// Освобождение идёт от самых крупных: одна операция даёт больше всего, а
    /// стоит каждая выталкивания блока. Порядок обязателен — `shrink_to`
    /// отказывается сжимать заряженный накопитель, поэтому сперва flush, потом
    /// возврат.
    fn enforce_memory(&mut self) {
        let Some(ceiling) = self.buffer_ceiling else {
            return;
        };
        let mut held = self.held_bytes();
        if held <= ceiling {
            return;
        }

        let mut by_size: Vec<(usize, usize, u64)> = self
            .active
            .iter()
            .filter_map(|&(ns, ch)| {
                let state = self.namespaces.get(ns)?.as_ref()?.channels.get(ch)?;
                Some((ns, ch, state.held_bytes()))
            })
            .collect();
        by_size.sort_unstable_by_key(|&(_, _, held)| std::cmp::Reverse(held));

        let counters = Arc::clone(&self.counters);
        for (ns, ch, was) in by_size {
            if held <= ceiling {
                break;
            }
            let Some(state) = self
                .namespaces
                .get_mut(ns)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch))
            else {
                continue;
            };
            // Выталкивать некуда: сегмент отпущен или не открылся, и flush
            // отбросил бы весь блок, посчитав его записи потерянными. Память
            // такой цены не стоит. Пустой накопитель этим не связан: у него
            // нечего терять, а ёмкость он держит.
            if state.segment.is_none() && !state.builder.is_empty() {
                continue;
            }
            if let Err(e) = Self::flush_block(state, &counters) {
                Counters::bump(&counters.io_errors);
                // Место кончилось — освобождать его надо там, где оно занято,
                // и обход по носителям об этом должен узнать.
                if e.is_no_space() {
                    self.pressured_roots |= 1 << state.root_key.min(7);
                }
            }
            state.release_buffers();
            state.buffers_released = true;
            // Слак индекса блоков заодно: живых записей он не теряет, а
            // отдаётся даром.
            state.footer.shrink_to_fit();
            held = held.saturating_sub(was.saturating_sub(state.held_bytes()));
        }

        if held > ceiling {
            Counters::bump(&counters.buffer_overruns);
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
                    text: crate::diag::drop_notice(count).into_boxed_str(),
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
            Control::CommitMigration {
                ns,
                channel,
                name,
                commit,
                reply,
            } => {
                let _ = reply.send(self.commit_migration(ns, channel, name, commit));
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

    /// Выполнить фиксацию миграции сегмента на потоке writer'а.
    ///
    /// Здесь, а не на потоке вызывающего, потому что инвентарь и ротация
    /// живут только тут: подмена файла мимо writer'а гонялась бы с его же
    /// `unlink` этого файла. Сама фиксация — rename и правка числа в
    /// инвентаре, микросекунды: writer не задерживается.
    fn commit_migration(
        &mut self,
        ns: NsId,
        channel: ChannelIdx,
        name: SegmentName,
        commit: MigrationCommit,
    ) -> Result<bool> {
        let Some(ch) = self
            .namespaces
            .get_mut(ns.0 as usize)
            .and_then(|n| n.as_mut())
            .and_then(|n| n.channels.get_mut(channel.0 as usize))
        else {
            // Неймспейс уже отпущен — фиксировать не во что.
            return Ok(false);
        };
        // Сегмент мог быть ротирован, пока его переписывали: тогда его
        // история уже выброшена, и воскрешать её из временного файла нельзя —
        // бюджет про неё забыл.
        if !ch.inventory.iter().any(|e| e.name == name) {
            return Ok(false);
        }
        debug_assert_ne!(
            Some(name),
            Self::live_segment(ch),
            "мигрируют только сегменты прежних версий, живой всегда текущей"
        );
        let path = ch.dir.join(name.to_string());
        match commit {
            MigrationCommit::Replace { tmp, size } => {
                std::fs::rename(&tmp, &path).ctx_path("подмена сегмента", &path)?;
                crate::fsutil::sync_dir(&ch.dir)?;
                ch.inventory.update_size_bytes(name, size);
            }
            MigrationCommit::Remove => {
                crate::fsutil::remove_synced(&path)?;
                ch.inventory.remove(name);
            }
        }
        Ok(true)
    }

    fn register(&mut self, setup: NsSetup) -> Result<NsId> {
        // Каталогов стало больше, а счётчиками сегментов это не видно: пустой
        // канал файлов ещё не завёл. Подписке на группу неймспейсов сервис,
        // стартовавший позже, обязан достаться без её перезапуска.
        self.roster_changed = true;
        let identity = SegmentIdentity {
            protocol_version: setup.protocol_version,
            store_id: setup.store_id,
            boot: setup.boot,
        };
        let mut channels = Vec::with_capacity(setup.channels.len());
        for (i, spec) in setup.channels.into_iter().enumerate() {
            crate::fsutil::create_dir_all_synced(&spec.dir)?;
            let root_key = self
                .groups
                .get(spec.group)
                .map(|g| g.root_key)
                .unwrap_or_default();
            channels.push(ChannelState::new(
                spec,
                root_key,
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
                // Отпущенный сегмент здесь возвращается и запечатывается —
                // в отличие от остановки процесса (см. `finish`). Разница в
                // масштабе: отпускают один неймспейс, и имя его могут занять
                // следующей же строкой, а останавливают весь флот сразу, где
                // те же открытия и обходы — это десятки тысяч операций ради
                // footer'а, который следующий запуск допишет даром.
                Self::unpark(ch, &counters);
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

        // Остановка — единственный момент, когда занятость носителя точно
        // окончательна: активных сегментов больше нет, преаллокация обрезана.
        // Оставить группу над бюджетом именно здесь значило бы оставить её
        // над бюджетом до следующего запуска.
        self.enforce_groups();
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
            ChannelSpec {
                dir: dir.path().to_path_buf(),
                group: 0,
                quota_bytes: None,
                config: ChannelConfig::new(64 * 1024 * 1024),
            },
            0,
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

    /// Цикл с единственной бюджетной группой. `None` — бюджет «без предела».
    fn empty_loop(group_budget: Option<u64>) -> WriterLoop {
        WriterLoop {
            namespaces: Vec::new(),
            counters: Arc::new(Counters::default()),
            diag_target: Arc::from(DIAG_TARGET),
            batch: Vec::new(),
            drops_seen: 0,
            active: Vec::new(),
            groups: vec![GroupBudget {
                budget_bytes: group_budget.unwrap_or(u64::MAX),
                root_key: 0,
            }],
            occupancy_seen: 0,
            pressured_roots: 0,
            buffer_ceiling: None,
            pulse: Arc::new(crate::pulse::Pulse::new()),
            pulsed_blocks: 0,
            pulsed_shape: 0,
            roster_changed: false,
        }
    }

    fn add_namespace_at(
        w: &mut WriterLoop,
        name: &str,
        dir: &Path,
        channels: Vec<ChannelConfig>,
        boot: u32,
    ) -> NsId {
        let drops = Arc::new(DropCounters::new(channels.len()));
        // Имена каталогов в тестах синтетические: writer безразличен к ним.
        // Все каналы — в группе 0, без личных квот.
        let channels = channels
            .into_iter()
            .enumerate()
            .map(|(i, c)| ChannelSpec {
                dir: dir.join(format!("ch{i}")),
                group: 0,
                quota_bytes: None,
                config: c,
            })
            .collect();
        w.register(NsSetup {
            name: name.to_owned(),
            protocol_version: ProtocolVersion(1),
            store_id: 0,
            boot: BootCounter(boot),
            channels,
            drops,
        })
        .unwrap()
    }

    fn add_namespace(
        w: &mut WriterLoop,
        name: &str,
        dir: &Path,
        channels: Vec<ChannelConfig>,
    ) -> NsId {
        add_namespace_at(w, name, dir, channels, 0)
    }

    fn loop_with_one_channel(dir: &Path, config: ChannelConfig) -> (WriterLoop, NsId) {
        let mut w = empty_loop(None);
        let ns = add_namespace(&mut w, "ns", dir, vec![config]);
        (w, ns)
    }

    fn held_bytes(w: &WriterLoop) -> usize {
        w.namespaces[0].as_ref().unwrap().channels[0].held_bytes() as usize
    }

    #[test]
    fn a_declared_memory_ceiling_takes_the_buffers_back() {
        // Память на канал — активный буфер блока и его сериализованная копия.
        // Пишущих в любой момент единицы, и обычно этого достаточно; там, где
        // «единицы» перестают быть правдой, потолок отбирает буферы у самых
        // крупных держателей. Отбирает именно буферы, а не записи: запись,
        // отброшенная ради бухгалтерии по памяти, — потеря данных.
        let dir = tempfile::tempdir().unwrap();
        let config = ChannelConfig {
            block_max_bytes: 4096,
            ..ChannelConfig::new(16 * 1024 * 1024)
        };
        let (mut w, ns) = loop_with_one_channel(dir.path(), config);
        // Потолок ниже того, что раздует один крупный blob.
        w.buffer_ceiling = Some(16 * 1024);

        let noise: Vec<u8> = (0..(1 << 20)).map(|i| (i * 2654435761u64) as u8).collect();
        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::Blob(noise.as_slice().into()),
            },
        });
        w.apply_batch();
        assert!(
            held_bytes(&w) > 1 << 20,
            "мегабайтный blob обязан раздуть буферы (иначе тест пуст): {}",
            held_bytes(&w)
        );

        w.tick();
        assert!(
            (held_bytes(&w) as u64) <= 16 * 1024,
            "потолок обязан вернуть память: держится {} Б",
            held_bytes(&w)
        );
        // Запись при этом дошла до носителя, а не была принесена в жертву.
        assert_eq!(w.counters.snapshot().dropped, 0, "потолок не теряет записи");
        assert!(w.counters.snapshot().blocks_written >= 1);
        assert_eq!(
            w.counters.snapshot().buffer_overruns,
            0,
            "потолок выполнен — жаловаться не на что"
        );

        // Канал остаётся рабочим: следующая запись выделит буферы заново.
        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(2),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(7),
            },
        });
        w.apply_batch();
        assert!(held_bytes(&w) > 0, "канал жив и пишет дальше");
    }

    #[test]
    fn an_unmeetable_memory_ceiling_is_counted_not_paid_for_with_records() {
        // Одна запись бывает крупнее любого разумного потолка: буфер обязан
        // вместить хотя бы её. Отбросить такую запись значило бы потерять
        // данные там, где память всего лишь неудобна, поэтому невыполнимость
        // объявляется счётчиком — как и невыполнимый бюджет носителя.
        let dir = tempfile::tempdir().unwrap();
        let config = ChannelConfig {
            block_max_bytes: 4096,
            ..ChannelConfig::new(16 * 1024 * 1024)
        };
        let (mut w, ns) = loop_with_one_channel(dir.path(), config);
        w.buffer_ceiling = Some(16 * 1024);

        // Блок открыт и заряжен записью, но сегмента нет: вытолкнуть его —
        // значит потерять её. Потолок такой цены не платит.
        let noise: Vec<u8> = (0..(1 << 20)).map(|i| (i * 2654435761u64) as u8).collect();
        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::Blob(noise.as_slice().into()),
            },
        });
        w.apply_batch();
        {
            use dduroc_format::record::Sample;
            use dduroc_format::{Record, Value};
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.segment = None;
            ch.builder
                .push(
                    Micros(2),
                    &Record::Sample(Sample {
                        metric: MetricId(1),
                        value: Value::Blob(&noise),
                    }),
                )
                .unwrap();
        }

        w.tick();
        assert!(
            held_bytes(&w) > 1 << 20,
            "заряженный накопитель без сегмента трогать нельзя"
        );
        assert_eq!(w.counters.snapshot().dropped, 0, "ни одной записи в жертву");
        assert!(
            w.counters.snapshot().buffer_overruns >= 1,
            "невыполнимый потолок обязан быть назван"
        );
    }

    #[test]
    fn an_immediate_channel_keeps_its_buffers_between_batches() {
        // Канал с немедленной долговечностью после КАЖДОЙ групповой фиксации
        // оказывается «без дела»: блок вытолкнут, синхронизировать нечего.
        // Возврат буферов на этом основании означал бы освобождение и
        // повторное выделение буфера блока со scratch'ем на каждой аварийной
        // записи — ровно на том пути, ради скорости которого канал и заведён.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) =
            loop_with_one_channel(dir.path(), ChannelConfig::critical(16 * 1024 * 1024));

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
        assert!(
            channel_of(&w).segment.is_some(),
            "но сегмент держит: он стоит на порядок дороже буферов и \
             отдаётся отдельной, куда более длинной паузой"
        );
        assert_eq!(
            w.active.len(),
            1,
            "и остаётся под наблюдением — иначе отдавать сегмент станет некому"
        );
    }

    /// Сколько дескрипторов процесса указывают внутрь каталога.
    ///
    /// Главное следствие открытого сегмента — занятый дескриптор, и заявленный
    /// масштаб упирается в их число задолго до всего остального; увидеть это
    /// можно только через procfs. Считаются именно свои: тесты идут потоками
    /// одного процесса, и общий счёт дескрипторов гулял бы от соседей.
    fn open_fds_under(dir: &Path) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .expect("procfs смонтирована")
            .filter_map(|e| e.ok())
            .filter_map(|e| std::fs::read_link(e.path()).ok())
            .filter(|target| target.starts_with(dir))
            .count()
    }

    fn channel_of(w: &WriterLoop) -> &ChannelState {
        &w.namespaces[0].as_ref().unwrap().channels[0]
    }

    #[test]
    fn a_channel_that_went_quiet_hands_back_its_segment() {
        // Открытый сегмент стоит дескриптора и числится в бюджете ЦЕЛИКОМ:
        // файл преаллоцирован на полный размер, а записанного в нём может быть
        // сто байт. При заявленных десятках тысяч каналов молчащие держали бы
        // десятки тысяч дескрипторов и сотни гигабайт зарезервированного места
        // — притом что пишущих в любой момент единицы. Простояв отведённое,
        // канал обязан отдать и файл, и дескриптор.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) = loop_with_one_channel(dir.path(), ChannelConfig::new(16 * 1024 * 1024));
        assert_eq!(
            open_fds_under(dir.path()),
            0,
            "до первой записи канал не держит ничего"
        );

        let record = |at: u64| Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(at),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(7),
            },
        };
        w.batch.push(record(1));
        w.apply_batch();

        let path = channel_of(&w)
            .segment
            .as_ref()
            .expect("запись открыла сегмент")
            .path()
            .to_owned();
        let full = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            full,
            64 << 10,
            "резерв взят окном в один блок, а не на весь сегмент"
        );
        assert!(
            full < channel_of(&w).config.segment_bytes,
            "иначе тест пуст: окно совпало с пределом роста"
        );
        assert_eq!(
            open_fds_under(dir.path()),
            1,
            "иначе тесту нечего возвращать: дескриптор не занят"
        );

        // Дедлайны позади — блок вытолкнут, синхронизация выполнена, каналу
        // больше нечего обслуживать: с этого момента идёт отсчёт бездействия.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.block_opened = Instant::now().checked_sub(Duration::from_secs(60));
            ch.last_sync = Instant::now()
                .checked_sub(Duration::from_secs(60))
                .expect("часы монотонны и уже идут");
        }
        w.tick();
        assert!(
            channel_of(&w).segment.is_some(),
            "короткая пауза сегмент не забирает: запечатывание стоит footer'а, \
             fdatasync, ftruncate и fsync каталога — и столько же при открытии"
        );

        // А настоящее бездействие — забирает.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.idle_since = Instant::now().checked_sub(PARK_AFTER * 2);
        }
        w.tick();

        assert!(channel_of(&w).segment.is_none(), "сегмент отдан");
        assert!(w.active.is_empty(), "и канал ушёл из списка обслуживаемых");
        assert_eq!(open_fds_under(dir.path()), 0, "дескриптор вернулся системе");
        let sealed = std::fs::metadata(&path).unwrap().len();
        assert!(
            sealed < full / 100,
            "файл обрезан до фактических данных: {sealed} против {full}"
        );

        // Но НЕ запечатан. Запечатать значило бы, что следующая запись
        // заведёт новый файл: канал, пишущий раз в час, оставлял бы восемь
        // тысяч крохотных сегментов в год, и байтовая ротация не убрала бы
        // ни одного — байт в них почти нет. Файлы кончились бы раньше места.
        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        assert!(
            !reader.is_sealed(),
            "отпущенный сегмент обязан остаться продолжаемым"
        );

        // И канал продолжает ТОТ ЖЕ сегмент, а не заводит новый.
        w.batch.push(record(2));
        w.apply_batch();
        assert_eq!(
            open_segment_path(&w, ns),
            path,
            "пробуждение обязано продолжить прежний сегмент"
        );
        assert_eq!(
            std::fs::read_dir(&channel_of(&w).dir).unwrap().count(),
            1,
            "молчание не имеет права плодить файлы"
        );
        assert_eq!(w.counters.snapshot().dropped, 0);

        // Записи по обе стороны молчания на месте и читаются как один сегмент.
        WriterLoop::flush_block(
            &mut w.namespaces[0].as_mut().unwrap().channels[0],
            &Arc::clone(&w.counters),
        )
        .unwrap();
        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        let (scan, _) = {
            let file = std::fs::File::open(&path).unwrap();
            let len = file.metadata().unwrap().len();
            let mut footer = dduroc_format::FooterBuilder::new();
            crate::segment::Scan::run_collecting(&file, len, &path, &mut footer).unwrap()
        };
        assert_eq!(scan.block_count, 2, "два блока: до молчания и после");
        assert_eq!(reader.header().base, Micros(1), "и это один и тот же файл");
    }

    /// Канал с мелкими сегментами и без сжатия: тесту нужно, чтобы сегменты
    /// действительно кончались, а не сжимались до нуля.
    fn dense_channel(channel_budget: u64) -> ChannelConfig {
        ChannelConfig {
            segment_bytes: 1 << 20,
            block_max_bytes: 64 << 10,
            compression: dduroc_format::Compression::None,
            ..ChannelConfig::new(channel_budget)
        }
    }

    fn blob(ns: NsId, at: u64, len: usize) -> Staged {
        Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(at),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::Blob(std::iter::repeat_n(0xA5, len).collect()),
            },
        }
    }

    fn open_segment_path(w: &WriterLoop, ns: NsId) -> PathBuf {
        w.namespaces[ns.0 as usize].as_ref().unwrap().channels[0]
            .segment
            .as_ref()
            .expect("сегмент открыт")
            .path()
            .to_owned()
    }

    #[test]
    fn the_store_ceiling_reaches_across_namespaces() {
        // Поканальный бюджет отвечает на вопрос «сколько истории хранить у
        // этого класса», а не «сколько занять на носителе»: каналов на приборе
        // тысячи, и сумма их бюджетов кратно больше любого носителя. Потолок
        // хранилища обязан вытеснять самое старое ГДЕ УГОДНО — иначе молчащий
        // канал держит место, которого не хватает шумному, а собственный его
        // бюджет при этом не превышен и ротация в нём не срабатывает никогда.
        const CAP: u64 = 3 << 20;
        let quiet_dir = tempfile::tempdir().unwrap();
        let noisy_dir = tempfile::tempdir().unwrap();

        // Бюджет каждого канала заведомо больше потолка всего хранилища:
        // поканальная ротация в этом тесте не срабатывает ни разу.
        let mut w = empty_loop(Some(CAP));
        let quiet = add_namespace(
            &mut w,
            "quiet",
            quiet_dir.path(),
            vec![dense_channel(64 << 20)],
        );
        let noisy = add_namespace(
            &mut w,
            "noisy",
            noisy_dir.path(),
            vec![dense_channel(64 << 20)],
        );

        // Молчащий пишет первым и замолкает: его сегмент — самый старый в
        // хранилище, и запечатанный, то есть вытеснить его можно.
        w.batch.push(blob(quiet, 1, 8));
        w.apply_batch();
        let quiet_path = open_segment_path(&w, quiet);
        {
            let counters = Arc::clone(&w.counters);
            let ch = &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0];
            WriterLoop::seal_segment(ch, &counters).unwrap();
            assert!(
                ch.inventory.total_bytes() < ch.config.budget_bytes / 1000,
                "молчащий канал и близко не подходит к своему бюджету"
            );
        }
        assert!(quiet_path.exists());

        // Шумный набивает несколько сегментов подряд.
        for i in 0..80 {
            w.batch.push(blob(noisy, 1_000 + i, 64 << 10));
        }
        w.apply_batch();
        assert!(
            w.group_totals().iter().sum::<u64>() > CAP,
            "иначе вытеснять нечего и тест пуст: {}",
            w.group_totals().iter().sum::<u64>()
        );

        w.tick();

        assert!(
            !quiet_path.exists(),
            "вытесняется самое старое по ХРАНИЛИЩУ, а не по каналу: место \
             понадобилось соседу"
        );
        assert!(
            w.group_totals().iter().sum::<u64>() <= CAP,
            "хранилище обязано влезть в объявленный потолок: {} против {CAP}",
            w.group_totals().iter().sum::<u64>()
        );
        assert!(
            open_segment_path(&w, noisy).exists(),
            "активный сегмент неприкосновенен: в него пишут"
        );
        assert_eq!(
            w.counters.snapshot().budget_overruns,
            0,
            "потолок выдержан — жаловаться не на что"
        );
    }

    #[test]
    fn a_segment_left_parked_is_sealed_by_the_next_run() {
        // Отпущенный сегмент остаётся незапечатанным, и при остановке процесса
        // так и остаётся. Открывать его ради footer'а на остановке нельзя —
        // это открытие и обход каждого молчащего канала, десятков тысяч при
        // заявленном масштабе. Обещание в том, что footer допишет следующий
        // запуск: восстановление и так обходит файл, чтобы найти конец данных.
        let dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(None);
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);
        w.batch.push(blob(ns, 1, 64));
        w.apply_batch();
        let path = open_segment_path(&w, ns);

        // Дедлайны позади: блок вытолкнут, синхронизация выполнена.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.block_opened = Instant::now().checked_sub(Duration::from_secs(60));
            ch.last_sync = Instant::now()
                .checked_sub(Duration::from_secs(60))
                .expect("часы монотонны и уже идут");
        }
        w.tick();
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.idle_since = Instant::now().checked_sub(PARK_AFTER * 2);
        }
        w.tick();
        assert!(channel_of(&w).parked_segment.is_some(), "сегмент отпущен");
        assert!(
            !crate::segment::SegmentReader::open(&path)
                .unwrap()
                .is_sealed(),
            "и не запечатан"
        );
        drop(w); // процесс кончился, ничего больше не дописав

        // Следующий запуск: тот же каталог, новый номер запуска.
        let mut next = empty_loop(None);
        add_namespace_at(
            &mut next,
            "ns",
            dir.path(),
            vec![dense_channel(64 << 20)],
            1,
        );

        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        assert!(reader.is_sealed(), "следующий запуск обязан его запечатать");
        let footer = reader.footer().expect("footer читается");
        assert_eq!(
            footer.metrics,
            vec![MetricId(1)],
            "и назвать типы: по ним миграция решает, затронут ли сегмент"
        );
        assert_eq!(footer.blocks.len(), 1, "индекс блоков на месте");
    }

    #[test]
    fn no_space_is_freed_where_it_is_taken_not_where_it_is_needed() {
        // Преаллокация делается ради одного: чтобы ENOSPC пришёл при создании
        // сегмента, а не посреди записи аварийного события. Сам этот путь до
        // сих пор не исполнялся ни одним тестом — воспроизвести отказ по месту
        // на настоящей ФС без прав root нельзя, — поэтому отказ подставляется
        // ровно там, где он приходит на самом деле.
        //
        // Проверяется то, чего прежний движок не делал: канал, упёршийся в
        // ENOSPC, ротирует ТОЛЬКО СЕБЯ. Своей истории у новичка нет, всё место
        // держит сосед — и без взгляда на хранилище целиком канал замирал бы
        // навсегда, хотя освободить место есть где.
        let hoard_dir = tempfile::tempdir().unwrap();
        let new_dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(None);
        let hoarder = add_namespace(
            &mut w,
            "hoarder",
            hoard_dir.path(),
            vec![dense_channel(64 << 20)],
        );
        let newcomer = add_namespace(
            &mut w,
            "newcomer",
            new_dir.path(),
            vec![dense_channel(64 << 20)],
        );

        // Сосед набил несколько сегментов и в свой бюджет прекрасно влезает:
        // его собственная ротация не сработает никогда.
        for i in 0..64 {
            w.batch.push(blob(hoarder, 1_000 + i, 64 << 10));
        }
        w.apply_batch();
        let hoarded: Vec<_> = {
            let ch = &w.namespaces[hoarder.0 as usize].as_ref().unwrap().channels[0];
            ch.inventory.iter().map(|e| e.path(&ch.dir)).collect()
        };
        assert!(hoarded.len() >= 2, "иначе вытеснять нечего: {hoarded:?}");
        let oldest = hoarded[0].clone();

        // Носитель отказывает новичку в его первом же сегменте.
        crate::segment::fault::no_space_for(1);
        w.batch.push(blob(newcomer, 9_000, 8));
        w.apply_batch();

        assert!(
            w.namespaces[newcomer.0 as usize].as_ref().unwrap().channels[0]
                .segment
                .is_none(),
            "сегмент не создался — место кончилось"
        );
        assert_eq!(w.counters.snapshot().dropped, 1, "запись потеряна и учтена");
        assert!(oldest.exists(), "пока никто ничего не освобождал");

        w.tick();

        assert!(
            !oldest.exists(),
            "место освобождается ТАМ, ГДЕ ОНО ЗАНЯТО: у соседа, а не у того, \
             кто в него упёрся"
        );

        // И запись продолжается: канал не заклинило.
        w.batch.push(blob(newcomer, 9_001, 8));
        w.apply_batch();
        assert!(
            w.namespaces[newcomer.0 as usize].as_ref().unwrap().channels[0]
                .segment
                .is_some(),
            "после освобождения места запись обязана пойти"
        );
        assert_eq!(w.counters.snapshot().dropped, 1, "второй потери не было");
    }

    #[test]
    fn the_budget_counts_what_a_segment_occupies_not_what_it_may_grow_into() {
        // Живой сегмент числится в бюджете класса своим окном резерва, а не
        // пределом роста. Разница — не бухгалтерская: числить предел значит
        // объявить занятыми мегабайты, которых на носителе нет, и вытеснять
        // за них чужую историю. На флоте это упирает практический предел
        // одновременно пишущих каналов в `budget_bytes / segment_bytes` —
        // тридцать два канала на бюджет в четверть гигабайта.
        let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
        // Бюджет с запасом покрывает четыре окна и вчетверо меньше того, что
        // заняли бы четыре предела роста.
        let budget = 4 * ChannelConfig::new(0).segment_bytes / 2;
        let mut w = empty_loop(Some(budget));
        let counters = Arc::clone(&w.counters);
        for (i, dir) in dirs.iter().enumerate() {
            let ns = add_namespace(
                &mut w,
                &format!("ns-{i}"),
                dir.path(),
                vec![ChannelConfig::new(64 << 20)],
            );
            w.batch.push(blob(ns, 1, 8));
            w.apply_batch();
            WriterLoop::flush_block(
                &mut w.namespaces[ns.0 as usize].as_mut().unwrap().channels[0],
                &counters,
            )
            .unwrap();
        }

        let occupied = w.group_totals().iter().sum::<u64>();
        assert!(
            occupied < budget,
            "четыре канала заняли {occupied} при бюджете {budget}"
        );
        assert!(
            4 * ChannelConfig::new(0).segment_bytes > budget,
            "иначе тест пуст: пределы роста и так влезли бы в бюджет"
        );

        let paths: Vec<_> = (0..4).map(|i| open_segment_path(&w, NsId(i))).collect();
        w.tick();
        assert_eq!(
            w.counters.snapshot().budget_overruns,
            0,
            "бюджет выдержан: занято {occupied} из {budget}"
        );
        assert_eq!(
            w.counters.snapshot().segments_rotated,
            0,
            "вытеснять нечего"
        );
        for path in &paths {
            assert!(path.exists(), "живой сегмент не тронут: {path:?}");
        }
    }

    #[test]
    fn a_window_that_cannot_grow_costs_the_oldest_segment_not_the_block() {
        // Резерв берётся окном, поэтому ENOSPC приходит и посреди сегмента —
        // на расширении окна, а не только при создании файла. Это новый род
        // отказа, и отвечать на него надо тем же, чем на отказ при создании:
        // канал сперва отдаёт СВОЁ старейшее и пробует ещё раз. Иначе
        // мгновенная нехватка места стоила бы целого блока записей при живой,
        // никому не нужной истории рядом.
        let dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(Some(u64::MAX));
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);

        // Несколько сегментов истории — есть чем расплатиться.
        for i in 0..48 {
            w.batch.push(blob(ns, 1_000 + i, 64 << 10));
        }
        w.apply_batch();
        let history: Vec<_> = {
            let ch = &w.namespaces[ns.0 as usize].as_ref().unwrap().channels[0];
            ch.inventory.iter().map(|e| e.path(&ch.dir)).collect()
        };
        assert!(history.len() >= 2, "иначе платить нечем: {history:?}");
        let before = w.counters.snapshot();

        // Носитель отказывает ровно один раз — на ближайшем расширении окна.
        // Три блока: окно растёт не под каждый (шаг — восьмая доля предела),
        // но за три раза расширение случается наверняка, а до конца сегмента
        // их ещё больше десятка — значит отказ придётся именно на рост окна,
        // и это доказывает неизменившийся счётчик созданий.
        crate::segment::fault::no_space_for(1);
        for i in 0..3 {
            w.batch.push(blob(ns, 9_000 + i, 64 << 10));
        }
        w.apply_batch();
        crate::segment::fault::no_space_for(0);

        let after = w.counters.snapshot();
        assert_eq!(
            after.segments_created, before.segments_created,
            "отказ пришёлся на рост окна, а не на создание сегмента"
        );
        assert_eq!(after.dropped, before.dropped, "блок не потерян");
        assert!(
            after.segments_rotated > before.segments_rotated,
            "старейшее обязано быть отдано: {before:?} → {after:?}"
        );
        assert!(!history[0].exists(), "отдано именно старейшее");
    }

    #[test]
    fn a_window_that_cannot_grow_with_nothing_to_give_counts_the_loss() {
        // Тот же отказ, но отдать нечего: единственный сегмент — тот, в
        // который пишут. Тогда блок теряется, и это обязано быть видно и в
        // общем счётчике, и поканально — иначе дыра в потоке осталась бы
        // необъявленной, а хранилище отчиталось бы о благополучии.
        let dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(Some(u64::MAX));
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);

        // Один блок помещается в первое окно — сегмент заводится без роста.
        w.batch.push(blob(ns, 1, 8));
        w.apply_batch();
        let path = open_segment_path(&w, ns);
        let counters = Arc::clone(&w.counters);
        WriterLoop::flush_block(
            &mut w.namespaces[ns.0 as usize].as_mut().unwrap().channels[0],
            &counters,
        )
        .unwrap();
        let before = w.counters.snapshot();
        assert_eq!(
            w.namespaces[ns.0 as usize].as_ref().unwrap().channels[0]
                .inventory
                .len(),
            1,
            "иначе тест не про «отдать нечего»"
        );

        // Дальше носитель не даёт ни байта.
        crate::segment::fault::free_space(0);
        w.batch.push(blob(ns, 9_000, 64 << 10));
        w.apply_batch();
        crate::segment::fault::unlimited_space();

        let after = w.counters.snapshot();
        assert!(
            after.dropped > before.dropped,
            "потеря обязана быть сосчитана: {before:?} → {after:?}"
        );
        assert!(
            after.io_errors > before.io_errors,
            "и отказ носителя — тоже"
        );
        assert!(path.exists(), "сегмент, в который писали, цел");
        assert_ne!(w.pressured_roots, 0, "носитель отмечен как переполненный");
    }

    #[test]
    fn writing_after_a_park_counts_against_the_ceiling_again() {
        // Проснувшийся канал занимает место не пробуждением, а записью: файл
        // остаётся обрезанным, пока в него не лёг блок, и вот тогда окно
        // резерва раздвигается заново.
        //
        // Сторож пересчёта бюджета считает именно «занятость выросла», а не
        // «создан файл»: со счётом созданий этот рост прошёл бы мимо, и
        // хранилище сидело бы над потолком до чьей-нибудь следующей ротации —
        // то есть неопределённо долго.
        let quiet_dir = tempfile::tempdir().unwrap();
        let noisy_dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(Some(u64::MAX));
        let quiet = add_namespace(
            &mut w,
            "quiet",
            quiet_dir.path(),
            vec![dense_channel(64 << 20)],
        );
        let noisy = add_namespace(
            &mut w,
            "noisy",
            noisy_dir.path(),
            vec![dense_channel(64 << 20)],
        );

        w.batch.push(blob(quiet, 1, 8));
        for i in 0..64 {
            w.batch.push(blob(noisy, 1_000 + i, 64 << 10));
        }
        w.apply_batch();

        // Тихий канал отпускает сегмент: файл сжимается до сотни байт.
        {
            let ch = &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0];
            ch.block_opened = None;
            ch.idle_since = Instant::now().checked_sub(RELEASE_AFTER * 2);
        }
        assert_eq!(w.park_idle(RELEASE_AFTER), 1, "сегмент отпущен");

        // Потолок — ровно по текущей занятости.
        let cap = w.group_totals().iter().sum::<u64>();
        w.groups[0].budget_bytes = cap;
        w.tick();
        assert_eq!(
            w.group_totals().iter().sum::<u64>(),
            cap,
            "пока всё на своих местах"
        );

        // Тихий просыпается. Само пробуждение места не просит — файл как был
        // обрезан, так и остался.
        w.batch.push(blob(quiet, 2, 8));
        w.apply_batch();
        assert!(
            w.namespaces[quiet.0 as usize].as_ref().unwrap().channels[0]
                .segment
                .is_some(),
            "сегмент вернулся в работу"
        );
        assert_eq!(
            w.group_totals().iter().sum::<u64>(),
            cap,
            "пробуждение без записи места не занимает"
        );

        // А вот лёгший блок раздвигает окно — и потолок пробит.
        {
            let ch = &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0];
            ch.block_opened = Instant::now().checked_sub(Duration::from_secs(60));
        }
        let counters = std::sync::Arc::clone(&w.counters);
        WriterLoop::flush_block(
            &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0],
            &counters,
        )
        .unwrap();
        assert!(
            w.group_totals().iter().sum::<u64>() > cap,
            "иначе тест пуст: запись обязана занять место"
        );

        w.tick();
        assert!(
            w.group_totals().iter().sum::<u64>() <= cap,
            "потолок обязан быть восстановлен: {} против {cap}",
            w.group_totals().iter().sum::<u64>()
        );
    }

    #[test]
    fn an_unreachable_ceiling_is_reported_rather_than_pretended() {
        // Вытеснить можно только запечатанный сегмент: в активный пишут.
        // Потолок ниже того, что занял один активный сегмент, невыполним по
        // построению, и единственный честный исход — сказать об этом, а не
        // удалить файл из-под записи и не сделать вид, что всё в порядке.
        let dir = tempfile::tempdir().unwrap();
        // Потолок ниже одного окна резерва: канал занял 64 КиБ первым же
        // блоком, и отдать их можно только вместе с сегментом, в который пишут.
        let mut w = empty_loop(Some(8 << 10));
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);

        w.batch.push(blob(ns, 1, 8));
        w.apply_batch();
        let path = open_segment_path(&w, ns);

        w.tick();

        assert!(path.exists(), "сегмент, в который пишут, не удаляют");
        assert_eq!(
            w.counters.snapshot().budget_overruns,
            1,
            "невыполнимый потолок обязан быть виден снаружи"
        );
        assert_eq!(w.counters.snapshot().dropped, 0, "и это не потеря записей");
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
        let (mut w, ns) = loop_with_one_channel(dir.path(), ChannelConfig::new(16 * 1024 * 1024));

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
        let writer = Writer::spawn(
            Arc::clone(&counters),
            QueueSizes::default(),
            Vec::new(),
            None,
            Arc::new(crate::pulse::Pulse::new()),
        )
        .unwrap();
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
