//! Хранилище: корень, блокировка, эпохи и writer.

use crate::channel::{ChannelConfig, validate_component};
use crate::clock::{Clock, boottime_us};
use crate::epochs::{EpochStore, SyncSource};
use crate::error::{Error, IoContext, Result};
use crate::fsutil;
use crate::namespace::{Namespace, NsMeta};
use crate::schema::{Schema, StorageClass};
use crate::staged::{DropCounters, NsId};
use crate::stats::{Counters, Stats};
use crate::writer::{NsSetup, QueueSizes, Writer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// Имя файла блокировки в корне хранилища.
const LOCK_FILE: &str = ".lock";
/// Имя файла метаданных хранилища.
const STORE_META: &str = "store-meta";

/// Метаданные хранилища.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreMeta {
    /// Версия контейнерного формата.
    pub container_version: u8,
    /// Идентичность хранилища. Штампуется в каждый сегмент, чтобы файлы,
    /// скопированные с другого прибора, не смешивались с локальными: у них
    /// своя нумерация запусков и своя привязка ко времени.
    pub store_id: u64,
}

/// Настройки хранилища.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub root: PathBuf,
    /// Конфигурации каналов по имени класса хранения. Класс, которого здесь
    /// нет, получает настройки по умолчанию для своего бюджета.
    pub channels: HashMap<String, ChannelConfig>,
    /// Бюджет по умолчанию на канал неймспейса.
    pub default_budget_bytes: u64,
    /// Ёмкости очередей записи. Выделяются целиком при открытии хранилища.
    pub queues: QueueSizes,
}

impl StoreConfig {
    /// Настройки с общим бюджетом на канал.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            channels: HashMap::new(),
            default_budget_bytes: 64 * 1024 * 1024,
            queues: QueueSizes::default(),
        }
    }

    pub fn with_budget(mut self, bytes: u64) -> Self {
        self.default_budget_bytes = bytes;
        self
    }

    /// Задать ёмкости очередей записи.
    ///
    /// Меньшая очередь экономит память, но раньше начинает терять записи на
    /// всплесках; большая переживает всплеск, но откладывает момент, когда
    /// отставание диска станет заметно.
    pub fn with_queues(mut self, queues: QueueSizes) -> Self {
        self.queues = queues;
        self
    }

    /// Задать настройки конкретного канала.
    pub fn channel(mut self, config: ChannelConfig) -> Self {
        self.channels.insert(config.name.clone(), config);
        self
    }

    fn config_for(&self, class: StorageClass) -> ChannelConfig {
        if let Some(c) = self.channels.get(class.as_str()) {
            return c.clone();
        }
        if class == StorageClass::CRITICAL {
            ChannelConfig::critical(class.as_str(), self.default_budget_bytes)
        } else {
            ChannelConfig::new(class.as_str(), self.default_budget_bytes)
        }
    }
}

/// Хранилище.
#[derive(Debug)]
pub struct Store {
    root: PathBuf,
    meta: StoreMeta,
    /// Версия контейнера, с которой поднято хранилище, если она была старее
    /// текущей. Приложению стоит записать это в журнал: часть накопленной
    /// истории этим билдом не читается.
    upgraded_from: Option<u8>,
    clock: Clock,
    epochs: Mutex<EpochStore>,
    writer: Arc<Writer>,
    counters: Arc<Counters>,
    /// Счётчик спанов, общий на процесс: спан живёт внутри одного запуска,
    /// поэтому персистить его незачем.
    next_span: Arc<AtomicU32>,
    /// Открытые неймспейсы: повторное открытие того же имени в одном
    /// процессе дало бы два независимых состояния на одном каталоге.
    open: Mutex<HashMap<String, Option<NsId>>>,
    /// Держится открытым, пока живёт хранилище: снимается ядром при
    /// завершении процесса, в том числе аварийном.
    _lock: File,
}

impl Store {
    /// Открыть (или создать) хранилище.
    ///
    /// Берёт эксклюзивную блокировку корня: два процесса на одном каталоге
    /// перезаписывали бы `epochs.bin` друг друга и выдавали бы одинаковые
    /// `boot_counter`, из-за чего имена сегментов столкнулись бы.
    pub fn open(config: StoreConfig) -> Result<Arc<Self>> {
        fsutil::create_dir_all_synced(&config.root)?;
        let lock = acquire_lock(&config.root)?;
        fsutil::sweep_tmp(&config.root)?;

        let (meta, upgraded_from) = load_or_create_meta(&config.root)?;

        // База часов и запись в epochs.bin берут одно и то же значение
        // BOOTTIME: расхождение между ними уехало бы в конверсию в UTC.
        let base_us = boottime_us();
        let epochs = EpochStore::open_and_register(&config.root, base_us)?;
        let clock = Clock::with_base(base_us);

        let counters = Arc::new(Counters::default());
        let writer = Writer::spawn(Arc::clone(&counters), config.queues)?;

        Ok(Arc::new(Self {
            root: config.root.clone(),
            meta,
            upgraded_from,
            clock,
            epochs: Mutex::new(epochs),
            writer,
            counters,
            next_span: Arc::new(AtomicU32::new(1)),
            open: Mutex::new(HashMap::new()),
            _lock: lock,
        }))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn meta(&self) -> StoreMeta {
        self.meta
    }

    /// Прежняя версия контейнера, если хранилище было поднято со старой.
    ///
    /// `Some(v)` означает: сегменты версии `v` в каталоге есть, но этим билдом
    /// они не читаются и уйдут при ротации. Стоит записать это событие —
    /// молчаливая потеря доступа к истории хуже, чем явная.
    pub fn upgraded_from(&self) -> Option<u8> {
        self.upgraded_from
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    pub fn stats(&self) -> Stats {
        self.counters.snapshot()
    }

    /// Поднять неймспейс с указанной схемой.
    pub fn namespace(
        self: &Arc<Self>,
        name: &str,
        schema: Schema,
        config: &StoreConfig,
    ) -> Result<Namespace> {
        validate_component(name).map_err(|reason| Error::BadNamespace {
            name: name.to_owned(),
            reason,
        })?;
        schema.validate().map_err(|e| Error::BadNamespace {
            name: name.to_owned(),
            reason: Box::leak(e.to_string().into_boxed_str()),
        })?;

        // Занятость помечается под той же блокировкой, что и проверка:
        // между «свободен» и «занял» два потока успевали получить по
        // независимому writer-состоянию на один каталог, и оба писали бы
        // сегменты с одинаковыми именами.
        {
            let mut open = self.open.lock().map_err(|_| Error::ShuttingDown)?;
            if open.contains_key(name) {
                return Err(Error::NamespaceBusy(name.to_owned()));
            }
            open.insert(name.to_owned(), None);
        }
        // Дальше любой ранний выход обязан снять пометку, иначе имя
        // останется занятым до конца жизни процесса.
        let guard = ReserveGuard {
            store: self,
            name,
            armed: true,
        };

        let dir = self.root.join(name);
        fsutil::create_dir_all_synced(&dir)?;
        fsutil::sweep_tmp(&dir)?;

        // Схема неймспейса фиксируется при первом открытии: одинаковые
        // идентификаторы событий в разных схемах означают разное, и смешать
        // их в одном каталоге — значит расшифровывать записи чужими
        // шаблонами.
        let meta = NsMeta::open(&dir, name, &schema)?;

        let classes = schema.classes();
        let channel_configs: Vec<ChannelConfig> =
            classes.iter().map(|c| config.config_for(*c)).collect();
        for c in &channel_configs {
            c.validate()?;
        }

        let drops = Arc::new(DropCounters::new(channel_configs.len()));
        let boot = dduroc_format::BootCounter(self.boot_counter());

        let id = self.writer.register(NsSetup {
            name: name.to_owned(),
            dir,
            protocol_version: schema.version,
            store_id: self.meta.store_id,
            boot,
            channels: channel_configs,
            drops: Arc::clone(&drops),
        })?;

        if let Ok(mut open) = self.open.lock() {
            open.insert(name.to_owned(), Some(id));
        }
        guard.disarm();

        Ok(Namespace::new(
            Arc::new(NamespaceLease {
                store: Arc::clone(self),
                name: name.to_owned(),
            }),
            id,
            name.to_owned(),
            schema,
            classes,
            Arc::clone(&self.writer),
            self.clock.clone(),
            drops,
            Arc::clone(&self.next_span),
            meta,
        ))
    }

    /// Эпохи под мьютексом, с восстановлением после отравления.
    ///
    /// Отравление означает панику в другом потоке, но не противоречивые
    /// данные: под мьютексом выполняются только короткие операции над уже
    /// разобранной структурой. Отказ обошёлся бы дороже — `boot_counter`
    /// подменился бы нулём, который неотличим от настоящего первого запуска,
    /// и сегменты чужого run'а стали бы «своими».
    fn locked_epochs(&self) -> std::sync::MutexGuard<'_, EpochStore> {
        self.epochs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Зафиксировать синхронизацию времени.
    ///
    /// Возвращает `false`, если источник менее достоверен, чем уже
    /// записанный якорь (ручной ввод не перебивает GPS).
    pub fn record_sync(&self, utc_ms: i64, source: SyncSource) -> Result<bool> {
        // Заведомо невозможное время не должно портить всю историю: якорь
        // ретроактивен, и один вызов с мусором исказил бы UTC у всех событий
        // этой загрузки.
        if !is_plausible_utc_ms(utc_ms) {
            return Ok(false);
        }
        self.locked_epochs().record_sync(utc_ms, source)
    }

    /// Перевести относительное время в UTC (мс). `None` — нет якоря.
    pub fn to_utc_ms(&self, boot_counter: u32, micros: u64) -> Option<i64> {
        self.locked_epochs()
            .epochs()
            .to_utc_ms(boot_counter, micros)
    }

    /// Текущий `boot_counter`.
    pub fn boot_counter(&self) -> u32 {
        self.locked_epochs().current_run().boot_counter
    }

    /// Жив ли writer-поток.
    ///
    /// `false` означает, что записи больше не доходят до диска: либо
    /// хранилище остановлено, либо поток погиб. Потери при этом учтены в
    /// [`Stats::dropped`].
    pub fn is_writing(&self) -> bool {
        self.writer.is_alive()
    }

    /// Дождаться, пока всё накопленное окажется на носителе.
    pub fn sync(&self) -> Result<()> {
        self.writer.sync(None)
    }

    /// Завершить работу: дописать, запечатать сегменты, остановить writer.
    pub fn shutdown(&self) {
        self.writer.shutdown();
    }
}

/// Держит хранилище живым, пока жива ручка неймспейса, и освобождает имя
/// при её уничтожении.
///
/// Без первого `Store` при уничтожении остановил бы writer, а переживший его
/// `Namespace` возвращал бы `Ok` в никуда. Без второго имя оставалось бы
/// занятым до конца жизни процесса, и сервис не смог бы переоткрыть свой
/// неймспейс после перенастройки.
#[derive(Debug)]
struct NamespaceLease {
    store: Arc<Store>,
    name: String,
}

impl Drop for NamespaceLease {
    fn drop(&mut self) {
        if let Ok(mut open) = self.store.open.lock() {
            open.remove(&self.name);
        }
    }
}

/// Снимает пометку «имя занято», если подъём неймспейса не дошёл до конца.
struct ReserveGuard<'a> {
    store: &'a Store,
    name: &'a str,
    armed: bool,
}

impl ReserveGuard<'_> {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ReserveGuard<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Ok(mut open) = self.store.open.lock()
        {
            open.remove(self.name);
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // Незапечатанный сегмент читается сканом, поэтому данные не теряются
        // и без этого — но footer экономит чтению целый проход по файлу.
        self.writer.shutdown();
    }
}

/// Разумен ли момент времени: между 2001-09-09 и 2100-01-01.
///
/// Ниже границы лежат нули и мусор от неинициализированных часов, выше —
/// переполнения и заведомо испорченные значения.
fn is_plausible_utc_ms(ms: i64) -> bool {
    const MIN: i64 = 1_000_000_000_000;
    const MAX: i64 = 4_102_444_800_000;
    (MIN..MAX).contains(&ms)
}

/// Взять эксклюзивную блокировку корня хранилища.
fn acquire_lock(root: &Path) -> Result<File> {
    let path = root.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .ctx_path("открытие файла блокировки", &path)?;

    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
        |e| {
            if matches!(e, rustix::io::Errno::WOULDBLOCK) {
                // Блокировка привязана к описанию открытого файла, а не к
                // процессу: конфликт возникает и при повторном открытии того
                // же корня внутри одного процесса — а это ровно та ошибка,
                // которую нужно поймать.
                Error::StoreBusy(root.to_owned())
            } else {
                Error::Io {
                    context: format!("блокировка {}", path.display()),
                    source: e.into(),
                }
            }
        },
    )?;
    Ok(file)
}

/// Прочитать или создать метаданные хранилища.
///
/// Хранилище, записанное **прежней** версией контейнера, поднимается: файл
/// метаданных переписывается на текущую версию, `store_id` сохраняется, а
/// старые сегменты остаются лежать до ротации.
///
/// Отказаться было бы хуже всего: обновление прошивки означало бы, что
/// `Store::open` не удался и прибор перестал логировать вовсе — ровно в тот
/// момент, когда журнал нужнее всего. Данные прежней версии при этом не
/// подменяются и не выдаются за свои: заголовок каждого сегмента несёт версию
/// контейнера, и читатель сообщает о них как о непрочитанном фрагменте, а не
/// разбирает их наугад.
///
/// Версия **из будущего** по-прежнему ошибка: раскладку, которой этот билд не
/// знает, угадывать нечем.
fn load_or_create_meta(root: &Path) -> Result<(StoreMeta, Option<u8>)> {
    let path = root.join(STORE_META);
    let current = dduroc_format::CONTAINER_VERSION;

    if let Some(bytes) = fsutil::read_optional(&path)? {
        let meta: StoreMeta = postcard::from_bytes(&bytes).map_err(|_| Error::Corrupt {
            path: path.clone(),
            reason: "метаданные хранилища не разбираются".to_owned(),
        })?;
        if meta.container_version > current {
            return Err(Error::Corrupt {
                path,
                reason: format!(
                    "версия контейнера {} новее поддерживаемой ({}): раскладку из \
                     будущего этот билд разобрать не может",
                    meta.container_version, current
                ),
            });
        }
        if meta.container_version < current {
            let from = meta.container_version;
            let upgraded = StoreMeta {
                container_version: current,
                store_id: meta.store_id,
            };
            fsutil::write_atomic(&path, &postcard::to_allocvec(&upgraded)?)?;
            return Ok((upgraded, Some(from)));
        }
        return Ok((meta, None));
    }

    let meta = StoreMeta {
        container_version: current,
        store_id: fresh_store_id(),
    };
    fsutil::write_atomic(&path, &postcard::to_allocvec(&meta)?)?;
    Ok((meta, None))
}

/// Идентификатор хранилища.
///
/// Криптостойкость не нужна — задача только различать приборы, поэтому
/// хватает смеси boot_id ядра, времени и адреса в куче: тянуть генератор
/// случайных чисел ради одного значения незачем.
fn fresh_store_id() -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |bytes: &[u8]| {
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
    };
    if let Ok(id) = std::fs::read("/proc/sys/kernel/random/boot_id") {
        mix(&id);
    }
    mix(&boottime_us().to_le_bytes());
    mix(&std::process::id().to_le_bytes());
    let probe = Box::new(0u8);
    mix(&(std::ptr::from_ref(&*probe) as usize).to_le_bytes());
    if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        mix(&d.as_nanos().to_le_bytes());
    }
    h
}

/// Выделить идентификатор спана.
///
/// Нумерация локальна для запуска и не персистится. При переполнении u32
/// счётчик заворачивается на 1: чтение сопоставляет начало и конец спана
/// в пределах временного окна, а между повторами одного номера пройдут
/// миллиарды событий.
pub(crate) fn next_span_id(counter: &AtomicU32) -> dduroc_format::SpanId {
    let raw = counter.fetch_add(1, Ordering::Relaxed);
    dduroc_format::SpanId(if raw == 0 { 1 } else { raw })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_id_is_stable_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());

        let a = Store::open(cfg.clone()).unwrap();
        let id = a.meta().store_id;
        assert_ne!(id, 0);
        a.shutdown();
        drop(a);

        let b = Store::open(cfg).unwrap();
        assert_eq!(b.meta().store_id, id, "идентичность хранилища постоянна");
    }

    #[test]
    fn different_stores_get_different_ids() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let sa = Store::open(StoreConfig::new(a.path())).unwrap();
        let sb = Store::open(StoreConfig::new(b.path())).unwrap();
        assert_ne!(
            sa.meta().store_id,
            sb.meta().store_id,
            "разные хранилища обязаны различаться"
        );
    }

    #[test]
    fn second_open_of_the_same_root_is_refused() {
        // Два писателя на одном каталоге перезаписывали бы epochs.bin друг
        // друга и выдавали бы одинаковые boot_counter — имена сегментов
        // столкнулись бы. flock привязан к описанию открытого файла, поэтому
        // конфликт ловится и внутри одного процесса.
        let dir = tempfile::tempdir().unwrap();
        let first = Store::open(StoreConfig::new(dir.path())).unwrap();

        let err = Store::open(StoreConfig::new(dir.path())).unwrap_err();
        assert!(matches!(err, Error::StoreBusy(_)), "получено {err}");

        // Освобождённый корень открывается снова: иначе перезапуск сервиса
        // упирался бы в собственный файл блокировки.
        first.shutdown();
        drop(first);
        Store::open(StoreConfig::new(dir.path())).expect("корень освободился");
    }

    #[test]
    fn older_container_version_is_upgraded_not_fatal() {
        // Отказ открыть хранилище означал бы, что обновление прошивки лишает
        // прибор журнала — ровно в тот момент, когда он нужнее всего.
        // Поднимаемся, сохранив идентичность хранилища, и сообщаем о том, что
        // часть истории этим билдом не читается.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());

        let first = Store::open(cfg.clone()).unwrap();
        let store_id = first.meta().store_id;
        assert_eq!(first.upgraded_from(), None, "новое хранилище не «поднято»");
        first.shutdown();
        drop(first);

        // Подделываем прежнюю версию контейнера.
        let path = dir.path().join(STORE_META);
        let old = StoreMeta {
            container_version: 1,
            store_id,
        };
        std::fs::write(&path, postcard::to_allocvec(&old).unwrap()).unwrap();

        let upgraded = Store::open(cfg.clone()).unwrap();
        assert_eq!(upgraded.upgraded_from(), Some(1), "подъём объявлен");
        assert_eq!(
            upgraded.meta().store_id,
            store_id,
            "идентичность хранилища обязана сохраниться: иначе свои же сегменты \
             стали бы чужими"
        );
        assert_eq!(
            upgraded.meta().container_version,
            dduroc_format::CONTAINER_VERSION
        );
        upgraded.shutdown();
        drop(upgraded);

        // Повторное открытие уже не считается подъёмом.
        let again = Store::open(cfg).unwrap();
        assert_eq!(again.upgraded_from(), None);
    }

    #[test]
    fn future_container_version_is_refused() {
        // Раскладку из будущего угадывать нечем: тут отказ — единственный
        // честный ответ.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());
        let s = Store::open(cfg.clone()).unwrap();
        let store_id = s.meta().store_id;
        s.shutdown();
        drop(s);

        let future = StoreMeta {
            container_version: dduroc_format::CONTAINER_VERSION + 1,
            store_id,
        };
        std::fs::write(
            dir.path().join(STORE_META),
            postcard::to_allocvec(&future).unwrap(),
        )
        .unwrap();

        let err = Store::open(cfg).unwrap_err();
        assert!(matches!(err, Error::Corrupt { .. }), "получено {err}");
    }

    #[test]
    fn writer_liveness_is_reported_honestly() {
        // До остановки писать можно, после — нет. Прежняя проверка смотрела
        // на заполненность очередей и отвечала «жив» на любом состоянии,
        // включая мёртвый поток.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        assert!(store.is_writing(), "сразу после открытия writer работает");
        store.shutdown();
        assert!(!store.is_writing(), "после остановки записи не идут");
    }

    #[test]
    fn boot_counter_advances_between_runs() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());

        let a = Store::open(cfg.clone()).unwrap();
        assert_eq!(a.boot_counter(), 0);
        a.shutdown();
        drop(a);

        let b = Store::open(cfg).unwrap();
        assert_eq!(b.boot_counter(), 1);
    }

    #[test]
    fn implausible_utc_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).unwrap();

        assert!(!s.record_sync(0, SyncSource::Gps).unwrap(), "нулевое время");
        assert!(
            !s.record_sync(-1, SyncSource::Gps).unwrap(),
            "отрицательное время"
        );
        assert!(
            !s.record_sync(i64::MAX, SyncSource::Gps).unwrap(),
            "время за пределами разумного"
        );
        assert!(
            s.record_sync(1_700_000_000_000, SyncSource::Ntp).unwrap(),
            "правдоподобное время принимается"
        );
        // Менее достоверный источник не перебивает.
        assert!(!s.record_sync(1_800_000_000_000, SyncSource::User).unwrap());
        assert!(s.record_sync(1_800_000_000_000, SyncSource::Gps).unwrap());
    }

    #[test]
    fn span_ids_never_zero() {
        let c = AtomicU32::new(u32::MAX - 1);
        let a = next_span_id(&c);
        let b = next_span_id(&c);
        let wrapped = next_span_id(&c);
        assert_ne!(a.0, 0);
        assert_ne!(b.0, 0);
        assert_ne!(wrapped.0, 0, "после переполнения нельзя выдавать сентинел");
    }

    #[test]
    fn plausible_range() {
        assert!(!is_plausible_utc_ms(0));
        assert!(!is_plausible_utc_ms(999_999_999_999));
        assert!(is_plausible_utc_ms(1_700_000_000_000));
        assert!(!is_plausible_utc_ms(4_102_444_800_000));
    }
}
