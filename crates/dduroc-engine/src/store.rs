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
use crate::writer::{ChannelSpec, GroupBudget, NsSetup, QueueSizes, Writer};
use chrono::{DateTime, Utc};
use dduroc_format::BootTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::OpenOptionsExt;
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
///
/// Строятся цепочкой: каждый метод возвращает настройки, а не меняет их на
/// месте. `config.with_budget_per_class(n);` отдельным выражением не сделало бы ничего.
#[derive(Debug, Clone)]
#[must_use = "настройки строятся цепочкой: результат метода и есть настройки"]
pub struct StoreConfig {
    pub root: PathBuf,
    /// Конфигурации классов хранения. Класс, которого здесь нет, получает
    /// настройки по умолчанию с бюджетом [`StoreConfig::default_budget_bytes`].
    ///
    /// Ключ — [`StorageClass`], перечисление: класс с опечаткой непредставим,
    /// а имя каталога канала — производная от класса, второго источника имени
    /// не существует.
    pub channels: HashMap<StorageClass, ChannelConfig>,
    /// Бюджет по умолчанию **на класс** — для классов, не названных в
    /// [`StoreConfig::channels`].
    ///
    /// Бюджет класса — общий на всё хранилище: «вся телеметрия — столько-то».
    /// Каналы всех неймспейсов класса черпают из него, и при превышении
    /// вытесняется старейший сегмент класса, в чьём бы неймспейсе он ни
    /// лежал. Сумма бюджетов классов и есть потолок занятости; отдельной
    /// ручки «потолок хранилища» нет — при классах на разных носителях
    /// (см. [`ChannelConfig::custom_root`]) общий потолок не имел бы смысла.
    ///
    /// Потолок класса не может быть меньше того, что держат активные
    /// сегменты: их преаллокация — гарантия ENOSPC. Практический предел
    /// одновременно пишущих каналов класса — `budget_bytes / segment_bytes`;
    /// попытка выйти за него видна в [`crate::stats::Stats::budget_overruns`].
    pub default_budget_bytes: u64,
    /// Ёмкости очередей записи. Выделяются целиком при открытии хранилища.
    pub queues: QueueSizes,
}

impl StoreConfig {
    /// Настройки с общим бюджетом на каждый класс.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            channels: HashMap::new(),
            default_budget_bytes: 64 * 1024 * 1024,
            queues: QueueSizes::default(),
        }
    }

    /// Бюджет класса по умолчанию — см. [`StoreConfig::default_budget_bytes`].
    ///
    /// Столько получает каждый класс, не названный в
    /// [`StoreConfig::channel`] явно.
    pub fn with_budget_per_class(mut self, bytes: u64) -> Self {
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

    /// Задать настройки канала указанного класса хранения.
    pub fn channel(mut self, class: StorageClass, config: ChannelConfig) -> Self {
        self.channels.insert(class, config);
        self
    }

    fn config_for(&self, class: StorageClass) -> ChannelConfig {
        if let Some(c) = self.channels.get(&class) {
            return c.clone();
        }
        if class == StorageClass::Critical {
            ChannelConfig::critical(self.default_budget_bytes)
        } else {
            ChannelConfig::new(self.default_budget_bytes)
        }
    }

    /// Проверить всё, что задало приложение.
    ///
    /// Настройки канала приходят снаружи и до сих пор нигде не проверялись:
    /// бюджет меньше двух сегментов или блок размером с сегмент доезжали до
    /// writer'а как есть и ломали ротацию уже на работающем приборе.
    /// Отказать при открытии — единственный момент, когда это ещё поправимо.
    ///
    /// Проверяются и **синтезированные** конфигурации — те, что получит класс,
    /// для которого приложение ничего не задавало: `default_budget_bytes`
    /// меньше двух сегментов (16 МиБ при умолчаниях) даёт класс, у которого
    /// вытеснение съедало бы единственный сегмент сразу после запечатывания.
    fn validate(&self) -> Result<()> {
        // Немедленная синхронизация — определение критического класса, а не
        // настройка: канал, ради которого приложение готово ждать очередь,
        // не имеет права отставать от носителя. Молча перекрыть интервал
        // нельзя — оператор считал бы, что настройка действует.
        if self.config_for(StorageClass::Critical).sync_interval != std::time::Duration::ZERO {
            return Err(Error::BadChannel {
                name: StorageClass::Critical.as_str().to_owned(),
                reason: "критический канал синхронизируется сразу — в этом его \
                         смысл; настраивайте его от ChannelConfig::critical",
            });
        }

        for class in StorageClass::ALL {
            let config = self.config_for(class);
            config.validate(class.as_str())?;
        }
        Ok(())
    }
}

/// Личные квоты неймспейса внутри бюджетов классов.
///
/// Необязательны — и это умолчание осмысленно: обычный канал черпает из
/// общего бюджета своего класса и поканальной ротации не имеет вовсе. Квота
/// нужна, когда конкретный сервис нельзя пускать к общему бюджету без
/// предела: его каналы ротируются в рамках квоты, не дожидаясь, пока класс
/// упрётся в свой бюджет.
#[derive(Debug, Clone, Default)]
pub struct NsQuota {
    slots: [Option<u64>; StorageClass::ALL.len()],
}

impl NsQuota {
    pub fn new() -> Self {
        Self::default()
    }

    /// Ограничить каналы класса `class` этого неймспейса `bytes` байтами.
    pub fn limit_bytes(mut self, class: StorageClass, bytes: u64) -> Self {
        self.slots[class.index()] = Some(bytes);
        self
    }

    fn get(&self, class: StorageClass) -> Option<u64> {
        self.slots[class.index()]
    }
}

/// Хранилище.
#[derive(Debug)]
pub struct Store {
    root: PathBuf,
    /// Базовый каталог каждого класса (индекс — [`StorageClass::index`]):
    /// либо общий корень, либо собственный носитель класса.
    class_roots: Vec<PathBuf>,
    /// Все различные корни — для обходов имён (эпохи, уборка).
    scan_roots: Vec<PathBuf>,
    meta: StoreMeta,
    /// Версия контейнера, с которой поднято хранилище, если она была старее
    /// текущей. Приложению стоит записать это в журнал: часть накопленной
    /// истории этим билдом не читается.
    upgraded_from: Option<u8>,
    /// Настройки, с которыми хранилище открыто: неймспейсы берут политики
    /// каналов отсюда, а не из повторно переданного экземпляра.
    config: StoreConfig,
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
    /// Схемы, принесённые поднятыми неймспейсами, по имени схемы.
    ///
    /// Отдельно от `open` и **не** убирается вместе с ручкой: занятость имени
    /// — про запись, а умение расшифровать записи процесс не теряет от того,
    /// что ручку отпустили. Отсюда их берёт читатель, построенный по
    /// хранилищу.
    schemas: Mutex<HashMap<&'static str, Schema>>,
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
        config.validate()?;
        fsutil::create_dir_all_synced(&config.root)?;
        let lock = acquire_lock(&config.root)?;
        fsutil::sweep_tmp(&config.root)?;

        let (meta, upgraded_from) = load_or_create_meta(&config.root)?;

        // Корни классов и бюджетные группы. Ключ носителя — по фактическому
        // совпадению путей: классы на одном разделе делят давление ENOSPC,
        // классы на разных не мешают друг другу.
        let canon = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        let mut keys: Vec<PathBuf> = vec![canon(&config.root)];
        let mut class_roots = Vec::with_capacity(StorageClass::ALL.len());
        let mut scan_roots = vec![config.root.clone()];
        let mut groups = Vec::with_capacity(StorageClass::ALL.len());
        for class in StorageClass::ALL {
            let cfg = config.config_for(class);
            let base = cfg
                .custom_root
                .clone()
                .unwrap_or_else(|| config.root.clone());
            fsutil::create_dir_all_synced(&base)?;
            let c = canon(&base);
            let key = keys.iter().position(|k| *k == c).unwrap_or_else(|| {
                keys.push(c);
                scan_roots.push(base.clone());
                keys.len() - 1
            });
            groups.push(GroupBudget {
                budget_bytes: cfg.budget_bytes,
                root_key: key as u8,
            });
            class_roots.push(base);
        }

        // База часов и запись в epochs.bin берут одно и то же значение
        // BOOTTIME: расхождение между ними уехало бы в конверсию в UTC.
        let base_us = boottime_us();
        // Номер запуска не имеет права оказаться ниже того, что уже написан на
        // именах сегментов: файл эпох мог не пережить порчу или чистку, а
        // сегменты пережили, и повторно выданный номер увёл бы новые записи в
        // прошлое (см. `EpochStore::open_and_register`). Обход имён делается
        // лениво — только когда файла эпох не осталось; обходятся ВСЕ корни:
        // история класса может целиком жить на своём носителе.
        let epochs = EpochStore::open_and_register(&config.root, base_us, &|| {
            Ok(live_boots(&scan_roots)?.into_iter().next_back())
        })?;
        let clock = Clock::with_base(
            dduroc_format::BootCounter(epochs.current_run().boot_counter),
            base_us,
        );

        let counters = Arc::new(Counters::default());
        let writer = Writer::spawn(Arc::clone(&counters), config.queues, groups)?;

        let store = Arc::new(Self {
            root: config.root.clone(),
            class_roots,
            scan_roots,
            meta,
            upgraded_from,
            config,
            clock,
            epochs: Mutex::new(epochs),
            writer,
            counters,
            next_span: Arc::new(AtomicU32::new(1)),
            open: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            _lock: lock,
        });

        // Файл эпох растёт на запись за перезапуск и читается целиком при
        // каждом старте. Подметаем его, когда он действительно вырос, — иначе
        // обход имён сегментов стоил бы дороже, чем экономит.
        let runs = store.locked_epochs().epochs().runs.len();
        if runs > EPOCH_COMPACT_THRESHOLD {
            let _ = store.compact_epochs();
        }
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Все корни, в которых лежат данные хранилища; первый — основной.
    ///
    /// Их больше одного, когда класс вынесен на свой носитель
    /// ([`ChannelConfig::custom_root`]): критика на защищённом разделе — это
    /// второе дерево `<корень>/<неймспейс>/<класс>/`. Читатель обязан знать их
    /// все, иначе покажет хранилище без вынесенного класса и промолчит об
    /// этом.
    pub fn roots(&self) -> &[PathBuf] {
        &self.scan_roots
    }

    /// Схемы, которые это хранилище умеет расшифровывать.
    ///
    /// Их приносят поднятые неймспейсы; отпущенная ручка схему не уносит.
    /// Одна схема на несколько неймспейсов считается один раз.
    pub fn schemas(&self) -> Vec<Schema> {
        self.schemas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .copied()
            .collect()
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
    ///
    /// Настройки каналов берутся из тех, с которыми открыто хранилище: раньше
    /// их приходилось передавать вторым экземпляром (`Store::open(config.clone())`
    /// плюс `namespace(.., &config)`), и ничто не мешало передать сюда другие —
    /// каналы получили бы бюджет, о котором хранилище не знает.
    ///
    /// Каналы черпают из общих бюджетов своих классов; неймспейсу, которому
    /// положен личный предел, есть [`Store::namespace_with_quota`].
    pub fn namespace(self: &Arc<Self>, name: &str, schema: Schema) -> Result<Namespace> {
        self.namespace_with_quota(name, schema, NsQuota::default())
    }

    /// Поднять неймспейс с личными квотами — см. [`NsQuota`].
    pub fn namespace_with_quota(
        self: &Arc<Self>,
        name: &str,
        schema: Schema,
        quota: NsQuota,
    ) -> Result<Namespace> {
        validate_component(name).map_err(|reason| Error::BadNamespace {
            name: name.to_owned(),
            reason,
        })?;
        // Причина — обычная строка. Раньше здесь стоял `Box::leak`, потому что
        // `reason` был `&'static str`: каждая неудачная попытка подъёма
        // неймспейса навсегда оставляла в памяти свой текст, а на устройстве
        // такую попытку повторяют в цикле переподключения.
        schema.validate().map_err(|e| Error::BadSchema {
            name: schema.name.to_owned(),
            reason: e.to_string(),
        })?;

        // Занятость помечается под той же блокировкой, что и проверка:
        // между «свободен» и «занял» два потока успевали получить по
        // независимому writer-состоянию на один каталог, и оба писали бы
        // сегменты с одинаковыми именами.
        {
            let mut open = self.locked_open();
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
        // Канал живёт в каталоге своего класса — возможно, на другом
        // носителе; группа бюджета и есть класс.
        let mut specs = Vec::with_capacity(classes.len());
        for class in &classes {
            let config = self.config.config_for(*class);
            let personal = quota.get(*class);
            if let Some(q) = personal
                && q < config.segment_bytes.saturating_mul(2)
            {
                return Err(Error::BadChannel {
                    name: format!("{name}/{}", class.as_str()),
                    reason: "квота меньше двух сегментов — ротация съедала бы \
                             данные сразу после запечатывания",
                });
            }
            specs.push(ChannelSpec {
                dir: self.class_roots[class.index()]
                    .join(name)
                    .join(class.as_str()),
                group: class.index(),
                quota_bytes: personal,
                config,
            });
        }
        let channel_dirs: Vec<PathBuf> = specs.iter().map(|s| s.dir.clone()).collect();

        let drops = Arc::new(DropCounters::new(specs.len()));
        let boot = dduroc_format::BootCounter(self.boot_counter());

        let id = self.writer.register(NsSetup {
            name: name.to_owned(),
            protocol_version: schema.version,
            store_id: self.meta.store_id,
            boot,
            channels: specs,
            drops: Arc::clone(&drops),
        })?;

        self.locked_open().insert(name.to_owned(), Some(id));
        self.schemas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schema.name, schema);
        guard.disarm();

        Ok(Namespace::new(
            Arc::new(NamespaceLease {
                store: Arc::clone(self),
                name: name.to_owned(),
                id,
            }),
            id,
            name.to_owned(),
            dir,
            channel_dirs,
            self.meta.store_id,
            schema,
            classes,
            Arc::clone(&self.writer),
            self.clock.clone(),
            drops,
            Arc::clone(&self.next_span),
            meta,
        ))
    }

    /// Реестр открытых неймспейсов, с восстановлением после отравления.
    ///
    /// Причина та же, что у [`Store::locked_epochs`]: отравление означает
    /// панику в другом потоке, а не противоречивые данные — под мьютексом
    /// только вставки и удаления в готовой таблице. Отказ обошёлся бы дороже:
    /// ручка неймспейса при уничтожении не смогла бы снять пометку «занято»,
    /// имя осталось бы занятым навсегда, а его строка — неосвобождённой.
    fn locked_open(&self) -> std::sync::MutexGuard<'_, HashMap<String, Option<NsId>>> {
        self.open.lock().unwrap_or_else(|e| e.into_inner())
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
    pub fn record_sync(&self, utc: DateTime<Utc>, source: SyncSource) -> Result<bool> {
        // Заведомо невозможное время не должно портить всю историю: якорь
        // ретроактивен, и один вызов с мусором исказил бы UTC у всех событий
        // этой загрузки.
        if !is_plausible_utc(utc) {
            return Ok(false);
        }
        self.locked_epochs().record_sync(utc, source)
    }

    /// Убрать из `epochs.bin` записи о запусках, от которых не осталось
    /// сегментов. Возвращает число удалённых записей.
    ///
    /// Обходятся только **имена** файлов: `readdir` по каналам, без единого
    /// открытия сегмента. Текущий запуск не удаляется никогда — он ещё пишет.
    ///
    /// Вызывается автоматически при подъёме хранилища, но лишь когда файл
    /// действительно вырос (порядка тысячи запусков): обход каталогов при
    /// тысячах неймспейсов не бесплатен, а лишняя запись об эпохе стоит
    /// десятки байт. Приложение вправе позвать уборку и само.
    pub fn compact_epochs(&self) -> Result<usize> {
        let live = live_boots(&self.scan_roots)?;
        let mut epochs = self.locked_epochs();
        let before = epochs.epochs().runs.len();
        epochs.retain_runs(&|boot| live.contains(&boot))?;
        Ok(before - epochs.epochs().runs.len())
    }

    /// Перевести относительное время в настенное. `None` — нет якоря.
    pub fn to_utc(&self, at: BootTime) -> Option<DateTime<Utc>> {
        self.locked_epochs().epochs().to_utc(at)
    }

    /// Текущий момент в тех же координатах, что и у записей.
    pub fn now(&self) -> BootTime {
        self.clock.now_at()
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
    id: NsId,
}

impl Drop for NamespaceLease {
    fn drop(&mut self) {
        // Writer'у — ПЕРВЫМ, и только потом снимается пометка «имя занято».
        // Обратный порядок оставлял окно: между снятием пометки и отправкой
        // команды другой поток успевает поднять тот же неймспейс, его
        // `Register` встаёт в очередь команд впереди нашего `Release`, и на
        // один каталог оказывается два состояния канала — с двумя
        // инвентарями и двумя ротациями. Ротация одного удалила бы сегмент,
        // открытый другим: запись продолжалась бы в файл без имени и пропала
        // при закрытии.
        //
        // Порядок между самими командами держит их общая очередь.
        self.store.writer.release(self.id);
        self.store.locked_open().remove(&self.name);
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
        if self.armed {
            self.store.locked_open().remove(self.name);
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

/// При каком числе записей о запусках подметать `epochs.bin` на старте.
///
/// Уборка стоит обхода имён во всех каналах, а невычищенная запись — десятки
/// байт, поэтому платить за обход на каждом старте незачем: порог
/// амортизирует его на тысячу с лишним перезапусков. Файл при этом остаётся
/// ограниченным, а данные — нет: запуск, от которого остались сегменты,
/// уборка не трогает, и UTC его записей не теряется.
const EPOCH_COMPACT_THRESHOLD: usize = 1024;

/// Номера запусков, за которыми ещё стоят сегменты.
///
/// Только `readdir` и разбор имён — ни одного открытия файла: имя сегмента
/// несёт `boot_counter` первыми восемью символами.
///
/// Ошибка обхода — **отказ**, а не пустое множество. Результат используется
/// двояко, и в обеих ролях недосмотр необратим: уборка эпох по неполному
/// списку удалила бы якоря запусков, чьи сегменты просто не удалось
/// перечислить, а нижняя граница номера запуска по нему же дала бы повторную
/// нумерацию. «Не смог посмотреть» и «там ничего нет» — разные ответы.
///
/// Строгость при этом распространяется только на **своё**. Корень хранилища
/// бывает точкой монтирования, и рядом с неймспейсами лежит чужое: `lost+found`
/// с правами root, каталоги соседних подсистем, висячие симлинки. Уронить на
/// них `Store::open` значило бы оставить прибор без журнала ровно тогда, когда
/// он нужнее всего, — и ровно в том сценарии, ради которого обход заведён
/// (файл эпох не пережил прошлый запуск, в том числе при первом же старте).
///
/// Признак «это не наше» — имя: каталог неймспейса создаётся только через
/// [`Store::namespace`], которая проверяет имя тем же [`validate_component`].
/// Вход с недопустимым именем не может содержать сегментов **этого**
/// хранилища, поэтому в него не заходят вовсе — до всякого `read_dir`.
/// Всё, что прошло проверку имени, обходится строго.
fn live_boots(roots: &[PathBuf]) -> Result<std::collections::BTreeSet<u32>> {
    use crate::channel::validate_component;
    use dduroc_format::segment::SegmentName;

    /// Перечислить каталог. `NotADirectory` — обычный файл на месте каталога,
    /// `NotFound` — вход исчез между перечислением и заходом (внешняя чистка,
    /// висячий симлинк): и то, и другое означает «сегментов здесь нет», а не
    /// «не смог посмотреть».
    fn entries(path: &Path) -> Result<Vec<std::fs::DirEntry>> {
        use std::io::ErrorKind::{NotADirectory, NotFound};
        match std::fs::read_dir(path) {
            Ok(it) => it
                .collect::<std::io::Result<Vec<_>>>()
                .ctx_path("обход каталога", path),
            Err(e) if matches!(e.kind(), NotADirectory | NotFound) => Ok(Vec::new()),
            Err(e) => Err(e).ctx_path("обход каталога", path),
        }
    }

    /// Могло ли это имя быть создано хранилищем.
    fn ours(entry: &std::fs::DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .is_some_and(|n| validate_component(n).is_ok())
    }

    let mut out = std::collections::BTreeSet::new();
    for root in roots {
        for ns in entries(root)?.iter().filter(|e| ours(e)) {
            for ch in entries(&ns.path())?.iter().filter(|e| ours(e)) {
                for seg in entries(&ch.path())? {
                    if let Some(name) = seg.file_name().to_str().and_then(SegmentName::parse) {
                        out.insert(name.boot.0);
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Разумен ли момент времени: между 2001-09-09 и 2100-01-01.
///
/// Ниже границы лежат нули и мусор от неинициализированных часов, выше —
/// переполнения и заведомо испорченные значения.
fn is_plausible_utc(utc: DateTime<Utc>) -> bool {
    const MIN_MS: i64 = 1_000_000_000_000;
    const MAX_MS: i64 = 4_102_444_800_000;
    (MIN_MS..MAX_MS).contains(&utc.timestamp_millis())
}

/// Взять эксклюзивную блокировку корня хранилища.
fn acquire_lock(root: &Path) -> Result<File> {
    let path = root.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(fsutil::FILE_MODE)
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

    /// Момент времени из миллисекунд эпохи.
    fn utc_ms(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).expect("миллисекунды в пределах эпохи")
    }

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
    fn a_lost_epochs_file_never_renumbers_over_existing_segments() {
        // Сквозная проверка связки «граница ↔ скан имён»: `boot_counter`
        // попадает в имя каждого сегмента, а порядок имён объявлен временным.
        // Начать нумерацию заново поверх сегментов, переживших потерю файла
        // эпох, значит поставить новые записи в историю ПЕРЕД старыми:
        // ротация примется за свежее, читатель отдаст историю вперемешку.
        use crate::schema::{EventDesc, Language, Schema, StorageClass};
        use dduroc_format::segment::SegmentName;
        use dduroc_format::{EventId, Level, ProtocolVersion};

        static LANGS: &[Language] = &[Language("en")];
        static EVENTS: &[EventDesc] = &[EventDesc {
            id: EventId(1),
            name: "Tick",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &[],
            templates: &["tick"],
            fields: &[],
            decoders: None,
        }];
        let schema = Schema {
            name: "probe",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: &[],
            spans: &[],
            migrations: &[],
        };

        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);

        // Три запуска, каждый оставляет свой сегмент.
        for _ in 0..3 {
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-0", schema).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let channel = dir.path().join("orc-0").join("default");
        let names = |dir: &std::path::Path| -> Vec<SegmentName> {
            let mut v: Vec<SegmentName> = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().to_str().and_then(SegmentName::parse))
                .collect();
            v.sort_by_key(|n| n.to_string());
            v
        };
        let before = names(&channel);
        let highest = before.iter().map(|n| n.boot.0).max().unwrap();
        assert_eq!(highest, 2, "три запуска — сегменты запусков 0, 1, 2");

        // Файл эпох потерян: чистка носителя, карантин после порчи — исход
        // один и тот же.
        std::fs::remove_file(dir.path().join(crate::epochs::EPOCHS_FILE)).unwrap();

        let store = Store::open(cfg).unwrap();
        assert!(
            store.boot_counter() > highest,
            "номер запуска обязан продолжить, а не начать заново: \
             {} против {highest} на диске",
            store.boot_counter()
        );
        let ns = store.namespace("orc-0", schema).unwrap();
        ns.log_raw(EventId(1), &[2], None);
        ns.sync().unwrap();
        store.shutdown();

        // И самое главное — следствие: новый сегмент сортируется ПОСЛЕ всех
        // прежних, то есть попадает в конец истории, а не в её начало.
        let after = names(&channel);
        let fresh = after
            .iter()
            .find(|n| !before.contains(n))
            .expect("новый сегмент создан");
        assert!(
            before.iter().all(|old| old.to_string() < fresh.to_string()),
            "новое имя обязано сортироваться после старых: {fresh} против {before:?}"
        );
    }

    #[test]
    fn epoch_cleanup_keeps_runs_that_still_have_segments() {
        // `epochs.bin` читается целиком при каждом старте и растёт на запись
        // за перезапуск: двадцать перезапусков в сутки за пять лет — тридцать
        // шесть тысяч записей. Уборка обязана выбрасывать запуски, от которых
        // не осталось сегментов, и не трогать те, от которых осталось: их UTC
        // потерялся бы вместе с записью об эпохе.
        use crate::schema::{EventDesc, Language, Schema, StorageClass};
        use dduroc_format::{EventId, Level, ProtocolVersion};

        static LANGS: &[Language] = &[Language("en")];
        static EVENTS: &[EventDesc] = &[EventDesc {
            id: EventId(1),
            name: "Tick",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &[],
            templates: &["tick"],
            fields: &[],
            decoders: None,
        }];
        let schema = Schema {
            name: "probe",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: &[],
            spans: &[],
            migrations: &[],
        };

        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);

        // Запуск 0 оставляет после себя сегмент.
        {
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-0", schema).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store.shutdown();
        }
        // Запуски 1..4 не пишут ничего — сегментов за ними нет.
        for _ in 0..3 {
            let store = Store::open(cfg.clone()).unwrap();
            store.shutdown();
        }

        let store = Store::open(cfg).unwrap();
        assert_eq!(store.boot_counter(), 4);
        assert_eq!(store.locked_epochs().epochs().runs().len(), 5);

        let removed = store.compact_epochs().unwrap();
        assert_eq!(removed, 3, "убраны запуски без сегментов");

        let kept: Vec<u32> = store
            .locked_epochs()
            .epochs()
            .runs()
            .iter()
            .map(|r| r.boot_counter)
            .collect();
        assert_eq!(
            kept,
            vec![0, 4],
            "остались запуск с сегментами и текущий — он ещё пишет"
        );

        // Записи запуска 0 по-прежнему переводятся в UTC.
        store
            .record_sync(utc_ms(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        assert!(
            store.to_utc(BootTime::from_raw(0, 0)).is_some(),
            "уборка не имеет права лишать UTC живые данные"
        );

        // Повторная уборка ничего не находит.
        assert_eq!(store.compact_epochs().unwrap(), 0);
        store.shutdown();
    }

    #[test]
    fn implausible_utc_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).unwrap();

        assert!(
            !s.record_sync(DateTime::UNIX_EPOCH, SyncSource::Gps)
                .unwrap(),
            "нулевое время"
        );
        assert!(
            !s.record_sync(utc_ms(-1), SyncSource::Gps).unwrap(),
            "отрицательное время"
        );
        assert!(
            !s.record_sync(DateTime::<Utc>::MAX_UTC, SyncSource::Gps)
                .unwrap(),
            "время за пределами разумного"
        );
        assert!(
            s.record_sync(utc_ms(1_700_000_000_000), SyncSource::Ntp)
                .unwrap(),
            "правдоподобное время принимается"
        );
        // Менее достоверный источник не перебивает.
        assert!(
            !s.record_sync(utc_ms(1_800_000_000_000), SyncSource::User)
                .unwrap()
        );
        assert!(
            s.record_sync(utc_ms(1_800_000_000_000), SyncSource::Gps)
                .unwrap()
        );

        // Конверсия туда и обратно: запись текущего момента получает UTC.
        let at = s.now();
        let utc = s.to_utc(at).expect("якорь есть");
        assert!(
            (1_800_000_000_000..1_800_000_001_000).contains(&utc.timestamp_millis()),
            "UTC рядом с точкой синхронизации: {utc}"
        );
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
        assert!(!is_plausible_utc(utc_ms(0)));
        assert!(!is_plausible_utc(utc_ms(999_999_999_999)));
        assert!(is_plausible_utc(utc_ms(1_700_000_000_000)));
        assert!(!is_plausible_utc(utc_ms(4_102_444_800_000)));
    }
}
