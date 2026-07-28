//! Читатель: слияние потоков и резолв схемы.

use crate::cursor::{ChannelCursor, Damage, OwnedRecord, OwnedSampleValue};
use crate::error::{ReadError, Result};
use crate::query::{Bounds, Filter, KindFilter, Order, Query};
use chrono::{DateTime, Utc};
use dduroc_engine::epochs::{EpochStore, Epochs};
use dduroc_engine::namespace::{NS_META, NsMeta};
use dduroc_engine::schema::{MetricKind, Schema, Severity};
use dduroc_format::{BootCounter, BootTime, EventId, Level, MetricId, SpanId, Value};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

/// Сколько сегментов назад искать состояние на левый край окна.
///
/// Граница нужна, чтобы поиск не уходил в историю на всю глубину хранения:
/// ряд, не менявшийся месяц, обошёлся бы чтением месяца данных ради одного
/// значения. Два сегмента — это десятки мегабайт истории при типичном
/// размере, чего хватает состоянию, которое пишут при каждом изменении.
const SEED_SEGMENTS: usize = 2;

/// Одолжить владеющее значение как значение формата — для вычисления важности.
fn as_format_value(v: &OwnedSampleValue) -> Value<'_> {
    match v {
        OwnedSampleValue::F32(x) => Value::F32(*x),
        OwnedSampleValue::F64(x) => Value::F64(*x),
        OwnedSampleValue::I64(x) => Value::I64(*x),
        OwnedSampleValue::U64(x) => Value::U64(*x),
        OwnedSampleValue::Bool(x) => Value::Bool(*x),
        OwnedSampleValue::Blob(b) => Value::Blob(b),
    }
}

/// Разновидность записи в ответе.
#[derive(Debug, Clone, PartialEq)]
pub enum EntryKind {
    /// Схемное сообщение.
    Message {
        event: EventId,
        /// Имя типа из схемы; `None` — схема неизвестна этому билду.
        name: Option<&'static str>,
        level: Option<Level>,
        tags: &'static [&'static str],
        payload: Vec<u8>,
    },
    /// Свободный текст без схемы (мост из tracing, panic-handler).
    Text {
        level: Level,
        target: String,
        text: String,
    },
    SpanStart {
        span: SpanId,
        kind_name: Option<&'static str>,
        parent: Option<SpanId>,
    },
    SpanEnd {
        span: SpanId,
    },
    /// Отсчёт телеметрии.
    ///
    /// Из записи приходят только метрика и значение; всё, что нужно для
    /// показа, резолвится по схеме и на диске места не занимает. Поля
    /// `Option`, потому что схема может быть неизвестна этому билду — тогда
    /// остаются идентификатор и число, и это честнее, чем выдумать имя.
    Sample {
        metric: MetricId,
        metric_name: Option<&'static str>,
        unit: Option<&'static str>,
        /// Статические тэги-категории метрики.
        tags: &'static [&'static str],
        /// Как величину рисовать: состояние держится ступенькой, непрерывная
        /// величина интерполируется. Прямая через значения, которых не было,
        /// это не косметика, а ложь на графике.
        kind: Option<MetricKind>,
        /// Подпись состояния, если метрика — перечисление и код объявлен.
        state: Option<&'static str>,
        /// Важность значения по пределам **из схемы**.
        ///
        /// Рантайм-переопределения читателю недоступны by design: он может
        /// читать дамп с чужого прибора, где действовали другие настройки, а
        /// в сам дамп пределы не пишутся (см. `dduroc_engine::limits`).
        severity: Option<Severity>,
        value: OwnedSampleValue,
    },
    /// Нераспознанное расширение формата.
    Ext {
        bytes: Vec<u8>,
    },
}

/// Запись ответа.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub namespace: std::sync::Arc<str>,
    pub channel: std::sync::Arc<str>,
    /// Относительное время: запуск плюс микросекунды от его старта. Есть
    /// всегда — прибору для этого не нужны ни RTC, ни синхронизация.
    pub at: BootTime,
    /// Настенное время. `None` — для загрузки железа нет якоря
    /// синхронизации, и абсолютного времени у записи просто нет.
    pub utc: Option<DateTime<Utc>>,
    /// Спан, к которому привязана запись.
    pub span: Option<SpanId>,
    pub kind: EntryKind,
}

impl Entry {
    /// Уровень записи: у сообщений — из схемы, у текста — из самой записи.
    pub fn level(&self) -> Option<Level> {
        match &self.kind {
            EntryKind::Message { level, .. } => *level,
            EntryKind::Text { level, .. } => Some(*level),
            _ => None,
        }
    }
}

/// Ответ на запрос.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub entries: Vec<Entry>,
    /// Состояния на левый край окна: последний отсчёт каждого
    /// ряда-состояния **до** `from` (см. [`Query::seed_states`]).
    ///
    /// Лежат отдельно от `entries` намеренно: их время вне запрошенного
    /// диапазона, и подмешивать их к остальным значило бы нарушить обещание
    /// «всё в ответе внутри окна».
    pub seeds: Vec<Entry>,
    /// Фрагменты, которые не удалось прочитать. Пустой список — ответ полон.
    pub damaged: Vec<Damage>,
    /// Ответ обрезан по `limit`.
    pub truncated: bool,
    /// Запуски, чьи сегменты попали бы в выборку, но выпали: границы заданы
    /// настенным временем, а якоря синхронизации у этих запусков нет —
    /// сравнивать их записи с настенными часами нечем.
    ///
    /// Непустой список означает, что часть истории в ответ не попала, и **не
    /// потому, что её нет**. Без этого поля такой ответ выглядел бы как «в
    /// запрошенные часы прибор ничего не писал».
    pub unanchored: Vec<BootCounter>,
}

impl QueryResult {
    /// Полон ли ответ: ничего не пропущено из-за повреждений.
    pub fn is_complete(&self) -> bool {
        self.damaged.is_empty()
    }
}

/// Голова курсора в куче слияния.
///
/// [`BinaryHeap`] — максимальная куча, поэтому «лучший» обязан оказаться
/// наибольшим, и сравнение переворачивается для порядка от старого к новому.
/// При равном времени побеждает курсор с меньшим номером: одинаковые моменты
/// — обычное дело (часы монотонны, но не строго возрастают), и порядок между
/// ними обязан быть устойчивым, иначе один и тот же запрос выдавал бы записи
/// вразнобой от раза к разу.
#[derive(Debug, PartialEq, Eq)]
struct Head {
    at: BootTime,
    idx: usize,
    newest_first: bool,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        let by_time = if self.newest_first {
            self.at.cmp(&other.at)
        } else {
            other.at.cmp(&self.at)
        };
        by_time.then_with(|| other.idx.cmp(&self.idx))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Поток записей запроса: слияние каналов по времени без сборки ответа целиком.
///
/// Порядок тот же, что у [`Reader::query`]: курсоры сливаются по полному
/// моменту (запуск, µs), а не по микросекундам — сравнивать их между разными
/// запусками бессмысленно.
///
/// Слияние идёт кучей, а не перебором курсоров на каждую запись: курсор
/// заводится на каждую пару (неймспейс, канал), и при заявленных двадцати
/// четырёх тысячах неймспейсов линейный поиск головы означал бы десятки тысяч
/// сравнений на **каждую** выданную строчку.
#[derive(Debug)]
pub struct EntryStream<'a> {
    reader: &'a Reader,
    /// Копия запроса: разбор записи сверяется с фильтром на каждой.
    query: Query,
    bounds: Bounds,
    cursors: Vec<ChannelCursor>,
    /// Схема на каждый курсор, в том же порядке.
    schemas: Vec<Option<Schema>>,
    /// Головы непустых курсоров.
    heads: BinaryHeap<Head>,
    /// Каталоги, которые не удалось прочитать — известны сразу при открытии.
    damaged: Vec<Damage>,
    limit: usize,
    yielded: usize,
    truncated: bool,
    /// Обход окончен: повторный вызов не должен выдавать записи после того,
    /// как поток уже ответил `None`.
    done: bool,
}

impl EntryStream<'_> {
    /// Вернуть голову курсора в кучу, если она есть.
    fn requeue(&mut self, idx: usize) {
        let newest_first = self.query.order == Order::Newest;
        if let Some(head) = self.cursors[idx].peek() {
            let at = head.at;
            self.heads.push(Head {
                at,
                idx,
                newest_first,
            });
        }
    }
}

impl Iterator for EntryStream<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.done {
            return None;
        }
        loop {
            let Some(Head { idx, .. }) = self.heads.pop() else {
                self.done = true;
                return None;
            };
            let taken = self.cursors[idx].next_entry();
            self.requeue(idx);
            let Some(raw) = taken else { continue };

            // Записи вне окна пропускаются, но обход продолжается: сегмент мог
            // начаться раньше нижней границы.
            if !self.bounds.contains(raw.at) {
                continue;
            }

            let ns = std::sync::Arc::clone(&self.cursors[idx].namespace);
            let ch = std::sync::Arc::clone(&self.cursors[idx].channel);
            if let Some(entry) =
                self.reader
                    .build_entry(ns, ch, self.schemas[idx].as_ref(), &raw, &self.query)
            {
                // Обрезка объявляется, только когда очередная запись
                // действительно **есть**, а места в ответе уже нет. Ставить
                // отметку при входе значило бы объявлять обрезанным всякий
                // ответ ровно в `limit` записей, даже если больше их и не было
                // — а для веб-слоя это вечная кнопка «дальше».
                if self.yielded >= self.limit {
                    self.truncated = true;
                    self.done = true;
                    return None;
                }
                self.yielded += 1;
                return Some(entry);
            }
        }
    }
}

impl EntryStream<'_> {
    /// Фрагменты, которые не удалось прочитать. Полон список только после
    /// того, как поток исчерпан: повреждение обнаруживается при чтении блока.
    pub fn damaged(&self) -> Vec<Damage> {
        let mut out = self.damaged.clone();
        for c in &self.cursors {
            out.extend_from_slice(c.damaged());
        }
        out
    }

    /// Запуски, чьи сегменты выпали из выборки за неимением якоря — см.
    /// [`QueryResult::unanchored`].
    pub fn unanchored(&self) -> Vec<BootCounter> {
        let mut out: Vec<BootCounter> = Vec::new();
        for c in &self.cursors {
            for boot in c.unanchored() {
                if !out.contains(boot) {
                    out.push(*boot);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Оборван ли обход по `limit` запроса.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Сколько записей уже выдано.
    pub fn yielded(&self) -> usize {
        self.yielded
    }
}

/// Открытые под запрос курсоры вместе с разрешёнными схемами.
#[derive(Debug)]
struct OpenedCursors {
    cursors: Vec<ChannelCursor>,
    /// Схема на каждый курсор, в том же порядке.
    schemas: Vec<Option<Schema>>,
    /// Каталоги, которые не удалось прочитать.
    damaged: Vec<Damage>,
}

/// Сведения о неймспейсе хранилища.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub name: String,
    pub schema_name: String,
    pub protocol_version: u16,
    pub channels: Vec<String>,
    /// Суммарный размер сегментов, байт.
    pub bytes: u64,
}

/// Читатель хранилища.
#[derive(Debug)]
pub struct Reader {
    root: PathBuf,
    /// Схемы приложения по имени: без них записи остаются идентификаторами.
    schemas: HashMap<String, Schema>,
    epochs: Epochs,
    /// Идентичность хранилища; `None` — не проверять (чтение чужого дампа
    /// разрешено явно).
    store_id: Option<u64>,
    /// Схема, разрешённая по имени неймспейса. Резолв читает `ns-meta` с
    /// диска, а спрашивают о нём на **каждую** показываемую запись — файловая
    /// операция на запись была бы ровно тем дефектом, который уже убран с
    /// пути запроса. `None` в значении — «неймспейс есть, схемы к нему у
    /// этого билда нет»; такой ответ тоже стоит запомнить.
    schema_cache: RwLock<HashMap<String, Option<Schema>>>,
}

impl Reader {
    /// Открыть хранилище на чтение.
    ///
    /// Только чтение: ни восстановления, ни уборки временных файлов. Вьюер
    /// не имеет права изменять дамп, который ему дали посмотреть.
    pub fn open(root: impl Into<PathBuf>, schemas: &[Schema]) -> Result<Self> {
        let root = root.into();
        let epochs = EpochStore::open_read_only(&root).unwrap_or_default();
        let store_id = read_store_id(&root);
        Ok(Self {
            root,
            schemas: schemas.iter().map(|s| (s.name.to_owned(), *s)).collect(),
            epochs,
            store_id,
            schema_cache: RwLock::new(HashMap::new()),
        })
    }

    /// Не проверять принадлежность сегментов этому хранилищу.
    ///
    /// Нужно для разбора чужого дампа, собранного из нескольких приборов;
    /// абсолютное время таких записей доверять нельзя.
    pub fn allow_foreign_segments(mut self) -> Self {
        self.store_id = None;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    /// Текст записи на указанном языке.
    ///
    /// `None` — рендерить нечего: это не сообщение схемы, либо схема
    /// неизвестна этому билду (тогда у записи остаются идентификатор и
    /// payload, и выдумывать текст читатель не станет).
    ///
    /// Схема ищется по неймспейсу самой записи. Раньше её приходилось искать
    /// вызывающему и передавать вручную — а он мог передать чужую, и тогда
    /// payload разбирался бы декодером другого события: текст получался бы
    /// правдоподобным и неверным.
    pub fn render(&self, entry: &Entry, lang: &str) -> Option<String> {
        match &entry.kind {
            EntryKind::Message { event, payload, .. } => {
                let schema = self.schema_of(&entry.namespace)?;
                render(&schema, *event, payload, lang)
            }
            // Свободный текст уже текст: у него нет ни шаблона, ни языков.
            EntryKind::Text { text, .. } => Some(text.clone()),
            _ => None,
        }
    }

    /// Схема неймспейса — по его метаданным на диске.
    ///
    /// Ищется имя схемы каталога, а не первая подходящая: два неймспейса могут
    /// жить под разными схемами в одном хранилище.
    ///
    /// Ответ запоминается: `ns-meta` читается один раз на неймспейс, а не на
    /// каждый вызов. [`Reader::render`] зовут на каждую показываемую запись, и
    /// без этого отрисовка пятисот строк стоила бы пятисот открытий файла.
    /// Схема — `Copy` и лежит в `.rodata`, копия ничего не стоит.
    pub fn schema_of(&self, namespace: &str) -> Option<Schema> {
        if let Ok(cache) = self.schema_cache.read()
            && let Some(found) = cache.get(namespace)
        {
            return *found;
        }
        let resolved = read_ns_meta(&self.root.join(namespace))
            .and_then(|meta| self.schemas.get(&meta.schema_name).copied());
        if let Ok(mut cache) = self.schema_cache.write() {
            cache.insert(namespace.to_owned(), resolved);
        }
        resolved
    }

    /// Перечислить неймспейсы вместе с занятым объёмом.
    ///
    /// Объём стоит `stat` каждого сегмента, поэтому путь запроса им не
    /// пользуется: ему хватает имён неймспейсов и каналов.
    pub fn namespaces(&self) -> Result<Vec<NamespaceInfo>> {
        let mut out = Vec::new();
        for ns in self.namespace_dirs(&mut Vec::new())? {
            let mut bytes = 0;
            for channel in &ns.channels {
                if let Ok(inv) = dduroc_engine::rotation::Inventory::scan(
                    &self.root.join(&ns.name).join(channel),
                ) {
                    bytes += inv.total_bytes();
                }
            }
            out.push(NamespaceInfo { bytes, ..ns });
        }
        Ok(out)
    }

    /// Неймспейсы и их каналы — без обращения к размерам файлов.
    ///
    /// `bytes` в результате всегда нулевой: на пути запроса он не нужен, а
    /// стоил бы `stat` на каждый сегмент. Каталоги с нечитаемой метой
    /// накапливаются в `unreadable`.
    fn namespace_dirs(&self, unreadable: &mut Vec<PathBuf>) -> Result<Vec<NamespaceInfo>> {
        let mut out = Vec::new();
        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(source) => {
                return Err(ReadError::Io {
                    context: format!("чтение {}", self.root.display()),
                    source,
                });
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(meta) = read_ns_meta(&path) else {
                // Каталог без метаданных — не неймспейс (чужая директория в
                // корне хранилища). Каталог с НЕЧИТАЕМОЙ метой — это
                // неймспейс, который мы не можем показать, и молчать об этом
                // нельзя: его данные просто исчезли бы из всех ответов.
                if path.join(NS_META).exists() {
                    unreadable.push(path);
                }
                continue;
            };

            let mut channels = Vec::new();
            if let Ok(dir) = std::fs::read_dir(&path) {
                for ch in dir.flatten() {
                    let ch_path = ch.path();
                    if !ch_path.is_dir() {
                        continue;
                    }
                    if let Some(ch_name) = ch_path.file_name().and_then(|n| n.to_str()) {
                        channels.push(ch_name.to_owned());
                    }
                }
            }
            channels.sort();

            out.push(NamespaceInfo {
                name: name.to_owned(),
                schema_name: meta.schema_name,
                protocol_version: meta.protocol_version,
                channels,
                bytes: 0,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Открыть запрос потоком: записи разбираются по мере обхода.
    ///
    /// Память ограничена одним распакованным блоком на канал — тем же, чем
    /// ограничен курсор. Это единственный способ читать много: [`Reader::query`]
    /// собирает ответ целиком, и запрос без `limit` по двухсотгигабайтному
    /// хранилищу не поместился бы в памяти armv7.
    ///
    /// Обход можно прервать в любой момент; о повреждениях и выпавших запусках
    /// поток сообщает теми же средствами, что и `query`, — но полны эти
    /// сведения только после того, как поток исчерпан либо оставлен.
    pub fn stream(&self, q: &Query) -> Result<EntryStream<'_>> {
        // Границы приводятся к относительной шкале один раз: настенная
        // граница требует якоря на каждый запуск, и делать это на запись
        // значило бы линейный поиск по эпохам на каждую из сотен тысяч.
        let bounds = q.resolve(&self.epochs).bounds;
        let OpenedCursors {
            mut cursors,
            schemas,
            damaged,
        } = self.open_cursors(q, &bounds)?;

        // Кучу заряжаем сразу: пустой курсор в неё просто не попадает, и
        // дальше слияние не тратит на него ни одного сравнения.
        let newest_first = q.order == Order::Newest;
        let mut heads = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors.iter_mut().enumerate() {
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first,
                });
            }
        }

        Ok(EntryStream {
            reader: self,
            query: q.clone(),
            bounds,
            cursors,
            schemas,
            heads,
            damaged,
            limit: q.limit.unwrap_or(usize::MAX),
            yielded: 0,
            truncated: false,
            done: false,
        })
    }

    /// Выполнить запрос, собрав ответ целиком.
    ///
    /// Годится, когда объём ответа ограничен — `limit`, узкое окно, редкий тип
    /// событий. Для всего остального есть [`Reader::stream`].
    pub fn query(&self, q: &Query) -> Result<QueryResult> {
        let mut stream = self.stream(q)?;
        let entries: Vec<Entry> = stream.by_ref().collect();
        let mut result = QueryResult {
            entries,
            truncated: stream.truncated(),
            // Повреждения собираются и при обрезке по лимиту: ответ, из
            // которого часть данных выпала из-за порчи, не должен выглядеть
            // полным.
            damaged: stream.damaged(),
            unanchored: stream.unanchored(),
            seeds: Vec::new(),
        };

        if q.seed_states && q.from.is_some() {
            let bounds = stream.bounds.clone();
            result.seeds = self.collect_state_seeds(q, &bounds, &mut result.damaged)?;
        }
        Ok(result)
    }

    /// Найти последний отсчёт каждого ряда-состояния до начала окна.
    ///
    /// Ищется обратным обходом от `from` назад, и он **ограничен**
    /// [`SEED_SEGMENTS`] сегментами: ряд, не менявшийся очень долго, останется
    /// без затравки, и это честнее, чем читать всю историю ради одного
    /// значения. Приложению, которому важна полная картина, стоит писать
    /// состояние ещё и при старте — тогда затравка всегда рядом.
    ///
    /// Сегменты, в которых нужных метрик нет вовсе, отбрасываются по множеству
    /// идентификаторов из footer'а, без чтения блоков.
    fn collect_state_seeds(
        &self,
        q: &Query,
        window: &Bounds,
        damaged: &mut Vec<Damage>,
    ) -> Result<Vec<Entry>> {
        let Some(from) = q.from else {
            return Ok(Vec::new());
        };

        // Ряды-состояния собираются по схемам, а не по данным: их немного, и
        // знать их заранее дешевле, чем выяснять чтением.
        //
        // Объединение по всем схемам нужно только для отсечения сегментов по
        // множеству из footer'а: там оно работает как «в этом сегменте нет
        // ничего похожего», и лишний номер приводит лишь к лишнему открытию.
        // Отбирать записи по нему нельзя — пространство `metric_id` своё у
        // каждой схемы, и метрика чужой схемы с тем же номером означает
        // совсем другую величину.
        let union: HashSet<MetricId> = self
            .schemas
            .values()
            .flat_map(|s| s.metrics.iter())
            .filter(|m| m.kind == MetricKind::State)
            .map(|m| m.id)
            .collect();
        if union.is_empty() {
            return Ok(Vec::new());
        }
        let union = std::sync::Arc::new(union);

        // Ищем строго ДО `from`, порядок — от свежих к старым, чтобы первым
        // встреченным отсчётом ряда оказался последний по времени.
        let probe = Query {
            from: None,
            to: Some(from.just_before()),
            order: Order::Newest,
            limit: None,
            seed_states: false,
            filter: Filter {
                kinds: KindFilter::TELEMETRY,
                ..q.filter.clone()
            },
            ..q.clone()
        };
        let probe_bounds = probe.resolve(&self.epochs).bounds;

        let OpenedCursors {
            mut cursors,
            schemas,
            damaged: open_damaged,
        } = self.open_cursors_with(&probe, &probe_bounds, |scope| {
            scope.max_segments = Some(SEED_SEGMENTS);
            scope.require_metrics = Some(std::sync::Arc::clone(&union));
        })?;
        damaged.extend(open_damaged);

        // Первое встреченное значение ряда и есть последнее по времени.
        let mut seen: HashSet<(usize, MetricId)> = HashSet::new();
        let mut out: Vec<Entry> = Vec::new();

        for (idx, cursor) in cursors.iter_mut().enumerate() {
            // Ряды-состояния берутся из схемы ЭТОГО неймспейса: номера метрик
            // у разных схем свои, и общий набор дал бы затравку из чужого
            // ряда — с чужой подписью состояния.
            let wanted = state_metrics(schemas[idx].as_ref());
            if wanted.is_empty() {
                continue;
            }
            while let Some(raw) = cursor.next_entry() {
                // Затравка обязана лежать строго ДО окна. Проверяется именно
                // «не внутри окна», а не «раньше границы на микросекунду»:
                // граница может быть настенной, а у запуска, начавшегося с
                // нулевой микросекунды, вычитать не из чего.
                if window.contains(raw.at) || !probe_bounds.contains(raw.at) {
                    continue;
                }
                let OwnedRecord::Sample { metric, .. } = &raw.record else {
                    continue;
                };
                if !wanted.contains(metric) || !seen.insert((idx, *metric)) {
                    continue;
                }
                let ns = std::sync::Arc::clone(&cursor.namespace);
                let ch = std::sync::Arc::clone(&cursor.channel);
                if let Some(entry) = self.build_entry(ns, ch, schemas[idx].as_ref(), &raw, &probe) {
                    out.push(entry);
                }
                // Все ряды этого канала найдены — дальше читать нечего.
                if wanted.iter().all(|m| seen.contains(&(idx, *m))) {
                    break;
                }
            }
        }
        for c in &cursors {
            damaged.extend_from_slice(c.damaged());
        }

        out.sort_by_key(|e| e.at);
        Ok(out)
    }

    /// Открыть курсоры и разрешить схему **один раз на неймспейс**.
    ///
    /// Резолв схемы читает `ns-meta` с диска. Делать это на каждую запись,
    /// как было поначалу, означало бы файловую операцию на запись — именно
    /// это и оказалось главным ограничителем скорости чтения.
    fn open_cursors(&self, q: &Query, bounds: &Bounds) -> Result<OpenedCursors> {
        self.open_cursors_with(q, bounds, |_| {})
    }

    /// То же с правкой параметров открытия — для поиска затравок, где нужны
    /// граница просмотра и отсечение сегментов по метрикам.
    fn open_cursors_with(
        &self,
        q: &Query,
        bounds: &Bounds,
        adjust: impl Fn(&mut crate::cursor::ChannelScope),
    ) -> Result<OpenedCursors> {
        let mut cursors = Vec::new();
        let mut schemas = Vec::new();
        let mut unreadable = Vec::new();

        for ns in self.namespace_dirs(&mut unreadable)? {
            if !q.namespaces.matches(&ns.name) {
                continue;
            }
            let schema = self.schemas.get(&ns.schema_name).copied();
            let ns_name: std::sync::Arc<str> = std::sync::Arc::from(ns.name.as_str());
            let mut scope = crate::cursor::ChannelScope {
                bounds: bounds.clone(),
                boot: q.boot,
                reverse: q.order == Order::Newest,
                expect_store: self.store_id,
                prefilter: Some(build_prefilter(q, schema)),
                max_segments: None,
                require_metrics: None,
            };
            adjust(&mut scope);

            for channel in &ns.channels {
                if !q.channels.is_empty() && !q.channels.contains(channel) {
                    continue;
                }
                let dir = self.root.join(&ns.name).join(channel);
                cursors.push(ChannelCursor::open(
                    &dir,
                    std::sync::Arc::clone(&ns_name),
                    std::sync::Arc::from(channel.as_str()),
                    &scope,
                )?);
                schemas.push(schema);
            }
        }

        let damaged = unreadable
            .into_iter()
            .map(|path| Damage {
                path,
                offset: 0,
                reason: "метаданные неймспейса не читаются".to_owned(),
            })
            .collect();
        Ok(OpenedCursors {
            cursors,
            schemas,
            damaged,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_entry(
        &self,
        ns: std::sync::Arc<str>,
        channel: std::sync::Arc<str>,
        schema: Option<&Schema>,
        raw: &crate::cursor::RawEntry,
        q: &Query,
    ) -> Option<Entry> {
        let kinds = q.filter.kinds;
        let (kind, span) = match &raw.record {
            OwnedRecord::Message {
                event,
                span,
                payload,
            } => {
                if !kinds.messages {
                    return None;
                }
                let desc = schema.and_then(|s| s.event(*event));
                // Уровень и тэги — статические свойства типа, поэтому фильтр
                // применяется здесь, без чтения payload'а.
                if let Some(min) = q.filter.min_level {
                    match desc.map(|d| d.level) {
                        Some(l) if l >= min => {}
                        // Уровень неизвестен — запись не отбрасывается: это
                        // событие удалённого из схемы типа, и молча прятать
                        // его от того, кто ищет проблему, нельзя.
                        None => {}
                        _ => return None,
                    }
                }
                if !q.filter.any_tags.is_empty() {
                    let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                    if !q
                        .filter
                        .any_tags
                        .iter()
                        .any(|want| tags.iter().any(|t| t == want))
                    {
                        return None;
                    }
                }
                if let Some(want) = &q.filter.events
                    && !want.contains(event)
                {
                    return None;
                }
                if !q.filter.event_names.is_empty() {
                    let name = desc.map(|d| d.name).unwrap_or("");
                    if !q.filter.event_names.iter().any(|n| n == name) {
                        return None;
                    }
                }
                (
                    EntryKind::Message {
                        event: *event,
                        name: desc.map(|d| d.name),
                        level: desc.map(|d| d.level),
                        tags: desc.map(|d| d.tags).unwrap_or(&[]),
                        payload: payload.clone(),
                    },
                    *span,
                )
            }
            OwnedRecord::Text {
                level,
                span,
                target,
                text,
            } => {
                if !kinds.text {
                    return None;
                }
                if let Some(min) = q.filter.min_level
                    && *level < min
                {
                    return None;
                }
                (
                    EntryKind::Text {
                        level: *level,
                        target: target.clone(),
                        text: text.clone(),
                    },
                    *span,
                )
            }
            OwnedRecord::SpanStart { span, kind, parent } => {
                if !kinds.spans {
                    return None;
                }
                (
                    EntryKind::SpanStart {
                        span: *span,
                        kind_name: schema.and_then(|s| s.span(*kind)).map(|d| d.name),
                        parent: *parent,
                    },
                    Some(*span),
                )
            }
            OwnedRecord::SpanEnd { span } => {
                if !kinds.spans {
                    return None;
                }
                (EntryKind::SpanEnd { span: *span }, Some(*span))
            }
            OwnedRecord::Sample { metric, value } => {
                if !kinds.samples {
                    return None;
                }
                // Идентичность ряда лежит в самой записи: метрика и есть ряд.
                // Всё остальное — имя, единица, подпись состояния, важность,
                // поведение между отсчётами — резолвится по схеме и на диске
                // места не занимает.
                let desc = schema.and_then(|s| s.metric(*metric));
                let code = match value {
                    OwnedSampleValue::U64(v) => Some(*v),
                    OwnedSampleValue::I64(v) if *v >= 0 => Some(*v as u64),
                    OwnedSampleValue::Bool(b) => Some(u64::from(*b)),
                    _ => None,
                };
                (
                    EntryKind::Sample {
                        metric: *metric,
                        metric_name: desc.map(|d| d.name),
                        unit: desc.map(|d| d.unit),
                        tags: desc.map_or(&[][..], |d| d.tags),
                        kind: desc.map(|d| d.kind),
                        state: desc.zip(code).and_then(|(d, c)| d.state(c)).map(|s| s.name),
                        severity: desc.map(|d| d.severity_of(&as_format_value(value))),
                        value: value.clone(),
                    },
                    None,
                )
            }
            OwnedRecord::Ext { bytes } => (
                EntryKind::Ext {
                    bytes: bytes.clone(),
                },
                None,
            ),
        };

        // Записи вне спанов отбрасываются вместе со всеми, чей спан не
        // назван: «привязана к этому спану» неверно для обеих.
        if let Some(want) = &q.filter.spans
            && !span.is_some_and(|s| want.contains(&s))
        {
            return None;
        }

        Some(Entry {
            namespace: ns,
            channel,
            at: raw.at,
            utc: self.epochs.to_utc(raw.at),
            span,
            kind,
        })
    }
}

/// Собрать предикат отбора, применяемый до материализации записи.
///
/// Уровни и тэги — статические свойства типов, поэтому запрос вроде
/// «только ошибки» решается по схеме, без чтения payload'а; отброшенная
/// запись не стоит ни аллокации, ни копирования.
fn build_prefilter(q: &Query, schema: Option<Schema>) -> crate::cursor::Prefilter {
    let kinds = q.filter.kinds;
    let min_level = q.filter.min_level;
    let events = q.filter.events.clone();
    let event_names = q.filter.event_names.clone();
    let any_tags = q.filter.any_tags.clone();

    std::sync::Arc::new(move |record: &dduroc_format::Record<'_>| match record {
        dduroc_format::Record::Message(m) => {
            if !kinds.messages {
                return false;
            }
            if let Some(want) = &events
                && !want.contains(&m.event)
            {
                return false;
            }
            let desc = schema.and_then(|s| s.event(m.event));
            if let Some(min) = min_level {
                match desc.map(|d| d.level) {
                    Some(l) if l >= min => {}
                    // Уровень неизвестен — запись не прячем: это событие
                    // типа, удалённого из схемы, и скрыть его от того, кто
                    // ищет проблему, нельзя.
                    None => {}
                    _ => return false,
                }
            }
            if !any_tags.is_empty() {
                let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                if !any_tags.iter().any(|want| tags.iter().any(|t| t == want)) {
                    return false;
                }
            }
            if !event_names.is_empty() {
                let name = desc.map(|d| d.name).unwrap_or("");
                if !event_names.iter().any(|n| n == name) {
                    return false;
                }
            }
            true
        }
        dduroc_format::Record::Text(t) => kinds.text && min_level.is_none_or(|min| t.level >= min),
        dduroc_format::Record::SpanStart(_) | dduroc_format::Record::SpanEnd { .. } => kinds.spans,
        dduroc_format::Record::Sample(_) => kinds.samples,
        dduroc_format::Record::Ext { .. } => true,
    })
}

/// Метрики-состояния одной схемы. Пусто, если схема этому билду неизвестна.
fn state_metrics(schema: Option<&Schema>) -> HashSet<MetricId> {
    schema.map_or_else(HashSet::new, |s| {
        s.metrics
            .iter()
            .filter(|m| m.kind == MetricKind::State)
            .map(|m| m.id)
            .collect()
    })
}

fn read_ns_meta(dir: &Path) -> Option<NsMeta> {
    let bytes = std::fs::read(dir.join(NS_META)).ok()?;
    postcard::from_bytes(&bytes).ok()
}

fn read_store_id(root: &Path) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct Meta {
        #[allow(dead_code)]
        container_version: u8,
        store_id: u64,
    }
    let bytes = std::fs::read(root.join("store-meta")).ok()?;
    postcard::from_bytes::<Meta>(&bytes)
        .ok()
        .map(|m| m.store_id)
}

/// Отрендерить сообщение по шаблону схемы.
///
/// Низкоуровневый путь: схему приходится искать самому. Обычно нужен
/// [`Reader::render`] — он берёт её по неймспейсу записи.
///
/// Шаблоны на диске не хранятся: `{поле}` подставляется декодером,
/// сгенерированным макросом схемы. Без декодера возвращается сам шаблон.
pub fn render(schema: &Schema, event: EventId, payload: &[u8], lang: &str) -> Option<String> {
    let desc = schema.event(event)?;
    let lang_index = schema.language_index(lang).unwrap_or(0);
    match desc.decoders {
        Some(d) => (d.render)(payload, lang_index).ok(),
        None => desc.template(lang_index).map(|t| t.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::KindFilter;
    use dduroc_engine::schema::{EventDesc, Language, MetricDesc, SpanDesc, StorageClass};
    use dduroc_engine::store::{Store, StoreConfig};
    use dduroc_format::{MetricId, Micros, ProtocolVersion, SpanKindId, ValueType};

    static LANGS: &[Language] = &[Language("en"), Language("ru")];
    static EVENTS: &[EventDesc] = &[
        EventDesc {
            id: EventId(1),
            name: "PowerSet",
            level: Level::Info,
            class: StorageClass::DEFAULT,
            tags: &["rf"],
            templates: &["power set", "мощность задана"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::CRITICAL,
            tags: &["fault"],
            templates: &["alarm", "авария"],
            fields: &[],
            decoders: None,
        },
    ];
    static LINK_STATES: &[dduroc_engine::schema::StateDesc] = &[
        dduroc_engine::schema::StateDesc {
            code: 0,
            name: "Los",
            severity: Severity::Alarm,
        },
        dduroc_engine::schema::StateDesc {
            code: 1,
            name: "Lock",
            severity: Severity::Normal,
        },
    ];
    static METRICS: &[MetricDesc] = &[
        MetricDesc {
            id: MetricId(1),
            name: "temp",
            value_type: ValueType::F32,
            class: StorageClass::DEFAULT,
            unit: "°C",
            tags: &["thermal"],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: dduroc_engine::schema::Thresholds {
                warn: dduroc_engine::schema::Range {
                    min: None,
                    max: Some(25.0),
                },
                alarm: dduroc_engine::schema::Range {
                    min: None,
                    max: Some(28.0),
                },
            },
        },
        MetricDesc {
            id: MetricId(2),
            name: "link",
            value_type: ValueType::U64,
            class: StorageClass::DEFAULT,
            unit: "",
            tags: &["rf"],
            kind: MetricKind::State,
            states: LINK_STATES,
            thresholds: dduroc_engine::schema::Thresholds::NONE,
        },
    ];
    static SPANS: &[SpanDesc] = &[SpanDesc {
        id: SpanKindId(1),
        name: "Calibration",
        class: StorageClass::DEFAULT,
    }];

    fn schema() -> Schema {
        Schema {
            name: "radio",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: METRICS,
            spans: SPANS,
            migrations: &[],
        }
    }

    /// Типизированные константы метрик — то, что порождает макрос схемы.
    const TEMP: dduroc_engine::metric::Metric<f32> =
        dduroc_engine::metric::Metric::new(MetricId(1));

    /// Наполнить хранилище и закрыть его.
    fn populate(root: &Path) {
        let store = Store::open(StoreConfig::new(root).with_budget(16 * 1024 * 1024)).unwrap();
        for inst in 0..2 {
            let ns = store
                .namespace(&format!("orc-radio-{inst}"), schema())
                .unwrap();
            for i in 0..20u8 {
                ns.log_raw(EventId(1), &[i], None);
            }
            ns.log_raw(EventId(2), &[0xFF], None);

            let temp = ns.series(TEMP).unwrap();
            for i in 0..10 {
                temp.sample(20.0 + i as f32);
            }
            {
                let cal = ns.span(SpanKindId(1));
                cal.log_raw(EventId(1), &[99]);
            }
            ns.sync().unwrap();
        }
        let ns = store.namespace("apt-modem-0", schema()).unwrap();
        ns.log_raw(EventId(1), &[1], None);
        ns.sync().unwrap();
        store.shutdown();
    }

    #[test]
    fn reads_back_everything_written() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let namespaces = reader.namespaces().unwrap();
        assert_eq!(namespaces.len(), 3, "три неймспейса");
        assert_eq!(namespaces[0].name, "apt-modem-0");
        assert_eq!(namespaces[0].schema_name, "radio");
        assert!(namespaces[0].bytes > 0);

        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(result.is_complete(), "повреждений быть не должно");
        assert!(!result.entries.is_empty());

        // Сообщения обеих экземпляров и модема на месте.
        let messages = result
            .entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::Message { .. }))
            .count();
        assert_eq!(
            messages,
            2 * 22 + 1,
            "20 + аварийное + внутри спана, ×2, +1"
        );
    }

    #[test]
    fn schema_resolves_names_levels_and_units() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let result = reader
            .query(&Query::new().order(Order::Oldest).limit(500))
            .unwrap();

        let msg = result
            .entries
            .iter()
            .find(|e| {
                matches!(
                    &e.kind,
                    EntryKind::Message {
                        name: Some("PowerSet"),
                        ..
                    }
                )
            })
            .expect("имя события восстановлено по схеме");
        assert_eq!(msg.level(), Some(Level::Info), "уровень взят из схемы");

        let sample = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Sample { .. }))
            .expect("сэмпл найден");
        match &sample.kind {
            EntryKind::Sample {
                metric,
                metric_name,
                unit,
                tags,
                kind,
                state,
                severity,
                value,
            } => {
                assert_eq!(*metric, MetricId(1), "идентификатор пришёл из записи");
                assert_eq!(*metric_name, Some("temp"));
                assert_eq!(*unit, Some("°C"));
                assert_eq!(tags, &["thermal"]);
                assert_eq!(*kind, Some(MetricKind::Gauge));
                assert_eq!(*state, None, "не перечисление — подписи нет");
                assert!(severity.is_some(), "важность посчитана по схеме");
                assert!(value.as_f64().unwrap() >= 20.0);
            }
            other => panic!("ожидался сэмпл: {other:?}"),
        }

        let span = result.entries.iter().find(|e| {
            matches!(
                &e.kind,
                EntryKind::SpanStart {
                    kind_name: Some("Calibration"),
                    ..
                }
            )
        });
        assert!(span.is_some(), "вид спана восстановлен");
    }

    #[test]
    fn filters_by_group_level_and_kind() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        // Только оркестраторы.
        let orc = reader
            .query(&Query::new().group("orc-").order(Order::Oldest))
            .unwrap();
        assert!(
            orc.entries.iter().all(|e| e.namespace.starts_with("orc-")),
            "группа отобрана по префиксу"
        );

        // Только ошибки: уровень — свойство типа, читать payload не нужно.
        let errors = reader
            .query(
                &Query::new()
                    .min_level(Level::Error)
                    .kinds(KindFilter::LOGS)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(errors.entries.len(), 2, "по одной аварии на экземпляр");
        assert!(
            errors
                .entries
                .iter()
                .all(|e| e.level() == Some(Level::Error))
        );

        // Только телеметрия.
        let telemetry = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(telemetry.entries.len(), 20, "10 сэмплов × 2 экземпляра");
        assert!(
            telemetry
                .entries
                .iter()
                .all(|e| matches!(e.kind, EntryKind::Sample { .. }))
        );
    }

    #[test]
    fn telemetry_keeps_identity_in_newest_order() {
        // Определение серии пишется в тело один раз, перед первым сэмплом.
        // При обратном обходе — режиме по умолчанию — сэмпл встречается
        // РАНЬШЕ своего определения, поэтому восстанавливать идентичность
        // из потока нельзя: вся телеметрия приходила бы обезличенной.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        for order in [Order::Oldest, Order::Newest] {
            let result = reader
                .query(&Query::new().kinds(KindFilter::TELEMETRY).order(order))
                .unwrap();
            assert_eq!(result.entries.len(), 20, "порядок {order:?}");
            for e in &result.entries {
                match &e.kind {
                    EntryKind::Sample {
                        metric_name,
                        unit,
                        tags,
                        ..
                    } => {
                        assert_eq!(*metric_name, Some("temp"), "порядок {order:?}");
                        assert_eq!(*unit, Some("°C"));
                        assert_eq!(tags, &["thermal"]);
                    }
                    other => panic!("ожидался сэмпл: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn sparse_state_series_gets_seeded_at_the_window_edge() {
        // Состояния пишут по изменению. Окно, внутри которого состояние не
        // менялось, не содержит ни одного отсчёта — и полоса на графике
        // оказалась бы пустой, хотя состояние было известно всё это время.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // Переход в Lock — единственный отсчёт состояния, рано.
        let link = ns.series_untyped(MetricId(2)).unwrap();
        link.sample_raw(dduroc_engine::staged::OwnedValue::U64(1));
        let after_state = ns.now();

        // Дальше идёт только температура: окно будет без состояний.
        let temp = ns.series(TEMP).unwrap();
        for i in 0..20 {
            temp.sample(20.0 + i as f32);
        }
        ns.sync().unwrap();
        store.shutdown();
        drop(store);

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let window = Query {
            from: Some(BootTime::new(after_state.boot, Micros(after_state.at.0 + 1)).into()),
            order: Order::Oldest,
            filter: crate::Filter {
                kinds: KindFilter::TELEMETRY,
                ..Default::default()
            },
            ..Query::new()
        };

        // Без затравки состояния в ответе нет вовсе.
        let plain = reader.query(&window).unwrap();
        assert!(plain.seeds.is_empty());
        assert!(
            !plain
                .entries
                .iter()
                .any(|e| matches!(&e.kind, EntryKind::Sample { state: Some(_), .. })),
            "в окне действительно нет ни одного состояния"
        );

        // С затравкой — приходит последний отсчёт до окна, отдельно.
        let seeded = reader
            .query(&Query {
                seed_states: true,
                ..window
            })
            .unwrap();
        assert_eq!(seeded.seeds.len(), 1, "по одному на ряд-состояние");
        let seed = &seeded.seeds[0];
        assert!(
            seed.at <= after_state,
            "затравка обязана лежать ДО окна: {} vs {}",
            seed.at,
            after_state
        );
        match &seed.kind {
            EntryKind::Sample {
                metric,
                state,
                kind,
                severity,
                ..
            } => {
                assert_eq!(*metric, MetricId(2));
                assert_eq!(*state, Some("Lock"), "подпись состояния из схемы");
                assert_eq!(*kind, Some(MetricKind::State));
                assert_eq!(*severity, Some(Severity::Normal));
            }
            other => panic!("ожидался сэмпл состояния: {other:?}"),
        }
        // Окно от затравки не изменилось.
        assert_eq!(seeded.entries.len(), plain.entries.len());
    }

    #[test]
    fn state_seeds_come_from_the_schema_of_their_own_namespace() {
        // Пространство `metric_id` своё у каждой схемы. Ряды-состояния,
        // собранные объединением по всем схемам, дали бы затравку из чужого
        // ряда: метрика номер 2 у «radio» — конечный автомат, у «modem» —
        // обычная непрерывная величина, и подписывать её состояниями нельзя.
        static OTHER_METRICS: &[MetricDesc] = &[MetricDesc {
            id: MetricId(2), // тот же номер, другая величина
            name: "voltage",
            value_type: ValueType::F32,
            class: StorageClass::DEFAULT,
            unit: "V",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: dduroc_engine::schema::Thresholds::NONE,
        }];
        fn other() -> Schema {
            Schema {
                name: "modem",
                metrics: OTHER_METRICS,
                ..schema()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let after;
        {
            let cfg = StoreConfig::new(dir.path()).with_budget(16 * 1024 * 1024);
            let store = Store::open(cfg).unwrap();

            let radio = store.namespace("orc-radio-0", schema()).unwrap();
            radio
                .series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::U64(1));

            let modem = store.namespace("apt-modem-0", other()).unwrap();
            modem
                .series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::F32(3.3));

            after = radio.now();
            // Дальше — только то, что окно и увидит.
            radio.series(TEMP).unwrap().sample(21.0);
            radio.sync().unwrap();
            modem.sync().unwrap();
            store.shutdown();
        }

        let reader = Reader::open(dir.path(), &[schema(), other()]).unwrap();
        let seeded = reader
            .query(&Query {
                from: Some(BootTime::new(after.boot, Micros(after.at.0 + 1)).into()),
                order: Order::Oldest,
                seed_states: true,
                filter: crate::Filter {
                    kinds: KindFilter::TELEMETRY,
                    ..Default::default()
                },
                ..Query::new()
            })
            .unwrap();

        assert_eq!(
            seeded.seeds.len(),
            1,
            "затравка только у настоящего ряда-состояния: {:?}",
            seeded.seeds
        );
        let seed = &seeded.seeds[0];
        assert_eq!(&*seed.namespace, "orc-radio-0");
        match &seed.kind {
            EntryKind::Sample { state, kind, .. } => {
                assert_eq!(*state, Some("Lock"));
                assert_eq!(*kind, Some(MetricKind::State));
            }
            other => panic!("ожидалось состояние: {other:?}"),
        }
    }

    #[test]
    fn state_seed_is_absent_when_there_is_nothing_before_the_window() {
        // Запрос от начала времён: до окна ничего нет, и выдумывать затравку
        // неоткуда.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let r = reader
            .query(&Query {
                from: Some(BootTime::from_raw(0, 0).into()),
                seed_states: true,
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(r.seeds.is_empty());
        assert!(r.is_complete());
    }

    #[test]
    fn telemetry_identity_survives_an_unsealed_segment() {
        // Живое хранилище: сегмент ещё пишется, footer'а нет, а с ним нет и
        // таблицы серий. Идентичность приходится собирать проходом по телам —
        // тем же, которым находятся смещения блоков.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-radio-0", schema()).unwrap();
        let temp = ns.series(TEMP).unwrap();
        for i in 0..10 {
            temp.sample(20.0 + i as f32);
        }
        ns.sync().unwrap(); // данные на диске, но сегмент не запечатан

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        for order in [Order::Oldest, Order::Newest] {
            let result = reader
                .query(&Query::new().kinds(KindFilter::TELEMETRY).order(order))
                .unwrap();
            assert_eq!(result.entries.len(), 10, "порядок {order:?}");
            assert!(
                result.entries.iter().all(|e| matches!(
                    &e.kind,
                    EntryKind::Sample {
                        metric_name: Some("temp"),
                        unit: Some("°C"),
                        ..
                    }
                )),
                "порядок {order:?}: серия обезличена в незапечатанном сегменте"
            );
        }
        store.shutdown();
    }

    #[test]
    fn telemetry_identity_survives_time_range_seek() {
        // Запрос с нижней границей пропускает начальные блоки по
        // footer-индексу — вместе с лежащими там определениями серий.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let all = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid.into()),
                order: Order::Oldest,
                filter: crate::Filter {
                    kinds: KindFilter::TELEMETRY,
                    ..Default::default()
                },
                ..Query::new()
            })
            .unwrap();
        assert!(!narrowed.entries.is_empty());
        assert!(
            narrowed.entries.iter().all(|e| matches!(
                &e.kind,
                EntryKind::Sample {
                    metric_name: Some("temp"),
                    ..
                }
            )),
            "идентичность серии обязана пережить перескок по времени"
        );
    }

    #[test]
    fn newest_order_and_limit() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let newest = reader
            .query(&Query::new().order(Order::Newest).limit(5))
            .unwrap();
        assert_eq!(newest.entries.len(), 5);
        assert!(newest.truncated, "ответ обрезан по лимиту");

        // Время не возрастает.
        let times: Vec<BootTime> = newest.entries.iter().map(|e| e.at).collect();
        assert!(
            times.windows(2).all(|w| w[0] >= w[1]),
            "порядок от нового к старому: {times:?}"
        );

        let oldest = reader
            .query(&Query::new().order(Order::Oldest).limit(5))
            .unwrap();
        let times: Vec<BootTime> = oldest.entries.iter().map(|e| e.at).collect();
        assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");
    }

    #[test]
    fn truncation_is_announced_only_when_something_was_left_out() {
        // Отметка ставилась при входе в выдачу, а не тогда, когда запись
        // действительно не влезла: ответ ровно в `limit` записей объявлялся
        // обрезанным, даже если больше их и не было. Для веб-слоя это вечная
        // кнопка «дальше», ведущая в пустоту.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let total = all.entries.len();
        assert!(!all.truncated, "запрос без лимита не бывает обрезан");

        // Лимит ровно по числу записей: обрезать было нечего.
        let exact = reader
            .query(&Query::new().order(Order::Oldest).limit(total))
            .unwrap();
        assert_eq!(exact.entries.len(), total);
        assert!(
            !exact.truncated,
            "записей ровно {total}, больше нет — ответ полон"
        );

        // На единицу меньше — обрезан по-настоящему.
        let cut = reader
            .query(&Query::new().order(Order::Oldest).limit(total - 1))
            .unwrap();
        assert_eq!(cut.entries.len(), total - 1);
        assert!(cut.truncated, "одна запись осталась за бортом");

        // Пустой ответ при нулевом лимите: записи есть, места нет.
        let none = reader
            .query(&Query::new().order(Order::Oldest).limit(0))
            .unwrap();
        assert!(none.entries.is_empty());
        assert!(none.truncated);
    }

    #[test]
    fn render_resolves_the_schema_without_touching_the_disk() {
        // Резолв схемы читал `ns-meta` на КАЖДЫЙ вызов: отрисовка пятисот
        // строк стоила пятисот открытий файла — ровно тот дефект, который
        // уже убран с пути запроса. Проверяется тем, что рендер продолжает
        // работать, когда файла на диске больше нет.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        assert_eq!(reader.render(msg, "ru").as_deref(), Some("мощность задана"));

        std::fs::remove_file(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        assert_eq!(
            reader.render(msg, "ru").as_deref(),
            Some("мощность задана"),
            "схема обязана быть разрешена один раз, а не на каждый вызов"
        );
    }

    #[test]
    fn merge_is_ordered_across_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let keys: Vec<BootTime> = all.entries.iter().map(|e| e.at).collect();
        assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "слияние обязано давать глобальный порядок по времени"
        );
        // В ответе присутствуют оба неймспейса вперемешку.
        let namespaces: std::collections::HashSet<_> =
            all.entries.iter().map(|e| &*e.namespace).collect();
        assert!(namespaces.len() >= 2);
    }

    #[test]
    fn unknown_schema_leaves_raw_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        // Читаем без схем — как чужой билд.
        let reader = Reader::open(dir.path(), &[]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();

        assert!(!result.entries.is_empty(), "записи всё равно читаются");
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        match &msg.kind {
            EntryKind::Message { name, level, .. } => {
                assert!(name.is_none(), "имя без схемы неизвестно");
                assert!(level.is_none(), "уровень без схемы неизвестен");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn time_range_narrows_result() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid.into()),
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(narrowed.entries.iter().all(|e| e.at >= mid));
        assert!(narrowed.entries.len() < all.entries.len());
    }

    #[test]
    fn foreign_store_segments_are_reported_not_silently_dropped() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        // Подменяем store-meta: сегменты станут «чужими».
        let meta_path = dir.path().join("store-meta");
        #[derive(serde::Serialize)]
        struct Meta {
            container_version: u8,
            store_id: u64,
        }
        std::fs::write(
            &meta_path,
            postcard::to_allocvec(&Meta {
                container_version: 1,
                store_id: 0xDEAD_BEEF,
            })
            .unwrap(),
        )
        .unwrap();

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "чужие сегменты обязаны попасть в список повреждений, а не исчезнуть"
        );
        assert!(result.entries.is_empty());

        // Явное разрешение читает их как есть.
        let reader = Reader::open(dir.path(), &[schema()])
            .unwrap()
            .allow_foreign_segments();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!result.entries.is_empty());
    }

    #[test]
    fn corrupt_block_does_not_hide_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        // Портим середину одного сегмента.
        let ch = dir.path().join("orc-radio-0").join("default");
        let seg = std::fs::read_dir(&ch)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all_at(&[0xFF; 16], 40).unwrap();
        }

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "о повреждении обязано быть сказано явно"
        );
        // Данные других неймспейсов не пострадали.
        assert!(
            result
                .entries
                .iter()
                .any(|e| &*e.namespace == "orc-radio-1" || &*e.namespace == "apt-modem-0"),
            "порча одного сегмента не должна прятать остальные"
        );
    }

    #[test]
    fn utc_is_resolved_when_anchor_exists() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget(8 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store
                .record_sync(
                    DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                    dduroc_engine::SyncSource::Gps,
                )
                .unwrap();
            store.shutdown();
        }

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let entry = &result.entries[0];
        let utc = entry
            .utc
            .expect("якорь ретроактивен: событие записано ДО синхронизации");
        assert!(
            (1_699_999_000_000..1_700_001_000_000).contains(&utc.timestamp_millis()),
            "UTC рядом с точкой синхронизации: {utc}"
        );
    }

    #[test]
    fn wall_clock_window_selects_the_same_records_as_the_relative_one() {
        // Синхронизированный прибор: одно и то же окно, заданное настенными
        // часами и относительным временем, обязано дать одну выборку.
        // Иначе граница «с 12:00» врала бы ровно на ошибку конверсии.
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget(8 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            store
                .record_sync(
                    DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                    dduroc_engine::SyncSource::Gps,
                )
                .unwrap();
            for i in 0..40u8 {
                ns.log_raw(EventId(1), &[i], None);
            }
            ns.sync().unwrap();
            store.shutdown();
        }

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert_eq!(all.entries.len(), 40);
        let mid = &all.entries[all.entries.len() / 2];
        let mid_utc = mid.utc.expect("якорь есть");

        let by_wall = reader
            .query(&Query::new().order(Order::Oldest).since(mid_utc))
            .unwrap();
        let by_relative = reader
            .query(&Query::new().order(Order::Oldest).since(mid.at))
            .unwrap();

        assert!(by_wall.unanchored.is_empty(), "запуск синхронизирован");
        assert_eq!(
            by_wall.entries, by_relative.entries,
            "настенное и относительное окно обязаны дать одну выборку: \
             конверсия по якорю точна до микросекунды"
        );
        assert!(by_wall.entries.iter().all(|e| e.at >= mid.at));
        assert!(by_wall.entries.len() < all.entries.len());

        // Верхняя граница — симметрично.
        let until_wall = reader
            .query(&Query::new().order(Order::Oldest).until(mid_utc))
            .unwrap();
        let until_relative = reader
            .query(&Query::new().order(Order::Oldest).until(mid.at))
            .unwrap();
        assert_eq!(until_wall.entries, until_relative.entries);
        assert!(until_wall.entries.iter().all(|e| e.at <= mid.at));
        // Вместе половины покрывают всё; пересечение — записи ровно на
        // границе, а их может быть больше одной: часы монотонны, но не строго
        // возрастают, и во всплеске два события получают одну микросекунду.
        let on_edge = all.entries.iter().filter(|e| e.at == mid.at).count();
        assert_eq!(
            until_wall.entries.len() + by_wall.entries.len(),
            all.entries.len() + on_edge
        );
    }

    #[test]
    fn wall_clock_window_reports_runs_it_had_to_drop() {
        // Прибор без синхронизации. Запрос по настенным часам не может
        // сказать, попадают ли его записи в окно, — и обязан сообщить, что
        // выборка неполна, а не показать пустоту.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let utc = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).since(utc))
            .unwrap();

        assert!(r.entries.is_empty(), "сопоставить нечем");
        assert_eq!(
            r.unanchored,
            vec![BootCounter(0)],
            "выпавший запуск обязан быть назван: молчание выглядело бы как \
             «прибор ничего не писал»"
        );

        // Относительное окно работает и без синхронизации — оно ни от чего
        // не зависит.
        let r = reader
            .query(
                &Query::new()
                    .order(Order::Oldest)
                    .since(BootTime::from_raw(0, 0)),
            )
            .unwrap();
        assert!(!r.entries.is_empty());
        assert!(r.unanchored.is_empty());
    }

    #[test]
    fn stream_reads_without_collecting_everything() {
        // Ради этого поток и существует: ответ на запрос без `limit` по
        // большому хранилищу в память не поместился бы, а прервать обход
        // посередине `query` не позволяет.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let q = Query::new().order(Order::Oldest);

        // Первые пять записей — и обход прекращён, остальные сегменты даже не
        // дочитаны.
        let head: Vec<Entry> = reader.stream(&q).unwrap().take(5).collect();
        assert_eq!(head.len(), 5);

        // Поток и собранный ответ обязаны совпадать запись в запись: иначе
        // «то же самое, но лениво» было бы неправдой.
        let whole: Vec<Entry> = reader.stream(&q).unwrap().collect();
        let collected = reader.query(&q).unwrap();
        assert_eq!(whole, collected.entries);
        assert_eq!(&whole[..5], &head[..]);

        // Лимит и признак обрезки работают и в потоке.
        let mut limited = reader.stream(&q.clone().limit(3)).unwrap();
        assert_eq!(limited.by_ref().count(), 3);
        assert!(limited.truncated());
        assert_eq!(limited.yielded(), 3);
    }

    #[test]
    fn render_finds_the_schema_by_the_entry_itself() {
        // Схему больше не надо искать вызывающему: передать чужую значило бы
        // разобрать payload декодером другого события — текст вышел бы
        // правдоподобным и неверным.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();

        let msg = r
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        assert_eq!(reader.render(msg, "ru").as_deref(), Some("мощность задана"));
        assert_eq!(reader.render(msg, "en").as_deref(), Some("power set"));
        // Неизвестный язык — первый объявленный, а не отказ.
        assert_eq!(reader.render(msg, "de").as_deref(), Some("power set"));

        // Читателю без схем рендерить нечем, и выдумывать он не станет.
        let blind = Reader::open(dir.path(), &[]).unwrap();
        assert_eq!(blind.render(msg, "ru"), None);
    }

    #[test]
    fn dump_without_epochs_file_still_names_its_runs() {
        // Дамп скопировали без `epochs.bin` — так бывает, когда забирают
        // только каталог. Записи на месте, но настенного времени у них нет ни
        // у одной, а перечислить выпавшие запуски по эпохам невозможно:
        // единственный источник — имена сегментов.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        std::fs::remove_file(dir.path().join(dduroc_engine::epochs::EPOCHS_FILE)).unwrap();

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let utc = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).since(utc))
            .unwrap();

        assert!(r.entries.is_empty());
        assert_eq!(
            r.unanchored,
            vec![BootCounter(0)],
            "без реестра эпох запуск известен только по имени сегмента"
        );

        // Без настенных границ дамп читается как обычно: относительное время
        // лежит в самих файлах.
        let r = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!r.entries.is_empty());
        assert!(r.entries.iter().all(|e| e.utc.is_none()));
        assert!(r.unanchored.is_empty());
    }
}
