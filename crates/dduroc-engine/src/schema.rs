//! Описание схемы неймспейса.
//!
//! Схема — **compile-time** сущность: она принадлежит коду микросервиса и
//! описывает, какие события, метрики и спаны он умеет писать. На диск из неё
//! не попадает ничего, кроме номера версии в заголовке сегмента: уровни,
//! шаблоны текста, тэги и имена резолвятся при чтении по идентификаторам.
//!
//! Дескрипторы статические (`&'static`), поэтому схема ничего не стоит в
//! рантайме и целиком лежит в `.rodata` прошивки.
//!
//! Идентификаторы задаются **явно**. Позиционная авто-нумерация прототипа
//! молча перемапливала исторические записи на чужие декодеры при вставке
//! события в середину списка; здесь такой ошибки допустить нельзя, а
//! переименования и перенумерация делаются миграциями.

use dduroc_format::{EventId, Level, MetricId, ProtocolVersion, SpanKindId, ValueType};

/// Класс хранения: канал, в который попадают записи этого типа.
///
/// Канал определяет политику долговечности и бюджет. Значимые данные
/// объявляют [`StorageClass::Critical`], остальные — [`StorageClass::Default`].
///
/// Перечисление, а не имя строкой: множество классов закрыто (его знает и
/// макрос схемы, и настройка хранилища), и класс, которого не существует,
/// не должен быть представим — со строкой опечатка в конфигурации молча
/// заводила бы канал-сироту со своей политикой. Имя каталога канала — это
/// [`StorageClass::as_str`], производная от класса, а не второй источник.
///
/// Закрытое и в смысле `#[non_exhaustive]` тоже: новый класс — не добавление
/// варианта, а согласованная правка ключевого слова `store:` у макроса,
/// [`StorageClass::ALL`], квот и раскладки каталогов. Такое ломает сборку в
/// любом случае, и заставлять ради него каждый разбор класса заводить ветку
/// `_` значило бы платить за то, что всё равно не пройдёт молча.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StorageClass {
    /// Батчи с отложенной синхронизацией.
    Default,
    /// Синхронизация сразу (group commit) — устойчивость к потере питания.
    Critical,
    /// Отдельный канал под телеметрию: обычно самый большой бюджет.
    Telemetry,
}

impl StorageClass {
    /// Все классы — для настройки и проверки каждого.
    pub const ALL: [StorageClass; 3] = [Self::Default, Self::Critical, Self::Telemetry];

    /// Имя каталога канала.
    pub const fn as_str(self) -> &'static str {
        match self {
            StorageClass::Default => "default",
            StorageClass::Critical => "critical",
            StorageClass::Telemetry => "telemetry",
        }
    }

    /// Позиция в [`StorageClass::ALL`] — стабильный индекс для таблиц
    /// по классам (группы бюджетов, квоты).
    pub const fn index(self) -> usize {
        match self {
            StorageClass::Default => 0,
            StorageClass::Critical => 1,
            StorageClass::Telemetry => 2,
        }
    }

    /// Класс по имени его каталога — обратное к [`StorageClass::as_str`].
    ///
    /// `None` — каталог не является каналом ни одного класса этой сборки:
    /// либо чужая директория, либо дамп из будущей версии с новым классом.
    /// Читатель обязан различить это и сказать, а не разбирать неизвестное.
    pub fn from_dir_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.as_str() == name)
    }
}

impl std::fmt::Display for StorageClass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Код языка шаблонов (`"en"`, `"ru"`, `"ja"`…).
///
/// Набор языков задаёт приложение: `en`/`ru` — частный случай одного проекта,
/// другому нужен `en`+`ja`+`zh`. Библиотека к набору агностична.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Language(pub &'static str);

impl Language {
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

/// Поле payload события — для показа структурированных данных при чтении.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FieldDesc {
    pub name: &'static str,
    /// Имя типа как в исходнике (`"f32"`, `"String"`) — для UI.
    pub type_name: &'static str,
}

/// Ошибка декодирования payload'а.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("payload не соответствует схеме события")]
pub struct DecodeError;

/// Функции декодирования, сгенерированные макросом схемы.
///
/// Сигнатуры намеренно свободны от `serde_json` и прочих типов: движок не
/// должен тянуть зависимости слоя представления, а генерируемый код вправе
/// использовать что угодно.
#[derive(Clone, Copy)]
pub struct EventDecoders {
    /// Отрендерить сообщение на языке с индексом из [`Schema::languages`].
    pub render: fn(&[u8], usize) -> Result<String, DecodeError>,
    /// Поля payload'а как JSON-объект.
    pub json: fn(&[u8]) -> Result<String, DecodeError>,
}

impl std::fmt::Debug for EventDecoders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("EventDecoders { .. }")
    }
}

/// Тип сообщения.
#[derive(Debug, Clone, Copy)]
pub struct EventDesc {
    pub id: EventId,
    pub name: &'static str,
    /// Уровень — статическое свойство типа, на диск не пишется.
    pub level: Level,
    pub class: StorageClass,
    /// Статические тэги-категории. Живут в схеме, места на диске не занимают,
    /// поэтому фильтрация по ним бесплатна: она сводится к выбору множества
    /// идентификаторов ещё до сканирования.
    pub tags: &'static [&'static str],
    /// Шаблоны — по одному на каждый язык из [`Schema::languages`], в том же
    /// порядке.
    pub templates: &'static [&'static str],
    pub fields: &'static [FieldDesc],
    pub decoders: Option<EventDecoders>,
}

impl EventDesc {
    pub fn template(&self, lang_index: usize) -> Option<&'static str> {
        self.templates.get(lang_index).copied()
    }
}

/// Насколько значение метрики требует внимания.
///
/// Вычисляется при чтении по пределам, на диск не пишется: пределы —
/// настраиваемое свойство установки, а не свойство измерения. Одна и та же
/// температура нормальна для одного усилителя и аварийна для другого.
///
/// Слово `critical` здесь намеренно **не** используется: оно занято классом
/// хранения ([`StorageClass::Critical`] — устойчивость к потере питания). Это
/// разные оси: класс хранения говорит, *как записать*, важность — *что
/// значение означает*, а в объявлении метрики они стоят рядом. Пара
/// `warn` → `alarm` привычна в промышленной телеметрии и не путается.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub enum Severity {
    #[default]
    Normal,
    /// Величина вышла из нормы.
    Warn,
    /// Величина вышла за допустимое — авария.
    Alarm,
}

impl Severity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Severity::Normal => "normal",
            Severity::Warn => "warn",
            Severity::Alarm => "alarm",
        }
    }

    pub const fn is_normal(self) -> bool {
        matches!(self, Severity::Normal)
    }
}

/// Как величина ведёт себя между отсчётами — подсказка тому, кто её рисует.
///
/// Соединять точки прямой можно только у непрерывной величины. Состояние
/// между отсчётами **не меняется**: линия через промежуточные значения
/// показала бы состояния, которых не было. Разница не косметическая, поэтому
/// вид объявляется в схеме, а не угадывается по типу значения.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Hash)]
pub enum MetricKind {
    /// Непрерывная величина: температура, мощность. Интерполируется.
    #[default]
    Gauge,
    /// Дискретное состояние: держится ступенькой до следующего отсчёта.
    State,
    /// Монотонно растущий счётчик: осмысленна производная, не значение.
    Counter,
}

/// Одно состояние метрики-перечисления.
///
/// Код пишется на диск как обычное целое, имя и важность остаются в схеме —
/// ровно как уровень и шаблон у сообщения. Коды задаются **явно**: позиционная
/// нумерация сдвинулась бы при вставке состояния в середину списка, и старые
/// сегменты стали бы читаться неверно без единого признака ошибки.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateDesc {
    pub code: u64,
    pub name: &'static str,
    /// Важность самого факта нахождения в этом состоянии.
    pub severity: Severity,
}

/// Диапазон допустимых значений; `None` — граница не задана.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Range {
    pub min: Option<f64>,
    pub max: Option<f64>,
}

impl Range {
    pub const NONE: Self = Self {
        min: None,
        max: None,
    };

    pub const fn is_unset(&self) -> bool {
        self.min.is_none() && self.max.is_none()
    }

    /// Лежит ли значение внутри (границы включительно).
    ///
    /// NaN не внутри никакого диапазона: сравнения с ним ложны, и это
    /// правильный ответ — неизвестное значение не «нормально».
    pub fn contains(&self, v: f64) -> bool {
        if v.is_nan() {
            return false;
        }
        self.min.is_none_or(|m| v >= m) && self.max.is_none_or(|m| v <= m)
    }
}

impl<R: std::ops::RangeBounds<f64>> From<R> for Range {
    /// Диапазон из обычного range-выражения: `..=70.0`, `1.0..=1.5`, `10.0..`.
    ///
    /// Границы диапазона метрики включительные — за ними уже не норма, — а
    /// `Excluded` у `f64` не выразить без произвольного эпсилона, поэтому
    /// исключающая граница трактуется как включающая. В схеме макрос требует
    /// `..=` явно; здесь этого не потребовать, зато можно не соврать молча.
    fn from(r: R) -> Self {
        use std::ops::Bound;
        let bound = |b: Bound<&f64>| match b {
            Bound::Unbounded => None,
            Bound::Included(v) | Bound::Excluded(v) => Some(*v),
        };
        Self {
            min: bound(r.start_bound()),
            max: bound(r.end_bound()),
        }
    }
}

/// Пределы числовой метрики: диапазоны, **вне** которых значение требует
/// внимания.
///
/// `alarm` обязан включать `warn`: сначала величина выходит из нормы, потом
/// из допустимого. Обратное означало бы, что значение аварийно, не будучи
/// тревожным. Проверяется в [`Schema::validate`].
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Thresholds {
    /// Вне этого диапазона — [`Severity::Warn`].
    pub warn: Range,
    /// Вне этого — [`Severity::Alarm`].
    pub alarm: Range,
}

impl Thresholds {
    pub const NONE: Self = Self {
        warn: Range::NONE,
        alarm: Range::NONE,
    };

    /// Пределы из двух range-выражений — так же, как в объявлении схемы:
    ///
    /// ```text
    /// Thresholds::new(..=70.0, ..=85.0)        // только сверху
    /// Thresholds::new(1.0..=1.5, 1.0..=2.0)    // с двух сторон
    /// ```
    pub fn new(
        warn: impl std::ops::RangeBounds<f64>,
        alarm: impl std::ops::RangeBounds<f64>,
    ) -> Self {
        Self {
            warn: warn.into(),
            alarm: alarm.into(),
        }
    }

    pub const fn is_unset(&self) -> bool {
        self.warn.is_unset() && self.alarm.is_unset()
    }

    /// Важность числового значения. Более тяжёлый диагноз побеждает.
    pub fn severity_of(&self, v: f64) -> Severity {
        if !self.alarm.is_unset() && !self.alarm.contains(v) {
            return Severity::Alarm;
        }
        if !self.warn.is_unset() && !self.warn.contains(v) {
            return Severity::Warn;
        }
        Severity::Normal
    }
}

/// Тип метрики телеметрии.
#[derive(Debug, Clone, Copy)]
pub struct MetricDesc {
    pub id: MetricId,
    pub name: &'static str,
    pub value_type: ValueType,
    pub class: StorageClass,
    /// Единица измерения для UI (`"°C"`, `"dBm"`).
    pub unit: &'static str,
    /// Статические тэги-категории — как у [`EventDesc::tags`]. Живут в схеме,
    /// места на диске не занимают.
    ///
    /// Рантайм-размерностей у метрики нет: идентичность ряда равна метрике.
    /// Четыре датчика температуры — это четыре метрики схемы, а не одна с
    /// тэгом, иначе тэг пришлось бы писать в каждый отсчёт.
    pub tags: &'static [&'static str],
    /// Как величина ведёт себя между отсчётами.
    pub kind: MetricKind,
    /// Подписи кодов состояний. Пусто — метрика не перечисление.
    pub states: &'static [StateDesc],
    /// Пределы по умолчанию. Установка вправе переопределить их в рантайме
    /// (см. `Namespace::set_limits`) — например узнав модель железа.
    pub thresholds: Thresholds,
    /// Предикат срабатывания тревоги: истина — значение тревожно.
    ///
    /// Для форм, которые диапазоном нормы не выразить. Полярность обратна
    /// [`MetricDesc::thresholds`] — поэтому в схеме это отдельные ключи
    /// `warn_if:`/`alarm_if:`. С диапазонами предикат складывается по правилу
    /// «тяжелейший диагноз побеждает»; интроспекции у него нет, полосы на
    /// графике рисуются только по диапазонам.
    pub warn_if: Option<fn(f64) -> bool>,
    /// Предикат срабатывания аварии — см. [`MetricDesc::warn_if`].
    pub alarm_if: Option<fn(f64) -> bool>,
}

impl MetricDesc {
    /// Подпись состояния по коду. `None` — код не объявлен либо метрика не
    /// перечисление.
    pub fn state(&self, code: u64) -> Option<&'static StateDesc> {
        self.states.iter().find(|s| s.code == code)
    }

    /// Важность значения по пределам **из схемы**.
    ///
    /// Для перечислений и `bool` берётся важность состояния: их значения не
    /// упорядочены, и «выше порога» к ним неприменимо. Незнакомый код — не
    /// повод для тревоги сам по себе, но и не норма: у него нет подписи, и
    /// решать, что он значит, читателю нечем.
    pub fn severity_of(&self, value: &dduroc_format::Value<'_>) -> Severity {
        use dduroc_format::Value;
        if !self.states.is_empty() {
            let code = match value {
                Value::U64(v) => Some(*v),
                Value::I64(v) if *v >= 0 => Some(*v as u64),
                Value::Bool(b) => Some(u64::from(*b)),
                _ => None,
            };
            return code
                .and_then(|c| self.state(c))
                .map_or(Severity::Normal, |s| s.severity);
        }
        value.as_f64().map_or(Severity::Normal, |v| {
            self.numeric_severity(&self.thresholds, v)
        })
    }

    /// Важность числа по диапазонам и предикатам; диапазоны могут быть
    /// рантайм-переопределением, предикаты всегда схемные.
    pub(crate) fn numeric_severity(&self, thresholds: &Thresholds, v: f64) -> Severity {
        let by_predicate = if self.alarm_if.is_some_and(|hit| hit(v)) {
            Severity::Alarm
        } else if self.warn_if.is_some_and(|hit| hit(v)) {
            Severity::Warn
        } else {
            Severity::Normal
        };
        thresholds.severity_of(v).max(by_predicate)
    }
}

/// Вид спана.
#[derive(Debug, Clone, Copy)]
pub struct SpanDesc {
    pub id: SpanKindId,
    pub name: &'static str,
    pub class: StorageClass,
}

/// Шаг миграции `from → from + 1`.
#[derive(Clone, Copy)]
pub struct Migration {
    /// Версия, из которой мигрируем.
    pub from: u16,
    /// Шаг затрагивает любой сегмент, что бы в нём ни лежало.
    ///
    /// `true` — затронутые типы не объявлены, и решать по ним нельзя.
    /// Это **безопасное умолчание**: переписать лишний сегмент дорого ровно
    /// на один цикл записи флеша, а пропустить нужный — значит навсегда
    /// оставить его в прежней раскладке, притом молча.
    pub touches_all: bool,
    /// Типы, затронутые шагом. Сегменты, не содержащие ни одного из них,
    /// переписывать не нужно — прямая экономия ресурса флеша.
    pub events: &'static [EventId],
    pub metrics: &'static [MetricId],
    /// Виды спанов, затронутые шагом.
    ///
    /// В отличие от событий и метрик, экономии на сегментах они не дают:
    /// множества видов спанов в footer'е нет, и ответить «в этом сегменте
    /// таких спанов не бывает» не по чему. Шаг со спанами переписывает
    /// сегмент всегда — см. [`Migration::touches`].
    pub spans: &'static [SpanKindId],
    /// Преобразование одной записи. `Ok(None)` — запись удаляется.
    pub migrate: fn(MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError>,
}

impl Migration {
    /// Нужно ли переписывать сегмент с таким footer'ом.
    ///
    /// Единственное место, где решается этот вопрос: множества типов в
    /// footer'е отвечают на него без чтения блоков, а `touches_all` —
    /// без чтения вовсе.
    pub fn touches(&self, footer: &dduroc_format::Footer) -> bool {
        // Шаг, трогающий виды спанов, переписывает сегмент безусловно.
        // Footer знает множества событий и метрик, но не спанов: спросить
        // «есть ли здесь такой спан» не у кого, а решить «нет» наугад значило
        // бы навсегда оставить эти записи в прежней раскладке — притом молча,
        // потому что прогон отчитается об успехе и заштампует мету. Лишняя
        // перезапись стоит одного цикла флеша, пропуск — необратим.
        self.touches_all || !self.spans.is_empty() || footer.touches(self.events, self.metrics)
    }
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migration")
            .field("from", &self.from)
            .field("touches_all", &self.touches_all)
            .field("events", &self.events)
            .field("metrics", &self.metrics)
            .field("spans", &self.spans)
            .finish_non_exhaustive()
    }
}

/// Вход шага миграции: запись **в прежней раскладке**, как она лежит на диске.
///
/// Парна [`MigrationOutcome`] — тому, что шаг возвращает. Разведены именами
/// намеренно: причастие («мигрированная запись») переворачивало бы стороны
/// сигнатуры, а шаг получает именно то, что ещё не приведено.
#[derive(Debug, Clone, Copy)]
pub struct MigrationInput<'a> {
    pub record: dduroc_format::Record<'a>,
}

impl MigrationInput<'_> {
    /// Тип сообщения. `None` — запись не сообщение.
    pub fn event_id(&self) -> Option<EventId> {
        match &self.record {
            dduroc_format::Record::Message(m) => Some(m.event),
            _ => None,
        }
    }

    /// Метрика отсчёта. `None` — запись не отсчёт.
    pub fn metric_id(&self) -> Option<MetricId> {
        match &self.record {
            dduroc_format::Record::Sample(s) => Some(s.metric),
            _ => None,
        }
    }

    /// Вид спана. `None` — запись не начало спана.
    ///
    /// У конца спана вида нет: он ссылается на уже начатый по номеру, и
    /// переименование вида его не касается.
    pub fn span_kind(&self) -> Option<SpanKindId> {
        match &self.record {
            dduroc_format::Record::SpanStart(s) => Some(s.kind),
            _ => None,
        }
    }

    /// Значение отсчёта. `None` — запись не отсчёт.
    ///
    /// Значение самоописуемо: тип лежит в самой записи, а не берётся из
    /// схемы. Поэтому шаг видит то, что писали, — даже если с тех пор в схеме
    /// у метрики объявлен другой тип.
    pub fn value(&self) -> Option<dduroc_format::Value<'_>> {
        match &self.record {
            dduroc_format::Record::Sample(s) => Some(s.value),
            _ => None,
        }
    }

    /// Сырой payload сообщения. `None` — запись не сообщение.
    ///
    /// Нужен ремапу id: тип меняет номер, байты полей едут как есть.
    pub fn payload(&self) -> Option<&[u8]> {
        match &self.record {
            dduroc_format::Record::Message(m) => Some(m.payload),
            _ => None,
        }
    }

    /// Разобрать payload сообщения в старую раскладку.
    ///
    /// `T` — структура с полями той версии, из которой мигрирует шаг (при
    /// объявленной `history` её генерирует макрос). Ошибка и для записи,
    /// которая не сообщение: у остальных видов payload'а в этом смысле нет.
    pub fn decode<T: serde::de::DeserializeOwned>(&self) -> Result<T, DecodeError> {
        match &self.record {
            dduroc_format::Record::Message(m) => {
                postcard::from_bytes(m.payload).map_err(|_| DecodeError)
            }
            _ => Err(DecodeError),
        }
    }
}

/// Исход шага миграции — то, чем запись станет. Владеющая форма: шаг обычно
/// перекодирует payload.
///
/// Парен [`MigrationInput`]. Прежнее имя `OwnedRecord` сталкивалось с
/// одноимённым типом читателя (владеющая копия wire-записи) — при импорте
/// обоих крейтов они были неразличимы.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum MigrationOutcome {
    /// Оставить как есть.
    AsIs,
    /// Заменить тип и/или payload сообщения.
    Message { event: EventId, payload: Vec<u8> },
    /// Заменить метрику сэмпла, не трогая значение.
    ///
    /// Остаётся отдельным исходом ради значения: ремап не копирует его вовсе,
    /// а мегабайтный спектр стоил бы копии на каждой записи.
    SampleMetric(MetricId),
    /// Заменить значение отсчёта (и, если нужно, метрику).
    ///
    /// Значение владеющее: у исхода шага нет времени жизни, связывающего его
    /// с входной записью, — заимствовать байты блока ему не из чего.
    Sample {
        metric: MetricId,
        value: crate::staged::OwnedValue,
    },
    /// Переименовать вид спана.
    ///
    /// Только вид: номер спана и его родитель — личность записи, на которую
    /// ссылаются её сообщения и дочерние спаны, и переписать эти ссылки
    /// цепочке нечем.
    SpanKind(SpanKindId),
}

/// Схема неймспейса.
#[derive(Debug, Clone, Copy)]
pub struct Schema {
    /// Идентичность схемы. Неймспейс запоминает её и отказывается открываться
    /// чужой схемой: одинаковые id событий в разных схемах означают разное.
    pub name: &'static str,
    pub version: ProtocolVersion,
    pub languages: &'static [Language],
    pub events: &'static [EventDesc],
    pub metrics: &'static [MetricDesc],
    pub spans: &'static [SpanDesc],
    pub migrations: &'static [Migration],
}

/// Вид дескриптора схемы — чьё пространство идентификаторов имеется в виду.
///
/// Перечисление, а не строка: у события, метрики и спана id-пространства
/// раздельные, и диагностика, называющая вид строкой, позволяла бы назвать
/// несуществующий.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescriptorKind {
    Event,
    Metric,
    Span,
}

impl DescriptorKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            DescriptorKind::Event => "event",
            DescriptorKind::Metric => "metric",
            DescriptorKind::Span => "span",
        }
    }
}

impl std::fmt::Display for DescriptorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Ошибка валидации схемы: ловится при подъёме неймспейса, то есть на старте
/// процесса, а не через месяц работы на нечитаемых логах.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("схема {schema:?}: {kind} id {id} объявлен дважды ({first:?} и {second:?})")]
    DuplicateId {
        schema: &'static str,
        kind: DescriptorKind,
        id: u16,
        first: &'static str,
        second: &'static str,
    },

    #[error("схема {schema:?}: событие {event:?} имеет {got} шаблонов, а языков объявлено {want}")]
    TemplateCount {
        schema: &'static str,
        event: &'static str,
        got: usize,
        want: usize,
    },

    #[error("схема {schema:?}: языки не объявлены — рендерить сообщения будет нечем")]
    NoLanguages { schema: &'static str },

    #[error("схема {schema:?}: язык {lang:?} объявлен дважды")]
    DuplicateLanguage {
        schema: &'static str,
        lang: &'static str,
    },

    #[error("схема {schema:?}: версия 0 недопустима — нумерация версий начинается с 1")]
    ZeroVersion { schema: &'static str },

    #[error(
        "схема {schema:?}: нет шага миграции с версии {from} (цепочка обязана быть \
         непрерывной до текущей версии {version})"
    )]
    MigrationGap {
        schema: &'static str,
        from: u16,
        version: u16,
    },

    #[error("схема {schema:?}: шаг миграции с версии {from} объявлен дважды")]
    DuplicateMigration { schema: &'static str, from: u16 },

    #[error("схема {schema:?}: имя пустое")]
    EmptyName { schema: &'static str },

    #[error(
        "схема {schema:?}: {kind} {name:?} нарушает порядок идентификаторов — \
         поиск дескриптора идёт бинарно и требует возрастания"
    )]
    Unsorted {
        schema: &'static str,
        kind: DescriptorKind,
        name: &'static str,
    },

    #[error("схема {schema:?}: метрика {metric:?} объявляет код состояния {code} дважды")]
    DuplicateStateCode {
        schema: &'static str,
        metric: &'static str,
        code: u64,
    },

    #[error("схема {schema:?}: метрика {metric:?}: {reason}")]
    BadMetric {
        schema: &'static str,
        metric: &'static str,
        reason: &'static str,
    },
}

impl Schema {
    /// Проверить внутреннюю согласованность.
    pub fn validate(&self) -> Result<(), SchemaError> {
        if self.name.is_empty() {
            return Err(SchemaError::EmptyName { schema: self.name });
        }
        if self.version.0 == 0 {
            return Err(SchemaError::ZeroVersion { schema: self.name });
        }
        if self.languages.is_empty() {
            return Err(SchemaError::NoLanguages { schema: self.name });
        }
        for (i, a) in self.languages.iter().enumerate() {
            if self.languages[..i].contains(a) {
                return Err(SchemaError::DuplicateLanguage {
                    schema: self.name,
                    lang: a.0,
                });
            }
        }

        check_unique(
            self.name,
            DescriptorKind::Event,
            self.events.iter().map(|e| (e.id.0, e.name)),
        )?;
        check_unique(
            self.name,
            DescriptorKind::Metric,
            self.metrics.iter().map(|m| (m.id.0, m.name)),
        )?;
        check_unique(
            self.name,
            DescriptorKind::Span,
            self.spans.iter().map(|s| (s.id.0, s.name)),
        )?;

        // Дескрипторы обязаны идти по возрастанию идентификаторов: поиск по
        // ним бинарный и выполняется на каждую запись. Макрос `schema!`
        // раскладывает их сам, но схему можно объявить и вручную.
        check_sorted(
            self.name,
            DescriptorKind::Event,
            self.events.iter().map(|e| (e.id.0, e.name)),
        )?;
        check_sorted(
            self.name,
            DescriptorKind::Metric,
            self.metrics.iter().map(|m| (m.id.0, m.name)),
        )?;
        check_sorted(
            self.name,
            DescriptorKind::Span,
            self.spans.iter().map(|s| (s.id.0, s.name)),
        )?;

        for e in self.events {
            if e.templates.len() != self.languages.len() {
                return Err(SchemaError::TemplateCount {
                    schema: self.name,
                    event: e.name,
                    got: e.templates.len(),
                    want: self.languages.len(),
                });
            }
        }

        for m in self.metrics {
            self.check_metric(m)?;
        }

        // Цепочка миграций обязана быть непрерывной: 1→2→…→version. Пропуск
        // означает, что данные старой версии молча остались бы неверно
        // истолкованными.
        for (i, m) in self.migrations.iter().enumerate() {
            if self.migrations[..i].iter().any(|p| p.from == m.from) {
                return Err(SchemaError::DuplicateMigration {
                    schema: self.name,
                    from: m.from,
                });
            }
        }
        for from in 1..self.version.0 {
            if !self.migrations.iter().any(|m| m.from == from) {
                return Err(SchemaError::MigrationGap {
                    schema: self.name,
                    from,
                    version: self.version.0,
                });
            }
        }
        Ok(())
    }

    /// Проверить осмысленность метрики.
    ///
    /// Бессмысленную метрику нельзя объявить: ошибка ловится на старте
    /// процесса, а не через месяц работы на графике, который никто не может
    /// прочесть.
    fn check_metric(&self, m: &MetricDesc) -> Result<(), SchemaError> {
        let bad = |reason| SchemaError::BadMetric {
            schema: self.name,
            metric: m.name,
            reason,
        };

        // Перечисление и вид State — одно и то же свойство, объявленное
        // дважды. Расхождение означает, что одно из двух написано по ошибке,
        // а какое — знает только автор схемы.
        match (m.states.is_empty(), m.kind) {
            (true, MetricKind::State) => {
                return Err(bad(
                    "вид State без объявленных состояний: подписывать нечем",
                ));
            }
            (false, k) if k != MetricKind::State => {
                return Err(bad(
                    "состояния объявлены, но вид не State — график нарисовали бы \
                     прямой через значения, которых не было",
                ));
            }
            _ => {}
        }

        if !m.states.is_empty() {
            // Код состояния — целое на диске, поэтому дробный или блобовый
            // тип значения к перечислению неприменим.
            if !matches!(
                m.value_type,
                ValueType::U64 | ValueType::I64 | ValueType::Bool
            ) {
                return Err(bad(
                    "состояния допустимы только у целочисленной или булевой \
                     метрики: код состояния хранится целым",
                ));
            }
            if !m.thresholds.is_unset() {
                return Err(bad(
                    "числовые пределы у перечисления: его значения не упорядочены, \
                     важность задаётся на состояние",
                ));
            }
            if m.warn_if.is_some() || m.alarm_if.is_some() {
                return Err(bad(
                    "предикаты у перечисления: его значения не упорядочены, \
                     важность задаётся на состояние",
                ));
            }
            for (i, s) in m.states.iter().enumerate() {
                if m.states[..i].iter().any(|p| p.code == s.code) {
                    return Err(SchemaError::DuplicateStateCode {
                        schema: self.name,
                        metric: m.name,
                        code: s.code,
                    });
                }
                if s.name.is_empty() {
                    return Err(bad("состояние без имени"));
                }
                if m.value_type == ValueType::Bool && s.code > 1 {
                    return Err(bad("у булевой метрики допустимы только коды 0 и 1"));
                }
            }
        }

        for (r, what) in [
            (m.thresholds.warn, "тревожный"),
            (m.thresholds.alarm, "аварийный"),
        ] {
            if matches!((r.min, r.max), (Some(lo), Some(hi)) if lo > hi) {
                let _ = what;
                return Err(bad("диапазон задан вывернутым: min больше max"));
            }
            if r.min.is_some_and(f64::is_nan) || r.max.is_some_and(f64::is_nan) {
                return Err(bad("граница диапазона — NaN: сравнения с ним всегда ложны"));
            }
        }
        crate::limits::check_nesting(m.name, &m.thresholds).map_err(|_| {
            bad(
                "критический диапазон обязан включать тревожный: иначе значение \
                 оказалось бы критическим, не будучи тревожным",
            )
        })?;

        if m.value_type == ValueType::Blob && (m.warn_if.is_some() || m.alarm_if.is_some()) {
            return Err(bad(
                "предикат у blob-метрики: значение не приводится к числу, и \
                 проверять ему нечего",
            ));
        }

        Ok(())
    }

    /// Найти тип сообщения по идентификатору.
    ///
    /// Вызывается на **каждую** записываемую и читаемую запись, поэтому
    /// поиск бинарный: линейный обход двухсот с лишним дескрипторов на
    /// событие заметен уже на x86 и тем более на armv7. Упорядоченность
    /// проверяется в [`Schema::validate`], а не предполагается.
    pub fn event(&self, id: EventId) -> Option<&'static EventDesc> {
        match self.events.binary_search_by_key(&id.0, |e| e.id.0) {
            Ok(i) => Some(&self.events[i]),
            // Схема не отсортирована (не прошла validate) — честно ищем.
            Err(_) => self.events.iter().find(|e| e.id == id),
        }
    }

    pub fn metric(&self, id: MetricId) -> Option<&'static MetricDesc> {
        self.metric_index(id).map(|(_, d)| d)
    }

    /// Метрика вместе с её позицией в [`Schema::metrics`].
    ///
    /// Позиция — устойчивый ключ для того, что хранится параллельно схеме и
    /// не пишется на диск: рантайм-пределов (см. [`crate::limits`]).
    pub fn metric_index(&self, id: MetricId) -> Option<(usize, &'static MetricDesc)> {
        match self.metrics.binary_search_by_key(&id.0, |m| m.id.0) {
            Ok(i) => Some((i, &self.metrics[i])),
            // Схема не отсортирована (не прошла validate) — честно ищем.
            Err(_) => self
                .metrics
                .iter()
                .position(|m| m.id == id)
                .map(|i| (i, &self.metrics[i])),
        }
    }

    pub fn span(&self, id: SpanKindId) -> Option<&'static SpanDesc> {
        match self.spans.binary_search_by_key(&id.0, |s| s.id.0) {
            Ok(i) => Some(&self.spans[i]),
            Err(_) => self.spans.iter().find(|s| s.id == id),
        }
    }

    pub fn language_index(&self, code: &str) -> Option<usize> {
        self.languages.iter().position(|l| l.0 == code)
    }

    /// Все каналы, которые может использовать схема.
    pub fn classes(&self) -> Vec<StorageClass> {
        // Обычный канал есть всегда, даже если его не объявил ни один тип.
        // Свободному тексту своего класса взять неоткуда — у него нет типа в
        // схеме, — а писать его надо: это мост из чужих логов, обработчик
        // паники и однократное объявление дефекта сборки. Схема из одной
        // телеметрии оставляла бы всё это без канала, и объявление, ради
        // которого механизм заведён, молча отказывало бы.
        //
        // Пустой канал не стоит почти ничего: каталог создаётся при подъёме,
        // а сегмент — только первой записью.
        let mut out: Vec<StorageClass> = vec![StorageClass::Default];
        let all = self
            .events
            .iter()
            .map(|e| e.class)
            .chain(self.metrics.iter().map(|m| m.class))
            .chain(self.spans.iter().map(|s| s.class));
        for c in all {
            if !out.contains(&c) {
                out.push(c);
            }
        }
        out.sort_unstable();
        out
    }

    /// Шаг миграции с версии `from`.
    pub fn migration(&self, from: u16) -> Option<&'static Migration> {
        self.migrations.iter().find(|m| m.from == from)
    }
}

/// Проверить возрастание идентификаторов: по ним идёт бинарный поиск.
fn check_sorted(
    schema: &'static str,
    kind: DescriptorKind,
    items: impl Iterator<Item = (u16, &'static str)>,
) -> Result<(), SchemaError> {
    let mut prev: Option<u16> = None;
    for (id, name) in items {
        if prev.is_some_and(|p| id <= p) {
            return Err(SchemaError::Unsorted { schema, kind, name });
        }
        prev = Some(id);
    }
    Ok(())
}

fn check_unique(
    schema: &'static str,
    kind: DescriptorKind,
    items: impl Iterator<Item = (u16, &'static str)>,
) -> Result<(), SchemaError> {
    let mut seen: Vec<(u16, &'static str)> = Vec::new();
    for (id, name) in items {
        if let Some((_, first)) = seen.iter().find(|(i, _)| *i == id) {
            return Err(SchemaError::DuplicateId {
                schema,
                kind,
                id,
                first,
                second: name,
            });
        }
        seen.push((id, name));
    }
    Ok(())
}

/// Конструкторы схем для тестов соседних модулей.
///
/// Не `mod tests`: тем нужен готовый экземпляр, а не проверки этого модуля.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;

    /// Минимальная валидная схема с шагами миграции.
    pub(crate) fn minimal_schema_with_migrations(
        version: u16,
        migrations: &'static [Migration],
    ) -> Schema {
        static LANGS: &[Language] = &[Language("en")];
        Schema {
            name: "probe",
            version: ProtocolVersion(version),
            languages: LANGS,
            events: &[],
            metrics: &[],
            spans: &[],
            migrations,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LANGS: &[Language] = &[Language("en"), Language("ru")];

    fn schema(events: &'static [EventDesc], version: u16) -> Schema {
        Schema {
            name: "radio",
            version: ProtocolVersion(version),
            languages: LANGS,
            events,
            metrics: &[],
            spans: &[],
            migrations: &[],
        }
    }

    #[test]
    fn valid_schema_passes() {
        static EVENTS: &[EventDesc] = &[
            EventDesc {
                id: EventId(1),
                name: "PowerSet",
                level: Level::Info,
                class: StorageClass::Critical,
                tags: &["rf"],
                templates: &["power {dbm}", "мощность {dbm}"],
                fields: &[],
                decoders: None,
            },
            EventDesc {
                id: EventId(2),
                name: "Failed",
                level: Level::Error,
                class: StorageClass::Default,
                tags: &[],
                templates: &["failed", "сбой"],
                fields: &[],
                decoders: None,
            },
        ];
        let s = schema(EVENTS, 1);
        s.validate().expect("схема корректна");
        assert_eq!(s.event(EventId(1)).unwrap().name, "PowerSet");
        assert!(s.event(EventId(99)).is_none());
        assert_eq!(s.language_index("ru"), Some(1));
        assert_eq!(s.language_index("ja"), None);
        // Порядок — как в объявлении перечисления: он определяет индексы
        // каналов в рантайме, на диске каналы живут под именами.
        assert_eq!(
            s.classes(),
            vec![StorageClass::Default, StorageClass::Critical]
        );
        assert_eq!(
            s.event(EventId(1)).unwrap().template(1),
            Some("мощность {dbm}")
        );
    }

    #[test]
    fn the_default_channel_exists_even_if_no_type_asks_for_it() {
        // Схема из одной телеметрии не объявляет обычного канала, а
        // свободному тексту деться больше некуда: у него нет типа в схеме, а
        // значит и класса хранения. Без безусловного `default` объявление
        // дефекта сборки, сообщение моста и обработчик паники молча
        // отказывали бы — то есть механизм, заведённый против тишины, сам бы
        // и молчал.
        static METRICS: &[MetricDesc] = &[MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "Spectrum",
            unit: "",
            tags: &[],
            value_type: ValueType::Blob,
            kind: MetricKind::Gauge,
            class: StorageClass::Telemetry,
            states: &[],
            thresholds: Thresholds::NONE,
        }];
        let s = Schema {
            name: "telemetry-only",
            version: ProtocolVersion(1),
            languages: &[Language("en")],
            events: &[],
            metrics: METRICS,
            spans: &[],
            migrations: &[],
        };
        s.validate().expect("схема корректна");
        assert_eq!(
            s.classes(),
            vec![StorageClass::Default, StorageClass::Telemetry],
            "обычный канал есть всегда"
        );
    }

    #[test]
    fn duplicate_ids_rejected() {
        static EVENTS: &[EventDesc] = &[
            EventDesc {
                id: EventId(5),
                name: "A",
                level: Level::Info,
                class: StorageClass::Default,
                tags: &[],
                templates: &["a", "а"],
                fields: &[],
                decoders: None,
            },
            EventDesc {
                id: EventId(5),
                name: "B",
                level: Level::Info,
                class: StorageClass::Default,
                tags: &[],
                templates: &["b", "б"],
                fields: &[],
                decoders: None,
            },
        ];
        let err = schema(EVENTS, 1).validate().unwrap_err();
        assert!(
            matches!(
                err,
                SchemaError::DuplicateId {
                    id: 5,
                    first: "A",
                    second: "B",
                    ..
                }
            ),
            "получено {err}"
        );
    }

    #[test]
    fn template_count_must_match_languages() {
        static EVENTS: &[EventDesc] = &[EventDesc {
            id: EventId(1),
            name: "Only",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &[],
            templates: &["english only"],
            fields: &[],
            decoders: None,
        }];
        let err = schema(EVENTS, 1).validate().unwrap_err();
        assert!(
            matches!(
                err,
                SchemaError::TemplateCount {
                    got: 1,
                    want: 2,
                    ..
                }
            ),
            "получено {err}"
        );
    }

    #[test]
    fn migration_chain_must_be_continuous() {
        static EVENTS: &[EventDesc] = &[];
        fn noop(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::AsIs))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: true,
            events: &[],
            metrics: &[],
            spans: &[],
            migrate: noop,
        }];

        // Версия 3, а шаг есть только 1→2: разрыв 2→3.
        let s = Schema {
            migrations: STEPS,
            ..schema(EVENTS, 3)
        };
        let err = s.validate().unwrap_err();
        assert!(
            matches!(
                err,
                SchemaError::MigrationGap {
                    from: 2,
                    version: 3,
                    ..
                }
            ),
            "получено {err}"
        );

        // Полная цепочка проходит.
        static FULL: &[Migration] = &[
            Migration {
                from: 1,
                touches_all: true,
                events: &[],
                metrics: &[],
                spans: &[],
                migrate: noop,
            },
            Migration {
                from: 2,
                touches_all: true,
                events: &[],
                metrics: &[],
                spans: &[],
                migrate: noop,
            },
        ];
        Schema {
            migrations: FULL,
            ..schema(EVENTS, 3)
        }
        .validate()
        .expect("непрерывная цепочка");
    }

    /// Схема с одной метрикой — для проверок осмысленности метрик.
    fn with_metric(metrics: &'static [MetricDesc]) -> Schema {
        static EVENTS: &[EventDesc] = &[];
        Schema {
            metrics,
            ..schema(EVENTS, 1)
        }
    }

    const fn metric(
        value_type: ValueType,
        kind: MetricKind,
        states: &'static [StateDesc],
        thresholds: Thresholds,
    ) -> MetricDesc {
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "m",
            value_type,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind,
            states,
            thresholds,
        }
    }

    const fn range(min: Option<f64>, max: Option<f64>) -> Range {
        Range { min, max }
    }

    #[test]
    fn valid_state_metric_passes() {
        static STATES: &[StateDesc] = &[
            StateDesc {
                code: 0,
                name: "Los",
                severity: Severity::Alarm,
            },
            StateDesc {
                code: 2,
                name: "Lock",
                severity: Severity::Normal,
            },
        ];
        static M: &[MetricDesc] = &[metric(
            ValueType::U64,
            MetricKind::State,
            STATES,
            Thresholds::NONE,
        )];
        with_metric(M).validate().expect("перечисление корректно");

        let desc = with_metric(M).metric(MetricId(1)).unwrap();
        assert_eq!(desc.state(0).unwrap().name, "Los");
        assert_eq!(desc.state(2).unwrap().severity, Severity::Normal);
        assert!(desc.state(1).is_none(), "код 1 не объявлен — и подписи нет");
        // Пропуски в нумерации законны: коды явные, а не позиционные.
        assert_eq!(
            desc.severity_of(&dduroc_format::Value::U64(2)),
            Severity::Normal
        );
    }

    #[test]
    fn state_and_kind_must_agree() {
        static STATES: &[StateDesc] = &[StateDesc {
            code: 0,
            name: "A",
            severity: Severity::Normal,
        }];

        // Вид State без состояний: подписывать нечем.
        static NO_STATES: &[MetricDesc] = &[metric(
            ValueType::U64,
            MetricKind::State,
            &[],
            Thresholds::NONE,
        )];
        assert!(matches!(
            with_metric(NO_STATES).validate(),
            Err(SchemaError::BadMetric { .. })
        ));

        // Состояния при виде Gauge: график нарисовали бы прямой через
        // значения, которых не было.
        static WRONG_KIND: &[MetricDesc] = &[metric(
            ValueType::U64,
            MetricKind::Gauge,
            STATES,
            Thresholds::NONE,
        )];
        assert!(matches!(
            with_metric(WRONG_KIND).validate(),
            Err(SchemaError::BadMetric { .. })
        ));
    }

    #[test]
    fn states_require_an_integral_value_type() {
        static STATES: &[StateDesc] = &[StateDesc {
            code: 0,
            name: "A",
            severity: Severity::Normal,
        }];
        for vt in [ValueType::F32, ValueType::F64, ValueType::Blob] {
            let leaked: &'static [MetricDesc] = Box::leak(Box::new([metric(
                vt,
                MetricKind::State,
                STATES,
                Thresholds::NONE,
            )]));
            assert!(
                matches!(
                    with_metric(leaked).validate(),
                    Err(SchemaError::BadMetric { .. })
                ),
                "тип {vt:?} не может нести код состояния"
            );
        }
    }

    #[test]
    fn duplicate_and_nameless_states_rejected() {
        static DUP: &[StateDesc] = &[
            StateDesc {
                code: 5,
                name: "A",
                severity: Severity::Normal,
            },
            StateDesc {
                code: 5,
                name: "B",
                severity: Severity::Warn,
            },
        ];
        static M: &[MetricDesc] = &[metric(
            ValueType::U64,
            MetricKind::State,
            DUP,
            Thresholds::NONE,
        )];
        assert!(matches!(
            with_metric(M).validate(),
            Err(SchemaError::DuplicateStateCode { code: 5, .. })
        ));

        static NAMELESS: &[StateDesc] = &[StateDesc {
            code: 0,
            name: "",
            severity: Severity::Normal,
        }];
        static N: &[MetricDesc] = &[metric(
            ValueType::U64,
            MetricKind::State,
            NAMELESS,
            Thresholds::NONE,
        )];
        assert!(matches!(
            with_metric(N).validate(),
            Err(SchemaError::BadMetric { .. })
        ));
    }

    #[test]
    fn bool_states_are_limited_to_zero_and_one() {
        static BAD: &[StateDesc] = &[StateDesc {
            code: 7,
            name: "Weird",
            severity: Severity::Normal,
        }];
        static M: &[MetricDesc] = &[metric(
            ValueType::Bool,
            MetricKind::State,
            BAD,
            Thresholds::NONE,
        )];
        assert!(matches!(
            with_metric(M).validate(),
            Err(SchemaError::BadMetric { .. })
        ));

        static OK: &[StateDesc] = &[
            StateDesc {
                code: 0,
                name: "Unlocked",
                severity: Severity::Alarm,
            },
            StateDesc {
                code: 1,
                name: "Locked",
                severity: Severity::Normal,
            },
        ];
        static G: &[MetricDesc] = &[metric(
            ValueType::Bool,
            MetricKind::State,
            OK,
            Thresholds::NONE,
        )];
        with_metric(G).validate().expect("bool как два состояния");
        // Важность булева значения берётся из состояния.
        let d = with_metric(G).metric(MetricId(1)).unwrap();
        assert_eq!(
            d.severity_of(&dduroc_format::Value::Bool(false)),
            Severity::Alarm
        );
        assert_eq!(
            d.severity_of(&dduroc_format::Value::Bool(true)),
            Severity::Normal
        );
    }

    #[test]
    fn thresholds_must_be_sane() {
        // Вывернутый диапазон.
        static INVERTED: &[MetricDesc] = &[metric(
            ValueType::F32,
            MetricKind::Gauge,
            &[],
            Thresholds {
                warn: range(Some(10.0), Some(1.0)),
                alarm: Range::NONE,
            },
        )];
        assert!(matches!(
            with_metric(INVERTED).validate(),
            Err(SchemaError::BadMetric { .. })
        ));

        // Критический не включает тревожный.
        static NOT_NESTED: &[MetricDesc] = &[metric(
            ValueType::F32,
            MetricKind::Gauge,
            &[],
            Thresholds {
                warn: range(None, Some(80.0)),
                alarm: range(None, Some(50.0)),
            },
        )];
        assert!(matches!(
            with_metric(NOT_NESTED).validate(),
            Err(SchemaError::BadMetric { .. })
        ));

        // NaN как граница: сравнения с ним всегда ложны, значит предел
        // молча не работал бы.
        static NAN: &[MetricDesc] = &[metric(
            ValueType::F32,
            MetricKind::Gauge,
            &[],
            Thresholds {
                warn: range(None, Some(f64::NAN)),
                alarm: Range::NONE,
            },
        )];
        assert!(matches!(
            with_metric(NAN).validate(),
            Err(SchemaError::BadMetric { .. })
        ));

        // Пределы у перечисления.
        static STATES: &[StateDesc] = &[StateDesc {
            code: 0,
            name: "A",
            severity: Severity::Normal,
        }];
        static BOTH: &[MetricDesc] = &[metric(
            ValueType::U64,
            MetricKind::State,
            STATES,
            Thresholds {
                warn: range(Some(0.0), Some(1.0)),
                alarm: Range::NONE,
            },
        )];
        assert!(matches!(
            with_metric(BOTH).validate(),
            Err(SchemaError::BadMetric { .. })
        ));

        // Односторонние границы и полностью открытый диапазон законны.
        static OPEN: &[MetricDesc] = &[metric(
            ValueType::F32,
            MetricKind::Gauge,
            &[],
            Thresholds {
                warn: range(None, Some(70.0)),
                alarm: range(Some(-273.15), None),
            },
        )];
        with_metric(OPEN).validate().expect("открытые границы");
    }

    #[test]
    fn metric_index_is_stable_key_for_runtime_state() {
        // Позиция метрики — ключ для того, что хранится параллельно схеме и
        // на диск не идёт (пределы). Она обязана совпадать с порядком в
        // массиве, иначе пределы применились бы к чужой метрике.
        static M: &[MetricDesc] = &[
            MetricDesc {
                warn_if: None,
                alarm_if: None,
                id: MetricId(1),
                ..metric(ValueType::F32, MetricKind::Gauge, &[], Thresholds::NONE)
            },
            MetricDesc {
                warn_if: None,
                alarm_if: None,
                id: MetricId(9),
                name: "second",
                ..metric(ValueType::U64, MetricKind::Counter, &[], Thresholds::NONE)
            },
        ];
        let s = with_metric(M);
        s.validate().unwrap();
        assert_eq!(s.metric_index(MetricId(1)).unwrap().0, 0);
        assert_eq!(s.metric_index(MetricId(9)).unwrap().0, 1);
        assert_eq!(s.metric_index(MetricId(9)).unwrap().1.name, "second");
        assert!(s.metric_index(MetricId(5)).is_none());
    }

    #[test]
    fn degenerate_schemas_rejected() {
        static EVENTS: &[EventDesc] = &[];
        assert!(matches!(
            schema(EVENTS, 0).validate(),
            Err(SchemaError::ZeroVersion { .. })
        ));

        static NO_LANGS: &[Language] = &[];
        let s = Schema {
            languages: NO_LANGS,
            ..schema(EVENTS, 1)
        };
        assert!(matches!(s.validate(), Err(SchemaError::NoLanguages { .. })));

        static DUP_LANGS: &[Language] = &[Language("en"), Language("en")];
        let s = Schema {
            languages: DUP_LANGS,
            ..schema(EVENTS, 1)
        };
        assert!(matches!(
            s.validate(),
            Err(SchemaError::DuplicateLanguage { lang: "en", .. })
        ));
    }
}
