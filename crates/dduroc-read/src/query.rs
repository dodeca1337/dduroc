//! Запрос: что читать и в каком порядке.
//!
//! # Время в границах запроса
//!
//! Границы задаются одним типом — [`Timestamp`], — и он либо относительный
//! ([`BootTime`]: запуск плюс микросекунды от его старта), либо настенный
//! (`DateTime<Utc>`). Раньше здесь лежали `boot: Option<u32>` и
//! `from/to: Option<Micros>` по отдельности, и смысл пары зависел от того,
//! задан ли `boot`: без него микросекунды прикладывались к шкале **каждого**
//! запуска, то есть «первые десять секунд любого запуска» и «десять секунд
//! конкретного запуска» выражались одинаково.
//!
//! Настенные границы сравнимы с записями только через якорь синхронизации
//! (см. [`dduroc_engine::epochs`]), поэтому перед сканированием они
//! переводятся в шкалу каждого запуска — [`Query::resolve`]. Запуск без якоря
//! сопоставить с настенным временем нечем; он выпадает из выборки, и об этом
//! сообщается явно ([`Resolution::unanchored`]), а не молча.

use dduroc_engine::epochs::{Epochs, RunOffset};
use dduroc_engine::schema::StorageClass;
use dduroc_format::{BootCounter, BootTime, EventId, Level, Micros, SpanId};
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// Момент времени в границах запроса.
///
/// Две шкалы, а не два поля: у прибора без RTC есть относительное время
/// всегда, настенное — только после синхронизации, и подменять одно другим
/// нельзя.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp {
    /// Относительное время: запуск и микросекунды от его старта.
    Boot(BootTime),
    /// Настенное время. Записи запуска, чья загрузка не синхронизирована,
    /// сопоставить с ним нечем — они выпадут из выборки.
    Utc(chrono::DateTime<chrono::Utc>),
}

impl Timestamp {
    /// Относительная ли шкала.
    pub const fn is_relative(self) -> bool {
        matches!(self, Timestamp::Boot(_))
    }

    /// Момент на микросекунду раньше — граница «строго до».
    pub fn just_before(self) -> Self {
        match self {
            Timestamp::Boot(bt) => {
                Timestamp::Boot(BootTime::new(bt.boot, Micros(bt.at.0.saturating_sub(1))))
            }
            Timestamp::Utc(t) => Timestamp::Utc(t - chrono::TimeDelta::microseconds(1)),
        }
    }
}

impl From<BootTime> for Timestamp {
    fn from(at: BootTime) -> Self {
        Timestamp::Boot(at)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Timestamp {
    fn from(t: chrono::DateTime<chrono::Utc>) -> Self {
        Timestamp::Utc(t)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Timestamp::Boot(at) => write!(f, "{at}"),
            Timestamp::Utc(t) => write!(f, "{}", t.to_rfc3339()),
        }
    }
}

/// Границы окна в относительной шкале одного запуска. Обе включительные,
/// `None` — ограничения нет.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunBounds {
    pub from: Option<Micros>,
    pub to: Option<Micros>,
}

impl RunBounds {
    pub fn contains(&self, at: Micros) -> bool {
        self.from.is_none_or(|f| at >= f) && self.to.is_none_or(|t| at <= t)
    }
}

/// Границы запроса, приведённые к тому, что можно сравнивать с записями.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Bounds {
    /// Ограничений по времени нет.
    #[default]
    All,
    /// Относительные границы. Сравниваются лексикографически по (запуск, µs) —
    /// это и есть хронологический порядок, потому что счётчик запусков растёт.
    Relative {
        from: Option<BootTime>,
        to: Option<BootTime>,
    },
    /// Настенные границы, переведённые по якорям в шкалу каждого запуска.
    Wall {
        /// Запуски, попавшие в окно, с границами в их собственной шкале.
        runs: BTreeMap<u32, RunBounds>,
        /// Запуски, у которых есть якорь. Нужно, чтобы отличить «этого
        /// запуска нет в окне» от «его нечем с окном сравнить»: второе — про
        /// данные, которые есть, но остались за кадром, и о них надо сказать.
        anchored: BTreeSet<u32>,
    },
}

/// Как окно ложится на записи одного запуска.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    /// Запуск нужен, вот его границы.
    In(RunBounds),
    /// Запуск целиком вне окна.
    Outside,
    /// Окно задано настенным временем, а якоря у запуска нет: попадают ли его
    /// записи в окно — неизвестно, и они выпадают из выборки.
    Unanchored,
}

impl Bounds {
    /// Как окно ложится на записи запуска.
    pub fn fit(&self, boot: BootCounter) -> Fit {
        match self {
            Bounds::All => Fit::In(RunBounds::default()),
            Bounds::Relative { from, to } => {
                // Границы соседних запусков превращаются в «весь этот запуск
                // внутри окна» либо в «его тут нет вовсе»: сравнивать
                // микросекунды разных запусков нельзя.
                let from = match from {
                    Some(b) if b.boot > boot => return Fit::Outside,
                    Some(b) if b.boot == boot => Some(b.at),
                    _ => None,
                };
                let to = match to {
                    Some(b) if b.boot < boot => return Fit::Outside,
                    Some(b) if b.boot == boot => Some(b.at),
                    _ => None,
                };
                Fit::In(RunBounds { from, to })
            }
            Bounds::Wall { runs, anchored } => match runs.get(&boot.0) {
                Some(b) => Fit::In(*b),
                None if anchored.contains(&boot.0) => Fit::Outside,
                // Сюда попадает и запуск, которого нет в `epochs.bin` вовсе:
                // дамп могли скопировать без него, и тогда настенного времени
                // у его записей нет — ровно как без синхронизации.
                None => Fit::Unanchored,
            },
        }
    }

    /// Что разрешено записям этого запуска. `None` — запуск не нужен целиком,
    /// его сегменты можно не открывать вовсе.
    pub fn for_boot(&self, boot: BootCounter) -> Option<RunBounds> {
        match self.fit(boot) {
            Fit::In(b) => Some(b),
            Fit::Outside | Fit::Unanchored => None,
        }
    }

    /// Попадает ли момент в окно.
    pub fn contains(&self, at: BootTime) -> bool {
        self.for_boot(at.boot).is_some_and(|b| b.contains(at.at))
    }
}

/// Разрешённые границы вместе с тем, что пришлось из-за них отбросить.
#[derive(Debug, Clone, Default)]
pub struct Resolution {
    pub bounds: Bounds,
    /// Запуски **из реестра эпох**, которые настенное окно сопоставить не
    /// может: якоря у них нет.
    ///
    /// Это ответ про реестр, а не про выборку: у запуска может не оказаться
    /// ни одного сегмента в запрошенных каналах. То, что действительно выпало
    /// из ответа, читатель собирает по сегментам — см.
    /// `QueryResult::unanchored`.
    pub unanchored: Vec<BootCounter>,
}

/// Порядок выдачи.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    /// От старого к новому.
    Oldest,
    /// От нового к старому — то, что нужно интерфейсу по умолчанию.
    #[default]
    Newest,
}

/// Выбор неймспейсов.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NsSelect {
    /// Все.
    #[default]
    All,
    /// По точным именам.
    Names(Vec<String>),
    /// Группа: неймспейсы с общим префиксом (`orc-` — оркестраторы,
    /// `apt-` — адаптеры).
    Group(String),
}

impl NsSelect {
    pub fn matches(&self, name: &str) -> bool {
        match self {
            NsSelect::All => true,
            NsSelect::Names(names) => names.iter().any(|n| n == name),
            // Правило одно на чтение и на запись: «журналы оркестраторов» и
            // «настройки оркестраторов» обязаны обозначать одно множество.
            NsSelect::Group(prefix) => dduroc_engine::store::in_group(prefix, name),
        }
    }
}

/// Фильтр по содержимому.
///
/// Уровни и тэги — статические свойства типов, поэтому фильтрация по ним
/// не требует чтения записей: она сводится к вычислению множества
/// идентификаторов по схеме **до** сканирования.
///
/// Контентный фильтр (тэги, типы, имена) применяется к записям, у которых
/// такое свойство есть, и **исключает** записи, которые не могут ему
/// удовлетворить: свободный текст и спаны не несут ни тэгов, ни типа события
/// и при таком фильтре выпадают. Тэги есть у сообщений и отсчётов (у метрик —
/// свои), типы — только у сообщений. `min_level` — иное: уровень есть у
/// сообщений и текста, а телеметрия и спаны вне шкалы уровней и по нему не
/// отсеиваются.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Минимальный уровень (включительно).
    pub min_level: Option<Level>,
    /// Требуемые тэги: запись должна нести хотя бы один из них.
    pub any_tags: Vec<String>,
    /// Конкретные типы событий.
    pub events: Option<HashSet<EventId>>,
    /// Имена событий — резолвятся по схеме.
    pub event_names: Vec<String>,
    /// Только записи, привязанные к этим спанам.
    pub spans: Option<HashSet<SpanId>>,
    /// Какие разновидности записей нужны.
    pub kinds: KindFilter,
}

/// Какие разновидности записей включать.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindFilter {
    pub messages: bool,
    pub spans: bool,
    pub samples: bool,
    pub text: bool,
}

impl Default for KindFilter {
    fn default() -> Self {
        Self {
            messages: true,
            spans: true,
            samples: true,
            text: true,
        }
    }
}

impl KindFilter {
    /// Только сообщения и свободный текст — «журнал» в привычном смысле.
    pub const LOGS: Self = Self {
        messages: true,
        spans: false,
        samples: false,
        text: true,
    };

    /// Только телеметрия.
    pub const TELEMETRY: Self = Self {
        messages: false,
        spans: false,
        samples: true,
        text: false,
    };

    /// Только спаны.
    pub const SPANS: Self = Self {
        messages: false,
        spans: true,
        samples: false,
        text: false,
    };
}

/// Запрос.
///
/// Строится цепочкой: каждый метод возвращает новый запрос, а не меняет
/// существующий. `q.limit(500);` отдельным выражением потерялось бы целиком.
#[derive(Debug, Clone, Default)]
#[must_use = "запрос строится цепочкой: результат метода и есть настроенный запрос"]
pub struct Query {
    pub namespaces: NsSelect,
    /// Каналы по классам хранения. Пусто — все.
    pub channels: Vec<StorageClass>,
    /// Ограничение по запуску ПО: только его сегменты.
    pub boot: Option<BootCounter>,
    /// Нижняя граница окна, включительно.
    pub from: Option<Timestamp>,
    /// Верхняя граница окна, включительно.
    pub to: Option<Timestamp>,
    pub filter: Filter,
    pub order: Order,
    /// Максимум записей в ответе.
    pub limit: Option<usize>,
    /// Дотянуть состояния на левый край окна.
    ///
    /// Состояния пишут **по изменению**, а не периодически — иначе в них нет
    /// смысла. Значит окно `from..to` может не содержать ни одного отсчёта
    /// ряда, который всё это время держал одно значение, и полоса состояний
    /// на графике осталась бы пустой, хотя состояние было известно.
    ///
    /// С этим флагом ответ несёт ещё и последний отсчёт каждого
    /// ряда-состояния, сделанный **до** `from` — отдельно от `entries`, чтобы
    /// не нарушать обещание «всё в ответе лежит внутри диапазона».
    pub seed_states: bool,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namespaces(mut self, select: NsSelect) -> Self {
        self.namespaces = select;
        self
    }

    pub fn group(mut self, prefix: impl Into<String>) -> Self {
        self.namespaces = NsSelect::Group(prefix.into());
        self
    }

    /// Читать только каналы этого класса хранения.
    ///
    /// Класс — перечисление, а не имя: канал с опечаткой непредставим.
    pub fn channel(mut self, class: StorageClass) -> Self {
        self.channels.push(class);
        self
    }

    /// Окно времени `from..=to` — пара к [`Query::boot_window`], но границы
    /// любой шкалы. Шкалы могут и различаться, только смешивать их стоит
    /// осознанно: настенная граница переводится по якорю, и запуск без
    /// якоря выпадет из выборки целиком.
    pub fn time_window(mut self, from: impl Into<Timestamp>, to: impl Into<Timestamp>) -> Self {
        self.from = Some(from.into());
        self.to = Some(to.into());
        self
    }

    /// Только нижняя граница.
    pub fn since(mut self, from: impl Into<Timestamp>) -> Self {
        self.from = Some(from.into());
        self
    }

    /// Только верхняя граница.
    pub fn until(mut self, to: impl Into<Timestamp>) -> Self {
        self.to = Some(to.into());
        self
    }

    /// Окно внутри одного запуска — обычный случай для относительного времени.
    pub fn boot_window(mut self, boot: impl Into<BootCounter>, from: Micros, to: Micros) -> Self {
        let boot = boot.into();
        self.boot = Some(boot);
        self.from = Some(Timestamp::Boot(BootTime::new(boot, from)));
        self.to = Some(Timestamp::Boot(BootTime::new(boot, to)));
        self
    }

    pub fn boot(mut self, boot: impl Into<BootCounter>) -> Self {
        self.boot = Some(boot.into());
        self
    }

    pub fn min_level(mut self, level: Level) -> Self {
        self.filter.min_level = Some(level);
        self
    }

    /// Только записи с этим тэгом. Несколько вызовов — «хотя бы один из».
    ///
    /// Тэг несут сообщения и отсчёты; текст и спаны при таком фильтре
    /// выпадают — см. [`Filter`].
    pub fn any_tag(mut self, tag: impl Into<String>) -> Self {
        self.filter.any_tags.push(tag.into());
        self
    }

    /// Только события этого типа. Несколько вызовов — «любой из названных».
    pub fn event(mut self, id: EventId) -> Self {
        self.filter
            .events
            .get_or_insert_with(HashSet::new)
            .insert(id);
        self
    }

    /// Только события с этим именем (резолвится по схеме).
    pub fn event_name(mut self, name: impl Into<String>) -> Self {
        self.filter.event_names.push(name.into());
        self
    }

    /// Только записи, привязанные к этому спану.
    pub fn span(mut self, id: SpanId) -> Self {
        self.filter
            .spans
            .get_or_insert_with(HashSet::new)
            .insert(id);
        self
    }

    pub fn kinds(mut self, kinds: KindFilter) -> Self {
        self.filter.kinds = kinds;
        self
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Дотянуть состояния на левый край окна — см. [`Query::seed_states`].
    pub fn with_state_seed(mut self) -> Self {
        self.seed_states = true;
        self
    }

    /// Привести границы к тому, что можно сравнивать с записями.
    ///
    /// Пока обе границы относительные, эпохи не нужны вовсе — сравнение идёт
    /// по (запуск, µs). Стоит появиться настенной границе, и приходится
    /// разбирать окно по запускам: у каждого своя шкала и свой якорь.
    pub fn resolve(&self, epochs: &Epochs) -> Resolution {
        let relative = self.from.is_none_or(Timestamp::is_relative)
            && self.to.is_none_or(Timestamp::is_relative);
        if relative {
            let bounds = match (self.from, self.to) {
                (None, None) => Bounds::All,
                (from, to) => Bounds::Relative {
                    from: from.and_then(boot_time),
                    to: to.and_then(boot_time),
                },
            };
            return Resolution {
                bounds,
                unanchored: Vec::new(),
            };
        }

        let mut runs = BTreeMap::new();
        let mut anchored = BTreeSet::new();
        let mut unanchored = Vec::new();
        for run in epochs.runs() {
            match run_bounds(run.boot_counter, self.from, self.to, epochs) {
                Fit::In(b) => {
                    anchored.insert(run.boot_counter);
                    runs.insert(run.boot_counter, b);
                }
                Fit::Outside => {
                    anchored.insert(run.boot_counter);
                }
                Fit::Unanchored => unanchored.push(BootCounter(run.boot_counter)),
            }
        }
        Resolution {
            bounds: Bounds::Wall { runs, anchored },
            unanchored,
        }
    }
}

fn boot_time(t: Timestamp) -> Option<BootTime> {
    match t {
        Timestamp::Boot(at) => Some(at),
        Timestamp::Utc(_) => None,
    }
}

/// Перевести границы окна в шкалу одного запуска.
fn run_bounds(boot: u32, from: Option<Timestamp>, to: Option<Timestamp>, epochs: &Epochs) -> Fit {
    let lower = match from {
        None => None,
        // Запуск раньше границы — весь позади; позже — весь внутри.
        Some(Timestamp::Boot(b)) if b.boot.0 > boot => return Fit::Outside,
        Some(Timestamp::Boot(b)) if b.boot.0 == boot => Some(b.at),
        Some(Timestamp::Boot(_)) => None,
        Some(Timestamp::Utc(t)) => match epochs.from_utc(boot, t) {
            RunOffset::Unanchored => return Fit::Unanchored,
            // Запуск начался позже нижней границы — ограничивать нечего.
            RunOffset::BeforeStart => None,
            RunOffset::At(m) => Some(m),
        },
    };
    let upper = match to {
        None => None,
        Some(Timestamp::Boot(b)) if b.boot.0 < boot => return Fit::Outside,
        Some(Timestamp::Boot(b)) if b.boot.0 == boot => Some(b.at),
        Some(Timestamp::Boot(_)) => None,
        Some(Timestamp::Utc(t)) => match epochs.from_utc(boot, t) {
            RunOffset::Unanchored => return Fit::Unanchored,
            // Запуск начался позже верхней границы — его тут нет.
            RunOffset::BeforeStart => return Fit::Outside,
            RunOffset::At(m) => Some(m),
        },
    };
    Fit::In(RunBounds {
        from: lower,
        to: upper,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_selects_by_prefix() {
        let g = NsSelect::Group("orc-".to_owned());
        assert!(g.matches("orc-radio-0"));
        assert!(g.matches("orc-radio-3"));
        assert!(!g.matches("apt-modem-1"));

        let names = NsSelect::Names(vec!["orc-radio-0".to_owned()]);
        assert!(names.matches("orc-radio-0"));
        assert!(!names.matches("orc-radio-1"));

        assert!(NsSelect::All.matches("что угодно"));
    }

    #[test]
    fn relative_range_needs_no_epochs() {
        let epochs = Epochs::default();
        let q = Query::new().boot_window(0u32, Micros(100), Micros(200));
        let r = q.resolve(&epochs);
        assert!(r.unanchored.is_empty());

        let b = |at| BootTime::from_raw(0, at);
        assert!(!r.bounds.contains(b(99)));
        assert!(r.bounds.contains(b(100)));
        assert!(r.bounds.contains(b(150)));
        assert!(r.bounds.contains(b(200)));
        assert!(!r.bounds.contains(b(201)));

        // Без границ проходит всё, включая запуски, о которых эпохи не знают.
        let all = Query::new().resolve(&epochs);
        assert_eq!(all.bounds, Bounds::All);
        assert!(all.bounds.contains(BootTime::from_raw(9, u64::MAX)));
    }

    #[test]
    fn relative_bounds_span_runs_lexicographically() {
        // Граница живёт в шкале своего запуска, поэтому «от середины запуска 1»
        // означает: весь запуск 0 позади, у запуска 1 отсечена середина, запуск
        // 2 внутри целиком. Раньше это выражалось `boot` + `Micros` по
        // отдельности, и микросекунды прикладывались к каждому запуску.
        let q = Query::new().since(BootTime::from_raw(1, 500));
        let bounds = q.resolve(&Epochs::default()).bounds;

        assert_eq!(
            bounds.for_boot(BootCounter(0)),
            None,
            "запуск 0 весь позади"
        );
        assert_eq!(
            bounds.for_boot(BootCounter(1)),
            Some(RunBounds {
                from: Some(Micros(500)),
                to: None
            })
        );
        assert_eq!(
            bounds.for_boot(BootCounter(2)),
            Some(RunBounds::default()),
            "запуск 2 внутри окна целиком"
        );

        assert!(!bounds.contains(BootTime::from_raw(0, u64::MAX)));
        assert!(bounds.contains(BootTime::from_raw(1, 500)));
        assert!(bounds.contains(BootTime::from_raw(2, 0)));

        // Верхняя граница — симметрично.
        let bounds = Query::new()
            .until(BootTime::from_raw(1, 500))
            .resolve(&Epochs::default())
            .bounds;
        assert_eq!(bounds.for_boot(BootCounter(2)), None);
        assert!(bounds.contains(BootTime::from_raw(0, u64::MAX)));
        assert!(!bounds.contains(BootTime::from_raw(1, 501)));
    }

    #[test]
    fn wall_bounds_drop_unanchored_runs_and_say_so() {
        use dduroc_engine::epochs::{HwBoot, Run};

        // Два запуска одной загрузки железа плюс запуск загрузки без якоря.
        let anchor_ms = 1_700_000_000_000;
        let epochs = Epochs {
            runs: vec![
                Run {
                    boot_counter: 0,
                    hw_boot_id: 0,
                    boottime_at_init_us: 1_000_000,
                },
                Run {
                    boot_counter: 1,
                    hw_boot_id: 1,
                    boottime_at_init_us: 2_000_000,
                },
            ],
            hw_boots: vec![
                HwBoot {
                    hw_boot_id: 0,
                    kernel_boot_id: [0; 16],
                    utc_anchor_ms: Some(anchor_ms),
                    anchor_source: None,
                    anchor_captured_us: None,
                },
                HwBoot {
                    hw_boot_id: 1,
                    kernel_boot_id: [1; 16],
                    utc_anchor_ms: None,
                    anchor_source: None,
                    anchor_captured_us: None,
                },
            ],
        };

        let utc = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).unwrap();
        // Окно: от 3-й до 5-й секунды BOOTTIME первой загрузки.
        let r = Query::new()
            .time_window(utc(anchor_ms + 3_000), utc(anchor_ms + 5_000))
            .resolve(&epochs);

        assert_eq!(
            r.bounds.for_boot(BootCounter(0)),
            Some(RunBounds {
                from: Some(Micros(2_000_000)), // 3 с BOOTTIME − 1 с старта run'а
                to: Some(Micros(4_000_000)),
            })
        );
        assert_eq!(
            r.bounds.for_boot(BootCounter(1)),
            None,
            "запуск без якоря не сравнить с настенным временем"
        );
        assert_eq!(
            r.unanchored,
            vec![BootCounter(1)],
            "выпавший запуск обязан быть назван"
        );

        // Запуска, о котором эпохи не знают, при настенных границах тоже нет:
        // якоря у него взяться неоткуда.
        assert_eq!(r.bounds.for_boot(BootCounter(42)), None);
    }

    #[test]
    fn wall_lower_bound_before_run_start_keeps_whole_run() {
        use dduroc_engine::epochs::{HwBoot, Run};
        let anchor_ms = 1_700_000_000_000;
        let epochs = Epochs {
            runs: vec![Run {
                boot_counter: 0,
                hw_boot_id: 0,
                boottime_at_init_us: 10_000_000,
            }],
            hw_boots: vec![HwBoot {
                hw_boot_id: 0,
                kernel_boot_id: [0; 16],
                utc_anchor_ms: Some(anchor_ms),
                anchor_source: None,
                anchor_captured_us: None,
            }],
        };
        let utc = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).unwrap();

        // Нижняя граница раньше старта запуска: ограничивать нечего.
        let r = Query::new().since(utc(anchor_ms + 1_000)).resolve(&epochs);
        assert_eq!(
            r.bounds.for_boot(BootCounter(0)),
            Some(RunBounds::default())
        );

        // Верхняя граница раньше старта: запуска в окне нет вовсе.
        let r = Query::new().until(utc(anchor_ms + 1_000)).resolve(&epochs);
        assert_eq!(r.bounds.for_boot(BootCounter(0)), None);
        assert!(r.unanchored.is_empty(), "якорь есть, дело не в нём");
    }

    #[test]
    fn just_before_steps_one_microsecond_in_both_scales() {
        let t = Timestamp::Boot(BootTime::from_raw(2, 100));
        assert_eq!(
            t.just_before(),
            Timestamp::Boot(BootTime::from_raw(2, 99)),
            "запуск не меняется: шкала та же"
        );
        // На нуле шага нет — вычитать не из чего.
        assert_eq!(
            Timestamp::Boot(BootTime::from_raw(2, 0)).just_before(),
            Timestamp::Boot(BootTime::from_raw(2, 0))
        );

        let utc = chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let Timestamp::Utc(back) = Timestamp::Utc(utc).just_before() else {
            panic!("шкала не должна меняться");
        };
        assert_eq!(back.timestamp_micros(), 1_699_999_999_999_999);
    }

    #[test]
    fn kind_presets() {
        // Значения константные, поэтому сравниваем структуры целиком:
        // так проверка не вырождается в тавтологию для компилятора.
        assert_eq!(
            KindFilter::LOGS,
            KindFilter {
                messages: true,
                spans: false,
                samples: false,
                text: true
            }
        );
        assert_eq!(
            KindFilter::TELEMETRY,
            KindFilter {
                messages: false,
                spans: false,
                samples: true,
                text: false
            }
        );
        assert_eq!(
            KindFilter::SPANS,
            KindFilter {
                messages: false,
                spans: true,
                samples: false,
                text: false
            }
        );
        assert_eq!(
            KindFilter::default(),
            KindFilter {
                messages: true,
                spans: true,
                samples: true,
                text: true
            }
        );
    }
}
