//! Запрос: что читать и в каком порядке.

use dduroc_format::{EventId, Level, Micros};
use std::collections::HashSet;

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
            NsSelect::Group(prefix) => name.starts_with(prefix.as_str()),
        }
    }
}

/// Фильтр по содержимому.
///
/// Уровни и тэги — статические свойства типов, поэтому фильтрация по ним
/// не требует чтения записей: она сводится к вычислению множества
/// идентификаторов по схеме **до** сканирования.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Минимальный уровень (включительно).
    pub min_level: Option<Level>,
    /// Требуемые тэги: событие должно нести хотя бы один из них.
    pub any_tags: Vec<String>,
    /// Конкретные типы событий.
    pub events: Option<HashSet<EventId>>,
    /// Имена событий — резолвятся по схеме.
    pub event_names: Vec<String>,
    /// Только записи, привязанные к этим спанам.
    pub spans: Option<HashSet<u32>>,
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
#[derive(Debug, Clone, Default)]
pub struct Query {
    pub namespaces: NsSelect,
    /// Каналы по именам. Пусто — все.
    pub channels: Vec<String>,
    /// Ограничение по запуску ПО.
    pub boot: Option<u32>,
    pub from: Option<Micros>,
    pub to: Option<Micros>,
    pub filter: Filter,
    pub order: Order,
    /// Максимум записей в ответе.
    pub limit: Option<usize>,
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

    pub fn channel(mut self, name: impl Into<String>) -> Self {
        self.channels.push(name.into());
        self
    }

    pub fn range(mut self, from: Micros, to: Micros) -> Self {
        self.from = Some(from);
        self.to = Some(to);
        self
    }

    pub fn boot(mut self, boot: u32) -> Self {
        self.boot = Some(boot);
        self
    }

    pub fn min_level(mut self, level: Level) -> Self {
        self.filter.min_level = Some(level);
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

    /// Попадает ли время в диапазон запроса.
    pub fn in_range(&self, at: Micros) -> bool {
        if let Some(from) = self.from
            && at < from
        {
            return false;
        }
        if let Some(to) = self.to
            && at > to
        {
            return false;
        }
        true
    }
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
    fn range_check() {
        let q = Query::new().range(Micros(100), Micros(200));
        assert!(!q.in_range(Micros(99)));
        assert!(q.in_range(Micros(100)));
        assert!(q.in_range(Micros(150)));
        assert!(q.in_range(Micros(200)));
        assert!(!q.in_range(Micros(201)));

        // Без границ проходит всё.
        assert!(Query::new().in_range(Micros(u64::MAX)));
    }

    #[test]
    fn kind_presets() {
        assert!(KindFilter::LOGS.messages && KindFilter::LOGS.text);
        assert!(!KindFilter::LOGS.samples);
        assert!(KindFilter::TELEMETRY.samples && !KindFilter::TELEMETRY.messages);
        assert!(KindFilter::SPANS.spans && !KindFilter::SPANS.text);
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
