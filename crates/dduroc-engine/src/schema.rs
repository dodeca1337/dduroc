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

/// Класс хранения: имя канала, в который попадают записи этого типа.
///
/// Канал определяет политику долговечности и бюджет. Значимые данные
/// объявляют [`StorageClass::CRITICAL`], остальные — [`StorageClass::DEFAULT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StorageClass(pub &'static str);

impl StorageClass {
    /// Батчи с отложенной синхронизацией.
    pub const DEFAULT: Self = Self("default");
    /// Синхронизация сразу (group commit) — устойчивость к потере питания.
    pub const CRITICAL: Self = Self("critical");
    /// Отдельный канал под телеметрию: обычно самый большой бюджет.
    pub const TELEMETRY: Self = Self("telemetry");

    pub const fn as_str(self) -> &'static str {
        self.0
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

/// Тип метрики телеметрии.
#[derive(Debug, Clone, Copy)]
pub struct MetricDesc {
    pub id: MetricId,
    pub name: &'static str,
    pub value_type: ValueType,
    pub class: StorageClass,
    /// Единица измерения для UI (`"°C"`, `"dBm"`).
    pub unit: &'static str,
    /// Разрешённые ключи тэгов серии.
    pub tag_keys: &'static [&'static str],
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
    /// Типы, затронутые шагом. Сегменты, не содержащие ни одного из них,
    /// переписывать не нужно — прямая экономия ресурса флеша.
    pub events: &'static [EventId],
    pub metrics: &'static [MetricId],
    /// Преобразование одной записи. `Ok(None)` — запись удаляется.
    pub migrate: fn(MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError>,
}

impl std::fmt::Debug for Migration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Migration")
            .field("from", &self.from)
            .field("events", &self.events)
            .field("metrics", &self.metrics)
            .finish_non_exhaustive()
    }
}

/// Запись, поданная шагу миграции.
#[derive(Debug, Clone, Copy)]
pub struct MigratedRecord<'a> {
    pub record: dduroc_format::Record<'a>,
}

/// Результат преобразования: владеющая форма, так как шаг миграции обычно
/// перекодирует payload.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedRecord {
    /// Оставить как есть.
    AsIs,
    /// Заменить тип и/или payload сообщения.
    Message { event: EventId, payload: Vec<u8> },
    /// Заменить метрику сэмпла.
    SampleMetric(MetricId),
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

/// Ошибка валидации схемы: ловится при подъёме неймспейса, то есть на старте
/// процесса, а не через месяц работы на нечитаемых логах.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaError {
    #[error("схема {schema:?}: {kind} id {id} объявлен дважды ({first:?} и {second:?})")]
    DuplicateId {
        schema: &'static str,
        kind: &'static str,
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
            "event",
            self.events.iter().map(|e| (e.id.0, e.name)),
        )?;
        check_unique(
            self.name,
            "metric",
            self.metrics.iter().map(|m| (m.id.0, m.name)),
        )?;
        check_unique(
            self.name,
            "span",
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

    pub fn event(&self, id: EventId) -> Option<&'static EventDesc> {
        self.events.iter().find(|e| e.id == id)
    }

    pub fn metric(&self, id: MetricId) -> Option<&'static MetricDesc> {
        self.metrics.iter().find(|m| m.id == id)
    }

    pub fn span(&self, id: SpanKindId) -> Option<&'static SpanDesc> {
        self.spans.iter().find(|s| s.id == id)
    }

    pub fn language_index(&self, code: &str) -> Option<usize> {
        self.languages.iter().position(|l| l.0 == code)
    }

    /// Все каналы, которые может использовать схема.
    pub fn classes(&self) -> Vec<StorageClass> {
        let mut out: Vec<StorageClass> = Vec::new();
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

fn check_unique(
    schema: &'static str,
    kind: &'static str,
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
                class: StorageClass::CRITICAL,
                tags: &["rf"],
                templates: &["power {dbm}", "мощность {dbm}"],
                fields: &[],
                decoders: None,
            },
            EventDesc {
                id: EventId(2),
                name: "Failed",
                level: Level::Error,
                class: StorageClass::DEFAULT,
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
        assert_eq!(
            s.classes(),
            vec![StorageClass::CRITICAL, StorageClass::DEFAULT]
        );
        assert_eq!(
            s.event(EventId(1)).unwrap().template(1),
            Some("мощность {dbm}")
        );
    }

    #[test]
    fn duplicate_ids_rejected() {
        static EVENTS: &[EventDesc] = &[
            EventDesc {
                id: EventId(5),
                name: "A",
                level: Level::Info,
                class: StorageClass::DEFAULT,
                tags: &[],
                templates: &["a", "а"],
                fields: &[],
                decoders: None,
            },
            EventDesc {
                id: EventId(5),
                name: "B",
                level: Level::Info,
                class: StorageClass::DEFAULT,
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
            class: StorageClass::DEFAULT,
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
        fn noop(_: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            Ok(Some(OwnedRecord::AsIs))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            events: &[],
            metrics: &[],
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
                events: &[],
                metrics: &[],
                migrate: noop,
            },
            Migration {
                from: 2,
                events: &[],
                metrics: &[],
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
