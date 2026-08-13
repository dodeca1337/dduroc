//! Макрос `schema!` — декларация схемы неймспейса.
//!
//! Разворачивается в структуры событий, дескрипторы и константу схемы.
//! Всё статическое: схема целиком лежит в `.rodata` и в рантайме ничего
//! не стоит.
//!
//! ```ignore
//! dduroc::schema! {
//!     name: radio,
//!     version: 1,
//!     languages: [en, ru],
//!
//!     events {
//!         PowerSet = 0x01 {
//!             level: Info,
//!             store: critical,
//!             tags: [rf],
//!             en: "power set to {dbm} dBm",
//!             ru: "мощность {dbm} дБм",
//!             dbm: f32,
//!         },
//!         // Без полей — unit-структура: `ns.log(events::Started)`.
//!         Started = 0x02 { level: Info, en: "started", ru: "запущено" },
//!     }
//!
//!     metrics {
//!         // Диапазоны НОРМЫ — данные: рисуемы, обновляемы в рантайме.
//!         Temp = 0x01 { type: f32, unit: "°C", tags: [sensor],
//!                       warn: -40.0..=70.0, alarm: -40.0..=85.0 },
//!         // Форма, которую диапазоном не выразить, — предикат
//!         // СРАБАТЫВАНИЯ (`v` — значение): полярность обратная, поэтому
//!         // и ключ другой.
//!         Vswr = 0x02 { type: f32, warn_if: v > 1.5,
//!                       alarm_if: v > 3.0 || v < 1.0 },
//!         // Перечисление: важность — префиксом, без неё состояние
//!         // нормальное.
//!         Link = 0x03 { states: [alarm Los = 0, warn Sync = 1, Lock = 2] },
//!     }
//!
//!     spans {
//!         Calibration = 0x01,
//!     }
//!
//!     // Раскладки прежних версий — их потребляют шаги миграции.
//!     history {
//!         1 { events { PowerSet = 0x01 { dbm: i8 } } }
//!     }
//!
//!     // Шаг N приводит записи версии N к версии N+1.
//!     migrations {
//!         1 => {
//!             // Типизированное правило: декод старой раскладки, энкод новой
//!             // и диспетчер генерируются, затронутые типы выводятся из
//!             // ключей — разойтись с фактическим поведением им не из чего.
//!             v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) },
//!             event(0x05): drop,               // тип удалён — имени больше нет
//!             metric(0x07): metrics::Temp,     // ремап id, значение как было
//!             // Значение самоописуемо: тип называет само замыкание, и
//!             // history метрике не нужна.
//!             metrics::Vswr: |v: u64| v as f32 / 10.0,
//!             // У спана меняется только вид: номер и родитель — личность
//!             // записи, и удалить её начало правило не даст.
//!             span(0x07): spans::Calibration,
//!         },
//!         // Люк: сырой fn. Затронутые типы объявляются руками; не объявлены
//!         // — шаг считается затрагивающим всё.
//!         2 => migrate_v2,
//!     }
//! }
//! ```
//!
//! Идентификаторы **обязательно явные**. Позиционная авто-нумерация
//! прототипа при вставке события в середину списка молча перемапливала
//! исторические записи на чужие декодеры.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use std::collections::HashMap;
use syn::parse::{Parse, ParseStream};
use syn::{Ident, LitInt, LitStr, Token, Type, braced, bracketed, parse_macro_input};

mod template;

// ════════════════════════════════════════════════════════════════════════════
// AST
// ════════════════════════════════════════════════════════════════════════════

struct SchemaInput {
    name: Ident,
    version: u16,
    languages: Vec<Ident>,
    events: Vec<EventDef>,
    metrics: Vec<MetricDef>,
    spans: Vec<SpanDef>,
    migrations: Vec<MigrationDef>,
    history: Vec<HistoryDef>,
}

struct EventDef {
    name: Ident,
    id: u16,
    level: Ident,
    store: Option<Ident>,
    tags: Vec<Ident>,
    /// Шаблоны по языкам в порядке объявления.
    templates: Vec<(Ident, LitStr)>,
    fields: Vec<(Ident, Type)>,
}

struct MetricDef {
    name: Ident,
    id: u16,
    /// `None` — тип выводится: у перечисления это `u64`.
    value_type: Option<Ident>,
    unit: LitStr,
    tags: Vec<Ident>,
    store: Option<Ident>,
    kind: Option<Ident>,
    states: Vec<StateDef>,
    /// Диапазоны нормы: вне них — соответствующая важность.
    warn: Option<syn::ExprRange>,
    alarm: Option<syn::ExprRange>,
    /// Предикаты срабатывания (`v` — значение): истина — уровень достигнут.
    warn_if: Option<syn::Expr>,
    alarm_if: Option<syn::Expr>,
}

/// Одно состояние метрики-перечисления: `alarm Los = 0`.
#[derive(Clone)]
struct StateDef {
    name: Ident,
    code: u64,
    /// `None` — состояние нормальное.
    severity: Option<Ident>,
}

struct SpanDef {
    name: Ident,
    id: u16,
    store: Option<Ident>,
}

struct MigrationDef {
    from: u16,
    step: StepDef,
}

/// Тело шага миграции.
enum StepDef {
    /// Сырой fn — люк для того, что правилами не выразить.
    Raw {
        func: syn::Path,
        /// Типы, затронутые шагом. `None` — не объявлены, значит шаг
        /// считается затрагивающим **всё**: пропустить сегмент молча хуже,
        /// чем переписать лишний.
        touches: Option<Touches>,
    },
    /// Типизированные правила: диспетчер, декод и энкод генерирует макрос, а
    /// затронутые типы **выводятся** из ключей — разойтись с фактическим
    /// поведением им не из чего.
    Rules(Vec<RuleDef>),
}

/// Затронутые шагом миграции типы.
#[derive(Clone, Default)]
struct Touches {
    events: Vec<Ident>,
    metrics: Vec<Ident>,
    spans: Vec<Ident>,
}

/// Одно правило типизированного шага: `ключ: действие`.
struct RuleDef {
    key: RuleKey,
    action: RuleAction,
}

/// Что правило отбирает — по идентификатору В ТОЙ версии, которую шаг читает.
enum RuleKey {
    /// `v1::PowerSet` — раскладка из history: и старый id, и тип для декода.
    HistoryEvent { version: u16, name: Ident },
    /// `events::PowerSet` — текущая раскладка (чистка значений, drop).
    CurrentEvent { name: Ident },
    /// `event(0x05)` — голый id: тип удалён из схемы, имени больше нет.
    RawEvent { id: u16, span: proc_macro2::Span },
    /// `metrics::Temp` — текущая метрика.
    CurrentMetric { name: Ident },
    /// `metric(0x07)` — голый id метрики.
    RawMetric { id: u16, span: proc_macro2::Span },
    /// `spans::Calibration` — текущий вид спана.
    CurrentSpan { name: Ident },
    /// `span(0x03)` — голый id вида: вид удалён из схемы, имени больше нет.
    RawSpan { id: u16, span: proc_macro2::Span },
}

impl RuleKey {
    fn span(&self) -> proc_macro2::Span {
        match self {
            RuleKey::HistoryEvent { name, .. }
            | RuleKey::CurrentEvent { name }
            | RuleKey::CurrentMetric { name }
            | RuleKey::CurrentSpan { name } => name.span(),
            RuleKey::RawEvent { span, .. }
            | RuleKey::RawMetric { span, .. }
            | RuleKey::RawSpan { span, .. } => *span,
        }
    }
}

/// Что правило делает с записью.
enum RuleAction {
    /// `drop` — запись удаляется.
    Drop,
    /// Замыкание или функция: декод старой раскладки → новый payload.
    Map(syn::Expr),
    /// `events::New` — сменить id, payload байт в байт.
    RemapEvent(Ident),
    /// `metrics::New` — сменить метрику отсчёта.
    RemapMetric(Ident),
    /// `spans::New` — переименовать вид спана.
    RemapSpan(Ident),
}

/// Раскладки изменившихся типов, как они были в версии `version`, — то, что
/// потребляют шаги миграции. Порождает `pub mod v<N>` с Deserialize-типами.
struct HistoryDef {
    version: u16,
    span: proc_macro2::Span,
    events: Vec<HistoryEvent>,
}

/// Старое событие: только id и поля — уровень, шаблоны и тэги на диске не
/// живут, и старой раскладке они не нужны.
struct HistoryEvent {
    name: Ident,
    id: u16,
    fields: Vec<(Ident, Type)>,
}

// ════════════════════════════════════════════════════════════════════════════
// Разбор
// ════════════════════════════════════════════════════════════════════════════

fn parse_id(input: ParseStream) -> syn::Result<u16> {
    input.parse::<Token![=]>()?;
    let lit: LitInt = input.parse()?;
    lit.base10_parse::<u16>()
        .map_err(|e| syn::Error::new(lit.span(), format!("идентификатор не влезает в u16: {e}")))
}

fn parse_ident_list(input: ParseStream) -> syn::Result<Vec<Ident>> {
    let content;
    bracketed!(content in input);
    let mut out = Vec::new();
    while !content.is_empty() {
        out.push(content.parse()?);
        let _ = content.parse::<Token![,]>();
    }
    Ok(out)
}

impl Parse for SchemaInput {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut name = None;
        let mut version = None;
        let mut languages = Vec::new();
        let mut events = Vec::new();
        let mut metrics = Vec::new();
        let mut spans = Vec::new();
        let mut migrations = Vec::new();
        let mut history = Vec::new();

        while !input.is_empty() {
            let key: Ident = input.parse()?;
            match key.to_string().as_str() {
                "name" => {
                    input.parse::<Token![:]>()?;
                    name = Some(input.parse::<Ident>()?);
                    let _ = input.parse::<Token![,]>();
                }
                "version" => {
                    input.parse::<Token![:]>()?;
                    let lit: LitInt = input.parse()?;
                    version = Some(lit.base10_parse::<u16>()?);
                    let _ = input.parse::<Token![,]>();
                }
                "languages" => {
                    input.parse::<Token![:]>()?;
                    languages = parse_ident_list(input)?;
                    let _ = input.parse::<Token![,]>();
                }
                "events" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        events.push(parse_event(&content)?);
                        let _ = content.parse::<Token![,]>();
                    }
                }
                "metrics" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        metrics.push(parse_metric(&content)?);
                        let _ = content.parse::<Token![,]>();
                    }
                }
                "spans" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        spans.push(parse_span(&content)?);
                        let _ = content.parse::<Token![,]>();
                    }
                }
                "migrations" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        let lit: LitInt = content.parse()?;
                        content.parse::<Token![=>]>()?;
                        // Скобки сразу после `=>` — типизированные правила;
                        // иначе путь к сырому fn с необязательными touches.
                        let step = if content.peek(syn::token::Brace) {
                            StepDef::Rules(parse_rules(&content)?)
                        } else {
                            let func: syn::Path = content.parse()?;
                            let touches = if content.peek(syn::token::Brace) {
                                Some(parse_touches(&content)?)
                            } else {
                                None
                            };
                            StepDef::Raw { func, touches }
                        };
                        migrations.push(MigrationDef {
                            from: lit.base10_parse()?,
                            step,
                        });
                        let _ = content.parse::<Token![,]>();
                    }
                }
                "history" => {
                    let content;
                    braced!(content in input);
                    while !content.is_empty() {
                        history.push(parse_history(&content)?);
                        let _ = content.parse::<Token![,]>();
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "неизвестная секция `{other}`: ожидались name, version, \
                             languages, events, metrics, spans, history, migrations"
                        ),
                    ));
                }
            }
        }

        let name = name.ok_or_else(|| syn::Error::new(input.span(), "не задано `name:`"))?;
        let version =
            version.ok_or_else(|| syn::Error::new(name.span(), "не задано `version:`"))?;
        if languages.is_empty() {
            return Err(syn::Error::new(
                name.span(),
                "не задан `languages:` — рендерить сообщения будет нечем",
            ));
        }
        Self::check_templates(&events, &languages)?;

        Ok(Self {
            name,
            version,
            languages,
            events,
            metrics,
            spans,
            migrations,
            history,
        })
    }
}

impl SchemaInput {
    /// Сверить шаблоны событий с объявленными языками.
    ///
    /// Делается после разбора всего объявления, а не по ходу: секции не
    /// обязаны идти в каком-либо порядке, и до конца разбора список языков
    /// неизвестен.
    fn check_templates(events: &[EventDef], languages: &[Ident]) -> syn::Result<()> {
        for ev in events {
            // Шаблон обязан быть на каждом объявленном языке: недостающий
            // всплыл бы при чтении логов на языке, которого нет, — то есть в
            // самый неудачный момент.
            for lang in languages {
                if !ev.templates.iter().any(|(l, _)| l == lang) {
                    return Err(syn::Error::new(
                        ev.name.span(),
                        format!("у события `{}` нет шаблона для языка `{lang}`", ev.name),
                    ));
                }
            }
            // И наоборот: шаблон на языке, которого нет в `languages:`,
            // никогда не будет показан. Промолчать значило бы оставить
            // перевод, который пользователь считает работающим.
            for (lang, _) in &ev.templates {
                if !languages.contains(lang) {
                    let declared: Vec<String> = languages.iter().map(|l| l.to_string()).collect();
                    return Err(syn::Error::new(
                        lang.span(),
                        format!(
                            "у события `{}` шаблон на языке `{lang}`, но в `languages:` \
                             объявлены только {}",
                            ev.name,
                            declared.join(", ")
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Разобрать объявление события.
///
/// Список языков сюда **не передаётся**: секции объявления не обязаны идти в
/// каком-то порядке, и `events` выше `languages` — законная запись. Раньше
/// шаблон отличался от поля принадлежностью ключа к списку языков, поэтому при
/// таком порядке `en: "мощность {dbm}"` разбиралось как поле типа
/// `"мощность {dbm}"`, и пользователь получал ошибку о неразобранном типе
/// вместо внятного сообщения о своей схеме.
///
/// Признак теперь синтаксический: значение шаблона — строковый литерал,
/// значение поля — тип, а тип строковым литералом быть не может. Полнота
/// шаблонов по языкам проверяется потом, когда объявление разобрано целиком
/// (см. [`SchemaInput::check_templates`]).
fn parse_event(input: ParseStream) -> syn::Result<EventDef> {
    let name: Ident = input.parse()?;
    let id = parse_id(input)?;
    let content;
    braced!(content in input);

    let mut level = None;
    let mut store = None;
    let mut tags = Vec::new();
    let mut templates: Vec<(Ident, LitStr)> = Vec::new();
    let mut fields = Vec::new();

    while !content.is_empty() {
        let key: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        let key_str = key.to_string();

        if key_str == "level" {
            level = Some(content.parse::<Ident>()?);
        } else if key_str == "store" {
            store = Some(content.parse::<Ident>()?);
        } else if key_str == "tags" {
            tags = parse_ident_list(&content)?;
        } else if content.peek(LitStr) {
            if templates.iter().any(|(l, _)| *l == key) {
                return Err(syn::Error::new(
                    key.span(),
                    format!("шаблон `{key_str}` задан дважды"),
                ));
            }
            templates.push((key.clone(), content.parse::<LitStr>()?));
        } else {
            // Всё остальное — поле payload'а.
            fields.push((key, content.parse::<Type>()?));
        }
        let _ = content.parse::<Token![,]>();
    }

    let level = level.ok_or_else(|| {
        syn::Error::new(name.span(), format!("у события `{name}` не задан `level:`"))
    })?;

    // Плейсхолдеры обязаны ссылаться на существующие поля. Это проверяется
    // здесь: список языков для такой проверки не нужен.
    let field_names: Vec<String> = fields.iter().map(|(n, _)| n.to_string()).collect();
    for (lang, tmpl) in &templates {
        for placeholder in template::placeholders(&tmpl.value()) {
            if !field_names.contains(&placeholder) {
                return Err(syn::Error::new(
                    tmpl.span(),
                    format!(
                        "шаблон `{lang}` события `{name}` ссылается на `{{{placeholder}}}`, \
                         но такого поля нет"
                    ),
                ));
            }
        }
    }

    Ok(EventDef {
        name,
        id,
        level,
        store,
        tags,
        templates,
        fields,
    })
}

/// Разобрать список состояний: `[alarm Los = 0, warn Sync = 1, Lock = 2]`.
///
/// Важность стоит префиксом и читается как строка журнала: «авария — Los».
/// Коды **обязательно** явные: позиционная нумерация сдвинулась бы при вставке
/// состояния в середину списка, и уже записанные сегменты стали бы читаться
/// неверно, без единого признака ошибки.
fn parse_states(input: ParseStream) -> syn::Result<Vec<StateDef>> {
    let content;
    bracketed!(content in input);
    let mut out = Vec::new();
    while !content.is_empty() {
        // Два идентификатора подряд — первый из них важность.
        let first: Ident = content.parse()?;
        let (severity, name) = if content.peek(Ident) {
            (Some(first), content.parse::<Ident>()?)
        } else {
            (None, first)
        };
        content.parse::<Token![=]>().map_err(|_| {
            syn::Error::new(
                name.span(),
                format!(
                    "у состояния `{name}` не задан код: пишите `{name} = 0`. \
                     Код обязан быть явным — позиционная нумерация сдвинулась бы \
                     при вставке состояния в середину списка"
                ),
            )
        })?;
        let lit: LitInt = content.parse()?;
        let code = lit.base10_parse::<u64>()?;
        if content.peek(Token![:]) {
            content.parse::<Token![:]>()?;
            let sev: Ident = content.parse()?;
            return Err(syn::Error::new(
                sev.span(),
                format!("важность пишется префиксом: `{sev} {name} = {code}`"),
            ));
        }
        out.push(StateDef {
            name,
            code,
            severity,
        });
        let _ = content.parse::<Token![,]>();
    }
    Ok(out)
}

fn parse_metric(input: ParseStream) -> syn::Result<MetricDef> {
    let name: Ident = input.parse()?;
    let id = parse_id(input)?;
    let content;
    braced!(content in input);

    let mut value_type = None;
    let mut unit = None;
    let mut tags = Vec::new();
    let mut store = None;
    let mut kind = None;
    let mut states = Vec::new();
    let mut warn = None;
    let mut alarm = None;
    let mut warn_if = None;
    let mut alarm_if = None;

    while !content.is_empty() {
        // `type` — ключевое слово Rust, обычным `Ident` его не разобрать.
        let key: Ident = if content.peek(Token![type]) {
            let t = content.parse::<Token![type]>()?;
            Ident::new("type", t.span)
        } else {
            content.parse()?
        };
        content.parse::<Token![:]>()?;
        match key.to_string().as_str() {
            "type" => value_type = Some(content.parse::<Ident>()?),
            "unit" => unit = Some(content.parse::<LitStr>()?),
            "tags" => tags = parse_ident_list(&content)?,
            "store" => store = Some(content.parse::<Ident>()?),
            "kind" => kind = Some(content.parse::<Ident>()?),
            "states" => states = parse_states(&content)?,
            "warn" => warn = Some(parse_norm_range(&content, &key)?),
            "alarm" => alarm = Some(parse_norm_range(&content, &key)?),
            // Предикат срабатывания: у него полярность обратная диапазону,
            // поэтому и ключ другой — перепутать их молча невозможно.
            "warn_if" => warn_if = Some(content.parse::<syn::Expr>()?),
            "alarm_if" => alarm_if = Some(content.parse::<syn::Expr>()?),
            "value_type" => {
                return Err(syn::Error::new(
                    key.span(),
                    "ключ называется `type`: приставка `v` не несла смысла",
                ));
            }
            // Подсказка вместо «неизвестный ключ»: пара warn/critical —
            // естественная догадка, а слово занято классом хранения.
            "critical" => {
                return Err(syn::Error::new(
                    key.span(),
                    "аварийный диапазон называется `alarm`, а не `critical`: \
                     слово `critical` занято классом хранения (`store: critical`), \
                     и в одном объявлении метрики они означали бы разное",
                ));
            }
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "у метрики неизвестный ключ `{other}`: ожидались type, unit, \
                         tags, store, kind, states, warn, alarm, warn_if, alarm_if"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }

    // Диапазон и предикат одного уровня вместе неоднозначны: у них
    // противоположная полярность (норма против срабатывания), и объединять
    // их молча значило бы гадать за пользователя.
    for (range, predicate, key) in [(&warn, &warn_if, "warn"), (&alarm, &alarm_if, "alarm")] {
        if range.is_some() && predicate.is_some() {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "у метрики `{name}` заданы и `{key}:`, и `{key}_if:`: диапазон \
                     описывает норму, предикат — срабатывание; выберите одну форму"
                ),
            ));
        }
    }
    if !states.is_empty() && (warn_if.is_some() || alarm_if.is_some()) {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "у перечисления `{name}` предикаты не имеют смысла: важность \
                 задаётся самим состояниям — `alarm Los = 0`"
            ),
        ));
    }

    // Тип перечисления выводится: код состояния — целое.
    if value_type.is_none() && !states.is_empty() {
        value_type = Some(Ident::new("u64", name.span()));
    }
    if value_type.is_none() {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "у метрики `{name}` не задан `type:` (у перечисления он выводится \
                 из `states:`)"
            ),
        ));
    }
    if value_type.as_ref().is_some_and(|t| t == "blob") && (warn_if.is_some() || alarm_if.is_some())
    {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "у метрики `{name}` тип blob: он не приводится к числу, и предикату нечего проверять"
            ),
        ));
    }
    let unit = unit.unwrap_or_else(|| LitStr::new("", name.span()));

    Ok(MetricDef {
        name,
        id,
        value_type,
        unit,
        tags,
        store,
        kind,
        states,
        warn,
        alarm,
        warn_if,
        alarm_if,
    })
}

/// Диапазон нормы `warn:`/`alarm:` — с подсказкой, если написано условие.
///
/// Перепутать формы легко, а ошибка парсера про «ожидался диапазон» не
/// объясняет главного: у форм противоположная полярность.
fn parse_norm_range(input: ParseStream, key: &Ident) -> syn::Result<syn::ExprRange> {
    match input.parse::<syn::Expr>()? {
        syn::Expr::Range(r) => Ok(r),
        other => Err(syn::Error::new_spanned(
            &other,
            format!(
                "`{key}:` принимает диапазон НОРМЫ (`{key}: -40.0..=70.0`); условие \
                 срабатывания пишется предикатом: `{key}_if: v > 70.0`"
            ),
        )),
    }
}

/// Разобрать затронутые шагом типы: `{ events: [A, B], metrics: [Temp] }`.
///
/// Объявление необязательно, и это осознанный выбор: миграция переписывает
/// только сегменты, содержащие затронутые типы (множества их идентификаторов
/// лежат в footer'е), а не переписанный сегмент — сэкономленный ресурс флеша.
/// Но экономия обязана быть **включена явно**: забытый список не должен
/// означать «ничего не трогаем», иначе шаг молча обошёл бы всю историю
/// стороной, оставив её в прежней раскладке.
fn parse_touches(input: ParseStream) -> syn::Result<Touches> {
    let content;
    braced!(content in input);
    let mut out = Touches::default();
    while !content.is_empty() {
        let key: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        match key.to_string().as_str() {
            "events" => out.events = parse_ident_list(&content)?,
            "metrics" => out.metrics = parse_ident_list(&content)?,
            "spans" => out.spans = parse_ident_list(&content)?,
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "у шага миграции неизвестный ключ `{other}`: ожидались \
                         events, metrics, spans"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }
    Ok(out)
}

/// Разобрать типизированные правила шага: `{ ключ: действие, ... }`.
fn parse_rules(input: ParseStream) -> syn::Result<Vec<RuleDef>> {
    let content;
    braced!(content in input);
    let mut out = Vec::new();
    while !content.is_empty() {
        let key = parse_rule_key(&content)?;
        content.parse::<Token![:]>()?;
        let action = parse_rule_action(&content)?;
        out.push(RuleDef { key, action });
        let _ = content.parse::<Token![,]>();
    }
    Ok(out)
}

/// Ключ правила: `v1::PowerSet`, `events::X`, `metrics::X`,
/// `event(0x05)`, `metric(0x07)`.
fn parse_rule_key(input: ParseStream) -> syn::Result<RuleKey> {
    if input.peek(Ident) && input.peek2(syn::token::Paren) {
        let kind: Ident = input.parse()?;
        let content;
        syn::parenthesized!(content in input);
        let lit: LitInt = content.parse()?;
        let id = lit.base10_parse::<u16>()?;
        return match kind.to_string().as_str() {
            "event" => Ok(RuleKey::RawEvent {
                id,
                span: lit.span(),
            }),
            "metric" => Ok(RuleKey::RawMetric {
                id,
                span: lit.span(),
            }),
            "span" => Ok(RuleKey::RawSpan {
                id,
                span: lit.span(),
            }),
            other => Err(syn::Error::new(
                kind.span(),
                format!("неизвестный вид ключа `{other}(..)`: ожидались event, metric, span"),
            )),
        };
    }

    let path: syn::Path = input.parse()?;
    let bad = |msg: &str| Err(syn::Error::new_spanned(&path, msg.to_owned()));
    if path.segments.len() != 2 {
        return bad(
            "ключ правила — путь из двух сегментов: `v1::Тип`, `events::Тип`, \
             `metrics::Метрика`, `spans::Вид`, либо голый id: `event(0x05)`, \
             `metric(0x07)`, `span(0x03)`",
        );
    }
    let head = path.segments[0].ident.to_string();
    let name = path.segments[1].ident.clone();
    if head == "events" {
        return Ok(RuleKey::CurrentEvent { name });
    }
    if head == "metrics" {
        return Ok(RuleKey::CurrentMetric { name });
    }
    if head == "spans" {
        return Ok(RuleKey::CurrentSpan { name });
    }
    if let Some(digits) = head.strip_prefix('v')
        && let Ok(version) = digits.parse::<u16>()
    {
        return Ok(RuleKey::HistoryEvent { version, name });
    }
    bad("первый сегмент ключа — `v<N>` (раскладка из history), `events`, `metrics` или `spans`")
}

/// Действие правила: `drop`, путь-ремап или замыкание/функция.
fn parse_rule_action(input: ParseStream) -> syn::Result<RuleAction> {
    let expr: syn::Expr = input.parse()?;
    if let syn::Expr::Path(p) = &expr {
        let segs = &p.path.segments;
        if segs.len() == 1 && segs[0].ident == "drop" {
            return Ok(RuleAction::Drop);
        }
        if segs.len() == 2 {
            let head = segs[0].ident.to_string();
            let name = segs[1].ident.clone();
            if head == "events" {
                return Ok(RuleAction::RemapEvent(name));
            }
            if head == "metrics" {
                return Ok(RuleAction::RemapMetric(name));
            }
            if head == "spans" {
                return Ok(RuleAction::RemapSpan(name));
            }
        }
    }
    Ok(RuleAction::Map(expr))
}

/// Разобрать одну запись history: `N { events { Имя = 0xID { поле: тип } } }`.
fn parse_history(input: ParseStream) -> syn::Result<HistoryDef> {
    let lit: LitInt = input.parse()?;
    let version = lit.base10_parse::<u16>()?;
    let content;
    braced!(content in input);

    let mut events = Vec::new();
    while !content.is_empty() {
        let key: Ident = content.parse()?;
        match key.to_string().as_str() {
            "events" => {
                let inner;
                braced!(inner in content);
                while !inner.is_empty() {
                    events.push(parse_history_event(&inner)?);
                    let _ = inner.parse::<Token![,]>();
                }
            }
            // У отсчётов и спанов payload-раскладки нет: их идентификаторы
            // ремапятся правилами напрямую, объявлять в history нечего.
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "в history объявляются только `events`, получено `{other}`: \
                         у отсчётов и спанов нет payload-раскладки"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }
    if events.is_empty() {
        return Err(syn::Error::new(
            lit.span(),
            format!("history {version} пуста: раз раскладки не изменились, запись не нужна"),
        ));
    }
    Ok(HistoryDef {
        version,
        span: lit.span(),
        events,
    })
}

/// Старое событие: `Имя = 0xID { поле: тип, ... }`. Только поля — уровень,
/// шаблоны и тэги на диск не пишутся, и старой раскладке они не нужны.
fn parse_history_event(input: ParseStream) -> syn::Result<HistoryEvent> {
    let name: Ident = input.parse()?;
    let id = parse_id(input)?;
    let content;
    braced!(content in input);
    let mut fields = Vec::new();
    while !content.is_empty() {
        let field: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        fields.push((field, content.parse::<Type>()?));
        let _ = content.parse::<Token![,]>();
    }
    Ok(HistoryEvent { name, id, fields })
}

fn parse_span(input: ParseStream) -> syn::Result<SpanDef> {
    let name: Ident = input.parse()?;
    let id = parse_id(input)?;
    let mut store = None;
    if input.peek(syn::token::Brace) {
        let content;
        braced!(content in input);
        while !content.is_empty() {
            let key: Ident = content.parse()?;
            content.parse::<Token![:]>()?;
            match key.to_string().as_str() {
                "store" => store = Some(content.parse::<Ident>()?),
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!("у спана неизвестный ключ `{other}`"),
                    ));
                }
            }
            let _ = content.parse::<Token![,]>();
        }
    }
    Ok(SpanDef { name, id, store })
}

// ════════════════════════════════════════════════════════════════════════════
// Генерация
// ════════════════════════════════════════════════════════════════════════════

fn level_path(level: &Ident) -> syn::Result<TokenStream2> {
    Ok(match level.to_string().as_str() {
        "DevTrace" => quote!(::dduroc::Level::DevTrace),
        "Trace" => quote!(::dduroc::Level::Trace),
        "Debug" => quote!(::dduroc::Level::Debug),
        "Info" => quote!(::dduroc::Level::Info),
        "Warn" => quote!(::dduroc::Level::Warn),
        "Error" => quote!(::dduroc::Level::Error),
        other => {
            return Err(syn::Error::new(
                level.span(),
                format!(
                    "неизвестный уровень `{other}`: ожидались \
                     DevTrace/Trace/Debug/Info/Warn/Error"
                ),
            ));
        }
    })
}

fn class_path(store: Option<&Ident>) -> syn::Result<TokenStream2> {
    let Some(store) = store else {
        return Ok(quote!(::dduroc::StorageClass::Default));
    };
    Ok(match store.to_string().as_str() {
        "default" => quote!(::dduroc::StorageClass::Default),
        "critical" => quote!(::dduroc::StorageClass::Critical),
        "telemetry" => quote!(::dduroc::StorageClass::Telemetry),
        // Неизвестное имя — ошибка, а не новый класс. Раньше любой
        // идентификатор молча превращался в `StorageClass("…")`: описка в
        // `critical` давала канал с другим именем, другой политикой
        // долговечности и другим бюджетом — то есть ровно то, от чего класс
        // хранения защищает, и без единого признака. Уровень (`level:`) на
        // опечатку отказывает с самого начала; здесь была единственная
        // молчаливая ветка во всём макросе.
        other => {
            return Err(syn::Error::new(
                store.span(),
                format!(
                    "неизвестный класс хранения `{other}`: ожидались \
                     default/critical/telemetry"
                ),
            ));
        }
    })
}

fn value_type_path(value_type: &Ident) -> syn::Result<TokenStream2> {
    Ok(match value_type.to_string().as_str() {
        "f32" => quote!(::dduroc::ValueType::F32),
        "f64" => quote!(::dduroc::ValueType::F64),
        "i64" => quote!(::dduroc::ValueType::I64),
        "u64" => quote!(::dduroc::ValueType::U64),
        "bool" => quote!(::dduroc::ValueType::Bool),
        "blob" => quote!(::dduroc::ValueType::Blob),
        other => {
            return Err(syn::Error::new(
                value_type.span(),
                format!("неизвестный тип значения `{other}`: ожидались f32/f64/i64/u64/bool/blob"),
            ));
        }
    })
}

/// Rust-тип, которым метрика параметризует свою константу.
///
/// Он и определяет, что примет `sample`: у `Metric<f32>` — только `f32`.
/// У перечисления это сам сгенерированный enum, поэтому состояние чужой
/// метрики не проходит проверку типов.
fn marker_path(value_type: &Ident, states_enum: Option<&Ident>) -> syn::Result<TokenStream2> {
    if let Some(name) = states_enum {
        return Ok(quote!(#name));
    }
    Ok(match value_type.to_string().as_str() {
        "f32" => quote!(f32),
        "f64" => quote!(f64),
        "i64" => quote!(i64),
        "u64" => quote!(u64),
        "bool" => quote!(bool),
        "blob" => quote!(::dduroc::Blob),
        other => {
            return Err(syn::Error::new(
                value_type.span(),
                format!("неизвестный тип значения `{other}`: ожидались f32/f64/i64/u64/bool/blob"),
            ));
        }
    })
}

fn severity_path(severity: Option<&Ident>) -> syn::Result<TokenStream2> {
    let Some(s) = severity else {
        return Ok(quote!(::dduroc::Severity::Normal));
    };
    Ok(match s.to_string().as_str() {
        "normal" => quote!(::dduroc::Severity::Normal),
        "warn" => quote!(::dduroc::Severity::Warn),
        "alarm" => quote!(::dduroc::Severity::Alarm),
        // `critical` занято классом хранения: `store: critical` и
        // `Los = 0: critical` в одном объявлении означали бы совсем разное.
        "critical" => {
            return Err(syn::Error::new(
                s.span(),
                "важность `critical` переименована в `alarm`: слово `critical` \
                 занято классом хранения (`store: critical`), и в одном \
                 объявлении метрики они означали бы разное",
            ));
        }
        other => {
            return Err(syn::Error::new(
                s.span(),
                format!("неизвестная важность `{other}`: ожидались normal/warn/alarm"),
            ));
        }
    })
}

fn metric_kind_path(kind: Option<&Ident>, has_states: bool) -> syn::Result<TokenStream2> {
    let Some(k) = kind else {
        // Перечисление держится ступенькой по определению; всё остальное по
        // умолчанию непрерывно.
        return Ok(if has_states {
            quote!(::dduroc::MetricKind::State)
        } else {
            quote!(::dduroc::MetricKind::Gauge)
        });
    };
    let name = k.to_string();
    if has_states && name != "state" {
        return Err(syn::Error::new(
            k.span(),
            format!(
                "метрика объявляет `states:`, значит её вид — state, а не `{name}`: \
                 непрерывной величиной график соединил бы состояния прямой, \
                 показав значения, которых не было"
            ),
        ));
    }
    Ok(match name.as_str() {
        "gauge" => quote!(::dduroc::MetricKind::Gauge),
        "state" => quote!(::dduroc::MetricKind::State),
        "counter" => quote!(::dduroc::MetricKind::Counter),
        other => {
            return Err(syn::Error::new(
                k.span(),
                format!("неизвестный вид метрики `{other}`: ожидались gauge/state/counter"),
            ));
        }
    })
}

/// Числовое значение границы диапазона: литерал, возможно со знаком минус.
fn bound_value(expr: &syn::Expr) -> syn::Result<f64> {
    use syn::{Expr, Lit, UnOp};
    match expr {
        Expr::Lit(l) => match &l.lit {
            Lit::Float(f) => f.base10_parse::<f64>(),
            Lit::Int(i) => i.base10_parse::<f64>(),
            other => Err(syn::Error::new_spanned(
                other,
                "граница диапазона обязана быть числом",
            )),
        },
        Expr::Unary(u) if matches!(u.op, UnOp::Neg(_)) => Ok(-bound_value(&u.expr)?),
        Expr::Group(g) => bound_value(&g.expr),
        Expr::Paren(p) => bound_value(&p.expr),
        other => Err(syn::Error::new_spanned(
            other,
            "граница диапазона обязана быть числовым литералом",
        )),
    }
}

/// Собрать [`dduroc::Range`] из диапазона Rust.
///
/// Верхняя граница требует `..=`: она **включительная**, и позволить писать
/// `..70.0` значило бы тихо переопределить смысл общеизвестного синтаксиса.
fn range_tokens(range: Option<&syn::ExprRange>) -> syn::Result<TokenStream2> {
    let Some(r) = range else {
        return Ok(quote!(::dduroc::Range {
            min: None,
            max: None
        }));
    };
    let min = match &r.start {
        Some(e) => {
            let v = bound_value(e)?;
            quote!(Some(#v))
        }
        None => quote!(None),
    };
    let max = match &r.end {
        Some(e) => {
            if matches!(r.limits, syn::RangeLimits::HalfOpen(_)) {
                return Err(syn::Error::new_spanned(
                    r,
                    "верхняя граница предела включительная — пишите `..=`: \
                     значение, равное границе, ещё нормально",
                ));
            }
            let v = bound_value(e)?;
            quote!(Some(#v))
        }
        None => quote!(None),
    };
    Ok(quote!(::dduroc::Range { min: #min, max: #max }))
}

/// Найти идентификатор объявленного типа по его имени.
///
/// Опечатка в списке затронутых типов обязана быть ошибкой компиляции: иначе
/// шаг миграции тихо не нашёл бы свой тип и обошёл бы стороной ровно те
/// сегменты, ради которых написан.
fn lookup_id<'a>(
    name: &Ident,
    mut declared: impl Iterator<Item = (&'a Ident, u16)>,
    what: &str,
) -> syn::Result<u16> {
    declared
        .find(|(n, _)| *n == name)
        .map(|(_, id)| id)
        .ok_or_else(|| {
            syn::Error::new(
                name.span(),
                format!("шаг миграции называет {what} `{name}`, которого нет в схеме"),
            )
        })
}

/// Проверить уникальность идентификаторов.
fn check_unique(kind: &str, items: &[(u16, Ident)]) -> syn::Result<()> {
    let mut seen: HashMap<u16, &Ident> = HashMap::new();
    for (id, name) in items {
        if let Some(prev) = seen.insert(*id, name) {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "{kind} id {id:#x} уже занят `{prev}` — идентификаторы обязаны быть уникальны"
                ),
            ));
        }
    }
    Ok(())
}

fn codegen(input: &SchemaInput) -> syn::Result<TokenStream2> {
    // Дескрипторы укладываются по возрастанию идентификаторов: поиск по
    // схеме идёт бинарно и выполняется на каждую запись. Порядок в
    // объявлении при этом остаётся свободным — это забота макроса.
    let mut input = SchemaInput {
        name: input.name.clone(),
        version: input.version,
        languages: input.languages.clone(),
        events: input.events.iter().map(clone_event).collect(),
        metrics: input.metrics.iter().map(clone_metric).collect(),
        spans: input.spans.iter().map(clone_span).collect(),
        migrations: input.migrations.iter().map(clone_migration).collect(),
        history: input.history.iter().map(clone_history).collect(),
    };
    input.events.sort_by_key(|e| e.id);
    input.metrics.sort_by_key(|m| m.id);
    input.spans.sort_by_key(|s| s.id);
    let input = &input;

    check_unique(
        "событие",
        &input
            .events
            .iter()
            .map(|e| (e.id, e.name.clone()))
            .collect::<Vec<_>>(),
    )?;
    check_unique(
        "метрика",
        &input
            .metrics
            .iter()
            .map(|m| (m.id, m.name.clone()))
            .collect::<Vec<_>>(),
    )?;
    check_unique(
        "спан",
        &input
            .spans
            .iter()
            .map(|s| (s.id, s.name.clone()))
            .collect::<Vec<_>>(),
    )?;

    let schema_name = input.name.to_string();
    let version = input.version;
    let lang_strs: Vec<String> = input.languages.iter().map(|l| l.to_string()).collect();

    // ── события ──────────────────────────────────────────────────────────
    let mut event_structs = Vec::new();
    let mut event_descs = Vec::new();

    for ev in &input.events {
        let name = &ev.name;
        let name_str = name.to_string();
        let id = ev.id;
        let level = level_path(&ev.level)?;
        let class = class_path(ev.store.as_ref())?;
        let tags: Vec<String> = ev.tags.iter().map(|t| t.to_string()).collect();

        let field_decls: Vec<TokenStream2> =
            ev.fields.iter().map(|(n, t)| quote!(pub #n: #t)).collect();
        // Событие без полей — unit-структура, а не пустые фигурные скобки:
        // `ns.log(events::Started)` вместо `ns.log(events::Started {})`. На
        // проводе разницы нет — postcard кодирует и то, и другое нулём байт.
        let struct_decl = if ev.fields.is_empty() {
            quote!(pub struct #name;)
        } else {
            quote!(pub struct #name { #(#field_decls,)* })
        };
        let field_descs: Vec<TokenStream2> = ev
            .fields
            .iter()
            .map(|(n, t)| {
                let n_str = n.to_string();
                let t_str = quote!(#t).to_string();
                quote!(::dduroc::FieldDesc { name: #n_str, type_name: #t_str })
            })
            .collect();

        // Шаблоны в порядке объявления языков.
        let templates: Vec<&LitStr> = input
            .languages
            .iter()
            .map(|lang| {
                &ev.templates
                    .iter()
                    .find(|(l, _)| l == lang)
                    .expect("наличие шаблона проверено при разборе")
                    .1
            })
            .collect();

        // Рендер: каждый язык — свой format! с переставленными аргументами.
        let render_arms: Vec<TokenStream2> = templates
            .iter()
            .enumerate()
            .map(|(i, tmpl)| {
                let (fmt, order) = template::rewrite(&tmpl.value());
                let args: Vec<TokenStream2> = order
                    .iter()
                    .map(|f| {
                        let id = format_ident!("{}", f);
                        quote!(value.#id)
                    })
                    .collect();
                quote!(#i => ::std::format!(#fmt #(, #args)*),)
            })
            .collect();
        let first_template = templates[0];

        let decoder_render = format_ident!("__render_{}", name);
        let decoder_json = format_ident!("__json_{}", name);

        event_structs.push(quote! {
            #[derive(Debug, Clone, PartialEq, ::dduroc::serde::Serialize, ::dduroc::serde::Deserialize)]
            #[serde(crate = "::dduroc::serde")]
            #struct_decl

            impl ::dduroc::Event for #name {
                const ID: ::dduroc::EventId = ::dduroc::EventId(#id);
                const LEVEL: ::dduroc::Level = #level;
                const NAME: &'static str = #name_str;
            }

            #[doc(hidden)]
            pub fn #decoder_render(bytes: &[u8], lang: usize)
                -> ::core::result::Result<::std::string::String, ::dduroc::DecodeError>
            {
                let value: #name = ::dduroc::postcard::from_bytes(bytes)
                    .map_err(|_| ::dduroc::DecodeError)?;
                let _ = &value;
                ::core::result::Result::Ok(match lang {
                    #(#render_arms)*
                    _ => ::std::string::String::from(#first_template),
                })
            }

            #[doc(hidden)]
            pub fn #decoder_json(bytes: &[u8])
                -> ::core::result::Result<::std::string::String, ::dduroc::DecodeError>
            {
                let value: #name = ::dduroc::postcard::from_bytes(bytes)
                    .map_err(|_| ::dduroc::DecodeError)?;
                ::dduroc::serde_json::to_string(&value).map_err(|_| ::dduroc::DecodeError)
            }
        });

        event_descs.push(quote! {
            ::dduroc::EventDesc {
                id: ::dduroc::EventId(#id),
                name: #name_str,
                level: #level,
                class: #class,
                tags: &[#(#tags),*],
                templates: &[#(#templates),*],
                fields: &[#(#field_descs),*],
                decoders: ::core::option::Option::Some(::dduroc::EventDecoders {
                    render: events::#decoder_render,
                    json: events::#decoder_json,
                }),
            }
        });
    }

    // ── метрики ──────────────────────────────────────────────────────────
    let mut metric_consts = Vec::new();
    let mut metric_descs = Vec::new();
    let mut metric_items = Vec::new();
    for m in &input.metrics {
        let name = &m.name;
        let name_str = name.to_string();
        let id = m.id;
        let value_type_ident = m
            .value_type
            .as_ref()
            .expect("value_type проверен при разборе");
        let value_type = value_type_path(value_type_ident)?;
        let class = class_path(m.store.as_ref())?;
        let unit = &m.unit;
        let tags: Vec<String> = m.tags.iter().map(|t| t.to_string()).collect();
        let kind = metric_kind_path(m.kind.as_ref(), !m.states.is_empty())?;
        let warn = range_tokens(m.warn.as_ref())?;
        let alarm = range_tokens(m.alarm.as_ref())?;

        // Предикаты срабатывания компилируются в обычные fn схемного модуля:
        // указатель на них лежит в дескрипторе, и читатель пользуется ими так
        // же, как диапазонами, — дамп со своей схемой раскрашивается одинаково
        // на приборе и во вьюере.
        let mut predicate = |key: &str, expr: Option<&syn::Expr>| match expr {
            None => quote!(::core::option::Option::None),
            Some(expr) => {
                let fn_name = quote::format_ident!("__{}_{}", key, name_str.to_lowercase());
                // Двустороннее условие естественно пишется `v > a || v < b`,
                // а clippy предлагает переписать его диапазоном — ровно той
                // формой, вместо которой предикат и выбран.
                metric_items.push(quote! {
                    #[allow(clippy::manual_range_contains)]
                    fn #fn_name(v: f64) -> bool {
                        #expr
                    }
                });
                quote!(::core::option::Option::Some(#fn_name))
            }
        };
        let warn_if = predicate("warn_if", m.warn_if.as_ref());
        let alarm_if = predicate("alarm_if", m.alarm_if.as_ref());

        // Константа несёт тип значения: `Metric<f32>` не даст записать в эту
        // метрику целое, а `Metric<LinkState>` — состояние чужой метрики.
        // Имя занимает пространство значений, поэтому перечисление состояний
        // может называться так же.
        let marker = marker_path(
            value_type_ident,
            if m.states.is_empty() {
                None
            } else {
                Some(name)
            },
        )?;
        metric_consts.push(quote! {
            pub const #name: ::dduroc::Metric<#marker> =
                ::dduroc::Metric::new(::dduroc::MetricId(#id));
        });

        // Статика подписей состояний: имена и важность живут в схеме, на диск
        // уходит только код.
        let states_ref = if m.states.is_empty() {
            quote!(&[])
        } else {
            let states_static = quote::format_ident!("STATES_{}", name_str.to_uppercase());
            let entries: Vec<TokenStream2> = m
                .states
                .iter()
                .map(|s| {
                    let code = s.code;
                    let sname = s.name.to_string();
                    let sev = severity_path(s.severity.as_ref())?;
                    Ok(quote! {
                        ::dduroc::StateDesc { code: #code, name: #sname, severity: #sev }
                    })
                })
                .collect::<syn::Result<_>>()?;
            metric_items.push(quote! {
                static #states_static: &[::dduroc::StateDesc] = &[#(#entries),*];
            });

            // Rust-тип перечисления, чтобы на месте вызова стояло
            // `metrics::LinkState::Lock`, а не голое число. Константа с тем же
            // именем не конфликтует: у значений и типов разные пространства имён.
            let variants: Vec<TokenStream2> = m
                .states
                .iter()
                .map(|s| {
                    let v = &s.name;
                    let code = s.code;
                    quote!(#v = #code)
                })
                .collect();
            let name_arms: Vec<TokenStream2> = m
                .states
                .iter()
                .map(|s| {
                    let v = &s.name;
                    let sname = s.name.to_string();
                    quote!(Self::#v => #sname)
                })
                .collect();
            metric_consts.push(quote! {
                #[doc = concat!("Состояния метрики `", #name_str, "`.")]
                #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
                #[repr(u64)]
                pub enum #name {
                    #(#variants),*
                }

                impl ::dduroc::MetricState for #name {
                    fn metric() -> ::dduroc::MetricId {
                        ::dduroc::MetricId(#id)
                    }
                    fn code(self) -> u64 {
                        self as u64
                    }
                    fn name(self) -> &'static str {
                        match self {
                            #(#name_arms),*
                        }
                    }
                }

                // Значение, допустимое для этой метрики, — только её же
                // состояния. Реализация именно здесь, а не общая по всем
                // перечислениям: общая пересеклась бы со встроенными типами,
                // потому что компилятор не умеет опираться на «f32 никогда не
                // станет состоянием метрики».
                impl ::dduroc::MetricValue<#name> for #name {
                    fn into_owned(self) -> ::dduroc::OwnedValue {
                        ::dduroc::OwnedValue::U64(self as u64)
                    }
                }
            });
            quote!(#states_static)
        };

        metric_descs.push(quote! {
            ::dduroc::MetricDesc {
                id: ::dduroc::MetricId(#id),
                name: #name_str,
                value_type: #value_type,
                class: #class,
                unit: #unit,
                tags: &[#(#tags),*],
                kind: #kind,
                states: #states_ref,
                thresholds: ::dduroc::Thresholds { warn: #warn, alarm: #alarm },
                warn_if: #warn_if,
                alarm_if: #alarm_if,
            }
        });
    }

    // ── спаны ────────────────────────────────────────────────────────────
    let mut span_consts = Vec::new();
    let mut span_descs = Vec::new();
    for s in &input.spans {
        let name = &s.name;
        let name_str = name.to_string();
        let id = s.id;
        let class = class_path(s.store.as_ref())?;
        span_consts.push(quote! {
            pub const #name: ::dduroc::SpanKindId = ::dduroc::SpanKindId(#id);
        });
        span_descs.push(quote! {
            ::dduroc::SpanDesc {
                id: ::dduroc::SpanKindId(#id),
                name: #name_str,
                class: #class,
            }
        });
    }

    // ── history и миграции ───────────────────────────────────────────────
    let history_mods = codegen_history(input)?;
    let (migration_descs, migration_fns) = codegen_migrations(input)?;

    let mod_name = &input.name;

    Ok(quote! {
        #[allow(non_snake_case, non_upper_case_globals, clippy::all, unused_imports)]
        pub mod #mod_name {
            // Имена из области, где объявлена схема, видны и здесь: иначе
            // `migrations { 2 => migrate_v2 }` требовало бы писать
            // `super::migrate_v2` — функция шага лежит рядом с объявлением,
            // а раскрывается внутрь порождённого модуля.
            use super::*;

            /// Типы событий этой схемы.
            pub mod events {
                use super::*;
                #(#event_structs)*
            }

            /// Идентификаторы метрик.
            pub mod metrics {
                #(#metric_consts)*
            }

            /// Идентификаторы видов спанов.
            pub mod spans {
                #(#span_consts)*
            }

            #(#history_mods)*
            #(#migration_fns)*

            #(#metric_items)*

            static EVENTS: &[::dduroc::EventDesc] = &[#(#event_descs),*];
            static METRICS: &[::dduroc::MetricDesc] = &[#(#metric_descs),*];
            static SPANS: &[::dduroc::SpanDesc] = &[#(#span_descs),*];
            static LANGUAGES: &[::dduroc::Language] =
                &[#(::dduroc::Language(#lang_strs)),*];
            static MIGRATIONS: &[::dduroc::Migration] = &[#(#migration_descs),*];

            /// Схема неймспейса.
            pub const SCHEMA: ::dduroc::Schema = ::dduroc::Schema {
                name: #schema_name,
                version: ::dduroc::ProtocolVersion(#version),
                languages: LANGUAGES,
                events: EVENTS,
                metrics: METRICS,
                spans: SPANS,
                migrations: MIGRATIONS,
            };
        }
    })
}

/// Породить модули `v<N>` со старыми раскладками.
///
/// Каждый тип — обычная serde-структура плюс `EventShape` со **старым** id:
/// правило `v1::PowerSet` получает и то, и другое из одного объявления.
/// `Serialize` тоже выводится: тесты миграций пишут фикстуры старых версий, и
/// собирать их байты руками значило бы дублировать раскладку в тесте.
fn codegen_history(input: &SchemaInput) -> syn::Result<Vec<TokenStream2>> {
    let mut seen_versions: HashMap<u16, proc_macro2::Span> = HashMap::new();
    let mut out = Vec::new();
    for h in &input.history {
        if h.version == 0 || h.version >= input.version {
            return Err(syn::Error::new(
                h.span,
                format!(
                    "history описывает прошлое: версия {} обязана быть в 1..{} \
                     (текущая версия схемы — {})",
                    h.version, input.version, input.version
                ),
            ));
        }
        if seen_versions.insert(h.version, h.span).is_some() {
            return Err(syn::Error::new(
                h.span,
                format!("history {} объявлена дважды", h.version),
            ));
        }
        check_unique(
            "событие history",
            &h.events
                .iter()
                .map(|e| (e.id, e.name.clone()))
                .collect::<Vec<_>>(),
        )?;
        let mut names: HashMap<String, &Ident> = HashMap::new();
        for e in &h.events {
            if let Some(prev) = names.insert(e.name.to_string(), &e.name) {
                return Err(syn::Error::new(
                    e.name.span(),
                    format!("тип `{prev}` в history {} объявлен дважды", h.version),
                ));
            }
        }

        let mod_ident = format_ident!("v{}", h.version);
        let structs: Vec<TokenStream2> = h
            .events
            .iter()
            .map(|e| {
                let name = &e.name;
                let id = e.id;
                let fields: Vec<TokenStream2> =
                    e.fields.iter().map(|(n, t)| quote!(pub #n: #t)).collect();
                // Как и у текущих раскладок: без полей — unit-структура.
                let decl = if e.fields.is_empty() {
                    quote!(pub struct #name;)
                } else {
                    quote!(pub struct #name { #(#fields,)* })
                };
                quote! {
                    #[derive(
                        Debug, Clone, PartialEq,
                        ::dduroc::serde::Serialize, ::dduroc::serde::Deserialize,
                    )]
                    #[serde(crate = "::dduroc::serde")]
                    #decl

                    impl ::dduroc::EventShape for #name {
                        const SHAPE_ID: ::dduroc::EventId = ::dduroc::EventId(#id);
                    }
                }
            })
            .collect();
        let doc = format!(
            "Раскладки версии {}: их потребляют шаги миграции, текущему коду \
             они не нужны.",
            h.version
        );
        out.push(quote! {
            #[doc = #doc]
            pub mod #mod_ident {
                use super::*;
                #(#structs)*
            }
        });
    }
    Ok(out)
}

/// Разрешённое правило: старый идентификатор и тело match-ветки.
struct ResolvedRule {
    old_id: u16,
    arm: TokenStream2,
}

/// Породить дескрипторы шагов и функции-диспетчеры типизированных правил.
fn codegen_migrations(input: &SchemaInput) -> syn::Result<(Vec<TokenStream2>, Vec<TokenStream2>)> {
    // Каждая history-запись обязана быть кем-то использована: мёртвое
    // объявление — почти наверняка забытое правило, и молчать о нём значит
    // оставить историю без преобразования.
    let mut used_history: HashMap<(u16, String), bool> = input
        .history
        .iter()
        .flat_map(|h| {
            h.events
                .iter()
                .map(|e| ((h.version, e.name.to_string()), false))
        })
        .collect();

    let mut descs = Vec::new();
    let mut fns = Vec::new();
    for m in &input.migrations {
        match &m.step {
            StepDef::Raw { func, touches } => {
                descs.push(codegen_raw_step(input, m.from, func, touches)?);
            }
            StepDef::Rules(rules) => {
                let (desc, f) = codegen_rules_step(input, m.from, rules, &mut used_history)?;
                descs.push(desc);
                fns.push(f);
            }
        }
    }

    for h in &input.history {
        for e in &h.events {
            if !used_history[&(h.version, e.name.to_string())] {
                return Err(syn::Error::new(
                    e.name.span(),
                    format!(
                        "раскладка v{}::{} объявлена, но ни одно правило её не \
                         использует: либо шаг забыт, либо запись лишняя",
                        h.version, e.name
                    ),
                ));
            }
        }
    }
    Ok((descs, fns))
}

/// Сырой fn: как и раньше — сам fn плюс необязательные touches.
///
/// Затронутые типы решают, переписывать ли сегмент, И какие записи шаг
/// увидит: множества — связывающий фильтр. Не объявлены — `touches_all`:
/// шаг видит всё, а переписывается каждый сегмент. Молча пропустить историю
/// хуже, чем переписать лишнюю.
fn codegen_raw_step(
    input: &SchemaInput,
    from: u16,
    func: &syn::Path,
    touches: &Option<Touches>,
) -> syn::Result<TokenStream2> {
    let (all, events, metrics, spans) = match touches {
        None => (quote!(true), Vec::new(), Vec::new(), Vec::new()),
        Some(t) => {
            let events = t
                .events
                .iter()
                .map(|name| {
                    let id = lookup_id(
                        name,
                        input.events.iter().map(|e| (&e.name, e.id)),
                        "событие",
                    )?;
                    Ok(quote!(::dduroc::EventId(#id)))
                })
                .collect::<syn::Result<Vec<_>>>()?;
            let metrics = t
                .metrics
                .iter()
                .map(|name| {
                    let id = lookup_id(
                        name,
                        input.metrics.iter().map(|m| (&m.name, m.id)),
                        "метрику",
                    )?;
                    Ok(quote!(::dduroc::MetricId(#id)))
                })
                .collect::<syn::Result<Vec<_>>>()?;
            let spans = t
                .spans
                .iter()
                .map(|name| {
                    let id = lookup_id(name, input.spans.iter().map(|s| (&s.name, s.id)), "спан")?;
                    Ok(quote!(::dduroc::SpanKindId(#id)))
                })
                .collect::<syn::Result<Vec<_>>>()?;
            (quote!(false), events, metrics, spans)
        }
    };
    Ok(quote! {
        ::dduroc::Migration {
            from: #from,
            touches_all: #all,
            events: &[#(#events),*],
            metrics: &[#(#metrics),*],
            spans: &[#(#spans),*],
            migrate: #func,
        }
    })
}

/// Типизированные правила: диспетчер по старым id, декод старой раскладки,
/// энкод результата — всё генерируется, а затронутые типы **выводятся** из
/// ключей. Расхождению объявленного с фактическим взяться неоткуда.
fn codegen_rules_step(
    input: &SchemaInput,
    from: u16,
    rules: &[RuleDef],
    used_history: &mut HashMap<(u16, String), bool>,
) -> syn::Result<(TokenStream2, TokenStream2)> {
    if rules.is_empty() {
        return Err(syn::Error::new(
            input.name.span(),
            format!("шаг {from} пуст: ни одного правила — такой шаг ничего не делает"),
        ));
    }

    let mut event_rules: Vec<ResolvedRule> = Vec::new();
    let mut metric_rules: Vec<ResolvedRule> = Vec::new();
    let mut span_rules: Vec<ResolvedRule> = Vec::new();
    for rule in rules {
        resolve_rule(
            input,
            rule,
            &mut event_rules,
            &mut metric_rules,
            &mut span_rules,
            used_history,
        )?;
    }
    // Два правила на один старый id — неоднозначность, а не приоритет.
    // Корзины разные: пространства идентификаторов у событий, метрик и видов
    // спанов свои, и общая корзина отвергала бы законное совпадение номеров.
    for (what, list) in [
        ("событие", &event_rules),
        ("метрику", &metric_rules),
        ("вид спана", &span_rules),
    ] {
        let mut seen: HashMap<u16, ()> = HashMap::new();
        for r in list {
            if seen.insert(r.old_id, ()).is_some() {
                return Err(syn::Error::new(
                    input.name.span(),
                    format!(
                        "шаг {from}: два правила на {what} с id {:#x} — какое из них \
                         применять, неоднозначно",
                        r.old_id
                    ),
                ));
            }
        }
    }

    let fn_name = format_ident!("__migrate_from_{}", from);
    let event_ids: Vec<TokenStream2> = event_rules
        .iter()
        .map(|r| {
            let id = r.old_id;
            quote!(::dduroc::EventId(#id))
        })
        .collect();
    let metric_ids: Vec<TokenStream2> = metric_rules
        .iter()
        .map(|r| {
            let id = r.old_id;
            quote!(::dduroc::MetricId(#id))
        })
        .collect();
    let span_ids: Vec<TokenStream2> = span_rules
        .iter()
        .map(|r| {
            let id = r.old_id;
            quote!(::dduroc::SpanKindId(#id))
        })
        .collect();
    let event_arms: Vec<TokenStream2> = event_rules.iter().map(|r| r.arm.clone()).collect();
    let metric_arms: Vec<TokenStream2> = metric_rules.iter().map(|r| r.arm.clone()).collect();
    let span_arms: Vec<TokenStream2> = span_rules.iter().map(|r| r.arm.clone()).collect();

    let desc = quote! {
        ::dduroc::Migration {
            from: #from,
            touches_all: false,
            events: &[#(#event_ids),*],
            metrics: &[#(#metric_ids),*],
            spans: &[#(#span_ids),*],
            migrate: #fn_name,
        }
    };
    let f = quote! {
        #[doc(hidden)]
        pub fn #fn_name(
            __r: ::dduroc::MigrationInput<'_>,
        ) -> ::core::result::Result<
            ::core::option::Option<::dduroc::MigrationOutcome>,
            ::dduroc::DecodeError,
        > {
            if let ::core::option::Option::Some(__ev) = __r.event_id() {
                return match __ev.0 {
                    #(#event_arms)*
                    _ => ::core::result::Result::Ok(::core::option::Option::Some(
                        ::dduroc::MigrationOutcome::AsIs,
                    )),
                };
            }
            if let ::core::option::Option::Some(__m) = __r.metric_id() {
                return match __m.0 {
                    #(#metric_arms)*
                    _ => ::core::result::Result::Ok(::core::option::Option::Some(
                        ::dduroc::MigrationOutcome::AsIs,
                    )),
                };
            }
            if let ::core::option::Option::Some(__k) = __r.span_kind() {
                return match __k.0 {
                    #(#span_arms)*
                    _ => ::core::result::Result::Ok(::core::option::Option::Some(
                        ::dduroc::MigrationOutcome::AsIs,
                    )),
                };
            }
            ::core::result::Result::Ok(::core::option::Option::Some(
                ::dduroc::MigrationOutcome::AsIs,
            ))
        }
    };
    Ok((desc, f))
}

/// Разрешить одно правило: найти старый id, проверить сочетаемость ключа с
/// действием и породить ветку диспетчера.
fn resolve_rule(
    input: &SchemaInput,
    rule: &RuleDef,
    event_rules: &mut Vec<ResolvedRule>,
    metric_rules: &mut Vec<ResolvedRule>,
    span_rules: &mut Vec<ResolvedRule>,
    used_history: &mut HashMap<(u16, String), bool>,
) -> syn::Result<()> {
    let span = rule.key.span();
    let err = |msg: String| Err(syn::Error::new(span, msg));

    // Ключ → старый id + тип для декода (если раскладка известна).
    enum Kind {
        Event { decode: Option<TokenStream2> },
        Metric,
        Span,
    }
    let (old_id, kind) = match &rule.key {
        RuleKey::HistoryEvent { version, name } => {
            let Some(h) = input.history.iter().find(|h| h.version == *version) else {
                return err(format!(
                    "history версии {version} не объявлена — раскладку `v{version}::{name}` \
                     взять неоткуда"
                ));
            };
            let Some(e) = h.events.iter().find(|e| e.name == *name) else {
                return err(format!("в history {version} нет типа `{name}`"));
            };
            used_history.insert((*version, name.to_string()), true);
            let mod_ident = format_ident!("v{}", version);
            (
                e.id,
                Kind::Event {
                    decode: Some(quote!(#mod_ident::#name)),
                },
            )
        }
        RuleKey::CurrentEvent { name } => {
            let id = lookup_id(
                name,
                input.events.iter().map(|e| (&e.name, e.id)),
                "событие",
            )?;
            (
                id,
                Kind::Event {
                    decode: Some(quote!(events::#name)),
                },
            )
        }
        RuleKey::RawEvent { id, .. } => (*id, Kind::Event { decode: None }),
        RuleKey::CurrentMetric { name } => (
            lookup_id(
                name,
                input.metrics.iter().map(|m| (&m.name, m.id)),
                "метрику",
            )?,
            Kind::Metric,
        ),
        RuleKey::RawMetric { id, .. } => (*id, Kind::Metric),
        RuleKey::CurrentSpan { name } => (
            lookup_id(name, input.spans.iter().map(|s| (&s.name, s.id)), "спан")?,
            Kind::Span,
        ),
        RuleKey::RawSpan { id, .. } => (*id, Kind::Span),
    };

    match (kind, &rule.action) {
        (Kind::Event { .. }, RuleAction::Drop) => {
            event_rules.push(ResolvedRule {
                old_id,
                arm: quote!(#old_id => ::core::result::Result::Ok(::core::option::Option::None),),
            });
        }
        (Kind::Event { decode: Some(ty) }, RuleAction::Map(expr)) => {
            event_rules.push(ResolvedRule {
                old_id,
                // Вызов через хелпер, а не `(#expr)(__old)`: тело замыкания
                // проверяется раньше, чем немедленный вызов подсказал бы тип
                // параметра, и `|old| old.dbm` не компилировалось бы.
                arm: quote! {
                    #old_id => {
                        let __old: #ty = __r.decode()?;
                        ::dduroc::__migrate_map(#expr, __old)
                    }
                },
            });
        }
        (Kind::Event { decode: None }, RuleAction::Map(_)) => {
            return err(format!(
                "у `event({old_id:#x})` нет раскладки — декодировать нечем: объявите тип \
                 в history и напишите `v<N>::Тип`, либо используйте drop или ремап"
            ));
        }
        (Kind::Event { .. }, RuleAction::RemapEvent(target)) => {
            let target_id = lookup_id(
                target,
                input.events.iter().map(|e| (&e.name, e.id)),
                "событие",
            )?;
            event_rules.push(ResolvedRule {
                old_id,
                arm: quote! {
                    #old_id => ::core::result::Result::Ok(::core::option::Option::Some(
                        ::dduroc::MigrationOutcome::Message {
                            event: ::dduroc::EventId(#target_id),
                            payload: __r.payload().unwrap_or(&[]).to_vec(),
                        },
                    )),
                },
            });
        }
        (Kind::Event { .. }, RuleAction::RemapMetric(_)) => {
            return err("событие не превратить в метрику: у них разные виды записей".to_owned());
        }
        (Kind::Metric, RuleAction::Drop) => {
            metric_rules.push(ResolvedRule {
                old_id,
                arm: quote!(#old_id => ::core::result::Result::Ok(::core::option::Option::None),),
            });
        }
        (Kind::Metric, RuleAction::RemapMetric(target)) => {
            let target_id = lookup_id(
                target,
                input.metrics.iter().map(|m| (&m.name, m.id)),
                "метрику",
            )?;
            metric_rules.push(ResolvedRule {
                old_id,
                arm: quote! {
                    #old_id => ::core::result::Result::Ok(::core::option::Option::Some(
                        ::dduroc::MigrationOutcome::SampleMetric(::dduroc::MetricId(#target_id)),
                    )),
                },
            });
        }
        (Kind::Metric, RuleAction::Map(expr)) => {
            metric_rules.push(ResolvedRule {
                old_id,
                // Тип значения объявляет само замыкание: значение на диске
                // самоописуемо, и `history` метрике не нужна. Ремап id
                // отдельным правилом — здесь метрика остаётся своей.
                arm: quote! {
                    #old_id => {
                        let __v = __r.value().ok_or(::dduroc::DecodeError)?;
                        ::dduroc::__migrate_value(#expr, __v, ::dduroc::MetricId(#old_id))
                    }
                },
            });
        }
        (Kind::Metric, RuleAction::RemapEvent(_)) => {
            return err("метрику не превратить в событие: у них разные виды записей".to_owned());
        }
        (Kind::Metric, RuleAction::RemapSpan(_)) => {
            return err("метрику не превратить в спан: у них разные виды записей".to_owned());
        }
        (Kind::Event { .. }, RuleAction::RemapSpan(_)) => {
            return err("событие не превратить в спан: у них разные виды записей".to_owned());
        }
        (Kind::Span, RuleAction::RemapSpan(target)) => {
            let target_id = lookup_id(target, input.spans.iter().map(|s| (&s.name, s.id)), "спан")?;
            span_rules.push(ResolvedRule {
                old_id,
                arm: quote! {
                    #old_id => ::core::result::Result::Ok(::core::option::Option::Some(
                        ::dduroc::MigrationOutcome::SpanKind(::dduroc::SpanKindId(#target_id)),
                    )),
                },
            });
        }
        (Kind::Span, RuleAction::Drop) => {
            return err(
                "начало спана не удаляется: на него ссылаются его конец, его \
                 сообщения и его дочерние спаны, а переписать эти ссылки цепочке \
                 нечем — остались бы висячие. Виду спана доступен только ремап \
                 (`spans::Другой`)"
                    .to_owned(),
            );
        }
        (Kind::Span, RuleAction::Map(_)) => {
            return err(
                "у спана нет полей: он несёт только вид, номер и родителя, и \
                 декодировать в нём нечего. Доступен ремап вида (`spans::Другой`)"
                    .to_owned(),
            );
        }
        (Kind::Span, RuleAction::RemapEvent(_) | RuleAction::RemapMetric(_)) => {
            return err(
                "спан не превратить в событие или метрику: у них разные виды записей".to_owned(),
            );
        }
    }
    Ok(())
}

fn clone_event(e: &EventDef) -> EventDef {
    EventDef {
        name: e.name.clone(),
        id: e.id,
        level: e.level.clone(),
        store: e.store.clone(),
        tags: e.tags.clone(),
        templates: e.templates.clone(),
        fields: e.fields.clone(),
    }
}

fn clone_metric(m: &MetricDef) -> MetricDef {
    MetricDef {
        name: m.name.clone(),
        id: m.id,
        value_type: m.value_type.clone(),
        unit: m.unit.clone(),
        tags: m.tags.clone(),
        store: m.store.clone(),
        kind: m.kind.clone(),
        states: m.states.clone(),
        warn: m.warn.clone(),
        alarm: m.alarm.clone(),
        warn_if: m.warn_if.clone(),
        alarm_if: m.alarm_if.clone(),
    }
}

fn clone_span(s: &SpanDef) -> SpanDef {
    SpanDef {
        name: s.name.clone(),
        id: s.id,
        store: s.store.clone(),
    }
}

fn clone_migration(m: &MigrationDef) -> MigrationDef {
    MigrationDef {
        from: m.from,
        step: match &m.step {
            StepDef::Raw { func, touches } => StepDef::Raw {
                func: func.clone(),
                touches: touches.clone(),
            },
            StepDef::Rules(rules) => StepDef::Rules(
                rules
                    .iter()
                    .map(|r| RuleDef {
                        key: match &r.key {
                            RuleKey::HistoryEvent { version, name } => RuleKey::HistoryEvent {
                                version: *version,
                                name: name.clone(),
                            },
                            RuleKey::CurrentEvent { name } => {
                                RuleKey::CurrentEvent { name: name.clone() }
                            }
                            RuleKey::RawEvent { id, span } => RuleKey::RawEvent {
                                id: *id,
                                span: *span,
                            },
                            RuleKey::CurrentMetric { name } => {
                                RuleKey::CurrentMetric { name: name.clone() }
                            }
                            RuleKey::RawMetric { id, span } => RuleKey::RawMetric {
                                id: *id,
                                span: *span,
                            },
                            RuleKey::CurrentSpan { name } => {
                                RuleKey::CurrentSpan { name: name.clone() }
                            }
                            RuleKey::RawSpan { id, span } => RuleKey::RawSpan {
                                id: *id,
                                span: *span,
                            },
                        },
                        action: match &r.action {
                            RuleAction::Drop => RuleAction::Drop,
                            RuleAction::Map(e) => RuleAction::Map(e.clone()),
                            RuleAction::RemapEvent(i) => RuleAction::RemapEvent(i.clone()),
                            RuleAction::RemapMetric(i) => RuleAction::RemapMetric(i.clone()),
                            RuleAction::RemapSpan(i) => RuleAction::RemapSpan(i.clone()),
                        },
                    })
                    .collect(),
            ),
        },
    }
}

fn clone_history(h: &HistoryDef) -> HistoryDef {
    HistoryDef {
        version: h.version,
        span: h.span,
        events: h
            .events
            .iter()
            .map(|e| HistoryEvent {
                name: e.name.clone(),
                id: e.id,
                fields: e.fields.clone(),
            })
            .collect(),
    }
}

/// Объявить схему неймспейса. См. документацию модуля.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as SchemaInput);
    match codegen(&parsed) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Разобрать и, если разбор прошёл, сгенерировать код: часть диагностик
    /// живёт в разборе, часть — в кодогенерации, а пользователю разницы нет.
    fn check(src: &str) -> Result<(), String> {
        let parsed: SchemaInput = syn::parse_str(src).map_err(|e| e.to_string())?;
        codegen(&parsed).map(|_| ()).map_err(|e| e.to_string())
    }

    fn err(src: &str) -> String {
        check(src).expect_err("объявление обязано быть отвергнуто")
    }

    const GOOD: &str = r#"
        name: radio, version: 1, languages: [en, ru],
        events { PowerSet = 0x01 { level: Info, en: "power {dbm}", ru: "мощность {dbm}", dbm: f32 } }
        metrics { Temp = 0x01 { type: f32 } }
        spans { Cal = 0x01 }
    "#;

    #[test]
    fn a_correct_declaration_compiles() {
        check(GOOD).expect("образцовое объявление");
    }

    #[test]
    fn sections_may_come_in_any_order() {
        // Порядок секций — дело вкуса пишущего, а не требование макроса.
        // Раньше `events` выше `languages` разбирался с пустым списком
        // языков, шаблон `en: "…"` принимался за поле типа `"…"`, и
        // пользователь получал жалобу на неразобранный тип вместо внятного
        // сообщения о своей схеме.
        check(
            r#"
            events { PowerSet = 0x01 { level: Info, en: "power {dbm}", ru: "мощность {dbm}", dbm: f32 } }
            languages: [en, ru],
            version: 1,
            name: radio,
        "#,
        )
        .expect("секции в любом порядке");
    }

    #[test]
    fn an_unknown_storage_class_is_refused() {
        // Единственная молчаливая ветка макроса: описка в `critical` давала
        // канал с другим именем, другой политикой долговечности и другим
        // бюджетом — без единого признака, что что-то не так.
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, store: critcal, en: "x" } }"#);
        assert!(
            e.contains("critcal"),
            "сказано, что именно не опознано: {e}"
        );
        assert!(e.contains("critical"), "и что ожидалось: {e}");
    }

    #[test]
    fn a_template_for_an_undeclared_language_is_refused() {
        // Перевод, который никогда не будет показан, — это молчание там, где
        // пишущий уверен в обратном.
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "x", ru: "х" } }"#);
        assert!(e.contains("ru"), "названо, какой шаблон лишний: {e}");
    }

    #[test]
    fn a_missing_template_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en, ru],
               events { Boom = 0x01 { level: Error, en: "x" } }"#);
        assert!(e.contains("ru"), "названо, какого языка не хватает: {e}");
    }

    #[test]
    fn a_placeholder_without_a_field_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "перегрев {t}" } }"#);
        // Проверять одну букву нельзя: если признак «шаблон — строковый
        // литерал» сломается, `en: "…"` снова уедет в поля, syn пожалуется
        // «expected type», и латинская `t` найдётся уже там.
        assert!(
            e.contains("{t}") && e.contains("такого поля нет"),
            "жалоба — на шаблон, а не на неразобранный тип: {e}"
        );
    }

    #[test]
    fn a_field_named_like_a_language_is_still_a_field() {
        // Обратное направление того же признака: ключ совпал с языком, но
        // значение — тип, значит это поле. Различать по списку языков нельзя,
        // и по имени ключа — тоже.
        check(
            r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "код {ru}", ru: u8 } }"#,
        )
        .expect("поле с именем языка — это поле");
    }

    #[test]
    fn an_event_without_fields_is_a_unit_struct() {
        // `ns.log(events::Started)` — писать `Started {}` ради структуры,
        // у которой нечего заполнять, значит платить синтаксисом за пустоту.
        let src = r#"name: radio, version: 1, languages: [en],
               events {
                   Started = 0x01 { level: Info, en: "started" },
                   PowerSet = 0x02 { level: Info, en: "power {dbm}", dbm: f32 }
               }"#;
        let parsed: SchemaInput = syn::parse_str(src).expect("образцовое объявление");
        let out = codegen(&parsed).expect("кодогенерация").to_string();
        assert!(
            out.contains("pub struct Started ;"),
            "событие без полей обязано быть unit-структурой: {out}"
        );
        assert!(
            out.contains("pub struct PowerSet { pub dbm : f32 , }"),
            "событие с полями остаётся структурой с полями: {out}"
        );
    }

    #[test]
    fn an_unknown_level_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Fatal, en: "x" } }"#);
        assert!(e.contains("Fatal"), "{e}");
    }

    #[test]
    fn a_missing_level_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { en: "x" } }"#);
        assert!(e.contains("level"), "{e}");
    }

    #[test]
    fn an_unknown_section_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en], metrix { }"#);
        assert!(e.contains("metrix"), "{e}");
    }

    #[test]
    fn a_schema_without_languages_is_refused() {
        let e = err(r#"name: radio, version: 1, events { }"#);
        assert!(e.contains("languages"), "{e}");
    }

    #[test]
    fn a_duplicate_template_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "x", en: "y" } }"#);
        assert!(e.contains("дважды"), "{e}");
    }

    #[test]
    fn an_identifier_too_large_for_u16_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 70000 { level: Error, en: "x" } }"#);
        assert!(e.contains("u16"), "{e}");
    }

    #[test]
    fn the_new_metric_forms_parse() {
        // Предикаты срабатывания и префиксная важность состояний.
        check(
            r#"name: radio, version: 1, languages: [en],
               metrics {
                   Vswr = 0x01 { type: f32, warn_if: v > 1.5,
                                 alarm_if: v > 3.0 || v < 1.0 },
                   Link = 0x02 { states: [alarm Los = 0, warn Sync = 1, Lock = 2] },
               }"#,
        )
        .expect("новые формы компилируются");
    }

    #[test]
    fn old_metric_spellings_get_pointed_at_the_new_ones() {
        // Прежнее имя ключа — подсказка, а не «неизвестный ключ».
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Temp = 0x01 { value_type: f32 } }"#);
        assert!(e.contains("`type`"), "{e}");

        // Условие на месте диапазона: у форм противоположная полярность,
        // и об этом сказано прямо.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Temp = 0x01 { type: f32, warn: v > 70.0 } }"#);
        assert!(e.contains("warn_if"), "{e}");
        assert!(e.contains("НОРМЫ"), "{e}");

        // Суффиксная важность состояния переехала в префикс.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Link = 0x01 { states: [Los = 0: alarm] } }"#);
        assert!(e.contains("префиксом"), "{e}");
        assert!(e.contains("alarm Los = 0"), "{e}");
    }

    #[test]
    fn a_range_and_a_predicate_of_one_level_are_mutually_exclusive() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Temp = 0x01 { type: f32, warn: ..=70.0, warn_if: v > 70.0 } }"#);
        assert!(e.contains("одну форму"), "{e}");
    }

    #[test]
    fn predicates_are_refused_where_a_number_never_comes() {
        // У перечисления важность носят состояния.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Link = 0x01 { states: [Lock = 0], warn_if: v > 1.0 } }"#);
        assert!(e.contains("состояни"), "{e}");

        // Blob к числу не приводится.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Spec = 0x01 { type: blob, alarm_if: v > 1.0 } }"#);
        assert!(e.contains("blob"), "{e}");
    }

    // ── history и типизированные правила ─────────────────────────────────

    /// Заготовка схемы v2 с history и одним типизированным шагом.
    const MIGRATING: &str = r#"
        name: radio, version: 2, languages: [en],
        events { PowerSet = 0x01 { level: Info, en: "power {dbm}", dbm: f32 } }
        metrics { Temp = 0x01 { type: f32 }, TempPa = 0x02 { type: f32 } }
        history { 1 { events { PowerSet = 0x01 { dbm: i8 } } } }
        migrations {
            1 => {
                v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) },
                event(0x05): drop,
                metric(0x07): metrics::TempPa,
            },
        }
    "#;

    #[test]
    fn history_with_typed_rules_compiles() {
        check(MIGRATING).expect("образцовая миграция");
    }

    #[test]
    fn a_history_version_not_in_the_past_is_refused() {
        // history описывает прошлое: текущая версия и версии из будущего в
        // ней бессмысленны, а ноль не существует — нумерация с единицы.
        for v in ["2", "3", "0"] {
            let e = err(&format!(
                r#"name: radio, version: 2, languages: [en],
                   events {{ E = 0x01 {{ level: Info, en: "x" }} }}
                   history {{ {v} {{ events {{ E = 0x01 {{ }} }} }} }}
                   migrations {{ 1 => {{ v{v}::E: drop }} }}"#
            ));
            assert!(e.contains("history"), "{v}: {e}");
        }
    }

    #[test]
    fn an_unused_history_entry_is_refused() {
        // Мёртвая раскладка — почти наверняка забытое правило: молчать о ней
        // значит оставить историю без преобразования.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations { 1 => { event(0x09): drop } }"#);
        assert!(e.contains("не") && e.contains("использует"), "{e}");
    }

    #[test]
    fn a_rule_for_an_undeclared_history_version_is_refused() {
        let e = err(r#"name: radio, version: 3, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations {
                   1 => { v1::E: drop },
                   2 => { v2::E: drop },
               }"#);
        assert!(e.contains("v2") || e.contains("версии 2"), "{e}");
    }

    #[test]
    fn a_rule_for_a_type_missing_from_history_is_refused() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations { 1 => { v1::Ghost: drop, v1::E: drop } }"#);
        assert!(e.contains("Ghost"), "{e}");
    }

    #[test]
    fn a_raw_id_cannot_be_mapped_because_it_has_no_shape() {
        // У голого id нет раскладки — декодировать нечем. Подсказка обязана
        // называть выход: объявить тип в history.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               migrations { 1 => { event(0x05): |old| old } }"#);
        assert!(e.contains("history"), "{e}");
    }

    #[test]
    fn metric_values_are_mapped_by_a_closure_that_names_its_type() {
        // Значение отсчёта самоописуемо — тип лежит в самой записи, — поэтому
        // правилу не нужна ни history, ни объявление прежнего типа: тип
        // называет параметр замыкания, он же говорит, что ожидалось на диске.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { metrics::Temp: |v: f32| v * 10.0 } }"#,
        )
        .expect("преобразование значения — законное правило");

        // И по голому id: у значения раскладку брать неоткуда, она на диске.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { metric(0x07): |v: i64| v as f64 } }"#,
        )
        .expect("голый id метрики годится: тип значения на диске");
    }

    #[test]
    fn span_kinds_are_renamed_but_never_deleted() {
        // Ремап вида — законное правило.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01, Calibration = 0x02 }
               migrations { 1 => { spans::Calib: spans::Calibration } }"#,
        )
        .expect("переименование вида спана");

        // И по голому id: вид мог исчезнуть из схемы вместе с именем.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calibration = 0x02 }
               migrations { 1 => { span(0x01): spans::Calibration } }"#,
        )
        .expect("голый id вида спана");

        // А вот удаление — нет: на начало спана ссылаются его конец, его
        // сообщения и его дети, и переписать эти ссылки цепочке нечем.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01 }
               migrations { 1 => { spans::Calib: drop } }"#);
        assert!(e.contains("висячие"), "{e}");

        // Замыкание тоже нет: декодировать у спана нечего.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01 }
               migrations { 1 => { spans::Calib: |s| s } }"#);
        assert!(e.contains("нет полей"), "{e}");
    }

    #[test]
    fn span_kinds_have_their_own_id_space_in_rules() {
        // Номера видов спанов, событий и метрик живут в разных пространствах:
        // общая корзина проверок отвергала бы законное совпадение номеров.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               spans { Calib = 0x01, Calibration = 0x02 }
               migrations { 1 => {
                   events::E: drop,
                   metrics::Temp: drop,
                   spans::Calib: spans::Calibration,
               } }"#,
        )
        .expect("три правила с id 0x01 в трёх пространствах");

        // А два правила на один вид — по-прежнему неоднозначность.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01, A = 0x02, B = 0x03 }
               migrations { 1 => { spans::Calib: spans::A, span(0x01): spans::B } }"#);
        assert!(e.contains("неоднозначно"), "{e}");
    }

    #[test]
    fn kinds_do_not_cross() {
        // Событие не превратить в метрику и наоборот: разные виды записей.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { events::E: metrics::Temp } }"#);
        assert!(e.contains("разные виды"), "{e}");

        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { metrics::Temp: events::E } }"#);
        assert!(e.contains("разные виды"), "{e}");
    }

    #[test]
    fn two_rules_for_one_old_id_are_ambiguous() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations { 1 => { v1::E: drop, events::E: drop } }"#);
        assert!(e.contains("неоднозначно"), "{e}");
    }

    #[test]
    fn an_empty_rules_step_is_refused() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               migrations { 1 => { } }"#);
        assert!(e.contains("пуст"), "{e}");
    }

    #[test]
    fn a_duplicate_history_version_is_refused() {
        let e = err(r#"name: radio, version: 3, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history {
                   1 { events { E = 0x01 { n: u8 } } }
                   1 { events { E = 0x01 { n: u16 } } }
               }
               migrations { 1 => { v1::E: drop }, 2 => { event(0x09): drop } }"#);
        assert!(e.contains("дважды"), "{e}");
    }

    #[test]
    fn history_declares_events_only() {
        // У отсчётов payload-раскладки нет — объявлять в history нечего, и
        // попытка обязана быть названа, а не молча проглочена.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { metrics { Temp = 0x01 { } } } }
               migrations { 1 => { event(0x09): drop } }"#);
        assert!(e.contains("events"), "{e}");
    }

    #[test]
    fn an_empty_history_entry_is_refused() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { } } }
               migrations { 1 => { event(0x09): drop } }"#);
        assert!(e.contains("пуста"), "{e}");
    }

    #[test]
    fn the_raw_fn_escape_hatch_still_parses() {
        // Люк остаётся люком: сырой fn с touches и без, вперемешку с
        // типизированными шагами.
        check(
            r#"name: radio, version: 4, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations {
                   1 => { v1::E: |old| events::E { } },
                   2 => migrate_v2,
                   3 => migrate_v3 { events: [E] },
               }"#,
        )
        .expect("смешанное объявление законно");
    }

    #[test]
    fn rule_keys_are_validated_syntactically() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               migrations { 1 => { foo::E: drop } }"#);
        assert!(e.contains("v<N>") || e.contains("events"), "{e}");

        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               migrations { 1 => { bucket(0x01): drop } }"#);
        assert!(e.contains("event, metric, span"), "{e}");
    }
}
