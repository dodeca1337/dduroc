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
//!     }
//!
//!     metrics {
//!         Temp = 0x01 { vtype: f32, unit: "°C", tags: [sensor] },
//!     }
//!
//!     spans {
//!         Calibration = 0x01,
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
    vtype: Option<Ident>,
    unit: LitStr,
    tags: Vec<Ident>,
    store: Option<Ident>,
    kind: Option<Ident>,
    states: Vec<StateDef>,
    /// Диапазоны допустимых значений: вне них — соответствующая важность.
    warn: Option<syn::ExprRange>,
    alarm: Option<syn::ExprRange>,
}

/// Одно состояние метрики-перечисления: `Los = 0: alarm`.
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
    func: syn::Path,
    /// Типы, затронутые шагом. `None` — не объявлены, значит шаг считается
    /// затрагивающим **всё**: пропустить сегмент молча хуже, чем переписать
    /// лишний.
    touches: Option<Touches>,
}

/// Затронутые шагом миграции типы.
#[derive(Clone, Default)]
struct Touches {
    events: Vec<Ident>,
    metrics: Vec<Ident>,
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
                        let func: syn::Path = content.parse()?;
                        let touches = if content.peek(syn::token::Brace) {
                            Some(parse_touches(&content)?)
                        } else {
                            None
                        };
                        migrations.push(MigrationDef {
                            from: lit.base10_parse()?,
                            func,
                            touches,
                        });
                        let _ = content.parse::<Token![,]>();
                    }
                }
                other => {
                    return Err(syn::Error::new(
                        key.span(),
                        format!(
                            "неизвестная секция `{other}`: ожидались name, version, \
                             languages, events, metrics, spans, migrations"
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

/// Разобрать список состояний: `[Los = 0: alarm, Sync = 1: warn, Lock = 2]`.
///
/// Коды **обязательно** явные: позиционная нумерация сдвинулась бы при вставке
/// состояния в середину списка, и уже записанные сегменты стали бы читаться
/// неверно, без единого признака ошибки.
fn parse_states(input: ParseStream) -> syn::Result<Vec<StateDef>> {
    let content;
    bracketed!(content in input);
    let mut out = Vec::new();
    while !content.is_empty() {
        let name: Ident = content.parse()?;
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
        // Важность необязательна: без неё состояние считается нормальным.
        let severity = if content.peek(Token![:]) {
            content.parse::<Token![:]>()?;
            Some(content.parse::<Ident>()?)
        } else {
            None
        };
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

    let mut vtype = None;
    let mut unit = None;
    let mut tags = Vec::new();
    let mut store = None;
    let mut kind = None;
    let mut states = Vec::new();
    let mut warn = None;
    let mut alarm = None;

    while !content.is_empty() {
        let key: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        match key.to_string().as_str() {
            "vtype" => vtype = Some(content.parse::<Ident>()?),
            "unit" => unit = Some(content.parse::<LitStr>()?),
            "tags" => tags = parse_ident_list(&content)?,
            "store" => store = Some(content.parse::<Ident>()?),
            "kind" => kind = Some(content.parse::<Ident>()?),
            "states" => states = parse_states(&content)?,
            "warn" => warn = Some(content.parse::<syn::ExprRange>()?),
            "alarm" => alarm = Some(content.parse::<syn::ExprRange>()?),
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
                        "у метрики неизвестный ключ `{other}`: ожидались vtype, unit, \
                         tags, store, kind, states, warn, alarm"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }

    // Тип перечисления выводится: код состояния — целое.
    if vtype.is_none() && !states.is_empty() {
        vtype = Some(Ident::new("u64", name.span()));
    }
    if vtype.is_none() {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "у метрики `{name}` не задан `vtype:` (у перечисления он выводится \
                 из `states:`)"
            ),
        ));
    }
    let unit = unit.unwrap_or_else(|| LitStr::new("", name.span()));

    Ok(MetricDef {
        name,
        id,
        vtype,
        unit,
        tags,
        store,
        kind,
        states,
        warn,
        alarm,
    })
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
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "у шага миграции неизвестный ключ `{other}`: ожидались \
                         events, metrics"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }
    Ok(out)
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
        return Ok(quote!(::dduroc::StorageClass::DEFAULT));
    };
    Ok(match store.to_string().as_str() {
        "default" => quote!(::dduroc::StorageClass::DEFAULT),
        "critical" => quote!(::dduroc::StorageClass::CRITICAL),
        "telemetry" => quote!(::dduroc::StorageClass::TELEMETRY),
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

fn vtype_path(vtype: &Ident) -> syn::Result<TokenStream2> {
    Ok(match vtype.to_string().as_str() {
        "f32" => quote!(::dduroc::ValueType::F32),
        "f64" => quote!(::dduroc::ValueType::F64),
        "i64" => quote!(::dduroc::ValueType::I64),
        "u64" => quote!(::dduroc::ValueType::U64),
        "bool" => quote!(::dduroc::ValueType::Bool),
        "blob" => quote!(::dduroc::ValueType::Blob),
        other => {
            return Err(syn::Error::new(
                vtype.span(),
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
fn marker_path(vtype: &Ident, states_enum: Option<&Ident>) -> syn::Result<TokenStream2> {
    if let Some(name) = states_enum {
        return Ok(quote!(#name));
    }
    Ok(match vtype.to_string().as_str() {
        "f32" => quote!(f32),
        "f64" => quote!(f64),
        "i64" => quote!(i64),
        "u64" => quote!(u64),
        "bool" => quote!(bool),
        "blob" => quote!(::dduroc::Blob),
        other => {
            return Err(syn::Error::new(
                vtype.span(),
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
            pub struct #name {
                #(#field_decls,)*
            }

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
    let mut state_statics = Vec::new();
    for m in &input.metrics {
        let name = &m.name;
        let name_str = name.to_string();
        let id = m.id;
        let vtype_ident = m.vtype.as_ref().expect("vtype проверен при разборе");
        let vtype = vtype_path(vtype_ident)?;
        let class = class_path(m.store.as_ref())?;
        let unit = &m.unit;
        let tags: Vec<String> = m.tags.iter().map(|t| t.to_string()).collect();
        let kind = metric_kind_path(m.kind.as_ref(), !m.states.is_empty())?;
        let warn = range_tokens(m.warn.as_ref())?;
        let alarm = range_tokens(m.alarm.as_ref())?;

        // Константа несёт тип значения: `Metric<f32>` не даст записать в эту
        // метрику целое, а `Metric<LinkState>` — состояние чужой метрики.
        // Имя занимает пространство значений, поэтому перечисление состояний
        // может называться так же.
        let marker = marker_path(
            vtype_ident,
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
            state_statics.push(quote! {
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
                value_type: #vtype,
                class: #class,
                unit: #unit,
                tags: &[#(#tags),*],
                kind: #kind,
                states: #states_ref,
                thresholds: ::dduroc::Thresholds { warn: #warn, alarm: #alarm },
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

    // ── миграции ─────────────────────────────────────────────────────────
    //
    // Затронутые типы решают, переписывать ли сегмент: множества их
    // идентификаторов лежат в footer'е, и незатронутый сегмент не тратит
    // цикла записи флеша. Не объявлены — значит `touches_all`, то есть
    // переписывается всё: молча пропустить историю хуже, чем переписать её.
    let migration_descs: Vec<TokenStream2> = input
        .migrations
        .iter()
        .map(|m| {
            let from = m.from;
            let func = &m.func;
            let (all, events, metrics) = match &m.touches {
                None => (quote!(true), Vec::new(), Vec::new()),
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
                    (quote!(false), events, metrics)
                }
            };
            Ok(quote! {
                ::dduroc::Migration {
                    from: #from,
                    touches_all: #all,
                    events: &[#(#events),*],
                    metrics: &[#(#metrics),*],
                    migrate: #func,
                }
            })
        })
        .collect::<syn::Result<Vec<_>>>()?;

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

            #(#state_statics)*

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
        vtype: m.vtype.clone(),
        unit: m.unit.clone(),
        tags: m.tags.clone(),
        store: m.store.clone(),
        kind: m.kind.clone(),
        states: m.states.clone(),
        warn: m.warn.clone(),
        alarm: m.alarm.clone(),
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
        func: m.func.clone(),
        touches: m.touches.clone(),
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
        metrics { Temp = 0x01 { vtype: f32 } }
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
}
