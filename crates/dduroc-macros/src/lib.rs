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
    critical: Option<syn::ExprRange>,
}

/// Одно состояние метрики-перечисления: `Los = 0: critical`.
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
                        events.push(parse_event(&content, &languages)?);
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
                        migrations.push(MigrationDef {
                            from: lit.base10_parse()?,
                            func: content.parse()?,
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

fn parse_event(input: ParseStream, languages: &[Ident]) -> syn::Result<EventDef> {
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
        } else if languages.contains(&key) {
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

    // Шаблон обязан быть на каждом объявленном языке: недостающий всплыл бы
    // при чтении логов на языке, которого нет, — то есть в самый неудачный
    // момент.
    for lang in languages {
        if !templates.iter().any(|(l, _)| l == lang) {
            return Err(syn::Error::new(
                name.span(),
                format!("у события `{name}` нет шаблона для языка `{lang}`"),
            ));
        }
    }

    // Плейсхолдеры обязаны ссылаться на существующие поля.
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

/// Разобрать список состояний: `[Los = 0: critical, Sync = 1: warn, Lock = 2]`.
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
    let mut critical = None;

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
            "critical" => critical = Some(content.parse::<syn::ExprRange>()?),
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "у метрики неизвестный ключ `{other}`: ожидались vtype, unit, \
                         tags, store, kind, states, warn, critical"
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
        critical,
    })
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
        other => {
            let name = other.to_owned();
            quote!(::dduroc::StorageClass(#name))
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

fn severity_path(severity: Option<&Ident>) -> syn::Result<TokenStream2> {
    let Some(s) = severity else {
        return Ok(quote!(::dduroc::Severity::Normal));
    };
    Ok(match s.to_string().as_str() {
        "normal" => quote!(::dduroc::Severity::Normal),
        "warn" => quote!(::dduroc::Severity::Warn),
        "critical" => quote!(::dduroc::Severity::Critical),
        other => {
            return Err(syn::Error::new(
                s.span(),
                format!("неизвестная важность `{other}`: ожидались normal/warn/critical"),
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
        let critical = range_tokens(m.critical.as_ref())?;

        metric_consts.push(quote! {
            pub const #name: ::dduroc::MetricId = ::dduroc::MetricId(#id);
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
                thresholds: ::dduroc::Thresholds { warn: #warn, critical: #critical },
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
    let migration_descs: Vec<TokenStream2> = input
        .migrations
        .iter()
        .map(|m| {
            let from = m.from;
            let func = &m.func;
            quote! {
                ::dduroc::Migration {
                    from: #from,
                    events: &[],
                    metrics: &[],
                    migrate: #func,
                }
            }
        })
        .collect();

    let mod_name = &input.name;

    Ok(quote! {
        #[allow(non_snake_case, non_upper_case_globals, clippy::all, unused_imports)]
        pub mod #mod_name {

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
        critical: m.critical.clone(),
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
