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
    vtype: Ident,
    unit: LitStr,
    tags: Vec<Ident>,
    store: Option<Ident>,
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
        } else if languages.iter().any(|l| *l == key) {
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

fn parse_metric(input: ParseStream) -> syn::Result<MetricDef> {
    let name: Ident = input.parse()?;
    let id = parse_id(input)?;
    let content;
    braced!(content in input);

    let mut vtype = None;
    let mut unit = None;
    let mut tags = Vec::new();
    let mut store = None;

    while !content.is_empty() {
        let key: Ident = content.parse()?;
        content.parse::<Token![:]>()?;
        match key.to_string().as_str() {
            "vtype" => vtype = Some(content.parse::<Ident>()?),
            "unit" => unit = Some(content.parse::<LitStr>()?),
            "tags" => tags = parse_ident_list(&content)?,
            "store" => store = Some(content.parse::<Ident>()?),
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!("у метрики неизвестный ключ `{other}`"),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }

    let vtype = vtype.ok_or_else(|| {
        syn::Error::new(name.span(), format!("у метрики `{name}` не задан `vtype:`"))
    })?;
    let unit = unit.unwrap_or_else(|| LitStr::new("", name.span()));

    Ok(MetricDef {
        name,
        id,
        vtype,
        unit,
        tags,
        store,
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
    for m in &input.metrics {
        let name = &m.name;
        let name_str = name.to_string();
        let id = m.id;
        let vtype = vtype_path(&m.vtype)?;
        let class = class_path(m.store.as_ref())?;
        let unit = &m.unit;
        let tags: Vec<String> = m.tags.iter().map(|t| t.to_string()).collect();

        metric_consts.push(quote! {
            pub const #name: ::dduroc::MetricId = ::dduroc::MetricId(#id);
        });
        metric_descs.push(quote! {
            ::dduroc::MetricDesc {
                id: ::dduroc::MetricId(#id),
                name: #name_str,
                value_type: #vtype,
                class: #class,
                unit: #unit,
                tag_keys: &[#(#tags),*],
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
        pub mod #mod_name {
            #![allow(clippy::all, unused_imports)]

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

/// Объявить схему неймспейса. См. документацию модуля.
#[proc_macro]
pub fn schema(input: TokenStream) -> TokenStream {
    let parsed = parse_macro_input!(input as SchemaInput);
    match codegen(&parsed) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
