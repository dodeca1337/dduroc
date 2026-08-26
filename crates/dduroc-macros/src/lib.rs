//! The `schema!` macro — the declaration of a namespace schema.
//!
//! It expands into event structs, descriptors and a schema constant.
//! Everything is static: the schema lies wholly in `.rodata` and costs nothing
//! at runtime.
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
//!         // With no fields it is a unit struct: `ns.log(events::Started)`.
//!         Started = 0x02 { level: Info, en: "started", ru: "запущено" },
//!     }
//!
//!     metrics {
//!         // Ranges of what is NORMAL are data: drawable, updatable at runtime.
//!         Temp = 0x01 { type: f32, unit: "°C", tags: [sensor],
//!                       warn: -40.0..=70.0, alarm: -40.0..=85.0 },
//!         // A shape a range cannot express is a TRIGGER predicate (`v` is the
//!         // value): the polarity is the opposite, hence a different key.
//!         Vswr = 0x02 { type: f32, warn_if: v > 1.5,
//!                       alarm_if: v > 3.0 || v < 1.0 },
//!         // An enum: severity goes in front; without it a state is normal.
//!         Link = 0x03 { states: [alarm Los = 0, warn Sync = 1, Lock = 2] },
//!     }
//!
//!     spans {
//!         Calibration = 0x01,
//!     }
//!
//!     // The layouts of earlier versions — what the migration steps consume.
//!     history {
//!         1 { events { PowerSet = 0x01 { dbm: i8 } } }
//!     }
//!
//!     // Step N brings records of version N up to version N+1.
//!     migrations {
//!         1 => {
//!             // A typed rule: decoding the old layout, encoding the new one and
//!             // the dispatcher are all generated, and the affected types are
//!             // inferred from the keys — there is nothing for them to drift
//!             // apart from the actual behaviour on.
//!             v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) },
//!             event(0x05): drop,               // the type is gone — so is its name
//!             metric(0x07): metrics::Temp,     // an id remap, the value as it was
//!             // A value is self-describing: the closure names the type itself,
//!             // and a metric needs no history.
//!             metrics::Vswr: |v: u64| v as f32 / 10.0,
//!             // Only a span's kind changes: the number and the parent are the
//!             // record's identity, and a rule will not let its start be deleted.
//!             span(0x07): spans::Calibration,
//!         },
//!         // The hatch: a raw fn. The affected types are declared by hand; if
//!         // they are not, the step counts as touching everything.
//!         2 => migrate_v2,
//!     }
//! }
//! ```
//!
//! Identifiers are **mandatory and explicit**. When an event was inserted into
//! the middle of a list, the prototype's positional auto-numbering silently
//! remapped historical records onto the wrong decoders.

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
    /// The templates per language, in declaration order.
    templates: Vec<(Ident, LitStr)>,
    fields: Vec<(Ident, Type)>,
}

struct MetricDef {
    name: Ident,
    id: u16,
    /// `None` means the type is inferred: for an enum that is `u64`.
    value_type: Option<Ident>,
    unit: LitStr,
    tags: Vec<Ident>,
    store: Option<Ident>,
    kind: Option<Ident>,
    states: Vec<StateDef>,
    /// The ranges of what is normal: outside them lies the matching severity.
    warn: Option<syn::ExprRange>,
    alarm: Option<syn::ExprRange>,
    /// The trigger predicates (`v` is the value): true means the level is
    /// reached.
    warn_if: Option<syn::Expr>,
    alarm_if: Option<syn::Expr>,
}

/// One state of an enum metric: `alarm Los = 0`.
#[derive(Clone)]
struct StateDef {
    name: Ident,
    code: u64,
    /// `None` means the state is normal.
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

/// The body of a migration step.
enum StepDef {
    /// A raw fn — the hatch for what the rules cannot express.
    Raw {
        func: syn::Path,
        /// The types the step affects. `None` means they were not declared, so
        /// the step counts as touching **everything**: skipping a segment
        /// silently is worse than rewriting a superfluous one.
        touches: Option<Touches>,
    },
    /// Typed rules: the macro generates the dispatcher, the decoding and the
    /// encoding, and the affected types are **inferred** from the keys — there
    /// is nothing for them to drift apart from the actual behaviour on.
    Rules(Vec<RuleDef>),
}

/// The types a migration step affects.
#[derive(Clone, Default)]
struct Touches {
    events: Vec<Ident>,
    metrics: Vec<Ident>,
    spans: Vec<Ident>,
}

/// One rule of a typed step: `key: action`.
struct RuleDef {
    key: RuleKey,
    action: RuleAction,
}

/// What a rule selects — by identifier IN THE version the step reads.
enum RuleKey {
    /// `v1::PowerSet` — a layout from history: both the old id and the type to
    /// decode with.
    HistoryEvent { version: u16, name: Ident },
    /// `events::PowerSet` — the current layout (scrubbing values, drop).
    CurrentEvent { name: Ident },
    /// `event(0x05)` — a bare id: the type is gone from the schema and so is
    /// its name.
    RawEvent { id: u16, span: proc_macro2::Span },
    /// `metrics::Temp` — a current metric.
    CurrentMetric { name: Ident },
    /// `metric(0x07)` — a metric's bare id.
    RawMetric { id: u16, span: proc_macro2::Span },
    /// `spans::Calibration` — a current span kind.
    CurrentSpan { name: Ident },
    /// `span(0x03)` — a kind's bare id: the kind is gone from the schema and so
    /// is its name.
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

/// What a rule does with a record.
enum RuleAction {
    /// `drop` — the record is deleted.
    Drop,
    /// A closure or a function: decode the old layout, produce a new payload.
    Map(syn::Expr),
    /// `events::New` — change the id, the payload byte for byte.
    RemapEvent(Ident),
    /// `metrics::New` — change a sample's metric.
    RemapMetric(Ident),
    /// `spans::New` — rename a span kind.
    RemapSpan(Ident),
}

/// The layouts of changed types as they were in version `version` — what the
/// migration steps consume. Produces a `pub mod v<N>` of Deserialize types.
struct HistoryDef {
    version: u16,
    span: proc_macro2::Span,
    events: Vec<HistoryEvent>,
}

/// An old event: the id and the fields only — the level, the templates and the
/// tags do not live on disk, and an old layout has no need of them.
struct HistoryEvent {
    name: Ident,
    id: u16,
    fields: Vec<(Ident, Type)>,
}

// ════════════════════════════════════════════════════════════════════════════
// Parsing
// ════════════════════════════════════════════════════════════════════════════

fn parse_id(input: ParseStream) -> syn::Result<u16> {
    input.parse::<Token![=]>()?;
    let lit: LitInt = input.parse()?;
    lit.base10_parse::<u16>().map_err(|e| {
        syn::Error::new(
            lit.span(),
            format!("the identifier does not fit in a u16: {e}"),
        )
    })
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
                        // Braces right after `=>` mean typed rules; otherwise
                        // it is the path to a raw fn with optional touches.
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
                            "unknown section `{other}`: expected name, version, \
                             languages, events, metrics, spans, history, migrations"
                        ),
                    ));
                }
            }
        }

        let name = name.ok_or_else(|| syn::Error::new(input.span(), "`name:` is not set"))?;
        let version =
            version.ok_or_else(|| syn::Error::new(name.span(), "`version:` is not set"))?;
        if languages.is_empty() {
            return Err(syn::Error::new(
                name.span(),
                "`languages:` is not set — there would be nothing to render messages with",
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
    /// Check the event templates against the languages declared.
    ///
    /// Done after the whole declaration is parsed rather than along the way:
    /// the sections need not come in any particular order, and until parsing
    /// ends the list of languages is unknown.
    fn check_templates(events: &[EventDef], languages: &[Ident]) -> syn::Result<()> {
        for ev in events {
            // A template is required in every language declared: a missing one
            // would surface when reading logs in a language that is not there —
            // that is, at the worst possible moment.
            for lang in languages {
                if !ev.templates.iter().any(|(l, _)| l == lang) {
                    return Err(syn::Error::new(
                        ev.name.span(),
                        format!("event `{}` has no template for language `{lang}`", ev.name),
                    ));
                }
            }
            // And the other way round: a template in a language absent from
            // `languages:` will never be shown. Staying silent would mean
            // leaving a translation the user believes is working.
            for (lang, _) in &ev.templates {
                if !languages.contains(lang) {
                    let declared: Vec<String> = languages.iter().map(|l| l.to_string()).collect();
                    return Err(syn::Error::new(
                        lang.span(),
                        format!(
                            "event `{}` has a template in language `{lang}`, but `languages:` \
                             declares only {}",
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

/// Parse an event declaration.
///
/// The list of languages is **not** passed in here: the sections of a
/// declaration need not come in any particular order, and `events` above
/// `languages` is a legitimate way to write it. A template used to be told from
/// a field by whether its key was in the list of languages, so under that order
/// `en: "мощность {dbm}"` parsed as a field of type `"мощность {dbm}"`, and the
/// user got an error about an unparsed type instead of a clear message about
/// their schema.
///
/// The sign is now syntactic: a template's value is a string literal, a field's
/// value is a type, and a type cannot be a string literal. Whether the
/// templates cover every language is checked afterwards, once the declaration
/// is parsed whole (see [`SchemaInput::check_templates`]).
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
                    format!("template `{key_str}` is given twice"),
                ));
            }
            templates.push((key.clone(), content.parse::<LitStr>()?));
        } else {
            // Everything else is a payload field.
            fields.push((key, content.parse::<Type>()?));
        }
        let _ = content.parse::<Token![,]>();
    }

    let level = level
        .ok_or_else(|| syn::Error::new(name.span(), format!("event `{name}` has no `level:`")))?;

    // Placeholders have to refer to fields that exist. That is checked here:
    // the list of languages is not needed for such a check.
    let field_names: Vec<String> = fields.iter().map(|(n, _)| n.to_string()).collect();
    for (lang, tmpl) in &templates {
        for placeholder in template::placeholders(&tmpl.value()) {
            if !field_names.contains(&placeholder) {
                return Err(syn::Error::new(
                    tmpl.span(),
                    format!(
                        "the `{lang}` template of event `{name}` refers to `{{{placeholder}}}`, \
                         but there is no such field"
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

/// Parse a list of states: `[alarm Los = 0, warn Sync = 1, Lock = 2]`.
///
/// The severity goes in front and reads like a journal line: "alarm — Los".
/// The codes are **mandatory and explicit**: positional numbering would shift
/// when a state was inserted into the middle of the list, and segments already
/// written would start reading wrongly, without a single sign of an error.
fn parse_states(input: ParseStream) -> syn::Result<Vec<StateDef>> {
    let content;
    bracketed!(content in input);
    let mut out = Vec::new();
    while !content.is_empty() {
        // Two identifiers in a row: the first of them is the severity.
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
                    "state `{name}` has no code: write `{name} = 0`. The code has to \
                     be explicit — positional numbering would shift when a state was \
                     inserted into the middle of the list"
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
                format!("the severity goes in front: `{sev} {name} = {code}`"),
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
        // `type` is a Rust keyword — an ordinary `Ident` will not parse it.
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
            // A trigger predicate: its polarity is the opposite of a range's,
            // hence a different key — the two cannot be confused silently.
            "warn_if" => warn_if = Some(content.parse::<syn::Expr>()?),
            "alarm_if" => alarm_if = Some(content.parse::<syn::Expr>()?),
            "value_type" => {
                return Err(syn::Error::new(
                    key.span(),
                    "the key is called `type`: the `v` prefix carried no meaning",
                ));
            }
            // A hint instead of "unknown key": the pair warn/critical is a
            // natural guess, and the word is taken by the storage class.
            "critical" => {
                return Err(syn::Error::new(
                    key.span(),
                    "the alarm range is called `alarm`, not `critical`: the word \
                     `critical` is taken by the storage class (`store: critical`), and in \
                     one metric declaration the two would mean different things",
                ));
            }
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "unknown key `{other}` on a metric: expected type, unit, \
                         tags, store, kind, states, warn, alarm, warn_if, alarm_if"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }

    // A range and a predicate of the same level together are ambiguous: their
    // polarity is opposite (normal versus triggering), and combining them
    // silently would mean guessing on the user's behalf.
    for (range, predicate, key) in [(&warn, &warn_if, "warn"), (&alarm, &alarm_if, "alarm")] {
        if range.is_some() && predicate.is_some() {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "metric `{name}` sets both `{key}:` and `{key}_if:`: a range \
                     describes what is normal, a predicate what triggers; choose one form"
                ),
            ));
        }
    }
    if !states.is_empty() && (warn_if.is_some() || alarm_if.is_some()) {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "predicates make no sense on enum `{name}`: the severity is set on the \
                 states themselves — `alarm Los = 0`"
            ),
        ));
    }

    // An enum's type is inferred: a state code is an integer.
    if value_type.is_none() && !states.is_empty() {
        value_type = Some(Ident::new("u64", name.span()));
    }
    if value_type.is_none() {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "metric `{name}` has no `type:` (for an enum it is inferred from \
                 `states:`)"
            ),
        ));
    }
    if value_type.as_ref().is_some_and(|t| t == "blob") && (warn_if.is_some() || alarm_if.is_some())
    {
        return Err(syn::Error::new(
            name.span(),
            format!(
                "metric `{name}` is of type blob: it cannot be reduced to a number, and a predicate has nothing to check"
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

/// The range of what is normal for `warn:`/`alarm:` — with a hint if a
/// condition was written instead.
///
/// The forms are easy to confuse, and a parser error about "a range was
/// expected" does not explain the main thing: the forms have opposite polarity.
fn parse_norm_range(input: ParseStream, key: &Ident) -> syn::Result<syn::ExprRange> {
    match input.parse::<syn::Expr>()? {
        syn::Expr::Range(r) => Ok(r),
        other => Err(syn::Error::new_spanned(
            &other,
            format!(
                "`{key}:` takes the range of what is NORMAL (`{key}: -40.0..=70.0`); a \
                 trigger condition is written as a predicate: `{key}_if: v > 70.0`"
            ),
        )),
    }
}

/// Parse the types a step affects: `{ events: [A, B], metrics: [Temp] }`.
///
/// The declaration is optional, and that is a deliberate choice: a migration
/// rewrites only the segments holding affected types (the sets of their
/// identifiers lie in the footer), and a segment not rewritten is flash wear
/// saved. But the saving has to be **turned on explicitly**: a forgotten list
/// must not mean "we touch nothing", or the step would silently walk past the
/// whole history, leaving it in the earlier layout.
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
                        "unknown key `{other}` on a migration step: expected \
                         events, metrics, spans"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }
    Ok(out)
}

/// Parse a step's typed rules: `{ key: action, ... }`.
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

/// A rule's key: `v1::PowerSet`, `events::X`, `metrics::X`, `event(0x05)`,
/// `metric(0x07)`.
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
                format!("unknown key kind `{other}(..)`: expected event, metric, span"),
            )),
        };
    }

    let path: syn::Path = input.parse()?;
    let bad = |msg: &str| Err(syn::Error::new_spanned(&path, msg.to_owned()));
    if path.segments.len() != 2 {
        return bad(
            "a rule's key is a two-segment path: `v1::Type`, `events::Type`, \
             `metrics::Metric`, `spans::Kind`, or a bare id: `event(0x05)`, \
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
    bad("a key's first segment is `v<N>` (a layout from history), `events`, `metrics` or `spans`")
}

/// A rule's action: `drop`, a remap path, or a closure/function.
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

/// Parse one history entry: `N { events { Name = 0xID { field: type } } }`.
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
            // Samples and spans have no payload layout: their identifiers are
            // remapped by rules directly, and there is nothing to declare in
            // history.
            other => {
                return Err(syn::Error::new(
                    key.span(),
                    format!(
                        "history declares only `events`, got `{other}`: samples and \
                         spans have no payload layout"
                    ),
                ));
            }
        }
        let _ = content.parse::<Token![,]>();
    }
    if events.is_empty() {
        return Err(syn::Error::new(
            lit.span(),
            format!(
                "history {version} is empty: if the layouts did not change, the entry is not needed"
            ),
        ));
    }
    Ok(HistoryDef {
        version,
        span: lit.span(),
        events,
    })
}

/// An old event: `Name = 0xID { field: type, ... }`. Fields only — the level,
/// the templates and the tags are not written to disk, and an old layout has
/// no need of them.
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
                        format!("unknown key `{other}` on a span"),
                    ));
                }
            }
            let _ = content.parse::<Token![,]>();
        }
    }
    Ok(SpanDef { name, id, store })
}

// ════════════════════════════════════════════════════════════════════════════
// Generation
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
                    "unknown level `{other}`: expected \
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
        // An unknown name is an error, not a new class. Any identifier used to
        // turn silently into a `StorageClass("…")`: a typo in `critical` gave a
        // channel with a different name, a different durability policy and a
        // different budget — that is, exactly what a storage class protects
        // against, and with no sign at all. The level (`level:`) has refused a
        // typo from the start; this was the one silent branch in the whole
        // macro.
        other => {
            return Err(syn::Error::new(
                store.span(),
                format!(
                    "unknown storage class `{other}`: expected \
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
                format!("unknown value type `{other}`: expected f32/f64/i64/u64/bool/blob"),
            ));
        }
    })
}

/// The Rust type a metric parameterizes its constant with.
///
/// It is what determines what `sample` will accept: a `Metric<f32>` takes an
/// `f32` alone. For an enum it is the generated enum itself, so a state of
/// another metric fails the type check.
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
                format!("unknown value type `{other}`: expected f32/f64/i64/u64/bool/blob"),
            ));
        }
    })
}

/// The Rust type of a metric's value **on disk**.
///
/// It differs from [`marker_path`]: there an enum gives the generated enum
/// itself (the metric constant is parameterized by it), while what reaches disk
/// is the state code, that is, a `u64`. What matters to a migration rule is
/// precisely what will lie on disk.
fn wire_type(m: &MetricDef) -> syn::Result<TokenStream2> {
    if !m.states.is_empty() {
        return Ok(quote!(u64));
    }
    let Some(value_type) = &m.value_type else {
        return Ok(quote!(u64));
    };
    Ok(match value_type.to_string().as_str() {
        "f32" => quote!(f32),
        "f64" => quote!(f64),
        "i64" => quote!(i64),
        "u64" => quote!(u64),
        "bool" => quote!(bool),
        "blob" => quote!(::std::vec::Vec<u8>),
        other => {
            return Err(syn::Error::new(
                value_type.span(),
                format!("unknown value type `{other}`: expected f32/f64/i64/u64/bool/blob"),
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
        // `critical` is taken by the storage class: `store: critical` and `Los
        // = 0: critical` in one declaration would mean entirely different
        // things.
        "critical" => {
            return Err(syn::Error::new(
                s.span(),
                "the severity `critical` was renamed to `alarm`: the word `critical` \
                 is taken by the storage class (`store: critical`), and in one metric \
                 declaration the two would mean different things",
            ));
        }
        other => {
            return Err(syn::Error::new(
                s.span(),
                format!("unknown severity `{other}`: expected normal/warn/alarm"),
            ));
        }
    })
}

fn metric_kind_path(kind: Option<&Ident>, has_states: bool) -> syn::Result<TokenStream2> {
    let Some(k) = kind else {
        // An enum is held as a step by definition; everything else is
        // continuous by default.
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
                "the metric declares `states:`, so its kind is state rather than \
                 `{name}`: as a continuous quantity a chart would join the states with a \
                 straight line, showing values that never were"
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
                format!("unknown metric kind `{other}`: expected gauge/state/counter"),
            ));
        }
    })
}

/// A numeric range bound: a literal, possibly with a minus sign.
fn bound_value(expr: &syn::Expr) -> syn::Result<f64> {
    use syn::{Expr, Lit, UnOp};
    match expr {
        Expr::Lit(l) => match &l.lit {
            Lit::Float(f) => f.base10_parse::<f64>(),
            Lit::Int(i) => i.base10_parse::<f64>(),
            other => Err(syn::Error::new_spanned(
                other,
                "a range bound has to be a number",
            )),
        },
        Expr::Unary(u) if matches!(u.op, UnOp::Neg(_)) => Ok(-bound_value(&u.expr)?),
        Expr::Group(g) => bound_value(&g.expr),
        Expr::Paren(p) => bound_value(&p.expr),
        other => Err(syn::Error::new_spanned(
            other,
            "a range bound has to be a numeric literal",
        )),
    }
}

/// Build a [`dduroc::Range`] from a Rust range.
///
/// The upper bound requires `..=`: it is **inclusive**, and allowing `..70.0`
/// would mean quietly redefining the meaning of a well-known syntax.
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
                    "a limit's upper bound is inclusive — write `..=`: a value equal \
                     to the bound is still normal",
                ));
            }
            let v = bound_value(e)?;
            quote!(Some(#v))
        }
        None => quote!(None),
    };
    Ok(quote!(::dduroc::Range { min: #min, max: #max }))
}

/// Find the identifier of a declared type by its name.
///
/// A typo in the list of affected types has to be a compile error: otherwise a
/// migration step would quietly fail to find its type and walk past exactly
/// the segments it was written for.
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
                format!("a migration step names {what} `{name}`, which is not in the schema"),
            )
        })
}

/// Check that the identifiers are unique.
fn check_unique(kind: &str, items: &[(u16, Ident)]) -> syn::Result<()> {
    let mut seen: HashMap<u16, &Ident> = HashMap::new();
    for (id, name) in items {
        if let Some(prev) = seen.insert(*id, name) {
            return Err(syn::Error::new(
                name.span(),
                format!(
                    "{kind} id {id:#x} is already taken by `{prev}` — identifiers have to be unique"
                ),
            ));
        }
    }
    Ok(())
}

fn codegen(input: &SchemaInput) -> syn::Result<TokenStream2> {
    // The descriptors are laid out in ascending order of identifier: the search
    // over the schema is binary and happens on every record. The order in the
    // declaration stays free — that is the macro's business.
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
        "event",
        &input
            .events
            .iter()
            .map(|e| (e.id, e.name.clone()))
            .collect::<Vec<_>>(),
    )?;
    check_unique(
        "metric",
        &input
            .metrics
            .iter()
            .map(|m| (m.id, m.name.clone()))
            .collect::<Vec<_>>(),
    )?;
    check_unique(
        "span",
        &input
            .spans
            .iter()
            .map(|s| (s.id, s.name.clone()))
            .collect::<Vec<_>>(),
    )?;

    let schema_name = input.name.to_string();
    let version = input.version;
    let lang_strs: Vec<String> = input.languages.iter().map(|l| l.to_string()).collect();

    // ── events ───────────────────────────────────────────────────────────
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
        // An event with no fields is a unit struct rather than empty braces:
        // `ns.log(events::Started)` instead of `ns.log(events::Started {})`. On
        // the wire there is no difference — postcard encodes both as zero
        // bytes.
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

        // The templates in the order the languages were declared.
        let templates: Vec<&LitStr> = input
            .languages
            .iter()
            .map(|lang| {
                &ev.templates
                    .iter()
                    .find(|(l, _)| l == lang)
                    .expect("the template's presence was checked while parsing")
                    .1
            })
            .collect();

        // Rendering: every language gets its own format! with the arguments
        // reordered.
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

    // ── metrics ──────────────────────────────────────────────────────────
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
            .expect("value_type was checked while parsing");
        let value_type = value_type_path(value_type_ident)?;
        let class = class_path(m.store.as_ref())?;
        let unit = &m.unit;
        let tags: Vec<String> = m.tags.iter().map(|t| t.to_string()).collect();
        let kind = metric_kind_path(m.kind.as_ref(), !m.states.is_empty())?;
        let warn = range_tokens(m.warn.as_ref())?;
        let alarm = range_tokens(m.alarm.as_ref())?;

        // The trigger predicates compile into ordinary fns of the schema
        // module: a pointer to them lies in the descriptor, and a reader uses
        // them exactly as it uses ranges — a dump with its own schema is
        // coloured the same way on the device and in a viewer.
        let mut predicate = |key: &str, expr: Option<&syn::Expr>| match expr {
            None => quote!(::core::option::Option::None),
            Some(expr) => {
                let fn_name = quote::format_ident!("__{}_{}", key, name_str.to_lowercase());
                // A two-sided condition is naturally written `v > a || v < b`,
                // and clippy suggests rewriting it as a range — the very form
                // the predicate was chosen instead of.
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

        // The constant carries the value type: a `Metric<f32>` will not let an
        // integer be written to this metric, and a `Metric<LinkState>` will not
        // take a state of another metric. The name occupies the value
        // namespace, so the enum of states may be called the same.
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

        // The static state labels: the names and the severities live in the
        // schema, and only the code reaches disk.
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

            // The Rust type of the enum, so that the call site reads
            // `metrics::LinkState::Lock` rather than a bare number. A constant
            // of the same name does not clash: values and types have different
            // namespaces.
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
                #[doc = concat!("The states of metric `", #name_str, "`.")]
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

                // A value admissible for this metric is only this metric's own
                // states. The implementation is here rather than shared across
                // all enums: a shared one would overlap with the built-in
                // types, because the compiler cannot lean on "an f32 will never
                // become a metric state".
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

    // ── spans ────────────────────────────────────────────────────────────
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

    // ── history and migrations ───────────────────────────────────────────
    let history_mods = codegen_history(input)?;
    let (migration_descs, migration_fns) = codegen_migrations(input)?;

    let mod_name = &input.name;

    Ok(quote! {
        #[allow(non_snake_case, non_upper_case_globals, clippy::all, unused_imports)]
        pub mod #mod_name {
            // The names from the scope where the schema is declared are visible
            // here too: otherwise `migrations { 2 => migrate_v2 }` would
            // require writing `super::migrate_v2` — the step function lies next
            // to the declaration but expands inside the generated module.
            use super::*;

            /// This schema's event types.
            pub mod events {
                use super::*;
                #(#event_structs)*
            }

            /// The metric identifiers.
            pub mod metrics {
                #(#metric_consts)*
            }

            /// The span kind identifiers.
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

            /// The namespace schema.
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

/// Produce the `v<N>` modules with the old layouts.
///
/// Every type is an ordinary serde struct plus an `EventShape` with the **old**
/// id: the rule `v1::PowerSet` gets both from one declaration. `Serialize` is
/// derived too: migration tests write fixtures of old versions, and assembling
/// their bytes by hand would duplicate the layout in the test.
fn codegen_history(input: &SchemaInput) -> syn::Result<Vec<TokenStream2>> {
    let mut seen_versions: HashMap<u16, proc_macro2::Span> = HashMap::new();
    let mut out = Vec::new();
    for h in &input.history {
        if h.version == 0 || h.version >= input.version {
            return Err(syn::Error::new(
                h.span,
                format!(
                    "history describes the past: version {} has to be in 1..{} \
                     (the schema's current version is {})",
                    h.version, input.version, input.version
                ),
            ));
        }
        if seen_versions.insert(h.version, h.span).is_some() {
            return Err(syn::Error::new(
                h.span,
                format!("history {} is declared twice", h.version),
            ));
        }
        check_unique(
            "history event",
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
                    format!("type `{prev}` is declared twice in history {}", h.version),
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
                // As with the current layouts: no fields means a unit struct.
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
            "The layouts of version {}: the migration steps consume them, the current \
             code has no need of them.",
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

/// A resolved rule: the old identifier and the body of a match arm.
struct ResolvedRule {
    old_id: u16,
    arm: TokenStream2,
}

/// Produce the step descriptors and the dispatcher functions of the typed
/// rules.
fn codegen_migrations(input: &SchemaInput) -> syn::Result<(Vec<TokenStream2>, Vec<TokenStream2>)> {
    // Every history entry has to be used by somebody: a dead declaration is
    // almost certainly a forgotten rule, and staying silent about it means
    // leaving history untransformed.
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
                        "the layout v{}::{} is declared but no rule uses it: either a \
                         step was forgotten or the entry is superfluous",
                        h.version, e.name
                    ),
                ));
            }
        }
    }
    Ok((descs, fns))
}

/// A raw fn: as before — the fn itself plus optional touches.
///
/// The affected types decide both whether a segment is rewritten AND which
/// records the step sees: the sets are a binding filter. Not declared means
/// `touches_all`: the step sees everything and every segment is rewritten.
/// Skipping history silently is worse than rewriting a superfluous segment.
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
                    let id =
                        lookup_id(name, input.events.iter().map(|e| (&e.name, e.id)), "event")?;
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
                        "metric",
                    )?;
                    Ok(quote!(::dduroc::MetricId(#id)))
                })
                .collect::<syn::Result<Vec<_>>>()?;
            let spans = t
                .spans
                .iter()
                .map(|name| {
                    let id = lookup_id(name, input.spans.iter().map(|s| (&s.name, s.id)), "span")?;
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

/// Typed rules: the dispatcher over old ids, the decoding of the old layout
/// and the encoding of the result are all generated, and the affected types
/// are **inferred** from the keys. There is nowhere for the declared and the
/// actual to diverge.
fn codegen_rules_step(
    input: &SchemaInput,
    from: u16,
    rules: &[RuleDef],
    used_history: &mut HashMap<(u16, String), bool>,
) -> syn::Result<(TokenStream2, TokenStream2)> {
    if rules.is_empty() {
        return Err(syn::Error::new(
            input.name.span(),
            format!("step {from} is empty: not one rule — such a step does nothing"),
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
    // Two rules for one old id are an ambiguity, not a precedence. The buckets
    // are separate: events, metrics and span kinds have identifier spaces of
    // their own, and a shared bucket would reject a legitimate coincidence of
    // numbers.
    for (what, list) in [
        ("event", &event_rules),
        ("metric", &metric_rules),
        ("span kind", &span_rules),
    ] {
        let mut seen: HashMap<u16, ()> = HashMap::new();
        for r in list {
            if seen.insert(r.old_id, ()).is_some() {
                return Err(syn::Error::new(
                    input.name.span(),
                    format!(
                        "step {from}: two rules for {what} with id {:#x} — which of them \
                         applies is ambiguous",
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

/// Resolve one rule: find the old id, check that the key and the action go
/// together, and produce a dispatcher arm.
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

    // Key → old id plus the type to decode with (if the layout is known).
    enum Kind {
        Event {
            decode: Option<TokenStream2>,
        },
        Metric {
            /// The type the schema declared for the metric — the one that will
            /// lie on disk. `None` means the metric was named by a bare id and
            /// has no declared type in this schema.
            declared: Option<TokenStream2>,
        },
        Span,
    }
    let (old_id, kind) = match &rule.key {
        RuleKey::HistoryEvent { version, name } => {
            let Some(h) = input.history.iter().find(|h| h.version == *version) else {
                return err(format!(
                    "history for version {version} is not declared — there is nowhere to \
                     take the layout `v{version}::{name}` from"
                ));
            };
            let Some(e) = h.events.iter().find(|e| e.name == *name) else {
                return err(format!("history {version} has no type `{name}`"));
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
            let id = lookup_id(name, input.events.iter().map(|e| (&e.name, e.id)), "event")?;
            (
                id,
                Kind::Event {
                    decode: Some(quote!(events::#name)),
                },
            )
        }
        RuleKey::RawEvent { id, .. } => (*id, Kind::Event { decode: None }),
        RuleKey::CurrentMetric { name } => {
            let id = lookup_id(
                name,
                input.metrics.iter().map(|m| (&m.name, m.id)),
                "metric",
            )?;
            let declared = input
                .metrics
                .iter()
                .find(|m| m.name == *name)
                .map(wire_type)
                .transpose()?;
            (id, Kind::Metric { declared })
        }
        RuleKey::RawMetric { id, .. } => (*id, Kind::Metric { declared: None }),
        RuleKey::CurrentSpan { name } => (
            lookup_id(name, input.spans.iter().map(|s| (&s.name, s.id)), "span")?,
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
                // The call goes through a helper rather than `(#expr)(__old)`:
                // a closure's body is checked before an immediate call would
                // suggest the parameter's type, and `|old| old.dbm` would not
                // compile.
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
                "`event({old_id:#x})` has no layout — there is nothing to decode with: \
                 declare the type in history and write `v<N>::Type`, or use drop or a remap"
            ));
        }
        (Kind::Event { .. }, RuleAction::RemapEvent(target)) => {
            let target_id = lookup_id(
                target,
                input.events.iter().map(|e| (&e.name, e.id)),
                "event",
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
            return err(
                "an event cannot be turned into a metric: they are different record kinds"
                    .to_owned(),
            );
        }
        (Kind::Metric { .. }, RuleAction::Drop) => {
            metric_rules.push(ResolvedRule {
                old_id,
                arm: quote!(#old_id => ::core::result::Result::Ok(::core::option::Option::None),),
            });
        }
        (Kind::Metric { .. }, RuleAction::RemapMetric(target)) => {
            let target_id = lookup_id(
                target,
                input.metrics.iter().map(|m| (&m.name, m.id)),
                "metric",
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
        (
            Kind::Metric {
                declared: Some(declared),
            },
            RuleAction::Map(expr),
        ) => {
            metric_rules.push(ResolvedRule {
                old_id,
                // WHAT a rule reads is declared by the closure itself: a value
                // on disk is self-describing, and a metric needs no `history`.
                // WHAT it returns, though, is held by the schema: a sample
                // whose type contradicts it has no right to reach the disk — a
                // typed write does not let such a thing through, and a
                // migration must not either.
                arm: quote! {
                    #old_id => {
                        let __v = __r.value().ok_or(::dduroc::DecodeError)?;
                        ::dduroc::__migrate_value::<_, #declared, _>(
                            #expr,
                            __v,
                            ::dduroc::MetricId(#old_id),
                        )
                    }
                },
            });
        }
        (Kind::Metric { declared: None }, RuleAction::Map(_)) => {
            return err(format!(
                "`metric({old_id:#x})` has no declared value type — there is nothing to \
                 hold the rule's return, and a sample would reach disk with a type nothing \
                 in the schema corresponds to. Name the metric (`metrics::Name`), or use \
                 drop or a remap"
            ));
        }
        (Kind::Metric { .. }, RuleAction::RemapEvent(_)) => {
            return err(
                "a metric cannot be turned into an event: they are different record kinds"
                    .to_owned(),
            );
        }
        (Kind::Metric { .. }, RuleAction::RemapSpan(_)) => {
            return err(
                "a metric cannot be turned into a span: they are different record kinds".to_owned(),
            );
        }
        (Kind::Event { .. }, RuleAction::RemapSpan(_)) => {
            return err(
                "an event cannot be turned into a span: they are different record kinds".to_owned(),
            );
        }
        (Kind::Span, RuleAction::RemapSpan(target)) => {
            let target_id = lookup_id(target, input.spans.iter().map(|s| (&s.name, s.id)), "span")?;
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
                "a span's start is not deleted: its end, its messages and its child \
                 spans refer to it, and a chain has nothing to rewrite those references \
                 with — they would be left dangling. A span kind allows only a remap \
                 (`spans::Other`)"
                    .to_owned(),
            );
        }
        (Kind::Span, RuleAction::Map(_)) => {
            return err(
                "a span has no fields: it carries only a kind, a number and a parent, \
                 and there is nothing in it to decode. A kind remap is available \
                 (`spans::Other`)"
                    .to_owned(),
            );
        }
        (Kind::Span, RuleAction::RemapEvent(_) | RuleAction::RemapMetric(_)) => {
            return err(
                "a span cannot be turned into an event or a metric: they are different record kinds".to_owned(),
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

/// Declare a namespace schema. See the module documentation.
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

    /// Parse and, if parsing succeeded, generate the code: some diagnostics
    /// live in the parsing, some in the code generation, and to the user it is
    /// all one.
    fn check(src: &str) -> Result<(), String> {
        let parsed: SchemaInput = syn::parse_str(src).map_err(|e| e.to_string())?;
        codegen(&parsed).map(|_| ()).map_err(|e| e.to_string())
    }

    fn err(src: &str) -> String {
        check(src).expect_err("the declaration must be rejected")
    }

    const GOOD: &str = r#"
        name: radio, version: 1, languages: [en, ru],
        events { PowerSet = 0x01 { level: Info, en: "power {dbm}", ru: "мощность {dbm}", dbm: f32 } }
        metrics { Temp = 0x01 { type: f32 } }
        spans { Cal = 0x01 }
    "#;

    #[test]
    fn a_correct_declaration_compiles() {
        check(GOOD).expect("a model declaration");
    }

    #[test]
    fn sections_may_come_in_any_order() {
        // The order of the sections is the writer's taste, not the macro's
        // requirement. `events` above `languages` used to parse with an empty
        // list of languages, the template `en: "…"` was taken for a field of
        // type `"…"`, and the user got a complaint about an unparsed type
        // instead of a clear message about their schema.
        check(
            r#"
            events { PowerSet = 0x01 { level: Info, en: "power {dbm}", ru: "мощность {dbm}", dbm: f32 } }
            languages: [en, ru],
            version: 1,
            name: radio,
        "#,
        )
        .expect("the sections in any order");
    }

    #[test]
    fn an_unknown_storage_class_is_refused() {
        // The macro's one silent branch: a typo in `critical` gave a channel
        // with a different name, a different durability policy and a different
        // budget — with no sign at all that anything was wrong.
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, store: critcal, en: "x" } }"#);
        assert!(
            e.contains("critcal"),
            "it says exactly what was not recognized: {e}"
        );
        assert!(e.contains("critical"), "and what was expected: {e}");
    }

    #[test]
    fn a_template_for_an_undeclared_language_is_refused() {
        // A translation that will never be shown is silence in a place where
        // the writer is sure of the opposite.
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "x", ru: "х" } }"#);
        assert!(
            e.contains("ru"),
            "it names which template is superfluous: {e}"
        );
    }

    #[test]
    fn a_missing_template_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en, ru],
               events { Boom = 0x01 { level: Error, en: "x" } }"#);
        assert!(e.contains("ru"), "it names which language is missing: {e}");
    }

    #[test]
    fn a_placeholder_without_a_field_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "overheat {t}" } }"#);
        // Checking a single letter is not enough: if the rule "a template is a
        // string literal" broke, `en: "…"` would slide back into the fields,
        // syn would complain "expected type", and the Latin `t` would turn up
        // there.
        assert!(
            e.contains("{t}") && e.contains("no such field"),
            "the complaint is about the template, not about an unparsed type: {e}"
        );
    }

    #[test]
    fn a_field_named_like_a_language_is_still_a_field() {
        // The reverse direction of the same rule: the key matched a language
        // but the value is a type, so this is a field. Telling them apart by
        // the list of languages is not possible, and by the key's name neither.
        check(
            r#"name: radio, version: 1, languages: [en],
               events { Boom = 0x01 { level: Error, en: "code {ru}", ru: u8 } }"#,
        )
        .expect("a field named after a language is a field");
    }

    #[test]
    fn an_event_without_fields_is_a_unit_struct() {
        // `ns.log(events::Started)` — writing `Started {}` for a struct with
        // nothing to fill in means paying syntax for emptiness.
        let src = r#"name: radio, version: 1, languages: [en],
               events {
                   Started = 0x01 { level: Info, en: "started" },
                   PowerSet = 0x02 { level: Info, en: "power {dbm}", dbm: f32 }
               }"#;
        let parsed: SchemaInput = syn::parse_str(src).expect("a model declaration");
        let out = codegen(&parsed).expect("code generation").to_string();
        assert!(
            out.contains("pub struct Started ;"),
            "an event with no fields must be a unit struct: {out}"
        );
        assert!(
            out.contains("pub struct PowerSet { pub dbm : f32 , }"),
            "an event with fields stays a struct with fields: {out}"
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
        assert!(e.contains("twice"), "{e}");
    }

    #[test]
    fn an_identifier_too_large_for_u16_is_refused() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               events { Boom = 70000 { level: Error, en: "x" } }"#);
        assert!(e.contains("u16"), "{e}");
    }

    #[test]
    fn the_new_metric_forms_parse() {
        // Trigger predicates and prefix severity on states.
        check(
            r#"name: radio, version: 1, languages: [en],
               metrics {
                   Vswr = 0x01 { type: f32, warn_if: v > 1.5,
                                 alarm_if: v > 3.0 || v < 1.0 },
                   Link = 0x02 { states: [alarm Los = 0, warn Sync = 1, Lock = 2] },
               }"#,
        )
        .expect("the new forms compile");
    }

    #[test]
    fn old_metric_spellings_get_pointed_at_the_new_ones() {
        // The former key name is a hint rather than "unknown key".
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Temp = 0x01 { value_type: f32 } }"#);
        assert!(e.contains("`type`"), "{e}");

        // A condition where a range belongs: the forms have opposite polarity,
        // and that is said outright.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Temp = 0x01 { type: f32, warn: v > 70.0 } }"#);
        assert!(e.contains("warn_if"), "{e}");
        assert!(e.contains("NORMAL"), "{e}");

        // A state's suffix severity moved to the front.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Link = 0x01 { states: [Los = 0: alarm] } }"#);
        assert!(e.contains("in front"), "{e}");
        assert!(e.contains("alarm Los = 0"), "{e}");
    }

    #[test]
    fn a_range_and_a_predicate_of_one_level_are_mutually_exclusive() {
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Temp = 0x01 { type: f32, warn: ..=70.0, warn_if: v > 70.0 } }"#);
        assert!(e.contains("choose one form"), "{e}");
    }

    #[test]
    fn predicates_are_refused_where_a_number_never_comes() {
        // On an enum the states carry the severity.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Link = 0x01 { states: [Lock = 0], warn_if: v > 1.0 } }"#);
        assert!(e.contains("states"), "{e}");

        // A blob cannot be reduced to a number.
        let e = err(r#"name: radio, version: 1, languages: [en],
               metrics { Spec = 0x01 { type: blob, alarm_if: v > 1.0 } }"#);
        assert!(e.contains("blob"), "{e}");
    }

    // ── history and typed rules ──────────────────────────────────────────

    /// A v2 schema skeleton with history and one typed step.
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
        check(MIGRATING).expect("a model migration");
    }

    #[test]
    fn a_history_version_not_in_the_past_is_refused() {
        // History describes the past: the current version and versions from the
        // future are meaningless in it, and zero does not exist — numbering
        // starts at one.
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
        // A dead layout is almost certainly a forgotten rule: staying silent
        // about it means leaving history untransformed.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations { 1 => { event(0x09): drop } }"#);
        assert!(e.contains("no rule uses it"), "{e}");
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
        assert!(e.contains("v2") || e.contains("version 2"), "{e}");
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
        // A bare id has no layout — there is nothing to decode with. The hint
        // has to name the way out: declare the type in history.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               migrations { 1 => { event(0x05): |old| old } }"#);
        assert!(e.contains("history"), "{e}");
    }

    #[test]
    fn metric_values_are_mapped_by_a_closure_that_names_its_type() {
        // A sample's value is self-describing — the type lies in the record
        // itself — so a rule needs neither history nor a declaration of the
        // earlier type: the closure's parameter names the type, and it also
        // says what was expected on disk.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { metrics::Temp: |v: f32| v * 10.0 } }"#,
        )
        .expect("transforming a value is a legitimate rule");

        // What a rule READS is declared by the closure; what it RETURNS is held
        // by the schema: the return is parameterized by the metric's declared
        // type (`IntoSampleOutcome<V>`), and a sample that contradicts the
        // schema will not compile. There is nothing to check that with here —
        // the macro produces correct code and the compiler rejects it.

        // And a bare id has nothing to hold it: it has no declared type in the
        // schema, and a sample would reach disk with a type nothing corresponds
        // to.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { metric(0x07): |v: i64| v as f64 } }"#);
        assert!(e.contains("no declared value type"), "{e}");

        // For an enum metric the declared type is what lies on disk, that is,
        // the state code: a rule may renumber the codes.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Link = 0x02 { states: [alarm Los = 0, Lock = 2] } }
               migrations { 1 => { metrics::Link: |code: u64| code + 1 } }"#,
        )
        .expect("state codes can be renumbered");
    }

    #[test]
    fn span_kinds_are_renamed_but_never_deleted() {
        // A kind remap is a legitimate rule.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01, Calibration = 0x02 }
               migrations { 1 => { spans::Calib: spans::Calibration } }"#,
        )
        .expect("renaming a span kind");

        // And by a bare id too: a kind may have disappeared from the schema
        // along with its name.
        check(
            r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calibration = 0x02 }
               migrations { 1 => { span(0x01): spans::Calibration } }"#,
        )
        .expect("a span kind by bare id");

        // But deletion is not: a span's start is referred to by its end, its
        // messages and its children, and a chain cannot rewrite those
        // references.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01 }
               migrations { 1 => { spans::Calib: drop } }"#);
        assert!(e.contains("dangling"), "{e}");

        // Nor is a closure: a span has nothing to decode.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01 }
               migrations { 1 => { spans::Calib: |s| s } }"#);
        assert!(e.contains("no fields"), "{e}");
    }

    #[test]
    fn span_kinds_have_their_own_id_space_in_rules() {
        // The numbers of span kinds, events and metrics live in different
        // spaces: a shared bucket of checks would reject a legitimate
        // coincidence of numbers.
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
        .expect("three rules with id 0x01 in three spaces");

        // And two rules for one kind are still an ambiguity.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               spans { Calib = 0x01, A = 0x02, B = 0x03 }
               migrations { 1 => { spans::Calib: spans::A, span(0x01): spans::B } }"#);
        assert!(e.contains("ambiguous"), "{e}");
    }

    #[test]
    fn kinds_do_not_cross() {
        // An event cannot be turned into a metric or the reverse: different
        // record kinds.
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { events::E: metrics::Temp } }"#);
        assert!(e.contains("different record kinds"), "{e}");

        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               metrics { Temp = 0x01 { type: f32 } }
               migrations { 1 => { metrics::Temp: events::E } }"#);
        assert!(e.contains("different record kinds"), "{e}");
    }

    #[test]
    fn two_rules_for_one_old_id_are_ambiguous() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               history { 1 { events { E = 0x01 { n: u8 } } } }
               migrations { 1 => { v1::E: drop, events::E: drop } }"#);
        assert!(e.contains("ambiguous"), "{e}");
    }

    #[test]
    fn an_empty_rules_step_is_refused() {
        let e = err(r#"name: radio, version: 2, languages: [en],
               events { E = 0x01 { level: Info, en: "x" } }
               migrations { 1 => { } }"#);
        assert!(e.contains("is empty"), "{e}");
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
        assert!(e.contains("twice"), "{e}");
    }

    #[test]
    fn history_declares_events_only() {
        // Samples have no payload layout — there is nothing to declare in
        // history, and the attempt has to be named rather than swallowed
        // silently.
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
        assert!(e.contains("is empty"), "{e}");
    }

    #[test]
    fn the_raw_fn_escape_hatch_still_parses() {
        // The hatch stays a hatch: a raw fn with touches and without,
        // interleaved with typed steps.
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
        .expect("a mixed declaration is legitimate");
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
