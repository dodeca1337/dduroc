//! Versioned, lossless presentation models. No native paths or schema TypeIds leak.
use crate::error::ServiceError;
use async_graphql::{
    Enum, InputValueError, InputValueResult, Scalar, ScalarType, SimpleObject, Value,
};
use base64::Engine as _;
use dduroc_engine::schema::{DecodedValue, DecodedValueLimits};
use dduroc_read::{Entry, EntryKind, OwnedSampleValue, Reader};

pub const API_VERSION: &str = "1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UInt64(pub u64);
/// Unsigned 64-bit integer encoded as a canonical decimal JSON string.
/// GraphQL Int and JSON numeric input are rejected; no leading zeros or plus sign.
#[Scalar(name = "UInt64")]
impl ScalarType for UInt64 {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(text)
                if text == "0"
                    || (!text.starts_with('0') && text.bytes().all(|c| c.is_ascii_digit())) =>
            {
                text.parse().map(Self).map_err(InputValueError::custom)
            }
            _ => Err(InputValueError::custom(
                "expected a canonical decimal string",
            )),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Int64(pub i64);
/// Signed 64-bit integer encoded as a canonical decimal JSON string.
/// GraphQL Int and JSON numeric input are rejected; no leading zeros or negative zero.
#[Scalar(name = "Int64")]
impl ScalarType for Int64 {
    fn parse(value: Value) -> InputValueResult<Self> {
        match value {
            Value::String(text)
                if text == "0"
                    || (!text.is_empty()
                        && text != "-0"
                        && !text.starts_with('0')
                        && !text.starts_with("-0")
                        && text
                            .trim_start_matches('-')
                            .bytes()
                            .all(|c| c.is_ascii_digit())
                        && text.matches('-').count() <= 1
                        && !text.ends_with('-')) =>
            {
                text.parse().map(Self).map_err(InputValueError::custom)
            }
            _ => Err(InputValueError::custom(
                "expected a canonical decimal string",
            )),
        }
    }
    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum Class {
    Default,
    Critical,
    Telemetry,
}
impl From<Class> for dduroc_engine::schema::StorageClass {
    fn from(value: Class) -> Self {
        match value {
            Class::Default => Self::Default,
            Class::Critical => Self::Critical,
            Class::Telemetry => Self::Telemetry,
        }
    }
}
impl From<dduroc_engine::schema::StorageClass> for Class {
    fn from(value: dduroc_engine::schema::StorageClass) -> Self {
        match value {
            dduroc_engine::schema::StorageClass::Default => Self::Default,
            dduroc_engine::schema::StorageClass::Critical => Self::Critical,
            dduroc_engine::schema::StorageClass::Telemetry => Self::Telemetry,
        }
    }
}
#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum Level {
    DevTrace,
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
impl From<Level> for dduroc_format::Level {
    fn from(value: Level) -> Self {
        match value {
            Level::DevTrace => Self::DevTrace,
            Level::Trace => Self::Trace,
            Level::Debug => Self::Debug,
            Level::Info => Self::Info,
            Level::Warn => Self::Warn,
            Level::Error => Self::Error,
        }
    }
}
impl From<dduroc_format::Level> for Level {
    fn from(value: dduroc_format::Level) -> Self {
        match value {
            dduroc_format::Level::DevTrace => Self::DevTrace,
            dduroc_format::Level::Trace => Self::Trace,
            dduroc_format::Level::Debug => Self::Debug,
            dduroc_format::Level::Info => Self::Info,
            dduroc_format::Level::Warn => Self::Warn,
            dduroc_format::Level::Error => Self::Error,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConversionLimits {
    pub max_text_bytes: usize,
    pub max_blob_bytes: usize,
    pub max_output_bytes: usize,
    pub max_value_nodes: usize,
    pub max_value_depth: usize,
}
impl Default for ConversionLimits {
    fn default() -> Self {
        Self {
            max_text_bytes: 64 * 1024,
            max_blob_bytes: 4 * 1024 * 1024,
            max_output_bytes: 4 * 1024 * 1024,
            max_value_nodes: 16_384,
            max_value_depth: 32,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Projection {
    pub fields: bool,
    pub rendered: bool,
    pub payload: bool,
    pub sample_value: bool,
    pub text: bool,
    pub target: bool,
    pub extension: bool,
}
impl Default for Projection {
    fn default() -> Self {
        Self {
            fields: true,
            rendered: true,
            payload: true,
            sample_value: true,
            text: true,
            target: true,
            extension: true,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct Presentation {
    /// `None` inherits the reader's default language preferences.
    pub languages: Option<dduroc_read::LanguagePreferences>,
    pub projection: Projection,
}

pub(crate) struct ConversionBudget<'a> {
    limits: &'a ConversionLimits,
    bytes: usize,
    nodes: usize,
}
impl<'a> ConversionBudget<'a> {
    pub fn new(limits: &'a ConversionLimits) -> Self {
        Self {
            limits,
            bytes: 0,
            nodes: 0,
        }
    }
    fn charge(&mut self, n: usize) -> Result<(), ServiceError> {
        self.bytes = self
            .bytes
            .checked_add(n)
            .ok_or_else(|| refused("output size overflow"))?;
        if self.bytes > self.limits.max_output_bytes {
            return Err(refused("output byte limit"));
        }
        Ok(())
    }
    fn text(&mut self, text: &str) -> Result<String, ServiceError> {
        if text.len() > self.limits.max_text_bytes {
            return Err(refused("text byte limit"));
        }
        self.charge(text.len().saturating_mul(6).saturating_add(64))?;
        Ok(text.to_owned())
    }
    fn blob(&mut self, bytes: &[u8]) -> Result<BytesView, ServiceError> {
        if bytes.len() > self.limits.max_blob_bytes {
            return Err(refused("blob byte limit"));
        }
        let size = bytes
            .len()
            .checked_add(2)
            .and_then(|n| n.checked_div(3))
            .and_then(|n| n.checked_mul(4))
            .ok_or_else(|| refused("blob length overflow"))?;
        self.charge(size)?;
        Ok(BytesView {
            base64: base64::engine::general_purpose::STANDARD.encode(bytes),
            bytes: UInt64(bytes.len() as u64),
        })
    }
    fn value(&mut self, value: DecodedValue, depth: usize) -> Result<ValueView, ServiceError> {
        if depth > self.limits.max_value_depth || self.nodes >= self.limits.max_value_nodes {
            return Err(refused("value tree limit"));
        }
        self.nodes += 1;
        self.charge(std::mem::size_of::<ValueView>())?;
        let mut out = ValueView {
            kind: ValueKind::Null,
            text: None,
            boolean: None,
            float: None,
            bytes: None,
            items: Vec::new(),
            pairs: Vec::new(),
            variant: None,
        };
        match value {
            DecodedValue::Null => {}
            DecodedValue::Bool(value) => {
                out.kind = ValueKind::Boolean;
                out.boolean = Some(value);
            }
            DecodedValue::Signed(value) => {
                out.kind = ValueKind::Signed;
                out.text = Some(self.text(&value.to_string())?);
            }
            DecodedValue::Unsigned(value) => {
                out.kind = ValueKind::Unsigned;
                out.text = Some(self.text(&value.to_string())?);
            }
            DecodedValue::Float32(bits) => {
                out.kind = ValueKind::Float;
                out.float = Some(FloatView::f32(bits));
            }
            DecodedValue::Float64(bits) => {
                out.kind = ValueKind::Float;
                out.float = Some(FloatView::f64(bits));
            }
            DecodedValue::String(value) => {
                out.kind = ValueKind::String;
                out.text = Some(self.text(&value)?);
            }
            DecodedValue::Bytes(value) => {
                out.kind = ValueKind::Bytes;
                out.bytes = Some(self.blob(&value)?);
            }
            DecodedValue::Sequence(values) => {
                out.kind = ValueKind::Sequence;
                for value in values {
                    out.items.push(self.value(value, depth + 1)?);
                }
            }
            DecodedValue::Map(pairs) => {
                out.kind = ValueKind::Map;
                for (key, value) in pairs {
                    out.pairs.push(ValuePair {
                        key: self.value(key, depth + 1)?,
                        value: self.value(value, depth + 1)?,
                    });
                }
            }
            DecodedValue::Variant { name, value } => {
                out.kind = ValueKind::Variant;
                out.variant = Some(self.text(&name)?);
                out.items.push(self.value(*value, depth + 1)?);
            }
        }
        Ok(out)
    }
}
fn refused(reason: &str) -> ServiceError {
    ServiceError::Conversion(reason.to_owned())
}
#[derive(Debug, Clone, SimpleObject)]
pub struct BytesView {
    pub base64: String,
    pub bytes: UInt64,
}
#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum FloatClass {
    Finite,
    Nan,
    PositiveInfinity,
    NegativeInfinity,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct FloatView {
    /// IEEE-754 source width, 32 or 64 bits.
    pub width: i32,
    /// Finite, NaN or a signed infinity. No non-finite JSON number is emitted.
    pub class: FloatClass,
    /// Human-readable value; bits remain authoritative for signed zero and NaN payloads.
    pub decimal: String,
    /// Fixed-width lowercase hexadecimal IEEE-754 bit pattern (8 or 16 digits).
    pub bits: String,
}
impl FloatView {
    fn f32(bits: u32) -> Self {
        let n = f32::from_bits(bits);
        Self {
            width: 32,
            class: if n.is_nan() {
                FloatClass::Nan
            } else if n == f32::INFINITY {
                FloatClass::PositiveInfinity
            } else if n == f32::NEG_INFINITY {
                FloatClass::NegativeInfinity
            } else {
                FloatClass::Finite
            },
            decimal: n.to_string(),
            bits: format!("{bits:08x}"),
        }
    }
    fn f64(bits: u64) -> Self {
        let n = f64::from_bits(bits);
        Self {
            width: 64,
            class: if n.is_nan() {
                FloatClass::Nan
            } else if n == f64::INFINITY {
                FloatClass::PositiveInfinity
            } else if n == f64::NEG_INFINITY {
                FloatClass::NegativeInfinity
            } else {
                FloatClass::Finite
            },
            decimal: n.to_string(),
            bits: format!("{bits:016x}"),
        }
    }
}
#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum ValueKind {
    Null,
    Boolean,
    Signed,
    Unsigned,
    Float,
    String,
    Bytes,
    Sequence,
    Map,
    Variant,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct ValuePair {
    pub key: ValueView,
    pub value: ValueView,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct ValueView {
    pub kind: ValueKind,
    pub text: Option<String>,
    pub boolean: Option<bool>,
    pub float: Option<FloatView>,
    pub bytes: Option<BytesView>,
    pub items: Vec<ValueView>,
    pub pairs: Vec<ValuePair>,
    pub variant: Option<String>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct BootTimeView {
    pub boot: UInt64,
    pub micros: UInt64,
}
#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum EntryType {
    Message,
    Text,
    SpanStart,
    SpanEnd,
    Sample,
    Extension,
}
#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum FieldsStatus {
    Decoded,
    UnknownSchema,
    UnsupportedDecoder,
    NotRequested,
}
#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum RenderStatus {
    Rendered,
    UnknownSchema,
    UnsupportedDecoder,
    NotRequested,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct MessageView {
    pub event_id: UInt64,
    pub name: Option<String>,
    pub level: Option<Level>,
    pub tags: Vec<String>,
    pub rendered: Option<String>,
    pub render_status: RenderStatus,
    pub language: Option<String>,
    // Matching preference index, or null for fallback or an unknown schema.
    pub language_preference_index: Option<i32>,
    pub payload: Option<BytesView>,
    pub fields_status: FieldsStatus,
    pub fields: Option<ValueView>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct TextView {
    pub level: Level,
    pub target: String,
    pub text: String,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct SpanView {
    pub id: UInt64,
    pub kind_name: Option<String>,
    pub parent: Option<UInt64>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct SampleView {
    pub metric_id: UInt64,
    pub name: Option<String>,
    pub unit: Option<String>,
    pub tags: Vec<String>,
    pub kind: Option<String>,
    pub state_name: Option<String>,
    pub severity: Option<String>,
    pub value: Option<ValueView>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct EntryView {
    pub namespace: String,
    pub class: Class,
    pub at: BootTimeView,
    pub utc: Option<String>,
    pub span: Option<UInt64>,
    pub kind: EntryType,
    pub message: Option<MessageView>,
    pub text: Option<TextView>,
    pub sample: Option<SampleView>,
    pub span_event: Option<SpanView>,
    pub extension: Option<BytesView>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct DamageView {
    pub offset: UInt64,
    pub message: String,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct PageView {
    pub entries: Vec<EntryView>,
    pub seeds: Vec<EntryView>,
    pub exhausted: bool,
    pub truncated: bool,
    pub damaged: Vec<DamageView>,
    pub unanchored: Vec<UInt64>,
}

fn convert_entry(
    reader: &Reader,
    entry: Entry,
    presentation: &Presentation,
    budget: &mut ConversionBudget<'_>,
) -> Result<EntryView, ServiceError> {
    budget.charge(std::mem::size_of::<EntryView>())?;
    let schema = reader
        .schema_of(&entry.namespace)
        .filter(|schema| schema.identity == entry.schema_identity);

    let mut out = EntryView {
        namespace: budget.text(&entry.namespace)?,
        class: entry.channel.into(),
        at: BootTimeView {
            boot: UInt64(entry.at.boot.0.into()),
            micros: UInt64(entry.at.at.0),
        },
        utc: entry.utc.map(|time| time.to_rfc3339()),
        span: entry.span.map(|id| UInt64(id.0.into())),
        kind: EntryType::Extension,
        message: None,
        text: None,
        sample: None,
        span_event: None,
        extension: None,
    };
    let mut strings = |items: &[&str]| -> Result<Vec<String>, ServiceError> {
        items.iter().map(|text| budget.text(text)).collect()
    };
    match entry.kind {
        EntryKind::Message {
            event,
            name,
            level,
            tags,
            payload,
        } => {
            let tags = strings(tags)?;
            let preferences = presentation
                .languages
                .as_ref()
                .unwrap_or_else(|| reader.language_preferences());
            let selection = schema.and_then(|schema| preferences.select(&schema));
            let lang_index = selection.map_or(0, |selection| selection.language_index);
            let lang = schema
                .and_then(|schema| schema.languages.get(lang_index))
                .map(|lang| budget.text(lang.0))
                .transpose()?;
            let render = schema
                .and_then(|schema| schema.event(event))
                .and_then(|desc| desc.decoders)
                .and_then(|decoders| decoders.render_bounded);
            let (render_status, rendered) = if !presentation.projection.rendered {
                (RenderStatus::NotRequested, None)
            } else {
                match render {
                    Some(render) => {
                        let limits = DecodedValueLimits {
                            max_depth: budget.limits.max_value_depth,
                            max_nodes: budget.limits.max_value_nodes.saturating_sub(budget.nodes),
                            max_bytes: budget.limits.max_output_bytes.saturating_sub(budget.bytes),
                        };
                        let text = render(
                            &payload,
                            lang_index,
                            limits,
                            budget.limits.max_text_bytes.min(limits.max_bytes),
                        )
                        .map_err(|_| refused("bounded rendering failed"))?;
                        (RenderStatus::Rendered, Some(budget.text(&text)?))
                    }
                    None => (
                        if schema.is_some() {
                            RenderStatus::UnsupportedDecoder
                        } else {
                            RenderStatus::UnknownSchema
                        },
                        None,
                    ),
                }
            };
            let decoder = schema
                .and_then(|schema| schema.event(event))
                .and_then(|desc| desc.decoders)
                .and_then(|decoders| decoders.fields);
            let (fields_status, fields) = if !presentation.projection.fields {
                (FieldsStatus::NotRequested, None)
            } else {
                match decoder {
                    Some(decode) => {
                        let limits = DecodedValueLimits {
                            max_depth: budget.limits.max_value_depth,
                            max_nodes: budget.limits.max_value_nodes.saturating_sub(budget.nodes),
                            max_bytes: budget.limits.max_output_bytes.saturating_sub(budget.bytes),
                        };
                        let value = decode(&payload, limits)
                            .map_err(|_| refused("bounded field decoder failed"))?;
                        (FieldsStatus::Decoded, Some(budget.value(value, 0)?))
                    }
                    None => (
                        if schema.is_some() {
                            FieldsStatus::UnsupportedDecoder
                        } else {
                            FieldsStatus::UnknownSchema
                        },
                        None,
                    ),
                }
            };
            out.kind = EntryType::Message;
            out.message = Some(MessageView {
                event_id: UInt64(event.0.into()),
                name: name.map(|text| budget.text(text)).transpose()?,
                level: level.map(Into::into),
                tags,
                rendered,
                render_status,
                language: lang,
                language_preference_index: selection
                    .and_then(|selection| selection.preference_index)
                    .map(|index| index as i32),
                payload: if presentation.projection.payload {
                    Some(budget.blob(&payload)?)
                } else {
                    None
                },
                fields_status,
                fields,
            });
        }
        EntryKind::Text {
            level,
            target,
            text,
        } => {
            out.kind = EntryType::Text;
            out.text = Some(TextView {
                level: level.into(),
                target: if presentation.projection.target {
                    budget.text(&target)?
                } else {
                    String::new()
                },
                text: if presentation.projection.text {
                    budget.text(&text)?
                } else {
                    String::new()
                },
            });
        }
        EntryKind::SpanStart {
            span,
            kind_name,
            parent,
        } => {
            out.kind = EntryType::SpanStart;
            out.span_event = Some(SpanView {
                id: UInt64(span.0.into()),
                kind_name: kind_name.map(|text| budget.text(text)).transpose()?,
                parent: parent.map(|id| UInt64(id.0.into())),
            });
        }
        EntryKind::SpanEnd { span } => {
            out.kind = EntryType::SpanEnd;
            out.span_event = Some(SpanView {
                id: UInt64(span.0.into()),
                kind_name: None,
                parent: None,
            });
        }
        EntryKind::Sample {
            metric,
            metric_name,
            unit,
            tags,
            kind,
            state_name,
            severity,
            value,
        } => {
            let tags = strings(tags)?;
            let value = match value {
                OwnedSampleValue::F32(n) => DecodedValue::Float32(n.to_bits()),
                OwnedSampleValue::F64(n) => DecodedValue::Float64(n.to_bits()),
                OwnedSampleValue::I64(n) => DecodedValue::Signed(n.into()),
                OwnedSampleValue::U64(n) => DecodedValue::Unsigned(n.into()),
                OwnedSampleValue::Bool(n) => DecodedValue::Bool(n),
                OwnedSampleValue::Blob(n) => DecodedValue::Bytes(n),
            };
            out.kind = EntryType::Sample;
            out.sample = Some(SampleView {
                metric_id: UInt64(metric.0.into()),
                name: metric_name.map(|text| budget.text(text)).transpose()?,
                unit: unit.map(|text| budget.text(text)).transpose()?,
                tags,
                kind: kind.map(|kind| format!("{kind:?}")),
                state_name: state_name.map(|text| budget.text(text)).transpose()?,
                severity: severity.map(|value| value.as_str().to_owned()),
                value: if presentation.projection.sample_value {
                    Some(budget.value(value, 0)?)
                } else {
                    None
                },
            });
        }
        EntryKind::Ext { bytes } => {
            if presentation.projection.extension {
                out.extension = Some(budget.blob(&bytes)?);
            }
        }
    }
    Ok(out)
}

pub fn convert_page(
    reader: &Reader,
    page: dduroc_read::QueryPage,
    presentation: &Presentation,
    limits: &ConversionLimits,
) -> Result<PageView, ServiceError> {
    let mut budget = ConversionBudget::new(limits);
    let mut entries = Vec::new();
    let mut seeds = Vec::new();
    for entry in page.entries {
        entries.push(convert_entry(reader, entry, presentation, &mut budget)?);
    }
    for entry in page.seeds {
        seeds.push(convert_entry(reader, entry, presentation, &mut budget)?);
    }
    let damaged = page
        .damaged
        .into_iter()
        .map(|damage| {
            budget.charge(std::mem::size_of::<DamageView>())?;
            Ok(DamageView {
                offset: UInt64(damage.offset),
                message: budget.text("stored fragment could not be read")?,
            })
        })
        .collect::<Result<_, ServiceError>>()?;
    budget.charge(page.unanchored.len().saturating_mul(8))?;
    Ok(PageView {
        entries,
        seeds,
        damaged,
        exhausted: page.exhausted,
        truncated: page.truncated,
        unanchored: page
            .unanchored
            .into_iter()
            .map(|boot| UInt64(boot.0.into()))
            .collect(),
    })
}

#[derive(Debug, Clone, SimpleObject)]
pub struct FieldView {
    pub name: String,
    pub type_name: String,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct EventDescriptorView {
    pub id: UInt64,
    pub name: String,
    pub level: Level,
    pub class: Class,
    pub tags: Vec<String>,
    pub templates: Vec<String>,
    pub fields: Vec<FieldView>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct StateView {
    pub code: UInt64,
    pub name: String,
    pub severity: String,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct MetricDescriptorView {
    pub id: UInt64,
    pub name: String,
    pub value_type: String,
    pub class: Class,
    pub unit: String,
    pub tags: Vec<String>,
    pub kind: String,
    pub states: Vec<StateView>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct SpanDescriptorView {
    pub id: UInt64,
    pub name: String,
    pub class: Class,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct SchemaView {
    pub name: String,
    pub version: UInt64,
    pub languages: Vec<String>,
    pub events: Vec<EventDescriptorView>,
    pub metrics: Vec<MetricDescriptorView>,
    pub spans: Vec<SpanDescriptorView>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct NamespaceView {
    pub namespace: String,
    pub class: Class,
    pub schema_name: String,
    pub protocol_version: UInt64,
    pub retained_bytes: UInt64,
    pub quota_bytes: Option<UInt64>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct CatalogView {
    pub api_version: String,
    // Reader default preferences, in order.
    pub default_languages: Vec<String>,
    pub namespaces: Vec<NamespaceView>,
    pub schemas: Vec<SchemaView>,
}

pub fn convert_catalog(
    rows: Vec<dduroc_read::CatalogChannel>,
    limits: &ConversionLimits,
    default_languages: &dduroc_read::LanguagePreferences,
) -> Result<CatalogView, ServiceError> {
    let mut budget = ConversionBudget::new(limits);
    let mut namespaces = Vec::new();
    let mut schemas = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for row in rows {
        budget.charge(std::mem::size_of::<NamespaceView>())?;
        namespaces.push(NamespaceView {
            namespace: budget.text(&row.namespace)?,
            class: row.class.into(),
            schema_name: budget.text(&row.schema_name)?,
            protocol_version: UInt64(row.protocol_version.into()),
            retained_bytes: UInt64(row.retained_bytes),
            quota_bytes: row.quota_bytes.map(UInt64),
        });
        let Some(schema) = row.schema else { continue };
        if !seen.insert(schema.name) {
            continue;
        }
        let mut languages = Vec::new();
        for language in schema.languages {
            languages.push(budget.text(language.0)?);
        }
        let mut events = Vec::new();
        for event in schema.events {
            budget.charge(std::mem::size_of::<EventDescriptorView>())?;
            let mut fields = Vec::new();
            for field in event.fields {
                budget.charge(std::mem::size_of::<FieldView>())?;
                fields.push(FieldView {
                    name: budget.text(field.name)?,
                    type_name: budget.text(field.type_name)?,
                });
            }
            let mut tags = Vec::new();
            for tag in event.tags {
                tags.push(budget.text(tag)?);
            }
            let mut templates = Vec::new();
            for template in event.templates {
                templates.push(budget.text(template)?);
            }
            events.push(EventDescriptorView {
                id: UInt64(event.id.0.into()),
                name: budget.text(event.name)?,
                level: event.level.into(),
                class: event.class.into(),
                tags,
                templates,
                fields,
            });
        }
        let mut metrics = Vec::new();
        for metric in schema.metrics {
            budget.charge(std::mem::size_of::<MetricDescriptorView>())?;
            let mut tags = Vec::new();
            for tag in metric.tags {
                tags.push(budget.text(tag)?);
            }
            let mut states = Vec::new();
            for state in metric.states {
                budget.charge(std::mem::size_of::<StateView>())?;
                states.push(StateView {
                    code: UInt64(state.code),
                    name: budget.text(state.name)?,
                    severity: budget.text(state.severity.as_str())?,
                });
            }
            metrics.push(MetricDescriptorView {
                id: UInt64(metric.id.0.into()),
                name: budget.text(metric.name)?,
                value_type: budget.text(metric.value_type.as_str())?,
                class: metric.class.into(),
                unit: budget.text(metric.unit)?,
                tags,
                kind: budget.text(&format!("{:?}", metric.kind))?,
                states,
            });
        }
        let mut spans = Vec::new();
        for span in schema.spans {
            budget.charge(std::mem::size_of::<SpanDescriptorView>())?;
            spans.push(SpanDescriptorView {
                id: UInt64(span.id.0.into()),
                name: budget.text(span.name)?,
                class: span.class.into(),
            });
        }
        schemas.push(SchemaView {
            name: budget.text(schema.name)?,
            version: UInt64(schema.version.0.into()),
            languages,
            events,
            metrics,
            spans,
        });
    }
    Ok(CatalogView {
        api_version: API_VERSION.to_owned(),
        default_languages: default_languages
            .codes()
            .iter()
            .map(|code| budget.text(code))
            .collect::<Result<_, _>>()?,
        namespaces,
        schemas,
    })
}

#[derive(Debug, Clone, SimpleObject)]
pub struct FollowBatchView {
    pub entries: Vec<EntryView>,
    pub seeds: Vec<EntryView>,
    pub damaged: Vec<DamageView>,
    pub unanchored: Vec<UInt64>,
    pub ended: bool,
}

pub(crate) struct FollowRecords {
    pub entries: Vec<Entry>,
    pub seeds: Vec<Entry>,
    pub damaged: Vec<dduroc_read::Damage>,
    pub unanchored: Vec<dduroc_read::BootCounter>,
    pub ended: bool,
}

pub(crate) fn convert_follow(
    reader: &Reader,
    records: FollowRecords,
    presentation: &Presentation,
    limits: &ConversionLimits,
) -> Result<(FollowBatchView, usize), ServiceError> {
    let FollowRecords {
        entries,
        seeds,
        damaged,
        unanchored,
        ended,
    } = records;
    let mut budget = ConversionBudget::new(limits);
    let mut views = Vec::new();
    let mut seed_views = Vec::new();
    for entry in entries {
        views.push(convert_entry(reader, entry, presentation, &mut budget)?);
    }
    for entry in seeds {
        seed_views.push(convert_entry(reader, entry, presentation, &mut budget)?);
    }
    let damaged = damaged
        .into_iter()
        .map(|damage| {
            budget.charge(std::mem::size_of::<DamageView>())?;
            Ok(DamageView {
                offset: UInt64(damage.offset),
                message: budget.text("stored fragment could not be read")?,
            })
        })
        .collect::<Result<_, ServiceError>>()?;
    budget.charge(unanchored.len().saturating_mul(32))?;
    let bytes = budget.bytes;
    Ok((
        FollowBatchView {
            entries: views,
            seeds: seed_views,
            damaged,
            unanchored: unanchored
                .into_iter()
                .map(|boot| UInt64(boot.0.into()))
                .collect(),
            ended,
        },
        bytes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_scalars_refuse_numeric_coercion_and_noncanonical_values() {
        assert!(UInt64::parse(Value::Number(1.into())).is_err());
        for text in ["+1", "01", "-1", "18446744073709551616", ""] {
            assert!(UInt64::parse(Value::String(text.into())).is_err());
        }
        assert_eq!(
            UInt64::parse(Value::String(u64::MAX.to_string()))
                .unwrap()
                .0,
            u64::MAX
        );
        assert_eq!(
            Int64::parse(Value::String(i64::MIN.to_string())).unwrap().0,
            i64::MIN
        );
        assert!(Int64::parse(Value::String("-0".into())).is_err());
    }
    #[test]
    fn float_view_and_tree_budget_do_not_silently_normalize_values() {
        let view = FloatView::f64(0x7ff8_0000_0000_0001);
        assert_eq!(view.class, FloatClass::Nan);
        assert_eq!(view.bits, "7ff8000000000001");
        let limits = ConversionLimits {
            max_value_nodes: 2,
            ..ConversionLimits::default()
        };
        let mut budget = ConversionBudget::new(&limits);
        let value = DecodedValue::Sequence(vec![DecodedValue::Null, DecodedValue::Null]);
        assert!(budget.value(value, 0).is_err());
    }
}

#[derive(Debug, Clone, Copy, Enum, PartialEq, Eq)]
pub enum CountingState {
    Complete,
    BackfillRequired,
    InProgress,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct RuntimeStatisticsView {
    pub records_written: UInt64,
    pub blocks_written: UInt64,
    pub bytes_written: UInt64,
    pub dropped: UInt64,
    pub rejected: UInt64,
    pub io_errors: UInt64,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct RootStatisticsView {
    pub index: UInt64,
    pub allocated_bytes: UInt64,
    pub log_bytes: UInt64,
    pub rewrite_bytes: UInt64,
    pub metadata_bytes: UInt64,
    pub physical_limit_bytes: Option<UInt64>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct ClassStatisticsView {
    pub class: Class,
    pub logical_retained_bytes: UInt64,
    pub limit_bytes: Option<UInt64>,
}
#[derive(Debug, Clone, SimpleObject)]
pub struct StatisticsView {
    pub captured_at: String,
    pub counting_state: CountingState,
    pub retained_records: Option<UInt64>,
    pub retained_blocks: Option<UInt64>,
    pub logical_retained_bytes: UInt64,
    pub channels: UInt64,
    pub classes: Vec<ClassStatisticsView>,
    pub runtime: Option<RuntimeStatisticsView>,
    pub roots: Option<Vec<RootStatisticsView>>,
}
impl From<dduroc_read::Statistics> for StatisticsView {
    fn from(value: dduroc_read::Statistics) -> Self {
        let live = value.runtime.is_some();
        Self {
            captured_at: value.captured_at.to_rfc3339(),
            counting_state: match value.counting_state {
                dduroc_engine::backend::CountingState::Complete => CountingState::Complete,
                dduroc_engine::backend::CountingState::BackfillRequired => {
                    CountingState::BackfillRequired
                }
                dduroc_engine::backend::CountingState::InProgress => CountingState::InProgress,
            },
            retained_records: value.retained_records.map(UInt64),
            retained_blocks: value.retained_blocks.map(UInt64),
            logical_retained_bytes: UInt64(value.logical_retained_bytes),
            channels: UInt64(value.channels),
            classes: value
                .classes
                .into_iter()
                .map(|c| ClassStatisticsView {
                    class: c.class.into(),
                    logical_retained_bytes: UInt64(c.logical_retained_bytes),
                    limit_bytes: c.limit_bytes.map(UInt64),
                })
                .collect(),
            runtime: value.runtime.map(|r| RuntimeStatisticsView {
                records_written: UInt64(r.records_written),
                blocks_written: UInt64(r.blocks_written),
                bytes_written: UInt64(r.bytes_written),
                dropped: UInt64(r.dropped),
                rejected: UInt64(r.rejected),
                io_errors: UInt64(r.io_errors),
            }),
            roots: value.roots.map(|roots| {
                roots
                    .into_iter()
                    .map(|r| RootStatisticsView {
                        index: UInt64(r.index as u64),
                        allocated_bytes: UInt64(r.allocated_bytes),
                        log_bytes: UInt64(r.log_bytes),
                        rewrite_bytes: UInt64(r.rewrite_bytes),
                        metadata_bytes: UInt64(r.metadata_bytes),
                        physical_limit_bytes: live.then_some(UInt64(r.physical_limit_bytes)),
                    })
                    .collect()
            }),
        }
    }
}
#[derive(Debug, Clone, SimpleObject)]
pub struct BackfillStatisticsView {
    pub more: bool,
}
