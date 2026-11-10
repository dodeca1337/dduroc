//! # dduroc
//!
//! Logging and telemetry for embedded Linux systems.
//!
//! It stores the **minimum**: on disk there is only what varies — an event's
//! type, its time as a delta from the neighbouring record, and binary fields.
//! Levels, text templates, tags and names live in the firmware's schema and are
//! substituted at read time. The hardware has no absolute clock, so a stamp is
//! a [`BootTime`] (a run plus microseconds since it started), and wall-clock
//! time appears after the fact, once a synchronization arrives: the anchor is
//! retroactive, and one synchronization gives a UTC to every record of that
//! boot, those made before it included.
//!
//! Generated events, metrics and span kinds are bound to their exact schema
//! declaration and current version. Equal names or IDs do not make declarations
//! interchangeable. A manual [`Schema`] sets `identity: None` and uses explicit
//! raw-ID/payload APIs; it does not acquire a typed binding by matching a name.
//!
//! ```no_run
//! dduroc::schema! {
//!     name: radio,
//!     version: 1,
//!     languages: [en, ru],
//!
//!     events {
//!         PowerSet = 0x01 {
//!             level: Info,
//!             tags: [rf],
//!             en: "power set to {dbm} dBm",
//!             ru: "мощность {dbm} дБм",
//!             dbm: f32,
//!         },
//!         Overheat = 0x02 {
//!             level: Error,
//!             store: critical,
//!             en: "overheat: {t} °C",
//!             ru: "перегрев: {t} °C",
//!             t: f32,
//!         },
//!     }
//!
//!     metrics {
//!         // A continuous quantity with limits: outside `warn` is a warning,
//!         // outside `alarm` a fault. The upper bound is inclusive, hence `..=`.
//!         TempPa = 0x01 { type: f32, unit: "°C", tags: [thermal],
//!                         warn: ..=70.0, alarm: ..=85.0 },
//!         // The second sensor is a metric of its own rather than a dimension of
//!         // the first: there are no runtime tags, and what tells them apart takes
//!         // no room in every sample.
//!         TempLna = 0x02 { type: f32, unit: "°C", tags: [thermal] },
//!         // A state machine as a time series. The codes are explicit: positional
//!         // numbering would shift when a state was inserted into the middle.
//!         LinkState = 0x03 {
//!             states: [alarm Los = 0, warn Sync = 1, Lock = 2],
//!             tags: [rf],
//!         },
//!     }
//!
//!     spans {
//!         Calibration = 0x01,
//!     }
//! }
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use dduroc::prelude::*;
//!
//! let store = dduroc::Store::open(
//!     dduroc::StoreConfig::new("/data/logs")
//!         // The budget of a CLASS across the whole store: "all telemetry gets
//!         // this much, all logs get that much". The channels of every namespace
//!         // of a class draw on the shared budget, and the class's oldest block
//!         // is evicted whoever's namespace it lies in. with_budget_per_class
//!         // gives that budget to every class not named explicitly in .channel().
//!         .with_budget_per_class(64 << 20),
//! )?;
//!
//! // An instance of a microservice brings up its own namespace.
//! let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;
//!
//! // Writing returns nothing: logging must not influence control flow. Losses
//! // are counted in `store.stats()` and announced in the stream itself.
//! ns.log(radio::events::PowerSet { dbm: 27.5 });
//!
//! // A sample's type comes from the metric constant: a `Metric<f32>` will not
//! // take an integer.
//! let temp = ns.series(radio::metrics::TempPa)?;
//! temp.sample(36.6);
//!
//! // A state is the same `sample`: the code goes to disk while the name and the
//! // severity come from the schema. A state of another metric fails the type check.
//! let link = ns.series(radio::metrics::LinkState)?;
//! link.sample(radio::metrics::LinkState::Lock);
//!
//! // Bounds known only at runtime (the hardware model was determined by an
//! // external system) — with the same range expressions as in the schema. Never
//! // written to disk.
//! ns.set_thresholds(radio::metrics::TempPa, ..=60.0, ..=75.0)?;
//!
//! {
//!     let cal = ns.span(radio::spans::Calibration);
//!     cal.log(radio::events::PowerSet { dbm: 30.0 });
//! } // the span's end is written here
//!
//! // GPS arrived: the anchor is retroactive, and the records above get a UTC too.
//! store.record_sync(Utc::now(), SyncSource::Gps)?;
//!
//! // Whoever needs a verdict at the call site has the paired `try_*`.
//! if let Err(e) = ns.try_log(radio::events::Overheat { t: 91.0 }) {
//!     assert!(e.loses_record() || e.breaks_contract());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Examples
//!
//! Fuller scenarios live in the crate's `examples/`; each runs on its own and
//! prints what it did:
//!
//! - `01_device_writes` — the writing side: a whole schema, events, telemetry,
//!   spans, free text, time synchronization;
//! - `02_viewer_reads` — the reading side: queries, filters, windows in two time
//!   scales, restoring text and states, being honest about an incomplete answer;
//! - `03_schema_grows` — migrations: `history {}`, rules, reading through the
//!   steps and the explicit physical run `Namespace::migrate`;
//! - `04_operations` — operating it: channel policy, rotation, the store
//!   ceiling, accounting for and announcing losses;
//! - `05_quickstart` — a minimal critical event in a caller-selected directory;
//! - `06_bounded_reads` — bounded pages and cooperative cancellation;
//! - `07_small_resource_profile` — explicit queue, writer and read limits.
//!
//! `cargo run -p dduroc --example 01_device_writes`

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

// The macro generates paths of the form `::dduroc::...`, so the crate has to
// be visible to itself — otherwise `schema!` would not compile inside its own
// tests and doc examples.
extern crate self as dduroc;

/// Declare a namespace schema.
mod lossless;
pub use dduroc_macros::schema;
pub use lossless::{decode_fields, decode_payload_bounded, render_bounded};

/// Wall-clock time is [`chrono`], so as not to invent a date type of our own
/// and not to make a user add a dependency for one call.
pub use chrono;

/// Reading a store: the same layer on a device and in an offline viewer.
///
/// No separate dependency is needed — with one exception: firmware that only
/// writes can turn reading off (`default-features = false`).
#[cfg(feature = "read")]
pub use dduroc_read as read;

// Re-exports for the code the macro generates: a user need not add serde and
// postcard to their own dependencies.
#[doc(hidden)]
pub use postcard;
#[doc(hidden)]
pub use serde;
#[doc(hidden)]
pub use serde_json;

pub use dduroc_engine::MigrationReport;
pub use dduroc_engine::QueueByteLimits;
/// Bounded block access through the store's shared read backend.
pub use dduroc_engine::backend::{
    BlockPage, ChannelInfo, ChannelKey, ReadBackend, RootUsage, StoredBlock,
};
pub use dduroc_engine::channel::{ChannelConfig, ChannelOverride};
pub use dduroc_engine::epochs::SyncSource;
pub use dduroc_engine::limits::{EffectiveLimits, MetricLimits, SeverityFn, StateStatus};
pub use dduroc_engine::metric::{Blob, Metric, MetricState, MetricValue, NumericValue, Untyped};
pub use dduroc_engine::namespace::{Namespace, Series, SpanGuard};
#[cfg(feature = "recovery")]
pub use dduroc_engine::recovery;
pub use dduroc_engine::schema::{
    BoundedFieldsDecoder, BoundedMessageRenderer, DecodeError, DecodedValue, DecodedValueLimits,
    EventDecoders, EventDesc, FieldDesc, Language, MetricDesc, MetricKind, Migration,
    MigrationInput, MigrationOutcome, Range, Schema, SchemaIdentity, Severity, SpanDesc, SpanKind,
    StateDesc, StorageClass, Thresholds,
};
pub use dduroc_engine::staged::{OwnedValue, Payload};
pub use dduroc_engine::stats::Stats;
pub use dduroc_engine::store::{GroupPolicy, NsQuota, Store, StoreConfig, in_group};
pub use dduroc_engine::writer::QueueSizes;
pub use dduroc_engine::{Clock, Error, Result};
pub use dduroc_format::{
    BootCounter, BootTime, Compression, EventId, Level, MetricId, Micros, ProtocolVersion, SpanId,
    SpanKindId, Value, ValueType,
};

/// Everything needed for writing, plus the bridge to reading one's own store
/// (`StoreExt::reader`): the types and traits in one line.
pub mod prelude {
    pub use crate::{
        BootTime, Event, Level, MetricLimits, MetricState, NamespaceExt, OwnedValue, Severity,
        SpanExt, Store, StoreConfig, SyncSource, Thresholds,
    };
    #[cfg(feature = "read")]
    pub use crate::{ReaderExt, StoreExt};
    /// Wall-clock types for synchronization and UTC query bounds. Without a
    /// synchronization, records have only relative time and UTC queries report
    /// the unanchored runs.
    pub use chrono::{DateTime, Utc};
}

/// An event type declared by the [`schema!`] macro.
///
/// Implemented by the generated code; there is no need to implement it by hand.
///
/// An event with no fields is a unit struct: `ns.log(events::Started)`, with no
/// empty braces.
pub trait Event: serde::Serialize {
    /// Exact generated schema declaration and version.
    const SCHEMA: SchemaIdentity;
    /// The identifier within the schema.
    const ID: EventId;
    /// The level is a static property of the type and is never written to disk.
    const LEVEL: Level;
    /// The type's name, for interfaces.
    const NAME: &'static str;
}

// Logging only borrows an event while serializing it, so reusable payloads
// need not be cloned or moved just to satisfy the typed write API.
impl<E: Event + ?Sized> Event for &E {
    const SCHEMA: SchemaIdentity = E::SCHEMA;
    const ID: EventId = E::ID;
    const LEVEL: Level = E::LEVEL;
    const NAME: &'static str = E::NAME;
}

/// The payload of an event of some schema version — what can go into a record.
///
/// Implemented by the generated code for the events of the **current** version
/// (through [`Event`]) and for the types from `history {}`, the layouts of
/// earlier versions. A migration step returns any of them: usually the current
/// type, and in a long chain the next version's layout, which the next step
/// will bring up to the current one.
pub trait EventShape: serde::Serialize {
    /// The generated declaration owning this migration layout.
    const SHAPE_SCHEMA: SchemaIdentity;
    /// The event's identifier in the version the layout belongs to.
    const SHAPE_ID: EventId;
}

impl<E: Event> EventShape for E {
    const SHAPE_SCHEMA: SchemaIdentity = E::SCHEMA;
    const SHAPE_ID: EventId = E::ID;
}

/// Converting a rule's return into a [`MigrationOutcome`].
///
/// A rule's closure returns either a payload (an [`EventShape`] — the record is
/// re-encoded) or an `Option` of one (`None` deletes the record). The trait
/// exists so that the generated code accepts both without making every rule
/// write `Some(...)`.
pub trait IntoMigrationOutcome {
    /// Turn a rule's return into a step outcome.
    fn into_outcome(
        self,
        expected: SchemaIdentity,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError>;
}

impl<E: EventShape> IntoMigrationOutcome for E {
    fn into_outcome(
        self,
        expected: SchemaIdentity,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        if E::SHAPE_SCHEMA != expected {
            return Err(DecodeError);
        }
        Ok(Some(MigrationOutcome::Message {
            event: E::SHAPE_ID,
            payload: postcard::to_allocvec(&self).map_err(|_| DecodeError)?,
        }))
    }
}

impl<E: EventShape> IntoMigrationOutcome for Option<E> {
    fn into_outcome(
        self,
        expected: SchemaIdentity,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        match self {
            Some(e) => e.into_outcome(expected),
            None => Ok(None),
        }
    }
}

/// The type of a sample's value as a migration rule sees it.
///
/// A value is self-describing — the type lies in the record itself — so a rule
/// needs neither `history` nor a declaration of the earlier type: naming the
/// closure parameter's type is enough, and it also says what was expected on
/// disk.
///
/// The check is strict: a rule that declared `f32` refuses a record holding a
/// `u64` rather than converting silently. A conversion here would be a guess at
/// what the schema's author meant, and the price of being wrong is history
/// quietly rewritten.
#[diagnostic::on_unimplemented(
    message = "a sample value is never of this type: `{Self}`",
    label = "one of the types a metric's `type:` can be is needed here",
    note = "that is f32, f64, i64, u64, bool, or Vec<u8> for `type: blob`"
)]
pub trait SampleValue: Sized {
    /// Read the value as it lies on disk.
    fn from_wire(value: Value<'_>) -> std::result::Result<Self, DecodeError>;
    /// Hand the value back in the form it will lie in.
    fn into_wire(self) -> OwnedValue;
}

macro_rules! impl_sample_value {
    ($($t:ty => $variant:ident),* $(,)?) => { $(
        impl SampleValue for $t {
            fn from_wire(value: Value<'_>) -> std::result::Result<Self, DecodeError> {
                match value {
                    Value::$variant(v) => Ok(v),
                    _ => Err(DecodeError),
                }
            }
            fn into_wire(self) -> OwnedValue {
                OwnedValue::$variant(self)
            }
        }
    )* };
}

impl_sample_value!(f32 => F32, f64 => F64, i64 => I64, u64 => U64, bool => Bool);

impl SampleValue for Vec<u8> {
    fn from_wire(value: Value<'_>) -> std::result::Result<Self, DecodeError> {
        match value {
            Value::Blob(b) => Ok(b.to_vec()),
            _ => Err(DecodeError),
        }
    }
    fn into_wire(self) -> OwnedValue {
        OwnedValue::Blob(Payload::from_vec(self))
    }
}

/// Converting the return of a value-transforming rule into a step outcome.
///
/// The parameter `V` is the type **the schema declared for the metric**: a
/// rule's return has to be it (or an `Option` of it — `None` deletes the
/// sample). Without that parameter a migration could put a sample on disk whose
/// type contradicts the schema — exactly what a typed `sample` protects writing
/// from; and it would be found out not at build time but at display time: the
/// severity would be computed the wrong way, the state's label would not be
/// found, and the number would look like nonsense.
#[diagnostic::on_unimplemented(
    message = "the rule returned `{Self}` while the metric is declared as `{V}`",
    label = "the type from the schema is needed here — or an `Option` of it, to delete the sample",
    note = "a sample's type is a property of the metric, and a migration has no right to \
            change it: an ordinary `sample` does not let through a record whose type \
            nothing in the schema corresponds to either",
    note = "if the type really has to change, that is an edit to `type:` in the schema, \
            not a conversion inside a rule"
)]
pub trait IntoSampleOutcome<V: SampleValue> {
    /// Turn a rule's return into a step outcome for metric `metric`.
    fn into_sample_outcome(
        self,
        metric: MetricId,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError>;
}

impl<V: SampleValue> IntoSampleOutcome<V> for V {
    fn into_sample_outcome(
        self,
        metric: MetricId,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        Ok(Some(MigrationOutcome::Sample {
            metric,
            value: self.into_wire(),
        }))
    }
}

impl<V: SampleValue> IntoSampleOutcome<V> for Option<V> {
    fn into_sample_outcome(
        self,
        metric: MetricId,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        match self {
            Some(v) => v.into_sample_outcome(metric),
            None => Ok(None),
        }
    }
}

/// Call a value transformation from a typed rule. For the macro only. It
/// exists for the same reason as [`__migrate_map`]: the type of a closure's
/// parameter has to be known before its body is checked.
///
/// `D` is the metric's schema-declared type; the macro substitutes it
/// explicitly, and a rule's return has to match it.
#[doc(hidden)]
pub fn __migrate_value<T: SampleValue, D: SampleValue, O: IntoSampleOutcome<D>>(
    map: impl FnOnce(T) -> O,
    value: Value<'_>,
    metric: MetricId,
) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
    map(T::from_wire(value)?).into_sample_outcome(metric)
}

/// Call a typed rule's transformation. For the macro only.
///
/// It exists because of the order type inference takes on closures:
/// `(|old| old.dbm)(x)` does not compile — the body is checked before the
/// immediate call gets to suggest the parameter's type. Expecting an
/// `impl FnOnce(T)` announces it before the body is checked, and a rule is
/// written without an annotation: `|old| ...`.
#[doc(hidden)]
pub fn __migrate_map<T, O: IntoMigrationOutcome>(
    map: impl FnOnce(T) -> O,
    old: T,
    expected: SchemaIdentity,
) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
    map(old).into_outcome(expected)
}

/// An extension of [`Namespace`] with typed writing.
///
/// The writing methods **return nothing**: logging must not influence control
/// flow in application code, and there is nothing to do with a failure at the
/// call site — retrying against a lagging disk only makes it worse. Losses are
/// counted in [`Stats::dropped`] and announced in the record stream itself;
/// whoever needs an answer on the spot has a paired `try_*` on every method.
pub trait NamespaceExt {
    /// Write an event: the fields are serialized into a compact binary form.
    fn log<E: Event>(&self, event: E);

    /// Write an event, attaching it to a span.
    fn log_in<E: Event>(&self, span: &SpanGuard, event: E);

    /// The same with an answer about the outcome. An `Err` is told apart by two
    /// predicates: [`Error::loses_record`] means delivery was not confirmed
    /// (the record may nevertheless have persisted), [`Error::breaks_contract`]
    /// means a typed descriptor does not belong to this schema declaration.
    /// Critical success acknowledges a synchronous backend write; ordinary
    /// success acknowledges queue admission and needs [`Namespace::sync`] for
    /// persistence. Do not automatically retry an unconfirmed write.
    ///
    /// The verdict belongs to the caller entirely: a contract violation here
    /// does not reach the counters and is not announced in the journal — the
    /// "quiet" [`NamespaceExt::log`] does that. To hand a handled failure to
    /// the engine there is [`Namespace::note_failure`].
    fn try_log<E: Event>(&self, event: E) -> Result<()>;

    /// The same for an event inside a span. The span may belong to another
    /// namespace of this live Store; inactive or foreign-Store guards are refused.
    fn try_log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()>;
}

impl NamespaceExt for Namespace {
    #[inline]
    fn log<E: Event>(&self, event: E) {
        self.report_here(self.try_log(event));
    }

    #[inline]
    fn log_in<E: Event>(&self, span: &SpanGuard, event: E) {
        self.report_here(self.try_log_in(span, event));
    }

    #[inline]
    fn try_log<E: Event>(&self, event: E) -> Result<()> {
        self.check_schema(E::SCHEMA)?;
        self.try_log_payload(E::ID, encode(&event)?, None)
    }

    #[inline]
    fn try_log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()> {
        self.check_schema(E::SCHEMA)?;
        self.check_span(span)?;
        self.try_log_payload(E::ID, encode(&event)?, Some(span.id()))
    }
}

/// An extension of [`SpanGuard`] with typed writing.
pub trait SpanExt {
    /// Write an event inside a span.
    fn log<E: Event>(&self, event: E);

    /// The same with an answer about the outcome.
    fn try_log<E: Event>(&self, event: E) -> Result<()>;
}

impl SpanExt for SpanGuard {
    #[inline]
    fn log<E: Event>(&self, event: E) {
        let outcome = self.try_log(event);
        self.namespace().report_here(outcome);
    }

    #[inline]
    fn try_log<E: Event>(&self, event: E) -> Result<()> {
        self.namespace().check_schema(E::SCHEMA)?;
        self.namespace().check_span(self)?;
        self.try_log_payload(E::ID, encode(&event)?)
    }
}

/// An extension of [`Store`] with reading one's own store.
///
/// The reader is a separate type for a reason: reading is a different set of
/// guarantees (selection, corruption reports, offline dumps). An offline raft
/// view takes an exclusive engine lock; a live reader shares the Store engine. But
/// there is no reason to name **one's own** store a second time: it already has
/// its roots and schemas.
#[cfg(feature = "read")]
pub trait StoreExt {
    /// A live reader of this store: parallel to writing by construction.
    ///
    /// Created once and kept for as long as needed — it asks the store for the
    /// truth (the roots, the schemas, the time anchors) on every query, and
    /// compaction and new committed blocks are ordinary events rather than
    /// damage. What is visible is the committed block journal; fresh records
    /// have to be flushed first with [`Store::sync`]. The details are in
    /// [`read::Reader::of_store`].
    fn reader(&self) -> read::Reader;
}

#[cfg(feature = "read")]
impl StoreExt for std::sync::Arc<Store> {
    #[inline]
    fn reader(&self) -> read::Reader {
        read::Reader::of_store(self)
    }
}

/// An extension of [`read::Reader`] with typed parsing of events.
///
/// [`read::Reader::render`] gives the text from a template; here is the way
/// back to the **fields**: a record's `payload` is parsed into the same type
/// the event was written with. It lives on the reader rather than on the record
/// because an event identifier is unique only within a schema: a record of
/// another namespace may hold a different type under the same id, and parsing
/// it with a foreign layout would give plausible and wrong fields. The reader
/// checks both the entry's effective schema provenance and the selected
/// namespace schema against the generated type before decoding.
#[cfg(feature = "read")]
pub trait ReaderExt {
    /// The fields of an event, if the record is an event `E`.
    ///
    /// `None` means the record is not an event `E`: another type, telemetry,
    /// text, a span, or the record's namespace lives under a different schema
    /// (or its effective schema provenance is unknown or belongs to another
    /// declaration/version — it will not invent a parse).
    /// `Some(Err)` means the record declares itself an `E` but the fields did
    /// not parse: corruption, or a divergence of layout that must not be passed
    /// over.
    ///
    /// Segments of earlier versions are brought to the current layout while
    /// they are read (migrations), so `E` is always a type of the **current**
    /// schema.
    fn decode<E: Event + serde::de::DeserializeOwned>(
        &self,
        entry: &read::Entry,
    ) -> Option<std::result::Result<E, DecodeError>>;
}

#[cfg(feature = "read")]
impl ReaderExt for read::Reader {
    fn decode<E: Event + serde::de::DeserializeOwned>(
        &self,
        entry: &read::Entry,
    ) -> Option<std::result::Result<E, DecodeError>> {
        let read::EntryKind::Message { event, payload, .. } = &entry.kind else {
            return None;
        };
        if entry.schema_identity != Some(E::SCHEMA) || *event != E::ID {
            return None;
        }
        // A matching id is not yet a matching type: an id is unique within a
        // schema. The type is confirmed by the schema of the record's own
        // namespace.
        let schema = self.schema_of(&entry.namespace)?;
        if !schema.accepts(E::SCHEMA) || schema.event(*event)?.name != E::NAME {
            return None;
        }
        Some(postcard::from_bytes(payload).map_err(|_| DecodeError))
    }
}

/// Account for a serialization failure where the engine accounts for its own.
trait ReportHere {
    fn report_here(&self, outcome: Result<()>);
}

impl ReportHere for Namespace {
    #[inline]
    fn report_here(&self, outcome: Result<()>) {
        if let Err(e) = outcome {
            self.note_failure(e);
        }
    }
}

/// Serialize an event's fields straight into the write buffer.
///
/// This is where the cost of logging concentrates, so there is no intermediate
/// `Vec`: postcard writes into the very buffer that goes into the queue. At a
/// typical field size it stays inline, and an event never touches the heap at
/// all.
#[inline]
fn encode<E: Event>(event: &E) -> Result<Payload> {
    postcard::to_extend(event, Payload::new()).map_err(|_| Error::EncodeFailed { event: E::NAME })
}

#[cfg(test)]
mod tests {
    use super::*;

    schema! {
        name: testing,
        version: 1,
        languages: [en, ru],

        events {
            PowerSet = 0x01 {
                level: Info,
                tags: [rf],
                en: "power set to {dbm} dBm",
                ru: "мощность {dbm} дБм",
                dbm: f32,
            },
            Overheat = 0x02 {
                level: Error,
                store: critical,
                en: "overheat: {t:.1} °C on {sensor}",
                ru: "перегрев: {t:.1} °C на {sensor}",
                t: f32,
                sensor: u8,
            },
            Padded = 0x11 { level: Info, en: "{value:界>4}", ru: "{value:界>4}", value: u32 },
            Started = 0x10 {
                level: Debug,
                en: "started",
                ru: "запущено",
            },
        }

        metrics {
            Temp = 0x01 { type: f32, unit: "°C", tags: [thermal],
                          warn: ..=70.0, alarm: ..=85.0 },
            Spectrum = 0x02 { type: blob, store: telemetry },
            LinkState = 0x03 {
                states: [alarm Los = 0, warn Sync = 1, Lock = 2],
                tags: [rf],
            },
            Locked = 0x04 {
                type: bool,
                states: [alarm Unlocked = 0, Locked = 1],
            },
            Vswr = 0x05 {
                type: f32,
                warn_if: v > 1.5,
                alarm_if: v > 3.0 || v < 1.0,
            },
        }

        spans {
            Calibration = 0x01,
            PowerRamp = 0x02,
        }
    }

    mod collision {
        use super::*;
        schema! {
            name: testing, version: 1, languages: [en],
            events { PowerSet = 1 { level: Info, en: "power {dbm}", dbm: f32 } }
            metrics {
                Temp = 1 { type: f32 },
                LinkState = 3 { states: [Los = 0, Lock = 2] },
            }
            spans { Calibration = 1 }
        }
    }

    mod next_version {
        use super::*;
        schema! {
            name: testing, version: 2, languages: [en],
            events { PowerSet = 1 { level: Info, en: "power {dbm}", dbm: f32 } }
            migrations { 1 => migrate_v1 }
        }
    }

    #[test]
    fn schema_binding_rejects_collisions_before_serialization_and_mutation() {
        struct MustNotSerialize;
        impl serde::Serialize for MustNotSerialize {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                panic!("foreign event must be refused before serialization")
            }
        }
        impl Event for MustNotSerialize {
            const SCHEMA: SchemaIdentity = collision::testing::SCHEMA.identity.unwrap();
            const ID: EventId = EventId(1);
            const LEVEL: Level = Level::Info;
            const NAME: &'static str = "PowerSet";
        }
        let dir = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024))
                .unwrap();
        let ns = store.namespace("same-name", testing::SCHEMA).unwrap();
        let conflict = store
            .namespace("second-name", collision::testing::SCHEMA)
            .unwrap_err();
        assert!(conflict.breaks_contract());
        assert!(
            !dir.path().join("second-name").exists(),
            "schema conflict precedes metadata effects"
        );
        let _same_scope = store.namespace("same-scope", testing::SCHEMA).unwrap();
        let version_conflict = store
            .namespace("next-version", next_version::testing::SCHEMA)
            .unwrap_err();
        assert!(matches!(version_conflict, Error::ForeignSchema { .. }));
        assert!(!dir.path().join("next-version").exists());
        assert!(ns.try_log(MustNotSerialize).unwrap_err().breaks_contract());
        assert!(
            ns.try_log(collision::testing::events::PowerSet { dbm: 3.0 })
                .is_err()
        );
        assert!(ns.series(collision::testing::metrics::Temp).is_err());
        assert!(ns.series(collision::testing::metrics::LinkState).is_err());
        assert!(
            ns.set_thresholds(collision::testing::metrics::Temp, ..=3.0, ..=4.0)
                .is_err()
        );
        assert!(
            ns.set_limits(collision::testing::metrics::Temp, MetricLimits::default())
                .is_err()
        );
        assert!(ns.clear_limits(collision::testing::metrics::Temp).is_err());
        assert!(
            ns.set_severity_fn(collision::testing::metrics::Temp, |_| Severity::Alarm)
                .is_err()
        );
        assert!(
            ns.clear_severity_fn(collision::testing::metrics::Temp)
                .is_err()
        );
        assert!(ns.limits(collision::testing::metrics::Temp).is_err());
        assert!(
            ns.severity_of(collision::testing::metrics::Temp, 2.0)
                .is_err()
        );
        assert!(ns.try_span(collision::testing::spans::Calibration).is_err());
        assert_eq!(store.stats().records_written, 0);
        assert_eq!(
            store.stats().rejected,
            0,
            "handled contract failures are not diagnostics"
        );
        assert!(!testing::SCHEMA.accepts(collision::testing::SCHEMA.identity.unwrap()));
        let changed_version = Schema {
            version: ProtocolVersion(2),
            ..testing::SCHEMA
        };
        assert!(!changed_version.accepts(testing::SCHEMA.identity.unwrap()));
        // Raw writing is explicit and stays available to manual/foreign format code.
        ns.try_log_raw(
            EventId(1),
            &postcard::to_allocvec(&testing::events::PowerSet { dbm: 2.0 }).unwrap(),
            None,
        )
        .unwrap();
        ns.sync().unwrap();
        assert_eq!(store.stats().records_written, 1);
        #[cfg(feature = "read")]
        {
            let reader = store.reader();
            let entry = reader.query(&read::Query::new()).unwrap().entries.remove(0);
            assert_eq!(
                reader
                    .decode::<testing::events::PowerSet>(&entry)
                    .unwrap()
                    .unwrap()
                    .dbm,
                2.0
            );
            assert!(
                reader
                    .decode::<collision::testing::events::PowerSet>(&entry)
                    .is_none()
            );
        }
    }

    #[test]
    fn typed_migration_output_must_belong_to_the_target_declaration() {
        let identity = testing::SCHEMA.identity.unwrap();
        assert!(
            __migrate_map(
                |()| collision::testing::events::PowerSet { dbm: 1.0 },
                (),
                identity
            )
            .is_err()
        );
        assert!(
            __migrate_map(|()| testing::events::PowerSet { dbm: 1.0 }, (), identity)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn rejected_span_is_inert_even_when_explicitly_closed() {
        struct MustNotSerialize;
        impl serde::Serialize for MustNotSerialize {
            fn serialize<S: serde::Serializer>(
                &self,
                _: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                panic!("inactive span must be refused before serialization")
            }
        }
        impl Event for MustNotSerialize {
            const SCHEMA: SchemaIdentity = testing::SCHEMA.identity.unwrap();
            const ID: EventId = EventId(1);
            const LEVEL: Level = Level::Info;
            const NAME: &'static str = "PowerSet";
        }
        let dir = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024))
                .unwrap();
        let ns = store.namespace("same-name", testing::SCHEMA).unwrap();
        let invalid = ns.span(collision::testing::spans::Calibration);
        assert_eq!(invalid.id(), SpanId(0));
        assert!(invalid.try_child(testing::spans::Calibration).is_err());
        assert!(invalid.try_log(testing::events::Started).is_err());
        assert!(invalid.try_log(MustNotSerialize).is_err());
        assert!(ns.try_log_in(&invalid, MustNotSerialize).is_err());
        assert!(invalid.close().is_err());
        ns.sync().unwrap();
        #[cfg(feature = "read")]
        assert!(
            store
                .reader()
                .query(&read::Query::new().kinds(read::KindFilter::SPANS))
                .unwrap()
                .entries
                .is_empty()
        );
    }

    #[cfg(feature = "read")]
    #[test]
    fn typed_decode_checks_entry_provenance_across_readers_with_equal_names() {
        let a_dir = tempfile::tempdir().unwrap();
        let b_dir = tempfile::tempdir().unwrap();
        let a = Store::open(StoreConfig::new(a_dir.path()).with_budget_per_class(16 * 1024 * 1024))
            .unwrap();
        let b = Store::open(StoreConfig::new(b_dir.path()).with_budget_per_class(16 * 1024 * 1024))
            .unwrap();
        let a_ns = a.namespace("same-name", testing::SCHEMA).unwrap();
        let _b_ns = b
            .namespace("same-name", collision::testing::SCHEMA)
            .unwrap();
        a_ns.try_log(testing::events::PowerSet { dbm: 2.0 })
            .unwrap();
        a_ns.sync().unwrap();
        let a_reader = a.reader();
        let b_reader = b.reader();
        let entry = a_reader
            .query(&read::Query::new())
            .unwrap()
            .entries
            .remove(0);
        assert!(
            a_reader
                .decode::<testing::events::PowerSet>(&entry)
                .unwrap()
                .is_ok()
        );
        assert!(
            a_reader
                .decode::<collision::testing::events::PowerSet>(&entry)
                .is_none()
        );
        assert!(
            b_reader
                .decode::<collision::testing::events::PowerSet>(&entry)
                .is_none()
        );
    }

    /// Migration steps live next to the schema declaration; the macro expands
    /// into a module of its own, but the names from this scope are visible
    /// there.
    fn migrate_v1(
        _: MigrationInput<'_>,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        Ok(Some(MigrationOutcome::AsIs))
    }
    fn migrate_v2(
        _: MigrationInput<'_>,
    ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        Ok(Some(MigrationOutcome::AsIs))
    }

    schema! {
        name: migrating,
        version: 3,
        languages: [en],

        events {
            Renamed = 0x01 { level: Info, en: "renamed" },
            Untouched = 0x02 { level: Info, en: "untouched" },
        }

        metrics {
            Temp = 0x01 { type: f32 },
        }

        // The key is the version being migrated FROM: the chain has to be
        // unbroken from 1 to the current one.
        migrations {
            // The affected types are named: segments without them are not
            // rewritten.
            1 => migrate_v1 { events: [Renamed], metrics: [Temp] },
            // Not named means it affects everything.
            2 => migrate_v2,
        }
    }

    #[test]
    fn migration_touches_everything_unless_told_otherwise() {
        // The type sets in the footer decide whether a segment is rewritten.
        // Empty lists meant "affects nothing", and a step whose lists were
        // forgotten would silently walk past the whole history — and that would
        // only be discovered on unreadable logs a month later.
        use dduroc_format::{FooterBuilder, MetricId};

        migrating::SCHEMA.validate().expect("the schema is valid");

        // Types reach a segment's sets together with the block they were met
        // in: a block that did not fit a segment moves to the next, and its
        // types have to move with it. So the footer is assembled the way the
        // writer assembles it — the records, then the block.
        let footer_of = |events: &[EventId], metrics: &[MetricId]| {
            use dduroc_format::block::{BlockHeader, Compression};
            let mut b = FooterBuilder::new();
            for e in events {
                b.add_event(*e);
            }
            for m in metrics {
                b.add_metric(*m);
            }
            b.add_block(
                32,
                &BlockHeader {
                    body_len: 10,
                    raw_len: 10,
                    seq: 0,
                    base: dduroc_format::Micros(0),
                    count: 1,
                    compression: Compression::None,
                    crc: 0,
                },
                dduroc_format::Micros(1),
            );
            let bytes = b.build();
            dduroc_format::Footer::parse(&bytes).unwrap().unwrap()
        };

        let narrow = migrating::SCHEMA
            .migration(1)
            .expect("the step is declared");
        assert!(
            !narrow.touches_all,
            "the affected types are named explicitly"
        );
        assert_eq!(narrow.events, &[EventId(1)]);
        assert_eq!(narrow.metrics, &[MetricId(1)]);
        assert!(
            narrow.touches(&footer_of(&[EventId(1)], &[])),
            "a segment holding an affected event is rewritten"
        );
        assert!(
            !narrow.touches(&footer_of(&[EventId(2)], &[])),
            "a segment with no affected types spends no flash write cycle"
        );

        let wide = migrating::SCHEMA
            .migration(2)
            .expect("the step is declared");
        assert!(
            wide.touches_all,
            "the lists are not declared — the step must count as affecting everything"
        );
        assert!(
            wide.touches(&footer_of(&[EventId(2)], &[])),
            "skipping a segment silently is worse than rewriting a superfluous one"
        );
        assert!(wide.touches(&footer_of(&[], &[])), "and an empty one too");
    }

    #[test]
    fn channel_policy_is_configurable_through_the_facade_alone() {
        // The facade is an application's only dependency, so everything
        // appearing in the signatures of its own re-exports has to be named by
        // it. `Compression` was not re-exported, and configuring a channel's
        // compression without adding `dduroc-engine` to the project by hand was
        // impossible — even though the field in `ChannelConfig` is public. The
        // directory name is derived from the class: there is no name in the
        // config at all, and sending a class's records into a foreign directory
        // by a typo became unrepresentable.
        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path())
            .with_budget_per_class(16 * 1024 * 1024)
            .channel(
                StorageClass::Telemetry,
                ChannelConfig {
                    compression: Compression::None,
                    sync_interval: std::time::Duration::from_secs(60),
                    ..ChannelConfig::new(16 * 1024 * 1024)
                },
            );
        {
            let store = Store::open(config).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();
            ns.series(testing::metrics::Spectrum)
                .unwrap()
                .sample(vec![1u8, 2, 3]);
            ns.sync().unwrap();
            assert!(store.stats().is_clean(), "{:?}", store.stats());
            store.shutdown();
        }

        #[cfg(feature = "read")]
        {
            let reader = read::Reader::open_dump([dir.path()], &[testing::SCHEMA]).unwrap();
            let listing = reader.namespaces().unwrap();
            assert!(
                listing.namespaces[0]
                    .channels
                    .contains(&StorageClass::Telemetry)
            );
            assert!(listing.namespaces[0].total_bytes > 0);
        }
    }

    #[test]
    fn an_invalid_channel_config_is_refused_at_open() {
        // Channel settings arrive from outside and until now were checked
        // nowhere: a budget smaller than two segments reached the writer as it
        // was, and rotation ate the only segment right after it was sealed.
        let dir = tempfile::tempdir().unwrap();
        let broken = StoreConfig::new(dir.path()).channel(
            StorageClass::Default,
            ChannelConfig {
                budget_bytes: 4 * 1024 * 1024,
                segment_bytes: 4 * 1024 * 1024,
                ..ChannelConfig::new(4 * 1024 * 1024)
            },
        );
        let err = Store::open(broken).expect_err("a budget of one segment is meaningless");
        assert!(
            matches!(err, Error::BadChannel { .. }),
            "the refusal came from the channel-settings check itself: {err}"
        );
    }

    #[test]
    fn a_lagging_critical_channel_is_refused_at_open() {
        // Immediate syncing is the definition of the critical class, not a
        // setting: a channel the caller is prepared to wait in a queue for has
        // no right to fall behind the medium. An interval on the critical one
        // is a configuration error, and overriding it silently is not an
        // option: the operator would believe the setting was in force.
        let dir = tempfile::tempdir().unwrap();
        let lagging = StoreConfig::new(dir.path()).channel(
            StorageClass::Critical,
            ChannelConfig {
                sync_interval: std::time::Duration::from_secs(10),
                ..ChannelConfig::critical(16 * 1024 * 1024)
            },
        );
        let err = Store::open(lagging).expect_err("the critical channel cannot fall behind");
        assert!(matches!(err, Error::BadChannel { .. }), "{err}");
        assert!(err.to_string().contains("critical"), "{err}");
    }

    #[test]
    fn generated_schema_is_valid() {
        testing::SCHEMA.validate().expect("the schema is valid");
        assert_eq!(testing::SCHEMA.name, "testing");
        assert_eq!(testing::SCHEMA.version, ProtocolVersion(1));
        assert_eq!(testing::SCHEMA.events.len(), 4);
        assert_eq!(testing::SCHEMA.metrics.len(), 5);
        assert_eq!(testing::SCHEMA.spans.len(), 2);
    }

    #[test]
    fn storage_class_comes_from_declaration() {
        let overheat = testing::SCHEMA.event(EventId(2)).unwrap();
        assert_eq!(overheat.class, StorageClass::Critical);
        assert_eq!(overheat.level, Level::Error);

        let power = testing::SCHEMA.event(EventId(1)).unwrap();
        assert_eq!(power.class, StorageClass::Default);
        assert_eq!(power.tags, &["rf"]);

        let spectrum = testing::SCHEMA.metric(MetricId(2)).unwrap();
        assert_eq!(spectrum.class, StorageClass::Telemetry);
        assert_eq!(spectrum.value_type, ValueType::Blob);
    }

    #[test]
    fn decoders_render_and_serialize() {
        let event = testing::events::Overheat {
            t: 87.25,
            sensor: 3,
        };
        let payload = encode(&event).unwrap();

        let desc = testing::SCHEMA.event(EventId(2)).unwrap();
        let decoders = desc.decoders.expect("the macro generated the decoders");

        // Language 0 is en, language 1 is ru: the order from `languages:`.
        assert_eq!(
            (decoders.render)(&payload, 0).unwrap(),
            "overheat: 87.2 °C on 3"
        );
        assert_eq!(
            (decoders.render)(&payload, 1).unwrap(),
            "перегрев: 87.2 °C на 3"
        );
        // An unknown language does not panic.
        assert!((decoders.render)(&payload, 99).is_ok());

        let json = (decoders.json)(&payload).unwrap();
        assert!(json.contains("\"t\":87.25"), "got {json}");
        assert!(json.contains("\"sensor\":3"));

        // A garbage payload is an error, not a panic.
        assert!((decoders.render)(&[0xFF, 0xFF, 0xFF, 0xFF], 0).is_err());
    }

    #[test]
    fn typed_writes_accept_reusable_borrowed_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        let ns = store.namespace("borrowed", testing::SCHEMA).unwrap();
        let event = testing::events::PowerSet { dbm: 7.0 };
        ns.try_log(&event).unwrap();
        let span = ns.try_span(testing::spans::Calibration).unwrap();
        ns.try_log_in(&span, &event).unwrap();
        span.try_log(&event).unwrap();
        assert_eq!(event.dbm, 7.0);
        span.close().unwrap();
        ns.sync().unwrap();
        #[cfg(feature = "read")]
        {
            let reader = store.reader();
            let answer = reader.query(&read::Query::new()).unwrap();
            let values: Vec<_> = answer
                .entries
                .iter()
                .filter_map(|entry| reader.decode::<testing::events::PowerSet>(entry))
                .map(|value| value.unwrap().dbm)
                .collect();
            assert_eq!(values, vec![7.0, 7.0, 7.0]);
        }
    }

    #[test]
    fn generated_renderers_preserve_unicode_format_fill() {
        let bytes = encode(&testing::events::Padded { value: 7 }).unwrap();
        let decoders = testing::SCHEMA
            .event(EventId(0x11))
            .unwrap()
            .decoders
            .unwrap();
        assert_eq!((decoders.render)(&bytes, 0).unwrap(), "界界界7");
        assert_eq!(
            (decoders.render_bounded.unwrap())(
                &bytes,
                0,
                DecodedValueLimits {
                    max_depth: 8,
                    max_nodes: 16,
                    max_bytes: 1024
                },
                10
            )
            .unwrap(),
            "界界界7"
        );
    }

    #[test]
    fn event_without_fields_renders_template_as_is() {
        let payload = encode(&testing::events::Started).unwrap();
        assert!(
            payload.is_empty(),
            "an event with no fields has an empty payload"
        );
        let d = testing::SCHEMA
            .event(EventId(0x10))
            .unwrap()
            .decoders
            .unwrap();
        assert_eq!((d.render)(&payload, 0).unwrap(), "started");
        assert_eq!((d.render)(&payload, 1).unwrap(), "запущено");
    }

    #[test]
    fn payload_is_compact() {
        // f32 + u8 = 5 bytes: neither field names nor types are on disk.
        let payload = encode(&testing::events::Overheat { t: 1.0, sensor: 2 }).unwrap();
        assert_eq!(payload.len(), 5, "got {payload:?}");
    }

    #[test]
    fn end_to_end_write_and_read() {
        use dduroc_read::{KindFilter, Order, Query, Reader};

        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
        {
            let store = Store::open(config.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

            ns.log(testing::events::PowerSet { dbm: 27.5 });
            ns.log(testing::events::Overheat { t: 91.0, sensor: 1 });

            let temp = ns.series(testing::metrics::Temp).unwrap();
            temp.sample(36.6);

            {
                let cal = ns.span(testing::spans::Calibration);
                cal.log(testing::events::PowerSet { dbm: 30.0 });
            }

            ns.sync().unwrap();
            assert!(store.stats().is_clean(), "{:?}", store.stats());
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[testing::SCHEMA]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();
        assert!(result.is_complete());
        assert_eq!(result.entries.len(), 3);

        // The text was restored from the schema's template: it was not on disk.
        let rendered: Vec<String> = result
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                dduroc_read::EntryKind::Message { event, payload, .. } => {
                    dduroc_read::render_with_schema(&testing::SCHEMA, *event, payload, "ru")
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            rendered,
            vec![
                "мощность 27.5 дБм",
                "перегрев: 91.0 °C на 1",
                "мощность 30 дБм"
            ]
        );

        // An event inside a span is attached to it.
        let in_span = result.entries.iter().filter(|e| e.span.is_some()).count();
        assert_eq!(in_span, 1);
    }

    #[test]
    fn macro_generates_typed_states_and_limits() {
        // A check of what the macro gives: the metric constant and the Rust
        // type of its states under one name (values and types have different
        // namespaces), the labels and severities in the schema, the limits from
        // the ranges.
        testing::SCHEMA.validate().expect("the schema is valid");

        let link = testing::SCHEMA.metric(MetricId(3)).unwrap();
        assert_eq!(link.kind, MetricKind::State);
        assert_eq!(
            link.value_type,
            ValueType::U64,
            "a state code is an integer"
        );
        assert_eq!(link.tags, &["rf"]);
        assert_eq!(link.states.len(), 3);
        assert_eq!(link.state(0).unwrap().name, "Los");
        assert_eq!(link.state(0).unwrap().severity, Severity::Alarm);
        assert_eq!(link.state(1).unwrap().severity, Severity::Warn);
        assert_eq!(link.state(2).unwrap().severity, Severity::Normal);

        // One name, two namespaces: the metric constant and the type of its
        // states.
        let id: MetricId = testing::metrics::LinkState.into();
        assert_eq!(id, MetricId(3));
        assert_eq!(
            <testing::metrics::LinkState as MetricState>::metric(),
            MetricId(3)
        );
        assert_eq!(testing::metrics::LinkState::Lock.code(), 2);
        assert_eq!(testing::metrics::LinkState::Los.name(), "Los");

        // A bool as two states.
        let locked = testing::SCHEMA.metric(MetricId(4)).unwrap();
        assert_eq!(locked.value_type, ValueType::Bool);
        assert_eq!(locked.kind, MetricKind::State);
        assert_eq!(locked.state(0).unwrap().severity, Severity::Alarm);

        // Limits from ranges: the upper bound is inclusive.
        let temp = testing::SCHEMA.metric(MetricId(1)).unwrap();
        assert_eq!(temp.thresholds.warn.max, Some(70.0));
        assert_eq!(temp.thresholds.warn.min, None);
        assert_eq!(temp.thresholds.alarm.max, Some(85.0));
        assert_eq!(
            temp.severity_of(&dduroc_format::Value::F32(70.0)),
            Severity::Normal
        );
        assert_eq!(
            temp.severity_of(&dduroc_format::Value::F32(71.0)),
            Severity::Warn
        );
        assert_eq!(
            temp.severity_of(&dduroc_format::Value::F32(90.0)),
            Severity::Alarm
        );

        // A metric with neither limits nor states.
        let spec = testing::SCHEMA.metric(MetricId(2)).unwrap();
        assert!(spec.thresholds.is_unset());
        assert!(spec.states.is_empty());
        assert_eq!(spec.kind, MetricKind::Gauge);
    }

    #[test]
    fn predicate_limits_are_compiled_into_the_descriptor() {
        // `warn_if`/`alarm_if` are trigger conditions: VSWR is critical both
        // above and below one, which a range of what is normal cannot express.
        // The macro compiles the expression into a descriptor fn, and everyone
        // who asks for a severity uses it — a reader of a dump with the same
        // schema included.
        use dduroc_format::Value;
        let vswr = testing::SCHEMA.metric(MetricId(5)).unwrap();
        assert!(
            vswr.thresholds.is_unset(),
            "there is no data — only predicates"
        );
        assert_eq!(vswr.severity_of(&Value::F32(1.2)), Severity::Normal);
        assert_eq!(vswr.severity_of(&Value::F32(2.0)), Severity::Warn);
        assert_eq!(vswr.severity_of(&Value::F32(3.5)), Severity::Alarm);
        assert_eq!(
            vswr.severity_of(&Value::F32(0.5)),
            Severity::Alarm,
            "triggering from below is what the predicate is for"
        );
    }

    #[test]
    fn a_severity_fn_with_captured_context_overrides_everything() {
        // Limits the data cannot express: a closure with captured context takes
        // the diagnosis over entirely and can be removed again.
        let dir = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024))
                .unwrap();
        let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

        let hw_max = 40.0;
        ns.set_severity_fn(testing::metrics::Temp, move |v| {
            if v > hw_max {
                Severity::Alarm
            } else {
                Severity::Normal
            }
        })
        .unwrap();
        assert_eq!(
            ns.severity_of(testing::metrics::Temp, 75.0).unwrap(),
            Severity::Alarm,
            "by the schema it would be Warn: the closure won"
        );
        assert!(ns.limits(testing::metrics::Temp).unwrap().has_severity_fn);

        ns.clear_severity_fn(testing::metrics::Temp).unwrap();
        assert_eq!(
            ns.severity_of(testing::metrics::Temp, 75.0).unwrap(),
            Severity::Warn,
            "removing it brings the schema ranges back"
        );
        store.shutdown();
    }

    #[test]
    fn states_roundtrip_through_disk_as_bare_codes() {
        use dduroc_read::{EntryKind, KindFilter, Order, Query, Reader};

        let dir = tempfile::tempdir().unwrap();
        let config = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
        {
            let store = Store::open(config.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

            let link = ns.series(testing::metrics::LinkState).unwrap();
            for st in [
                testing::metrics::LinkState::Los,
                testing::metrics::LinkState::Sync,
                testing::metrics::LinkState::Lock,
            ] {
                link.sample(st);
            }
            let locked = ns.series(testing::metrics::Locked).unwrap();
            locked.sample(testing::metrics::Locked::Locked);

            ns.sync().unwrap();
            assert!(store.stats().is_clean(), "{:?}", store.stats());
            assert_eq!(
                store.stats().records_written,
                4,
                "not one bookkeeping record: a series is identified by its metric"
            );
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[testing::SCHEMA]).unwrap();
        let result = reader
            .query(
                &Query::new()
                    .order(Order::Oldest)
                    .kinds(KindFilter::TELEMETRY),
            )
            .unwrap();
        assert!(result.is_complete());

        let seen: Vec<(&str, Severity)> = result
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                EntryKind::Sample {
                    state_name: Some(name),
                    severity: Some(sev),
                    ..
                } => Some((*name, *sev)),
                _ => None,
            })
            .collect();
        assert_eq!(
            seen,
            vec![
                ("Los", Severity::Alarm),
                ("Sync", Severity::Warn),
                ("Lock", Severity::Normal),
                ("Locked", Severity::Normal),
            ],
            "the labels and severities were restored from the schema — they were not on disk"
        );
    }

    #[test]
    fn every_lost_record_is_accounted_for_in_the_stream() {
        // A hole nobody mentions is indistinguishable from silence. Loss
        // notices are flushed on a timer, so losses that happened between the
        // last timer and the stop reach the stream only if they are flushed at
        // shutdown as well — and that is exactly when the queue is most often
        // full: a process is stopped under load.
        //
        // We stop without a `sync`: what is being checked is the stopping path
        // itself.
        use dduroc_read::{EntryKind, KindFilter, Order, Query, Reader};

        let dir = tempfile::tempdir().unwrap();
        // A tiny queue makes the overflow reproducible rather than dependent on
        // what else the machine is busy with.
        let config = StoreConfig::new(dir.path())
            .with_budget_per_class(32 * 1024 * 1024)
            .with_queues(QueueSizes {
                normal: 4,
                critical: 4,
            });
        let refused = {
            let store = Store::open(config.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", testing::SCHEMA).unwrap();

            let mut refused = 0u64;
            for _ in 0..20_000 {
                if let Err(e) = ns.try_log(testing::events::PowerSet { dbm: 27.5 }) {
                    assert!(e.loses_record(), "a loss, not a defect in the call: {e}");
                    refused += 1;
                }
            }
            store.shutdown();
            assert_eq!(
                store.stats().dropped,
                refused,
                "the counter must match the number of refusals"
            );
            refused
        };
        assert!(
            refused > 0,
            "the test is meaningless without a queue overflow"
        );

        let reader = Reader::open_dump([dir.path()], &[testing::SCHEMA]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter {
                text: true,
                ..KindFilter::LOGS
            }))
            .unwrap();

        let announced: u64 = result
            .entries
            .iter()
            .filter_map(|e| match &e.kind {
                EntryKind::Text { text, .. } => text
                    .strip_prefix("records lost: ")
                    .and_then(|rest| rest.split_whitespace().next())
                    .and_then(|n| n.parse::<u64>().ok()),
                _ => None,
            })
            .sum();

        assert_eq!(
            announced, refused,
            "{announced} losses announced in the stream against {refused} real ones: \
             the remainder is not flushed at shutdown"
        );
    }
}
