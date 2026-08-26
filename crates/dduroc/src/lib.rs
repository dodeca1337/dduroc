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
//!         // of a class draw on the shared budget, and the class's oldest segment
//!         // is evicted whoever's namespace it lies in. with_budget_per_class
//!         // gives that budget to every class not named explicitly in .channel().
//!         .with_budget_per_class(4 << 30),
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
//!   ceiling, accounting for and announcing losses.
//!
//! `cargo run -p dduroc --example 01_device_writes`

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

// The macro generates paths of the form `::dduroc::...`, so the crate has to
// be visible to itself — otherwise `schema!` would not compile inside its own
// tests and doc examples.
extern crate self as dduroc;

/// Declare a namespace schema.
pub use dduroc_macros::schema;

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
pub use dduroc_engine::channel::{ChannelConfig, ChannelOverride};
pub use dduroc_engine::epochs::SyncSource;
pub use dduroc_engine::limits::{EffectiveLimits, MetricLimits, SeverityFn, StateStatus};
pub use dduroc_engine::metric::{Blob, Metric, MetricState, MetricValue, NumericValue, Untyped};
pub use dduroc_engine::namespace::{Namespace, Series, SpanGuard};
pub use dduroc_engine::schema::{
    DecodeError, EventDecoders, EventDesc, FieldDesc, Language, MetricDesc, MetricKind, Migration,
    MigrationInput, MigrationOutcome, Range, Schema, Severity, SpanDesc, StateDesc, StorageClass,
    Thresholds,
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
/// ([`StoreExt::reader`]): the types and traits in one line.
pub mod prelude {
    pub use crate::{
        BootTime, Event, Level, MetricLimits, MetricState, NamespaceExt, OwnedValue, Severity,
        SpanExt, Store, StoreConfig, SyncSource, Thresholds,
    };
    #[cfg(feature = "read")]
    pub use crate::{ReaderExt, StoreExt};
    /// Wall-clock time is needed for exactly [`crate::Store::record_sync`], but
    /// it is needed almost always: without a synchronization records have only
    /// relative time.
    pub use chrono::{DateTime, Utc};
}

/// An event type declared by the [`schema!`] macro.
///
/// Implemented by the generated code; there is no need to implement it by hand.
///
/// An event with no fields is a unit struct: `ns.log(events::Started)`, with no
/// empty braces.
pub trait Event: serde::Serialize {
    /// The identifier within the schema.
    const ID: EventId;
    /// The level is a static property of the type and is never written to disk.
    const LEVEL: Level;
    /// The type's name, for interfaces.
    const NAME: &'static str;
}

/// The payload of an event of some schema version — what can go into a record.
///
/// Implemented by the generated code for the events of the **current** version
/// (through [`Event`]) and for the types from `history {}`, the layouts of
/// earlier versions. A migration step returns any of them: usually the current
/// type, and in a long chain the next version's layout, which the next step
/// will bring up to the current one.
pub trait EventShape: serde::Serialize {
    /// The event's identifier in the version the layout belongs to.
    const SHAPE_ID: EventId;
}

impl<E: Event> EventShape for E {
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
    fn into_outcome(self) -> std::result::Result<Option<MigrationOutcome>, DecodeError>;
}

impl<E: EventShape> IntoMigrationOutcome for E {
    fn into_outcome(self) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        Ok(Some(MigrationOutcome::Message {
            event: E::SHAPE_ID,
            payload: postcard::to_allocvec(&self).map_err(|_| DecodeError)?,
        }))
    }
}

impl<E: EventShape> IntoMigrationOutcome for Option<E> {
    fn into_outcome(self) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
        match self {
            Some(e) => e.into_outcome(),
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
) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
    map(old).into_outcome()
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
    /// predicates: [`Error::loses_record`] means the record did not reach the
    /// medium, [`Error::breaks_contract`] that the event is not from this
    /// schema.
    ///
    /// The verdict belongs to the caller entirely: a contract violation here
    /// does not reach the counters and is not announced in the journal — the
    /// "quiet" [`NamespaceExt::log`] does that. To hand a handled failure to
    /// the engine there is [`Namespace::note_failure`].
    fn try_log<E: Event>(&self, event: E) -> Result<()>;

    /// The same for an event inside a span.
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
        self.try_log_payload(E::ID, encode(&event)?, None)
    }

    #[inline]
    fn try_log_in<E: Event>(&self, span: &SpanGuard, event: E) -> Result<()> {
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
        self.try_log_payload(E::ID, encode(&event)?)
    }
}

/// An extension of [`Store`] with reading one's own store.
///
/// The reader is a separate type for a reason: reading is a different set of
/// guarantees (change nothing, survive corruption, show a foreign dump), and a
/// viewer must not open a `Store` at all — that takes a lock on the root. But
/// there is no reason to name **one's own** store a second time: it already has
/// its roots and schemas.
#[cfg(feature = "read")]
pub trait StoreExt {
    /// A live reader of this store: parallel to writing by construction.
    ///
    /// Created once and kept for as long as needed — it asks the store for the
    /// truth (the roots, the schemas, the time anchors) on every query, and
    /// rotation and a segment's growing tail are ordinary events for it rather
    /// than damage. What is visible is what is on the medium; fresh records
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
/// knows the schema of the record's namespace and checks the type against it.
#[cfg(feature = "read")]
pub trait ReaderExt {
    /// The fields of an event, if the record is an event `E`.
    ///
    /// `None` means the record is not an event `E`: another type, telemetry,
    /// text, a span, or the record's namespace lives under a different schema
    /// (or the schema is unknown to the reader — it will not invent a parse).
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
        if *event != E::ID {
            return None;
        }
        // A matching id is not yet a matching type: an id is unique within a
        // schema. The type is confirmed by the schema of the record's own
        // namespace.
        let schema = self.schema_of(&entry.namespace)?;
        if schema.event(*event)?.name != E::NAME {
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

        let channels: Vec<String> = std::fs::read_dir(dir.path().join("orc-radio-0"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect();
        assert!(
            channels.iter().any(|c| c == "telemetry"),
            "the directory is named after the class: {channels:?}"
        );
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
        assert_eq!(testing::SCHEMA.events.len(), 3);
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
            ns.severity_of(testing::metrics::Temp, 75.0),
            Severity::Alarm,
            "by the schema it would be Warn: the closure won"
        );
        assert!(ns.limits(testing::metrics::Temp).unwrap().has_severity_fn);

        ns.clear_severity_fn(testing::metrics::Temp).unwrap();
        assert_eq!(
            ns.severity_of(testing::metrics::Temp, 75.0),
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
