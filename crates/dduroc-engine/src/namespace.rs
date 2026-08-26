//! The namespace: a microservice's working handle.
//!
//! A namespace is a runtime entity: a directory with its own segments, bound
//! to a compile-time schema. Four instances of an amplifier service bring up
//! `orc-radio-0`…`orc-radio-3` with one schema, and a record's ownership is
//! determined by where it lies — nothing about "who wrote this" is stored in
//! the records themselves.

use crate::Clock;
use crate::error::{Error, Result};
use crate::fsutil;
use crate::limits::{EffectiveLimits, LimitsRegistry, MetricLimits};
use crate::metric::{Metric, MetricValue, NumericValue, Untyped};
use crate::schema::{MetricDesc, MetricKind, Schema, Severity, StorageClass, Thresholds};
use crate::staged::{ChannelIdx, DropCounters, NsId, OwnedValue, Payload, Staged, StagedRecord};
use crate::stats::Counters;
use crate::store::next_span_id;
use crate::writer::Writer;
use dduroc_format::{
    BootTime, EventId, Level, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

/// The name of a namespace's metadata file.
pub const NS_META: &str = "ns-meta";

/// A namespace's metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NsMeta {
    /// The schema name. A namespace cannot be opened under a foreign schema:
    /// the same event identifiers mean different things in different schemas.
    pub schema_name: String,
    /// The protocol version every segment in the directory has been brought to.
    pub protocol_version: u16,
}

impl NsMeta {
    /// Read or create the metadata, checking schema compatibility.
    pub fn open(dir: &Path, ns_name: &str, schema: &Schema) -> Result<Self> {
        let path = dir.join(NS_META);
        match fsutil::read_optional(&path)? {
            None => {
                let meta = Self {
                    schema_name: schema.name.to_owned(),
                    protocol_version: schema.version.0,
                };
                fsutil::write_atomic(&path, &postcard::to_allocvec(&meta)?)?;
                Ok(meta)
            }
            Some(bytes) => {
                let meta: Self = postcard::from_bytes(&bytes).map_err(|_| Error::Corrupt {
                    path: path.clone(),
                    reason: "the namespace metadata does not parse".to_owned(),
                })?;
                if meta.schema_name != schema.name {
                    return Err(Error::SchemaMismatch {
                        namespace: ns_name.to_owned(),
                        stored: meta.schema_name,
                        opening: schema.name.to_owned(),
                    });
                }
                // Data written by newer firmware is beyond this one's
                // understanding: it has neither the new types nor steps that
                // migrate forward.
                if meta.protocol_version > schema.version.0 {
                    return Err(Error::ProtocolFromFuture {
                        namespace: ns_name.to_owned(),
                        stored: meta.protocol_version,
                        current: schema.version.0,
                    });
                }
                // The chain of steps was checked when the schema was validated;
                // a missing step here would mean there is nothing to bring the
                // old segments up to the current shape with.
                for from in meta.protocol_version..schema.version.0 {
                    if schema.migration(from).is_none() {
                        return Err(Error::MissingMigration {
                            schema: schema.name.to_owned(),
                            from,
                            to: from + 1,
                        });
                    }
                }
                // `protocol_version` in the metadata is the version of the last
                // **completed** migration, and it must not be rewritten here: coming up is
                // not a run, and the segments are still in the earlier layout. By stamping
                // the metadata, this build would declare the namespace migrated, and a
                // future `Namespace::migrate` would walk past the old segments — they
                // would be parsed with the new version's decoders, silently and wrongly. A
                // mixed state is legitimate: every segment header carries its own version.
                // Only a successful run stamps the metadata.
                Ok(meta)
            }
        }
    }
}

/// A range from a range expression over a metric's values.
///
/// The bounds are converted to `f64` — that is what the engine stores them in —
/// but they are written with the same type as the samples: `..=60.0` for a
/// `Metric<f32>`, `..=10` for a `Metric<u64>`. An exclusive bound is treated as
/// inclusive for the same reason as in [`crate::schema::Range`].
fn numeric_range<T: NumericValue>(r: impl std::ops::RangeBounds<T>) -> crate::schema::Range {
    use std::ops::Bound;
    let bound = |b: Bound<&T>| match b {
        Bound::Unbounded => None,
        Bound::Included(v) | Bound::Excluded(v) => Some((*v).into_f64()),
    };
    crate::schema::Range {
        min: bound(r.start_bound()),
        max: bound(r.end_bound()),
    }
}

/// A namespace handle.
///
/// Cheap to clone and to hand around a service's tasks: records are addressed
/// explicitly, with no implicit thread context.
#[derive(Debug, Clone)]
pub struct Namespace {
    inner: Arc<NamespaceInner>,
}

#[derive(Debug)]
struct NamespaceInner {
    /// The store the namespace belongs to.
    ///
    /// Kept alive for as long as the handle lives: a `Store` being dropped
    /// stops the writer, and a `Namespace` that outlived it would write into
    /// nothing while returning `Ok` on every call.
    _store: Arc<dyn std::any::Any + Send + Sync>,
    id: NsId,
    name: String,
    /// The namespace's directory — this is where `ns-meta` lives.
    dir: std::path::PathBuf,
    /// The channel directories in `classes` order: classes may live on
    /// different media, and a migration run needs the paths ready.
    channel_dirs: Vec<std::path::PathBuf>,
    /// The store's identity: a run does not touch another device's segments.
    store_id: u64,
    schema: Schema,
    /// The storage classes in the same order the channels were registered in.
    classes: Vec<StorageClass>,
    writer: Arc<Writer>,
    clock: Clock,
    drops: Arc<DropCounters>,
    next_span: Arc<AtomicU32>,
    meta: NsMeta,
    /// The version of the last completed migration — the live value.
    ///
    /// `meta` holds what was read at open time; a successful
    /// `Namespace::migrate` moves the version without reopening the namespace.
    migrated_to: std::sync::atomic::AtomicU16,
    /// A run is exclusive: two at once would rewrite the same files.
    migrate_lock: std::sync::Mutex<()>,
    /// The value limits: the schema's defaults plus whatever the installation
    /// set. They live in memory and are **never written to disk** — see
    /// [`crate::limits`].
    limits: LimitsRegistry,
    /// Which contract violations have already been announced in the stream —
    /// one bit per kind.
    ///
    /// Announcing every one would flood the journal: a contract violation is no
    /// accident, it repeats on every turn of a loop. Once is enough for the
    /// defect to be found; after that only the counter grows.
    announced: std::sync::atomic::AtomicU8,
}

impl Namespace {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Arc<dyn std::any::Any + Send + Sync>,
        id: NsId,
        name: String,
        dir: std::path::PathBuf,
        channel_dirs: Vec<std::path::PathBuf>,
        store_id: u64,
        schema: Schema,
        classes: Vec<StorageClass>,
        writer: Arc<Writer>,
        clock: Clock,
        drops: Arc<DropCounters>,
        next_span: Arc<AtomicU32>,
        meta: NsMeta,
    ) -> Self {
        let limits = LimitsRegistry::new();
        let migrated_to = std::sync::atomic::AtomicU16::new(meta.protocol_version);
        Self {
            inner: Arc::new(NamespaceInner {
                _store: store,
                id,
                name,
                dir,
                channel_dirs,
                store_id,
                schema,
                classes,
                writer,
                clock,
                drops,
                next_span,
                meta,
                migrated_to,
                migrate_lock: std::sync::Mutex::new(()),
                limits,
                announced: std::sync::atomic::AtomicU8::new(0),
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn id(&self) -> NsId {
        self.inner.id
    }

    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }

    /// The namespace's metadata — with the live protocol version.
    ///
    /// By value rather than by reference: a successful [`Namespace::migrate`]
    /// moves the version, and a snapshot has to reflect that.
    pub fn meta(&self) -> NsMeta {
        NsMeta {
            schema_name: self.inner.meta.schema_name.clone(),
            protocol_version: self
                .inner
                .migrated_to
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }

    /// The schema version of THIS build — the one new segments are written
    /// with.
    ///
    /// Not to be confused with the version of the last completed migration
    /// ([`NsMeta::protocol_version`]): the gap between them is the outstanding
    /// debt [`Namespace::pending_migration`] names.
    pub fn schema_version(&self) -> ProtocolVersion {
        self.inner.schema.version
    }

    /// An unfinished migration: `(from which version, to which)`.
    ///
    /// `Some` means the directory holds segments in an earlier layout: this
    /// build's schema is newer than the version of the last completed
    /// migration. They read correctly — a segment of an earlier version goes
    /// through the steps as it is read — but every read pays for the
    /// transformation, and the history lies in a layout no build writes any
    /// more. To bring it up physically there is [`Namespace::migrate`]; when to
    /// call it is the application's decision: a run burns flash wear and takes
    /// up the medium.
    ///
    /// New segments are written at the current schema version; a mixed state of
    /// the directory is legitimate and expected.
    pub fn pending_migration(&self) -> Option<(u16, u16)> {
        let stored = self
            .inner
            .migrated_to
            .load(std::sync::atomic::Ordering::Acquire);
        let current = self.inner.schema.version.0;
        (stored < current).then_some((stored, current))
    }

    /// Bring the namespace's segments up to the current schema version.
    ///
    /// An explicit call rather than something automatic at open time: a run
    /// reads and rewrites the whole history — minutes on gigabytes — and burns
    /// flash wear. Only the application knows when a device can afford that
    /// (after a start, in a quiet hour); meanwhile the debt is visible in
    /// [`Namespace::pending_migration`], and everything reads correctly without
    /// a run — the steps are applied at read time.
    ///
    /// The heavy work happens on the calling thread; the writer takes part only
    /// in committing each segment (a rename over the old name), so writing does
    /// not stop. A run is idempotent and resumable: an interruption anywhere
    /// leaves every segment either as it was or already rewritten, and the next
    /// call carries on with the rest.
    ///
    /// Segments no step touches are not rewritten and keep the earlier version
    /// in their header — which is legitimate: being untouched is precisely what
    /// makes the current decoders read them correctly.
    ///
    /// # Space on the medium
    ///
    /// A run rewrites a segment into a temporary file next to it and swaps it for
    /// the original: while the rewrite goes on, both lie on the medium. So roughly
    /// **one segment** (`ChannelConfig::segment_bytes`) has to be free on top of
    /// what is occupied — and no more: the temporary file's capacity is taken from
    /// the original's actual data and grows only if a step really did swell the
    /// records. That file does not count towards the class budget, and the engine
    /// does not evict anything to make room for it: an `Err` with
    /// [`Error::is_no_space`] means the run is deferred until the space appears.
    /// The original is untouched.
    pub fn migrate(&self) -> Result<crate::migrate::MigrationReport> {
        let Some((_, to)) = self.pending_migration() else {
            return Ok(crate::migrate::MigrationReport::default());
        };
        let Ok(_guard) = self.inner.migrate_lock.try_lock() else {
            return Err(Error::MigrationBusy(self.inner.name.clone()));
        };

        let report = crate::migrate::run_namespace(
            &self.inner.schema,
            self.inner.store_id,
            &self.inner.writer,
            self.inner.id,
            &self.inner.channel_dirs,
        )?;

        // The stamp comes only after EVERY segment turned out to be either
        // current, or rewritten, or provably untouched: the metadata means
        // "completed", and declaring that earlier would mean future runs walk
        // past what was left undone.
        let meta = NsMeta {
            schema_name: self.inner.meta.schema_name.clone(),
            protocol_version: to,
        };
        fsutil::write_atomic(
            &self.inner.dir.join(NS_META),
            &postcard::to_allocvec(&meta)?,
        )?;
        self.inner
            .migrated_to
            .store(to, std::sync::atomic::Ordering::Release);
        Ok(report)
    }

    /// The current moment — in the same coordinates a reader will return it in.
    ///
    /// The moment specifically, not just the microseconds: comparing `Micros`
    /// from different runs is meaningless, and the type forbids it.
    pub fn now(&self) -> BootTime {
        self.inner.clock.now_at()
    }

    /// The stamp for a record: microseconds only.
    ///
    /// The run does not go into the record — it is implicit from the segment
    /// header, and repeating it in every record would mean paying four bytes
    /// for something already known from the file name.
    #[inline]
    fn stamp(&self) -> Micros {
        self.inner.clock.now()
    }

    /// The channel corresponding to a storage class.
    ///
    /// The channel list is built from the schema itself, so the class has to be
    /// there. Silently returning channel zero on a miss would mean a critical
    /// record going to the ordinary channel without warning — with a different
    /// durability policy and a different budget.
    fn channel_of(&self, class: StorageClass) -> Result<ChannelIdx> {
        self.inner
            .classes
            .iter()
            .position(|c| *c == class)
            .map(|i| ChannelIdx(i as u16))
            .ok_or(Error::ClassNotDeclared {
                schema: self.inner.schema.name,
                class,
            })
    }

    /// Account for a write outcome the caller is not told about.
    ///
    /// The write path returns nothing — application code can do nothing
    /// sensible with a failure anyway — but the outcome does not disappear:
    ///
    /// - a **loss** (the queue is falling behind, the writer is dead) is already
    ///   counted by the writer's counter and announced by a notice in the stream
    ///   itself;
    /// - a **contract violation** (an id from a foreign schema) is a build defect:
    ///   it is counted separately and announced once by a record in the journal, so
    ///   that it gets found rather than searched for as the cause of silence.
    fn report(&self, outcome: Result<()>) {
        let Err(e) = outcome else { return };
        if e.loses_record() {
            return;
        }
        use std::sync::atomic::Ordering;
        Counters::bump(&self.inner.writer.counters().rejected);
        let bit = contract_bit(&e);
        if self.inner.announced.fetch_or(bit, Ordering::Relaxed) & bit == 0 {
            // The announcement itself goes by the ordinary path and may be lost
            // under load; there is no reason to repeat it — the counter
            // remains.
            let _ = self.try_log_text(Level::Error, "dduroc", e.to_string(), None);
        }
    }

    /// Account for a failure that happened a layer above — while serializing an
    /// event's fields, or in the bridge from `tracing`.
    ///
    /// Needed so that such a failure reaches the same counters and the same
    /// one-off announcement as the engine's own: otherwise a typed layer over
    /// the handle would lose records more quietly than the handle itself.
    pub fn note_failure(&self, e: Error) {
        self.report(Err(e));
    }

    /// Write an event with an already serialized payload.
    pub fn log_raw(&self, event: EventId, payload: &[u8], span: Option<SpanId>) {
        self.report(self.try_log_raw(event, payload, span));
    }

    /// The same with an answer about the outcome — see
    /// [`Namespace::try_log_payload`].
    pub fn try_log_raw(&self, event: EventId, payload: &[u8], span: Option<SpanId>) -> Result<()> {
        self.try_log_payload(event, Payload::from_slice(payload), span)
    }

    /// Write an event; the payload is already assembled in the write buffer.
    ///
    /// It deliberately returns nothing: logging must not influence control flow
    /// in application code, and there is nothing to do with a failure at the
    /// call site — retrying against a lagging disk only makes it worse. What
    /// happened is visible in [`crate::stats::Stats`] and in the record stream
    /// itself; whoever needs an answer on the spot has
    /// [`Namespace::try_log_payload`].
    pub fn log_payload(&self, event: EventId, payload: Payload, span: Option<SpanId>) {
        self.report(self.try_log_payload(event, payload, span));
    }

    /// Write an event and say how it ended.
    ///
    /// There are two kinds of `Err`, and telling them apart matters more than
    /// the text: [`Error::loses_record`] means the record did not reach the
    /// medium (the disk is lagging), [`Error::breaks_contract`] that the event
    /// is not from this schema, that is, a build defect.
    ///
    /// The verdict belongs to the caller entirely: a contract violation here does
    /// **not** reach the counters and is not announced in the journal (the "quiet"
    /// methods do that). Once the failure is handled it belongs to whoever handled
    /// it; to hand it to the engine there is [`Namespace::note_failure`]. Disk
    /// losses are counted in any case: the writer counts them itself.
    pub fn try_log_payload(
        &self,
        event: EventId,
        payload: Payload,
        span: Option<SpanId>,
    ) -> Result<()> {
        let Some(desc) = self.inner.schema.event(event) else {
            return Err(Error::UnknownEvent {
                schema: self.inner.schema.name,
                event: event.0,
            });
        };
        let item = Staged {
            ns: self.inner.id,
            channel: self.channel_of(desc.class)?,
            at: self.stamp(),
            record: StagedRecord::Message {
                event,
                span,
                payload,
            },
        };
        self.inner.writer.write(
            item,
            desc.class == StorageClass::Critical,
            &self.inner.drops,
        )
    }

    /// Write free text without a schema: the bridge from `tracing`, a panic
    /// handler.
    ///
    /// `target` is the string's source (`"app"`, a module name); it is interned
    /// (`Arc<str>`) because the bridge repeats one and the same target
    /// thousands of times, while a string literal is enough at the call site.
    pub fn log_text(
        &self,
        level: Level,
        target: impl Into<Arc<str>>,
        text: impl Into<Box<str>>,
        span: Option<SpanId>,
    ) {
        self.report(self.try_log_text(level, target, text, span));
    }

    /// The same with an answer about the outcome.
    pub fn try_log_text(
        &self,
        level: Level,
        target: impl Into<Arc<str>>,
        text: impl Into<Box<str>>,
        span: Option<SpanId>,
    ) -> Result<()> {
        let target = target.into();
        // Free text is not declared in the schema, so it has no storage class
        // either. Text at an error level asks for the critical channel, but the
        // schema is not obliged to declare one: refusing would mean staying
        // silent exactly where text is written in order not to be silent (the
        // announcement of a build defect, a bridge message, a panic). The
        // critical one is taken if it exists, otherwise the ordinary one, which
        // always does (`Schema::classes`).
        //
        // The queue is chosen by the channel that was actually obtained:
        // waiting for room in order to deliver into a channel with deferred
        // syncing would be a price paid without a guarantee.
        let (channel, critical) = match level >= Level::Error {
            true => match self.channel_of(StorageClass::Critical) {
                Ok(idx) => (idx, true),
                Err(_) => (self.channel_of(StorageClass::Default)?, false),
            },
            false => (self.channel_of(StorageClass::Default)?, false),
        };
        let item = Staged {
            ns: self.inner.id,
            channel,
            at: self.stamp(),
            record: StagedRecord::Text {
                level,
                span,
                target,
                text: text.into(),
            },
        };
        self.inner.writer.write(item, critical, &self.inner.drops)
    }

    /// Open a telemetry series.
    ///
    /// A series is a metric and nothing else: there are no runtime dimensions.
    /// The handle resolves the descriptor once, after which a sample costs one
    /// call with no lookup at all. Four temperature sensors are four schema
    /// metrics: otherwise whatever told them apart would have to be written
    /// into every sample.
    ///
    /// The value type comes from the metric constant, so `sample` accepts only
    /// what was declared — see [`crate::metric`]. Opening returns a `Result`
    /// because the metric may be from a foreign schema; that is one call per
    /// series, not per sample.
    pub fn series<M>(&self, metric: Metric<M>) -> Result<Series<M>> {
        let id: MetricId = metric.into();
        let desc = self.metric_desc(id)?;
        Ok(Series {
            ns: self.clone(),
            metric: id,
            channel: self.channel_of(desc.class)?,
            critical: desc.class == StorageClass::Critical,
            value_type: desc.value_type,
            desc,
            _value: std::marker::PhantomData,
        })
    }

    /// Open a series by an identifier known only at runtime.
    ///
    /// The value type is unknown at compile time, so such a series accepts only
    /// [`Series::sample_raw`], which checks the type against the schema. Needed
    /// by the web layer and by migrations — application code wants
    /// [`Namespace::series`].
    pub fn series_untyped(&self, metric: MetricId) -> Result<Series<Untyped>> {
        let desc = self.metric_desc(metric)?;
        Ok(Series {
            ns: self.clone(),
            metric,
            channel: self.channel_of(desc.class)?,
            critical: desc.class == StorageClass::Critical,
            value_type: desc.value_type,
            desc,
            _value: std::marker::PhantomData,
        })
    }

    fn metric_desc(&self, metric: MetricId) -> Result<&'static MetricDesc> {
        self.inner
            .schema
            .metric(metric)
            .ok_or(Error::UnknownMetric {
                metric_id: metric.0,
            })
    }

    /// Set value bounds over the schema's.
    ///
    /// For the case where they are known only at runtime: the external system
    /// has determined the hardware model and knows what is normal for this
    /// amplifier. The ranges are written just as in the schema, as ordinary
    /// range expressions:
    ///
    /// ```text
    /// ns.set_thresholds(metrics::TempPa, ..=60.0, ..=75.0)?;   // from above
    /// ns.set_thresholds(metrics::Vswr, 1.0..=1.5, 1.0..=2.0)?; // from both sides
    /// ```
    ///
    /// The bounds have the same type as the metric's samples
    /// ([`NumericValue`]): they cannot be set on an enum metric or on `type:
    /// blob`, and it is the compiler that says so rather than a refusal on the
    /// device. A metric known only at runtime is served by
    /// [`Namespace::set_thresholds_raw`].
    ///
    /// **Never written to disk.** Bounds are a property of the installation, not of
    /// the measurement; more in [`crate::limits`].
    pub fn set_thresholds<T: NumericValue>(
        &self,
        metric: Metric<T>,
        warn: impl std::ops::RangeBounds<T>,
        alarm: impl std::ops::RangeBounds<T>,
    ) -> Result<()> {
        self.set_limits(
            metric,
            MetricLimits::numeric(Thresholds {
                warn: numeric_range(warn),
                alarm: numeric_range(alarm),
            }),
        )
    }

    /// The same for a metric known only at runtime.
    ///
    /// The web layer's path: the metric arrived as a string in a request, the
    /// bounds as numbers from a configuration. Incompatibility with the
    /// declared type is still caught only at runtime here — the compiler has
    /// nothing to lean on.
    pub fn set_thresholds_raw(
        &self,
        metric: impl Into<MetricId>,
        warn: impl std::ops::RangeBounds<f64>,
        alarm: impl std::ops::RangeBounds<f64>,
    ) -> Result<()> {
        self.set_limits(metric, MetricLimits::numeric(Thresholds::new(warn, alarm)))
    }

    /// Set the whole override: the bounds, the state severities, or both.
    pub fn set_limits(&self, metric: impl Into<MetricId>, limits: MetricLimits) -> Result<()> {
        self.inner
            .limits
            .set(&self.inner.schema, metric.into(), Some(limits))
    }

    /// Remove the override: what the schema declared applies again.
    pub fn clear_limits(&self, metric: impl Into<MetricId>) -> Result<()> {
        self.inner
            .limits
            .set(&self.inner.schema, metric.into(), None)
    }

    /// Take a metric's diagnosis over entirely: a closure instead of ranges.
    ///
    /// For rules the data cannot express — hysteresis, a dependence on captured
    /// context (the hardware model, the operating mode). It beats both the
    /// schema and [`Namespace::set_limits`]; the value arrives as a number (a
    /// state code as a number too), and a blob metric cannot be given a
    /// closure. Like the other limits it is never written to disk — a reader of
    /// a dump will not see it.
    pub fn set_severity_fn(
        &self,
        metric: impl Into<MetricId>,
        check: impl Fn(f64) -> Severity + Send + Sync + 'static,
    ) -> Result<()> {
        self.inner
            .limits
            .set_fn(&self.inner.schema, metric.into(), Some(Box::new(check)))
    }

    /// Remove the diagnosis closure: the data — the overrides and the schema —
    /// applies again.
    pub fn clear_severity_fn(&self, metric: impl Into<MetricId>) -> Result<()> {
        self.inner
            .limits
            .set_fn(&self.inner.schema, metric.into(), None)
    }

    /// A metric's effective limits: the schema plus the overrides.
    pub fn limits(&self, metric: impl Into<MetricId>) -> Result<EffectiveLimits> {
        self.inner
            .limits
            .effective(&self.inner.schema, metric.into())
    }

    /// How much a value calls for attention by the effective limits.
    ///
    /// Writing itself does not touch the limits: the engine does not decide for
    /// the application what to do about a bound being crossed, and it raises no
    /// events of its own. This method is for whoever wants to check a value
    /// (and, say, write an event).
    ///
    /// It accepts exactly what a sample would: `65.0` for a metric declared
    /// `type: f32`, a state for an enum. The compiler checks the type — from
    /// the same metric constant that opens a series:
    ///
    /// ```ignore
    /// ns.severity_of(radio::metrics::TempPa, 65.0);
    /// ns.severity_of(radio::metrics::LinkState, radio::metrics::LinkState::Los);
    /// ```
    ///
    /// A metric known only at runtime is served by
    /// [`Namespace::severity_of_raw`].
    #[inline]
    pub fn severity_of<T, V: MetricValue<T>>(&self, metric: Metric<T>, value: V) -> Severity {
        self.severity_of_raw(metric, &value.into_owned())
    }

    /// The same for a metric and a value assembled at runtime.
    ///
    /// The path of the web layer and the viewer: the metric arrived as a string
    /// in a request and there is no compile-time type. Application code wants
    /// [`Namespace::severity_of`].
    pub fn severity_of_raw(&self, metric: impl Into<MetricId>, value: &OwnedValue) -> Severity {
        self.inner
            .limits
            .severity_of(&self.inner.schema, metric.into(), &value.as_value())
    }

    /// Begin a span. Returns a guard: the end is written when it is dropped,
    /// stack unwinding included — an unclosed span is indistinguishable from a
    /// crash.
    ///
    /// The guard is handed out always, even if the span's start could not be
    /// written: otherwise application code would get a `?` in a place where
    /// nesting matters more than the record, and a reader has to be able to
    /// show a span without a start anyway — that is exactly what a span cut
    /// short by a process crash looks like.
    pub fn span(&self, kind: SpanKindId) -> SpanGuard {
        self.span_with_parent(kind, None)
    }

    /// Begin a span with an explicit parent.
    pub fn span_with_parent(&self, kind: SpanKindId, parent: Option<SpanId>) -> SpanGuard {
        let span = next_span_id(&self.inner.next_span);
        // A span kind not from this schema is no reason not to open the span:
        // the ordinary storage class is taken, and the contract violation is
        // reported as usual.
        let class = self
            .inner
            .schema
            .span(kind)
            .map_or(StorageClass::Default, |d| d.class);
        let channel = match self.channel_of(class) {
            Ok(c) => c,
            Err(e) => {
                self.report(Err(e));
                ChannelIdx(0)
            }
        };
        let critical = class == StorageClass::Critical;

        if self.inner.schema.span(kind).is_none() {
            self.report(Err(Error::UnknownSpanKind {
                schema: self.inner.schema.name,
                kind: kind.0,
            }));
        }

        self.report(self.inner.writer.write(
            Staged {
                ns: self.inner.id,
                channel,
                at: self.stamp(),
                record: StagedRecord::SpanStart { span, kind, parent },
            },
            critical,
            &self.inner.drops,
        ));

        SpanGuard {
            ns: self.clone(),
            span,
            channel,
            critical,
            closed: false,
        }
    }

    /// Wait until what has accumulated is on the medium.
    pub fn sync(&self) -> Result<()> {
        self.inner.writer.sync(Some(self.inner.id))
    }
}

/// An open telemetry series.
///
/// A series is identified by its metric. The descriptor is resolved at open
/// time, so a sample does no schema lookup and consults no registry.
///
/// The parameter `M` is the value type marker from the metric constant, and it
/// is what determines what [`Series::sample`] will accept.
#[derive(Debug, Clone)]
pub struct Series<M> {
    ns: Namespace,
    metric: MetricId,
    channel: ChannelIdx,
    critical: bool,
    /// A copy of the declared value type. The descriptor weighs a hundred and
    /// fifty-odd bytes and lies in `.rodata`; the check on every sample must
    /// not go to it.
    value_type: dduroc_format::ValueType,
    desc: &'static crate::schema::MetricDesc,
    _value: std::marker::PhantomData<fn() -> M>,
}

impl<M> Series<M> {
    pub fn metric(&self) -> MetricId {
        self.metric
    }

    pub fn value_type(&self) -> dduroc_format::ValueType {
        self.value_type
    }

    /// How the quantity behaves between samples.
    pub fn kind(&self) -> MetricKind {
        self.desc.kind
    }

    /// The metric's name from the schema.
    pub fn name(&self) -> &'static str {
        self.desc.name
    }

    /// Write a sample with an already assembled value.
    ///
    /// The path for when the type is known only at runtime. Application code
    /// wants [`Series::sample`]: there the compiler checks the type.
    pub fn sample_raw(&self, value: OwnedValue) {
        self.ns.report(self.try_sample_raw(value));
    }

    /// The same with an answer about the outcome.
    pub fn try_sample_raw(&self, value: OwnedValue) -> Result<()> {
        if value.value_type() != self.value_type {
            return Err(Error::ValueTypeMismatch {
                metric_id: self.metric.0,
                declared: self.value_type,
                got: value.value_type(),
            });
        }
        let item = Staged {
            ns: self.ns.inner.id,
            channel: self.channel,
            at: self.ns.stamp(),
            record: StagedRecord::Sample {
                metric: self.metric,
                value,
            },
        };
        self.ns
            .inner
            .writer
            .write(item, self.critical, &self.ns.inner.drops)
    }

    /// The severity of a value assembled at runtime.
    ///
    /// The only form for a [`Series<Untyped>`]; a typed series wants
    /// [`Series::severity_of`].
    pub fn severity_of_raw(&self, value: &OwnedValue) -> Severity {
        self.ns.severity_of_raw(self.metric, value)
    }
}

impl<M> Series<M> {
    /// Write a sample.
    ///
    /// It accepts only what the metric declared: a `Series<f32>` takes an
    /// `f32`, a `Series<LinkState>` the states of that metric and nobody
    /// else's. It returns nothing for the same reason event writing does (see
    /// [`Namespace::log_payload`]); whoever needs an answer has
    /// [`Series::try_sample`].
    #[inline]
    pub fn sample<V: MetricValue<M>>(&self, value: V) {
        self.ns.report(self.try_sample(value));
    }

    /// The same with an answer about the outcome.
    #[inline]
    pub fn try_sample<V: MetricValue<M>>(&self, value: V) -> Result<()> {
        self.try_sample_raw(self.coerce(value.into_owned()))
    }

    /// A value's severity by the effective limits.
    ///
    /// It accepts the same as [`Series::sample`] — the series already knows its
    /// metric and the compiler knows its type:
    /// `link.severity_of(LinkState::Los)`. A value can be checked before
    /// sampling without assembling it by hand.
    #[inline]
    pub fn severity_of<V: MetricValue<M>>(&self, value: V) -> Severity {
        self.ns
            .severity_of_raw(self.metric, &self.coerce(value.into_owned()))
    }

    /// Bring a value to the declared representation.
    ///
    /// Needed for enums exactly: a state code arrives as an integer while the
    /// metric may be declared as `bool` (two states) or `i64`. The other values
    /// pass through as they are — their type already matches by construction.
    #[inline]
    fn coerce(&self, value: OwnedValue) -> OwnedValue {
        match (self.value_type, &value) {
            (dduroc_format::ValueType::Bool, OwnedValue::U64(code)) => OwnedValue::Bool(*code != 0),
            (dduroc_format::ValueType::I64, OwnedValue::U64(code)) => OwnedValue::I64(*code as i64),
            _ => value,
        }
    }
}

/// A span guard: the end is written when it is dropped.
///
/// The value has to be put somewhere: `ns.span(kind);` with no binding drops
/// the guard in the same expression that created it, and the span collapses to
/// zero duration — two records in a row instead of a stretch of work. From
/// outside it looks like "there is a span, but it is empty", and there is
/// nothing in the journal to make sense of that with.
#[derive(Debug)]
#[must_use = "a span lives while its guard lives: `ns.span(kind);` closes it at once"]
pub struct SpanGuard {
    ns: Namespace,
    span: SpanId,
    channel: ChannelIdx,
    critical: bool,
    closed: bool,
}

impl SpanGuard {
    pub fn id(&self) -> SpanId {
        self.span
    }

    /// The namespace the span belongs to — needed by a layer over the handle.
    pub fn namespace(&self) -> &Namespace {
        &self.ns
    }

    /// A nested span: this one becomes its parent.
    pub fn child(&self, kind: SpanKindId) -> SpanGuard {
        self.ns.span_with_parent(kind, Some(self.span))
    }

    /// Write an event attached to this span.
    pub fn log_raw(&self, event: EventId, payload: &[u8]) {
        self.ns.log_raw(event, payload, Some(self.span));
    }

    /// The same, but with the payload already assembled in the write buffer.
    pub fn log_payload(&self, event: EventId, payload: Payload) {
        self.ns.log_payload(event, payload, Some(self.span));
    }

    /// The same with an answer about the outcome.
    pub fn try_log_payload(&self, event: EventId, payload: Payload) -> Result<()> {
        self.ns.try_log_payload(event, payload, Some(self.span))
    }

    /// Close explicitly on seeing a write error. Usually unnecessary: closing
    /// happens when the guard is dropped.
    ///
    /// Unlike dropping, the channel's ordinary policy applies here: on a
    /// critical span the caller will wait for room in the queue, but the span's
    /// end is guaranteed not to be lost.
    pub fn close(mut self) -> Result<()> {
        self.closed = true;
        self.write_end(true)
    }

    fn write_end(&self, may_wait: bool) -> Result<()> {
        let item = Staged {
            ns: self.ns.inner.id,
            channel: self.channel,
            at: self.ns.stamp(),
            record: StagedRecord::SpanEnd { span: self.span },
        };
        let writer = &self.ns.inner.writer;
        if may_wait {
            writer.write(item, self.critical, &self.ns.inner.drops)
        } else {
            writer.write_no_wait(item, self.critical, &self.ns.inner.drops)
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if !self.closed {
            // Without waiting for room — deliberately. `Drop` is called during
            // stack unwinding after a panic too, and on a critical span the
            // ordinary path would wait up to five seconds for room in the
            // queue: an emergency shutdown would turn into a hang, and nested
            // guards would stack their timeouts on top of one another.
            //
            // The price is that a span's end may be lost under pressure. It is
            // counted by the loss counter and by a notice in the stream, and a
            // reader has to be able to show an unclosed span anyway: a span cut
            // short by a process crash looks exactly the same.
            let _ = self.write_end(false);
        }
    }
}

/// A bit per kind of contract violation: each is announced once.
fn contract_bit(e: &Error) -> u8 {
    match e {
        Error::UnknownEvent { .. } => 1 << 0,
        Error::UnknownMetric { .. } => 1 << 1,
        Error::UnknownSpanKind { .. } => 1 << 2,
        Error::ValueTypeMismatch { .. } => 1 << 3,
        Error::ClassNotDeclared { .. } => 1 << 4,
        Error::EncodeFailed { .. } => 1 << 5,
        // The rest (a failure of the medium, say) once too, but under its own
        // bit: it can flood the journal with repeats just as easily.
        _ => 1 << 7,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EventDesc, Language, MetricDesc, SpanDesc};
    use crate::store::{Store, StoreConfig};
    use dduroc_format::ValueType;

    static LANGS: &[Language] = &[Language("en"), Language("ru")];
    static EVENTS: &[EventDesc] = &[
        EventDesc {
            id: EventId(1),
            name: "PowerSet",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &["rf"],
            templates: &["power {dbm}", "мощность {dbm}"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::Critical,
            tags: &[],
            templates: &["alarm", "авария"],
            fields: &[],
            decoders: None,
        },
    ];
    static LINK_STATES: &[crate::schema::StateDesc] = &[
        crate::schema::StateDesc {
            code: 0,
            name: "Los",
            severity: Severity::Alarm,
        },
        crate::schema::StateDesc {
            code: 1,
            name: "Sync",
            severity: Severity::Warn,
        },
        crate::schema::StateDesc {
            code: 2,
            name: "Lock",
            severity: Severity::Normal,
        },
    ];
    static METRICS: &[MetricDesc] = &[
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "temp",
            value_type: ValueType::F32,
            class: StorageClass::Default,
            unit: "°C",
            tags: &["thermal"],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: crate::schema::Thresholds {
                warn: crate::schema::Range {
                    min: None,
                    max: Some(70.0),
                },
                alarm: crate::schema::Range {
                    min: None,
                    max: Some(85.0),
                },
            },
        },
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(2),
            name: "link",
            value_type: ValueType::U64,
            class: StorageClass::Default,
            unit: "",
            tags: &["rf"],
            kind: MetricKind::State,
            states: LINK_STATES,
            thresholds: crate::schema::Thresholds::NONE,
        },
    ];

    use crate::metric::{Metric, MetricState, MetricValue};

    /// Typed metric constants — what the macro produces for a real schema: the
    /// value type arrives together with the identifier.
    const TEMP: Metric<f32> = Metric::new(MetricId(1));
    const LINK: Metric<LinkState> = Metric::new(MetricId(2));

    /// The channel state — what the macro would produce for a real schema.
    #[derive(Debug, Clone, Copy)]
    enum LinkState {
        Los = 0,
        Sync = 1,
        Lock = 2,
    }

    impl MetricValue<LinkState> for LinkState {
        fn into_owned(self) -> OwnedValue {
            OwnedValue::U64(self as u64)
        }
    }

    impl MetricState for LinkState {
        fn metric() -> MetricId {
            MetricId(2)
        }
        fn code(self) -> u64 {
            self as u64
        }
        fn name(self) -> &'static str {
            LINK_STATES[self as usize].name
        }
    }
    static SPANS: &[SpanDesc] = &[SpanDesc {
        id: SpanKindId(1),
        name: "Calibration",
        class: StorageClass::Default,
    }];

    fn schema() -> Schema {
        Schema {
            name: "radio",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: METRICS,
            spans: SPANS,
            migrations: &[],
        }
    }

    fn open_store(dir: &Path) -> Arc<Store> {
        Store::open(StoreConfig::new(dir).with_budget_per_class(16 * 1024 * 1024)).unwrap()
    }

    #[test]
    fn writes_land_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        for i in 0..100 {
            ns.log_raw(EventId(1), &[i as u8; 4], None);
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(stats.records_written, 100);
        assert!(stats.blocks_written >= 1);
        assert!(stats.is_clean(), "there must be no losses: {stats:?}");

        // A segment appeared in the "default" channel.
        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let files: Vec<_> = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
            .collect();
        assert_eq!(files.len(), 1, "exactly one segment was created");
    }

    #[test]
    fn critical_events_are_synced_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        ns.log_raw(EventId(2), &[1, 2, 3], None);
        // Wait for the writer to get to it: sync is a barrier over the same
        // queue.
        ns.sync().unwrap();
        assert!(store.stats().syncs >= 1, "a critical event must be synced");

        let crit_dir = dir.path().join("orc-radio-0").join("critical");
        assert!(
            crit_dir.is_dir(),
            "the critical channel is created separately"
        );
    }

    #[test]
    fn overload_loses_only_what_it_reports() {
        // Under pressure the ordinary channel may lose records — but exactly as
        // many as it admitted to losing. A discrepancy between "accepted" and
        // "written" would mean a silent hole.
        //
        // Throughput as such is not checked here: it depends on how loaded the
        // machine is and is measured by benchmarks.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const N: u64 = 50_000;
        let mut accepted = 0u64;
        let mut refused = 0u64;
        for i in 0..N {
            match ns.try_log_raw(EventId(1), &[i as u8; 8], None) {
                Ok(()) => accepted += 1,
                Err(_) => refused += 1,
            }
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(accepted + refused, N);
        assert!(accepted > 0, "at least something must get through");
        assert!(
            stats.records_written >= accepted,
            "{} written against {accepted} accepted — a silent loss",
            stats.records_written
        );
        assert_eq!(
            stats.dropped, refused,
            "exactly the losses that happened are accounted for"
        );
        assert_eq!(stats.io_errors, 0, "there must be no I/O errors");
    }

    #[test]
    fn critical_burst_is_one_group_commit() {
        // The point of the Immediate policy is not "an fdatasync per record"
        // but "a sync at the first opportunity". A burst of critical messages
        // should cost a handful of trips to the medium: on eMMC every fdatasync
        // is 1–10 ms, and five hundred of them would turn an incident into
        // seconds of writing and needless flash wear.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const BURST: usize = 500;
        for i in 0..BURST {
            while ns.try_log_raw(EventId(2), &[i as u8], None).is_err() {
                std::thread::yield_now();
            }
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert!(stats.records_written >= BURST as u64);
        assert!(
            stats.syncs < BURST as u64 / 4,
            "a burst of {BURST} records cost {} trips to the medium — \
             that is not a group commit",
            stats.syncs
        );
        assert!(
            stats.blocks_written < BURST as u64 / 4,
            "{BURST} records fit into {} blocks — a header per record",
            stats.blocks_written
        );
    }

    #[test]
    fn namespace_cannot_be_opened_twice() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let a = store.namespace("orc-radio-0", schema()).unwrap();
        let err = store.namespace("orc-radio-0", schema()).unwrap_err();
        assert!(matches!(err, Error::NamespaceBusy(_)), "got {err}");

        // A released name must become free: otherwise a service could not
        // reopen its namespace after being reconfigured.
        drop(a);
        store
            .namespace("orc-radio-0", schema())
            .expect("the name is free once the handle is dropped");
    }

    #[test]
    fn foreign_schema_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        drop(store.namespace("orc-radio-0", schema()).unwrap());
        store.shutdown();
        drop(store);

        let store2 = open_store(dir.path());
        let other = Schema {
            name: "another-schema",
            ..schema()
        };
        let err = store2.namespace("orc-radio-0", other).unwrap_err();
        assert!(matches!(err, Error::SchemaMismatch { .. }), "got {err}");
    }

    #[test]
    fn protocol_from_future_refused() {
        let dir = tempfile::tempdir().unwrap();
        let ns_dir = dir.path().join("orc-radio-0");
        std::fs::create_dir_all(&ns_dir).unwrap();
        let meta = NsMeta {
            schema_name: "radio".to_owned(),
            protocol_version: 99,
        };
        std::fs::write(ns_dir.join(NS_META), postcard::to_allocvec(&meta).unwrap()).unwrap();

        let store = open_store(dir.path());
        let err = store.namespace("orc-radio-0", schema()).unwrap_err();
        assert!(
            matches!(err, Error::ProtocolFromFuture { stored: 99, .. }),
            "got {err}"
        );
    }

    #[test]
    fn pending_migration_is_reported_and_meta_is_not_stamped() {
        // `protocol_version` in the metadata is the version of the last
        // COMPLETED migration. By stamping it when coming up, this build would
        // declare the namespace migrated although there has been no physical
        // run. A future migration would walk past the old segments, and they
        // would be parsed with the new version's decoders — silently and
        // wrongly. The regression would have been entirely quiet.
        use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};

        fn noop(
            _: MigrationInput<'_>,
        ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::AsIs))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: true,
            events: &[],
            metrics: &[],
            spans: &[],
            migrate: noop,
        }];

        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            assert_eq!(ns.pending_migration(), None, "the versions match");
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let v2 = Schema {
            version: ProtocolVersion(2),
            migrations: STEPS,
            ..schema()
        };
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", v2).unwrap();
        assert_eq!(
            ns.pending_migration(),
            Some((1, 2)),
            "an unfinished migration must be named"
        );
        assert_eq!(
            ns.meta().protocol_version,
            1,
            "the metadata stays at the version of the last completed migration"
        );
        ns.log_raw(EventId(1), &[2], None);
        ns.sync().unwrap();
        store.shutdown();

        // On disk the metadata still declares version 1.
        let raw = std::fs::read(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        let meta: NsMeta = postcard::from_bytes(&raw).unwrap();
        assert_eq!(meta.protocol_version, 1);

        // And the segments carry a version each — a mixed state is legitimate.
        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let mut versions: Vec<u16> = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .map(|p| {
                crate::segment::SegmentReader::open(&p)
                    .unwrap()
                    .header()
                    .protocol_version
                    .0
            })
            .collect();
        versions.sort_unstable();
        assert_eq!(
            versions,
            vec![1, 2],
            "the old segment kept its version and the new one got the current"
        );
    }

    #[test]
    fn migrate_rewrites_history_and_stamps_the_meta() {
        // The full cycle of a physical run: write v1 → come up at v2 →
        // `Namespace::migrate` — the old segment is rewritten by the step, the
        // active one is untouched, the metadata is stamped, a repeat is empty.
        // All of it on a live store with a running writer: the commit goes
        // through it.
        use crate::migrate::MigrationReport;
        use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};

        fn step(
            r: MigrationInput<'_>,
        ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
            match (r.event_id(), r.metric_id()) {
                // PowerSet is re-encoded into a fixed new payload.
                (Some(EventId(1)), _) => Ok(Some(MigrationOutcome::Message {
                    event: EventId(1),
                    payload: vec![0xAA, 0xBB],
                })),
                // The temp samples are deleted entirely.
                (_, Some(MetricId(1))) => Ok(None),
                _ => Ok(Some(MigrationOutcome::AsIs)),
            }
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            events: &[EventId(1)],
            metrics: &[MetricId(1)],
            spans: &[],
            migrate: step,
        }];

        let dir = tempfile::tempdir().unwrap();
        // Run 1: a version 1 history — an event, a sample and a span.
        {
            let store = open_store(dir.path());
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[1, 2, 3], None);
            ns.series(TEMP).unwrap().sample(36.6);
            ns.log_raw(EventId(1), &[4, 5], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let v2 = Schema {
            version: ProtocolVersion(2),
            migrations: STEPS,
            ..schema()
        };
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", v2).unwrap();
        assert_eq!(ns.pending_migration(), Some((1, 2)));

        // A live version 2 segment — the run has to walk past it.
        ns.log_raw(EventId(1), &[9], None);
        ns.sync().unwrap();

        let report = ns.migrate().expect("the run goes through");
        assert_eq!(report.rewritten, 1, "{report:?}");
        assert_eq!(report.already_current, 1, "the active segment is untouched");
        assert_eq!(
            report.records_rewritten, 2,
            "two PowerSet records survived the step"
        );
        assert_eq!(report.records_dropped, 1, "the temp sample was deleted");
        assert_eq!(report.skipped_untouched + report.emptied, 0, "{report:?}");

        assert_eq!(ns.pending_migration(), None, "there is no debt left");
        assert_eq!(
            ns.meta().protocol_version,
            2,
            "the metadata is stamped in memory"
        );
        let raw = std::fs::read(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        let meta: NsMeta = postcard::from_bytes(&raw).unwrap();
        assert_eq!(meta.protocol_version, 2, "and on disk");

        // The rewritten segment: the version is current, there is a footer, and
        // the content follows the step — the payloads are replaced and no
        // samples are left.
        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let mut rewritten_payloads = Vec::new();
        let mut samples = 0;
        for entry in std::fs::read_dir(&seg_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|x| x != "seg") {
                continue;
            }
            let mut reader = crate::segment::SegmentReader::open(&path).unwrap();
            assert_eq!(
                reader.header().protocol_version.0,
                2,
                "no earlier versions are left on disk: {path:?}"
            );
            let mut buf = Vec::new();
            for offset in reader.scan_block_offsets().0 {
                reader.read_block_at(offset, &mut buf).unwrap();
                let block = crate::segment::parse_block(&buf).unwrap().unwrap();
                for item in block.records() {
                    match item.unwrap().1 {
                        dduroc_format::Record::Message(m) => {
                            rewritten_payloads.push(m.payload.to_vec());
                        }
                        dduroc_format::Record::Sample(_) => samples += 1,
                        _ => {}
                    }
                }
            }
        }
        rewritten_payloads.sort();
        assert_eq!(
            rewritten_payloads,
            vec![vec![9], vec![0xAA, 0xBB], vec![0xAA, 0xBB]],
            "the old payloads were rewritten by the step, the new one is as it was"
        );
        assert_eq!(samples, 0, "samples deleted by a step do not come back");

        // A second run is an honest no-op.
        assert_eq!(ns.migrate().unwrap(), MigrationReport::default());
        store.shutdown();
    }

    #[test]
    fn an_untouched_segment_is_skipped_and_keeps_its_version() {
        // A segment holding not one affected type spends no flash write cycle:
        // it is not rewritten and keeps the earlier version in its header —
        // with the metadata stamped. That is legitimate: being untouched is
        // precisely what makes the current decoders read it correctly.
        use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};

        fn nope(
            _: MigrationInput<'_>,
        ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::Message {
                event: EventId(2),
                payload: vec![0xEE],
            }))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            // The step touches only Alarm, which will not be in the history.
            events: &[EventId(2)],
            metrics: &[],
            spans: &[],
            migrate: nope,
        }];

        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[7], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let v2 = Schema {
            version: ProtocolVersion(2),
            migrations: STEPS,
            ..schema()
        };
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", v2).unwrap();
        let report = ns.migrate().unwrap();
        assert_eq!(report.skipped_untouched, 1, "{report:?}");
        assert_eq!(report.rewritten, 0, "the flash is untouched");
        assert_eq!(ns.pending_migration(), None, "the metadata is stamped");

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let old = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        let reader = crate::segment::SegmentReader::open(&old).unwrap();
        assert_eq!(
            reader.header().protocol_version.0,
            1,
            "an untouched segment keeps its version, and that is legitimate"
        );
        store.shutdown();
    }

    #[test]
    fn bad_namespace_names_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        for bad in ["../escape", "a/b", "", ".hidden"] {
            assert!(
                store.namespace(bad, schema()).is_err(),
                "{bad:?} must be rejected"
            );
        }
    }

    #[test]
    fn telemetry_series_and_spans() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let temp = ns.series(TEMP).unwrap();
        for i in 0..50 {
            temp.sample(20.0 + i as f32);
        }
        // The dynamic path checks the type against the schema itself: with a
        // typed `sample` such a value simply would not have been assembled.
        let dyn_temp = ns.series_untyped(MetricId(1)).unwrap();
        let e = dyn_temp
            .try_sample_raw(OwnedValue::U64(1))
            .expect_err("the value type must match what was declared");
        assert!(
            e.breaks_contract(),
            "this is a defect in the call, not a loss: {e}"
        );

        {
            let cal = ns.span(SpanKindId(1));
            cal.log_raw(EventId(1), &[7]);
            let _child = cal.child(SpanKindId(1));
        } // both spans close here

        ns.sync().unwrap();
        let stats = store.stats();
        // 50 samples + 2 SpanStart + 1 event + 2 SpanEnd
        assert!(stats.records_written >= 55, "got {stats:?}");
        assert!(stats.is_clean());
    }

    #[test]
    fn sync_waits_for_everything_already_enqueued() {
        // Control commands travel on a separate queue and without an explicit
        // drain would overtake records in flight: sync would report success
        // without having written them, and shutdown would seal segments over
        // what was not written yet. Checked on a thread that knowably outruns
        // the writer.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const N: usize = 20_000;
        let mut accepted = 0u64;
        for i in 0..N {
            // Retries, to separate queue losses (the ordinary channel's normal
            // behaviour) from losses during syncing.
            while ns.try_log_raw(EventId(1), &[i as u8; 4], None).is_err() {
                std::thread::yield_now();
            }
            accepted += 1;
        }
        ns.sync().unwrap();

        let stats = store.stats();
        // There may be MORE records than were accepted: failed try_send
        // attempts leave a loss notice in the stream. Fewer is not allowed.
        assert!(
            stats.records_written >= accepted,
            "sync must wait for every accepted record, not only those that made it: \
             {} written, {accepted} accepted",
            stats.records_written
        );
    }

    #[test]
    fn shutdown_persists_everything_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const N: usize = 20_000;
        let mut accepted = 0u64;
        for i in 0..N {
            while ns.try_log_raw(EventId(1), &[i as u8; 4], None).is_err() {
                std::thread::yield_now();
            }
            accepted += 1;
        }
        store.shutdown();

        let written = store.stats().records_written;
        assert!(
            written >= accepted,
            "shutdown must drain the queue rather than seal over it: \
             {written} written, {accepted} accepted"
        );
    }

    #[test]
    fn segment_name_matches_time_of_its_first_record() {
        // A segment's name and base have to match the time of its FIRST record:
        // selecting segments by range at read time rests on that. Taking the
        // time of the previous record (zero for a new channel) would mean
        // silently handing a reader segments it did not ask for and skipping
        // the ones it needs.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // A short busy pause so that the start time is knowably above 0.
        let mut spin = 0u64;
        while ns.now().at.0 < 1_000 && spin < 200_000_000 {
            spin += 1;
        }
        let before = ns.now();
        ns.log_raw(EventId(1), &[1], None);
        ns.sync().unwrap();
        let after = ns.now();

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let name = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|n| n.ends_with(".seg"))
            .expect("a segment was created");
        let parsed = dduroc_format::segment::SegmentName::parse(&name).expect("the name parses");

        assert!(
            parsed.start() >= before && parsed.start() <= after,
            "segment name {name} must carry the time of the first record ({before}..{after})"
        );
        assert_ne!(parsed.base.0, 0, "a zero base is the sign of the old bug");
    }

    #[test]
    fn namespace_keeps_store_alive() {
        // A Store being dropped stops the writer. A Namespace that outlived it
        // would write into nothing while returning Ok on every call — the worst
        // kind of data loss: without a single sign.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();
        drop(store);

        for i in 0..100 {
            ns.try_log_raw(EventId(1), &[i as u8], None)
                .expect("writing must go on working");
        }
        ns.sync().expect("syncing must work");

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let total: u64 = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
            .count() as u64;
        assert!(total >= 1, "the data was written to disk");
    }

    #[test]
    fn failed_namespace_open_releases_the_name() {
        // The "taken" mark is set before the namespace comes up; an early
        // return has to clear it, or the name stays unavailable for the life of
        // the process.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());

        let broken = Schema {
            version: ProtocolVersion(0), // not allowed
            ..schema()
        };
        assert!(store.namespace("orc-radio-0", broken).is_err());
        // The name is free again.
        store
            .namespace("orc-radio-0", schema())
            .expect("the name must be freed after a failure");
    }

    #[test]
    fn enum_states_are_written_as_plain_integers() {
        // The point of the model: a state on disk is an ordinary integer, and
        // everything else (the name, the severity) lives in the schema and
        // takes no room.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let link = ns.series(LINK).unwrap();
        assert_eq!(link.kind(), MetricKind::State);
        for st in [LinkState::Los, LinkState::Sync, LinkState::Lock] {
            link.sample(st);
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(stats.records_written, 3, "not one bookkeeping record");
        assert!(stats.is_clean(), "{stats:?}");

        // Three samples: each a header, a time delta, a metric and a code.
        assert!(
            stats.bytes_written < 32 + 3 * 8,
            "three states took {} bytes including the block header",
            stats.bytes_written
        );
    }

    #[test]
    fn sealed_segment_lists_its_metrics_in_the_footer() {
        // A migration decides from this set whether to rewrite a segment, and a
        // reader whether it is worth opening. An empty set would mean a
        // migration silently skips the whole telemetry history, and a search
        // for a state before a window throws away the segment that holds it.
        // The regression would have been entirely quiet — hence this test.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        ns.series(TEMP).unwrap().sample(36.6);
        ns.series(LINK).unwrap().sample(LinkState::Lock);
        ns.log_raw(EventId(1), &[1], None);
        store.shutdown();

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let path = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .expect("a segment was created");

        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        assert!(reader.is_sealed(), "shutdown seals the segment");
        let footer = reader.footer().expect("the footer reads");
        assert_eq!(
            footer.metrics,
            vec![MetricId(1), MetricId(2)],
            "both metrics must be listed"
        );
        assert_eq!(footer.events, vec![EventId(1)]);
        assert!(
            footer.touches(&[], &[MetricId(2)]),
            "a migration must see that the segment is affected"
        );
        assert!(!footer.touches(&[], &[MetricId(9)]));
    }

    #[test]
    fn state_of_a_foreign_metric_is_refused() {
        // State codes belong to their own metrics. Writing a foreign code would
        // give a chart labelled with someone else's names.
        //
        // On the typed path this is a **compile error**: a `Series<f32>` will
        // not accept a `LinkState`, and there is nothing to check. What is
        // checked is the dynamic path — the only remaining way to slip a
        // foreign value in.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let temp = ns.series_untyped(MetricId(1)).unwrap();
        let err = temp
            .try_sample_raw(OwnedValue::U64(LinkState::Lock as u64))
            .unwrap_err();
        assert!(matches!(err, Error::ValueTypeMismatch { .. }), "got {err}");
        assert!(err.breaks_contract());
    }

    #[test]
    fn limits_default_from_schema_and_override_at_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // The schema's defaults.
        let sev = |v: f32| ns.severity_of(TEMP, v);
        assert_eq!(sev(36.6), Severity::Normal);
        assert_eq!(sev(75.0), Severity::Warn);
        assert_eq!(sev(90.0), Severity::Alarm);

        // The external system determined the amplifier model and narrowed the
        // limits — with the same range expressions as in the schema.
        ns.set_thresholds(TEMP, ..=40.0, ..=50.0).unwrap();
        assert_eq!(
            sev(45.0),
            Severity::Warn,
            "the schema would have said Normal"
        );
        let eff = ns.limits(MetricId(1)).unwrap();
        assert!(eff.overridden);
        assert_eq!(eff.unit, "°C");
        assert_eq!(eff.kind, MetricKind::Gauge);

        // States: the severity comes from the state, not from ranges.
        let link = ns.limits(MetricId(2)).unwrap();
        assert_eq!(link.kind, MetricKind::State);
        assert_eq!(link.states.len(), 3);
        assert_eq!(link.states[0].name, "Los");
        assert_eq!(link.states[0].severity, Severity::Alarm);
        assert_eq!(ns.severity_of(LINK, LinkState::Los), Severity::Alarm);

        // A series knows its metric: severity is asked with the same value as a
        // sample — without assembling an OwnedValue by hand and without
        // repeating the metric.
        let link_series = ns.series(LINK).unwrap();
        assert_eq!(link_series.severity_of(LinkState::Los), Severity::Alarm);
        assert_eq!(link_series.severity_of(LinkState::Sync), Severity::Warn);
        assert_eq!(link_series.severity_of(LinkState::Lock), Severity::Normal);
        assert_eq!(
            link_series.severity_of(LinkState::Los),
            link_series.severity_of_raw(&OwnedValue::U64(0)),
            "the typed path must answer the same as the runtime one"
        );

        // The limits do not reach the record stream.
        ns.sync().unwrap();
        assert_eq!(
            store.stats().records_written,
            0,
            "limits are a setting, not data: there is nowhere to write them"
        );
        // Removing the override brings back what the schema declared.
        ns.clear_limits(TEMP).unwrap();
        assert_eq!(sev(45.0), Severity::Normal);
        assert!(!ns.limits(TEMP).unwrap().overridden);

        // A metric not from this schema is a defect in the call, not a loss.
        let e = ns.clear_limits(MetricId(99)).unwrap_err();
        assert!(e.breaks_contract(), "{e}");
    }

    #[test]
    fn numeric_bounds_on_a_state_metric_are_refused_at_runtime_too() {
        // Typing `set_thresholds` removed this error from the typed call —
        // `set_thresholds(LINK, ..)` no longer compiles — but the runtime path
        // remains and has to answer the same way: the web layer and
        // `set_limits` go through it, and there the metric has no type.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let e = ns
            .set_thresholds_raw(MetricId(2), ..=1.0, ..=2.0)
            .unwrap_err();
        assert!(matches!(e, Error::BadLimits { .. }), "{e}");

        // And on a numeric metric the same bounds go in through the hatch.
        ns.set_thresholds_raw(MetricId(1), ..=40.0, ..=50.0)
            .unwrap();
        assert_eq!(ns.severity_of(TEMP, 45.0), Severity::Warn);
    }

    #[test]
    fn limits_are_per_namespace_not_per_process() {
        // Every instance has hardware of its own: process-wide limits would be
        // wrong for all of them at once.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let a = store.namespace("orc-radio-0", schema()).unwrap();
        let b = store.namespace("orc-radio-1", schema()).unwrap();

        a.set_thresholds(TEMP, ..=10.0, ..).unwrap();

        assert_eq!(a.severity_of(TEMP, 20.0), Severity::Warn);
        assert_eq!(
            b.severity_of(TEMP, 20.0),
            Severity::Normal,
            "the neighbour kept its limits"
        );
    }

    #[test]
    fn unknown_ids_are_counted_and_announced_once() {
        // An id from a foreign schema is a build defect. The write path returns
        // nothing, so such a call must not vanish without trace: it is counted
        // by a separate counter and announced **once** by a record in the
        // journal. Announcing every one would flood the journal — a contract
        // violation repeats on every turn of a loop.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let e = ns.try_log_raw(EventId(99), &[], None).unwrap_err();
        assert!(e.breaks_contract(), "{e}");
        assert!(ns.series_untyped(MetricId(99)).is_err());

        for _ in 0..100 {
            ns.log_raw(EventId(99), &[], None);
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(stats.rejected, 100, "every refusal is accounted for");
        assert_eq!(stats.dropped, 0, "this is not a loss caused by the disk");
        assert!(!stats.is_clean(), "the defect must be visible");
        assert_eq!(
            stats.records_written, 1,
            "one announcement for all hundred calls: got {}",
            stats.records_written
        );

        // A span kind from a foreign schema: the guard is handed out all the
        // same, or application code's nesting would depend on the schema.
        let span = ns.span(SpanKindId(99));
        assert!(span.id().0 > 0);
    }
}
