//! The reader: merging streams and resolving the schema.

use crate::cursor::{ChannelCursor, Damage, OwnedRecord, OwnedSampleValue};
use crate::error::{ReadError, Result};
use crate::query::{Bounds, Filter, KindFilter, Order, Query};
use chrono::{DateTime, Utc};
use dduroc_engine::backend::ReadBackend;
use dduroc_engine::epochs::{EpochStore, Epochs};
use dduroc_engine::namespace::{NS_META, NsMeta};
use dduroc_engine::schema::{MetricKind, Schema, Severity, StorageClass};
use dduroc_engine::store::Store;
use dduroc_format::{BootCounter, BootTime, EventId, Level, MetricId, SpanId, Value};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// How many segments back to look for the state at a window's left edge.
///
/// The bound is there so the search does not walk back through the whole
/// retention depth: a series unchanged for a month would cost reading a month
/// of data for one value. Two segments are tens of megabytes of history at a
/// typical size. The raft backend instead bounds this probe to 256 eligible
/// committed blocks; a state older than that horizon may have no seed.
const SEED_SEGMENTS: usize = 2;

/// Borrow an owning value as a format value — for computing severity.
fn as_format_value(v: &OwnedSampleValue) -> Value<'_> {
    match v {
        OwnedSampleValue::F32(x) => Value::F32(*x),
        OwnedSampleValue::F64(x) => Value::F64(*x),
        OwnedSampleValue::I64(x) => Value::I64(*x),
        OwnedSampleValue::U64(x) => Value::U64(*x),
        OwnedSampleValue::Bool(x) => Value::Bool(*x),
        OwnedSampleValue::Blob(b) => Value::Blob(b),
    }
}

/// The kind of a record in an answer.
///
/// The enum is deliberately **closed**, unlike [`QueryResult`] and [`Damage`].
/// The difference is what silence costs: a surplus field in a report can go
/// unread, whereas a record kind the displaying code does not know is a line
/// that silently vanished from the screen. Better that the build not compile.
/// Records of a kind **this build itself** does not know arrive as
/// [`EntryKind::Ext`] — there is always an arm for those.
#[derive(Debug, Clone, PartialEq)]
pub enum EntryKind {
    /// A schema message.
    Message {
        event: EventId,
        /// The type name from the schema; `None` means the schema is unknown to
        /// this build.
        name: Option<&'static str>,
        level: Option<Level>,
        tags: &'static [&'static str],
        payload: Vec<u8>,
    },
    /// Free text without a schema (the tracing bridge, a panic handler).
    Text {
        level: Level,
        target: String,
        text: String,
    },
    SpanStart {
        span: SpanId,
        kind_name: Option<&'static str>,
        parent: Option<SpanId>,
    },
    SpanEnd {
        span: SpanId,
    },
    /// A telemetry sample.
    ///
    /// Only the metric and the value come from the record; everything needed to
    /// display it is resolved from the schema and takes no room on disk. The
    /// fields are `Option` because the schema may be unknown to this build —
    /// then the identifier and the number remain, which is more honest than
    /// inventing a name.
    Sample {
        metric: MetricId,
        metric_name: Option<&'static str>,
        unit: Option<&'static str>,
        /// The metric's static category tags.
        tags: &'static [&'static str],
        /// How to draw the quantity: a state is held as a step, a continuous
        /// quantity is interpolated. A straight line through values that never
        /// were is not cosmetic but a lie on the chart.
        kind: Option<MetricKind>,
        /// The state's label, if the metric is an enum and the code is
        /// declared.
        state_name: Option<&'static str>,
        /// The value's severity by the limits **from the schema**.
        ///
        /// Runtime overrides are unavailable to a reader by design: it may be
        /// reading a dump from another device where different settings applied,
        /// and the limits are never written into a dump (see
        /// `dduroc_engine::limits`).
        severity: Option<Severity>,
        value: OwnedSampleValue,
    },
    /// An unrecognized format extension.
    Ext {
        bytes: Vec<u8>,
    },
}

/// A record in an answer.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Entry {
    pub namespace: std::sync::Arc<str>,
    /// In-process identity of the effective schema used to decode this entry.
    /// Unknown schemas do not have an identity.
    pub schema_identity: Option<dduroc_engine::schema::SchemaIdentity>,
    /// The storage class whose channel the record lay in. A channel is a class:
    /// it has no string name — it has a directory named after the class.
    pub channel: StorageClass,
    /// Relative time: a run plus microseconds since it started. Always present
    /// — a device needs neither an RTC nor a synchronization for it.
    pub at: BootTime,
    /// Wall-clock time. `None` means the hardware boot has no synchronization
    /// anchor, and the record simply has no absolute time.
    pub utc: Option<DateTime<Utc>>,
    /// The span the record is attached to.
    pub span: Option<SpanId>,
    pub kind: EntryKind,
}

impl Entry {
    /// The record's level: from the schema for messages, from the record itself
    /// for text.
    pub fn level(&self) -> Option<Level> {
        match &self.kind {
            EntryKind::Message { level, .. } => *level,
            EntryKind::Text { level, .. } => Some(*level),
            _ => None,
        }
    }

    /// The engine's notice about lost records: how many dropped out right
    /// before this notice.
    ///
    /// Losses are announced in the stream itself — a hole nobody mentions is
    /// indistinguishable from silence. The notice's format belongs to the
    /// engine ([`dduroc_engine::diag`]), and application code has no need to
    /// parse its text by hand.
    pub fn dropped_records(&self) -> Option<u64> {
        match &self.kind {
            EntryKind::Text { target, text, .. } => {
                dduroc_engine::diag::parse_drop_notice(target, text)
            }
            _ => None,
        }
    }
}

/// The answer to a query.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct QueryResult {
    pub entries: Vec<Entry>,
    /// The states carried through to the window's left edge: the last sample of
    /// every state series **before** `from` (see [`Query::seed_states`]).
    ///
    /// They lie apart from `entries` deliberately: their time is outside the
    /// range asked for, and mixing them in with the rest would break the
    /// promise that everything in the answer is inside the window.
    pub seeds: Vec<Entry>,
    /// The fragments that could not be read. An empty list means the answer is
    /// complete.
    pub damaged: Vec<Damage>,
    /// The answer was cut short by `limit`.
    pub truncated: bool,
    /// The runs whose segments would have reached the selection but dropped
    /// out: the bounds are in wall-clock time and those runs have no
    /// synchronization anchor — there is nothing to compare their records with
    /// a wall clock with.
    ///
    /// A non-empty list means part of the history did not reach the answer, and
    /// **not because it is not there**. Without this field such an answer would
    /// look like "the device wrote nothing in the hours asked for".
    pub unanchored: Vec<BootCounter>,
}

/// A page from an owned, in-process query session.
#[derive(Debug, Clone)]
pub struct QueryPage {
    pub entries: Vec<Entry>,
    pub seeds: Vec<Entry>,
    pub exhausted: bool,
    /// Whether the query limit cut the answer. With a zero query limit this
    /// is conservative: retained blocks imply possible, not proven, matches.
    pub truncated: bool,
    pub damaged: Vec<Damage>,
    pub unanchored: Vec<BootCounter>,
}

/// One bounded catalog row and its in-process schema, when known.
#[derive(Debug, Clone)]
pub struct CatalogChannel {
    pub namespace: String,
    pub class: StorageClass,
    pub schema_name: String,
    pub protocol_version: u16,
    pub retained_bytes: u64,
    pub quota_bytes: Option<u64>,
    pub schema: Option<Schema>,
}

#[derive(Debug, Clone)]
pub struct ClassStatistics {
    pub class: StorageClass,
    pub logical_retained_bytes: u64,
    /// Configured budget is available only from a live Store.
    pub limit_bytes: Option<u64>,
}

/// Global retained wire-record counts and independently sampled storage use.
/// Counts include text, samples, span boundaries and extension records.
#[derive(Debug, Clone)]
pub struct Statistics {
    pub captured_at: chrono::DateTime<chrono::Utc>,
    pub counting_state: dduroc_engine::backend::CountingState,
    pub retained_records: Option<u64>,
    pub retained_blocks: Option<u64>,
    pub logical_retained_bytes: u64,
    pub channels: u64,
    pub classes: Vec<ClassStatistics>,
    /// Process-lifetime writer counters; absent for an offline reader.
    pub runtime: Option<dduroc_engine::Stats>,
    /// Optional separately sampled allocated filesystem blocks per root.
    pub roots: Option<Vec<dduroc_engine::backend::RootPhysicalStats>>,
}

/// A fixed roster and high-water snapshot. This is not a portable cursor and
/// cannot be resumed after the session or process has ended.
#[derive(Debug)]
pub struct QuerySession {
    reader: Arc<Reader>,
    query: Query,
    epochs: Epochs,
    bounds: Bounds,
    snapshots: Vec<dduroc_engine::backend::ChannelSnapshot>,
    cursors: Vec<ChannelCursor>,
    schemas: Vec<Option<Schema>>,
    heads: BinaryHeap<Head>,
    budget: Arc<crate::budget::Budget>,
    pending: Option<PendingEntry>,
    seeds: std::collections::VecDeque<Entry>,
    seed_damage: Vec<Damage>,
    yielded: usize,
    done: bool,
    zero_limit_truncated: bool,
    navigation: Navigation,
}

#[derive(Debug, Default)]
enum Navigation {
    #[default]
    Unset,
    Sequential,
    Indexed {
        records: usize,
        bytes: usize,
        page_number: u64,
        page: Option<QueryPage>,
    },
}

/// Exact totals for a complete snapshot with fixed page dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryTotals {
    /// Matching query entries, across all record types; excludes state seeds.
    pub total_matches: u64,
    /// State seeds included in the page layout, separate from query entries.
    pub total_seeds: u64,
    /// Pages with the requested dimensions; an empty result has one page.
    pub total_pages: u64,
}

/// Result of a bounded indexed navigation step in a fixed snapshot.
#[derive(Debug)]
pub struct SeekPage {
    pub reached_page: u64,
    pub target_reached: bool,
    pub exhausted: bool,
    pub page: Option<QueryPage>,
}

#[derive(Debug)]
struct PendingEntry {
    entry: Entry,
    cursor: usize,
    required: Option<u64>,
}

fn session_error(stop: crate::budget::Stop) -> ReadError {
    match stop {
        crate::budget::Stop::Cancelled => ReadError::Cancelled,
        crate::budget::Stop::Deadline => ReadError::Deadline,
        crate::budget::Stop::Limit(name) => ReadError::LimitExceeded(name),
        crate::budget::Stop::Expired => ReadError::SnapshotExpired,
        crate::budget::Stop::RetentionGap => ReadError::RetentionGap,
        crate::budget::Stop::PageTooSmall => ReadError::PageTooSmall,
        crate::budget::Stop::Backend(message) => ReadError::SessionFailed(message),
    }
}

fn entry_size(entry: &Entry) -> usize {
    let payload = match &entry.kind {
        EntryKind::Message { payload, .. } | EntryKind::Ext { bytes: payload } => payload.len(),
        EntryKind::Text { target, text, .. } => target.len().saturating_add(text.len()),
        EntryKind::Sample {
            value: OwnedSampleValue::Blob(bytes),
            ..
        } => bytes.len(),
        _ => 0,
    };
    std::mem::size_of::<Entry>()
        .saturating_add(entry.namespace.len())
        .saturating_add(payload)
}

impl QuerySession {
    fn check(&self) -> Result<()> {
        self.budget.checkpoint();
        if let Some(stop) = self.budget.failure() {
            return Err(session_error(stop));
        }
        let backend = self
            .reader
            .backend
            .as_ref()
            .expect("session has raft backend");
        for (idx, (snapshot, cursor)) in self.snapshots.iter().zip(&self.cursors).enumerate() {
            if !self.budget.checkpoint() {
                return Err(session_error(self.budget.failure().unwrap()));
            }
            let mut required = cursor.required_index();
            if let Some(pending) = self
                .pending
                .as_ref()
                .filter(|pending| pending.cursor == idx)
            {
                required = match (required, pending.required) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
            }
            if !self.seeds.is_empty() {
                required = match (required, snapshot.info.first_index) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                };
            }
            let status = backend
                .snapshot_status(snapshot, required)
                .map_err(|error| {
                    self.budget
                        .stop(crate::budget::Stop::Backend(error.to_string()));
                    session_error(self.budget.failure().unwrap())
                })?;
            match status {
                dduroc_engine::backend::SnapshotStatus::Current => {}
                dduroc_engine::backend::SnapshotStatus::Replaced => {
                    self.budget.stop(crate::budget::Stop::Expired)
                }
                dduroc_engine::backend::SnapshotStatus::Evicted => {
                    self.budget.stop(crate::budget::Stop::RetentionGap)
                }
            }
            if let Some(stop) = self.budget.failure() {
                return Err(session_error(stop));
            }
        }
        Ok(())
    }

    fn rewind(&mut self) -> Result<()> {
        self.budget.checkpoint();
        if let Some(stop) = self.budget.failure() {
            return Err(session_error(stop));
        }
        let backend = self.reader.backend.as_ref().expect("session has backend");
        for snapshot in &self.snapshots {
            if !self.budget.checkpoint() {
                return Err(session_error(self.budget.failure().unwrap()));
            }
            let status = backend
                .snapshot_status(snapshot, snapshot.info.first_index)
                .map_err(|error| {
                    self.budget
                        .stop(crate::budget::Stop::Backend(error.to_string()));
                    session_error(self.budget.failure().unwrap())
                })?;
            match status {
                dduroc_engine::backend::SnapshotStatus::Current => {}
                dduroc_engine::backend::SnapshotStatus::Replaced => {
                    self.budget.stop(crate::budget::Stop::Expired);
                    return Err(session_error(self.budget.failure().unwrap()));
                }
                dduroc_engine::backend::SnapshotStatus::Evicted => {
                    self.budget.stop(crate::budget::Stop::RetentionGap);
                    return Err(session_error(self.budget.failure().unwrap()));
                }
            }
        }
        let (seeds, damage) = self.reader.snapshot_seeds(
            &self.query,
            &self.bounds,
            &self.epochs,
            &self.snapshots,
            &self.budget,
        )?;
        let mut cursors = Vec::with_capacity(self.snapshots.len());
        for (snapshot, schema) in self.snapshots.iter().zip(&self.schemas) {
            if !self.budget.checkpoint() {
                return Err(session_error(self.budget.failure().unwrap()));
            }
            let scope = crate::cursor::ChannelScope {
                budget: Some(Arc::clone(&self.budget)),
                bounds: self.bounds.clone(),
                boot: self.query.boot,
                reverse: self.query.order == Order::Newest,
                expect_store: self.reader.store_id,
                prefilter: Some(build_prefilter(&self.query, *schema)),
                max_segments: None,
                require_metrics: None,
                migrations: schema.as_ref().map(crate::cursor::MigrationCtx::of),
                liveness: crate::cursor::Liveness::Frozen,
            };
            cursors.push(ChannelCursor::open_snapshot(
                Arc::clone(backend),
                snapshot.clone(),
                &scope,
            ));
        }
        let mut heads = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors
            .iter_mut()
            .enumerate()
            .filter(|_| self.query.limit != Some(0))
        {
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first: self.query.order == Order::Newest,
                });
            }
            if let Some(stop) = self.budget.failure() {
                return Err(session_error(stop));
            }
        }
        self.cursors = cursors;
        self.heads = heads;
        self.pending = None;
        self.seeds = seeds.into();
        self.seed_damage = damage;
        self.yielded = 0;
        self.done = self.query.limit == Some(0);
        self.check()
    }

    /// Navigate within the original snapshot, examining at most `max_pages` per call.
    /// Page dimensions are fixed for the session; work budgets remain cumulative.
    pub fn seek_page(
        &mut self,
        target: u64,
        page_limits: crate::PageLimits,
        max_pages: usize,
    ) -> Result<SeekPage> {
        let records = page_limits.max_records();
        let bytes = page_limits.max_bytes();
        if target == 0 || max_pages == 0 {
            return Err(ReadError::InvalidNavigation);
        }
        self.check()?;
        match &self.navigation {
            Navigation::Sequential => return Err(ReadError::InvalidNavigation),
            Navigation::Indexed {
                records: r,
                bytes: b,
                ..
            } if *r != records || *b != bytes => return Err(ReadError::InvalidNavigation),
            _ => {}
        }
        let previous = std::mem::take(&mut self.navigation);
        let (mut number, mut page) = match previous {
            Navigation::Indexed {
                page_number, page, ..
            } => (page_number, page),
            _ => (0, None),
        };
        let mut page_read_this_call = false;
        if target < number {
            self.rewind()?;
            number = 0;
            page = None;
        }
        for _ in 0..max_pages {
            if number >= target || page.as_ref().is_some_and(|p| p.exhausted) {
                break;
            }
            let next = match self.read_page(records, bytes) {
                Ok(page) => page,
                Err(error) => {
                    if matches!(error, ReadError::PageTooSmall) {
                        self.budget.stop(crate::budget::Stop::PageTooSmall);
                    }
                    self.navigation = Navigation::Indexed {
                        records,
                        bytes,
                        page_number: number,
                        page,
                    };
                    return Err(error);
                }
            };
            // A full preceding page can be followed only by filtered-out
            // records. Fold that empty terminal scan into the actual last page.
            if next.exhausted && next.entries.is_empty() && next.seeds.is_empty() && number > 0 {
                if let Some(previous) = page.as_mut() {
                    previous.exhausted = true;
                    previous.truncated |= next.truncated;
                    previous.damaged = next.damaged;
                    previous.unanchored = next.unanchored;
                }
                break;
            }
            number += 1;
            page = Some(next);
            page_read_this_call = true;
        }
        let exhausted = page.as_ref().is_some_and(|p| p.exhausted);
        let reached = number == target;
        let output = if reached || exhausted {
            if !page_read_this_call && let Some(cached) = &page {
                for entry in cached.entries.iter().chain(&cached.seeds) {
                    if !self.budget.returned(entry_size(entry) as u64) {
                        return Err(session_error(self.budget.failure().unwrap()));
                    }
                }
            }
            page.clone()
        } else {
            None
        };
        self.navigation = Navigation::Indexed {
            records,
            bytes,
            page_number: number,
            page,
        };
        Ok(SeekPage {
            reached_page: number,
            target_reached: reached,
            exhausted,
            page: output,
        })
    }

    /// Advance the session by one bounded page. `PageTooSmall` is retryable
    /// with a larger byte limit and retains the pending entry. All cancellation,
    /// resource, expiry and backend failures are terminal: any partial page
    /// accumulated during the failed call is discarded and is never reported
    /// as EOF.
    pub fn next_page(&mut self, limits: crate::PageLimits) -> Result<QueryPage> {
        if matches!(self.navigation, Navigation::Indexed { .. }) {
            return Err(ReadError::InvalidNavigation);
        }
        self.navigation = Navigation::Sequential;
        self.read_page(limits.max_records(), limits.max_bytes())
    }

    fn read_page(&mut self, max_records: usize, max_bytes: usize) -> Result<QueryPage> {
        self.check()?;
        if max_records == 0 || max_bytes == 0 {
            return Err(ReadError::PageTooSmall);
        }
        let mut entries = Vec::new();
        let mut seeds = Vec::new();
        let mut bytes = 0usize;
        let mut truncated = self.zero_limit_truncated;
        while entries.len() + seeds.len() < max_records {
            let Some(seed) = self.seeds.front() else {
                break;
            };
            let size = entry_size(seed);
            if bytes.saturating_add(size) > max_bytes {
                if seeds.is_empty() {
                    return Err(ReadError::PageTooSmall);
                }
                break;
            }
            bytes += size;
            seeds.push(self.seeds.pop_front().unwrap());
        }
        if !self.seeds.is_empty() {
            self.check()?;
            return Ok(QueryPage {
                entries,
                seeds,
                exhausted: false,
                truncated,
                damaged: self.seed_damage.clone(),
                unanchored: Vec::new(),
            });
        }
        while entries.len() + seeds.len() < max_records && !self.done {
            let entry = if let Some(entry) = self.pending.take() {
                Some(entry)
            } else {
                loop {
                    if !self.budget.checkpoint() {
                        return Err(session_error(self.budget.failure().unwrap()));
                    }
                    let Some(Head { idx, .. }) = self.heads.pop() else {
                        self.done = true;
                        break None;
                    };
                    let required = self.cursors[idx].required_index();
                    let raw = self.cursors[idx].next_entry();
                    if let Some(head) = self.cursors[idx].peek() {
                        self.heads.push(Head {
                            at: head.at,
                            idx,
                            newest_first: self.query.order == Order::Newest,
                        });
                    }
                    if let Some(stop) = self.budget.failure() {
                        return Err(session_error(stop));
                    }
                    let Some(raw) = raw else {
                        continue;
                    };
                    if !self.bounds.contains(raw.at) {
                        continue;
                    }
                    let cursor = &self.cursors[idx];
                    if let Some(entry) = self.reader.build_entry(
                        Arc::clone(&cursor.namespace),
                        cursor.channel,
                        self.schemas[idx].as_ref(),
                        raw,
                        &self.query,
                        &self.epochs,
                    ) {
                        break Some(PendingEntry {
                            entry,
                            cursor: idx,
                            required,
                        });
                    }
                }
            };
            let Some(entry) = entry else {
                break;
            };
            if self.query.limit.is_some_and(|limit| self.yielded >= limit) {
                self.done = true;
                truncated = true;
                break;
            }
            let size = entry_size(&entry.entry);
            if bytes.saturating_add(size) > max_bytes {
                self.pending = Some(entry);
                if entries.is_empty() && seeds.is_empty() {
                    return Err(ReadError::PageTooSmall);
                }
                break;
            }
            if !self.budget.returned(size as u64) {
                self.pending = Some(entry);
                return Err(session_error(self.budget.failure().unwrap()));
            }
            self.yielded += 1;
            bytes += size;
            entries.push(entry.entry);
        }
        if self.pending.is_none() && self.heads.is_empty() {
            self.done = true;
        }
        self.check()?;
        let mut damaged: Vec<_> = self
            .cursors
            .iter()
            .flat_map(ChannelCursor::damaged)
            .collect();
        damaged.extend(self.seed_damage.clone());
        let mut unanchored: Vec<_> = self
            .cursors
            .iter()
            .flat_map(|c| c.unanchored().iter().copied())
            .collect();
        unanchored.sort_unstable();
        unanchored.dedup();
        Ok(QueryPage {
            entries,
            seeds,
            exhausted: self.done,
            truncated,
            damaged,
            unanchored,
        })
    }
}

impl QueryResult {
    /// Whether no data was skipped due to damage, truncation or absent anchors.
    pub fn is_complete(&self) -> bool {
        self.damaged.is_empty() && !self.truncated && self.unanchored.is_empty()
    }
    /// Whether no damaged fragments were reported.
    pub fn is_undamaged(&self) -> bool {
        self.damaged.is_empty()
    }
}

/// A cursor's head in the merge heap.
///
/// A [`BinaryHeap`] is a max-heap, so the "best" has to come out greatest, and
/// the comparison is inverted for oldest-to-newest order. On equal times the
/// cursor with the smaller number wins: identical moments are ordinary (the
/// clock is monotonic but not strictly increasing), and the order between them
/// has to be stable, or one and the same query would hand records back in a
/// different order each time.
#[derive(Debug, PartialEq, Eq)]
struct Head {
    at: BootTime,
    idx: usize,
    newest_first: bool,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        let by_time = if self.newest_first {
            self.at.cmp(&other.at)
        } else {
            other.at.cmp(&self.at)
        };
        by_time.then_with(|| other.idx.cmp(&self.idx))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// The record stream of a query: channels merged by time without assembling
/// the whole answer.
///
/// The order is the same as [`Reader::query`]'s: the cursors are merged by the
/// full moment (run, µs) rather than by microseconds, which are meaningless to
/// compare across runs.
///
/// The merge uses a heap rather than scanning the cursors on every record: a
/// cursor is created per (namespace, channel) pair, and with the twenty-four
/// thousand namespaces claimed a linear search for the head would mean tens of
/// thousands of comparisons for **every** line handed out.
#[derive(Debug)]
pub struct EntryStream<'a> {
    reader: &'a Reader,
    /// A copy of the query: parsing a record checks it against the filter each
    /// time.
    query: Query,
    /// A snapshot of the epochs for the whole stream: the window's bounds and
    /// the records' UTC are computed from one set of anchors, even if a live
    /// store's change underfoot.
    epochs: Epochs,
    bounds: Bounds,
    cursors: Vec<ChannelCursor>,
    /// A schema per cursor, in the same order.
    schemas: Vec<Option<Schema>>,
    /// The heads of the non-empty cursors.
    heads: BinaryHeap<Head>,
    /// The directories that could not be read — known at once when opening.
    damaged: Vec<Damage>,
    limit: usize,
    yielded: usize,
    truncated: bool,
    /// The walk is over: a repeat call must not hand out records after the
    /// stream has already answered `None`.
    done: bool,
}

impl EntryStream<'_> {
    /// Return a cursor's head to the heap, if it has one.
    fn requeue(&mut self, idx: usize) {
        let newest_first = self.query.order == Order::Newest;
        if let Some(head) = self.cursors[idx].peek() {
            let at = head.at;
            self.heads.push(Head {
                at,
                idx,
                newest_first,
            });
        }
    }
}

impl Iterator for EntryStream<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.done {
            return None;
        }
        loop {
            let Some(Head { idx, .. }) = self.heads.pop() else {
                self.done = true;
                return None;
            };
            let taken = self.cursors[idx].next_entry();
            self.requeue(idx);
            let Some(raw) = taken else { continue };

            // Records outside the window are skipped but the walk goes on: the
            // segment may have begun before the lower bound.
            if !self.bounds.contains(raw.at) {
                continue;
            }

            let ns = std::sync::Arc::clone(&self.cursors[idx].namespace);
            let ch = self.cursors[idx].channel;
            if let Some(entry) = self.reader.build_entry(
                ns,
                ch,
                self.schemas[idx].as_ref(),
                raw,
                &self.query,
                &self.epochs,
            ) {
                // Truncation is declared only when there really **is** another
                // record and there is no room left in the answer. Setting the
                // mark on entry would mean declaring every answer of exactly
                // `limit` records truncated even when there were no more — and
                // for the web layer that is a "next" button that never goes
                // away.
                if self.yielded >= self.limit {
                    self.truncated = true;
                    self.done = true;
                    return None;
                }
                self.yielded += 1;
                return Some(entry);
            }
        }
    }
}

impl EntryStream<'_> {
    /// The fragments that could not be read.
    ///
    /// The list reflects everything found **so far**, damage in half-read
    /// segments included: the walk breaks off on `limit`, and an answer that
    /// data dropped out of because of corruption has no right to look complete.
    /// Further along the stream the list can only grow — damage is discovered
    /// when a block is read.
    pub fn damaged(&self) -> Vec<Damage> {
        let mut out = self.damaged.clone();
        for c in &self.cursors {
            out.extend(c.damaged());
        }
        out
    }

    /// The runs whose segments dropped out of the selection for want of an
    /// anchor — see [`QueryResult::unanchored`].
    pub fn unanchored(&self) -> Vec<BootCounter> {
        let mut out: Vec<BootCounter> = Vec::new();
        for c in &self.cursors {
            for boot in c.unanchored() {
                if !out.contains(boot) {
                    out.push(*boot);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Whether the walk was cut short by the query's `limit`.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// How many records have been handed out.
    pub fn yielded(&self) -> usize {
        self.yielded
    }
}

/// No single wait of a subscription lasts longer than this.
///
/// There is nothing to poll for less often than once an hour and no reason to,
/// while a `Duration::MAX` deadline is a panic on the addition, that is, the
/// worst possible answer to a request to wait a while longer. The same limit
/// as the mark's own: two numbers with one justification would drift apart.
const MAX_WAIT: Duration = dduroc_engine::pulse::LONGEST_WAIT;

/// No more often than this does a subscription walk the roots for new
/// namespaces.
///
/// It is the delay before a service appears on a live screen; chosen so that a
/// human does not notice it but a device with thousands of namespaces does.
const ADOPT_EVERY: Duration = Duration::from_millis(500);

/// What a subscription handed out this time.
///
/// The enum is deliberately **closed**: a new subscription outcome is a new
/// decision for the caller, not something to be passed over silently. Marking
/// it `#[non_exhaustive]` would force every polling loop to carry a `_ => {}` —
/// that is, to swallow in advance whatever appears later.
#[derive(Debug)]
pub enum Tail {
    /// The next record.
    Entry(Box<Entry>),
    /// Nothing new appeared in the time allotted. Asking again is possible and
    /// right: this is silence, not an end.
    Idle,
    /// There is nobody left to write: the store has stopped, and everything
    /// that reached the medium has been handed out. From here on it is only
    /// `Ended`.
    Ended,
}

/// A controlled follow item with explicit state-seed provenance.
#[derive(Debug)]
pub enum FollowItem {
    Entry { entry: Box<Entry>, seed: bool },
    Idle,
    Ended,
}

/// A live subscription with cumulative cooperative limits.
#[derive(Debug)]
pub struct ControlledFollow<'a> {
    inner: Follow<'a>,
    budget: Arc<crate::budget::Budget>,
    charged_seeds: usize,
    seed_snapshots: Vec<dduroc_engine::backend::ChannelSnapshot>,
}
impl ControlledFollow<'_> {
    /// Equivalent to `next`, preserving whether an entry is an initial seed.
    pub fn next_item(&mut self, wait: Duration) -> Result<FollowItem> {
        let seed = self.charged_seeds > 0;
        Ok(match self.next(wait)? {
            Tail::Entry(entry) => FollowItem::Entry { entry, seed },
            Tail::Idle => FollowItem::Idle,
            Tail::Ended => FollowItem::Ended,
        })
    }
    fn check(&self) -> Result<()> {
        if !self.budget.checkpoint() {
            return Err(session_error(self.budget.failure().unwrap()));
        }
        if self.charged_seeds > 0 {
            let backend = self
                .inner
                .reader
                .backend
                .as_ref()
                .expect("controlled follow has backend");
            for snapshot in &self.seed_snapshots {
                if !self.budget.checkpoint() {
                    return Err(session_error(self.budget.failure().unwrap()));
                }
                let status = backend
                    .snapshot_status(snapshot, snapshot.info.first_index)
                    .map_err(|error| {
                        self.budget
                            .stop(crate::budget::Stop::Backend(error.to_string()));
                        session_error(self.budget.failure().unwrap())
                    })?;
                match status {
                    dduroc_engine::backend::SnapshotStatus::Current => {}
                    dduroc_engine::backend::SnapshotStatus::Replaced => {
                        self.budget.stop(crate::budget::Stop::Expired)
                    }
                    dduroc_engine::backend::SnapshotStatus::Evicted => {
                        self.budget.stop(crate::budget::Stop::RetentionGap)
                    }
                }
                if let Some(stop) = self.budget.failure() {
                    return Err(session_error(stop));
                }
            }
        }
        for cursor in &self.inner.cursors {
            if !self.budget.checkpoint() {
                return Err(session_error(self.budget.failure().unwrap()));
            }
            if let Err(error) = cursor.validate_follow() {
                let stop = match error {
                    ReadError::SnapshotExpired => crate::budget::Stop::Expired,
                    ReadError::RetentionGap => crate::budget::Stop::RetentionGap,
                    other => crate::budget::Stop::Backend(other.to_string()),
                };
                self.budget.stop(stop);
                return Err(session_error(self.budget.failure().unwrap()));
            }
        }
        Ok(())
    }
    /// Cancellation and resource failures are terminal. The wait is checked
    /// cooperatively at least every 25 ms, apart from a blocking I/O operation.
    pub fn next(&mut self, wait: Duration) -> Result<Tail> {
        self.check()?;
        let tail = self.inner.next(wait);
        if let Some(stop) = self.budget.failure() {
            return Err(session_error(stop));
        }
        self.check()?;
        if let Tail::Entry(entry) = &tail {
            if self.charged_seeds > 0 {
                self.charged_seeds -= 1;
            } else if !self.budget.returned(entry_size(entry) as u64) {
                return Err(session_error(self.budget.failure().unwrap()));
            }
        }
        Ok(tail)
    }
    pub fn take_damage(&mut self) -> Vec<Damage> {
        self.inner.take_damage()
    }
    /// Runs excluded from a UTC window because no clock anchor is available.
    pub fn unanchored(&self) -> Result<Vec<BootCounter>> {
        let mut out = std::collections::BTreeSet::new();
        for cursor in &self.inner.cursors {
            for boot in cursor.unanchored() {
                if !self.budget.checkpoint() {
                    return Err(session_error(self.budget.failure().unwrap()));
                }
                out.insert(*boot);
            }
        }
        Ok(out.into_iter().collect())
    }
}

/// A subscription to the record stream of a live store.
///
/// It reads the same things by the same means as [`Reader::query`] — but
/// instead of ending at the end of the data it waits for more. There are three
/// differences from a query, and all of them follow from writing still going
/// on:
///
/// - **oldest-to-newest order only** ([`Order::Oldest`]): "the last hundred" of
///   a stream that has no last record means nothing;
/// - **there is no upper window bound**: a subscription reads what is not there
///   yet;
/// - **what is unfinished is deferred** rather than skipped: both a block's
///   tail and a segment caught at birth. A one-off query has a next query, a
///   subscription does not, and passing something by means losing it.
///
/// The order between channels is by time within what is visible at the moment
/// of waking, and that is all that can honestly be promised: channels sync by
/// their own policies (the critical one at once, the ordinary one once a
/// second), so an ordinary channel's record may become visible later than a
/// critical one that happened after it. Within one channel the order is exact —
/// it is the order on disk.
///
/// A subscription keeps the store alive, as any reader does: while it lives,
/// the writer's stop will not complete either.
#[derive(Debug)]
pub struct Follow<'a> {
    reader: &'a Reader,
    budget: Option<Arc<crate::budget::Budget>>,
    query: Query,
    /// The window's bounds, computed once at open time.
    ///
    /// They must not be recomputed on every wake-up: a wall-clock bound is
    /// converted into a run's scale by an anchor, and time synchronization is
    /// retroactive — the window would move under a subscription that has
    /// already walked part of the stream and cannot go back.
    bounds: Bounds,
    /// The time anchors, by contrast, are fresh on every wake-up: a record's
    /// UTC comes from a synchronization that happened after the record itself.
    epochs: Epochs,
    cursors: Vec<ChannelCursor>,
    schemas: Vec<Option<Schema>>,
    heads: BinaryHeap<Head>,
    /// Whose head is in the heap right now. A cursor that ran dry before new
    /// data arrived falls out of the heap, and there is nothing left to put it
    /// back.
    queued: Vec<bool>,
    /// What is already open: namespace → a mask of classes by
    /// [`StorageClass::index`].
    ///
    /// A subscription to a group has to pick up a service that started after it
    /// — but there is no reason for it to reopen what is already open. A mask
    /// rather than a set of pairs: asking "is this channel familiar" costs one
    /// hash of the name, without building a key, and the name is stored once
    /// per namespace rather than once per channel.
    known: HashMap<String, u8>,
    /// Which of what the root walk saw has already been reported.
    ///
    /// The walk repeats when directories appear in the store, and an unreadable
    /// directory does not go anywhere — without this memory a subscription
    /// would announce one and the same damage on every walk. The set is bounded
    /// by the number of broken paths, not by the number of walks.
    reported: HashSet<(PathBuf, String)>,
    damaged: Vec<Damage>,
    pulse: Arc<dduroc_engine::pulse::Pulse>,
    seen: dduroc_engine::pulse::Beat,
    seeds: std::vec::IntoIter<Entry>,
    /// When the roots were last walked for new namespaces. `None` means never
    /// yet.
    last_adopt: Option<Instant>,
    /// The last walk after the store stopped has already been done.
    swept: bool,
    /// The store's roster changed while a walk was deferred by rate: that debt
    /// has no right to disappear — a namespace announced once would otherwise
    /// never be picked up.
    adopt_due: bool,
    limit: usize,
    yielded: usize,
    ended: bool,
}

impl Follow<'_> {
    /// Wait for the next record, but no longer than `wait`.
    ///
    /// [`Tail::Idle`] means "quiet for now", not "the end": a subscription is
    /// polled in a loop, and the timeout sets how quickly that loop can notice
    /// a stop the application itself ordered.
    pub fn next(&mut self, wait: Duration) -> Tail {
        if self
            .budget
            .as_ref()
            .is_some_and(|budget| !budget.checkpoint())
        {
            return Tail::Ended;
        }
        if let Some(seed) = self.seeds.next() {
            return Tail::Entry(Box::new(seed));
        }
        if self.ended || self.limit == 0 {
            // Zero records means zero records, not "one will do": a check after
            // handing out would give the first one away and only then stop.
            self.ended = true;
            return Tail::Ended;
        }
        // The deadline is computed once per call: waking on someone else's data
        // (the mark is one for the whole store) has no right to extend the wait
        // beyond what was promised. The deadline saturates: a subscription that
        // asked to wait longer than the clock can count is answered with an
        // hour — a panic instead of a wait would be the worst reading of such a
        // request.
        let deadline = Instant::now()
            .checked_add(wait.min(MAX_WAIT))
            .unwrap_or_else(Instant::now);
        loop {
            if self
                .budget
                .as_ref()
                .is_some_and(|budget| !budget.checkpoint())
            {
                return Tail::Ended;
            }
            if let Some(entry) = self.pop_ready() {
                return Tail::Entry(Box::new(entry));
            }
            if self.ended {
                return Tail::Ended;
            }
            if self.seen.closed {
                if !self.swept {
                    // One last walk. The mark is read as three fields, and the
                    // stop may have happened between them: then "closed"
                    // arrives together with a shape of the store not yet seen,
                    // and the last segment would stay unlisted. After the stop
                    // the medium is final — one walk is enough.
                    self.swept = true;
                    self.adopt_new_channels();
                    self.rearm(true);
                    continue;
                }
                // The store has stopped, and everything that reached the medium
                // has been handed out.
                self.ended = true;
                return Tail::Ended;
            }
            if self.adopt_if_due() {
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Tail::Idle;
            }
            // The wait is shortened to the deferred walk's deadline: otherwise
            // a subscription with a long timeout would learn of a new service
            // only after somebody wrote something.
            let mut rest = deadline - now;
            if self.budget.is_some() {
                rest = rest.min(Duration::from_millis(25));
            }
            if let Some(at) = self.adopt_deadline() {
                rest = rest.min(at);
            }
            let beat = self.pulse.wait(self.seen, rest);
            if beat == self.seen {
                // A writer thread killed by a panic never gets to set the close
                // mark: without this check a subscription would wait forever
                // for someone there is nobody left to write for.
                if !self.reader.writer_alive() {
                    self.ended = true;
                    return Tail::Ended;
                }
                // We woke before our own deadline — so it was for the deferred
                // walk: it is too early to declare silence.
                if Instant::now() < deadline {
                    continue;
                }
                return Tail::Idle;
            }
            let shape_changed = beat.shape != self.seen.shape;
            let roster_changed = beat.roster != self.seen.roster;
            self.seen = beat;
            self.refresh(shape_changed, roster_changed);
        }
    }

    /// Take the damage found since last time.
    ///
    /// Take, specifically: a subscription lives a long time, and a list that
    /// only grows would repeat one and the same damage in every batch — and
    /// grow without bound besides. An empty answer means everything read
    /// cleanly since last time.
    pub fn take_damage(&mut self) -> Vec<Damage> {
        let mut out = std::mem::take(&mut self.damaged);
        for c in &mut self.cursors {
            out.append(&mut c.take_damage());
        }
        out
    }

    /// The runs that dropped out of a wall-clock window for want of an anchor —
    /// see [`QueryResult::unanchored`].
    ///
    /// Computed from the cursors each time rather than remembered at open time:
    /// a subscription lists the directories anew, and a run whose segments
    /// turned up later has to reach the answer — otherwise nobody would mention
    /// it.
    pub fn unanchored(&self) -> Vec<BootCounter> {
        let mut out: Vec<BootCounter> = Vec::new();
        for c in &self.cursors {
            for boot in c.unanchored() {
                if !out.contains(boot) {
                    out.push(*boot);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// How many records have been handed out.
    pub fn yielded(&self) -> usize {
        self.yielded
    }

    /// Take a record from what has already been read off the medium.
    fn pop_ready(&mut self) -> Option<Entry> {
        loop {
            let Head { idx, .. } = self.heads.pop()?;
            self.queued[idx] = false;
            let taken = self.cursors[idx].next_entry();
            self.requeue(idx);
            let Some(raw) = taken else { continue };

            if !self.bounds.contains(raw.at) {
                continue;
            }
            let ns = std::sync::Arc::clone(&self.cursors[idx].namespace);
            let ch = self.cursors[idx].channel;
            let Some(entry) = self.reader.build_entry(
                ns,
                ch,
                self.schemas[idx].as_ref(),
                raw,
                &self.query,
                &self.epochs,
            ) else {
                continue;
            };
            self.yielded += 1;
            if self.yielded >= self.limit {
                self.ended = true;
            }
            return Some(entry);
        }
    }

    fn requeue(&mut self, idx: usize) {
        if let Some(head) = self.cursors[idx].peek() {
            let at = head.at;
            self.heads.push(Head {
                at,
                idx,
                newest_first: false,
            });
            self.queued[idx] = true;
        }
    }

    /// Take everything that has appeared since the last wake-up.
    fn refresh(&mut self, shape_changed: bool, roster_changed: bool) {
        if shape_changed || roster_changed {
            self.prune_backend_channels();
        }
        if let Some(budget) = &self.budget {
            if !budget.checkpoint() {
                return;
            }
            if let Source::Store(store) = &self.reader.source {
                match store.epochs_limited(budget.limits.epoch_runs, budget.limits.hardware_boots) {
                    Ok(epochs) => self.epochs = epochs,
                    Err(dduroc_engine::Error::EpochLimit {
                        resource: "epoch_runs",
                        ..
                    }) => {
                        budget.stop(crate::budget::Stop::Limit(crate::ReadResource::EpochRuns));
                        return;
                    }
                    Err(dduroc_engine::Error::EpochLimit {
                        resource: "hardware_boots",
                        ..
                    }) => {
                        budget.stop(crate::budget::Stop::Limit(
                            crate::ReadResource::HardwareBoots,
                        ));
                        return;
                    }
                    Err(error) => {
                        budget.stop(crate::budget::Stop::Backend(error.to_string()));
                        return;
                    }
                }
            }
        } else {
            self.epochs = self.reader.epochs_now().into_owned();
        }
        // A root walk is triggered only by the store's roster — that is, by a
        // namespace coming up. While rotation triggered it too, a subscription
        // listed the whole store every half second on end: segments change
        // constantly, while services start once in their life.
        if roster_changed || self.reader.backend.is_some() {
            // Fully drained compacted channels are pruned. A later append to
            // an existing channel changes data, not necessarily the roster.
            self.adopt_due = true;
        }
        self.rearm(shape_changed);
    }

    /// Read the cursors on and return to the heap those that fell out of it.
    ///
    /// A cursor that ran dry before new data arrived falls out, and there is
    /// nothing left to put it back — including a cursor just opened for a new
    /// namespace: without this it would lie there with its data outside the
    /// merge.
    ///
    /// It is tempting to skip the cursors whose head is already in the heap:
    /// the new data will queue behind it, the order of hand-outs will not
    /// change, and reading on costs a trip to the medium. It would gain
    /// nothing: this is reached **only** after [`Follow::pop_ready`] returned
    /// `None`, which means the heap is empty and holds nobody's head. There is
    /// nothing to skip, and the check would be a branch that never runs.
    fn rearm(&mut self, relist: bool) {
        for idx in 0..self.cursors.len() {
            // The directory is walked only when the store has announced that
            // the segments changed: while the writer pours into the same file,
            // a subscription gets by with one read of the tail.
            self.cursors[idx].extend(relist);
            if !self.queued[idx] {
                self.requeue(idx);
            }
        }
    }

    /// Walk the roots if it is time. `true` means we walked and it is worth
    /// looking again.
    ///
    /// The walk is the most expensive thing a subscription does (listing the
    /// root and reading the metadata of every matching directory), and it is
    /// triggered by a change in the store's shape, that is, by every rotation.
    /// So it is rate limited but not cancelled: the debt is remembered and paid
    /// when the time comes.
    fn adopt_if_due(&mut self) -> bool {
        if !self.adopt_due || self.adopt_deadline().is_some() {
            return false;
        }
        self.adopt_due = false;
        self.last_adopt = Some(Instant::now());
        self.prune_backend_channels();
        self.adopt_new_channels();
        self.rearm(true);
        true
    }

    /// How much longer to wait for the deferred walk. `None` means there is
    /// nothing to wait for.
    fn adopt_deadline(&self) -> Option<Duration> {
        if !self.adopt_due {
            return None;
        }
        let at = self.last_adopt?;
        ADOPT_EVERY
            .checked_sub(at.elapsed())
            .filter(|d| !d.is_zero())
    }

    /// Announce damage from the walk, if it has not been mentioned yet.
    fn note_once(&mut self, damage: Damage) {
        if self.reported.len() >= 512 {
            self.reported.clear();
        }
        if self
            .reported
            .insert((damage.path.clone(), damage.reason.clone()))
        {
            self.damaged.push(damage);
        }
    }

    /// Open the channels of namespaces that came up after the subscription
    /// began.
    ///
    /// Familiar channels are filtered out **before** a cursor is opened rather
    /// than after: otherwise every walk would list the directory anew and open
    /// a segment for each channel already being read, only to throw the result
    /// away.
    /// Retain a delivery watermark only while its channel has retained data.
    /// Empty channels can later start at their new monotonic index; reopening
    /// them cannot replay history that the backend has already purged.
    fn prune_backend_channels(&mut self) {
        // Controlled subscriptions retain every observed watermark. Dropping an
        // empty channel would hide writes that are completely evicted between
        // polls; the cumulative channel budget bounds this state instead.
        if self.budget.is_some() {
            return;
        }
        let Some(backend) = &self.reader.backend else {
            return;
        };
        let catalog_result = if let Some(budget) = &self.budget {
            backend
                .snapshot_channels(|_| {
                    if !budget.roster() {
                        return Err(dduroc_engine::Error::Backend("session stopped".into()));
                    }
                    Ok(true)
                })
                .map(|items| items.into_iter().map(|item| item.info).collect())
        } else {
            backend.catalog()
        };
        let catalog = match catalog_result {
            Ok(catalog) => catalog,
            Err(error) => {
                self.note_once(Damage {
                    path: self.reader.root().to_owned(),
                    offset: 0,
                    reason: error.to_string(),
                });
                return;
            }
        };
        let retained: HashSet<_> = catalog
            .iter()
            .filter(|info| info.retained_bytes > 0)
            .map(|info| (info.key.namespace.as_str(), info.key.class))
            .collect();
        let mut cursors = Vec::new();
        let mut schemas = Vec::new();
        for (cursor, schema) in self.cursors.drain(..).zip(self.schemas.drain(..)) {
            if cursor.drained() && !retained.contains(&(cursor.namespace.as_ref(), cursor.channel))
            {
                continue;
            }
            cursors.push(cursor);
            schemas.push(schema);
        }
        self.cursors = cursors;
        self.schemas = schemas;
        self.known.clear();
        self.heads.clear();
        self.queued = vec![false; self.cursors.len()];
        for idx in 0..self.cursors.len() {
            let cursor = &self.cursors[idx];
            *self.known.entry(cursor.namespace.to_string()).or_default() |=
                1 << cursor.channel.index();
            self.requeue(idx);
        }
        if let Ok(mut cache) = self.reader.schema_cache.write() {
            cache.retain(|namespace, _| {
                catalog
                    .iter()
                    .any(|info| info.key.namespace == *namespace && info.retained_bytes > 0)
            });
        }
    }

    fn adopt_new_channels(&mut self) {
        let opened = if let Some(budget) = &self.budget {
            self.reader
                .open_follow_cursors(&self.query, &self.bounds, &self.known, budget)
                .map(|(opened, _)| opened)
        } else {
            self.reader.open_cursors_beyond(
                &self.query,
                &self.bounds,
                |scope| {
                    scope.liveness = crate::cursor::Liveness::Following;
                },
                &self.known,
            )
        };
        let OpenedCursors {
            cursors,
            schemas,
            damaged,
        } = match opened {
            Ok(o) => o,
            // The directory did not list — almost always because it was being
            // changed right then. That must not bring a subscription down, and
            // silence is not allowed either: a namespace that never got picked
            // up would look like a service that never came up.
            Err(e) => {
                self.note_once(Damage {
                    path: self.reader.root().to_owned(),
                    offset: 0,
                    reason: format!("the namespaces did not list: {e}"),
                });
                return;
            }
        };
        for d in damaged {
            self.note_once(d);
        }
        for (cursor, schema) in cursors.into_iter().zip(schemas) {
            let bit = 1u8 << cursor.channel.index();
            let mask = self.known.entry(cursor.namespace.to_string()).or_default();
            if *mask & bit != 0 {
                continue;
            }
            *mask |= bit;
            self.cursors.push(cursor);
            self.schemas.push(schema);
            self.queued.push(false);
        }
    }
}

/// A namespace found by a scan of the roots: the channels together with their
/// directories.
///
/// Every channel has a directory of its own: a class may have been moved to a
/// separate medium, and the path cannot be reconstructed from one root.
#[derive(Debug)]
struct NsScan {
    name: String,
    schema_name: String,
    protocol_version: u16,
    channels: Vec<(StorageClass, PathBuf)>,
}

/// The cursors opened for a query together with the schemas resolved.
#[derive(Debug)]
struct OpenedCursors {
    cursors: Vec<ChannelCursor>,
    /// A schema per cursor, in the same order.
    schemas: Vec<Option<Schema>>,
    /// The directories that could not be read.
    damaged: Vec<Damage>,
}

/// Information about a namespace of the store.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct NamespaceInfo {
    pub name: String,
    pub schema_name: String,
    pub protocol_version: u16,
    /// The storage classes the namespace has channels for, in
    /// [`StorageClass::index`] order.
    pub channels: Vec<StorageClass>,
    /// Retained encoded block bytes for raft stores; segment file bytes for legacy dumps.
    /// Shared physical raft log overhead is not attributable to one namespace.
    pub total_bytes: u64,
}

/// What was found in the root when the namespaces were listed.
///
/// A type of its own rather than just a list: a listing that an unreadable
/// namespace silently dropped out of looks like "there is no such service on
/// the device" — the same silence [`QueryResult::damaged`] exists against. An
/// empty `damaged` means everything was listed.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct NamespaceListing {
    pub namespaces: Vec<NamespaceInfo>,
    /// The directories recognized as namespaces but not readable.
    pub damaged: Vec<Damage>,
}

impl NamespaceListing {
    /// Whether everything in the root was listed.
    pub fn is_complete(&self) -> bool {
        self.damaged.is_empty()
    }
}

/// Where the reader takes its truth about the store from.
#[derive(Debug)]
enum Source {
    /// The live store of this same process: the roots, the schemas and the
    /// epochs are asked of it at the moment of every query — there is nothing
    /// for them to go stale from. The store is kept alive, as a namespace
    /// handle keeps it.
    Store(Arc<Store>),
    /// A dump: every root is named at open time, everything is read from disk
    /// and frozen. For a dump that is not a flaw but a definition — nobody is
    /// appending to it.
    Dump { roots: Vec<PathBuf>, epochs: Epochs },
}

/// The store's reader.
///
/// It lives in two modes, and what separates them is the source of truth:
///
/// - **live** ([`Reader::of_store`]) reads a store this same process is writing
///   right now. Working in parallel with writing is its definition: every query
///   sees fresh roots, schemas and time anchors, while rotation underfoot and a
///   segment's growing tail are ordinary events rather than damage;
/// - **a dump** ([`Reader::open_dump`]) is a snapshot nobody appends to: every
///   root is named at open time and checked for completeness, and any sign of
///   something unfinished is honestly reported as damage.
#[derive(Debug)]
pub struct Reader {
    source: Source,
    backend: Option<Arc<ReadBackend>>,
    /// The schemas named by hand (at open time and by [`Reader::with_schema`]).
    /// A live reader sees the schemas of namespaces that came up on top of
    /// them.
    schemas: HashMap<String, Schema>,
    /// The store's identity; `None` means do not check (reading a foreign dump
    /// is allowed explicitly).
    store_id: Option<u64>,
    /// The schema resolved by namespace name. Resolution reads `ns-meta` from
    /// disk, and it is asked for on **every** record displayed — a file
    /// operation per record would be exactly the defect already removed from
    /// the query path. A `None` value means "the namespace exists, this build
    /// has no schema for it"; that answer is worth remembering too.
    schema_cache: RwLock<HashMap<String, Option<Schema>>>,
    language_preferences: crate::LanguagePreferences,
}

/// Rendered text and the actual schema language, if this is a schema message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RenderedMessage {
    /// Rendered text or the unchanged free text.
    pub text: String,
    /// Actual schema language; `None` for free text.
    pub language: Option<String>,
    /// Matching preference index; `None` for fallback or free text.
    pub preference_index: Option<usize>,
}

impl Reader {
    /// Sample global retained statistics without scanning block payloads.
    /// Allocated root usage is sampled separately when requested.
    pub fn statistics_controlled(
        &self,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
        include_roots: bool,
    ) -> Result<Statistics> {
        let backend = self
            .backend
            .as_ref()
            .ok_or(ReadError::UnsupportedSession("legacy statistics"))?;
        let budget = crate::budget::Budget::new(limits, cancellation);
        if !budget.checkpoint() {
            return Err(session_error(budget.failure().unwrap()));
        }
        let cached = backend.cached_statistics()?;
        let bytes = cached
            .logical_class_bytes
            .iter()
            .try_fold(0u64, |sum, value| sum.checked_add(*value))
            .ok_or_else(|| {
                ReadError::from(dduroc_engine::Error::Backend("statistics overflow".into()))
            })?;
        let roots = if include_roots {
            Some(
                backend
                    .physical_stats(4096, || budget.checkpoint())
                    .map_err(|e| {
                        budget
                            .failure()
                            .map(session_error)
                            .unwrap_or_else(|| e.into())
                    })?,
            )
        } else {
            None
        };
        let runtime = match &self.source {
            Source::Store(store) => Some(store.stats()),
            Source::Dump { .. } => None,
        };
        let limits = runtime.as_ref().map(|_| cached.class_limits);
        let classes = StorageClass::ALL
            .into_iter()
            .map(|class| ClassStatistics {
                class,
                logical_retained_bytes: cached.logical_class_bytes[class.index()],
                limit_bytes: limits.map(|limits| limits[class.index()]),
            })
            .collect();
        Ok(Statistics {
            captured_at: chrono::Utc::now(),
            counting_state: cached.counting_state,
            retained_records: cached.retained_records,
            retained_blocks: cached.retained_blocks,
            logical_retained_bytes: bytes,
            channels: cached.channels,
            classes,
            runtime,
            roots,
        })
    }

    /// Perform one explicitly requested, bounded count-index maintenance step.
    /// Returns whether more work remained immediately after the step.
    pub fn backfill_statistics_step(
        &self,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
    ) -> Result<bool> {
        if !matches!(self.source, Source::Store(_)) {
            return Err(ReadError::UnsupportedSession("offline count backfill"));
        }
        let backend = self
            .backend
            .as_ref()
            .ok_or(ReadError::UnsupportedSession("legacy count backfill"))?;
        let budget = crate::budget::Budget::new(limits, cancellation);
        if !budget.checkpoint() {
            return Err(session_error(budget.failure().unwrap()));
        }
        let Some(channel) = backend.pending_count_channel()? else {
            return Ok(false);
        };
        let max_blocks = budget.limits.blocks.min(128);
        let max_bytes = usize::try_from(budget.limits.compressed_bytes.min(8 * 1024 * 1024))
            .unwrap_or(8 * 1024 * 1024);
        if max_blocks == 0 {
            return Err(ReadError::LimitExceeded(crate::ReadResource::Blocks));
        }
        if max_bytes == 0 {
            return Err(ReadError::LimitExceeded(
                crate::ReadResource::CompressedBytes,
            ));
        }
        backend
            .backfill_count_step(&channel, max_blocks, max_bytes, || budget.checkpoint())
            .map_err(|e| {
                budget
                    .failure()
                    .map(session_error)
                    .unwrap_or_else(|| e.into())
            })?;
        Ok(backend.pending_count_channel()?.is_some())
    }

    /// List selected channels using the same cooperative roster and output
    /// budgets as sessions. Legacy segment discovery is not supported here.
    pub fn catalog_controlled(
        &self,
        query: &Query,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
    ) -> Result<Vec<CatalogChannel>> {
        let backend = self
            .backend
            .as_ref()
            .ok_or(ReadError::UnsupportedSession("legacy catalog"))?;
        let budget = crate::budget::Budget::new(limits, cancellation);
        let snapshots = backend
            .snapshot_channels(|key| {
                if !budget.roster() {
                    return Err(dduroc_engine::Error::Backend("catalog stopped".into()));
                }
                if !query.namespaces.matches(&key.namespace)
                    || (!query.channels.is_empty() && !query.channels.contains(&key.class))
                {
                    return Ok(false);
                }
                if !budget.channel() {
                    return Err(dduroc_engine::Error::Backend("catalog stopped".into()));
                }
                Ok(true)
            })
            .map_err(|error| {
                budget
                    .failure()
                    .map(session_error)
                    .unwrap_or_else(|| error.into())
            })?;
        let mut rows = Vec::with_capacity(snapshots.len());
        for snapshot in snapshots {
            if !budget.checkpoint() {
                return Err(session_error(budget.failure().unwrap()));
            }
            let info = snapshot.info;
            let size = std::mem::size_of::<CatalogChannel>()
                .saturating_add(info.key.namespace.len())
                .saturating_add(info.schema_name.len());
            if !budget.returned(size as u64) {
                return Err(session_error(budget.failure().unwrap()));
            }
            let schema = self.schema_by_name(&info.schema_name);
            rows.push(CatalogChannel {
                namespace: info.key.namespace,
                class: info.key.class,
                schema_name: info.schema_name,
                protocol_version: info.protocol_version,
                retained_bytes: info.retained_bytes,
                quota_bytes: info.quota_bytes,
                schema,
            });
        }
        Ok(rows)
    }
    fn open_follow_cursors(
        &self,
        query: &Query,
        bounds: &Bounds,
        known: &HashMap<String, u8>,
        budget: &Arc<crate::budget::Budget>,
    ) -> Result<(OpenedCursors, Vec<dduroc_engine::backend::ChannelSnapshot>)> {
        let backend = self
            .backend
            .as_ref()
            .ok_or(ReadError::UnsupportedSession("legacy follow"))?;
        let snapshots = backend
            .snapshot_channels(|key| {
                if !budget.roster() {
                    return Err(dduroc_engine::Error::Backend("session stopped".into()));
                }
                if !query.namespaces.matches(&key.namespace)
                    || (!query.channels.is_empty() && !query.channels.contains(&key.class))
                    || known.get(&key.namespace).copied().unwrap_or(0) & (1 << key.class.index())
                        != 0
                {
                    return Ok(false);
                }
                if !budget.channel() {
                    return Err(dduroc_engine::Error::Backend("session stopped".into()));
                }
                Ok(true)
            })
            .map_err(|error| {
                budget
                    .failure()
                    .map(session_error)
                    .unwrap_or_else(|| error.into())
            })?;
        let mut cursors = Vec::with_capacity(snapshots.len());
        let mut schemas = Vec::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            if !budget.checkpoint() {
                return Err(session_error(budget.failure().unwrap()));
            }
            let schema = self.schema_by_name(&snapshot.info.schema_name);
            let scope = crate::cursor::ChannelScope {
                budget: Some(Arc::clone(budget)),
                bounds: bounds.clone(),
                boot: query.boot,
                reverse: false,
                expect_store: self.store_id,
                prefilter: Some(build_prefilter(query, schema)),
                max_segments: None,
                require_metrics: None,
                migrations: schema.as_ref().map(crate::cursor::MigrationCtx::of),
                liveness: crate::cursor::Liveness::Following,
            };
            cursors.push(ChannelCursor::open_snapshot(
                Arc::clone(backend),
                snapshot.clone(),
                &scope,
            ));
            schemas.push(schema);
        }
        Ok((
            OpenedCursors {
                cursors,
                schemas,
                damaged: Vec::new(),
            },
            snapshots,
        ))
    }

    fn snapshot_seeds(
        &self,
        query: &Query,
        window: &Bounds,
        epochs: &Epochs,
        snapshots: &[dduroc_engine::backend::ChannelSnapshot],
        budget: &Arc<crate::budget::Budget>,
    ) -> Result<(Vec<Entry>, Vec<Damage>)> {
        let Some(from) = query.from.filter(|_| query.seed_states) else {
            return Ok((Vec::new(), Vec::new()));
        };
        let probe = Query {
            from: None,
            to: Some(from.just_before()),
            order: Order::Newest,
            limit: None,
            seed_states: false,
            filter: Filter {
                kinds: KindFilter::TELEMETRY,
                ..query.filter.clone()
            },
            ..query.clone()
        };
        let probe_bounds = probe
            .resolve_controlled(epochs, || budget.checkpoint())
            .ok_or_else(|| session_error(budget.failure().unwrap()))?
            .bounds;
        let backend = self.backend.as_ref().expect("snapshot has backend");
        let mut seeds = Vec::new();
        let mut damage = Vec::new();
        for snapshot in snapshots {
            if !budget.checkpoint() {
                return Err(session_error(budget.failure().unwrap()));
            }
            let schema = self.schema_by_name(&snapshot.info.schema_name);
            let wanted = state_metrics(schema.as_ref());
            if wanted.is_empty() {
                continue;
            }
            let scope = crate::cursor::ChannelScope {
                budget: Some(Arc::clone(budget)),
                bounds: probe_bounds.clone(),
                boot: probe.boot,
                reverse: true,
                expect_store: self.store_id,
                prefilter: Some(build_prefilter(&probe, schema)),
                max_segments: None,
                require_metrics: None,
                migrations: schema.as_ref().map(crate::cursor::MigrationCtx::of),
                liveness: crate::cursor::Liveness::Frozen,
            };
            let mut cursor =
                ChannelCursor::open_snapshot(Arc::clone(backend), snapshot.clone(), &scope);
            let mut seen = HashSet::new();
            while let Some(raw) = cursor.next_entry() {
                if window.contains(raw.at) || !probe_bounds.contains(raw.at) {
                    continue;
                }
                let OwnedRecord::Sample { metric, .. } = &raw.record else {
                    continue;
                };
                let metric = *metric;
                if !wanted.contains(&metric) || !seen.insert(metric) {
                    continue;
                }
                if let Some(entry) = self.build_entry(
                    Arc::clone(&cursor.namespace),
                    cursor.channel,
                    schema.as_ref(),
                    raw,
                    &probe,
                    epochs,
                ) {
                    if !budget.returned(entry_size(&entry) as u64) {
                        return Err(session_error(budget.failure().unwrap()));
                    }
                    seeds.push(entry);
                }
                if wanted.iter().all(|metric| seen.contains(metric)) {
                    break;
                }
            }
            if let Some(stop) = budget.failure() {
                return Err(session_error(stop));
            }
            damage.extend(cursor.damaged());
        }
        seeds.sort_by_key(|entry| entry.at);
        Ok((seeds, damage))
    }

    /// Open an owned, bounded snapshot session. Legacy segment dumps are not
    /// supported by this session API.
    pub fn open_session(
        self: &Arc<Self>,
        query: Query,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
    ) -> Result<QuerySession> {
        self.open_session_inner(query, limits, cancellation, false)
    }

    /// Open and count the same frozen snapshot using fixed seek dimensions.
    /// Counting consumes the session's cumulative work limits, but not its
    /// returned-byte limit. Incomplete or truncated snapshots are refused.
    /// The returned session supports indexed navigation with these dimensions.
    pub fn open_counted_session(
        self: &Arc<Self>,
        query: Query,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
        page_limits: crate::PageLimits,
    ) -> Result<(QuerySession, QueryTotals)> {
        let records = page_limits.max_records();
        let bytes = page_limits.max_bytes();
        let mut session = self.open_session_inner(query, limits, cancellation, true)?;
        let mut totals = QueryTotals {
            total_matches: 0,
            total_seeds: 0,
            total_pages: 0,
        };
        loop {
            let page = session.read_page(records, bytes)?;
            if page.truncated || !page.damaged.is_empty() || !page.unanchored.is_empty() {
                return Err(ReadError::IncompleteCount);
            }
            let empty = page.entries.is_empty() && page.seeds.is_empty();
            totals.total_matches = totals
                .total_matches
                .checked_add(page.entries.len() as u64)
                .ok_or(ReadError::IncompleteCount)?;
            totals.total_seeds = totals
                .total_seeds
                .checked_add(page.seeds.len() as u64)
                .ok_or(ReadError::IncompleteCount)?;
            if !empty || totals.total_pages == 0 {
                totals.total_pages = totals
                    .total_pages
                    .checked_add(1)
                    .ok_or(ReadError::IncompleteCount)?;
            }
            if page.exhausted {
                break;
            }
        }
        session.rewind()?;
        session.budget.suppress_returned(false);
        for seed in &session.seeds {
            if !session.budget.returned(entry_size(seed) as u64) {
                return Err(session_error(session.budget.failure().unwrap()));
            }
        }
        session.navigation = Navigation::Indexed {
            records,
            bytes,
            page_number: 0,
            page: None,
        };
        Ok((session, totals))
    }

    fn open_session_inner(
        self: &Arc<Self>,
        query: Query,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
        counting: bool,
    ) -> Result<QuerySession> {
        let backend = self
            .backend
            .as_ref()
            .ok_or(ReadError::UnsupportedSession("legacy segment store"))?;
        let budget = crate::budget::Budget::new(limits, cancellation);
        budget.suppress_returned(counting);
        if !budget.checkpoint() {
            return Err(session_error(budget.failure().unwrap()));
        }
        let (epochs, snapshots) = backend
            .snapshot_channels_with(
                || {
                    if !budget.checkpoint() {
                        return Err(dduroc_engine::Error::Backend("session stopped".into()));
                    }
                    match &self.source {
                        Source::Store(store) => store
                            .epochs_limited(budget.limits.epoch_runs, budget.limits.hardware_boots),
                        Source::Dump { epochs, .. } => {
                            if epochs.runs.len() > budget.limits.epoch_runs {
                                return Err(dduroc_engine::Error::EpochLimit {
                                    resource: "epoch_runs",
                                    limit: budget.limits.epoch_runs,
                                });
                            }
                            if epochs.hw_boots.len() > budget.limits.hardware_boots {
                                return Err(dduroc_engine::Error::EpochLimit {
                                    resource: "hardware_boots",
                                    limit: budget.limits.hardware_boots,
                                });
                            }
                            Ok(epochs.clone())
                        }
                    }
                },
                |key| {
                    if !budget.roster() {
                        return Err(dduroc_engine::Error::Backend("session stopped".into()));
                    }
                    if !query.namespaces.matches(&key.namespace)
                        || (!query.channels.is_empty() && !query.channels.contains(&key.class))
                    {
                        return Ok(false);
                    }
                    if !budget.channel() {
                        return Err(dduroc_engine::Error::Backend("session stopped".into()));
                    }
                    Ok(true)
                },
            )
            .map_err(|error| {
                budget
                    .failure()
                    .map(session_error)
                    .unwrap_or_else(|| match error {
                        dduroc_engine::Error::EpochLimit {
                            resource: "epoch_runs",
                            ..
                        } => ReadError::LimitExceeded(crate::ReadResource::EpochRuns),
                        dduroc_engine::Error::EpochLimit {
                            resource: "hardware_boots",
                            ..
                        } => ReadError::LimitExceeded(crate::ReadResource::HardwareBoots),
                        other => other.into(),
                    })
            })?;
        let bounds = query
            .resolve_controlled(&epochs, || budget.checkpoint())
            .ok_or_else(|| session_error(budget.failure().unwrap()))?
            .bounds;
        let (seeds, seed_damage) =
            self.snapshot_seeds(&query, &bounds, &epochs, &snapshots, &budget)?;
        let mut cursors = Vec::with_capacity(snapshots.len());
        let mut schemas = Vec::with_capacity(snapshots.len());
        for snapshot in &snapshots {
            if !budget.checkpoint() {
                return Err(session_error(budget.failure().unwrap()));
            }
            let schema = self.schema_by_name(&snapshot.info.schema_name);
            let scope = crate::cursor::ChannelScope {
                budget: Some(Arc::clone(&budget)),
                bounds: bounds.clone(),
                boot: query.boot,
                reverse: query.order == Order::Newest,
                expect_store: self.store_id,
                prefilter: Some(build_prefilter(&query, schema)),
                max_segments: None,
                require_metrics: None,
                migrations: schema.as_ref().map(crate::cursor::MigrationCtx::of),
                liveness: crate::cursor::Liveness::Frozen,
            };
            cursors.push(ChannelCursor::open_snapshot(
                Arc::clone(backend),
                snapshot.clone(),
                &scope,
            ));
            schemas.push(schema);
        }
        let mut heads = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors
            .iter_mut()
            .enumerate()
            .filter(|_| query.limit != Some(0))
        {
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first: query.order == Order::Newest,
                });
            }
            if let Some(stop) = budget.failure() {
                return Err(session_error(stop));
            }
        }
        let done = query.limit == Some(0);
        let zero_limit_truncated = done
            && snapshots
                .iter()
                .any(|snapshot| snapshot.info.first_index.is_some());
        let session = QuerySession {
            reader: Arc::clone(self),
            query,
            epochs,
            bounds,
            snapshots,
            cursors,
            schemas,
            heads,
            budget,
            pending: None,
            seeds: seeds.into(),
            seed_damage,
            yielded: 0,
            done,
            zero_limit_truncated,
            navigation: Navigation::Unset,
        };
        session.check()?;
        Ok(session)
    }

    /// Follow a live store with cumulative limits and cooperative cancellation.
    pub fn follow_controlled(
        self: &Arc<Self>,
        query: Query,
        limits: crate::ReadLimits,
        cancellation: crate::Cancellation,
    ) -> Result<ControlledFollow<'_>> {
        let Source::Store(store) = &self.source else {
            return Err(ReadError::NotFollowable("a dump has no live writer"));
        };
        if query.order == Order::Newest {
            return Err(ReadError::NotFollowable(
                "follow requires oldest-first order",
            ));
        }
        if query.to.is_some() {
            return Err(ReadError::NotFollowable(
                "follow cannot have an upper bound",
            ));
        }
        let pulse = Arc::clone(store.pulse());
        let seen = pulse.beat();
        let session = self.open_session(query, limits, cancellation)?;
        let QuerySession {
            query,
            epochs,
            bounds,
            snapshots,
            mut cursors,
            schemas,
            heads,
            budget,
            seeds,
            seed_damage,
            ..
        } = session;
        for cursor in &mut cursors {
            cursor.set_following();
        }
        let mut known: HashMap<String, u8> = HashMap::new();
        for cursor in &cursors {
            *known.entry(cursor.namespace.to_string()).or_default() |= 1 << cursor.channel.index();
        }
        let mut queued = vec![false; cursors.len()];
        for head in &heads {
            queued[head.idx] = true;
        }
        let charged_seeds = seeds.len();
        let inner = Follow {
            reader: self.as_ref(),
            budget: Some(Arc::clone(&budget)),
            limit: query.limit.unwrap_or(usize::MAX),
            query,
            bounds,
            epochs,
            cursors,
            schemas,
            heads,
            queued,
            known,
            reported: seed_damage
                .iter()
                .map(|d| (d.path.clone(), d.reason.clone()))
                .collect(),
            damaged: seed_damage,
            pulse,
            seen,
            seeds: seeds.into_iter().collect::<Vec<_>>().into_iter(),
            last_adopt: None,
            swept: false,
            adopt_due: false,
            yielded: 0,
            ended: false,
        };
        Ok(ControlledFollow {
            inner,
            budget,
            charged_seeds,
            seed_snapshots: snapshots,
        })
    }

    /// Open a dump of a store: **all** of its roots and schemas at once.
    ///
    /// The first root is the main one (`ns-meta`, the epochs and the store's
    /// identity live in it); the rest are the trees of classes moved to media
    /// of their own ([`ChannelConfig::custom_root`]). The roots are enumerated
    /// rather than added one at a time by an optional builder: a store with
    /// critical data on a protected partition is two trees, and a reader that
    /// was not told all of them would show the history without the critical
    /// part, giving nothing away. Completeness is checked at open time against
    /// the schemas: for every namespace of a known schema, every class it
    /// declares has to be found in one of the roots — otherwise
    /// [`ReadError::IncompleteDump`] rather than a silently short answer.
    ///
    /// Raft dumps require an exclusive engine lock. Opening validates the
    /// complete journal with AbsoluteConsistency: a torn tail or I/O error
    /// refuses the dump instead of truncating it. Engine bookkeeping may create
    /// or rotate its own active files; take a copy first when byte-for-byte
    /// preservation is required. A live process must use Reader::of_store,
    /// which shares the already open engine. Legacy segment dumps use the
    /// read-only segment path and perform no recovery.
    ///
    /// A store this same process writes needs [`Reader::of_store`]: a snapshot
    /// of the epochs and schemas taken here goes stale with the first time
    /// synchronization or the first new namespace.
    ///
    /// [`ChannelConfig::custom_root`]:
    /// dduroc_engine::channel::ChannelConfig::custom_root
    pub fn open_dump<P: Into<PathBuf>>(
        roots: impl IntoIterator<Item = P>,
        schemas: &[Schema],
    ) -> Result<Self> {
        let mut schema_map = HashMap::new();
        for schema in schemas {
            if let Some(existing) = schema_map.get(schema.name) {
                let existing: &Schema = existing;
                if existing.identity.is_none()
                    || existing.identity != schema.identity
                    || existing.version != schema.version
                {
                    return Err(ReadError::ConflictingSchema(schema.name.to_owned()));
                }
            } else {
                schema_map.insert(schema.name.to_owned(), *schema);
            }
        }
        let roots: Vec<PathBuf> = roots.into_iter().map(Into::into).collect();
        let Some(main_root) = roots.first() else {
            return Err(ReadError::Io {
                context: "opening a dump".to_owned(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "not one root was named",
                ),
            });
        };
        // A typo in a path has no right to look like an empty store.
        if !main_root.is_dir() {
            return Err(ReadError::Io {
                context: format!("opening dump {}", main_root.display()),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "the main root does not exist",
                ),
            });
        }
        let backend = if main_root.join("dduroc-raft-v1").exists()
            || main_root.join("raft-engine").exists()
        {
            Some(ReadBackend::open_dump(&roots)?)
        } else {
            None
        };
        let epochs = if let Some(backend) = &backend {
            backend.epochs()?
        } else {
            EpochStore::open_read_only(main_root)?
        };
        let store_id = read_store_id(main_root);
        let reader = Self {
            source: Source::Dump { roots, epochs },
            backend,
            schemas: schema_map,
            store_id,
            schema_cache: RwLock::new(HashMap::new()),
            language_preferences: crate::LanguagePreferences::default(),
        };
        reader.check_dump_completeness()?;
        Ok(reader)
    }

    /// Make sure every class has a tree among the dump's roots.
    ///
    /// The check is structural and does not depend on where the dump was moved
    /// to: a namespace's schema declares its classes, and every class's
    /// directory is created when the namespace comes up — so each has to be
    /// found in one of the roots named. A namespace of an unknown schema cannot
    /// be checked — its records will stay identifiers anyway.
    fn check_dump_completeness(&self) -> Result<()> {
        let mut damaged = Vec::new();
        for ns in self.scan_namespaces(&mut damaged)? {
            let Some(schema) = self.schema_by_name(&ns.schema_name) else {
                continue;
            };
            for class in schema.classes() {
                if !ns.channels.iter().any(|&(have, _)| have == class) {
                    return Err(ReadError::IncompleteDump {
                        namespace: ns.name,
                        class,
                    });
                }
            }
        }
        Ok(())
    }

    /// A live reader: parallel to writing by construction.
    ///
    /// The truth is taken from the store itself at the moment of **every
    /// query** rather than when the reader is created, so it can be created
    /// once at start and held for as long as you like:
    ///
    /// - the roots — all of them, the media of classes moved out included
    ///   ([`ChannelConfig::custom_root`]): a reader that was not told every tree
    ///   would show the history without the critical part, giving nothing away;
    /// - the schemas — of the namespaces that have come up by now: a service that
    ///   started after the reader was created is read with its texts rather than
    ///   bare ids;
    /// - the epochs — with the anchors as of this moment: time synchronization is
    ///   retroactive, and a query after one shows a UTC for records made before it.
    ///
    /// Within one query the snapshot is single: every record of the answer is
    /// converted to UTC by one and the same set of anchors.
    ///
    /// Rotation and appending do not get in the way of reading: a segment
    /// evicted between the listing and the open is passed over silently (its
    /// data was evicted, not lost), and the unfinished tail of a live segment
    /// is "not data yet" rather than damage. In a dump both are honestly
    /// declared damage.
    ///
    /// What is visible is exactly what is on the medium: records still sitting
    /// in the writer's queue are visible to no reader — they have to be flushed
    /// first ([`Store::sync`]). A reader keeps the store alive, as a namespace
    /// handle does.
    ///
    /// [`ChannelConfig::custom_root`]:
    /// dduroc_engine::channel::ChannelConfig::custom_root
    pub fn of_store(store: &Arc<Store>) -> Self {
        let store_id = store.meta().store_id;
        Self {
            source: Source::Store(Arc::clone(store)),
            backend: Some(store.read_backend()),
            schemas: HashMap::new(),
            store_id: Some(store_id),
            schema_cache: RwLock::new(HashMap::new()),
            language_preferences: crate::LanguagePreferences::default(),
        }
    }

    /// Whether this is a live reader — see [`Reader::of_store`].
    fn live(&self) -> bool {
        matches!(self.source, Source::Store(_))
    }

    /// Every root of the store in walk order; the first is the main one.
    fn all_roots(&self) -> Vec<PathBuf> {
        match &self.source {
            Source::Store(store) => store.roots().to_vec(),
            Source::Dump { roots, .. } => roots.clone(),
        }
    }

    /// The epochs as of this moment: from the store's memory for a live reader,
    /// from the open-time snapshot for a dump. Taken once per query: every
    /// record of one answer has to be converted to UTC by one set of anchors.
    fn epochs_now(&self) -> std::borrow::Cow<'_, Epochs> {
        match &self.source {
            Source::Store(store) => std::borrow::Cow::Owned(store.epochs()),
            Source::Dump { epochs, .. } => std::borrow::Cow::Borrowed(epochs),
        }
    }

    /// The schema by name: those named by hand first (they are the caller's
    /// explicit decision), then the schemas of a live store's namespaces.
    fn schema_by_name(&self, name: &str) -> Option<Schema> {
        if let Some(s) = self.schemas.get(name) {
            return Some(*s);
        }
        match &self.source {
            Source::Store(store) => store.schema(name),
            Source::Dump { .. } => None,
        }
    }

    /// Every schema known: those named by hand plus — for a live reader — the
    /// schemas of the namespaces that came up.
    fn all_schemas(&self) -> Vec<Schema> {
        let mut out: HashMap<&'static str, Schema> = match &self.source {
            Source::Store(store) => store.schemas().into_iter().map(|s| (s.name, s)).collect(),
            Source::Dump { .. } => HashMap::new(),
        };
        for s in self.schemas.values() {
            out.insert(s.name, *s);
        }
        out.into_values().collect()
    }

    /// Add a schema that was not among those passed at open time.
    ///
    /// Needed when the store holds another service's namespace: without a
    /// schema its records stay identifiers. A schema of the same name replaces
    /// the earlier one.
    pub fn with_schema(mut self, schema: Schema) -> Result<Self> {
        if let Some(existing) = self.schema_by_name(schema.name)
            && (existing.identity.is_none()
                || existing.identity != schema.identity
                || existing.version != schema.version)
        {
            return Err(ReadError::ConflictingSchema(schema.name.to_owned()));
        }
        self.schemas.insert(schema.name.to_owned(), schema);
        self.schema_cache
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
        Ok(self)
    }

    /// Permit foreign legacy segments. This applies only to the legacy segment
    /// reader; raft dump roots must always have matching durable store identities.
    /// Absolute time of foreign legacy records cannot be trusted.
    pub fn allow_foreign_segments(mut self) -> Self {
        self.store_id = None;
        self
    }

    /// The store's main root.
    pub fn root(&self) -> &Path {
        match &self.source {
            Source::Store(store) => store.root(),
            Source::Dump { roots, .. } => &roots[0],
        }
    }

    /// The epochs as of this moment. For a live reader the answer changes from
    /// call to call, as time is synchronized; for a dump it is frozen at open
    /// time.
    pub fn epochs(&self) -> Epochs {
        self.epochs_now().into_owned()
    }

    /// Set the default message languages for this reader.
    pub fn with_language_preferences(mut self, preferences: crate::LanguagePreferences) -> Self {
        self.language_preferences = preferences;
        self
    }

    /// This reader's ordered default message languages.
    pub fn language_preferences(&self) -> &crate::LanguagePreferences {
        &self.language_preferences
    }

    /// Render with this reader's preferences, falling back to the first schema language.
    ///
    /// Schema messages require a matching generated schema identity. Unknown or
    /// raw/manual schemas have no such identity and return `None`; use
    /// [`render`] explicitly for a caller-selected manual schema.
    pub fn render_preferred(&self, entry: &Entry) -> Option<RenderedMessage> {
        self.render_with_preferences(entry, &self.language_preferences)
    }

    /// Render one record with an explicit ordered language preference list.
    ///
    /// Schema messages require a matching generated schema identity. Unknown or
    /// raw/manual schemas return `None`; free text is returned unchanged.
    pub fn render_with_preferences(
        &self,
        entry: &Entry,
        preferences: &crate::LanguagePreferences,
    ) -> Option<RenderedMessage> {
        match &entry.kind {
            EntryKind::Message { event, payload, .. } => {
                let schema = self.schema_of(&entry.namespace)?;
                if entry.schema_identity.is_none() || entry.schema_identity != schema.identity {
                    return None;
                }
                let selection = preferences.select(&schema)?;
                let language = schema.languages[selection.language_index].0;
                Some(RenderedMessage {
                    text: render(&schema, *event, payload, language)?,
                    language: Some(language.to_owned()),
                    preference_index: selection.preference_index,
                })
            }
            EntryKind::Text { text, .. } => Some(RenderedMessage {
                text: text.clone(),
                language: None,
                preference_index: None,
            }),
            _ => None,
        }
    }

    /// A record's text in the given language.
    ///
    /// `None` means there is nothing to render: this is not a schema message,
    /// or the schema is unknown to this build (then the record keeps its
    /// identifier and payload, and the reader will not invent a text).
    ///
    /// The schema is looked up by the record's own namespace. It used to be the
    /// caller's job to find it and pass it in by hand — and it could pass a
    /// foreign one, in which case the payload would be parsed by another
    /// event's decoder: the text would come out plausible and wrong.
    pub fn render(&self, entry: &Entry, lang: &str) -> Option<String> {
        match &entry.kind {
            EntryKind::Message { event, payload, .. } => {
                let schema = self.schema_of(&entry.namespace)?;
                if entry.schema_identity != schema.identity {
                    return None;
                }
                render(&schema, *event, payload, lang)
            }
            // Free text is already text: it has neither a template nor
            // languages.
            EntryKind::Text { text, .. } => Some(text.clone()),
            _ => None,
        }
    }

    /// A namespace's schema — from its metadata on disk.
    ///
    /// The directory's schema name is looked up rather than the first that
    /// fits: two namespaces may live under different schemas in one store.
    ///
    /// The answer is remembered: `ns-meta` is read once per namespace rather
    /// than on every call. [`Reader::render`] is called for every record
    /// displayed, and without this, drawing five hundred lines would cost five
    /// hundred file opens. A schema is `Copy` and lies in `.rodata`; a copy
    /// costs nothing.
    pub fn schema_of(&self, namespace: &str) -> Option<Schema> {
        if let Ok(cache) = self.schema_cache.read()
            && let Some(found) = cache.get(namespace)
        {
            return *found;
        }
        let resolved = if let Some(backend) = &self.backend {
            backend
                .schema_name(namespace)
                .ok()
                .flatten()
                .and_then(|name| self.schema_by_name(&name))
        } else {
            read_ns_meta(&self.root().join(namespace))
                .and_then(|meta| self.schema_by_name(&meta.schema_name))
        };
        // A live reader must not remember "there is no schema": the namespace
        // may not have come up yet, and a cached `None` would hide its texts
        // for the reader's whole life. A schema once found is immutable in both
        // modes.
        if (resolved.is_some() || !self.live())
            && let Ok(mut cache) = self.schema_cache.write()
        {
            // Bound presentation caching under namespace churn. Schema copies
            // are cheap; clearing this cache changes no cursor or delivery mark.
            if cache.len() >= 512 {
                cache.clear();
            }
            cache.insert(namespace.to_owned(), resolved);
        }
        resolved
    }

    /// List the namespaces together with the space they occupy.
    ///
    /// The size costs a `stat` per segment, so the query path does not use it:
    /// the names of the namespaces and channels are enough for it.
    ///
    /// A namespace with unreadable metadata goes not into the list but into
    /// [`NamespaceListing::damaged`]: there is nothing to show it with, but
    /// silence is not allowed either — otherwise the listing would declare
    /// complete an answer a whole namespace dropped out of, and its data would
    /// vanish without trace.
    pub fn namespaces(&self) -> Result<NamespaceListing> {
        let mut damaged = Vec::new();
        let found = self.scan_namespaces(&mut damaged)?;
        let mut namespaces = Vec::with_capacity(found.len());
        let retained = self.backend.as_ref().map(|b| b.catalog()).transpose()?;
        for ns in found {
            let mut total_bytes = 0;
            for (_, dir) in &ns.channels {
                if let Ok(inv) = dduroc_engine::rotation::Inventory::scan(dir) {
                    total_bytes += inv.total_bytes();
                }
            }
            if let Some(catalog) = &retained {
                total_bytes = catalog
                    .iter()
                    .filter(|info| info.key.namespace == ns.name)
                    .map(|info| info.retained_bytes)
                    .sum();
            }
            namespaces.push(NamespaceInfo {
                name: ns.name,
                schema_name: ns.schema_name,
                protocol_version: ns.protocol_version,
                channels: ns.channels.into_iter().map(|(n, _)| n).collect(),
                total_bytes,
            });
        }
        Ok(NamespaceListing {
            namespaces,
            damaged,
        })
    }

    /// The namespaces and their channels — without touching file sizes.
    ///
    /// Unreadable metadata and directories of unknown classes pile up in
    /// `damaged`.
    fn scan_namespaces(&self, damaged: &mut Vec<Damage>) -> Result<Vec<NsScan>> {
        self.scan_namespaces_matching(damaged, |_| true)
    }

    /// The same, but with selection by name **before** the metadata is read.
    ///
    /// A namespace's name is its directory's name, and a query almost always
    /// selects by it: one service, one group. Reading `ns-meta` for all of them
    /// only to throw almost all away is a file operation for each of the
    /// twenty-four thousand directories claimed; on the path of a subscription
    /// that walks the roots on every change of the store's shape, that is the
    /// difference between cheap and unacceptable.
    fn scan_namespaces_matching(
        &self,
        damaged: &mut Vec<Damage>,
        wanted: impl Fn(&str) -> bool,
    ) -> Result<Vec<NsScan>> {
        let mut out = Vec::new();
        if let Some(backend) = &self.backend {
            let mut grouped: std::collections::BTreeMap<String, NsScan> =
                std::collections::BTreeMap::new();
            for info in backend.catalog()? {
                if !wanted(&info.key.namespace) {
                    continue;
                }
                let namespace = info.key.namespace.clone();
                let ns = grouped.entry(namespace.clone()).or_insert_with(|| NsScan {
                    name: namespace.clone(),
                    schema_name: info.schema_name.clone(),
                    protocol_version: info.protocol_version,
                    channels: Vec::new(),
                });
                ns.channels.push((
                    info.key.class,
                    PathBuf::from(namespace).join(info.key.class.as_str()),
                ));
            }
            for ns in grouped.values_mut() {
                ns.channels.sort_by_key(|(class, _)| class.index());
            }
            return Ok(grouped.into_values().collect());
        }
        let roots = self.all_roots();
        let main_root = &roots[0];
        let entries = match std::fs::read_dir(main_root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(source) => {
                return Err(ReadError::Io {
                    context: format!("reading {}", main_root.display()),
                    source,
                });
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !wanted(name) {
                continue;
            }
            let Some(meta) = read_ns_meta(&path) else {
                // A directory with no metadata is not a namespace (a foreign
                // directory in the store root). A directory with UNREADABLE
                // metadata is a namespace we cannot show, and silence about it
                // is not allowed: its data would simply disappear from every
                // answer.
                if path.join(NS_META).exists() {
                    damaged.push(Damage {
                        path,
                        offset: 0,
                        reason: "the namespace metadata does not read".to_owned(),
                    });
                }
                continue;
            };

            // A namespace's channels are gathered across every root: a class
            // may have been moved to a medium of its own, and its directory
            // does not live next to ns-meta. A channel name is unique within a
            // namespace — a class lives in exactly one root.
            let mut channels: Vec<(StorageClass, PathBuf)> = Vec::new();
            for root in &roots {
                let ns_dir = root.join(name);
                let Ok(dir) = std::fs::read_dir(&ns_dir) else {
                    continue;
                };
                for ch in dir.flatten() {
                    let ch_path = ch.path();
                    if !ch_path.is_dir() {
                        continue;
                    }
                    let class = ch_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(StorageClass::from_dir_name);
                    match class {
                        Some(class) => channels.push((class, ch_path)),
                        // A directory that is the channel of no class this
                        // build knows is either a foreign directory or a dump
                        // from a future version with a new class. There is
                        // nothing to parse it with, and it has no right to drop
                        // out silently.
                        None => damaged.push(Damage {
                            path: ch_path,
                            offset: 0,
                            reason: "the directory is not the channel of any known storage class"
                                .to_owned(),
                        }),
                    }
                }
            }
            channels.sort_by_key(|(c, _)| c.index());

            out.push(NsScan {
                name: name.to_owned(),
                schema_name: meta.schema_name,
                protocol_version: meta.protocol_version,
                channels,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Open a query as a stream: records are parsed as the walk goes.
    ///
    /// Memory is bounded by one decompressed block per channel — the same bound
    /// a cursor has. This is the only way to read a lot: [`Reader::query`]
    /// assembles the whole answer, and a query without a `limit` over a
    /// two-hundred-gigabyte store would not fit in armv7's memory.
    ///
    /// The walk can be broken off at any moment; the stream reports damage and
    /// dropped runs by the same means as `query` — but that information is
    /// complete only once the stream is exhausted or abandoned.
    pub fn stream(&self, q: &Query) -> Result<EntryStream<'_>> {
        // The epochs are taken once for the whole stream: both the window's
        // bounds and every record's UTC are computed from one set of anchors —
        // the answer is internally consistent even if a time synchronization
        // arrives mid-walk.
        let epochs = self.epochs_now().into_owned();
        // The bounds are brought to the relative scale once: a wall-clock bound
        // needs an anchor per run, and doing that per record would mean a
        // linear search through the epochs for each of hundreds of thousands.
        let bounds = q.resolve(&epochs).bounds;
        let OpenedCursors {
            mut cursors,
            schemas,
            damaged,
        } = self.open_cursors(q, &bounds)?;

        // The heap is loaded straight away: an empty cursor simply never enters
        // it, and the merge then spends not one comparison on it.
        let newest_first = q.order == Order::Newest;
        let mut heads = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors.iter_mut().enumerate() {
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first,
                });
            }
        }

        Ok(EntryStream {
            reader: self,
            query: q.clone(),
            epochs,
            bounds,
            cursors,
            schemas,
            heads,
            damaged,
            limit: q.limit.unwrap_or(usize::MAX),
            yielded: 0,
            truncated: false,
            done: false,
        })
    }

    /// Subscribe to the record stream: read as records appear.
    ///
    /// A live reader sees records by polling, and without a subscription the
    /// polling would have to be done on a timer — a frequent one (trips to the
    /// medium for nothing, and on a device flash wear besides) or a rare one (a
    /// chart lagging by the period). A subscription sleeps while there is
    /// nothing to write and wakes on the very first block that lands in a file.
    ///
    /// It begins where a query begins: `Query::new()` with no bounds from the
    /// start of the history, `since(...)` from the given moment,
    /// `since(ns.now())` from this second. There is no separate "new only"
    /// knob: that is what a query's window is.
    ///
    /// What a subscription cannot do is refused rather than silently adjusted:
    /// reverse order ([`Order::Newest`]) and an upper window bound. A dump does
    /// not accept a subscription at all — nobody appends to it.
    ///
    /// Namespaces that came up after the subscription began are picked up: a
    /// service that started after the viewer must not require restarting it.
    /// The root walk for their sake is rate limited — it lists the whole store
    /// — so a new service appears on screen a fraction of a second late.
    ///
    /// A subscription hands out what survives until it gets there. Budget
    /// eviction may take a segment it has not reached yet: that is an ordinary
    /// event of the store (`Stats::segments_rotated`) rather than damage, and a
    /// subscription that cannot keep up with rotation falls behind the history
    /// by definition.
    ///
    /// State seeds (`Query::with_state_seed`) come first and carry a time
    /// **before** the window — as in [`Reader::query`], where they lie in a
    /// field of their own: a series unchanged since last week would otherwise
    /// look empty on a live chart.
    pub fn follow(&self, q: &Query) -> Result<Follow<'_>> {
        let Source::Store(store) = &self.source else {
            return Err(ReadError::NotFollowable(
                "nobody appends to a dump — there is nothing to subscribe to",
            ));
        };
        if q.order == Order::Newest {
            return Err(ReadError::NotFollowable(
                "a stream runs oldest to newest: `Order::Newest` has nothing to mean for it",
            ));
        }
        if q.to.is_some() {
            return Err(ReadError::NotFollowable(
                "a subscription has no upper window bound: it reads what is not there yet",
            ));
        }

        let pulse = Arc::clone(store.pulse());
        // The mark is taken BEFORE the cursors are opened: whatever lands
        // between those two moments the cursors will either see themselves or
        // the very first wake-up will show. The reverse order would lose such
        // records until the next mark.
        let seen = pulse.beat();

        let epochs = self.epochs_now().into_owned();
        let bounds = q.resolve(&epochs).bounds;
        let OpenedCursors {
            mut cursors,
            schemas,
            mut damaged,
        } = self.open_cursors_with(q, &bounds, |scope| {
            scope.liveness = crate::cursor::Liveness::Following;
        })?;

        let mut heads = BinaryHeap::with_capacity(cursors.len());
        let mut queued = vec![false; cursors.len()];
        let mut known: HashMap<String, u8> = HashMap::new();
        for (idx, cursor) in cursors.iter_mut().enumerate() {
            *known.entry(cursor.namespace.to_string()).or_default() |= 1 << cursor.channel.index();
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first: false,
                });
                queued[idx] = true;
            }
        }

        // The state seed is the same as `query`'s: a series unchanged since
        // last week would otherwise look empty on a live chart.
        let seeds = if q.seed_states && q.from.is_some() {
            self.collect_state_seeds(q, &bounds, &mut damaged, &epochs)?
        } else {
            Vec::new()
        };

        Ok(Follow {
            reader: self,
            budget: None,
            query: q.clone(),
            bounds,
            epochs,
            cursors,
            schemas,
            heads,
            queued,
            known,
            reported: damaged
                .iter()
                .map(|d| (d.path.clone(), d.reason.clone()))
                .collect(),
            damaged,
            pulse,
            seen,
            seeds: seeds.into_iter(),
            last_adopt: None,
            swept: false,
            adopt_due: false,
            limit: q.limit.unwrap_or(usize::MAX),
            yielded: 0,
            ended: false,
        })
    }

    /// Whether the store's writer thread is alive; a dump has nobody to write
    /// by definition.
    fn writer_alive(&self) -> bool {
        match &self.source {
            Source::Store(store) => store.is_writing(),
            Source::Dump { .. } => false,
        }
    }

    /// Run a query, assembling the whole answer.
    ///
    /// Suitable when the answer's size is bounded — a `limit`, a narrow window,
    /// a rare event type. For everything else there is [`Reader::stream`].
    pub fn query(&self, q: &Query) -> Result<QueryResult> {
        let mut stream = self.stream(q)?;
        let entries: Vec<Entry> = stream.by_ref().collect();
        let mut result = QueryResult {
            entries,
            truncated: stream.truncated(),
            // Damage is gathered even when the answer is cut short by the
            // limit: an answer that data dropped out of because of corruption
            // must not look complete.
            damaged: stream.damaged(),
            unanchored: stream.unanchored(),
            seeds: Vec::new(),
        };

        if q.seed_states && q.from.is_some() {
            let bounds = stream.bounds.clone();
            result.seeds =
                self.collect_state_seeds(q, &bounds, &mut result.damaged, &stream.epochs)?;
        }
        Ok(result)
    }

    /// Find the last sample of every state series before the window begins.
    ///
    /// Legacy dumps inspect at most two eligible segments using their footer
    /// metric sets. Raft stores inspect at most 256 blocks containing records
    /// inside the seed window per channel, with one block buffered at a time.
    /// Raft has no per-channel time index: blocks newer than `from` may be
    /// scanned before reaching that window and do not consume this horizon.
    /// An older state can remain unseeded; seeds are not a complete state
    /// reconstruction. State capture at startup keeps the needed value close.
    fn collect_state_seeds(
        &self,
        q: &Query,
        window: &Bounds,
        damaged: &mut Vec<Damage>,
        epochs: &Epochs,
    ) -> Result<Vec<Entry>> {
        let Some(from) = q.from else {
            return Ok(Vec::new());
        };

        // The state series are gathered from the schemas rather than from the
        // data: there are few of them, and knowing them in advance is cheaper
        // than finding out by reading.
        //
        // The union over all schemas is needed only for cutting segments off by
        // the footer set: there it works as "there is nothing like this in this
        // segment", and a superfluous number costs only a superfluous open.
        // Records must not be selected by it — the `metric_id` space belongs to
        // each schema, and a metric of a foreign schema with the same number
        // means an entirely different quantity.
        let union: HashSet<MetricId> = self
            .all_schemas()
            .iter()
            .flat_map(|s| s.metrics.iter())
            .filter(|m| m.kind == MetricKind::State)
            .map(|m| m.id)
            .collect();
        if union.is_empty() {
            return Ok(Vec::new());
        }
        let union = std::sync::Arc::new(union);

        // We search strictly BEFORE `from`, newest to oldest, so that the first
        // sample of a series encountered is the latest in time.
        let probe = Query {
            from: None,
            to: Some(from.just_before()),
            order: Order::Newest,
            limit: None,
            seed_states: false,
            filter: Filter {
                kinds: KindFilter::TELEMETRY,
                ..q.filter.clone()
            },
            ..q.clone()
        };
        let probe_bounds = probe.resolve(epochs).bounds;

        let OpenedCursors {
            mut cursors,
            schemas,
            damaged: open_damaged,
        } = self.open_cursors_with(&probe, &probe_bounds, |scope| {
            scope.max_segments = Some(SEED_SEGMENTS);
            scope.require_metrics = Some(std::sync::Arc::clone(&union));
        })?;
        damaged.extend(open_damaged);

        // The first value of a series encountered is the latest in time.
        let mut seen: HashSet<(usize, MetricId)> = HashSet::new();
        let mut out: Vec<Entry> = Vec::new();

        for (idx, cursor) in cursors.iter_mut().enumerate() {
            // The state series come from THIS namespace's schema: metric
            // numbers belong to each schema, and a shared set would give a seed
            // from a foreign series — with a foreign state label.
            let wanted = state_metrics(schemas[idx].as_ref());
            if wanted.is_empty() {
                continue;
            }
            while let Some(raw) = cursor.next_entry() {
                // A seed has to lie strictly BEFORE the window. What is checked
                // is "not inside the window" rather than "a microsecond earlier
                // than the bound": the bound may be a wall-clock one, and for a
                // run that began at microsecond zero there is nothing to
                // subtract from.
                if window.contains(raw.at) || !probe_bounds.contains(raw.at) {
                    continue;
                }
                let OwnedRecord::Sample { metric, .. } = &raw.record else {
                    continue;
                };
                let metric = *metric;
                if !wanted.contains(&metric) || !seen.insert((idx, metric)) {
                    continue;
                }
                let ns = std::sync::Arc::clone(&cursor.namespace);
                let ch = cursor.channel;
                if let Some(entry) =
                    self.build_entry(ns, ch, schemas[idx].as_ref(), raw, &probe, epochs)
                {
                    out.push(entry);
                }
                // Every series of this channel has been found — there is
                // nothing more to read.
                if wanted.iter().all(|m| seen.contains(&(idx, *m))) {
                    break;
                }
            }
        }
        // The seed search breaks off the moment every series wanted is found —
        // that is, almost always mid-segment. Damage in the half-read segment
        // has to reach the report from here too.
        for c in &cursors {
            damaged.extend(c.damaged());
        }

        out.sort_by_key(|e| e.at);
        Ok(out)
    }

    /// Open the cursors and resolve the schema **once per namespace**.
    ///
    /// Resolving a schema reads `ns-meta` from disk. Doing that per record, as
    /// it was at first, would mean a file operation per record — and that
    /// turned out to be the main limiter on read speed.
    fn open_cursors(&self, q: &Query, bounds: &Bounds) -> Result<OpenedCursors> {
        self.open_cursors_with(q, bounds, |_| {})
    }

    /// The same with the open parameters adjusted — for the seed search, which
    /// needs a look-at bound and segment cut-off by metric.
    fn open_cursors_with(
        &self,
        q: &Query,
        bounds: &Bounds,
        adjust: impl Fn(&mut crate::cursor::ChannelScope),
    ) -> Result<OpenedCursors> {
        self.open_cursors_beyond(q, bounds, adjust, &HashMap::new())
    }

    /// The same, skipping channels already open: `known` is a mask of classes
    /// by namespace name ([`StorageClass::index`]).
    ///
    /// A subscription needs it: it walks the roots again and again, and opening
    /// a cursor means listing a channel's directory and parsing a segment
    /// header. Paying that for channels already being read means paying for the
    /// whole store for the sake of one newcomer.
    fn open_cursors_beyond(
        &self,
        q: &Query,
        bounds: &Bounds,
        adjust: impl Fn(&mut crate::cursor::ChannelScope),
        known: &HashMap<String, u8>,
    ) -> Result<OpenedCursors> {
        let mut cursors = Vec::new();
        let mut schemas = Vec::new();
        let mut damaged = Vec::new();
        let backend_channels: HashMap<_, _> = if let Some(backend) = &self.backend {
            backend
                .catalog()?
                .into_iter()
                .map(|info| (info.key.clone(), info))
                .collect()
        } else {
            HashMap::new()
        };

        for ns in self.scan_namespaces_matching(&mut damaged, |name| q.namespaces.matches(name))? {
            let schema = self.schema_by_name(&ns.schema_name);
            let ns_name: std::sync::Arc<str> = std::sync::Arc::from(ns.name.as_str());
            let mut scope = crate::cursor::ChannelScope {
                budget: None,
                bounds: bounds.clone(),
                boot: q.boot,
                reverse: q.order == Order::Newest,
                expect_store: self.store_id,
                prefilter: Some(build_prefilter(q, schema)),
                max_segments: None,
                require_metrics: None,
                // Segments of earlier versions are brought to the current one
                // as they are read: the answer's correctness does not wait for
                // a physical run.
                migrations: schema.as_ref().map(crate::cursor::MigrationCtx::of),
                liveness: if self.live() {
                    crate::cursor::Liveness::Live
                } else {
                    crate::cursor::Liveness::Frozen
                },
            };
            adjust(&mut scope);

            let seen = known.get(&ns.name).copied().unwrap_or(0);
            for &(channel, ref dir) in &ns.channels {
                if !q.channels.is_empty() && !q.channels.contains(&channel) {
                    continue;
                }
                if seen & (1 << channel.index()) != 0 {
                    continue;
                }
                if let Some(backend) = &self.backend {
                    let key = dduroc_engine::backend::ChannelKey {
                        namespace: ns.name.clone(),
                        class: channel,
                    };
                    if let Some(info) = backend_channels.get(&key) {
                        if info.first_index.is_none() {
                            continue;
                        }
                        cursors.push(ChannelCursor::open_backend(
                            backend.clone(),
                            info.clone(),
                            &scope,
                        ));
                    } else {
                        continue;
                    }
                } else {
                    cursors.push(ChannelCursor::open(
                        dir,
                        std::sync::Arc::clone(&ns_name),
                        channel,
                        &scope,
                    )?);
                }
                schemas.push(schema);
            }
        }

        Ok(OpenedCursors {
            cursors,
            schemas,
            damaged,
        })
    }

    /// Assemble an answer record from one that was read.
    ///
    /// The record read is taken **by value**: its payload (text, blob) has
    /// already been copied out of the block buffer by the cursor, and a second
    /// copy of the same content is an allocation per record displayed. A record
    /// that is filtered out is simply destroyed right here.
    #[allow(clippy::too_many_arguments)]
    fn build_entry(
        &self,
        ns: std::sync::Arc<str>,
        channel: StorageClass,
        schema: Option<&Schema>,
        raw: crate::cursor::RawEntry,
        q: &Query,
        epochs: &Epochs,
    ) -> Option<Entry> {
        let crate::cursor::RawEntry { at, record } = raw;
        let kinds = q.filter.kinds;
        // The filters that speak about content. A record without such a
        // property (text with no tags, a span with no event type) cannot
        // satisfy them and is excluded — otherwise the filter "only with the rf
        // tag" would let through things that never have tags. `min_level` is
        // not among them: messages and text have a level, while telemetry and
        // spans are outside the level scale.
        let content_filtered = !q.filter.any_tags.is_empty()
            || q.filter.events.is_some()
            || !q.filter.event_names.is_empty();
        let (kind, span) = match record {
            OwnedRecord::Message {
                event,
                span,
                payload,
            } => {
                if !kinds.messages {
                    return None;
                }
                let desc = schema.and_then(|s| s.event(event));
                // The level and the tags are static properties of the type, so
                // the filter is applied here, without reading the payload.
                if let Some(min) = q.filter.min_level {
                    match desc.map(|d| d.level) {
                        Some(l) if l >= min => {}
                        // The level is unknown — the record is not discarded:
                        // this is an event of a type removed from the schema,
                        // and hiding it silently from whoever is looking for a
                        // problem is not allowed.
                        None => {}
                        _ => return None,
                    }
                }
                if !q.filter.any_tags.is_empty() {
                    let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                    if !q
                        .filter
                        .any_tags
                        .iter()
                        .any(|want| tags.iter().any(|t| t == want))
                    {
                        return None;
                    }
                }
                if let Some(want) = &q.filter.events
                    && !want.contains(&event)
                {
                    return None;
                }
                if !q.filter.event_names.is_empty() {
                    let name = desc.map(|d| d.name).unwrap_or("");
                    if !q.filter.event_names.iter().any(|n| n == name) {
                        return None;
                    }
                }
                (
                    EntryKind::Message {
                        event,
                        name: desc.map(|d| d.name),
                        level: desc.map(|d| d.level),
                        tags: desc.map(|d| d.tags).unwrap_or(&[]),
                        payload,
                    },
                    span,
                )
            }
            OwnedRecord::Text {
                level,
                span,
                target,
                text,
            } => {
                if !kinds.text || content_filtered {
                    return None;
                }
                if let Some(min) = q.filter.min_level
                    && level < min
                {
                    return None;
                }
                (
                    EntryKind::Text {
                        level,
                        target,
                        text,
                    },
                    span,
                )
            }
            OwnedRecord::SpanStart { span, kind, parent } => {
                if !kinds.spans || content_filtered {
                    return None;
                }
                (
                    EntryKind::SpanStart {
                        span,
                        kind_name: schema.and_then(|s| s.span(kind)).map(|d| d.name),
                        parent,
                    },
                    Some(span),
                )
            }
            OwnedRecord::SpanEnd { span } => {
                if !kinds.spans || content_filtered {
                    return None;
                }
                (EntryKind::SpanEnd { span }, Some(span))
            }
            OwnedRecord::Sample { metric, value } => {
                if !kinds.samples || q.filter.events.is_some() || !q.filter.event_names.is_empty() {
                    return None;
                }
                // A series is identified by the record itself: the metric is
                // the series. Everything else — the name, the unit, the state
                // label, the severity, the behaviour between samples — is
                // resolved from the schema and takes no room on disk.
                let desc = schema.and_then(|s| s.metric(metric));
                // A sample does have tags — its metric's: the filter on them
                // applies.
                if !q.filter.any_tags.is_empty() {
                    let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                    if !q
                        .filter
                        .any_tags
                        .iter()
                        .any(|want| tags.iter().any(|t| t == want))
                    {
                        return None;
                    }
                }
                let code = match &value {
                    OwnedSampleValue::U64(v) => Some(*v),
                    OwnedSampleValue::I64(v) if *v >= 0 => Some(*v as u64),
                    OwnedSampleValue::Bool(b) => Some(u64::from(*b)),
                    _ => None,
                };
                // The severity is computed before the value is moved:
                // afterwards there would be nothing left to borrow.
                let severity = desc.map(|d| d.severity_of(&as_format_value(&value)));
                (
                    EntryKind::Sample {
                        metric,
                        metric_name: desc.map(|d| d.name),
                        unit: desc.map(|d| d.unit),
                        tags: desc.map_or(&[][..], |d| d.tags),
                        kind: desc.map(|d| d.kind),
                        state_name: desc.zip(code).and_then(|(d, c)| d.state(c)).map(|s| s.name),
                        severity,
                        value,
                    },
                    None,
                )
            }
            OwnedRecord::Ext { bytes } => {
                if content_filtered {
                    return None;
                }
                (EntryKind::Ext { bytes }, None)
            }
        };

        // Records outside any span are discarded along with all those whose
        // span is not named: "attached to this span" is false for both.
        if let Some(want) = &q.filter.spans
            && !span.is_some_and(|s| want.contains(&s))
        {
            return None;
        }

        Some(Entry {
            namespace: ns,
            schema_identity: schema.and_then(|s| s.identity),
            channel,
            at,
            utc: epochs.to_utc(at),
            span,
            kind,
        })
    }
}

/// Build the selection predicate applied before a record is materialized.
///
/// Levels and tags are static properties of types, so a query like "errors
/// only" is decided from the schema without reading any payload; a record that
/// is filtered out costs neither an allocation nor a copy.
fn build_prefilter(q: &Query, schema: Option<Schema>) -> crate::cursor::Prefilter {
    let kinds = q.filter.kinds;
    let min_level = q.filter.min_level;
    let events = q.filter.events.clone();
    let event_names = q.filter.event_names.clone();
    let any_tags = q.filter.any_tags.clone();

    std::sync::Arc::new(move |record: &dduroc_format::Record<'_>| match record {
        dduroc_format::Record::Message(m) => {
            if !kinds.messages {
                return false;
            }
            if let Some(want) = &events
                && !want.contains(&m.event)
            {
                return false;
            }
            let desc = schema.and_then(|s| s.event(m.event));
            if let Some(min) = min_level {
                match desc.map(|d| d.level) {
                    Some(l) if l >= min => {}
                    // The level is unknown — the record is not hidden: this is
                    // an event of a type removed from the schema, and hiding it
                    // from whoever is looking for a problem is not allowed.
                    None => {}
                    _ => return false,
                }
            }
            if !any_tags.is_empty() {
                let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                if !any_tags.iter().any(|want| tags.iter().any(|t| t == want)) {
                    return false;
                }
            }
            if !event_names.is_empty() {
                let name = desc.map(|d| d.name).unwrap_or("");
                if !event_names.iter().any(|n| n == name) {
                    return false;
                }
            }
            true
        }
        // Content filters exclude the records that cannot satisfy them: text
        // and spans have neither tags nor an event type, and "passed the tag
        // filter" would be a lie for them. The level is not a content filter:
        // text has one and it is checked, while telemetry and spans are outside
        // the level scale and are not filtered out by it.
        dduroc_format::Record::Text(t) => {
            kinds.text
                && min_level.is_none_or(|min| t.level >= min)
                && any_tags.is_empty()
                && events.is_none()
                && event_names.is_empty()
        }
        dduroc_format::Record::SpanStart(_) | dduroc_format::Record::SpanEnd { .. } => {
            kinds.spans && any_tags.is_empty() && events.is_none() && event_names.is_empty()
        }
        dduroc_format::Record::Sample(s) => {
            if !kinds.samples || events.is_some() || !event_names.is_empty() {
                return false;
            }
            // A sample does have tags — its metric's: the filter on them
            // applies.
            if !any_tags.is_empty() {
                let tags = schema
                    .and_then(|sc| sc.metric(s.metric))
                    .map(|d| d.tags)
                    .unwrap_or(&[]);
                if !any_tags.iter().any(|want| tags.iter().any(|t| t == want)) {
                    return false;
                }
            }
            true
        }
        dduroc_format::Record::Ext { .. } => {
            any_tags.is_empty() && events.is_none() && event_names.is_empty()
        }
    })
}

/// The state metrics of one schema. Empty if the schema is unknown to this
/// build.
fn state_metrics(schema: Option<&Schema>) -> HashSet<MetricId> {
    schema.map_or_else(HashSet::new, |s| {
        s.metrics
            .iter()
            .filter(|m| m.kind == MetricKind::State)
            .map(|m| m.id)
            .collect()
    })
}

fn read_ns_meta(dir: &Path) -> Option<NsMeta> {
    NsMeta::read(dir).ok().flatten()
}

fn read_store_id(root: &Path) -> Option<u64> {
    dduroc_engine::store::StoreMeta::read(root)
        .ok()
        .flatten()
        .map(|meta| meta.store_id)
}

/// Render a message from the schema's template.
///
/// A low-level path: the schema has to be found by the caller. Usually
/// [`Reader::render`] is what is wanted — it takes the schema from the
/// record's namespace.
///
/// Templates are not stored on disk: `{field}` is substituted by the decoder
/// generated by the schema macro. Without a decoder the template itself comes
/// back.
pub fn render(schema: &Schema, event: EventId, payload: &[u8], lang: &str) -> Option<String> {
    let desc = schema.event(event)?;
    let lang_index = schema.language_index(lang).unwrap_or(0);
    match desc.decoders {
        Some(d) => (d.render)(payload, lang_index).ok(),
        None => desc.template(lang_index).map(|t| t.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::KindFilter;
    use dduroc_engine::schema::{EventDesc, Language, MetricDesc, SpanDesc, StorageClass};
    use dduroc_engine::store::{Store, StoreConfig};
    use dduroc_format::{MetricId, Micros, ProtocolVersion, Record, SpanKindId, ValueType};

    #[test]
    fn completeness_includes_truncation_and_unanchored_runs() {
        let mut result = QueryResult::default();
        assert!(result.is_complete());
        result.truncated = true;
        assert!(!result.is_complete());
        assert!(result.is_undamaged());
        result.truncated = false;
        result.unanchored.push(BootCounter(1));
        assert!(!result.is_complete());
    }

    static LANGS: &[Language] = &[Language("en"), Language("ru")];
    static EVENTS: &[EventDesc] = &[
        EventDesc {
            id: EventId(1),
            name: "PowerSet",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &["rf"],
            templates: &["power set", "мощность задана"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::Critical,
            tags: &["fault"],
            templates: &["alarm", "авария"],
            fields: &[],
            decoders: None,
        },
    ];
    static LINK_STATES: &[dduroc_engine::schema::StateDesc] = &[
        dduroc_engine::schema::StateDesc {
            code: 0,
            name: "Los",
            severity: Severity::Alarm,
        },
        dduroc_engine::schema::StateDesc {
            code: 1,
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
            thresholds: dduroc_engine::schema::Thresholds {
                warn: dduroc_engine::schema::Range {
                    min: None,
                    max: Some(25.0),
                },
                alarm: dduroc_engine::schema::Range {
                    min: None,
                    max: Some(28.0),
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
            thresholds: dduroc_engine::schema::Thresholds::NONE,
        },
    ];
    static SPANS: &[SpanDesc] = &[SpanDesc {
        id: SpanKindId(1),
        name: "Calibration",
        class: StorageClass::Default,
    }];

    struct ReaderTestSchema;
    const READER_ID: dduroc_engine::schema::SchemaIdentity =
        dduroc_engine::schema::SchemaIdentity::of::<ReaderTestSchema>(ProtocolVersion(1));
    fn schema() -> Schema {
        Schema {
            identity: Some(READER_ID),
            name: "radio",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: METRICS,
            spans: SPANS,
            migrations: &[],
        }
    }

    /// Explicit v1 segment fixtures remain readable without opening a new Store.
    fn populate_legacy(root: &Path) {
        use dduroc_engine::segment::SegmentWriter;
        use dduroc_format::{
            Compression, FooterBuilder, block::BlockBuilder, record::Message,
            segment::SegmentHeader,
        };
        #[derive(serde::Serialize)]
        struct Meta {
            container_version: u8,
            store_id: u64,
        }
        std::fs::write(
            root.join("store-meta"),
            postcard::to_allocvec(&Meta {
                container_version: 1,
                store_id: 17,
            })
            .unwrap(),
        )
        .unwrap();
        for namespace in ["orc-radio-0", "orc-radio-1"] {
            let dir = root.join(namespace);
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                dir.join(NS_META),
                postcard::to_allocvec(&NsMeta {
                    schema_name: schema().name.into(),
                    protocol_version: 1,
                })
                .unwrap(),
            )
            .unwrap();
            for class in StorageClass::ALL {
                std::fs::create_dir_all(dir.join(class.as_str())).unwrap();
            }
            let mut segment = SegmentWriter::create(
                &dir.join("default"),
                SegmentHeader {
                    protocol_version: ProtocolVersion(1),
                    boot: BootCounter(0),
                    base: Micros(100),
                    store_id: 17,
                },
                1 << 20,
            )
            .unwrap();
            let mut footer = FooterBuilder::new();
            for index in 0..8 {
                let at = Micros(100 + index);
                let mut builder = BlockBuilder::new();
                builder
                    .push(
                        at,
                        &Record::Message(Message {
                            event: EventId(1),
                            span: None,
                            payload: &[1],
                        }),
                    )
                    .unwrap();
                let mut bytes = Vec::new();
                let header = builder
                    .finish(segment.next_seq(), Compression::None, &mut bytes)
                    .unwrap();
                let offset = segment.append_block(&bytes).unwrap();
                footer.add_event(EventId(1));
                footer.add_block(offset, &header, at);
            }
            segment.seal(&footer.build()).unwrap();
        }
    }

    /// Typed metric constants — what the schema macro produces.
    const TEMP: dduroc_engine::metric::Metric<f32> =
        dduroc_engine::metric::Metric::bound(MetricId(1), READER_ID);

    /// Fill the store and close it.
    fn populate(root: &Path) {
        let store =
            Store::open(StoreConfig::new(root).with_budget_per_class(16 * 1024 * 1024)).unwrap();
        for inst in 0..2 {
            let ns = store
                .namespace(&format!("orc-radio-{inst}"), schema())
                .unwrap();
            for i in 0..20u8 {
                ns.log_raw(EventId(1), &[i], None);
            }
            ns.log_raw(EventId(2), &[0xFF], None);

            let temp = ns.series(TEMP).unwrap();
            for i in 0..10 {
                temp.sample(20.0 + i as f32);
            }
            {
                let cal = ns.span_raw(SpanKindId(1));
                cal.log_raw(EventId(1), &[99]);
            }
            ns.sync().unwrap();
        }
        let ns = store.namespace("apt-modem-0", schema()).unwrap();
        ns.log_raw(EventId(1), &[1], None);
        ns.sync().unwrap();
        store.shutdown();
    }

    #[test]
    fn damaged_legacy_control_metadata_retains_existing_fallbacks() {
        let dir = tempfile::tempdir().unwrap();
        populate_legacy(dir.path());
        let ns_path = dir.path().join("orc-radio-0").join(NS_META);
        let mut ns_bytes = std::fs::read(&ns_path).unwrap();
        ns_bytes.push(0);
        std::fs::write(&ns_path, &ns_bytes).unwrap();
        let store_path = dir.path().join("store-meta");
        let mut store_bytes = std::fs::read(&store_path).unwrap();
        store_bytes.push(0);
        std::fs::write(&store_path, &store_bytes).unwrap();
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        assert!(reader.store_id.is_none());
        let result = reader.query(&Query::new()).unwrap();
        assert!(
            result
                .damaged
                .iter()
                .any(|damage| damage.path == ns_path.parent().unwrap())
        );
        assert!(
            result
                .entries
                .iter()
                .all(|entry| &*entry.namespace == "orc-radio-1")
        );
        assert_eq!(std::fs::read(&ns_path).unwrap(), ns_bytes);
        assert_eq!(std::fs::read(&store_path).unwrap(), store_bytes);
        std::fs::File::create(&ns_path)
            .unwrap()
            .set_len(256 * 1024 + 1)
            .unwrap();
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        assert!(
            reader
                .query(&Query::new())
                .unwrap()
                .damaged
                .iter()
                .any(|damage| damage.path == ns_path.parent().unwrap())
        );
    }

    #[test]
    fn owned_session_pages_match_stream_and_preserve_small_page_entry() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Arc::new(Reader::open_dump([dir.path()], &[schema()]).unwrap());
        for order in [Order::Oldest, Order::Newest] {
            let query = Query::new().order(order);
            let expected = reader.query(&query).unwrap().entries;
            let mut session = reader
                .open_session(
                    query,
                    crate::ReadLimits::default(),
                    crate::Cancellation::new(),
                )
                .unwrap();
            assert!(matches!(
                session.next_page(crate::PageLimits::new(1, 1).unwrap()),
                Err(ReadError::PageTooSmall)
            ));
            let mut actual = Vec::new();
            loop {
                let page = session
                    .next_page(crate::PageLimits::new(3, 1024 * 1024).unwrap())
                    .unwrap();
                actual.extend(page.entries);
                if page.exhausted {
                    break;
                }
            }
            assert_eq!(actual, expected);
        }
        fn assert_send_static<T: Send + 'static>() {}
        assert_send_static::<QuerySession>();
    }

    #[test]
    fn session_limits_and_cancellation_are_not_eof() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Arc::new(Reader::open_dump([dir.path()], &[schema()]).unwrap());
        let cancel = crate::Cancellation::new();
        cancel.cancel();
        assert!(matches!(
            reader.open_session(Query::new(), crate::ReadLimits::default(), cancel),
            Err(ReadError::Cancelled)
        ));
        let limits = crate::ReadLimits {
            blocks: 0,
            ..Default::default()
        };
        assert!(matches!(
            reader.open_session(Query::new(), limits, crate::Cancellation::new()),
            Err(ReadError::LimitExceeded(crate::ReadResource::Blocks))
        ));
    }

    #[test]
    fn reads_back_everything_written() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let listing = reader.namespaces().unwrap();
        assert!(listing.is_complete(), "every namespace was listed");
        let namespaces = listing.namespaces;
        assert_eq!(namespaces.len(), 3, "three namespaces");
        assert_eq!(namespaces[0].name, "apt-modem-0");
        assert_eq!(namespaces[0].schema_name, "radio");
        assert!(namespaces[0].total_bytes > 0);

        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(result.is_complete(), "there must be no damage");
        assert!(!result.entries.is_empty());

        // The messages of both instances and of the modem are there.
        let messages = result
            .entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::Message { .. }))
            .count();
        assert_eq!(
            messages,
            2 * 22 + 1,
            "20 + the alarm + the one inside a span, ×2, +1"
        );
    }

    #[test]
    fn schema_resolves_names_levels_and_units() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let result = reader
            .query(&Query::new().order(Order::Oldest).limit(500))
            .unwrap();

        let msg = result
            .entries
            .iter()
            .find(|e| {
                matches!(
                    &e.kind,
                    EntryKind::Message {
                        name: Some("PowerSet"),
                        ..
                    }
                )
            })
            .expect("the event name was restored from the schema");
        assert_eq!(
            msg.level(),
            Some(Level::Info),
            "the level was taken from the schema"
        );

        let sample = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Sample { .. }))
            .expect("the sample was found");
        match &sample.kind {
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
                assert_eq!(*metric, MetricId(1), "the identifier came from the record");
                assert_eq!(*metric_name, Some("temp"));
                assert_eq!(*unit, Some("°C"));
                assert_eq!(tags, &["thermal"]);
                assert_eq!(*kind, Some(MetricKind::Gauge));
                assert_eq!(*state_name, None, "not an enum, so there is no label");
                assert!(
                    severity.is_some(),
                    "the severity was computed from the schema"
                );
                assert!(value.as_f64().unwrap() >= 20.0);
            }
            other => panic!("expected a sample: {other:?}"),
        }

        let span = result.entries.iter().find(|e| {
            matches!(
                &e.kind,
                EntryKind::SpanStart {
                    kind_name: Some("Calibration"),
                    ..
                }
            )
        });
        assert!(span.is_some(), "the span kind was restored");
    }

    #[test]
    fn filters_by_group_level_and_kind() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        // Orchestrators only.
        let orc = reader
            .query(&Query::new().group("orc-").order(Order::Oldest))
            .unwrap();
        assert!(
            orc.entries.iter().all(|e| e.namespace.starts_with("orc-")),
            "the group was selected by prefix"
        );

        // Errors only: the level is a property of the type, so no payload need
        // be read.
        let errors = reader
            .query(
                &Query::new()
                    .min_level(Level::Error)
                    .kinds(KindFilter::LOGS)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(errors.entries.len(), 2, "one alarm per instance");
        assert!(
            errors
                .entries
                .iter()
                .all(|e| e.level() == Some(Level::Error))
        );

        // Telemetry only.
        let telemetry = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(telemetry.entries.len(), 20, "10 samples × 2 instances");
        assert!(
            telemetry
                .entries
                .iter()
                .all(|e| matches!(e.kind, EntryKind::Sample { .. }))
        );
    }

    #[test]
    fn content_filters_skip_records_that_cannot_match_them() {
        // The filter "only with the rf tag" has no right to let free text
        // through: text has no tags, and that would be no match. Text and spans
        // used to pass any filter on tags and types — loss notices surfaced in
        // the middle of an answer meant to be "only rf subsystem events". And
        // tags belong not only to events: a sample carries its metric's tags,
        // and the filter has to work on those.
        let dir = tempfile::tempdir().unwrap();
        {
            let store =
                Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024))
                    .unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[7], None); // PowerSet, tag rf
            ns.log_raw(EventId(2), &[1], None); // Alarm, tag fault
            ns.log_text(Level::Warn, "app", "free text with no tags", None);
            ns.series(TEMP).unwrap().sample(21.0); // metric temp, tag thermal
            ns.series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::U64(1)); // link, tag rf
            {
                let cal = ns.span_raw(SpanKindId(1));
                cal.log_raw(EventId(1), &[8]); // rf, inside a span
            }
            ns.sync().unwrap();
            store.shutdown();
        }
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let rf = reader
            .query(&Query::new().any_tag("rf").order(Order::Oldest))
            .unwrap();
        assert!(rf.is_complete());
        let mut messages = 0;
        let mut samples = 0;
        for e in &rf.entries {
            match &e.kind {
                EntryKind::Message { .. } => messages += 1,
                EntryKind::Sample { .. } => samples += 1,
                other => panic!("{other:?} carries no tag and could not have passed the filter"),
            }
        }
        assert_eq!(messages, 2, "PowerSet on its own and inside a span");
        assert_eq!(samples, 1, "the link sample passed by its metric's tag");

        // A filter on event type: samples, text and spans are not events and
        // cannot satisfy it.
        let alarms = reader
            .query(&Query::new().event(EventId(2)).order(Order::Oldest))
            .unwrap();
        assert_eq!(alarms.entries.len(), 1, "{:?}", alarms.entries);
        assert!(matches!(
            &alarms.entries[0].kind,
            EntryKind::Message {
                name: Some("Alarm"),
                ..
            }
        ));

        let by_name = reader
            .query(&Query::new().event_name("PowerSet").order(Order::Oldest))
            .unwrap();
        assert_eq!(by_name.entries.len(), 2, "{:?}", by_name.entries);
    }

    #[test]
    fn telemetry_keeps_identity_in_newest_order() {
        // A series definition is written into the body once, before the first
        // sample. On a reverse walk — the default mode — a sample is met BEFORE
        // its definition, so identity cannot be restored from the stream: all
        // the telemetry would arrive anonymous.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        for order in [Order::Oldest, Order::Newest] {
            let result = reader
                .query(&Query::new().kinds(KindFilter::TELEMETRY).order(order))
                .unwrap();
            assert_eq!(result.entries.len(), 20, "order {order:?}");
            for e in &result.entries {
                match &e.kind {
                    EntryKind::Sample {
                        metric_name,
                        unit,
                        tags,
                        ..
                    } => {
                        assert_eq!(*metric_name, Some("temp"), "order {order:?}");
                        assert_eq!(*unit, Some("°C"));
                        assert_eq!(tags, &["thermal"]);
                    }
                    other => panic!("expected a sample: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn sparse_state_series_gets_seeded_at_the_window_edge() {
        // States are written on change. A window inside which the state did not
        // change holds not one sample — and the band on a chart would come out
        // empty although the state was known the whole time.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // The transition to Lock is the only state sample, and it is early.
        let link = ns.series_untyped(MetricId(2)).unwrap();
        link.sample_raw(dduroc_engine::staged::OwnedValue::U64(1));
        let after_state = ns.now();

        // After that only temperature: the window will be without states.
        let temp = ns.series(TEMP).unwrap();
        for i in 0..20 {
            temp.sample(20.0 + i as f32);
        }
        ns.sync().unwrap();
        store.shutdown();
        drop(temp);
        drop(link);
        drop(ns);
        drop(store);

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let window = Query {
            from: Some(BootTime::new(after_state.boot, Micros(after_state.at.0 + 1)).into()),
            order: Order::Oldest,
            filter: crate::Filter {
                kinds: KindFilter::TELEMETRY,
                ..Default::default()
            },
            ..Query::new()
        };

        // Without a seed there is no state in the answer at all.
        let plain = reader.query(&window).unwrap();
        assert!(plain.seeds.is_empty());
        assert!(
            !plain.entries.iter().any(|e| matches!(
                &e.kind,
                EntryKind::Sample {
                    state_name: Some(_),
                    ..
                }
            )),
            "the window really holds not one state"
        );

        // With a seed the last sample before the window arrives, separately.
        let seeded = reader
            .query(&Query {
                seed_states: true,
                ..window
            })
            .unwrap();
        assert_eq!(seeded.seeds.len(), 1, "one per state series");
        let seed = &seeded.seeds[0];
        assert!(
            seed.at <= after_state,
            "a seed must lie BEFORE the window: {} vs {}",
            seed.at,
            after_state
        );
        match &seed.kind {
            EntryKind::Sample {
                metric,
                state_name,
                kind,
                severity,
                ..
            } => {
                assert_eq!(*metric, MetricId(2));
                assert_eq!(*state_name, Some("Lock"), "the state label from the schema");
                assert_eq!(*kind, Some(MetricKind::State));
                assert_eq!(*severity, Some(Severity::Normal));
            }
            other => panic!("expected a state sample: {other:?}"),
        }
        // The window itself is unchanged by the seed.
        assert_eq!(seeded.entries.len(), plain.entries.len());
    }

    #[test]
    fn state_seeds_come_from_the_schema_of_their_own_namespace() {
        // The `metric_id` space belongs to each schema. State series gathered
        // as a union over all schemas would give a seed from a foreign series:
        // metric number 2 in "radio" is a state machine, in "modem" an ordinary
        // continuous quantity, and labelling it with states is not allowed.
        static OTHER_METRICS: &[MetricDesc] = &[MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(2), // the same number, a different quantity
            name: "voltage",
            value_type: ValueType::F32,
            class: StorageClass::Default,
            unit: "V",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: dduroc_engine::schema::Thresholds::NONE,
        }];
        fn other() -> Schema {
            Schema {
                name: "modem",
                metrics: OTHER_METRICS,
                ..schema()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let after;
        {
            let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
            let store = Store::open(cfg).unwrap();

            let radio = store.namespace("orc-radio-0", schema()).unwrap();
            radio
                .series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::U64(1));

            let modem = store.namespace("apt-modem-0", other()).unwrap();
            modem
                .series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::F32(3.3));

            after = radio.now();
            // From here on, only what the window will see.
            radio.series(TEMP).unwrap().sample(21.0);
            radio.sync().unwrap();
            modem.sync().unwrap();
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[schema(), other()]).unwrap();
        let seeded = reader
            .query(&Query {
                from: Some(BootTime::new(after.boot, Micros(after.at.0 + 1)).into()),
                order: Order::Oldest,
                seed_states: true,
                filter: crate::Filter {
                    kinds: KindFilter::TELEMETRY,
                    ..Default::default()
                },
                ..Query::new()
            })
            .unwrap();

        assert_eq!(
            seeded.seeds.len(),
            1,
            "only a real state series gets a seed: {:?}",
            seeded.seeds
        );
        let seed = &seeded.seeds[0];
        assert_eq!(&*seed.namespace, "orc-radio-0");
        match &seed.kind {
            EntryKind::Sample {
                state_name, kind, ..
            } => {
                assert_eq!(*state_name, Some("Lock"));
                assert_eq!(*kind, Some(MetricKind::State));
            }
            other => panic!("expected a state: {other:?}"),
        }
    }

    #[test]
    fn state_seed_is_absent_when_there_is_nothing_before_the_window() {
        // A query from the beginning of time: there is nothing before the
        // window, and nowhere to invent a seed from.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let r = reader
            .query(&Query {
                from: Some(BootTime::from_raw(0, 0).into()),
                seed_states: true,
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(r.seeds.is_empty());
        assert!(r.is_complete());
    }

    #[test]
    fn telemetry_identity_survives_an_unsealed_segment() {
        // A live store: the segment is still being written, there is no footer
        // and with it no series table. Identity has to be gathered by a pass
        // over the bodies — the same pass that finds the block offsets.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-radio-0", schema()).unwrap();
        let temp = ns.series(TEMP).unwrap();
        for i in 0..10 {
            temp.sample(20.0 + i as f32);
        }
        ns.sync().unwrap(); // the data is on disk but the segment is not sealed

        let reader = Reader::of_store(&store);
        for order in [Order::Oldest, Order::Newest] {
            let result = reader
                .query(&Query::new().kinds(KindFilter::TELEMETRY).order(order))
                .unwrap();
            assert_eq!(result.entries.len(), 10, "order {order:?}");
            assert!(
                result.entries.iter().all(|e| matches!(
                    &e.kind,
                    EntryKind::Sample {
                        metric_name: Some("temp"),
                        unit: Some("°C"),
                        ..
                    }
                )),
                "order {order:?}: the series is anonymous in an unsealed segment"
            );
        }
        store.shutdown();
    }

    #[test]
    fn telemetry_identity_survives_time_range_seek() {
        // A query with a lower bound skips the leading blocks by the footer
        // index — and the series definitions that lie in them with them.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid.into()),
                order: Order::Oldest,
                filter: crate::Filter {
                    kinds: KindFilter::TELEMETRY,
                    ..Default::default()
                },
                ..Query::new()
            })
            .unwrap();
        assert!(!narrowed.entries.is_empty());
        assert!(
            narrowed.entries.iter().all(|e| matches!(
                &e.kind,
                EntryKind::Sample {
                    metric_name: Some("temp"),
                    ..
                }
            )),
            "a series identity must survive a jump in time"
        );
    }

    #[test]
    fn newest_order_and_limit() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let newest = reader
            .query(&Query::new().order(Order::Newest).limit(5))
            .unwrap();
        assert_eq!(newest.entries.len(), 5);
        assert!(newest.truncated, "the answer is cut short by the limit");

        // Time does not increase.
        let times: Vec<BootTime> = newest.entries.iter().map(|e| e.at).collect();
        assert!(
            times.windows(2).all(|w| w[0] >= w[1]),
            "newest to oldest order: {times:?}"
        );

        let oldest = reader
            .query(&Query::new().order(Order::Oldest).limit(5))
            .unwrap();
        let times: Vec<BootTime> = oldest.entries.iter().map(|e| e.at).collect();
        assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");
    }

    #[test]
    fn truncation_is_announced_only_when_something_was_left_out() {
        // The mark used to be set on entering the hand-out rather than when a
        // record really did not fit: an answer of exactly `limit` records was
        // declared truncated even when there were no more. For a web layer that
        // is a "next" button that never goes away and leads nowhere.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let total = all.entries.len();
        assert!(!all.truncated, "a query without a limit is never truncated");

        // A limit exactly equal to the record count: there was nothing to
        // truncate.
        let exact = reader
            .query(&Query::new().order(Order::Oldest).limit(total))
            .unwrap();
        assert_eq!(exact.entries.len(), total);
        assert!(
            !exact.truncated,
            "exactly {total} records and no more — the answer is complete"
        );

        // One less — truncated for real.
        let cut = reader
            .query(&Query::new().order(Order::Oldest).limit(total - 1))
            .unwrap();
        assert_eq!(cut.entries.len(), total - 1);
        assert!(cut.truncated, "one record was left out");

        // An empty answer at a zero limit: there are records, there is no room.
        let none = reader
            .query(&Query::new().order(Order::Oldest).limit(0))
            .unwrap();
        assert!(none.entries.is_empty());
        assert!(none.truncated);
    }

    #[test]
    fn render_resolves_the_schema_without_touching_the_disk() {
        // Resolving a schema read `ns-meta` on EVERY call: drawing five hundred
        // lines cost five hundred file opens — exactly the defect already
        // removed from the query path. It is checked by the fact that rendering
        // goes on working once the file is no longer on disk.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        assert_eq!(reader.render(msg, "ru").as_deref(), Some("мощность задана"));

        std::fs::remove_file(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        assert_eq!(
            reader.render(msg, "ru").as_deref(),
            Some("мощность задана"),
            "the schema must be resolved once rather than on every call"
        );
    }

    #[test]
    fn merge_is_ordered_across_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let keys: Vec<BootTime> = all.entries.iter().map(|e| e.at).collect();
        assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "the merge must give a global order by time"
        );
        // Both namespaces are present in the answer, interleaved.
        let namespaces: std::collections::HashSet<_> =
            all.entries.iter().map(|e| &*e.namespace).collect();
        assert!(namespaces.len() >= 2);
    }

    #[test]
    fn unknown_schema_leaves_raw_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        // Read without schemas — as a foreign build would.
        let reader = Reader::open_dump([dir.path()], &[]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();

        assert!(
            !result.entries.is_empty(),
            "the records are read all the same"
        );
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        match &msg.kind {
            EntryKind::Message { name, level, .. } => {
                assert!(name.is_none(), "without a schema the name is unknown");
                assert!(level.is_none(), "without a schema the level is unknown");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn time_range_narrows_result() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid.into()),
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(narrowed.entries.iter().all(|e| e.at >= mid));
        assert!(narrowed.entries.len() < all.entries.len());
    }

    #[test]
    fn foreign_store_segments_are_reported_not_silently_dropped() {
        let dir = tempfile::tempdir().unwrap();
        populate_legacy(dir.path());

        // Substitute the store metadata: the segments become "foreign".
        let meta_path = dir.path().join("store-meta");
        #[derive(serde::Serialize)]
        struct Meta {
            container_version: u8,
            store_id: u64,
        }
        std::fs::write(
            &meta_path,
            postcard::to_allocvec(&Meta {
                container_version: 1,
                store_id: 0xDEAD_BEEF,
            })
            .unwrap(),
        )
        .unwrap();

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "foreign segments must reach the damage list rather than vanish"
        );
        assert!(result.entries.is_empty());

        // An explicit permission reads them as they are.
        let reader = Reader::open_dump([dir.path()], &[schema()])
            .unwrap()
            .allow_foreign_segments();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!result.entries.is_empty());
    }

    #[test]
    fn corrupt_block_does_not_hide_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        populate_legacy(dir.path());

        // Damage the middle of one segment.
        let ch = dir.path().join("orc-radio-0").join("default");
        let seg = std::fs::read_dir(&ch)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all_at(&[0xFF; 16], 40).unwrap();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!result.is_complete(), "damage must be reported explicitly");
        // The other namespaces' data is unharmed.
        assert!(
            result
                .entries
                .iter()
                .any(|e| &*e.namespace == "orc-radio-1" || &*e.namespace == "apt-modem-0"),
            "corruption of one segment must not hide the others"
        );
    }

    #[test]
    fn damage_is_reported_even_when_the_walk_stops_early() {
        // Damage moved from a segment into the report only when the segment
        // closed, while the walk breaks off on `limit` mid-segment. An answer a
        // block had dropped out of declared itself complete — that is, lied in
        // exactly the field that exists for it.
        let dir = tempfile::tempdir().unwrap();
        populate_legacy(dir.path());

        let ch = dir.path().join("orc-radio-0").join("default");
        let seg = std::fs::read_dir(&ch)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        {
            // The segment's first block: its header begins right after the
            // segment header (32 bytes).
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all_at(&[0xFF; 16], 40).unwrap();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).limit(1))
            .unwrap();
        assert_eq!(result.entries.len(), 1, "the limit is honoured");
        assert!(
            result.truncated,
            "the walk broke off and the segment was not read to the end"
        );
        assert!(
            !result.is_complete(),
            "some data dropped out because of corruption — the answer is not complete"
        );

        // The same answer on the lazy path: a reader has to admit a loss when
        // the caller breaks off the walk too.
        let mut stream = reader.stream(&Query::new().order(Order::Oldest)).unwrap();
        let _first = stream.next().expect("there is at least one record");
        assert!(
            !stream.damaged().is_empty(),
            "the damage is visible without waiting for the end of the walk"
        );
    }

    #[test]
    fn utc_is_resolved_when_anchor_exists() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store
                .record_sync(
                    DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                    dduroc_engine::SyncSource::Gps,
                )
                .unwrap();
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let entry = &result.entries[0];
        let utc = entry
            .utc
            .expect("the anchor is retroactive: the event was written BEFORE the synchronization");
        assert!(
            (1_699_999_000_000..1_700_001_000_000).contains(&utc.timestamp_millis()),
            "the UTC is close to the synchronization point: {utc}"
        );
    }

    #[test]
    fn wall_clock_window_selects_the_same_records_as_the_relative_one() {
        // A synchronized device: one and the same window given by a wall clock
        // and by relative time has to yield one selection. Otherwise the bound
        // "from 12:00" would lie by exactly the conversion error.
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            store
                .record_sync(
                    DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                    dduroc_engine::SyncSource::Gps,
                )
                .unwrap();
            for i in 0..40u8 {
                ns.log_raw(EventId(1), &[i], None);
            }
            ns.sync().unwrap();
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert_eq!(all.entries.len(), 40);
        let mid = &all.entries[all.entries.len() / 2];
        let mid_utc = mid.utc.expect("there is an anchor");

        let by_wall = reader
            .query(&Query::new().order(Order::Oldest).since(mid_utc))
            .unwrap();
        let by_relative = reader
            .query(&Query::new().order(Order::Oldest).since(mid.at))
            .unwrap();

        assert!(by_wall.unanchored.is_empty(), "the run is synchronized");
        assert_eq!(
            by_wall.entries, by_relative.entries,
            "a wall-clock and a relative window must give one selection: \
             conversion by the anchor is exact to the microsecond"
        );
        assert!(by_wall.entries.iter().all(|e| e.at >= mid.at));
        assert!(by_wall.entries.len() < all.entries.len());

        // The upper bound, symmetrically.
        let until_wall = reader
            .query(&Query::new().order(Order::Oldest).until(mid_utc))
            .unwrap();
        let until_relative = reader
            .query(&Query::new().order(Order::Oldest).until(mid.at))
            .unwrap();
        assert_eq!(until_wall.entries, until_relative.entries);
        assert!(until_wall.entries.iter().all(|e| e.at <= mid.at));
        // Together the halves cover everything; the intersection is the records
        // exactly on the bound, and there may be more than one: the clock is
        // monotonic but not strictly increasing, and in a burst two events get
        // one microsecond.
        let on_edge = all.entries.iter().filter(|e| e.at == mid.at).count();
        assert_eq!(
            until_wall.entries.len() + by_wall.entries.len(),
            all.entries.len() + on_edge
        );
    }

    #[test]
    fn wall_clock_window_reports_runs_it_had_to_drop() {
        // A device with no synchronization. A query by the wall clock cannot
        // say whether its records fall in the window — and has to report that
        // the selection is incomplete rather than show emptiness.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let utc = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).since(utc))
            .unwrap();

        assert!(r.entries.is_empty(), "there is nothing to match with");
        assert_eq!(
            r.unanchored,
            vec![BootCounter(0)],
            "a run that dropped out must be named: silence would look like \
             \"the device wrote nothing\""
        );

        // A relative window works without a synchronization too — it depends on
        // nothing.
        let r = reader
            .query(
                &Query::new()
                    .order(Order::Oldest)
                    .since(BootTime::from_raw(0, 0)),
            )
            .unwrap();
        assert!(!r.entries.is_empty());
        assert!(r.unanchored.is_empty());
    }

    #[test]
    fn stream_reads_without_collecting_everything() {
        // This is what the stream exists for: the answer to a query without a
        // `limit` over a large store would not fit in memory, and `query` does
        // not allow breaking the walk off half way.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let q = Query::new().order(Order::Oldest);

        // The first five records — and the walk stops; the remaining segments
        // are not even read to the end.
        let head: Vec<Entry> = reader.stream(&q).unwrap().take(5).collect();
        assert_eq!(head.len(), 5);

        // The stream and the assembled answer have to match record for record:
        // otherwise "the same thing, but lazily" would not be true.
        let whole: Vec<Entry> = reader.stream(&q).unwrap().collect();
        let collected = reader.query(&q).unwrap();
        assert_eq!(whole, collected.entries);
        assert_eq!(&whole[..5], &head[..]);

        // The limit and the truncation flag work in the stream too.
        let mut limited = reader.stream(&q.clone().limit(3)).unwrap();
        assert_eq!(limited.by_ref().count(), 3);
        assert!(limited.truncated());
        assert_eq!(limited.yielded(), 3);
    }

    #[test]
    fn render_finds_the_schema_by_the_entry_itself() {
        // The caller no longer has to find the schema: passing a foreign one
        // would mean parsing the payload with another event's decoder — the text
        // would come out plausible and wrong.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();

        let msg = r
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        assert_eq!(reader.render(msg, "ru").as_deref(), Some("мощность задана"));
        assert_eq!(reader.render(msg, "en").as_deref(), Some("power set"));
        // An unknown language falls back to the first declared, not to a refusal.
        assert_eq!(reader.render(msg, "de").as_deref(), Some("power set"));

        let reader = reader
            .with_language_preferences(crate::LanguagePreferences::new(["de", "RU"]).unwrap());
        let rendered = reader.render_preferred(msg).unwrap();
        assert_eq!(rendered.text, "мощность задана");
        assert_eq!(rendered.language.as_deref(), Some("ru"));
        assert_eq!(rendered.preference_index, Some(1));
        let fallback = crate::LanguagePreferences::new(["de"]).unwrap();
        let rendered = reader.render_with_preferences(msg, &fallback).unwrap();
        assert_eq!(rendered.text, "power set");
        assert_eq!(rendered.language.as_deref(), Some("en"));
        assert_eq!(rendered.preference_index, None);
        assert_eq!(
            reader.render_preferred(msg).unwrap().text,
            "мощность задана"
        );
        let mut mismatch = msg.clone();
        mismatch.schema_identity = None;
        assert_eq!(reader.render_preferred(&mismatch), None);
        let mut free_text = msg.clone();
        free_text.kind = EntryKind::Text {
            level: Level::Info,
            target: "test".into(),
            text: "literal".into(),
        };
        assert_eq!(
            reader.render_preferred(&free_text),
            Some(RenderedMessage {
                text: "literal".into(),
                language: None,
                preference_index: None,
            })
        );

        // Offline readers own an exclusive backend lock; release it before reopening.
        drop(reader);
        // A reader without schemas has nothing to render with, and will not invent.
        let blind = Reader::open_dump([dir.path()], &[]).unwrap();
        assert_eq!(blind.render(msg, "ru"), None);
    }

    #[test]
    fn dump_without_epochs_file_still_names_its_runs() {
        // The dump was copied without `epochs.bin` — which happens when only the
        // directory is taken. The records are there, but not one of them has a
        // wall-clock time, and the runs that dropped out cannot be listed from the
        // epochs: the only source is the segment names.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        std::fs::remove_file(dir.path().join(dduroc_engine::epochs::EPOCHS_FILE)).unwrap();

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let utc = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).since(utc))
            .unwrap();

        assert!(r.entries.is_empty());
        assert_eq!(
            r.unanchored,
            vec![BootCounter(0)],
            "without the epoch registry a run is known only by a segment name"
        );

        // Without wall-clock bounds a dump reads as usual: relative time lies in
        // the files themselves.
        let r = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!r.entries.is_empty());
        assert!(r.entries.iter().all(|e| e.utc.is_none()));
        assert!(r.unanchored.is_empty());
    }
    #[test]
    fn repeated_offline_open_releases_the_lock_and_does_not_accumulate_files() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let mut samples = Vec::new();
        for _ in 0..24 {
            let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
            let result = reader.query(&Query::new().limit(1)).unwrap();
            assert_eq!(result.entries.len(), 1);
            drop(reader);
            let files: Vec<_> = std::fs::read_dir(dir.path().join("raft-engine"))
                .unwrap()
                .map(|e| e.unwrap().metadata().unwrap())
                .collect();
            samples.push((
                files.len(),
                files.iter().map(|m| m.blocks() * 512).sum::<u64>(),
            ));
        }
        let min_files = samples.iter().map(|s| s.0).min().unwrap();
        let max_files = samples.iter().map(|s| s.0).max().unwrap();
        let min_bytes = samples.iter().map(|s| s.1).min().unwrap();
        let max_bytes = samples.iter().map(|s| s.1).max().unwrap();
        assert!(
            max_files <= min_files + 1,
            "offline open leaked files: {samples:?}"
        );
        assert!(
            max_bytes <= min_bytes + 65536,
            "offline open leaked allocated blocks: {samples:?}"
        );
    }

    #[test]
    fn follow_drains_existing_tail_and_new_namespace_after_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
        let existing = store.namespace("existing", schema()).unwrap();
        existing.log_text(Level::Info, "test", "before", None);
        existing.sync().unwrap();
        let reader = Reader::of_store(&store);
        let mut follow = reader.follow(&Query::new().order(Order::Oldest)).unwrap();
        let Tail::Entry(initial) = follow.next(Duration::from_secs(1)) else {
            panic!("initial entry missing");
        };
        assert!(matches!(&initial.kind, EntryKind::Text { text, .. } if text == "before"));
        existing.log_text(Level::Info, "test", "tail", None);
        let new_namespace = store.namespace("new", schema()).unwrap();
        new_namespace.log_text(Level::Info, "test", "new", None);
        store.shutdown();
        let mut observed = Vec::new();
        loop {
            match follow.next(Duration::from_secs(1)) {
                Tail::Entry(entry) => match &entry.kind {
                    EntryKind::Text { text, .. } => {
                        observed.push((entry.namespace.to_string(), text.clone()))
                    }
                    other => panic!("unexpected entry: {other:?}"),
                },
                Tail::Ended => break,
                Tail::Idle => panic!("a stopped writer must not leave the follower idle"),
            }
        }
        observed.sort();
        assert_eq!(
            observed,
            [
                ("existing".into(), "tail".into()),
                ("new".into(), "new".into())
            ]
        );
    }

    #[test]
    fn follow_prunes_compacted_namespace_cursors_without_replaying_old_records() {
        use dduroc_engine::channel::ChannelConfig;
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(
            StoreConfig::new(dir.path())
                .with_budget_per_class(16 << 20)
                .channel(
                    StorageClass::Default,
                    ChannelConfig {
                        segment_bytes: 64 << 10,
                        block_max_bytes: 8 << 10,
                        compression: dduroc_format::Compression::None,
                        ..ChannelConfig::new(128 << 10)
                    },
                ),
        )
        .unwrap();
        let reader = Reader::of_store(&store);
        let mut follow = reader.follow(&Query::new().order(Order::Oldest)).unwrap();
        let text = "x".repeat(40 << 10);
        for n in 0..12 {
            let namespace = store.namespace(&format!("churn-{n:03}"), schema()).unwrap();
            namespace.log_text(Level::Info, "churn", text.as_str(), None);
            namespace.sync().unwrap();
            // Remove rate throttling from this focused lifecycle regression.
            follow.last_adopt = None;
            let entry = match follow.next(Duration::from_secs(2)) {
                Tail::Entry(entry) => entry,
                other => panic!("missing namespace {n}: {other:?}"),
            };
            assert_eq!(entry.namespace.as_ref(), format!("churn-{n:03}"));
            assert!(matches!(follow.next(Duration::ZERO), Tail::Idle));
        }
        follow.prune_backend_channels();
        let retained = store
            .read_backend()
            .catalog()
            .unwrap()
            .iter()
            .filter(|info| info.retained_bytes > 0)
            .count();
        assert!(
            retained < 12,
            "the fixture must really evict old namespaces"
        );
        assert!(
            follow.cursors.len() <= retained,
            "drained cursors outlived retained data"
        );
        assert!(
            follow.known.len() <= retained,
            "known namespace roster outlived retained data"
        );
        assert!(
            matches!(follow.next(Duration::ZERO), Tail::Idle),
            "pruning must not replay retained history"
        );
        store.shutdown();
    }
    #[test]
    fn presentation_schema_cache_is_bounded_under_namespace_churn() {
        let dir = tempfile::tempdir().unwrap();
        let store =
            Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
        let reader = Reader::of_store(&store);
        for index in 0..530 {
            let name = format!("cache-{index:04}");
            let namespace = store.namespace(&name, schema()).unwrap();
            namespace.log_raw(EventId(1), &[1], None);
            namespace.sync().unwrap();
            assert!(reader.schema_of(&name).is_some());
        }
        assert!(reader.schema_cache.read().unwrap().len() <= 512);
        store.shutdown();
    }
}
