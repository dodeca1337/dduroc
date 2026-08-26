//! A query: what to read and in what order.
//!
//! # Time in a query's bounds
//!
//! The bounds are given by one type — [`Timestamp`] — and it is either relative
//! ([`BootTime`]: a run plus microseconds since it started) or wall-clock
//! (`DateTime<Utc>`). This used to be `boot: Option<u32>` and `from/to:
//! Option<Micros>` separately, and the meaning of the pair depended on whether
//! `boot` was given: without it the microseconds applied to the scale of
//! **every** run, so "the first ten seconds of any run" and "ten seconds of a
//! particular run" were expressed the same way.
//!
//! Wall-clock bounds are comparable with records only through a
//! synchronization anchor (see [`dduroc_engine::epochs`]), so before scanning
//! they are converted into the scale of each run — [`Query::resolve`]. A run
//! with no anchor cannot be matched against wall-clock time at all; it drops
//! out of the selection, and that is reported explicitly
//! ([`Resolution::unanchored`]) rather than silently.

use dduroc_engine::epochs::{Epochs, RunOffset};
use dduroc_engine::schema::StorageClass;
use dduroc_format::{BootCounter, BootTime, EventId, Level, Micros, SpanId};
use std::collections::{BTreeMap, BTreeSet, HashSet};

/// A moment in time in a query's bounds.
///
/// Two scales rather than two fields: a device without an RTC always has
/// relative time and wall-clock time only after a synchronization, and one
/// must not be substituted for the other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Timestamp {
    /// Relative time: a run and microseconds since it started.
    Boot(BootTime),
    /// Wall-clock time. The records of a run whose boot was never synchronized
    /// cannot be matched against it — they will drop out of the selection.
    Utc(chrono::DateTime<chrono::Utc>),
}

impl Timestamp {
    /// Whether the scale is the relative one.
    pub const fn is_relative(self) -> bool {
        matches!(self, Timestamp::Boot(_))
    }

    /// The moment one microsecond earlier — a "strictly before" bound.
    pub fn just_before(self) -> Self {
        match self {
            Timestamp::Boot(bt) => {
                Timestamp::Boot(BootTime::new(bt.boot, Micros(bt.at.0.saturating_sub(1))))
            }
            Timestamp::Utc(t) => Timestamp::Utc(t - chrono::TimeDelta::microseconds(1)),
        }
    }
}

impl From<BootTime> for Timestamp {
    fn from(at: BootTime) -> Self {
        Timestamp::Boot(at)
    }
}

impl From<chrono::DateTime<chrono::Utc>> for Timestamp {
    fn from(t: chrono::DateTime<chrono::Utc>) -> Self {
        Timestamp::Utc(t)
    }
}

impl std::fmt::Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Timestamp::Boot(at) => write!(f, "{at}"),
            Timestamp::Utc(t) => write!(f, "{}", t.to_rfc3339()),
        }
    }
}

/// The bounds of a window in one run's relative scale. Both inclusive; `None`
/// means there is no bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RunBounds {
    pub from: Option<Micros>,
    pub to: Option<Micros>,
}

impl RunBounds {
    pub fn contains(&self, at: Micros) -> bool {
        self.from.is_none_or(|f| at >= f) && self.to.is_none_or(|t| at <= t)
    }
}

/// A query's bounds brought to something comparable with records.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Bounds {
    /// There are no time bounds.
    #[default]
    All,
    /// Relative bounds. Compared lexicographically by (run, µs) — which is the
    /// chronological order, because the run counter grows.
    Relative {
        from: Option<BootTime>,
        to: Option<BootTime>,
    },
    /// Wall-clock bounds converted by the anchors into each run's scale.
    Wall {
        /// The runs that fell in the window, with bounds in their own scale.
        runs: BTreeMap<u32, RunBounds>,
        /// The runs that have an anchor. Needed to tell "this run is not in the
        /// window" from "there is nothing to compare it with": the second is
        /// about data that exists but stayed out of frame, and that has to be
        /// said.
        anchored: BTreeSet<u32>,
    },
}

/// How a window falls on one run's records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fit {
    /// The run is wanted; here are its bounds.
    In(RunBounds),
    /// The run is entirely outside the window.
    Outside,
    /// The window is given in wall-clock time and the run has no anchor:
    /// whether its records fall in the window is unknown, and they drop out of
    /// the selection.
    Unanchored,
}

impl Bounds {
    /// How a window falls on a run's records.
    pub fn fit(&self, boot: BootCounter) -> Fit {
        match self {
            Bounds::All => Fit::In(RunBounds::default()),
            Bounds::Relative { from, to } => {
                // The bounds of neighbouring runs turn into "this whole run is
                // inside the window" or "it is not here at all": microseconds
                // of different runs cannot be compared.
                let from = match from {
                    Some(b) if b.boot > boot => return Fit::Outside,
                    Some(b) if b.boot == boot => Some(b.at),
                    _ => None,
                };
                let to = match to {
                    Some(b) if b.boot < boot => return Fit::Outside,
                    Some(b) if b.boot == boot => Some(b.at),
                    _ => None,
                };
                Fit::In(RunBounds { from, to })
            }
            Bounds::Wall { runs, anchored } => match runs.get(&boot.0) {
                Some(b) => Fit::In(*b),
                None if anchored.contains(&boot.0) => Fit::Outside,
                // This also covers a run that is not in `epochs.bin` at all: a
                // dump may have been copied without it, and then its records
                // have no wall-clock time — exactly as without a
                // synchronization.
                None => Fit::Unanchored,
            },
        }
    }

    /// What is allowed for this run's records. `None` means the run is not
    /// wanted at all and its segments need not be opened.
    pub fn for_boot(&self, boot: BootCounter) -> Option<RunBounds> {
        match self.fit(boot) {
            Fit::In(b) => Some(b),
            Fit::Outside | Fit::Unanchored => None,
        }
    }

    /// Whether a moment falls in the window.
    pub fn contains(&self, at: BootTime) -> bool {
        self.for_boot(at.boot).is_some_and(|b| b.contains(at.at))
    }
}

/// The resolved bounds together with what had to be dropped because of them.
#[derive(Debug, Clone, Default)]
pub struct Resolution {
    pub bounds: Bounds,
    /// The runs **from the epoch registry** that a wall-clock window cannot
    /// match: they have no anchor.
    ///
    /// This is an answer about the registry, not about the selection: a run may
    /// turn out to have no segments in the channels asked for. What really
    /// dropped out of the answer the reader collects from the segments — see
    /// `QueryResult::unanchored`.
    pub unanchored: Vec<BootCounter>,
}

/// The order records come back in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Order {
    /// Oldest to newest.
    Oldest,
    /// Newest to oldest — what an interface wants by default.
    #[default]
    Newest,
}

/// The choice of namespaces.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum NsSelect {
    /// All of them.
    #[default]
    All,
    /// By exact name.
    Names(Vec<String>),
    /// A group: the namespaces sharing a prefix (`orc-` for orchestrators,
    /// `apt-` for adapters).
    Group(String),
}

impl NsSelect {
    pub fn matches(&self, name: &str) -> bool {
        match self {
            NsSelect::All => true,
            NsSelect::Names(names) => names.iter().any(|n| n == name),
            // One rule for reading and for writing: "the orchestrators'
            // journals" and "the orchestrators' settings" have to denote one
            // set.
            NsSelect::Group(prefix) => dduroc_engine::store::in_group(prefix, name),
        }
    }
}

/// A filter on content.
///
/// Levels and tags are static properties of types, so filtering by them
/// requires no reading of records: it reduces to computing a set of
/// identifiers from the schema **before** any scanning.
///
/// A content filter (tags, types, names) applies to the records that have such
/// a property and **excludes** the records that cannot satisfy it: free text
/// and spans carry neither tags nor an event type and drop out under such a
/// filter. Messages and samples have tags (metrics have their own); only
/// messages have types. `min_level` is different: messages and text have a
/// level, while telemetry and spans are outside the level scale and are not
/// filtered out by it.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// The minimum level (inclusive).
    pub min_level: Option<Level>,
    /// The tags required: a record has to carry at least one of them.
    pub any_tags: Vec<String>,
    /// Particular event types.
    pub events: Option<HashSet<EventId>>,
    /// Event names — resolved from the schema.
    pub event_names: Vec<String>,
    /// Only records attached to these spans.
    pub spans: Option<HashSet<SpanId>>,
    /// Which kinds of record are wanted.
    pub kinds: KindFilter,
}

/// Which kinds of record to include.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KindFilter {
    pub messages: bool,
    pub spans: bool,
    pub samples: bool,
    pub text: bool,
}

impl Default for KindFilter {
    fn default() -> Self {
        Self {
            messages: true,
            spans: true,
            samples: true,
            text: true,
        }
    }
}

impl KindFilter {
    /// Messages and free text only — a "journal" in the usual sense.
    pub const LOGS: Self = Self {
        messages: true,
        spans: false,
        samples: false,
        text: true,
    };

    /// Telemetry only.
    pub const TELEMETRY: Self = Self {
        messages: false,
        spans: false,
        samples: true,
        text: false,
    };

    /// Spans only.
    pub const SPANS: Self = Self {
        messages: false,
        spans: true,
        samples: false,
        text: false,
    };
}

/// A query.
///
/// Built as a chain: every method returns a new query rather than mutating the
/// existing one. `q.limit(500);` as a standalone expression would be lost
/// whole.
#[derive(Debug, Clone, Default)]
#[must_use = "a query is built as a chain: a method's result is the configured query"]
pub struct Query {
    pub namespaces: NsSelect,
    /// The channels, by storage class. Empty means all of them.
    pub channels: Vec<StorageClass>,
    /// A restriction to one software run: its segments only.
    pub boot: Option<BootCounter>,
    /// The window's lower bound, inclusive.
    pub from: Option<Timestamp>,
    /// The window's upper bound, inclusive.
    pub to: Option<Timestamp>,
    pub filter: Filter,
    pub order: Order,
    /// The maximum number of records in the answer.
    pub limit: Option<usize>,
    /// Carry states through to the window's left edge.
    ///
    /// States are written **on change** rather than periodically — otherwise
    /// there is no point to them. So a `from..to` window may contain not a
    /// single sample of a series that held one value the whole time, and the
    /// state band on a chart would stay empty although the state was known.
    ///
    /// With this flag the answer also carries the last sample of every state
    /// series taken **before** `from` — separately from `entries`, so as not to
    /// break the promise that everything in the answer lies inside the range.
    pub seed_states: bool,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn namespaces(mut self, select: NsSelect) -> Self {
        self.namespaces = select;
        self
    }

    pub fn group(mut self, prefix: impl Into<String>) -> Self {
        self.namespaces = NsSelect::Group(prefix.into());
        self
    }

    /// Read only the channels of this storage class.
    ///
    /// The class is an enum rather than a name: a misspelled channel is
    /// unrepresentable.
    pub fn channel(mut self, class: StorageClass) -> Self {
        self.channels.push(class);
        self
    }

    /// The time window `from..=to` — a companion to [`Query::boot_window`], but
    /// with bounds in either scale. The scales may differ, only mixing them is
    /// worth doing knowingly: a wall-clock bound is converted by an anchor, and
    /// a run without one drops out of the selection entirely.
    pub fn time_window(mut self, from: impl Into<Timestamp>, to: impl Into<Timestamp>) -> Self {
        self.from = Some(from.into());
        self.to = Some(to.into());
        self
    }

    /// The lower bound only.
    pub fn since(mut self, from: impl Into<Timestamp>) -> Self {
        self.from = Some(from.into());
        self
    }

    /// The upper bound only.
    pub fn until(mut self, to: impl Into<Timestamp>) -> Self {
        self.to = Some(to.into());
        self
    }

    /// A window inside one run — the ordinary case for relative time.
    pub fn boot_window(mut self, boot: impl Into<BootCounter>, from: Micros, to: Micros) -> Self {
        let boot = boot.into();
        self.boot = Some(boot);
        self.from = Some(Timestamp::Boot(BootTime::new(boot, from)));
        self.to = Some(Timestamp::Boot(BootTime::new(boot, to)));
        self
    }

    pub fn boot(mut self, boot: impl Into<BootCounter>) -> Self {
        self.boot = Some(boot.into());
        self
    }

    pub fn min_level(mut self, level: Level) -> Self {
        self.filter.min_level = Some(level);
        self
    }

    /// Only records with this tag. Several calls mean "at least one of".
    ///
    /// Messages and samples carry tags; text and spans drop out under such a
    /// filter — see [`Filter`].
    pub fn any_tag(mut self, tag: impl Into<String>) -> Self {
        self.filter.any_tags.push(tag.into());
        self
    }

    /// Only events of this type. Several calls mean "any of those named".
    pub fn event(mut self, id: EventId) -> Self {
        self.filter
            .events
            .get_or_insert_with(HashSet::new)
            .insert(id);
        self
    }

    /// Only events with this name (resolved from the schema).
    pub fn event_name(mut self, name: impl Into<String>) -> Self {
        self.filter.event_names.push(name.into());
        self
    }

    /// Only records attached to this span.
    pub fn span(mut self, id: SpanId) -> Self {
        self.filter
            .spans
            .get_or_insert_with(HashSet::new)
            .insert(id);
        self
    }

    pub fn kinds(mut self, kinds: KindFilter) -> Self {
        self.filter.kinds = kinds;
        self
    }

    pub fn order(mut self, order: Order) -> Self {
        self.order = order;
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Carry states through to the window's left edge — see
    /// [`Query::seed_states`].
    pub fn with_state_seed(mut self) -> Self {
        self.seed_states = true;
        self
    }

    /// Bring the bounds to something comparable with records.
    ///
    /// While both bounds are relative, the epochs are not needed at all — the
    /// comparison goes by (run, µs). The moment a wall-clock bound appears, the
    /// window has to be broken down by run: each has its own scale and its own
    /// anchor.
    pub fn resolve(&self, epochs: &Epochs) -> Resolution {
        let relative = self.from.is_none_or(Timestamp::is_relative)
            && self.to.is_none_or(Timestamp::is_relative);
        if relative {
            let bounds = match (self.from, self.to) {
                (None, None) => Bounds::All,
                (from, to) => Bounds::Relative {
                    from: from.and_then(boot_time),
                    to: to.and_then(boot_time),
                },
            };
            return Resolution {
                bounds,
                unanchored: Vec::new(),
            };
        }

        let mut runs = BTreeMap::new();
        let mut anchored = BTreeSet::new();
        let mut unanchored = Vec::new();
        for run in epochs.runs() {
            match run_bounds(run.boot_counter, self.from, self.to, epochs) {
                Fit::In(b) => {
                    anchored.insert(run.boot_counter);
                    runs.insert(run.boot_counter, b);
                }
                Fit::Outside => {
                    anchored.insert(run.boot_counter);
                }
                Fit::Unanchored => unanchored.push(BootCounter(run.boot_counter)),
            }
        }
        Resolution {
            bounds: Bounds::Wall { runs, anchored },
            unanchored,
        }
    }
}

fn boot_time(t: Timestamp) -> Option<BootTime> {
    match t {
        Timestamp::Boot(at) => Some(at),
        Timestamp::Utc(_) => None,
    }
}

/// Convert a window's bounds into one run's scale.
fn run_bounds(boot: u32, from: Option<Timestamp>, to: Option<Timestamp>, epochs: &Epochs) -> Fit {
    let lower = match from {
        None => None,
        // A run earlier than the bound is all behind it; later, all inside.
        Some(Timestamp::Boot(b)) if b.boot.0 > boot => return Fit::Outside,
        Some(Timestamp::Boot(b)) if b.boot.0 == boot => Some(b.at),
        Some(Timestamp::Boot(_)) => None,
        Some(Timestamp::Utc(t)) => match epochs.from_utc(boot, t) {
            RunOffset::Unanchored => return Fit::Unanchored,
            // The run started later than the lower bound — there is nothing to
            // restrict.
            RunOffset::BeforeStart => None,
            RunOffset::At(m) => Some(m),
        },
    };
    let upper = match to {
        None => None,
        Some(Timestamp::Boot(b)) if b.boot.0 < boot => return Fit::Outside,
        Some(Timestamp::Boot(b)) if b.boot.0 == boot => Some(b.at),
        Some(Timestamp::Boot(_)) => None,
        Some(Timestamp::Utc(t)) => match epochs.from_utc(boot, t) {
            RunOffset::Unanchored => return Fit::Unanchored,
            // The run started later than the upper bound — it is not here.
            RunOffset::BeforeStart => return Fit::Outside,
            RunOffset::At(m) => Some(m),
        },
    };
    Fit::In(RunBounds {
        from: lower,
        to: upper,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn group_selects_by_prefix() {
        let g = NsSelect::Group("orc-".to_owned());
        assert!(g.matches("orc-radio-0"));
        assert!(g.matches("orc-radio-3"));
        assert!(!g.matches("apt-modem-1"));

        let names = NsSelect::Names(vec!["orc-radio-0".to_owned()]);
        assert!(names.matches("orc-radio-0"));
        assert!(!names.matches("orc-radio-1"));

        assert!(NsSelect::All.matches("anything at all"));
    }

    #[test]
    fn relative_range_needs_no_epochs() {
        let epochs = Epochs::default();
        let q = Query::new().boot_window(0u32, Micros(100), Micros(200));
        let r = q.resolve(&epochs);
        assert!(r.unanchored.is_empty());

        let b = |at| BootTime::from_raw(0, at);
        assert!(!r.bounds.contains(b(99)));
        assert!(r.bounds.contains(b(100)));
        assert!(r.bounds.contains(b(150)));
        assert!(r.bounds.contains(b(200)));
        assert!(!r.bounds.contains(b(201)));

        // With no bounds everything passes, runs the epochs know nothing of
        // included.
        let all = Query::new().resolve(&epochs);
        assert_eq!(all.bounds, Bounds::All);
        assert!(all.bounds.contains(BootTime::from_raw(9, u64::MAX)));
    }

    #[test]
    fn relative_bounds_span_runs_lexicographically() {
        // A bound lives in the scale of its own run, so "from the middle of run
        // 1" means: all of run 0 is behind, run 1 is cut in the middle, run 2
        // is inside entirely. This used to be expressed by `boot` plus `Micros`
        // separately, and the microseconds applied to every run.
        let q = Query::new().since(BootTime::from_raw(1, 500));
        let bounds = q.resolve(&Epochs::default()).bounds;

        assert_eq!(
            bounds.for_boot(BootCounter(0)),
            None,
            "run 0 is entirely behind"
        );
        assert_eq!(
            bounds.for_boot(BootCounter(1)),
            Some(RunBounds {
                from: Some(Micros(500)),
                to: None
            })
        );
        assert_eq!(
            bounds.for_boot(BootCounter(2)),
            Some(RunBounds::default()),
            "run 2 is inside the window entirely"
        );

        assert!(!bounds.contains(BootTime::from_raw(0, u64::MAX)));
        assert!(bounds.contains(BootTime::from_raw(1, 500)));
        assert!(bounds.contains(BootTime::from_raw(2, 0)));

        // The upper bound, symmetrically.
        let bounds = Query::new()
            .until(BootTime::from_raw(1, 500))
            .resolve(&Epochs::default())
            .bounds;
        assert_eq!(bounds.for_boot(BootCounter(2)), None);
        assert!(bounds.contains(BootTime::from_raw(0, u64::MAX)));
        assert!(!bounds.contains(BootTime::from_raw(1, 501)));
    }

    #[test]
    fn wall_bounds_drop_unanchored_runs_and_say_so() {
        use dduroc_engine::epochs::{HwBoot, Run};

        // Two runs of one hardware boot plus a run of a boot with no anchor.
        let anchor_ms = 1_700_000_000_000;
        let epochs = Epochs {
            runs: vec![
                Run {
                    boot_counter: 0,
                    hw_boot_id: 0,
                    boottime_at_init_us: 1_000_000,
                },
                Run {
                    boot_counter: 1,
                    hw_boot_id: 1,
                    boottime_at_init_us: 2_000_000,
                },
            ],
            hw_boots: vec![
                HwBoot {
                    hw_boot_id: 0,
                    kernel_boot_id: [0; 16],
                    utc_anchor_ms: Some(anchor_ms),
                    anchor_source: None,
                    anchor_captured_us: None,
                },
                HwBoot {
                    hw_boot_id: 1,
                    kernel_boot_id: [1; 16],
                    utc_anchor_ms: None,
                    anchor_source: None,
                    anchor_captured_us: None,
                },
            ],
        };

        let utc = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).unwrap();
        // The window: from the 3rd to the 5th second of the first boot's
        // BOOTTIME.
        let r = Query::new()
            .time_window(utc(anchor_ms + 3_000), utc(anchor_ms + 5_000))
            .resolve(&epochs);

        assert_eq!(
            r.bounds.for_boot(BootCounter(0)),
            Some(RunBounds {
                from: Some(Micros(2_000_000)), // 3 s of BOOTTIME minus the run's 1 s start
                to: Some(Micros(4_000_000)),
            })
        );
        assert_eq!(
            r.bounds.for_boot(BootCounter(1)),
            None,
            "a run with no anchor cannot be matched against wall-clock time"
        );
        assert_eq!(
            r.unanchored,
            vec![BootCounter(1)],
            "a run that dropped out must be named"
        );

        // A run the epochs know nothing of is absent under wall-clock bounds
        // too: there is nowhere for it to get an anchor.
        assert_eq!(r.bounds.for_boot(BootCounter(42)), None);
    }

    #[test]
    fn wall_lower_bound_before_run_start_keeps_whole_run() {
        use dduroc_engine::epochs::{HwBoot, Run};
        let anchor_ms = 1_700_000_000_000;
        let epochs = Epochs {
            runs: vec![Run {
                boot_counter: 0,
                hw_boot_id: 0,
                boottime_at_init_us: 10_000_000,
            }],
            hw_boots: vec![HwBoot {
                hw_boot_id: 0,
                kernel_boot_id: [0; 16],
                utc_anchor_ms: Some(anchor_ms),
                anchor_source: None,
                anchor_captured_us: None,
            }],
        };
        let utc = |ms: i64| chrono::DateTime::from_timestamp_millis(ms).unwrap();

        // A lower bound earlier than the run started: there is nothing to
        // restrict.
        let r = Query::new().since(utc(anchor_ms + 1_000)).resolve(&epochs);
        assert_eq!(
            r.bounds.for_boot(BootCounter(0)),
            Some(RunBounds::default())
        );

        // An upper bound earlier than the start: the run is not in the window
        // at all.
        let r = Query::new().until(utc(anchor_ms + 1_000)).resolve(&epochs);
        assert_eq!(r.bounds.for_boot(BootCounter(0)), None);
        assert!(
            r.unanchored.is_empty(),
            "there is an anchor; it is not the problem"
        );
    }

    #[test]
    fn just_before_steps_one_microsecond_in_both_scales() {
        let t = Timestamp::Boot(BootTime::from_raw(2, 100));
        assert_eq!(
            t.just_before(),
            Timestamp::Boot(BootTime::from_raw(2, 99)),
            "the run does not change: the scale is the same"
        );
        // At zero there is no step back — there is nothing to subtract from.
        assert_eq!(
            Timestamp::Boot(BootTime::from_raw(2, 0)).just_before(),
            Timestamp::Boot(BootTime::from_raw(2, 0))
        );

        let utc = chrono::DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let Timestamp::Utc(back) = Timestamp::Utc(utc).just_before() else {
            panic!("the scale must not change");
        };
        assert_eq!(back.timestamp_micros(), 1_699_999_999_999_999);
    }

    #[test]
    fn kind_presets() {
        // The values are constants, so whole structs are compared: that keeps
        // the check from degenerating into a tautology for the compiler.
        assert_eq!(
            KindFilter::LOGS,
            KindFilter {
                messages: true,
                spans: false,
                samples: false,
                text: true
            }
        );
        assert_eq!(
            KindFilter::TELEMETRY,
            KindFilter {
                messages: false,
                spans: false,
                samples: true,
                text: false
            }
        );
        assert_eq!(
            KindFilter::SPANS,
            KindFilter {
                messages: false,
                spans: true,
                samples: false,
                text: false
            }
        );
        assert_eq!(
            KindFilter::default(),
            KindFilter {
                messages: true,
                spans: true,
                samples: true,
                text: true
            }
        );
    }
}
