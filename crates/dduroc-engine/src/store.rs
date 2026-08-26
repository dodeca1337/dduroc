//! The store: the root, the lock, the epochs and the writer.

use crate::channel::{ChannelConfig, ChannelOverride, validate_component};
use crate::clock::{Clock, boottime_us};
use crate::epochs::{EpochStore, SyncSource};
use crate::error::{Error, IoContext, Result};
use crate::fsutil;
use crate::namespace::{Namespace, NsMeta};
use crate::schema::{Schema, StorageClass};
use crate::staged::{DropCounters, NsId};
use crate::stats::{Counters, Stats};
use crate::writer::{ChannelSpec, GroupBudget, NsSetup, QueueSizes, Writer};
use chrono::{DateTime, Utc};
use dduroc_format::BootTime;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

/// The name of the lock file in the store root.
const LOCK_FILE: &str = ".lock";
/// The name of the store metadata file.
const STORE_META: &str = "store-meta";

/// The store's metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreMeta {
    /// The container format version.
    pub container_version: u8,
    /// The store's identity. Stamped into every segment so that files copied
    /// from another device do not blend with the local ones: they have their
    /// own run numbering and their own anchoring to time.
    pub store_id: u64,
}

/// The store's settings.
///
/// Built as a chain: every method returns the settings rather than mutating
/// them in place. `config.with_budget_per_class(n);` as a standalone
/// expression would do nothing.
#[derive(Debug, Clone)]
#[must_use = "the settings are built as a chain: a method's result is the settings"]
pub struct StoreConfig {
    pub root: PathBuf,
    /// The configurations of the storage classes. A class not named here gets
    /// the defaults with a budget of [`StoreConfig::default_budget_bytes`].
    ///
    /// The key is [`StorageClass`], an enum: a misspelled class is
    /// unrepresentable, and a channel's directory name is derived from the
    /// class — there is no second source of that name.
    pub channels: HashMap<StorageClass, ChannelConfig>,
    /// The default budget **per class** — for classes not named in
    /// [`StoreConfig::channels`].
    ///
    /// A class budget is shared across the whole store: "all telemetry gets
    /// this much". The channels of every namespace of the class draw on it, and
    /// when it is exceeded the class's oldest segment is evicted whoever's
    /// namespace it lies in. The sum of the class budgets is the occupancy
    /// ceiling; there is no separate "store ceiling" knob — with classes on
    /// different media (see [`ChannelConfig::custom_root`]) a shared ceiling
    /// would mean nothing.
    ///
    /// A class ceiling cannot be smaller than what the active segments hold.
    /// They hold their reserve window, which grows along with what was written,
    /// rather than `segment_bytes` — so how many channels can write at once is
    /// set by how much they wrote. An attempt to exceed the ceiling is visible
    /// in [`crate::stats::Stats::budget_overruns`].
    pub default_budget_bytes: u64,
    /// The write queue capacities. Allocated whole when the store is opened.
    pub queues: QueueSizes,
    /// The ceiling on the total bytes writing channels hold in memory for
    /// blocks. `None` means there is no ceiling, and that is the default.
    ///
    /// The memory per channel is the active block buffer and its serialized
    /// copy; they grow to the largest block that passed through and are given
    /// back once the channel goes quiet. Only a handful of channels write at
    /// any moment, and usually that is enough. The ceiling is for where "a
    /// handful" stops being true: a class with hundreds of channels writing at
    /// once at 64 KiB per block is tens of megabytes, and armv7 may not have
    /// them.
    ///
    /// Not a budget: a budget is about space on the medium and belongs to a
    /// class, while this ceiling is about RAM and belongs to the process. It is
    /// honoured by freeing the buffers of the largest holders; an unmeetable
    /// ceiling is announced by the [`crate::stats::Stats::buffer_overruns`]
    /// counter rather than by discarded records.
    pub buffer_ceiling_bytes: Option<u64>,
    /// The policies of namespace groups: a name prefix and how the group
    /// differs from the shared class settings.
    ///
    /// A list rather than a map: declaration order decides nothing — the
    /// longest matching prefix wins — and a device's settings must not depend
    /// on the order a hash table happens to be walked in.
    pub groups: Vec<(String, GroupPolicy)>,
}

impl StoreConfig {
    /// Settings with a shared budget for every class.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            channels: HashMap::new(),
            default_budget_bytes: 64 * 1024 * 1024,
            queues: QueueSizes::default(),
            buffer_ceiling_bytes: None,
            groups: Vec::new(),
        }
    }

    /// Bound the total block buffers of the writing channels.
    ///
    /// See [`StoreConfig::buffer_ceiling_bytes`].
    pub fn with_buffer_ceiling(mut self, bytes: u64) -> Self {
        self.buffer_ceiling_bytes = Some(bytes);
        self
    }

    /// The default class budget — see [`StoreConfig::default_budget_bytes`].
    ///
    /// Every class not named explicitly in [`StoreConfig::channel`] gets this
    /// much.
    pub fn with_budget_per_class(mut self, bytes: u64) -> Self {
        self.default_budget_bytes = bytes;
        self
    }

    /// Set the write queue capacities.
    ///
    /// A smaller queue saves memory but starts losing records sooner on bursts;
    /// a larger one survives a burst but puts off the moment the disk falling
    /// behind becomes noticeable.
    pub fn with_queues(mut self, queues: QueueSizes) -> Self {
        self.queues = queues;
        self
    }

    /// Set the channel settings of the given storage class.
    pub fn channel(mut self, class: StorageClass, config: ChannelConfig) -> Self {
        self.channels.insert(class, config);
        self
    }

    /// Set the policy of a namespace group — those whose names begin with
    /// `prefix`.
    ///
    /// A group here and a group in a read query (`Query::group`) are the same
    /// set: the selection rule is shared ([`in_group`]). Otherwise "the
    /// orchestrators' journals" and "the orchestrators' settings" would denote
    /// different things.
    ///
    /// When several prefixes match, the longest wins: `orc-radio-` refines
    /// `orc-` rather than arguing with it. Namespace settings given at open
    /// time (the quota) beat the group's: they are more specific.
    pub fn group(mut self, prefix: impl Into<String>, policy: GroupPolicy) -> Self {
        self.groups.push((prefix.into(), policy));
        self
    }

    /// The policies a namespace belongs to, from the general to the specific.
    ///
    /// Several may match: `orc-radio-` **refines** `orc-` rather than arguing
    /// with it — and they lay over one another the way a group lays over a
    /// class. Otherwise refining one setting would silently remove all the
    /// others the general group set: the quota, the compression, the intervals.
    fn policies_for<'a>(&'a self, namespace: &'a str) -> impl Iterator<Item = &'a GroupPolicy> {
        let mut matched: Vec<&(String, GroupPolicy)> = self
            .groups
            .iter()
            .filter(|(prefix, _)| in_group(prefix, namespace))
            .collect();
        // From short to long: the last layer is the most specific. Length is
        // unambiguous — two different prefixes of the same length cannot both
        // begin one name.
        matched.sort_by_key(|(prefix, _)| prefix.len());
        matched.into_iter().map(|(_, policy)| policy)
    }

    /// The channel settings: the class, with the namespace's group laid over
    /// it.
    ///
    /// `namespace: None` gives the class's own settings, refined by nobody.
    /// Those, not the group's, answer for the budget and the medium: both are
    /// shared by the whole class, and a group does not set them (see
    /// [`ChannelOverride`]).
    fn config_for(&self, namespace: Option<&str>, class: StorageClass) -> ChannelConfig {
        let mut config = match self.channels.get(&class) {
            Some(c) => c.clone(),
            None if class == StorageClass::Critical => {
                ChannelConfig::critical(self.default_budget_bytes)
            }
            None => ChannelConfig::new(self.default_budget_bytes),
        };
        if let Some(name) = namespace {
            for policy in self.policies_for(name) {
                if let Some(over) = policy.channels.get(&class) {
                    over.apply_to(&mut config);
                }
            }
        }
        config
    }

    /// The quota a namespace gets from its groups: the most specific of those
    /// naming one wins.
    fn group_quota(&self, namespace: &str, class: StorageClass) -> Option<u64> {
        self.policies_for(namespace)
            .filter_map(|p| p.quota.get(class))
            .last()
    }

    /// Check everything the application set.
    ///
    /// Channel settings arrive from outside and used to be checked nowhere: a
    /// budget smaller than two segments, or a block the size of a segment,
    /// reached the writer as they were and broke rotation on a device already
    /// in service. Refusing at open time is the only moment when that is still
    /// fixable.
    ///
    /// **Synthesized** configurations are checked too — the ones a class gets when
    /// the application set nothing for it: a `default_budget_bytes` smaller than
    /// two segments (16 MiB with the defaults) yields a class whose eviction would
    /// eat the only segment right after it was sealed.
    fn validate(&self) -> Result<()> {
        // Immediate syncing is the definition of the critical class, not a
        // setting: a channel the application is prepared to wait in a queue for
        // has no right to fall behind the medium. Overriding the interval
        // silently is not an option — the operator would believe the setting
        // was in force.
        if self.config_for(None, StorageClass::Critical).sync_interval != std::time::Duration::ZERO
        {
            return Err(Error::BadChannel {
                class: StorageClass::Critical,
                namespace: None,
                reason: "the critical channel syncs at once — that is the point of \
                         it; configure it from ChannelConfig::critical",
            });
        }

        for class in StorageClass::ALL {
            let config = self.config_for(None, class);
            config.validate(class)?;
        }

        // A ceiling below four blocks is unmeetable by construction: one
        // writing channel holds about three — the accumulator's body, the
        // compression output and the serialized copy — and the block-closing
        // threshold is checked AFTER a record is added, so each of them
        // overshoots by one record. Such a ceiling would bound nothing while
        // driving the allocator on every turn of the loop and counting
        // unmeetability.
        if let Some(ceiling) = self.buffer_ceiling_bytes {
            let block = StorageClass::ALL
                .iter()
                .map(|&c| self.config_for(None, c).block_max_bytes as u64)
                .chain(self.groups.iter().flat_map(|(_, p)| {
                    p.channels
                        .values()
                        .filter_map(|o| o.block_max_bytes.map(|b| b as u64))
                }))
                .max()
                .unwrap_or(0);
            if ceiling < block.saturating_mul(4) {
                return Err(Error::BadStore {
                    setting: "buffer_ceiling_bytes",
                    reason: "the memory ceiling is below four blocks — that is what one \
                             writing channel holds, and such a ceiling would only drive the allocator",
                });
            }
        }

        // Groups are checked in exactly the same way and by the same rules: a
        // setting unfit for a class does not become fit by being given to a
        // group. They have to be checked separately — a group's configuration
        // applies to a name, and there are no names yet when the store is
        // opened.
        for (i, (prefix, policy)) in self.groups.iter().enumerate() {
            let bad = |reason| Error::BadGroup {
                prefix: prefix.clone(),
                reason,
            };
            if prefix.is_empty() {
                return Err(bad(
                    "an empty prefix matches every name — that is the store's settings, \
                     not a group's",
                ));
            }
            if crate::channel::validate_component(prefix).is_err() {
                return Err(bad(
                    "the prefix cannot begin a namespace name: ASCII letters, digits, \
                     '-', '_' and '.' are allowed",
                ));
            }
            if self.groups[..i].iter().any(|(p, _)| p == prefix) {
                return Err(bad(
                    "two policies for one prefix — there is nothing to answer \
                     which of them applies with",
                ));
            }
            for (&class, over) in &policy.channels {
                let mut config = self.config_for(None, class);
                over.apply_to(&mut config);
                if class == StorageClass::Critical && !config.sync_interval.is_zero() {
                    return Err(bad(
                        "the critical channel syncs at once — that is the point of it; \
                         a group has no right to cancel that",
                    ));
                }
                config.check().map_err(bad)?;
            }
            // The quota is checked right here rather than when the first
            // matching namespace comes up: a bad setting has to be named while
            // it can still be fixed — and the first matching name may come up
            // months into a device's service.
            for class in StorageClass::ALL {
                let Some(quota) = policy.quota.get(class) else {
                    continue;
                };
                let mut config = self.config_for(None, class);
                if let Some(over) = policy.channels.get(&class) {
                    over.apply_to(&mut config);
                }
                if quota < config.segment_bytes.saturating_mul(2) {
                    return Err(bad(
                        "the quota is smaller than two segments — rotation would eat the \
                         data right after sealing",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Whether a namespace belongs to a group.
///
/// A group is a name prefix, and the rule has to be one and the same for
/// writing and for reading: "the orchestrators' journals" (`Query::group`) and
/// "the orchestrators' quota" ([`StoreConfig::group`]) have no right to denote
/// different sets.
pub fn in_group(prefix: &str, namespace: &str) -> bool {
    namespace.starts_with(prefix)
}

/// How a namespace group differs from the store's shared settings.
///
/// Channel settings are given for the whole store, per storage class — and
/// that holds exactly as long as the namespaces are uniform. Twenty-four
/// thousand of them never are: an orchestrator's telemetry is heavy and
/// expendable, a diagnostic service's records are rare and must not be lost. A
/// group makes it possible to say that once about everyone a name prefix
/// unites, instead of repeating it at every namespace open.
///
/// What a group can NOT set is the class budget and its medium: see
/// [`ChannelOverride`].
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct GroupPolicy {
    channels: HashMap<StorageClass, ChannelOverride>,
    quota: NsQuota,
}

impl GroupPolicy {
    pub fn new() -> Self {
        Self::default()
    }

    /// How this class's channels differ for the group.
    pub fn channel(mut self, class: StorageClass, over: ChannelOverride) -> Self {
        self.channels.insert(class, over);
        self
    }

    /// A personal quota for **each** namespace of the group.
    ///
    /// Each, not the group as a whole: a shared pot for a group is a budget,
    /// and a budget belongs to a class and is divided by segment age across the
    /// whole store. A quota is a limit inside a budget, and it is about one
    /// namespace: "no orchestrator takes more than a gigabyte of telemetry". A
    /// quota given when a namespace is opened beats the group's.
    pub fn limit_bytes(mut self, class: StorageClass, bytes: u64) -> Self {
        self.quota = self.quota.limit_bytes(class, bytes);
        self
    }
}

/// A namespace's personal quotas inside the class budgets.
///
/// Optional — and the default makes sense: an ordinary channel draws on the
/// shared budget of its class and has no per-channel rotation at all. A quota
/// is for when a particular service must not be let near the shared budget
/// unbounded: its channels rotate within the quota without waiting for the
/// class to hit its budget.
#[derive(Debug, Clone, Default)]
pub struct NsQuota {
    slots: [Option<u64>; StorageClass::ALL.len()],
}

impl NsQuota {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bound this namespace's channels of class `class` to `bytes` bytes.
    pub fn limit_bytes(mut self, class: StorageClass, bytes: u64) -> Self {
        self.slots[class.index()] = Some(bytes);
        self
    }

    fn get(&self, class: StorageClass) -> Option<u64> {
        self.slots[class.index()]
    }
}

/// The store.
#[derive(Debug)]
pub struct Store {
    root: PathBuf,
    /// The base directory of every class (indexed by [`StorageClass::index`]):
    /// either the shared root or the class's own medium.
    class_roots: Vec<PathBuf>,
    /// Every distinct root — for walking names (epochs, cleanup).
    scan_roots: Vec<PathBuf>,
    meta: StoreMeta,
    /// The container version the store came up with, if it was older than the
    /// current one. The application should log this: part of the accumulated
    /// history is not readable by this build.
    upgraded_from: Option<u8>,
    /// The settings the store was opened with: namespaces take their channel
    /// policies from here rather than from a second instance passed in again.
    config: StoreConfig,
    clock: Clock,
    epochs: Mutex<EpochStore>,
    writer: Arc<Writer>,
    counters: Arc<Counters>,
    /// The span counter, shared by the process: a span lives inside one run, so
    /// there is no reason to persist it.
    next_span: Arc<AtomicU32>,
    /// The open namespaces: opening the same name twice in one process would
    /// give two independent states over one directory.
    open: Mutex<HashMap<String, Option<NsId>>>,
    /// The marks saying there is a point in looking again: the writer raises
    /// them and a reader's subscription waits on them.
    pulse: Arc<crate::pulse::Pulse>,
    /// The schemas brought in by the namespaces that came up, by schema name.
    ///
    /// Kept apart from `open` and **not** removed with the handle: holding a
    /// name is about writing, and the process does not lose the ability to
    /// decode records because a handle was released. This is where a reader
    /// built over the store takes them from.
    schemas: Mutex<HashMap<&'static str, Schema>>,
    /// Held open for as long as the store lives: the kernel releases it when
    /// the process ends, a crash included.
    _lock: File,
}

impl Store {
    /// Open (or create) a store.
    ///
    /// Takes an exclusive lock on the root: two processes over one directory
    /// would overwrite each other's `epochs.bin` and hand out equal
    /// `boot_counter` values, which would make segment names collide.
    pub fn open(config: StoreConfig) -> Result<Arc<Self>> {
        config.validate()?;
        fsutil::create_dir_all_synced(&config.root)?;
        let lock = acquire_lock(&config.root)?;
        fsutil::sweep_tmp(&config.root)?;

        let (meta, upgraded_from) = load_or_create_meta(&config.root)?;

        // The class roots and the budget groups. The medium key comes from
        // paths actually matching: classes on one partition share ENOSPC
        // pressure, classes on different ones do not get in each other's way.
        let canon = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        let mut keys: Vec<PathBuf> = vec![canon(&config.root)];
        let mut class_roots = Vec::with_capacity(StorageClass::ALL.len());
        let mut scan_roots = vec![config.root.clone()];
        let mut groups = Vec::with_capacity(StorageClass::ALL.len());
        for class in StorageClass::ALL {
            let cfg = config.config_for(None, class);
            let base = cfg
                .custom_root
                .clone()
                .unwrap_or_else(|| config.root.clone());
            fsutil::create_dir_all_synced(&base)?;
            let c = canon(&base);
            let key = keys.iter().position(|k| *k == c).unwrap_or_else(|| {
                keys.push(c);
                scan_roots.push(base.clone());
                keys.len() - 1
            });
            groups.push(GroupBudget {
                budget_bytes: cfg.budget_bytes,
                root_key: key as u8,
            });
            class_roots.push(base);
        }

        // The clock's base and the entry in epochs.bin take one and the same
        // BOOTTIME value: a discrepancy between them would carry into the UTC
        // conversion.
        let base_us = boottime_us();
        // A run number has no right to come out below what is already written
        // on segment names: the epochs file may not have survived corruption or
        // a cleanup while the segments did, and a number handed out twice would
        // carry new records into the past (see
        // `EpochStore::open_and_register`). The walk over names is lazy — only
        // when no epochs file is left; and ALL roots are walked: a class's
        // history may live entirely on its own medium.
        let epochs = EpochStore::open_and_register(&config.root, base_us, &|| {
            Ok(live_boots(&scan_roots)?.into_iter().next_back())
        })?;
        let clock = Clock::with_base(
            dduroc_format::BootCounter(epochs.current_run().boot_counter),
            base_us,
        );

        let counters = Arc::new(Counters::default());
        let pulse = Arc::new(crate::pulse::Pulse::new());
        let writer = Writer::spawn(
            Arc::clone(&counters),
            config.queues,
            groups,
            config.buffer_ceiling_bytes,
            Arc::clone(&pulse),
        )?;

        let store = Arc::new(Self {
            root: config.root.clone(),
            class_roots,
            scan_roots,
            meta,
            upgraded_from,
            config,
            clock,
            epochs: Mutex::new(epochs),
            writer,
            counters,
            next_span: Arc::new(AtomicU32::new(1)),
            pulse,
            open: Mutex::new(HashMap::new()),
            schemas: Mutex::new(HashMap::new()),
            _lock: lock,
        });

        // The epochs file grows by an entry per restart and is read whole at
        // every start. It is swept when it really has grown — otherwise walking
        // segment names would cost more than it saves.
        let runs = store.locked_epochs().epochs().runs.len();
        if runs > EPOCH_COMPACT_THRESHOLD {
            let _ = store.compact_epochs();
        }
        Ok(store)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Every root the store's data lies in; the first is the main one.
    ///
    /// There is more than one when a class has been moved to its own medium
    /// ([`ChannelConfig::custom_root`]): critical data on a protected partition
    /// is a second `<root>/<namespace>/<class>/` tree. A reader has to know
    /// them all, or it will show the store without the class that was moved out
    /// and say nothing about it.
    pub fn roots(&self) -> &[PathBuf] {
        &self.scan_roots
    }

    /// The schemas this store can decode.
    ///
    /// They are brought in by the namespaces that came up; a released handle
    /// does not take its schema away. One schema shared by several namespaces
    /// counts once.
    pub fn schemas(&self) -> Vec<Schema> {
        self.schemas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .copied()
            .collect()
    }

    pub fn meta(&self) -> StoreMeta {
        self.meta
    }

    /// The store's marks about new data — what a reader's subscription sleeps
    /// on.
    ///
    /// A mark does not replace reading and says nothing about the records: it
    /// only says there is a point in looking again. The truth is still on the
    /// medium.
    pub fn pulse(&self) -> &Arc<crate::pulse::Pulse> {
        &self.pulse
    }

    /// The earlier container version, if the store came up from an old one.
    ///
    /// `Some(v)` means there are segments of version `v` in the directory but
    /// this build does not read them and they will go with rotation. This event
    /// is worth logging — silently losing access to history is worse than
    /// saying so.
    pub fn upgraded_from(&self) -> Option<u8> {
        self.upgraded_from
    }

    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    pub fn stats(&self) -> Stats {
        self.counters.snapshot()
    }

    /// Bring up a namespace with the given schema.
    ///
    /// The channel settings come from those the store was opened with: they
    /// used to have to be passed as a second instance
    /// (`Store::open(config.clone())` plus `namespace(.., &config)`), and
    /// nothing stopped a different one being passed here — the channels would
    /// have got a budget the store knew nothing about.
    ///
    /// The channels draw on the shared budgets of their classes; for a
    /// namespace that deserves a personal limit there is
    /// [`Store::namespace_with_quota`].
    pub fn namespace(self: &Arc<Self>, name: &str, schema: Schema) -> Result<Namespace> {
        self.namespace_with_quota(name, schema, NsQuota::default())
    }

    /// Bring up a namespace with personal quotas — see [`NsQuota`].
    pub fn namespace_with_quota(
        self: &Arc<Self>,
        name: &str,
        schema: Schema,
        quota: NsQuota,
    ) -> Result<Namespace> {
        validate_component(name).map_err(|reason| Error::BadNamespace {
            name: name.to_owned(),
            reason,
        })?;
        // The reason is an ordinary string. This used to be a `Box::leak`,
        // because `reason` was a `&'static str`: every failed attempt to bring
        // a namespace up left its text in memory forever, and on a device such
        // an attempt is repeated in a reconnection loop.
        schema.validate().map_err(|e| Error::BadSchema {
            name: schema.name.to_owned(),
            reason: e.to_string(),
        })?;

        // The name is marked taken under the same lock that checks it: between
        // "free" and "taken" two threads managed to get an independent writer
        // state each over one directory, and both would write segments with
        // equal names.
        {
            let mut open = self.locked_open();
            if open.contains_key(name) {
                return Err(Error::NamespaceBusy(name.to_owned()));
            }
            open.insert(name.to_owned(), None);
        }
        // From here on, any early return has to clear the mark, or the name
        // stays taken for the life of the process.
        let guard = ReserveGuard {
            store: self,
            name,
            armed: true,
        };

        let dir = self.root.join(name);
        fsutil::create_dir_all_synced(&dir)?;
        fsutil::sweep_tmp(&dir)?;

        // A namespace's schema is fixed at the first open: equal event
        // identifiers in different schemas mean different things, and mixing
        // them in one directory means decoding records with the wrong
        // templates.
        let meta = NsMeta::open(&dir, name, &schema)?;

        let classes = schema.classes();
        // A channel lives in the directory of its class — possibly on another
        // medium; the budget group is the class.
        let mut specs = Vec::with_capacity(classes.len());
        for class in &classes {
            let config = self.config.config_for(Some(name), *class);
            // A namespace quota beats a group's: it is more specific. The
            // group's is "no orchestrator takes more than", the personal one is
            // about this one.
            let personal = quota
                .get(*class)
                .or_else(|| self.config.group_quota(name, *class));
            if let Some(q) = personal
                && q < config.segment_bytes.saturating_mul(2)
            {
                return Err(Error::BadChannel {
                    class: *class,
                    namespace: Some(name.to_owned()),
                    reason: "the quota is smaller than two segments — rotation would \
                             eat the data right after sealing",
                });
            }
            specs.push(ChannelSpec {
                dir: self.class_roots[class.index()]
                    .join(name)
                    .join(class.as_str()),
                group: class.index(),
                quota_bytes: personal,
                config,
            });
        }
        let channel_dirs: Vec<PathBuf> = specs.iter().map(|s| s.dir.clone()).collect();

        let drops = Arc::new(DropCounters::new(specs.len()));
        let boot = dduroc_format::BootCounter(self.boot_counter());

        let id = self.writer.register(NsSetup {
            name: name.to_owned(),
            protocol_version: schema.version,
            store_id: self.meta.store_id,
            boot,
            channels: specs,
            drops: Arc::clone(&drops),
        })?;

        self.locked_open().insert(name.to_owned(), Some(id));
        self.schemas
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(schema.name, schema);
        guard.disarm();

        Ok(Namespace::new(
            Arc::new(NamespaceLease {
                store: Arc::clone(self),
                name: name.to_owned(),
                id,
            }),
            id,
            name.to_owned(),
            dir,
            channel_dirs,
            self.meta.store_id,
            schema,
            classes,
            Arc::clone(&self.writer),
            self.clock.clone(),
            drops,
            Arc::clone(&self.next_span),
            meta,
        ))
    }

    /// The registry of open namespaces, with recovery from poisoning.
    ///
    /// The reason is the same as for [`Store::locked_epochs`]: poisoning means
    /// a panic on another thread, not contradictory data — only insertions into
    /// and removals from a ready table happen under the mutex. Failing would
    /// cost more: a namespace handle being dropped could not clear the "taken"
    /// mark, the name would stay taken forever and its string would never be
    /// freed.
    fn locked_open(&self) -> std::sync::MutexGuard<'_, HashMap<String, Option<NsId>>> {
        self.open.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The epochs under a mutex, with recovery from poisoning.
    ///
    /// Poisoning means a panic on another thread but not contradictory data:
    /// only short operations over an already parsed structure happen under the
    /// mutex. Failing would cost more — `boot_counter` would be replaced by a
    /// zero, indistinguishable from a genuine first run, and another run's
    /// segments would become "ours".
    fn locked_epochs(&self) -> std::sync::MutexGuard<'_, EpochStore> {
        self.epochs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record a time synchronization.
    ///
    /// Returns `false` if the source is less trustworthy than the anchor
    /// already written (a manual entry does not beat GPS).
    pub fn record_sync(&self, utc: DateTime<Utc>, source: SyncSource) -> Result<bool> {
        // A knowably impossible time must not spoil the whole history: the
        // anchor is retroactive, and one call with garbage would distort the
        // UTC of every event of this boot.
        if !is_plausible_utc(utc) {
            return Ok(false);
        }
        self.locked_epochs().record_sync(utc, source)
    }

    /// Remove entries from `epochs.bin` about runs of which no segments are
    /// left. Returns the number of entries removed.
    ///
    /// Only file **names** are walked: a `readdir` over the channels, without
    /// opening a single segment. The current run is never removed — it is still
    /// writing.
    ///
    /// Called automatically when the store comes up, but only once the file
    /// really has grown (of the order of a thousand runs): walking directories
    /// with thousands of namespaces is not free, while a stale epoch entry
    /// costs tens of bytes. An application may call the cleanup itself.
    pub fn compact_epochs(&self) -> Result<usize> {
        let live = live_boots(&self.scan_roots)?;
        let mut epochs = self.locked_epochs();
        let before = epochs.epochs().runs.len();
        epochs.retain_runs(&|boot| live.contains(&boot))?;
        Ok(before - epochs.epochs().runs.len())
    }

    /// Convert relative time into wall-clock time. `None` means there is no
    /// anchor.
    pub fn to_utc(&self, at: BootTime) -> Option<DateTime<Utc>> {
        self.locked_epochs().epochs().to_utc(at)
    }

    /// A snapshot of the epochs at this moment — with every anchor, including
    /// the ones [`Store::record_sync`] has just written.
    ///
    /// This is where a live reader takes them from on every query: the
    /// `epochs.bin` parsed when it opened goes stale with the first time
    /// synchronization, and records that do have an anchor would be left
    /// without a UTC.
    pub fn epochs(&self) -> crate::epochs::Epochs {
        self.locked_epochs().epochs().clone()
    }

    /// The current moment in the same coordinates as the records.
    pub fn now(&self) -> BootTime {
        self.clock.now_at()
    }

    /// The current `boot_counter`.
    pub fn boot_counter(&self) -> u32 {
        self.locked_epochs().current_run().boot_counter
    }

    /// Whether records still reach the medium — that is, whether the writer
    /// thread is alive.
    ///
    /// `false` means records no longer reach the disk: either the store has
    /// stopped or the thread has died. The losses are accounted for in
    /// [`Stats::dropped`].
    ///
    /// A reader's subscription needs this so as not to wait forever for someone
    /// there is nobody left to write for: a thread killed by a panic never gets
    /// to set the close mark ([`crate::pulse::Pulse::close`]).
    pub fn is_writing(&self) -> bool {
        self.writer.is_alive()
    }

    /// Wait until everything accumulated is on the medium.
    pub fn sync(&self) -> Result<()> {
        self.writer.sync(None)
    }

    /// Finish: write out, seal the segments, stop the writer.
    pub fn shutdown(&self) {
        self.writer.shutdown();
    }
}

/// Keeps the store alive for as long as a namespace handle lives, and frees
/// the name when the handle is dropped.
///
/// Without the first, a `Store` being dropped would stop the writer while a
/// `Namespace` that outlived it went on returning `Ok` into nothing. Without
/// the second, the name would stay taken for the life of the process, and a
/// service could not reopen its namespace after being reconfigured.
#[derive(Debug)]
struct NamespaceLease {
    store: Arc<Store>,
    name: String,
    id: NsId,
}

impl Drop for NamespaceLease {
    fn drop(&mut self) {
        // The writer FIRST, and only then the "name taken" mark is cleared. The
        // reverse order left a window: between clearing the mark and sending
        // the command another thread manages to bring the same namespace up,
        // its `Register` queues ahead of our `Release`, and one directory ends
        // up with two channel states — with two inventories and two rotations.
        // One's rotation would delete a segment the other had open: writing
        // would go on into a file with no name and vanish when it closed.
        //
        // The order between the commands themselves is kept by their shared
        // queue.
        self.store.writer.release(self.id);
        self.store.locked_open().remove(&self.name);
    }
}

/// Clears the "name taken" mark if bringing a namespace up did not finish.
struct ReserveGuard<'a> {
    store: &'a Store,
    name: &'a str,
    armed: bool,
}

impl ReserveGuard<'_> {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for ReserveGuard<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.store.locked_open().remove(self.name);
        }
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        // An unsealed segment reads by scanning, so no data is lost without
        // this — but a footer saves the reader a whole pass over the file.
        self.writer.shutdown();
    }
}

/// At how many run entries `epochs.bin` is swept at start.
///
/// The cleanup costs a walk over the names in every channel, while an uncleaned
/// entry costs tens of bytes, so there is no reason to pay for the walk at
/// every start: the threshold amortizes it over a thousand-odd restarts. The
/// file stays bounded that way, and the data does not: the cleanup does not
/// touch a run whose segments are still there, and the UTC of its records is
/// not lost.
const EPOCH_COMPACT_THRESHOLD: usize = 1024;

/// The run numbers segments still stand behind.
///
/// Only `readdir` and name parsing — not one file is opened: a segment's name
/// carries `boot_counter` in its first eight characters.
///
/// An error while walking is a **failure**, not an empty set. The result is
/// used in two ways, and in both an oversight is irreversible: cleaning epochs
/// from an incomplete list would delete the anchors of runs whose segments
/// merely could not be listed, and the lower bound on the run number derived
/// from the same list would give repeated numbering. "Could not look" and
/// "there is nothing there" are different answers.
///
/// The strictness extends only to what is **ours**. A store root is sometimes
/// a mount point, and foreign things lie next to the namespaces: a `lost+found`
/// owned by root, the directories of neighbouring subsystems, dangling
/// symlinks. Falling over them in `Store::open` would leave the device without
/// a journal exactly when it is needed most — and in exactly the scenario the
/// walk exists for (the epochs file did not survive the previous run, the very
/// first start included).
///
/// The sign of "not ours" is the name: a namespace directory is created only
/// through [`Store::namespace`], which checks the name with the same
/// [`validate_component`]. An entry with an invalid name cannot hold segments
/// of **this** store, so it is not entered at all — before any `read_dir`.
/// Everything that passed the name check is walked strictly.
fn live_boots(roots: &[PathBuf]) -> Result<std::collections::BTreeSet<u32>> {
    use crate::channel::validate_component;
    use dduroc_format::segment::SegmentName;

    /// List a directory. `NotADirectory` means an ordinary file where a
    /// directory should be, `NotFound` that the entry vanished between the
    /// listing and the descent (an external cleanup, a dangling symlink): both
    /// mean "there are no segments here" rather than "could not look".
    fn entries(path: &Path) -> Result<Vec<std::fs::DirEntry>> {
        use std::io::ErrorKind::{NotADirectory, NotFound};
        match std::fs::read_dir(path) {
            Ok(it) => it
                .collect::<std::io::Result<Vec<_>>>()
                .ctx_path("walking a directory", path),
            Err(e) if matches!(e.kind(), NotADirectory | NotFound) => Ok(Vec::new()),
            Err(e) => Err(e).ctx_path("walking a directory", path),
        }
    }

    /// Whether this name could have been created by the store.
    fn ours(entry: &std::fs::DirEntry) -> bool {
        entry
            .file_name()
            .to_str()
            .is_some_and(|n| validate_component(n).is_ok())
    }

    let mut out = std::collections::BTreeSet::new();
    for root in roots {
        for ns in entries(root)?.iter().filter(|e| ours(e)) {
            for ch in entries(&ns.path())?.iter().filter(|e| ours(e)) {
                for seg in entries(&ch.path())? {
                    if let Some(name) = seg.file_name().to_str().and_then(SegmentName::parse) {
                        out.insert(name.boot.0);
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Whether a moment in time is sensible: between 2001-09-09 and 2100-01-01.
///
/// Below the lower bound lie zeros and garbage from uninitialized clocks, above
/// the upper one overflows and knowably corrupted values.
fn is_plausible_utc(utc: DateTime<Utc>) -> bool {
    const MIN_MS: i64 = 1_000_000_000_000;
    const MAX_MS: i64 = 4_102_444_800_000;
    (MIN_MS..MAX_MS).contains(&utc.timestamp_millis())
}

/// Take an exclusive lock on the store root.
fn acquire_lock(root: &Path) -> Result<File> {
    let path = root.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .mode(fsutil::FILE_MODE)
        .open(&path)
        .ctx_path("opening the lock file", &path)?;

    rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive).map_err(
        |e| {
            if matches!(e, rustix::io::Errno::WOULDBLOCK) {
                // The lock is tied to an open file description rather than to a
                // process: the conflict arises when the same root is opened
                // twice within one process too — and that is exactly the
                // mistake worth catching.
                Error::StoreBusy(root.to_owned())
            } else {
                Error::Io {
                    context: format!("locking {}", path.display()),
                    source: e.into(),
                }
            }
        },
    )?;
    Ok(file)
}

/// Read or create the store's metadata.
///
/// A store written by an **earlier** container version comes up: the metadata
/// file is rewritten to the current version, `store_id` is preserved, and the
/// old segments stay where they are until rotation.
///
/// Refusing would be the worst of all: a firmware update would mean
/// `Store::open` failed and the device stopped logging entirely — exactly when
/// the journal is needed most. The earlier version's data is not substituted
/// for or passed off as ours: every segment header carries its container
/// version, and the reader reports them as an unread fragment rather than
/// parsing them on a guess.
///
/// A version **from the future** is still an error: there is nothing to guess a
/// layout this build does not know with.
fn load_or_create_meta(root: &Path) -> Result<(StoreMeta, Option<u8>)> {
    let path = root.join(STORE_META);
    let current = dduroc_format::CONTAINER_VERSION;

    if let Some(bytes) = fsutil::read_optional(&path)? {
        let meta: StoreMeta = postcard::from_bytes(&bytes).map_err(|_| Error::Corrupt {
            path: path.clone(),
            reason: "the store metadata does not parse".to_owned(),
        })?;
        if meta.container_version > current {
            return Err(Error::Corrupt {
                path,
                reason: format!(
                    "container version {} is newer than the supported one ({}): this \
                     build cannot parse a layout from the future",
                    meta.container_version, current
                ),
            });
        }
        if meta.container_version < current {
            let from = meta.container_version;
            let upgraded = StoreMeta {
                container_version: current,
                store_id: meta.store_id,
            };
            fsutil::write_atomic(&path, &postcard::to_allocvec(&upgraded)?)?;
            return Ok((upgraded, Some(from)));
        }
        return Ok((meta, None));
    }

    let meta = StoreMeta {
        container_version: current,
        store_id: fresh_store_id(),
    };
    fsutil::write_atomic(&path, &postcard::to_allocvec(&meta)?)?;
    Ok((meta, None))
}

/// The store identifier.
///
/// Cryptographic strength is not needed — the job is only to tell devices
/// apart — so a mix of the kernel boot_id, the time and a heap address is
/// enough: pulling in a random number generator for one value is pointless.
fn fresh_store_id() -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut mix = |bytes: &[u8]| {
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x1000_0000_01b3);
        }
    };
    if let Ok(id) = std::fs::read("/proc/sys/kernel/random/boot_id") {
        mix(&id);
    }
    mix(&boottime_us().to_le_bytes());
    mix(&std::process::id().to_le_bytes());
    let probe = Box::new(0u8);
    mix(&(std::ptr::from_ref(&*probe) as usize).to_le_bytes());
    if let Ok(d) = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        mix(&d.as_nanos().to_le_bytes());
    }
    h
}

/// Allocate a span identifier.
///
/// The numbering is local to a run and is not persisted. On a u32 overflow the
/// counter wraps to 1: reading matches a span's start with its end within a
/// time window, and billions of events pass between repeats of one number.
pub(crate) fn next_span_id(counter: &AtomicU32) -> dduroc_format::SpanId {
    let raw = counter.fetch_add(1, Ordering::Relaxed);
    dduroc_format::SpanId(if raw == 0 { 1 } else { raw })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A moment in time from epoch milliseconds.
    fn utc_ms(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).expect("the milliseconds are within the epoch")
    }

    #[test]
    fn a_longer_prefix_refines_the_shorter_one_and_the_namespace_beats_them_all() {
        // Prefixes nest inside one another: `orc-radio-` REFINES `orc-` rather
        // than replacing it. Otherwise refining one setting would silently
        // remove every other the general group set. Such an answer has no right
        // to depend on declaration order or on how a hash table is walked.
        let cfg = StoreConfig::new("/tmp/nowhere")
            .group(
                "orc-",
                GroupPolicy::new()
                    .channel(
                        StorageClass::Default,
                        ChannelOverride::new().segment_bytes(2 << 20),
                    )
                    .limit_bytes(StorageClass::Default, 100 << 20),
            )
            .group(
                "orc-radio-",
                GroupPolicy::new().channel(
                    StorageClass::Default,
                    ChannelOverride::new().segment_bytes(1 << 20),
                ),
            );

        let seg = |ns| {
            cfg.config_for(Some(ns), StorageClass::Default)
                .segment_bytes
        };
        assert_eq!(seg("orc-radio-0"), 1 << 20, "the longer prefix wins");
        assert_eq!(seg("orc-power-0"), 2 << 20);
        assert_eq!(
            seg("diag-0"),
            8 << 20,
            "an outsider gets the class's shared settings"
        );
        assert_eq!(
            cfg.config_for(None, StorageClass::Default).segment_bytes,
            8 << 20,
            "with no name the groups are not consulted at all"
        );

        // The quota is set by the general group alone — the refining one does
        // not cancel it.
        assert_eq!(
            cfg.group_quota("orc-radio-0", StorageClass::Default),
            Some(100 << 20),
            "refining the segment size does not remove the general group's quota"
        );
        assert_eq!(
            cfg.group_quota("orc-power-0", StorageClass::Default),
            Some(100 << 20)
        );
        assert_eq!(cfg.group_quota("diag-0", StorageClass::Default), None);

        // And the other way round: a class setting the refining group does not
        // mention comes from the general one.
        let refining = StoreConfig::new("/tmp/nowhere")
            .group(
                "orc-",
                GroupPolicy::new().channel(
                    StorageClass::Telemetry,
                    ChannelOverride::new()
                        .segment_bytes(2 << 20)
                        .compression(dduroc_format::Compression::None),
                ),
            )
            .group(
                "orc-radio-",
                GroupPolicy::new().channel(
                    StorageClass::Telemetry,
                    ChannelOverride::new().segment_bytes(1 << 20),
                ),
            );
        let refined = refining.config_for(Some("orc-radio-0"), StorageClass::Telemetry);
        assert_eq!(refined.segment_bytes, 1 << 20, "its own is its own");
        assert_eq!(
            refined.compression,
            dduroc_format::Compression::None,
            "what the refinement does not mention comes from the general group"
        );
    }

    #[test]
    fn the_class_keeps_its_budget_and_its_medium_whatever_a_group_says() {
        // The budget and the medium are properties of a class, shared across
        // the whole store. A group cannot set them by construction:
        // `ChannelOverride` has no such fields. What is checked here is that
        // resolution does not touch them either — otherwise the occupancy
        // ceiling a class declared would stop being a ceiling.
        let vault = std::path::PathBuf::from("/tmp/vault");
        let cfg = StoreConfig::new("/tmp/nowhere")
            .channel(
                StorageClass::Default,
                ChannelConfig {
                    custom_root: Some(vault.clone()),
                    ..ChannelConfig::new(32 << 20)
                },
            )
            .group(
                "orc-",
                GroupPolicy::new().channel(
                    StorageClass::Default,
                    ChannelOverride::new()
                        .segment_bytes(2 << 20)
                        .compression(dduroc_format::Compression::None),
                ),
            );

        let grouped = cfg.config_for(Some("orc-0"), StorageClass::Default);
        assert_eq!(grouped.segment_bytes, 2 << 20, "its own is its own");
        assert_eq!(grouped.compression, dduroc_format::Compression::None);
        assert_eq!(
            grouped.budget_bytes,
            32 << 20,
            "the budget stayed the class's"
        );
        assert_eq!(grouped.custom_root, Some(vault), "and so did the medium");
    }

    #[test]
    fn a_group_is_refused_for_what_a_class_would_be_refused_for() {
        let base = || StoreConfig::new("/tmp/nowhere").with_budget_per_class(64 << 20);

        // A setting unfit for a class does not become fit by being given to a
        // group: a segment larger than half the budget means rotation would eat
        // the data right after it was sealed.
        let e = base()
            .group(
                "orc-",
                GroupPolicy::new().channel(
                    StorageClass::Default,
                    ChannelOverride::new().segment_bytes(60 << 20),
                ),
            )
            .validate()
            .unwrap_err();
        assert!(matches!(e, Error::BadGroup { .. }), "{e}");

        // The immediacy of the critical channel is its definition, and a group
        // is no more allowed to cancel it than the store is.
        let e = base()
            .group(
                "orc-",
                GroupPolicy::new().channel(
                    StorageClass::Critical,
                    ChannelOverride::new().sync_interval(std::time::Duration::from_secs(5)),
                ),
            )
            .validate()
            .unwrap_err();
        assert!(matches!(e, Error::BadGroup { .. }), "{e}");

        // A prefix no name can begin with is a dead setting: the operator would
        // believe it was in force.
        assert!(base().group("orc/", GroupPolicy::new()).validate().is_err());
        assert!(base().group("", GroupPolicy::new()).validate().is_err());

        // Two policies for one prefix: there is nothing to answer "which
        // applies" with.
        let e = base()
            .group("orc-", GroupPolicy::new())
            .group("orc-", GroupPolicy::new())
            .validate()
            .unwrap_err();
        assert!(matches!(e, Error::BadGroup { .. }), "{e}");

        // A sound group passes.
        base()
            .group(
                "orc-",
                GroupPolicy::new()
                    .channel(
                        StorageClass::Default,
                        ChannelOverride::new().segment_bytes(2 << 20),
                    )
                    .limit_bytes(StorageClass::Telemetry, 64 << 20),
            )
            .validate()
            .expect("a sound group policy");

        // A group's quota is checked at open time rather than when the first
        // matching namespace comes up: that may happen months into service, and
        // there would be nowhere left to fix the setting.
        let e = base()
            .group(
                "orc-",
                GroupPolicy::new().limit_bytes(StorageClass::Default, 4 << 20),
            )
            .validate()
            .unwrap_err();
        assert!(matches!(e, Error::BadGroup { .. }), "{e}");
    }

    #[test]
    fn store_id_is_stable_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());

        let a = Store::open(cfg.clone()).unwrap();
        let id = a.meta().store_id;
        assert_ne!(id, 0);
        a.shutdown();
        drop(a);

        let b = Store::open(cfg).unwrap();
        assert_eq!(b.meta().store_id, id, "the store identity is constant");
    }

    #[test]
    fn different_stores_get_different_ids() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        let sa = Store::open(StoreConfig::new(a.path())).unwrap();
        let sb = Store::open(StoreConfig::new(b.path())).unwrap();
        assert_ne!(
            sa.meta().store_id,
            sb.meta().store_id,
            "different stores must differ"
        );
    }

    #[test]
    fn second_open_of_the_same_root_is_refused() {
        // Two writers over one directory would overwrite each other's
        // epochs.bin and hand out equal boot_counter values — segment names
        // would collide. flock is tied to an open file description, so the
        // conflict is caught within one process too.
        let dir = tempfile::tempdir().unwrap();
        let first = Store::open(StoreConfig::new(dir.path())).unwrap();

        let err = Store::open(StoreConfig::new(dir.path())).unwrap_err();
        assert!(matches!(err, Error::StoreBusy(_)), "got {err}");

        // A released root opens again: otherwise restarting a service would run
        // into its own lock file.
        first.shutdown();
        drop(first);
        Store::open(StoreConfig::new(dir.path())).expect("the root was freed");
    }

    #[test]
    fn older_container_version_is_upgraded_not_fatal() {
        // Refusing to open the store would mean a firmware update deprives the
        // device of its journal — exactly when it is needed most. We come up,
        // preserving the store's identity, and report that part of the history
        // is not readable by this build.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());

        let first = Store::open(cfg.clone()).unwrap();
        let store_id = first.meta().store_id;
        assert_eq!(
            first.upgraded_from(),
            None,
            "a new store did not come up from anything"
        );
        first.shutdown();
        drop(first);

        // Forge an earlier container version.
        let path = dir.path().join(STORE_META);
        let old = StoreMeta {
            container_version: 1,
            store_id,
        };
        std::fs::write(&path, postcard::to_allocvec(&old).unwrap()).unwrap();

        let upgraded = Store::open(cfg.clone()).unwrap();
        assert_eq!(upgraded.upgraded_from(), Some(1), "coming up is announced");
        assert_eq!(
            upgraded.meta().store_id,
            store_id,
            "the store identity must be preserved: otherwise our own segments \
             would become foreign"
        );
        assert_eq!(
            upgraded.meta().container_version,
            dduroc_format::CONTAINER_VERSION
        );
        upgraded.shutdown();
        drop(upgraded);

        // Opening it again no longer counts as coming up.
        let again = Store::open(cfg).unwrap();
        assert_eq!(again.upgraded_from(), None);
    }

    #[test]
    fn future_container_version_is_refused() {
        // There is nothing to guess a layout from the future with: a refusal is
        // the only honest answer here.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());
        let s = Store::open(cfg.clone()).unwrap();
        let store_id = s.meta().store_id;
        s.shutdown();
        drop(s);

        let future = StoreMeta {
            container_version: dduroc_format::CONTAINER_VERSION + 1,
            store_id,
        };
        std::fs::write(
            dir.path().join(STORE_META),
            postcard::to_allocvec(&future).unwrap(),
        )
        .unwrap();

        let err = Store::open(cfg).unwrap_err();
        assert!(matches!(err, Error::Corrupt { .. }), "got {err}");
    }

    #[test]
    fn writer_liveness_is_reported_honestly() {
        // Before the stop writing is possible, after it not. The former check
        // looked at how full the queues were and answered "alive" in any state,
        // a dead thread included.
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(StoreConfig::new(dir.path())).unwrap();
        assert!(store.is_writing(), "right after opening the writer runs");
        store.shutdown();
        assert!(!store.is_writing(), "after the stop no writes go through");
    }

    #[test]
    fn boot_counter_advances_between_runs() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path());

        let a = Store::open(cfg.clone()).unwrap();
        assert_eq!(a.boot_counter(), 0);
        a.shutdown();
        drop(a);

        let b = Store::open(cfg).unwrap();
        assert_eq!(b.boot_counter(), 1);
    }

    #[test]
    fn a_lost_epochs_file_never_renumbers_over_existing_segments() {
        // An end-to-end check of the "floor ↔ name scan" pairing:
        // `boot_counter` goes into the name of every segment, and the order of
        // names is declared to be chronological. Restarting the numbering on
        // top of segments that survived the loss of the epochs file means
        // placing new records BEFORE the old ones: rotation will start on the
        // fresh ones and a reader will hand the history back jumbled.
        use crate::schema::{EventDesc, Language, Schema, StorageClass};
        use dduroc_format::segment::SegmentName;
        use dduroc_format::{EventId, Level, ProtocolVersion};

        static LANGS: &[Language] = &[Language("en")];
        static EVENTS: &[EventDesc] = &[EventDesc {
            id: EventId(1),
            name: "Tick",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &[],
            templates: &["tick"],
            fields: &[],
            decoders: None,
        }];
        let schema = Schema {
            name: "probe",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: &[],
            spans: &[],
            migrations: &[],
        };

        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);

        // Three runs, each leaving a segment of its own.
        for _ in 0..3 {
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-0", schema).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let channel = dir.path().join("orc-0").join("default");
        let names = |dir: &std::path::Path| -> Vec<SegmentName> {
            let mut v: Vec<SegmentName> = std::fs::read_dir(dir)
                .unwrap()
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().to_str().and_then(SegmentName::parse))
                .collect();
            v.sort_by_key(|n| n.to_string());
            v
        };
        let before = names(&channel);
        let highest = before.iter().map(|n| n.boot.0).max().unwrap();
        assert_eq!(highest, 2, "three runs: the segments of runs 0, 1, 2");

        // The epochs file is lost: a cleanup of the medium, a quarantine after
        // corruption — the outcome is the same.
        std::fs::remove_file(dir.path().join(crate::epochs::EPOCHS_FILE)).unwrap();

        let store = Store::open(cfg).unwrap();
        assert!(
            store.boot_counter() > highest,
            "the run number must continue rather than start over: \
             {} against {highest} on disk",
            store.boot_counter()
        );
        let ns = store.namespace("orc-0", schema).unwrap();
        ns.log_raw(EventId(1), &[2], None);
        ns.sync().unwrap();
        store.shutdown();

        // And most importantly the consequence: the new segment sorts AFTER all
        // the earlier ones, that is, it lands at the end of the history rather
        // than at its start.
        let after = names(&channel);
        let fresh = after
            .iter()
            .find(|n| !before.contains(n))
            .expect("a new segment was created");
        assert!(
            before.iter().all(|old| old.to_string() < fresh.to_string()),
            "the new name must sort after the old ones: {fresh} against {before:?}"
        );
    }

    #[test]
    fn epoch_cleanup_keeps_runs_that_still_have_segments() {
        // `epochs.bin` is read whole at every start and grows by an entry per
        // restart: twenty restarts a day over five years is thirty-six thousand
        // entries. The cleanup has to throw out runs of which no segments are
        // left and leave alone those of which some are: their UTC would be lost
        // along with the epoch entry.
        use crate::schema::{EventDesc, Language, Schema, StorageClass};
        use dduroc_format::{EventId, Level, ProtocolVersion};

        static LANGS: &[Language] = &[Language("en")];
        static EVENTS: &[EventDesc] = &[EventDesc {
            id: EventId(1),
            name: "Tick",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &[],
            templates: &["tick"],
            fields: &[],
            decoders: None,
        }];
        let schema = Schema {
            name: "probe",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: &[],
            spans: &[],
            migrations: &[],
        };

        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);

        // Run 0 leaves a segment behind.
        {
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-0", schema).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store.shutdown();
        }
        // Runs 1..4 write nothing — no segments stand behind them.
        for _ in 0..3 {
            let store = Store::open(cfg.clone()).unwrap();
            store.shutdown();
        }

        let store = Store::open(cfg).unwrap();
        assert_eq!(store.boot_counter(), 4);
        assert_eq!(store.locked_epochs().epochs().runs().len(), 5);

        let removed = store.compact_epochs().unwrap();
        assert_eq!(removed, 3, "runs without segments were removed");

        let kept: Vec<u32> = store
            .locked_epochs()
            .epochs()
            .runs()
            .iter()
            .map(|r| r.boot_counter)
            .collect();
        assert_eq!(
            kept,
            vec![0, 4],
            "the run with segments and the current one, which is still writing, remain"
        );

        // Run 0's records still convert to UTC.
        store
            .record_sync(utc_ms(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        assert!(
            store.to_utc(BootTime::from_raw(0, 0)).is_some(),
            "the cleanup has no right to deprive live data of its UTC"
        );

        // A second cleanup finds nothing.
        assert_eq!(store.compact_epochs().unwrap(), 0);
        store.shutdown();
    }

    #[test]
    fn implausible_utc_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let s = Store::open(StoreConfig::new(dir.path())).unwrap();

        assert!(
            !s.record_sync(DateTime::UNIX_EPOCH, SyncSource::Gps)
                .unwrap(),
            "a zero time"
        );
        assert!(
            !s.record_sync(utc_ms(-1), SyncSource::Gps).unwrap(),
            "a negative time"
        );
        assert!(
            !s.record_sync(DateTime::<Utc>::MAX_UTC, SyncSource::Gps)
                .unwrap(),
            "a time beyond the sensible"
        );
        assert!(
            s.record_sync(utc_ms(1_700_000_000_000), SyncSource::Ntp)
                .unwrap(),
            "a plausible time is accepted"
        );
        // A less trustworthy source does not override.
        assert!(
            !s.record_sync(utc_ms(1_800_000_000_000), SyncSource::User)
                .unwrap()
        );
        assert!(
            s.record_sync(utc_ms(1_800_000_000_000), SyncSource::Gps)
                .unwrap()
        );

        // A round trip: a record of the current moment gets a UTC.
        let at = s.now();
        let utc = s.to_utc(at).expect("there is an anchor");
        assert!(
            (1_800_000_000_000..1_800_000_001_000).contains(&utc.timestamp_millis()),
            "the UTC is close to the synchronization point: {utc}"
        );
    }

    #[test]
    fn span_ids_never_zero() {
        let c = AtomicU32::new(u32::MAX - 1);
        let a = next_span_id(&c);
        let b = next_span_id(&c);
        let wrapped = next_span_id(&c);
        assert_ne!(a.0, 0);
        assert_ne!(b.0, 0);
        assert_ne!(
            wrapped.0, 0,
            "the sentinel must not be handed out after an overflow"
        );
    }

    #[test]
    fn plausible_range() {
        assert!(!is_plausible_utc(utc_ms(0)));
        assert!(!is_plausible_utc(utc_ms(999_999_999_999)));
        assert!(is_plausible_utc(utc_ms(1_700_000_000_000)));
        assert!(!is_plausible_utc(utc_ms(4_102_444_800_000)));
    }
}
