//! The writer: the single thread that writes to files.
//!
//! # Why one thread
//!
//! A segment is an append-only file, and two writers into it would mean a lock
//! on every record. One thread removes the question entirely, and batching
//! turns a burst of events into one block and one `fdatasync`.
//!
//! # Two queues rather than one
//!
//! Critical and ordinary records travel on separate queues. With a shared one,
//! a telemetry stream of thousands of samples a second would queue up ahead of
//! a critical message — the classic priority inversion in which a "critical"
//! channel is critical only on paper.
//!
//! - the ordinary queue: on overflow a record is **discarded**, the counter
//!   grows, and a loss notice goes into the stream — a hole must not be
//!   indistinguishable from silence;
//! - the critical one: the caller **waits** for room (with a timeout). Critical
//!   events are rare, so waiting is all but unreachable, but the promise "not
//!   lost" becomes an honest one.
//!
//! # What the writer does not do
//!
//! It does **not log through the public API**. Only it can free the queue, so
//! writing from its own thread is a guaranteed self-deadlock once the queue is
//! full. All the diagnostics are atomic counters ([`crate::stats`]) and notices
//! inserted straight into the record stream.

use crate::channel::ChannelConfig;
use crate::error::{Error, IoContext, Result};
use crate::rotation::{Inventory, SegmentEntry};
use crate::segment::SegmentWriter;
use crate::staged::{ChannelIdx, DropCounters, NsId, Staged, StagedRecord};
use crate::stats::Counters;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use dduroc_format::block::{BlockBuilder, BlockHeader};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{BootCounter, FooterBuilder, Level, Micros, ProtocolVersion};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// The queue capacities.
///
/// A queue is allocated whole when the store is opened: crossbeam reserves the
/// entire buffer up front. With a 32-byte inline payload a record takes under a
/// hundred bytes, so the defaults cost about three quarters of a megabyte per
/// process — noticeable on armv7, and a device that writes rarely is entitled
/// to choose a smaller queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueSizes {
    /// The ordinary queue: on overflow a record is lost.
    pub normal: usize,
    /// The critical one: on overflow the caller waits for room.
    pub critical: usize,
}

impl Default for QueueSizes {
    fn default() -> Self {
        Self {
            normal: 8192,
            critical: 1024,
        }
    }
}

impl QueueSizes {
    /// A zero capacity would mean a rendezvous on every record: writing would
    /// only be possible at the writer's pace, and the ordinary channel would
    /// stop differing from the critical one.
    fn sanitized(self) -> Self {
        Self {
            normal: self.normal.max(1),
            critical: self.critical.max(1),
        }
    }
}

/// How many records the writer takes in one go before turning to the timers.
/// The bound is needed so that a telemetry stream does not freeze the servicing
/// of flush and sync deadlines.
const DRAIN_LIMIT: usize = 4096;

/// The longest wait for room in the critical queue.
const BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to sleep when there is nothing to service.
const IDLE_TIMEOUT: Duration = Duration::from_millis(250);

/// The lower bound on the loop's wait: without it an overdue deadline gives a
/// zero timeout and degenerates into a busy-wait on a whole core.
const MIN_TIMEOUT: Duration = Duration::from_millis(1);

/// The ceiling on the number of passes when draining the queues before
/// `sync`/`shutdown`.
///
/// Without it a thread writing faster than the writer can keep up would let
/// neither `sync` nor `shutdown` finish — the process could not stop.
const DRAIN_ROUNDS: usize = 64;

/// How long a channel has to sit idle before it gives its buffers back.
///
/// Giving them back at once is wrong: a channel with immediate syncing
/// (`sync_interval == 0`, the critical one) turns out to be "idle" after
/// **every** group commit — the block is flushed and there is nothing to sync —
/// and would give up its block buffer and scratch on every batch only to
/// allocate them again. That is precisely the hot path of critical records. The
/// pause does not affect it, and genuine idleness will not escape it: holding a
/// peak for a few extra seconds is cheaper than paying a couple of allocations
/// for every critical record.
const RELEASE_AFTER: Duration = Duration::from_secs(2);

/// How long a channel has to sit idle before it gives up its **segment**.
///
/// An open segment costs a descriptor and is counted in the budget together
/// with the unwritten tail of its reserve window, while what is written in it
/// may be a hundred bytes. With the tens of thousands of channels claimed,
/// that is tens of thousands of descriptors held by channels that have gone
/// quiet.
///
/// Releasing gives both back: the file is truncated to its actual data and
/// closed. But it is **not sealed** — the next write continues it rather than
/// starting a new one (see [`WriterLoop::unpark`]). The difference matters: a
/// channel writing once an hour would, if sealing, leave a file per record —
/// thousands of tiny segments a year per channel, which byte-based rotation
/// will not touch because there are hardly any bytes in them.
///
/// The pause is noticeably longer than [`RELEASE_AFTER`] because the price is
/// different: buffers cost a couple of allocations, a segment an `fdatasync`
/// and an `ftruncate`, and a walk over the file when it comes back.
const PARK_AFTER: Duration = Duration::from_secs(300);

/// The source of notices about the engine's own events in the record stream.
const DIAG_TARGET: &str = crate::diag::TARGET;

// ════════════════════════════════════════════════════════════════════════════
// Commands
// ════════════════════════════════════════════════════════════════════════════

/// A control command.
///
/// Commands travel on a separate queue, so on their own they **overtake** data
/// in flight. So that `sync` does not report success while some records are
/// still in the queue, and `shutdown` does not seal segments over what has not
/// been written, both commands first drain the data queues dry (see
/// [`WriterLoop::drain_pending`]).
#[derive(Debug)]
enum Control {
    /// Register a namespace.
    Register(Box<NsSetup>, Sender<Result<NsId>>),
    /// Release a namespace: seal its segments and free the slot.
    ///
    /// It travels on the same queue as `Register`, so bringing the same name up
    /// again is guaranteed to be handled **after** the release. Otherwise one
    /// directory would have two channel states with inventories of their own,
    /// and one's rotation would delete a segment the other had open — records
    /// would go into a file with no name and vanish when it closed.
    Release(NsId),
    /// Flush and sync everything a namespace has accumulated.
    Sync(Option<NsId>, Sender<Result<()>>),
    /// Swap a segment for the result of a migration (or delete an emptied one).
    ///
    /// A migration's heavy work — reading, transforming, writing the temporary
    /// file — happens on the calling thread; only the **commit** arrives here:
    /// a rename over the old name and an edit to the inventory. It cannot be
    /// done behind the writer's back — it is the sole owner of the inventory
    /// and of rotation, and an external file swap would race with rotation
    /// deleting that same file.
    CommitMigration {
        ns: NsId,
        channel: ChannelIdx,
        name: SegmentName,
        commit: MigrationCommit,
        /// `Ok(false)` means the segment is gone (rotated) and there is nothing
        /// to commit.
        reply: Sender<Result<bool>>,
    },
    /// Seal the active segments and finish.
    Shutdown(Sender<()>),
}

/// How the migration of one segment ended.
#[derive(Debug)]
pub(crate) enum MigrationCommit {
    /// Replace the segment file with the temporary one (already written and
    /// synced).
    Replace {
        tmp: PathBuf,
        /// The new file's size — the inventory has to learn it at once rather
        /// than at the next scan: rotation and the store ceiling walk those
        /// sums.
        size: u64,
    },
    /// Delete the segment: every record of it was deleted by the migration
    /// steps.
    Remove,
}

/// The parameters for registering a namespace.
#[derive(Debug)]
pub struct NsSetup {
    pub name: String,
    pub protocol_version: ProtocolVersion,
    pub store_id: u64,
    pub boot: BootCounter,
    pub channels: Vec<ChannelSpec>,
    pub drops: Arc<DropCounters>,
}

/// One channel of a namespace as the writer sees it. The writer knows nothing
/// of storage classes: its business is the directory, the policy, the budget
/// group and the personal quota.
#[derive(Debug)]
pub struct ChannelSpec {
    /// The full path of the channel directory — with the class root already
    /// applied.
    pub dir: PathBuf,
    /// The budget group (an index into the writer's list of groups).
    pub group: usize,
    /// The namespace's personal quota within the group. `None` means the
    /// channel draws on the group's shared budget with no individual limit.
    pub quota_bytes: Option<u64>,
    pub config: ChannelConfig,
}

/// A budget group: the shared limit on the total size of its channels'
/// segments.
#[derive(Debug, Clone, Copy)]
pub struct GroupBudget {
    pub budget_bytes: u64,
    /// The medium key: groups with one `root_key` live on one partition and
    /// share ENOSPC pressure.
    pub root_key: u8,
}

// ════════════════════════════════════════════════════════════════════════════
// The handle
// ════════════════════════════════════════════════════════════════════════════

/// The writer's handle: what application threads use.
#[derive(Debug)]
pub struct Writer {
    normal: Sender<Staged>,
    critical: Sender<Staged>,
    control: Sender<Control>,
    counters: Arc<Counters>,
    /// Stopping has begun: the queue still accepts, but there is nobody left to
    /// drain it.
    ///
    /// Between draining the queue in `shutdown` and the thread exiting the
    /// queue is alive, and `try_send` would answer `Ok` to records that then
    /// die in the channel's destructor — with no counter, no notice and no
    /// answer to the caller. Refusing here is more honest:
    /// [`Error::ShuttingDown`] is already declared a loss
    /// ([`Error::loses_record`]), it is simply that nobody produced it until
    /// now.
    ///
    /// The price is one relaxed read per record; a predictable branch,
    /// indistinguishable on armv7 against the enqueueing itself.
    stopping: std::sync::atomic::AtomicBool,
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Writer {
    /// Start the writer thread.
    ///
    /// `groups` are the budget groups (one per storage class): the writer knows
    /// nothing of classes, "the channels of such a group share such a budget on
    /// such a medium" is enough for it.
    pub fn spawn(
        counters: Arc<Counters>,
        queues: QueueSizes,
        groups: Vec<GroupBudget>,
        buffer_ceiling: Option<u64>,
        pulse: Arc<crate::pulse::Pulse>,
    ) -> Result<Arc<Self>> {
        let queues = queues.sanitized();
        let (normal_tx, normal_rx) = crossbeam_channel::bounded(queues.normal);
        let (critical_tx, critical_rx) = crossbeam_channel::bounded(queues.critical);
        let (control_tx, control_rx) = crossbeam_channel::bounded(64);

        let loop_state = WriterLoop {
            namespaces: Vec::new(),
            counters: Arc::clone(&counters),
            diag_target: Arc::from(DIAG_TARGET),
            batch: Vec::new(),
            drops_seen: 0,
            active: Vec::new(),
            groups,
            occupancy_seen: 0,
            pressured_roots: 0,
            buffer_ceiling,
            pulse,
            pulsed_blocks: 0,
            pulsed_shape: 0,
            roster_changed: false,
        };

        let handle = std::thread::Builder::new()
            .name("dduroc-writer".to_owned())
            .spawn(move || loop_state.run(normal_rx, critical_rx, control_rx))
            .map_err(|source| Error::Io {
                context: "starting the writer thread".to_owned(),
                source,
            })?;

        Ok(Arc::new(Self {
            normal: normal_tx,
            critical: critical_tx,
            control: control_tx,
            counters,
            stopping: std::sync::atomic::AtomicBool::new(false),
            handle: Mutex::new(Some(handle)),
        }))
    }

    /// Register a namespace, receiving its identifier.
    pub fn register(&self, setup: NsSetup) -> Result<NsId> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Register(Box::new(setup), tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }

    /// Put a record in the queue.
    ///
    /// `critical` chooses the behaviour on overflow: waiting instead of losing.
    /// `drops` are the channel's counters: a loss has to be marked where the
    /// hole appeared.
    #[inline]
    pub fn write(&self, item: Staged, critical: bool, drops: &DropCounters) -> Result<()> {
        if critical {
            self.write_critical(item, drops)
        } else {
            self.write_normal(item, drops)
        }
    }

    /// Put a record in the queue, blocking the caller under no circumstances.
    ///
    /// The queue chosen is the usual one — the order of critical records among
    /// themselves is preserved; only the reaction to overflow differs. Needed
    /// where waiting is unacceptable in principle: a span guard's `Drop` is
    /// called during stack unwinding after a panic too, and a five-second wait
    /// for room would turn an emergency shutdown into a hang.
    #[inline]
    pub fn write_no_wait(&self, item: Staged, critical: bool, drops: &DropCounters) -> Result<()> {
        // Stopping is under way — the queue still accepts, but there is nobody
        // left to drain it: everything put in now will die in the channel's
        // destructor. Refusing is more honest than answering `Ok` to a record
        // that will not be on the medium.
        if self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
            drops.record(item.channel);
            Counters::publish(&self.counters.dropped);
            return Err(Error::ShuttingDown);
        }
        let queue = if critical {
            &self.critical
        } else {
            &self.normal
        };
        match queue.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                // The order is mandatory: the per-channel counter first, then
                // the total — and the total with publication. The writer
                // decides whether to walk the channels for loss notices by its
                // change, and with the reverse order it could find the walk
                // empty, consider the notice issued and leave the hole
                // unannounced.
                drops.record(item.channel);
                Counters::publish(&self.counters.dropped);
                Err(Error::QueueFull)
            }
            Err(TrySendError::Disconnected(item)) => Err(self.writer_died(item)),
        }
    }

    #[inline]
    fn write_normal(&self, item: Staged, drops: &DropCounters) -> Result<()> {
        self.write_no_wait(item, false, drops)
    }

    fn write_critical(&self, item: Staged, drops: &DropCounters) -> Result<()> {
        // See `write_no_wait`: waiting for room in a queue nobody is draining
        // any more would mean waiting five seconds for a guaranteed loss.
        if self.stopping.load(std::sync::atomic::Ordering::Relaxed) {
            drops.record(item.channel);
            Counters::publish(&self.counters.dropped);
            return Err(Error::ShuttingDown);
        }
        match self.critical.try_send(item) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(item)) => {
                Counters::bump(&self.counters.backpressure_waits);
                // Wait with a timeout: blocking forever on a failed medium
                // would hang the application threads for good.
                match self.critical.send_timeout(item, BACKPRESSURE_TIMEOUT) {
                    Ok(()) => Ok(()),
                    Err(crossbeam_channel::SendTimeoutError::Timeout(item)) => {
                        // The same order and for the same reason as in
                        // `write_no_wait`.
                        drops.record(item.channel);
                        Counters::publish(&self.counters.dropped);
                        Err(Error::QueueFull)
                    }
                    Err(crossbeam_channel::SendTimeoutError::Disconnected(item)) => {
                        Err(self.writer_died(item))
                    }
                }
            }
            Err(TrySendError::Disconnected(item)) => Err(self.writer_died(item)),
        }
    }

    /// Account for a record lost to the writer's death.
    ///
    /// A queue refusing because there is no consumer is as much a loss as an
    /// overflow, and it has to be visible in `Stats`: otherwise `is_clean()`
    /// would report all is well over a store nothing has been written to in a
    /// long time.
    #[cold]
    fn writer_died(&self, _item: Staged) -> Error {
        Counters::bump(&self.counters.dropped);
        Error::WriterDead
    }

    /// Release a namespace: the writer will seal its segments and free the
    /// slot.
    ///
    /// Called when the last handle is dropped. With no reply: the caller is a
    /// `Drop`, and waiting for the medium in one is not allowed. The order with
    /// a subsequent `register` is kept by the command queue itself.
    pub fn release(&self, ns: NsId) {
        let _ = self.control.send(Control::Release(ns));
    }

    /// Flush what has accumulated and wait for the `fdatasync`.
    ///
    /// `None` means every namespace.
    pub fn sync(&self, ns: Option<NsId>) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Sync(ns, tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }

    /// Commit a segment's migration. `Ok(false)` means the segment was already
    /// rotated.
    pub(crate) fn commit_migration(
        &self,
        ns: NsId,
        channel: ChannelIdx,
        name: SegmentName,
        commit: MigrationCommit,
    ) -> Result<bool> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::CommitMigration {
                ns,
                channel,
                name,
                commit,
                reply: tx,
            })
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }

    /// Finish: write out, seal, wait for the thread.
    pub fn shutdown(&self) {
        // The flag is set BEFORE the command: between the thread draining the
        // queue and exiting, the queue stays alive, and without the flag
        // `try_send` would answer `Ok` to records that then die with the
        // channel.
        self.stopping
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = crossbeam_channel::bounded(1);
        if self.control.send(Control::Shutdown(tx)).is_ok() {
            let _ = rx.recv();
        }
        if let Ok(mut guard) = self.handle.lock()
            && let Some(h) = guard.take()
        {
            let _ = h.join();
        }
    }

    /// The counters shared with the store: the write path accounts in them for
    /// what it no longer tells the caller about.
    pub fn counters(&self) -> &Counters {
        &self.counters
    }

    /// Whether the writer thread is alive.
    ///
    /// Asked of the thread itself rather than of the queues: a full queue only
    /// means the disk is behind, and an empty one that there is nothing to
    /// write. Neither says whether the consumer is running.
    pub fn is_alive(&self) -> bool {
        match self.handle.lock() {
            // `None` means the thread was already joined in `shutdown`.
            Ok(guard) => guard.as_ref().is_some_and(|h| !h.is_finished()),
            // The mutex was poisoned by a panic in `shutdown`; the thread
            // itself is alive and well, and the receiving queue works.
            Err(poisoned) => poisoned
                .into_inner()
                .as_ref()
                .is_some_and(|h| !h.is_finished()),
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Channel state
// ════════════════════════════════════════════════════════════════════════════

/// The immutable attributes that go into the header of every segment of a
/// channel.
///
/// One type rather than three fields side by side: they always travel together
/// and always end up in one and the same header.
#[derive(Debug, Clone, Copy)]
struct SegmentIdentity {
    protocol_version: ProtocolVersion,
    store_id: u64,
    boot: BootCounter,
}

struct ChannelState {
    config: ChannelConfig,
    dir: PathBuf,
    /// The channel's budget group (a storage class as the writer sees it).
    group: usize,
    /// The group's medium key — a copy of [`GroupBudget::root_key`] so that the
    /// error path does not walk the table of groups.
    root_key: u8,
    /// The namespace's personal quota in bytes. `None` means the group budget
    /// only.
    quota_bytes: Option<u64>,
    /// The namespace's own number and loss counters: a loss discovered inside
    /// the writer has to be marked where the hole appeared, or it will not
    /// reach the stream as a record of its own.
    index: ChannelIdx,
    drops: Arc<DropCounters>,
    identity: SegmentIdentity,
    inventory: Inventory,
    /// The open segment. `None` means the channel has written nothing yet or
    /// gave its segment up for idleness ([`PARK_AFTER`]): a namespace that has
    /// gone quiet should hold neither a descriptor nor reserved bytes.
    segment: Option<SegmentWriter>,
    /// A segment released for idleness: the file is truncated to its data and
    /// closed, but **not sealed** — the next write continues that very one.
    ///
    /// Without this a quiet channel would start a new file on every wake-up:
    /// once an hour is eight thousand tiny segments a year, and byte-based
    /// rotation would remove none of them because there are hardly any bytes in
    /// them. The files would run out before the space did.
    parked_segment: Option<SegmentName>,
    builder: BlockBuilder,
    footer: FooterBuilder,
    /// The block serialization buffer. Reused: allocating and growing it on
    /// every flush is wasted work on a path taken thousands of times a second.
    scratch: Vec<u8>,
    /// The greatest time written: time that went backwards is pulled forward so
    /// the block index stays sorted.
    last_time: Micros,
    block_opened: Option<Instant>,
    last_sync: Instant,
    dirty_since_sync: bool,
    /// Whether the channel is listed in the writer's `active` list.
    ///
    /// A flag rather than `active.contains(..)`: the check runs on **every**
    /// record, and a linear search over a list of tens of thousands of pairs
    /// would make laying out a batch quadratic in the number of writing
    /// channels.
    is_in_active_list: bool,
    /// Since when the channel has had nothing to service. `None` means it has.
    ///
    /// Resources are given back in stages: the buffers first
    /// ([`RELEASE_AFTER`]), then the segment ([`PARK_AFTER`]).
    idle_since: Option<Instant>,
    /// Whether the buffers of this idle spell have already been given back.
    ///
    /// Without the mark the return would repeat on every turn of the loop for
    /// the whole pause before the segment is released — four times a second, to
    /// no purpose at all.
    buffers_released: bool,
}

impl ChannelState {
    fn new(
        spec: ChannelSpec,
        root_key: u8,
        identity: SegmentIdentity,
        index: ChannelIdx,
        drops: Arc<DropCounters>,
        counters: &Counters,
    ) -> Result<Self> {
        let ChannelSpec {
            dir,
            group,
            quota_bytes,
            config,
        } = spec;
        // The trace of an interrupted migration: an interruption before the
        // rename leaves a `*.tmp` whose contents nothing addresses any more.
        // Whoever comes to the directory first has to sweep it — that is,
        // registering the channel.
        crate::fsutil::sweep_tmp(&dir)?;
        let mut inventory = Inventory::scan(&dir)?;
        Self::recover_orphan(&dir, &mut inventory, identity, counters);
        let now = Instant::now();
        Ok(Self {
            // The buffer grows on the first record: a namespace that writes
            // nothing should take no memory. With twenty-four thousand
            // namespaces, reserving 64 KiB per channel up front would cost
            // gigabytes on a 32-bit target.
            builder: BlockBuilder::new(),
            config,
            dir,
            group,
            root_key,
            quota_bytes,
            index,
            drops,
            identity,
            inventory,
            segment: None,
            parked_segment: None,
            footer: FooterBuilder::new(),
            scratch: Vec::new(),
            last_time: Micros(0),
            block_opened: None,
            last_sync: now,
            dirty_since_sync: false,
            is_in_active_list: false,
            idle_since: None,
            buffers_released: true,
        })
    }

    /// Return an idle channel's memory to the allocator.
    ///
    /// A channel's buffers grow to the largest block that passed through them
    /// and, without a return, stay that way forever: one megabyte blob would
    /// pin ~2× its own size to a channel for the life of the process, and a
    /// steady 64–128 KiB per channel with the tens of thousands of channels
    /// claimed would add up to gigabytes. So on going idle the memory is given
    /// back **whole** rather than shrunk to a threshold: only a handful of
    /// channels write at any moment, and there is nothing to justify keeping
    /// empty buffers behind quiet ones.
    ///
    /// The price of the return is a reallocation on the next wake-up, but a
    /// channel does not go idle before it has synced, that is, no more often
    /// than its sync period: one allocation every few seconds per channel is
    /// invisible even on armv7.
    ///
    /// The footer's block index is not included here: it describes the **open**
    /// segment and will go on growing. Its capacity is returned after sealing,
    /// when the index is no longer needed.
    fn release_buffers(&mut self) {
        self.builder.shrink_to(0);
        self.scratch = Vec::new();
    }

    /// The bytes a channel holds in memory for blocks.
    ///
    /// Capacity is counted rather than what is occupied: the buffers grow to
    /// the largest block that passed through and stay that way until they are
    /// returned — capacity is exactly what a channel holds. `scratch` counts on
    /// equal terms with the accumulator: it holds a whole serialized block, and
    /// without it the count would be short by roughly a block.
    ///
    /// The footer's block index is deliberately not included: it describes the
    /// open segment, cannot be returned while its entries are live and goes
    /// only with sealing. The ceiling, meanwhile, is about what can be given
    /// back.
    fn held_bytes(&self) -> u64 {
        (self.builder.capacity() + self.scratch.capacity()) as u64
    }

    /// Seal a segment cut short by a previous run.
    ///
    /// The point is to give back the tail of the reserve window: an unsealed
    /// segment is counted in the channel's budget together with it, and several
    /// crash stops in a row eat the budget with emptiness, after which rotation
    /// starts on live history.
    ///
    /// **Only** a segment of a foreign run is touched. One of the current run may
    /// be open in the live state of this same process (the namespace was brought up
    /// again), and truncating it from under that would lose data. A change of run
    /// is enough for this: by the format a segment does not cross that boundary, so
    /// nobody is writing into a foreign one any more.
    ///
    /// A failed recovery is not fatal: the segment stays unsealed and will be
    /// read by scanning.
    fn recover_orphan(
        dir: &Path,
        inventory: &mut Inventory,
        identity: SegmentIdentity,
        counters: &Counters,
    ) {
        let Some(newest) = inventory.newest().cloned() else {
            return;
        };
        if newest.name.boot == identity.boot {
            return;
        }
        match crate::segment::seal_orphan(&newest.path(dir), Some(identity.store_id)) {
            Ok(Some(recovered)) => {
                inventory.update_size_bytes(recovered.name, recovered.size_bytes);
                Counters::bump(&counters.segments_sealed);
                if recovered.truncated {
                    Counters::bump(&counters.truncated_tails);
                }
            }
            Ok(None) => {}
            // A segment brought from another device is not a failure of the
            // medium but a legitimate state of the directory: someone put a
            // foreign dump here. It must not be touched, and declaring the
            // store broken over it even less so: the reader reports these files
            // honestly.
            Err(Error::ForeignSegment { .. }) => {}
            Err(_) => Counters::bump(&counters.io_errors),
        }
    }

    /// Whether it is time to flush an incomplete block.
    fn flush_deadline(&self) -> Option<Instant> {
        self.block_opened.map(|t| t + self.config.flush_interval)
    }

    /// Whether it is time to sync.
    fn sync_deadline(&self) -> Option<Instant> {
        if !self.dirty_since_sync {
            return None;
        }
        Some(self.last_sync + self.config.sync_interval)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Namespace state
// ════════════════════════════════════════════════════════════════════════════

struct NsState {
    #[allow(dead_code)]
    name: String,
    channels: Vec<ChannelState>,
    drops: Arc<DropCounters>,
}

// ════════════════════════════════════════════════════════════════════════════
// The writer loop
// ════════════════════════════════════════════════════════════════════════════

struct WriterLoop {
    /// The namespace slots. `None` means released: a [`NsId`] is an index, so a
    /// freed slot is zeroed and reused rather than removed — shifting the
    /// indices would carry records in flight into the wrong namespace.
    namespaces: Vec<Option<NsState>>,
    counters: Arc<Counters>,
    /// The source of loss notices — one for the writer's whole life:
    /// `Arc::from` allocates, and notices appear exactly when the system is
    /// under pressure.
    diag_target: Arc<str>,
    /// The reusable batch buffer.
    batch: Vec<Staged>,
    /// The value of the total loss counter at the last walk over the notices.
    ///
    /// The watchdog for a full walk over the fleet — see
    /// [`WriterLoop::emit_drop_notices`].
    drops_seen: u64,
    /// The channels that have something to service: an open block, unsynced
    /// data or an unsealed segment.
    ///
    /// Walking every channel is not an option: with the twenty-four thousand
    /// namespaces claimed there are tens of thousands of them, and a full pass
    /// on every turn of the loop would eat the CPU for nothing. Only a handful
    /// write at any moment.
    active: Vec<(usize, usize)>,
    /// The budget groups: one per storage class. Channels carry the group index
    /// ([`ChannelState::group`]); the writer does not need the class names.
    groups: Vec<GroupBudget>,
    /// The value of [`Counters::occupancy_raised`] at the last check of the
    /// group budgets.
    ///
    /// The watchdog for a full walk over the fleet, like `drops_seen` for the
    /// loss notices: the total occupancy grows **only** when a channel takes a
    /// segment into work or extends its reserve window; sealing, releasing and
    /// eviction reduce it. So an unchanged counter proves there is nothing to
    /// count.
    occupancy_seen: u64,
    /// The ceiling on the total bytes writing channels hold for blocks. `None`
    /// means there is no ceiling.
    buffer_ceiling: Option<u64>,
    /// The marks for a reader's subscription. Raised once per turn of the loop
    /// rather than per block: in one turn the writer manages a whole batch, and
    /// waking the reader more often would mean waking it for the same data.
    pulse: Arc<crate::pulse::Pulse>,
    /// The value of [`Counters::blocks_written`] at the last mark.
    pulsed_blocks: u64,
    /// The sum of the segment event counters at the last mark.
    pulsed_shape: u64,
    /// A namespace came up in this turn: directories the reader does not know
    /// about appeared in the store, and the segment counters do not show it —
    /// an empty channel has not created a file yet.
    roster_changed: bool,
    /// The bit mask of roots ([`GroupBudget::root_key`]) that refused for want
    /// of space since the last walk.
    ///
    /// Space has to be freed where it is taken, not where it was needed:
    /// rotation inside a channel that hit ENOSPC leaves a quiet channel's
    /// reserve window untouched. And on **the same medium** at that: classes
    /// may live on different partitions, and evicting on one frees no bytes on
    /// another.
    pressured_roots: u8,
}

impl WriterLoop {
    fn run(
        mut self,
        normal: Receiver<Staged>,
        critical: Receiver<Staged>,
        control: Receiver<Control>,
    ) {
        loop {
            // Critical records are taken first and in full: the ordinary stream
            // has no right to hold up critical messages.
            let mut got = self.drain(&critical);
            got += self.drain(&normal);

            if got > 0 {
                self.apply_batch();
            }

            match self.poll_control(&control, &normal, &critical) {
                ControlOutcome::Continue => {}
                ControlOutcome::Stop => break,
            }

            // Announce what was done before sleeping. A command arriving on
            // empty queues (`sync`, say) flushes the block right here, and the
            // mark would otherwise wait for the end of the turn, that is, for
            // the sleep to time out: a subscription would learn of a sync
            // seconds after it happened.
            self.publish_pulse();

            if got == 0 {
                // Wait for either a new record or the nearest deadline. An
                // overdue deadline gives a zero timeout, and a zero timeout in
                // `select!` returns immediately: the loop would burn a whole
                // core. The lower bound makes such a turn harmless.
                let timeout = self
                    .next_deadline()
                    .unwrap_or(IDLE_TIMEOUT)
                    .max(MIN_TIMEOUT);
                let mut stop = false;
                crossbeam_channel::select! {
                    recv(critical) -> item => if let Ok(item) = item { self.batch.push(item); self.apply_batch(); },
                    recv(normal) -> item => if let Ok(item) = item { self.batch.push(item); self.apply_batch(); },
                    recv(control) -> cmd => match cmd {
                        Ok(cmd) => {
                            stop = matches!(
                                self.handle_control(cmd, &normal, &critical),
                                ControlOutcome::Stop
                            );
                        }
                        Err(_) => stop = true,
                    },
                    default(timeout) => {}
                }
                if stop {
                    break;
                }
            }

            self.tick();
            self.publish_pulse();
        }

        self.finish();
        // What was written out at the stop is data too, and it has to be
        // announced before closing: a subscription has to read the stream to
        // its end rather than break off on the last batch.
        self.publish_pulse();
        // There is nobody left to wake those waiting: without this a
        // subscription to a stopped store would hang until its timeout and look
        // like a device gone quiet.
        self.pulse.close();
    }

    /// Announce to the readers what happened in this turn of the loop.
    ///
    /// It is computed from counters that already exist: separate accounting in
    /// `flush_block` would have to be threaded through all six of its callers,
    /// and a subscription does not need the difference between "there are more
    /// blocks" and "a block landed right here" — it reads the segment on from
    /// its own last position anyway.
    fn publish_pulse(&mut self) {
        let blocks = self.counters.blocks_written.load(Ordering::Relaxed);
        if blocks != self.pulsed_blocks {
            self.pulsed_blocks = blocks;
            self.pulse.data_written();
        }
        // Exactly four events change the shape of the channels, and all four
        // are already counted: a segment was created, taken into work, sealed,
        // evicted.
        let shape = self.counters.segments_created.load(Ordering::Relaxed)
            + self.counters.segments_opened.load(Ordering::Relaxed)
            + self.counters.segments_sealed.load(Ordering::Relaxed)
            + self.counters.segments_rotated.load(Ordering::Relaxed);
        if shape != self.pulsed_shape {
            self.pulsed_shape = shape;
            self.pulse.shape_changed();
        }
        // The store's roster is a separate mark for a separate reason:
        // answering it is proportional to the whole store, and it is raised
        // once in a service's life. Merging it with rotation would make a
        // subscription walk twenty-four thousand directories every half second.
        if self.roster_changed {
            self.roster_changed = false;
            self.pulse.roster_changed();
        }
    }

    /// Drain both data queues dry.
    ///
    /// Called before `sync` and `shutdown`: the commands travel on a separate
    /// queue and without this would overtake records already in the data queue.
    /// Then `sync` would report success without having written them, and
    /// `shutdown` would seal segments over what was unwritten — the records
    /// would disappear although `log()` returned `Ok`.
    fn drain_pending(
        &mut self,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
        leftovers: Leftovers,
    ) -> bool {
        self.drain_pending_rounds(normal, critical, leftovers, DRAIN_ROUNDS)
    }

    /// The same with an explicit number of passes: running out of the allotted
    /// passes cannot be reproduced by tuning the load — it depends on which
    /// thread the scheduler let run, and the behaviour at that boundary is
    /// exactly what decides whether records survive or are destroyed.
    fn drain_pending_rounds(
        &mut self,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
        leftovers: Leftovers,
        rounds: usize,
    ) -> bool {
        for _ in 0..rounds {
            let got = self.drain(critical) + self.drain(normal);
            if got == 0 {
                return true;
            }
            self.apply_batch();
        }
        // The queue is still filling faster than it drains. Waiting any longer
        // is not an option: stopping the process must not depend on whether the
        // application threads stop writing.
        if leftovers == Leftovers::Keep {
            // There will still be someone to write them — the ordinary course
            // of the loop. Throwing them away would mean destroying what was
            // accepted for the sake of an operation that gains nothing by it:
            // the queue will go on being drained regardless. The caller is told
            // the promise was not kept in full.
            return false;
        }
        // The remainder is taken by name rather than merely counted: a loss has
        // to be marked in the channel where the hole appeared, or it will not
        // reach the stream as a record of its own and will become
        // indistinguishable from silence. The number of passes is bounded by a
        // snapshot of the length: a producer writing faster must not hold up
        // the stop.
        let mut leftover = 0u64;
        for rx in [critical, normal] {
            for _ in 0..rx.len() {
                let Ok(item) = rx.try_recv() else { break };
                if let Some(ns) = self
                    .namespaces
                    .get(item.ns.0 as usize)
                    .and_then(|n| n.as_ref())
                {
                    ns.drops.record(item.channel);
                }
                leftover += 1;
            }
        }
        if leftover > 0 {
            Counters::add(&self.counters.dropped, leftover);
        }
        false
    }

    /// Take what can be taken from the queue without exceeding the limit.
    fn drain(&mut self, rx: &Receiver<Staged>) -> usize {
        let mut n = 0;
        while self.batch.len() < DRAIN_LIMIT {
            match rx.try_recv() {
                Ok(item) => {
                    self.batch.push(item);
                    n += 1;
                }
                Err(_) => break,
            }
        }
        n
    }

    /// Lay a batch out across the channels.
    fn apply_batch(&mut self) {
        // Sorting by time within a channel: records from different threads
        // arrive reordered (a thread took its stamp and was preempted), while
        // the block and the index have to stay monotonic. The sort is stable —
        // a SpanStart will not overtake a simultaneous SpanEnd.
        //
        // The "already sorted" check is no ornament: a stable sort allocates a
        // temporary buffer for half the batch, that is, up to a hundred and
        // fifty kilobytes per go. A batch arrives ordered almost always — the
        // queue is FIFO and time is monotonic — and a linear check spares that
        // allocation in the common case.
        let key = |s: &Staged| (s.ns.0, s.channel.0, s.at.0);
        if !self.batch.is_sorted_by_key(key) {
            self.batch.sort_by_key(key);
        }

        let batch = std::mem::take(&mut self.batch);
        for item in &batch {
            if let Err(e) = self.push(item) {
                // Logging is not an option — the queue is our own; nor is
                // falling over: a failure of the medium must not take the whole
                // logging mechanism with it, the other channels included. Count
                // it and move on; the error is visible through
                // `Stats::io_errors`.
                Counters::bump(&self.counters.io_errors);
                // Out of space: the channel already tried to free some of its
                // own and could not. What is needed next is a view of the whole
                // medium, and only the walk has one — see `enforce_limits`.
                if e.is_no_space() {
                    self.note_pressure(item.ns.0 as usize, item.channel.0 as usize);
                }
            }
        }
        self.batch = batch;
        self.batch.clear();

        // Group commit: the critical channel syncs ONCE per batch. Syncing per
        // record, as it was at first, turned a burst of five hundred critical
        // messages into five hundred blocks and five hundred fdatasyncs —
        // seconds of writing and needless flash wear where one trip to the
        // medium suffices.
        let counters = Arc::clone(&self.counters);
        let mut pressured = 0u8;
        for &(ns_idx, ch_idx) in &self.active {
            let Some(ch) = self
                .namespaces
                .get_mut(ns_idx)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch_idx))
            else {
                continue;
            };
            if ch.config.sync_interval == Duration::ZERO && ch.dirty_since_sync {
                let done = Self::flush_block(ch, &counters)
                    .and_then(|()| Self::sync_channel(ch, &counters));
                if let Err(e) = done {
                    Counters::bump(&counters.io_errors);
                    if e.is_no_space() {
                        pressured |= 1 << ch.root_key.min(7);
                    }
                }
            }
        }
        self.pressured_roots |= pressured;
    }

    fn push(&mut self, item: &Staged) -> Result<()> {
        let ns_idx = item.ns.0 as usize;
        let ch_idx = item.channel.0 as usize;
        let exists = self
            .namespaces
            .get(ns_idx)
            .and_then(|n| n.as_ref())
            .is_some_and(|n| ch_idx < n.channels.len());
        if !exists {
            // The address does not exist, or the namespace has already been
            // released — there is nowhere to write. Silence is not an option:
            // this is the caller's mistake and it has to be visible in the
            // counters.
            Counters::bump(&self.counters.dropped);
            return Ok(());
        }

        let ch = &mut self.namespaces[ns_idx]
            .as_mut()
            .expect("presence was checked above")
            .channels[ch_idx];

        // Monotonicity within a channel: time from the past is pulled forward.
        // It is computed BEFORE the block is opened: a new segment's name and
        // base have to be the time of its first record, as the format requires,
        // rather than the time of the previous one (zero for a new channel).
        let at = Micros(item.at.0.max(ch.last_time.0));
        ch.last_time = at;

        // A record that never reached the accumulator (no room for a segment,
        // it failed to encode) is lost, and it has to be accounted for where
        // the hole appeared: `io_errors` would say "something broke" but not
        // "how many records went missing", and a notice will not reach the
        // stream without a per-channel counter.
        if ch.builder.is_empty() {
            if let Err(e) = Self::ensure_room(ch, at, &self.counters) {
                Counters::bump(&self.counters.dropped);
                ch.drops.record(ch.index);
                return Err(e);
            }
            ch.block_opened = Some(Instant::now());
        }

        if let Err(e) = ch.builder.push(at, &item.record.as_record()) {
            Counters::bump(&self.counters.dropped);
            ch.drops.record(ch.index);
            return Err(e.into());
        }
        Counters::bump(&self.counters.records_written);
        // The type sets in the footer: a migration decides from them whether to
        // rewrite the segment, and a reader what is in the segment at all.
        let (event, metric) = item.footer_ids();
        if let Some(id) = event {
            ch.footer.add_event(id);
        }
        if let Some(id) = metric {
            ch.footer.add_metric(id);
        }
        ch.dirty_since_sync = true;
        // The channel has work again: the idleness count will restart when it
        // once more has nothing to service.
        ch.idle_since = None;
        ch.buffers_released = false;
        if !ch.is_in_active_list {
            ch.is_in_active_list = true;
            self.active.push((ns_idx, ch_idx));
        }

        if ch.builder.raw_len() >= ch.config.block_max_bytes {
            Self::flush_block(ch, &self.counters)?;
        }
        Ok(())
    }

    /// Release an idle channel's segment while keeping the option of continuing
    /// it.
    ///
    /// The file is truncated to its actual data (`ftruncate` after `fdatasync`)
    /// and closed — both the descriptor and the unwritten tail of the reserve
    /// window come back. The footer is **not** written: the block index stays
    /// in memory and will be finished when the channel wakes. If it does not
    /// wake before the process stops, the segment stays unsealed — it reads by
    /// scanning, and `recover_orphan` will append its footer on the next start.
    fn park_segment(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        // What has accumulated goes to disk before the file is released. The
        // normal path arrives here with the block already flushed, but there is
        // no reason to rely on that: an empty accumulator makes the call free.
        Self::flush_block(ch, counters)?;
        let Some(seg) = ch.segment.take() else {
            return Ok(());
        };
        let name = SegmentName::new(seg.header().boot, seg.header().base);
        let data_end = seg.data_end();
        seg.close_unsealed()?;
        // The budget has to see the return: an unsealed segment was counted in
        // it together with the unwritten tail of its window.
        ch.inventory.update_size_bytes(name, data_end);
        ch.parked_segment = Some(name);
        Ok(())
    }

    /// Bring a released segment back into work. `false` means it could not be.
    ///
    /// The walk over the file at open time is not superfluous: nobody watched
    /// the file between the release and the return, and restoring the position
    /// by walking is an already written and tested path. It is paid exactly
    /// once per wake-up and only by the channels that were quiet — that is, by
    /// those whose segment is small. The return asks for no space: the first
    /// write will extend the reserve window.
    fn unpark(ch: &mut ChannelState, counters: &Counters) -> bool {
        let Some(name) = ch.parked_segment.take() else {
            return false;
        };
        let path = ch.dir.join(name.to_string());
        match SegmentWriter::reopen(
            &path,
            Some(ch.identity.store_id),
            Some(ch.config.segment_bytes),
        ) {
            Ok(seg) => {
                // Opening asked for no space: the file stayed truncated to the
                // end of the data, and the first write will restore the window.
                // But the segment was counted in the budget as truncated, and
                // it still has to be checked against the medium — recovery may
                // have discarded a tail.
                ch.inventory.update_size_bytes(name, seg.capacity());
                ch.segment = Some(seg);
                Counters::bump(&counters.segments_opened);
                true
            }
            Err(_) => {
                // The file is gone (evicted, removed from outside) or would not
                // open. No matter: a new one will be started, and the old index
                // is of no more use.
                Counters::bump(&counters.io_errors);
                ch.inventory.remove(name);
                ch.footer.reset();
                ch.footer.discard_pending();
                false
            }
        }
    }

    /// Make sure there is room in the active segment for a whole block.
    fn ensure_room(ch: &mut ChannelState, at: Micros, counters: &Counters) -> Result<()> {
        let need = ch.config.block_max_bytes as u64 + BlockHeader::SIZE as u64 * 2;

        if ch.segment.is_none() && ch.parked_segment.is_some() {
            Self::unpark(ch, counters);
        }
        if let Some(seg) = &ch.segment
            && seg.fits(need)
        {
            return Ok(());
        }
        if ch.segment.is_some() {
            Self::seal_segment(ch, counters)?;
        }
        Self::open_segment(ch, at, counters)
    }

    fn open_segment(ch: &mut ChannelState, at: Micros, counters: &Counters) -> Result<()> {
        let SegmentIdentity {
            protocol_version,
            store_id,
            boot,
        } = ch.identity;
        // A segment's name is (boot, the time of its first record). Names can
        // coincide only when time goes backwards; shift by a microsecond so as
        // not to overwrite an existing file.
        let mut base = at;
        for attempt in 0..64 {
            let header = SegmentHeader {
                protocol_version,
                boot,
                base,
                store_id,
            };
            match SegmentWriter::create(&ch.dir, header, ch.config.segment_bytes) {
                Ok(seg) => {
                    // In the budget a segment is counted as what it took on the
                    // medium — its first reserve window, not its growth limit.
                    // Counting the limit would mean evicting someone else's
                    // history for the sake of emptiness, and across a fleet it
                    // would peg the practical number of channels writing at
                    // once to `budget_bytes / segment_bytes`.
                    ch.inventory.push_newest(SegmentEntry {
                        name: SegmentName::new(boot, base),
                        size_bytes: seg.capacity(),
                    });
                    ch.segment = Some(seg);
                    ch.footer.reset();
                    Counters::bump(&counters.segments_created);
                    Counters::bump(&counters.segments_opened);
                    // Occupancy grew by the first reserve window — the budget
                    // has to be recomputed. Bringing a released segment back
                    // (`unpark`), by contrast, asks for no space: the file
                    // stays truncated, and it is counted as what it was counted
                    // as before.
                    Counters::bump(&counters.occupancy_raised);
                    // The new segment took space — free the old ones.
                    Self::rotate(ch, counters)?;
                    return Ok(());
                }
                Err(Error::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::AlreadyExists =>
                {
                    base = Micros(base.0.saturating_add(1));
                    ch.last_time = base;
                }
                Err(e) if e.is_no_space() => {
                    // Out of space: delete the oldest segment and try again.
                    // Without this the channel would freeze forever, although
                    // freeing space is its own job.
                    if attempt > 8 {
                        return Err(e);
                    }
                    let freed = Self::rotate_one(ch, counters)?;
                    if !freed {
                        return Err(e);
                    }
                }
                Err(e) => return Err(e),
            }
        }
        Err(Error::Corrupt {
            path: ch.dir.clone(),
            reason: "could not find a name for the new segment".to_owned(),
        })
    }

    fn flush_block(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        // The mark of an open block is cleared FIRST of all, before any early
        // return and before a possible write error. Otherwise an overdue
        // deadline stays forever, `next_deadline` returns a zero timeout, and
        // the writer's loop turns into a busy loop on a whole core.
        ch.block_opened = None;

        if ch.builder.is_empty() {
            return Ok(());
        }
        let Some(seg) = ch.segment.as_mut() else {
            // There is nowhere to put the assembled block: no segment is open.
            // This should not happen (`ensure_room` opens it), but the loss
            // still has to be shown rather than answered with `Ok` — and shown
            // where it appeared, so that the notice reaches this channel's
            // stream.
            let lost = u64::from(ch.builder.count());
            Counters::add(&counters.dropped, lost);
            ch.drops.record_n(ch.index, lost);
            ch.builder.reset();
            ch.footer.discard_pending();
            return Ok(());
        };

        let mut out = std::mem::take(&mut ch.scratch);
        out.clear();
        let last = ch.builder.last().unwrap_or(ch.last_time);
        let seq = seg.next_seq();
        // The record count is taken BEFORE `finish`: on exceeding the body
        // ceiling it resets the accumulator, and there would be nothing left to
        // count the loss with.
        let pending = ch.builder.count();
        let header = match ch.builder.finish(seq, ch.config.compression, &mut out) {
            Ok(h) => h,
            Err(e) => {
                ch.scratch = out;
                // The block did not assemble — its records are lost, and that
                // has to be visible where the hole appeared. One `io_errors`
                // would say "something broke" but not "how much went missing",
                // and a notice will not reach the stream without a per-channel
                // counter.
                let lost = u64::from(pending);
                Counters::add(&counters.dropped, lost);
                ch.drops.record_n(ch.index, lost);
                // The accumulator is reset here too: `finish` does that only on
                // exceeding the ceiling, and a loaded accumulator would jam the
                // channel on the same error at every following flush.
                ch.builder.reset();
                ch.footer.discard_pending();
                return Err(e.into());
            }
        };

        // From here on every outcome goes through one point where the buffer
        // comes back: four early `?` used to drop `out` on the floor — along
        // with the capacity the next flush would reallocate — while the block's
        // records disappeared with an `io_errors` alone, unaccounted for in the
        // losses and with no notice in the channel's stream.
        let placed = Self::place_block(ch, counters, &mut out, &header);
        let written = out.len() as u64;
        ch.scratch = out;

        match placed {
            Ok(Placement::At(offset)) => {
                ch.footer.add_block(offset, &header, last);
                Counters::bump(&counters.blocks_written);
                Counters::add(&counters.bytes_written, written);
                Ok(())
            }
            Ok(Placement::Dropped) => {
                // The block will be in no segment at all — its types must
                // settle in the sets of neither.
                ch.footer.discard_pending();
                // An oversized block inflated the buffers to a size the channel
                // will not see again — there is no reason to keep them behind
                // it.
                ch.release_buffers();
                Ok(())
            }
            Err(e) => {
                let lost = u64::from(header.count);
                Counters::add(&counters.dropped, lost);
                ch.drops.record_n(ch.index, lost);
                ch.footer.discard_pending();
                Err(e)
            }
        }
    }

    /// Put an assembled block into a segment, changing segment if need be.
    ///
    /// An error means the block is lost: accounting for the loss and returning
    /// the buffer are the caller's business, and the buffer stays with it.
    fn place_block(
        ch: &mut ChannelState,
        counters: &Counters,
        out: &mut [u8],
        header: &BlockHeader,
    ) -> Result<Placement> {
        // The check in `ensure_room` is computed from `block_max_bytes`, but
        // one large record may have crossed the threshold: writing past the
        // growth limit is not allowed — it is the rotation boundary, and the
        // next segment begins beyond it.
        let fits = ch
            .segment
            .as_ref()
            .is_some_and(|seg| seg.fits(out.len() as u64));
        if !fits {
            Self::seal_segment(ch, counters)?;
            Self::open_segment(ch, header.base, counters)?;
            let next_seq = {
                let seg = ch.segment.as_ref().ok_or(Error::WriterDead)?;
                // A fresh segment may not fit the block either: one record can
                // be larger than a whole segment (an incompressible blob).
                // Growing the window past the limit would cancel the rotation
                // boundary: a segment would grow for one record without end,
                // while the class budget evicts only whole segments. The block
                // is discarded and the loss is announced by a notice in the
                // stream.
                if !seg.fits(out.len() as u64) {
                    let lost = u64::from(header.count);
                    Counters::add(&counters.dropped, lost);
                    ch.drops.record_n(ch.index, lost);
                    return Ok(Placement::Dropped);
                }
                seg.next_seq()
            };
            // Block numbering restarts in a new segment.
            dduroc_format::restamp_seq(out, next_seq)?;
        }
        let before = ch.segment.as_ref().ok_or(Error::WriterDead)?.capacity();
        let mut placed = ch
            .segment
            .as_mut()
            .ok_or(Error::WriterDead)?
            .append_block(out);
        // The window did not extend — the medium is out of space. First the
        // channel frees its own, as when creating a segment: a failure to
        // extend the window is no different from a failure to create a file,
        // and answering it by losing a block without having tried to give up
        // its own history would be inconsistent.
        //
        // Exactly one attempt: eviction gives up a whole segment while what is
        // asked for is an eighth of one. If there is still no room after that,
        // the channel is not the problem, and what is needed next is a view of
        // the whole medium (`enforce_limits`).
        //
        // An error from the eviction itself is swallowed deliberately: what has
        // to go up is the original cause — running out of space — not the fact
        // that a file could not be deleted as well. That is already counted in
        // `io_errors`.
        if placed.as_ref().err().is_some_and(Error::is_no_space)
            && Self::rotate_one(ch, counters).unwrap_or(false)
        {
            placed = ch
                .segment
                .as_mut()
                .ok_or(Error::WriterDead)?
                .append_block(out);
        }
        let offset = placed?;
        let seg = ch.segment.as_mut().ok_or(Error::WriterDead)?;
        // The reserve window may have been extended for this block — then
        // occupancy of the medium grew, and the budget has to see the growth
        // where it happened. Comparing rather than writing unconditionally is
        // not thrift but a condition: `update_size_bytes` looks the segment up
        // by name, and this is called on every block.
        if seg.capacity() != before {
            let name = SegmentName::new(seg.header().boot, seg.header().base);
            let grown = seg.capacity();
            ch.inventory.update_size_bytes(name, grown);
            Counters::bump(&counters.occupancy_raised);
            // A namespace's personal quota is a limit on what is occupied, and
            // it has to be checked where what is occupied grows. Creating a
            // segment alone is not enough for that: between two creations the
            // window goes from one block to a whole segment, and the channel
            // would be over its quota the whole time.
            //
            // An error from the rotation does not mean the block was lost — it
            // is already on the medium — so it does not go up: returning it
            // would mean declaring records lost that have been written.
            if Self::rotate(ch, counters).is_err() {
                Counters::bump(&counters.io_errors);
            }
        }
        Ok(Placement::At(offset))
    }

    fn sync_channel(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        if let Some(seg) = ch.segment.as_mut()
            && seg.is_dirty()
        {
            seg.sync()?;
            Counters::bump(&counters.syncs);
        }
        ch.last_sync = Instant::now();
        ch.dirty_since_sync = false;
        Ok(())
    }

    fn seal_segment(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        Self::flush_block(ch, counters)?;
        let Some(seg) = ch.segment.take() else {
            // A released segment will stay unsealed. There is no reason to open
            // it for a footer: the footer is a read optimization, the segment
            // reads by scanning without one, and on the next start
            // `recover_orphan` will seal it in the same walk it uses to find
            // the end of the data. Otherwise stopping would cost an open and a
            // walk for every quiet channel — tens of thousands at the scale
            // claimed.
            return Ok(());
        };
        let name = SegmentName::new(seg.header().boot, seg.header().base);
        let data_end = seg.data_end();
        let footer = ch.footer.build();
        let footer_bytes = footer.len() as u64;
        seg.seal(&footer)?;
        // A sealed file is truncated to its actual data — the budget has to
        // account for that, or rotation would treat the reserve as permanent.
        ch.inventory
            .update_size_bytes(name, data_end + footer_bytes);
        ch.footer.reset();
        ch.dirty_since_sync = false;
        Counters::bump(&counters.segments_sealed);
        Ok(())
    }

    /// The segment a channel is writing to right now or will go on writing to:
    /// it must be evicted by neither rotation nor the store ceiling.
    fn live_segment(ch: &ChannelState) -> Option<SegmentName> {
        ch.segment
            .as_ref()
            .map(|s| SegmentName::new(s.header().boot, s.header().base))
            .or(ch.parked_segment)
    }

    /// Keep a channel within the namespace's personal quota, if one is set.
    ///
    /// Without a quota there is no per-channel rotation at all: the limit is
    /// held by the group budget, and the oldest is evicted across the whole
    /// class (`enforce_groups`).
    ///
    /// Called at both points where occupancy grows: when a segment is created
    /// and when the reserve window is extended.
    fn rotate(ch: &mut ChannelState, counters: &Counters) -> Result<()> {
        let Some(quota_bytes) = ch.quota_bytes else {
            return Ok(());
        };
        let live = Self::live_segment(ch);
        let removed = ch.inventory.enforce_budget(&ch.dir, quota_bytes, live)?;
        Counters::add(&counters.segments_rotated, removed as u64);
        Ok(())
    }

    /// Delete exactly one oldest segment. Used when the medium has run out of
    /// space.
    fn rotate_one(ch: &mut ChannelState, counters: &Counters) -> Result<bool> {
        let live = Self::live_segment(ch);
        let Some(oldest) = ch.inventory.oldest().cloned() else {
            return Ok(false);
        };
        if Some(oldest.name) == live {
            return Ok(false);
        }
        crate::fsutil::remove_synced(&oldest.path(&ch.dir))?;
        ch.inventory.remove(oldest.name);
        Counters::bump(&counters.segments_rotated);
        Ok(true)
    }

    /// Each group's occupancy: one pass over the fleet.
    ///
    /// A full pass is expensive with tens of thousands of channels — so it is
    /// called only when the sum really could have grown (see `occupancy_seen`)
    /// rather than on every turn of the loop.
    fn group_totals(&self) -> Vec<u64> {
        let mut totals = vec![0u64; self.groups.len()];
        for ch in self
            .namespaces
            .iter()
            .flatten()
            .flat_map(|ns| ns.channels.iter())
        {
            if let Some(t) = totals.get_mut(ch.group) {
                *t += ch.inventory.total_bytes();
            }
        }
        totals
    }

    /// The largest segment among the channels of medium `root`.
    ///
    /// A measure of how much has to be freed for a write attempt to stand a
    /// chance. There is no reason to take less than a whole segment: a channel
    /// under space pressure has already tried its own rotation, and what is
    /// freed has to leave room for its growth rather than for one block —
    /// otherwise every turn would cost one more lost record.
    fn max_segment_bytes_on(&self, root: u8) -> u64 {
        self.namespaces
            .iter()
            .flatten()
            .flat_map(|ns| ns.channels.iter())
            .filter(|ch| ch.root_key == root)
            .map(|ch| ch.config.segment_bytes)
            .max()
            .unwrap_or(0)
    }

    /// Note a refusal of the medium for want of space at channel `(ns,
    /// channel)`.
    fn note_pressure(&mut self, ns_idx: usize, ch_idx: usize) {
        if let Some(ch) = self
            .namespaces
            .get(ns_idx)
            .and_then(|n| n.as_ref())
            .and_then(|n| n.channels.get(ch_idx))
        {
            self.pressured_roots |= 1 << ch.root_key.min(7);
        }
    }

    /// Delete the oldest segment among the channels that pass the `pick`
    /// filter. Returns the bytes freed; `None` means there is nothing to evict.
    ///
    /// The order is global by construction: a segment's name is the pair `(run
    /// number, microseconds since it started)`, and the run number is one for
    /// the whole store. So "the oldest" is meaningful across a channel boundary
    /// and across a namespace boundary alike.
    ///
    /// A live segment is untouchable — both the open one and one released for
    /// idleness: the first is being written to, the second will be continued.
    fn evict_oldest_where(&mut self, pick: impl Fn(&ChannelState) -> bool) -> Option<u64> {
        let mut victim: Option<(usize, usize, SegmentEntry)> = None;
        for (ns_idx, slot) in self.namespaces.iter().enumerate() {
            let Some(ns) = slot.as_ref() else { continue };
            for (ch_idx, ch) in ns.channels.iter().enumerate() {
                if !pick(ch) {
                    continue;
                }
                let Some(oldest) = ch.inventory.oldest() else {
                    continue;
                };
                if Some(oldest.name) == Self::live_segment(ch) {
                    continue;
                }
                if victim.as_ref().is_none_or(|(_, _, b)| oldest.name < b.name) {
                    victim = Some((ns_idx, ch_idx, oldest.clone()));
                }
            }
        }

        let (ns_idx, ch_idx, entry) = victim?;
        let ch = &mut self.namespaces[ns_idx].as_mut()?.channels[ch_idx];
        if crate::fsutil::remove_synced(&entry.path(&ch.dir)).is_err() {
            // It could not be deleted — evicting this segment makes no sense
            // any more: the next call would pick it again and spin.
            Counters::bump(&self.counters.io_errors);
            return None;
        }
        ch.inventory.remove(entry.name);
        Counters::bump(&self.counters.segments_rotated);
        Some(entry.size_bytes)
    }

    /// Release the segments of channels quiet for at least `quiet_for`, ahead
    /// of time.
    ///
    /// It returns the unwritten tail of the reserve window: the file is
    /// truncated to its actual data. This is the answer to "a quiet channel
    /// holds space while a noisy one starves" — what is occupied but unused is
    /// given up first, before eviction starts on anyone's history.
    ///
    /// A full pass over the fleet, so it is called only under pressure: when
    /// space has run out or a group's budget is not being met.
    fn park_idle(&mut self, quiet_for: Duration) -> usize {
        let now = Instant::now();
        let counters = Arc::clone(&self.counters);
        let mut parked = 0;
        for ns in self.namespaces.iter_mut().flatten() {
            for ch in &mut ns.channels {
                if ch.segment.is_none()
                    || !ch
                        .idle_since
                        .is_some_and(|t| now.duration_since(t) >= quiet_for)
                {
                    continue;
                }
                if Self::park_segment(ch, &counters).is_err() {
                    Counters::bump(&counters.io_errors);
                    continue;
                }
                ch.footer.shrink_to_fit();
                // A channel that gave up its segment needs its block buffers
                // even less. This used to be reached only under space pressure,
                // and the memory was not returned at all: an ordinary turn of
                // the loop with its staircase of idleness still had to be
                // survived first.
                ch.release_buffers();
                ch.buffers_released = true;
                parked += 1;
            }
        }
        parked
    }

    /// Keep the store within its declared limits.
    ///
    /// Both reasons for getting here are rare, and both call for a view wider
    /// than one channel: a per-channel quota by construction cannot see that
    /// the space is taken by a neighbour.
    fn enforce_limits(&mut self) {
        let pressured = std::mem::take(&mut self.pressured_roots);
        if pressured != 0 {
            // The medium has run out of space. Rotation inside the channel that
            // hit it has already been tried and did not help — free space where
            // it is actually taken, and on THE SAME medium: first the unwritten
            // tails of quiet channels' windows, then the oldest history.
            //
            // What has to be freed is not "something" but at least a whole
            // segment: less will not do for the next attempt, and every turn
            // would cost one more lost record.
            self.park_idle(RELEASE_AFTER);
            for root in 0..8u8 {
                if pressured & (1 << root) == 0 {
                    continue;
                }
                let need = self.max_segment_bytes_on(root);
                let mut freed = 0;
                while freed < need {
                    let Some(f) = self.evict_oldest_where(|ch| ch.root_key == root) else {
                        break;
                    };
                    freed += f;
                }
            }
        }

        // The sum grows only when a channel takes a segment into work or
        // extends its reserve window; while the counter has not moved there is
        // no reason to walk the fleet.
        let raised = self
            .counters
            .occupancy_raised
            .load(std::sync::atomic::Ordering::Relaxed);
        if raised == self.occupancy_seen && pressured == 0 {
            return;
        }
        self.occupancy_seen = raised;
        self.enforce_groups();
    }

    /// Bring every group back under its budget — unconditionally, with no
    /// watchdog.
    fn enforce_groups(&mut self) {
        let mut totals = self.group_totals();
        let mut parked = false;
        for g in 0..self.groups.len() {
            let budget_bytes = self.groups[g].budget_bytes;
            totals[g] = self.evict_group_down_to(g, budget_bytes, totals[g]);
            if totals[g] > budget_bytes && !parked {
                // There is nothing left to evict, but the unwritten tails of
                // quiet channels' windows remain — giving them up is cheaper
                // than declaring the budget unmeetable.
                parked = true;
                if self.park_idle(RELEASE_AFTER) > 0 {
                    totals = self.group_totals();
                    totals[g] = self.evict_group_down_to(g, budget_bytes, totals[g]);
                }
            }
            if totals[g] > budget_bytes {
                // Everything left is live segments: they cannot be evicted, and
                // the group's budget in this configuration is simply
                // unmeetable.
                Counters::bump(&self.counters.budget_overruns);
            }
        }
    }

    /// Evict the oldest in a group until its sum fits the budget.
    fn evict_group_down_to(&mut self, group: usize, budget_bytes: u64, mut total: u64) -> u64 {
        while total > budget_bytes {
            let Some(freed) = self.evict_oldest_where(|ch| ch.group == group) else {
                break;
            };
            total = total.saturating_sub(freed);
        }
        total
    }

    /// The nearest deadline — over the channels that have something to service
    /// only.
    ///
    /// Walking every channel is not an option: with the twenty-four thousand
    /// namespaces claimed there are tens of thousands of them, and this is
    /// called on **every** idle turn of the loop, that is, up to four times a
    /// second. Only a channel with an open block or unsynced data has a
    /// deadline — that is, exactly the one already in `active`.
    fn next_deadline(&self) -> Option<Duration> {
        let now = Instant::now();
        let mut best: Option<Instant> = None;
        for &(ns_idx, ch_idx) in &self.active {
            let Some(ch) = self
                .namespaces
                .get(ns_idx)
                .and_then(|n| n.as_ref())
                .and_then(|n| n.channels.get(ch_idx))
            else {
                continue;
            };
            for d in [ch.flush_deadline(), ch.sync_deadline()]
                .into_iter()
                .flatten()
            {
                best = Some(best.map_or(d, |b: Instant| b.min(d)));
            }
        }
        best.map(|d| d.saturating_duration_since(now))
    }

    /// Service the deadlines and the loss notices.
    fn tick(&mut self) {
        self.emit_drop_notices();

        let now = Instant::now();
        let counters = Arc::clone(&self.counters);
        let mut pressured = 0u8;
        let active = std::mem::take(&mut self.active);
        for &(ns_idx, ch_idx) in &active {
            let Some(ch) = self
                .namespaces
                .get_mut(ns_idx)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch_idx))
            else {
                continue;
            };

            if ch.flush_deadline().is_some_and(|d| d <= now)
                && let Err(e) = Self::flush_block(ch, &counters)
            {
                Counters::bump(&counters.io_errors);
                if e.is_no_space() {
                    pressured |= 1 << ch.root_key.min(7);
                }
            }
            if ch.sync_deadline().is_some_and(|d| d <= now)
                && let Err(e) = Self::sync_channel(ch, &counters)
            {
                Counters::bump(&counters.io_errors);
                if e.is_no_space() {
                    pressured |= 1 << ch.root_key.min(7);
                }
            }

            // A channel stays in the list only while it has something to
            // service: an open block or a sync deadline. Checking the raw
            // `dirty_since_sync` is not an option — on a Relaxed channel it is
            // not cleared until the seal, and the channel would live in the
            // list forever, bringing back the full walk the list exists to
            // remove.
            if ch.block_opened.is_some() || ch.sync_deadline().is_some() {
                ch.idle_since = None;
                self.active.push((ns_idx, ch_idx));
            } else {
                // Idleness is given up in two stages, at different prices.
                //
                // The buffers first: their capacity is the imprint of the
                // largest block, and holding it behind a quiet channel means
                // pinning a peak forever. But not instantly: an Immediate
                // channel gets here after every group commit (see
                // `RELEASE_AFTER`), and returning them on the spot would cost a
                // couple of allocations per critical record.
                //
                // Then the segment: it costs a descriptor and is counted in the
                // budget together with the unwritten tail of its window,
                // although what is written in it may be a hundred bytes (see
                // `PARK_AFTER`). Releasing truncates the file to its actual
                // data and closes the descriptor; a channel leaves the serviced
                // list only here — otherwise there would be nobody left to give
                // the segment up.
                let idle_since = *ch.idle_since.get_or_insert(now);
                let idle = now.duration_since(idle_since);
                if idle >= RELEASE_AFTER && !ch.buffers_released {
                    ch.release_buffers();
                    ch.buffers_released = true;
                }
                if idle >= PARK_AFTER {
                    if Self::park_segment(ch, &counters).is_err() {
                        Counters::bump(&counters.io_errors);
                    }
                    ch.footer.shrink_to_fit();
                    ch.is_in_active_list = false;
                    ch.idle_since = None;
                } else {
                    self.active.push((ns_idx, ch_idx));
                }
            }
        }

        self.pressured_roots |= pressured;
        // Right at the end: both servicing the deadlines and sealing for
        // idleness have just changed the occupancy of the medium.
        self.enforce_limits();
        // After the staircase of idleness: it may have freed everything
        // already, and the ceiling must not take buffers from those already
        // giving them up.
        self.enforce_memory();
    }

    /// The bytes the channels hold in memory for blocks.
    ///
    /// Only `active` is walked — and that is enough: a channel leaves the
    /// serviced list only when its segment is sealed for idleness, and by then
    /// its buffers have already been returned. A full pass over the fleet here
    /// would mean tens of thousands of channels on every turn of the loop —
    /// exactly what this list exists to remove.
    fn held_bytes(&self) -> u64 {
        self.active
            .iter()
            .filter_map(|&(ns, ch)| {
                self.namespaces
                    .get(ns)?
                    .as_ref()?
                    .channels
                    .get(ch)
                    .map(ChannelState::held_bytes)
            })
            .sum()
    }

    /// Bring the block buffers back under the declared memory ceiling.
    ///
    /// The ceiling is honoured **between turns of the loop** rather than on
    /// every record, and that is not a concession but the only honest option.
    /// The block-closing threshold is checked AFTER a record is added, so one
    /// record can inflate a buffer past any ceiling (an incompressible blob
    /// larger than a segment is the known case), and discarding it for the sake
    /// of memory accounting would mean losing data where memory is merely
    /// inconvenient. An unmeetable ceiling is announced by a counter — as an
    /// unmeetable budget is.
    ///
    /// Freeing goes from the largest down: one operation gives the most, and
    /// each costs a block flush. The order is mandatory — `shrink_to` refuses
    /// to shrink a loaded accumulator, so it is flush first, then the return.
    fn enforce_memory(&mut self) {
        let Some(ceiling) = self.buffer_ceiling else {
            return;
        };
        let mut held = self.held_bytes();
        if held <= ceiling {
            return;
        }

        let mut by_size: Vec<(usize, usize, u64)> = self
            .active
            .iter()
            .filter_map(|&(ns, ch)| {
                let state = self.namespaces.get(ns)?.as_ref()?.channels.get(ch)?;
                Some((ns, ch, state.held_bytes()))
            })
            .collect();
        by_size.sort_unstable_by_key(|&(_, _, held)| std::cmp::Reverse(held));

        let counters = Arc::clone(&self.counters);
        for (ns, ch, was) in by_size {
            if held <= ceiling {
                break;
            }
            let Some(state) = self
                .namespaces
                .get_mut(ns)
                .and_then(|n| n.as_mut())
                .and_then(|n| n.channels.get_mut(ch))
            else {
                continue;
            };
            // There is nowhere to flush to: the segment is released or never
            // opened, and a flush would discard the whole block, counting its
            // records as lost. Memory is not worth that price. An empty
            // accumulator is not bound by this: it has nothing to lose, and it
            // does hold capacity.
            if state.segment.is_none() && !state.builder.is_empty() {
                continue;
            }
            if let Err(e) = Self::flush_block(state, &counters) {
                Counters::bump(&counters.io_errors);
                // Out of space — it has to be freed where it is taken, and the
                // walk over the media has to learn about that.
                if e.is_no_space() {
                    self.pressured_roots |= 1 << state.root_key.min(7);
                }
            }
            state.release_buffers();
            state.buffers_released = true;
            // The block index's slack along the way: it loses no live entries
            // and is given up for free.
            state.footer.shrink_to_fit();
            held = held.saturating_sub(was.saturating_sub(state.held_bytes()));
        }

        if held > ceiling {
            Counters::bump(&counters.buffer_overruns);
        }
    }

    /// Insert notices about lost records into the stream.
    ///
    /// A hole nobody mentions is indistinguishable from silence — exactly the
    /// prototype defect this accounting exists for.
    fn emit_drop_notices(&mut self) {
        // A walk over every channel of every namespace is tens of thousands of
        // atomic exchanges at the scale claimed, and it is called on every turn
        // of the loop, that is, up to four times a second even when entirely
        // idle. Precisely the full pass the `active` list exists to remove.
        //
        // Every per-channel loss comes with an increment of the total counter —
        // otherwise it would not reach `Stats` — so an unchanged total proves
        // there is nothing to walk for. One atomic read instead of a pass over
        // the fleet.
        //
        // The read acquires: the per-channel counters grow on application
        // threads BEFORE the total is published ([`Counters::publish`]), and
        // seeing the new total means seeing them too. With `Relaxed` a walk
        // could find a per-channel counter still zero, consider the notice
        // issued and leave the hole unannounced until the next loss.
        let total = self
            .counters
            .dropped
            .load(std::sync::atomic::Ordering::Acquire);
        if total == self.drops_seen {
            return;
        }
        self.drops_seen = total;

        let mut notices: Vec<(NsId, ChannelIdx, u64, Micros)> = Vec::new();
        for (ns_idx, slot) in self.namespaces.iter().enumerate() {
            let Some(ns) = slot.as_ref() else { continue };
            for ch_idx in 0..ns.channels.len() {
                let channel = ChannelIdx(ch_idx as u16);
                let count = ns.drops.take(channel);
                if count > 0 {
                    notices.push((
                        NsId(ns_idx as u32),
                        channel,
                        count,
                        ns.channels[ch_idx].last_time,
                    ));
                }
            }
        }

        for (ns, channel, count, at) in notices {
            let item = Staged {
                ns,
                channel,
                at,
                record: StagedRecord::Text {
                    level: Level::Error,
                    span: None,
                    target: Arc::clone(&self.diag_target),
                    text: crate::diag::drop_notice(count).into_boxed_str(),
                },
            };
            if self.push(&item).is_err() {
                Counters::bump(&self.counters.io_errors);
            }
        }
    }

    fn poll_control(
        &mut self,
        control: &Receiver<Control>,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
    ) -> ControlOutcome {
        while let Ok(cmd) = control.try_recv() {
            if matches!(
                self.handle_control(cmd, normal, critical),
                ControlOutcome::Stop
            ) {
                return ControlOutcome::Stop;
            }
        }
        ControlOutcome::Continue
    }

    fn handle_control(
        &mut self,
        cmd: Control,
        normal: &Receiver<Staged>,
        critical: &Receiver<Staged>,
    ) -> ControlOutcome {
        match cmd {
            Control::Register(setup, reply) => {
                let _ = reply.send(self.register(*setup));
                ControlOutcome::Continue
            }
            Control::Release(ns) => {
                // The records first, the release after: what an application
                // thread managed to enqueue before the handle was dropped has
                // to reach the disk — `log()` already answered `Ok` to it.
                //
                // The rest of the queue is left alone. The queue is shared by
                // the process, and it holds records of OTHER, living
                // namespaces: releasing one namespace does not mean destroying
                // everything the rest managed to write. There is nowhere left
                // to write the released one's own records — `push` will account
                // for them against a non-existent address.
                self.drain_pending(normal, critical, Leftovers::Keep);
                self.release(ns);
                ControlOutcome::Continue
            }
            Control::CommitMigration {
                ns,
                channel,
                name,
                commit,
                reply,
            } => {
                let _ = reply.send(self.commit_migration(ns, channel, name, commit));
                ControlOutcome::Continue
            }
            Control::Sync(ns, reply) => {
                // The records first, the report after: otherwise `sync` would
                // confirm the safety of what is still sitting in the queue.
                let drained = self.drain_pending(normal, critical, Leftovers::Keep);
                let mut outcome = self.sync_all(ns);
                if outcome.is_ok() && !drained {
                    // What was taken is on the medium, but the queue is filling
                    // faster than it drains. The records are not lost — they
                    // are still in the queue — but the promise "everything
                    // accumulated is on the medium" was not kept this time, and
                    // returning `Ok` would be a lie.
                    outcome = Err(Error::SyncIncomplete);
                }
                let _ = reply.send(outcome);
                ControlOutcome::Continue
            }
            Control::Shutdown(reply) => {
                // The only case in which the rest of the queue is discarded:
                // after the stop there will be nobody to write it, and the
                // process cannot be kept alive until the writing threads fall
                // silent.
                self.drain_pending(normal, critical, Leftovers::Discard);
                self.finish();
                let _ = reply.send(());
                ControlOutcome::Stop
            }
        }
    }

    /// Perform a migration's segment commit on the writer's thread.
    ///
    /// Here rather than on the caller's thread because the inventory and
    /// rotation live only here: swapping a file behind the writer's back would
    /// race with its own `unlink` of that file. The commit itself is a rename
    /// and an edit to a number in the inventory — microseconds: the writer is
    /// not held up.
    fn commit_migration(
        &mut self,
        ns: NsId,
        channel: ChannelIdx,
        name: SegmentName,
        commit: MigrationCommit,
    ) -> Result<bool> {
        let Some(ch) = self
            .namespaces
            .get_mut(ns.0 as usize)
            .and_then(|n| n.as_mut())
            .and_then(|n| n.channels.get_mut(channel.0 as usize))
        else {
            // The namespace is already released — there is nothing to commit
            // into.
            return Ok(false);
        };
        // The segment may have been rotated while it was being rewritten: then
        // its history is already thrown out, and resurrecting it from a
        // temporary file is not allowed — the budget has forgotten about it.
        if !ch.inventory.iter().any(|e| e.name == name) {
            return Ok(false);
        }
        debug_assert_ne!(
            Some(name),
            Self::live_segment(ch),
            "only segments of earlier versions migrate; the live one is always current"
        );
        let path = ch.dir.join(name.to_string());
        match commit {
            MigrationCommit::Replace { tmp, size } => {
                std::fs::rename(&tmp, &path).ctx_path("swapping a segment in", &path)?;
                crate::fsutil::sync_dir(&ch.dir)?;
                ch.inventory.update_size_bytes(name, size);
            }
            MigrationCommit::Remove => {
                crate::fsutil::remove_synced(&path)?;
                ch.inventory.remove(name);
            }
        }
        Ok(true)
    }

    fn register(&mut self, setup: NsSetup) -> Result<NsId> {
        // There are more directories now, and the segment counters do not show
        // it: an empty channel has not created a file yet. A service that
        // started later has to reach a subscription over a group of namespaces
        // without restarting it.
        self.roster_changed = true;
        let identity = SegmentIdentity {
            protocol_version: setup.protocol_version,
            store_id: setup.store_id,
            boot: setup.boot,
        };
        let mut channels = Vec::with_capacity(setup.channels.len());
        for (i, spec) in setup.channels.into_iter().enumerate() {
            crate::fsutil::create_dir_all_synced(&spec.dir)?;
            let root_key = self
                .groups
                .get(spec.group)
                .map(|g| g.root_key)
                .unwrap_or_default();
            channels.push(ChannelState::new(
                spec,
                root_key,
                identity,
                ChannelIdx(i as u16),
                Arc::clone(&setup.drops),
                &self.counters,
            )?);
        }
        let state = NsState {
            name: setup.name,
            channels,
            drops: setup.drops,
        };
        // The slot of a released namespace is reused: otherwise bringing the
        // same name up again (a service reconnecting) would grow the table for
        // the life of the process.
        match self.namespaces.iter().position(Option::is_none) {
            Some(i) => {
                self.namespaces[i] = Some(state);
                Ok(NsId(i as u32))
            }
            None => {
                self.namespaces.push(Some(state));
                Ok(NsId((self.namespaces.len() - 1) as u32))
            }
        }
    }

    /// Release a namespace: write out, seal the segments, free the slot.
    ///
    /// Without this a channel's state would live until the end of the process,
    /// and bringing the same name up again would give **two** states over one
    /// directory with inventories of their own. One's rotation would not know
    /// about the other's active segment and would delete it: writing would go
    /// on into a file that no longer has a name, and everything written after
    /// that would vanish when it closed.
    fn release(&mut self, ns: NsId) {
        // The loss notices are flushed BEFORE sealing — otherwise a hole that
        // formed just before closing would not reach the stream at all.
        self.emit_drop_notices();

        let idx = ns.0 as usize;
        let counters = Arc::clone(&self.counters);
        if let Some(slot) = self.namespaces.get_mut(idx)
            && let Some(state) = slot.as_mut()
        {
            for ch in &mut state.channels {
                // A released segment is brought back and sealed here — unlike
                // at process stop (see `finish`). The difference is one of
                // scale: one namespace is released, and its name may be taken
                // on the very next line, whereas a whole fleet is stopped at
                // once, where the same opens and walks are tens of thousands of
                // operations for the sake of a footer the next start will
                // append for free.
                Self::unpark(ch, &counters);
                if Self::seal_segment(ch, &counters).is_err() {
                    Counters::bump(&counters.io_errors);
                }
            }
            *slot = None;
        }
        self.active.retain(|&(n, _)| n != idx);
    }

    fn sync_all(&mut self, only: Option<NsId>) -> Result<()> {
        let counters = Arc::clone(&self.counters);
        let mut first_error = None;
        for (idx, slot) in self.namespaces.iter_mut().enumerate() {
            if let Some(NsId(want)) = only
                && want as usize != idx
            {
                continue;
            }
            let Some(ns) = slot.as_mut() else { continue };
            for ch in &mut ns.channels {
                let r = Self::flush_block(ch, &counters)
                    .and_then(|()| Self::sync_channel(ch, &counters));
                if let Err(e) = r {
                    Counters::bump(&counters.io_errors);
                    first_error.get_or_insert(e);
                }
            }
        }
        match first_error {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    /// The final close: write out and seal everything.
    fn finish(&mut self) {
        // The loss notices are flushed BEFORE sealing. Otherwise a hole that
        // formed between the last `tick` and the stop would not reach the
        // stream at all — and that is exactly the moment the queue is most
        // often full: the process is ending under load.
        self.emit_drop_notices();

        let counters = Arc::clone(&self.counters);
        for ns in self.namespaces.iter_mut().flatten() {
            for ch in &mut ns.channels {
                if Self::seal_segment(ch, &counters).is_err() {
                    Counters::bump(&counters.io_errors);
                }
            }
        }

        // The stop is the only moment when the occupancy of the medium is
        // certainly final: there are no active segments left and the reserve is
        // trimmed. Leaving a group over its budget here would mean leaving it
        // over budget until the next start.
        self.enforce_groups();
    }
}

enum ControlOutcome {
    Continue,
    Stop,
}

/// What to do with records left in the queue after the allotted passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leftovers {
    /// Leave them in the queue: the ordinary course of the loop will write
    /// them.
    Keep,
    /// Discard and count as a loss: there is nobody left to write them.
    Discard,
}

/// Where an assembled block landed.
enum Placement {
    /// Written at an offset.
    At(u64),
    /// Discarded: it does not fit even a fresh segment. The loss is already
    /// counted.
    Dropped,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staged::{OwnedValue, StagedRecord as SR};
    use dduroc_format::MetricId;

    #[test]
    fn queue_sizes_never_degenerate_to_rendezvous() {
        // A zero capacity would turn enqueueing into a rendezvous with the
        // writer: the ordinary channel would stop differing from the critical
        // one, and an application thread would wait for the disk on every
        // record.
        let q = QueueSizes {
            normal: 0,
            critical: 0,
        }
        .sanitized();
        assert_eq!(q.normal, 1);
        assert_eq!(q.critical, 1);

        let d = QueueSizes::default();
        assert_eq!(d.sanitized(), d, "sensible values are not distorted");
    }

    #[test]
    fn idle_channel_gives_its_buffers_back() {
        // A channel's buffers grow to the largest block and without a return
        // stay that way forever: an RSS measurement showed +16 MiB after ONE 8
        // MiB blob — the block buffer plus scratch, both the size of the block.
        // When a channel goes idle the memory has to go back to the allocator.
        use dduroc_format::record::Sample;
        use dduroc_format::{MetricId, Record, Value};

        let dir = tempfile::tempdir().unwrap();
        let counters = Counters::default();
        let drops = Arc::new(DropCounters::new(1));
        let mut ch = ChannelState::new(
            ChannelSpec {
                dir: dir.path().to_path_buf(),
                group: 0,
                quota_bytes: None,
                config: ChannelConfig::new(64 * 1024 * 1024),
            },
            0,
            SegmentIdentity {
                protocol_version: ProtocolVersion(1),
                store_id: 0,
                boot: BootCounter(0),
            },
            ChannelIdx(0),
            drops,
            &counters,
        )
        .unwrap();

        // A megabyte incompressible blob — a block knowably larger than
        // block_max.
        let noise: Vec<u8> = {
            let mut s: u64 = 0x2545_F491_4F6C_DD1D;
            (0..1 << 20)
                .map(|_| {
                    s ^= s << 13;
                    s ^= s >> 7;
                    s ^= s << 17;
                    s as u8
                })
                .collect()
        };
        WriterLoop::ensure_room(&mut ch, Micros(0), &counters).unwrap();
        ch.builder
            .push(
                Micros(0),
                &Record::Sample(Sample {
                    metric: MetricId(1),
                    value: Value::Blob(&noise),
                }),
            )
            .unwrap();
        WriterLoop::flush_block(&mut ch, &counters).unwrap();

        let held = ch.builder.capacity() + ch.scratch.capacity();
        assert!(
            held >= 2 << 20,
            "after a blob the buffers must be inflated (or the test is empty): {held}"
        );

        // What tick does with a channel that has left active.
        ch.release_buffers();
        assert_eq!(
            ch.builder.capacity() + ch.scratch.capacity(),
            0,
            "an idle channel has no right to hold a peak"
        );

        // The channel stays usable: the next record reopens the buffers.
        ch.builder
            .push(
                Micros(10),
                &Record::Sample(Sample {
                    metric: MetricId(1),
                    value: Value::U64(7),
                }),
            )
            .unwrap();
        WriterLoop::flush_block(&mut ch, &counters).unwrap();
        assert_eq!(counters.snapshot().dropped, 0);
        assert_eq!(counters.snapshot().blocks_written, 2);
    }

    /// A loop with a single budget group. `None` means a budget "without a
    /// limit".
    fn empty_loop(group_budget: Option<u64>) -> WriterLoop {
        WriterLoop {
            namespaces: Vec::new(),
            counters: Arc::new(Counters::default()),
            diag_target: Arc::from(DIAG_TARGET),
            batch: Vec::new(),
            drops_seen: 0,
            active: Vec::new(),
            groups: vec![GroupBudget {
                budget_bytes: group_budget.unwrap_or(u64::MAX),
                root_key: 0,
            }],
            occupancy_seen: 0,
            pressured_roots: 0,
            buffer_ceiling: None,
            pulse: Arc::new(crate::pulse::Pulse::new()),
            pulsed_blocks: 0,
            pulsed_shape: 0,
            roster_changed: false,
        }
    }

    fn add_namespace_at(
        w: &mut WriterLoop,
        name: &str,
        dir: &Path,
        channels: Vec<ChannelConfig>,
        boot: u32,
    ) -> NsId {
        let drops = Arc::new(DropCounters::new(channels.len()));
        // The directory names in the tests are synthetic: the writer is
        // indifferent to them. Every channel is in group 0, with no personal
        // quotas.
        let channels = channels
            .into_iter()
            .enumerate()
            .map(|(i, c)| ChannelSpec {
                dir: dir.join(format!("ch{i}")),
                group: 0,
                quota_bytes: None,
                config: c,
            })
            .collect();
        w.register(NsSetup {
            name: name.to_owned(),
            protocol_version: ProtocolVersion(1),
            store_id: 0,
            boot: BootCounter(boot),
            channels,
            drops,
        })
        .unwrap()
    }

    fn add_namespace(
        w: &mut WriterLoop,
        name: &str,
        dir: &Path,
        channels: Vec<ChannelConfig>,
    ) -> NsId {
        add_namespace_at(w, name, dir, channels, 0)
    }

    fn loop_with_one_channel(dir: &Path, config: ChannelConfig) -> (WriterLoop, NsId) {
        let mut w = empty_loop(None);
        let ns = add_namespace(&mut w, "ns", dir, vec![config]);
        (w, ns)
    }

    fn held_bytes(w: &WriterLoop) -> usize {
        w.namespaces[0].as_ref().unwrap().channels[0].held_bytes() as usize
    }

    #[test]
    fn a_declared_memory_ceiling_takes_the_buffers_back() {
        // The memory per channel is the active block buffer and its serialized
        // copy. Only a handful of channels write at any moment, and usually
        // that is enough; where "a handful" stops being true, the ceiling takes
        // buffers from the largest holders. Buffers, not records: a record
        // discarded for the sake of memory accounting is data loss.
        let dir = tempfile::tempdir().unwrap();
        let config = ChannelConfig {
            block_max_bytes: 4096,
            ..ChannelConfig::new(16 * 1024 * 1024)
        };
        let (mut w, ns) = loop_with_one_channel(dir.path(), config);
        // A ceiling below what one large blob inflates things to.
        w.buffer_ceiling = Some(16 * 1024);

        let noise: Vec<u8> = (0..(1 << 20)).map(|i| (i * 2654435761u64) as u8).collect();
        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::Blob(noise.as_slice().into()),
            },
        });
        w.apply_batch();
        assert!(
            held_bytes(&w) > 1 << 20,
            "a megabyte blob must inflate the buffers (or the test is empty): {}",
            held_bytes(&w)
        );

        w.tick();
        assert!(
            (held_bytes(&w) as u64) <= 16 * 1024,
            "the ceiling must give memory back: {} B held",
            held_bytes(&w)
        );
        // The record reached the medium rather than being sacrificed.
        assert_eq!(
            w.counters.snapshot().dropped,
            0,
            "the ceiling loses no records"
        );
        assert!(w.counters.snapshot().blocks_written >= 1);
        assert_eq!(
            w.counters.snapshot().buffer_overruns,
            0,
            "the ceiling is met — there is nothing to complain about"
        );

        // The channel stays usable: the next record allocates the buffers
        // again.
        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(2),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(7),
            },
        });
        w.apply_batch();
        assert!(
            held_bytes(&w) > 0,
            "the channel is alive and goes on writing"
        );
    }

    #[test]
    fn an_unmeetable_memory_ceiling_is_counted_not_paid_for_with_records() {
        // One record can be larger than any reasonable ceiling: the buffer has
        // to hold at least that one. Discarding such a record would mean losing
        // data where memory is merely inconvenient, so unmeetability is
        // announced by a counter — as an unmeetable medium budget is.
        let dir = tempfile::tempdir().unwrap();
        let config = ChannelConfig {
            block_max_bytes: 4096,
            ..ChannelConfig::new(16 * 1024 * 1024)
        };
        let (mut w, ns) = loop_with_one_channel(dir.path(), config);
        w.buffer_ceiling = Some(16 * 1024);

        // The block is open and loaded with a record, but there is no segment:
        // flushing it means losing that record. The ceiling does not pay that
        // price.
        let noise: Vec<u8> = (0..(1 << 20)).map(|i| (i * 2654435761u64) as u8).collect();
        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::Blob(noise.as_slice().into()),
            },
        });
        w.apply_batch();
        {
            use dduroc_format::record::Sample;
            use dduroc_format::{Record, Value};
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.segment = None;
            ch.builder
                .push(
                    Micros(2),
                    &Record::Sample(Sample {
                        metric: MetricId(1),
                        value: Value::Blob(&noise),
                    }),
                )
                .unwrap();
        }

        w.tick();
        assert!(
            held_bytes(&w) > 1 << 20,
            "a loaded accumulator with no segment must not be touched"
        );
        assert_eq!(
            w.counters.snapshot().dropped,
            0,
            "not one record sacrificed"
        );
        assert!(
            w.counters.snapshot().buffer_overruns >= 1,
            "an unmeetable ceiling must be named"
        );
    }

    #[test]
    fn an_immediate_channel_keeps_its_buffers_between_batches() {
        // A channel with immediate durability turns out to be "idle" after
        // EVERY group commit: the block is flushed and there is nothing to
        // sync. Returning the buffers on those grounds would mean freeing and
        // reallocating the block buffer and its scratch on every critical
        // record — on exactly the path the channel exists to make fast.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) =
            loop_with_one_channel(dir.path(), ChannelConfig::critical(16 * 1024 * 1024));

        w.batch.push(Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(7),
            },
        });
        w.apply_batch();

        let held = held_bytes(&w);
        assert!(held > 0, "the buffers were allocated for the block written");
        assert_eq!(w.counters.snapshot().syncs, 1, "the group commit happened");

        w.tick();
        assert_eq!(
            held_bytes(&w),
            held,
            "the buffers must survive a serviced batch"
        );
        assert_eq!(
            w.active.len(),
            1,
            "the channel stays watched — otherwise there is nobody to give the buffers back"
        );

        // But genuine idleness does take them after all: wind the start of the
        // idle spell back by the allotted pause.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.idle_since = Instant::now().checked_sub(RELEASE_AFTER * 2);
            assert!(
                ch.idle_since.is_some(),
                "the clock is monotonic and already running"
            );
        }
        w.tick();
        assert_eq!(
            held_bytes(&w),
            0,
            "a channel that sat idle gives its memory back"
        );
        assert!(
            channel_of(&w).segment.is_some(),
            "but it holds the segment: that costs an order of magnitude more than \
             the buffers and is given up on a separate, far longer pause"
        );
        assert_eq!(
            w.active.len(),
            1,
            "and stays watched — otherwise there will be nobody to give the segment up"
        );
    }

    /// How many of the process's descriptors point inside a directory.
    ///
    /// The main consequence of an open segment is a descriptor held, and the
    /// scale claimed runs into their number long before anything else; the only
    /// way to see that is through procfs. Ours specifically are counted: the
    /// tests run as threads of one process, and a global descriptor count would
    /// drift with the neighbours.
    fn open_fds_under(dir: &Path) -> usize {
        std::fs::read_dir("/proc/self/fd")
            .expect("procfs is mounted")
            .filter_map(|e| e.ok())
            .filter_map(|e| std::fs::read_link(e.path()).ok())
            .filter(|target| target.starts_with(dir))
            .count()
    }

    fn channel_of(w: &WriterLoop) -> &ChannelState {
        &w.namespaces[0].as_ref().unwrap().channels[0]
    }

    #[test]
    fn a_channel_that_went_quiet_hands_back_its_segment() {
        // An open segment costs a descriptor and is counted in the budget
        // together with the unwritten tail of its reserve window, while what is
        // written in it may be a hundred bytes. With the tens of thousands of
        // channels claimed, quiet ones would hold tens of thousands of
        // descriptors — while only a handful write at any moment. Having sat
        // out its allotted time, a channel has to give up both the file and the
        // descriptor.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) = loop_with_one_channel(dir.path(), ChannelConfig::new(16 * 1024 * 1024));
        assert_eq!(
            open_fds_under(dir.path()),
            0,
            "before the first record a channel holds nothing"
        );

        let record = |at: u64| Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(at),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(7),
            },
        };
        w.batch.push(record(1));
        w.apply_batch();

        let path = channel_of(&w)
            .segment
            .as_ref()
            .expect("the write opened a segment")
            .path()
            .to_owned();
        let full = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            full,
            64 << 10,
            "the reserve was taken as a one-block window, not as the whole segment"
        );
        assert!(
            full < channel_of(&w).config.segment_bytes,
            "otherwise the test is empty: the window equals the growth limit"
        );
        assert_eq!(
            open_fds_under(dir.path()),
            1,
            "otherwise the test has nothing to give back: no descriptor is held"
        );

        // The deadlines are behind: the block is flushed, the sync is done and
        // the channel has nothing left to service — the idleness count starts
        // here.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.block_opened = Instant::now().checked_sub(Duration::from_secs(60));
            ch.last_sync = Instant::now()
                .checked_sub(Duration::from_secs(60))
                .expect("the clock is monotonic and already running");
        }
        w.tick();
        assert!(
            channel_of(&w).segment.is_some(),
            "a short pause does not take the segment: sealing costs a footer, an \
             fdatasync, an ftruncate and a directory fsync — and as much again on opening"
        );

        // And genuine idleness does take them.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.idle_since = Instant::now().checked_sub(PARK_AFTER * 2);
        }
        w.tick();

        assert!(channel_of(&w).segment.is_none(), "the segment was given up");
        assert!(
            w.active.is_empty(),
            "and the channel left the serviced list"
        );
        assert_eq!(
            open_fds_under(dir.path()),
            0,
            "the descriptor went back to the system"
        );
        let sealed = std::fs::metadata(&path).unwrap().len();
        assert!(
            sealed < full / 100,
            "the file is truncated to its actual data: {sealed} against {full}"
        );

        // But NOT sealed. Sealing would mean the next record starts a new file:
        // a channel writing once an hour would leave eight thousand tiny
        // segments a year, and byte-based rotation would remove none of them —
        // there are hardly any bytes in them. The files would run out before
        // the space did.
        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        assert!(
            !reader.is_sealed(),
            "a released segment must stay continuable"
        );

        // And the channel continues THAT SAME segment rather than starting a
        // new one.
        w.batch.push(record(2));
        w.apply_batch();
        assert_eq!(
            open_segment_path(&w, ns),
            path,
            "waking must continue the earlier segment"
        );
        assert_eq!(
            std::fs::read_dir(&channel_of(&w).dir).unwrap().count(),
            1,
            "silence has no right to breed files"
        );
        assert_eq!(w.counters.snapshot().dropped, 0);

        // The records on both sides of the silence are there and read as one
        // segment.
        WriterLoop::flush_block(
            &mut w.namespaces[0].as_mut().unwrap().channels[0],
            &Arc::clone(&w.counters),
        )
        .unwrap();
        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        let (scan, _) = {
            let file = std::fs::File::open(&path).unwrap();
            let len = file.metadata().unwrap().len();
            let mut footer = dduroc_format::FooterBuilder::new();
            crate::segment::Scan::run_collecting(&file, len, &path, &mut footer).unwrap()
        };
        assert_eq!(
            scan.block_count, 2,
            "two blocks: before the silence and after"
        );
        assert_eq!(
            reader.header().base,
            Micros(1),
            "and it is one and the same file"
        );
    }

    /// A channel with small segments and no compression: the test needs the
    /// segments really to run out rather than compress to nothing.
    fn dense_channel(channel_budget: u64) -> ChannelConfig {
        ChannelConfig {
            segment_bytes: 1 << 20,
            block_max_bytes: 64 << 10,
            compression: dduroc_format::Compression::None,
            ..ChannelConfig::new(channel_budget)
        }
    }

    fn blob(ns: NsId, at: u64, len: usize) -> Staged {
        Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(at),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::Blob(std::iter::repeat_n(0xA5, len).collect()),
            },
        }
    }

    fn open_segment_path(w: &WriterLoop, ns: NsId) -> PathBuf {
        w.namespaces[ns.0 as usize].as_ref().unwrap().channels[0]
            .segment
            .as_ref()
            .expect("the segment is open")
            .path()
            .to_owned()
    }

    #[test]
    fn the_store_ceiling_reaches_across_namespaces() {
        // A per-channel budget answers "how much history to keep for this
        // class", not "how much to take on the medium": a device has thousands
        // of channels, and the sum of their budgets is many times any medium.
        // The store ceiling has to evict the oldest ANYWHERE — otherwise a
        // quiet channel holds space a noisy one lacks, while its own budget is
        // not exceeded and its rotation never fires at all.
        const CAP: u64 = 3 << 20;
        let quiet_dir = tempfile::tempdir().unwrap();
        let noisy_dir = tempfile::tempdir().unwrap();

        // Every channel's budget is knowably larger than the whole store
        // ceiling: per-channel rotation never fires once in this test.
        let mut w = empty_loop(Some(CAP));
        let quiet = add_namespace(
            &mut w,
            "quiet",
            quiet_dir.path(),
            vec![dense_channel(64 << 20)],
        );
        let noisy = add_namespace(
            &mut w,
            "noisy",
            noisy_dir.path(),
            vec![dense_channel(64 << 20)],
        );

        // The quiet one writes first and falls silent: its segment is the
        // oldest in the store and sealed, that is, it can be evicted.
        w.batch.push(blob(quiet, 1, 8));
        w.apply_batch();
        let quiet_path = open_segment_path(&w, quiet);
        {
            let counters = Arc::clone(&w.counters);
            let ch = &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0];
            WriterLoop::seal_segment(ch, &counters).unwrap();
            assert!(
                ch.inventory.total_bytes() < ch.config.budget_bytes / 1000,
                "the quiet channel comes nowhere near its own budget"
            );
        }
        assert!(quiet_path.exists());

        // The noisy one fills several segments in a row.
        for i in 0..80 {
            w.batch.push(blob(noisy, 1_000 + i, 64 << 10));
        }
        w.apply_batch();
        assert!(
            w.group_totals().iter().sum::<u64>() > CAP,
            "otherwise there is nothing to evict and the test is empty: {}",
            w.group_totals().iter().sum::<u64>()
        );

        w.tick();

        assert!(
            !quiet_path.exists(),
            "the oldest across the STORE is evicted, not across the channel: a \
             neighbour needed the space"
        );
        assert!(
            w.group_totals().iter().sum::<u64>() <= CAP,
            "the store must fit the declared ceiling: {} against {CAP}",
            w.group_totals().iter().sum::<u64>()
        );
        assert!(
            open_segment_path(&w, noisy).exists(),
            "the active segment is untouchable: it is being written to"
        );
        assert_eq!(
            w.counters.snapshot().budget_overruns,
            0,
            "the ceiling is met — there is nothing to complain about"
        );
    }

    #[test]
    fn a_segment_left_parked_is_sealed_by_the_next_run() {
        // A released segment stays unsealed, and so it remains when the process
        // stops. Opening it for a footer at the stop is not an option — that is
        // an open and a walk for every quiet channel, tens of thousands at the
        // scale claimed. The promise is that the next start appends the footer:
        // recovery walks the file anyway to find the end of the data.
        let dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(None);
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);
        w.batch.push(blob(ns, 1, 64));
        w.apply_batch();
        let path = open_segment_path(&w, ns);

        // The deadlines are behind: the block is flushed and the sync is done.
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.block_opened = Instant::now().checked_sub(Duration::from_secs(60));
            ch.last_sync = Instant::now()
                .checked_sub(Duration::from_secs(60))
                .expect("the clock is monotonic and already running");
        }
        w.tick();
        {
            let ch = &mut w.namespaces[0].as_mut().unwrap().channels[0];
            ch.idle_since = Instant::now().checked_sub(PARK_AFTER * 2);
        }
        w.tick();
        assert!(
            channel_of(&w).parked_segment.is_some(),
            "the segment was released"
        );
        assert!(
            !crate::segment::SegmentReader::open(&path)
                .unwrap()
                .is_sealed(),
            "and not sealed"
        );
        drop(w); // the process ended without writing anything more

        // The next start: the same directory, a new run number.
        let mut next = empty_loop(None);
        add_namespace_at(
            &mut next,
            "ns",
            dir.path(),
            vec![dense_channel(64 << 20)],
            1,
        );

        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        assert!(reader.is_sealed(), "the next start must seal it");
        let footer = reader.footer().expect("the footer reads");
        assert_eq!(
            footer.metrics,
            vec![MetricId(1)],
            "and name the types: a migration decides from them whether the segment is affected"
        );
        assert_eq!(footer.blocks.len(), 1, "the block index is there");
    }

    #[test]
    fn no_space_is_freed_where_it_is_taken_not_where_it_is_needed() {
        // Space is reserved for one thing: so that ENOSPC arrives when a
        // segment is created rather than in the middle of writing a critical
        // event. That path itself was until now exercised by no test — a
        // refusal for want of space cannot be reproduced on a real filesystem
        // without root — so the refusal is injected exactly where it really
        // arrives.
        //
        // What is checked is what the former engine did not do: a channel that
        // hit ENOSPC rotates ONLY ITSELF. A newcomer has no history of its own,
        // all the space is held by a neighbour — and without a view of the
        // whole store the channel would freeze forever although there is
        // somewhere to free space.
        let hoard_dir = tempfile::tempdir().unwrap();
        let new_dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(None);
        let hoarder = add_namespace(
            &mut w,
            "hoarder",
            hoard_dir.path(),
            vec![dense_channel(64 << 20)],
        );
        let newcomer = add_namespace(
            &mut w,
            "newcomer",
            new_dir.path(),
            vec![dense_channel(64 << 20)],
        );

        // The neighbour has filled several segments and fits its own budget
        // perfectly well: its own rotation will never fire.
        for i in 0..64 {
            w.batch.push(blob(hoarder, 1_000 + i, 64 << 10));
        }
        w.apply_batch();
        let hoarded: Vec<_> = {
            let ch = &w.namespaces[hoarder.0 as usize].as_ref().unwrap().channels[0];
            ch.inventory.iter().map(|e| e.path(&ch.dir)).collect()
        };
        assert!(
            hoarded.len() >= 2,
            "otherwise there is nothing to evict: {hoarded:?}"
        );
        let oldest = hoarded[0].clone();

        // The medium refuses the newcomer its very first segment.
        crate::segment::fault::no_space_for(1);
        w.batch.push(blob(newcomer, 9_000, 8));
        w.apply_batch();

        assert!(
            w.namespaces[newcomer.0 as usize].as_ref().unwrap().channels[0]
                .segment
                .is_none(),
            "the segment was not created — the space ran out"
        );
        assert_eq!(
            w.counters.snapshot().dropped,
            1,
            "the record was lost and accounted for"
        );
        assert!(oldest.exists(), "nobody has freed anything yet");

        w.tick();

        assert!(
            !oldest.exists(),
            "space is freed WHERE IT IS TAKEN: at the neighbour, not at the one \
             that ran into it"
        );

        // And writing goes on: the channel is not jammed.
        w.batch.push(blob(newcomer, 9_001, 8));
        w.apply_batch();
        assert!(
            w.namespaces[newcomer.0 as usize].as_ref().unwrap().channels[0]
                .segment
                .is_some(),
            "once space is freed the write must go through"
        );
        assert_eq!(w.counters.snapshot().dropped, 1, "there was no second loss");
    }

    #[test]
    fn the_budget_counts_what_a_segment_occupies_not_what_it_may_grow_into() {
        // A live segment is counted in the class budget as its reserve window,
        // not as its growth limit. The difference is not bookkeeping: counting
        // the limit means declaring megabytes occupied that are not on the
        // medium, and evicting someone else's history for them. Across a fleet
        // that pegs the practical number of channels writing at once to
        // `budget_bytes / segment_bytes` — thirty-two channels on a
        // quarter-gigabyte budget.
        let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
        // The budget covers four windows with room to spare and is a quarter of
        // what four growth limits would take.
        let budget = 4 * ChannelConfig::new(0).segment_bytes / 2;
        let mut w = empty_loop(Some(budget));
        let counters = Arc::clone(&w.counters);
        for (i, dir) in dirs.iter().enumerate() {
            let ns = add_namespace(
                &mut w,
                &format!("ns-{i}"),
                dir.path(),
                vec![ChannelConfig::new(64 << 20)],
            );
            w.batch.push(blob(ns, 1, 8));
            w.apply_batch();
            WriterLoop::flush_block(
                &mut w.namespaces[ns.0 as usize].as_mut().unwrap().channels[0],
                &counters,
            )
            .unwrap();
        }

        let occupied = w.group_totals().iter().sum::<u64>();
        assert!(
            occupied < budget,
            "four channels took {occupied} on a budget of {budget}"
        );
        assert!(
            4 * ChannelConfig::new(0).segment_bytes > budget,
            "otherwise the test is empty: the growth limits would have fitted the budget anyway"
        );

        let paths: Vec<_> = (0..4).map(|i| open_segment_path(&w, NsId(i))).collect();
        w.tick();
        assert_eq!(
            w.counters.snapshot().budget_overruns,
            0,
            "the budget is met: {occupied} of {budget} taken"
        );
        assert_eq!(
            w.counters.snapshot().segments_rotated,
            0,
            "there is nothing to evict"
        );
        for path in &paths {
            assert!(path.exists(), "the live segment is untouched: {path:?}");
        }
    }

    #[test]
    fn a_window_that_cannot_grow_costs_the_oldest_segment_not_the_block() {
        // Space is reserved as a window, so ENOSPC arrives in the middle of a
        // segment too — when the window is extended, not only when the file is
        // created. That is a new kind of refusal, and it has to be answered the
        // same way as a refusal at creation: the channel gives up ITS OWN
        // oldest first and tries again. Otherwise a momentary shortage would
        // cost a whole block of records while live history nobody needs sat
        // right beside it.
        let dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(Some(u64::MAX));
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);

        // Several segments of history — there is something to pay with.
        for i in 0..48 {
            w.batch.push(blob(ns, 1_000 + i, 64 << 10));
        }
        w.apply_batch();
        let history: Vec<_> = {
            let ch = &w.namespaces[ns.0 as usize].as_ref().unwrap().channels[0];
            ch.inventory.iter().map(|e| e.path(&ch.dir)).collect()
        };
        assert!(
            history.len() >= 2,
            "otherwise there is nothing to pay with: {history:?}"
        );
        let before = w.counters.snapshot();

        // The medium refuses exactly once — at the next extension of the
        // window. Three blocks: the window does not grow for every one (the
        // step is an eighth of the limit), but over three an extension is
        // certain, while more than a dozen remain before the end of the segment
        // — so the refusal falls on the window's growth, and the unchanged
        // creation counter proves it.
        crate::segment::fault::no_space_for(1);
        for i in 0..3 {
            w.batch.push(blob(ns, 9_000 + i, 64 << 10));
        }
        w.apply_batch();
        crate::segment::fault::no_space_for(0);

        let after = w.counters.snapshot();
        assert_eq!(
            after.segments_created, before.segments_created,
            "the refusal fell on the window growing, not on a segment being created"
        );
        assert_eq!(after.dropped, before.dropped, "the block is not lost");
        assert!(
            after.segments_rotated > before.segments_rotated,
            "the oldest must be given up: {before:?} → {after:?}"
        );
        assert!(!history[0].exists(), "it is the oldest that was given up");
    }

    #[test]
    fn a_window_that_cannot_grow_with_nothing_to_give_counts_the_loss() {
        // The same refusal, but with nothing to give up: the only segment is
        // the one being written to. Then the block is lost, and that has to be
        // visible both in the total counter and per channel — otherwise the
        // hole in the stream would stay unannounced while the store reported
        // all was well.
        let dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(Some(u64::MAX));
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);

        // One block fits the first window — the segment is created without
        // growing.
        w.batch.push(blob(ns, 1, 8));
        w.apply_batch();
        let path = open_segment_path(&w, ns);
        let counters = Arc::clone(&w.counters);
        WriterLoop::flush_block(
            &mut w.namespaces[ns.0 as usize].as_mut().unwrap().channels[0],
            &counters,
        )
        .unwrap();
        let before = w.counters.snapshot();
        assert_eq!(
            w.namespaces[ns.0 as usize].as_ref().unwrap().channels[0]
                .inventory
                .len(),
            1,
            "otherwise the test is not about having nothing to give up"
        );

        // From here on the medium gives not a byte.
        crate::segment::fault::free_space(0);
        w.batch.push(blob(ns, 9_000, 64 << 10));
        w.apply_batch();
        crate::segment::fault::unlimited_space();

        let after = w.counters.snapshot();
        assert!(
            after.dropped > before.dropped,
            "the loss must be counted: {before:?} → {after:?}"
        );
        assert!(
            after.io_errors > before.io_errors,
            "and so must the medium's refusal"
        );
        assert!(
            path.exists(),
            "the segment that was being written to is intact"
        );
        assert_ne!(w.pressured_roots, 0, "the medium is marked as full");
    }

    #[test]
    fn writing_after_a_park_counts_against_the_ceiling_again() {
        // A channel that wakes takes space not by waking but by writing: the
        // file stays truncated until a block lands in it, and only then is the
        // reserve window extended again.
        //
        // The budget-recount watchdog counts "occupancy grew" rather than "a
        // file was created": counting creations would let this growth slip
        // past, and the store would sit over its ceiling until somebody's next
        // rotation — that is, indefinitely.
        let quiet_dir = tempfile::tempdir().unwrap();
        let noisy_dir = tempfile::tempdir().unwrap();
        let mut w = empty_loop(Some(u64::MAX));
        let quiet = add_namespace(
            &mut w,
            "quiet",
            quiet_dir.path(),
            vec![dense_channel(64 << 20)],
        );
        let noisy = add_namespace(
            &mut w,
            "noisy",
            noisy_dir.path(),
            vec![dense_channel(64 << 20)],
        );

        w.batch.push(blob(quiet, 1, 8));
        for i in 0..64 {
            w.batch.push(blob(noisy, 1_000 + i, 64 << 10));
        }
        w.apply_batch();

        // The quiet channel gives up its segment: the file shrinks to a hundred
        // bytes.
        {
            let ch = &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0];
            ch.block_opened = None;
            ch.idle_since = Instant::now().checked_sub(RELEASE_AFTER * 2);
        }
        assert_eq!(w.park_idle(RELEASE_AFTER), 1, "the segment was released");

        // The ceiling set to exactly the current occupancy.
        let cap = w.group_totals().iter().sum::<u64>();
        w.groups[0].budget_bytes = cap;
        w.tick();
        assert_eq!(
            w.group_totals().iter().sum::<u64>(),
            cap,
            "so far everything is where it was"
        );

        // The quiet one wakes. Waking itself asks for no space — the file is
        // truncated as it was and stays that way.
        w.batch.push(blob(quiet, 2, 8));
        w.apply_batch();
        assert!(
            w.namespaces[quiet.0 as usize].as_ref().unwrap().channels[0]
                .segment
                .is_some(),
            "the segment came back into work"
        );
        assert_eq!(
            w.group_totals().iter().sum::<u64>(),
            cap,
            "waking without writing takes no space"
        );

        // But a block landing extends the window — and the ceiling is broken.
        {
            let ch = &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0];
            ch.block_opened = Instant::now().checked_sub(Duration::from_secs(60));
        }
        let counters = std::sync::Arc::clone(&w.counters);
        WriterLoop::flush_block(
            &mut w.namespaces[quiet.0 as usize].as_mut().unwrap().channels[0],
            &counters,
        )
        .unwrap();
        assert!(
            w.group_totals().iter().sum::<u64>() > cap,
            "otherwise the test is empty: the write must take space"
        );

        w.tick();
        assert!(
            w.group_totals().iter().sum::<u64>() <= cap,
            "the ceiling must be restored: {} against {cap}",
            w.group_totals().iter().sum::<u64>()
        );
    }

    #[test]
    fn an_unreachable_ceiling_is_reported_rather_than_pretended() {
        // Only a sealed segment can be evicted: the active one is being written
        // to. A ceiling below what one active segment has taken is unmeetable
        // by construction, and the only honest outcome is to say so rather than
        // delete a file out from under a write and pretend all is well.
        let dir = tempfile::tempdir().unwrap();
        // A ceiling below one reserve window: the channel took 64 KiB with its
        // very first block, and they can only be given up along with the
        // segment being written to.
        let mut w = empty_loop(Some(8 << 10));
        let ns = add_namespace(&mut w, "ns", dir.path(), vec![dense_channel(64 << 20)]);

        w.batch.push(blob(ns, 1, 8));
        w.apply_batch();
        let path = open_segment_path(&w, ns);

        w.tick();

        assert!(path.exists(), "a segment being written to is not deleted");
        assert_eq!(
            w.counters.snapshot().budget_overruns,
            1,
            "an unmeetable ceiling must be visible from outside"
        );
        assert_eq!(
            w.counters.snapshot().dropped,
            0,
            "and this is not a loss of records"
        );
    }

    #[test]
    fn sync_never_throws_away_what_it_could_not_keep_up_with() {
        // Draining the queue is bounded by a number of passes: otherwise `sync`
        // would not return until the writing threads fell silent. But the
        // remainder is NOT thrown away — there is still someone to write it, in
        // the ordinary course of the loop. Discarding what was accepted for the
        // sake of an operation that gains nothing by it means destroying data
        // `log()` answered `Ok` to. Only `shutdown` discards, and only because
        // after it there is nobody to write.
        let dir = tempfile::tempdir().unwrap();
        let (mut w, ns) = loop_with_one_channel(dir.path(), ChannelConfig::new(16 * 1024 * 1024));

        // One pass takes no more than DRAIN_LIMIT records, so a queue one
        // record longer is knowably not drained in the single allotted pass —
        // with no dependence on the scheduler at all.
        let n = DRAIN_LIMIT + 1;
        let (tx, rx) = crossbeam_channel::bounded::<Staged>(n);
        let (_ctx, crx) = crossbeam_channel::bounded::<Staged>(1);
        let item = Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(1),
            },
        };
        for _ in 0..n {
            tx.send(item.clone()).unwrap();
        }

        let drained = w.drain_pending_rounds(&rx, &crx, Leftovers::Keep, 1);
        assert!(!drained, "one pass is allotted and there are more records");
        assert_eq!(
            rx.len(),
            1,
            "the remainder stayed in the queue rather than being thrown away"
        );
        assert_eq!(
            w.counters.snapshot().dropped,
            0,
            "nothing is lost: there is still someone to write the remainder"
        );
        assert_eq!(
            w.counters.snapshot().records_written,
            DRAIN_LIMIT as u64,
            "what was taken is written"
        );

        // For the stop it is the other way round: there will be nobody to write
        // the remainder, so it is discarded — but not silently: the loss is
        // accounted for. Again exactly one pass, so that the drain's failure is
        // real rather than a consequence of zero attempts.
        for _ in 0..DRAIN_LIMIT {
            tx.send(item.clone()).unwrap();
        }
        let before = w.counters.snapshot().dropped;
        let drained = w.drain_pending_rounds(&rx, &crx, Leftovers::Discard, 1);
        assert!(!drained, "one pass is allotted and there are more records");
        assert_eq!(rx.len(), 0, "the queue is emptied");
        assert_eq!(
            w.counters.snapshot().dropped - before,
            1,
            "a discarded record must be accounted for"
        );
    }

    #[test]
    fn a_stopping_store_refuses_instead_of_swallowing() {
        // Between draining the queue in `shutdown` and the thread exiting, the
        // queue stays alive. Without a sign of the stop, `try_send` would
        // answer `Ok` to records that die at once in the channel's destructor:
        // no counter, no notice, no answer to the caller — that is, exactly the
        // hole indistinguishable from silence all this loss accounting exists
        // against.
        let counters = Arc::new(Counters::default());
        let writer = Writer::spawn(
            Arc::clone(&counters),
            QueueSizes::default(),
            Vec::new(),
            None,
            Arc::new(crate::pulse::Pulse::new()),
        )
        .unwrap();
        let drops = DropCounters::new(1);
        let item = Staged {
            ns: NsId(0),
            channel: ChannelIdx(0),
            at: Micros(1),
            record: SR::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(1),
            },
        };

        writer.shutdown();

        let e = writer
            .write(item, false, &drops)
            .expect_err("after the stop there is nowhere to write");
        assert!(
            matches!(e, Error::ShuttingDown),
            "the cause is named for what it is: {e}"
        );
        assert!(
            e.loses_record(),
            "and this is a loss, not a defect in the call"
        );
        assert_eq!(
            counters.snapshot().dropped,
            1,
            "the loss is accounted for rather than swallowed"
        );
        assert_eq!(
            drops.take(ChannelIdx(0)),
            1,
            "and marked in its own channel"
        );
    }

    #[test]
    fn footer_ids_split_events_from_metrics() {
        // The type sets in the footer are kept separately: a migration asks
        // about events and metrics one at a time.
        let sample = Staged {
            ns: NsId(0),
            channel: ChannelIdx(0),
            at: Micros(0),
            record: SR::Sample {
                metric: MetricId(4),
                value: OwnedValue::F32(1.0),
            },
        };
        assert_eq!(sample.footer_ids(), (None, Some(MetricId(4))));

        let span = Staged {
            record: SR::SpanEnd {
                span: dduroc_format::SpanId(1),
            },
            ..sample
        };
        assert_eq!(span.footer_ids(), (None, None), "a span is not a data type");
    }
}
