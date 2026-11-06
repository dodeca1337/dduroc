//! Bounded client queues and block batching over raft-engine.
//! Critical writes receive their reply only after a nonempty synchronous write.
//! Any asynchronous write failure terminates the worker: subsequent operations
//! fail closed, without retrying an ambiguously committed block.
use crate::backend::{ChannelKey, ReadBackend};
use crate::channel::ChannelConfig;
use crate::error::{Error, Result};
use crate::schema::StorageClass;
use crate::staged::{ChannelIdx, DropCounters, NsId, Staged};
use crate::stats::Counters;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use dduroc_format::block::BlockBuilder;
use dduroc_format::{BootCounter, Micros, ProtocolVersion};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

const BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(5);
const DRAIN_LIMIT: usize = 4096;
const DRAIN_ROUNDS: usize = 64;
// Leave room for the header and one maximum-size record. Codec payload limits
// are enforced before a record reaches this accumulator.
const BLOCK_TARGET: usize = 1024 * 1024;
/// Maximum dynamic bytes in one queued event, text message, or blob sample.
pub const MAX_RECORD_BYTES: usize = BLOCK_TARGET;
fn validate_record(item: &Staged) -> Result<()> {
    if item.dynamic_bytes() > MAX_RECORD_BYTES {
        return Err(dduroc_format::Error::LimitExceeded {
            what: "queued record payload",
            value: item.dynamic_bytes() as u64,
            max: MAX_RECORD_BYTES as u64,
        }
        .into());
    }
    Ok(())
}

#[derive(Debug)]
pub struct NsSetup {
    pub name: String,
    pub schema_name: String,
    pub protocol_version: ProtocolVersion,
    pub store_id: u64,
    pub boot: BootCounter,
    pub channels: Vec<ChannelSpec>,
    pub drops: Arc<DropCounters>,
}
#[derive(Debug)]
pub struct ChannelSpec {
    pub dir: PathBuf,
    pub group: usize,
    pub class: StorageClass,
    pub root_index: usize,
    pub quota_bytes: Option<u64>,
    pub config: ChannelConfig,
}
#[derive(Debug, Clone, Copy)]
pub struct GroupBudget {
    pub budget_bytes: u64,
    pub root_key: u8,
}

// Separate byte pools preserve critical admission under telemetry pressure.
const QUEUE_BYTE_LIMITS: [usize; 2] = [16 * 1024 * 1024, 4 * 1024 * 1024];
#[derive(Debug)]
struct QueuePermit {
    held: Arc<AtomicUsize>,
    bytes: usize,
}
impl Drop for QueuePermit {
    fn drop(&mut self) {
        self.held.fetch_sub(self.bytes, Ordering::Release);
    }
}
#[derive(Debug)]
struct Queued {
    item: Staged,
    reply: Option<Sender<Result<()>>>,
    _permit: QueuePermit,
}
#[derive(Debug)]
enum Control {
    Register(Box<NsSetup>, Sender<Result<NsId>>),
    Release(NsId),
    Sync(Option<NsId>, Sender<Result<()>>),
    Key(NsId, ChannelIdx, Sender<Result<ChannelKey>>),
    Shutdown(Sender<()>),
}
#[derive(Debug)]
pub struct Writer {
    normal: Sender<Queued>,
    critical: Sender<Queued>,
    control: Sender<Control>,
    counters: Arc<Counters>,
    stopping: AtomicBool,
    backend: Arc<ReadBackend>,
    queued_bytes: [Arc<AtomicUsize>; 2],
    handle: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl Writer {
    pub fn spawn(
        counters: Arc<Counters>,
        queues: QueueSizes,
        _groups: Vec<GroupBudget>,
        buffer_ceiling: Option<u64>,
        pulse: Arc<crate::pulse::Pulse>,
        backend: Arc<ReadBackend>,
    ) -> Result<Arc<Self>> {
        let queues = queues.sanitized();
        let (normal, nr) = crossbeam_channel::bounded(queues.normal);
        let (critical, cr) = crossbeam_channel::bounded(queues.critical);
        let (control, ctl) = crossbeam_channel::bounded(64);
        let state = WriterLoop {
            namespaces: HashMap::new(),
            next_id: 0,
            drops_seen: 0,
            buffered_bytes: 0,
            counters: counters.clone(),
            backend: backend.clone(),
            pulse,
            buffer_ceiling,
            active: HashSet::new(),
        };
        // An unwinding panic drops all reply channels. With panic=abort the
        // process stops before any acknowledgement can be sent.
        let handle = std::thread::Builder::new()
            .name("dduroc-writer".into())
            .spawn(move || state.run(nr, cr, ctl))
            .map_err(|source| Error::Io {
                context: "starting writer thread".into(),
                source,
            })?;
        Ok(Arc::new(Self {
            normal,
            critical,
            control,
            counters,
            backend,
            stopping: AtomicBool::new(false),
            queued_bytes: std::array::from_fn(|_| Arc::new(AtomicUsize::new(0))),
            handle: Mutex::new(Some(handle)),
        }))
    }
    pub fn backend(&self) -> &Arc<ReadBackend> {
        &self.backend
    }
    pub fn channel_key(&self, ns: NsId, channel: ChannelIdx) -> Result<ChannelKey> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Key(ns, channel, tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }
    pub fn register(&self, setup: NsSetup) -> Result<NsId> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Register(Box::new(setup), tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }
    fn reserve(
        &self,
        item: &Staged,
        critical: bool,
        wait: bool,
        drops: &DropCounters,
    ) -> Result<QueuePermit> {
        let pool = usize::from(critical);
        let bytes = item
            .dynamic_bytes()
            .saturating_add(std::mem::size_of::<Queued>());
        let until = Instant::now() + BACKPRESSURE_TIMEOUT;
        let mut reported = false;
        loop {
            if self.queued_bytes[pool]
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
                    held.checked_add(bytes)
                        .filter(|n| *n <= QUEUE_BYTE_LIMITS[pool])
                })
                .is_ok()
            {
                return Ok(QueuePermit {
                    held: self.queued_bytes[pool].clone(),
                    bytes,
                });
            }
            if !wait || Instant::now() >= until {
                self.lost(item, drops);
                return Err(Error::QueueFull);
            }
            if !reported {
                Counters::bump(&self.counters.backpressure_waits);
                reported = true;
            }
            if self.stopping.load(Ordering::Acquire) {
                self.lost(item, drops);
                return Err(Error::ShuttingDown);
            }
            if !self.is_alive() {
                self.lost(item, drops);
                return Err(Error::WriterDead);
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    pub fn write(&self, item: Staged, critical: bool, drops: &DropCounters) -> Result<()> {
        validate_record(&item)?;
        if !critical {
            return self.write_no_wait(item, false, drops);
        }
        if self.stopping.load(Ordering::Acquire) {
            self.lost(&item, drops);
            return Err(Error::ShuttingDown);
        }
        let (tx, rx) = crossbeam_channel::bounded(1);
        let permit = self.reserve(&item, true, true, drops)?;
        let queued = Queued {
            item,
            reply: Some(tx),
            _permit: permit,
        };
        match self.critical.try_send(queued) {
            Ok(()) => {}
            Err(TrySendError::Full(q)) => {
                Counters::bump(&self.counters.backpressure_waits);
                match self.critical.send_timeout(q, BACKPRESSURE_TIMEOUT) {
                    Ok(()) => {}
                    Err(crossbeam_channel::SendTimeoutError::Timeout(q)) => {
                        self.lost(&q.item, drops);
                        return Err(Error::QueueFull);
                    }
                    Err(crossbeam_channel::SendTimeoutError::Disconnected(q)) => {
                        self.lost(&q.item, drops);
                        return Err(Error::WriterDead);
                    }
                }
            }
            Err(TrySendError::Disconnected(q)) => {
                self.lost(&q.item, drops);
                return Err(Error::WriterDead);
            }
        }
        // No timeout after submission: timing out cannot cancel the write and
        // must not encourage a duplicate retry. Disconnect means unconfirmed.
        rx.recv().map_err(|_| Error::WriterDead)?
    }
    pub fn write_no_wait(&self, item: Staged, critical: bool, drops: &DropCounters) -> Result<()> {
        validate_record(&item)?;
        if self.stopping.load(Ordering::Acquire) {
            self.lost(&item, drops);
            return Err(Error::ShuttingDown);
        }
        let queue = if critical {
            &self.critical
        } else {
            &self.normal
        };
        let permit = self.reserve(&item, critical, false, drops)?;
        match queue.try_send(Queued {
            item,
            reply: None,
            _permit: permit,
        }) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(q)) => {
                self.lost(&q.item, drops);
                Err(Error::QueueFull)
            }
            Err(TrySendError::Disconnected(q)) => {
                self.lost(&q.item, drops);
                Err(Error::WriterDead)
            }
        }
    }
    fn lost(&self, item: &Staged, drops: &DropCounters) {
        drops.record(item.channel);
        Counters::publish(&self.counters.dropped);
    }
    pub fn release(&self, ns: NsId) {
        let _ = self.control.send(Control::Release(ns));
    }
    pub fn sync(&self, ns: Option<NsId>) -> Result<()> {
        let (tx, rx) = crossbeam_channel::bounded(1);
        self.control
            .send(Control::Sync(ns, tx))
            .map_err(|_| Error::WriterDead)?;
        rx.recv().map_err(|_| Error::WriterDead)?
    }
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::Release);
        let (tx, rx) = crossbeam_channel::bounded(1);
        if self.control.send(Control::Shutdown(tx)).is_ok() {
            let _ = rx.recv();
        }
        if let Ok(mut h) = self.handle.lock()
            && let Some(h) = h.take()
        {
            let _ = h.join();
        }
    }
    pub fn counters(&self) -> &Counters {
        &self.counters
    }
    pub fn is_alive(&self) -> bool {
        self.handle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .is_some_and(|h| !h.is_finished())
    }
}
struct ChannelState {
    key: ChannelKey,
    config: ChannelConfig,
    quota: Option<u64>,
    boot: BootCounter,
    version: ProtocolVersion,
    builder: BlockBuilder,
    last_time: Micros,
    opened: Option<Instant>,
    last_sync: Instant,
    dirty: bool,
    drops: Arc<DropCounters>,
    index: ChannelIdx,
    pending: Vec<Sender<Result<()>>>,
}
struct WriterLoop {
    namespaces: HashMap<usize, Vec<ChannelState>>,
    next_id: u32,
    drops_seen: u64,
    buffered_bytes: u64,
    counters: Arc<Counters>,
    backend: Arc<ReadBackend>,
    pulse: Arc<crate::pulse::Pulse>,
    buffer_ceiling: Option<u64>,
    active: HashSet<(usize, usize)>,
}
impl WriterLoop {
    fn channel(&self, ns: NsId, channel: ChannelIdx) -> Result<&ChannelState> {
        self.namespaces
            .get(&(ns.0 as usize))
            .and_then(|n| n.get(channel.0 as usize))
            .ok_or(Error::WriterDead)
    }
    fn register(&mut self, s: NsSetup) -> Result<NsId> {
        if self.namespaces.len() >= 65536 {
            return Err(Error::Backend("active namespace capacity exhausted".into()));
        }
        let next_id = self.next_id.checked_add(1).ok_or_else(|| {
            Error::Backend("namespace address space exhausted; reopen store".into())
        })?;
        let mut channels = Vec::new();
        for (i, spec) in s.channels.into_iter().enumerate() {
            let key = ChannelKey {
                namespace: s.name.clone(),
                class: spec.class,
            };
            self.backend.register_channel(
                spec.root_index,
                key.clone(),
                &s.schema_name,
                s.protocol_version.0,
            )?;
            self.backend.set_quota(&key, spec.quota_bytes)?;
            channels.push(ChannelState {
                key,
                config: spec.config,
                quota: spec.quota_bytes,
                boot: s.boot,
                version: s.protocol_version,
                builder: BlockBuilder::new(),
                last_time: Micros(0),
                opened: None,
                last_sync: Instant::now(),
                dirty: false,
                drops: s.drops.clone(),
                index: ChannelIdx(i as u16),
                pending: Vec::new(),
            });
        }
        // Never reuse an address: a producer may have staged a record before
        // the final namespace handle was released.
        let id = NsId(self.next_id);
        self.next_id = next_id;
        self.namespaces.insert(id.0 as usize, channels);
        self.pulse.roster_changed();
        Ok(id)
    }
    fn flush(
        backend: &ReadBackend,
        counters: &Counters,
        pulse: &crate::pulse::Pulse,
        ch: &mut ChannelState,
        sync: bool,
    ) -> Result<()> {
        if ch.builder.is_empty() {
            return Ok(());
        }
        let count = u64::from(ch.builder.count());
        let mut bytes = Vec::new();
        ch.builder.finish(0, ch.config.compression, &mut bytes)?;
        ch.opened = None;
        if let Err(e) = backend.append_block(&ch.key, ch.boot, ch.version, &bytes, ch.quota, sync) {
            ch.drops.record_n(ch.index, count);
            Counters::add(&counters.dropped, count);
            for reply in ch.pending.drain(..) {
                // The record was already admitted: even a logical capacity
                // refusal now means delivery was not confirmed, not caller misuse.
                let error = Error::StorageUnavailable(e.to_string());
                let _ = reply.send(Err(error));
            }
            return Err(e);
        }
        if sync {
            for reply in ch.pending.drain(..) {
                let _ = reply.send(Ok(()));
            }
        }
        Counters::add(&counters.records_written, count);
        Counters::bump(&counters.blocks_written);
        Counters::add(&counters.bytes_written, bytes.len() as u64);
        ch.dirty = !sync;
        if sync {
            ch.last_sync = Instant::now();
            Counters::bump(&counters.syncs);
        }
        ch.builder.shrink_to(0);
        pulse.data_written();
        Ok(())
    }
    fn apply(&mut self, q: Queued) -> Result<()> {
        let item = q.item;
        drop(q._permit);
        if self.buffered_bytes >= self.buffer_ceiling.unwrap_or(16 * 1024 * 1024) {
            self.flush_all()?;
        }
        let address = (item.ns.0 as usize, item.channel.0 as usize);
        let ch = self
            .namespaces
            .get_mut(&address.0)
            .and_then(|n| n.get_mut(address.1))
            .ok_or(Error::WriterDead)?;
        let held_before = ch.builder.capacity() as u64;
        if ch.builder.raw_len() >= ch.config.block_max_bytes.min(BLOCK_TARGET)
            || ch.builder.count() == u16::MAX
        {
            Self::flush(
                &self.backend,
                &self.counters,
                &self.pulse,
                ch,
                ch.config.sync_interval.is_zero(),
            )?;
        }
        let at = Micros(item.at.0.max(ch.last_time.0));
        ch.last_time = at;
        ch.builder.push(at, &item.record.as_record())?;
        ch.opened.get_or_insert_with(Instant::now);
        if let Some(reply) = q.reply {
            ch.pending.push(reply);
        }
        self.buffered_bytes =
            self.buffered_bytes.saturating_sub(held_before) + ch.builder.capacity() as u64;
        self.active.insert(address);
        Ok(())
    }
    fn flush_critical(&mut self) -> Result<()> {
        for &(n, c) in &self.active {
            if let Some(ch) = self.namespaces.get_mut(&n).and_then(|ns| ns.get_mut(c))
                && (!ch.pending.is_empty() || ch.config.sync_interval.is_zero())
            {
                let held = ch.builder.capacity() as u64;
                Self::flush(&self.backend, &self.counters, &self.pulse, ch, true)?;
                self.buffered_bytes = self.buffered_bytes.saturating_sub(held);
            }
        }
        Ok(())
    }
    fn flush_all(&mut self) -> Result<()> {
        for ns in self.namespaces.values_mut() {
            for ch in ns {
                Self::flush(
                    &self.backend,
                    &self.counters,
                    &self.pulse,
                    ch,
                    !ch.pending.is_empty() || ch.config.sync_interval.is_zero(),
                )?;
            }
        }
        self.buffered_bytes = 0;
        Ok(())
    }
    fn barrier(&mut self) -> Result<()> {
        self.flush_all()?;
        // Engine::sync(empty batch) is a no-op. Backend writes a nonempty
        // barrier on EVERY physical engine, even with no active namespace.
        self.backend.barrier_all()?;
        Counters::bump(&self.counters.syncs);
        for ns in self.namespaces.values_mut() {
            for ch in ns {
                ch.dirty = false;
                ch.last_sync = Instant::now();
            }
        }
        Ok(())
    }
    fn drain(&mut self, normal: &Receiver<Queued>, critical: &Receiver<Queued>) -> Result<bool> {
        for _ in 0..DRAIN_ROUNDS {
            let mut n = 0;
            for _ in 0..DRAIN_LIMIT {
                if let Ok(q) = critical.try_recv().or_else(|_| normal.try_recv()) {
                    self.apply(q)?;
                    n += 1;
                } else {
                    self.flush_critical()?;
                    return Ok(true);
                }
            }
            if n == 0 {
                return Ok(true);
            }
        }
        self.flush_critical()?;
        Ok(normal.is_empty() && critical.is_empty())
    }
    fn tick(&mut self) -> Result<()> {
        let now = Instant::now();
        let mut due = false;
        let mut held = 0;
        let drops_changed = self.counters.dropped.load(Ordering::Acquire);
        // Loss notices need the roster only after a new loss; inactive healthy
        // channels are never walked by the steady-state timer.
        if drops_changed != self.drops_seen {
            self.drops_seen = drops_changed;
            for (&n, ns) in &mut self.namespaces {
                for (c, ch) in ns.iter_mut().enumerate() {
                    let lost = ch.drops.take(ch.index);
                    if lost != 0 {
                        let text = crate::diag::drop_notice(lost);
                        let rec = dduroc_format::Record::Text(dduroc_format::record::Text {
                            level: dduroc_format::Level::Warn,
                            span: None,
                            target: crate::diag::TARGET,
                            text: &text,
                        });
                        ch.builder.push(ch.last_time, &rec)?;
                        ch.opened.get_or_insert(now);
                        self.active.insert((n, c));
                    }
                }
            }
        }
        for &(n, c) in &self.active {
            if let Some(ch) = self.namespaces.get_mut(&n).and_then(|ns| ns.get_mut(c)) {
                if ch
                    .opened
                    .is_some_and(|t| now.duration_since(t) >= ch.config.flush_interval)
                {
                    Self::flush(
                        &self.backend,
                        &self.counters,
                        &self.pulse,
                        ch,
                        !ch.pending.is_empty() || ch.config.sync_interval.is_zero(),
                    )?;
                }
                due |= ch.dirty && now.duration_since(ch.last_sync) >= ch.config.sync_interval;
                held += ch.builder.capacity() as u64;
            }
        }
        self.buffered_bytes = held;
        if self.buffer_ceiling.is_some_and(|cap| held > cap) {
            self.flush_all()?;
        }
        if due {
            self.barrier()?;
        }
        self.active.retain(|&(n, c)| {
            self.namespaces
                .get(&n)
                .and_then(|ns| ns.get(c))
                .is_some_and(|ch| !ch.builder.is_empty() || ch.dirty || !ch.pending.is_empty())
        });
        Ok(())
    }
    fn run(
        mut self,
        normal: Receiver<Queued>,
        critical: Receiver<Queued>,
        control: Receiver<Control>,
    ) {
        let result = self.run_inner(&normal, &critical, &control);
        if result.is_err() {
            Counters::bump(&self.counters.io_errors);
        }
        // Dropping both receivers releases blocked producers and durable ACK
        // waiters. A failed worker is never restarted in this Store instance.
        for q in normal.try_iter().chain(critical.try_iter()) {
            Counters::bump(&self.counters.dropped);
            if let Ok(ch) = self.channel(q.item.ns, q.item.channel) {
                ch.drops.record(q.item.channel);
            }
        }
    }
    fn run_inner(
        &mut self,
        normal: &Receiver<Queued>,
        critical: &Receiver<Queued>,
        control: &Receiver<Control>,
    ) -> Result<()> {
        loop {
            for _ in 0..DRAIN_LIMIT {
                match critical.try_recv() {
                    Ok(q) => self.apply(q)?,
                    Err(_) => break,
                }
            }
            self.flush_critical()?;
            if let Ok(cmd) = control.try_recv() {
                match cmd {
                    Control::Register(s, reply) => {
                        let result = self.register(*s);
                        let _ = reply.send(result);
                    }
                    Control::Key(ns, ch, reply) => {
                        let _ = reply.send(self.channel(ns, ch).map(|c| c.key.clone()));
                    }
                    Control::Sync(_ns, reply) => {
                        let drained = self.drain(normal, critical)?;
                        self.barrier()?;
                        let _ = reply.send(if drained {
                            Ok(())
                        } else {
                            Err(Error::SyncIncomplete)
                        });
                    }
                    Control::Release(ns) => {
                        self.drain(normal, critical)?;
                        self.barrier()?;
                        self.namespaces.remove(&(ns.0 as usize));
                        self.active.retain(|&(n, _)| n != ns.0 as usize);
                    }
                    Control::Shutdown(reply) => {
                        self.drain(normal, critical)?;
                        self.barrier()?;
                        let _ = reply.send(());
                        return Ok(());
                    }
                }
            }
            for _ in 0..DRAIN_LIMIT {
                match normal.try_recv() {
                    Ok(q) => self.apply(q)?,
                    Err(_) => break,
                }
            }
            self.tick()?;
            if normal.is_empty() && critical.is_empty() && control.is_empty() {
                crossbeam_channel::select_biased! {
                    recv(critical) -> q => if let Ok(q) = q { self.apply(q)?; },
                    recv(normal) -> q => if let Ok(q) = q { self.apply(q)?; },
                    default(Duration::from_millis(5)) => {},
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::staged::{OwnedValue, StagedRecord};
    use dduroc_format::{MetricId, Value};

    struct Fixture {
        _dir: tempfile::TempDir,
        writer: Arc<Writer>,
        backend: Arc<ReadBackend>,
        drops: Arc<DropCounters>,
        ns: NsId,
        key: ChannelKey,
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.writer.shutdown();
        }
    }
    fn setup(config: ChannelConfig, queues: QueueSizes) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dduroc-raft-v1"), b"dduroc raft store v1\n").unwrap();
        let backend =
            ReadBackend::open_live(&[dir.path().to_owned()], &[64 << 20], &[4 << 20]).unwrap();
        let writer = Writer::spawn(
            Arc::new(Counters::default()),
            queues,
            vec![],
            Some(1024 * 1024),
            Arc::new(crate::pulse::Pulse::new()),
            backend.clone(),
        )
        .unwrap();
        let drops = Arc::new(DropCounters::new(1));
        let class = StorageClass::Telemetry;
        let ns = writer
            .register(NsSetup {
                name: "test".into(),
                schema_name: "test".into(),
                protocol_version: ProtocolVersion(1),
                store_id: 1,
                boot: BootCounter(1),
                drops: drops.clone(),
                channels: vec![ChannelSpec {
                    dir: dir.path().join("test"),
                    group: 0,
                    class,
                    root_index: 0,
                    quota_bytes: None,
                    config,
                }],
            })
            .unwrap();
        Fixture {
            _dir: dir,
            writer,
            backend,
            drops,
            ns,
            key: ChannelKey {
                namespace: "test".into(),
                class,
            },
        }
    }
    fn item(ns: NsId, value: u64) -> Staged {
        Staged {
            ns,
            channel: ChannelIdx(0),
            at: Micros(value),
            record: StagedRecord::Sample {
                metric: MetricId(1),
                value: OwnedValue::U64(value),
            },
        }
    }
    fn wait_until(mut f: impl FnMut() -> bool) {
        let until = Instant::now() + Duration::from_secs(3);
        while !f() {
            assert!(Instant::now() < until, "writer deadline elapsed");
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    #[test]
    fn critical_ack_means_nonempty_block_is_already_readable() {
        let f = setup(ChannelConfig::critical(16 << 20), QueueSizes::default());
        f.writer.write(item(f.ns, 42), true, &f.drops).unwrap();
        let page = f
            .backend
            .read_blocks(&f.key, 0, u64::MAX, 8, 4 << 20, false)
            .unwrap();
        assert_eq!(page.blocks.len(), 1);
        let block = dduroc_format::block::Block::parse(&page.blocks[0].bytes)
            .unwrap()
            .unwrap();
        let (_, record) = block.records().next().unwrap().unwrap();
        assert!(matches!(record, dduroc_format::Record::Sample(s) if s.value == Value::U64(42)));
        assert!(f.writer.counters.snapshot().syncs > 0);
    }
    #[test]
    fn critical_burst_shares_a_durable_block() {
        let f = setup(ChannelConfig::critical(16 << 20), QueueSizes::default());
        let mut joins = Vec::new();
        let barrier = Arc::new(std::sync::Barrier::new(33));
        for value in 0..32 {
            let writer = f.writer.clone();
            let drops = f.drops.clone();
            let barrier = barrier.clone();
            let ns = f.ns;
            joins.push(std::thread::spawn(move || {
                barrier.wait();
                writer.write(item(ns, value), true, &drops).unwrap();
            }));
        }
        barrier.wait();
        for join in joins {
            join.join().unwrap();
        }
        assert_eq!(f.writer.counters.snapshot().records_written, 32);
        assert!(
            f.writer.counters.snapshot().syncs < 32,
            "a burst must group commit"
        );
    }
    #[test]
    fn timer_flushes_and_syncs_without_a_manual_sync() {
        let mut config = ChannelConfig::new(16 << 20);
        config.flush_interval = Duration::from_millis(10);
        config.sync_interval = Duration::from_millis(25);
        let f = setup(config, QueueSizes::default());
        f.writer.write(item(f.ns, 1), false, &f.drops).unwrap();
        wait_until(|| f.writer.counters.snapshot().records_written == 1);
        wait_until(|| f.writer.counters.snapshot().syncs > 0);
    }
    #[test]
    fn asynchronous_failure_is_sticky_for_sync_and_later_critical_writes() {
        let mut config = ChannelConfig::new(16 << 20);
        config.flush_interval = Duration::from_millis(10);
        let f = setup(config, QueueSizes::default());
        f.backend.fail_for_test();
        f.writer.write(item(f.ns, 1), false, &f.drops).unwrap();
        wait_until(|| !f.writer.is_alive());
        assert!(f.writer.sync(None).is_err());
        assert!(f.writer.write(item(f.ns, 2), true, &f.drops).is_err());
        assert!(f.writer.counters.snapshot().io_errors > 0);
    }
    #[test]
    fn admitted_critical_capacity_failure_is_delivery_loss_and_fences_later_sync() {
        let f = setup(ChannelConfig::critical(16 << 20), QueueSizes::default());
        f.backend.set_class_limits([1; 3]).unwrap();
        let error = f
            .writer
            .write(item(f.ns, 1), true, &f.drops)
            .expect_err("the encoded block cannot fit");
        assert!(
            error.loses_record(),
            "admitted record failure cannot be caller misuse: {error}"
        );
        assert!(matches!(&error, Error::StorageUnavailable(_)));
        assert!(
            error.to_string().contains("class budget"),
            "keep backend diagnostics: {error}"
        );
        let sync = f
            .writer
            .sync(None)
            .expect_err("the failed writer cannot acknowledge a barrier");
        assert!(sync.loses_record());
        assert!(
            f.writer
                .write(item(f.ns, 2), true, &f.drops)
                .unwrap_err()
                .loses_record()
        );
        assert!(f.writer.counters.snapshot().dropped >= 1);
    }

    #[test]
    fn oversized_record_is_rejected_before_enqueue_and_does_not_poison_writer() {
        let f = setup(ChannelConfig::critical(16 << 20), QueueSizes::default());
        let mut oversized = item(f.ns, 1);
        oversized.record = StagedRecord::Sample {
            metric: MetricId(1),
            value: OwnedValue::Blob(vec![0; MAX_RECORD_BYTES + 1].into()),
        };
        assert!(f.writer.write(oversized.clone(), true, &f.drops).is_err());
        assert!(f.writer.write(oversized, false, &f.drops).is_err());
        assert_eq!(f.writer.counters.snapshot().records_written, 0);
        f.writer.write(item(f.ns, 2), true, &f.drops).unwrap();
        assert_eq!(f.writer.counters.snapshot().records_written, 1);
    }
    #[test]
    fn span_drop_path_does_not_wait_for_durable_ack() {
        let f = setup(ChannelConfig::critical(16 << 20), QueueSizes::default());
        let start = Instant::now();
        f.writer
            .write_no_wait(item(f.ns, 1), true, &f.drops)
            .unwrap();
        assert!(start.elapsed() < Duration::from_secs(1));
        f.writer.sync(None).unwrap();
        assert_eq!(f.writer.counters.snapshot().records_written, 1);
    }
    #[test]
    fn stopping_writer_refuses_and_counts_the_record() {
        let f = setup(ChannelConfig::new(16 << 20), QueueSizes::default());
        f.writer.shutdown();
        assert!(matches!(
            f.writer.write(item(f.ns, 1), false, &f.drops),
            Err(Error::ShuttingDown)
        ));
        assert_eq!(f.drops.take(ChannelIdx(0)), 1);
        assert_eq!(f.writer.counters.snapshot().dropped, 1);
    }
    #[test]
    fn empty_manual_sync_writes_a_real_backend_barrier() {
        let f = setup(ChannelConfig::new(16 << 20), QueueSizes::default());
        f.writer.sync(None).unwrap();
        assert_eq!(f.writer.counters.snapshot().syncs, 1);
    }
    fn parked_handle(
        backend: Arc<ReadBackend>,
    ) -> (Arc<Writer>, Receiver<Queued>, Receiver<Queued>) {
        let (normal, nr) = crossbeam_channel::bounded(1);
        let (critical, cr) = crossbeam_channel::bounded(1);
        let (control, _ctl) = crossbeam_channel::bounded(1);
        (
            Arc::new(Writer {
                normal,
                critical,
                control,
                backend,
                counters: Arc::new(Counters::default()),
                stopping: AtomicBool::new(false),
                queued_bytes: std::array::from_fn(|_| Arc::new(AtomicUsize::new(0))),
                handle: Mutex::new(None),
            }),
            nr,
            cr,
        )
    }
    #[test]
    fn normal_overflow_and_nonwaiting_critical_overflow_count_channel_losses() {
        let f = setup(ChannelConfig::new(16 << 20), QueueSizes::default());
        let (writer, _nr, _cr) = parked_handle(f.backend.clone());
        writer.write(item(f.ns, 1), false, &f.drops).unwrap();
        assert!(matches!(
            writer.write(item(f.ns, 2), false, &f.drops),
            Err(Error::QueueFull)
        ));
        writer.write_no_wait(item(f.ns, 3), true, &f.drops).unwrap();
        let start = Instant::now();
        assert!(matches!(
            writer.write_no_wait(item(f.ns, 4), true, &f.drops),
            Err(Error::QueueFull)
        ));
        assert!(start.elapsed() < Duration::from_millis(100));
        assert_eq!(f.drops.take(ChannelIdx(0)), 2);
        assert_eq!(writer.counters.snapshot().dropped, 2);
    }
    #[test]
    fn critical_full_queue_waits_for_room_then_waits_for_durable_reply() {
        let f = setup(ChannelConfig::new(16 << 20), QueueSizes::default());
        let (writer, _nr, cr) = parked_handle(f.backend.clone());
        writer.write_no_wait(item(f.ns, 1), true, &f.drops).unwrap();
        let producer = writer.clone();
        let drops = f.drops.clone();
        let ns = f.ns;
        let (done, completed) = crossbeam_channel::bounded(1);
        let thread = std::thread::spawn(move || {
            let result = producer.write(item(ns, 2), true, &drops);
            done.send(result).unwrap();
        });
        wait_until(|| writer.counters.snapshot().backpressure_waits == 1);
        assert!(completed.try_recv().is_err());
        drop(cr.recv().unwrap());
        let queued = cr.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            completed.try_recv().is_err(),
            "queue admission is not an ACK"
        );
        queued.reply.unwrap().send(Ok(())).unwrap();
        completed
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        thread.join().unwrap();
    }
    #[test]
    fn queue_byte_pressure_isolated_by_priority_and_tickets_are_reclaimed() {
        let f = setup(ChannelConfig::new(16 << 20), QueueSizes::default());
        let (normal, nr) = crossbeam_channel::bounded(32);
        let (critical, cr) = crossbeam_channel::bounded(32);
        let (control, _ctl) = crossbeam_channel::bounded(1);
        let writer = Writer {
            normal,
            critical,
            control,
            backend: f.backend.clone(),
            counters: Arc::new(Counters::default()),
            stopping: AtomicBool::new(false),
            queued_bytes: std::array::from_fn(|_| Arc::new(AtomicUsize::new(0))),
            handle: Mutex::new(None),
        };
        let mut blob = item(f.ns, 1);
        blob.record = StagedRecord::Sample {
            metric: MetricId(1),
            value: OwnedValue::Blob(vec![0; MAX_RECORD_BYTES].into()),
        };
        for _ in 0..15 {
            writer.write_no_wait(blob.clone(), false, &f.drops).unwrap();
        }
        assert!(matches!(
            writer.write_no_wait(blob.clone(), false, &f.drops),
            Err(Error::QueueFull)
        ));
        writer.write_no_wait(blob, true, &f.drops).unwrap();
        assert!(writer.queued_bytes[0].load(Ordering::Acquire) <= QUEUE_BYTE_LIMITS[0]);
        assert!(writer.queued_bytes[1].load(Ordering::Acquire) <= QUEUE_BYTE_LIMITS[1]);
        for queued in nr.try_iter().chain(cr.try_iter()) {
            drop(queued);
        }
        drop(nr);
        drop(cr);
        assert_eq!(writer.queued_bytes[0].load(Ordering::Acquire), 0);
        assert_eq!(writer.queued_bytes[1].load(Ordering::Acquire), 0);
    }
    #[test]
    fn release_reopen_reclaims_state_and_never_reuses_a_stale_address() {
        let f = setup(ChannelConfig::new(16 << 20), QueueSizes::default());
        let mut previous = f.ns;
        for _ in 0..64 {
            f.writer.write(item(previous, 1), false, &f.drops).unwrap();
            f.writer.release(previous);
            let next = f
                .writer
                .register(NsSetup {
                    name: "test".into(),
                    schema_name: "test".into(),
                    protocol_version: ProtocolVersion(1),
                    store_id: 1,
                    boot: BootCounter(1),
                    drops: f.drops.clone(),
                    channels: vec![ChannelSpec {
                        dir: f._dir.path().join("test"),
                        group: 0,
                        class: f.key.class,
                        root_index: 0,
                        quota_bytes: None,
                        config: ChannelConfig::new(16 << 20),
                    }],
                })
                .unwrap();
            assert!(next.0 > previous.0);
            assert!(f.writer.channel_key(previous, ChannelIdx(0)).is_err());
            previous = next;
        }
        f.writer.sync(None).unwrap();
        assert_eq!(f.writer.counters.snapshot().records_written, 64);
        assert_eq!(f.backend.catalog().unwrap().len(), 1);
    }
}
