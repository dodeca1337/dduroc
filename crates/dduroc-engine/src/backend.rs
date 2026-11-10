//! Bounded reads of the versioned raft-backed block store.
//!
//! Payloads are dduroc-format blocks, never raft-engine metadata values.
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use dduroc_format::{BootCounter, ProtocolVersion};
use protobuf::well_known_types::BytesValue;
use raft_engine::{Config, Engine, LogBatch, MessageExt, RecoveryMode};
use serde::{Deserialize, Serialize};

use crate::{Error, Result, schema::StorageClass};

const META_KEY: &[u8] = b"dduroc.channel.v1";
const COUNTS_KEY: &[u8] = b"dduroc.counts.v1";
const MAX_PAGE_ENTRIES: usize = 1024;
const MAX_PAGE_BYTES: usize = 64 * 1024 * 1024;
// A single block is one raft batch, so reading an entry cannot cause raft to
// allocate an arbitrarily large writer-produced batch.
const MAX_BLOCK_BYTES: usize = 4 * 1024 * 1024;
const ENVELOPE_BYTES: usize = 22;
const SHADOW_PREFIX: &[u8] = b"dduroc.shadow.";
fn shadow_key(id: u64) -> Vec<u8> {
    format!("dduroc.shadow.{id}").into_bytes()
}
const MAX_ROOT_ALLOCATED: u64 = 1536 * 1024 * 1024;

fn allocated_bytes(path: &Path) -> Result<u64> {
    let mut total = std::fs::symlink_metadata(path)
        .map_err(backend_error)?
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?;
    let mut dirs = vec![path.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for item in std::fs::read_dir(&dir).map_err(backend_error)? {
            let path = item.map_err(backend_error)?.path();
            let meta = std::fs::symlink_metadata(&path).map_err(backend_error)?;
            total = total
                .checked_add(meta.blocks().saturating_mul(512))
                .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?;
            if meta.is_dir() {
                dirs.push(path);
            }
        }
    }
    Ok(total)
}

#[derive(Default)]
struct MetadataAccounting {
    files: HashMap<PathBuf, u64>,
    total: u64,
}

impl MetadataAccounting {
    fn collect(root: &Path) -> Result<Self> {
        let mut accounting = Self::default();
        let mut pending = vec![root.to_path_buf()];
        while let Some(path) = pending.pop() {
            if path == root.join("raft-engine") {
                continue;
            }
            let metadata = std::fs::symlink_metadata(&path).map_err(backend_error)?;
            if metadata.is_dir() {
                for entry in std::fs::read_dir(&path).map_err(backend_error)? {
                    pending.push(entry.map_err(backend_error)?.path());
                }
            }
            accounting.update(&path)?;
            if accounting.files.len() > 262_144 {
                return Err(Error::Backend("metadata catalog exceeds limit".into()));
            }
        }
        Ok(accounting)
    }
    fn update(&mut self, path: &Path) -> Result<()> {
        let new = match std::fs::symlink_metadata(path) {
            Ok(meta) => meta
                .blocks()
                .checked_mul(512)
                .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(backend_error(error)),
        };
        let old = self.files.remove(path).unwrap_or(0);
        self.total = self
            .total
            .checked_sub(old)
            .and_then(|sum| sum.checked_add(new))
            .ok_or_else(|| Error::Backend("metadata byte count overflow".into()))?;
        if new != 0 {
            self.files.insert(path.to_path_buf(), new);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelKey {
    pub namespace: String,
    pub class: StorageClass,
}

#[derive(Debug, Clone)]
pub struct ChannelInfo {
    pub key: ChannelKey,
    pub schema_name: String,
    pub protocol_version: u16,
    pub first_index: Option<u64>,
    pub last_index: Option<u64>,
    /// The next monotonic block index, retained even when history is empty.
    pub next_write_index: u64,
    pub retained_bytes: u64,
    pub quota_bytes: Option<u64>,
    pub counting_state: CountingState,
    pub retained_records: Option<u64>,
    pub retained_blocks: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountingState {
    Complete,
    BackfillRequired,
    InProgress,
}

/// A channel identity and bounds captured under the backend mutation lock.
#[derive(Debug, Clone)]
pub struct ChannelSnapshot {
    pub info: ChannelInfo,
    /// Opaque physical generation, changed when a channel is rewritten.
    pub generation: (usize, u64),
    owner: Arc<()>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotStatus {
    Current,
    Replaced,
    Evicted,
}

#[derive(Debug, Clone)]
pub struct StoredBlock {
    pub index: u64,
    pub boot: BootCounter,
    pub protocol_version: ProtocolVersion,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct BlockPage {
    pub blocks: Vec<StoredBlock>,
    pub next_index: Option<u64>,
    pub first_index: Option<u64>,
    pub last_index: Option<u64>,
    pub next_write_index: u64,
}

#[derive(Debug, Clone)]
pub struct RootUsage {
    pub root: PathBuf,
    pub allocated_bytes: u64,
    pub logical_retained_bytes: u64,
    pub physical_limit_bytes: u64,
}

/// Constant-time global aggregates maintained with the persisted count index.
#[derive(Debug, Clone)]
pub struct CachedStatistics {
    pub counting_state: CountingState,
    pub retained_records: Option<u64>,
    pub retained_blocks: Option<u64>,
    pub logical_class_bytes: [u64; 3],
    pub class_limits: [u64; 3],
    pub channels: u64,
}

/// Allocated filesystem blocks, not logical retained payload bytes.
#[derive(Debug, Clone)]
pub struct RootPhysicalStats {
    pub index: usize,
    pub allocated_bytes: u64,
    pub log_bytes: u64,
    pub rewrite_bytes: u64,
    pub metadata_bytes: u64,
    pub physical_limit_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ChannelMeta {
    pub namespace: String,
    pub class: u8,
    pub schema_name: String,
    pub protocol_version: u16,
    pub retained_bytes: u64,
    pub next_index: u64,
    pub quota_bytes: Option<u64>,
}

/// Counts of retained on-disk wire records, including text and extensions.
/// Incomplete counts are never presented as totals.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct CountIndex {
    version: u8,
    next: u64,
    records: u64,
    blocks: u64,
    complete: bool,
}
impl CountIndex {
    fn empty(next: u64, complete: bool) -> Self {
        Self {
            version: 1,
            next,
            records: 0,
            blocks: 0,
            complete,
        }
    }
    fn add(&mut self, count: u64) -> Result<()> {
        self.records = self
            .records
            .checked_add(count)
            .ok_or_else(|| Error::Backend("record count overflow".into()))?;
        self.blocks = self
            .blocks
            .checked_add(1)
            .ok_or_else(|| Error::Backend("block count overflow".into()))?;
        Ok(())
    }
    fn remove(&mut self, count: u64) -> Result<()> {
        self.records = self
            .records
            .checked_sub(count)
            .ok_or_else(|| Error::Backend("invalid record count".into()))?;
        self.blocks = self
            .blocks
            .checked_sub(1)
            .ok_or_else(|| Error::Backend("invalid block count".into()))?;
        Ok(())
    }
}
fn block_record_count(bytes: &[u8]) -> Result<Option<u64>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let header = dduroc_format::block::BlockHeader::parse(bytes)?
        .ok_or_else(|| Error::Backend("empty block header".into()))?;
    let body = bytes
        .get(dduroc_format::block::BlockHeader::SIZE..)
        .ok_or_else(|| Error::Backend("truncated block".into()))?;
    if body.len() != header.body_len as usize {
        return Err(Error::Backend("invalid block length".into()));
    }
    header.verify(body)?;
    Ok(Some(u64::from(header.count)))
}
fn load_counts(engine: &Engine, id: u64) -> Result<Option<CountIndex>> {
    let Some(bytes) = engine.get(id, COUNTS_KEY) else {
        return Ok(None);
    };
    let counts: CountIndex = postcard::from_bytes(&bytes)?;
    if counts.version != 1 {
        return Err(Error::Backend("unsupported count index version".into()));
    }
    Ok(Some(counts))
}
fn count_state(counts: Option<CountIndex>) -> CountingState {
    match counts {
        None => CountingState::BackfillRequired,
        Some(counts) if counts.complete => CountingState::Complete,
        Some(_) => CountingState::InProgress,
    }
}
fn put_counts(batch: &mut LogBatch, id: u64, counts: &CountIndex) -> Result<()> {
    batch
        .put(id, COUNTS_KEY.to_vec(), postcard::to_allocvec(counts)?)
        .map_err(backend_error)
}

#[derive(Debug)]
pub(crate) struct BlockEntry;
impl MessageExt for BlockEntry {
    type Entry = BytesValue;
    fn index(entry: &BytesValue) -> u64 {
        // Only our encoder writes this header; malformed disk values are also
        // checked at the public read boundary before their contents are used.
        entry
            .value
            .get(..8)
            .and_then(|s| s.try_into().ok())
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Copy)]
pub(crate) enum CommitFault {
    BeforeEio = 1,
    BeforeEnospc = 2,
    AfterEio = 3,
    AfterEnospc = 4,
}

pub struct ReadBackend {
    roots: Vec<(PathBuf, Arc<Engine>)>,
    index: RwLock<HashMap<ChannelKey, (usize, u64)>>,
    mutation: Mutex<()>,
    failed: Arc<AtomicBool>,
    #[cfg(test)]
    commit_fault: std::sync::atomic::AtomicU8,
    physical_limits: Vec<u64>,
    rewrite_reserves: Vec<u64>,
    targets: Vec<u64>,
    retained_blocks: AtomicUsize,
    class_limits: RwLock<[u64; 3]>,
    class_index: Mutex<ClassIndex>,
    workers: Vec<RootWorker>,
    snapshot_owner: Arc<()>,
    metadata_accounting: Mutex<Vec<MetadataAccounting>>,
}

enum RootRequest {
    Read(
        u64,
        u64,
        std::sync::mpsc::SyncSender<std::result::Result<Option<BytesValue>, String>>,
    ),
    Purge(std::sync::mpsc::SyncSender<std::result::Result<(), String>>),
    #[cfg(test)]
    Panic,
}

struct RootWorker {
    sender: Option<std::sync::mpsc::SyncSender<RootRequest>>,
    thread: Option<std::thread::JoinHandle<()>>,
    failed: Arc<AtomicBool>,
}
impl RootWorker {
    fn start(engine: Arc<Engine>, failed: Arc<AtomicBool>) -> Result<Self> {
        let (sender, receiver) = std::sync::mpsc::sync_channel::<RootRequest>(64);
        let thread_failed = Arc::clone(&failed);
        let thread = std::thread::Builder::new()
            .name("dduroc-raft-reader".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    while let Ok(request) = receiver.recv() {
                        match request {
                            RootRequest::Read(id, at, reply) => {
                                let _ = reply.send(
                                    engine
                                        .get_entry::<BlockEntry>(id, at)
                                        .map_err(|e| e.to_string()),
                                );
                            }
                            RootRequest::Purge(reply) => {
                                let _ = reply.send(
                                    engine
                                        .purge_expired_files()
                                        .map(|_| ())
                                        .map_err(|e| e.to_string()),
                                );
                            }
                            #[cfg(test)]
                            RootRequest::Panic => panic!("injected root worker failure"),
                        }
                    }
                }));
                if result.is_err() {
                    thread_failed.store(true, Ordering::Release);
                }
            })
            .map_err(backend_error)?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
            failed,
        })
    }
    fn read(&self, id: u64, at: u64) -> Result<Option<BytesValue>> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or(Error::WriterDead)?
            .send(RootRequest::Read(id, at, tx))
            .map_err(|_| {
                self.failed.store(true, Ordering::Release);
                Error::WriterDead
            })?;
        rx.recv()
            .map_err(|_| {
                self.failed.store(true, Ordering::Release);
                Error::WriterDead
            })?
            .map_err(Error::StorageUnavailable)
    }
    fn purge(&self) -> Result<()> {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or(Error::WriterDead)?
            .send(RootRequest::Purge(tx))
            .map_err(|_| {
                self.failed.store(true, Ordering::Release);
                Error::WriterDead
            })?;
        rx.recv()
            .map_err(|_| {
                self.failed.store(true, Ordering::Release);
                Error::WriterDead
            })?
            .map_err(Error::StorageUnavailable)
    }
}
impl Drop for RootWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Default)]
struct ClassIndex {
    totals: [u64; 3],
    heads: [BTreeSet<(u64, usize, u64)>; 3],
    counts: CountTotals,
}
#[derive(Debug, Default)]
struct CountTotals {
    records: u64,
    blocks: u64,
    // value is true for backfill-required, false for in-progress.
    pending: BTreeMap<(usize, u64), (bool, ChannelKey)>,
    required: usize,
}
impl CountTotals {
    fn numbers(&self, changes: &[(Option<CountIndex>, Option<CountIndex>)]) -> Result<(u64, u64)> {
        let (mut records, mut blocks) = (self.records, self.blocks);
        for (old, new) in changes {
            if let Some(old) = old.filter(|c| c.complete) {
                records = records
                    .checked_sub(old.records)
                    .ok_or_else(|| Error::Backend("invalid aggregate record count".into()))?;
                blocks = blocks
                    .checked_sub(old.blocks)
                    .ok_or_else(|| Error::Backend("invalid aggregate block count".into()))?;
            }
            if let Some(new) = new.filter(|c| c.complete) {
                records = records
                    .checked_add(new.records)
                    .ok_or_else(|| Error::Backend("aggregate record count overflow".into()))?;
                blocks = blocks
                    .checked_add(new.blocks)
                    .ok_or_else(|| Error::Backend("aggregate block count overflow".into()))?;
            }
        }
        Ok((records, blocks))
    }
    fn insert_initial(
        &mut self,
        root: usize,
        id: u64,
        key: &ChannelKey,
        counts: Option<CountIndex>,
    ) -> Result<()> {
        if let Some(counts) = counts.filter(|c| c.complete) {
            self.records = self
                .records
                .checked_add(counts.records)
                .ok_or_else(|| Error::Backend("aggregate record count overflow".into()))?;
            self.blocks = self
                .blocks
                .checked_add(counts.blocks)
                .ok_or_else(|| Error::Backend("aggregate block count overflow".into()))?;
        } else {
            let required = counts.is_none();
            self.pending.insert((root, id), (required, key.clone()));
            self.required += usize::from(required);
        }
        Ok(())
    }
    fn apply(
        &mut self,
        root: usize,
        old_id: u64,
        new_id: u64,
        new: Option<CountIndex>,
        key: Option<&ChannelKey>,
        totals: (u64, u64),
    ) {
        self.records = totals.0;
        self.blocks = totals.1;
        let old_pending = self.pending.remove(&(root, old_id));
        if let Some((required, _)) = &old_pending {
            self.required -= usize::from(*required);
        }
        if !new.is_some_and(|c| c.complete) {
            let required = new.is_none();
            let key = key
                .cloned()
                .or_else(|| old_pending.map(|(_, key)| key))
                .expect("pending channel key");
            self.pending.insert((root, new_id), (required, key));
            self.required += usize::from(required);
        }
    }
    fn state(&self) -> CountingState {
        if self.required > 0 {
            CountingState::BackfillRequired
        } else if !self.pending.is_empty() {
            CountingState::InProgress
        } else {
            CountingState::Complete
        }
    }
}

fn subtract_counted_prefix(
    worker: &RootWorker,
    id: u64,
    counts: &mut CountIndex,
    start: u64,
    end: u64,
) -> Result<()> {
    for index in start..end.min(counts.next) {
        let entry = worker
            .read(id, index)?
            .ok_or_else(|| Error::Backend("missing counted block".into()))?;
        if entry.value.len() < ENVELOPE_BYTES
            || u64::from_le_bytes(entry.value[..8].try_into().unwrap()) != index
        {
            return Err(Error::Backend("invalid block envelope".into()));
        }
        if let Some(count) = block_record_count(&entry.value[ENVELOPE_BYTES..])? {
            counts.remove(count)?;
        }
    }
    Ok(())
}

fn head_sequence(worker: &RootWorker, id: u64, at: u64) -> Result<u64> {
    let entry = worker
        .read(id, at)?
        .ok_or_else(|| Error::Backend("missing retained block".into()))?;
    if entry.value.len() < ENVELOPE_BYTES {
        return Err(Error::Backend("truncated block envelope".into()));
    }
    Ok(u64::from_le_bytes(entry.value[14..22].try_into().unwrap()))
}

impl std::fmt::Debug for ReadBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadBackend")
            .field(
                "roots",
                &self.roots.iter().map(|(p, _)| p).collect::<Vec<_>>(),
            )
            .finish()
    }
}

fn backend_error(e: impl std::fmt::Display) -> Error {
    Error::Backend(e.to_string())
}

impl ReadBackend {
    /// Resolve a namespace schema without cloning or scanning the catalog.
    pub fn schema_name(&self, namespace: &str) -> Result<Option<String>> {
        let index = self.index.read().map_err(backend_error)?;
        for class in StorageClass::ALL {
            let key = ChannelKey {
                namespace: namespace.to_owned(),
                class,
            };
            if let Some(&(root, id)) = index.get(&key) {
                let data = self.roots[root]
                    .1
                    .get(id, META_KEY)
                    .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
                let meta: ChannelMeta = postcard::from_bytes(&data)?;
                return Ok(Some(meta.schema_name));
            }
        }
        Ok(None)
    }
    /// Visit a single, atomically captured roster. The callback runs under the
    /// mutation lock and must remain quick and never call back into the store.
    pub fn snapshot_channels(
        &self,
        accept: impl FnMut(&ChannelKey) -> Result<bool>,
    ) -> Result<Vec<ChannelSnapshot>> {
        self.snapshot_channels_with(|| Ok(()), accept)
            .map(|(_, channels)| channels)
    }

    /// Capture auxiliary metadata under the same mutation lock as the roster.
    /// Neither callback may re-enter the backend or wait on store schema locks.
    pub fn snapshot_channels_with<T>(
        &self,
        capture: impl FnOnce() -> Result<T>,
        mut accept: impl FnMut(&ChannelKey) -> Result<bool>,
    ) -> Result<(T, Vec<ChannelSnapshot>)> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let captured = capture()?;
        let index = self.index.read().map_err(backend_error)?;
        let mut out = Vec::new();
        for (key, &(root, id)) in index.iter() {
            if !accept(key)? {
                continue;
            }
            let engine = &self.roots[root].1;
            let bytes = engine
                .get(id, META_KEY)
                .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
            let meta: ChannelMeta = postcard::from_bytes(&bytes)?;
            let counts = load_counts(engine, id)?;
            out.push(ChannelSnapshot {
                info: ChannelInfo {
                    key: key.clone(),
                    schema_name: meta.schema_name,
                    protocol_version: meta.protocol_version,
                    first_index: engine.first_index(id),
                    last_index: engine.last_index(id),
                    next_write_index: meta.next_index,
                    retained_bytes: meta.retained_bytes,
                    quota_bytes: meta.quota_bytes,
                    counting_state: count_state(counts),
                    retained_records: counts.filter(|c| c.complete).map(|c| c.records),
                    retained_blocks: counts.filter(|c| c.complete).map(|c| c.blocks),
                },
                generation: (root, id),
                owner: Arc::clone(&self.snapshot_owner),
            });
        }
        out.sort_by(|a, b| {
            (&a.info.key.namespace, a.info.key.class)
                .cmp(&(&b.info.key.namespace, b.info.key.class))
        });
        Ok((captured, out))
    }

    /// Check generation and the next unread index without racing retention.
    pub fn validate_snapshot(
        &self,
        channel: &ChannelSnapshot,
        next_unread: Option<u64>,
    ) -> Result<bool> {
        if !Arc::ptr_eq(&self.snapshot_owner, &channel.owner) {
            return Ok(false);
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let index = self.index.read().map_err(backend_error)?;
        let Some(&(root, id)) = index.get(&channel.info.key) else {
            return Ok(false);
        };
        if (root, id) != channel.generation {
            return Ok(false);
        }
        if let Some(next) = next_unread
            && self.roots[root]
                .1
                .first_index(id)
                .is_none_or(|first| first > next)
        {
            return Ok(false);
        }
        Ok(true)
    }

    /// Atomically classify a frozen session's unread position.
    pub fn snapshot_status(
        &self,
        channel: &ChannelSnapshot,
        required: Option<u64>,
    ) -> Result<SnapshotStatus> {
        if !Arc::ptr_eq(&self.snapshot_owner, &channel.owner) {
            return Ok(SnapshotStatus::Replaced);
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let index = self.index.read().map_err(backend_error)?;
        let Some(&(root, id)) = index.get(&channel.info.key) else {
            return Ok(SnapshotStatus::Replaced);
        };
        if (root, id) != channel.generation {
            return Ok(SnapshotStatus::Replaced);
        }
        if required.is_some_and(|at| {
            self.roots[root]
                .1
                .first_index(id)
                .is_none_or(|first| first > at)
        }) {
            return Ok(SnapshotStatus::Evicted);
        }
        Ok(SnapshotStatus::Current)
    }

    /// Atomically check activation and an observed live-follow watermark.
    pub fn follow_status(
        &self,
        channel: &ChannelSnapshot,
        required: u64,
    ) -> Result<SnapshotStatus> {
        if !Arc::ptr_eq(&self.snapshot_owner, &channel.owner) {
            return Ok(SnapshotStatus::Replaced);
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let index = self.index.read().map_err(backend_error)?;
        let Some(&(root, id)) = index.get(&channel.info.key) else {
            return Ok(SnapshotStatus::Replaced);
        };
        if (root, id) != channel.generation {
            return Ok(SnapshotStatus::Replaced);
        }
        let engine = &self.roots[root].1;
        let data = engine
            .get(id, META_KEY)
            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
        let meta: ChannelMeta = postcard::from_bytes(&data)?;
        if engine.first_index(id).is_some_and(|first| first > required)
            || (engine.first_index(id).is_none() && meta.next_index > required)
        {
            return Ok(SnapshotStatus::Evicted);
        }
        Ok(SnapshotStatus::Current)
    }

    /// Read only if the captured activation still exists and the requested
    /// block was not evicted. The check and read share the mutation lock.
    pub fn read_snapshot_block(
        &self,
        channel: &ChannelSnapshot,
        index: u64,
    ) -> Result<Option<BlockPage>> {
        if channel.info.first_index.is_none_or(|first| index < first)
            || channel.info.last_index.is_none_or(|last| index > last)
        {
            return Ok(None);
        }
        self.read_generation_block(channel, index)
    }

    /// Read an index from the same generation, including blocks appended
    /// after capture. Intended for controlled live subscriptions.
    pub fn read_generation_block(
        &self,
        channel: &ChannelSnapshot,
        index: u64,
    ) -> Result<Option<BlockPage>> {
        if !Arc::ptr_eq(&self.snapshot_owner, &channel.owner) {
            return Ok(None);
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        {
            let index_map = self.index.read().map_err(backend_error)?;
            let Some(&(root, id)) = index_map.get(&channel.info.key) else {
                return Ok(None);
            };
            if (root, id) != channel.generation
                || self.roots[root]
                    .1
                    .first_index(id)
                    .is_none_or(|first| first > index)
            {
                return Ok(None);
            }
        }
        let end = index
            .checked_add(1)
            .ok_or_else(|| Error::Backend("block index exhausted".into()))?;
        Ok(Some(self.read_blocks(
            &channel.info.key,
            index,
            end,
            1,
            MAX_PAGE_BYTES,
            false,
        )?))
    }
    #[cfg(test)]
    pub(crate) fn inject_commit_fault(&self, fault: CommitFault) {
        self.commit_fault.store(fault as u8, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn fail_for_test(&self) {
        self.failed.store(true, Ordering::Release);
    }

    pub(crate) fn open_live(
        roots: &[PathBuf],
        budgets: &[u64],
        targets: &[u64],
    ) -> Result<Arc<Self>> {
        if roots.len() != budgets.len() || roots.len() != targets.len() {
            return Err(Error::Backend("root budget count mismatch".into()));
        }
        let mut opened = Vec::with_capacity(roots.len());
        for ((root, budget), target) in roots.iter().zip(budgets).zip(targets) {
            let marker = root.join("dduroc-raft-v1");
            let path = root.join("raft-engine");
            let missing_marker = match crate::fsutil::read_optional_bounded(&marker)? {
                Some(value)
                    if value == b"dduroc raft store v1\n"
                        || value == b"dduroc raft store v2 stats-writer\n" =>
                {
                    false
                }
                None => true,
                Some(_) => return Err(Error::Backend("unknown raft store marker".into())),
            };
            crate::fsutil::create_dir_all_synced(&path)?;
            let engine = Arc::new(open_root(&path, *budget, *target)?);
            // An interrupted first open may leave an empty engine before its
            // marker is installed. Never adopt unmarked existing history.
            if missing_marker && !engine.raft_groups().is_empty() {
                return Err(Error::Backend(
                    "raft store marker missing for nonempty root".into(),
                ));
            }
            opened.push((root.clone(), engine));
        }
        if opened.is_empty() {
            return Err(Error::Backend("no roots".into()));
        }
        let mut backend = Self::from_roots(opened)?;
        backend.physical_limits = budgets
            .iter()
            .map(|budget| {
                budget
                    .saturating_add((budget / 2).max(8 * 1024 * 1024))
                    .min(MAX_ROOT_ALLOCATED)
            })
            .collect();
        backend.rewrite_reserves = targets
            .iter()
            .map(|target| target.saturating_mul(2))
            .collect();
        backend.targets = targets.to_vec();
        Ok(Arc::new(backend))
    }

    pub(crate) fn root_index(&self, root: &Path) -> Result<usize> {
        let wanted = std::fs::canonicalize(root).map_err(backend_error)?;
        self.roots
            .iter()
            .position(|(path, _)| std::fs::canonicalize(path).ok().as_ref() == Some(&wanted))
            .ok_or_else(|| Error::Backend("unknown root".into()))
    }
    pub fn open_dump(roots: &[PathBuf]) -> Result<Arc<Self>> {
        for root in roots {
            let marker = root.join("dduroc-raft-v1");
            let value = crate::fsutil::read_required_bounded(&marker)?;
            if value != b"dduroc raft store v1\n" && value != b"dduroc raft store v2 stats-writer\n"
            {
                return Err(Error::Backend("unknown raft store marker".into()));
            }
            if !root.join("raft-engine").is_dir() {
                return Err(Error::Backend(format!(
                    "raft store missing at {}",
                    root.display()
                )));
            }
        }
        let mut opened = Vec::new();
        for root in roots {
            let path = root.join("raft-engine");
            if !path.is_dir() {
                return Err(Error::Backend(format!(
                    "raft store missing at {}",
                    path.display()
                )));
            }
            opened.push((
                root.clone(),
                Arc::new(open_root(&path, 1024 * 1024 * 1024, 4 * 1024 * 1024)?),
            ));
        }
        if opened.is_empty() {
            return Err(Error::Backend("no roots".into()));
        }
        let store_id = opened[0]
            .1
            .get(0, b"dduroc.store-id")
            .ok_or_else(|| Error::Backend("root store identity missing".into()))?;
        let store_meta = crate::store::StoreMeta::read(&roots[0])?
            .ok_or_else(|| Error::Backend("store metadata missing".into()))?;
        if store_id.as_slice() != store_meta.store_id.to_le_bytes() {
            return Err(Error::Backend("store metadata identity mismatch".into()));
        }
        if opened.iter().any(|(_, engine)| {
            engine.get(0, b"dduroc.store-id").as_deref() != Some(store_id.as_slice())
        }) {
            return Err(Error::Backend("foreign root store identity".into()));
        }
        Ok(Arc::new(Self::from_roots(opened)?))
    }

    pub fn epochs(&self) -> Result<crate::Epochs> {
        crate::EpochStore::open_read_only(&self.roots[0].0)
    }

    pub fn retained_boots(&self) -> Result<std::collections::BTreeSet<u32>> {
        let mut boots = std::collections::BTreeSet::new();
        for info in self.catalog()? {
            let (Some(first), Some(last)) = (info.first_index, info.last_index) else {
                continue;
            };
            let end = last
                .checked_add(1)
                .ok_or_else(|| Error::Backend("block index exhausted".into()))?;
            let mut next = first;
            while next < end {
                let page = self.read_blocks(&info.key, next, end, 1, MAX_PAGE_BYTES, false)?;
                for block in page.blocks {
                    boots.insert(block.boot.0);
                }
                match page.next_index {
                    Some(index) => next = index,
                    None => break,
                }
            }
        }
        Ok(boots)
    }

    /// Verify existing bindings without creating or modifying any root metadata.
    pub(crate) fn validate_existing_bindings(&self, expected: Option<u64>) -> Result<()> {
        for (_, engine) in &self.roots {
            match (expected, engine.get(0, b"dduroc.store-id")) {
                (Some(id), Some(value)) if value == id.to_le_bytes() => {}
                (_, None) if engine.raft_groups().is_empty() => {}
                (Some(_), Some(_)) => {
                    return Err(Error::Backend("foreign root store identity".into()));
                }
                _ => {
                    return Err(Error::Backend(
                        "root store identity missing or unexpected".into(),
                    ));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn bind_store(&self, store_id: u64) -> Result<()> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let expected = store_id.to_le_bytes();
        for (_, engine) in &self.roots {
            match engine.get(0, b"dduroc.store-id") {
                Some(value) if value == expected => {}
                Some(_) => return Err(Error::Backend("foreign root store identity".into())),
                None if !engine.raft_groups().is_empty() => {
                    return Err(Error::Backend("root store identity missing".into()));
                }
                None => {}
            }
        }
        for (_, engine) in &self.roots {
            if engine.get(0, b"dduroc.store-id").is_none() {
                let mut batch = LogBatch::default();
                batch
                    .put(0, b"dduroc.store-id".to_vec(), expected.to_vec())
                    .map_err(backend_error)?;
                self.commit(engine, &mut batch, true)?;
            }
        }
        Ok(())
    }

    fn cleanup_shadows_locked(&self) -> Result<()> {
        for (root, (_, engine)) in self.roots.iter().enumerate() {
            // A failed transform can leave only the durable reservation: the
            // staging group does not exist until its first entry is copied.
            // Enumerate reservations, not raft groups, or that ID could later
            // be reused by a real channel while its shadow marker survives.
            let mut reservations = Vec::new();
            engine
                .scan_raw_messages(0, Some(SHADOW_PREFIX), None, false, |key, value| {
                    if !key.starts_with(SHADOW_PREFIX) {
                        return false;
                    }
                    reservations.push((key.to_vec(), value.to_vec()));
                    true
                })
                .map_err(backend_error)?;
            for (pending, source) in reservations {
                let id = std::str::from_utf8(&pending[SHADOW_PREFIX.len()..])
                    .ok()
                    .and_then(|suffix| suffix.parse::<u64>().ok())
                    .filter(|&id| id != 0 && shadow_key(id) == pending)
                    .ok_or_else(|| Error::Backend("invalid staging reservation".into()))?;
                if source.len() != 8 || source == 0u64.to_le_bytes() || source == id.to_le_bytes() {
                    return Err(Error::Backend("invalid staging reservation source".into()));
                }
                if engine.get(id, META_KEY).is_some() {
                    return Err(Error::Backend(
                        "staging group has published metadata".into(),
                    ));
                }
                let mut batch = LogBatch::default();
                batch.add_command(id, raft_engine::Command::Clean);
                batch.delete(0, pending);
                self.commit_maintenance(engine, &mut batch, true)?;
                self.workers[root].purge().inspect_err(|_| {
                    self.failed.store(true, Ordering::Release);
                })?;
            }
        }
        Ok(())
    }

    pub(crate) fn cleanup_shadows(&self) -> Result<()> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        self.cleanup_shadows_locked()
    }

    pub(crate) fn from_roots(roots: Vec<(PathBuf, Arc<Engine>)>) -> Result<Self> {
        let mut index = HashMap::new();
        let mut retained_blocks = 0usize;
        let mut class_index = ClassIndex::default();
        let failed = Arc::new(AtomicBool::new(false));
        let workers: Vec<RootWorker> = roots
            .iter()
            .map(|(_, engine)| RootWorker::start(Arc::clone(engine), Arc::clone(&failed)))
            .collect::<Result<_>>()?;
        let metadata_accounting = roots
            .iter()
            .map(|(root, _)| MetadataAccounting::collect(root))
            .collect::<Result<Vec<_>>>()?;
        for (root_no, (_, engine)) in roots.iter().enumerate() {
            for id in engine.raft_groups() {
                if id == 0 {
                    continue;
                }
                let Some(bytes) = engine.get(id, META_KEY) else {
                    if engine.get(0, &shadow_key(id)).is_some() {
                        continue;
                    }
                    return Err(Error::Backend("channel metadata missing".into()));
                };
                let meta: ChannelMeta = postcard::from_bytes(&bytes)?;
                if let (Some(first), Some(last)) = (engine.first_index(id), engine.last_index(id)) {
                    let count = last
                        .checked_sub(first)
                        .and_then(|n| n.checked_add(1))
                        .and_then(|n| usize::try_from(n).ok())
                        .ok_or_else(|| Error::Backend("invalid retained block range".into()))?;
                    retained_blocks = retained_blocks
                        .checked_add(count)
                        .ok_or_else(|| Error::Backend("retained block count overflow".into()))?;
                    if retained_blocks > 1_000_000 {
                        return Err(Error::Backend("retained block limit reached".into()));
                    }
                }
                let class = StorageClass::ALL
                    .get(usize::from(meta.class))
                    .copied()
                    .ok_or_else(|| Error::Backend("invalid channel class".into()))?;
                class_index.totals[class.index()] = class_index.totals[class.index()]
                    .checked_add(meta.retained_bytes)
                    .ok_or_else(|| Error::Backend("class byte count overflow".into()))?;
                if let Some(first) = engine.first_index(id) {
                    class_index.heads[class.index()].insert((
                        head_sequence(&workers[root_no], id, first)?,
                        root_no,
                        id,
                    ));
                }
                let key = ChannelKey {
                    namespace: meta.namespace,
                    class,
                };
                class_index
                    .counts
                    .insert_initial(root_no, id, &key, load_counts(engine, id)?)?;
                if index.insert(key, (root_no, id)).is_some() {
                    return Err(Error::Backend("duplicate channel identity".into()));
                }
                if index.len() > 65536 {
                    return Err(Error::Backend("channel catalog exceeds limit".into()));
                }
            }
        }
        let physical_limits = vec![MAX_ROOT_ALLOCATED; roots.len()];
        let rewrite_reserves = vec![8 * 1024 * 1024; roots.len()];
        let targets = vec![4 * 1024 * 1024; roots.len()];
        Ok(Self {
            roots,
            index: RwLock::new(index),
            mutation: Mutex::new(()),
            failed,
            #[cfg(test)]
            commit_fault: std::sync::atomic::AtomicU8::new(0),
            physical_limits,
            rewrite_reserves,
            targets,
            retained_blocks: AtomicUsize::new(retained_blocks),
            class_limits: RwLock::new([u64::MAX; 3]),
            class_index: Mutex::new(class_index),
            workers,
            snapshot_owner: Arc::new(()),
            metadata_accounting: Mutex::new(metadata_accounting),
        })
    }

    fn root_allocation(&self, root: usize) -> Result<u64> {
        let metadata = self.metadata_accounting.lock().map_err(backend_error)?[root].total;
        metadata
            .checked_add(allocated_bytes(&self.roots[root].0.join("raft-engine"))?)
            .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))
    }

    pub(crate) fn metadata_update<T>(
        &self,
        root: usize,
        paths: &[PathBuf],
        operation: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.metadata_update_apply(root, paths, operation, false, |value| value)
    }

    // `publish` runs only after durability and accounting succeed, while the
    // mutation lock still serializes it with competing metadata writers.
    pub(crate) fn metadata_update_apply<T, R>(
        &self,
        root: usize,
        paths: &[PathBuf],
        operation: impl FnOnce() -> Result<T>,
        fence_operation_error: bool,
        publish: impl FnOnce(T) -> R,
    ) -> Result<R> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable(
                "backend fenced after a write failure".into(),
            ));
        }
        let mut reserve = 512 * 1024u64;
        for path in paths {
            if let Ok(metadata) = std::fs::symlink_metadata(path)
                && metadata.is_file()
            {
                reserve = reserve.saturating_add(metadata.len().saturating_mul(2));
            }
        }
        if self.root_allocation(root)?.saturating_add(reserve) > self.physical_limits[root] {
            return Err(Error::Backend(
                "insufficient physical headroom for metadata".into(),
            ));
        }
        let mut watched: BTreeSet<PathBuf> = paths.iter().cloned().collect();
        for dir in paths.iter().filter(|path| path.is_dir()) {
            for entry in std::fs::read_dir(dir).map_err(|error| {
                self.failed.store(true, Ordering::Release);
                Error::StorageUnavailable(error.to_string())
            })? {
                let path = entry
                    .map_err(|error| {
                        self.failed.store(true, Ordering::Release);
                        Error::StorageUnavailable(error.to_string())
                    })?
                    .path();
                if path.extension().is_some_and(|ext| ext == "tmp") {
                    watched.insert(path);
                }
            }
        }
        {
            let accounting = self.metadata_accounting.lock().map_err(backend_error)?;
            let new_paths = watched
                .iter()
                .filter(|path| !accounting[root].files.contains_key(*path))
                .count();
            if accounting[root].files.len().saturating_add(new_paths) > 262_144 {
                return Err(Error::Backend("metadata catalog exceeds limit".into()));
            }
        }
        let result = operation();
        for dir in paths.iter().filter(|path| path.is_dir()) {
            for entry in std::fs::read_dir(dir).map_err(|error| {
                self.failed.store(true, Ordering::Release);
                Error::StorageUnavailable(error.to_string())
            })? {
                let path = entry
                    .map_err(|error| {
                        self.failed.store(true, Ordering::Release);
                        Error::StorageUnavailable(error.to_string())
                    })?
                    .path();
                if path.extension().is_some_and(|ext| ext == "tmp") {
                    watched.insert(path);
                }
            }
        }
        let mut accounting = self.metadata_accounting.lock().map_err(backend_error)?;
        for path in &watched {
            if let Err(error) = accounting[root].update(path) {
                self.failed.store(true, Ordering::Release);
                return Err(Error::StorageUnavailable(error.to_string()));
            }
        }
        drop(accounting);
        let allocation = self.root_allocation(root).map_err(|error| {
            self.failed.store(true, Ordering::Release);
            Error::StorageUnavailable(error.to_string())
        })?;
        if allocation > self.physical_limits[root] {
            self.failed.store(true, Ordering::Release);
            return Err(Error::StorageUnavailable(
                "metadata exceeded physical root allocation limit".into(),
            ));
        }
        match result {
            Ok(value) => Ok(publish(value)),
            Err(error) if fence_operation_error => {
                self.failed.store(true, Ordering::Release);
                Err(Error::StorageUnavailable(error.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(test)]
    pub(crate) fn cached_allocation(&self, root: usize) -> Result<u64> {
        self.root_allocation(root)
    }

    pub(crate) fn set_class_limits(&self, limits: [u64; 3]) -> Result<()> {
        *self.class_limits.write().map_err(backend_error)? = limits;
        Ok(())
    }

    fn commit(&self, engine: &Engine, batch: &mut LogBatch, sync: bool) -> Result<()> {
        self.commit_inner(engine, batch, sync, false)
    }

    fn commit_maintenance(&self, engine: &Engine, batch: &mut LogBatch, sync: bool) -> Result<()> {
        self.commit_inner(engine, batch, sync, true)
    }

    fn commit_inner(
        &self,
        engine: &Engine,
        batch: &mut LogBatch,
        sync: bool,
        maintenance: bool,
    ) -> Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable(
                "backend fenced after a write failure".into(),
            ));
        }
        if batch.approximate_size() > MAX_BLOCK_BYTES + 1024 * 1024 {
            return Err(Error::Backend("raft batch exceeds size limit".into()));
        }
        let root = self
            .roots
            .iter()
            .position(|(_, candidate)| std::ptr::eq(candidate.as_ref(), engine))
            .ok_or_else(|| Error::Backend("unknown engine root".into()))?;
        validate_file_spans(&self.roots[root].0.join("raft-engine"), self.targets[root])?;
        let reserve = (batch.approximate_size() as u64).saturating_add(if maintenance {
            64 * 1024
        } else {
            self.rewrite_reserves[root].saturating_add(512 * 1024)
        });
        if self
            .root_allocation(root)
            .map_err(|e| Error::StorageUnavailable(e.to_string()))?
            .saturating_add(reserve)
            > self.physical_limits[root]
        {
            return Err(Error::Backend(
                "physical root allocation limit reached".into(),
            ));
        }
        #[cfg(test)]
        let fault = if sync && !maintenance {
            self.commit_fault.swap(0, Ordering::AcqRel)
        } else {
            0
        };
        #[cfg(test)]
        if fault == 1 || fault == 2 {
            self.failed.store(true, Ordering::Release);
            return Err(Error::StorageUnavailable(
                if fault == 1 {
                    "injected pre-write EIO"
                } else {
                    "injected pre-write ENOSPC"
                }
                .into(),
            ));
        }
        if let Err(error) = engine.write(batch, sync) {
            self.failed.store(true, Ordering::Release);
            return Err(Error::StorageUnavailable(error.to_string()));
        }
        #[cfg(test)]
        if fault == 3 || fault == 4 {
            self.failed.store(true, Ordering::Release);
            return Err(Error::StorageUnavailable(
                if fault == 3 {
                    "injected post-commit EIO"
                } else {
                    "injected post-commit ENOSPC"
                }
                .into(),
            ));
        }
        Ok(())
    }

    fn reserve_rewrite(&self, root: usize) -> Result<()> {
        if self
            .root_allocation(root)?
            .saturating_add(self.rewrite_reserves[root])
            > self.physical_limits[root]
        {
            return Err(Error::Backend(
                "insufficient physical headroom for rewrite".into(),
            ));
        }
        Ok(())
    }

    pub fn usage(&self) -> Result<Vec<RootUsage>> {
        let index = self.index.read().map_err(backend_error)?;
        let mut logical = vec![0u64; self.roots.len()];
        for &(root, id) in index.values() {
            let bytes = self.roots[root]
                .1
                .get(id, META_KEY)
                .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
            let meta: ChannelMeta = postcard::from_bytes(&bytes)?;
            logical[root] = logical[root]
                .checked_add(meta.retained_bytes)
                .ok_or_else(|| Error::Backend("retained byte count overflow".into()))?;
        }
        self.roots
            .iter()
            .enumerate()
            .map(|(i, (root, _))| {
                Ok(RootUsage {
                    root: root.clone(),
                    allocated_bytes: allocated_bytes(root)?,
                    logical_retained_bytes: logical[i],
                    physical_limit_bytes: self.physical_limits[i],
                })
            })
            .collect()
    }

    pub(crate) fn register_channel(
        &self,
        root: usize,
        key: ChannelKey,
        schema_name: &str,
        protocol_version: u16,
    ) -> Result<()> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let engine = &self
            .roots
            .get(root)
            .ok_or_else(|| Error::Backend("unknown root".into()))?
            .1;
        let mut index = self.index.write().map_err(backend_error)?;
        if let Some(&(existing_root, id)) = index.get(&key) {
            let bytes = self.roots[existing_root]
                .1
                .get(id, META_KEY)
                .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
            let mut meta: ChannelMeta = postcard::from_bytes(&bytes)?;
            if existing_root != root
                || meta.schema_name != schema_name
                || meta.protocol_version > protocol_version
            {
                return Err(Error::Backend("channel identity or schema mismatch".into()));
            }
            if meta.protocol_version < protocol_version {
                meta.protocol_version = protocol_version;
                let mut batch = LogBatch::default();
                batch
                    .put(id, META_KEY.to_vec(), postcard::to_allocvec(&meta)?)
                    .map_err(backend_error)?;
                self.commit(engine, &mut batch, true)?;
            }
            return Ok(());
        }
        if index.len() >= 65536 {
            return Err(Error::Backend("channel catalog exceeds limit".into()));
        }
        self.cleanup_shadows_locked()?;
        let id = engine
            .raft_groups()
            .into_iter()
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Backend("channel id exhausted".into()))?;
        let meta = ChannelMeta {
            namespace: key.namespace.clone(),
            class: key.class.index() as u8,
            schema_name: schema_name.to_owned(),
            protocol_version,
            retained_bytes: 0,
            next_index: 1,
            quota_bytes: None,
        };
        let mut batch = LogBatch::default();
        batch
            .put(id, META_KEY.to_vec(), postcard::to_allocvec(&meta)?)
            .map_err(backend_error)?;
        put_counts(&mut batch, id, &CountIndex::empty(meta.next_index, true))?;
        self.commit(engine, &mut batch, true)?;
        index.insert(key, (root, id));
        Ok(())
    }

    pub(crate) fn append_block(
        &self,
        key: &ChannelKey,
        boot: BootCounter,
        version: ProtocolVersion,
        bytes: &[u8],
        quota_bytes: Option<u64>,
        sync: bool,
    ) -> Result<u64> {
        if bytes.len() > MAX_BLOCK_BYTES
            || quota_bytes.is_some_and(|limit| bytes.len() as u64 > limit)
        {
            return Err(Error::Backend(
                "block exceeds channel or batch limit".into(),
            ));
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        // A page must retain consistent bounds and payloads while prefix
        // compaction removes entries, just as during a schema group switch.
        let index_guard = self.index.write().map_err(backend_error)?;
        let (root, id) = *index_guard
            .get(key)
            .ok_or_else(|| Error::Backend("unknown channel".into()))?;
        let engine = &self.roots[root].1;
        // Account all files under the physical root, including rewrite and
        // temporary files. Leave headroom for this batch and engine metadata.
        let data = engine
            .get(id, META_KEY)
            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
        let mut meta: ChannelMeta = postcard::from_bytes(&data)?;
        meta.quota_bytes = quota_bytes;
        let index = meta.next_index;
        meta.next_index = index
            .checked_add(1)
            .ok_or_else(|| Error::Backend("block index exhausted".into()))?;
        meta.retained_bytes = meta
            .retained_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| Error::Backend("retained byte count overflow".into()))?;
        let mut compact_to = engine.first_index(id).unwrap_or(index);
        let first_before = compact_to;
        if let Some(limit) = quota_bytes {
            while meta.retained_bytes > limit && compact_to < index {
                let old = self.workers[root]
                    .read(id, compact_to)?
                    .ok_or_else(|| Error::Backend("missing retained block".into()))?;
                let old_size = old
                    .value
                    .len()
                    .checked_sub(ENVELOPE_BYTES)
                    .ok_or_else(|| Error::Backend("truncated block envelope".into()))?;
                meta.retained_bytes = meta
                    .retained_bytes
                    .checked_sub(old_size as u64)
                    .ok_or_else(|| Error::Backend("invalid retained byte count".into()))?;
                compact_to += 1;
            }
        }
        let class_limit = self.class_limits.read().map_err(backend_error)?[key.class.index()];
        if bytes.len() as u64 > class_limit {
            return Err(Error::Backend("block exceeds class budget".into()));
        }
        let mut class_index = self.class_index.lock().map_err(backend_error)?;
        let class = key.class.index();
        // `meta` already excludes the current namespace's quota evictions.
        // Subtract those from the cached total before choosing class victims.
        let old_meta: ChannelMeta = postcard::from_bytes(&data)?;
        let mut class_total = class_index.totals[class]
            .checked_sub(old_meta.retained_bytes)
            .and_then(|total| total.checked_add(meta.retained_bytes))
            .ok_or_else(|| Error::Backend("class byte count overflow".into()))?;
        let mut others: Vec<(u64, ChannelMeta, u64, u64, u64)> = Vec::new();
        let mut changed = std::collections::HashSet::new();
        let mut replacement_heads = BTreeSet::new();
        while class_total > class_limit {
            let mut victim: Option<(u64, Option<usize>, usize)> = None;
            if compact_to < index {
                let old = self.workers[root]
                    .read(id, compact_to)?
                    .ok_or_else(|| Error::Backend("missing retained block".into()))?;
                if old.value.len() < ENVELOPE_BYTES {
                    return Err(Error::Backend("truncated block envelope".into()));
                }
                victim = Some((
                    u64::from_le_bytes(old.value[14..22].try_into().unwrap()),
                    None,
                    old.value.len() - ENVELOPE_BYTES,
                ));
            }
            let cached = class_index.heads[class]
                .iter()
                .find(|(_, r, g)| !(*r == root && *g == id) && !changed.contains(&(*r, *g)))
                .copied();
            let replacement = replacement_heads.iter().next().copied();
            let candidate = match (cached, replacement) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            if let Some((seq, other_root, other_id)) = candidate {
                if other_root != root {
                    return Err(Error::Backend("class spans multiple roots".into()));
                }
                let pos =
                    if let Some(pos) = others.iter().position(|(g, _, _, _, _)| *g == other_id) {
                        pos
                    } else {
                        let raw = engine
                            .get(other_id, META_KEY)
                            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
                        let other: ChannelMeta = postcard::from_bytes(&raw)?;
                        let first = engine
                            .first_index(other_id)
                            .ok_or_else(|| Error::Backend("missing channel head".into()))?;
                        let last = engine
                            .last_index(other_id)
                            .ok_or_else(|| Error::Backend("missing channel tail".into()))?;
                        others.push((other_id, other, first, first, last));
                        others.len() - 1
                    };
                let old = self.workers[root]
                    .read(other_id, others[pos].2)?
                    .ok_or_else(|| Error::Backend("missing retained block".into()))?;
                if old.value.len() < ENVELOPE_BYTES {
                    return Err(Error::Backend("truncated block envelope".into()));
                }
                if victim.as_ref().is_none_or(|(first, _, _)| seq < *first) {
                    victim = Some((seq, Some(pos), old.value.len() - ENVELOPE_BYTES));
                }
            }
            let (seq, other, size) = victim
                .ok_or_else(|| Error::Backend("class budget cannot retain new block".into()))?;
            class_total = class_total
                .checked_sub(size as u64)
                .ok_or_else(|| Error::Backend("invalid class byte count".into()))?;
            if let Some(position) = other {
                let (other_id, other_meta, before, _, last) = &mut others[position];
                replacement_heads.remove(&(seq, root, *other_id));
                changed.insert((root, *other_id));
                other_meta.retained_bytes = other_meta
                    .retained_bytes
                    .checked_sub(size as u64)
                    .ok_or_else(|| Error::Backend("invalid retained byte count".into()))?;
                *before += 1;
                if *before <= *last {
                    replacement_heads.insert((
                        head_sequence(&self.workers[root], *other_id, *before)?,
                        root,
                        *other_id,
                    ));
                }
            } else {
                meta.retained_bytes = meta
                    .retained_bytes
                    .checked_sub(size as u64)
                    .ok_or_else(|| Error::Backend("invalid retained byte count".into()))?;
                compact_to += 1;
            }
        }
        if index
            .checked_sub(compact_to)
            .ok_or_else(|| Error::Backend("invalid next block index".into()))?
            >= 65_536
        {
            return Err(Error::Backend(
                "channel retained block limit reached".into(),
            ));
        }
        let mut removed = usize::try_from(compact_to - first_before).map_err(backend_error)?;
        for (_, _, before, first, _) in &others {
            removed = removed
                .checked_add(usize::try_from(before - first).map_err(backend_error)?)
                .ok_or_else(|| Error::Backend("retained block count overflow".into()))?;
        }
        let total = self
            .retained_blocks
            .load(Ordering::Relaxed)
            .checked_sub(removed)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| Error::Backend("invalid retained block count".into()))?;
        if total > 1_000_000 {
            return Err(Error::Backend("retained block limit reached".into()));
        }
        let old_self_head = if first_before < index {
            Some(head_sequence(&self.workers[root], id, first_before)?)
        } else {
            None
        };
        let mut old_other_heads = Vec::new();
        for (other_id, _, before, first, _) in &others {
            if before > first {
                old_other_heads.push((
                    head_sequence(&self.workers[root], *other_id, *first)?,
                    root,
                    *other_id,
                ));
            }
        }
        let sequence = match engine.get(0, b"dduroc.sequence") {
            Some(raw) => u64::from_le_bytes(
                raw.as_slice()
                    .try_into()
                    .map_err(|_| Error::Backend("invalid sequence".into()))?,
            ),
            None => 0,
        }
        .checked_add(1)
        .ok_or_else(|| Error::Backend("sequence exhausted".into()))?;
        let new_head = if compact_to == index {
            sequence
        } else {
            head_sequence(&self.workers[root], id, compact_to)?
        };
        let mut raw = Vec::with_capacity(bytes.len() + ENVELOPE_BYTES);
        raw.extend_from_slice(&index.to_le_bytes());
        raw.extend_from_slice(&boot.0.to_le_bytes());
        raw.extend_from_slice(&version.0.to_le_bytes());
        raw.extend_from_slice(&sequence.to_le_bytes());
        raw.extend_from_slice(bytes);
        let mut entry = BytesValue::new();
        entry.set_value(raw);
        let old_own_counts = load_counts(engine, id)?;
        let mut own_counts = old_own_counts;
        if let Some(counts) = own_counts.as_mut() {
            subtract_counted_prefix(&self.workers[root], id, counts, first_before, compact_to)?;
            if counts.complete {
                match block_record_count(bytes) {
                    Ok(Some(count)) => {
                        counts.add(count)?;
                        counts.next = meta.next_index;
                    }
                    // A malformed block must never result in a fabricated count.
                    _ => own_counts = None,
                }
            }
        }
        let mut other_counts = Vec::new();
        for (other_id, _, before, first, _) in &others {
            if before > first
                && let Some(mut counts) = load_counts(engine, *other_id)?
            {
                subtract_counted_prefix(
                    &self.workers[root],
                    *other_id,
                    &mut counts,
                    *first,
                    *before,
                )?;
                let old = load_counts(engine, *other_id)?;
                other_counts.push((*other_id, old, counts));
            }
        }
        let mut changes = vec![(old_own_counts, own_counts)];
        changes.extend(other_counts.iter().map(|(_, old, new)| (*old, Some(*new))));
        let total_counts = class_index.counts.numbers(&changes)?;
        let mut batch = LogBatch::default();
        if let Some(counts) = own_counts {
            put_counts(&mut batch, id, &counts)?;
        } else {
            batch.delete(id, COUNTS_KEY.to_vec());
        }
        for (other_id, _, counts) in &other_counts {
            put_counts(&mut batch, *other_id, counts)?;
        }
        batch
            .put(
                0,
                b"dduroc.sequence".to_vec(),
                sequence.to_le_bytes().to_vec(),
            )
            .map_err(backend_error)?;
        let compacted = others.iter().any(|(_, _, before, first, _)| before > first)
            || engine
                .first_index(id)
                .is_some_and(|first| compact_to > first);
        if compacted {
            self.reserve_rewrite(root)?;
            if compact_to > first_before {
                batch.add_command(id, raft_engine::Command::Compact { index: compact_to });
            }
            for (other_id, other_meta, before, first, _) in &others {
                if before > first {
                    batch.add_command(*other_id, raft_engine::Command::Compact { index: *before });
                    batch
                        .put(
                            *other_id,
                            META_KEY.to_vec(),
                            postcard::to_allocvec(other_meta)?,
                        )
                        .map_err(backend_error)?;
                }
            }
        }
        batch
            .add_entries::<BlockEntry>(id, &[entry])
            .map_err(backend_error)?;
        batch
            .put(id, META_KEY.to_vec(), postcard::to_allocvec(&meta)?)
            .map_err(backend_error)?;
        self.commit(engine, &mut batch, sync)?;
        class_index
            .counts
            .apply(root, id, id, own_counts, Some(key), total_counts);
        for (other_id, _, counts) in &other_counts {
            class_index.counts.apply(
                root,
                *other_id,
                *other_id,
                Some(*counts),
                None,
                total_counts,
            );
        }
        self.retained_blocks.store(total, Ordering::Relaxed);
        class_index.totals[class] = class_total;
        if let Some(old) = old_self_head {
            class_index.heads[class].remove(&(old, root, id));
        }
        for head in old_other_heads {
            class_index.heads[class].remove(&head);
        }
        class_index.heads[class].extend(replacement_heads);
        class_index.heads[class].insert((new_head, root, id));
        if compacted {
            self.workers[root].purge().inspect_err(|_| {
                self.failed.store(true, Ordering::Release);
            })?;
        }
        Ok(index)
    }

    pub(crate) fn set_quota(&self, key: &ChannelKey, quota_bytes: Option<u64>) -> Result<()> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let (root, id) = *self
            .index
            .read()
            .map_err(backend_error)?
            .get(key)
            .ok_or_else(|| Error::Backend("unknown channel".into()))?;
        let engine = &self.roots[root].1;
        let data = engine
            .get(id, META_KEY)
            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
        let mut meta: ChannelMeta = postcard::from_bytes(&data)?;
        if meta.quota_bytes != quota_bytes {
            meta.quota_bytes = quota_bytes;
            let mut batch = LogBatch::default();
            batch
                .put(id, META_KEY.to_vec(), postcard::to_allocvec(&meta)?)
                .map_err(backend_error)?;
            self.commit(engine, &mut batch, true)?;
        }
        Ok(())
    }

    pub(crate) fn barrier_all(&self) -> Result<()> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        for (_, engine) in &self.roots {
            let mut batch = LogBatch::default();
            // 0.4.2 Engine::sync() writes an empty batch, which is a no-op.
            batch
                .put(0, b"dduroc.barrier".to_vec(), vec![1])
                .map_err(backend_error)?;
            self.commit_maintenance(engine, &mut batch, true)?;
        }
        Ok(())
    }

    pub(crate) fn rewrite_channel<F>(&self, key: &ChannelKey, mut transform: F) -> Result<()>
    where
        F: FnMut(StoredBlock) -> Result<Option<StoredBlock>>,
    {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        self.cleanup_shadows_locked()?;
        let (root, old_id) = *self
            .index
            .read()
            .map_err(backend_error)?
            .get(key)
            .ok_or_else(|| Error::Backend("unknown channel".into()))?;
        let engine = &self.roots[root].1;
        let (Some(first), Some(last)) = (engine.first_index(old_id), engine.last_index(old_id))
        else {
            return Ok(());
        };
        let raw_meta = engine
            .get(old_id, META_KEY)
            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
        let mut meta: ChannelMeta = postcard::from_bytes(&raw_meta)?;
        let old_counts = load_counts(engine, old_id)?;
        let mut rewritten_counts = old_counts
            .filter(|counts| counts.complete)
            .map(|_| CountIndex::empty(meta.next_index, true));
        let new_id = engine
            .raft_groups()
            .into_iter()
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| Error::Backend("channel id exhausted".into()))?;
        let pending = shadow_key(new_id);
        let mut start = LogBatch::default();
        start
            .put(0, pending.clone(), old_id.to_le_bytes().to_vec())
            .map_err(backend_error)?;
        self.commit(engine, &mut start, true)?;
        for at in first..=last {
            let mut entry = self.workers[root]
                .read(old_id, at)?
                .ok_or_else(|| Error::Backend("missing retained block".into()))?;
            if entry.value.len() < ENVELOPE_BYTES
                || u64::from_le_bytes(entry.value[..8].try_into().unwrap()) != at
            {
                return Err(Error::Backend("invalid block envelope".into()));
            }
            if entry.value.len() > ENVELOPE_BYTES {
                let old_size = entry.value.len() - ENVELOPE_BYTES;
                let old = StoredBlock {
                    index: at,
                    boot: BootCounter(u32::from_le_bytes(entry.value[8..12].try_into().unwrap())),
                    protocol_version: ProtocolVersion(u16::from_le_bytes(
                        entry.value[12..14].try_into().unwrap(),
                    )),
                    bytes: entry.value[ENVELOPE_BYTES..].to_vec(),
                };
                let result = transform(old)?;
                if result.as_ref().is_some_and(|block| {
                    block.index != at
                        || block.bytes.len() > MAX_BLOCK_BYTES
                        || block.bytes.is_empty()
                }) {
                    return Err(Error::Backend("invalid rewritten block".into()));
                }
                let new_bytes = result
                    .as_ref()
                    .map_or(&[][..], |block| block.bytes.as_slice());
                meta.retained_bytes = meta
                    .retained_bytes
                    .checked_sub(old_size as u64)
                    .and_then(|n| n.checked_add(new_bytes.len() as u64))
                    .ok_or_else(|| Error::Backend("invalid retained byte count".into()))?;
                if let Some(block) = &result {
                    entry.value[8..12].copy_from_slice(&block.boot.0.to_le_bytes());
                    entry.value[12..14].copy_from_slice(&block.protocol_version.0.to_le_bytes());
                }
                entry.value.truncate(ENVELOPE_BYTES);
                entry.value.extend_from_slice(new_bytes);
            }
            if let Some(counts) = rewritten_counts.as_mut() {
                match block_record_count(&entry.value[ENVELOPE_BYTES..]) {
                    Ok(Some(count)) => counts.add(count)?,
                    Ok(None) => {}
                    Err(_) => rewritten_counts = None,
                }
            }
            let mut copy = LogBatch::default();
            copy.add_entries::<BlockEntry>(new_id, &[entry])
                .map_err(backend_error)?;
            self.commit(engine, &mut copy, false)?;
        }
        if meta
            .quota_bytes
            .is_some_and(|quota| meta.retained_bytes > quota)
        {
            return Err(Error::Backend("rewritten channel exceeds its quota".into()));
        }
        let class_limit = self.class_limits.read().map_err(backend_error)?[key.class.index()];
        let mut class_total = meta.retained_bytes;
        for (other_key, &(other_root, other_id)) in self.index.read().map_err(backend_error)?.iter()
        {
            if other_key.class != key.class || other_root == root && other_id == old_id {
                continue;
            }
            let data = self.roots[other_root]
                .1
                .get(other_id, META_KEY)
                .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
            let other: ChannelMeta = postcard::from_bytes(&data)?;
            class_total = class_total
                .checked_add(other.retained_bytes)
                .ok_or_else(|| Error::Backend("class byte count overflow".into()))?;
        }
        if class_total > class_limit {
            return Err(Error::Backend(
                "rewritten channel exceeds class budget".into(),
            ));
        }
        let old_head = head_sequence(&self.workers[root], old_id, first)?;
        let mut index_guard = self.index.write().map_err(backend_error)?;
        let mut class_index = self.class_index.lock().map_err(backend_error)?;
        let total_counts = class_index
            .counts
            .numbers(&[(old_counts, rewritten_counts)])?;
        self.reserve_rewrite(root)?;
        let mut switch = LogBatch::default();
        switch
            .put(new_id, META_KEY.to_vec(), postcard::to_allocvec(&meta)?)
            .map_err(backend_error)?;
        if let Some(counts) = rewritten_counts {
            put_counts(&mut switch, new_id, &counts)?;
        }
        switch.add_command(old_id, raft_engine::Command::Clean);
        switch.delete(0, pending);
        self.commit(engine, &mut switch, true)?;
        class_index.counts.apply(
            root,
            old_id,
            new_id,
            rewritten_counts,
            Some(key),
            total_counts,
        );
        index_guard.insert(key.clone(), (root, new_id));
        let class = key.class.index();
        class_index.heads[class].remove(&(old_head, root, old_id));
        class_index.heads[class].insert((old_head, root, new_id));
        class_index.totals[class] = class_total;
        self.workers[root].purge().inspect_err(|_| {
            self.failed.store(true, Ordering::Release);
        })?;
        Ok(())
    }

    pub fn catalog(&self) -> Result<Vec<ChannelInfo>> {
        let index = self.index.read().map_err(backend_error)?;
        let mut channels = Vec::with_capacity(index.len());
        for (key, &(root_no, id)) in index.iter() {
            let engine = &self.roots[root_no].1;
            let bytes = engine
                .get(id, META_KEY)
                .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
            let meta: ChannelMeta = postcard::from_bytes(&bytes)?;
            let counts = load_counts(engine, id)?;
            channels.push(ChannelInfo {
                key: key.clone(),
                schema_name: meta.schema_name,
                protocol_version: meta.protocol_version,
                retained_bytes: meta.retained_bytes,
                quota_bytes: meta.quota_bytes,
                counting_state: count_state(counts),
                retained_records: counts.filter(|c| c.complete).map(|c| c.records),
                retained_blocks: counts.filter(|c| c.complete).map(|c| c.blocks),
                first_index: engine.first_index(id),
                last_index: engine.last_index(id),
                next_write_index: meta.next_index,
            });
        }
        channels
            .sort_by(|a, b| (&a.key.namespace, a.key.class).cmp(&(&b.key.namespace, b.key.class)));
        Ok(channels)
    }

    /// Global aggregates without visiting channels or reading retained blocks.
    pub fn cached_statistics(&self) -> Result<CachedStatistics> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable(
                "statistics unavailable after a storage failure".into(),
            ));
        }
        let channels = self.index.read().map_err(backend_error)?.len() as u64;
        let index = self.class_index.lock().map_err(backend_error)?;
        let state = index.counts.state();
        Ok(CachedStatistics {
            counting_state: state,
            retained_records: (state == CountingState::Complete).then_some(index.counts.records),
            retained_blocks: (state == CountingState::Complete).then_some(index.counts.blocks),
            logical_class_bytes: index.totals,
            class_limits: *self.class_limits.read().map_err(backend_error)?,
            channels,
        })
    }

    /// Select the first incomplete channel from the bounded in-memory index.
    pub fn pending_count_channel(&self) -> Result<Option<ChannelKey>> {
        let _guard = self.mutation.lock().map_err(backend_error)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable(
                "count index unavailable after a storage failure".into(),
            ));
        }
        Ok(self
            .class_index
            .lock()
            .map_err(backend_error)?
            .counts
            .pending
            .first_key_value()
            .map(|(_, (_, key))| key.clone()))
    }

    /// Current configured logical class budgets. Offline callers must not
    /// treat the backend's unconfigured defaults as persisted policy.
    pub fn class_limits(&self) -> Result<[u64; 3]> {
        Ok(*self.class_limits.read().map_err(backend_error)?)
    }

    /// Sample allocated storage under a bounded directory walk. The result is
    /// a separate instant from a catalog snapshot and never exposes paths.
    pub fn physical_stats(
        &self,
        max_entries: usize,
        mut keep_going: impl FnMut() -> bool,
    ) -> Result<Vec<RootPhysicalStats>> {
        if max_entries == 0 {
            return Err(Error::Backend("invalid physical statistics limit".into()));
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        if self.failed.load(Ordering::Acquire) {
            return Err(Error::StorageUnavailable(
                "statistics unavailable after a storage failure".into(),
            ));
        }
        let accounting = self.metadata_accounting.lock().map_err(backend_error)?;
        let mut result = Vec::with_capacity(self.roots.len());
        let mut visited = 0usize;
        for (index, (root, _)) in self.roots.iter().enumerate() {
            let mut pending = vec![root.join("raft-engine")];
            let (mut raft, mut log, mut rewrite) = (0u64, 0u64, 0u64);
            while let Some(path) = pending.pop() {
                if !keep_going() {
                    return Err(Error::Backend("physical statistics cancelled".into()));
                }
                visited += 1;
                if visited > max_entries {
                    return Err(Error::Backend(
                        "physical statistics directory limit exceeded".into(),
                    ));
                }
                let meta = std::fs::symlink_metadata(&path).map_err(backend_error)?;
                let allocated = meta
                    .blocks()
                    .checked_mul(512)
                    .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?;
                raft = raft
                    .checked_add(allocated)
                    .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?;
                match path.extension().and_then(|s| s.to_str()) {
                    Some("raftlog") => {
                        log = log
                            .checked_add(allocated)
                            .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?
                    }
                    Some("rewrite") => {
                        rewrite = rewrite
                            .checked_add(allocated)
                            .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?
                    }
                    _ => {}
                }
                if meta.is_dir() {
                    for entry in std::fs::read_dir(&path).map_err(backend_error)? {
                        if !keep_going() {
                            return Err(Error::Backend("physical statistics cancelled".into()));
                        }
                        if pending.len().saturating_add(visited) >= max_entries {
                            return Err(Error::Backend(
                                "physical statistics directory limit exceeded".into(),
                            ));
                        }
                        pending.push(entry.map_err(backend_error)?.path());
                    }
                }
            }
            let metadata = accounting[index].total;
            result.push(RootPhysicalStats {
                index,
                allocated_bytes: metadata
                    .checked_add(raft)
                    .ok_or_else(|| Error::Backend("allocated byte count overflow".into()))?,
                log_bytes: log,
                rewrite_bytes: rewrite,
                metadata_bytes: metadata,
                physical_limit_bytes: self.physical_limits[index],
            });
        }
        Ok(result)
    }

    /// Count one channel in a bounded durable step. This is explicit
    /// maintenance; ordinary statistics snapshots never scan history.
    /// The callback must be fast and must not re-enter this backend.
    pub fn backfill_count_step(
        &self,
        key: &ChannelKey,
        max_blocks: usize,
        max_bytes: usize,
        mut keep_going: impl FnMut() -> bool,
    ) -> Result<CountingState> {
        if max_blocks == 0 || max_blocks > 1024 || max_bytes == 0 || max_bytes > 64 * 1024 * 1024 {
            return Err(Error::Backend("invalid backfill work limit".into()));
        }
        let _guard = self.mutation.lock().map_err(backend_error)?;
        let marker = crate::fsutil::read_required_bounded(&self.roots[0].0.join("dduroc-raft-v1"))?;
        let store_meta = crate::store::StoreMeta::read(&self.roots[0].0)?
            .ok_or_else(|| Error::Backend("store metadata missing".into()))?;
        if marker != b"dduroc raft store v2 stats-writer\n"
            || store_meta.container_version != crate::store::STORE_METADATA_VERSION
        {
            return Err(Error::Backend(
                "count backfill requires a writer-upgraded store".into(),
            ));
        }
        let index = self.index.read().map_err(backend_error)?;
        let (root, id) = *index
            .get(key)
            .ok_or_else(|| Error::Backend("unknown channel".into()))?;
        if crate::fsutil::read_required_bounded(&self.roots[root].0.join("dduroc-raft-v1"))?
            != b"dduroc raft store v2 stats-writer\n"
        {
            return Err(Error::Backend(
                "count backfill requires an upgraded root".into(),
            ));
        }
        let engine = &self.roots[root].1;
        let raw = engine
            .get(id, META_KEY)
            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
        let meta: ChannelMeta = postcard::from_bytes(&raw)?;
        let first = engine.first_index(id).unwrap_or(meta.next_index);
        let old_counts = load_counts(engine, id)?;
        let mut counts = old_counts.unwrap_or_else(|| CountIndex::empty(first, false));
        if counts.complete {
            return Ok(CountingState::Complete);
        }
        counts.next = counts.next.max(first);
        if counts.next > meta.next_index {
            return Err(Error::Backend("invalid count index".into()));
        }
        let mut blocks = 0usize;
        let mut bytes = 0usize;
        while counts.next < meta.next_index && blocks < max_blocks {
            if !keep_going() {
                return Err(Error::Backend("count backfill cancelled".into()));
            }
            let entry = self.workers[root]
                .read(id, counts.next)?
                .ok_or_else(|| Error::Backend("missing retained block".into()))?;
            if entry.value.len() < ENVELOPE_BYTES
                || u64::from_le_bytes(entry.value[..8].try_into().unwrap()) != counts.next
            {
                return Err(Error::Backend("invalid block envelope".into()));
            }
            let size = entry.value.len();
            if size > max_bytes.saturating_sub(bytes) {
                if blocks == 0 {
                    return Err(Error::Backend("block exceeds backfill byte limit".into()));
                }
                break;
            }
            if let Some(count) = block_record_count(&entry.value[ENVELOPE_BYTES..])? {
                counts.add(count)?;
            }
            counts.next = counts
                .next
                .checked_add(1)
                .ok_or_else(|| Error::Backend("count index overflow".into()))?;
            bytes += size;
            blocks += 1;
        }
        if !keep_going() {
            return Err(Error::Backend("count backfill cancelled".into()));
        }
        counts.complete = counts.next == meta.next_index;
        let mut class_index = self.class_index.lock().map_err(backend_error)?;
        let totals = class_index.counts.numbers(&[(old_counts, Some(counts))])?;
        let mut batch = LogBatch::default();
        put_counts(&mut batch, id, &counts)?;
        self.commit(engine, &mut batch, true)?;
        class_index
            .counts
            .apply(root, id, id, Some(counts), Some(key), totals);
        Ok(count_state(Some(counts)))
    }

    pub fn read_blocks(
        &self,
        key: &ChannelKey,
        start: u64,
        end: u64,
        max_entries: usize,
        max_bytes: usize,
        reverse: bool,
    ) -> Result<BlockPage> {
        if start > end
            || max_entries == 0
            || max_entries > MAX_PAGE_ENTRIES
            || max_bytes == 0
            || max_bytes > MAX_PAGE_BYTES
        {
            return Err(Error::Backend("invalid block page limits".into()));
        }
        let index_guard = self.index.read().map_err(backend_error)?;
        let (root_no, id) = *index_guard
            .get(key)
            .ok_or_else(|| Error::Backend("unknown channel".into()))?;
        let engine = &self.roots[root_no].1;
        let first = engine.first_index(id);
        let last = engine.last_index(id);
        let meta = engine
            .get(id, META_KEY)
            .ok_or_else(|| Error::Backend("channel metadata missing".into()))?;
        let next_write_index = postcard::from_bytes::<ChannelMeta>(&meta)?.next_index;
        if first.is_none() && last.is_none() {
            return Ok(BlockPage {
                blocks: Vec::new(),
                next_index: None,
                first_index: None,
                last_index: None,
                next_write_index,
            });
        }
        if first.is_none() || last.is_none() {
            return Err(Error::Backend("inconsistent channel bounds".into()));
        }
        let low = start.max(first.unwrap_or(start));
        let high = end.min(last.and_then(|n| n.checked_add(1)).unwrap_or(end));
        let mut blocks = Vec::new();
        let mut total = 0usize;
        let mut next = if reverse { high } else { low };
        let mut scanned = 0usize;
        while low < high && scanned < max_entries {
            let index = if reverse {
                if next <= low {
                    break;
                }
                next - 1
            } else {
                if next >= high {
                    break;
                }
                next
            };
            let Some(entry) = self.workers[root_no].read(id, index)? else {
                return Err(Error::Backend(format!("missing block {index}")));
            };
            let raw = entry.value;
            if raw.len() < ENVELOPE_BYTES {
                return Err(Error::Backend("truncated block envelope".into()));
            }
            if u64::from_le_bytes(raw[..8].try_into().unwrap()) != index {
                return Err(Error::Backend("block index mismatch".into()));
            }
            let body = &raw[ENVELOPE_BYTES..];
            scanned += 1;
            if body.is_empty() {
                next = if reverse { index } else { index + 1 };
                continue;
            }
            if total.checked_add(body.len()).is_none_or(|n| n > max_bytes) {
                if blocks.is_empty() {
                    return Err(Error::Backend("block exceeds page byte limit".into()));
                }
                break;
            }
            total += body.len();
            blocks.push(StoredBlock {
                index,
                boot: BootCounter(u32::from_le_bytes(raw[8..12].try_into().unwrap())),
                protocol_version: ProtocolVersion(u16::from_le_bytes(
                    raw[12..14].try_into().unwrap(),
                )),
                bytes: body.to_vec(),
            });
            next = if reverse { index } else { index + 1 };
        }
        Ok(BlockPage {
            blocks,
            next_index: if if reverse { next > low } else { next < high } {
                Some(next)
            } else {
                None
            },
            first_index: first,
            last_index: last,
            next_write_index,
        })
    }
}

pub(crate) fn validate_file_spans(path: &Path, target: u64) -> Result<()> {
    let mut spans: [Option<(u64, u64)>; 2] = [None, None];
    if path.is_dir() {
        for entry in std::fs::read_dir(path).map_err(backend_error)? {
            let name = entry.map_err(backend_error)?.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let queue = if name.ends_with(".raftlog") {
                0
            } else if name.ends_with(".rewrite") {
                1
            } else {
                continue;
            };
            let Some(prefix) = name.as_bytes().get(..16) else {
                continue;
            };
            if !prefix.iter().all(u8::is_ascii_digit) {
                continue;
            }
            let seq: u64 = std::str::from_utf8(prefix)
                .map_err(backend_error)?
                .parse()
                .map_err(backend_error)?;
            spans[queue] = Some(match spans[queue] {
                Some((min, max)) => (min.min(seq), max.max(seq)),
                None => (seq, seq),
            });
        }
    }
    let mut calculated = 0u64;
    for span in spans {
        let count = match span {
            Some((min, max)) => max.checked_sub(min).and_then(|n| n.checked_add(3)),
            None => Some(3),
        }
        .ok_or_else(|| Error::Backend("raft file span overflows".into()))?;
        calculated = calculated
            .checked_add(
                count
                    .checked_mul(target)
                    .ok_or_else(|| Error::Backend("raft span size overflows".into()))?,
            )
            .ok_or_else(|| Error::Backend("raft span size overflows".into()))?;
    }
    if calculated > u64::from(u32::MAX) {
        return Err(Error::Backend(
            "raft file spans exceed 32-bit size accounting".into(),
        ));
    }
    Ok(())
}

pub(crate) fn open_root(path: &Path, budget: u64, target: u64) -> Result<Engine> {
    validate_file_spans(path, target)?;
    let path = path
        .to_str()
        .ok_or_else(|| Error::Backend("non-UTF8 raft path".into()))?;
    let config = Config {
        dir: path.into(),
        recovery_mode: RecoveryMode::AbsoluteConsistency,
        batch_compression_threshold: raft_engine::ReadableSize(0),
        target_file_size: raft_engine::ReadableSize(target),
        purge_threshold: raft_engine::ReadableSize(budget.max(target)),
        purge_rewrite_threshold: Some(raft_engine::ReadableSize(target * 2)),
        enable_log_recycle: false,
        prefill_for_recycle: false,
        ..Config::default()
    };
    Engine::open(config).map_err(backend_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (tempfile::TempDir, Arc<ReadBackend>, ChannelKey) {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dduroc-raft-v1"), b"dduroc raft store v1\n").unwrap();
        let meta = crate::store::StoreMeta {
            container_version: dduroc_format::CONTAINER_VERSION,
            store_id: 42,
        };
        std::fs::write(
            dir.path().join("store-meta"),
            postcard::to_allocvec(&meta).unwrap(),
        )
        .unwrap();
        let backend =
            ReadBackend::open_live(&[dir.path().to_path_buf()], &[1024 * 1024], &[128 * 1024])
                .unwrap();
        backend.bind_store(42).unwrap();
        let key = ChannelKey {
            namespace: "radio".into(),
            class: StorageClass::Critical,
        };
        backend
            .register_channel(0, key.clone(), "schema", 1)
            .unwrap();
        (dir, backend, key)
    }

    fn valid_block(records: usize) -> Vec<u8> {
        let mut builder = dduroc_format::block::BlockBuilder::new();
        for at in 0..records {
            builder
                .push(
                    dduroc_format::Micros(at as u64),
                    &dduroc_format::Record::Ext { bytes: b"x" },
                )
                .unwrap();
        }
        let mut bytes = Vec::new();
        builder
            .finish(0, dduroc_format::block::Compression::None, &mut bytes)
            .unwrap();
        bytes
    }

    #[test]
    fn retained_counts_follow_eviction_rewrite_and_bounded_backfill() {
        let (dir, backend, key) = setup();
        std::fs::write(
            dir.path().join("dduroc-raft-v1"),
            b"dduroc raft store v2 stats-writer\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("store-meta"),
            postcard::to_allocvec(&crate::store::StoreMeta {
                container_version: crate::store::STORE_METADATA_VERSION,
                store_id: 42,
            })
            .unwrap(),
        )
        .unwrap();
        let bytes = valid_block(2);
        let quota = (bytes.len() * 2) as u64;
        for _ in 0..3 {
            backend
                .append_block(
                    &key,
                    BootCounter(0),
                    ProtocolVersion(1),
                    &bytes,
                    Some(quota),
                    true,
                )
                .unwrap();
        }
        let info = &backend.catalog().unwrap()[0];
        assert_eq!(
            (info.retained_records, info.retained_blocks),
            (Some(4), Some(2))
        );
        let summary = backend.cached_statistics().unwrap();
        assert_eq!(
            (summary.retained_records, summary.retained_blocks),
            (Some(4), Some(2))
        );
        let first = info.first_index.unwrap();
        backend
            .rewrite_channel(&key, |block| {
                if block.index == first {
                    Ok(None)
                } else {
                    Ok(Some(block))
                }
            })
            .unwrap();
        let info = &backend.catalog().unwrap()[0];
        assert_eq!(
            (info.retained_records, info.retained_blocks),
            (Some(2), Some(1))
        );
        let summary = backend.cached_statistics().unwrap();
        assert_eq!(
            (summary.retained_records, summary.retained_blocks),
            (Some(2), Some(1))
        );
        let (_, id) = backend.index.read().unwrap()[&key];
        let mut batch = LogBatch::default();
        batch.delete(id, COUNTS_KEY.to_vec());
        backend
            .commit(&backend.roots[0].1, &mut batch, true)
            .unwrap();
        drop(backend);
        let backend =
            ReadBackend::open_live(&[dir.path().to_path_buf()], &[1024 * 1024], &[128 * 1024])
                .unwrap();
        assert_eq!(
            backend.cached_statistics().unwrap().counting_state,
            CountingState::BackfillRequired
        );
        assert_eq!(
            backend.catalog().unwrap()[0].counting_state,
            CountingState::BackfillRequired
        );
        assert_eq!(
            backend
                .backfill_count_step(&key, 1, 1024 * 1024, || true)
                .unwrap(),
            CountingState::InProgress
        );
        assert_eq!(backend.catalog().unwrap()[0].retained_records, None);
        assert_eq!(
            backend
                .backfill_count_step(&key, 1, 1024 * 1024, || true)
                .unwrap(),
            CountingState::Complete
        );
        let info = &backend.catalog().unwrap()[0];
        assert_eq!(
            (info.retained_records, info.retained_blocks),
            (Some(2), Some(1))
        );
        let summary = backend.cached_statistics().unwrap();
        assert_eq!(
            (summary.retained_records, summary.retained_blocks),
            (Some(2), Some(1))
        );
        assert!(backend.pending_count_channel().unwrap().is_none());
        drop(backend);
        let reopened = ReadBackend::open_dump(&[dir.path().to_path_buf()]).unwrap();
        assert_eq!(reopened.catalog().unwrap()[0].retained_records, Some(2));
    }

    #[test]
    fn interrupted_backfill_tracks_concurrent_retention_and_append() {
        let (dir, backend, key) = setup();
        std::fs::write(
            dir.path().join("dduroc-raft-v1"),
            b"dduroc raft store v2 stats-writer\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("store-meta"),
            postcard::to_allocvec(&crate::store::StoreMeta {
                container_version: crate::store::STORE_METADATA_VERSION,
                store_id: 42,
            })
            .unwrap(),
        )
        .unwrap();
        let bytes = valid_block(2);
        let quota = (bytes.len() * 2) as u64;
        for _ in 0..2 {
            backend
                .append_block(
                    &key,
                    BootCounter(0),
                    ProtocolVersion(1),
                    &bytes,
                    Some(quota),
                    true,
                )
                .unwrap();
        }
        let (_, id) = backend.index.read().unwrap()[&key];
        let mut batch = LogBatch::default();
        batch.delete(id, COUNTS_KEY.to_vec());
        backend
            .commit(&backend.roots[0].1, &mut batch, true)
            .unwrap();
        drop(backend);
        let backend =
            ReadBackend::open_live(&[dir.path().to_path_buf()], &[1024 * 1024], &[128 * 1024])
                .unwrap();
        let mut checkpoints = 0;
        assert!(
            backend
                .backfill_count_step(&key, 2, 1024 * 1024, || {
                    checkpoints += 1;
                    checkpoints < 2
                })
                .is_err()
        );
        assert_eq!(
            backend.cached_statistics().unwrap().counting_state,
            CountingState::BackfillRequired
        );
        assert_eq!(
            backend
                .backfill_count_step(&key, 1, 1024 * 1024, || true)
                .unwrap(),
            CountingState::InProgress
        );
        backend
            .append_block(
                &key,
                BootCounter(0),
                ProtocolVersion(1),
                &bytes,
                Some(quota),
                true,
            )
            .unwrap();
        assert_eq!(
            backend
                .backfill_count_step(&key, 2, 1024 * 1024, || true)
                .unwrap(),
            CountingState::Complete
        );
        let stats = backend.cached_statistics().unwrap();
        assert_eq!(
            (stats.retained_records, stats.retained_blocks),
            (Some(4), Some(2))
        );
    }

    #[test]
    fn corrupt_block_never_becomes_a_fabricated_count() {
        let (dir, backend, key) = setup();
        std::fs::write(
            dir.path().join("dduroc-raft-v1"),
            b"dduroc raft store v2 stats-writer\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("store-meta"),
            postcard::to_allocvec(&crate::store::StoreMeta {
                container_version: crate::store::STORE_METADATA_VERSION,
                store_id: 42,
            })
            .unwrap(),
        )
        .unwrap();
        let mut bytes = valid_block(1);
        *bytes.last_mut().unwrap() ^= 1;
        backend
            .append_block(&key, BootCounter(0), ProtocolVersion(1), &bytes, None, true)
            .unwrap();
        assert_eq!(backend.cached_statistics().unwrap().retained_records, None);
        assert!(
            backend
                .backfill_count_step(&key, 2, 1024 * 1024, || true)
                .is_err()
        );
        let stats = backend.cached_statistics().unwrap();
        assert_eq!(stats.counting_state, CountingState::BackfillRequired);
        assert_eq!(stats.retained_records, None);
    }

    #[test]
    fn class_eviction_updates_both_channels_cached_counts() {
        let (_dir, backend, first) = setup();
        let second = ChannelKey {
            namespace: "other".into(),
            class: StorageClass::Critical,
        };
        backend
            .register_channel(0, second.clone(), "schema", 1)
            .unwrap();
        let bytes = valid_block(2);
        backend
            .set_class_limits([u64::MAX, (2 * bytes.len()) as u64, u64::MAX])
            .unwrap();
        backend
            .append_block(
                &first,
                BootCounter(0),
                ProtocolVersion(1),
                &bytes,
                None,
                true,
            )
            .unwrap();
        backend
            .append_block(
                &second,
                BootCounter(0),
                ProtocolVersion(1),
                &bytes,
                None,
                true,
            )
            .unwrap();
        backend
            .append_block(
                &second,
                BootCounter(0),
                ProtocolVersion(1),
                &bytes,
                None,
                true,
            )
            .unwrap();
        let rows = backend.catalog().unwrap();
        assert_eq!(
            rows.iter()
                .find(|row| row.key == first)
                .unwrap()
                .retained_records,
            Some(0)
        );
        assert_eq!(
            rows.iter()
                .find(|row| row.key == second)
                .unwrap()
                .retained_records,
            Some(4)
        );
        let stats = backend.cached_statistics().unwrap();
        assert_eq!(
            (stats.retained_records, stats.retained_blocks),
            (Some(4), Some(2))
        );
    }

    #[test]
    fn cached_global_counts_cover_more_than_256_channels() {
        let (_dir, backend, _) = setup();
        for n in 0..300 {
            backend
                .register_channel(
                    0,
                    ChannelKey {
                        namespace: format!("node-{n:03}"),
                        class: StorageClass::Default,
                    },
                    "schema",
                    1,
                )
                .unwrap();
        }
        let stats = backend.cached_statistics().unwrap();
        assert_eq!(stats.channels, 301);
        assert_eq!(stats.retained_records, Some(0));
        assert_eq!(stats.retained_blocks, Some(0));
        assert_eq!(stats.counting_state, CountingState::Complete);
    }

    #[test]
    fn live_pages_remain_consistent_during_retention() {
        let (_dir, backend, key) = setup();
        for _ in 0..8 {
            backend
                .append_block(
                    &key,
                    BootCounter(1),
                    ProtocolVersion(1),
                    &[7; 1024],
                    Some(8192),
                    false,
                )
                .unwrap();
        }
        let finished = AtomicBool::new(false);
        let start = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                start.wait();
                for _ in 0..512 {
                    backend
                        .append_block(
                            &key,
                            BootCounter(1),
                            ProtocolVersion(1),
                            &[7; 1024],
                            Some(8192),
                            false,
                        )
                        .unwrap();
                }
                finished.store(true, Ordering::Release);
            });
            start.wait();
            let mut pages = 0;
            while !finished.load(Ordering::Acquire) || pages == 0 {
                let page = backend
                    .read_blocks(&key, 0, u64::MAX, 8, 8192, false)
                    .unwrap();
                assert!(!page.blocks.is_empty());
                for block in page.blocks {
                    assert_eq!(block.bytes, [7; 1024]);
                }
                pages += 1;
            }
        });
    }

    #[test]
    fn quota_compacts_old_blocks_atomically_and_reopens() {
        let (dir, backend, key) = setup();
        for i in 0..3 {
            backend
                .append_block(
                    &key,
                    BootCounter(9),
                    ProtocolVersion(1),
                    &[i; 8],
                    Some(16),
                    true,
                )
                .unwrap();
        }
        let page = backend.read_blocks(&key, 1, 4, 8, 1024, false).unwrap();
        assert_eq!(
            page.blocks.iter().map(|b| b.index).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(backend.catalog().unwrap()[0].retained_bytes, 16);
        assert_eq!(
            backend
                .retained_boots()
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>(),
            vec![9]
        );
        backend.barrier_all().unwrap();
        drop(backend);
        let reopened = ReadBackend::open_dump(&[dir.path().to_path_buf()]).unwrap();
        assert_eq!(
            reopened
                .read_blocks(&key, 1, 4, 8, 1024, false)
                .unwrap()
                .blocks
                .len(),
            2
        );
    }

    #[test]
    fn impossible_block_does_not_replace_a_durable_record() {
        let (_dir, backend, key) = setup();
        backend
            .append_block(
                &key,
                BootCounter(1),
                ProtocolVersion(1),
                b"old",
                Some(4),
                true,
            )
            .unwrap();
        assert!(
            backend
                .append_block(
                    &key,
                    BootCounter(1),
                    ProtocolVersion(1),
                    b"larger",
                    Some(4),
                    true
                )
                .is_err()
        );
        let page = backend.read_blocks(&key, 1, 10, 8, 1024, false).unwrap();
        assert_eq!(page.blocks.len(), 1);
        assert_eq!(page.blocks[0].bytes, b"old");
    }

    #[test]
    fn class_budget_evicts_closed_namespaces_and_survives_reopen() {
        let (dir, backend, _) = setup();
        backend
            .set_class_limits([u64::MAX, 256 * 1024, u64::MAX])
            .unwrap();
        let mut payload = vec![0u8; 40 * 1024];
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte = (i.wrapping_mul(37) % 251) as u8;
        }
        for i in 0..120 {
            let key = ChannelKey {
                namespace: format!("radio-{i}"),
                class: StorageClass::Critical,
            };
            backend
                .register_channel(0, key.clone(), "schema", 1)
                .unwrap();
            backend.set_quota(&key, Some(80 * 1024)).unwrap();
            backend
                .append_block(
                    &key,
                    BootCounter(1),
                    ProtocolVersion(1),
                    &payload,
                    Some(80 * 1024),
                    true,
                )
                .unwrap();
        }
        let usage = backend.usage().unwrap();
        assert!(usage[0].logical_retained_bytes <= 256 * 1024);
        assert!(usage[0].allocated_bytes < 9 * 1024 * 1024);
        assert!(
            backend
                .catalog()
                .unwrap()
                .iter()
                .filter(|info| info.first_index.is_some())
                .count()
                <= 6
        );
        drop(backend);
        let reopened = ReadBackend::open_dump(&[dir.path().to_path_buf()]).unwrap();
        assert!(reopened.usage().unwrap()[0].logical_retained_bytes <= 256 * 1024);
        assert_eq!(reopened.catalog().unwrap().len(), 121);
    }

    #[test]
    fn rewrite_switches_atomically_and_ignores_interrupted_shadow() {
        let (dir, backend, key) = setup();
        for value in 0..3 {
            backend
                .append_block(
                    &key,
                    BootCounter(1),
                    ProtocolVersion(1),
                    &[value],
                    None,
                    true,
                )
                .unwrap();
        }
        let failed = backend.rewrite_channel(&key, |block| {
            if block.index == 2 {
                return Err(Error::Backend("injected transform failure".into()));
            }
            Ok(Some(block))
        });
        assert!(failed.is_err());
        assert_eq!(
            backend
                .read_blocks(&key, 1, 4, 8, 1024, false)
                .unwrap()
                .blocks
                .len(),
            3
        );
        backend
            .rewrite_channel(&key, |mut block| {
                if block.index == 2 {
                    return Ok(None);
                }
                block.bytes.push(42);
                Ok(Some(block))
            })
            .unwrap();
        let page = backend.read_blocks(&key, 1, 4, 8, 1024, false).unwrap();
        assert_eq!(
            page.blocks
                .iter()
                .map(|block| block.index)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        drop(backend);
        let reopened = ReadBackend::open_dump(&[dir.path().to_path_buf()]).unwrap();
        assert_eq!(
            reopened
                .read_blocks(&key, 1, 4, 8, 1024, false)
                .unwrap()
                .blocks
                .len(),
            2
        );
    }

    #[test]
    fn rewrite_cannot_exceed_persisted_quota_after_reopen() {
        let (dir, backend, key) = setup();
        backend.set_quota(&key, Some(8)).unwrap();
        backend
            .append_block(
                &key,
                BootCounter(1),
                ProtocolVersion(1),
                b"old",
                Some(8),
                true,
            )
            .unwrap();
        drop(backend);
        let backend =
            ReadBackend::open_live(&[dir.path().to_path_buf()], &[1024 * 1024], &[128 * 1024])
                .unwrap();
        backend.bind_store(42).unwrap();
        assert!(
            backend
                .rewrite_channel(&key, |mut block| {
                    block.bytes = vec![7; 9];
                    Ok(Some(block))
                })
                .is_err()
        );
        assert_eq!(
            backend
                .read_blocks(&key, 1, 2, 1, 100, false)
                .unwrap()
                .blocks[0]
                .bytes,
            b"old"
        );
    }

    #[test]
    fn failed_staging_near_limit_can_be_cleaned_on_reopen() {
        let (dir, backend, key) = setup();
        let mut payload = vec![0u8; 40 * 1024];
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte = (i.wrapping_mul(37) % 251) as u8;
        }
        for _ in 0..115 {
            backend
                .append_block(
                    &key,
                    BootCounter(1),
                    ProtocolVersion(1),
                    &payload,
                    None,
                    false,
                )
                .unwrap();
        }
        assert!(
            backend
                .rewrite_channel(&key, |block| Ok(Some(block)))
                .is_err()
        );
        drop(backend);
        let reopened =
            ReadBackend::open_live(&[dir.path().to_path_buf()], &[1024 * 1024], &[128 * 1024])
                .unwrap();
        reopened.bind_store(42).unwrap();
        reopened.cleanup_shadows().unwrap();
        reopened
            .append_block(&key, BootCounter(2), ProtocolVersion(1), b"new", None, true)
            .unwrap();
        assert_eq!(
            reopened
                .read_blocks(&key, 116, 117, 1, 1024, false)
                .unwrap()
                .blocks
                .len(),
            1
        );
    }

    #[test]
    fn equal_group_ids_on_separate_roots_do_not_alias() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let roots = [first.path().to_path_buf(), second.path().to_path_buf()];
        for root in &roots {
            std::fs::write(root.join("dduroc-raft-v1"), b"dduroc raft store v1\n").unwrap();
        }
        let backend = ReadBackend::open_live(&roots, &[1024 * 1024; 2], &[128 * 1024; 2]).unwrap();
        backend.bind_store(42).unwrap();
        let a = ChannelKey {
            namespace: "radio".into(),
            class: StorageClass::Default,
        };
        let b = ChannelKey {
            namespace: "radio".into(),
            class: StorageClass::Critical,
        };
        backend.register_channel(0, a.clone(), "schema", 1).unwrap();
        backend.register_channel(1, b.clone(), "schema", 1).unwrap();
        backend
            .append_block(&a, BootCounter(1), ProtocolVersion(1), b"first", None, true)
            .unwrap();
        backend
            .append_block(&b, BootCounter(1), ProtocolVersion(1), b"other", None, true)
            .unwrap();
        for _ in 0..4 {
            assert_eq!(
                backend.read_blocks(&a, 1, 2, 1, 100, false).unwrap().blocks[0].bytes,
                b"first"
            );
            assert_eq!(
                backend.read_blocks(&b, 1, 2, 1, 100, false).unwrap().blocks[0].bytes,
                b"other"
            );
        }
        for _ in 0..8 {
            backend
                .append_block(
                    &a,
                    BootCounter(1),
                    ProtocolVersion(1),
                    b"first",
                    Some(10),
                    true,
                )
                .unwrap();
            backend
                .append_block(
                    &b,
                    BootCounter(1),
                    ProtocolVersion(1),
                    b"other",
                    Some(10),
                    true,
                )
                .unwrap();
        }
        for (key, expected) in [(&a, &b"first"[..]), (&b, &b"other"[..])] {
            let page = backend.read_blocks(key, 1, 10, 10, 100, false).unwrap();
            assert_eq!(page.blocks.len(), 2);
            assert!(page.blocks.iter().all(|block| block.bytes == expected));
        }
        drop(backend);
        let backend = ReadBackend::open_live(&roots, &[1024 * 1024; 2], &[128 * 1024; 2]).unwrap();
        backend.bind_store(42).unwrap();
        for (key, expected) in [(&a, &b"first"[..]), (&b, &b"other"[..])] {
            let page = backend.read_blocks(key, 1, 10, 10, 100, false).unwrap();
            assert!(page.blocks.iter().all(|block| block.bytes == expected));
        }
    }

    #[test]
    fn snapshot_from_another_backend_cannot_read_same_geometry() {
        let (_a_dir, a, a_key) = setup();
        let (_b_dir, b, b_key) = setup();
        a.append_block(
            &a_key,
            BootCounter(1),
            ProtocolVersion(1),
            b"first",
            None,
            true,
        )
        .unwrap();
        b.append_block(
            &b_key,
            BootCounter(1),
            ProtocolVersion(1),
            b"other",
            None,
            true,
        )
        .unwrap();
        let snapshot = a
            .snapshot_channels(|key| Ok(*key == a_key))
            .unwrap()
            .pop()
            .unwrap();
        assert!(!b.validate_snapshot(&snapshot, Some(1)).unwrap());
        assert!(b.read_snapshot_block(&snapshot, 1).unwrap().is_none());
        assert_eq!(
            b.follow_status(&snapshot, 1).unwrap(),
            SnapshotStatus::Replaced
        );
    }

    #[test]
    fn root_workers_isolate_reads_during_real_gc_rewrite() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let roots = [first.path().to_path_buf(), second.path().to_path_buf()];
        for root in &roots {
            std::fs::write(root.join("dduroc-raft-v1"), b"dduroc raft store v1\n").unwrap();
        }
        let backend = ReadBackend::open_live(&roots, &[1024 * 1024; 2], &[128 * 1024; 2]).unwrap();
        backend.bind_store(42).unwrap();
        let keys: Vec<_> = [StorageClass::Default, StorageClass::Critical]
            .into_iter()
            .map(|class| ChannelKey {
                namespace: "hot".into(),
                class,
            })
            .collect();
        let cold: Vec<_> = [StorageClass::Default, StorageClass::Critical]
            .into_iter()
            .map(|class| ChannelKey {
                namespace: "cold".into(),
                class,
            })
            .collect();
        let payloads = [vec![11u8; 40 * 1024], vec![37u8; 40 * 1024]];
        for i in 0..2 {
            backend
                .register_channel(i, cold[i].clone(), "schema", 1)
                .unwrap();
            backend
                .register_channel(i, keys[i].clone(), "schema", 1)
                .unwrap();
            backend
                .append_block(
                    &cold[i],
                    BootCounter(1),
                    ProtocolVersion(1),
                    &payloads[i],
                    None,
                    true,
                )
                .unwrap();
        }
        for _ in 0..48 {
            for i in 0..2 {
                backend
                    .append_block(
                        &keys[i],
                        BootCounter(1),
                        ProtocolVersion(1),
                        &payloads[i],
                        Some(80 * 1024),
                        true,
                    )
                    .unwrap();
            }
        }
        for i in 0..2 {
            assert!(
                !roots[i]
                    .join("raft-engine/0000000000000001.raftlog")
                    .exists(),
                "append files must actually be purged"
            );
            assert_eq!(
                backend
                    .read_blocks(&cold[i], 1, 2, 1, 64 * 1024, false)
                    .unwrap()
                    .blocks[0]
                    .bytes,
                payloads[i]
            );
        }
        drop(backend);
        let backend = ReadBackend::open_live(&roots, &[1024 * 1024; 2], &[128 * 1024; 2]).unwrap();
        backend.bind_store(42).unwrap();
        for i in 0..2 {
            assert_eq!(
                backend
                    .read_blocks(&cold[i], 1, 2, 1, 64 * 1024, false)
                    .unwrap()
                    .blocks[0]
                    .bytes,
                payloads[i]
            );
        }
    }

    #[test]
    fn file_span_guard_rejects_32_bit_overflow_before_open() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("0000000000000001.raftlog"), b"preserve").unwrap();
        std::fs::write(dir.path().join("0000000000001200.raftlog"), b"preserve").unwrap();
        assert!(validate_file_spans(dir.path(), 4 * 1024 * 1024).is_err());
        assert!(validate_file_spans(dir.path(), 64 * 1024).is_ok());
        assert_eq!(
            std::fs::read(dir.path().join("0000000000000001.raftlog")).unwrap(),
            b"preserve"
        );
    }

    #[test]
    fn worker_panic_fences_later_durable_writes() {
        let (_dir, backend, key) = setup();
        backend.workers[0]
            .sender
            .as_ref()
            .unwrap()
            .send(RootRequest::Panic)
            .unwrap();
        let _ = backend.workers[0].read(1, 1);
        let error = backend
            .append_block(&key, BootCounter(1), ProtocolVersion(1), b"new", None, true)
            .unwrap_err();
        assert!(matches!(error, Error::StorageUnavailable(_)));
    }

    #[test]
    fn failed_metadata_operation_tracks_leftover_temporary_file() {
        let (dir, backend, _) = setup();
        let path = dir.path().join("staged.tmp");
        let paths = [dir.path().to_path_buf(), path.clone()];
        let result: Result<()> = backend.metadata_update(0, &paths, || {
            std::fs::write(&path, vec![9u8; 8192]).map_err(backend_error)?;
            Err(Error::Backend("injected metadata failure".into()))
        });
        assert!(result.is_err());
        assert_eq!(
            backend.cached_allocation(0).unwrap(),
            backend.usage().unwrap()[0].allocated_bytes
        );
    }

    #[test]
    fn metadata_catalog_limit_refuses_before_mutation() {
        let (dir, backend, _) = setup();
        {
            let mut state = backend.metadata_accounting.lock().unwrap();
            while state[0].files.len() < 262_144 {
                let path = dir
                    .path()
                    .join(format!("reserved-{}", state[0].files.len()));
                state[0].files.insert(path, 0);
            }
        }
        let path = dir.path().join("not-created");
        let result = backend.metadata_update(0, std::slice::from_ref(&path), || {
            std::fs::write(&path, b"unexpected").map_err(backend_error)
        });
        assert!(result.is_err());
        assert!(!path.exists());
    }

    #[test]
    fn bounded_head_index_with_many_registered_channels() {
        let (_dir, backend, key) = setup();
        backend
            .set_class_limits([u64::MAX, 64 * 1024, u64::MAX])
            .unwrap();
        let payload = vec![7u8; 1024];
        for count in 0..1000 {
            let other = ChannelKey {
                namespace: format!("n-{count}"),
                class: StorageClass::Critical,
            };
            backend
                .register_channel(0, other.clone(), "schema", 1)
                .unwrap();
            backend
                .append_block(
                    &other,
                    BootCounter(1),
                    ProtocolVersion(1),
                    &payload,
                    None,
                    false,
                )
                .unwrap();
            if count == 99 || count == 999 {
                let start = std::time::Instant::now();
                for _ in 0..100 {
                    backend
                        .append_block(
                            &key,
                            BootCounter(1),
                            ProtocolVersion(1),
                            &payload,
                            Some(256 * 1024),
                            false,
                        )
                        .unwrap();
                }
                eprintln!("channels={} 100 appends={:?}", count + 1, start.elapsed());
            }
        }
        assert_eq!(backend.catalog().unwrap().len(), 1001);
    }
}
