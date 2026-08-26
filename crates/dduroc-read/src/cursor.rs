//! Read cursors: a segment's records, a channel's segments.
//!
//! Memory is bounded by one decompressed block per cursor — segments can be
//! hundreds of megabytes, and reading them whole on armv7 is not an option.
//!
//! # Damage is not swept under the carpet
//!
//! A broken block does **not** break off the reading of a segment: the next
//! block is found through the footer index, and the skip is reported to the
//! caller. Stopping silently would pass an incomplete answer off as a complete
//! one — the worst possible outcome for diagnostics.

use crate::error::{ReadError, Result};
use crate::query::{Bounds, Fit};
use dduroc_engine::migrate::{self, Chained};
use dduroc_engine::schema::{Migration, Schema, StorageClass};
use dduroc_engine::segment::{SegmentReader, parse_block};
use dduroc_format::segment::SegmentName;
use dduroc_format::{BootCounter, BootTime, Micros, Record};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A selection predicate applied **before** a record is materialized.
///
/// An owning copy of a record costs a payload allocation, so a query like
/// "errors only" must not pay it for each of hundreds of thousands of filtered
/// records. Series definitions are always let through: without them a sample's
/// identity cannot be restored.
pub type Prefilter = Arc<dyn Fn(&Record<'_>) -> bool + Send + Sync>;

/// One record that has been read.
///
/// The time in full: the microseconds came from the record, the run from the
/// segment header, and apart they are not comparable.
#[derive(Debug, Clone)]
pub struct RawEntry {
    pub at: BootTime,
    pub record: OwnedRecord,
}

/// An owning copy of a record: the cursor reuses the block buffer, so there is
/// nothing to lend outwards.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedRecord {
    Message {
        event: dduroc_format::EventId,
        span: Option<dduroc_format::SpanId>,
        payload: Vec<u8>,
    },
    SpanStart {
        span: dduroc_format::SpanId,
        kind: dduroc_format::SpanKindId,
        parent: Option<dduroc_format::SpanId>,
    },
    SpanEnd {
        span: dduroc_format::SpanId,
    },
    Sample {
        metric: dduroc_format::MetricId,
        value: OwnedSampleValue,
    },
    Text {
        level: dduroc_format::Level,
        span: Option<dduroc_format::SpanId>,
        target: String,
        text: String,
    },
    Ext {
        bytes: Vec<u8>,
    },
}

/// A sample value in owning form.
#[derive(Debug, Clone, PartialEq)]
pub enum OwnedSampleValue {
    F32(f32),
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Blob(Vec<u8>),
}

impl OwnedSampleValue {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            OwnedSampleValue::F32(v) => Some(f64::from(*v)),
            OwnedSampleValue::F64(v) => Some(*v),
            OwnedSampleValue::I64(v) => Some(*v as f64),
            OwnedSampleValue::U64(v) => Some(*v as f64),
            OwnedSampleValue::Bool(v) => Some(if *v { 1.0 } else { 0.0 }),
            OwnedSampleValue::Blob(_) => None,
        }
    }
}

fn own(record: &Record<'_>) -> OwnedRecord {
    match record {
        Record::Message(m) => OwnedRecord::Message {
            event: m.event,
            span: m.span,
            payload: m.payload.to_vec(),
        },
        Record::SpanStart(s) => OwnedRecord::SpanStart {
            span: s.span,
            kind: s.kind,
            parent: s.parent,
        },
        Record::SpanEnd { span } => OwnedRecord::SpanEnd { span: *span },
        Record::Sample(s) => OwnedRecord::Sample {
            metric: s.metric,
            value: match s.value {
                dduroc_format::Value::F32(v) => OwnedSampleValue::F32(v),
                dduroc_format::Value::F64(v) => OwnedSampleValue::F64(v),
                dduroc_format::Value::I64(v) => OwnedSampleValue::I64(v),
                dduroc_format::Value::U64(v) => OwnedSampleValue::U64(v),
                dduroc_format::Value::Bool(v) => OwnedSampleValue::Bool(v),
                dduroc_format::Value::Blob(b) => OwnedSampleValue::Blob(b.to_vec()),
            },
        },
        Record::Text(t) => OwnedRecord::Text {
            level: t.level,
            span: t.span,
            target: t.target.to_owned(),
            text: t.text.to_owned(),
        },
        Record::Ext { bytes } => OwnedRecord::Ext {
            bytes: bytes.to_vec(),
        },
    }
}

/// A schema version and its steps — everything a cursor needs to bring the
/// records of old segments up to the current layout.
///
/// Not a whole [`Schema`]: a cursor needs neither the decoders nor the names,
/// only the version and the chain, and both fields are `'static`, so the
/// context is copied.
#[derive(Debug, Clone, Copy)]
pub struct MigrationCtx {
    pub current_version: u16,
    pub steps: &'static [Migration],
}

impl MigrationCtx {
    pub fn of(schema: &Schema) -> Self {
        Self {
            current_version: schema.version.0,
            steps: schema.migrations,
        }
    }
}

/// What the cursor does with this segment's records.
enum MigrationState {
    /// Nothing: the segment is at the current version, or no step touches it.
    None,
    /// Push every record through the chain.
    Chain(Vec<&'static Migration>),
}

/// How the cursor regards the possibility that the segment is being appended
/// to right now.
///
/// The difference is not in what is read but in what an unfinished tail counts
/// as.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Liveness {
    /// A dump: there is nobody to append to it, so any unfinished part is
    /// damage.
    #[default]
    Frozen,
    /// A live store, a one-off query: a tail caught mid-word is not shown and
    /// does not count as damage — the next query will see it.
    Live,
    /// A live store, a subscription: there will be no next query, and skipping
    /// a tail would mean losing it forever. So an unfinished block is deferred
    /// until it arrives whole.
    Following,
}

impl Liveness {
    /// Whether anybody is writing to this store right now.
    pub fn is_live(self) -> bool {
        !matches!(self, Liveness::Frozen)
    }

    /// Whether the cursor will come back to a deferred tail.
    pub fn is_following(self) -> bool {
        matches!(self, Liveness::Following)
    }
}

/// A cursor over the records of one segment.
pub struct SegmentCursor {
    reader: SegmentReader,
    path: PathBuf,
    /// The block offsets in ascending order of time.
    offsets: Vec<u64>,
    /// The index of the next block.
    next_block: usize,
    /// The decompressed records of the current block.
    ///
    /// An `Option` per record so as to hand it out by **move** rather than by
    /// copy: the payload has already been copied here from the block buffer,
    /// and a second copy of the same content is an allocation per record handed
    /// out. The cursor never walks back, so the emptied slot will not be needed
    /// again.
    buffered: Vec<Option<RawEntry>>,
    /// The position in `buffered`.
    pos: usize,
    /// Reverse order.
    reverse: bool,
    /// Selection before materialization.
    prefilter: Option<Prefilter>,
    /// Bringing records up to the current schema version.
    migration: MigrationState,
    /// The blocks that could not be read.
    damaged: Vec<Damage>,
    /// How an unfinished tail is regarded.
    liveness: Liveness,
    /// The offset the tail scan will continue from and the block number
    /// expected there: a subscription reads a segment on as it grows rather
    /// than re-reading it whole for every batch.
    scan_end: u64,
    expected_seq: u32,
    /// The segment is sealed: there is nobody left to append to it and the
    /// block index is complete.
    sealed: bool,
    /// The byte offset of the newest block of an unsealed segment during live
    /// reading.
    ///
    /// The one place in the file where unreadability is normal: the writer may
    /// be appending to that block right now, and the reader sees a page before
    /// the write has landed whole. A failure at this offset is "the data is not
    /// ready yet", not damage; the blocks before it were written earlier and
    /// read as usual.
    live_tail_offset: Option<u64>,
}

/// Details of a fragment that was skipped.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Damage {
    pub path: PathBuf,
    pub offset: u64,
    pub reason: String,
}

impl SegmentCursor {
    pub fn open(
        path: &Path,
        reverse: bool,
        expect_store: Option<u64>,
        prefilter: Option<Prefilter>,
        migrations: Option<MigrationCtx>,
        liveness: Liveness,
    ) -> Result<Self> {
        let mut reader = SegmentReader::open(path).map_err(ReadError::Engine)?;
        if let Some(id) = expect_store
            && reader.header().store_id != id
        {
            return Err(ReadError::ForeignStore {
                path: path.to_owned(),
                expected: id,
                found: reader.header().store_id,
            });
        }
        let mut damaged = Vec::new();

        // A segment of an old version is read through the chain of migration
        // steps: without it the current decoders would parse the old layout
        // silently and wrongly (postcard is not self-describing — an i8 reads
        // as the tail of an f32, with no error). A segment the chain does not
        // touch (by its footer) goes through as it is; a segment NEWER than the
        // schema is not read at all — there is nothing to parse a layout from
        // the future with, and silence here would look like "the device wrote
        // nothing".
        let seg_version = reader.header().protocol_version.0;
        let mut migration = MigrationState::None;
        if let Some(ctx) = migrations {
            if seg_version > ctx.current_version {
                damaged.push(Damage {
                    path: path.to_owned(),
                    offset: 0,
                    reason: format!(
                        "protocol version {seg_version} is newer than the schema ({}): this \
                         build cannot parse records of that layout",
                        ctx.current_version
                    ),
                });
                return Ok(Self::empty(reader, path, reverse, damaged));
            }
            if seg_version < ctx.current_version {
                match migrate::chain_between(ctx.steps, seg_version, ctx.current_version) {
                    Ok(steps) => {
                        let untouched = reader
                            .footer()
                            .is_some_and(|f| !migrate::chain_touches(&steps, &f));
                        if !untouched {
                            migration = MigrationState::Chain(steps);
                        }
                    }
                    Err(e) => {
                        // A hole in the chain: the old layout cannot be read
                        // with the current decoders, and there is nothing to
                        // bring it up with.
                        damaged.push(Damage {
                            path: path.to_owned(),
                            offset: 0,
                            reason: format!("a version {seg_version} segment cannot be read: {e}"),
                        });
                        return Ok(Self::empty(reader, path, reverse, damaged));
                    }
                }
            }
        }
        // A sealed segment gives its block offsets from the footer; otherwise
        // it is a scan of the headers. A scan breaking off is the ordinary
        // consequence of a power loss: the blocks already found stay in the
        // selection, and the place of the break is reported explicitly.
        //
        // A reverse walk no longer needs a preliminary pass: a sample used to
        // refer to a local series number whose definition lay in the stream
        // BEFORE it, that is, already behind when reading from the end.
        let unsealed = reader.footer().is_none();
        let mut scan_end = u64::MAX;
        let mut expected_seq = 0;
        let mut offsets: Vec<u64> = match reader.footer() {
            Some(footer) => footer.blocks.iter().map(|b| b.offset).collect(),
            None => {
                // The scan catches a gap in block numbering along the way:
                // parsing the bodies is not needed for that, the number is in
                // the header.
                let scan = reader.scan_block_offsets_from(SegmentReader::first_block_offset(), 0);
                let (offsets, stopped) = (scan.offsets, scan.stopped);
                scan_end = scan.end;
                expected_seq = scan.next_seq;
                // A scan of an unsealed segment breaking off during live
                // reading is not damage but the present tense: the writer is
                // appending to the tail (a block, a footer when sealing) and
                // the reader caught it mid-word. The intact blocks before the
                // break are already in the selection; the tail will arrive by
                // the next query. In a dump the same break is corruption or a
                // power loss, and it is reported.
                if let Some((offset, reason)) = stopped
                    && !liveness.is_live()
                {
                    damaged.push(Damage {
                        path: path.to_owned(),
                        offset,
                        reason,
                    });
                }
                offsets
            }
        };
        let live_tail_offset = if liveness.is_live() && unsealed {
            offsets.last().copied()
        } else {
            None
        };

        if reverse {
            offsets.reverse();
        }
        // The descriptor is released at once: everything worth a trip to the
        // medium has already been parsed, and a cursor is created per channel —
        // an open file behind each is not an option (see
        // [`SegmentReader::detach`]).
        reader.detach();
        Ok(Self {
            reader,
            path: path.to_owned(),
            offsets,
            next_block: 0,
            buffered: Vec::new(),
            pos: 0,
            reverse,
            prefilter,
            migration,
            damaged,
            liveness,
            scan_end,
            expected_seq,
            sealed: !unsealed,
            live_tail_offset,
        })
    }

    /// Read on through the tail of a segment being written right now.
    ///
    /// `true` means new blocks appeared. The tail is re-read rather than
    /// deduced from memory: under the cursor the file both grows (new blocks)
    /// and shortens (the segment was released for idleness, or sealed). The
    /// header is unchanged by definition, so only the tail is re-read
    /// ([`SegmentReader::refresh`]) — a subscription calls this on every cursor
    /// at every wake-up, and a superfluous header parse would be multiplied by
    /// their number.
    pub fn extend(&mut self) -> bool {
        if self.sealed || self.reverse {
            return false;
        }
        match self.reader.refresh() {
            Ok(()) => {}
            Err(dduroc_engine::Error::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                // The file is gone. So is the data that "disappeared": nobody
                // was writing to an evicted segment any more. Unread blocks, if
                // there were any, are reported by `fill` as read failures.
                self.sealed = true;
                return false;
            }
            // Any other failure to open is not a verdict: it may be transient,
            // and declaring the segment finished because of it would mean going
            // deaf on this channel forever.
            Err(_) => return false,
        }
        // The index is taken in a separate step: it borrows the footer's bytes
        // from the reader, while the tail scan needs the reader mutably.
        let sealed_offsets: Option<Vec<u64>> = self
            .reader
            .footer()
            .map(|f| f.blocks.iter().map(|b| b.offset).collect());
        let grew = match sealed_offsets {
            Some(blocks) => {
                // The segment is sealed: the block index has become
                // authoritative and there is no unfinished part left in it —
                // which means there is nothing left to tolerate either.
                let before = self.offsets.len();
                for offset in blocks {
                    if offset >= self.scan_end {
                        self.offsets.push(offset);
                    }
                }
                self.sealed = true;
                self.live_tail_offset = None;
                self.offsets.len() != before
            }
            None => {
                let scan = self
                    .reader
                    .scan_block_offsets_from(self.scan_end, self.expected_seq);
                let grew = !scan.offsets.is_empty();
                self.offsets.extend_from_slice(&scan.offsets);
                self.scan_end = scan.end;
                self.expected_seq = scan.next_seq;
                // The tolerance moves to the new tail: the previous one, if
                // another block has queued behind it, is complete and has to
                // read as an ordinary one.
                self.live_tail_offset = self.offsets.last().copied();
                grew
            }
        };
        self.reader.detach();
        grew
    }

    /// Take the damage that has accumulated, leaving the list empty.
    ///
    /// A subscription lives a long time and reports damage once: a list that
    /// only grows would repeat one and the same damage in every batch and grow
    /// without bound.
    pub fn take_damage(&mut self) -> Vec<Damage> {
        std::mem::take(&mut self.damaged)
    }

    /// A cursor that will hand out no record — for segments that must not be
    /// parsed. Damage is still reported as usual.
    fn empty(mut reader: SegmentReader, path: &Path, reverse: bool, damaged: Vec<Damage>) -> Self {
        reader.detach();
        Self {
            reader,
            path: path.to_owned(),
            offsets: Vec::new(),
            next_block: 0,
            buffered: Vec::new(),
            pos: 0,
            reverse,
            prefilter: None,
            migration: MigrationState::None,
            damaged,
            liveness: Liveness::Frozen,
            scan_end: u64::MAX,
            expected_seq: 0,
            sealed: true,
            live_tail_offset: None,
        }
    }

    pub fn boot(&self) -> BootCounter {
        self.reader.header().boot
    }

    /// The fragments skipped so far.
    pub fn damaged(&self) -> &[Damage] {
        &self.damaged
    }

    /// Whether the segment holds at least one of the given metrics.
    ///
    /// `None` means the segment is not sealed, there is no metric set, and the
    /// question cannot be answered without reading blocks. This is what the set
    /// is in the footer for: the search for the last state before a window
    /// would otherwise read the whole history.
    pub fn contains_any_metric(
        &self,
        wanted: &std::collections::HashSet<dduroc_format::MetricId>,
    ) -> Option<bool> {
        // A segment the migration chain rewrites is not judged by its footer:
        // the sets describe PRE-migration identifiers while `wanted` are the
        // current ones. A remapped metric would lie in the segment under its
        // old number, and an answer of "no" would throw the segment away along
        // with it.
        if matches!(self.migration, MigrationState::Chain(_)) {
            return None;
        }
        let footer = self.reader.footer()?;
        Some(
            wanted
                .iter()
                .any(|m| footer.metrics.binary_search(m).is_ok()),
        )
    }

    /// Peek at the next record without consuming it.
    pub fn peek(&mut self) -> Option<&RawEntry> {
        if self.pos >= self.buffered.len() && !self.fill() {
            return None;
        }
        self.buffered.get(self.pos)?.as_ref()
    }

    /// Take the next record.
    pub fn next_entry(&mut self) -> Option<RawEntry> {
        if self.pos >= self.buffered.len() && !self.fill() {
            return None;
        }
        let item = self.buffered.get_mut(self.pos).and_then(Option::take);
        self.pos += 1;
        item
    }

    /// Discard the blocks that lie entirely outside the window.
    ///
    /// Block bounds are known from the footer, so the selection happens without
    /// reading any bodies — that is what the footer exists for.
    ///
    /// Both bounds and both directions. Previously the skip worked only on the
    /// lower bound and only in forward order, while a query's default order is
    /// [`crate::Order::Newest`]: the block index did not work in the commonest
    /// scenario at all, and "the last hundred records" read the whole segment.
    ///
    /// Called once, before the walk begins: the window is cut out of the list
    /// of offsets rather than remembered as separate state.
    pub fn clip_to_window(&mut self, from: Option<Micros>, to: Option<Micros>) {
        debug_assert_eq!(
            self.next_block, 0,
            "the window is cut out before the walk begins"
        );
        if from.is_none() && to.is_none() || self.offsets.is_empty() {
            return;
        }
        let Some(footer) = self.reader.footer() else {
            return;
        };
        let total = footer.blocks.len();
        debug_assert_eq!(
            total,
            self.offsets.len(),
            "the offsets came from the footer"
        );

        // The lower bound: the block that MAY contain `from` is the last one
        // whose base is no later than it; everything before it is entirely in
        // the past.
        let lo = from.map_or(0, |t| footer.block_for_time(t).unwrap_or(0));
        // The upper: a block's base is the time of its first record, so a block
        // that began later than `to` consists of records later still.
        let hi = to.map_or(total, |t| footer.blocks.partition_point(|b| b.base <= t));

        if lo >= hi {
            self.offsets.clear();
            return;
        }
        // On a reverse walk `offsets` is already reversed: the forward-order
        // window [lo, hi) is [total - hi, total - lo) in walk order.
        let (head, tail) = if self.reverse {
            (total - hi, total - lo)
        } else {
            (lo, hi)
        };
        self.offsets.truncate(tail);
        self.offsets.drain(..head);
    }

    /// Load the next block. `false` means there are no more blocks.
    ///
    /// The descriptor lives exactly as long as the reading and is released on
    /// the way out: a cursor is created per channel, and a permanent file for
    /// each would mean tens of thousands of open descriptors for one query (see
    /// [`SegmentReader::detach`]). The open is paid once per batch rather than
    /// per block: in one visit here as many blocks are read as it took to
    /// gather a non-empty selection.
    fn fill(&mut self) -> bool {
        let got = self.fill_blocks();
        self.reader.detach();
        got
    }

    fn fill_blocks(&mut self) -> bool {
        let mut buf = Vec::new();
        while self.next_block < self.offsets.len() {
            let offset = self.offsets[self.next_block];
            self.next_block += 1;

            // A live tail: the scan saw the block header, but the body may not
            // have arrived yet — the header and the body are laid down by one
            // write, and the pages become visible to a reader with no guarantee
            // of being whole. A one-off query passes such a block over
            // silently: the next one will see it. A subscription has no next
            // query, so it defers the block and comes back to it once it has
            // arrived whole.
            let tolerate_tear = self.live_tail_offset == Some(offset);
            let defer_tear = tolerate_tear && self.liveness.is_following();

            match self.reader.read_block_at(offset, &mut buf) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    if defer_tear {
                        self.next_block -= 1;
                        return false;
                    }
                    continue;
                }
                Err(e) => {
                    if defer_tear {
                        self.next_block -= 1;
                        return false;
                    }
                    // The file disappeared between batches: rotation evicted
                    // the segment while the cursor was handing out what it had
                    // read. The same ordinary event of a live store as a
                    // segment vanishing between the listing and the open — the
                    // engine removed the history itself, and there is no "lost"
                    // data. There is nothing more to read in this file.
                    if self.liveness.is_live() && is_gone(&e) {
                        self.next_block = self.offsets.len();
                        return false;
                    }
                    // A broken block does not break off the segment: the other
                    // blocks are addressed independently and there is no reason
                    // to lose them.
                    if !tolerate_tear {
                        self.damaged.push(Damage {
                            path: self.path.clone(),
                            offset,
                            reason: e.to_string(),
                        });
                    }
                    continue;
                }
            }

            let block = match parse_block(&buf) {
                Ok(Some(b)) => b,
                Ok(None) => {
                    if defer_tear {
                        self.next_block -= 1;
                        return false;
                    }
                    continue;
                }
                Err(e) => {
                    if defer_tear {
                        self.next_block -= 1;
                        return false;
                    }
                    if !tolerate_tear {
                        self.damaged.push(Damage {
                            path: self.path.clone(),
                            offset,
                            reason: e.to_string(),
                        });
                    }
                    continue;
                }
            };

            let boot = self.reader.header().boot;
            self.buffered.clear();
            self.pos = 0;
            let mut broken = None;
            // Migration failures pile up per block rather than raining down per
            // record: they are systematic (a whole block of one layout), and a
            // thousand identical damage entries are worse than one with a
            // count.
            let mut unmigrated: u32 = 0;
            let mut first_chain_error = None;
            for item in block.records() {
                match item {
                    Ok((at, record)) => {
                        // A record of an old segment is first brought up to the
                        // current layout: the filters and the owning copy have
                        // to see what the caller will see, not raw material
                        // from disk.
                        let migrated = match &self.migration {
                            MigrationState::None => Chained::Same(record),
                            MigrationState::Chain(steps) => match migrate::apply(steps, record) {
                                Ok(m) => m,
                                Err(e) => {
                                    unmigrated += 1;
                                    first_chain_error.get_or_insert(e);
                                    continue;
                                }
                            },
                        };
                        let Some(record) = migrated.record() else {
                            // A record deleted by a step is a decision of the
                            // schema, not a loss: it has no place in a damage
                            // report.
                            continue;
                        };
                        // Selection before the owning copy: a record that is
                        // filtered out must not cost an allocation for its
                        // payload.
                        if let Some(f) = &self.prefilter
                            && !f(&record)
                        {
                            continue;
                        }
                        self.buffered.push(Some(RawEntry {
                            at: BootTime::new(boot, at),
                            record: own(&record),
                        }));
                    }
                    Err(e) => {
                        broken = Some(e.to_string());
                        break;
                    }
                }
            }
            if unmigrated > 0 {
                let e = first_chain_error.expect("the counter grows together with the error");
                self.damaged.push(Damage {
                    path: self.path.clone(),
                    offset,
                    reason: format!(
                        "{e}: records not brought to the current version: {unmigrated}"
                    ),
                });
            }
            // Record parsing breaking off inside a block: for a live tail that
            // is the same unfinished write (record frames tear at the boundary
            // of what has arrived), and the records before the break stay in
            // the selection as they are.
            if broken.is_some() && defer_tear {
                // Half a block is not handed out even to a subscription: coming
                // back to it whole, it would hand those records out a second
                // time.
                self.buffered.clear();
                self.pos = 0;
                self.next_block -= 1;
                return false;
            }
            if let Some(reason) = broken
                && !tolerate_tear
            {
                self.damaged.push(Damage {
                    path: self.path.clone(),
                    offset,
                    reason,
                });
            }
            if self.reverse {
                self.buffered.reverse();
            }
            if !self.buffered.is_empty() {
                return true;
            }
        }
        false
    }
}

/// A cursor over the segments of one channel.
pub struct ChannelCursor {
    dir: PathBuf,
    /// The segment names in walk order.
    segments: Vec<SegmentName>,
    /// The index of the next segment in `segments`.
    next_segment: usize,
    current: Option<SegmentCursor>,
    reverse: bool,
    bounds: Bounds,
    expect_store: Option<u64>,
    prefilter: Option<Prefilter>,
    require_metrics: Option<Arc<std::collections::HashSet<dduroc_format::MetricId>>>,
    migrations: Option<MigrationCtx>,
    liveness: Liveness,
    /// The run the selection is restricted to: a subscription lists the
    /// directory again and has to select segments by the same rule as at open
    /// time.
    boot: Option<BootCounter>,
    /// The newest segment of the listing: during live reading it may have been
    /// caught at birth (the file created, the header not yet written). For a
    /// subscription it is also the boundary beyond which the unlisted begins.
    newest: Option<SegmentName>,
    damaged: Vec<Damage>,
    /// The runs whose segments had to be skipped: a wall-clock window and no
    /// anchor.
    unanchored: Vec<BootCounter>,
    /// The namespace — for labelling the records handed out.
    ///
    /// An `Arc<str>` rather than a `String`: the name is copied into every
    /// record handed out, and over a hundred thousand records that would be a
    /// hundred thousand allocations.
    pub namespace: Arc<str>,
    /// The channel's storage class: a channel is a class, and it has no second
    /// name.
    pub channel: StorageClass,
}

/// The parameters for opening a channel.
#[derive(Clone, Default)]
pub struct ChannelScope {
    /// The window, already brought to the runs' relative scale: converting
    /// wall-clock bounds by anchors is the query's business, not the cursor's.
    pub bounds: Bounds,
    pub boot: Option<BootCounter>,
    pub reverse: bool,
    pub expect_store: Option<u64>,
    pub prefilter: Option<Prefilter>,
    /// At most how many segments to look at (in walk order).
    ///
    /// The search for "what came before the window" needs it: without a bound
    /// it could walk back through the whole retention depth, reading megabytes
    /// for one value.
    pub max_segments: Option<usize>,
    /// Skip sealed segments that hold none of these metrics. The check goes by
    /// the set in the footer, without reading any blocks.
    pub require_metrics: Option<Arc<std::collections::HashSet<dduroc_format::MetricId>>>,
    /// The schema version and the migration steps: segments of earlier versions
    /// are read through the chain. `None` means the schema is unknown and
    /// records go through as they are.
    pub migrations: Option<MigrationCtx>,
    /// Whether the store is being written to right now and whether the cursor
    /// will come back for more.
    ///
    /// Live reading has to tolerate two ordinary coincidences that in a dump
    /// would mean corruption: a segment evicted by rotation between the listing
    /// and the open (the file is gone — but so is the data that disappeared),
    /// and the tail of an unsealed segment that the writer is appending to at
    /// that very moment (a page is visible to the reader before the write has
    /// landed whole). A dump has neither, and there the same signs are honestly
    /// reported as damage.
    pub liveness: Liveness,
}

/// Select the segments that may hold records from the window.
///
/// The bounds are taken per run: microseconds of different runs cannot be
/// compared, and a run that is not in the window at all is discarded whole —
/// without opening any of its files.
///
/// A segment's name carries the time of its **first** record, so the upper
/// bound cuts precisely: a segment that began later than `to` is knowably not
/// needed.
///
/// The lower bound cannot cut that way: a segment may have begun before `from`
/// and hold the records wanted. Only one is discarded — the one followed by a
/// segment of the same run that begins **strictly earlier** than `from`, in
/// which case every record of the first lies before the second begins, that
/// is, before `from`.
///
/// The comparison is strict on purpose. With `next.base == from` the last
/// record of the current segment may have exactly the time `from`: the clock
/// is monotonic but not strictly increasing, and in a burst two neighbouring
/// events get one and the same microsecond. A non-strict comparison would
/// throw such a record out of a selection that includes it.
fn select_segments(
    all: &[SegmentName],
    bounds: &Bounds,
    boot: Option<BootCounter>,
    unanchored: &mut Vec<BootCounter>,
) -> Vec<SegmentName> {
    let mut segments = Vec::new();
    for (i, name) in all.iter().enumerate() {
        if let Some(b) = boot
            && name.boot != b
        {
            continue;
        }
        let run = match bounds.fit(name.boot) {
            Fit::In(run) => run,
            Fit::Outside => continue,
            // The data is there, but there is nothing to apply it to the
            // wall-clock window with. Silence here would look like "the device
            // wrote nothing in those hours".
            Fit::Unanchored => {
                if !unanchored.contains(&name.boot) {
                    unanchored.push(name.boot);
                }
                continue;
            }
        };
        if let Some(to) = run.to
            && name.base > to
        {
            continue;
        }
        if let Some(from) = run.from
            && let Some(next) = all.get(i + 1)
            && next.base < from
            && next.boot == name.boot
        {
            continue;
        }
        segments.push(*name);
    }
    segments
}

impl ChannelCursor {
    /// Open a channel, selecting segments by time range.
    pub fn open(
        dir: &Path,
        namespace: Arc<str>,
        channel: StorageClass,
        scope: &ChannelScope,
    ) -> Result<Self> {
        let (boot, reverse, expect_store) = (scope.boot, scope.reverse, scope.expect_store);
        // Names only: segment sizes cost a `stat` per file, while selection by
        // window goes by name — the time of the first record is in it.
        let all = dduroc_engine::rotation::Inventory::scan_names(dir).map_err(ReadError::Engine)?;

        let mut unanchored = Vec::new();
        let mut segments = select_segments(&all, &scope.bounds, boot, &mut unanchored);
        if reverse {
            segments.reverse();
        }
        // The look-at bound is applied AFTER the reversal: its meaning is "this
        // many segments from the start of the walk", and in reverse order the
        // walk goes from the fresh to the old.
        if let Some(k) = scope.max_segments {
            segments.truncate(k);
        }
        let newest = segments.iter().max().copied();

        Ok(Self {
            dir: dir.to_owned(),
            segments,
            next_segment: 0,
            current: None,
            reverse,
            bounds: scope.bounds.clone(),
            expect_store,
            prefilter: scope.prefilter.clone(),
            require_metrics: scope.require_metrics.clone(),
            migrations: scope.migrations,
            liveness: scope.liveness,
            boot,
            newest,
            damaged: Vec::new(),
            unanchored,
            namespace,
            channel,
        })
    }

    /// The fragments of the channel that could not be read.
    ///
    /// This includes damage in the segment being read **right now**. Without
    /// that it would surface only in `finish_current`, that is, once the
    /// segment had been read to the end — while the walk breaks off on `limit`
    /// and on leaving `stream` mid-segment. A skipped block would disappear
    /// from the report, and `QueryResult::is_complete()` would declare complete
    /// an answer that data had dropped out of.
    pub fn damaged(&self) -> Vec<Damage> {
        let mut out = self.damaged.clone();
        if let Some(c) = &self.current {
            out.extend_from_slice(c.damaged());
        }
        out
    }

    /// The runs whose segments lie in this channel but did not reach the
    /// selection: the window is in wall-clock time and they have no anchor.
    pub fn unanchored(&self) -> &[BootCounter] {
        &self.unanchored
    }

    pub fn peek(&mut self) -> Option<&RawEntry> {
        loop {
            if self.current.is_none() && !self.advance() {
                return None;
            }
            let has = self.current.as_mut().and_then(|c| c.peek()).is_some();
            if has {
                return self.current.as_mut().and_then(|c| c.peek());
            }
            if self.holds_the_live_segment() {
                return None;
            }
            self.finish_current();
        }
    }

    pub fn next_entry(&mut self) -> Option<RawEntry> {
        loop {
            if self.current.is_none() && !self.advance() {
                return None;
            }
            if let Some(item) = self.current.as_mut().and_then(|c| c.next_entry()) {
                return Some(item);
            }
            if self.holds_the_live_segment() {
                return None;
            }
            self.finish_current();
        }
    }

    /// Whether the cursor holds a segment that is being written to right now.
    ///
    /// Such a segment must not be closed once it is read to the end: a
    /// subscription will come back to it for more. The sign is that not one
    /// segment is listed after it: a writer that has started a new segment will
    /// not go back to the previous one.
    fn holds_the_live_segment(&self) -> bool {
        self.liveness.is_following() && self.next_segment >= self.segments.len()
    }

    /// Read on through what has appeared since last time. `true` means
    /// something has.
    ///
    /// Two different prices: reading on through the tail of an open segment is
    /// an open and a read of the fresh blocks, while noticing a new segment is
    /// a walk of the directory. So the walk is not done always but only when
    /// the store has announced that the directories changed (`relist`): while
    /// the writer pours into the same file, a subscription does not do a single
    /// `readdir`.
    pub fn extend(&mut self, relist: bool) -> bool {
        let grew = self.current.as_mut().is_some_and(|c| c.extend());
        // Listing is needed anyway: the segment may have changed exactly
        // between two wake-ups, and then the tail growing means nothing.
        if relist { self.relist() || grew } else { grew }
    }

    /// List the directory again and add the segments that appeared since.
    fn relist(&mut self) -> bool {
        if self.reverse {
            return false;
        }
        let Ok(all) = dduroc_engine::rotation::Inventory::scan_names(&self.dir) else {
            return false;
        };
        // Throw away what has been walked. A subscription lives for weeks, a
        // channel changes segment after segment, and a list that is only
        // appended to grows exactly with its lifetime: a month of rotation
        // every five minutes is close to a hundred thousand names per channel,
        // of which only the unread ones are needed. The newest is remembered
        // separately (`newest`), so selection is unaffected: it compares
        // against that rather than against the start of the list.
        if self.next_segment > 0 {
            self.segments.drain(..self.next_segment);
            self.next_segment = 0;
        }
        let selected = select_segments(&all, &self.bounds, self.boot, &mut self.unanchored);
        let mut grew = false;
        for name in selected {
            // Strictly newer than everything listed: the list grows only at the
            // end, and `next_segment` is an index into it — an insertion in the
            // middle would lead the subscription back over what it has already
            // read.
            if Some(name) > self.newest {
                self.segments.push(name);
                self.newest = Some(name);
                grew = true;
            }
        }
        grew
    }

    /// Take the damage that has accumulated, leaving the lists empty.
    pub fn take_damage(&mut self) -> Vec<Damage> {
        let mut out = std::mem::take(&mut self.damaged);
        if let Some(c) = &mut self.current {
            out.append(&mut c.take_damage());
        }
        out
    }

    fn finish_current(&mut self) {
        if let Some(c) = self.current.take() {
            self.damaged.extend_from_slice(c.damaged());
        }
    }

    fn advance(&mut self) -> bool {
        while self.next_segment < self.segments.len() {
            let name = self.segments[self.next_segment];
            self.next_segment += 1;
            let path = self.dir.join(name.to_string());
            match SegmentCursor::open(
                &path,
                self.reverse,
                self.expect_store,
                self.prefilter.clone(),
                self.migrations,
                self.liveness,
            ) {
                Ok(mut c) => {
                    // A segment that knowably holds none of the metrics wanted
                    // is not read at all: the set of identifiers is in the
                    // footer, and the answer comes without a single block read.
                    if let Some(wanted) = &self.require_metrics
                        && c.contains_any_metric(wanted) == Some(false)
                    {
                        continue;
                    }
                    // The bounds are in the scale of the run the segment
                    // belongs to: microseconds of different runs are not
                    // comparable.
                    if let Some(run) = self.bounds.for_boot(c.boot()) {
                        c.clip_to_window(run.from, run.to);
                    }
                    self.current = Some(c);
                    return true;
                }
                Err(e) => {
                    // A segment evicted by rotation between the listing and the
                    // open: the file is gone — but so is the data that
                    // "disappeared", the engine removed the history itself.
                    if self.liveness.is_live() && is_not_found(&e) {
                        continue;
                    }
                    // The newest segment caught at birth: the file is already
                    // created but the header is not yet written. A one-off
                    // query passes it by — the next query will see it. A
                    // subscription cannot pass it by: it has no next query, and
                    // the segment would vanish entirely. It steps back and will
                    // return.
                    if self.liveness.is_live() && Some(name) == self.newest {
                        if self.liveness.is_following() {
                            self.next_segment -= 1;
                            return false;
                        }
                        continue;
                    }
                    // A segment that failed to open must not stop the walk of
                    // the channel: the others are read independently.
                    self.damaged.push(Damage {
                        path,
                        offset: 0,
                        reason: e.to_string(),
                    });
                }
            }
        }
        false
    }
}

/// The file does not exist — the segment vanished from under the cursor.
fn is_gone(e: &dduroc_engine::Error) -> bool {
    matches!(e, dduroc_engine::Error::Io { source, .. } if source.kind() == std::io::ErrorKind::NotFound)
}

/// The same for a read error — the segment vanished between the listing and the
/// open.
fn is_not_found(e: &ReadError) -> bool {
    matches!(
        e,
        ReadError::Engine(dduroc_engine::Error::Io { source, .. })
            if source.kind() == std::io::ErrorKind::NotFound
    )
}

impl std::fmt::Debug for ChannelScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelScope")
            .field("bounds", &self.bounds)
            .field("boot", &self.boot)
            .field("reverse", &self.reverse)
            .field("expect_store", &self.expect_store)
            .field("prefilter", &self.prefilter.is_some())
            .finish()
    }
}

impl std::fmt::Debug for SegmentCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SegmentCursor")
            .field("path", &self.path)
            .field("blocks", &self.offsets.len())
            .field("next_block", &self.next_block)
            .field("reverse", &self.reverse)
            .field("damaged", &self.damaged.len())
            .finish()
    }
}

impl std::fmt::Debug for ChannelCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelCursor")
            .field("namespace", &self.namespace)
            .field("channel", &self.channel)
            .field("segments", &self.segments.len())
            .field("next_segment", &self.next_segment)
            .field("damaged", &self.damaged.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Query;
    use dduroc_engine::epochs::Epochs;

    fn seg(boot: u32, base: u64) -> SegmentName {
        SegmentName::new(BootCounter(boot), Micros(base))
    }

    fn bases(v: &[SegmentName]) -> Vec<u64> {
        v.iter().map(|n| n.base.0).collect()
    }

    /// Selection with no interest in dropped runs — a separate test checks
    /// those.
    fn select(all: &[SegmentName], bounds: &Bounds, boot: Option<BootCounter>) -> Vec<SegmentName> {
        select_segments(all, bounds, boot, &mut Vec::new())
    }

    /// The bounds of one run — as a query will build them.
    fn within(boot: u32, from: Option<u64>, to: Option<u64>) -> Bounds {
        let mut q = Query::new();
        q.from = from.map(|m| BootTime::from_raw(boot, m).into());
        q.to = to.map(|m| BootTime::from_raw(boot, m).into());
        q.resolve(&Epochs::default()).bounds
    }

    #[test]
    fn lower_bound_keeps_the_segment_that_may_hold_it() {
        // Three segments of one run: [0..100), [100..200), [200..).
        let all = [seg(0, 0), seg(0, 100), seg(0, 200)];

        // `from` exactly on a boundary. The last record of the first segment
        // may have the time exactly 100: the clock is monotonic but not
        // strictly increasing, and in a burst two neighbouring events get one
        // microsecond. Discarding the first segment would lose that record.
        assert_eq!(
            bases(&select(&all, &within(0, Some(100), None), None)),
            vec![0, 100, 200],
            "a segment whose last record may lie exactly on the bound is needed"
        );

        // `from` strictly inside the second: the first is knowably all behind.
        assert_eq!(
            bases(&select(&all, &within(0, Some(101), None), None)),
            vec![100, 200]
        );
        assert_eq!(
            bases(&select(&all, &within(0, Some(250), None), None)),
            vec![200]
        );
        // Later than all the data: the last segment is checked anyway — it is
        // open and may have received records after the inventory was taken.
        assert_eq!(
            bases(&select(&all, &within(0, Some(9_999), None), None)),
            vec![200]
        );
    }

    #[test]
    fn upper_bound_is_exact() {
        let all = [seg(0, 0), seg(0, 100), seg(0, 200)];
        // The name carries the time of the first record, so a segment that
        // began later than `to` is knowably not needed. One that began exactly
        // at `to` is.
        assert_eq!(
            bases(&select(&all, &within(0, None, Some(100)), None)),
            vec![0, 100]
        );
        assert_eq!(
            bases(&select(&all, &within(0, None, Some(99)), None)),
            vec![0]
        );
        assert_eq!(select(&all, &within(0, None, Some(0)), None).len(), 1);
    }

    #[test]
    fn bounds_of_one_run_do_not_touch_another() {
        // Every run has its own time, so "the next one began earlier" means
        // nothing across a run boundary.
        let all = [seg(0, 500), seg(1, 10), seg(1, 900)];
        assert_eq!(
            bases(&select(&all, &within(0, Some(400), None), None)),
            vec![500, 10, 900],
            "a run 0 segment is not discarded by run 1 time"
        );

        // But a bound in run 1's scale discards run 0 entirely: it is all
        // behind. This used to be inexpressible — microseconds without a run
        // applied to every scale.
        assert_eq!(
            bases(&select(&all, &within(1, Some(400), None), None)),
            vec![10, 900]
        );
        assert_eq!(
            bases(&select(&all, &within(0, None, Some(600)), None)),
            vec![500],
            "an upper bound in run 0 cuts off all of run 1"
        );
        // The same bound below the start of run 0's only segment leaves
        // nothing: 500 > 400, and run 1 is entirely later.
        assert!(select(&all, &within(0, None, Some(400)), None).is_empty());
    }

    #[test]
    fn boot_filter_selects_one_run() {
        let all = [seg(0, 500), seg(1, 10), seg(1, 900)];
        assert_eq!(
            bases(&select(&all, &Bounds::All, Some(BootCounter(1)))),
            vec![10, 900]
        );
        assert!(select(&all, &Bounds::All, Some(BootCounter(7))).is_empty());
    }

    #[test]
    fn segments_of_unanchored_runs_are_named_not_just_skipped() {
        // A wall-clock window and an empty epoch registry — a dump copied
        // without `epochs.bin`. The segments are on disk, but there is nothing
        // to apply them to a wall clock with; such runs can only be listed from
        // the directory.
        let all = [seg(0, 100), seg(0, 900), seg(3, 50)];
        let utc = chrono::DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let bounds = Query::new().since(utc).resolve(&Epochs::default()).bounds;

        let mut unanchored = Vec::new();
        let picked = select_segments(&all, &bounds, None, &mut unanchored);
        assert!(picked.is_empty(), "there is nothing to match with");
        assert_eq!(
            unanchored,
            vec![BootCounter(0), BootCounter(3)],
            "every run is named once"
        );

        // A relative window does not depend on the epochs and loses nothing.
        let mut unanchored = Vec::new();
        let bounds = within(0, Some(0), None);
        assert_eq!(
            bases(&select_segments(&all, &bounds, None, &mut unanchored)),
            vec![100, 900, 50]
        );
        assert!(unanchored.is_empty());
    }

    #[test]
    fn a_segment_rotated_away_under_a_live_cursor_is_not_damage() {
        // Between listing the channel and opening the segment, a live store's
        // rotation may have evicted it. The file is gone — but so is the data
        // that "disappeared": eviction is normal, and a live walk moves on
        // silently. In a dump segments do not vanish by themselves, and the
        // same picture there is damage.
        let dir = tempfile::tempdir().unwrap();
        sealed_segment(dir.path(), &[100, 200]);
        let later = sealed_segment(dir.path(), &[900]);

        let run = |liveness: Liveness| {
            let scope = ChannelScope {
                liveness,
                ..ChannelScope::default()
            };
            let mut c =
                ChannelCursor::open(dir.path(), Arc::from("ns"), StorageClass::Default, &scope)
                    .unwrap();
            // The listing is already done — now "rotation" takes the later
            // segment.
            std::fs::remove_file(&later).unwrap();
            let mut times = Vec::new();
            while let Some(e) = c.next_entry() {
                times.push(e.at.at.0);
            }
            (times, c.damaged())
        };

        let (times, damaged) = run(Liveness::Live);
        assert_eq!(times, vec![100, 200], "the surviving segments are read");
        assert!(damaged.is_empty(), "eviction is not damage: {damaged:?}");

        sealed_segment(dir.path(), &[900]);
        let (times, damaged) = run(Liveness::Frozen);
        assert_eq!(times, vec![100, 200]);
        assert_eq!(damaged.len(), 1, "in a dump a vanished file is damage");
    }

    #[test]
    fn a_torn_tail_block_is_invisible_to_a_live_cursor() {
        // The writer lays a block down with one write, but the pages become
        // visible to a reader with no guarantee of being whole: the header is
        // already there, the body is not. Such a tail on an unsealed segment is
        // "not data yet", and a live cursor passes it over silently; the intact
        // blocks before it are read. Nobody appends to a dump — there a torn
        // block is honestly declared damage.
        use dduroc_engine::segment::SegmentWriter;
        use dduroc_format::block::BlockBuilder;
        use dduroc_format::record::Message;
        use dduroc_format::segment::SegmentHeader;
        use dduroc_format::{Compression, EventId, ProtocolVersion};

        let dir = tempfile::tempdir().unwrap();
        let header = SegmentHeader {
            protocol_version: ProtocolVersion(1),
            boot: BootCounter(0),
            base: Micros(100),
            store_id: 0,
        };
        let mut seg = SegmentWriter::create(dir.path(), header, 1 << 20).unwrap();
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        let record = Record::Message(Message {
            event: EventId(1),
            span: None,
            payload: &[0xAB; 4],
        });

        // An intact block…
        builder.push(Micros(100), &record).unwrap();
        builder
            .finish(seg.next_seq(), Compression::None, &mut out)
            .unwrap();
        seg.append_block(&out).unwrap();

        // …and a torn one: the header is valid, the body is damaged — exactly
        // what a block whose write has not arrived whole looks like.
        out.clear();
        builder.push(Micros(200), &record).unwrap();
        builder
            .finish(seg.next_seq(), Compression::None, &mut out)
            .unwrap();
        let last = out.len() - 1;
        out[last] ^= 0xFF;
        seg.append_block(&out).unwrap();
        // The segment stays unsealed — like an active one at the writer.
        drop(seg);
        let path = dir
            .path()
            .join(SegmentName::new(BootCounter(0), Micros(100)).to_string());

        let mut live = SegmentCursor::open(&path, false, None, None, None, Liveness::Live).unwrap();
        let mut times = Vec::new();
        while let Some(e) = live.next_entry() {
            times.push(e.at.at.0);
        }
        assert_eq!(
            times,
            vec![100],
            "the intact block is read, the torn tail waits"
        );
        assert!(live.damaged().is_empty(), "{:?}", live.damaged());

        let mut dump =
            SegmentCursor::open(&path, false, None, None, None, Liveness::Frozen).unwrap();
        while dump.next_entry().is_some() {}
        assert_eq!(
            dump.damaged().len(),
            1,
            "in a dump a torn block is corruption"
        );
    }

    #[test]
    fn a_torn_tail_is_kept_for_the_subscription_instead_of_stepped_over() {
        // A one-off query passes an unfinished tail over because the NEXT query
        // will see it. A subscription has no next query: passing it by, it
        // would lose those records forever. So it defers the block and comes
        // back to it once it has arrived whole.
        use std::os::unix::fs::FileExt;

        let (path, tail_offset, whole_tail) = segment_with_a_half_written_tail();

        // A subscription: the intact block is handed out, the torn one deferred
        // — and not declared damage, because it is not data yet.
        let mut following =
            SegmentCursor::open(&path, false, None, None, None, Liveness::Following).unwrap();
        let mut times = Vec::new();
        while let Some(e) = following.next_entry() {
            times.push(e.at.at.0);
        }
        assert_eq!(times, vec![100], "a torn tail is not handed out by halves");
        assert!(following.damaged().is_empty(), "{:?}", following.damaged());

        // A one-off query in the same place: the tail is skipped FOR GOOD.
        let mut once = SegmentCursor::open(&path, false, None, None, None, Liveness::Live).unwrap();
        while once.next_entry().is_some() {}

        // The writer finished the block.
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(&whole_tail, tail_offset)
            .unwrap();

        following.extend();
        let mut rest = Vec::new();
        while let Some(e) = following.next_entry() {
            rest.push(e.at.at.0);
        }
        assert_eq!(
            rest,
            vec![200],
            "a block that arrived must reach the subscription"
        );
        assert!(following.damaged().is_empty(), "{:?}", following.damaged());

        once.extend();
        assert!(
            once.next_entry().is_none(),
            "for a one-off query the next query shows this block, not this one"
        );
    }

    #[test]
    fn a_newborn_segment_is_waited_for_by_a_subscription_not_walked_past() {
        // A segment is born in two steps: the file first, then the header. In
        // between it does not open. A one-off query passes it by — the next
        // query will show it; a subscription cannot pass it by, it has no next
        // query, and the segment would vanish entirely.
        let dir = tempfile::tempdir().unwrap();
        sealed_segment(dir.path(), &[100]);
        let newborn = dir
            .path()
            .join(SegmentName::new(BootCounter(0), Micros(900)).to_string());
        // The file is there, the header is not yet.
        std::fs::File::create(&newborn).unwrap();

        let open = |liveness| {
            let scope = ChannelScope {
                liveness,
                ..ChannelScope::default()
            };
            ChannelCursor::open(dir.path(), Arc::from("ns"), StorageClass::Default, &scope).unwrap()
        };
        let mut following = open(Liveness::Following);
        let mut once = open(Liveness::Live);
        for c in [&mut following, &mut once] {
            assert_eq!(c.next_entry().map(|e| e.at.at.0), Some(100));
            assert_eq!(c.next_entry().map(|e| e.at.at.0), None, "the newborn waits");
            assert!(
                c.damaged().is_empty(),
                "birth is not damage: {:?}",
                c.damaged()
            );
        }

        // The writer finished the header and the block.
        std::fs::remove_file(&newborn).unwrap();
        sealed_segment(dir.path(), &[900]);

        following.extend(true);
        assert_eq!(
            following.next_entry().map(|e| e.at.at.0),
            Some(900),
            "a segment that was born must reach the subscription whole"
        );

        once.extend(true);
        assert!(
            once.next_entry().is_none(),
            "for a one-off query the next query shows this segment, not this one"
        );
    }

    #[test]
    fn a_long_subscription_does_not_remember_every_segment_it_ever_read() {
        // A subscription lives for weeks while a channel changes segment after
        // segment. A list of names that is only appended to grows exactly with
        // its lifetime: a month of rotation every five minutes is close to a
        // hundred thousand names per channel, and all but the unread ones are
        // dead. The newest is remembered separately, so the cleanup does not
        // affect selection.
        let dir = tempfile::tempdir().unwrap();
        sealed_segment(dir.path(), &[100]);

        let scope = ChannelScope {
            liveness: Liveness::Following,
            ..ChannelScope::default()
        };
        let mut c = ChannelCursor::open(dir.path(), Arc::from("ns"), StorageClass::Default, &scope)
            .unwrap();

        let mut seen = Vec::new();
        for i in 1..40u64 {
            // The segment has been read to the end and another has appeared
            // behind it.
            while let Some(e) = c.next_entry() {
                seen.push(e.at.at.0);
            }
            sealed_segment(dir.path(), &[100 * (i + 1)]);
            c.extend(true);
            assert!(
                c.segments.len() <= 3,
                "after {i} rotations the list holds {} names — the cleanup of walked names does not work",
                c.segments.len()
            );
        }
        while let Some(e) = c.next_entry() {
            seen.push(e.at.at.0);
        }

        assert_eq!(
            seen,
            (1..=40u64).map(|i| i * 100).collect::<Vec<_>>(),
            "the cleanup has no right to cost a single record"
        );
        assert!(c.damaged().is_empty(), "{:?}", c.damaged());
    }

    #[test]
    fn a_growing_segment_is_read_further_without_being_reopened() {
        // A segment being written to is read on as it grows: a subscription
        // does not re-read the file from the start — otherwise an
        // eight-megabyte segment would be read whole for every batch of fresh
        // records.
        let dir = tempfile::tempdir().unwrap();
        let (mut seg, mut builder, record) = growing_segment(dir.path());
        let mut out = Vec::new();

        builder.push(Micros(100), &record).unwrap();
        builder
            .finish(seg.next_seq(), dduroc_format::Compression::None, &mut out)
            .unwrap();
        seg.append_block(&out).unwrap();

        let path = dir
            .path()
            .join(SegmentName::new(BootCounter(0), Micros(100)).to_string());
        let mut c =
            SegmentCursor::open(&path, false, None, None, None, Liveness::Following).unwrap();
        assert_eq!(c.next_entry().map(|e| e.at.at.0), Some(100));
        assert!(
            c.next_entry().is_none(),
            "that is all that has been written so far"
        );

        out.clear();
        builder.push(Micros(200), &record).unwrap();
        builder
            .finish(seg.next_seq(), dduroc_format::Compression::None, &mut out)
            .unwrap();
        seg.append_block(&out).unwrap();

        assert!(c.extend(), "the appended block must be found");
        assert_eq!(c.next_entry().map(|e| e.at.at.0), Some(200));
    }

    #[test]
    fn a_subscription_holds_the_live_segment_and_picks_up_the_next_one() {
        // Having read a segment to its end, a subscription has no right to
        // close it: it is still being written to, and with the segment cursor
        // the place to read on from would go too. It can be closed only once
        // the next one is listed: a writer that has started a new file will not
        // go back.
        let dir = tempfile::tempdir().unwrap();
        let (mut seg, mut builder, record) = growing_segment(dir.path());
        let mut out = Vec::new();
        let mut write = |at: u64, seg: &mut dduroc_engine::segment::SegmentWriter| {
            out.clear();
            builder.push(Micros(at), &record).unwrap();
            builder
                .finish(seg.next_seq(), dduroc_format::Compression::None, &mut out)
                .unwrap();
            seg.append_block(&out).unwrap();
        };
        write(100, &mut seg);

        let scope = ChannelScope {
            liveness: Liveness::Following,
            ..ChannelScope::default()
        };
        let mut c = ChannelCursor::open(dir.path(), Arc::from("ns"), StorageClass::Default, &scope)
            .unwrap();
        assert_eq!(c.next_entry().map(|e| e.at.at.0), Some(100));
        assert!(
            c.next_entry().is_none(),
            "that is all that has been written so far"
        );

        // The same file grew: the directory did not change, so there is no
        // reason to walk it.
        write(200, &mut seg);
        assert!(c.extend(false), "the appended tail of a live segment");
        assert_eq!(c.next_entry().map(|e| e.at.at.0), Some(200));

        // Rotation: the writer sealed the previous segment and started the
        // next.
        drop(seg);
        sealed_segment(dir.path(), &[900]);
        assert!(
            !c.extend(false),
            "without an announcement that the directories changed a subscription does not walk them"
        );
        assert!(c.extend(true), "announced, so it walked and found");
        assert_eq!(c.next_entry().map(|e| e.at.at.0), Some(900));
    }

    /// An unsealed segment ready to take blocks, together with its accumulator.
    fn growing_segment(
        dir: &Path,
    ) -> (
        dduroc_engine::segment::SegmentWriter,
        dduroc_format::block::BlockBuilder,
        Record<'static>,
    ) {
        use dduroc_format::record::Message;
        use dduroc_format::segment::SegmentHeader;
        use dduroc_format::{EventId, ProtocolVersion};

        let header = SegmentHeader {
            protocol_version: ProtocolVersion(1),
            boot: BootCounter(0),
            base: Micros(100),
            store_id: 0,
        };
        let seg = dduroc_engine::segment::SegmentWriter::create(dir, header, 1 << 20).unwrap();
        let record = Record::Message(Message {
            event: EventId(1),
            span: None,
            payload: &[0xAB; 4],
        });
        (seg, dduroc_format::block::BlockBuilder::new(), record)
    }

    /// A segment whose last block reached the medium half way — exactly what a
    /// block whose `write` a reader caught mid-word looks like. Returns the
    /// path, the tail's offset and its intact bytes.
    fn segment_with_a_half_written_tail() -> (PathBuf, u64, Vec<u8>) {
        use std::os::unix::fs::FileExt;

        // The tempdir lives until the process ends: the path is handed
        // outwards.
        let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
        let (mut seg, mut builder, record) = growing_segment(dir.path());
        let mut out = Vec::new();

        builder.push(Micros(100), &record).unwrap();
        builder
            .finish(seg.next_seq(), dduroc_format::Compression::None, &mut out)
            .unwrap();
        seg.append_block(&out).unwrap();

        let mut tail = Vec::new();
        builder.push(Micros(200), &record).unwrap();
        builder
            .finish(seg.next_seq(), dduroc_format::Compression::None, &mut tail)
            .unwrap();
        let tail_offset = seg.data_end();
        drop(seg);

        let path = dir
            .path()
            .join(SegmentName::new(BootCounter(0), Micros(100)).to_string());
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(&tail[..tail.len() / 2], tail_offset)
            .unwrap();
        (path, tail_offset, tail)
    }

    /// A sealed segment of the given version built from ready records: one
    /// record is one block, and the footer is assembled as the writer does it
    /// (the types together with the block). The blocks are needed one at a
    /// time: what is being checked is precisely the selection of blocks.
    fn sealed_versioned(dir: &Path, version: u16, records: &[(u64, Record<'_>)]) -> PathBuf {
        use dduroc_engine::segment::SegmentWriter;
        use dduroc_format::block::BlockBuilder;
        use dduroc_format::segment::SegmentHeader;
        use dduroc_format::{Compression, FooterBuilder, ProtocolVersion};

        let base = Micros(records[0].0);
        let header = SegmentHeader {
            protocol_version: ProtocolVersion(version),
            boot: BootCounter(0),
            base,
            store_id: 0,
        };
        let mut seg = SegmentWriter::create(dir, header, 1 << 20).unwrap();
        let mut footer = FooterBuilder::new();
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        for (t, rec) in records {
            match rec {
                Record::Message(m) => footer.add_event(m.event),
                Record::Sample(s) => footer.add_metric(s.metric),
                _ => {}
            }
            builder.push(Micros(*t), rec).unwrap();
            out.clear();
            let h = builder
                .finish(seg.next_seq(), Compression::None, &mut out)
                .unwrap();
            let offset = seg.append_block(&out).unwrap();
            footer.add_block(offset, &h, Micros(*t));
        }
        seg.seal(&footer.build()).unwrap();
        dir.join(SegmentName::new(BootCounter(0), base).to_string())
    }

    fn sealed_segment(dir: &Path, times: &[u64]) -> PathBuf {
        use dduroc_engine::segment::SegmentWriter;
        use dduroc_format::block::BlockBuilder;
        use dduroc_format::record::Message;
        use dduroc_format::segment::SegmentHeader;
        use dduroc_format::{Compression, EventId, FooterBuilder, ProtocolVersion, Record};

        let base = Micros(times[0]);
        let header = SegmentHeader {
            protocol_version: ProtocolVersion(1),
            boot: BootCounter(0),
            base,
            store_id: 0,
        };
        let mut seg = SegmentWriter::create(dir, header, 1 << 20).unwrap();
        let mut footer = FooterBuilder::new();
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        for &t in times {
            builder
                .push(
                    Micros(t),
                    &Record::Message(Message {
                        event: EventId(1),
                        span: None,
                        payload: &[],
                    }),
                )
                .unwrap();
            out.clear();
            let h = builder
                .finish(seg.next_seq(), Compression::None, &mut out)
                .unwrap();
            let offset = seg.append_block(&out).unwrap();
            footer.add_block(offset, &h, Micros(t));
        }
        seg.seal(&footer.build()).unwrap();
        dir.join(SegmentName::new(BootCounter(0), base).to_string())
    }

    #[test]
    fn an_old_segment_is_read_through_the_migration_chain() {
        // A v1 segment under a v2 schema: the changed type is re-encoded, the
        // deleted one drops out, the remapped metric changes its number, the
        // untouched type goes through byte for byte. Without the chain the
        // current decoders would parse the old layout silently and wrongly —
        // postcard is not self-describing.
        use dduroc_engine::schema::{DecodeError, MigrationInput, MigrationOutcome as Out};
        use dduroc_format::record::{Message, Sample};
        use dduroc_format::{EventId, MetricId, Value};

        fn step1(r: MigrationInput<'_>) -> std::result::Result<Option<Out>, DecodeError> {
            match (r.event_id(), r.metric_id()) {
                (Some(EventId(1)), _) => {
                    let old: (u8,) = r.decode()?;
                    Ok(Some(Out::Message {
                        event: EventId(1),
                        payload: postcard::to_allocvec(&(u16::from(old.0) * 2,)).unwrap(),
                    }))
                }
                (Some(EventId(2)), _) => Ok(None),
                (_, Some(MetricId(0x10))) => Ok(Some(Out::SampleMetric(MetricId(0x20)))),
                _ => Ok(Some(Out::AsIs)),
            }
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            events: &[EventId(1), EventId(2)],
            metrics: &[MetricId(0x10)],
            spans: &[],
            migrate: step1,
        }];
        let ctx = MigrationCtx {
            current_version: 2,
            steps: STEPS,
        };

        let dir = tempfile::tempdir().unwrap();
        let changed = postcard::to_allocvec(&(21u8,)).unwrap();
        let path = sealed_versioned(
            dir.path(),
            1,
            &[
                (
                    100,
                    Record::Message(Message {
                        event: EventId(1),
                        span: None,
                        payload: &changed,
                    }),
                ),
                (
                    200,
                    Record::Message(Message {
                        event: EventId(2),
                        span: None,
                        payload: &[7],
                    }),
                ),
                (
                    300,
                    Record::Message(Message {
                        event: EventId(3),
                        span: None,
                        payload: &[9],
                    }),
                ),
                (
                    400,
                    Record::Sample(Sample {
                        metric: MetricId(0x10),
                        value: Value::U64(555),
                    }),
                ),
            ],
        );

        let mut c =
            SegmentCursor::open(&path, false, None, None, Some(ctx), Liveness::Frozen).unwrap();
        let got: Vec<RawEntry> = std::iter::from_fn(|| c.next_entry()).collect();
        assert!(c.damaged().is_empty(), "{:?}", c.damaged());

        assert_eq!(got.len(), 3, "the deleted type dropped out: {got:?}");
        match &got[0].record {
            OwnedRecord::Message { event, payload, .. } => {
                assert_eq!(*event, EventId(1));
                let v: (u16,) = postcard::from_bytes(payload).unwrap();
                assert_eq!(v.0, 42, "the payload was re-encoded from the old layout");
            }
            other => panic!("{other:?}"),
        }
        match &got[1].record {
            OwnedRecord::Message { event, payload, .. } => {
                assert_eq!(*event, EventId(3));
                assert_eq!(payload, &[9], "an untouched type is byte for byte");
            }
            other => panic!("{other:?}"),
        }
        match &got[2].record {
            OwnedRecord::Sample { metric, value } => {
                assert_eq!(*metric, MetricId(0x20), "the metric was remapped");
                assert_eq!(
                    *value,
                    OwnedSampleValue::U64(555),
                    "the value is the original"
                );
            }
            other => panic!("{other:?}"),
        }

        // The same segment written at the current version does not go through
        // the chain: event 1's payload stays in the NEW layout as it is.
        let dir2 = tempfile::tempdir().unwrap();
        let fresh = postcard::to_allocvec(&(42u16,)).unwrap();
        let path = sealed_versioned(
            dir2.path(),
            2,
            &[(
                100,
                Record::Message(Message {
                    event: EventId(1),
                    span: None,
                    payload: &fresh,
                }),
            )],
        );
        let mut c =
            SegmentCursor::open(&path, false, None, None, Some(ctx), Liveness::Frozen).unwrap();
        match &c.next_entry().unwrap().record {
            OwnedRecord::Message { payload, .. } => {
                assert_eq!(payload, &fresh, "the current version is not touched");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_segment_from_the_future_is_named_not_misread() {
        // A dump from a device with new firmware in an old viewer: there is
        // nothing to parse a layout from the future with, and reading it with
        // the current decoders would mean showing garbage as data. The segment
        // drops out entirely — with an announcement, not silently.
        use dduroc_format::EventId;
        use dduroc_format::record::Message;

        let dir = tempfile::tempdir().unwrap();
        let path = sealed_versioned(
            dir.path(),
            5,
            &[(
                100,
                Record::Message(Message {
                    event: EventId(1),
                    span: None,
                    payload: &[1],
                }),
            )],
        );

        let ctx = MigrationCtx {
            current_version: 2,
            steps: &[],
        };
        let mut c =
            SegmentCursor::open(&path, false, None, None, Some(ctx), Liveness::Frozen).unwrap();
        assert!(
            c.next_entry().is_none(),
            "no records from the future are handed out"
        );
        let damaged = c.damaged();
        assert_eq!(damaged.len(), 1);
        assert!(
            damaged[0].reason.contains("newer than the schema"),
            "the cause is named: {}",
            damaged[0].reason
        );

        // Without a migration context (no schema at all) the behaviour is as
        // before: records are handed out as they are — whoever has the schema
        // parses them.
        let mut c = SegmentCursor::open(&path, false, None, None, None, Liveness::Frozen).unwrap();
        assert!(c.next_entry().is_some());
    }

    #[test]
    fn a_step_that_cannot_decode_reports_one_damage_per_block() {
        // A step's failure is systematic: a whole block of one layout. A
        // thousand identical damage entries are worse than one with a count —
        // but silence is not allowed either: a record dropped out of the
        // answer.
        use dduroc_engine::schema::{DecodeError, MigrationInput, MigrationOutcome as Out};
        use dduroc_format::EventId;
        use dduroc_format::record::Message;

        fn broken(r: MigrationInput<'_>) -> std::result::Result<Option<Out>, DecodeError> {
            let _: (u64, u64, u64, u64) = r.decode()?;
            Ok(Some(Out::AsIs))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            events: &[EventId(1)],
            metrics: &[],
            spans: &[],
            migrate: broken,
        }];

        let dir = tempfile::tempdir().unwrap();
        let path = sealed_versioned(
            dir.path(),
            1,
            &[
                (
                    100,
                    Record::Message(Message {
                        event: EventId(1),
                        span: None,
                        payload: &[1],
                    }),
                ),
                (
                    200,
                    Record::Message(Message {
                        event: EventId(9),
                        span: None,
                        payload: &[2],
                    }),
                ),
            ],
        );
        let ctx = MigrationCtx {
            current_version: 2,
            steps: STEPS,
        };
        let mut c =
            SegmentCursor::open(&path, false, None, None, Some(ctx), Liveness::Frozen).unwrap();
        let got: Vec<RawEntry> = std::iter::from_fn(|| c.next_entry()).collect();
        assert_eq!(got.len(), 1, "the surviving record is read");
        let damaged = c.damaged();
        assert_eq!(damaged.len(), 1, "one damage entry: {damaged:?}");
        assert!(
            damaged[0].reason.contains("migration step 1 → 2") && damaged[0].reason.contains(": 1"),
            "the culprit and the count are named: {}",
            damaged[0].reason
        );
    }

    #[test]
    fn the_block_index_works_in_both_directions_and_on_both_bounds() {
        // A query's default order is `Order::Newest`, while skipping blocks by
        // the footer worked only on the lower bound and only in a forward walk.
        // That is, the index the footer exists for did not work at all in the
        // commonest scenario: "the last hundred records" read the whole
        // segment, block by block.
        let dir = tempfile::tempdir().unwrap();
        let times: Vec<u64> = (0..20).map(|i| 100 + i * 10).collect();
        let path = sealed_segment(dir.path(), &times);

        let read = |reverse: bool, from: Option<u64>, to: Option<u64>| -> Vec<u64> {
            let mut c =
                SegmentCursor::open(&path, reverse, None, None, None, Liveness::Frozen).unwrap();
            assert_eq!(
                c.offsets.len(),
                times.len(),
                "otherwise the test is not about selection"
            );
            c.clip_to_window(from.map(Micros), to.map(Micros));
            std::iter::from_fn(|| c.next_entry())
                .map(|e| e.at.at.0)
                .collect()
        };

        // The window [150, 200] is six blocks out of twenty, and not one extra.
        // The cursor does not filter records by time: everything it handed out
        // is what it read from disk.
        assert_eq!(
            read(false, Some(150), Some(200)),
            vec![150, 160, 170, 180, 190, 200]
        );
        assert_eq!(
            read(true, Some(150), Some(200)),
            vec![200, 190, 180, 170, 160, 150]
        );

        // One bound out of two is a bound too.
        assert_eq!(read(true, None, Some(120)), vec![120, 110, 100]);
        assert_eq!(read(true, Some(270), None), vec![290, 280, 270]);

        // A window inside one block leaves exactly that block: the base is the
        // time of the block's FIRST record, and what lies further inside it
        // cannot be known without reading. Discarding it would lose records
        // rather than save a read.
        assert_eq!(read(true, Some(151), Some(159)), vec![150]);
        assert_eq!(read(false, Some(1_000), None), vec![290]);
        // But a window ending before the first block leaves nothing.
        assert!(read(true, None, Some(50)).is_empty());
        // With no bounds the walk is complete.
        assert_eq!(read(true, None, None).len(), times.len());
    }

    #[test]
    fn empty_and_single() {
        assert!(select(&[], &within(0, Some(5), Some(9)), None).is_empty());
        let one = [seg(0, 100)];
        // A single segment is never discarded: there is nothing after it, and
        // its upper bound is unknown without reading.
        assert_eq!(
            bases(&select(&one, &within(0, Some(u64::MAX), None), None)),
            vec![100]
        );
    }
}
