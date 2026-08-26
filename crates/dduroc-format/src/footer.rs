//! The footer of a sealed segment: the block index and the sets of types seen.
//!
//! ```text
//! [block index] [event_ids] [metric_ids] [Trailer 32B]
//! ```
//!
//! The footer is an **optimization, not a necessity**: it makes it possible to
//! find a block by time without reading any bodies, and to learn which types a
//! segment holds without scanning it. If the footer is damaged or missing (the
//! segment is active, or power was lost during the seal), the reader degrades
//! to a sequential walk of the block headers — no data is lost.
//!
//! The type sets are what migrations need: a segment that holds none of the
//! affected `event_id`/`metric_id` values needs no rewriting, which saves flash
//! wear. The metric set also answers the question "what telemetry is in this
//! segment" — the very question the series table existed for while a series was
//! identified by the pair `(metric, runtime tags)`. There are no tags any more,
//! a series is identified by its metric, and a set of identifiers suffices.
//!
//! Sealedness is marked by a signature in the file's last four bytes.

use crate::block::BlockHeader;
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::ids::{EventId, MetricId, Micros};
use crate::segment::SegmentHeader;
use crate::varint;

/// The signature in the last 4 bytes of a sealed segment.
pub const FOOTER_MAGIC: [u8; 4] = *b"DFTR";

/// An entry of the block index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockIndexEntry {
    /// The block's offset from the start of the file.
    pub offset: u64,
    /// Time of the block's first record.
    pub base: Micros,
    pub count: u16,
}

/// The fixed-size trailing block of the footer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trailer {
    /// Length of the footer sections before the trailer.
    pub sections_len: u32,
    pub block_count: u32,
    /// Time of the segment's first record.
    pub min: Micros,
    /// Time of the segment's last record.
    pub max: Micros,
    pub crc: u32,
}

impl Trailer {
    pub const SIZE: usize = 32;

    /// Parse the trailer from the file's last [`Trailer::SIZE`] bytes.
    /// `Ok(None)` means the segment is not sealed (no signature).
    pub fn parse(last_bytes: &[u8]) -> Result<Option<Self>> {
        let raw: &[u8; Self::SIZE] = last_bytes
            .get(last_bytes.len().wrapping_sub(Self::SIZE)..)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::Truncated)?;

        let magic: [u8; 4] = raw[28..32].try_into().expect("a 4-byte slice");
        if magic != FOOTER_MAGIC {
            return Ok(None);
        }

        Ok(Some(Self {
            sections_len: u32::from_le_bytes(raw[0..4].try_into().expect("a 4-byte slice")),
            block_count: u32::from_le_bytes(raw[4..8].try_into().expect("a 4-byte slice")),
            min: Micros(u64::from_le_bytes(
                raw[8..16].try_into().expect("an 8-byte slice"),
            )),
            max: Micros(u64::from_le_bytes(
                raw[16..24].try_into().expect("an 8-byte slice"),
            )),
            crc: u32::from_le_bytes(raw[24..28].try_into().expect("a 4-byte slice")),
        }))
    }

    /// The footer's full size on disk (sections plus trailer).
    pub fn total_len(&self) -> u64 {
        u64::from(self.sections_len) + Self::SIZE as u64
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.sections_len.to_le_bytes());
        out.extend_from_slice(&self.block_count.to_le_bytes());
        out.extend_from_slice(&self.min.0.to_le_bytes());
        out.extend_from_slice(&self.max.0.to_le_bytes());
        out.extend_from_slice(&self.crc.to_le_bytes());
        out.extend_from_slice(&FOOTER_MAGIC);
    }
}

/// A parsed footer.
#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub blocks: Vec<BlockIndexEntry>,
    /// Message types occurring in the segment (ascending order).
    pub events: Vec<EventId>,
    /// Metrics occurring in the segment (ascending order).
    ///
    /// This answers both the migration's question "is this segment affected"
    /// and the reader's "what telemetry is in here": a series is identified by
    /// its metric, so listing the metrics is listing the series.
    pub metrics: Vec<MetricId>,
    pub min: Micros,
    pub max: Micros,
}

impl Footer {
    /// Parse a footer from the tail of a file. `bytes` must **end** at the
    /// file's last byte and contain the whole footer (its length is known from
    /// [`Trailer::parse`]).
    pub fn parse(bytes: &[u8]) -> Result<Option<Self>> {
        let Some(trailer) = Trailer::parse(bytes)? else {
            return Ok(None);
        };

        let total = trailer.total_len();
        let start = (bytes.len() as u64)
            .checked_sub(total)
            .ok_or(Error::Truncated)?;
        let start = usize::try_from(start).map_err(|_| Error::Truncated)?;
        let sections = &bytes[start..bytes.len() - Trailer::SIZE];

        let crc = {
            let trailer_start = bytes.len() - Trailer::SIZE;
            let c = crc32c::crc32c(sections);
            crc32c::crc32c_append(c, &bytes[trailer_start..trailer_start + 24])
        };
        if crc != trailer.crc {
            return Err(Error::CrcMismatch {
                expected: trailer.crc,
                actual: crc,
            });
        }

        let mut c = Cursor::new(sections);

        // The block index: offset deltas from the end of the segment header and
        // time deltas from the previous block.
        //
        // Capacity is NEVER allocated from a counter taken out of a file:
        // CRC32C is not a signature, anyone can recompute it, and `block_count`
        // in the trailer would drive the size of the allocation directly. The
        // ceiling is how many entries physically fit in the sections (at least
        // 3 bytes each).
        let mut blocks = Vec::with_capacity(bounded(trailer.block_count, sections.len(), 3));
        let mut offset = SegmentHeader::SIZE as u64;
        let mut base = 0u64;
        for _ in 0..trailer.block_count {
            offset = offset.checked_add(c.varint()?).ok_or(Error::Truncated)?;
            base = base.checked_add(c.varint()?).ok_or(Error::Truncated)?;
            let count = c.varint_u16("records per block")?;
            blocks.push(BlockIndexEntry {
                offset,
                base: Micros(base),
                count,
            });
        }

        let events = read_id_set(&mut c, "event_id", sections.len())?
            .into_iter()
            .map(|v| EventId(v as u16))
            .collect();
        let metrics = read_id_set(&mut c, "metric_id", sections.len())?
            .into_iter()
            .map(|v| MetricId(v as u16))
            .collect();

        // The sections must parse with nothing left over: the length in the
        // trailer is covered by the CRC, so surplus bytes mean not slack but
        // content that does not match the declared structure.
        if c.pos() != sections.len() {
            return Err(Error::LimitExceeded {
                what: "footer sections",
                value: c.pos() as u64,
                max: sections.len() as u64,
            });
        }

        Ok(Some(Self {
            blocks,
            events,
            metrics,
            min: trailer.min,
            max: trailer.max,
        }))
    }

    /// Index of the block that may hold records with time `at`: the **first**
    /// of the blocks whose base does not exceed `at`. `None` means `at` is
    /// earlier than the first block.
    ///
    /// Binary search over duplicates returns an arbitrary match, so a partition
    /// point is used instead: blocks sharing a base are ordinary (a burst of
    /// records within one microsecond), and starting in the middle of such a
    /// group would lose its beginning.
    pub fn block_for_time(&self, at: Micros) -> Option<usize> {
        let first_after = self.blocks.partition_point(|b| b.base <= at);
        if first_after == 0 {
            return None;
        }
        // Step back to the start of the group of blocks sharing that base.
        let base = self.blocks[first_after - 1].base;
        let start = self.blocks[..first_after].partition_point(|b| b.base < base);
        Some(start)
    }

    /// Whether the segment intersects the given types — the migration's
    /// criterion: no intersection means the segment need not be rewritten.
    pub fn touches(&self, events: &[EventId], metrics: &[MetricId]) -> bool {
        events.iter().any(|e| self.events.binary_search(e).is_ok())
            || metrics
                .iter()
                .any(|m| self.metrics.binary_search(m).is_ok())
    }
}

/// A safe starting capacity: no more than would physically fit in
/// `available` bytes at `min_entry` bytes per element.
///
/// Parsing will run into the end of the sections and return an error anyway,
/// but allocating gigabytes from a counter in an untrusted file is not an
/// option: on armv7 that is a "capacity overflow" panic, on a 64-bit viewer
/// the OOM killer.
fn bounded(count: u32, available: usize, min_entry: usize) -> usize {
    (count as usize)
        .min(available / min_entry.max(1))
        .min(PREALLOC_MAX)
}

/// The ceiling on preallocation driven by a counter from a file.
///
/// [`bounded`] alone is not enough: it counts how many elements would
/// physically fit in the sections, but they fit *on disk*, and in memory an
/// element is larger — the block index by three (three varints against
/// twenty-four bytes), an id set by eight (a byte against a `u64`). Without
/// the ceiling, a sixty-megabyte section would demand half a gigabyte, and a
/// file with a valid CRC (and CRC32C is not a signature — anyone can
/// recompute it) would lay the viewer out under the OOM killer.
///
/// The ceiling breaks the link between a number in a file and the size of an
/// allocation. The vector grows on its own if the data really is that large:
/// no parse fails because of it, and the extra reallocations are invisible
/// next to reading the file.
const PREALLOC_MAX: usize = 4096;

fn read_id_set(c: &mut Cursor<'_>, what: &'static str, available: usize) -> Result<Vec<u64>> {
    let n = c.varint_u32(what)?;
    let mut out = Vec::with_capacity(bounded(n, available, 1));
    let mut prev = 0u64;
    for i in 0..n {
        let delta = c.varint()?;
        // The first element is an absolute value, the rest are strictly
        // increasing deltas: a zero delta would mean a duplicate in the set.
        if i > 0 && delta == 0 {
            return Err(Error::ReservedValue);
        }
        prev = prev.checked_add(delta).ok_or(Error::Truncated)?;
        if prev > u64::from(u16::MAX) {
            return Err(Error::LimitExceeded {
                what,
                value: prev,
                max: u64::from(u16::MAX),
            });
        }
        out.push(prev);
    }
    Ok(out)
}

// ════════════════════════════════════════════════════════════════════════════
// Assembly
// ════════════════════════════════════════════════════════════════════════════

/// A sorted set of identifiers on a flat vector.
///
/// Not a `BTreeSet`, because `insert` is called **on every record**: a
/// message brings its type, a sample its metric. A schema has hundreds of
/// types, so the set is tiny, and at that size contiguous memory with a
/// binary search is noticeably faster than a tree with its indirections.
/// Plus a cheap latch on the last insert: consecutive records of one type are
/// ordinary.
#[derive(Debug, Default)]
struct IdSet {
    ids: Vec<u16>,
    last: Option<u16>,
}

impl IdSet {
    #[inline]
    fn insert(&mut self, id: u16) {
        if self.last == Some(id) {
            return;
        }
        self.last = Some(id);
        if let Err(pos) = self.ids.binary_search(&id) {
            self.ids.insert(pos, id);
        }
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.last = None;
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.ids.iter().copied()
    }
}

/// The footer accumulator: the engine feeds it as blocks are written and
/// gets the finished bytes back at seal time.
#[derive(Debug, Default)]
pub struct FooterBuilder {
    blocks: Vec<BlockIndexEntry>,
    events: IdSet,
    metrics: IdSet,
    /// Types seen in a block that has **not been written yet**.
    ///
    /// A separate stage, because a block need not land in the segment it was
    /// assembled over: a block that does not fit moves to a fresh one (see the
    /// write rules in SPEC §5). Without the stage its types would end up in the
    /// footer of a segment that does **not** hold the block, and be missing
    /// from the one that does — and these sets are what a migration uses to
    /// decide whether to rewrite a segment and what a reader uses to judge
    /// whether the telemetry it wants is there. Both answers would be wrong.
    pending_events: IdSet,
    pending_metrics: IdSet,
    min: Option<Micros>,
    max: Micros,
}

impl FooterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a written block.
    ///
    /// The index has to stay non-decreasing in time: it is binary-searched, and
    /// `build` encodes deltas. A block whose base precedes the previous one
    /// (records reordered between threads) is not discarded — its base is
    /// pulled up to the previous one so the index stays sorted, while the
    /// actual minimum is accounted for separately in `min`. The accumulated
    /// block's types move into the segment's sets right here: the block has
    /// been written, so its types are in this segment, and in exactly this one.
    pub fn add_block(&mut self, offset: u64, header: &BlockHeader, last: Micros) {
        for id in self.pending_events.iter() {
            self.events.insert(id);
        }
        for id in self.pending_metrics.iter() {
            self.metrics.insert(id);
        }
        self.discard_pending();

        let prev = self.blocks.last().map_or(0, |b| b.base.0);
        self.blocks.push(BlockIndexEntry {
            offset,
            base: Micros(header.base.0.max(prev)),
            count: header.count,
        });
        // The minimum and the maximum come from the actual values rather than
        // from the first and last block: otherwise selecting segments by time
        // range would silently throw away a segment that holds the records
        // sought.
        self.min = Some(match self.min {
            Some(m) => Micros(m.0.min(header.base.0)),
            None => header.base,
        });
        self.max = Micros(self.max.0.max(last.0).max(header.base.0));
    }

    /// Note a message type that was seen. Called on every record.
    ///
    /// It lands in the stage of the unclosed block and moves into the segment's
    /// set in [`Self::add_block`] — that is, once it is known which segment the
    /// block landed in.
    #[inline]
    pub fn add_event(&mut self, id: EventId) {
        self.pending_events.insert(id.0);
    }

    /// Note a metric that was seen. Called on every sample.
    ///
    /// The set answers both the migration ("is this segment affected") and the
    /// reader ("what telemetry is in here"). It must not be empty in any
    /// segment that holds telemetry — otherwise a migration would silently skip
    /// history.
    #[inline]
    pub fn add_metric(&mut self, id: MetricId) {
        self.pending_metrics.insert(id.0);
    }

    /// Forget the unclosed block's types: the block is discarded and will land
    /// nowhere.
    pub fn discard_pending(&mut self) {
        self.pending_events.clear();
        self.pending_metrics.clear();
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Assemble the footer bytes (sections plus trailer).
    pub fn build(&self) -> Vec<u8> {
        let mut sections = Vec::new();

        let mut prev_offset = SegmentHeader::SIZE as u64;
        let mut prev_base = 0u64;
        for b in &self.blocks {
            varint::write_u64(&mut sections, b.offset.saturating_sub(prev_offset));
            varint::write_u64(&mut sections, b.base.0.saturating_sub(prev_base));
            varint::write_u64(&mut sections, u64::from(b.count));
            prev_offset = b.offset;
            prev_base = b.base.0;
        }

        write_id_set(&mut sections, &self.events);
        write_id_set(&mut sections, &self.metrics);

        let mut trailer = Trailer {
            sections_len: sections.len() as u32,
            block_count: self.blocks.len() as u32,
            min: self.min.unwrap_or(Micros(0)),
            max: self.max,
            crc: 0,
        };

        // The CRC covers the sections and the trailer's first 24 bytes — that
        // is, the whole footer except the CRC field itself and the signature.
        let mut trailer_bytes = Vec::with_capacity(Trailer::SIZE);
        trailer.write(&mut trailer_bytes);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&sections), &trailer_bytes[..24]);
        trailer.crc = crc;

        let mut out = sections;
        trailer.write(&mut out);
        out
    }

    /// Begin the footer of a new segment.
    ///
    /// The unclosed block's stage is **kept**: the segment is being changed
    /// precisely because the block did not fit the previous one, and its types
    /// have to travel with it. A block that is not to be written at all is
    /// cleared by a separate [`Self::discard_pending`].
    pub fn reset(&mut self) {
        self.blocks.clear();
        self.events.clear();
        self.metrics.clear();
        self.min = None;
        self.max = Micros(0);
    }

    /// Give back the index's slack, keeping what has accumulated.
    ///
    /// The block index grows by one entry per flush and keeps its peak capacity
    /// after `reset`: a critical channel with 8 KiB blocks and a 256 MiB
    /// segment accumulates up to ~770 KiB per channel — forever. The live
    /// entries of an open segment are needed right up to the seal, so this is
    /// `shrink_to_fit` rather than a reset. The engine calls it when a channel
    /// goes idle.
    pub fn shrink_to_fit(&mut self) {
        self.blocks.shrink_to_fit();
        self.events.ids.shrink_to_fit();
        self.metrics.ids.shrink_to_fit();
        self.pending_events.ids.shrink_to_fit();
        self.pending_metrics.ids.shrink_to_fit();
    }
}

fn write_id_set(out: &mut Vec<u8>, set: &IdSet) {
    varint::write_u64(out, set.len() as u64);
    let mut prev = 0u64;
    for id in set.iter() {
        varint::write_u64(out, u64::from(id) - prev);
        prev = u64::from(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Compression;

    fn header(base: u64, count: u16) -> BlockHeader {
        BlockHeader {
            body_len: 10,
            raw_len: 10,
            seq: 0,
            base: Micros(base),
            count,
            compression: Compression::None,
            crc: 0,
        }
    }

    /// The same order the writer uses: types arrive with the records of the
    /// block being assembled and are pinned to a segment once the block lands.
    fn sample_builder() -> FooterBuilder {
        let mut b = FooterBuilder::new();
        b.add_event(EventId(1));
        b.add_metric(MetricId(5));
        b.add_block(32, &header(1_000, 5), Micros(1_900));

        b.add_event(EventId(300));
        b.add_event(EventId(1)); // duplicates collapse
        b.add_metric(MetricId(6));
        b.add_metric(MetricId(5)); // duplicates collapse
        b.add_block(200, &header(2_000, 7), Micros(2_500));

        b.add_block(512, &header(9_000, 3), Micros(9_100));
        b
    }

    #[test]
    fn types_of_a_relocated_block_follow_it_to_the_new_segment() {
        // A block that did not fit its segment moves to a fresh one (SPEC §5).
        // Its types have to move with it: a migration uses these sets to decide
        // whether to rewrite a segment, and a reader to decide whether the
        // telemetry it wants is there. Leave them in the old footer and both
        // answers go wrong: the old segment declares types it does not hold,
        // the new one stays silent about types it does, and the migration walks
        // past exactly the history it was written for.
        let mut b = FooterBuilder::new();

        // The first block landed in the current segment.
        b.add_event(EventId(1));
        b.add_block(32, &header(1_000, 5), Micros(1_100));

        // The second is assembled but did not fit: its types are already noted.
        b.add_event(EventId(7));
        b.add_metric(MetricId(3));

        // The segment is sealed WITHOUT it.
        let sealed = Footer::parse(&b.build()).unwrap().expect("sealed");
        assert_eq!(
            sealed.events,
            vec![EventId(1)],
            "the old segment holds only the types of the blocks that are in it"
        );
        assert!(sealed.metrics.is_empty());

        // A new segment, the same accumulator — and the travelling block lands
        // here.
        b.reset();
        b.add_block(32, &header(2_000, 9), Micros(2_100));
        let fresh = Footer::parse(&b.build()).unwrap().expect("sealed");
        assert_eq!(
            fresh.events,
            vec![EventId(7)],
            "the types travelled with the block"
        );
        assert_eq!(fresh.metrics, vec![MetricId(3)]);
        assert_eq!(fresh.blocks.len(), 1);
    }

    #[test]
    fn a_discarded_block_leaves_its_types_nowhere() {
        // A block larger than a whole segment is discarded, and its types are
        // in no segment at all — declaring them would send a migration to
        // rewrite a segment for records that are not there.
        let mut b = FooterBuilder::new();
        b.add_event(EventId(1));
        b.add_block(32, &header(1_000, 5), Micros(1_100));

        b.add_event(EventId(7));
        b.add_metric(MetricId(3));
        b.discard_pending();

        b.add_event(EventId(2));
        b.add_block(200, &header(2_000, 5), Micros(2_100));

        let footer = Footer::parse(&b.build()).unwrap().expect("sealed");
        assert_eq!(footer.events, vec![EventId(1), EventId(2)]);
        assert!(
            footer.metrics.is_empty(),
            "the metric belonged only to the discarded block"
        );
    }

    #[test]
    fn footer_larger_than_the_prealloc_cap_still_parses() {
        // Preallocation is bounded by the ceiling so that a counter from an
        // untrusted file cannot drive the size of an allocation. The ceiling
        // has to be a hint only: a real 256 MiB segment with 64 KiB blocks
        // gives four thousand index entries, and parsing such a footer has to
        // run to the end, growing the vector on its own.
        let n = PREALLOC_MAX * 2 + 7;
        let mut b = FooterBuilder::new();
        // The id sets exceed the ceiling too: the u16 space allows it.
        for id in 1..=(PREALLOC_MAX as u16 + 500) {
            b.add_event(EventId(id));
        }
        for i in 0..n {
            b.add_block(
                32 + i as u64 * 64,
                &header(i as u64 * 1_000, 4),
                Micros(i as u64 * 1_000 + 900),
            );
        }
        let bytes = b.build();
        let footer = Footer::parse(&bytes)
            .unwrap()
            .expect("the segment is sealed");
        assert_eq!(footer.blocks.len(), n);
        assert_eq!(footer.blocks[n - 1].offset, 32 + (n as u64 - 1) * 64);
        assert_eq!(footer.events.len(), PREALLOC_MAX + 500);
        assert_eq!(footer.min, Micros(0));
        assert_eq!(footer.max, Micros((n as u64 - 1) * 1_000 + 900));
    }

    #[test]
    fn a_forged_count_cannot_drive_the_allocation() {
        // What the ceiling exists for: `block_count` sits in the trailer, and
        // the trailer is protected only by CRC32C — not a signature; anyone can
        // recompute it. A number from there has no right to drive the size of
        // an allocation: on armv7 that is a "capacity overflow" panic, on a
        // 64-bit viewer the OOM killer.
        //
        // Assemble a plausible footer, swap the block counter for a knowably
        // impossible one and recompute the CRC — exactly what a planted dump
        // would do. Parsing has to refuse for want of bytes rather than try to
        // allocate what was declared.
        let bytes = sample_builder().build();
        let mut forged = bytes.clone();
        let trailer_at = forged.len() - Trailer::SIZE;
        forged[trailer_at + 4..trailer_at + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        let crc = {
            let sections = &forged[..trailer_at];
            crc32c::crc32c_append(
                crc32c::crc32c(sections),
                &forged[trailer_at..trailer_at + 24],
            )
        };
        forged[trailer_at + 24..trailer_at + 28].copy_from_slice(&crc.to_le_bytes());

        // The CRC agrees, so parsing did reach the records and stopped on
        // running out of section bytes rather than on an integrity check.
        assert!(
            matches!(Footer::parse(&forged), Err(Error::Truncated)),
            "a counter from a file must run into the real bytes"
        );
        assert!(
            bounded(u32::MAX, forged.len(), 3) <= PREALLOC_MAX,
            "and has no right to ask for more than the ceiling"
        );
    }

    #[test]
    fn roundtrip() {
        let bytes = sample_builder().build();
        let footer = Footer::parse(&bytes)
            .unwrap()
            .expect("the segment is sealed");

        assert_eq!(footer.blocks.len(), 3);
        assert_eq!(footer.blocks[0].offset, 32);
        assert_eq!(footer.blocks[1].offset, 200);
        assert_eq!(footer.blocks[2].offset, 512);
        assert_eq!(footer.blocks[2].base, Micros(9_000));
        assert_eq!(footer.blocks[1].count, 7);

        assert_eq!(footer.events, vec![EventId(1), EventId(300)]);
        assert_eq!(footer.metrics, vec![MetricId(5), MetricId(6)]);
        assert_eq!(footer.min, Micros(1_000));
        assert_eq!(footer.max, Micros(9_100));
    }

    #[test]
    fn metric_set_answers_what_telemetry_is_here() {
        // This is the question the series table existed for while a series was
        // identified by the pair "metric plus runtime tags". There are no tags,
        // a series is identified by its metric, and a set of identifiers
        // suffices — and it was already in the footer for the migrations' sake.
        let bytes = sample_builder().build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert_eq!(f.metrics, vec![MetricId(5), MetricId(6)]);
        assert!(f.metrics.binary_search(&MetricId(5)).is_ok());
        assert!(
            f.metrics.binary_search(&MetricId(7)).is_err(),
            "what is not in the segment is not in the set either"
        );
    }

    #[test]
    fn trailer_reports_size_for_two_phase_read() {
        // A reader takes 32 bytes first, learns the length, then reads the
        // rest.
        let bytes = sample_builder().build();
        let trailer = Trailer::parse(&bytes).unwrap().unwrap();
        assert_eq!(trailer.total_len(), bytes.len() as u64);
        assert_eq!(trailer.block_count, 3);
    }

    #[test]
    fn unsealed_segment_has_no_footer() {
        let data = vec![0xAB; 100];
        assert_eq!(Trailer::parse(&data).unwrap(), None);
        assert_eq!(Footer::parse(&data).unwrap(), None);
    }

    #[test]
    fn corrupt_footer_detected() {
        let mut bytes = sample_builder().build();
        bytes[0] ^= 0xFF;
        assert!(matches!(
            Footer::parse(&bytes),
            Err(Error::CrcMismatch { .. })
        ));
    }

    #[test]
    fn block_lookup_by_time() {
        let bytes = sample_builder().build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert_eq!(
            f.block_for_time(Micros(0)),
            None,
            "earlier than the first block"
        );
        assert_eq!(f.block_for_time(Micros(1_000)), Some(0), "an exact match");
        assert_eq!(f.block_for_time(Micros(1_500)), Some(0), "inside the first");
        assert_eq!(f.block_for_time(Micros(2_000)), Some(1));
        assert_eq!(f.block_for_time(Micros(8_999)), Some(1));
        assert_eq!(f.block_for_time(Micros(9_000)), Some(2));
        assert_eq!(
            f.block_for_time(Micros(u64::MAX)),
            Some(2),
            "after the last"
        );
    }

    #[test]
    fn lookup_returns_start_of_equal_base_group() {
        // Blocks sharing a base are ordinary: a burst of records within one
        // microsecond. Binary search over duplicates returns an arbitrary
        // match, and starting in the middle of a group would lose its
        // beginning.
        let mut b = FooterBuilder::new();
        for (i, base) in [100u64, 500, 500, 500, 900].into_iter().enumerate() {
            b.add_block(32 + i as u64 * 64, &header(base, 1), Micros(base + 10));
        }
        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();

        assert_eq!(
            f.block_for_time(Micros(500)),
            Some(1),
            "the start of the group"
        );
        assert_eq!(f.block_for_time(Micros(700)), Some(1), "inside the group");
        assert_eq!(f.block_for_time(Micros(900)), Some(4));
        assert_eq!(f.block_for_time(Micros(50)), None);
    }

    #[test]
    fn migration_can_skip_untouched_segments() {
        let bytes = sample_builder().build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert!(
            f.touches(&[EventId(300)], &[]),
            "the type is in the segment"
        );
        assert!(f.touches(&[], &[MetricId(6)]));
        assert!(
            !f.touches(&[EventId(2), EventId(299)], &[MetricId(7)]),
            "none of the affected types are here, so the segment is not rewritten"
        );
    }

    /// Assemble a footer with an arbitrary trailer and a correct CRC — an
    /// imitation of a file prepared maliciously or damaged.
    fn forge(sections: Vec<u8>, mut trailer: Trailer) -> Vec<u8> {
        trailer.sections_len = sections.len() as u32;
        let mut tb = Vec::new();
        trailer.write(&mut tb);
        trailer.crc = crc32c::crc32c_append(crc32c::crc32c(&sections), &tb[..24]);
        let mut bytes = sections;
        trailer.write(&mut bytes);
        bytes
    }

    #[test]
    fn absurd_counts_do_not_allocate() {
        // CRC32C is not a signature: anyone can recompute it after editing the
        // counters. Parsing has to hold, not allocate gigabytes from a number
        // in a file (on armv7 that is a "capacity overflow" panic, on a 64-bit
        // viewer the OOM killer).
        let bytes = forge(
            Vec::new(),
            Trailer {
                sections_len: 0,
                block_count: u32::MAX,
                min: Micros(0),
                max: Micros(0),
                crc: 0,
            },
        );
        let err = Footer::parse(&bytes).unwrap_err();
        assert!(err.is_torn_tail(), "expected a parse error, got {err}");

        // The same for the identifier sets. The block count is taken from the
        // trailer, so the sections begin straight away with the event set.
        let empty_trailer = Trailer {
            sections_len: 0,
            block_count: 0,
            min: Micros(0),
            max: Micros(0),
            crc: 0,
        };

        let mut sections = Vec::new();
        varint::write_u64(&mut sections, u64::from(u32::MAX)); // "events" — four billion
        assert!(Footer::parse(&forge(sections, empty_trailer)).is_err());

        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 0); // no events
        varint::write_u64(&mut sections, u64::from(u32::MAX)); // "metrics" — four billion
        assert!(Footer::parse(&forge(sections, empty_trailer)).is_err());
    }

    #[test]
    fn trailing_garbage_in_footer_rejected() {
        // Surplus bytes after the parsed sections mean the footer is not what
        // it appears to be: the section length from the trailer is covered by
        // the CRC, so a discrepancy is a sign of forgery or damage, not of
        // slack.
        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 0); // no events
        varint::write_u64(&mut sections, 0); // no metrics
        sections.extend_from_slice(&[0xAA, 0xBB, 0xCC]);

        let bytes = forge(
            sections,
            Trailer {
                sections_len: 0,
                block_count: 0,
                min: Micros(0),
                max: Micros(0),
                crc: 0,
            },
        );
        assert!(
            Footer::parse(&bytes).is_err(),
            "a tail in the footer sections must be rejected"
        );
    }

    #[test]
    fn index_stays_sorted_and_bounds_are_exact() {
        // Records may reach the writer reordered between threads: the index has
        // to stay non-decreasing (it is binary-searched), and min/max have to
        // reflect the actual bounds, or selecting segments by range would
        // silently throw away a segment holding the records sought.
        let mut b = FooterBuilder::new();
        b.add_block(32, &header(5_000, 1), Micros(5_100));
        b.add_block(100, &header(1_000, 1), Micros(1_100)); // "from the past"
        b.add_block(200, &header(9_000, 1), Micros(9_100));

        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();

        let bases: Vec<u64> = f.blocks.iter().map(|e| e.base.0).collect();
        assert!(
            bases.windows(2).all(|w| w[0] <= w[1]),
            "the index must be non-decreasing: {bases:?}"
        );
        assert_eq!(f.min, Micros(1_000), "min is the actual minimum");
        assert_eq!(f.max, Micros(9_100));
        // The offsets are not distorted by pulling the time up.
        assert_eq!(
            f.blocks.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![32, 100, 200]
        );
    }

    #[test]
    fn empty_footer_roundtrips() {
        let b = FooterBuilder::new();
        assert!(b.is_empty());
        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert!(f.blocks.is_empty());
        assert!(f.events.is_empty());
        assert!(f.metrics.is_empty());
    }

    #[test]
    fn duplicate_ids_in_set_rejected() {
        // The block count is taken from the trailer (0), so the sections begin
        // straight away with the event set. It is assembled by hand with a zero
        // delta after the first element — that is a duplicate, and it has to be
        // rejected.
        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 2); // 2 events
        varint::write_u64(&mut sections, 5); // the first is an absolute value
        varint::write_u64(&mut sections, 0); // a duplicate
        varint::write_u64(&mut sections, 0); // no metrics

        let mut trailer = Trailer {
            sections_len: sections.len() as u32,
            block_count: 0,
            min: Micros(0),
            max: Micros(0),
            crc: 0,
        };
        let mut tb = Vec::new();
        trailer.write(&mut tb);
        trailer.crc = crc32c::crc32c_append(crc32c::crc32c(&sections), &tb[..24]);
        let mut bytes = sections;
        trailer.write(&mut bytes);

        assert_eq!(Footer::parse(&bytes), Err(Error::ReservedValue));
    }
}
