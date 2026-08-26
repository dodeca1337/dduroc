//! The segment file: creation, appending blocks, sealing, recovery.
//!
//! # Why space is reserved — and why as a window
//!
//! Space for a segment is reserved in advance (`fallocate`). That buys three
//! things:
//!
//! 1. **A cheap `fdatasync`**: appending a block does not change the file size,
//!    so it does not touch the inode's metadata — only the data has to be
//!    synced.
//! 2. **An early refusal for want of space**: ENOSPC arrives when space is
//!    reserved rather than in the middle of writing an event.
//! 3. **A scan terminator**: the unwritten tail is filled with zeros, and by
//!    the format a zero block header means "end of data" — recovery tells an
//!    unbroken end from corruption with no extra marks.
//!
//! Reserving the **whole** segment at once is not required by any of the three,
//! and the price of doing so is not paid by the store but by every channel of
//! the fleet. Twenty-four thousand namespaces at eight megabytes each is a
//! hundred and ninety-two gigabytes on a device's medium holding a few
//! kilobytes of writes, and as many unwritten extents for `fdatasync` to push
//! through on the very first block. Measured over two thousand channels: with
//! an eight-megabyte segment, closing takes 40 s and syncing 2.2 s; the same
//! fleet with a sixty-four-kilobyte segment takes 16 ms and 136 ms.
//!
//! So the reserve is a **window**: `FIRST_EXTENT` is taken at once, and the
//! window then grows by eighths of the limit until it reaches `segment_bytes`.
//! All three properties hold inside the window; one thing changed — ENOSPC now
//! arrives when the window is extended, not only when the file is created.
//! There are eight extensions to a whole segment, and each goes through the
//! same code as creation.
//!
//! # The order of operations across a power loss
//!
//! - *creation*: fallocate → write the header → fdatasync → fsync the
//!   directory. A cut before the directory fsync leaves no file; after it, a
//!   valid empty one.
//! - *extending the window*: fallocate to the new size. A cut at any step
//!   leaves the file either as it was or extended with zeros — a scan reads
//!   both as the end of data in the same place.
//! - *appending*: pwrite the block → (by policy) fdatasync. A cut in the middle
//!   of a block leaves a CRC that does not agree, and recovery trims the tail.
//! - *sealing*: ftruncate to the end of the data → append the footer →
//!   fdatasync. A cut at any step leaves the segment unsealed, that is,
//!   readable by an ordinary scan.

use crate::error::{Error, IoContext, Result};
use crate::fsutil;
use dduroc_format::block::BlockHeader;
use dduroc_format::footer::{FOOTER_MAGIC, Trailer};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{FooterBuilder, Micros, block};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// The ceiling on a footer size a reader agrees to read.
///
/// The length comes from the trailer, and the trailer is verified only by its
/// signature — the CRC covers the footer, but checking it takes reading
/// exactly as much as the trailer says. "It fits in the file" is not enough:
/// on a quarter-gigabyte segment that would permit a quarter-gigabyte read on
/// the strength of one number in the tail, and on armv7 such an allocation is
/// not an abstract threat.
///
/// Eight megabytes covers reality with room to spare: a block index entry
/// costs a handful of bytes, and even a segment of the maximum size packed
/// with minimal blocks does not reach it.
const MAX_FOOTER: u64 = 8 * 1024 * 1024;

/// How much a segment takes on the medium at creation — the first reserve
/// window.
///
/// It equals the default `block_max_bytes`: the fleet's commonest case — a
/// channel that wrote one block and went quiet — costs exactly one `fallocate`
/// and exactly one extent. The window then grows by eighths of the limit (see
/// [`SegmentWriter::reserve`]), reaching `segment_bytes` in eight extensions
/// whatever that limit happens to be.
const FIRST_EXTENT: u64 = 64 * 1024;

/// A segment open for writing.
#[derive(Debug)]
pub struct SegmentWriter {
    file: File,
    path: PathBuf,
    /// The offset the next block will land at.
    end: u64,
    /// How many bytes are reserved on the medium right now — the current
    /// window.
    capacity: u64,
    /// The growth limit: the rotation boundary beyond which a segment does not
    /// grow. It is what [`Self::fits`] answers by — the window is stretched to
    /// it as needed.
    limit: u64,
    header: SegmentHeader,
    /// The number of the next block: a gap in the numbering tells a lost block
    /// from corruption at read time.
    next_seq: u32,
    dirty: bool,
}

impl SegmentWriter {
    /// Create a new segment with a growth limit of `limit` bytes.
    ///
    /// On the medium it will take the first window (`FIRST_EXTENT`) — the limit
    /// is the rotation boundary, not the file's size.
    pub fn create(dir: &Path, header: SegmentHeader, limit: u64) -> Result<Self> {
        let name = SegmentName::new(header.boot, header.base);
        Self::create_at(&dir.join(name.to_string()), header, limit)
    }

    /// The same by an explicit path — for files whose name is not the
    /// segment's.
    ///
    /// The migration needs it: it assembles a new segment in a temporary file
    /// next to the old one and swaps it in with an atomic `rename` only after
    /// an `fdatasync`. Writing straight to the final name is impossible — it is
    /// taken by the original, which has to survive any interruption until the
    /// last moment.
    pub fn create_at(path: &Path, header: SegmentHeader, limit: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            // create_new: a segment of this name cannot already exist —
            // otherwise we would overwrite someone else's data under the same
            // (boot, time) key.
            .create_new(true)
            .mode(fsutil::FILE_MODE)
            .open(path)
            .ctx_path("creating a segment", path)?;

        // A floor shared by the limit and the window: a segment that cannot
        // hold even a header plus a zero terminator is not a segment.
        let floor = SegmentHeader::SIZE as u64 + BlockHeader::SIZE as u64;
        let limit = limit.max(floor);
        let capacity = FIRST_EXTENT.clamp(floor, limit);
        if let Err(e) = grow_to(&file, capacity, path) {
            // A file with no space is useless and gets in the way of the next
            // attempt.
            let _ = std::fs::remove_file(path);
            return Err(e);
        }

        file.write_all_at(&header.to_bytes(), 0)
            .ctx_path("writing the header", path)?;
        fsutil::sync_data(&file, path)?;
        fsutil::sync_dir(path.parent().unwrap_or(Path::new(".")))?;

        Ok(Self {
            file,
            path: path.to_owned(),
            end: SegmentHeader::SIZE as u64,
            capacity,
            limit,
            header,
            next_seq: 0,
            dirty: false,
        })
    }

    /// Open an existing segment to continue writing, restoring the position of
    /// the end of the data.
    ///
    /// After recovery the file is **truncated** to the end of the intact data.
    /// Without that, bytes of an earlier write would remain behind a new,
    /// shorter block, and the next scan would read them as a block. In the
    /// worst case a surviving old block would agree on its CRC, and records
    /// from an already discarded tail would come back into the log with times
    /// that break monotonicity.
    ///
    /// `limit` is the growth limit up to which the segment may be appended to.
    /// `None` means "the limit is unknown": the file's current size is taken
    /// for it, so the segment is opened only to be read to the end.
    ///
    /// Opening asks the medium for **no space**: the file stays truncated to
    /// the end of the data, and the first write restores the reserve window. A
    /// segment released for idleness gave its tail back precisely so as not to
    /// hold it — taking it back on waking would mean paying a whole segment's
    /// `fallocate` for a channel that may write another hundred bytes.
    pub fn reopen(path: &Path, expect_store: Option<u64>, limit: Option<u64>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .ctx_path("opening a segment", path)?;
        let on_disk = file.metadata().ctx_path("stat", path)?.len();
        let limit = limit.unwrap_or(on_disk).max(on_disk);

        let scan = Scan::run(&file, on_disk, path)?;
        scan.check_store(expect_store, path)?;

        file.set_len(scan.data_end)
            .ctx_path("truncating a damaged tail", path)?;
        fsutil::sync_data(&file, path)?;

        Ok(Self {
            file,
            path: path.to_owned(),
            end: scan.data_end,
            // The window is collapsed to the data: everything beyond it has
            // just been cut off. `reserve` will extend it on the first write.
            capacity: scan.data_end,
            limit,
            header: scan.header,
            next_seq: scan.next_seq,
            dirty: false,
        })
    }

    /// The number the next block will get.
    pub fn next_seq(&self) -> u32 {
        self.next_seq
    }

    pub fn header(&self) -> SegmentHeader {
        self.header
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The offset of the end of the data (= the size of the useful part).
    pub fn data_end(&self) -> u64 {
        self.end
    }

    /// How many bytes still fit before the growth limit.
    pub fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.end)
    }

    /// How many bytes the segment takes on the medium right now: the header,
    /// the data and the unwritten tail of the current reserve window.
    ///
    /// This is the quantity the segment is counted as in the class budget — not
    /// the growth limit: keeping space in the budget that is not occupied means
    /// evicting someone else's history for the sake of emptiness.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Whether there is unsynced data.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Whether a block of this size fits — by the growth limit, not by the
    /// window.
    ///
    /// [`Self::reserve`] will stretch the window to it; there is no reason to
    /// ask the caller about that — it is deciding a different question: whether
    /// it is time to rotate the segment.
    pub fn fits(&self, block_len: u64) -> bool {
        // Leave room for the zero terminator header: without it a scan would
        // run into the end of the file instead of an honest sign of the end of
        // the data.
        self.remaining() >= block_len + BlockHeader::SIZE as u64
    }

    /// Stretch the reserve window to fit a block of this size.
    ///
    /// Called from [`Self::append_block`], that is, on every write, and almost
    /// always returns on the first line: there are eight extensions to a whole
    /// segment.
    ///
    /// The step is an eighth of the **limit**, not of the current window. A
    /// fraction of the window would give forty extensions on the way from the
    /// first extent to eight megabytes, and doubling would ask for twice the
    /// space — exactly what the window exists to avoid. A fraction of the limit
    /// depends on neither: eight extensions for any `segment_bytes`.
    ///
    /// The step never raises the window above the limit — only a concrete need
    /// does: a migration's limit equals the size of the original, and only the
    /// chain of steps knows how much the records will swell. Asking for slack
    /// there "just in case" means taking space on a device that may not have
    /// it; growing by actual need means refusing exactly when the space really
    /// did run out. Sealing trims the surplus tail.
    pub fn reserve(&mut self, block_len: u64) -> Result<()> {
        let need = self
            .end
            .saturating_add(block_len)
            .saturating_add(BlockHeader::SIZE as u64);
        if need <= self.capacity {
            return Ok(());
        }
        let step = (self.limit / 8).max(FIRST_EXTENT.min(self.limit));
        let want = need.max(self.capacity.saturating_add(step).min(self.limit));
        grow_to(&self.file, want, &self.path)?;
        self.capacity = want;
        Ok(())
    }

    /// Append a finished block (header plus body). Returns its offset.
    ///
    /// The block has to be assembled with the number [`Self::next_seq`] gives.
    pub fn append_block(&mut self, bytes: &[u8]) -> Result<u64> {
        // The reserve happens here rather than in the caller: forgetting it
        // would mean writing past what is reserved, that is, getting ENOSPC in
        // the middle of a block — the very thing the reserve exists to prevent.
        self.reserve(bytes.len() as u64)?;
        let offset = self.end;
        self.file
            .write_all_at(bytes, offset)
            .ctx_path("writing a block", &self.path)?;
        self.end += bytes.len() as u64;
        self.next_seq = self.next_seq.saturating_add(1);
        self.dirty = true;
        Ok(offset)
    }

    /// `fdatasync`: once it returns, the data will survive a power loss.
    pub fn sync(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        fsutil::sync_data(&self.file, &self.path)?;
        self.dirty = false;
        Ok(())
    }

    /// Seal the segment: truncate to the end of the data and append the footer.
    ///
    /// The footer is a read optimization, so its absence is no loss: an
    /// interruption at any step leaves the segment valid but unsealed.
    pub fn seal(mut self, footer: &[u8]) -> Result<()> {
        self.sync()?;
        self.file
            .set_len(self.end)
            .ctx_path("truncating to the end of the data", &self.path)?;
        self.file
            .write_all_at(footer, self.end)
            .ctx_path("writing the footer", &self.path)?;
        fsutil::sync_data(&self.file, &self.path)?;
        Ok(())
    }

    /// Truncate to the end of the data without a footer — for an emergency
    /// close.
    pub fn close_unsealed(mut self) -> Result<()> {
        self.sync()?;
        self.file
            .set_len(self.end)
            .ctx_path("truncating to the end of the data", &self.path)
    }
}

/// Bring the file up to `capacity` bytes, reserving the space on the medium.
///
/// Reserving is not an optimization but the way to get ENOSPC when the window
/// is taken rather than in the middle of writing a critical event.
fn grow_to(file: &File, capacity: u64, path: &Path) -> Result<()> {
    #[cfg(test)]
    if fault::refuses(capacity) {
        return Err(Error::NoSpace(path.to_owned()));
    }
    match rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, capacity) {
        Ok(()) => Ok(()),
        // Some filesystems (tmpfs on older kernels, certain overlays) cannot do
        // fallocate. The space is not reserved, but the format does not suffer:
        // the size is reached with an ordinary ftruncate, and the tail is zero
        // too.
        Err(rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::NOSYS) => {
            file.set_len(capacity).ctx_path("ftruncate", path)
        }
        Err(rustix::io::Errno::NOSPC) => Err(Error::NoSpace(path.to_owned())),
        Err(e) => Err(e).ctx_path("fallocate", path),
    }
}

/// Refusal for want of space — for the engine's tests only.
///
/// ENOSPC is the only thing the reserve exists for, and it cannot be
/// reproduced on a real filesystem without being root: that takes a medium or
/// a mount of one's own. Leaving this path untested would mean testing
/// everything except what it was all built for — so here sits a stub at
/// exactly the point where the refusal really arrives.
///
/// The counter is thread-local: the writer lives on its own thread, and a test
/// driving its loop directly must not break writing in neighbouring tests.
#[cfg(test)]
pub(crate) mod fault {
    use std::cell::Cell;

    thread_local! {
        static NO_SPACE: Cell<u32> = const { Cell::new(0) };
        static CEILING: Cell<u64> = const { Cell::new(u64::MAX) };
    }

    /// The next `n` attempts to reserve space are refused.
    pub(crate) fn no_space_for(n: u32) {
        NO_SPACE.with(|c| c.set(n));
    }

    /// A medium with exactly `bytes` free: an attempt to reserve more is
    /// refused.
    ///
    /// Needed where what is checked is not "a refusal for want of space at all"
    /// but **how much** space an operation asks for: a migration run that takes
    /// twice what it needs will fall over on a real device and pass unnoticed
    /// on a developer's roomy machine.
    pub(crate) fn free_space(bytes: u64) {
        CEILING.with(|c| c.set(bytes));
    }

    /// Remove the ceiling set by [`free_space`].
    pub(crate) fn unlimited_space() {
        CEILING.with(|c| c.set(u64::MAX));
    }

    pub(crate) fn refuses(want: u64) -> bool {
        let over = CEILING.with(|c| want > c.get());
        let counted = NO_SPACE.with(|c| {
            let left = c.get();
            c.set(left.saturating_sub(1));
            left > 0
        });
        over || counted
    }
}

/// What ended the walk over blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanEnd {
    /// A zero header: the unwritten tail of the reserve window. The normal
    /// case.
    ZeroTail,
    /// The data ended exactly at the file boundary.
    FileEnd,
    /// The header or the CRC did not agree — a torn write from a power loss, or
    /// corruption of the medium.
    Corrupt,
    /// A block number that is not one greater than the previous: part of the
    /// data did not reach the medium although later parts did (writeback
    /// reordering).
    SeqGap { expected: u32, found: u32 },
}

impl ScanEnd {
    /// Whether this outcome calls for discarding the tail.
    pub fn is_damage(self) -> bool {
        matches!(self, ScanEnd::Corrupt | ScanEnd::SeqGap { .. })
    }
}

/// The result of recovering a segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scan {
    pub header: SegmentHeader,
    /// The offset of the end of the last intact block.
    pub data_end: u64,
    pub block_count: u32,
    /// The number the next block should get.
    pub next_seq: u32,
    /// The time of the last block's first record (a lower bound on the
    /// segment's maximum time: the exact value requires parsing the body).
    pub last_base: Micros,
    /// What ended the walk: the end of the data, corruption or a tear.
    pub stopped_by: ScanEnd,
}

impl Scan {
    /// The tail was damaged and discarded.
    pub fn truncated(&self) -> bool {
        self.stopped_by.is_damage()
    }

    /// Check that the segment belongs to this store.
    ///
    /// Files copied from another device have their own `boot_counter` numbering
    /// and their own anchoring to time: merging them with the local ones would
    /// give another device's events the local UTC anchor, that is, a knowably
    /// wrong absolute time.
    pub fn check_store(&self, expect: Option<u64>, path: &Path) -> Result<()> {
        match expect {
            Some(id) if self.header.store_id != id => Err(Error::ForeignSegment {
                path: path.to_owned(),
                expected: id,
                found: self.header.store_id,
            }),
            _ => Ok(()),
        }
    }
    /// Walk the segment by block headers, finding the end of the intact data.
    ///
    /// Blocks are read one at a time into a reusable buffer: a segment can be
    /// hundreds of megabytes, and armv7 is short of memory — the file must not
    /// be read whole.
    pub fn run(file: &File, file_len: u64, path: &Path) -> Result<Self> {
        Self::run_inner(file, file_len, path, None).map(|(scan, _)| scan)
    }

    /// The same, assembling the footer along the way.
    ///
    /// The walk reads every body and verifies its CRC anyway, so parsing the
    /// records for the `event_id`/`metric_id` sets and the block index costs
    /// little more than the walk itself — no second pass over the segment is
    /// needed.
    ///
    /// The second value says whether the footer assembled is **complete**. A
    /// body that fails to parse with an agreeing CRC stops the assembly but not
    /// the walk: the bytes passed the integrity check, so they are data, and
    /// trimming them because there is nothing to break them into records with
    /// (the codec is not built into this build) is not allowed. An incomplete
    /// footer has nowhere to go — with it a reader would take the segment to
    /// end sooner than it does.
    pub fn run_collecting(
        file: &File,
        file_len: u64,
        path: &Path,
        footer: &mut FooterBuilder,
    ) -> Result<(Self, bool)> {
        Self::run_inner(file, file_len, path, Some(footer))
    }

    fn run_inner(
        file: &File,
        file_len: u64,
        path: &Path,
        mut footer: Option<&mut FooterBuilder>,
    ) -> Result<(Self, bool)> {
        let mut footer_complete = true;
        let mut head = [0u8; SegmentHeader::SIZE];
        file.read_exact_at(&mut head, 0)
            .ctx_path("reading a segment header", path)?;
        let header = SegmentHeader::parse(&head).map_err(|e| Error::Corrupt {
            path: path.to_owned(),
            reason: format!("segment header: {e}"),
        })?;

        let mut offset = SegmentHeader::SIZE as u64;
        let mut block_count = 0u32;
        let mut last_base = header.base;
        let mut buf: Vec<u8> = Vec::new();
        let end;

        loop {
            if file_len.saturating_sub(offset) < BlockHeader::SIZE as u64 {
                end = ScanEnd::FileEnd;
                break;
            }
            let mut hdr = [0u8; BlockHeader::SIZE];
            if file.read_exact_at(&mut hdr, offset).is_err() {
                end = ScanEnd::Corrupt;
                break;
            }
            let parsed = match BlockHeader::parse(&hdr) {
                Ok(Some(h)) => h,
                // An entirely zero header is the unwritten tail.
                Ok(None) => {
                    end = ScanEnd::ZeroTail;
                    break;
                }
                Err(_) => {
                    end = ScanEnd::Corrupt;
                    break;
                }
            };

            // A gap in the numbering: earlier blocks settled on the medium, and
            // this one is from another epoch of writing. The diagnosis differs
            // from corruption because the cause differs: not a broken medium
            // but a write that never arrived.
            if parsed.seq != block_count {
                end = ScanEnd::SeqGap {
                    expected: block_count,
                    found: parsed.seq,
                };
                break;
            }

            let body_len = u64::from(parsed.body_len);
            let block_end = offset + BlockHeader::SIZE as u64 + body_len;
            // A length from a damaged header can be anything: before allocating
            // a buffer, check it against what is actually left of the file.
            if block_end > file_len {
                end = ScanEnd::Corrupt;
                break;
            }

            buf.clear();
            buf.resize(body_len as usize, 0);
            if file
                .read_exact_at(&mut buf, offset + BlockHeader::SIZE as u64)
                .is_err()
            {
                end = ScanEnd::Corrupt;
                break;
            }
            if parsed.verify(&buf).is_err() {
                end = ScanEnd::Corrupt;
                break;
            }

            if let Some(fb) = footer.as_deref_mut() {
                // The body is parsed for the sake of the type sets: a migration
                // decides from them whether to rewrite the segment, and a
                // reader what is in it at all.
                //
                // A body that fails to parse with an agreeing CRC (the codec is
                // not built into this build) does not break the walk: the bytes
                // passed the integrity check, so they are data. Only the
                // assembly stops — an incomplete footer is worse than none.
                match block::Block::from_parts(parsed, &buf) {
                    Ok(block) => {
                        let mut last = parsed.base;
                        for item in block.records() {
                            let Ok((at, record)) = item else { break };
                            last = at;
                            match record {
                                dduroc_format::Record::Message(m) => fb.add_event(m.event),
                                dduroc_format::Record::Sample(s) => fb.add_metric(s.metric),
                                _ => {}
                            }
                        }
                        fb.add_block(offset, &parsed, last);
                    }
                    Err(_) => {
                        footer_complete = false;
                        footer = None;
                    }
                }
            }

            block_count += 1;
            last_base = parsed.base;
            offset = block_end;
        }

        Ok((
            Self {
                header,
                data_end: offset,
                block_count,
                next_seq: block_count,
                last_base,
                stopped_by: end,
            },
            footer_complete,
        ))
    }

    /// Read a segment from disk and restore its boundaries.
    pub fn of_path(path: &Path) -> Result<Self> {
        let file = File::open(path).ctx_path("opening a segment", path)?;
        let len = file.metadata().ctx_path("stat", path)?.len();
        Self::run(&file, len, path)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Recovering a torn segment
// ════════════════════════════════════════════════════════════════════════════

/// What sealing a torn segment produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovered {
    pub name: SegmentName,
    /// The file's size after sealing.
    pub size_bytes: u64,
    /// How many bytes of the reserve window went back to the medium.
    pub reclaimed: u64,
    /// The tail was damaged and discarded (power lost mid-block).
    pub truncated: bool,
}

/// Whether a segment is sealed — by the signature in the last four bytes.
///
/// A cheap check: one read of four bytes. There is no reason to parse the
/// whole footer to answer "does this need recovery", and when a store comes up
/// the question is asked of every channel.
fn is_sealed(file: &File, len: u64) -> Result<bool> {
    if len < (SegmentHeader::SIZE + Trailer::SIZE) as u64 {
        return Ok(false);
    }
    let mut magic = [0u8; 4];
    match file.read_exact_at(&mut magic, len - 4) {
        Ok(()) => Ok(magic == FOOTER_MAGIC),
        // The file is shorter than stat said: someone is editing it. Treat it
        // as unsealed — recovery will work it out more honestly than a guess.
        Err(_) => Ok(false),
    }
}

/// Truncate a torn segment to the end of its intact data and seal it.
///
/// A torn segment from a previous run reads by scanning too, so no data is
/// lost without this. But while it is unsealed, the unwritten tail of its
/// reserve window is counted against the channel along with it. Several crash
/// stops in a row and rotation starts deleting live history to make room for
/// emptiness.
///
/// The segment gets a footer into the bargain: a block index for the reader
/// and type sets for migrations. That costs no second pass over the file —
/// [`Scan::run_collecting`] assembles them in the same walk it uses to find
/// the end of the data.
///
/// `Ok(None)` means the segment is already sealed, or it is not a segment.
pub fn seal_orphan(path: &Path, expect_store: Option<u64>) -> Result<Option<Recovered>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .ctx_path("opening a segment for recovery", path)?;
    let len = file.metadata().ctx_path("stat", path)?.len();
    if len < SegmentHeader::SIZE as u64 || is_sealed(&file, len)? {
        return Ok(None);
    }

    let mut footer = FooterBuilder::new();
    let (scan, footer_complete) = Scan::run_collecting(&file, len, path, &mut footer)?;
    // A foreign segment is not ours to rewrite: it has its own run numbering
    // and its own anchoring to time.
    scan.check_store(expect_store, path)?;

    // Truncating gives the reserve window back in any case — that is what this
    // is all for. The footer is appended only if it is complete: an index that
    // does not list every block would lead a reader past the rest, and without
    // one the segment reads honestly by scanning.
    file.set_len(scan.data_end)
        .ctx_path("truncating to the end of the data", path)?;
    let bytes = if footer_complete {
        let bytes = footer.build();
        file.write_all_at(&bytes, scan.data_end)
            .ctx_path("writing the footer", path)?;
        bytes
    } else {
        Vec::new()
    };
    fsutil::sync_data(&file, path)?;

    let size = scan.data_end + bytes.len() as u64;
    Ok(Some(Recovered {
        name: scan.header.file_name(),
        size_bytes: size,
        reclaimed: len.saturating_sub(size),
        truncated: scan.truncated(),
    }))
}

/// A segment open for reading.
///
/// The descriptor is held **only while reading** and released by
/// [`SegmentReader::detach`]: everything worth a trip to the medium — the
/// header, the footer, the data boundaries — is parsed once and lives in
/// memory, while reopening the file costs one call. A reader keeps a cursor
/// per (namespace, channel) pair, and with the twenty-four thousand namespaces
/// claimed, a permanent descriptor for each is tens of thousands of open files
/// for one query.
#[derive(Debug)]
pub struct SegmentReader {
    /// `None` means the descriptor is released; a read will reopen the file.
    file: Option<File>,
    path: PathBuf,
    len: u64,
    header: SegmentHeader,
    /// The footer bytes, if the segment is sealed.
    footer_bytes: Option<Vec<u8>>,
    data_end: u64,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).ctx_path("opening a segment", path)?;
        let len = file.metadata().ctx_path("stat", path)?.len();

        let mut head = [0u8; SegmentHeader::SIZE];
        file.read_exact_at(&mut head, 0)
            .ctx_path("reading the header", path)?;
        let header = SegmentHeader::parse(&head).map_err(|e| Error::Corrupt {
            path: path.to_owned(),
            reason: format!("segment header: {e}"),
        })?;

        let (footer_bytes, data_end) = Self::probe_tail(&file, path, len)?;

        Ok(Self {
            file: Some(file),
            path: path.to_owned(),
            len,
            header,
            footer_bytes,
            data_end,
        })
    }

    /// Re-read the tail of the file without parsing the header again.
    ///
    /// A subscription needs this: the segment under it grows (new blocks) and
    /// changes length (sealing trims the reserve window and appends the footer,
    /// releasing for idleness just trims). The header is unchanged by
    /// definition — a segment changes neither its run nor its base — so reading
    /// and parsing it is not repeated here.
    pub fn refresh(&mut self) -> Result<()> {
        self.attach()?;
        let file = self
            .file
            .as_ref()
            .expect("after attach there is a descriptor");
        let len = file.metadata().ctx_path("stat", &self.path)?.len();
        let (footer_bytes, data_end) = Self::probe_tail(file, &self.path, len)?;
        self.len = len;
        self.footer_bytes = footer_bytes;
        self.data_end = data_end;
        Ok(())
    }

    /// A two-phase read of the footer: first the fixed-size trailer, from it
    /// the length, then the footer itself. That way nothing extra is read.
    ///
    /// Returns the footer bytes (if the segment is sealed) and the data
    /// boundary.
    fn probe_tail(file: &File, path: &Path, len: u64) -> Result<(Option<Vec<u8>>, u64)> {
        if len < (SegmentHeader::SIZE + Trailer::SIZE) as u64 {
            return Ok((None, len));
        }
        let mut tail = [0u8; Trailer::SIZE];
        file.read_exact_at(&mut tail, len - Trailer::SIZE as u64)
            .ctx_path("reading the trailer", path)?;
        let Ok(Some(trailer)) = Trailer::parse(&tail) else {
            return Ok((None, len));
        };
        let total = trailer.total_len();
        // The length from the trailer drives both the size of the read and the
        // data boundary, and the trailer itself is verified only by its
        // signature. So a footer is accepted only after the CRC is checked:
        // otherwise a corrupted length field would silently cut off part of the
        // blocks, passing a truncated segment off as a whole one. There are two
        // bounds and both are necessary. The file size rules out the knowably
        // impossible, but on a quarter-gigabyte segment it would permit a
        // quarter-gigabyte read on the strength of one number in the tail — and
        // the trailer is verified only by a signature, which costs nothing to
        // fake. The ceiling limits the footer to what footers actually are: a
        // block index entry costs a handful of bytes, and even a segment packed
        // with minimal blocks yields no more than a few megabytes.
        if total > len - SegmentHeader::SIZE as u64 || total > MAX_FOOTER {
            return Ok((None, len));
        }
        let mut buf = vec![0u8; total as usize];
        file.read_exact_at(&mut buf, len - total)
            .ctx_path("reading the footer", path)?;
        if matches!(dduroc_format::Footer::parse(&buf), Ok(Some(_))) {
            Ok((Some(buf), len - total))
        } else {
            Ok((None, len))
        }
    }

    /// Release the descriptor, keeping everything already parsed.
    ///
    /// Nothing parsed is lost: the header, the footer and the data boundaries
    /// are in memory, and reading a block reopens the file. This lets a reader
    /// keep a cursor per channel without keeping an open file per channel.
    ///
    /// The price is one open per batch of reading (not per block: within one open
    /// a cursor reads as many blocks as it needed). What is paid for it is a
    /// **window**: a file evicted by rotation between batches will not open under
    /// that name again. For live reading that is the same ordinary event as a
    /// segment vanishing between the listing and the open: the engine removed the
    /// history itself. A dump faces no rotation at all.
    pub fn detach(&mut self) {
        self.file = None;
    }

    /// Make sure the file is open.
    fn attach(&mut self) -> Result<()> {
        if self.file.is_none() {
            self.file = Some(File::open(&self.path).ctx_path("reopening a segment", &self.path)?);
        }
        Ok(())
    }

    pub fn header(&self) -> SegmentHeader {
        self.header
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len <= SegmentHeader::SIZE as u64
    }

    /// The boundary of the useful data: the end of the blocks, before the
    /// footer.
    ///
    /// It differs from [`SegmentReader::len`] at both ends: for a sealed segment
    /// the file length includes the footer, for an unsealed one the unwritten tail
    /// of the reserve window. Those who ask for it want the volume of the
    /// **records**, not of the file — a migration sizing a rewrite, for instance.
    pub fn data_end(&self) -> u64 {
        self.data_end
    }

    pub fn is_sealed(&self) -> bool {
        self.footer_bytes.is_some()
    }

    /// The parsed footer of a sealed segment.
    pub fn footer(&self) -> Option<dduroc_format::Footer> {
        let bytes = self.footer_bytes.as_ref()?;
        dduroc_format::Footer::parse(bytes).ok().flatten()
    }

    /// Read the block at an offset into `buf`. `Ok(None)` means the end of the
    /// data.
    ///
    /// Returns the offset of the next block.
    pub fn read_block_at(&mut self, offset: u64, buf: &mut Vec<u8>) -> Result<Option<u64>> {
        if offset >= self.data_end || self.data_end - offset < BlockHeader::SIZE as u64 {
            return Ok(None);
        }
        self.attach()?;
        let file = self
            .file
            .as_ref()
            .expect("after attach there is a descriptor");
        let mut hdr = [0u8; BlockHeader::SIZE];
        file.read_exact_at(&mut hdr, offset)
            .ctx_path("reading a block header", &self.path)?;
        let Some(header) = BlockHeader::parse(&hdr).map_err(|e| Error::Corrupt {
            path: self.path.clone(),
            reason: format!("block header at {offset}: {e}"),
        })?
        else {
            return Ok(None);
        };

        let total = BlockHeader::SIZE as u64 + u64::from(header.body_len);
        if offset + total > self.data_end {
            return Err(Error::Corrupt {
                path: self.path.clone(),
                reason: format!("the block at {offset} runs past the end of the data"),
            });
        }

        buf.clear();
        buf.resize(total as usize, 0);
        file.read_exact_at(buf, offset)
            .ctx_path("reading a block", &self.path)?;
        Ok(Some(offset + total))
    }

    /// The offset of the first block.
    pub const fn first_block_offset() -> u64 {
        SegmentHeader::SIZE as u64
    }

    /// The offsets of the blocks: from the footer if there is one, otherwise by
    /// a sequential scan.
    ///
    /// Damage breaks the **scan**, not the whole selection: the blocks already
    /// found are returned. Otherwise one broken header in the tail of an
    /// unsealed segment — the ordinary consequence of a power loss — would hide
    /// the entire segment from the reader.
    pub fn block_offsets(&mut self) -> Result<Vec<u64>> {
        if let Some(footer) = self.footer() {
            return Ok(footer.blocks.iter().map(|b| b.offset).collect());
        }
        Ok(self.scan_block_offsets().0)
    }

    /// The same, but reporting where the scan broke off.
    ///
    /// Besides corruption the scan catches a **gap in block numbering**: the
    /// numbers run consecutively from zero, and a skip means a piece of the
    /// write did not reach the medium although later ones did. The diagnosis
    /// differs from corruption because the cause differs, and staying silent
    /// about it is not an option: further along the file lie valid blocks with
    /// a hole between them, and an answer without a single sign of it would
    /// look complete.
    pub fn scan_block_offsets(&mut self) -> (Vec<u64>, Option<(u64, String)>) {
        let scan = self.scan_block_offsets_from(Self::first_block_offset(), 0);
        (scan.offsets, scan.stopped)
    }

    /// Continue the block scan from a known place.
    ///
    /// A stream subscription needs this: a segment being written right now is
    /// re-read as it grows, and starting from the beginning every time would
    /// mean reading the whole file for every batch of new records — an
    /// eight-megabyte segment instead of ten kilobytes of fresh tail.
    ///
    /// `expected_seq` carries the block numbering forward: it is per-segment
    /// and starts at zero, so a partial scan has to bring it along — otherwise
    /// the continuation would declare a numbering gap at the very first block.
    pub fn scan_block_offsets_from(&mut self, start: u64, expected_seq: u32) -> BlockScan {
        let mut scan = BlockScan {
            offsets: Vec::new(),
            end: start,
            next_seq: expected_seq,
            stopped: None,
        };
        let mut buf = Vec::new();
        let mut offset = start;
        loop {
            match self.read_block_at(offset, &mut buf) {
                Ok(Some(next)) => {
                    // The header is already read into the buffer — parsing it
                    // costs not one trip to the medium.
                    if let Ok(Some(header)) = BlockHeader::parse(&buf)
                        && header.seq != scan.next_seq
                    {
                        scan.stopped = Some((
                            offset,
                            format!(
                                "a gap in block numbering: expected {}, the file says {}",
                                scan.next_seq, header.seq
                            ),
                        ));
                        return scan;
                    }
                    scan.next_seq = scan.next_seq.saturating_add(1);
                    scan.offsets.push(offset);
                    scan.end = next;
                    offset = next;
                }
                Ok(None) => return scan,
                Err(e) => {
                    scan.stopped = Some((offset, e.to_string()));
                    return scan;
                }
            }
        }
    }
}

/// The result of a block scan together with the place to continue it.
#[derive(Debug, Clone)]
pub struct BlockScan {
    /// The offsets of the blocks found, oldest first.
    pub offsets: Vec<u64>,
    /// The offset the next scan will continue from: the end of the last intact
    /// block, or the place of the tear.
    pub end: u64,
    /// The number the next scan expects of the block at `end`.
    pub next_seq: u32,
    /// Where and why the scan broke off. `None` means it reached the end of the
    /// data.
    pub stopped: Option<(u64, String)>,
}

/// Parse a block from the raw bytes read by [`SegmentReader::read_block_at`].
pub fn parse_block(bytes: &[u8]) -> Result<Option<block::Block<'_>>> {
    Ok(block::Block::parse(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::block::{BlockBuilder, Compression};
    use dduroc_format::record::Message;
    use dduroc_format::{BootCounter, EventId, ProtocolVersion, Record};

    fn header(base: u64) -> SegmentHeader {
        SegmentHeader {
            protocol_version: ProtocolVersion(1),
            boot: BootCounter(7),
            base: Micros(base),
            store_id: 0,
        }
    }

    fn block_bytes(seq: u32, base: u64, count: usize) -> (Vec<u8>, BlockHeader) {
        let mut b = BlockBuilder::new();
        for i in 0..count {
            b.push(
                Micros(base + i as u64 * 100),
                &Record::Message(Message {
                    event: EventId(1),
                    span: None,
                    payload: &[0xAB; 8],
                }),
            )
            .unwrap();
        }
        let mut out = Vec::new();
        let h = b.finish(seq, Compression::None, &mut out).unwrap();
        (out, h)
    }

    /// Write a block with the number the segment expects.
    fn append(w: &mut SegmentWriter, base: u64, count: usize) -> u64 {
        let (bytes, _) = block_bytes(w.next_seq(), base, count);
        w.append_block(&bytes).unwrap()
    }

    #[test]
    fn create_append_and_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let mut w = SegmentWriter::create(dir.path(), header(1_000), 64 * 1024).unwrap();
        assert_eq!(w.data_end(), SegmentHeader::SIZE as u64);
        let offset = append(&mut w, 1_000, 3);
        assert_eq!(offset, SegmentHeader::SIZE as u64);
        w.sync().unwrap();
        assert!(!w.is_dirty());
        let end = w.data_end();
        let path = w.path().to_owned();
        drop(w);

        // Space is reserved — the size exceeds the data.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 64 * 1024);

        let w2 = SegmentWriter::reopen(&path, None, None).unwrap();
        assert_eq!(
            w2.data_end(),
            end,
            "the position of the end of the data is restored"
        );
        assert_eq!(w2.header(), header(1_000));
        assert_eq!(w2.next_seq(), 1, "block numbering continues");
    }

    #[test]
    fn zero_tail_terminates_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 32 * 1024).unwrap();
        append(&mut w, 1_000, 2);
        append(&mut w, 2_000, 2);
        w.sync().unwrap();
        let end = w.data_end();
        let path = w.path().to_owned();
        drop(w);

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert_eq!(scan.stopped_by, ScanEnd::ZeroTail);
        assert!(
            !scan.truncated(),
            "an intact tail does not count as damaged"
        );
        assert_eq!(scan.data_end, end);
    }

    #[test]
    fn torn_block_is_truncated_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 32 * 1024).unwrap();
        append(&mut w, 1_000, 4);
        let good_end = w.data_end();
        append(&mut w, 2_000, 4);
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        // Simulate a power loss in the middle of the second block by damaging
        // its tail.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF; 4], good_end + 34).unwrap();
        }

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 1, "the first block survived");
        assert_eq!(
            scan.stopped_by,
            ScanEnd::Corrupt,
            "the tear is recognized as corruption"
        );
        assert_eq!(
            scan.data_end, good_end,
            "the data ends before the broken block"
        );

        // Continuing the write from the restored position wipes the broken
        // tail.
        let mut w = SegmentWriter::reopen(&path, None, None).unwrap();
        assert_eq!(w.data_end(), good_end);
        assert_eq!(w.next_seq(), 1);
        append(&mut w, 3_000, 4);
        w.sync().unwrap();
        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert!(!scan.truncated());
    }

    #[test]
    fn tail_is_zeroed_after_recovery() {
        // The new block is shorter than the discarded one: no bytes of the
        // earlier write may remain behind it, or the next scan will read them
        // as a block — in the worst case with an agreeing CRC, bringing
        // discarded records back into the log.
        //
        // Recovery achieves that by truncating rather than by wiping: the file
        // ends where the intact data ends. The bytes after the new block are
        // brought in by extending the reserve window, and `fallocate` yields
        // zeros.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 1);
        let good_end = w.data_end();
        append(&mut w, 1_000, 50); // a long block that will not make it
        w.sync().unwrap();
        let long_end = w.data_end();
        let path = w.path().to_owned();
        drop(w);

        // Damage the header of the long block.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF; 8], good_end + 4).unwrap();
        }

        let mut w = SegmentWriter::reopen(&path, None, Some(32 * 1024)).unwrap();
        assert_eq!(w.data_end(), good_end);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            good_end,
            "the area of the former long block must be cut off"
        );

        append(&mut w, 2_000, 1);
        w.sync().unwrap();
        let new_end = w.data_end();
        assert!(
            new_end < long_end,
            "otherwise the test is empty: the block must be shorter"
        );

        // The tail brought in by extending the window for this block is zero.
        let f = File::open(&path).unwrap();
        let grown = std::fs::metadata(&path).unwrap().len();
        assert!(grown > new_end, "the window was extended for the write");
        let mut probe = vec![0u8; (grown - new_end) as usize];
        f.read_exact_at(&mut probe, new_end).unwrap();
        assert!(
            probe.iter().all(|&b| b == 0),
            "the tail of the window must be zero"
        );

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert_eq!(scan.stopped_by, ScanEnd::ZeroTail, "a clean end of data");
    }

    #[test]
    fn seq_gap_is_distinguished_from_corruption() {
        // Blocks B1 and B3 settled on the medium, B2 did not (writeback
        // reordering). This is not corruption of the medium, and the diagnosis
        // has to differ.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 1);
        let end_after_first = w.data_end();
        let (bytes, _) = block_bytes(5, 1_000, 1); // a number from the future
        w.append_block(&bytes).unwrap();
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 1);
        assert_eq!(
            scan.stopped_by,
            ScanEnd::SeqGap {
                expected: 1,
                found: 5
            }
        );
        assert!(scan.truncated());
        assert_eq!(scan.data_end, end_after_first);
    }

    #[test]
    fn foreign_store_segment_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = header(0);
        h.store_id = 0xAAAA_BBBB_CCCC_DDDD;
        let w = SegmentWriter::create(dir.path(), h, 8 * 1024).unwrap();
        let path = w.path().to_owned();
        drop(w);

        let err = SegmentWriter::reopen(&path, Some(0x1111_2222_3333_4444), None).unwrap_err();
        assert!(
            matches!(err, Error::ForeignSegment { .. }),
            "a segment of a foreign store must be refused: {err}"
        );
        // With its own identifier it opens as normal.
        SegmentWriter::reopen(&path, Some(0xAAAA_BBBB_CCCC_DDDD), None).unwrap();
    }

    #[test]
    fn garbage_block_length_does_not_allocate_wildly() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 8 * 1024).unwrap();
        append(&mut w, 0, 1);
        w.sync().unwrap();
        let path = w.path().to_owned();
        let end = w.data_end();
        drop(w);

        // body_len = 0xFFFF_FFFF on a file of 8 KiB.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF, 0xFF, 0xFF, 0xFF], end).unwrap();
        }
        let scan = Scan::of_path(&path).unwrap();
        assert!(scan.truncated());
        assert_eq!(
            scan.data_end, end,
            "a garbage length did not lead the scan past the file"
        );
    }

    #[test]
    fn orphan_is_sealed_and_gives_back_its_preallocation() {
        // A torn segment reads by scanning too, so no data is lost without
        // sealing. But while there is no footer, the unwritten tail of the
        // reserve window is counted against the file. Several crash stops in a
        // row eat the channel's budget with emptiness, after which rotation
        // starts on live history — that is what recovery is for.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 32 * 1024).unwrap();
        append(&mut w, 1_000, 3);
        append(&mut w, 2_000, 3);
        w.sync().unwrap();
        let path = w.path().to_owned();
        let data_end = w.data_end();
        drop(w); // a power loss: seal was never called

        assert_eq!(std::fs::metadata(&path).unwrap().len(), 32 * 1024);
        assert!(!SegmentReader::open(&path).unwrap().is_sealed());

        let rec = seal_orphan(&path, Some(0))
            .unwrap()
            .expect("the segment is torn");
        assert_eq!(rec.name, SegmentName::new(BootCounter(7), Micros(1_000)));
        assert!(!rec.truncated, "an intact tail is not damage");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            rec.size_bytes,
            "the file size matched what was declared"
        );
        assert!(
            rec.size_bytes < 4 * 1024,
            "the reserve window is given back: {}",
            rec.size_bytes
        );
        assert_eq!(rec.reclaimed, 32 * 1024 - rec.size_bytes);

        // The footer was assembled in the same walk that looked for the end of
        // the data.
        let mut r = SegmentReader::open(&path).unwrap();
        assert!(r.is_sealed());
        let footer = r.footer().expect("the footer reads");
        assert_eq!(footer.blocks.len(), 2);
        assert_eq!(footer.blocks[0].offset, SegmentHeader::SIZE as u64);
        assert_eq!(
            footer.events,
            vec![EventId(1)],
            "the type set was assembled from the block bodies: without it a migration \
             would walk past the segment"
        );
        assert_eq!(r.block_offsets().unwrap().len(), 2);
        assert!(data_end > 0);

        // A second call does nothing: the segment is already sealed.
        assert_eq!(seal_orphan(&path, Some(0)).unwrap(), None);
    }

    #[test]
    fn orphan_recovery_drops_the_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 2);
        let good_end = w.data_end();
        append(&mut w, 1_000, 2);
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        // Damage the second block — a power loss in the middle of a write.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF; 4], good_end + 34).unwrap();
        }

        let rec = seal_orphan(&path, Some(0)).unwrap().unwrap();
        assert!(rec.truncated, "damage to the tail must be announced");
        let r = SegmentReader::open(&path).unwrap();
        assert!(r.is_sealed());
        assert_eq!(
            r.footer().unwrap().blocks.len(),
            1,
            "only the surviving block reached the footer"
        );
    }

    #[test]
    fn orphan_of_a_foreign_store_is_left_alone() {
        // A file brought from another device must not be rewritten: it has its
        // own run numbering and its own anchoring to time.
        let dir = tempfile::tempdir().unwrap();
        let mut h = header(0);
        h.store_id = 0xAAAA_BBBB_CCCC_DDDD;
        let mut w = SegmentWriter::create(dir.path(), h, 16 * 1024).unwrap();
        append(&mut w, 0, 1);
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        let err = seal_orphan(&path, Some(0x1111_2222_3333_4444)).unwrap_err();
        assert!(matches!(err, Error::ForeignSegment { .. }), "got {err}");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            16 * 1024,
            "the foreign file is untouched"
        );
    }

    #[test]
    fn scan_reports_a_hole_in_block_numbering() {
        // Block numbers run consecutively from zero. A skip means a piece of
        // the write did not reach the medium although later ones did — and a
        // hole formed between valid blocks. An answer without a single sign of
        // it would look complete, so the reader has to say so.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 1);
        let (bytes, _) = block_bytes(5, 1_000, 1); // a number from the future
        w.append_block(&bytes).unwrap();
        w.sync().unwrap();
        let path = w.path().to_owned();
        w.close_unsealed().unwrap();

        let mut r = SegmentReader::open(&path).unwrap();
        let (offsets, stopped) = r.scan_block_offsets();
        assert_eq!(
            offsets.len(),
            1,
            "the surviving block stays in the selection"
        );
        let (_, reason) = stopped.expect("the gap must be named");
        assert!(
            reason.contains("gap in block numbering"),
            "the diagnosis must differ from corruption: {reason}"
        );
    }

    #[test]
    fn seal_writes_footer_and_trims() {
        use dduroc_format::FooterBuilder;

        let dir = tempfile::tempdir().unwrap();
        let (bytes, bh) = block_bytes(0, 1_000, 3);
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 64 * 1024).unwrap();
        let offset = w.append_block(&bytes).unwrap();
        let path = w.path().to_owned();

        let mut fb = FooterBuilder::new();
        fb.add_block(offset, &bh, Micros(1_200));
        fb.add_event(EventId(1));
        w.seal(&fb.build()).unwrap();

        let size = std::fs::metadata(&path).unwrap().len();
        assert!(
            size < 64 * 1024,
            "the tail of the reserve window is trimmed: {size}"
        );

        let mut r = SegmentReader::open(&path).unwrap();
        assert!(r.is_sealed());
        let footer = r.footer().expect("the footer reads");
        assert_eq!(footer.blocks.len(), 1);
        assert_eq!(footer.blocks[0].offset, offset);
        assert_eq!(r.block_offsets().unwrap(), vec![offset]);
    }

    #[test]
    fn reader_falls_back_to_scan_without_footer() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 16 * 1024).unwrap();
        let o1 = append(&mut w, 1_000, 2);
        let o2 = append(&mut w, 2_000, 2);
        w.sync().unwrap();
        let path = w.path().to_owned();
        // Close without a footer — as after a power loss.
        w.close_unsealed().unwrap();

        let mut r = SegmentReader::open(&path).unwrap();
        assert!(!r.is_sealed());
        assert_eq!(
            r.block_offsets().unwrap(),
            vec![o1, o2],
            "the scan found the blocks"
        );

        let mut buf = Vec::new();
        let next = r.read_block_at(o1, &mut buf).unwrap();
        assert_eq!(next, Some(o2));
        let block = parse_block(&buf).unwrap().unwrap();
        assert_eq!(block.records().count(), 2);
    }

    #[test]
    fn refuses_to_overwrite_existing_segment() {
        let dir = tempfile::tempdir().unwrap();
        let w = SegmentWriter::create(dir.path(), header(1_000), 8 * 1024).unwrap();
        drop(w);
        // The same (boot, time): an existing file must not be touched.
        let err = SegmentWriter::create(dir.path(), header(1_000), 8 * 1024);
        assert!(err.is_err(), "creating it again must fail");
    }

    #[test]
    fn fits_reserves_room_for_terminator() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = SegmentHeader::SIZE as u64 + 200;
        let w = SegmentWriter::create(dir.path(), header(0), capacity).unwrap();
        assert!(w.fits(100));
        assert!(
            !w.fits(200 - BlockHeader::SIZE as u64 + 1),
            "room for the zero terminator must remain"
        );
    }
}
