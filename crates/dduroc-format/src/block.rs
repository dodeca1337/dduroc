//! A block — the unit of writing, of flushing and of integrity checking.
//!
//! ```text
//! [BlockHeader 32B] [body: records back to back, optionally compressed whole]
//! ```
//!
//! A block corresponds to one writer batch. The CRC and the compression are
//! amortized over a block rather than over a record, which is why a record
//! costs a handful of bytes.
//!
//! # Three distinguishable states of the tail
//!
//! A segment is preallocated, so the unwritten tail is filled with zeros. The
//! header is arranged so that a normal end of data, a lost piece and
//! corruption are **different** diagnoses rather than one:
//!
//! | state | sign |
//! |---|---|
//! | end of data | all 32 header bytes are zero |
//! | corruption | the magic, the CRC or the reserved bits do not agree |
//! | a hole | `seq` is not one greater than the previous |
//!
//! The naive rule "`body_len == 0` means the end" is dangerous: a single
//! flipped bit in the length field is indistinguishable from the end of the
//! log, and blocks already confirmed by `fdatasync` would vanish silently
//! without a single error.

use crate::error::{Error, Result};
use crate::ids::Micros;
use crate::record::{self, Record};
use std::borrow::Cow;

/// The compression algorithm of a block body (the low 2 bits of `flags`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Compression {
    #[default]
    None = 0,
    Lz4 = 1,
    /// Recognized, but the codec is not built in: zstd pulls in a C dependency
    /// that complicates cross-building for armv7. Reading such a block is a
    /// clear error rather than garbage.
    Zstd = 2,
}

impl Compression {
    const MASK: u8 = 0b0000_0011;

    pub const fn from_bits(bits: u8) -> Result<Self> {
        match bits & Self::MASK {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            2 => Ok(Compression::Zstd),
            _ => Err(Error::UnknownCompression(bits & Self::MASK)),
        }
    }
}

/// The block header signature.
pub const BLOCK_MAGIC: [u8; 2] = *b"DB";

/// How many times larger than the compressed body the decompressed one may be.
///
/// For LZ4 that is 255: the shortest sequence yielding the most output is a
/// token, a two-byte offset and a chain of length bytes of 255 each.
const MAX_EXPANSION: u32 = 255;

/// The block header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    /// Length of the body on disk (after compression).
    pub body_len: u32,
    /// Length of the body before compression. Equals `body_len` for
    /// [`Compression::None`].
    pub raw_len: u32,
    /// The block's ordinal number within the segment, from zero. A gap in the
    /// numbering means a lost block — a state otherwise indistinguishable from
    /// corruption.
    pub seq: u32,
    /// Time of the block's first record.
    pub base: Micros,
    /// The number of records.
    pub count: u16,
    pub compression: Compression,
    /// CRC32C of the header (first 28 bytes) and of the body **as it lies on
    /// disk**.
    pub crc: u32,
}

impl BlockHeader {
    pub const SIZE: usize = 32;

    /// The ceiling on a block body's size: 64 MiB.
    ///
    /// The field holds a u32, but such values must not be accepted from disk —
    /// the length drives the size of an allocation at read time, and the file
    /// may be damaged or may have come from another device. Real blocks are
    /// tens of kilobytes.
    pub const MAX_BODY: u32 = 64 * 1024 * 1024;

    /// Serialize the header.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..2].copy_from_slice(&BLOCK_MAGIC);
        b[2] = self.compression as u8;
        b[3] = 0; // reserved
        b[4..8].copy_from_slice(&self.body_len.to_le_bytes());
        b[8..12].copy_from_slice(&self.raw_len.to_le_bytes());
        b[12..16].copy_from_slice(&self.seq.to_le_bytes());
        b[16..24].copy_from_slice(&self.base.0.to_le_bytes());
        b[24..26].copy_from_slice(&self.count.to_le_bytes());
        b[26] = 0; // reserved
        b[27] = 0; // reserved
        b[28..32].copy_from_slice(&self.crc.to_le_bytes());
        b
    }

    /// Parse the header **without** checking the CRC (the body is not read
    /// yet).
    ///
    /// `Ok(None)` means the header is entirely zero, that is, the unwritten
    /// tail of a preallocated file: a normal end of data. Any deviation from
    /// "all zeros" is parsed as a real header and has to pass the checks —
    /// otherwise a flipped bit in a length would look like the end of the log.
    pub fn parse(input: &[u8]) -> Result<Option<Self>> {
        let raw: &[u8; Self::SIZE] = input
            .get(..Self::SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::Truncated)?;

        if raw.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        let magic: [u8; 2] = [raw[0], raw[1]];
        if magic != BLOCK_MAGIC {
            return Err(Error::BadMagic {
                expected: [BLOCK_MAGIC[0], BLOCK_MAGIC[1], 0, 0],
                actual: [magic[0], magic[1], 0, 0],
            });
        }
        if raw[3] != 0 || raw[26] != 0 || raw[27] != 0 {
            return Err(Error::ReservedValue);
        }
        let compression = Compression::from_bits(raw[2])?;
        if raw[2] & !Compression::MASK != 0 {
            return Err(Error::ReservedValue);
        }

        let body_len = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
        let raw_len = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]);
        // Lengths are checked before anything is allocated from them.
        if body_len == 0 || body_len > Self::MAX_BODY || raw_len > Self::MAX_BODY {
            return Err(Error::LimitExceeded {
                what: "block body",
                value: u64::from(body_len.max(raw_len)),
                max: u64::from(Self::MAX_BODY),
            });
        }
        // The ceiling alone is not enough: the decompression buffer is
        // allocated from `raw_len`, and a thirty-byte body cannot expand into
        // sixty-four megabytes. Checking one length against the other bounds
        // the allocation by what actually lies in the file, not merely by a
        // format constant.
        let bound = match compression {
            Compression::None => body_len,
            // The LZ4 expansion limit is 255:1: a "token plus offset plus
            // length bytes" sequence yields no more than that per input byte.
            Compression::Lz4 | Compression::Zstd => body_len.saturating_mul(MAX_EXPANSION),
        };
        if raw_len > bound {
            return Err(Error::LimitExceeded {
                what: "block raw_len",
                value: u64::from(raw_len),
                max: u64::from(bound),
            });
        }

        Ok(Some(Self {
            body_len,
            raw_len,
            seq: u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]),
            base: Micros(u64::from_le_bytes(
                raw[16..24].try_into().expect("an 8-byte slice"),
            )),
            count: u16::from_le_bytes([raw[24], raw[25]]),
            compression,
            crc: u32::from_le_bytes([raw[28], raw[29], raw[30], raw[31]]),
        }))
    }

    /// Check the header CRC together with the body (the body as on disk).
    pub fn verify(&self, body_on_disk: &[u8]) -> Result<()> {
        let actual = compute_crc(self, body_on_disk);
        if actual != self.crc {
            return Err(Error::CrcMismatch {
                expected: self.crc,
                actual,
            });
        }
        Ok(())
    }

    /// The block's full size on disk.
    pub fn total_len(&self) -> u64 {
        Self::SIZE as u64 + u64::from(self.body_len)
    }

    /// Decompress the body. Without compression this borrows, with no copy.
    pub fn decompress<'a>(&self, body_on_disk: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        match self.compression {
            Compression::None => {
                if self.raw_len != self.body_len {
                    return Err(Error::Decompress("raw_len != body_len without compression"));
                }
                Ok(Cow::Borrowed(body_on_disk))
            }
            Compression::Lz4 => {
                let raw_len = self.raw_len as usize;
                let out = lz4_flex::block::decompress(body_on_disk, raw_len)
                    .map_err(|_| Error::Decompress("lz4: the body is damaged"))?;
                if out.len() != raw_len {
                    return Err(Error::Decompress("lz4: the length did not match raw_len"));
                }
                Ok(Cow::Owned(out))
            }
            Compression::Zstd => Err(Error::Decompress("zstd is not built into this build")),
        }
    }
}

/// Restamp the block number in already assembled bytes, recomputing the CRC.
///
/// Needed when a finished block does not fit the current segment and moves to
/// a new one: block numbering restarts in every segment, and there is no
/// reason to rebuild the body for the sake of one field.
pub fn restamp_seq(bytes: &mut [u8], seq: u32) -> Result<()> {
    let Some(mut header) = BlockHeader::parse(bytes)? else {
        return Err(Error::EmptyBlock);
    };
    let body = bytes
        .get(BlockHeader::SIZE..BlockHeader::SIZE + header.body_len as usize)
        .ok_or(Error::Truncated)?;
    header.seq = seq;
    header.crc = compute_crc(&header, body);
    bytes[..BlockHeader::SIZE].copy_from_slice(&header.to_bytes());
    Ok(())
}

fn compute_crc(header: &BlockHeader, body_on_disk: &[u8]) -> u32 {
    let bytes = header.to_bytes();
    let crc = crc32c::crc32c(&bytes[..28]);
    crc32c::crc32c_append(crc, body_on_disk)
}

// ════════════════════════════════════════════════════════════════════════════
// Assembling a block
// ════════════════════════════════════════════════════════════════════════════

/// The accumulator for one block's records.
///
/// It keeps the body in a reusable buffer, so the hot write path allocates
/// nothing: the buffer grows to its working size and is only cleared after.
#[derive(Debug, Default)]
pub struct BlockBuilder {
    base: Option<Micros>,
    last: Micros,
    count: u16,
    body: Vec<u8>,
    /// The LZ4 output, reused between flushes. `compress` used to allocate a
    /// fresh vector the size of the body for every block: at five flushes a
    /// second that is hundreds of kilobytes of allocation traffic wasted, and
    /// on a megabyte blob a megabyte spike on every sample.
    lz4: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// With the body capacity reserved up front.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            body: Vec::with_capacity(bytes),
            ..Self::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn count(&self) -> u16 {
        self.count
    }

    /// The current body size (before compression).
    pub fn raw_len(&self) -> usize {
        self.body.len()
    }

    /// Time of the block's first record.
    pub fn base(&self) -> Option<Micros> {
        self.base
    }

    /// Time of the block's last record.
    pub fn last(&self) -> Option<Micros> {
        self.base.map(|_| self.last)
    }

    /// Add a record with time `at`.
    ///
    /// Time that went backwards (a clock change does not matter — the source is
    /// monotonic — but data may arrive from another thread) is recorded as a
    /// zero delta: losing a record is worse than losing microseconds of
    /// resolution.
    pub fn push(&mut self, at: Micros, rec: &Record<'_>) -> Result<usize> {
        if self.count == u16::MAX {
            return Err(Error::LimitExceeded {
                what: "records per block",
                value: u64::from(self.count) + 1,
                max: u64::from(u16::MAX),
            });
        }

        let dt = match self.base {
            None => 0,
            Some(_) => at.saturating_delta(self.last),
        };

        // Encode FIRST, move the state AFTER. A record the codec rejected has
        // no right to leave a time base behind it: the base is the time of the
        // block's first record, and the decoder assigns it to the first
        // surviving record while ignoring that record's own delta
        // (`BlockRecords` below). A poisoned base would mean the whole block
        // reads shifted backwards by the difference — silently, with no error
        // and no counter. `encode` rewinds its own buffer (`record::encode`),
        // but it has no way to tell the accumulator about the rewind.
        let n = record::encode(rec, dt, &mut self.body)?;

        if self.base.is_none() {
            self.base = Some(at);
        }
        self.last = Micros(self.last.0.max(at.0));
        self.count += 1;
        Ok(n)
    }

    /// Finish the block: append `[header][body]` to `out` and clear the
    /// accumulator. Returns the header of the block written.
    ///
    /// `seq` is the block's ordinal within the segment (from zero): a gap in
    /// the numbering is how a reader tells a lost block from corruption.
    ///
    /// Compression is applied only when it actually shrinks the body: on short
    /// blocks LZ4 often grows them, and there is no reason to store a bloated
    /// body.
    pub fn finish(
        &mut self,
        seq: u32,
        compression: Compression,
        out: &mut Vec<u8>,
    ) -> Result<BlockHeader> {
        // Emptiness is judged by `count`, not by the presence of a base: the
        // two have to agree (see `push`), but what makes a block empty is the
        // absence of records, and tying the check to them makes a header with
        // `body_len == 0` physically impossible. `BlockHeader::parse` treats
        // such a header as corruption — a codec has no right to produce bytes
        // it stops at itself.
        if self.count == 0 {
            return Err(Error::EmptyBlock);
        }
        let base = self.base.ok_or(Error::EmptyBlock)?;
        let len = self.body.len();
        if len > BlockHeader::MAX_BODY as usize {
            // The accumulator is reset even on failure: otherwise a buffer that
            // grew past the ceiling would stay in it forever, and the channel
            // would jam on the same error at every following attempt.
            self.reset();
            return Err(Error::LimitExceeded {
                what: "block body",
                value: len as u64,
                max: u64::from(BlockHeader::MAX_BODY),
            });
        }
        let raw_len = len as u32;

        let compressed: Option<&[u8]> = match compression {
            Compression::None | Compression::Zstd => None,
            Compression::Lz4 => {
                // Compression into the reusable buffer. The buffer's `len`
                // stays at its high-water mark and is not reset: `resize`
                // zeroes only the growth, so repeated flushes pay neither an
                // allocation nor a memset.
                let max = lz4_flex::block::get_maximum_output_size(len);
                if self.lz4.len() < max {
                    self.lz4.resize(max, 0);
                }
                match lz4_flex::block::compress_into(&self.body, &mut self.lz4) {
                    Ok(n) if n < len => Some(&self.lz4[..n]),
                    _ => None,
                }
            }
        };

        let (used, body_on_disk): (Compression, &[u8]) = match compressed {
            Some(c) => (Compression::Lz4, c),
            None => (Compression::None, &self.body),
        };

        let mut header = BlockHeader {
            body_len: body_on_disk.len() as u32,
            raw_len,
            seq,
            base,
            count: self.count,
            compression: used,
            crc: 0,
        };
        header.crc = compute_crc(&header, body_on_disk);

        out.extend_from_slice(&header.to_bytes());
        out.extend_from_slice(body_on_disk);

        self.reset();
        Ok(header)
    }

    /// Drop what has accumulated, keeping the buffer capacity.
    pub fn reset(&mut self) {
        self.base = None;
        self.last = Micros(0);
        self.count = 0;
        self.body.clear();
    }

    /// Total capacity of the internal buffers (body plus compression output),
    /// in bytes.
    ///
    /// For tracking retention: `reset` and `finish` deliberately keep capacity
    /// for reuse, and how much has piled up must be visible from outside.
    pub fn capacity(&self) -> usize {
        self.body.capacity() + self.lz4.capacity()
    }

    /// Give back memory above `capacity` bytes per buffer. Takes effect only on
    /// an empty accumulator — an unfinished block must not be lost.
    ///
    /// Buffer capacity grows to the largest block that ever passed through the
    /// accumulator and, without this call, stays there forever: one megabyte
    /// blob would pin megabytes to a channel for the life of the process. The
    /// engine calls it when a channel goes idle.
    pub fn shrink_to(&mut self, capacity: usize) {
        if self.count != 0 {
            return;
        }
        self.body.shrink_to(capacity);
        // The contents of the compression output are garbage between flushes;
        // the length has to be reset, or `shrink_to` will not drop capacity
        // below it.
        self.lz4.clear();
        self.lz4.shrink_to(capacity);
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Reading a block
// ════════════════════════════════════════════════════════════════════════════

/// A block that has been read and verified.
#[derive(Debug)]
pub struct Block<'a> {
    pub header: BlockHeader,
    body: Cow<'a, [u8]>,
}

impl<'a> Block<'a> {
    /// Parse a block from the start of `input` (header plus body), checking the
    /// CRC.
    ///
    /// `Ok(None)` means a zero header, that is, the end of the segment's data.
    pub fn parse(input: &'a [u8]) -> Result<Option<Self>> {
        let Some(header) = BlockHeader::parse(input)? else {
            return Ok(None);
        };
        let body_end = BlockHeader::SIZE + header.body_len as usize;
        let body = input
            .get(BlockHeader::SIZE..body_end)
            .ok_or(Error::Truncated)?;
        // `from_parts` computes the CRC — there is nothing to compute a second
        // time here: it walks the whole body, and blocks are read by the tens
        // of thousands.
        Ok(Some(Self::from_parts(header, body)?))
    }

    /// Assemble a block from an already parsed header and a body **as it lies
    /// on disk**.
    ///
    /// For callers that read the header and the body separately (segment
    /// recovery reads the header first to tell a zero tail from data, and only
    /// then knows the body length). The CRC is checked right here: skipping the
    /// check would mean handing out unverified bytes.
    pub fn from_parts(header: BlockHeader, body_on_disk: &'a [u8]) -> Result<Self> {
        if body_on_disk.len() != header.body_len as usize {
            return Err(Error::Truncated);
        }
        header.verify(body_on_disk)?;
        Ok(Self {
            header,
            body: header.decompress(body_on_disk)?,
        })
    }

    /// The body, decompressed.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// An iterator over the records, each with its absolute time.
    pub fn records(&self) -> BlockRecords<'_> {
        BlockRecords {
            inner: record::iter(&self.body),
            at: self.header.base,
            first: true,
        }
    }
}

/// A block's records with absolute time restored.
#[derive(Debug)]
pub struct BlockRecords<'a> {
    inner: record::RecordIter<'a>,
    at: Micros,
    first: bool,
}

impl<'a> Iterator for BlockRecords<'a> {
    type Item = Result<(Micros, Record<'a>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let framed = match self.inner.next()? {
            Ok(f) => f,
            Err(e) => return Some(Err(e)),
        };
        // The first record sets the base; its delta is zero by construction.
        if !self.first {
            match self.at.checked_add_delta(framed.dt) {
                Some(t) => self.at = t,
                None => return Some(Err(Error::Truncated)),
            }
        }
        self.first = false;
        Some(Ok((self.at, framed.record)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{EventId, MetricId};
    use crate::record::{Message, Sample};
    use crate::value::Value;

    fn msg(event: u16, payload: &[u8]) -> Record<'_> {
        Record::Message(Message {
            event: EventId(event),
            span: None,
            payload,
        })
    }

    #[test]
    fn roundtrip_uncompressed() {
        let mut b = BlockBuilder::new();
        b.push(Micros(1_000), &msg(1, &[1, 2, 3])).unwrap();
        b.push(Micros(1_500), &msg(2, &[4])).unwrap();
        b.push(Micros(9_000), &msg(3, &[])).unwrap();

        let mut out = Vec::new();
        let header = b.finish(0, Compression::None, &mut out).unwrap();
        assert_eq!(header.count, 3);
        assert_eq!(header.base, Micros(1_000));
        assert_eq!(out.len() as u64, header.total_len());
        assert!(b.is_empty(), "after finish the accumulator is empty");

        let block = Block::parse(&out).unwrap().expect("there is a block");
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 3);
        // Absolute time restored from the deltas.
        assert_eq!(recs[0].0, Micros(1_000));
        assert_eq!(recs[1].0, Micros(1_500));
        assert_eq!(recs[2].0, Micros(9_000));
        assert_eq!(recs[0].1, msg(1, &[1, 2, 3]));
        assert_eq!(recs[2].1, msg(3, &[]));
    }

    #[test]
    fn lz4_roundtrip_and_only_when_smaller() {
        // A highly compressible body: a hundred identical messages.
        let mut b = BlockBuilder::new();
        for i in 0..100 {
            b.push(Micros(i * 10), &msg(7, &[0xAA; 16])).unwrap();
        }
        let mut out = Vec::new();
        let header = b.finish(0, Compression::Lz4, &mut out).unwrap();
        assert_eq!(header.compression, Compression::Lz4);
        assert!(
            header.body_len < header.raw_len,
            "compression must shrink the body: {} → {}",
            header.raw_len,
            header.body_len
        );

        let block = Block::parse(&out).unwrap().unwrap();
        assert_eq!(block.records().count(), 100);
        assert_eq!(block.body().len(), header.raw_len as usize);

        // An incompressible body: LZ4 must not inflate the block.
        let mut b = BlockBuilder::new();
        let noise: Vec<u8> = (0..64u16)
            .map(|i| (i.wrapping_mul(7919) >> 3) as u8)
            .collect();
        b.push(Micros(0), &msg(1, &noise)).unwrap();
        let mut out = Vec::new();
        let header = b.finish(0, Compression::Lz4, &mut out).unwrap();
        if header.compression == Compression::None {
            assert_eq!(header.body_len, header.raw_len);
        } else {
            assert!(header.body_len < header.raw_len);
        }
    }

    #[test]
    fn rejected_record_leaves_no_trace_in_the_accumulator() {
        // A block's base is the time of its FIRST record, and the decoder
        // assigns it to the first record unconditionally, ignoring that
        // record's own delta. So a record the codec rejected has no right to
        // set the base: it would become the base of a block it never entered,
        // and the whole block would read shifted backwards — with no error, no
        // counter and no trace in the file. The defect was reachable from the
        // public API via `SpanId(0)`.
        use crate::ids::SpanId;

        let mut b = BlockBuilder::new();
        let bad = Record::Message(Message {
            event: EventId(1),
            span: Some(SpanId(0)),
            payload: &[],
        });
        assert!(matches!(
            b.push(Micros(100), &bad),
            Err(Error::ReservedValue)
        ));
        assert!(b.is_empty(), "a rejected record does not count");
        assert_eq!(b.base(), None, "and does not set the time base");
        assert_eq!(b.last(), None);
        assert_eq!(b.raw_len(), 0, "the body buffer was rewound by the codec");

        // On an empty run the accumulator yields EmptyBlock rather than a
        // header with a zero body, which it would itself treat as corruption.
        let mut out = Vec::new();
        assert!(matches!(
            b.finish(0, Compression::None, &mut out),
            Err(Error::EmptyBlock)
        ));

        // The following records get their own real time.
        b.push(Micros(500), &msg(2, &[1])).unwrap();
        b.push(Micros(600), &msg(3, &[2])).unwrap();
        out.clear();
        let header = b.finish(0, Compression::None, &mut out).unwrap();
        assert_eq!(
            header.base,
            Micros(500),
            "the base is the time of the first record WRITTEN"
        );
        assert_eq!(header.count, 2);

        let block = Block::parse(&out).unwrap().expect("there is a block");
        let times: Vec<Micros> = block.records().map(|r| r.unwrap().0).collect();
        assert_eq!(
            times,
            vec![Micros(500), Micros(600)],
            "times read back as they were written"
        );
    }

    #[test]
    fn shrink_returns_peak_capacity_but_never_a_pending_block() {
        // Buffer capacity is the imprint of the largest block: one megabyte
        // blob with nothing given back would pin megabytes to a channel
        // forever.
        let big = vec![0xA5u8; 1 << 20];
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &big)).unwrap();

        // Compression does not touch an unfinished block: records must not be
        // lost.
        let before = b.capacity();
        b.shrink_to(0);
        assert_eq!(
            b.capacity(),
            before,
            "a non-empty accumulator does not shrink"
        );

        let mut out = Vec::new();
        b.finish(0, Compression::Lz4, &mut out).unwrap();
        assert!(
            b.capacity() >= 1 << 20,
            "capacity held after a megabyte block: {}",
            b.capacity()
        );

        b.shrink_to(0);
        assert_eq!(
            b.capacity(),
            0,
            "an empty accumulator must give everything back"
        );

        // The accumulator stays usable after the memory is given back.
        b.push(Micros(1), &msg(2, &[1, 2, 3])).unwrap();
        let mut out = Vec::new();
        b.finish(1, Compression::None, &mut out).unwrap();
        assert!(Block::parse(&out).unwrap().is_some());
    }

    #[test]
    fn lz4_buffer_is_reused_across_blocks() {
        // The compression output is reused between flushes; the second block
        // must neither read the first one's garbage nor depend on its length.
        let mut b = BlockBuilder::new();
        let mut bytes = Vec::new();

        for i in 0..50 {
            b.push(Micros(i * 10), &msg(7, &[0xAA; 64])).unwrap();
        }
        b.finish(0, Compression::Lz4, &mut bytes).unwrap();

        // The second block is noticeably shorter and holds different content.
        b.push(Micros(1_000), &msg(9, &[0x55; 16])).unwrap();
        let mut second = Vec::new();
        let h = b.finish(1, Compression::Lz4, &mut second).unwrap();

        let block = Block::parse(&second).unwrap().unwrap();
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].1, msg(9, &[0x55; 16]));
        assert_eq!(h.seq, 1);
    }

    #[test]
    fn zero_header_terminates() {
        let zeros = [0u8; BlockHeader::SIZE * 2];
        assert!(Block::parse(&zeros).unwrap().is_none());
        assert!(BlockHeader::parse(&zeros).unwrap().is_none());
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &[1, 2, 3, 4])).unwrap();
        let mut out = Vec::new();
        b.finish(0, Compression::None, &mut out).unwrap();

        // A corrupted body byte.
        let last = out.len() - 1;
        out[last] ^= 0xFF;
        let err = Block::parse(&out).unwrap_err();
        assert!(matches!(err, Error::CrcMismatch { .. }), "got {err}");
        assert!(err.is_torn_tail(), "a torn tail is recognized by recovery");
    }

    #[test]
    fn truncated_body_detected() {
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &[0xEE; 32])).unwrap();
        let mut out = Vec::new();
        b.finish(0, Compression::None, &mut out).unwrap();

        out.truncate(out.len() - 5); // power lost in the middle of writing a block
        assert_eq!(Block::parse(&out).unwrap_err(), Error::Truncated);
    }

    #[test]
    fn mixed_records_and_time_reconstruction() {
        let mut b = BlockBuilder::new();
        b.push(Micros(500), &msg(9, &[1])).unwrap();
        for i in 0..10u64 {
            b.push(
                Micros(1_000 + i * 250),
                &Record::Sample(Sample {
                    metric: MetricId(1),
                    value: Value::F32(20.0 + i as f32),
                }),
            )
            .unwrap();
        }
        let mut out = Vec::new();
        b.finish(0, Compression::None, &mut out).unwrap();

        let block = Block::parse(&out).unwrap().unwrap();
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 11);
        assert_eq!(recs[0].0, Micros(500));
        assert_eq!(recs[1].0, Micros(1_000));
        assert_eq!(recs[10].0, Micros(1_000 + 9 * 250));
    }

    #[test]
    fn non_monotonic_time_does_not_lose_records() {
        let mut b = BlockBuilder::new();
        b.push(Micros(1_000), &msg(1, &[])).unwrap();
        b.push(Micros(900), &msg(2, &[])).unwrap(); // time went backwards
        b.push(Micros(1_100), &msg(3, &[])).unwrap();
        let mut out = Vec::new();
        b.finish(0, Compression::None, &mut out).unwrap();

        let block = Block::parse(&out).unwrap().unwrap();
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 3, "no record is lost");
        assert_eq!(
            recs[1].0,
            Micros(1_000),
            "the step back collapses into a zero delta"
        );
        assert_eq!(recs[2].0, Micros(1_100));
    }

    #[test]
    fn empty_block_cannot_be_finished() {
        let mut b = BlockBuilder::new();
        let mut out = Vec::new();
        assert_eq!(
            b.finish(0, Compression::None, &mut out),
            Err(Error::EmptyBlock)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn header_roundtrip_bytes() {
        let h = BlockHeader {
            body_len: 1234,
            raw_len: 5678,
            seq: 0,
            base: Micros(0x0102_0304_0506_0708),
            count: 99,
            compression: Compression::Lz4,
            crc: 0xDEAD_BEEF,
        };
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), BlockHeader::SIZE);
        let parsed = BlockHeader::parse(&bytes).unwrap().unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn reserved_bytes_must_be_zero() {
        let h = BlockHeader {
            body_len: 8,
            raw_len: 8,
            seq: 0,
            base: Micros(0),
            count: 1,
            compression: Compression::None,
            crc: 0,
        };
        for offset in [3usize, 26, 27] {
            let mut bytes = h.to_bytes();
            bytes[offset] = 1;
            assert_eq!(
                BlockHeader::parse(&bytes),
                Err(Error::ReservedValue),
                "reserved bytes at offset {offset}"
            );
        }

        let mut bytes = h.to_bytes();
        bytes[2] = 0b1000_0000; // the high flag bits are reserved
        assert_eq!(BlockHeader::parse(&bytes), Err(Error::ReservedValue));
    }

    #[test]
    fn zero_body_len_is_corruption_not_end_of_data() {
        // The key difference from the naive "`body_len == 0` means the end": a
        // single flipped bit in a length has no right to look like the end of
        // the log, or blocks already confirmed by fdatasync would disappear
        // silently.
        let h = BlockHeader {
            body_len: 4096,
            raw_len: 4096,
            seq: 12,
            base: Micros(999),
            count: 7,
            compression: Compression::None,
            crc: 0x1234,
        };
        let mut bytes = h.to_bytes();
        bytes[4..8].copy_from_slice(&0u32.to_le_bytes());
        let err = BlockHeader::parse(&bytes).unwrap_err();
        assert!(
            matches!(err, Error::LimitExceeded { .. }),
            "a zero length with a non-empty header is corruption, not an end: {err}"
        );

        // Only an entirely zero header remains a terminator.
        assert_eq!(BlockHeader::parse(&[0u8; BlockHeader::SIZE]).unwrap(), None);
    }

    #[test]
    fn absurd_lengths_rejected_before_allocation() {
        let h = BlockHeader {
            body_len: 16,
            raw_len: 16,
            seq: 0,
            base: Micros(0),
            count: 1,
            compression: Compression::Lz4,
            crc: 0,
        };
        // A body_len past the format ceiling: parsing has to refuse without
        // trying to allocate anything.
        let mut bytes = h.to_bytes();
        bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            BlockHeader::parse(&bytes),
            Err(Error::LimitExceeded { .. })
        ));

        // The same for raw_len: the decompression buffer is allocated from it.
        let mut bytes = h.to_bytes();
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            BlockHeader::parse(&bytes),
            Err(Error::LimitExceeded { .. })
        ));
    }

    #[test]
    fn raw_len_is_checked_against_body_len() {
        // A raw_len within the format ceiling but out of proportion to the
        // body: thirty bytes do not expand into sixty-four megabytes. Without
        // checking one length against the other, such a header would make a
        // reader allocate a buffer for a size the file comes nowhere near.
        let h = BlockHeader {
            body_len: 30,
            raw_len: BlockHeader::MAX_BODY - 1,
            seq: 0,
            base: Micros(0),
            count: 1,
            compression: Compression::Lz4,
            crc: 0,
        };
        let err = BlockHeader::parse(&h.to_bytes()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::LimitExceeded {
                    what: "block raw_len",
                    ..
                }
            ),
            "got {err}"
        );

        // A reachable expansion is accepted: the LZ4 limit is 255:1.
        let ok = BlockHeader {
            raw_len: 30 * 255,
            ..h
        };
        assert!(BlockHeader::parse(&ok.to_bytes()).is_ok());

        // Without compression the lengths have to match.
        let uncompressed = BlockHeader {
            body_len: 30,
            raw_len: 31,
            compression: Compression::None,
            ..h
        };
        assert!(matches!(
            BlockHeader::parse(&uncompressed.to_bytes()),
            Err(Error::LimitExceeded { .. })
        ));
    }

    #[test]
    fn foreign_bytes_rejected_by_magic() {
        let mut bytes = [0xAAu8; BlockHeader::SIZE];
        bytes[0] = b'X';
        assert!(matches!(
            BlockHeader::parse(&bytes),
            Err(Error::BadMagic { .. })
        ));
    }

    #[test]
    fn restamping_seq_keeps_block_valid() {
        let mut b = BlockBuilder::new();
        b.push(Micros(10), &msg(1, &[1, 2, 3])).unwrap();
        b.push(Micros(20), &msg(2, &[4, 5])).unwrap();
        let mut out = Vec::new();
        b.finish(0, Compression::Lz4, &mut out).unwrap();

        restamp_seq(&mut out, 7).unwrap();
        let block = Block::parse(&out).unwrap().expect("the CRC was recomputed");
        assert_eq!(block.header.seq, 7);
        assert_eq!(block.records().count(), 2, "the content is untouched");
    }

    #[test]
    fn seq_is_preserved_for_hole_detection() {
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &[1])).unwrap();
        let mut out = Vec::new();
        let h = b.finish(42, Compression::None, &mut out).unwrap();
        assert_eq!(h.seq, 42);
        assert_eq!(Block::parse(&out).unwrap().unwrap().header.seq, 42);
    }
}
