//! The dduroc on-disk byte format: segments, blocks, records.
//!
//! This crate contains **codecs and validation only** — no I/O, no state,
//! nothing asynchronous. Everything here is a pure function over byte slices,
//! so the layer is fully covered by unit and property tests, while the engine
//! ([`dduroc-engine`]) answers separately for files, threads and policies.
//!
//! # Structure
//!
//! ```text
//! <root>/<namespace>/<channel>/<boot:08x>-<micros:016x>.seg
//!   [SegmentHeader 32B] [Block]* [Footer]?
//!        Block = [BlockHeader 32B] [body: records, optionally compressed]
//! ```
//!
//! The namespace and the channel are not encoded in the bytes at all — they
//! are implicit in the file path.
//!
//! # Principles
//!
//! - **Only what varies goes to disk.** Levels, text templates, message tags
//!   and type names live in the binary's schema and are resolved at read time.
//! - **Time deltas.** Records within a block store a varint delta from the
//!   previous record: 1–3 bytes against the 10-byte key of the LSM prototype.
//! - **Integrity per block.** CRC32C and compression are amortized over a
//!   block, not over a record.
//! - **Power loss is normal.** An unfinished tail of an active segment is
//!   distinguishable from corruption: see [`Error::is_torn_tail`].
//! - **armv7.** File sizes and offsets are always `u64`, never `usize`; mmap
//!   is not used.
//!
//! [`dduroc-engine`]: https://docs.rs/dduroc-engine

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod block;
mod cursor;
mod error;
pub mod footer;
mod ids;
mod level;
pub mod record;
pub mod segment;
mod value;
pub mod varint;

pub use block::{Block, BlockBuilder, BlockHeader, Compression, restamp_seq};
pub use error::{Error, Result};
pub use footer::{Footer, FooterBuilder, Trailer};
pub use ids::{
    BootCounter, BootTime, EventId, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId,
};
pub use level::Level;
pub use record::{Framed, Message, Record, RecordKind, Sample, SpanStart, Text};
pub use segment::{SegmentHeader, SegmentName};
pub use value::{Value, ValueType};

/// The container version — the byte format as such. Not to be confused with
/// the schema protocol version ([`ProtocolVersion`]), which belongs to the
/// application and changes with its migrations.
///
/// | version | what changed |
/// |---|---|
/// | 1 | the first layout: a sample referred to a segment-local series number, a separate `SeriesDef` record tied that number to a metric and to runtime tags, and the series table was duplicated in the footer |
/// | 2 | no runtime tags: a series is identified by its metric, a sample carries `metric_id`, and both the `SeriesDef` record and the series table are gone |
///
/// A reader accepts **only** the current version: a mismatch is a clear
/// [`Error::UnsupportedContainerVersion`], not an attempt to guess the layout.
/// A file of an earlier version stays readable by the earlier build.
pub const CONTAINER_VERSION: u8 = 2;
