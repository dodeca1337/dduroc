//! Reading a dduroc store.
//!
//! This layer answers the question "what happened": it selects namespaces and
//! channels, merges their streams by time, restores what is not on disk
//! (levels, names, templates, UTC) and reports damaged fragments honestly
//! instead of passing an incomplete answer off as a complete one.
//!
//! It works both on a device and in an offline viewer, but differently:
//!
//! - a **live** reader ([`Reader::of_store`], `store.reader()` on the facade)
//!   is parallel to writing by construction: it asks the store for the roots,
//!   the schemas and the time anchors on every query, and tolerates rotation
//!   and a segment's growing tail as ordinary events. The subscription belongs
//!   to it too ([`Reader::follow`]): instead of polling on a timer, the reader
//!   sleeps while there is nothing to write and wakes on the very first block
//!   that lands in a file;
//! - a **dump** ([`Reader::open_dump`]) is a frozen snapshot: every root and
//!   schema is named at once, because records cannot be decoded without a
//!   schema — only identifiers and binary fields lie on disk — and a dump
//!   missing some class's tree is refused at open time rather than silently
//!   showing part of the history.
//!
//! # Time
//!
//! A record always has a relative time ([`BootTime`] — a run plus microseconds
//! since it started) and **sometimes** a wall-clock one (`Entry::utc`): the
//! second appears only where a synchronization anchor was recorded for the
//! hardware boot. Query bounds accept either scale — see [`Timestamp`].

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

// The cursors are the internal machinery of the walk: `SegmentCursor`,
// `ChannelScope`, `Prefilter` and the rest live here, but only the fruits of
// their work, listed below, belong to the public API. An open module would
// oblige us to keep the machinery compatible too.
mod cursor;
mod error;
pub mod query;
mod reader;

/// Wall-clock time is [`chrono`]: there is no reason to invent a date type of
/// our own, and a user should not have to add a dependency for a query bound.
pub use chrono;

pub use cursor::{Damage, Liveness, OwnedRecord, OwnedSampleValue, RawEntry};
/// The storage class — the same type as on the writing side: a channel is a
/// class.
pub use dduroc_engine::schema::StorageClass;
pub use dduroc_format::{BootCounter, BootTime, Micros};
pub use error::{ReadError, Result};
pub use query::{
    Bounds, Filter, KindFilter, NsSelect, Order, Query, Resolution, RunBounds, Timestamp,
};
pub use reader::{
    Entry, EntryKind, EntryStream, Follow, NamespaceInfo, NamespaceListing, QueryResult, Reader,
    Tail, render as render_with_schema,
};
