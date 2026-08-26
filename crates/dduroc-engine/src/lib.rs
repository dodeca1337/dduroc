//! The dduroc storage engine.
//!
//! It answers for everything [`dduroc_format`] does not: files, threads,
//! policies. Synchronous and **without tokio** — the writer lives on a
//! dedicated OS thread, so the library fits async and ordinary applications
//! alike, and an offline viewer needs no runtime.
//!
//! ```text
//! <root>/
//!   epochs.bin                 relative time tied to UTC
//!   <namespace>/
//!     ns-meta                  the namespace's schema and protocol version
//!     <channel>/
//!       <boot>-<micros>.seg    segments
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod channel;
pub mod clock;
pub mod diag;
pub mod epochs;
mod error;
mod fsutil;
pub mod limits;
pub mod metric;
pub mod migrate;
pub mod namespace;
pub mod pulse;
pub mod rotation;
pub mod schema;
pub mod segment;
pub mod staged;
pub mod stats;
pub mod store;
pub mod writer;

pub use channel::ChannelConfig;
pub use clock::Clock;
pub use epochs::{EpochStore, Epochs, SyncSource};
pub use error::{Error, Result};
pub use limits::{EffectiveLimits, MetricLimits, StateStatus};
pub use migrate::MigrationReport;
pub use namespace::{Namespace, Series, SpanGuard};
pub use pulse::{Beat, Pulse};
pub use rotation::Inventory;
pub use schema::{
    EventDesc, Language, MetricDesc, MetricKind, Range, Schema, Severity, SpanDesc, StateDesc,
    StorageClass, Thresholds,
};
pub use segment::{BlockScan, Recovered, Scan, SegmentReader, SegmentWriter, seal_orphan};
pub use staged::{NsId, Staged, StagedRecord};
pub use stats::Stats;
pub use store::{Store, StoreConfig};
pub use writer::QueueSizes;
