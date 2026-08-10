//! Движок хранилища dduroc.
//!
//! Отвечает за всё, чего нет в [`dduroc_format`]: файлы, потоки, политики.
//! Синхронный и **без tokio** — writer живёт на выделенном OS-потоке, поэтому
//! библиотека одинаково пригодна в async- и в обычных приложениях, а
//! офлайн-вьюеру не нужен рантайм.
//!
//! ```text
//! <root>/
//!   epochs.bin                 связь относительного времени с UTC
//!   <namespace>/
//!     ns-meta                  схема и версия протокола неймспейса
//!     <channel>/
//!       <boot>-<micros>.seg    сегменты
//! ```

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod channel;
pub mod clock;
pub mod epochs;
mod error;
mod fsutil;
pub mod limits;
pub mod metric;
pub mod migrate;
pub mod namespace;
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
pub use rotation::Inventory;
pub use schema::{
    EventDesc, Language, MetricDesc, MetricKind, Range, Schema, Severity, SpanDesc, StateDesc,
    StorageClass, Thresholds,
};
pub use segment::{Recovered, Scan, SegmentReader, SegmentWriter, seal_orphan};
pub use staged::{NsId, Staged, StagedRecord};
pub use stats::Stats;
pub use store::{Store, StoreConfig};
pub use writer::QueueSizes;
