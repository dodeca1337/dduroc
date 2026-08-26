//! Read errors.

use std::path::PathBuf;

/// A read failure.
///
/// The enum is **open** for the same reason as [`dduroc_engine::Error`]: a new
/// cause of failure is a new check, not a new decision by the caller.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ReadError {
    #[error(transparent)]
    Engine(#[from] dduroc_engine::Error),

    #[error("format: {0}")]
    Format(#[from] dduroc_format::Error),

    #[error(
        "segment {path} belongs to another store (expected {expected:#018x}, \
         the file says {found:#018x})"
    )]
    ForeignStore {
        path: PathBuf,
        expected: u64,
        found: u64,
    },

    #[error(
        "the dump is incomplete: namespace {namespace:?} has no tree for class {class} — \
         the store wrote that class to a root of its own, name ALL the dump's roots"
    )]
    IncompleteDump {
        namespace: String,
        class: dduroc_engine::schema::StorageClass,
    },

    #[error("invalid namespace selection pattern {0:?}")]
    BadPattern(String),

    #[error("such a query cannot be subscribed to: {0}")]
    NotFollowable(&'static str),

    #[error("invalid pagination cursor")]
    BadCursor,

    #[error("io: {context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

pub type Result<T> = std::result::Result<T, ReadError>;
