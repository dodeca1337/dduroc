//! Engine errors.

use std::path::PathBuf;

/// An engine failure.
///
/// The enum is **open** (`#[non_exhaustive]`): a new cause of failure comes
/// from a new check, not from a new decision by the caller — on the write
/// paths the questions are "was the record lost" ([`Error::loses_record`]) and
/// "is this a build defect" ([`Error::breaks_contract`]), and new variants
/// already answer them.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },

    #[error("format: {0}")]
    Format(#[from] dduroc_format::Error),

    #[error("serializing metadata: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("invalid namespace name {name:?}: {reason}")]
    BadNamespace { name: String, reason: &'static str },

    #[error("schema {name:?} fails validation: {reason}")]
    BadSchema { name: String, reason: String },

    #[error(
        "event {event} is not declared in schema {schema:?}: an id from a foreign schema \
         would mean a foreign decoder at read time"
    )]
    UnknownEvent { schema: &'static str, event: u16 },

    #[error("span kind {kind} is not declared in schema {schema:?}")]
    UnknownSpanKind { schema: &'static str, kind: u16 },

    #[error("the fields of event {event:?} do not serialize")]
    EncodeFailed { event: &'static str },

    #[error(
        "storage class {class:?} is declared by no type of schema {schema:?}: \
         there is no channel for it"
    )]
    ClassNotDeclared {
        schema: &'static str,
        class: crate::schema::StorageClass,
    },

    #[error("invalid store setting {setting}: {reason}")]
    BadStore {
        setting: &'static str,
        reason: &'static str,
    },

    #[error("invalid policy for group {prefix:?}: {reason}")]
    BadGroup {
        prefix: String,
        reason: &'static str,
    },

    #[error(
        "invalid channel {class}{}: {reason}",
        namespace.as_deref().map(|n| format!(" of namespace {n:?}")).unwrap_or_default()
    )]
    BadChannel {
        /// The storage class: a channel is a class, and it has no second name.
        class: crate::schema::StorageClass,
        /// Whose channel; `None` means the class configuration before any
        /// namespace.
        namespace: Option<String>,
        reason: &'static str,
    },

    #[error("namespace {0:?} is already open in this process")]
    NamespaceBusy(String),

    #[error(
        "namespace {namespace:?} was written by schema {stored:?} and is being opened by \
         schema {opening:?}: different schemas in one namespace would mix incompatible event ids"
    )]
    SchemaMismatch {
        namespace: String,
        stored: String,
        opening: String,
    },

    #[error(
        "namespace {namespace:?} is at protocol version {stored}, this build's schema is \
         {current}: data from the future, which this firmware cannot understand"
    )]
    ProtocolFromFuture {
        namespace: String,
        stored: u16,
        current: u16,
    },

    #[error("no migration step {from} → {to} for schema {schema:?}")]
    MissingMigration { schema: String, from: u16, to: u16 },

    /// A second concurrent `Namespace::migrate` on one namespace.
    ///
    /// A refusal rather than a wait: a run takes minutes, and a second call
    /// hanging silently would look like the application had frozen. Repeating
    /// it makes sense only once the first has finished — by which time there is
    /// most likely nothing left to do.
    #[error("a migration of namespace {0:?} is already running")]
    MigrationBusy(String),

    #[error("metric {metric_id} is not declared in the schema")]
    UnknownMetric { metric_id: u16 },

    #[error(
        "metric {metric_id} is declared as {declared:?} but the value is {got:?}: \
         a sample's type is a property of the metric, not of an individual record"
    )]
    ValueTypeMismatch {
        metric_id: u16,
        declared: dduroc_format::ValueType,
        got: dduroc_format::ValueType,
    },

    #[error("invalid limits for metric {metric_name:?}: {reason}")]
    BadLimits {
        metric_name: &'static str,
        reason: &'static str,
    },

    #[error("{path} is damaged: {reason}")]
    Corrupt { path: PathBuf, reason: String },

    #[error(
        "store {0} is already open: two writers on one directory would hand out \
         the same run numbers and collide on segment names"
    )]
    StoreBusy(PathBuf),

    #[error("the store is shutting down")]
    ShuttingDown,

    #[error("the write queue is full: the disk cannot keep up")]
    QueueFull,

    #[error("the writer thread died: writing is impossible")]
    WriterDead,

    /// The sync did not catch up with the queue.
    ///
    /// Application threads enqueue records faster than the writer can consume
    /// them, and `sync` stopped trying after its allotted number of passes:
    /// otherwise it would not return until the writers fell silent.
    ///
    /// The records are **not lost** — they are still in the queue and will
    /// reach the medium in the ordinary course; they are merely not on the
    /// medium yet. So this is neither a loss ([`Error::loses_record`] is
    /// `false`) nor a defect in the call: it is a report that the promise
    /// "everything accumulated is on the medium" was this time not kept in
    /// full.
    #[error("the sync did not catch up with the queue: some records are not on the medium yet")]
    SyncIncomplete,

    #[error("no space left on device: {0}")]
    NoSpace(PathBuf),

    #[error(
        "segment {path} was created by another store (expected {expected:#018x}, \
         the file says {found:#018x}): it has its own run numbering and its own \
         anchoring to time"
    )]
    ForeignSegment {
        path: PathBuf,
        expected: u64,
        found: u64,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Adds a path and an operation to an io error: without them a "No such file
/// or directory" in a log says nothing about which of a thousand files failed
/// to open.
pub(crate) trait IoContext<T> {
    fn ctx(self, what: &str) -> Result<T>;
    fn ctx_path(self, what: &str, path: &std::path::Path) -> Result<T>;
}

impl<T> IoContext<T> for std::io::Result<T> {
    fn ctx(self, what: &str) -> Result<T> {
        self.map_err(|source| Error::Io {
            context: what.to_owned(),
            source,
        })
    }

    fn ctx_path(self, what: &str, path: &std::path::Path) -> Result<T> {
        self.map_err(|source| Error::Io {
            context: format!("{what} {}", path.display()),
            source,
        })
    }
}

impl<T> IoContext<T> for std::result::Result<T, rustix::io::Errno> {
    fn ctx(self, what: &str) -> Result<T> {
        self.map_err(|e| Error::Io {
            context: what.to_owned(),
            source: e.into(),
        })
    }

    fn ctx_path(self, what: &str, path: &std::path::Path) -> Result<T> {
        self.map_err(|e| Error::Io {
            context: format!("{what} {}", path.display()),
            source: e.into(),
        })
    }
}

impl Error {
    /// Whether the error means the **record was lost**.
    ///
    /// The caller's main question on the write path, and it must not require
    /// matching the whole enum: a loss is a state of the medium (the queue is
    /// falling behind, the writer is dead, the store is stopping), not an error
    /// in the call. A contract violation (an id from a foreign schema) is the
    /// opposite — a defect in the code, and it calls for a different response:
    /// not a retry but a fix.
    pub fn loses_record(&self) -> bool {
        matches!(
            self,
            Error::QueueFull | Error::WriterDead | Error::ShuttingDown
        )
    }

    /// An error in the call: what the schema declared and what was passed do
    /// not match.
    ///
    /// A retry does not cure it and load does not cause it — this is a build
    /// defect (usually a type from one schema passed to a namespace of
    /// another).
    pub fn breaks_contract(&self) -> bool {
        matches!(
            self,
            Error::UnknownEvent { .. }
                | Error::UnknownMetric { .. }
                | Error::UnknownSpanKind { .. }
                | Error::ValueTypeMismatch { .. }
                | Error::ClassNotDeclared { .. }
                | Error::EncodeFailed { .. }
        )
    }

    /// The device ran out of space — the policy for this case is special: the
    /// engine has to keep rotating and must not lose critical data silently.
    pub fn is_no_space(&self) -> bool {
        match self {
            Error::NoSpace(_) => true,
            Error::Io { source, .. } => source.raw_os_error() == Some(libc_enospc()),
            _ => false,
        }
    }
}

const fn libc_enospc() -> i32 {
    // ENOSPC is the same on every Linux ABI (armv7 included).
    28
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lost_records_are_distinguishable_from_caller_bugs() {
        // This split is what the predicates exist for: application code answers
        // a loss with a counter and with degradation, and a contract violation
        // with a fix to the build. Confusing them means either tolerating a bug
        // silently or raising an alarm over a full disk.
        for lost in [Error::QueueFull, Error::WriterDead, Error::ShuttingDown] {
            assert!(lost.loses_record(), "{lost}");
            assert!(!lost.breaks_contract(), "{lost}");
        }

        let bugs = [
            Error::UnknownEvent {
                schema: "radio",
                event: 7,
            },
            Error::UnknownMetric { metric_id: 7 },
            Error::UnknownSpanKind {
                schema: "radio",
                kind: 7,
            },
            Error::ValueTypeMismatch {
                metric_id: 1,
                declared: dduroc_format::ValueType::F32,
                got: dduroc_format::ValueType::U64,
            },
            Error::ClassNotDeclared {
                schema: "radio",
                class: crate::schema::StorageClass::Critical,
            },
            Error::EncodeFailed { event: "PowerSet" },
        ];
        for bug in bugs {
            assert!(bug.breaks_contract(), "{bug}");
            assert!(!bug.loses_record(), "{bug}");
        }

        // A failure of the medium is neither: the record may well have landed.
        let io = Error::NoSpace(PathBuf::from("/data"));
        assert!(!io.loses_record());
        assert!(!io.breaks_contract());
    }
}
