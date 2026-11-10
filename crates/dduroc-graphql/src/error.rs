//! Stable GraphQL errors, independent of an HTTP transport.
use async_graphql::{Error, ErrorExtensions};

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ServiceError {
    #[error(transparent)]
    Native(#[from] dduroc_read::ReadError),
    #[error("invalid request: {0}")]
    InvalidInput(String),
    #[error("read session does not exist")]
    NotFound,
    #[error("read session is busy")]
    Busy,
    #[error("read session revision changed")]
    RevisionConflict { actual: u64 },
    #[error("service capacity exhausted")]
    Capacity,
    #[error("read session expired")]
    Expired,
    #[error("read worker unavailable")]
    WorkerUnavailable,
    #[error("service deadline exceeded")]
    Timeout,
    #[error("subscription consumer fell behind")]
    SlowConsumer,
    #[error("service is shutting down")]
    Shutdown,
    #[error("operation cancelled")]
    Cancelled,
    #[error("value conversion refused: {0}")]
    Conversion(String),
}

impl ServiceError {
    pub fn code(&self) -> &'static str {
        use dduroc_read::ReadError;
        match self {
            Self::Native(ReadError::Cancelled) | Self::Cancelled => "CANCELLED",
            Self::Native(ReadError::Deadline) | Self::Timeout => "DEADLINE_EXCEEDED",
            Self::Native(ReadError::LimitExceeded(_)) => "READ_LIMIT_EXCEEDED",
            Self::Native(ReadError::SnapshotExpired) => "SNAPSHOT_EXPIRED",
            Self::Native(ReadError::RetentionGap) => "RETENTION_GAP",
            Self::Native(ReadError::InvalidNavigation) => "INVALID_INPUT",
            Self::Native(ReadError::IncompleteCount) => "INCOMPLETE_COUNT",
            Self::Native(ReadError::PageTooSmall) => "PAGE_TOO_SMALL",
            Self::Native(ReadError::UnsupportedSession(_) | ReadError::NotFollowable(_)) => {
                "UNSUPPORTED_OPERATION"
            }
            Self::Native(_) => "READ_FAILED",
            Self::InvalidInput(_) => "INVALID_INPUT",
            Self::NotFound => "SESSION_NOT_FOUND",
            Self::Busy => "SESSION_BUSY",
            Self::RevisionConflict { .. } => "REVISION_CONFLICT",
            Self::Capacity => "CAPACITY_EXCEEDED",
            Self::Expired => "SESSION_EXPIRED",
            Self::WorkerUnavailable => "WORKER_UNAVAILABLE",
            Self::SlowConsumer => "SLOW_CONSUMER",
            Self::Shutdown => "SHUTDOWN",
            Self::Conversion(_) => "CONVERSION_FAILED",
        }
    }

    pub fn graphql(&self) -> Error {
        // Native errors may contain long paths or decoder diagnostics. The code
        // and typed resource remain authoritative even when text is bounded.
        let message = if matches!(self, Self::Native(_)) {
            self.code().replace('_', " ").to_lowercase()
        } else {
            bounded_message(&self.to_string(), 1024)
        };
        Error::new(message).extend_with(|_, extensions| {
            extensions.set("code", self.code());
            if let Self::RevisionConflict { actual } = self {
                extensions.set("actualRevision", actual.to_string());
            }
            if let Self::Native(dduroc_read::ReadError::LimitExceeded(resource)) = self {
                extensions.set("resource", format!("{resource:?}"));
            }
        })
    }
}

pub(crate) fn bounded_message(message: &str, max_bytes: usize) -> String {
    if message.len() <= max_bytes {
        return message.to_owned();
    }
    if max_bytes < 3 {
        return ".".repeat(max_bytes);
    }
    let mut end = max_bytes.saturating_sub(3).min(message.len());
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &message[..end])
}
