//! Format encoding and decoding errors.
//!
//! The split matters for recovery: [`Error::Truncated`] and
//! [`Error::CrcMismatch`] on the **last** block of a segment are normal (power
//! lost mid-write) and the reader simply trims the tail. The remaining
//! variants mean corruption or a foreign format.

/// Format errors.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("data is truncated: more bytes expected")]
    Truncated,

    #[error("varint does not fit in a u64, or is not canonical")]
    VarintOverflow,

    #[error("CRC32C mismatch: header says {expected:#010x}, computed {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("wrong magic: expected {expected:?}, got {actual:?}")]
    BadMagic { expected: [u8; 4], actual: [u8; 4] },

    #[error("container version {0} is not supported (expected {1})")]
    UnsupportedContainerVersion(u8, u8),

    #[error("unknown record kind {0:#x}")]
    UnknownRecordKind(u8),

    #[error(
        "record kind {0:#x} belongs to container version 1: the block body was \
         written in the earlier layout, where a sample referred to a local series number"
    )]
    RetiredRecordKind(u8),

    #[error("unknown series value type {0:#x}")]
    UnknownValueType(u8),

    #[error("invalid level {0}")]
    BadLevel(u8),

    #[error("the string is not valid UTF-8")]
    BadUtf8,

    #[error("limit exceeded: {what} = {value}, maximum {max}")]
    LimitExceeded {
        what: &'static str,
        value: u64,
        max: u64,
    },

    #[error("unknown compression algorithm {0:#x}")]
    UnknownCompression(u8),

    #[error("failed to decompress a block: {0}")]
    Decompress(&'static str),

    #[error("attempt to finish an empty block")]
    EmptyBlock,

    /// A reserved-value contract was broken: reserved bits are not zero, or a
    /// value the format reserves was used (a zero `span_id`, a zero delta in a
    /// strictly increasing set).
    #[error("a reserved-value contract was broken")]
    ReservedValue,
}

pub type Result<T> = core::result::Result<T, Error>;

impl Error {
    /// `true` for errors expected on the last block after a power loss. On such
    /// an error recovery trims the tail rather than declaring the file corrupt.
    pub fn is_torn_tail(&self) -> bool {
        matches!(
            self,
            Error::Truncated | Error::CrcMismatch { .. } | Error::VarintOverflow
        )
    }
}
