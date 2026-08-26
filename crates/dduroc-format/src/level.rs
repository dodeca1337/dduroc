//! Severity levels.
//!
//! For schema messages the level is a **static property of the type**: it is
//! declared in the schema, known at compile time and never written to disk
//! (it is resolved at read time from the `EventId`). Within the format,
//! `Level` is needed only by [`Text`] records, which have no schema: foreign
//! text from the tracing/log bridge or from a panic handler.
//!
//! [`Text`]: crate::record::Text

use crate::error::{Error, Result};

/// Ordered by increasing severity, so a "Warn and above" filter is a `>=`
/// comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Level {
    /// Call tracing — the most detailed level.
    DevTrace = 0,
    Trace = 1,
    Debug = 2,
    Info = 3,
    Warn = 4,
    Error = 5,
}

impl Level {
    pub const ALL: [Level; 6] = [
        Level::DevTrace,
        Level::Trace,
        Level::Debug,
        Level::Info,
        Level::Warn,
        Level::Error,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Level::DevTrace => "DEV_TRACE",
            Level::Trace => "TRACE",
            Level::Debug => "DEBUG",
            Level::Info => "INFO",
            Level::Warn => "WARN",
            Level::Error => "ERROR",
        }
    }

    pub const fn from_u8(raw: u8) -> Result<Self> {
        match raw {
            0 => Ok(Level::DevTrace),
            1 => Ok(Level::Trace),
            2 => Ok(Level::Debug),
            3 => Ok(Level::Info),
            4 => Ok(Level::Warn),
            5 => Ok(Level::Error),
            other => Err(Error::BadLevel(other)),
        }
    }
}

impl core::fmt::Display for Level {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_all() {
        for l in Level::ALL {
            assert_eq!(Level::from_u8(l as u8), Ok(l));
        }
    }

    #[test]
    fn rejects_unknown() {
        assert_eq!(Level::from_u8(6), Err(Error::BadLevel(6)));
        assert_eq!(Level::from_u8(255), Err(Error::BadLevel(255)));
    }

    #[test]
    fn ordering_is_by_severity() {
        assert!(Level::Error > Level::Warn);
        assert!(Level::Warn > Level::Info);
        assert!(Level::DevTrace < Level::Trace);
    }
}
