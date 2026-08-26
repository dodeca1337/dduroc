//! A channel — a storage class within a namespace.
//!
//! Physically it is a subdirectory of segments; logically it is a set of
//! policies: segment and block size, compression and, above all, durability.
//! The record format is the same in every channel — only the policies differ,
//! which is why "critical" and "not critical" live in one engine.
//!
//! A budget is declared for a **whole class**: "all telemetry gets this much,
//! all logs get that much". The channels of every namespace of one class draw
//! on a shared budget, and the oldest segment of the class is evicted
//! regardless of whose namespace it lies in.

use crate::error::{Error, Result};
use dduroc_format::Compression;
use std::path::PathBuf;
use std::time::Duration;

/// The settings of a storage class — the policy of all its channels.
///
/// There is deliberately no name here: a channel lives in the directory of its
/// storage class ([`crate::schema::StorageClass::as_str`]), and a second source
/// of the same name in the configuration could only drift away from the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelConfig {
    /// The budget of a **class across the whole store**: the total size of that
    /// class's segments in every namespace. When it is exceeded, the class's
    /// oldest segment is evicted whoever it belongs to: a quiet service does
    /// not hold space a noisy one lacks. A quota for an individual namespace is
    /// a separate, optional setting ([`crate::store::NsQuota`]).
    pub budget_bytes: u64,
    /// The growth limit of one segment — the rotation boundary.
    ///
    /// This is **not** the file's size at creation: space is reserved as a
    /// window starting at one block and grows by eighths of the limit as
    /// writing goes on (see the `segment` module). A channel that wrote a
    /// hundred bytes takes one extent on the medium rather than a whole
    /// segment, and is counted as that in the class budget. So the number of
    /// channels that can write at once is set not by `budget_bytes /
    /// segment_bytes` but by how much they actually wrote.
    ///
    /// Fixed rather than derived from the budget: the budget is shared by the
    /// class, and a segment that grew along with it would give one noisy
    /// channel the right to take the whole thing before the first rotation.
    pub segment_bytes: u64,
    /// The threshold at which a block is closed.
    pub block_max_bytes: usize,
    /// The longest an incomplete block may wait before being flushed.
    pub flush_interval: Duration,
    /// `fdatasync` no more often than this interval. The window of loss on a
    /// power cut equals the interval; syncing on sealing a segment and on
    /// shutdown happens in any case.
    ///
    /// `ZERO` means syncing right after every group commit. That is a group
    /// commit, not a sync per record: the writer takes everything that has
    /// piled up in the queue, writes it as one block and syncs once — a burst
    /// of a hundred events costs one `fdatasync` (~1–10 ms on eMMC). This is
    /// how the critical channel works, and for it this is not a setting but a
    /// definition: a non-zero interval on the critical class is refused when
    /// the store is opened.
    pub sync_interval: Duration,
    pub compression: Compression,
    /// The class's own root. `None` means the store's shared root.
    ///
    /// Needed when a class belongs on a different medium: critical data on a
    /// protected partition (jffs2), heavy telemetry on a large one. The layout
    /// inside the root is the same: `<root>/<namespace>/<class>/`. A live
    /// reader (`store.reader()`) learns every root from the store itself; a
    /// dump is told them all at once (`Reader::open_dump`). Changing the root
    /// does not move the history: new segments are written to the new place,
    /// the old one stays readable as one more dump root but no longer counts
    /// towards the class budget.
    pub custom_root: Option<PathBuf>,
}

impl ChannelConfig {
    /// Sensible defaults for the given class budget.
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            budget_bytes,
            segment_bytes: 8 * 1024 * 1024,
            block_max_bytes: 64 * 1024,
            flush_interval: Duration::from_secs(1),
            sync_interval: Duration::from_secs(10),
            compression: Compression::Lz4,
            custom_root: None,
        }
    }

    /// The class of critical data: sync immediately, compression off.
    ///
    /// Compression on a critical channel is harmful: it makes the system hoard
    /// data for the sake of efficiency, and hoarding is exactly what a critical
    /// channel avoids. The segment is half the usual size: critical partitions
    /// are small.
    pub fn critical(budget_bytes: u64) -> Self {
        Self {
            sync_interval: Duration::ZERO,
            compression: Compression::None,
            block_max_bytes: 8 * 1024,
            flush_interval: Duration::from_millis(50),
            segment_bytes: 4 * 1024 * 1024,
            ..Self::new(budget_bytes)
        }
    }

    /// Validate the configuration; `class` says whose channel it is in the
    /// error.
    pub fn validate(&self, class: crate::schema::StorageClass) -> Result<()> {
        self.check().map_err(|reason| Error::BadChannel {
            class,
            namespace: None,
            reason,
        })
    }

    /// The same check, but with one cause and no address.
    ///
    /// One and the same bad setting has two addresses: the store's class and
    /// the class of a namespace group ([`crate::store::GroupPolicy`]). The
    /// cause they share, and there must not be a second copy of it — once the
    /// copies drifted apart they would declare different things invalid.
    pub(crate) fn check(&self) -> std::result::Result<(), &'static str> {
        // A budget smaller than two segments means that on sealing the only
        // segment rotation would delete it at once — the channel would hold
        // nothing.
        //
        // The multiplication saturates: `segment_bytes` comes from the
        // application's configuration, and an ordinary one would overflow a u64
        // into a debug panic where the answer is obvious — such a budget is
        // knowably too small.
        if self.budget_bytes < self.segment_bytes.saturating_mul(2) {
            return Err(
                "the budget is smaller than two segments: rotation would eat the data at once",
            );
        }
        if self.block_max_bytes < 512 {
            return Err("the block is too small: the overhead would eat the savings");
        }
        if (self.block_max_bytes as u64) * 2 > self.segment_bytes {
            return Err(
                "the block is comparable to a segment: a segment would not hold even two blocks",
            );
        }
        Ok(())
    }

    /// How many segments fit in the budget.
    pub fn max_segments(&self) -> u64 {
        (self.budget_bytes / self.segment_bytes).max(1)
    }
}

/// How a namespace group may differ from the class's shared settings.
///
/// There is neither `budget_bytes` nor `custom_root` here, and that is no
/// oversight. The budget and the medium are properties of the **class**,
/// shared across the whole store: "all telemetry gets this much, critical data
/// goes on the protected partition". Giving them to a group would either raise
/// the occupancy ceiling above what was declared (a budget on top of the
/// class's) or spread one class across two media, where its shared budget
/// would stop meaning anything. A group does have a limit of its own — a quota
/// ([`GroupPolicy::limit_bytes`]), a limit INSIDE the class budget.
///
/// The rest are properties of each writing channel separately, and belong to a
/// group by right: heavy orchestrator telemetry deserves its own segment size,
/// a rarely used diagnostic service its own flush delay.
///
/// What is not set is inherited from the class; it is built as a chain, like
/// everything else.
///
/// [`GroupPolicy::limit_bytes`]: crate::store::GroupPolicy::limit_bytes
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct ChannelOverride {
    pub segment_bytes: Option<u64>,
    pub block_max_bytes: Option<usize>,
    pub flush_interval: Option<Duration>,
    pub sync_interval: Option<Duration>,
    pub compression: Option<Compression>,
}

impl ChannelOverride {
    pub fn new() -> Self {
        Self::default()
    }

    /// Its own segment growth limit.
    pub fn segment_bytes(mut self, bytes: u64) -> Self {
        self.segment_bytes = Some(bytes);
        self
    }

    /// Its own threshold for closing a block.
    pub fn block_max_bytes(mut self, bytes: usize) -> Self {
        self.block_max_bytes = Some(bytes);
        self
    }

    /// Its own longest delay before flushing an incomplete block.
    pub fn flush_interval(mut self, every: Duration) -> Self {
        self.flush_interval = Some(every);
        self
    }

    /// Its own sync interval. On the critical class only zero is allowed:
    /// immediacy is its definition, not a setting.
    pub fn sync_interval(mut self, every: Duration) -> Self {
        self.sync_interval = Some(every);
        self
    }

    /// Its own compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Lay these over the class's settings.
    pub(crate) fn apply_to(&self, config: &mut ChannelConfig) {
        if let Some(v) = self.segment_bytes {
            config.segment_bytes = v;
        }
        if let Some(v) = self.block_max_bytes {
            config.block_max_bytes = v;
        }
        if let Some(v) = self.flush_interval {
            config.flush_interval = v;
        }
        if let Some(v) = self.sync_interval {
            config.sync_interval = v;
        }
        if let Some(v) = self.compression {
            config.compression = v;
        }
    }
}

/// Validation of a path component (a namespace or channel name).
///
/// The names come from the application's configuration and code but are
/// substituted into a filesystem path, so they are checked strictly: `..`,
/// path separators and control characters can lead a write outside the store.
pub(crate) fn validate_component(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("an empty name");
    }
    if name.len() > 64 {
        return Err("longer than 64 bytes");
    }
    if name == "." || name == ".." {
        return Err("a reserved name");
    }
    if name.starts_with('.') {
        return Err("a name may not begin with a dot");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err("only ASCII letters, digits, '-', '_' and '.' are allowed");
    }
    // The .tmp extension is reserved for unfinished atomic writes and .seg for
    // segments: a directory with such a name would confuse cleanup and
    // scanning.
    if name.ends_with(".tmp") || name.ends_with(".seg") || name.ends_with(".corrupt") {
        return Err("a reserved extension");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_size_does_not_scale_with_the_class_budget() {
        // The budget is shared by the class, while every writing namespace
        // holds a segment of its own: a segment that grew along with the budget
        // would let the active ones eat it. By the formula, 20 GiB of telemetry
        // would give every writer an 80 MiB segment.
        let c = ChannelConfig::new(20 * 1024 * 1024 * 1024);
        assert_eq!(c.segment_bytes, 8 * 1024 * 1024);
        c.validate(crate::schema::StorageClass::Default).unwrap();

        let c = ChannelConfig::new(64 * 1024 * 1024);
        assert_eq!(
            c.segment_bytes,
            8 * 1024 * 1024,
            "and does not depend on a small one either"
        );
        assert_eq!(c.max_segments(), 8);
        c.validate(crate::schema::StorageClass::Default).unwrap();
    }

    #[test]
    fn critical_channel_avoids_buffering() {
        let c = ChannelConfig::critical(256 * 1024 * 1024);
        assert_eq!(
            c.sync_interval,
            Duration::ZERO,
            "immediacy is the definition of the critical channel"
        );
        assert_eq!(c.compression, Compression::None);
        assert!(c.flush_interval < Duration::from_secs(1));
        c.validate(crate::schema::StorageClass::Default).unwrap();
    }

    #[test]
    fn degenerate_configs_rejected() {
        let mut c = ChannelConfig::new(64 * 1024 * 1024);
        c.budget_bytes = c.segment_bytes; // exactly one segment
        assert!(
            c.validate(crate::schema::StorageClass::Default).is_err(),
            "a budget of one segment is meaningless"
        );

        let mut c = ChannelConfig::new(64 * 1024 * 1024);
        c.block_max_bytes = 16;
        assert!(c.validate(crate::schema::StorageClass::Default).is_err());

        let mut c = ChannelConfig::new(64 * 1024 * 1024);
        c.block_max_bytes = c.segment_bytes as usize;
        assert!(c.validate(crate::schema::StorageClass::Default).is_err());
    }

    #[test]
    fn path_components_are_validated_strictly() {
        for good in ["default", "orc-radio-0", "apt_x", "a.b", "A1"] {
            assert!(validate_component(good).is_ok(), "{good} must pass");
        }
        for bad in [
            "",
            "..",
            ".",
            ".hidden",
            "a/b",
            "a\\b",
            "../etc",
            "a b",
            "naïve", // non-ASCII
            "a\0b",
            "x.tmp",
            "x.seg",
            "x.corrupt",
            "\n",
        ] {
            assert!(validate_component(bad).is_err(), "{bad:?} must be rejected");
        }
        assert!(validate_component(&"x".repeat(64)).is_ok());
        assert!(validate_component(&"x".repeat(65)).is_err());
    }
}
