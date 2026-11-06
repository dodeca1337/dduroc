//! A channel — a storage class within a namespace.
//!
//! A channel is a logical stream with policies for its class budget, blocks,
//! compression and durability. Channels on one root share raft log files.
//! The record format is the same in every channel — only the policies differ,
//! which is why "critical" and "not critical" live in one engine.
//!
//! A budget is declared for a **whole class**: "all telemetry gets this much,
//! all logs get that much". The channels of every namespace of one class draw
//! on a shared budget, and the oldest block of the class is evicted
//! regardless of whose namespace it lies in.

use crate::error::{Error, Result};
use dduroc_format::Compression;
use std::path::PathBuf;
use std::time::Duration;

/// The settings of a storage class — the policy of all its channels.
///
/// The typed storage class identifies the logical stream. Channels on one
/// physical root share the raft log; there is no per-channel file name here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelConfig {
    /// The budget of a **class across the whole store**: the total size of that
    /// class's encoded blocks in every namespace. When it is exceeded, the class's
    /// oldest block is evicted whoever it belongs to: a quiet service does
    /// not hold space a noisy one lacks. A quota for an individual namespace is
    /// a separate, optional setting ([`crate::store::NsQuota`]).
    pub budget_bytes: u64,
    /// Target size for shared raft log files on this class's root. When
    /// classes share a root, their minimum configured target is used. A
    /// namespace-group override is rejected because it cannot control an
    /// individual channel's physical files.
    pub segment_bytes: u64,
    /// The threshold at which a block is closed.
    pub block_max_bytes: usize,
    /// The longest an incomplete block may wait before being flushed.
    pub flush_interval: Duration,
    /// Sync no more often than this interval. Normal events can be lost in a
    /// power cut before the next successful sync; shutdown also syncs.
    ///
    /// `ZERO` requires a successful synchronous journal write before each
    /// critical try_* call returns. Concurrent pending critical writes can
    /// share a group commit; sequential calls each await their own completion.
    /// There is no fixed burst-to-sync ratio. A nonzero interval is refused for
    /// the critical class. The std_fs backend uses File::sync_all().
    pub sync_interval: Duration,
    pub compression: Compression,
    /// The class's own root. `None` means the store's shared root.
    ///
    /// Each configured root contains its own shared `raft-engine` directory.
    /// A live reader shares those engines; an offline dump names every root
    /// and opens them exclusively. Changing this setting does not migrate old
    /// history: moving an existing class requires an explicit storage migration.
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
    /// Defaults avoid compression work and use a smaller block and log-file
    /// target. Immediate acknowledgement comes from sync, not from these sizes.
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
        // Conservatively require two shared log targets of logical budget.
        // This preserves rotation/rewrite headroom; it is not a per-channel
        // active-segment reservation.
        //
        // The multiplication saturates: `segment_bytes` comes from the
        // application's configuration, and an ordinary one would overflow a u64
        // into a debug panic where the answer is obvious — such a budget is
        // knowably too small.
        if self.budget_bytes < self.segment_bytes.saturating_mul(2) {
            return Err(
                "the class budget is below two shared log targets required for rotation/rewrite headroom",
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

    /// Ratio of the logical budget to the shared log target; not a physical
    /// file count, because raft files interleave namespaces and classes.
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
/// Block size, compression and intervals can vary by group. The legacy
/// segment_bytes field remains representable for API compatibility, but a
/// group override is rejected: shared physical files have one root-wide target.
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

    /// Legacy compatibility setter. Store::open rejects group-specific log
    /// targets; configure ChannelConfig::segment_bytes on the class instead.
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
