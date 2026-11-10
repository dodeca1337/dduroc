//! Cooperative limits for a complete read session.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadResource {
    RosterEntries,
    Channels,
    Blocks,
    CompressedBytes,
    DecodedBytes,
    ExaminedRecords,
    ReturnedBytes,
    EpochRuns,
    HardwareBoots,
}

/// Nonzero per-page record and accounted entry-byte ceilings.
/// Bytes account for the native Entry, namespace and owned payload lengths;
/// they are not serialized wire bytes or a total-RSS measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimits {
    max_records: std::num::NonZeroUsize,
    max_bytes: std::num::NonZeroUsize,
}
impl PageLimits {
    /// Validate page dimensions before opening or advancing a session.
    pub fn new(max_records: usize, max_bytes: usize) -> crate::Result<Self> {
        Ok(Self {
            max_records: std::num::NonZeroUsize::new(max_records)
                .ok_or(crate::ReadError::InvalidNavigation)?,
            max_bytes: std::num::NonZeroUsize::new(max_bytes)
                .ok_or(crate::ReadError::InvalidNavigation)?,
        })
    }
    pub const fn max_records(self) -> usize {
        self.max_records.get()
    }
    pub const fn max_bytes(self) -> usize {
        self.max_bytes.get()
    }
}

/// Cooperative cancellation, shared by a caller and a running read.
#[derive(Debug, Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Cumulative ceilings for one session, including cursor initialization.
#[derive(Debug, Clone)]
pub struct ReadLimits {
    pub epoch_runs: usize,
    pub hardware_boots: usize,
    pub roster_entries: usize,
    pub channels: usize,
    pub blocks: usize,
    pub compressed_bytes: u64,
    pub decoded_bytes: u64,
    pub examined_records: u64,
    pub returned_bytes: u64,
    pub deadline: Option<Instant>,
}
impl Default for ReadLimits {
    fn default() -> Self {
        Self {
            epoch_runs: 4096,
            hardware_boots: 1024,
            roster_entries: 65_536,
            channels: 256,
            blocks: 16_384,
            compressed_bytes: 128 * 1024 * 1024,
            decoded_bytes: 256 * 1024 * 1024,
            examined_records: 1_000_000,
            returned_bytes: 16 * 1024 * 1024,
            deadline: None,
        }
    }
}
#[derive(Debug, Clone)]
pub(crate) enum Stop {
    Cancelled,
    Deadline,
    Limit(ReadResource),
    Expired,
    RetentionGap,
    PageTooSmall,
    Backend(String),
}
#[derive(Debug, Default)]
struct Counts {
    roster: usize,
    channels: usize,
    blocks: usize,
    compressed: u64,
    decoded: u64,
    examined: u64,
    returned: u64,
    stop: Option<Stop>,
}
#[derive(Debug)]
pub(crate) struct Budget {
    pub limits: ReadLimits,
    cancel: Cancellation,
    counts: Mutex<Counts>,
    suppress_returned: AtomicBool,
}
impl Budget {
    pub fn new(limits: ReadLimits, cancel: Cancellation) -> Arc<Self> {
        Arc::new(Self {
            limits,
            cancel,
            counts: Mutex::new(Counts::default()),
            suppress_returned: AtomicBool::new(false),
        })
    }
    pub fn checkpoint(&self) -> bool {
        let mut c = self.counts.lock().unwrap();
        if c.stop.is_some() {
            return false;
        }
        if self.cancel.is_cancelled() {
            c.stop = Some(Stop::Cancelled);
        } else if self.limits.deadline.is_some_and(|d| Instant::now() >= d) {
            c.stop = Some(Stop::Deadline);
        }
        c.stop.is_none()
    }
    pub fn stop(&self, why: Stop) {
        let mut c = self.counts.lock().unwrap();
        if c.stop.is_none() {
            c.stop = Some(why);
        }
    }
    pub fn failure(&self) -> Option<Stop> {
        self.counts.lock().unwrap().stop.clone()
    }
    fn take(
        &self,
        name: ReadResource,
        n: u64,
        max: u64,
        slot: impl FnOnce(&mut Counts) -> &mut u64,
    ) -> bool {
        if !self.checkpoint() {
            return false;
        }
        let mut counts = self.counts.lock().unwrap();
        let value = slot(&mut counts);
        match value.checked_add(n) {
            Some(next) if next <= max => {
                *value = next;
                true
            }
            _ => {
                counts.stop = Some(Stop::Limit(name));
                false
            }
        }
    }
    pub fn roster(&self) -> bool {
        if !self.checkpoint() {
            return false;
        }
        let mut c = self.counts.lock().unwrap();
        if c.roster >= self.limits.roster_entries {
            c.stop = Some(Stop::Limit(ReadResource::RosterEntries));
            false
        } else {
            c.roster += 1;
            true
        }
    }
    pub fn channel(&self) -> bool {
        if !self.checkpoint() {
            return false;
        }
        let mut c = self.counts.lock().unwrap();
        if c.channels >= self.limits.channels {
            c.stop = Some(Stop::Limit(ReadResource::Channels));
            false
        } else {
            c.channels += 1;
            true
        }
    }
    pub fn block(&self) -> bool {
        if !self.checkpoint() {
            return false;
        }
        let mut c = self.counts.lock().unwrap();
        if c.blocks >= self.limits.blocks {
            c.stop = Some(Stop::Limit(ReadResource::Blocks));
            false
        } else {
            c.blocks += 1;
            true
        }
    }
    pub fn compressed(&self, n: u64) -> bool {
        self.take(
            ReadResource::CompressedBytes,
            n,
            self.limits.compressed_bytes,
            |c| &mut c.compressed,
        )
    }
    pub fn decoded(&self, n: u64) -> bool {
        self.take(
            ReadResource::DecodedBytes,
            n,
            self.limits.decoded_bytes,
            |c| &mut c.decoded,
        )
    }
    pub fn record(&self) -> bool {
        self.take(
            ReadResource::ExaminedRecords,
            1,
            self.limits.examined_records,
            |c| &mut c.examined,
        )
    }
    pub fn suppress_returned(&self, suppress: bool) {
        self.suppress_returned.store(suppress, Ordering::Release);
    }
    pub fn returned(&self, n: u64) -> bool {
        if self.suppress_returned.load(Ordering::Acquire) {
            return self.checkpoint();
        }
        self.take(
            ReadResource::ReturnedBytes,
            n,
            self.limits.returned_bytes,
            |c| &mut c.returned,
        )
    }
}

#[cfg(test)]
mod page_limit_tests {
    use super::PageLimits;

    #[test]
    fn page_limits_are_nonzero_and_preserve_requested_dimensions() {
        assert!(PageLimits::new(0, 1).is_err());
        assert!(PageLimits::new(1, 0).is_err());
        let limits = PageLimits::new(17, 4096).unwrap();
        assert_eq!((limits.max_records(), limits.max_bytes()), (17, 4096));
    }
}
