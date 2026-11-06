//! The engine's counters.
//!
//! The only way to learn what goes on inside: the writer cannot log its own
//! work — calling the public write path from its own thread would mean waiting
//! for room in a queue only it can drain. So the diagnostics are atomic
//! counters, sampled from outside.

use std::sync::atomic::{AtomicU64, Ordering};

/// Atomic counters. Every operation is `Relaxed`: the counters order nothing,
/// and superfluous barriers are noticeable on armv7.
#[derive(Debug, Default)]
pub struct Counters {
    /// Records appended to the block journal; ordinary writes need a later barrier.
    pub records_written: AtomicU64,
    pub blocks_written: AtomicU64,
    /// Encoded dduroc block bytes, excluding raft metadata, rewrites and file overhead.
    pub bytes_written: AtomicU64,
    /// Successful writer sync/barrier operations, not a count of physical syscalls.
    pub syncs: AtomicU64,
    /// Legacy segment counter, retained for API compatibility; zero in the raft writer.
    pub segments_created: AtomicU64,
    /// Legacy segment counter, retained for API compatibility; zero in the raft writer.
    pub segments_sealed: AtomicU64,
    /// Legacy segment counter, retained for API compatibility; zero in the raft writer.
    pub segments_rotated: AtomicU64,
    /// Records lost to bounded admission or writer/storage failure.
    pub dropped: AtomicU64,
    /// Refusals reported by the quiet API for schema/type/size validation.
    /// Explicit try_* callers handle such refusals themselves. They differ
    /// from a storage failure after a write was accepted.
    pub rejected: AtomicU64,
    /// How many times a write to the critical channel waited for room in the
    /// queue.
    pub backpressure_waits: AtomicU64,
    /// Terminal writer/backend failures, including I/O errors.
    pub io_errors: AtomicU64,
    /// Legacy recovery counter. Strict raft recovery refuses damaged tails;
    /// it never truncates them, so this remains zero for raft stores.
    pub truncated_tails: AtomicU64,
    /// Legacy internal segment counter; not used by the raft writer.
    pub segments_opened: AtomicU64,
    /// Legacy reserve-window counter; not used for raft occupancy accounting.
    /// Current physical/logical usage is ReadBackend::usage().
    pub occupancy_raised: AtomicU64,
    /// Legacy soft-ceiling counter, retained for API compatibility. The raft
    /// backend returns explicit retention/headroom errors instead of raising it.
    pub budget_overruns: AtomicU64,
    /// Legacy soft-buffer-ceiling counter, retained for API compatibility.
    /// Current producer payload and queue memory limits reject before admission.
    pub buffer_overruns: AtomicU64,
}

impl Counters {
    #[inline]
    pub fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// The same, but publishing everything written before the call.
    ///
    /// Exactly one counter needs this, and on exactly one transition: the
    /// writer decides whether to walk the channels for loss notices by the
    /// change in the total counter, because walking the whole fleet on every
    /// turn of the loop costs too much. The per-channel counter meanwhile grows
    /// on an application thread, and with `Relaxed` the writer could see the
    /// new total before the per-channel one: the walk would find zero, consider
    /// the notice issued, and the hole would stay unannounced until the next
    /// loss.
    ///
    /// So the order is mandatory: the per-channel counter first, then this
    /// call. The barrier is paid only on the loss path — that is, at a point
    /// where the record has already failed to happen.
    #[inline]
    pub fn publish(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Release);
    }

    pub fn snapshot(&self) -> Stats {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        Stats {
            records_written: g(&self.records_written),
            blocks_written: g(&self.blocks_written),
            bytes_written: g(&self.bytes_written),
            syncs: g(&self.syncs),
            segments_created: g(&self.segments_created),
            segments_sealed: g(&self.segments_sealed),
            segments_rotated: g(&self.segments_rotated),
            dropped: g(&self.dropped),
            rejected: g(&self.rejected),
            backpressure_waits: g(&self.backpressure_waits),
            io_errors: g(&self.io_errors),
            truncated_tails: g(&self.truncated_tails),
            budget_overruns: g(&self.budget_overruns),
            buffer_overruns: g(&self.buffer_overruns),
        }
    }
}

/// A snapshot of the counters.
///
/// The struct is **open**: a counter is added whenever the engine learns to
/// notice something new, and that must not break the build of anyone who only
/// reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Stats {
    pub records_written: u64,
    pub blocks_written: u64,
    pub bytes_written: u64,
    pub syncs: u64,
    /// Legacy segment counter; zero for raft-backed stores.
    pub segments_created: u64,
    /// Legacy segment counter; zero for raft-backed stores.
    pub segments_sealed: u64,
    /// Legacy segment counter; zero for raft-backed stores.
    pub segments_rotated: u64,
    pub dropped: u64,
    /// Refused as breaking the schema contract — see the field of the same name
    /// on [`Counters`].
    pub rejected: u64,
    pub backpressure_waits: u64,
    pub io_errors: u64,
    /// Legacy recovery counter; strict raft recovery never truncates a tail.
    pub truncated_tails: u64,
    /// Legacy soft-ceiling counter; see [`Counters::budget_overruns`].
    pub budget_overruns: u64,
    /// Legacy soft-buffer counter; see [`Counters::buffer_overruns`].
    pub buffer_overruns: u64,
}

impl Stats {
    /// Whether all is well: nothing lost, nothing refused, nothing broken.
    pub fn is_clean(&self) -> bool {
        self.dropped == 0 && self.rejected == 0 && self.io_errors == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_accumulate() {
        let c = Counters::default();
        Counters::bump(&c.records_written);
        Counters::add(&c.records_written, 9);
        Counters::bump(&c.dropped);

        let s = c.snapshot();
        assert_eq!(s.records_written, 10);
        assert_eq!(s.dropped, 1);
        assert!(!s.is_clean(), "losses must be visible");
        assert!(Stats::default().is_clean());

        // A contract violation is accounted for separately from losses: the
        // causes differ, and so does the response.
        let c = Counters::default();
        Counters::bump(&c.rejected);
        let s = c.snapshot();
        assert_eq!(s.dropped, 0);
        assert_eq!(s.rejected, 1);
        assert!(!s.is_clean(), "a refused record must be visible");
    }
}
