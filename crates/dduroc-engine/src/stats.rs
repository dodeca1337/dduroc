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
    pub records_written: AtomicU64,
    pub blocks_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub syncs: AtomicU64,
    pub segments_created: AtomicU64,
    pub segments_sealed: AtomicU64,
    pub segments_rotated: AtomicU64,
    /// Records dropped because the normal channel's queue overflowed.
    pub dropped: AtomicU64,
    /// Records refused as breaking the schema contract: an id from a foreign
    /// schema, a value of the wrong type. A build defect rather than a
    /// consequence of load, hence a separate counter — mixing it with losses
    /// would hide a bug behind "the disk cannot keep up".
    pub rejected: AtomicU64,
    /// How many times a write to the critical channel waited for room in the
    /// queue.
    pub backpressure_waits: AtomicU64,
    /// I/O errors in the writer.
    pub io_errors: AtomicU64,
    /// Damaged tails discarded during recovery.
    pub truncated_tails: AtomicU64,
    /// How many times a channel took a segment into work: created a new one
    /// **or** brought back one released for idleness.
    pub segments_opened: AtomicU64,
    /// How many times occupancy of the medium grew.
    ///
    /// Not handed out: this is the watchdog for recomputing the total budget.
    /// It grows in three cases — a segment was created, a released one was
    /// brought back, the reserve window was extended (see
    /// `SegmentWriter::reserve`); everything else only reduces occupancy. While
    /// the counter has not moved, there is no reason to walk the whole fleet
    /// for the sum.
    ///
    /// It is separate from `segments_opened` because that counter is what
    /// announces a change of the store's shape to readers: extending a window
    /// changes no shape — not one segment appeared or vanished — and waking
    /// subscriptions with it would mean calling them for every megabyte
    /// written.
    pub occupancy_raised: AtomicU64,
    /// How many times the store could not be brought back under its declared
    /// total ceiling.
    ///
    /// Only sealed segments can be evicted: the active one is being written to,
    /// and its reserve is not extravagance but the guarantee that ENOSPC
    /// arrives when the window is extended rather than in the middle of a
    /// critical event. So the ceiling has to exceed what the live segments have
    /// taken — and they take not `segment_bytes` but their actual reserve
    /// window, which grows along with what has been written. An unmeetable
    /// ceiling is better learned from a counter than from the medium running
    /// out.
    pub budget_overruns: AtomicU64,
    /// How many times the block buffers could not be brought back under the
    /// declared memory ceiling.
    ///
    /// The ceiling cannot be made hard without losing data: one record can be
    /// larger than any reasonable ceiling (an incompressible blob), and a block
    /// buffer has to hold at least that one. So the ceiling is honoured by
    /// freeing what can be freed, and its unmeetability is announced by a
    /// counter — which is more honest than discarding a record for the sake of
    /// memory accounting.
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
    pub segments_created: u64,
    pub segments_sealed: u64,
    pub segments_rotated: u64,
    pub dropped: u64,
    /// Refused as breaking the schema contract — see the field of the same name
    /// on [`Counters`].
    pub rejected: u64,
    pub backpressure_waits: u64,
    pub io_errors: u64,
    pub truncated_tails: u64,
    /// The store did not fit its declared total ceiling — see the field of the
    /// same name on [`Counters`].
    pub budget_overruns: u64,
    /// The block buffers did not fit the declared memory ceiling — see the
    /// field of the same name on [`Counters`].
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
