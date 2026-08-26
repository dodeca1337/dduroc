//! The store's mark that there is a point in looking again.
//!
//! A reader of a live store sees records by polling: it opens a segment and
//! reads what has already reached the medium. Without a "look again" mark, a
//! subscription to the stream would degenerate into polling on a timer —
//! either frequent (CPU and medium accesses wasted, and on a device that also
//! means flash wear) or rare (a live chart lagging by the polling period). The
//! mark removes the choice: the reader sleeps while there is nothing to write
//! and wakes on the very first block.
//!
//! There are three marks, and they differ in the cost of the answer:
//!
//! - **data** ([`Pulse::data_written`]) — a block landed in a file. Seeing it
//!   takes only reading the rest of a segment that is already open: not one
//!   `readdir`;
//! - **the shape of a channel** ([`Pulse::shape_changed`]) — a segment was
//!   created, sealed or evicted. The reader has to list the directory of
//!   **that** channel, which it holds open anyway;
//! - **the roster of the store** ([`Pulse::roster_changed`]) — a namespace came
//!   up, and with it the directories of its channels. This is the expensive
//!   answer: walk the root, read `ns-meta` in every matching directory and open
//!   cursors for the newcomers — work proportional to the whole store.
//!
//! One mark cannot express this: blocks come by the thousand, segments change
//! over minutes, and a namespace comes up once in a service's life. A single
//! counter would force listing directories on every block — and "segment" and
//! "namespace" fused together would force walking the whole store on every
//! rotation, that is, constantly and for nothing.
//!
//! The writer pays nothing for a mark while nobody is subscribed: raising a
//! counter is one atomic operation, and there is nobody to wake. The cost
//! arrives with the first waiter.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// What a deadline the clock cannot add up is cut down to.
///
/// An hour is knowably longer than any meaningful polling period and knowably
/// shorter than what makes [`Instant`] overflow.
pub const LONGEST_WAIT: Duration = Duration::from_secs(3600);

/// What the reader has already seen.
///
/// It is compared as a whole: if anything changed, there is a point in looking
/// again. The reader needs the individual fields only to decide whether to
/// walk directories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Beat {
    /// The data generation: it grows with every block that lands in a file.
    pub data: u64,
    /// The generation of channel shape: a segment was created, taken into work,
    /// sealed or evicted.
    pub shape: u64,
    /// The generation of the store's roster: a namespace came up with its
    /// channels.
    pub roster: u64,
    /// The store has stopped — there will be nothing new.
    pub closed: bool,
}

/// The marks of a live store.
///
/// It lives in [`Store`](crate::store::Store) and is held by the writer; the
/// reader asks the store.
#[derive(Debug, Default)]
pub struct Pulse {
    data: AtomicU64,
    shape: AtomicU64,
    roster: AtomicU64,
    closed: AtomicBool,
    /// How many readers are asleep right now. Zero means there is nobody to
    /// wake, and the writer does not take the mutex at all.
    waiters: AtomicUsize,
    /// It holds nothing: only the Condvar needs it. The state lives in atomics
    /// and is read without it.
    lock: Mutex<()>,
    wake: Condvar,
}

impl Pulse {
    pub fn new() -> Self {
        Self::default()
    }

    /// What there is right now.
    pub fn beat(&self) -> Beat {
        Beat {
            data: self.data.load(Ordering::SeqCst),
            shape: self.shape.load(Ordering::SeqCst),
            roster: self.roster.load(Ordering::SeqCst),
            closed: self.closed.load(Ordering::SeqCst),
        }
    }

    /// A block landed in a file: whoever read to the end has something to take.
    pub fn data_written(&self) {
        self.data.fetch_add(1, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// The segment changed: the channel's directory has to be listed again.
    pub fn shape_changed(&self) {
        self.shape.fetch_add(1, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// A namespace came up: directories the reader does not know about have
    /// appeared in the store.
    ///
    /// Separate from [`Pulse::shape_changed`] because answering it is the only
    /// work proportional to the **whole** store: walking the root and reading
    /// `ns-meta` in every matching directory. Rotation raises the shape mark
    /// constantly, and were it the same mark, a subscription would list
    /// twenty-four thousand directories over a segment that changed in one
    /// single channel.
    pub fn roster_changed(&self) {
        self.roster.fetch_add(1, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// The store has stopped: those waiting have nothing left to wait for.
    ///
    /// Without this, a subscription to a stopped store would hang until its
    /// timeout and look like "the device has gone quiet" although there is
    /// nobody left to write.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // Unconditionally: closing happens once, and sleeping through it is not
        // allowed.
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.wake.notify_all();
    }

    /// Wait for a difference from `seen`, but no longer than `timeout`.
    ///
    /// Returns the current state: equal to `seen` means nothing happened in the
    /// time allotted.
    ///
    /// A deadline the clock cannot add up (`Duration::MAX` and anything close
    /// to it) is cut down to [`LONGEST_WAIT`]: a panic on the addition would be
    /// the worst possible answer to a request to wait a while longer, and
    /// asking again costs a subscription nothing.
    pub fn wait(&self, seen: Beat, timeout: Duration) -> Beat {
        // A waiter announces itself BEFORE reading the counters, and the writer
        // raises a counter BEFORE reading the number of waiters. In the total
        // SeqCst order at least one side sees the other, so a mark set in the
        // gap between the check and falling asleep is not lost. A weaker
        // ordering here would mean a subscription that occasionally sleeps to
        // its timeout on a record stream running at full speed.
        self.waiters.fetch_add(1, Ordering::SeqCst);
        let mut now = self.beat();
        if now == seen && !timeout.is_zero() {
            let start = Instant::now();
            let deadline = start
                .checked_add(timeout)
                .unwrap_or_else(|| start + LONGEST_WAIT);
            let mut guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                now = self.beat();
                if now != seen {
                    break;
                }
                let Some(rest) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                guard = self
                    .wake
                    .wait_timeout(guard, rest)
                    .unwrap_or_else(|e| e.into_inner())
                    .0;
            }
            now = self.beat();
        }
        self.waiters.fetch_sub(1, Ordering::SeqCst);
        now
    }

    fn wake_waiters(&self) {
        if self.waiters.load(Ordering::SeqCst) == 0 {
            return;
        }
        // The mutex is taken right before the wake-up: a waiter holds it both
        // while checking the counter and inside `wait_timeout`, so we get here
        // either before its check or once it is already asleep — never in
        // between.
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.wake.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn nothing_new_means_waiting_out_the_timeout() {
        let pulse = Pulse::new();
        let seen = pulse.beat();
        let started = Instant::now();
        let now = pulse.wait(seen, Duration::from_millis(30));
        assert_eq!(now, seen, "nothing happened, so the state is unchanged");
        assert!(started.elapsed() >= Duration::from_millis(25));
    }

    #[test]
    fn a_beat_that_happened_before_the_wait_is_not_slept_through() {
        // A mark set before entering the wait has to be read immediately:
        // otherwise a subscription would sleep on data already written.
        let pulse = Pulse::new();
        let seen = pulse.beat();
        pulse.data_written();
        let started = Instant::now();
        let now = pulse.wait(seen, Duration::from_secs(30));
        assert_ne!(now, seen);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "it did not sleep"
        );
    }

    #[test]
    fn a_wait_longer_than_the_clock_can_count_is_shortened_not_fatal() {
        // "Wait as long as you like" is a legitimate request, and a panic on
        // the addition would be the worst possible answer to it: the wait would
        // turn into a killed viewer thread. The mark still has to arrive as
        // usual — cutting the deadline down has no right to sleep through
        // anything.
        let pulse = Arc::new(Pulse::new());
        let seen = pulse.beat();
        let writer = Arc::clone(&pulse);
        let hand = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(20));
            writer.data_written();
        });
        let started = Instant::now();
        let now = pulse.wait(seen, Duration::MAX);
        assert_ne!(
            now, seen,
            "the mark arrived and was not lost in the shortened deadline"
        );
        assert!(started.elapsed() < Duration::from_secs(5));
        hand.join().unwrap();
    }

    #[test]
    fn kinds_of_change_are_told_apart() {
        let pulse = Pulse::new();
        let start = pulse.beat();
        pulse.data_written();
        let after_data = pulse.beat();
        assert_eq!(
            after_data.shape, start.shape,
            "the directories did not change"
        );
        assert_ne!(after_data.data, start.data);

        pulse.shape_changed();
        let after_shape = pulse.beat();
        assert_eq!(
            after_shape.data, after_data.data,
            "there were no new blocks"
        );
        assert_ne!(after_shape.shape, after_data.shape);
    }

    #[test]
    fn a_sleeping_reader_is_woken_by_a_block() {
        let pulse = Arc::new(Pulse::new());
        let seen = pulse.beat();
        let writer = {
            let pulse = Arc::clone(&pulse);
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                pulse.data_written();
            })
        };
        // The timeout is knowably longer than the writer's pause: if a wake-up
        // is lost the test will not hang but return the previous state and fail
        // the assert.
        let now = pulse.wait(seen, Duration::from_secs(10));
        writer.join().unwrap();
        assert_ne!(
            now, seen,
            "a sleeper must be woken by the writer, not by the timeout"
        );
    }

    #[test]
    fn closing_wakes_everyone_and_stays_closed() {
        let pulse = Arc::new(Pulse::new());
        let seen = pulse.beat();
        assert!(!seen.closed);
        let sleeper = {
            let pulse = Arc::clone(&pulse);
            std::thread::spawn(move || pulse.wait(seen, Duration::from_secs(10)))
        };
        std::thread::sleep(Duration::from_millis(10));
        pulse.close();
        let now = sleeper.join().unwrap();
        assert!(now.closed, "stopping must wake those waiting");
        assert!(pulse.beat().closed, "stopping is irreversible");
    }
}
