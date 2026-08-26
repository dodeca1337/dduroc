//! The source of time.
//!
//! Hardware without an RTC does not know absolute time, so an event's stamp is
//! `(boot_counter, µs since the run started)`. The source is `CLOCK_BOOTTIME`:
//! unlike `CLOCK_MONOTONIC` it keeps running across suspend, so it measures an
//! event's real age rather than CPU uptime.
//!
//! Absolute time appears only at read time, and only if a UTC anchor was
//! recorded for that hardware boot (see [`crate::epochs`]).

use dduroc_format::{BootCounter, BootTime, Micros};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Read `CLOCK_BOOTTIME` in microseconds.
pub fn boottime_us() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
}

/// The clock of one run: it turns `CLOCK_BOOTTIME` into microseconds since
/// the start.
///
/// It guarantees the values it hands out are **non-decreasing**.
/// `CLOCK_BOOTTIME` is monotonic by kernel contract, but `Clock` additionally
/// pins the last value handed out: an NTP adjustment does not affect
/// BOOTTIME, but a driver or virtualization bug can produce a step backwards,
/// which in the format would collapse deltas and lose resolution.
#[derive(Debug, Clone)]
pub struct Clock {
    inner: Arc<ClockInner>,
}

#[derive(Debug)]
struct ClockInner {
    /// The run in whose scale the counting happens.
    boot: BootCounter,
    /// `CLOCK_BOOTTIME` at the moment the run was registered.
    base_us: u64,
    last: AtomicU64,
}

impl Clock {
    /// A clock counting from this moment.
    pub fn start(boot: BootCounter) -> Self {
        Self::with_base(boot, boottime_us())
    }

    /// A clock with an explicit base — used when registering a run so that the
    /// clock's base and `boottime_at_init_us` in `epochs.bin` are **the same**
    /// value. In the prototype they came from independent calls, and the UTC
    /// conversion drifted by the difference between them.
    pub fn with_base(boot: BootCounter, base_us: u64) -> Self {
        Self {
            inner: Arc::new(ClockInner {
                boot,
                base_us,
                last: AtomicU64::new(0),
            }),
        }
    }

    /// The `CLOCK_BOOTTIME` value the counting starts from.
    pub fn base_us(&self) -> u64 {
        self.inner.base_us
    }

    /// The run the stamps handed out belong to.
    pub fn boot(&self) -> BootCounter {
        self.inner.boot
    }

    /// The current time since the run started.
    ///
    /// This is exactly what records store: their run is implicit from the
    /// segment header, and there is no reason to repeat it in every record.
    pub fn now(&self) -> Micros {
        let raw = boottime_us().saturating_sub(self.inner.base_us);
        Micros(self.inner.last.fetch_max(raw, Ordering::Relaxed).max(raw))
    }

    /// The full current moment — in the same coordinates records arrive in from
    /// a reader.
    pub fn now_at(&self) -> BootTime {
        BootTime::new(self.inner.boot, self.now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boottime_is_monotonic() {
        let a = boottime_us();
        let b = boottime_us();
        assert!(b >= a, "CLOCK_BOOTTIME must not run backwards: {a} → {b}");
        assert!(
            a > 0,
            "the system has been up for more than zero microseconds"
        );
    }

    #[test]
    fn starts_near_zero_and_advances() {
        let clock = Clock::start(BootCounter(7));
        let t0 = clock.now();
        assert!(t0.0 < 1_000_000, "counting starts close to zero: {t0}");

        // A short busy pause — no sleep is needed in tests.
        let mut spin = 0u64;
        while clock.now().0 == t0.0 && spin < 50_000_000 {
            spin += 1;
        }
        assert!(clock.now().0 >= t0.0);

        // The full moment carries the run: without it a stamp is not comparable
        // to another's.
        let at = clock.now_at();
        assert_eq!(at.boot, BootCounter(7));
        assert!(at.at >= t0);
    }

    #[test]
    fn never_goes_backwards_across_threads() {
        let clock = Clock::with_base(BootCounter(3), boottime_us());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = clock.clone();
            handles.push(std::thread::spawn(move || {
                let mut prev = Micros(0);
                for _ in 0..2_000 {
                    let now = c.now();
                    assert!(now.0 >= prev.0, "time went backwards: {prev} → {now}");
                    prev = now;
                }
                prev
            }));
        }
        for h in handles {
            h.join().expect("the thread must not panic");
        }
    }

    #[test]
    fn base_in_future_clamps_to_zero() {
        // A base "from the future" (an epochs.bin from another machine, say)
        // must not lead to an overflow — only to a zero time.
        let clock = Clock::with_base(BootCounter(0), boottime_us() + 60_000_000);
        assert_eq!(clock.now(), Micros(0));
    }
}
