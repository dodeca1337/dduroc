//! Format identifiers and timestamps.
//!
//! Identifiers are transparent newtypes over integers: the type checker
//! catches swapped arguments and the cost is nothing. A timestamp is
//! [`BootTime`], the pair "boot plus microseconds since it started": on their
//! own those two numbers are not a moment at all, and holding them in one type
//! is cheaper than chasing a comparison of microseconds from different boots
//! through a debugger.
//!
//! # Spaces
//!
//! `EventId` / `MetricId` / `SpanKindId` are u16, assigned **explicitly** in
//! the schema declaration. Auto-numbering is forbidden: positional ids
//! silently remap historical records onto the wrong decoders, which is the
//! trap the prototype fell into. Remapping is what migrations are for.
//!
//! # Metric numbering density is not a matter of taste
//!
//! `metric_id` goes into **every** telemetry sample as a varint: values below
//! 128 cost one byte, values from 128 up cost two. Telemetry is the most
//! frequent record there is, so the price of holes in the numbering is
//! measurable:
//!
//! | id layout | telemetry volume |
//! |---|---|
//! | dense from 1 | baseline (+0.1%) |
//! | grouped (`0x0101`, `0x0201`…) | **+10% raw, +15% after LZ4** |
//! | large (`0x4000`+) | **+23% raw, +29…33% after LZ4** |
//!
//! (Measured on the real codec: 150 metrics, one-second scan, one minute of
//! data. Compression does not rescue it — on noisy values the gap widens.)
//!
//! This is deliberately not enforced: after a few migrations that remove
//! metrics, holes arise legitimately, and forbidding them would forbid schema
//! evolution. But **number metrics densely from 1** — otherwise a byte is lost
//! on every sample, permanently.
//!
//! `event_id` and `span_kind_id` have no such sensitivity: a message carries a
//! payload anyway, and spans are rare.
//!
//! The design limits (256 message types, 160 metrics and so on) belong to
//! schema validation and engine sizing, not to the format.

use core::fmt;

/// Software boot counter. `u32` **everywhere** — the u16/u32 mismatch in the
/// prototype silently broke UTC conversion after 65536 boots.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct BootCounter(pub u32);

impl From<u32> for BootCounter {
    #[inline]
    fn from(raw: u32) -> Self {
        Self(raw)
    }
}

/// Microseconds since the current run started (`CLOCK_BOOTTIME` minus base).
///
/// A full u64: unlike the prototype, `boot_counter` is no longer packed into
/// the same 64 bits (it lives in the segment header), so neither the 48-bit
/// limit nor the overflow after ~8.9 years it brought with it exists here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Micros(pub u64);

/// A moment in relative time: a software boot plus microseconds since it
/// started.
///
/// On a device without an RTC a timestamp consists of **exactly these two
/// numbers**, and neither is a moment on its own: `Micros(500)` from two
/// different boots are two different moments, and comparing them is
/// meaningless. That is why they live in one type rather than as two fields
/// side by side.
///
/// # Ordering
///
/// The ordering is lexicographic — `boot` first, then `at` — and it coincides
/// with the chronological one: `boot_counter` grows with every software boot.
/// Hence the order of the fields in the declaration: the derived `Ord` reads
/// them top to bottom, and swapping them would silently turn the comparison
/// into garbage.
///
/// The same ordering gives the segment file its name (`<boot:08x>-
/// <micros:016x>`), which is why walking the directory lexicographically is
/// walking it in time order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct BootTime {
    /// The software boot in whose scale `at` is counted.
    pub boot: BootCounter,
    /// Microseconds since that boot started.
    pub at: Micros,
}

impl BootTime {
    #[inline]
    pub const fn new(boot: BootCounter, at: Micros) -> Self {
        Self { boot, at }
    }

    /// The same from bare numbers — for tests and for places where the
    /// widths have already been checked.
    #[inline]
    pub const fn from_raw(boot: u32, micros: u64) -> Self {
        Self {
            boot: BootCounter(boot),
            at: Micros(micros),
        }
    }
}

impl fmt::Display for BootTime {
    /// `#3 01:23:45.678_901` — the boot and the time since it started.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} {}", self.boot.0, self.at)
    }
}

/// Version of a namespace schema protocol. Grows monotonically; a migration is
/// a chain of `vN → vN+1` steps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ProtocolVersion(pub u16);

/// A message type within a namespace schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct EventId(pub u16);

/// A telemetry metric type within a namespace schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct MetricId(pub u16);

/// A span kind within a namespace schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SpanKindId(pub u16);

/// A span identifier, local to one run (the boot is implicit from the segment —
/// a span does not outlive a process restart).
///
/// The value `0` is reserved to mean "no span" and is not a valid `SpanId`:
/// the engine numbers from 1. The absence of a span is encoded by a flag (on
/// messages) or by zero (for `parent` in `SpanStart`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SpanId(pub u32);

impl SpanId {
    /// The "no span" sentinel in the wire representation of `parent`.
    pub const NONE_RAW: u32 = 0;

    /// `None` for the reserved zero.
    #[inline]
    pub const fn from_raw(raw: u32) -> Option<Self> {
        if raw == Self::NONE_RAW {
            None
        } else {
            Some(Self(raw))
        }
    }

    /// The inverse of [`Self::from_raw`]: `None` becomes 0.
    #[inline]
    pub const fn raw_or_none(this: Option<Self>) -> u32 {
        match this {
            Some(s) => s.0,
            None => Self::NONE_RAW,
        }
    }
}

macro_rules! impl_display {
    ($($t:ty),*) => { $(
        impl fmt::Display for $t {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
    )* };
}
impl_display!(
    BootCounter,
    ProtocolVersion,
    EventId,
    MetricId,
    SpanKindId,
    SpanId
);

impl Micros {
    /// Saturating difference — the basis for varint deltas: a negative step is
    /// impossible (records within a block are monotonic), and panicking on
    /// corrupt data is not acceptable.
    #[inline]
    pub const fn saturating_delta(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }

    #[inline]
    pub const fn checked_add_delta(self, delta: u64) -> Option<Self> {
        match self.0.checked_add(delta) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

impl fmt::Display for Micros {
    /// `01:23:45.678_901` — since the run started.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let us = self.0 % 1_000;
        let ms = (self.0 / 1_000) % 1_000;
        let total_s = self.0 / 1_000_000;
        let (h, m, s) = (total_s / 3600, (total_s % 3600) / 60, total_s % 60);
        write!(f, "{h:02}:{m:02}:{s:02}.{ms:03}_{us:03}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn span_id_zero_is_none() {
        assert_eq!(SpanId::from_raw(0), None);
        assert_eq!(SpanId::from_raw(1), Some(SpanId(1)));
        assert_eq!(SpanId::raw_or_none(None), 0);
        assert_eq!(SpanId::raw_or_none(Some(SpanId(7))), 7);
    }

    #[test]
    fn micros_delta_saturates() {
        assert_eq!(Micros(10).saturating_delta(Micros(4)), 6);
        // A non-monotonic input (corrupt data) does not panic.
        assert_eq!(Micros(4).saturating_delta(Micros(10)), 0);
        assert_eq!(Micros(u64::MAX).checked_add_delta(1), None);
    }

    #[test]
    fn boot_time_orders_chronologically() {
        // Lexicographic order of the pair equals chronological order:
        // boot_counter grows with every boot. This check pins the field order
        // down: swap the fields and the comparison starts comparing
        // microseconds from different boots against each other.
        let mut v = [
            BootTime::from_raw(1, 500),
            BootTime::from_raw(0, 900),
            BootTime::from_raw(1, 100),
            BootTime::from_raw(0, 100),
        ];
        v.sort();
        assert_eq!(
            v,
            [
                BootTime::from_raw(0, 100),
                BootTime::from_raw(0, 900),
                BootTime::from_raw(1, 100),
                BootTime::from_raw(1, 500),
            ]
        );
        // A later time in an earlier boot still comes first.
        assert!(BootTime::from_raw(0, u64::MAX) < BootTime::from_raw(1, 0));
    }

    #[test]
    fn boot_time_display() {
        assert_eq!(
            BootTime::from_raw(3, 3_723_456_789).to_string(),
            "#3 01:02:03.456_789"
        );
    }

    #[test]
    fn micros_display() {
        assert_eq!(Micros(0).to_string(), "00:00:00.000_000");
        assert_eq!(
            Micros(3_723_456_789).to_string(),
            "01:02:03.456_789",
            "1h 2m 3.456789s"
        );
    }
}
