//! A metric together with the type of its value.
//!
//! The type of a sample is a property of the metric, declared in the schema,
//! not a property of an individual record. So it is the compiler's job to
//! check it, not the engine's at runtime: `sample_f32` next to a metric
//! declared as `u64` used to compile and then fail with an error on the
//! device.
//!
//! That is why a metric constant carries the value type — [`Metric<T>`] — and
//! a series opened from it accepts only that type:
//!
//! ```text
//! metrics::TempPa: Metric<f32>          series(TempPa)?.sample(36.6)   ✓
//!                                       series(TempPa)?.sample(36u64)  does not compile
//! metrics::LinkState: Metric<LinkState>  series(LinkState)?.sample(LinkState::Lock) ✓
//! ```
//!
//! The parameter is a marker rather than a stored value, so [`Metric`] stays
//! `Copy` and fits in the same four bytes as `MetricId`.

use crate::staged::OwnedValue;
use dduroc_format::MetricId;
use std::marker::PhantomData;

/// A metric declared with value type `T`.
///
/// Produced by the schema macro; needed by hand only by those who describe a
/// schema without the macro.
pub struct Metric<T> {
    id: MetricId,
    /// `fn() -> T` rather than `T`: the marker affects neither `Copy` nor
    /// `Send` nor variance — a metric stores no value.
    _value: PhantomData<fn() -> T>,
}

impl<T> Metric<T> {
    pub const fn new(id: MetricId) -> Self {
        Self {
            id,
            _value: PhantomData,
        }
    }

    pub const fn id(self) -> MetricId {
        self.id
    }
}

impl<T> Clone for Metric<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Metric<T> {}

impl<T> PartialEq for Metric<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for Metric<T> {}

impl<T> std::fmt::Debug for Metric<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Metric({})", self.id)
    }
}

impl<T> From<Metric<T>> for MetricId {
    fn from(m: Metric<T>) -> Self {
        m.id
    }
}

/// The marker of a metric with a binary value (`type: blob`): a spectrum, a
/// register dump.
///
/// A separate type, because it accepts several Rust types (`&[u8]`,
/// `Vec<u8>`) while declaring one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blob;

/// The marker of a series opened by a runtime identifier.
///
/// There is no compile-time value type — only the schema knows it — so such a
/// series accepts [`Series::sample_raw`] alone. Needed by the web layer and by
/// migrations, where a metric arrives as a string in a request.
///
/// [`Series::sample_raw`]: crate::namespace::Series::sample_raw
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Untyped;

/// A value admissible for a metric declared as `M`.
///
/// The implementations are written by the schema macro (for enums) and by this
/// module (for built-in types). Application code needs none of its own.
#[diagnostic::on_unimplemented(
    message = "this metric is declared with a different value type: `{Self}` does not fit",
    label = "a value of the type named in the schema is needed here",
    note = "a sample's type is a property of the metric (`type:` in the declaration), not of \
            an individual record: for an enum metric it is that metric's own states, for \
            `type: blob` it is bytes",
    note = "if the type really has to change, that is a schema edit and a migration, \
            not a cast at the call site"
)]
pub trait MetricValue<M> {
    /// Convert into the form that goes into the write queue.
    fn into_owned(self) -> OwnedValue;
}

macro_rules! impl_scalar {
    ($($t:ty => $variant:ident),* $(,)?) => { $(
        impl MetricValue<$t> for $t {
            #[inline]
            fn into_owned(self) -> OwnedValue {
                OwnedValue::$variant(self)
            }
        }
    )* };
}

impl_scalar!(f32 => F32, f64 => F64, i64 => I64, u64 => U64, bool => Bool);

impl MetricValue<Blob> for &[u8] {
    #[inline]
    fn into_owned(self) -> OwnedValue {
        OwnedValue::Blob(self.into())
    }
}

impl MetricValue<Blob> for Vec<u8> {
    #[inline]
    fn into_owned(self) -> OwnedValue {
        // Ownership moves without a copy: the vector's buffer becomes the
        // SmallVec's heap storage. The former `as_slice().into()` copied a
        // megabyte spectrum snapshot whole — a superfluous copy and a
        // superfluous spike of transient memory on every blob sample.
        OwnedValue::Blob(crate::staged::Payload::from_vec(self))
    }
}

/// A numeric metric value: bounds can be set only where values are ordered.
///
/// Implemented for the built-in numbers and **not** implemented for state
/// enums, `bool` or [`Blob`]: "above the threshold" does not apply to them.
/// The runtime used to catch this — `set_thresholds` on an enum metric
/// compiled and then failed with an error on the device, although the value
/// type is known from the metric constant right at the call site. The name
/// belongs to a family of neighbours: [`MetricValue`] is a value,
/// [`MetricState`] is a state.
///
/// Sealed: the set of numeric types is closed by the schema (`type:` in the
/// declaration), and an outside implementation would mean bypassing that very
/// check.
#[diagnostic::on_unimplemented(
    message = "bounds can be set only on a numeric metric: `{Self}` is not a number",
    label = "a metric with a numeric `type:` from the schema is needed here",
    note = "for an enum metric severity comes from the state, not from a range: \
            set it through MetricLimits::states",
    note = "bounds do not apply to a binary value (`type: blob`) at all"
)]
pub trait NumericValue: Copy + sealed::Sealed {
    /// A bound in the form the engine stores it in.
    fn into_f64(self) -> f64;
}

mod sealed {
    pub trait Sealed {}
}

macro_rules! impl_numeric {
    ($($t:ty),* $(,)?) => { $(
        impl sealed::Sealed for $t {}
        impl NumericValue for $t {
            #[inline]
            fn into_f64(self) -> f64 {
                self as f64
            }
        }
    )* };
}

impl_numeric!(f32, f64, i64, u64);

/// A state of an enum metric, produced by the schema macro.
///
/// It gives a human-readable name and the code that goes to disk. Belonging to
/// a metric is checked not by it but by the series type: a `Series<LinkState>`
/// will not accept a state of another metric.
pub trait MetricState: Copy {
    /// The metric this enum belongs to.
    fn metric() -> MetricId;
    /// The code that reaches the disk.
    fn code(self) -> u64;
    /// The state's name from the schema.
    fn name(self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::ValueType;

    #[test]
    fn marker_costs_nothing() {
        assert_eq!(
            std::mem::size_of::<Metric<f32>>(),
            std::mem::size_of::<MetricId>(),
            "the type marker must take no space"
        );
        // Copy in a generic context too: the marker takes no part in auto
        // traits.
        fn takes_copy<T: Copy>(_: T) {}
        takes_copy(Metric::<Vec<u8>>::new(MetricId(1)));
    }

    #[test]
    fn values_convert_to_declared_types() {
        assert_eq!(
            MetricValue::<f32>::into_owned(1.5f32).value_type(),
            ValueType::F32
        );
        assert_eq!(
            MetricValue::<bool>::into_owned(true).value_type(),
            ValueType::Bool
        );
        assert_eq!(
            MetricValue::<Blob>::into_owned(&[1u8, 2][..]).value_type(),
            ValueType::Blob
        );
        assert_eq!(
            MetricValue::<Blob>::into_owned(vec![1u8, 2]).value_type(),
            ValueType::Blob
        );
    }
}
