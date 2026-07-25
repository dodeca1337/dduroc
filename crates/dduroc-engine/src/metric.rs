//! Метрика вместе с типом своего значения.
//!
//! Тип отсчёта — свойство метрики, объявленное в схеме, а не свойство
//! отдельной записи. Значит проверять его должен компилятор, а не движок в
//! рантайме: раньше `sample_f32` рядом с метрикой, объявленной как `u64`,
//! собирался и падал ошибкой на устройстве.
//!
//! Поэтому константа метрики несёт тип значения — [`Metric<T>`], — а ряд,
//! открытый по ней, принимает только его:
//!
//! ```text
//! metrics::TempPa: Metric<f32>          series(TempPa)?.sample(36.6)   ✓
//!                                       series(TempPa)?.sample(36u64)  ошибка компиляции
//! metrics::LinkState: Metric<LinkState>  series(LinkState)?.sample(LinkState::Lock) ✓
//! ```
//!
//! Параметр — маркер, а не хранимое значение, поэтому [`Metric`] остаётся
//! `Copy` и укладывается в те же четыре байта, что и `MetricId`.

use crate::staged::OwnedValue;
use dduroc_format::MetricId;
use std::marker::PhantomData;

/// Метрика, объявленная со типом значения `T`.
///
/// Порождается макросом схемы; вручную нужна только тем, кто описывает схему
/// без макроса.
pub struct Metric<T> {
    id: MetricId,
    /// `fn() -> T`, а не `T`: маркер не влияет ни на `Copy`, ни на `Send`,
    /// ни на вариантность — метрика не хранит значение.
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

/// Маркер метрики с двоичным значением (`vtype: blob`): спектр, дамп регистра.
///
/// Отдельный тип, потому что принимаемых Rust-типов у неё несколько
/// (`&[u8]`, `Vec<u8>`), а объявленный — один.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Blob;

/// Маркер ряда, открытого по идентификатору из рантайма.
///
/// Типа значения на этапе компиляции нет — его знает только схема, — поэтому
/// такой ряд принимает лишь [`Series::sample_raw`]. Нужен веб-слою и
/// миграциям: там метрика приходит строкой из запроса.
///
/// [`Series::sample_raw`]: crate::namespace::Series::sample_raw
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Untyped;

/// Значение, допустимое для метрики, объявленной как `M`.
///
/// Реализации пишет макрос схемы (для перечислений) и этот модуль (для
/// встроенных типов). Своих реализаций прикладному коду не нужно.
#[diagnostic::on_unimplemented(
    message = "у этой метрики объявлен другой тип значения: `{Self}` не подходит",
    label = "здесь нужно значение того типа, что стоит в схеме",
    note = "тип отсчёта — свойство метрики (`vtype:` в объявлении), а не отдельной записи: \
            у метрики-перечисления это её собственные состояния, у `vtype: blob` — байты",
    note = "если тип действительно должен измениться, это правка схемы и миграция, \
            а не приведение на месте вызова"
)]
pub trait MetricValue<M> {
    /// Перевести в форму, которая уйдёт в очередь записи.
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
        OwnedValue::Blob(self.as_slice().into())
    }
}

/// Состояние метрики-перечисления, порождённое макросом схемы.
///
/// Даёт человекочитаемое имя и код, уходящий на диск. Принадлежность метрике
/// проверяет уже не он, а тип ряда: `Series<LinkState>` не примет состояние
/// другой метрики.
pub trait MetricState: Copy {
    /// Метрика, которой принадлежит это перечисление.
    fn metric() -> MetricId;
    /// Код, попадающий на диск.
    fn code(self) -> u64;
    /// Имя состояния из схемы.
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
            "маркер типа не должен занимать места"
        );
        // Copy и в generic-контексте: маркер не участвует в автотрейтах.
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
