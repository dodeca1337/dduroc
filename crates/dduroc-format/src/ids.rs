//! Идентификаторы и метки времени формата.
//!
//! Все — прозрачные newtype'ы над целыми: типизация ловит перепутанные
//! аргументы, стоимость нулевая.
//!
//! # Пространства
//!
//! `EventId` / `MetricId` / `SpanKindId` — u16, назначаются **явно** в
//! декларации схемы (авто-нумерация запрещена: позиционные id молча
//! перемапливают исторические записи на чужие декодеры — грабли прототипа).
//! Ремапы делаются миграциями. Формат к плотности нумерации безразличен —
//! varint кодирует id < 128 одним байтом, дырки ничего не стоят.
//!
//! Расчётные пределы (256 типов сообщений, 160 метрик и т.п.) —
//! предмет валидации в схеме и sizing'а в движке, не в формате.

use core::fmt;

/// Счётчик запусков ПО. `u32` **везде** — рассинхрон u16/u32 из прототипа
/// приводил к тихой поломке конверсии в UTC после 65536 запусков.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct BootCounter(pub u32);

/// Микросекунды от старта текущего run'а (`CLOCK_BOOTTIME` минус базa).
///
/// Полный u64: в отличие от прототипа, `boot_counter` больше не упакован
/// в те же 64 бита (он живёт в заголовке сегмента), поэтому 48-битного
/// ограничения и связанного с ним переполнения через ~8.9 лет нет.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Micros(pub u64);

/// Версия протокола схемы неймспейса. Монотонно растёт, миграции —
/// цепочка шагов `vN → vN+1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct ProtocolVersion(pub u16);

/// Тип сообщения в пределах схемы неймспейса.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct EventId(pub u16);

/// Тип метрики телеметрии в пределах схемы неймспейса.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct MetricId(pub u16);

/// Вид спана в пределах схемы неймспейса.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SpanKindId(pub u16);

/// Локальный идентификатор серии **в пределах сегмента**: нумерация с нуля,
/// по порядку появления. Определение серии (`SeriesDef`) пишется при первом
/// сэмпле в сегменте и дублируется в footer — сегмент самодостаточен, FIFO-
/// ротация не рвёт ссылок.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SeriesLocal(pub u32);

/// Идентификатор спана, локальный для run'а (boot имплицитен из сегмента —
/// спан не переживает перезапуск процесса).
///
/// Значение `0` зарезервировано как «спана нет» и допустимым `SpanId` не
/// является: движок нумерует с 1. Отсутствие спана в записи кодируется
/// флагом (у сообщений) либо нулём (у `parent` в `SpanStart`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SpanId(pub u32);

impl SpanId {
    /// Сентинел «спана нет» в wire-представлении `parent`.
    pub const NONE_RAW: u32 = 0;

    /// `None` для зарезервированного нуля.
    #[inline]
    pub const fn from_raw(raw: u32) -> Option<Self> {
        if raw == Self::NONE_RAW {
            None
        } else {
            Some(Self(raw))
        }
    }

    /// Обратное к [`Self::from_raw`]: `None` → 0.
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
    SeriesLocal,
    SpanId
);

impl Micros {
    /// Насыщающая разность — база для varint-дельт: отрицательный шаг
    /// невозможен (записи в блоке монотонны), а паника на битых данных
    /// недопустима.
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
    /// `01:23:45.678_901` — от старта run'а.
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
        // Немонотонный вход (битые данные) не паникует.
        assert_eq!(Micros(4).saturating_delta(Micros(10)), 0);
        assert_eq!(Micros(u64::MAX).checked_add_delta(1), None);
    }

    #[test]
    fn micros_display() {
        assert_eq!(Micros(0).to_string(), "00:00:00.000_000");
        assert_eq!(
            Micros(3_723_456_789).to_string(),
            "01:02:03.456_789",
            "1ч 2м 3.456789с"
        );
    }
}
