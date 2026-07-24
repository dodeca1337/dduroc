//! Источник времени.
//!
//! Оборудование без RTC не знает абсолютного времени, поэтому метка события —
//! это `(boot_counter, µs от старта run'а)`. Источник — `CLOCK_BOOTTIME`:
//! в отличие от `CLOCK_MONOTONIC` он продолжает идти во время suspend'а, то
//! есть измеряет реальный возраст события, а не время работы CPU.
//!
//! Абсолютное время появляется только при чтении и только если для этого
//! hardware-boot'а был зафиксирован UTC-якорь (см. [`crate::epochs`]).

use dduroc_format::Micros;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Прочитать `CLOCK_BOOTTIME` в микросекундах.
pub fn boottime_us() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::Boottime);
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
}

/// Часы одного run'а: превращают `CLOCK_BOOTTIME` в микросекунды от старта.
///
/// Гарантирует **неубывание** выдаваемых значений. `CLOCK_BOOTTIME` монотонен
/// по контракту ядра, но `Clock` дополнительно фиксирует последнее выданное
/// значение: NTP-подкрутка не влияет на BOOTTIME, а вот баг драйвера или
/// виртуализации способен дать откат, который в формате обернулся бы
/// схлопыванием дельт и потерей разрешения.
#[derive(Debug, Clone)]
pub struct Clock {
    inner: Arc<ClockInner>,
}

#[derive(Debug)]
struct ClockInner {
    /// `CLOCK_BOOTTIME` на момент регистрации run'а.
    base_us: u64,
    last: AtomicU64,
}

impl Clock {
    /// Часы, отсчитывающие от текущего момента.
    pub fn start() -> Self {
        Self::with_base(boottime_us())
    }

    /// Часы с явной базой — используется при регистрации run'а, чтобы база
    /// часов и `boottime_at_init_us` в `epochs.bin` были **одним и тем же**
    /// значением. В прототипе они брались независимыми вызовами, и конверсия
    /// в UTC уезжала на разницу между ними.
    pub fn with_base(base_us: u64) -> Self {
        Self {
            inner: Arc::new(ClockInner {
                base_us,
                last: AtomicU64::new(0),
            }),
        }
    }

    /// `CLOCK_BOOTTIME`, от которого ведётся отсчёт.
    pub fn base_us(&self) -> u64 {
        self.inner.base_us
    }

    /// Текущая метка времени.
    pub fn now(&self) -> Micros {
        let raw = boottime_us().saturating_sub(self.inner.base_us);
        Micros(self.inner.last.fetch_max(raw, Ordering::Relaxed).max(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn boottime_is_monotonic() {
        let a = boottime_us();
        let b = boottime_us();
        assert!(b >= a, "CLOCK_BOOTTIME не должен идти назад: {a} → {b}");
        assert!(a > 0, "система загружена дольше нуля микросекунд");
    }

    #[test]
    fn starts_near_zero_and_advances() {
        let clock = Clock::start();
        let t0 = clock.now();
        assert!(t0.0 < 1_000_000, "старт отсчёта близок к нулю: {t0}");

        // Небольшая занятая пауза — sleep в тестах не нужен.
        let mut spin = 0u64;
        while clock.now().0 == t0.0 && spin < 50_000_000 {
            spin += 1;
        }
        assert!(clock.now().0 >= t0.0);
    }

    #[test]
    fn never_goes_backwards_across_threads() {
        let clock = Clock::with_base(boottime_us());
        let mut handles = Vec::new();
        for _ in 0..4 {
            let c = clock.clone();
            handles.push(std::thread::spawn(move || {
                let mut prev = Micros(0);
                for _ in 0..2_000 {
                    let now = c.now();
                    assert!(now.0 >= prev.0, "откат времени: {prev} → {now}");
                    prev = now;
                }
                prev
            }));
        }
        for h in handles {
            h.join().expect("поток не должен паниковать");
        }
    }

    #[test]
    fn base_in_future_clamps_to_zero() {
        // База «из будущего» (например, epochs.bin с чужой машины) не должна
        // приводить к переполнению — только к нулевому времени.
        let clock = Clock::with_base(boottime_us() + 60_000_000);
        assert_eq!(clock.now(), Micros(0));
    }
}
