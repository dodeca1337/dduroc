//! Счётчики движка.
//!
//! Единственный способ узнать, что происходит внутри: логировать свою работу
//! writer не может — вызов публичного пути записи из его собственного потока
//! означал бы ожидание места в очереди, освободить которую может только он
//! сам. Поэтому диагностика — атомарные счётчики, снимаемые снаружи.

use std::sync::atomic::{AtomicU64, Ordering};

/// Атомарные счётчики. Все операции — `Relaxed`: счётчики ничего не
/// упорядочивают, а лишние барьеры на armv7 заметны.
#[derive(Debug, Default)]
pub struct Counters {
    pub records_written: AtomicU64,
    pub blocks_written: AtomicU64,
    pub bytes_written: AtomicU64,
    pub syncs: AtomicU64,
    pub segments_created: AtomicU64,
    pub segments_sealed: AtomicU64,
    pub segments_rotated: AtomicU64,
    /// Записи, отброшенные из-за переполнения очереди обычного канала.
    pub dropped: AtomicU64,
    /// Сколько раз запись в критический канал ждала места в очереди.
    pub backpressure_waits: AtomicU64,
    /// Ошибки ввода-вывода в writer'е.
    pub io_errors: AtomicU64,
    /// Повреждённые хвосты, отброшенные при восстановлении.
    pub recovered_tails: AtomicU64,
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
            backpressure_waits: g(&self.backpressure_waits),
            io_errors: g(&self.io_errors),
            recovered_tails: g(&self.recovered_tails),
        }
    }
}

/// Снимок счётчиков.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Stats {
    pub records_written: u64,
    pub blocks_written: u64,
    pub bytes_written: u64,
    pub syncs: u64,
    pub segments_created: u64,
    pub segments_sealed: u64,
    pub segments_rotated: u64,
    pub dropped: u64,
    pub backpressure_waits: u64,
    pub io_errors: u64,
    pub recovered_tails: u64,
}

impl Stats {
    /// Всё ли благополучно: ничего не потеряно и не сломалось.
    pub fn is_clean(&self) -> bool {
        self.dropped == 0 && self.io_errors == 0
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
        assert!(!s.is_clean(), "потери обязаны быть видны");
        assert!(Stats::default().is_clean());
    }
}
