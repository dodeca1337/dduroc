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
    /// Записи, отвергнутые как нарушающие контракт схемы: id из чужой схемы,
    /// значение не того типа. Дефект сборки, а не следствие нагрузки, поэтому
    /// счётчик отдельный — смешать его с потерями значило бы спрятать баг за
    /// «диск не успевает».
    pub rejected: AtomicU64,
    /// Сколько раз запись в критический канал ждала места в очереди.
    pub backpressure_waits: AtomicU64,
    /// Ошибки ввода-вывода в writer'е.
    pub io_errors: AtomicU64,
    /// Повреждённые хвосты, отброшенные при восстановлении.
    pub truncated_tails: AtomicU64,
    /// Сколько раз канал брал сегмент в работу: создавал новый **или**
    /// возвращал отпущенный по бездействию.
    ///
    /// Наружу не отдаётся — это сторож пересчёта суммарного бюджета. Занятость
    /// носителя растёт ровно в этих двух случаях (создание резервирует полный
    /// размер, возвращение — восстанавливает обрезанную преаллокацию), а всё
    /// остальное её уменьшает. Пока счётчик не сдвинулся, обходить весь флот
    /// ради суммы незачем.
    pub segments_opened: AtomicU64,
    /// Сколько раз хранилище не удалось вернуть в объявленный суммарный
    /// потолок.
    ///
    /// Вытеснять можно только запечатанные сегменты: в активный пишут, и его
    /// преаллокация — не расточительство, а единственная гарантия того, что
    /// ENOSPC придёт при создании сегмента, а не посреди аварийного события.
    /// Поэтому потолок обязан быть больше, чем `segment_bytes` × число
    /// одновременно пишущих каналов; иначе он невыполним, и об этом лучше
    /// узнать по счётчику, чем по кончившемуся месту.
    pub budget_overruns: AtomicU64,
    /// Сколько раз буферы блоков не удалось вернуть в объявленный потолок
    /// памяти.
    ///
    /// Потолок нельзя сделать жёстким, не теряя данных: одна запись бывает
    /// крупнее любого разумного потолка (несжимаемый blob), а буфер блока
    /// обязан вместить хотя бы её. Поэтому потолок соблюдается освобождением
    /// того, что освободить можно, а невыполнимость объявляется счётчиком —
    /// это честнее, чем отбросить запись ради бухгалтерии по памяти.
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

    /// То же, но с публикацией всего, что записано до вызова.
    ///
    /// Нужно ровно одному счётчику и ровно на одном переходе: writer решает,
    /// обходить ли каналы за отметками о потерях, по изменению общего
    /// счётчика — обход всего флота на каждом обороте цикла стоит слишком
    /// дорого. Поканальный счётчик при этом растёт на прикладном потоке, и с
    /// `Relaxed` writer мог бы увидеть новый общий счётчик раньше
    /// поканального: обход нашёл бы ноль, отметку счёл бы выданной, и дыра
    /// осталась бы необъявленной до следующей потери.
    ///
    /// Поэтому порядок обязателен: сначала поканальный счётчик, потом этот
    /// вызов. Барьер платится только на пути потери — то есть тогда, когда
    /// запись всё равно уже не состоялась.
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

/// Снимок счётчиков.
///
/// Структура **открытая**: счётчик добавляется всякий раз, когда движок
/// научается замечать что-то новое, и это не должно ломать сборку тем, кто их
/// только читает.
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
    /// Отвергнутые как нарушающие контракт схемы — см. одноимённое поле
    /// [`Counters`].
    pub rejected: u64,
    pub backpressure_waits: u64,
    pub io_errors: u64,
    pub truncated_tails: u64,
    /// Хранилище не влезло в объявленный суммарный потолок — см. одноимённое
    /// поле [`Counters`].
    pub budget_overruns: u64,
    /// Буферы блоков не влезли в объявленный потолок памяти — см. одноимённое
    /// поле [`Counters`].
    pub buffer_overruns: u64,
}

impl Stats {
    /// Всё ли благополучно: ничего не потеряно, не отвергнуто и не сломалось.
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
        assert!(!s.is_clean(), "потери обязаны быть видны");
        assert!(Stats::default().is_clean());

        // Нарушение контракта учитывается отдельно от потерь: причины разные,
        // и реакция на них разная.
        let c = Counters::default();
        Counters::bump(&c.rejected);
        let s = c.snapshot();
        assert_eq!(s.dropped, 0);
        assert_eq!(s.rejected, 1);
        assert!(!s.is_clean(), "отвергнутая запись обязана быть видна");
    }
}
