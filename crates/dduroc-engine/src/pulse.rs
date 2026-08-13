//! Отметка хранилища о том, что смотреть снова есть смысл.
//!
//! Читатель живого хранилища видит записи опросом: открывает сегмент и читает
//! то, что уже легло на носитель. Без отметки «смотреть снова» подписка на
//! поток выродилась бы в опрос по таймеру — либо частый (процессор и обращения
//! к носителю впустую, а на приборе это ещё и износ флеша), либо редкий (живой
//! график отстаёт на период опроса). Отметка убирает выбор: читатель спит,
//! пока писать нечего, и просыпается на первом же блоке.
//!
//! Отметок три, и различаются они ценой ответа:
//!
//! - **данные** ([`Pulse::data_written`]) — в файл лёг блок. Чтобы его увидеть,
//!   хватает дочитать сегмент, который уже открыт: ни одного `readdir`;
//! - **устройство канала** ([`Pulse::shape_changed`]) — сегмент создан,
//!   запечатан или вытеснен. Читателю приходится перечислить каталог **того**
//!   канала, который он и так держит открытым;
//! - **состав хранилища** ([`Pulse::roster_changed`]) — поднялся неймспейс,
//!   а с ним каталоги его каналов. Вот это и есть дорогой ответ: обойти
//!   корень, прочитать `ns-meta` у каждого подходящего каталога и завести
//!   курсоры новичкам — работа, пропорциональная всему хранилищу.
//!
//! Одной отметкой это не выражается: блоки идут тысячами, сегменты сменяются
//! минутами, а неймспейс поднимается раз за жизнь сервиса. Общий счётчик
//! заставлял бы перечислять каталоги на каждый блок — а слитые вместе
//! «сегмент» и «неймспейс» заставляли бы обходить всё хранилище на каждую
//! ротацию, то есть постоянно и впустую.
//!
//! Писатель за отметку не платит, пока никто не подписан: поднять счётчик —
//! одна атомарная операция, а будить некого. Стоимость появляется вместе с
//! первым ожидающим.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

/// Во что урезается срок, который часы не умеют сложить.
///
/// Час — заведомо больше любого осмысленного периода опроса и заведомо
/// меньше того, на чём переполняется [`Instant`].
pub const LONGEST_WAIT: Duration = Duration::from_secs(3600);

/// Что читатель уже видел.
///
/// Сравнивается целиком: изменилось что угодно — значит, смотреть снова есть
/// смысл. Отдельные поля читателю нужны, только чтобы решить, обходить ли
/// каталоги.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct Beat {
    /// Поколение данных: растёт с каждым блоком, легшим в файл.
    pub data: u64,
    /// Поколение устройства каналов: сегмент создан, взят в работу,
    /// запечатан, вытеснен.
    pub shape: u64,
    /// Поколение состава хранилища: поднялся неймспейс со своими каналами.
    pub roster: u64,
    /// Хранилище остановлено — нового не будет.
    pub closed: bool,
}

/// Отметки живого хранилища.
///
/// Живёт в [`Store`](crate::store::Store) и держится writer'ом; читатель
/// спрашивает у хранилища.
#[derive(Debug, Default)]
pub struct Pulse {
    data: AtomicU64,
    shape: AtomicU64,
    roster: AtomicU64,
    closed: AtomicBool,
    /// Сколько читателей сейчас спит. Ноль означает, что будить некого, и
    /// писатель не берёт мьютекс вовсе.
    waiters: AtomicUsize,
    /// Не хранит ничего: нужен только Condvar'у. Состояние — в атомиках, и
    /// читаются они без него.
    lock: Mutex<()>,
    wake: Condvar,
}

impl Pulse {
    pub fn new() -> Self {
        Self::default()
    }

    /// Что есть сейчас.
    pub fn beat(&self) -> Beat {
        Beat {
            data: self.data.load(Ordering::SeqCst),
            shape: self.shape.load(Ordering::SeqCst),
            roster: self.roster.load(Ordering::SeqCst),
            closed: self.closed.load(Ordering::SeqCst),
        }
    }

    /// Блок лёг в файл: дочитавшему хвост есть что забрать.
    pub fn data_written(&self) {
        self.data.fetch_add(1, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// Сменился сегмент: каталог канала надо перечислить снова.
    pub fn shape_changed(&self) {
        self.shape.fetch_add(1, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// Поднялся неймспейс: в хранилище появились каталоги, о которых читатель
    /// не знает.
    ///
    /// Отдельно от [`Pulse::shape_changed`] потому, что ответ на неё —
    /// единственная работа, пропорциональная **всему** хранилищу: обход корня
    /// и чтение `ns-meta` у каждого подходящего каталога. Ротация поднимает
    /// отметку устройства постоянно, и, будь она той же самой, подписка
    /// перечисляла бы двадцать четыре тысячи каталогов ради сегмента, который
    /// сменился в одном-единственном канале.
    pub fn roster_changed(&self) {
        self.roster.fetch_add(1, Ordering::SeqCst);
        self.wake_waiters();
    }

    /// Хранилище остановлено: ожидающим больше нечего ждать.
    ///
    /// Без этого подписка на остановленном хранилище висела бы до таймаута и
    /// выглядела бы как «прибор молчит», хотя писать уже некому.
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
        // Безусловно: закрытие случается один раз, а проспать его нельзя.
        let _guard = self.lock.lock().unwrap_or_else(|e| e.into_inner());
        self.wake.notify_all();
    }

    /// Дождаться отличия от `seen`, но не дольше `timeout`.
    ///
    /// Возвращает текущее состояние: равное `seen` означает, что ничего не
    /// произошло за отведённое время.
    ///
    /// Срок, который часы не умеют сложить (`Duration::MAX` и всё, что близко
    /// к нему), урезается до [`LONGEST_WAIT`]: паника на сложении была бы
    /// худшим из возможных ответов на просьбу подождать подольше, а спросить
    /// снова подписке ничего не стоит.
    pub fn wait(&self, seen: Beat, timeout: Duration) -> Beat {
        // Ожидающий объявляется ДО чтения счётчиков, а писатель поднимает
        // счётчик ДО чтения числа ожидающих. В общем порядке SeqCst хотя бы
        // одна из сторон видит другую, поэтому отметка, выставленная в зазоре
        // между проверкой и засыпанием, не теряется. Порядок послабее здесь
        // означал бы подписку, которая изредка спит до таймаута на потоке
        // записей, идущем полным ходом.
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
        // Мьютекс берётся именно перед пробуждением: ожидающий держит его и
        // при проверке счётчика, и внутри `wait_timeout`, поэтому сюда мы
        // войдём либо до его проверки, либо когда он уже спит, — но не между.
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
        assert_eq!(now, seen, "ничего не произошло — состояние прежнее");
        assert!(started.elapsed() >= Duration::from_millis(25));
    }

    #[test]
    fn a_beat_that_happened_before_the_wait_is_not_slept_through() {
        // Отметка, выставленная до захода в ожидание, обязана прочитаться
        // сразу: иначе подписка спала бы на уже написанных данных.
        let pulse = Pulse::new();
        let seen = pulse.beat();
        pulse.data_written();
        let started = Instant::now();
        let now = pulse.wait(seen, Duration::from_secs(30));
        assert_ne!(now, seen);
        assert!(started.elapsed() < Duration::from_secs(1), "не спало");
    }

    #[test]
    fn a_wait_longer_than_the_clock_can_count_is_shortened_not_fatal() {
        // «Подожди сколько угодно» — законная просьба, и паника на сложении
        // была бы худшим из возможных ответов на неё: ожидание превратилось бы
        // в снятый поток вьюера. Отметка при этом обязана прийти как обычно —
        // урезание срока не имеет права ничего проспать.
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
            "отметка пришла, а не потерялась в урезанном сроке"
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
        assert_eq!(after_data.shape, start.shape, "каталоги не менялись");
        assert_ne!(after_data.data, start.data);

        pulse.shape_changed();
        let after_shape = pulse.beat();
        assert_eq!(after_shape.data, after_data.data, "новых блоков не было");
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
        // Таймаут заведомо больше паузы писателя: если пробуждение потеряется,
        // тест не повиснет, а вернёт прежнее состояние и упадёт по assert'у.
        let now = pulse.wait(seen, Duration::from_secs(10));
        writer.join().unwrap();
        assert_ne!(now, seen, "спящего обязан разбудить писатель, а не таймаут");
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
        assert!(now.closed, "остановка обязана будить ожидающих");
        assert!(pulse.beat().closed, "остановка необратима");
    }
}
