//! Читатель: слияние потоков и резолв схемы.

use crate::cursor::{ChannelCursor, Damage, OwnedRecord, OwnedSampleValue};
use crate::error::{ReadError, Result};
use crate::query::{Bounds, Filter, KindFilter, Order, Query};
use chrono::{DateTime, Utc};
use dduroc_engine::epochs::{EpochStore, Epochs};
use dduroc_engine::namespace::{NS_META, NsMeta};
use dduroc_engine::schema::{MetricKind, Schema, Severity, StorageClass};
use dduroc_engine::store::Store;
use dduroc_format::{BootCounter, BootTime, EventId, Level, MetricId, SpanId, Value};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Сколько сегментов назад искать состояние на левый край окна.
///
/// Граница нужна, чтобы поиск не уходил в историю на всю глубину хранения:
/// ряд, не менявшийся месяц, обошёлся бы чтением месяца данных ради одного
/// значения. Два сегмента — это десятки мегабайт истории при типичном
/// размере, чего хватает состоянию, которое пишут при каждом изменении.
const SEED_SEGMENTS: usize = 2;

/// Одолжить владеющее значение как значение формата — для вычисления важности.
fn as_format_value(v: &OwnedSampleValue) -> Value<'_> {
    match v {
        OwnedSampleValue::F32(x) => Value::F32(*x),
        OwnedSampleValue::F64(x) => Value::F64(*x),
        OwnedSampleValue::I64(x) => Value::I64(*x),
        OwnedSampleValue::U64(x) => Value::U64(*x),
        OwnedSampleValue::Bool(x) => Value::Bool(*x),
        OwnedSampleValue::Blob(b) => Value::Blob(b),
    }
}

/// Разновидность записи в ответе.
#[derive(Debug, Clone, PartialEq)]
pub enum EntryKind {
    /// Схемное сообщение.
    Message {
        event: EventId,
        /// Имя типа из схемы; `None` — схема неизвестна этому билду.
        name: Option<&'static str>,
        level: Option<Level>,
        tags: &'static [&'static str],
        payload: Vec<u8>,
    },
    /// Свободный текст без схемы (мост из tracing, panic-handler).
    Text {
        level: Level,
        target: String,
        text: String,
    },
    SpanStart {
        span: SpanId,
        kind_name: Option<&'static str>,
        parent: Option<SpanId>,
    },
    SpanEnd {
        span: SpanId,
    },
    /// Отсчёт телеметрии.
    ///
    /// Из записи приходят только метрика и значение; всё, что нужно для
    /// показа, резолвится по схеме и на диске места не занимает. Поля
    /// `Option`, потому что схема может быть неизвестна этому билду — тогда
    /// остаются идентификатор и число, и это честнее, чем выдумать имя.
    Sample {
        metric: MetricId,
        metric_name: Option<&'static str>,
        unit: Option<&'static str>,
        /// Статические тэги-категории метрики.
        tags: &'static [&'static str],
        /// Как величину рисовать: состояние держится ступенькой, непрерывная
        /// величина интерполируется. Прямая через значения, которых не было,
        /// это не косметика, а ложь на графике.
        kind: Option<MetricKind>,
        /// Подпись состояния, если метрика — перечисление и код объявлен.
        state_name: Option<&'static str>,
        /// Важность значения по пределам **из схемы**.
        ///
        /// Рантайм-переопределения читателю недоступны by design: он может
        /// читать дамп с чужого прибора, где действовали другие настройки, а
        /// в сам дамп пределы не пишутся (см. `dduroc_engine::limits`).
        severity: Option<Severity>,
        value: OwnedSampleValue,
    },
    /// Нераспознанное расширение формата.
    Ext {
        bytes: Vec<u8>,
    },
}

/// Запись ответа.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub namespace: std::sync::Arc<str>,
    /// Класс хранения, в чьём канале лежала запись. Канал и есть класс:
    /// строкового имени у него нет — есть каталог, названный по классу.
    pub channel: StorageClass,
    /// Относительное время: запуск плюс микросекунды от его старта. Есть
    /// всегда — прибору для этого не нужны ни RTC, ни синхронизация.
    pub at: BootTime,
    /// Настенное время. `None` — для загрузки железа нет якоря
    /// синхронизации, и абсолютного времени у записи просто нет.
    pub utc: Option<DateTime<Utc>>,
    /// Спан, к которому привязана запись.
    pub span: Option<SpanId>,
    pub kind: EntryKind,
}

impl Entry {
    /// Уровень записи: у сообщений — из схемы, у текста — из самой записи.
    pub fn level(&self) -> Option<Level> {
        match &self.kind {
            EntryKind::Message { level, .. } => *level,
            EntryKind::Text { level, .. } => Some(*level),
            _ => None,
        }
    }

    /// Отметка движка о потерянных записях: сколько их выпало прямо перед
    /// этой отметкой.
    ///
    /// Потери объявляются в самом потоке — дыра, о которой нигде не сказано,
    /// неотличима от тишины. Формат отметки принадлежит движку
    /// ([`dduroc_engine::diag`]), и разбирать её текст руками прикладному
    /// коду не нужно.
    pub fn dropped_records(&self) -> Option<u64> {
        match &self.kind {
            EntryKind::Text { target, text, .. } => {
                dduroc_engine::diag::parse_drop_notice(target, text)
            }
            _ => None,
        }
    }
}

/// Ответ на запрос.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub entries: Vec<Entry>,
    /// Состояния на левый край окна: последний отсчёт каждого
    /// ряда-состояния **до** `from` (см. [`Query::seed_states`]).
    ///
    /// Лежат отдельно от `entries` намеренно: их время вне запрошенного
    /// диапазона, и подмешивать их к остальным значило бы нарушить обещание
    /// «всё в ответе внутри окна».
    pub seeds: Vec<Entry>,
    /// Фрагменты, которые не удалось прочитать. Пустой список — ответ полон.
    pub damaged: Vec<Damage>,
    /// Ответ обрезан по `limit`.
    pub truncated: bool,
    /// Запуски, чьи сегменты попали бы в выборку, но выпали: границы заданы
    /// настенным временем, а якоря синхронизации у этих запусков нет —
    /// сравнивать их записи с настенными часами нечем.
    ///
    /// Непустой список означает, что часть истории в ответ не попала, и **не
    /// потому, что её нет**. Без этого поля такой ответ выглядел бы как «в
    /// запрошенные часы прибор ничего не писал».
    pub unanchored: Vec<BootCounter>,
}

impl QueryResult {
    /// Полон ли ответ: ничего не пропущено из-за повреждений.
    pub fn is_complete(&self) -> bool {
        self.damaged.is_empty()
    }
}

/// Голова курсора в куче слияния.
///
/// [`BinaryHeap`] — максимальная куча, поэтому «лучший» обязан оказаться
/// наибольшим, и сравнение переворачивается для порядка от старого к новому.
/// При равном времени побеждает курсор с меньшим номером: одинаковые моменты
/// — обычное дело (часы монотонны, но не строго возрастают), и порядок между
/// ними обязан быть устойчивым, иначе один и тот же запрос выдавал бы записи
/// вразнобой от раза к разу.
#[derive(Debug, PartialEq, Eq)]
struct Head {
    at: BootTime,
    idx: usize,
    newest_first: bool,
}

impl Ord for Head {
    fn cmp(&self, other: &Self) -> Ordering {
        let by_time = if self.newest_first {
            self.at.cmp(&other.at)
        } else {
            other.at.cmp(&self.at)
        };
        by_time.then_with(|| other.idx.cmp(&self.idx))
    }
}

impl PartialOrd for Head {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Поток записей запроса: слияние каналов по времени без сборки ответа целиком.
///
/// Порядок тот же, что у [`Reader::query`]: курсоры сливаются по полному
/// моменту (запуск, µs), а не по микросекундам — сравнивать их между разными
/// запусками бессмысленно.
///
/// Слияние идёт кучей, а не перебором курсоров на каждую запись: курсор
/// заводится на каждую пару (неймспейс, канал), и при заявленных двадцати
/// четырёх тысячах неймспейсов линейный поиск головы означал бы десятки тысяч
/// сравнений на **каждую** выданную строчку.
#[derive(Debug)]
pub struct EntryStream<'a> {
    reader: &'a Reader,
    /// Копия запроса: разбор записи сверяется с фильтром на каждой.
    query: Query,
    /// Снимок эпох на весь поток: границы окна и UTC записей считаются
    /// одними якорями, даже если у живого хранилища они меняются под ногами.
    epochs: Epochs,
    bounds: Bounds,
    cursors: Vec<ChannelCursor>,
    /// Схема на каждый курсор, в том же порядке.
    schemas: Vec<Option<Schema>>,
    /// Головы непустых курсоров.
    heads: BinaryHeap<Head>,
    /// Каталоги, которые не удалось прочитать — известны сразу при открытии.
    damaged: Vec<Damage>,
    limit: usize,
    yielded: usize,
    truncated: bool,
    /// Обход окончен: повторный вызов не должен выдавать записи после того,
    /// как поток уже ответил `None`.
    done: bool,
}

impl EntryStream<'_> {
    /// Вернуть голову курсора в кучу, если она есть.
    fn requeue(&mut self, idx: usize) {
        let newest_first = self.query.order == Order::Newest;
        if let Some(head) = self.cursors[idx].peek() {
            let at = head.at;
            self.heads.push(Head {
                at,
                idx,
                newest_first,
            });
        }
    }
}

impl Iterator for EntryStream<'_> {
    type Item = Entry;

    fn next(&mut self) -> Option<Entry> {
        if self.done {
            return None;
        }
        loop {
            let Some(Head { idx, .. }) = self.heads.pop() else {
                self.done = true;
                return None;
            };
            let taken = self.cursors[idx].next_entry();
            self.requeue(idx);
            let Some(raw) = taken else { continue };

            // Записи вне окна пропускаются, но обход продолжается: сегмент мог
            // начаться раньше нижней границы.
            if !self.bounds.contains(raw.at) {
                continue;
            }

            let ns = std::sync::Arc::clone(&self.cursors[idx].namespace);
            let ch = self.cursors[idx].channel;
            if let Some(entry) = self.reader.build_entry(
                ns,
                ch,
                self.schemas[idx].as_ref(),
                &raw,
                &self.query,
                &self.epochs,
            ) {
                // Обрезка объявляется, только когда очередная запись
                // действительно **есть**, а места в ответе уже нет. Ставить
                // отметку при входе значило бы объявлять обрезанным всякий
                // ответ ровно в `limit` записей, даже если больше их и не было
                // — а для веб-слоя это вечная кнопка «дальше».
                if self.yielded >= self.limit {
                    self.truncated = true;
                    self.done = true;
                    return None;
                }
                self.yielded += 1;
                return Some(entry);
            }
        }
    }
}

impl EntryStream<'_> {
    /// Фрагменты, которые не удалось прочитать.
    ///
    /// Список отражает всё, что обнаружено **к этому моменту**, включая
    /// повреждения в недочитанных сегментах: обход обрывается по `limit`, и
    /// ответ, из которого часть данных выпала из-за порчи, не имеет права
    /// выглядеть полным. Дальше по потоку список может только пополниться —
    /// повреждение обнаруживается при чтении блока.
    pub fn damaged(&self) -> Vec<Damage> {
        let mut out = self.damaged.clone();
        for c in &self.cursors {
            out.extend(c.damaged());
        }
        out
    }

    /// Запуски, чьи сегменты выпали из выборки за неимением якоря — см.
    /// [`QueryResult::unanchored`].
    pub fn unanchored(&self) -> Vec<BootCounter> {
        let mut out: Vec<BootCounter> = Vec::new();
        for c in &self.cursors {
            for boot in c.unanchored() {
                if !out.contains(boot) {
                    out.push(*boot);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Оборван ли обход по `limit` запроса.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Сколько записей уже выдано.
    pub fn yielded(&self) -> usize {
        self.yielded
    }
}

/// Дольше этого одно ожидание подписки не длится.
///
/// Опрашивать реже раза в час нечего и незачем, а `Duration::MAX` в сроке
/// — это паника на сложении, то есть худший из возможных ответов на просьбу
/// подождать подольше.
const MAX_WAIT: Duration = Duration::from_secs(3600);

/// Не чаще этого подписка обходит корни в поисках новых неймспейсов.
///
/// Задержка появления сервиса на живом экране; выбрана так, чтобы её не
/// замечал человек, но замечал прибор с тысячами неймспейсов.
const ADOPT_EVERY: Duration = Duration::from_millis(500);

/// Что подписка отдала на этот раз.
#[derive(Debug)]
pub enum Tail {
    /// Очередная запись.
    Entry(Box<Entry>),
    /// За отведённое время нового не появилось. Спросить снова — можно и нужно:
    /// это не конец, а тишина.
    Idle,
    /// Писать больше некому: хранилище остановлено, и всё, что успело лечь на
    /// носитель, уже отдано. Дальше будет только `Ended`.
    Ended,
}

/// Подписка на поток записей живого хранилища.
///
/// Читает то же и теми же средствами, что и [`Reader::query`], — но не
/// заканчивается на конце данных, а ждёт продолжения. Отличий от запроса три,
/// и все они следуют из того, что запись ещё идёт:
///
/// - **порядок только от старого к новому** ([`Order::Oldest`]): «последние
///   сто» у потока, у которого нет последней записи, ничего не означают;
/// - **верхней границы окна нет**: подписка читает то, чего ещё нет;
/// - **недописанное откладывается**, а не пропускается: и хвост блока, и
///   сегмент, застигнутый в момент рождения. У разового запроса есть
///   следующий запрос, у подписки — нет, и пройти мимо значит потерять.
///
/// Порядок между каналами — по времени в пределах того, что видно на момент
/// пробуждения, и это всё, что честно можно обещать: каналы синхронизируются
/// по своим политикам (критический — сразу, обычный — раз в секунду), поэтому
/// запись обычного канала может стать видимой позже критической, случившейся
/// после неё. Внутри одного канала порядок точен — он и есть порядок на диске.
///
/// Подписка держит хранилище живым, как и всякий читатель: пока она жива, не
/// отработает и остановка writer'а.
#[derive(Debug)]
pub struct Follow<'a> {
    reader: &'a Reader,
    query: Query,
    /// Границы окна, посчитанные один раз при открытии.
    ///
    /// Пересчитывать их на каждое пробуждение нельзя: настенная граница
    /// переводится в шкалу запуска по якорю, а синхронизация времени
    /// ретроактивна — окно ездило бы под ногами у подписки, которая уже
    /// прошла часть потока и вернуться назад не может.
    bounds: Bounds,
    /// Якоря времени — наоборот, свежие на каждое пробуждение: UTC у записи
    /// появляется от синхронизации, случившейся позже самой записи.
    epochs: Epochs,
    cursors: Vec<ChannelCursor>,
    schemas: Vec<Option<Schema>>,
    heads: BinaryHeap<Head>,
    /// Чья голова сейчас в куче. Курсор, исчерпавшийся до подхода новых
    /// данных, из кучи выпадает, и вернуть его туда больше нечему.
    queued: Vec<bool>,
    /// Уже открытые пары (неймспейс, класс): подписка на группу обязана
    /// подхватить сервис, стартовавший позже неё.
    known: HashSet<(String, StorageClass)>,
    /// О чём из увиденного при обходе корней уже сказано.
    ///
    /// Обход повторяется на каждую смену устройства хранилища, а нечитаемый
    /// каталог никуда не девается — без этой памяти подписка объявляла бы одно
    /// и то же повреждение при каждой ротации. Множество ограничено числом
    /// сломанных путей, а не числом обходов.
    reported: HashSet<(PathBuf, String)>,
    damaged: Vec<Damage>,
    pulse: Arc<dduroc_engine::pulse::Pulse>,
    seen: dduroc_engine::pulse::Beat,
    seeds: std::vec::IntoIter<Entry>,
    /// Когда корни в последний раз обходились ради новых неймспейсов.
    /// `None` — ещё ни разу.
    last_adopt: Option<Instant>,
    /// Последний обход после остановки хранилища уже сделан.
    swept: bool,
    /// Каталоги менялись, а обход отложен по частоте: его долг не имеет права
    /// пропасть — неймспейс, о котором объявили один раз, иначе не
    /// подхватился бы никогда.
    adopt_due: bool,
    limit: usize,
    yielded: usize,
    ended: bool,
}

impl Follow<'_> {
    /// Дождаться очередной записи, но не дольше `wait`.
    ///
    /// [`Tail::Idle`] означает «пока тихо», а не «конец»: подписку опрашивают
    /// в цикле, и таймаут задаёт, как быстро этот цикл сможет заметить
    /// остановку, заданную самим приложением.
    pub fn next(&mut self, wait: Duration) -> Tail {
        if let Some(seed) = self.seeds.next() {
            return Tail::Entry(Box::new(seed));
        }
        if self.ended || self.limit == 0 {
            // Ноль записей — это ноль записей, а не «одна и хватит»: проверка
            // после выдачи отдала бы первую и только потом остановилась.
            self.ended = true;
            return Tail::Ended;
        }
        // Срок считается один раз на вызов: пробуждение на чужих данных
        // (отметка одна на всё хранилище) не имеет права продлевать ожидание
        // сверх обещанного. Срок насыщается: подписке, попросившей ждать
        // дольше, чем часы умеют считать, отвечают часом — паника вместо
        // ожидания была бы худшим прочтением такой просьбы.
        let deadline = Instant::now()
            .checked_add(wait.min(MAX_WAIT))
            .unwrap_or_else(Instant::now);
        loop {
            if let Some(entry) = self.pop_ready() {
                return Tail::Entry(Box::new(entry));
            }
            if self.ended {
                return Tail::Ended;
            }
            if self.seen.closed {
                if !self.swept {
                    // Один обход напоследок. Отметка читается тремя полями, и
                    // остановка могла случиться между ними: тогда «закрыто»
                    // придёт вместе с ещё не увиденным устройством хранилища,
                    // и последний сегмент остался бы неперечисленным. После
                    // остановки носитель окончателен — одного обхода хватает.
                    self.swept = true;
                    self.adopt_new_channels();
                    self.rearm(true);
                    continue;
                }
                // Хранилище остановлено, и всё, что легло на носитель, отдано.
                self.ended = true;
                return Tail::Ended;
            }
            if self.adopt_if_due() {
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return Tail::Idle;
            }
            // Ожидание укорачивается до срока отложенного обхода: иначе
            // подписка с длинным таймаутом узнала бы о новом сервисе только
            // после того, как кто-нибудь что-нибудь напишет.
            let mut rest = deadline - now;
            if let Some(at) = self.adopt_deadline() {
                rest = rest.min(at);
            }
            let beat = self.pulse.wait(self.seen, rest);
            if beat == self.seen {
                // Поток writer'а, снятый паникой, отметку о закрытии выставить
                // не успевает: без этой проверки подписка ждала бы вечно того,
                // кого уже некому писать.
                if !self.reader.writer_alive() {
                    self.ended = true;
                    return Tail::Ended;
                }
                // Проснулись раньше своего срока — значит, ради отложенного
                // обхода: тишиной это объявлять рано.
                if Instant::now() < deadline {
                    continue;
                }
                return Tail::Idle;
            }
            let shape_changed = beat.shape != self.seen.shape;
            self.seen = beat;
            self.refresh(shape_changed);
        }
    }

    /// Забрать повреждения, обнаруженные с прошлого раза.
    ///
    /// Именно забрать: подписка живёт долго, и список, который только растёт,
    /// повторял бы одно и то же повреждение в каждой порции — а заодно рос бы
    /// без предела. Пустой ответ означает, что с прошлого раза всё прочиталось.
    pub fn take_damage(&mut self) -> Vec<Damage> {
        let mut out = std::mem::take(&mut self.damaged);
        for c in &mut self.cursors {
            out.append(&mut c.take_damage());
        }
        out
    }

    /// Запуски, выпавшие из настенного окна за неимением якоря —
    /// см. [`QueryResult::unanchored`].
    ///
    /// Считается по курсорам каждый раз, а не запоминается при открытии:
    /// подписка перечисляет каталоги заново, и запуск, чьи сегменты нашлись
    /// позже, обязан попасть в ответ — иначе о нём не сказал бы никто.
    pub fn unanchored(&self) -> Vec<BootCounter> {
        let mut out: Vec<BootCounter> = Vec::new();
        for c in &self.cursors {
            for boot in c.unanchored() {
                if !out.contains(boot) {
                    out.push(*boot);
                }
            }
        }
        out.sort_unstable();
        out
    }

    /// Сколько записей уже отдано.
    pub fn yielded(&self) -> usize {
        self.yielded
    }

    /// Взять запись из того, что уже прочитано с носителя.
    fn pop_ready(&mut self) -> Option<Entry> {
        loop {
            let Head { idx, .. } = self.heads.pop()?;
            self.queued[idx] = false;
            let taken = self.cursors[idx].next_entry();
            self.requeue(idx);
            let Some(raw) = taken else { continue };

            if !self.bounds.contains(raw.at) {
                continue;
            }
            let ns = std::sync::Arc::clone(&self.cursors[idx].namespace);
            let ch = self.cursors[idx].channel;
            let Some(entry) = self.reader.build_entry(
                ns,
                ch,
                self.schemas[idx].as_ref(),
                &raw,
                &self.query,
                &self.epochs,
            ) else {
                continue;
            };
            self.yielded += 1;
            if self.yielded >= self.limit {
                self.ended = true;
            }
            return Some(entry);
        }
    }

    fn requeue(&mut self, idx: usize) {
        if let Some(head) = self.cursors[idx].peek() {
            let at = head.at;
            self.heads.push(Head {
                at,
                idx,
                newest_first: false,
            });
            self.queued[idx] = true;
        }
    }

    /// Забрать всё, что появилось с прошлого пробуждения.
    fn refresh(&mut self, shape_changed: bool) {
        self.epochs = self.reader.epochs_now().into_owned();
        if shape_changed {
            self.adopt_due = true;
        }
        self.rearm(shape_changed);
    }

    /// Дочитать курсоры и вернуть в кучу тех, кто из неё выпал.
    ///
    /// Выпадает курсор, исчерпавшийся до подхода новых данных, и вернуть его
    /// туда больше нечему — в том числе только что открытому курсору нового
    /// неймспейса: без этого он лежал бы с данными мимо слияния.
    fn rearm(&mut self, relist: bool) {
        for idx in 0..self.cursors.len() {
            // Обход каталога — только когда хранилище объявило, что каталоги
            // менялись: пока писатель льёт в тот же файл, подписка обходится
            // одним чтением хвоста.
            self.cursors[idx].extend(relist);
            if !self.queued[idx] {
                self.requeue(idx);
            }
        }
    }

    /// Обойти корни, если пора. `true` — обошли, стоит посмотреть заново.
    ///
    /// Обход — самое дорогое, что делает подписка (перечисление корня и чтение
    /// меты у каждого подходящего каталога), а заводится он от смены
    /// устройства хранилища, то есть от каждой ротации. Поэтому он ограничен
    /// по частоте, но не отменяется: долг помнится и отдаётся, когда срок
    /// подойдёт.
    fn adopt_if_due(&mut self) -> bool {
        if !self.adopt_due || self.adopt_deadline().is_some() {
            return false;
        }
        self.adopt_due = false;
        self.last_adopt = Some(Instant::now());
        self.adopt_new_channels();
        self.rearm(true);
        true
    }

    /// Сколько ещё ждать до отложенного обхода. `None` — ждать нечего.
    fn adopt_deadline(&self) -> Option<Duration> {
        if !self.adopt_due {
            return None;
        }
        let at = self.last_adopt?;
        ADOPT_EVERY
            .checked_sub(at.elapsed())
            .filter(|d| !d.is_zero())
    }

    /// Объявить повреждение обхода, если о нём ещё не говорили.
    fn note_once(&mut self, damage: Damage) {
        if self
            .reported
            .insert((damage.path.clone(), damage.reason.clone()))
        {
            self.damaged.push(damage);
        }
    }

    /// Открыть каналы неймспейсов, поднятых уже после начала подписки.
    fn adopt_new_channels(&mut self) {
        let opened = self
            .reader
            .open_cursors_with(&self.query, &self.bounds, |scope| {
                scope.liveness = crate::cursor::Liveness::Following;
            });
        let OpenedCursors {
            cursors,
            schemas,
            damaged,
        } = match opened {
            Ok(o) => o,
            // Каталог не перечислился — почти всегда потому, что его меняли
            // прямо сейчас. Подписку это ронять не должно, а молчать нельзя:
            // неймспейс, который так и не подхватился, выглядел бы как
            // невзлетевший сервис.
            Err(e) => {
                self.note_once(Damage {
                    path: self.reader.root().to_owned(),
                    offset: 0,
                    reason: format!("неймспейсы не перечислились: {e}"),
                });
                return;
            }
        };
        for d in damaged {
            self.note_once(d);
        }
        for (cursor, schema) in cursors.into_iter().zip(schemas) {
            let key = (cursor.namespace.to_string(), cursor.channel);
            if !self.known.insert(key) {
                continue;
            }
            self.cursors.push(cursor);
            self.schemas.push(schema);
            self.queued.push(false);
        }
    }
}

/// Неймспейс, найденный сканом корней: каналы вместе с их каталогами.
///
/// Каталог у каждого канала свой: класс мог быть вынесен на отдельный
/// носитель, и путь не восстановить из одного корня.
#[derive(Debug)]
struct NsScan {
    name: String,
    schema_name: String,
    protocol_version: u16,
    channels: Vec<(StorageClass, PathBuf)>,
}

/// Открытые под запрос курсоры вместе с разрешёнными схемами.
#[derive(Debug)]
struct OpenedCursors {
    cursors: Vec<ChannelCursor>,
    /// Схема на каждый курсор, в том же порядке.
    schemas: Vec<Option<Schema>>,
    /// Каталоги, которые не удалось прочитать.
    damaged: Vec<Damage>,
}

/// Сведения о неймспейсе хранилища.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub name: String,
    pub schema_name: String,
    pub protocol_version: u16,
    /// Классы хранения, чьи каналы есть у неймспейса, в порядке
    /// [`StorageClass::index`].
    pub channels: Vec<StorageClass>,
    /// Суммарный размер сегментов, байт.
    pub total_bytes: u64,
}

/// Что нашлось в корне при перечислении неймспейсов.
///
/// Отдельный тип, а не просто список: перечисление, из которого молча выпал
/// нечитаемый неймспейс, выглядит как «такого сервиса на приборе нет» — то же
/// молчание, против которого заведено [`QueryResult::damaged`]. Пустой
/// `damaged` означает, что перечислено всё.
#[derive(Debug, Clone, Default)]
pub struct NamespaceListing {
    pub namespaces: Vec<NamespaceInfo>,
    /// Каталоги, опознанные как неймспейсы, но не поддавшиеся чтению.
    pub damaged: Vec<Damage>,
}

impl NamespaceListing {
    /// Перечислено ли всё, что есть в корне.
    pub fn is_complete(&self) -> bool {
        self.damaged.is_empty()
    }
}

/// Откуда читатель берёт правду о хранилище.
#[derive(Debug)]
enum Source {
    /// Живое хранилище этого же процесса: корни, схемы и эпохи спрашиваются
    /// у него в момент каждого запроса — устареть им не из чего. Хранилище
    /// удерживается живым, как его удерживает и ручка неймспейса.
    Store(Arc<Store>),
    /// Дамп: все корни названы при открытии, всё прочитано с диска и
    /// заморожено. Для дампа это не изъян, а определение — его никто не
    /// дописывает.
    Dump { roots: Vec<PathBuf>, epochs: Epochs },
}

/// Читатель хранилища.
///
/// Живёт в двух режимах, и различие между ними — источник правды:
///
/// - **живой** ([`Reader::of_store`]) — читает хранилище, которое этот же
///   процесс пишет прямо сейчас. Работать параллельно с записью — его
///   определение: каждый запрос видит свежие корни, схемы и якоря времени,
///   ротация под ногами и дописываемый хвост сегмента — штатные события,
///   а не порча;
/// - **дамп** ([`Reader::open_dump`]) — снимок, который никто не дописывает:
///   все корни названы при открытии и проверены на полноту, а любой признак
///   недописанности честно доносится как повреждение.
#[derive(Debug)]
pub struct Reader {
    source: Source,
    /// Схемы, названные руками (при открытии и [`Reader::with_schema`]).
    /// Живой читатель поверх них видит схемы поднятых неймспейсов.
    schemas: HashMap<String, Schema>,
    /// Идентичность хранилища; `None` — не проверять (чтение чужого дампа
    /// разрешено явно).
    store_id: Option<u64>,
    /// Схема, разрешённая по имени неймспейса. Резолв читает `ns-meta` с
    /// диска, а спрашивают о нём на **каждую** показываемую запись — файловая
    /// операция на запись была бы ровно тем дефектом, который уже убран с
    /// пути запроса. `None` в значении — «неймспейс есть, схемы к нему у
    /// этого билда нет»; такой ответ тоже стоит запомнить.
    schema_cache: RwLock<HashMap<String, Option<Schema>>>,
}

impl Reader {
    /// Открыть дамп хранилища: **все** его корни и схемы — разом.
    ///
    /// Первый корень — основной (в нём живут `ns-meta`, эпохи и идентичность
    /// хранилища); остальные — деревья классов, вынесенных на свои носители
    /// ([`ChannelConfig::custom_root`]). Корни именно перечисляются, а не
    /// добавляются по одному опциональным строителем: хранилище с критикой на
    /// защищённом разделе — это два дерева, и читатель, которому назвали не
    /// все, показывал бы историю без критики, ничем не выдав пропажу.
    /// Полнота проверяется при открытии по схемам: у каждого неймспейса
    /// известной схемы каждый объявленный ею класс обязан найтись в одном из
    /// корней — иначе [`ReadError::IncompleteDump`], а не молча короткий
    /// ответ.
    ///
    /// Только чтение: ни восстановления, ни уборки временных файлов. Вьюер
    /// не имеет права изменять дамп, который ему дали посмотреть. `Store`
    /// для дампа не открывают вовсе — тот берёт блокировку корня и подметает
    /// временные файлы, поэтому корни и схемы называются руками.
    ///
    /// Хранилищу, которое пишет этот же процесс, нужен [`Reader::of_store`]:
    /// снимок эпох и схем, сделанный здесь, устаревает первой же
    /// синхронизацией времени или подъёмом нового неймспейса.
    ///
    /// [`ChannelConfig::custom_root`]: dduroc_engine::channel::ChannelConfig::custom_root
    pub fn open_dump<P: Into<PathBuf>>(
        roots: impl IntoIterator<Item = P>,
        schemas: &[Schema],
    ) -> Result<Self> {
        let roots: Vec<PathBuf> = roots.into_iter().map(Into::into).collect();
        let Some(main_root) = roots.first() else {
            return Err(ReadError::Io {
                context: "открытие дампа".to_owned(),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "не назван ни один корень",
                ),
            });
        };
        // Опечатка в пути не имеет права выглядеть как пустое хранилище.
        if !main_root.is_dir() {
            return Err(ReadError::Io {
                context: format!("открытие дампа {}", main_root.display()),
                source: std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "основной корень не существует",
                ),
            });
        }
        let epochs = EpochStore::open_read_only(main_root).unwrap_or_default();
        let store_id = read_store_id(main_root);
        let reader = Self {
            source: Source::Dump { roots, epochs },
            schemas: schemas.iter().map(|s| (s.name.to_owned(), *s)).collect(),
            store_id,
            schema_cache: RwLock::new(HashMap::new()),
        };
        reader.check_dump_completeness()?;
        Ok(reader)
    }

    /// Убедиться, что среди корней дампа есть дерево каждого класса.
    ///
    /// Проверка структурная и не зависит от того, куда дамп перенесли:
    /// схема неймспейса объявляет его классы, каталог каждого класса
    /// создаётся при подъёме неймспейса — значит, каждый обязан найтись в
    /// одном из названных корней. Неймспейс неизвестной схемы проверить
    /// нечем — его записи и так останутся идентификаторами.
    fn check_dump_completeness(&self) -> Result<()> {
        let mut damaged = Vec::new();
        for ns in self.scan_namespaces(&mut damaged)? {
            let Some(schema) = self.schema_by_name(&ns.schema_name) else {
                continue;
            };
            for class in schema.classes() {
                if !ns.channels.iter().any(|&(have, _)| have == class) {
                    return Err(ReadError::IncompleteDump {
                        namespace: ns.name,
                        class,
                    });
                }
            }
        }
        Ok(())
    }

    /// Живой читатель: параллелен записи по построению.
    ///
    /// Правда берётся у самого хранилища в момент **каждого запроса**, а не
    /// при создании читателя, поэтому создать его можно один раз при старте
    /// и держать сколько угодно:
    ///
    /// - корни — все, включая вынесенные носители классов
    ///   ([`ChannelConfig::custom_root`]): читатель, которому назвали не все
    ///   деревья, показывал бы историю без критики, ничем не выдав пропажу;
    /// - схемы — поднятых к этому моменту неймспейсов: сервис, стартовавший
    ///   после создания читателя, читается с текстами, а не голыми id;
    /// - эпохи — с якорями на этот момент: синхронизация времени ретроактивна,
    ///   и запрос после неё показывает UTC у записей, сделанных до.
    ///
    /// Внутри одного запроса снимок один: все записи ответа переведены в UTC
    /// одними и теми же якорями.
    ///
    /// Ротация и дописывание не мешают чтению: сегмент, вытесненный между
    /// листингом и открытием, молча пропускается (его данные вытеснены, а не
    /// потеряны), недописанный хвост живого сегмента — «ещё не данные», а не
    /// порча. У дампа то и другое честно объявляется повреждением.
    ///
    /// Видно ровно то, что уже на носителе: записи, ещё лежащие в очереди
    /// writer'а, не видны никакому читателю — их сперва надо вытолкнуть
    /// ([`Store::sync`]). Читатель держит хранилище живым, как держит его
    /// ручка неймспейса.
    ///
    /// [`ChannelConfig::custom_root`]: dduroc_engine::channel::ChannelConfig::custom_root
    pub fn of_store(store: &Arc<Store>) -> Self {
        let store_id = store.meta().store_id;
        Self {
            source: Source::Store(Arc::clone(store)),
            schemas: HashMap::new(),
            store_id: Some(store_id),
            schema_cache: RwLock::new(HashMap::new()),
        }
    }

    /// Живой ли это читатель — см. [`Reader::of_store`].
    fn live(&self) -> bool {
        matches!(self.source, Source::Store(_))
    }

    /// Все корни хранилища в порядке обхода; первый — основной.
    fn all_roots(&self) -> Vec<PathBuf> {
        match &self.source {
            Source::Store(store) => store.roots().to_vec(),
            Source::Dump { roots, .. } => roots.clone(),
        }
    }

    /// Эпохи на этот момент: у живого читателя — из памяти хранилища, у
    /// дампа — снимок открытия. Берутся один раз на запрос: все записи
    /// одного ответа обязаны быть переведены в UTC одними якорями.
    fn epochs_now(&self) -> std::borrow::Cow<'_, Epochs> {
        match &self.source {
            Source::Store(store) => std::borrow::Cow::Owned(store.epochs()),
            Source::Dump { epochs, .. } => std::borrow::Cow::Borrowed(epochs),
        }
    }

    /// Схема по имени: названные руками — прежде всего (они — явное решение
    /// вызывающего), затем схемы поднятых неймспейсов живого хранилища.
    fn schema_by_name(&self, name: &str) -> Option<Schema> {
        if let Some(s) = self.schemas.get(name) {
            return Some(*s);
        }
        match &self.source {
            Source::Store(store) => store.schemas().into_iter().find(|s| s.name == name),
            Source::Dump { .. } => None,
        }
    }

    /// Все известные схемы: названные руками плюс — у живого читателя —
    /// схемы поднятых неймспейсов.
    fn all_schemas(&self) -> Vec<Schema> {
        let mut out: HashMap<&'static str, Schema> = match &self.source {
            Source::Store(store) => store.schemas().into_iter().map(|s| (s.name, s)).collect(),
            Source::Dump { .. } => HashMap::new(),
        };
        for s in self.schemas.values() {
            out.insert(s.name, *s);
        }
        out.into_values().collect()
    }

    /// Добавить схему, которой не было среди переданных при открытии.
    ///
    /// Нужно, когда в хранилище есть неймспейс чужого сервиса: его записи без
    /// схемы остаются идентификаторами. Схема с тем же именем заменяет
    /// прежнюю.
    pub fn with_schema(mut self, schema: Schema) -> Self {
        self.schemas.insert(schema.name.to_owned(), schema);
        self
    }

    /// Не проверять принадлежность сегментов этому хранилищу.
    ///
    /// Нужно для разбора чужого дампа, собранного из нескольких приборов;
    /// абсолютное время таких записей доверять нельзя.
    pub fn allow_foreign_segments(mut self) -> Self {
        self.store_id = None;
        self
    }

    /// Основной корень хранилища.
    pub fn root(&self) -> &Path {
        match &self.source {
            Source::Store(store) => store.root(),
            Source::Dump { roots, .. } => &roots[0],
        }
    }

    /// Эпохи на этот момент. У живого читателя ответ меняется от вызова к
    /// вызову — по мере синхронизаций времени; у дампа он заморожен открытием.
    pub fn epochs(&self) -> Epochs {
        self.epochs_now().into_owned()
    }

    /// Текст записи на указанном языке.
    ///
    /// `None` — рендерить нечего: это не сообщение схемы, либо схема
    /// неизвестна этому билду (тогда у записи остаются идентификатор и
    /// payload, и выдумывать текст читатель не станет).
    ///
    /// Схема ищется по неймспейсу самой записи. Раньше её приходилось искать
    /// вызывающему и передавать вручную — а он мог передать чужую, и тогда
    /// payload разбирался бы декодером другого события: текст получался бы
    /// правдоподобным и неверным.
    pub fn render(&self, entry: &Entry, lang: &str) -> Option<String> {
        match &entry.kind {
            EntryKind::Message { event, payload, .. } => {
                let schema = self.schema_of(&entry.namespace)?;
                render(&schema, *event, payload, lang)
            }
            // Свободный текст уже текст: у него нет ни шаблона, ни языков.
            EntryKind::Text { text, .. } => Some(text.clone()),
            _ => None,
        }
    }

    /// Схема неймспейса — по его метаданным на диске.
    ///
    /// Ищется имя схемы каталога, а не первая подходящая: два неймспейса могут
    /// жить под разными схемами в одном хранилище.
    ///
    /// Ответ запоминается: `ns-meta` читается один раз на неймспейс, а не на
    /// каждый вызов. [`Reader::render`] зовут на каждую показываемую запись, и
    /// без этого отрисовка пятисот строк стоила бы пятисот открытий файла.
    /// Схема — `Copy` и лежит в `.rodata`, копия ничего не стоит.
    pub fn schema_of(&self, namespace: &str) -> Option<Schema> {
        if let Ok(cache) = self.schema_cache.read()
            && let Some(found) = cache.get(namespace)
        {
            return *found;
        }
        let resolved = read_ns_meta(&self.root().join(namespace))
            .and_then(|meta| self.schema_by_name(&meta.schema_name));
        // Живому читателю «схемы нет» запоминать нельзя: неймспейс мог ещё
        // не подняться, и закешированный `None` прятал бы его тексты до
        // конца жизни читателя. Найденная схема неизменна в обоих режимах.
        if (resolved.is_some() || !self.live())
            && let Ok(mut cache) = self.schema_cache.write()
        {
            cache.insert(namespace.to_owned(), resolved);
        }
        resolved
    }

    /// Перечислить неймспейсы вместе с занятым объёмом.
    ///
    /// Объём стоит `stat` каждого сегмента, поэтому путь запроса им не
    /// пользуется: ему хватает имён неймспейсов и каналов.
    ///
    /// Неймспейс с нечитаемой метой попадает не в список, а в
    /// [`NamespaceListing::damaged`]: показать его нечем, но и промолчать
    /// нельзя — иначе перечисление объявляло бы полным ответ, из которого
    /// целый неймспейс выпал, и его данные исчезли бы бесследно.
    pub fn namespaces(&self) -> Result<NamespaceListing> {
        let mut damaged = Vec::new();
        let found = self.scan_namespaces(&mut damaged)?;
        let mut namespaces = Vec::with_capacity(found.len());
        for ns in found {
            let mut total_bytes = 0;
            for (_, dir) in &ns.channels {
                if let Ok(inv) = dduroc_engine::rotation::Inventory::scan(dir) {
                    total_bytes += inv.total_bytes();
                }
            }
            namespaces.push(NamespaceInfo {
                name: ns.name,
                schema_name: ns.schema_name,
                protocol_version: ns.protocol_version,
                channels: ns.channels.into_iter().map(|(n, _)| n).collect(),
                total_bytes,
            });
        }
        Ok(NamespaceListing {
            namespaces,
            damaged,
        })
    }

    /// Неймспейсы и их каналы — без обращения к размерам файлов.
    ///
    /// Нечитаемая мета и каталоги неизвестных классов копятся в `damaged`.
    fn scan_namespaces(&self, damaged: &mut Vec<Damage>) -> Result<Vec<NsScan>> {
        self.scan_namespaces_matching(damaged, |_| true)
    }

    /// То же, но с отбором по имени **до** чтения метаданных.
    ///
    /// Имя неймспейса — это имя каталога, и запрос почти всегда отбирает по
    /// нему: один сервис, одна группа. Читать `ns-meta` у всех, чтобы потом
    /// выбросить почти всех, — файловая операция на каждый из заявленных
    /// двадцати четырёх тысяч каталогов; на пути подписки, которая обходит
    /// корни на каждую смену устройства хранилища, это разница между «дёшево»
    /// и «неприемлемо».
    fn scan_namespaces_matching(
        &self,
        damaged: &mut Vec<Damage>,
        wanted: impl Fn(&str) -> bool,
    ) -> Result<Vec<NsScan>> {
        let mut out = Vec::new();
        let roots = self.all_roots();
        let main_root = &roots[0];
        let entries = match std::fs::read_dir(main_root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(source) => {
                return Err(ReadError::Io {
                    context: format!("чтение {}", main_root.display()),
                    source,
                });
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !wanted(name) {
                continue;
            }
            let Some(meta) = read_ns_meta(&path) else {
                // Каталог без метаданных — не неймспейс (чужая директория в
                // корне хранилища). Каталог с НЕЧИТАЕМОЙ метой — это
                // неймспейс, который мы не можем показать, и молчать об этом
                // нельзя: его данные просто исчезли бы из всех ответов.
                if path.join(NS_META).exists() {
                    damaged.push(Damage {
                        path,
                        offset: 0,
                        reason: "метаданные неймспейса не читаются".to_owned(),
                    });
                }
                continue;
            };

            // Каналы неймспейса собираются по всем корням: класс мог быть
            // вынесен на свой носитель, и его каталог живёт не рядом с
            // ns-meta. Имя канала уникально в пределах неймспейса — класс
            // живёт ровно в одном корне.
            let mut channels: Vec<(StorageClass, PathBuf)> = Vec::new();
            for root in &roots {
                let ns_dir = root.join(name);
                let Ok(dir) = std::fs::read_dir(&ns_dir) else {
                    continue;
                };
                for ch in dir.flatten() {
                    let ch_path = ch.path();
                    if !ch_path.is_dir() {
                        continue;
                    }
                    let class = ch_path
                        .file_name()
                        .and_then(|n| n.to_str())
                        .and_then(StorageClass::from_dir_name);
                    match class {
                        Some(class) => channels.push((class, ch_path)),
                        // Каталог, не являющийся каналом ни одного класса
                        // этой сборки, — либо чужая директория, либо дамп из
                        // будущей версии с новым классом. Разбирать его
                        // нечем, а выпасть молча он не имеет права.
                        None => damaged.push(Damage {
                            path: ch_path,
                            offset: 0,
                            reason: "каталог не является каналом известного                                      класса хранения"
                                .to_owned(),
                        }),
                    }
                }
            }
            channels.sort_by_key(|(c, _)| c.index());

            out.push(NsScan {
                name: name.to_owned(),
                schema_name: meta.schema_name,
                protocol_version: meta.protocol_version,
                channels,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Открыть запрос потоком: записи разбираются по мере обхода.
    ///
    /// Память ограничена одним распакованным блоком на канал — тем же, чем
    /// ограничен курсор. Это единственный способ читать много: [`Reader::query`]
    /// собирает ответ целиком, и запрос без `limit` по двухсотгигабайтному
    /// хранилищу не поместился бы в памяти armv7.
    ///
    /// Обход можно прервать в любой момент; о повреждениях и выпавших запусках
    /// поток сообщает теми же средствами, что и `query`, — но полны эти
    /// сведения только после того, как поток исчерпан либо оставлен.
    pub fn stream(&self, q: &Query) -> Result<EntryStream<'_>> {
        // Эпохи берутся один раз на весь поток: и границы окна, и UTC каждой
        // записи считаются одними якорями — ответ внутренне согласован, даже
        // если синхронизация времени придёт посреди обхода.
        let epochs = self.epochs_now().into_owned();
        // Границы приводятся к относительной шкале один раз: настенная
        // граница требует якоря на каждый запуск, и делать это на запись
        // значило бы линейный поиск по эпохам на каждую из сотен тысяч.
        let bounds = q.resolve(&epochs).bounds;
        let OpenedCursors {
            mut cursors,
            schemas,
            damaged,
        } = self.open_cursors(q, &bounds)?;

        // Кучу заряжаем сразу: пустой курсор в неё просто не попадает, и
        // дальше слияние не тратит на него ни одного сравнения.
        let newest_first = q.order == Order::Newest;
        let mut heads = BinaryHeap::with_capacity(cursors.len());
        for (idx, cursor) in cursors.iter_mut().enumerate() {
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first,
                });
            }
        }

        Ok(EntryStream {
            reader: self,
            query: q.clone(),
            epochs,
            bounds,
            cursors,
            schemas,
            heads,
            damaged,
            limit: q.limit.unwrap_or(usize::MAX),
            yielded: 0,
            truncated: false,
            done: false,
        })
    }

    /// Подписаться на поток записей: читать по мере появления.
    ///
    /// Живой читатель видит записи опросом, и без подписки опрос пришлось бы
    /// делать по таймеру — частому (обращения к носителю впустую, а на приборе
    /// это ещё и износ флеша) или редкому (график отстаёт на период). Подписка
    /// спит, пока писать нечего, и просыпается на первом же блоке, легшем в
    /// файл.
    ///
    /// Начинается там, где начинается запрос: `Query::new()` без границ —
    /// с начала истории, `since(...)` — с указанного момента, `since(ns.now())`
    /// — с этой секунды. Отдельной ручки «только новое» нет: это и есть окно
    /// запроса.
    ///
    /// Что подписке нельзя, то отвергается, а не подгоняется молча: обратный
    /// порядок ([`Order::Newest`]) и верхняя граница окна. Дамп подписки не
    /// принимает вовсе — его никто не дописывает.
    ///
    /// Неймспейсы, поднятые уже после начала подписки, подхватываются: сервис,
    /// стартовавший позже вьюера, не должен требовать его перезапуска. Обход
    /// корней ради них ограничен по частоте — он перечисляет всё хранилище, —
    /// поэтому появление нового сервиса на экране отстаёт на доли секунды.
    ///
    /// Подписка отдаёт то, что доживает до неё. Вытеснение по бюджету может
    /// забрать сегмент, до которого она ещё не дошла: это штатное событие
    /// хранилища (`Stats::segments_rotated`), а не повреждение, и подписка,
    /// не поспевающая за ротацией, отстаёт от истории по определению.
    ///
    /// Затравки состояний (`Query::with_state_seed`) идут первыми и несут
    /// время **до** окна — как и в [`Reader::query`], где они лежат отдельным
    /// полем: ряд, не менявшийся с прошлой недели, иначе выглядел бы на живом
    /// графике пустым.
    pub fn follow(&self, q: &Query) -> Result<Follow<'_>> {
        let Source::Store(store) = &self.source else {
            return Err(ReadError::NotFollowable(
                "дамп никто не дописывает — подписываться не на что",
            ));
        };
        if q.order == Order::Newest {
            return Err(ReadError::NotFollowable(
                "поток идёт от старого к новому: `Order::Newest` у него нечего означать",
            ));
        }
        if q.to.is_some() {
            return Err(ReadError::NotFollowable(
                "у подписки нет верхней границы окна: она читает то, чего ещё нет",
            ));
        }

        let pulse = Arc::clone(store.pulse());
        // Отметка снимается ДО открытия курсоров: всё, что ляжет между этими
        // двумя моментами, курсоры либо уже увидят сами, либо покажет первое
        // же пробуждение. Обратный порядок терял бы такие записи до следующей
        // отметки.
        let seen = pulse.beat();

        let epochs = self.epochs_now().into_owned();
        let bounds = q.resolve(&epochs).bounds;
        let OpenedCursors {
            mut cursors,
            schemas,
            mut damaged,
        } = self.open_cursors_with(q, &bounds, |scope| {
            scope.liveness = crate::cursor::Liveness::Following;
        })?;

        let mut heads = BinaryHeap::with_capacity(cursors.len());
        let mut queued = vec![false; cursors.len()];
        let mut known = HashSet::with_capacity(cursors.len());
        for (idx, cursor) in cursors.iter_mut().enumerate() {
            known.insert((cursor.namespace.to_string(), cursor.channel));
            if let Some(head) = cursor.peek() {
                heads.push(Head {
                    at: head.at,
                    idx,
                    newest_first: false,
                });
                queued[idx] = true;
            }
        }

        // Затравка состояний — та же, что у `query`: ряд, не менявшийся с
        // прошлой недели, иначе выглядел бы у живого графика пустым.
        let seeds = if q.seed_states && q.from.is_some() {
            self.collect_state_seeds(q, &bounds, &mut damaged, &epochs)?
        } else {
            Vec::new()
        };

        Ok(Follow {
            reader: self,
            query: q.clone(),
            bounds,
            epochs,
            cursors,
            schemas,
            heads,
            queued,
            known,
            reported: damaged
                .iter()
                .map(|d| (d.path.clone(), d.reason.clone()))
                .collect(),
            damaged,
            pulse,
            seen,
            seeds: seeds.into_iter(),
            last_adopt: None,
            swept: false,
            adopt_due: false,
            limit: q.limit.unwrap_or(usize::MAX),
            yielded: 0,
            ended: false,
        })
    }

    /// Жив ли поток writer'а хранилища; у дампа писать некому по определению.
    fn writer_alive(&self) -> bool {
        match &self.source {
            Source::Store(store) => store.writer_alive(),
            Source::Dump { .. } => false,
        }
    }

    /// Выполнить запрос, собрав ответ целиком.
    ///
    /// Годится, когда объём ответа ограничен — `limit`, узкое окно, редкий тип
    /// событий. Для всего остального есть [`Reader::stream`].
    pub fn query(&self, q: &Query) -> Result<QueryResult> {
        let mut stream = self.stream(q)?;
        let entries: Vec<Entry> = stream.by_ref().collect();
        let mut result = QueryResult {
            entries,
            truncated: stream.truncated(),
            // Повреждения собираются и при обрезке по лимиту: ответ, из
            // которого часть данных выпала из-за порчи, не должен выглядеть
            // полным.
            damaged: stream.damaged(),
            unanchored: stream.unanchored(),
            seeds: Vec::new(),
        };

        if q.seed_states && q.from.is_some() {
            let bounds = stream.bounds.clone();
            result.seeds =
                self.collect_state_seeds(q, &bounds, &mut result.damaged, &stream.epochs)?;
        }
        Ok(result)
    }

    /// Найти последний отсчёт каждого ряда-состояния до начала окна.
    ///
    /// Ищется обратным обходом от `from` назад, и он **ограничен**
    /// [`SEED_SEGMENTS`] сегментами: ряд, не менявшийся очень долго, останется
    /// без затравки, и это честнее, чем читать всю историю ради одного
    /// значения. Приложению, которому важна полная картина, стоит писать
    /// состояние ещё и при старте — тогда затравка всегда рядом.
    ///
    /// Сегменты, в которых нужных метрик нет вовсе, отбрасываются по множеству
    /// идентификаторов из footer'а, без чтения блоков.
    fn collect_state_seeds(
        &self,
        q: &Query,
        window: &Bounds,
        damaged: &mut Vec<Damage>,
        epochs: &Epochs,
    ) -> Result<Vec<Entry>> {
        let Some(from) = q.from else {
            return Ok(Vec::new());
        };

        // Ряды-состояния собираются по схемам, а не по данным: их немного, и
        // знать их заранее дешевле, чем выяснять чтением.
        //
        // Объединение по всем схемам нужно только для отсечения сегментов по
        // множеству из footer'а: там оно работает как «в этом сегменте нет
        // ничего похожего», и лишний номер приводит лишь к лишнему открытию.
        // Отбирать записи по нему нельзя — пространство `metric_id` своё у
        // каждой схемы, и метрика чужой схемы с тем же номером означает
        // совсем другую величину.
        let union: HashSet<MetricId> = self
            .all_schemas()
            .iter()
            .flat_map(|s| s.metrics.iter())
            .filter(|m| m.kind == MetricKind::State)
            .map(|m| m.id)
            .collect();
        if union.is_empty() {
            return Ok(Vec::new());
        }
        let union = std::sync::Arc::new(union);

        // Ищем строго ДО `from`, порядок — от свежих к старым, чтобы первым
        // встреченным отсчётом ряда оказался последний по времени.
        let probe = Query {
            from: None,
            to: Some(from.just_before()),
            order: Order::Newest,
            limit: None,
            seed_states: false,
            filter: Filter {
                kinds: KindFilter::TELEMETRY,
                ..q.filter.clone()
            },
            ..q.clone()
        };
        let probe_bounds = probe.resolve(epochs).bounds;

        let OpenedCursors {
            mut cursors,
            schemas,
            damaged: open_damaged,
        } = self.open_cursors_with(&probe, &probe_bounds, |scope| {
            scope.max_segments = Some(SEED_SEGMENTS);
            scope.require_metrics = Some(std::sync::Arc::clone(&union));
        })?;
        damaged.extend(open_damaged);

        // Первое встреченное значение ряда и есть последнее по времени.
        let mut seen: HashSet<(usize, MetricId)> = HashSet::new();
        let mut out: Vec<Entry> = Vec::new();

        for (idx, cursor) in cursors.iter_mut().enumerate() {
            // Ряды-состояния берутся из схемы ЭТОГО неймспейса: номера метрик
            // у разных схем свои, и общий набор дал бы затравку из чужого
            // ряда — с чужой подписью состояния.
            let wanted = state_metrics(schemas[idx].as_ref());
            if wanted.is_empty() {
                continue;
            }
            while let Some(raw) = cursor.next_entry() {
                // Затравка обязана лежать строго ДО окна. Проверяется именно
                // «не внутри окна», а не «раньше границы на микросекунду»:
                // граница может быть настенной, а у запуска, начавшегося с
                // нулевой микросекунды, вычитать не из чего.
                if window.contains(raw.at) || !probe_bounds.contains(raw.at) {
                    continue;
                }
                let OwnedRecord::Sample { metric, .. } = &raw.record else {
                    continue;
                };
                if !wanted.contains(metric) || !seen.insert((idx, *metric)) {
                    continue;
                }
                let ns = std::sync::Arc::clone(&cursor.namespace);
                let ch = cursor.channel;
                if let Some(entry) =
                    self.build_entry(ns, ch, schemas[idx].as_ref(), &raw, &probe, epochs)
                {
                    out.push(entry);
                }
                // Все ряды этого канала найдены — дальше читать нечего.
                if wanted.iter().all(|m| seen.contains(&(idx, *m))) {
                    break;
                }
            }
        }
        // Поиск затравки обрывается, едва найдены все нужные ряды, — то есть
        // почти всегда посреди сегмента. Повреждения недочитанного сегмента
        // обязаны попасть в отчёт и отсюда.
        for c in &cursors {
            damaged.extend(c.damaged());
        }

        out.sort_by_key(|e| e.at);
        Ok(out)
    }

    /// Открыть курсоры и разрешить схему **один раз на неймспейс**.
    ///
    /// Резолв схемы читает `ns-meta` с диска. Делать это на каждую запись,
    /// как было поначалу, означало бы файловую операцию на запись — именно
    /// это и оказалось главным ограничителем скорости чтения.
    fn open_cursors(&self, q: &Query, bounds: &Bounds) -> Result<OpenedCursors> {
        self.open_cursors_with(q, bounds, |_| {})
    }

    /// То же с правкой параметров открытия — для поиска затравок, где нужны
    /// граница просмотра и отсечение сегментов по метрикам.
    fn open_cursors_with(
        &self,
        q: &Query,
        bounds: &Bounds,
        adjust: impl Fn(&mut crate::cursor::ChannelScope),
    ) -> Result<OpenedCursors> {
        let mut cursors = Vec::new();
        let mut schemas = Vec::new();
        let mut damaged = Vec::new();

        for ns in self.scan_namespaces_matching(&mut damaged, |name| q.namespaces.matches(name))? {
            let schema = self.schema_by_name(&ns.schema_name);
            let ns_name: std::sync::Arc<str> = std::sync::Arc::from(ns.name.as_str());
            let mut scope = crate::cursor::ChannelScope {
                bounds: bounds.clone(),
                boot: q.boot,
                reverse: q.order == Order::Newest,
                expect_store: self.store_id,
                prefilter: Some(build_prefilter(q, schema)),
                max_segments: None,
                require_metrics: None,
                // Сегменты прежних версий приводятся к текущей прямо при
                // чтении: корректность ответа не ждёт физического прогона.
                migrations: schema.as_ref().map(crate::cursor::MigrationCtx::of),
                liveness: if self.live() {
                    crate::cursor::Liveness::Live
                } else {
                    crate::cursor::Liveness::Frozen
                },
            };
            adjust(&mut scope);

            for &(channel, ref dir) in &ns.channels {
                if !q.channels.is_empty() && !q.channels.contains(&channel) {
                    continue;
                }
                cursors.push(ChannelCursor::open(
                    dir,
                    std::sync::Arc::clone(&ns_name),
                    channel,
                    &scope,
                )?);
                schemas.push(schema);
            }
        }

        Ok(OpenedCursors {
            cursors,
            schemas,
            damaged,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_entry(
        &self,
        ns: std::sync::Arc<str>,
        channel: StorageClass,
        schema: Option<&Schema>,
        raw: &crate::cursor::RawEntry,
        q: &Query,
        epochs: &Epochs,
    ) -> Option<Entry> {
        let kinds = q.filter.kinds;
        // Фильтры, говорящие о содержимом. Запись, у которой такого свойства
        // нет (текст без тэгов, спан без типа события), удовлетворить им не
        // может и исключается — иначе фильтр «только с тэгом rf» пропускал бы
        // то, у чего тэгов не бывает. `min_level` сюда не входит: уровень есть
        // у сообщений и текста, а телеметрия и спаны вне шкалы уровней.
        let content_filtered = !q.filter.any_tags.is_empty()
            || q.filter.events.is_some()
            || !q.filter.event_names.is_empty();
        let (kind, span) = match &raw.record {
            OwnedRecord::Message {
                event,
                span,
                payload,
            } => {
                if !kinds.messages {
                    return None;
                }
                let desc = schema.and_then(|s| s.event(*event));
                // Уровень и тэги — статические свойства типа, поэтому фильтр
                // применяется здесь, без чтения payload'а.
                if let Some(min) = q.filter.min_level {
                    match desc.map(|d| d.level) {
                        Some(l) if l >= min => {}
                        // Уровень неизвестен — запись не отбрасывается: это
                        // событие удалённого из схемы типа, и молча прятать
                        // его от того, кто ищет проблему, нельзя.
                        None => {}
                        _ => return None,
                    }
                }
                if !q.filter.any_tags.is_empty() {
                    let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                    if !q
                        .filter
                        .any_tags
                        .iter()
                        .any(|want| tags.iter().any(|t| t == want))
                    {
                        return None;
                    }
                }
                if let Some(want) = &q.filter.events
                    && !want.contains(event)
                {
                    return None;
                }
                if !q.filter.event_names.is_empty() {
                    let name = desc.map(|d| d.name).unwrap_or("");
                    if !q.filter.event_names.iter().any(|n| n == name) {
                        return None;
                    }
                }
                (
                    EntryKind::Message {
                        event: *event,
                        name: desc.map(|d| d.name),
                        level: desc.map(|d| d.level),
                        tags: desc.map(|d| d.tags).unwrap_or(&[]),
                        payload: payload.clone(),
                    },
                    *span,
                )
            }
            OwnedRecord::Text {
                level,
                span,
                target,
                text,
            } => {
                if !kinds.text || content_filtered {
                    return None;
                }
                if let Some(min) = q.filter.min_level
                    && *level < min
                {
                    return None;
                }
                (
                    EntryKind::Text {
                        level: *level,
                        target: target.clone(),
                        text: text.clone(),
                    },
                    *span,
                )
            }
            OwnedRecord::SpanStart { span, kind, parent } => {
                if !kinds.spans || content_filtered {
                    return None;
                }
                (
                    EntryKind::SpanStart {
                        span: *span,
                        kind_name: schema.and_then(|s| s.span(*kind)).map(|d| d.name),
                        parent: *parent,
                    },
                    Some(*span),
                )
            }
            OwnedRecord::SpanEnd { span } => {
                if !kinds.spans || content_filtered {
                    return None;
                }
                (EntryKind::SpanEnd { span: *span }, Some(*span))
            }
            OwnedRecord::Sample { metric, value } => {
                if !kinds.samples || q.filter.events.is_some() || !q.filter.event_names.is_empty() {
                    return None;
                }
                // Идентичность ряда лежит в самой записи: метрика и есть ряд.
                // Всё остальное — имя, единица, подпись состояния, важность,
                // поведение между отсчётами — резолвится по схеме и на диске
                // места не занимает.
                let desc = schema.and_then(|s| s.metric(*metric));
                // Тэги у отсчёта есть — метрики: фильтр по ним применяется.
                if !q.filter.any_tags.is_empty() {
                    let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                    if !q
                        .filter
                        .any_tags
                        .iter()
                        .any(|want| tags.iter().any(|t| t == want))
                    {
                        return None;
                    }
                }
                let code = match value {
                    OwnedSampleValue::U64(v) => Some(*v),
                    OwnedSampleValue::I64(v) if *v >= 0 => Some(*v as u64),
                    OwnedSampleValue::Bool(b) => Some(u64::from(*b)),
                    _ => None,
                };
                (
                    EntryKind::Sample {
                        metric: *metric,
                        metric_name: desc.map(|d| d.name),
                        unit: desc.map(|d| d.unit),
                        tags: desc.map_or(&[][..], |d| d.tags),
                        kind: desc.map(|d| d.kind),
                        state_name: desc.zip(code).and_then(|(d, c)| d.state(c)).map(|s| s.name),
                        severity: desc.map(|d| d.severity_of(&as_format_value(value))),
                        value: value.clone(),
                    },
                    None,
                )
            }
            OwnedRecord::Ext { bytes } => {
                if content_filtered {
                    return None;
                }
                (
                    EntryKind::Ext {
                        bytes: bytes.clone(),
                    },
                    None,
                )
            }
        };

        // Записи вне спанов отбрасываются вместе со всеми, чей спан не
        // назван: «привязана к этому спану» неверно для обеих.
        if let Some(want) = &q.filter.spans
            && !span.is_some_and(|s| want.contains(&s))
        {
            return None;
        }

        Some(Entry {
            namespace: ns,
            channel,
            at: raw.at,
            utc: epochs.to_utc(raw.at),
            span,
            kind,
        })
    }
}

/// Собрать предикат отбора, применяемый до материализации записи.
///
/// Уровни и тэги — статические свойства типов, поэтому запрос вроде
/// «только ошибки» решается по схеме, без чтения payload'а; отброшенная
/// запись не стоит ни аллокации, ни копирования.
fn build_prefilter(q: &Query, schema: Option<Schema>) -> crate::cursor::Prefilter {
    let kinds = q.filter.kinds;
    let min_level = q.filter.min_level;
    let events = q.filter.events.clone();
    let event_names = q.filter.event_names.clone();
    let any_tags = q.filter.any_tags.clone();

    std::sync::Arc::new(move |record: &dduroc_format::Record<'_>| match record {
        dduroc_format::Record::Message(m) => {
            if !kinds.messages {
                return false;
            }
            if let Some(want) = &events
                && !want.contains(&m.event)
            {
                return false;
            }
            let desc = schema.and_then(|s| s.event(m.event));
            if let Some(min) = min_level {
                match desc.map(|d| d.level) {
                    Some(l) if l >= min => {}
                    // Уровень неизвестен — запись не прячем: это событие
                    // типа, удалённого из схемы, и скрыть его от того, кто
                    // ищет проблему, нельзя.
                    None => {}
                    _ => return false,
                }
            }
            if !any_tags.is_empty() {
                let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                if !any_tags.iter().any(|want| tags.iter().any(|t| t == want)) {
                    return false;
                }
            }
            if !event_names.is_empty() {
                let name = desc.map(|d| d.name).unwrap_or("");
                if !event_names.iter().any(|n| n == name) {
                    return false;
                }
            }
            true
        }
        // Контентные фильтры исключают записи, которые не могут им
        // удовлетворить: у текста и спанов нет ни тэгов, ни типа события,
        // и «прошёл фильтр по тэгу» для них было бы ложью. Уровень — не
        // контентный фильтр: у текста он есть и проверяется, телеметрия и
        // спаны вне шкалы уровней и им не отсеиваются.
        dduroc_format::Record::Text(t) => {
            kinds.text
                && min_level.is_none_or(|min| t.level >= min)
                && any_tags.is_empty()
                && events.is_none()
                && event_names.is_empty()
        }
        dduroc_format::Record::SpanStart(_) | dduroc_format::Record::SpanEnd { .. } => {
            kinds.spans && any_tags.is_empty() && events.is_none() && event_names.is_empty()
        }
        dduroc_format::Record::Sample(s) => {
            if !kinds.samples || events.is_some() || !event_names.is_empty() {
                return false;
            }
            // Тэги у отсчёта есть — метрики: фильтр по ним применяется.
            if !any_tags.is_empty() {
                let tags = schema
                    .and_then(|sc| sc.metric(s.metric))
                    .map(|d| d.tags)
                    .unwrap_or(&[]);
                if !any_tags.iter().any(|want| tags.iter().any(|t| t == want)) {
                    return false;
                }
            }
            true
        }
        dduroc_format::Record::Ext { .. } => {
            any_tags.is_empty() && events.is_none() && event_names.is_empty()
        }
    })
}

/// Метрики-состояния одной схемы. Пусто, если схема этому билду неизвестна.
fn state_metrics(schema: Option<&Schema>) -> HashSet<MetricId> {
    schema.map_or_else(HashSet::new, |s| {
        s.metrics
            .iter()
            .filter(|m| m.kind == MetricKind::State)
            .map(|m| m.id)
            .collect()
    })
}

fn read_ns_meta(dir: &Path) -> Option<NsMeta> {
    let bytes = std::fs::read(dir.join(NS_META)).ok()?;
    postcard::from_bytes(&bytes).ok()
}

fn read_store_id(root: &Path) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct Meta {
        #[allow(dead_code)]
        container_version: u8,
        store_id: u64,
    }
    let bytes = std::fs::read(root.join("store-meta")).ok()?;
    postcard::from_bytes::<Meta>(&bytes)
        .ok()
        .map(|m| m.store_id)
}

/// Отрендерить сообщение по шаблону схемы.
///
/// Низкоуровневый путь: схему приходится искать самому. Обычно нужен
/// [`Reader::render`] — он берёт её по неймспейсу записи.
///
/// Шаблоны на диске не хранятся: `{поле}` подставляется декодером,
/// сгенерированным макросом схемы. Без декодера возвращается сам шаблон.
pub fn render(schema: &Schema, event: EventId, payload: &[u8], lang: &str) -> Option<String> {
    let desc = schema.event(event)?;
    let lang_index = schema.language_index(lang).unwrap_or(0);
    match desc.decoders {
        Some(d) => (d.render)(payload, lang_index).ok(),
        None => desc.template(lang_index).map(|t| t.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::KindFilter;
    use dduroc_engine::schema::{EventDesc, Language, MetricDesc, SpanDesc, StorageClass};
    use dduroc_engine::store::{Store, StoreConfig};
    use dduroc_format::{MetricId, Micros, ProtocolVersion, SpanKindId, ValueType};

    static LANGS: &[Language] = &[Language("en"), Language("ru")];
    static EVENTS: &[EventDesc] = &[
        EventDesc {
            id: EventId(1),
            name: "PowerSet",
            level: Level::Info,
            class: StorageClass::Default,
            tags: &["rf"],
            templates: &["power set", "мощность задана"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::Critical,
            tags: &["fault"],
            templates: &["alarm", "авария"],
            fields: &[],
            decoders: None,
        },
    ];
    static LINK_STATES: &[dduroc_engine::schema::StateDesc] = &[
        dduroc_engine::schema::StateDesc {
            code: 0,
            name: "Los",
            severity: Severity::Alarm,
        },
        dduroc_engine::schema::StateDesc {
            code: 1,
            name: "Lock",
            severity: Severity::Normal,
        },
    ];
    static METRICS: &[MetricDesc] = &[
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "temp",
            value_type: ValueType::F32,
            class: StorageClass::Default,
            unit: "°C",
            tags: &["thermal"],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: dduroc_engine::schema::Thresholds {
                warn: dduroc_engine::schema::Range {
                    min: None,
                    max: Some(25.0),
                },
                alarm: dduroc_engine::schema::Range {
                    min: None,
                    max: Some(28.0),
                },
            },
        },
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(2),
            name: "link",
            value_type: ValueType::U64,
            class: StorageClass::Default,
            unit: "",
            tags: &["rf"],
            kind: MetricKind::State,
            states: LINK_STATES,
            thresholds: dduroc_engine::schema::Thresholds::NONE,
        },
    ];
    static SPANS: &[SpanDesc] = &[SpanDesc {
        id: SpanKindId(1),
        name: "Calibration",
        class: StorageClass::Default,
    }];

    fn schema() -> Schema {
        Schema {
            name: "radio",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: METRICS,
            spans: SPANS,
            migrations: &[],
        }
    }

    /// Типизированные константы метрик — то, что порождает макрос схемы.
    const TEMP: dduroc_engine::metric::Metric<f32> =
        dduroc_engine::metric::Metric::new(MetricId(1));

    /// Наполнить хранилище и закрыть его.
    fn populate(root: &Path) {
        let store =
            Store::open(StoreConfig::new(root).with_budget_per_class(16 * 1024 * 1024)).unwrap();
        for inst in 0..2 {
            let ns = store
                .namespace(&format!("orc-radio-{inst}"), schema())
                .unwrap();
            for i in 0..20u8 {
                ns.log_raw(EventId(1), &[i], None);
            }
            ns.log_raw(EventId(2), &[0xFF], None);

            let temp = ns.series(TEMP).unwrap();
            for i in 0..10 {
                temp.sample(20.0 + i as f32);
            }
            {
                let cal = ns.span(SpanKindId(1));
                cal.log_raw(EventId(1), &[99]);
            }
            ns.sync().unwrap();
        }
        let ns = store.namespace("apt-modem-0", schema()).unwrap();
        ns.log_raw(EventId(1), &[1], None);
        ns.sync().unwrap();
        store.shutdown();
    }

    #[test]
    fn reads_back_everything_written() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let listing = reader.namespaces().unwrap();
        assert!(listing.is_complete(), "все неймспейсы перечислены");
        let namespaces = listing.namespaces;
        assert_eq!(namespaces.len(), 3, "три неймспейса");
        assert_eq!(namespaces[0].name, "apt-modem-0");
        assert_eq!(namespaces[0].schema_name, "radio");
        assert!(namespaces[0].total_bytes > 0);

        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(result.is_complete(), "повреждений быть не должно");
        assert!(!result.entries.is_empty());

        // Сообщения обеих экземпляров и модема на месте.
        let messages = result
            .entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::Message { .. }))
            .count();
        assert_eq!(
            messages,
            2 * 22 + 1,
            "20 + аварийное + внутри спана, ×2, +1"
        );
    }

    #[test]
    fn schema_resolves_names_levels_and_units() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let result = reader
            .query(&Query::new().order(Order::Oldest).limit(500))
            .unwrap();

        let msg = result
            .entries
            .iter()
            .find(|e| {
                matches!(
                    &e.kind,
                    EntryKind::Message {
                        name: Some("PowerSet"),
                        ..
                    }
                )
            })
            .expect("имя события восстановлено по схеме");
        assert_eq!(msg.level(), Some(Level::Info), "уровень взят из схемы");

        let sample = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Sample { .. }))
            .expect("сэмпл найден");
        match &sample.kind {
            EntryKind::Sample {
                metric,
                metric_name,
                unit,
                tags,
                kind,
                state_name,
                severity,
                value,
            } => {
                assert_eq!(*metric, MetricId(1), "идентификатор пришёл из записи");
                assert_eq!(*metric_name, Some("temp"));
                assert_eq!(*unit, Some("°C"));
                assert_eq!(tags, &["thermal"]);
                assert_eq!(*kind, Some(MetricKind::Gauge));
                assert_eq!(*state_name, None, "не перечисление — подписи нет");
                assert!(severity.is_some(), "важность посчитана по схеме");
                assert!(value.as_f64().unwrap() >= 20.0);
            }
            other => panic!("ожидался сэмпл: {other:?}"),
        }

        let span = result.entries.iter().find(|e| {
            matches!(
                &e.kind,
                EntryKind::SpanStart {
                    kind_name: Some("Calibration"),
                    ..
                }
            )
        });
        assert!(span.is_some(), "вид спана восстановлен");
    }

    #[test]
    fn filters_by_group_level_and_kind() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        // Только оркестраторы.
        let orc = reader
            .query(&Query::new().group("orc-").order(Order::Oldest))
            .unwrap();
        assert!(
            orc.entries.iter().all(|e| e.namespace.starts_with("orc-")),
            "группа отобрана по префиксу"
        );

        // Только ошибки: уровень — свойство типа, читать payload не нужно.
        let errors = reader
            .query(
                &Query::new()
                    .min_level(Level::Error)
                    .kinds(KindFilter::LOGS)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(errors.entries.len(), 2, "по одной аварии на экземпляр");
        assert!(
            errors
                .entries
                .iter()
                .all(|e| e.level() == Some(Level::Error))
        );

        // Только телеметрия.
        let telemetry = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(telemetry.entries.len(), 20, "10 сэмплов × 2 экземпляра");
        assert!(
            telemetry
                .entries
                .iter()
                .all(|e| matches!(e.kind, EntryKind::Sample { .. }))
        );
    }

    #[test]
    fn content_filters_skip_records_that_cannot_match_them() {
        // Фильтр «только с тэгом rf» не имеет права пропустить свободный
        // текст: у текста тэгов нет, и совпадением это не было бы. Раньше
        // текст и спаны проходили любой фильтр по тэгам и типам — отметки о
        // потерях всплывали посреди ответа «только события подсистемы rf».
        // Тэги при этом есть не только у событий: отсчёт несёт тэги своей
        // метрики, и по ним фильтр обязан работать.
        let dir = tempfile::tempdir().unwrap();
        {
            let store =
                Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024))
                    .unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[7], None); // PowerSet, тэг rf
            ns.log_raw(EventId(2), &[1], None); // Alarm, тэг fault
            ns.log_text(Level::Warn, "app", "свободный текст без тэгов", None);
            ns.series(TEMP).unwrap().sample(21.0); // метрика temp, тэг thermal
            ns.series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::U64(1)); // link, тэг rf
            {
                let cal = ns.span(SpanKindId(1));
                cal.log_raw(EventId(1), &[8]); // rf, внутри спана
            }
            ns.sync().unwrap();
            store.shutdown();
        }
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let rf = reader
            .query(&Query::new().any_tag("rf").order(Order::Oldest))
            .unwrap();
        assert!(rf.is_complete());
        let mut messages = 0;
        let mut samples = 0;
        for e in &rf.entries {
            match &e.kind {
                EntryKind::Message { .. } => messages += 1,
                EntryKind::Sample { .. } => samples += 1,
                other => panic!("{other:?} не несёт тэга и не мог пройти фильтр"),
            }
        }
        assert_eq!(messages, 2, "PowerSet сам по себе и внутри спана");
        assert_eq!(samples, 1, "отсчёт link прошёл по тэгу своей метрики");

        // Фильтр по типу события: отсчёты, текст и спаны событиями не
        // являются и удовлетворить ему не могут.
        let alarms = reader
            .query(&Query::new().event(EventId(2)).order(Order::Oldest))
            .unwrap();
        assert_eq!(alarms.entries.len(), 1, "{:?}", alarms.entries);
        assert!(matches!(
            &alarms.entries[0].kind,
            EntryKind::Message {
                name: Some("Alarm"),
                ..
            }
        ));

        let by_name = reader
            .query(&Query::new().event_name("PowerSet").order(Order::Oldest))
            .unwrap();
        assert_eq!(by_name.entries.len(), 2, "{:?}", by_name.entries);
    }

    #[test]
    fn telemetry_keeps_identity_in_newest_order() {
        // Определение серии пишется в тело один раз, перед первым сэмплом.
        // При обратном обходе — режиме по умолчанию — сэмпл встречается
        // РАНЬШЕ своего определения, поэтому восстанавливать идентичность
        // из потока нельзя: вся телеметрия приходила бы обезличенной.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        for order in [Order::Oldest, Order::Newest] {
            let result = reader
                .query(&Query::new().kinds(KindFilter::TELEMETRY).order(order))
                .unwrap();
            assert_eq!(result.entries.len(), 20, "порядок {order:?}");
            for e in &result.entries {
                match &e.kind {
                    EntryKind::Sample {
                        metric_name,
                        unit,
                        tags,
                        ..
                    } => {
                        assert_eq!(*metric_name, Some("temp"), "порядок {order:?}");
                        assert_eq!(*unit, Some("°C"));
                        assert_eq!(tags, &["thermal"]);
                    }
                    other => panic!("ожидался сэмпл: {other:?}"),
                }
            }
        }
    }

    #[test]
    fn sparse_state_series_gets_seeded_at_the_window_edge() {
        // Состояния пишут по изменению. Окно, внутри которого состояние не
        // менялось, не содержит ни одного отсчёта — и полоса на графике
        // оказалась бы пустой, хотя состояние было известно всё это время.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // Переход в Lock — единственный отсчёт состояния, рано.
        let link = ns.series_untyped(MetricId(2)).unwrap();
        link.sample_raw(dduroc_engine::staged::OwnedValue::U64(1));
        let after_state = ns.now();

        // Дальше идёт только температура: окно будет без состояний.
        let temp = ns.series(TEMP).unwrap();
        for i in 0..20 {
            temp.sample(20.0 + i as f32);
        }
        ns.sync().unwrap();
        store.shutdown();
        drop(store);

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let window = Query {
            from: Some(BootTime::new(after_state.boot, Micros(after_state.at.0 + 1)).into()),
            order: Order::Oldest,
            filter: crate::Filter {
                kinds: KindFilter::TELEMETRY,
                ..Default::default()
            },
            ..Query::new()
        };

        // Без затравки состояния в ответе нет вовсе.
        let plain = reader.query(&window).unwrap();
        assert!(plain.seeds.is_empty());
        assert!(
            !plain.entries.iter().any(|e| matches!(
                &e.kind,
                EntryKind::Sample {
                    state_name: Some(_),
                    ..
                }
            )),
            "в окне действительно нет ни одного состояния"
        );

        // С затравкой — приходит последний отсчёт до окна, отдельно.
        let seeded = reader
            .query(&Query {
                seed_states: true,
                ..window
            })
            .unwrap();
        assert_eq!(seeded.seeds.len(), 1, "по одному на ряд-состояние");
        let seed = &seeded.seeds[0];
        assert!(
            seed.at <= after_state,
            "затравка обязана лежать ДО окна: {} vs {}",
            seed.at,
            after_state
        );
        match &seed.kind {
            EntryKind::Sample {
                metric,
                state_name,
                kind,
                severity,
                ..
            } => {
                assert_eq!(*metric, MetricId(2));
                assert_eq!(*state_name, Some("Lock"), "подпись состояния из схемы");
                assert_eq!(*kind, Some(MetricKind::State));
                assert_eq!(*severity, Some(Severity::Normal));
            }
            other => panic!("ожидался сэмпл состояния: {other:?}"),
        }
        // Окно от затравки не изменилось.
        assert_eq!(seeded.entries.len(), plain.entries.len());
    }

    #[test]
    fn state_seeds_come_from_the_schema_of_their_own_namespace() {
        // Пространство `metric_id` своё у каждой схемы. Ряды-состояния,
        // собранные объединением по всем схемам, дали бы затравку из чужого
        // ряда: метрика номер 2 у «radio» — конечный автомат, у «modem» —
        // обычная непрерывная величина, и подписывать её состояниями нельзя.
        static OTHER_METRICS: &[MetricDesc] = &[MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(2), // тот же номер, другая величина
            name: "voltage",
            value_type: ValueType::F32,
            class: StorageClass::Default,
            unit: "V",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: dduroc_engine::schema::Thresholds::NONE,
        }];
        fn other() -> Schema {
            Schema {
                name: "modem",
                metrics: OTHER_METRICS,
                ..schema()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let after;
        {
            let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
            let store = Store::open(cfg).unwrap();

            let radio = store.namespace("orc-radio-0", schema()).unwrap();
            radio
                .series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::U64(1));

            let modem = store.namespace("apt-modem-0", other()).unwrap();
            modem
                .series_untyped(MetricId(2))
                .unwrap()
                .sample_raw(dduroc_engine::staged::OwnedValue::F32(3.3));

            after = radio.now();
            // Дальше — только то, что окно и увидит.
            radio.series(TEMP).unwrap().sample(21.0);
            radio.sync().unwrap();
            modem.sync().unwrap();
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[schema(), other()]).unwrap();
        let seeded = reader
            .query(&Query {
                from: Some(BootTime::new(after.boot, Micros(after.at.0 + 1)).into()),
                order: Order::Oldest,
                seed_states: true,
                filter: crate::Filter {
                    kinds: KindFilter::TELEMETRY,
                    ..Default::default()
                },
                ..Query::new()
            })
            .unwrap();

        assert_eq!(
            seeded.seeds.len(),
            1,
            "затравка только у настоящего ряда-состояния: {:?}",
            seeded.seeds
        );
        let seed = &seeded.seeds[0];
        assert_eq!(&*seed.namespace, "orc-radio-0");
        match &seed.kind {
            EntryKind::Sample {
                state_name, kind, ..
            } => {
                assert_eq!(*state_name, Some("Lock"));
                assert_eq!(*kind, Some(MetricKind::State));
            }
            other => panic!("ожидалось состояние: {other:?}"),
        }
    }

    #[test]
    fn state_seed_is_absent_when_there_is_nothing_before_the_window() {
        // Запрос от начала времён: до окна ничего нет, и выдумывать затравку
        // неоткуда.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let r = reader
            .query(&Query {
                from: Some(BootTime::from_raw(0, 0).into()),
                seed_states: true,
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(r.seeds.is_empty());
        assert!(r.is_complete());
    }

    #[test]
    fn telemetry_identity_survives_an_unsealed_segment() {
        // Живое хранилище: сегмент ещё пишется, footer'а нет, а с ним нет и
        // таблицы серий. Идентичность приходится собирать проходом по телам —
        // тем же, которым находятся смещения блоков.
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        let ns = store.namespace("orc-radio-0", schema()).unwrap();
        let temp = ns.series(TEMP).unwrap();
        for i in 0..10 {
            temp.sample(20.0 + i as f32);
        }
        ns.sync().unwrap(); // данные на диске, но сегмент не запечатан

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        for order in [Order::Oldest, Order::Newest] {
            let result = reader
                .query(&Query::new().kinds(KindFilter::TELEMETRY).order(order))
                .unwrap();
            assert_eq!(result.entries.len(), 10, "порядок {order:?}");
            assert!(
                result.entries.iter().all(|e| matches!(
                    &e.kind,
                    EntryKind::Sample {
                        metric_name: Some("temp"),
                        unit: Some("°C"),
                        ..
                    }
                )),
                "порядок {order:?}: серия обезличена в незапечатанном сегменте"
            );
        }
        store.shutdown();
    }

    #[test]
    fn telemetry_identity_survives_time_range_seek() {
        // Запрос с нижней границей пропускает начальные блоки по
        // footer-индексу — вместе с лежащими там определениями серий.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid.into()),
                order: Order::Oldest,
                filter: crate::Filter {
                    kinds: KindFilter::TELEMETRY,
                    ..Default::default()
                },
                ..Query::new()
            })
            .unwrap();
        assert!(!narrowed.entries.is_empty());
        assert!(
            narrowed.entries.iter().all(|e| matches!(
                &e.kind,
                EntryKind::Sample {
                    metric_name: Some("temp"),
                    ..
                }
            )),
            "идентичность серии обязана пережить перескок по времени"
        );
    }

    #[test]
    fn newest_order_and_limit() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let newest = reader
            .query(&Query::new().order(Order::Newest).limit(5))
            .unwrap();
        assert_eq!(newest.entries.len(), 5);
        assert!(newest.truncated, "ответ обрезан по лимиту");

        // Время не возрастает.
        let times: Vec<BootTime> = newest.entries.iter().map(|e| e.at).collect();
        assert!(
            times.windows(2).all(|w| w[0] >= w[1]),
            "порядок от нового к старому: {times:?}"
        );

        let oldest = reader
            .query(&Query::new().order(Order::Oldest).limit(5))
            .unwrap();
        let times: Vec<BootTime> = oldest.entries.iter().map(|e| e.at).collect();
        assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");
    }

    #[test]
    fn truncation_is_announced_only_when_something_was_left_out() {
        // Отметка ставилась при входе в выдачу, а не тогда, когда запись
        // действительно не влезла: ответ ровно в `limit` записей объявлялся
        // обрезанным, даже если больше их и не было. Для веб-слоя это вечная
        // кнопка «дальше», ведущая в пустоту.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let total = all.entries.len();
        assert!(!all.truncated, "запрос без лимита не бывает обрезан");

        // Лимит ровно по числу записей: обрезать было нечего.
        let exact = reader
            .query(&Query::new().order(Order::Oldest).limit(total))
            .unwrap();
        assert_eq!(exact.entries.len(), total);
        assert!(
            !exact.truncated,
            "записей ровно {total}, больше нет — ответ полон"
        );

        // На единицу меньше — обрезан по-настоящему.
        let cut = reader
            .query(&Query::new().order(Order::Oldest).limit(total - 1))
            .unwrap();
        assert_eq!(cut.entries.len(), total - 1);
        assert!(cut.truncated, "одна запись осталась за бортом");

        // Пустой ответ при нулевом лимите: записи есть, места нет.
        let none = reader
            .query(&Query::new().order(Order::Oldest).limit(0))
            .unwrap();
        assert!(none.entries.is_empty());
        assert!(none.truncated);
    }

    #[test]
    fn render_resolves_the_schema_without_touching_the_disk() {
        // Резолв схемы читал `ns-meta` на КАЖДЫЙ вызов: отрисовка пятисот
        // строк стоила пятисот открытий файла — ровно тот дефект, который
        // уже убран с пути запроса. Проверяется тем, что рендер продолжает
        // работать, когда файла на диске больше нет.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        assert_eq!(reader.render(msg, "ru").as_deref(), Some("мощность задана"));

        std::fs::remove_file(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        assert_eq!(
            reader.render(msg, "ru").as_deref(),
            Some("мощность задана"),
            "схема обязана быть разрешена один раз, а не на каждый вызов"
        );
    }

    #[test]
    fn merge_is_ordered_across_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let keys: Vec<BootTime> = all.entries.iter().map(|e| e.at).collect();
        assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "слияние обязано давать глобальный порядок по времени"
        );
        // В ответе присутствуют оба неймспейса вперемешку.
        let namespaces: std::collections::HashSet<_> =
            all.entries.iter().map(|e| &*e.namespace).collect();
        assert!(namespaces.len() >= 2);
    }

    #[test]
    fn unknown_schema_leaves_raw_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        // Читаем без схем — как чужой билд.
        let reader = Reader::open_dump([dir.path()], &[]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();

        assert!(!result.entries.is_empty(), "записи всё равно читаются");
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        match &msg.kind {
            EntryKind::Message { name, level, .. } => {
                assert!(name.is_none(), "имя без схемы неизвестно");
                assert!(level.is_none(), "уровень без схемы неизвестен");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn time_range_narrows_result() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid.into()),
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(narrowed.entries.iter().all(|e| e.at >= mid));
        assert!(narrowed.entries.len() < all.entries.len());
    }

    #[test]
    fn foreign_store_segments_are_reported_not_silently_dropped() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        // Подменяем store-meta: сегменты станут «чужими».
        let meta_path = dir.path().join("store-meta");
        #[derive(serde::Serialize)]
        struct Meta {
            container_version: u8,
            store_id: u64,
        }
        std::fs::write(
            &meta_path,
            postcard::to_allocvec(&Meta {
                container_version: 1,
                store_id: 0xDEAD_BEEF,
            })
            .unwrap(),
        )
        .unwrap();

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "чужие сегменты обязаны попасть в список повреждений, а не исчезнуть"
        );
        assert!(result.entries.is_empty());

        // Явное разрешение читает их как есть.
        let reader = Reader::open_dump([dir.path()], &[schema()])
            .unwrap()
            .allow_foreign_segments();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!result.entries.is_empty());
    }

    #[test]
    fn corrupt_block_does_not_hide_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        // Портим середину одного сегмента.
        let ch = dir.path().join("orc-radio-0").join("default");
        let seg = std::fs::read_dir(&ch)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all_at(&[0xFF; 16], 40).unwrap();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "о повреждении обязано быть сказано явно"
        );
        // Данные других неймспейсов не пострадали.
        assert!(
            result
                .entries
                .iter()
                .any(|e| &*e.namespace == "orc-radio-1" || &*e.namespace == "apt-modem-0"),
            "порча одного сегмента не должна прятать остальные"
        );
    }

    #[test]
    fn damage_is_reported_even_when_the_walk_stops_early() {
        // Повреждения переезжали из сегмента в отчёт только при его
        // закрытии, а обход обрывается по `limit` посреди сегмента. Ответ, из
        // которого выпал блок, объявлял себя полным — то есть врал ровно в том
        // поле, ради которого оно заведено.
        let dir = tempfile::tempdir().unwrap();
        {
            let store =
                Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024))
                    .unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            // Восемь блоков в одном сегменте: `sync` закрывает блок, поэтому
            // сегмент заведомо не будет дочитан до конца под лимитом.
            for round in 0..8u8 {
                for i in 0..5u8 {
                    ns.log_raw(EventId(1), &[round, i], None);
                }
                ns.sync().unwrap();
            }
            store.shutdown();
        }

        let ch = dir.path().join("orc-radio-0").join("default");
        let seg = std::fs::read_dir(&ch)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        {
            // Первый блок сегмента: заголовок начинается сразу за заголовком
            // сегмента (32 байта).
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all_at(&[0xFF; 16], 40).unwrap();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader
            .query(&Query::new().order(Order::Oldest).limit(1))
            .unwrap();
        assert_eq!(result.entries.len(), 1, "лимит соблюдён");
        assert!(result.truncated, "обход оборван, сегмент не дочитан");
        assert!(
            !result.is_complete(),
            "часть данных выпала из-за порчи — ответ не полон"
        );

        // Тот же ответ у ленивого пути: читатель обязан признаваться в
        // пропаже и когда обход прерывает сам вызывающий.
        let mut stream = reader.stream(&Query::new().order(Order::Oldest)).unwrap();
        let _first = stream.next().expect("хоть одна запись есть");
        assert!(
            !stream.damaged().is_empty(),
            "повреждение видно, не дожидаясь конца обхода"
        );
    }

    #[test]
    fn utc_is_resolved_when_anchor_exists() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store
                .record_sync(
                    DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                    dduroc_engine::SyncSource::Gps,
                )
                .unwrap();
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let entry = &result.entries[0];
        let utc = entry
            .utc
            .expect("якорь ретроактивен: событие записано ДО синхронизации");
        assert!(
            (1_699_999_000_000..1_700_001_000_000).contains(&utc.timestamp_millis()),
            "UTC рядом с точкой синхронизации: {utc}"
        );
    }

    #[test]
    fn wall_clock_window_selects_the_same_records_as_the_relative_one() {
        // Синхронизированный прибор: одно и то же окно, заданное настенными
        // часами и относительным временем, обязано дать одну выборку.
        // Иначе граница «с 12:00» врала бы ровно на ошибку конверсии.
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget_per_class(16 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            store
                .record_sync(
                    DateTime::from_timestamp_millis(1_700_000_000_000).unwrap(),
                    dduroc_engine::SyncSource::Gps,
                )
                .unwrap();
            for i in 0..40u8 {
                ns.log_raw(EventId(1), &[i], None);
            }
            ns.sync().unwrap();
            store.shutdown();
        }

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert_eq!(all.entries.len(), 40);
        let mid = &all.entries[all.entries.len() / 2];
        let mid_utc = mid.utc.expect("якорь есть");

        let by_wall = reader
            .query(&Query::new().order(Order::Oldest).since(mid_utc))
            .unwrap();
        let by_relative = reader
            .query(&Query::new().order(Order::Oldest).since(mid.at))
            .unwrap();

        assert!(by_wall.unanchored.is_empty(), "запуск синхронизирован");
        assert_eq!(
            by_wall.entries, by_relative.entries,
            "настенное и относительное окно обязаны дать одну выборку: \
             конверсия по якорю точна до микросекунды"
        );
        assert!(by_wall.entries.iter().all(|e| e.at >= mid.at));
        assert!(by_wall.entries.len() < all.entries.len());

        // Верхняя граница — симметрично.
        let until_wall = reader
            .query(&Query::new().order(Order::Oldest).until(mid_utc))
            .unwrap();
        let until_relative = reader
            .query(&Query::new().order(Order::Oldest).until(mid.at))
            .unwrap();
        assert_eq!(until_wall.entries, until_relative.entries);
        assert!(until_wall.entries.iter().all(|e| e.at <= mid.at));
        // Вместе половины покрывают всё; пересечение — записи ровно на
        // границе, а их может быть больше одной: часы монотонны, но не строго
        // возрастают, и во всплеске два события получают одну микросекунду.
        let on_edge = all.entries.iter().filter(|e| e.at == mid.at).count();
        assert_eq!(
            until_wall.entries.len() + by_wall.entries.len(),
            all.entries.len() + on_edge
        );
    }

    #[test]
    fn wall_clock_window_reports_runs_it_had_to_drop() {
        // Прибор без синхронизации. Запрос по настенным часам не может
        // сказать, попадают ли его записи в окно, — и обязан сообщить, что
        // выборка неполна, а не показать пустоту.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let utc = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).since(utc))
            .unwrap();

        assert!(r.entries.is_empty(), "сопоставить нечем");
        assert_eq!(
            r.unanchored,
            vec![BootCounter(0)],
            "выпавший запуск обязан быть назван: молчание выглядело бы как \
             «прибор ничего не писал»"
        );

        // Относительное окно работает и без синхронизации — оно ни от чего
        // не зависит.
        let r = reader
            .query(
                &Query::new()
                    .order(Order::Oldest)
                    .since(BootTime::from_raw(0, 0)),
            )
            .unwrap();
        assert!(!r.entries.is_empty());
        assert!(r.unanchored.is_empty());
    }

    #[test]
    fn stream_reads_without_collecting_everything() {
        // Ради этого поток и существует: ответ на запрос без `limit` по
        // большому хранилищу в память не поместился бы, а прервать обход
        // посередине `query` не позволяет.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let q = Query::new().order(Order::Oldest);

        // Первые пять записей — и обход прекращён, остальные сегменты даже не
        // дочитаны.
        let head: Vec<Entry> = reader.stream(&q).unwrap().take(5).collect();
        assert_eq!(head.len(), 5);

        // Поток и собранный ответ обязаны совпадать запись в запись: иначе
        // «то же самое, но лениво» было бы неправдой.
        let whole: Vec<Entry> = reader.stream(&q).unwrap().collect();
        let collected = reader.query(&q).unwrap();
        assert_eq!(whole, collected.entries);
        assert_eq!(&whole[..5], &head[..]);

        // Лимит и признак обрезки работают и в потоке.
        let mut limited = reader.stream(&q.clone().limit(3)).unwrap();
        assert_eq!(limited.by_ref().count(), 3);
        assert!(limited.truncated());
        assert_eq!(limited.yielded(), 3);
    }

    #[test]
    fn render_finds_the_schema_by_the_entry_itself() {
        // Схему больше не надо искать вызывающему: передать чужую значило бы
        // разобрать payload декодером другого события — текст вышел бы
        // правдоподобным и неверным.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).kinds(KindFilter::LOGS))
            .unwrap();

        let msg = r
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        assert_eq!(reader.render(msg, "ru").as_deref(), Some("мощность задана"));
        assert_eq!(reader.render(msg, "en").as_deref(), Some("power set"));
        // Неизвестный язык — первый объявленный, а не отказ.
        assert_eq!(reader.render(msg, "de").as_deref(), Some("power set"));

        // Читателю без схем рендерить нечем, и выдумывать он не станет.
        let blind = Reader::open_dump([dir.path()], &[]).unwrap();
        assert_eq!(blind.render(msg, "ru"), None);
    }

    #[test]
    fn dump_without_epochs_file_still_names_its_runs() {
        // Дамп скопировали без `epochs.bin` — так бывает, когда забирают
        // только каталог. Записи на месте, но настенного времени у них нет ни
        // у одной, а перечислить выпавшие запуски по эпохам невозможно:
        // единственный источник — имена сегментов.
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        std::fs::remove_file(dir.path().join(dduroc_engine::epochs::EPOCHS_FILE)).unwrap();

        let reader = Reader::open_dump([dir.path()], &[schema()]).unwrap();
        let utc = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let r = reader
            .query(&Query::new().order(Order::Oldest).since(utc))
            .unwrap();

        assert!(r.entries.is_empty());
        assert_eq!(
            r.unanchored,
            vec![BootCounter(0)],
            "без реестра эпох запуск известен только по имени сегмента"
        );

        // Без настенных границ дамп читается как обычно: относительное время
        // лежит в самих файлах.
        let r = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!r.entries.is_empty());
        assert!(r.entries.iter().all(|e| e.utc.is_none()));
        assert!(r.unanchored.is_empty());
    }
}
