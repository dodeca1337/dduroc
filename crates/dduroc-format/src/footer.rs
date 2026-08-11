//! Footer запечатанного сегмента: индекс блоков и множества встреченных типов.
//!
//! ```text
//! [индекс блоков] [event_id'ы] [metric_id'ы] [Trailer 32B]
//! ```
//!
//! Footer — **оптимизация, а не необходимость**: он позволяет найти блок по
//! времени без чтения тел и понять, какие типы есть в сегменте, не сканируя
//! его. Если footer повреждён или отсутствует (сегмент активен либо оборвано
//! питание при seal'е), читатель деградирует к последовательному обходу
//! заголовков блоков — данные не теряются.
//!
//! Множества типов нужны миграции: сегмент, не содержащий затронутых
//! `event_id`/`metric_id`, переписывать не нужно — экономия ресурса флеша.
//! Множество метрик заодно отвечает на вопрос «какая телеметрия есть в этом
//! сегменте» — ради него таблица серий и существовала, пока идентичность ряда
//! была парой `(метрика, рантайм-тэги)`. Тэгов больше нет, идентичность равна
//! метрике, и множества идентификаторов достаточно.
//!
//! Признак запечатанности — сигнатура в последних четырёх байтах файла.

use crate::block::BlockHeader;
use crate::cursor::Cursor;
use crate::error::{Error, Result};
use crate::ids::{EventId, MetricId, Micros};
use crate::segment::SegmentHeader;
use crate::varint;

/// Сигнатура в последних 4 байтах запечатанного сегмента.
pub const FOOTER_MAGIC: [u8; 4] = *b"DFTR";

/// Запись индекса блоков.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockIndexEntry {
    /// Смещение блока от начала файла.
    pub offset: u64,
    /// Время первой записи блока.
    pub base: Micros,
    pub count: u16,
}

/// Хвостовой блок footer'а фиксированного размера.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trailer {
    /// Длина секций footer'а до трейлера.
    pub sections_len: u32,
    pub block_count: u32,
    /// Время первой записи сегмента.
    pub min: Micros,
    /// Время последней записи сегмента.
    pub max: Micros,
    pub crc: u32,
}

impl Trailer {
    pub const SIZE: usize = 32;

    /// Разобрать трейлер из последних [`Trailer::SIZE`] байт файла.
    /// `Ok(None)` — сегмент не запечатан (нет сигнатуры).
    pub fn parse(last_bytes: &[u8]) -> Result<Option<Self>> {
        let raw: &[u8; Self::SIZE] = last_bytes
            .get(last_bytes.len().wrapping_sub(Self::SIZE)..)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::Truncated)?;

        let magic: [u8; 4] = raw[28..32].try_into().expect("срез 4 байта");
        if magic != FOOTER_MAGIC {
            return Ok(None);
        }

        Ok(Some(Self {
            sections_len: u32::from_le_bytes(raw[0..4].try_into().expect("срез 4 байта")),
            block_count: u32::from_le_bytes(raw[4..8].try_into().expect("срез 4 байта")),
            min: Micros(u64::from_le_bytes(raw[8..16].try_into().expect("срез 8"))),
            max: Micros(u64::from_le_bytes(raw[16..24].try_into().expect("срез 8"))),
            crc: u32::from_le_bytes(raw[24..28].try_into().expect("срез 4 байта")),
        }))
    }

    /// Полный размер footer'а на диске (секции + трейлер).
    pub fn total_len(&self) -> u64 {
        u64::from(self.sections_len) + Self::SIZE as u64
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.sections_len.to_le_bytes());
        out.extend_from_slice(&self.block_count.to_le_bytes());
        out.extend_from_slice(&self.min.0.to_le_bytes());
        out.extend_from_slice(&self.max.0.to_le_bytes());
        out.extend_from_slice(&self.crc.to_le_bytes());
        out.extend_from_slice(&FOOTER_MAGIC);
    }
}

/// Разобранный footer.
#[derive(Debug, Clone, PartialEq)]
pub struct Footer {
    pub blocks: Vec<BlockIndexEntry>,
    /// Типы сообщений, встречающиеся в сегменте (возрастающий порядок).
    pub events: Vec<EventId>,
    /// Метрики, встречающиеся в сегменте (возрастающий порядок).
    ///
    /// Отвечает и на вопрос миграции «затронут ли сегмент», и на вопрос
    /// читателя «какая телеметрия здесь есть»: идентичность ряда равна
    /// метрике, поэтому перечислить метрики значит перечислить ряды.
    pub metrics: Vec<MetricId>,
    pub min: Micros,
    pub max: Micros,
}

impl Footer {
    /// Разобрать footer из хвоста файла. `bytes` обязан **заканчиваться**
    /// последним байтом файла и содержать footer целиком
    /// (длина известна из [`Trailer::parse`]).
    pub fn parse(bytes: &[u8]) -> Result<Option<Self>> {
        let Some(trailer) = Trailer::parse(bytes)? else {
            return Ok(None);
        };

        let total = trailer.total_len();
        let start = (bytes.len() as u64)
            .checked_sub(total)
            .ok_or(Error::Truncated)?;
        let start = usize::try_from(start).map_err(|_| Error::Truncated)?;
        let sections = &bytes[start..bytes.len() - Trailer::SIZE];

        let crc = {
            let trailer_start = bytes.len() - Trailer::SIZE;
            let c = crc32c::crc32c(sections);
            crc32c::crc32c_append(c, &bytes[trailer_start..trailer_start + 24])
        };
        if crc != trailer.crc {
            return Err(Error::CrcMismatch {
                expected: trailer.crc,
                actual: crc,
            });
        }

        let mut c = Cursor::new(sections);

        // Индекс блоков: дельты смещений от конца заголовка сегмента и
        // дельты времени от предыдущего блока.
        //
        // Ёмкость НИКОГДА не выделяется по счётчику из файла: CRC32C — не
        // подпись, его пересчитывает кто угодно, а `block_count` в трейлере
        // напрямую управлял бы размером аллокации. Потолок — сколько записей
        // физически помещается в секции (минимум 3 байта на запись).
        let mut blocks = Vec::with_capacity(bounded(trailer.block_count, sections.len(), 3));
        let mut offset = SegmentHeader::SIZE as u64;
        let mut base = 0u64;
        for _ in 0..trailer.block_count {
            offset = offset.checked_add(c.varint()?).ok_or(Error::Truncated)?;
            base = base.checked_add(c.varint()?).ok_or(Error::Truncated)?;
            let count = c.varint_u16("records per block")?;
            blocks.push(BlockIndexEntry {
                offset,
                base: Micros(base),
                count,
            });
        }

        let events = read_id_set(&mut c, "event_id", sections.len())?
            .into_iter()
            .map(|v| EventId(v as u16))
            .collect();
        let metrics = read_id_set(&mut c, "metric_id", sections.len())?
            .into_iter()
            .map(|v| MetricId(v as u16))
            .collect();

        // Секции обязаны быть разобраны без остатка: длина в трейлере
        // согласована с CRC, поэтому лишние байты означают не запас, а
        // несоответствие содержимого заявленной структуре.
        if c.pos() != sections.len() {
            return Err(Error::LimitExceeded {
                what: "footer sections",
                value: c.pos() as u64,
                max: sections.len() as u64,
            });
        }

        Ok(Some(Self {
            blocks,
            events,
            metrics,
            min: trailer.min,
            max: trailer.max,
        }))
    }

    /// Индекс блока, который может содержать записи со временем `at`:
    /// последний блок с `base <= at`. `None` — `at` раньше первого блока.
    /// Индекс блока, который может содержать записи со временем `at`:
    /// **первый** из блоков с одинаковой базой, не превышающей `at`.
    ///
    /// Бинарный поиск на дубликатах отдаёт произвольное совпадение, поэтому
    /// используется граница разбиения: блоки с равной базой — обычное дело
    /// (пачка записей одной микросекунды), и начать с середины такой группы
    /// значило бы потерять её начало.
    pub fn block_for_time(&self, at: Micros) -> Option<usize> {
        let first_after = self.blocks.partition_point(|b| b.base <= at);
        if first_after == 0 {
            return None;
        }
        // Отступаем к началу группы блоков с той же базой.
        let base = self.blocks[first_after - 1].base;
        let start = self.blocks[..first_after].partition_point(|b| b.base < base);
        Some(start)
    }

    /// Пересекается ли сегмент с указанными типами — критерий для миграции:
    /// не пересекается ⇒ переписывать сегмент не нужно.
    pub fn touches(&self, events: &[EventId], metrics: &[MetricId]) -> bool {
        events.iter().any(|e| self.events.binary_search(e).is_ok())
            || metrics
                .iter()
                .any(|m| self.metrics.binary_search(m).is_ok())
    }
}

/// Безопасная стартовая ёмкость: не больше, чем физически влезло бы в
/// `available` байт при `min_entry` байтах на элемент.
///
/// Разбор всё равно упрётся в конец секций и вернёт ошибку, но выделять
/// гигабайты по счётчику из недоверенного файла нельзя: на armv7 это паника
/// «capacity overflow», на 64-битном вьюере — OOM-killer.
fn bounded(count: u32, available: usize, min_entry: usize) -> usize {
    (count as usize)
        .min(available / min_entry.max(1))
        .min(PREALLOC_MAX)
}

/// Потолок предварительного выделения по счётчику из файла.
///
/// Одного [`bounded`] мало: он считает, сколько элементов физически влезло бы
/// в секции, но влезает-то на диске, а в памяти элемент крупнее — индекс
/// блоков втрое (три varint'а против двадцати четырёх байт), множество id
/// ввосьмеро (байт против `u64`). Без потолка секция в шестьдесят мегабайт
/// требовала бы полгигабайта, и файл с валидным CRC (а CRC32C — не подпись,
/// его пересчитывает кто угодно) укладывал бы вьюер OOM-killer'ом.
///
/// Потолок разрывает связь между числом из файла и размером аллокации.
/// Вектор дорастёт сам, если данных действительно столько: ни один разбор от
/// этого не откажет, а лишние реаллокации на фоне чтения файла не видны.
const PREALLOC_MAX: usize = 4096;

fn read_id_set(c: &mut Cursor<'_>, what: &'static str, available: usize) -> Result<Vec<u64>> {
    let n = c.varint_u32(what)?;
    let mut out = Vec::with_capacity(bounded(n, available, 1));
    let mut prev = 0u64;
    for i in 0..n {
        let delta = c.varint()?;
        // Первый элемент — абсолютное значение, дальше строго возрастающие
        // дельты: нулевая дельта означала бы дубль в множестве.
        if i > 0 && delta == 0 {
            return Err(Error::ReservedValue);
        }
        prev = prev.checked_add(delta).ok_or(Error::Truncated)?;
        if prev > u64::from(u16::MAX) {
            return Err(Error::LimitExceeded {
                what,
                value: prev,
                max: u64::from(u16::MAX),
            });
        }
        out.push(prev);
    }
    Ok(out)
}

// ════════════════════════════════════════════════════════════════════════════
// Сборка
// ════════════════════════════════════════════════════════════════════════════

/// Накопитель footer'а: движок кормит его по мере записи блоков, а при seal'е
/// получает готовые байты.
/// Отсортированное множество идентификаторов на плоском векторе.
///
/// Не `BTreeSet`, потому что `insert` вызывается **на каждую запись**: у
/// сообщения — его тип, у отсчёта — его метрика. Типов в схеме сотни, то есть
/// множество крошечное, и на таком размере непрерывная память с бинарным
/// поиском заметно быстрее дерева с его разыменованиями. Плюс дешёвая защёлка
/// на последнее добавленное: подряд идущие записи одного типа — обычное дело.
#[derive(Debug, Default)]
struct IdSet {
    ids: Vec<u16>,
    last: Option<u16>,
}

impl IdSet {
    #[inline]
    fn insert(&mut self, id: u16) {
        if self.last == Some(id) {
            return;
        }
        self.last = Some(id);
        if let Err(pos) = self.ids.binary_search(&id) {
            self.ids.insert(pos, id);
        }
    }

    fn clear(&mut self) {
        self.ids.clear();
        self.last = None;
    }

    fn len(&self) -> usize {
        self.ids.len()
    }

    fn iter(&self) -> impl Iterator<Item = u16> + '_ {
        self.ids.iter().copied()
    }
}

#[derive(Debug, Default)]
pub struct FooterBuilder {
    blocks: Vec<BlockIndexEntry>,
    events: IdSet,
    metrics: IdSet,
    /// Типы, встреченные в **ещё не записанном** блоке.
    ///
    /// Отдельная ступень, потому что блок не обязан лечь в тот сегмент, над
    /// которым он собирался: не поместившийся блок переезжает в свежий
    /// (см. правила записи в SPEC §5). Без ступени его типы оказывались бы в
    /// footer'е сегмента, где блока **нет**, и отсутствовали бы в том, где он
    /// есть, — а по этим множествам миграция решает, переписывать ли сегмент,
    /// и читатель судит, есть ли в нём нужная телеметрия. Оба ответа
    /// оказывались бы неверными.
    pending_events: IdSet,
    pending_metrics: IdSet,
    min: Option<Micros>,
    max: Micros,
}

impl FooterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Зарегистрировать записанный блок.
    ///
    /// Индекс обязан оставаться неубывающим по времени: по нему делается
    /// бинарный поиск, а `build` кодирует дельты. Блок с базой раньше
    /// предыдущей (переупорядочивание записей между потоками) не отбрасывается
    /// — его база подтягивается к предыдущей, чтобы индекс остался
    /// сортированным, а фактический минимум учитывается в `min` отдельно.
    /// Типы накопленного блока переходят в множества сегмента здесь же:
    /// блок записан, значит его типы в этом сегменте есть — и ровно в этом.
    pub fn add_block(&mut self, offset: u64, header: &BlockHeader, last: Micros) {
        for id in self.pending_events.iter() {
            self.events.insert(id);
        }
        for id in self.pending_metrics.iter() {
            self.metrics.insert(id);
        }
        self.discard_pending();

        let prev = self.blocks.last().map_or(0, |b| b.base.0);
        self.blocks.push(BlockIndexEntry {
            offset,
            base: Micros(header.base.0.max(prev)),
            count: header.count,
        });
        // Минимум и максимум — по фактическим значениям, а не по первому и
        // последнему блоку: иначе отбор сегментов по диапазону времени молча
        // выбрасывал бы сегмент, содержащий искомые записи.
        self.min = Some(match self.min {
            Some(m) => Micros(m.0.min(header.base.0)),
            None => header.base,
        });
        self.max = Micros(self.max.0.max(last.0).max(header.base.0));
    }

    /// Отметить встреченный тип сообщения. Вызывается на каждую запись.
    ///
    /// Попадает в ступень незакрытого блока и переходит в множество сегмента
    /// в [`Self::add_block`] — то есть тогда, когда известно, в какой сегмент
    /// блок лёг.
    #[inline]
    pub fn add_event(&mut self, id: EventId) {
        self.pending_events.insert(id.0);
    }

    /// Отметить встреченную метрику. Вызывается на каждый отсчёт.
    ///
    /// Множество отвечает и миграции («затронут ли сегмент»), и читателю
    /// («какая телеметрия здесь есть»). Пустым оно быть не должно ни в одном
    /// сегменте с телеметрией — иначе миграция молча пропустила бы историю.
    #[inline]
    pub fn add_metric(&mut self, id: MetricId) {
        self.pending_metrics.insert(id.0);
    }

    /// Забыть типы незакрытого блока: блок отброшен и никуда не ляжет.
    pub fn discard_pending(&mut self) {
        self.pending_events.clear();
        self.pending_metrics.clear();
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty()
    }

    /// Собрать байты footer'а (секции + трейлер).
    pub fn build(&self) -> Vec<u8> {
        let mut sections = Vec::new();

        let mut prev_offset = SegmentHeader::SIZE as u64;
        let mut prev_base = 0u64;
        for b in &self.blocks {
            varint::write_u64(&mut sections, b.offset.saturating_sub(prev_offset));
            varint::write_u64(&mut sections, b.base.0.saturating_sub(prev_base));
            varint::write_u64(&mut sections, u64::from(b.count));
            prev_offset = b.offset;
            prev_base = b.base.0;
        }

        write_id_set(&mut sections, &self.events);
        write_id_set(&mut sections, &self.metrics);

        let mut trailer = Trailer {
            sections_len: sections.len() as u32,
            block_count: self.blocks.len() as u32,
            min: self.min.unwrap_or(Micros(0)),
            max: self.max,
            crc: 0,
        };

        // CRC считается по секциям и первым 24 байтам трейлера — то есть по
        // всему footer'у, кроме самого поля CRC и сигнатуры.
        let mut trailer_bytes = Vec::with_capacity(Trailer::SIZE);
        trailer.write(&mut trailer_bytes);
        let crc = crc32c::crc32c_append(crc32c::crc32c(&sections), &trailer_bytes[..24]);
        trailer.crc = crc;

        let mut out = sections;
        trailer.write(&mut out);
        out
    }

    /// Начать footer нового сегмента.
    ///
    /// Ступень незакрытого блока **сохраняется**: сегмент меняют как раз
    /// потому, что блок в прежний не поместился, и его типы обязаны уехать
    /// вместе с ним. Блок, который решили не писать вовсе, снимается
    /// отдельным [`Self::discard_pending`].
    pub fn reset(&mut self) {
        self.blocks.clear();
        self.events.clear();
        self.metrics.clear();
        self.min = None;
        self.max = Micros(0);
    }

    /// Отдать слак индекса, сохранив накопленное.
    ///
    /// Индекс блоков растёт на запись за каждый flush и после `reset`
    /// сохраняет пиковую ёмкость: критический канал с 8-КиБ блоками при
    /// 256-МиБ сегменте накапливает до ~770 КиБ на канал — навсегда.
    /// Живые записи открытого сегмента при этом нужны до самого seal, поэтому
    /// здесь именно `shrink_to_fit`, а не сброс. Вызывается движком при
    /// бездействии канала.
    pub fn shrink_to_fit(&mut self) {
        self.blocks.shrink_to_fit();
        self.events.ids.shrink_to_fit();
        self.metrics.ids.shrink_to_fit();
        self.pending_events.ids.shrink_to_fit();
        self.pending_metrics.ids.shrink_to_fit();
    }
}

fn write_id_set(out: &mut Vec<u8>, set: &IdSet) {
    varint::write_u64(out, set.len() as u64);
    let mut prev = 0u64;
    for id in set.iter() {
        varint::write_u64(out, u64::from(id) - prev);
        prev = u64::from(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Compression;

    fn header(base: u64, count: u16) -> BlockHeader {
        BlockHeader {
            body_len: 10,
            raw_len: 10,
            seq: 0,
            base: Micros(base),
            count,
            compression: Compression::None,
            crc: 0,
        }
    }

    /// Порядок как у writer'а: типы приходят с записями собираемого блока и
    /// закрепляются за сегментом, когда блок в него лёг.
    fn sample_builder() -> FooterBuilder {
        let mut b = FooterBuilder::new();
        b.add_event(EventId(1));
        b.add_metric(MetricId(5));
        b.add_block(32, &header(1_000, 5), Micros(1_900));

        b.add_event(EventId(300));
        b.add_event(EventId(1)); // дубли схлопываются
        b.add_metric(MetricId(6));
        b.add_metric(MetricId(5)); // дубли схлопываются
        b.add_block(200, &header(2_000, 7), Micros(2_500));

        b.add_block(512, &header(9_000, 3), Micros(9_100));
        b
    }

    #[test]
    fn types_of_a_relocated_block_follow_it_to_the_new_segment() {
        // Блок, не поместившийся в сегмент, переезжает в свежий (SPEC §5).
        // Его типы обязаны переехать вместе с ним: по этим множествам
        // миграция решает, переписывать ли сегмент, а читатель — есть ли в
        // нём нужная телеметрия. Оставь их в старом footer'е — и оба ответа
        // окажутся неверными: старый сегмент объявит типы, которых в нём нет,
        // новый умолчит о тех, которые в нём есть, и миграция обойдёт
        // стороной ровно ту историю, ради которой написана.
        let mut b = FooterBuilder::new();

        // Первый блок лёг в текущий сегмент.
        b.add_event(EventId(1));
        b.add_block(32, &header(1_000, 5), Micros(1_100));

        // Второй собран, но в сегмент не влез: типы уже отмечены.
        b.add_event(EventId(7));
        b.add_metric(MetricId(3));

        // Сегмент запечатывается БЕЗ него.
        let sealed = Footer::parse(&b.build()).unwrap().expect("запечатан");
        assert_eq!(
            sealed.events,
            vec![EventId(1)],
            "в старом сегменте только типы блоков, которые в нём лежат"
        );
        assert!(sealed.metrics.is_empty());

        // Новый сегмент, тот же накопитель — и переехавший блок ложится сюда.
        b.reset();
        b.add_block(32, &header(2_000, 9), Micros(2_100));
        let fresh = Footer::parse(&b.build()).unwrap().expect("запечатан");
        assert_eq!(fresh.events, vec![EventId(7)], "типы уехали с блоком");
        assert_eq!(fresh.metrics, vec![MetricId(3)]);
        assert_eq!(fresh.blocks.len(), 1);
    }

    #[test]
    fn a_discarded_block_leaves_its_types_nowhere() {
        // Блок крупнее целого сегмента отбрасывается, и его типов нет ни в
        // одном сегменте — объявить их значило бы отправить миграцию
        // переписывать сегмент ради записей, которых там нет.
        let mut b = FooterBuilder::new();
        b.add_event(EventId(1));
        b.add_block(32, &header(1_000, 5), Micros(1_100));

        b.add_event(EventId(7));
        b.add_metric(MetricId(3));
        b.discard_pending();

        b.add_event(EventId(2));
        b.add_block(200, &header(2_000, 5), Micros(2_100));

        let footer = Footer::parse(&b.build()).unwrap().expect("запечатан");
        assert_eq!(footer.events, vec![EventId(1), EventId(2)]);
        assert!(
            footer.metrics.is_empty(),
            "метрика была только у отброшенного"
        );
    }

    #[test]
    fn footer_larger_than_the_prealloc_cap_still_parses() {
        // Предварительное выделение ограничено потолком, чтобы счётчик из
        // недоверенного файла не управлял размером аллокации. Потолок обязан
        // быть только подсказкой: настоящий сегмент на 256 МиБ с блоками по
        // 64 КиБ даёт четыре тысячи записей индекса, и разбор такого footer'а
        // должен пройти целиком, дорастив вектор сам.
        let n = PREALLOC_MAX * 2 + 7;
        let mut b = FooterBuilder::new();
        // Множества id тоже крупнее потолка: пространство u16 это позволяет.
        for id in 1..=(PREALLOC_MAX as u16 + 500) {
            b.add_event(EventId(id));
        }
        for i in 0..n {
            b.add_block(
                32 + i as u64 * 64,
                &header(i as u64 * 1_000, 4),
                Micros(i as u64 * 1_000 + 900),
            );
        }
        let bytes = b.build();
        let footer = Footer::parse(&bytes).unwrap().expect("сегмент запечатан");
        assert_eq!(footer.blocks.len(), n);
        assert_eq!(footer.blocks[n - 1].offset, 32 + (n as u64 - 1) * 64);
        assert_eq!(footer.events.len(), PREALLOC_MAX + 500);
        assert_eq!(footer.min, Micros(0));
        assert_eq!(footer.max, Micros((n as u64 - 1) * 1_000 + 900));
    }

    #[test]
    fn a_forged_count_cannot_drive_the_allocation() {
        // Ради чего потолок и заведён: `block_count` лежит в трейлере, а
        // трейлер защищён только CRC32C — не подписью, его пересчитывает кто
        // угодно. Число оттуда не имеет права управлять размером аллокации:
        // на armv7 это паника «capacity overflow», на 64-битном вьюере —
        // OOM-killer.
        //
        // Собираем правдоподобный footer, подменяем счётчик блоков на
        // заведомо невозможный и пересчитываем CRC — ровно то, что сделал бы
        // подсунутый дамп. Разбор обязан отказать по нехватке байт, а не
        // попытаться выделить память под объявленное.
        let bytes = sample_builder().build();
        let mut forged = bytes.clone();
        let trailer_at = forged.len() - Trailer::SIZE;
        forged[trailer_at + 4..trailer_at + 8].copy_from_slice(&u32::MAX.to_le_bytes());
        let crc = {
            let sections = &forged[..trailer_at];
            crc32c::crc32c_append(
                crc32c::crc32c(sections),
                &forged[trailer_at..trailer_at + 24],
            )
        };
        forged[trailer_at + 24..trailer_at + 28].copy_from_slice(&crc.to_le_bytes());

        // CRC сходится — значит разбор дошёл до чтения записей и остановился
        // на исчерпании секций, а не на проверке целостности.
        assert!(
            matches!(Footer::parse(&forged), Err(Error::Truncated)),
            "счётчик из файла обязан упереться в реальные байты"
        );
        assert!(
            bounded(u32::MAX, forged.len(), 3) <= PREALLOC_MAX,
            "и не имеет права заказать больше потолка"
        );
    }

    #[test]
    fn roundtrip() {
        let bytes = sample_builder().build();
        let footer = Footer::parse(&bytes).unwrap().expect("сегмент запечатан");

        assert_eq!(footer.blocks.len(), 3);
        assert_eq!(footer.blocks[0].offset, 32);
        assert_eq!(footer.blocks[1].offset, 200);
        assert_eq!(footer.blocks[2].offset, 512);
        assert_eq!(footer.blocks[2].base, Micros(9_000));
        assert_eq!(footer.blocks[1].count, 7);

        assert_eq!(footer.events, vec![EventId(1), EventId(300)]);
        assert_eq!(footer.metrics, vec![MetricId(5), MetricId(6)]);
        assert_eq!(footer.min, Micros(1_000));
        assert_eq!(footer.max, Micros(9_100));
    }

    #[test]
    fn metric_set_answers_what_telemetry_is_here() {
        // Ради этого вопроса и существовала таблица серий, пока идентичность
        // ряда была парой «метрика + рантайм-тэги». Тэгов нет, идентичность
        // равна метрике, и множества идентификаторов достаточно — притом оно
        // уже было в footer'е ради миграций.
        let bytes = sample_builder().build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert_eq!(f.metrics, vec![MetricId(5), MetricId(6)]);
        assert!(f.metrics.binary_search(&MetricId(5)).is_ok());
        assert!(
            f.metrics.binary_search(&MetricId(7)).is_err(),
            "чего нет в сегменте — того нет и во множестве"
        );
    }

    #[test]
    fn trailer_reports_size_for_two_phase_read() {
        // Читатель сначала берёт 32 байта, узнаёт длину, потом дочитывает.
        let bytes = sample_builder().build();
        let trailer = Trailer::parse(&bytes).unwrap().unwrap();
        assert_eq!(trailer.total_len(), bytes.len() as u64);
        assert_eq!(trailer.block_count, 3);
    }

    #[test]
    fn unsealed_segment_has_no_footer() {
        let data = vec![0xAB; 100];
        assert_eq!(Trailer::parse(&data).unwrap(), None);
        assert_eq!(Footer::parse(&data).unwrap(), None);
    }

    #[test]
    fn corrupt_footer_detected() {
        let mut bytes = sample_builder().build();
        bytes[0] ^= 0xFF;
        assert!(matches!(
            Footer::parse(&bytes),
            Err(Error::CrcMismatch { .. })
        ));
    }

    #[test]
    fn block_lookup_by_time() {
        let bytes = sample_builder().build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert_eq!(f.block_for_time(Micros(0)), None, "раньше первого блока");
        assert_eq!(
            f.block_for_time(Micros(1_000)),
            Some(0),
            "точное совпадение"
        );
        assert_eq!(f.block_for_time(Micros(1_500)), Some(0), "внутри первого");
        assert_eq!(f.block_for_time(Micros(2_000)), Some(1));
        assert_eq!(f.block_for_time(Micros(8_999)), Some(1));
        assert_eq!(f.block_for_time(Micros(9_000)), Some(2));
        assert_eq!(
            f.block_for_time(Micros(u64::MAX)),
            Some(2),
            "после последнего"
        );
    }

    #[test]
    fn lookup_returns_start_of_equal_base_group() {
        // Блоки с одинаковой базой — обычное дело: пачка записей одной
        // микросекунды. Бинарный поиск на дубликатах отдаёт произвольное
        // совпадение, и начать с середины группы значило бы потерять её
        // начало.
        let mut b = FooterBuilder::new();
        for (i, base) in [100u64, 500, 500, 500, 900].into_iter().enumerate() {
            b.add_block(32 + i as u64 * 64, &header(base, 1), Micros(base + 10));
        }
        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();

        assert_eq!(f.block_for_time(Micros(500)), Some(1), "начало группы");
        assert_eq!(f.block_for_time(Micros(700)), Some(1), "внутри группы");
        assert_eq!(f.block_for_time(Micros(900)), Some(4));
        assert_eq!(f.block_for_time(Micros(50)), None);
    }

    #[test]
    fn migration_can_skip_untouched_segments() {
        let bytes = sample_builder().build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert!(f.touches(&[EventId(300)], &[]), "тип есть в сегменте");
        assert!(f.touches(&[], &[MetricId(6)]));
        assert!(
            !f.touches(&[EventId(2), EventId(299)], &[MetricId(7)]),
            "затронутых типов нет — сегмент не переписываем"
        );
    }

    /// Собрать footer с произвольным трейлером и корректным CRC —
    /// имитация файла, подготовленного злонамеренно или испорченного.
    fn forge(sections: Vec<u8>, mut trailer: Trailer) -> Vec<u8> {
        trailer.sections_len = sections.len() as u32;
        let mut tb = Vec::new();
        trailer.write(&mut tb);
        trailer.crc = crc32c::crc32c_append(crc32c::crc32c(&sections), &tb[..24]);
        let mut bytes = sections;
        trailer.write(&mut bytes);
        bytes
    }

    #[test]
    fn absurd_counts_do_not_allocate() {
        // CRC32C — не подпись: кто угодно пересчитает его после правки
        // счётчиков. Разбор обязан устоять, а не выделять гигабайты по
        // числу из файла (на armv7 это паника «capacity overflow»,
        // на 64-битном вьюере — OOM-killer).
        let bytes = forge(
            Vec::new(),
            Trailer {
                sections_len: 0,
                block_count: u32::MAX,
                min: Micros(0),
                max: Micros(0),
                crc: 0,
            },
        );
        let err = Footer::parse(&bytes).unwrap_err();
        assert!(
            err.is_torn_tail(),
            "ожидалась ошибка разбора, получено {err}"
        );

        // То же для множеств идентификаторов.
        // Число блоков берётся из трейлера, поэтому секции начинаются сразу
        // с множества событий.
        let empty_trailer = Trailer {
            sections_len: 0,
            block_count: 0,
            min: Micros(0),
            max: Micros(0),
            crc: 0,
        };

        let mut sections = Vec::new();
        varint::write_u64(&mut sections, u64::from(u32::MAX)); // «событий» — 4 млрд
        assert!(Footer::parse(&forge(sections, empty_trailer)).is_err());

        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 0); // событий нет
        varint::write_u64(&mut sections, u64::from(u32::MAX)); // «метрик» — 4 млрд
        assert!(Footer::parse(&forge(sections, empty_trailer)).is_err());
    }

    #[test]
    fn trailing_garbage_in_footer_rejected() {
        // Лишние байты после разобранных секций означают, что footer описан
        // не тем, чем кажется: длина секций из трейлера согласована с CRC,
        // поэтому расхождение — признак подделки или порчи, а не запаса.
        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 0); // событий нет
        varint::write_u64(&mut sections, 0); // метрик нет
        sections.extend_from_slice(&[0xAA, 0xBB, 0xCC]);

        let bytes = forge(
            sections,
            Trailer {
                sections_len: 0,
                block_count: 0,
                min: Micros(0),
                max: Micros(0),
                crc: 0,
            },
        );
        assert!(
            Footer::parse(&bytes).is_err(),
            "хвост в секциях footer'а обязан отвергаться"
        );
    }

    #[test]
    fn index_stays_sorted_and_bounds_are_exact() {
        // Записи могут прийти к writer'у переупорядоченными между потоками:
        // индекс обязан остаться неубывающим (по нему идёт бинарный поиск),
        // а min/max — отражать фактические границы, иначе отбор сегментов
        // по диапазону молча выбросил бы сегмент с нужными записями.
        let mut b = FooterBuilder::new();
        b.add_block(32, &header(5_000, 1), Micros(5_100));
        b.add_block(100, &header(1_000, 1), Micros(1_100)); // «из прошлого»
        b.add_block(200, &header(9_000, 1), Micros(9_100));

        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();

        let bases: Vec<u64> = f.blocks.iter().map(|e| e.base.0).collect();
        assert!(
            bases.windows(2).all(|w| w[0] <= w[1]),
            "индекс обязан быть неубывающим: {bases:?}"
        );
        assert_eq!(f.min, Micros(1_000), "min — фактический минимум");
        assert_eq!(f.max, Micros(9_100));
        // Смещения не искажены подтягиванием времени.
        assert_eq!(
            f.blocks.iter().map(|e| e.offset).collect::<Vec<_>>(),
            vec![32, 100, 200]
        );
    }

    #[test]
    fn empty_footer_roundtrips() {
        let b = FooterBuilder::new();
        assert!(b.is_empty());
        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert!(f.blocks.is_empty());
        assert!(f.events.is_empty());
        assert!(f.metrics.is_empty());
    }

    #[test]
    fn duplicate_ids_in_set_rejected() {
        // Число блоков берётся из трейлера (0), поэтому секции начинаются
        // сразу с множества событий. Собираем его вручную с нулевой дельтой
        // после первого элемента — это дубль, и он обязан быть отвергнут.
        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 2); // 2 события
        varint::write_u64(&mut sections, 5); // первое — абсолютное значение
        varint::write_u64(&mut sections, 0); // дубль
        varint::write_u64(&mut sections, 0); // метрик нет

        let mut trailer = Trailer {
            sections_len: sections.len() as u32,
            block_count: 0,
            min: Micros(0),
            max: Micros(0),
            crc: 0,
        };
        let mut tb = Vec::new();
        trailer.write(&mut tb);
        trailer.crc = crc32c::crc32c_append(crc32c::crc32c(&sections), &tb[..24]);
        let mut bytes = sections;
        trailer.write(&mut bytes);

        assert_eq!(Footer::parse(&bytes), Err(Error::ReservedValue));
    }
}
