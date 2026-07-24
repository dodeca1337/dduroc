//! Footer запечатанного сегмента: индекс блоков, множества встреченных типов
//! и таблица серий.
//!
//! ```text
//! [индекс блоков] [event_id'ы] [metric_id'ы] [таблица серий] [Trailer 32B]
//! ```
//!
//! Footer — **оптимизация, а не необходимость**: он позволяет найти блок по
//! времени без чтения тел и понять, какие серии есть в сегменте, не сканируя
//! его. Если footer повреждён или отсутствует (сегмент активен либо оборвано
//! питание при seal'е), читатель деградирует к последовательному обходу
//! заголовков блоков — данные не теряются.
//!
//! Множества типов нужны миграции: сегмент, не содержащий затронутых
//! `event_id`/`metric_id`, переписывать не нужно — экономия ресурса флеша.
//!
//! Признак запечатанности — сигнатура в последних четырёх байтах файла.

use crate::block::BlockHeader;
use crate::cursor::{Cursor, write_str};
use crate::error::{Error, Result};
use crate::ids::{EventId, MetricId, Micros, SeriesLocal};
use crate::record::Tags;
use crate::segment::SegmentHeader;
use crate::value::ValueType;
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

/// Серия сегмента (позиция в таблице = [`SeriesLocal`]).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SeriesInfo<'a> {
    pub metric: MetricId,
    pub value_type: ValueType,
    pub tags: Tags<'a>,
}

/// Хвостовой блок footer'а фиксированного размера.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trailer {
    /// Длина секций footer'а до трейлера.
    pub footer_len: u32,
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
            footer_len: u32::from_le_bytes(raw[0..4].try_into().expect("срез 4 байта")),
            block_count: u32::from_le_bytes(raw[4..8].try_into().expect("срез 4 байта")),
            min: Micros(u64::from_le_bytes(raw[8..16].try_into().expect("срез 8"))),
            max: Micros(u64::from_le_bytes(raw[16..24].try_into().expect("срез 8"))),
            crc: u32::from_le_bytes(raw[24..28].try_into().expect("срез 4 байта")),
        }))
    }

    /// Полный размер footer'а на диске (секции + трейлер).
    pub fn total_len(&self) -> u64 {
        u64::from(self.footer_len) + Self::SIZE as u64
    }

    fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.footer_len.to_le_bytes());
        out.extend_from_slice(&self.block_count.to_le_bytes());
        out.extend_from_slice(&self.min.0.to_le_bytes());
        out.extend_from_slice(&self.max.0.to_le_bytes());
        out.extend_from_slice(&self.crc.to_le_bytes());
        out.extend_from_slice(&FOOTER_MAGIC);
    }
}

/// Разобранный footer.
#[derive(Debug, Clone, PartialEq)]
pub struct Footer<'a> {
    pub blocks: Vec<BlockIndexEntry>,
    /// Типы сообщений, встречающиеся в сегменте (возрастающий порядок).
    pub events: Vec<EventId>,
    /// Метрики, встречающиеся в сегменте (возрастающий порядок).
    pub metrics: Vec<MetricId>,
    /// Таблица серий: индекс = [`SeriesLocal`].
    pub series: Vec<SeriesInfo<'a>>,
    pub min: Micros,
    pub max: Micros,
}

impl<'a> Footer<'a> {
    /// Разобрать footer из хвоста файла. `bytes` обязан **заканчиваться**
    /// последним байтом файла и содержать footer целиком
    /// (длина известна из [`Trailer::parse`]).
    pub fn parse(bytes: &'a [u8]) -> Result<Option<Self>> {
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
        let mut blocks = Vec::with_capacity(trailer.block_count as usize);
        let mut offset = SegmentHeader::SIZE as u64;
        let mut base = 0u64;
        for _ in 0..trailer.block_count {
            offset = offset.checked_add(c.varint()?).ok_or(Error::Truncated)?;
            base = base.checked_add(c.varint()?).ok_or(Error::Truncated)?;
            let count = c.varint_u16("block count")?;
            blocks.push(BlockIndexEntry {
                offset,
                base: Micros(base),
                count,
            });
        }

        let events = read_id_set(&mut c, "event_id")?
            .into_iter()
            .map(|v| EventId(v as u16))
            .collect();
        let metrics = read_id_set(&mut c, "metric_id")?
            .into_iter()
            .map(|v| MetricId(v as u16))
            .collect();

        // Таблица серий.
        let n = c.varint_u32("series count")?;
        let mut series = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let metric = MetricId(c.varint_u16("metric_id")?);
            let value_type = ValueType::from_u8(c.u8()?)?;
            let count = c.varint_u32("n_tags")?;
            let tags_start = c.pos();
            for _ in 0..count {
                let _ = c.str("tag_key")?;
                let _ = c.str("tag_value")?;
            }
            series.push(SeriesInfo {
                metric,
                value_type,
                tags: Tags::Raw {
                    count,
                    bytes: &sections[tags_start..c.pos()],
                },
            });
        }

        Ok(Some(Self {
            blocks,
            events,
            metrics,
            series,
            min: trailer.min,
            max: trailer.max,
        }))
    }

    /// Индекс блока, который может содержать записи со временем `at`:
    /// последний блок с `base <= at`. `None` — `at` раньше первого блока.
    pub fn block_for_time(&self, at: Micros) -> Option<usize> {
        match self.blocks.binary_search_by(|b| b.base.cmp(&at)) {
            Ok(i) => Some(i),
            Err(0) => None,
            Err(i) => Some(i - 1),
        }
    }

    /// Описание серии по её локальному идентификатору.
    pub fn series(&self, id: SeriesLocal) -> Option<&SeriesInfo<'a>> {
        self.series.get(id.0 as usize)
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

fn read_id_set(c: &mut Cursor<'_>, what: &'static str) -> Result<Vec<u64>> {
    let n = c.varint_u32(what)?;
    let mut out = Vec::with_capacity(n as usize);
    let mut prev = 0u64;
    for i in 0..n {
        let delta = c.varint()?;
        // Первый элемент — абсолютное значение, дальше строго возрастающие
        // дельты: нулевая дельта означала бы дубль в множестве.
        if i > 0 && delta == 0 {
            return Err(Error::ReservedNotZero);
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
#[derive(Debug, Default)]
pub struct FooterBuilder {
    blocks: Vec<BlockIndexEntry>,
    events: std::collections::BTreeSet<u16>,
    metrics: std::collections::BTreeSet<u16>,
    series: Vec<OwnedSeries>,
    min: Option<Micros>,
    max: Micros,
}

#[derive(Debug, Clone, PartialEq)]
struct OwnedSeries {
    metric: MetricId,
    value_type: ValueType,
    tags: Vec<(String, String)>,
}

impl FooterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Зарегистрировать записанный блок.
    pub fn add_block(&mut self, offset: u64, header: &BlockHeader, last: Micros) {
        self.blocks.push(BlockIndexEntry {
            offset,
            base: header.base,
            count: header.count,
        });
        self.min.get_or_insert(header.base);
        self.max = Micros(self.max.0.max(last.0));
    }

    pub fn add_event(&mut self, id: EventId) {
        self.events.insert(id.0);
    }

    pub fn add_metric(&mut self, id: MetricId) {
        self.metrics.insert(id.0);
    }

    /// Зарегистрировать серию. Порядок вызовов задаёт [`SeriesLocal`],
    /// поэтому он обязан совпадать с порядком `SeriesDef` в блоках.
    pub fn add_series(
        &mut self,
        metric: MetricId,
        value_type: ValueType,
        tags: &[(&str, &str)],
    ) -> SeriesLocal {
        self.metrics.insert(metric.0);
        let id = SeriesLocal(self.series.len() as u32);
        self.series.push(OwnedSeries {
            metric,
            value_type,
            tags: tags
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
        });
        id
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

        varint::write_u64(&mut sections, self.series.len() as u64);
        for s in &self.series {
            varint::write_u64(&mut sections, u64::from(s.metric.0));
            sections.push(s.value_type as u8);
            varint::write_u64(&mut sections, s.tags.len() as u64);
            for (k, v) in &s.tags {
                write_str(&mut sections, k);
                write_str(&mut sections, v);
            }
        }

        let mut trailer = Trailer {
            footer_len: sections.len() as u32,
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

    pub fn reset(&mut self) {
        self.blocks.clear();
        self.events.clear();
        self.metrics.clear();
        self.series.clear();
        self.min = None;
        self.max = Micros(0);
    }
}

fn write_id_set(out: &mut Vec<u8>, set: &std::collections::BTreeSet<u16>) {
    varint::write_u64(out, set.len() as u64);
    let mut prev = 0u64;
    for &id in set {
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
            base: Micros(base),
            count,
            compression: Compression::None,
            crc: 0,
        }
    }

    fn sample_builder() -> FooterBuilder {
        let mut b = FooterBuilder::new();
        b.add_block(32, &header(1_000, 5), Micros(1_900));
        b.add_block(200, &header(2_000, 7), Micros(2_500));
        b.add_block(512, &header(9_000, 3), Micros(9_100));
        b.add_event(EventId(1));
        b.add_event(EventId(300));
        b.add_event(EventId(1)); // дубли схлопываются
        b.add_series(MetricId(5), ValueType::F32, &[("sensor", "pa")]);
        b.add_series(MetricId(6), ValueType::Blob, &[]);
        b
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

        assert_eq!(footer.series.len(), 2);
        let s0 = footer.series(SeriesLocal(0)).unwrap();
        assert_eq!(s0.metric, MetricId(5));
        assert_eq!(s0.value_type, ValueType::F32);
        assert_eq!(s0.tags, Tags::Slice(&[("sensor", "pa")]));
        assert_eq!(footer.series(SeriesLocal(1)).unwrap().tags.len(), 0);
        assert!(footer.series(SeriesLocal(2)).is_none());
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

    #[test]
    fn empty_footer_roundtrips() {
        let b = FooterBuilder::new();
        assert!(b.is_empty());
        let bytes = b.build();
        let f = Footer::parse(&bytes).unwrap().unwrap();
        assert!(f.blocks.is_empty());
        assert!(f.events.is_empty());
        assert!(f.series.is_empty());
    }

    #[test]
    fn duplicate_ids_in_set_rejected() {
        // Собираем множество вручную с нулевой дельтой — дубль.
        let mut sections = Vec::new();
        varint::write_u64(&mut sections, 0); // блоков нет
        varint::write_u64(&mut sections, 2); // 2 события
        varint::write_u64(&mut sections, 5);
        varint::write_u64(&mut sections, 0); // дубль
        varint::write_u64(&mut sections, 0); // метрик нет
        varint::write_u64(&mut sections, 0); // серий нет

        let mut trailer = Trailer {
            footer_len: sections.len() as u32,
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

        assert_eq!(Footer::parse(&bytes), Err(Error::ReservedNotZero));
    }
}
