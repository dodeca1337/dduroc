//! Блок — единица записи, flush'а и проверки целостности.
//!
//! ```text
//! [BlockHeader 24B] [тело: записи подряд, опционально сжатое целиком]
//! ```
//!
//! Блок соответствует одному батчу writer'а. CRC и сжатие амортизируются на
//! блок, а не на запись, поэтому запись стоит единицы байт.
//!
//! `body_len == 0` — признак конца данных сегмента: файл преаллоцирован
//! (`fallocate`) и хвост заполнен нулями, так что «нулевой заголовок»
//! естественно терминирует обход, отличая непрописанный хвост от порчи.

use crate::error::{Error, Result};
use crate::ids::Micros;
use crate::record::{self, Record};
use std::borrow::Cow;

/// Алгоритм сжатия тела блока (младшие 2 бита `flags`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Compression {
    #[default]
    None = 0,
    Lz4 = 1,
    /// Распознаётся, но кодек не встроен: zstd тянет C-зависимость, которая
    /// осложняет кросс-сборку под armv7. Чтение такого блока — внятная
    /// ошибка, а не мусор.
    Zstd = 2,
}

impl Compression {
    const MASK: u8 = 0b0000_0011;

    pub const fn from_bits(bits: u8) -> Result<Self> {
        match bits & Self::MASK {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Lz4),
            2 => Ok(Compression::Zstd),
            _ => Err(Error::UnknownCompression(bits & Self::MASK)),
        }
    }
}

/// Заголовок блока.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    /// Длина тела на диске (после сжатия). `0` — конец данных сегмента.
    pub body_len: u32,
    /// Длина тела до сжатия. Равна `body_len` при [`Compression::None`].
    pub raw_len: u32,
    /// Время первой записи блока.
    pub base: Micros,
    /// Число записей.
    pub count: u16,
    pub compression: Compression,
    /// CRC32C заголовка (первые 20 байт) и тела **как оно лежит на диске**.
    pub crc: u32,
}

impl BlockHeader {
    pub const SIZE: usize = 24;

    /// Максимальный размер тела блока. Ограничение поля `body_len` (u32),
    /// на практике движок держит блоки в десятках килобайт.
    pub const MAX_BODY: u32 = u32::MAX;

    /// Сериализовать заголовок.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..4].copy_from_slice(&self.body_len.to_le_bytes());
        b[4..8].copy_from_slice(&self.raw_len.to_le_bytes());
        b[8..16].copy_from_slice(&self.base.0.to_le_bytes());
        b[16..18].copy_from_slice(&self.count.to_le_bytes());
        b[18] = self.compression as u8;
        b[19] = 0; // резерв
        b[20..24].copy_from_slice(&self.crc.to_le_bytes());
        b
    }

    /// Разобрать заголовок **без** проверки CRC (тело ещё не прочитано).
    ///
    /// Возвращает `Ok(None)`, если заголовок нулевой — это непрописанный
    /// хвост преаллоцированного файла, то есть штатный конец данных.
    pub fn parse(input: &[u8]) -> Result<Option<Self>> {
        let raw: &[u8; Self::SIZE] = input
            .get(..Self::SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::Truncated)?;

        let body_len = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
        if body_len == 0 {
            return Ok(None);
        }

        if raw[19] != 0 {
            return Err(Error::ReservedNotZero);
        }
        let compression = Compression::from_bits(raw[18])?;
        if raw[18] & !Compression::MASK != 0 {
            return Err(Error::ReservedNotZero);
        }

        Ok(Some(Self {
            body_len,
            raw_len: u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]),
            base: Micros(u64::from_le_bytes(
                raw[8..16].try_into().expect("срез 8 байт"),
            )),
            count: u16::from_le_bytes([raw[16], raw[17]]),
            compression,
            crc: u32::from_le_bytes([raw[20], raw[21], raw[22], raw[23]]),
        }))
    }

    /// Проверить CRC заголовка вместе с телом (тело — как на диске).
    pub fn verify(&self, body_on_disk: &[u8]) -> Result<()> {
        let actual = compute_crc(self, body_on_disk);
        if actual != self.crc {
            return Err(Error::CrcMismatch {
                expected: self.crc,
                actual,
            });
        }
        Ok(())
    }

    /// Полный размер блока на диске.
    pub fn total_len(&self) -> u64 {
        Self::SIZE as u64 + u64::from(self.body_len)
    }

    /// Распаковать тело. Без сжатия — заимствование, копирования нет.
    pub fn decompress<'a>(&self, body_on_disk: &'a [u8]) -> Result<Cow<'a, [u8]>> {
        match self.compression {
            Compression::None => {
                if self.raw_len != self.body_len {
                    return Err(Error::Decompress("raw_len != body_len без сжатия"));
                }
                Ok(Cow::Borrowed(body_on_disk))
            }
            Compression::Lz4 => {
                let raw_len = self.raw_len as usize;
                let out = lz4_flex::block::decompress(body_on_disk, raw_len)
                    .map_err(|_| Error::Decompress("lz4: тело повреждено"))?;
                if out.len() != raw_len {
                    return Err(Error::Decompress("lz4: длина не совпала с raw_len"));
                }
                Ok(Cow::Owned(out))
            }
            Compression::Zstd => Err(Error::Decompress("zstd не встроен в этот билд")),
        }
    }
}

fn compute_crc(header: &BlockHeader, body_on_disk: &[u8]) -> u32 {
    let bytes = header.to_bytes();
    let crc = crc32c::crc32c(&bytes[..20]);
    crc32c::crc32c_append(crc, body_on_disk)
}

// ════════════════════════════════════════════════════════════════════════════
// Сборка блока
// ════════════════════════════════════════════════════════════════════════════

/// Накопитель записей одного блока.
///
/// Держит тело в переиспользуемом буфере: аллокации на горячем пути записи
/// нет — буфер растёт до рабочего размера и дальше только очищается.
#[derive(Debug, Default)]
pub struct BlockBuilder {
    base: Option<Micros>,
    last: Micros,
    count: u16,
    body: Vec<u8>,
}

impl BlockBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// С заранее выделенной ёмкостью тела.
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            body: Vec::with_capacity(bytes),
            ..Self::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn count(&self) -> u16 {
        self.count
    }

    /// Текущий размер тела (до сжатия).
    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    /// Время первой записи блока.
    pub fn base(&self) -> Option<Micros> {
        self.base
    }

    /// Время последней записи блока.
    pub fn last(&self) -> Option<Micros> {
        self.base.map(|_| self.last)
    }

    /// Добавить запись со временем `at`.
    ///
    /// Время, ушедшее назад (перевод часов не влияет — источник монотонный,
    /// но данные могут прийти из чужого потока), фиксируется как нулевая
    /// дельта: терять запись хуже, чем потерять микросекунды разрешения.
    pub fn push(&mut self, at: Micros, rec: &Record<'_>) -> Result<usize> {
        if self.count == u16::MAX {
            return Err(Error::LimitExceeded {
                what: "records per block",
                value: u64::from(self.count) + 1,
                max: u64::from(u16::MAX),
            });
        }

        let dt = match self.base {
            None => {
                self.base = Some(at);
                0
            }
            Some(_) => at.saturating_delta(self.last),
        };
        self.last = Micros(self.last.0.max(at.0));

        let n = record::encode(rec, dt, &mut self.body)?;
        self.count += 1;
        Ok(n)
    }

    /// Завершить блок: дописать `[заголовок][тело]` в `out` и очистить
    /// накопитель. Возвращает заголовок записанного блока.
    ///
    /// Сжатие применяется, только если реально уменьшает тело: на коротких
    /// блоках LZ4 нередко даёт прирост, и хранить раздутое тело незачем.
    pub fn finish(&mut self, compression: Compression, out: &mut Vec<u8>) -> Result<BlockHeader> {
        let base = self.base.ok_or(Error::EmptyBlock)?;
        let raw_len = u32::try_from(self.body.len()).map_err(|_| Error::LimitExceeded {
            what: "block body",
            value: self.body.len() as u64,
            max: u64::from(BlockHeader::MAX_BODY),
        })?;

        let compressed = match compression {
            Compression::None | Compression::Zstd => None,
            Compression::Lz4 => {
                let c = lz4_flex::block::compress(&self.body);
                (c.len() < self.body.len()).then_some(c)
            }
        };

        let (used, body_on_disk): (Compression, &[u8]) = match &compressed {
            Some(c) => (Compression::Lz4, c),
            None => (Compression::None, &self.body),
        };

        let mut header = BlockHeader {
            body_len: body_on_disk.len() as u32,
            raw_len,
            base,
            count: self.count,
            compression: used,
            crc: 0,
        };
        header.crc = compute_crc(&header, body_on_disk);

        out.extend_from_slice(&header.to_bytes());
        out.extend_from_slice(body_on_disk);

        self.reset();
        Ok(header)
    }

    /// Сбросить накопленное, сохранив ёмкость буфера.
    pub fn reset(&mut self) {
        self.base = None;
        self.last = Micros(0);
        self.count = 0;
        self.body.clear();
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Чтение блока
// ════════════════════════════════════════════════════════════════════════════

/// Прочитанный и проверенный блок.
#[derive(Debug)]
pub struct Block<'a> {
    pub header: BlockHeader,
    body: Cow<'a, [u8]>,
}

impl<'a> Block<'a> {
    /// Разобрать блок из начала `input` (заголовок + тело), проверив CRC.
    ///
    /// `Ok(None)` — нулевой заголовок, то есть конец данных сегмента.
    pub fn parse(input: &'a [u8]) -> Result<Option<Self>> {
        let Some(header) = BlockHeader::parse(input)? else {
            return Ok(None);
        };
        let body_end = BlockHeader::SIZE + header.body_len as usize;
        let body = input
            .get(BlockHeader::SIZE..body_end)
            .ok_or(Error::Truncated)?;
        header.verify(body)?;
        Ok(Some(Self {
            header,
            body: header.decompress(body)?,
        }))
    }

    /// Тело в распакованном виде.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Итератор записей с абсолютным временем каждой.
    pub fn records(&self) -> BlockRecords<'_> {
        BlockRecords {
            inner: record::iter(&self.body),
            at: self.header.base,
            first: true,
        }
    }
}

/// Записи блока с восстановленным абсолютным временем.
#[derive(Debug)]
pub struct BlockRecords<'a> {
    inner: record::RecordIter<'a>,
    at: Micros,
    first: bool,
}

impl<'a> Iterator for BlockRecords<'a> {
    type Item = Result<(Micros, Record<'a>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let framed = match self.inner.next()? {
            Ok(f) => f,
            Err(e) => return Some(Err(e)),
        };
        // Первая запись задаёт базу; у неё дельта нулевая по построению.
        if !self.first {
            match self.at.checked_add_delta(framed.dt) {
                Some(t) => self.at = t,
                None => return Some(Err(Error::Truncated)),
            }
        }
        self.first = false;
        Some(Ok((self.at, framed.record)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{EventId, SeriesLocal};
    use crate::record::{Message, Sample};
    use crate::value::Value;

    fn msg(event: u16, payload: &[u8]) -> Record<'_> {
        Record::Message(Message {
            event: EventId(event),
            span: None,
            payload,
        })
    }

    #[test]
    fn roundtrip_uncompressed() {
        let mut b = BlockBuilder::new();
        b.push(Micros(1_000), &msg(1, &[1, 2, 3])).unwrap();
        b.push(Micros(1_500), &msg(2, &[4])).unwrap();
        b.push(Micros(9_000), &msg(3, &[])).unwrap();

        let mut out = Vec::new();
        let header = b.finish(Compression::None, &mut out).unwrap();
        assert_eq!(header.count, 3);
        assert_eq!(header.base, Micros(1_000));
        assert_eq!(out.len() as u64, header.total_len());
        assert!(b.is_empty(), "после finish накопитель пуст");

        let block = Block::parse(&out).unwrap().expect("блок есть");
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 3);
        // Абсолютное время восстановлено из дельт.
        assert_eq!(recs[0].0, Micros(1_000));
        assert_eq!(recs[1].0, Micros(1_500));
        assert_eq!(recs[2].0, Micros(9_000));
        assert_eq!(recs[0].1, msg(1, &[1, 2, 3]));
        assert_eq!(recs[2].1, msg(3, &[]));
    }

    #[test]
    fn lz4_roundtrip_and_only_when_smaller() {
        // Хорошо сжимаемое тело: сотня одинаковых сообщений.
        let mut b = BlockBuilder::new();
        for i in 0..100 {
            b.push(Micros(i * 10), &msg(7, &[0xAA; 16])).unwrap();
        }
        let mut out = Vec::new();
        let header = b.finish(Compression::Lz4, &mut out).unwrap();
        assert_eq!(header.compression, Compression::Lz4);
        assert!(
            header.body_len < header.raw_len,
            "сжатие обязано уменьшить тело: {} → {}",
            header.raw_len,
            header.body_len
        );

        let block = Block::parse(&out).unwrap().unwrap();
        assert_eq!(block.records().count(), 100);
        assert_eq!(block.body().len(), header.raw_len as usize);

        // Несжимаемое тело: LZ4 не должен раздувать блок.
        let mut b = BlockBuilder::new();
        let noise: Vec<u8> = (0..64u16)
            .map(|i| (i.wrapping_mul(7919) >> 3) as u8)
            .collect();
        b.push(Micros(0), &msg(1, &noise)).unwrap();
        let mut out = Vec::new();
        let header = b.finish(Compression::Lz4, &mut out).unwrap();
        if header.compression == Compression::None {
            assert_eq!(header.body_len, header.raw_len);
        } else {
            assert!(header.body_len < header.raw_len);
        }
    }

    #[test]
    fn zero_header_terminates() {
        let zeros = [0u8; BlockHeader::SIZE * 2];
        assert!(Block::parse(&zeros).unwrap().is_none());
        assert!(BlockHeader::parse(&zeros).unwrap().is_none());
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &[1, 2, 3, 4])).unwrap();
        let mut out = Vec::new();
        b.finish(Compression::None, &mut out).unwrap();

        // Порча байта тела.
        let last = out.len() - 1;
        out[last] ^= 0xFF;
        let err = Block::parse(&out).unwrap_err();
        assert!(matches!(err, Error::CrcMismatch { .. }), "получено {err}");
        assert!(err.is_torn_tail(), "битый хвост распознаётся recovery");
    }

    #[test]
    fn truncated_body_detected() {
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &[0xEE; 32])).unwrap();
        let mut out = Vec::new();
        b.finish(Compression::None, &mut out).unwrap();

        out.truncate(out.len() - 5); // обрыв питания посреди записи блока
        assert_eq!(Block::parse(&out).unwrap_err(), Error::Truncated);
    }

    #[test]
    fn mixed_records_and_time_reconstruction() {
        let mut b = BlockBuilder::new();
        b.push(
            Micros(500),
            &Record::SeriesDef(crate::record::SeriesDef {
                series: SeriesLocal(0),
                metric: crate::ids::MetricId(1),
                value_type: crate::value::ValueType::F32,
                tags: crate::record::Tags::Slice(&[("sensor", "pa")]),
            }),
        )
        .unwrap();
        for i in 0..10u64 {
            b.push(
                Micros(1_000 + i * 250),
                &Record::Sample(Sample {
                    series: SeriesLocal(0),
                    value: Value::F32(20.0 + i as f32),
                }),
            )
            .unwrap();
        }
        let mut out = Vec::new();
        b.finish(Compression::None, &mut out).unwrap();

        let block = Block::parse(&out).unwrap().unwrap();
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 11);
        assert_eq!(recs[0].0, Micros(500));
        assert_eq!(recs[1].0, Micros(1_000));
        assert_eq!(recs[10].0, Micros(1_000 + 9 * 250));
    }

    #[test]
    fn non_monotonic_time_does_not_lose_records() {
        let mut b = BlockBuilder::new();
        b.push(Micros(1_000), &msg(1, &[])).unwrap();
        b.push(Micros(900), &msg(2, &[])).unwrap(); // время «ушло назад»
        b.push(Micros(1_100), &msg(3, &[])).unwrap();
        let mut out = Vec::new();
        b.finish(Compression::None, &mut out).unwrap();

        let block = Block::parse(&out).unwrap().unwrap();
        let recs: Vec<_> = block.records().map(|r| r.unwrap()).collect();
        assert_eq!(recs.len(), 3, "ни одна запись не потеряна");
        assert_eq!(recs[1].0, Micros(1_000), "откат схлопнут в нулевую дельту");
        assert_eq!(recs[2].0, Micros(1_100));
    }

    #[test]
    fn empty_block_cannot_be_finished() {
        let mut b = BlockBuilder::new();
        let mut out = Vec::new();
        assert_eq!(
            b.finish(Compression::None, &mut out),
            Err(Error::EmptyBlock)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn header_roundtrip_bytes() {
        let h = BlockHeader {
            body_len: 1234,
            raw_len: 5678,
            base: Micros(0x0102_0304_0506_0708),
            count: 99,
            compression: Compression::Lz4,
            crc: 0xDEAD_BEEF,
        };
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), BlockHeader::SIZE);
        let parsed = BlockHeader::parse(&bytes).unwrap().unwrap();
        assert_eq!(parsed, h);
    }

    #[test]
    fn reserved_byte_must_be_zero() {
        let h = BlockHeader {
            body_len: 8,
            raw_len: 8,
            base: Micros(0),
            count: 1,
            compression: Compression::None,
            crc: 0,
        };
        let mut bytes = h.to_bytes();
        bytes[19] = 1;
        assert_eq!(BlockHeader::parse(&bytes), Err(Error::ReservedNotZero));

        let mut bytes = h.to_bytes();
        bytes[18] = 0b1000_0000; // старшие биты флагов зарезервированы
        assert_eq!(BlockHeader::parse(&bytes), Err(Error::ReservedNotZero));
    }
}
