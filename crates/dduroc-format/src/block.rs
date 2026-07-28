//! Блок — единица записи, flush'а и проверки целостности.
//!
//! ```text
//! [BlockHeader 32B] [тело: записи подряд, опционально сжатое целиком]
//! ```
//!
//! Блок соответствует одному батчу writer'а. CRC и сжатие амортизируются на
//! блок, а не на запись, поэтому запись стоит единицы байт.
//!
//! # Три различимых состояния хвоста
//!
//! Сегмент преаллоцирован, поэтому непрописанный хвост заполнен нулями.
//! Заголовок устроен так, чтобы штатный конец данных, потерянный кусок и
//! порча были **разными** диагнозами, а не одним:
//!
//! | состояние | признак |
//! |---|---|
//! | конец данных | все 32 байта заголовка нулевые |
//! | порча | не сошёлся magic, CRC или зарезервированные биты |
//! | дыра | `seq` не на единицу больше предыдущего |
//!
//! Наивный признак «`body_len == 0` ⇒ конец» опасен: одиночный перевёрнутый
//! бит в поле длины неотличим от конца лога, и уже подтверждённые
//! `fdatasync`'ом блоки молча исчезли бы без единого сообщения об ошибке.

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

/// Сигнатура заголовка блока.
pub const BLOCK_MAGIC: [u8; 2] = *b"DB";

/// Во сколько раз распакованное тело может превосходить сжатое.
///
/// Для LZ4 это 255: минимальная последовательность, дающая максимум выхода, —
/// токен, двухбайтовое смещение и цепочка байт длины по 255 каждый.
const MAX_EXPANSION: u32 = 255;

/// Заголовок блока.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHeader {
    /// Длина тела на диске (после сжатия).
    pub body_len: u32,
    /// Длина тела до сжатия. Равна `body_len` при [`Compression::None`].
    pub raw_len: u32,
    /// Порядковый номер блока в сегменте, с нуля. Разрыв нумерации означает
    /// потерянный блок — состояние, которое иначе неотличимо от порчи.
    pub seq: u32,
    /// Время первой записи блока.
    pub base: Micros,
    /// Число записей.
    pub count: u16,
    pub compression: Compression,
    /// CRC32C заголовка (первые 28 байт) и тела **как оно лежит на диске**.
    pub crc: u32,
}

impl BlockHeader {
    pub const SIZE: usize = 32;

    /// Потолок размера тела блока: 64 МиБ.
    ///
    /// Поле вмещает u32, но принимать такие значения с диска нельзя — длина
    /// управляет размером аллокации при чтении, а файл может быть повреждён
    /// или получен с чужого устройства. Реальные блоки — десятки килобайт.
    pub const MAX_BODY: u32 = 64 * 1024 * 1024;

    /// Сериализовать заголовок.
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..2].copy_from_slice(&BLOCK_MAGIC);
        b[2] = self.compression as u8;
        b[3] = 0; // резерв
        b[4..8].copy_from_slice(&self.body_len.to_le_bytes());
        b[8..12].copy_from_slice(&self.raw_len.to_le_bytes());
        b[12..16].copy_from_slice(&self.seq.to_le_bytes());
        b[16..24].copy_from_slice(&self.base.0.to_le_bytes());
        b[24..26].copy_from_slice(&self.count.to_le_bytes());
        b[26] = 0; // резерв
        b[27] = 0; // резерв
        b[28..32].copy_from_slice(&self.crc.to_le_bytes());
        b
    }

    /// Разобрать заголовок **без** проверки CRC (тело ещё не прочитано).
    ///
    /// `Ok(None)` — заголовок целиком нулевой, то есть непрописанный хвост
    /// преаллоцированного файла: штатный конец данных. Любое отклонение от
    /// «все нули» разбирается как настоящий заголовок и обязано пройти
    /// проверки — иначе битый бит в длине выглядел бы как конец лога.
    pub fn parse(input: &[u8]) -> Result<Option<Self>> {
        let raw: &[u8; Self::SIZE] = input
            .get(..Self::SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::Truncated)?;

        if raw.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        let magic: [u8; 2] = [raw[0], raw[1]];
        if magic != BLOCK_MAGIC {
            return Err(Error::BadMagic {
                expected: [BLOCK_MAGIC[0], BLOCK_MAGIC[1], 0, 0],
                actual: [magic[0], magic[1], 0, 0],
            });
        }
        if raw[3] != 0 || raw[26] != 0 || raw[27] != 0 {
            return Err(Error::ReservedNotZero);
        }
        let compression = Compression::from_bits(raw[2])?;
        if raw[2] & !Compression::MASK != 0 {
            return Err(Error::ReservedNotZero);
        }

        let body_len = u32::from_le_bytes([raw[4], raw[5], raw[6], raw[7]]);
        let raw_len = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]);
        // Длины проверяются до того, как по ним что-то выделяется.
        if body_len == 0 || body_len > Self::MAX_BODY || raw_len > Self::MAX_BODY {
            return Err(Error::LimitExceeded {
                what: "block body",
                value: u64::from(body_len.max(raw_len)),
                max: u64::from(Self::MAX_BODY),
            });
        }
        // Потолка мало: буфер распаковки выделяется по `raw_len`, а тело в
        // тридцать байт не может развернуться в шестьдесят четыре мегабайта.
        // Сверяем одну длину с другой — так размер аллокации ограничен тем,
        // что реально лежит в файле, а не только константой формата.
        let bound = match compression {
            Compression::None => body_len,
            // Предел раздувания LZ4 — 255:1: последовательность «токен +
            // смещение + байты длины» не даёт большего на байт входа.
            Compression::Lz4 | Compression::Zstd => body_len.saturating_mul(MAX_EXPANSION),
        };
        if raw_len > bound {
            return Err(Error::LimitExceeded {
                what: "block raw_len",
                value: u64::from(raw_len),
                max: u64::from(bound),
            });
        }

        Ok(Some(Self {
            body_len,
            raw_len,
            seq: u32::from_le_bytes([raw[12], raw[13], raw[14], raw[15]]),
            base: Micros(u64::from_le_bytes(
                raw[16..24].try_into().expect("срез 8 байт"),
            )),
            count: u16::from_le_bytes([raw[24], raw[25]]),
            compression,
            crc: u32::from_le_bytes([raw[28], raw[29], raw[30], raw[31]]),
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

/// Переставить номер блока в уже собранных байтах, пересчитав CRC.
///
/// Нужно, когда готовый блок не поместился в текущий сегмент и уезжает в
/// новый: нумерация блоков начинается в каждом сегменте заново, а пересобирать
/// тело ради одного поля незачем.
pub fn restamp_seq(bytes: &mut [u8], seq: u32) -> Result<()> {
    let Some(mut header) = BlockHeader::parse(bytes)? else {
        return Err(Error::EmptyBlock);
    };
    let body = bytes
        .get(BlockHeader::SIZE..BlockHeader::SIZE + header.body_len as usize)
        .ok_or(Error::Truncated)?;
    header.seq = seq;
    header.crc = compute_crc(&header, body);
    bytes[..BlockHeader::SIZE].copy_from_slice(&header.to_bytes());
    Ok(())
}

fn compute_crc(header: &BlockHeader, body_on_disk: &[u8]) -> u32 {
    let bytes = header.to_bytes();
    let crc = crc32c::crc32c(&bytes[..28]);
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
    /// `seq` — порядковый номер блока в сегменте (с нуля): по разрыву
    /// нумерации читатель отличает потерянный блок от порчи.
    ///
    /// Сжатие применяется, только если реально уменьшает тело: на коротких
    /// блоках LZ4 нередко даёт прирост, и хранить раздутое тело незачем.
    pub fn finish(
        &mut self,
        seq: u32,
        compression: Compression,
        out: &mut Vec<u8>,
    ) -> Result<BlockHeader> {
        let base = self.base.ok_or(Error::EmptyBlock)?;
        let len = self.body.len();
        if len > BlockHeader::MAX_BODY as usize {
            // Накопитель сбрасывается даже при отказе: иначе переросший
            // потолок буфер остался бы в нём навсегда, и канал заклинило бы
            // на этой же ошибке при каждой следующей попытке.
            self.reset();
            return Err(Error::LimitExceeded {
                what: "block body",
                value: len as u64,
                max: u64::from(BlockHeader::MAX_BODY),
            });
        }
        let raw_len = len as u32;

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
            seq,
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
        // CRC считает `from_parts` — второй раз здесь считать нечего: он
        // проходит по всему телу, а блоки читаются десятками тысяч.
        Ok(Some(Self::from_parts(header, body)?))
    }

    /// Собрать блок из уже разобранного заголовка и тела **как оно лежит на
    /// диске**.
    ///
    /// Для того, кто читает заголовок и тело по отдельности (восстановление
    /// сегмента читает заголовок первым, чтобы отличить нулевой хвост от
    /// данных, и только потом знает длину тела). CRC проверяется здесь же:
    /// пропустить проверку значило бы отдать наружу непроверенные байты.
    pub fn from_parts(header: BlockHeader, body_on_disk: &'a [u8]) -> Result<Self> {
        if body_on_disk.len() != header.body_len as usize {
            return Err(Error::Truncated);
        }
        header.verify(body_on_disk)?;
        Ok(Self {
            header,
            body: header.decompress(body_on_disk)?,
        })
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
    use crate::ids::{EventId, MetricId};
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
        let header = b.finish(0, Compression::None, &mut out).unwrap();
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
        let header = b.finish(0, Compression::Lz4, &mut out).unwrap();
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
        let header = b.finish(0, Compression::Lz4, &mut out).unwrap();
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
        b.finish(0, Compression::None, &mut out).unwrap();

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
        b.finish(0, Compression::None, &mut out).unwrap();

        out.truncate(out.len() - 5); // обрыв питания посреди записи блока
        assert_eq!(Block::parse(&out).unwrap_err(), Error::Truncated);
    }

    #[test]
    fn mixed_records_and_time_reconstruction() {
        let mut b = BlockBuilder::new();
        b.push(Micros(500), &msg(9, &[1])).unwrap();
        for i in 0..10u64 {
            b.push(
                Micros(1_000 + i * 250),
                &Record::Sample(Sample {
                    metric: MetricId(1),
                    value: Value::F32(20.0 + i as f32),
                }),
            )
            .unwrap();
        }
        let mut out = Vec::new();
        b.finish(0, Compression::None, &mut out).unwrap();

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
        b.finish(0, Compression::None, &mut out).unwrap();

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
            b.finish(0, Compression::None, &mut out),
            Err(Error::EmptyBlock)
        );
        assert!(out.is_empty());
    }

    #[test]
    fn header_roundtrip_bytes() {
        let h = BlockHeader {
            body_len: 1234,
            raw_len: 5678,
            seq: 0,
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
    fn reserved_bytes_must_be_zero() {
        let h = BlockHeader {
            body_len: 8,
            raw_len: 8,
            seq: 0,
            base: Micros(0),
            count: 1,
            compression: Compression::None,
            crc: 0,
        };
        for offset in [3usize, 26, 27] {
            let mut bytes = h.to_bytes();
            bytes[offset] = 1;
            assert_eq!(
                BlockHeader::parse(&bytes),
                Err(Error::ReservedNotZero),
                "резерв на смещении {offset}"
            );
        }

        let mut bytes = h.to_bytes();
        bytes[2] = 0b1000_0000; // старшие биты флагов зарезервированы
        assert_eq!(BlockHeader::parse(&bytes), Err(Error::ReservedNotZero));
    }

    #[test]
    fn zero_body_len_is_corruption_not_end_of_data() {
        // Ключевое отличие от наивного «body_len == 0 ⇒ конец»: одиночный
        // перевёрнутый бит в длине не имеет права выглядеть как конец лога,
        // иначе уже подтверждённые fdatasync'ом блоки исчезали бы молча.
        let h = BlockHeader {
            body_len: 4096,
            raw_len: 4096,
            seq: 12,
            base: Micros(999),
            count: 7,
            compression: Compression::None,
            crc: 0x1234,
        };
        let mut bytes = h.to_bytes();
        bytes[4..8].copy_from_slice(&0u32.to_le_bytes());
        let err = BlockHeader::parse(&bytes).unwrap_err();
        assert!(
            matches!(err, Error::LimitExceeded { .. }),
            "нулевая длина при непустом заголовке — порча, а не конец: {err}"
        );

        // Терминатором остаётся только полностью нулевой заголовок.
        assert_eq!(BlockHeader::parse(&[0u8; BlockHeader::SIZE]).unwrap(), None);
    }

    #[test]
    fn absurd_lengths_rejected_before_allocation() {
        let h = BlockHeader {
            body_len: 16,
            raw_len: 16,
            seq: 0,
            base: Micros(0),
            count: 1,
            compression: Compression::Lz4,
            crc: 0,
        };
        // body_len за потолком формата: разбор обязан отказать, не пытаясь
        // ничего выделить.
        let mut bytes = h.to_bytes();
        bytes[4..8].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            BlockHeader::parse(&bytes),
            Err(Error::LimitExceeded { .. })
        ));

        // raw_len тоже: по нему выделяется буфер распаковки.
        let mut bytes = h.to_bytes();
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            BlockHeader::parse(&bytes),
            Err(Error::LimitExceeded { .. })
        ));
    }

    #[test]
    fn raw_len_is_checked_against_body_len() {
        // raw_len в пределах потолка формата, но несоразмерна телу: тридцать
        // байт не разворачиваются в шестьдесят четыре мегабайта. Без сверки
        // одной длины с другой такой заголовок заставлял бы читателя выделить
        // буфер по размеру, которого в файле нет и близко.
        let h = BlockHeader {
            body_len: 30,
            raw_len: BlockHeader::MAX_BODY - 1,
            seq: 0,
            base: Micros(0),
            count: 1,
            compression: Compression::Lz4,
            crc: 0,
        };
        let err = BlockHeader::parse(&h.to_bytes()).unwrap_err();
        assert!(
            matches!(
                err,
                Error::LimitExceeded {
                    what: "block raw_len",
                    ..
                }
            ),
            "получено {err}"
        );

        // Достижимое раздувание принимается: предел LZ4 — 255:1.
        let ok = BlockHeader {
            raw_len: 30 * 255,
            ..h
        };
        assert!(BlockHeader::parse(&ok.to_bytes()).is_ok());

        // Без сжатия длины обязаны совпадать.
        let uncompressed = BlockHeader {
            body_len: 30,
            raw_len: 31,
            compression: Compression::None,
            ..h
        };
        assert!(matches!(
            BlockHeader::parse(&uncompressed.to_bytes()),
            Err(Error::LimitExceeded { .. })
        ));
    }

    #[test]
    fn foreign_bytes_rejected_by_magic() {
        let mut bytes = [0xAAu8; BlockHeader::SIZE];
        bytes[0] = b'X';
        assert!(matches!(
            BlockHeader::parse(&bytes),
            Err(Error::BadMagic { .. })
        ));
    }

    #[test]
    fn restamping_seq_keeps_block_valid() {
        let mut b = BlockBuilder::new();
        b.push(Micros(10), &msg(1, &[1, 2, 3])).unwrap();
        b.push(Micros(20), &msg(2, &[4, 5])).unwrap();
        let mut out = Vec::new();
        b.finish(0, Compression::Lz4, &mut out).unwrap();

        restamp_seq(&mut out, 7).unwrap();
        let block = Block::parse(&out).unwrap().expect("CRC пересчитан");
        assert_eq!(block.header.seq, 7);
        assert_eq!(block.records().count(), 2, "содержимое не тронуто");
    }

    #[test]
    fn seq_is_preserved_for_hole_detection() {
        let mut b = BlockBuilder::new();
        b.push(Micros(0), &msg(1, &[1])).unwrap();
        let mut out = Vec::new();
        let h = b.finish(42, Compression::None, &mut out).unwrap();
        assert_eq!(h.seq, 42);
        assert_eq!(Block::parse(&out).unwrap().unwrap().header.seq, 42);
    }
}
