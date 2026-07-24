//! Сегмент — файл-контейнер блоков.
//!
//! ```text
//! <boot:08x>-<micros:016x>.seg
//! [SegmentHeader 32B] [Block]* [Footer]?
//! ```
//!
//! Имя фиксированной ширины в hex: лексикографический порядок имён совпадает
//! с временным, поэтому выбор сегментов по диапазону — это сортировка строк,
//! без чтения содержимого.
//!
//! Сегмент никогда не пересекает границу run'а: `boot_counter` живёт в
//! заголовке, а не в каждой записи. Смена run'а — всегда новый сегмент.

use crate::error::{Error, Result};
use crate::ids::{BootCounter, Micros, ProtocolVersion};
use core::fmt;

/// Заголовок сегмента.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Версия протокола схемы неймспейса на момент записи сегмента.
    /// Миграция сравнивает её с текущей версией схемы.
    pub protocol_version: ProtocolVersion,
    pub boot: BootCounter,
    /// Время первой записи сегмента (совпадает с именем файла).
    pub base: Micros,
    /// Идентичность хранилища, в котором создан сегмент.
    ///
    /// Без неё файлы, скопированные с другого устройства, бесшовно
    /// смешались бы с локальными: `boot_counter` у них своя нумерация, и
    /// чужие события получили бы локальный UTC-якорь, то есть заведомо
    /// неверное абсолютное время.
    pub store_id: u64,
}

/// Сигнатура файла сегмента.
pub const SEGMENT_MAGIC: [u8; 4] = *b"DSEG";

impl SegmentHeader {
    pub const SIZE: usize = 32;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..4].copy_from_slice(&SEGMENT_MAGIC);
        b[4] = crate::CONTAINER_VERSION;
        b[5] = 0; // флаги, резерв
        b[6..8].copy_from_slice(&self.protocol_version.0.to_le_bytes());
        b[8..12].copy_from_slice(&self.boot.0.to_le_bytes());
        b[12..20].copy_from_slice(&self.base.0.to_le_bytes());
        b[20..28].copy_from_slice(&self.store_id.to_le_bytes());
        let crc = crc32c::crc32c(&b[..28]);
        b[28..32].copy_from_slice(&crc.to_le_bytes());
        b
    }

    pub fn parse(input: &[u8]) -> Result<Self> {
        let raw: &[u8; Self::SIZE] = input
            .get(..Self::SIZE)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::Truncated)?;

        let magic: [u8; 4] = raw[0..4].try_into().expect("срез 4 байта");
        if magic != SEGMENT_MAGIC {
            return Err(Error::BadMagic {
                expected: SEGMENT_MAGIC,
                actual: magic,
            });
        }

        let expected_crc = u32::from_le_bytes(raw[28..32].try_into().expect("срез 4 байта"));
        let actual_crc = crc32c::crc32c(&raw[..28]);
        if expected_crc != actual_crc {
            return Err(Error::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        // CRC проверен до версии: иначе мусорный байт версии выдавался бы за
        // «формат из будущего», маскируя порчу.
        if raw[4] != crate::CONTAINER_VERSION {
            return Err(Error::UnsupportedContainerVersion(
                raw[4],
                crate::CONTAINER_VERSION,
            ));
        }
        if raw[5] != 0 {
            return Err(Error::ReservedNotZero);
        }

        Ok(Self {
            protocol_version: ProtocolVersion(u16::from_le_bytes([raw[6], raw[7]])),
            boot: BootCounter(u32::from_le_bytes(
                raw[8..12].try_into().expect("срез 4 байта"),
            )),
            base: Micros(u64::from_le_bytes(
                raw[12..20].try_into().expect("срез 8 байт"),
            )),
            store_id: u64::from_le_bytes(raw[20..28].try_into().expect("срез 8 байт")),
        })
    }

    /// Имя файла этого сегмента.
    pub fn file_name(&self) -> SegmentName {
        SegmentName {
            boot: self.boot,
            base: self.base,
        }
    }
}

/// Имя файла сегмента: `<boot:08x>-<micros:016x>.seg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentName {
    pub boot: BootCounter,
    pub base: Micros,
}

/// Расширение файлов сегментов.
pub const SEGMENT_EXT: &str = "seg";

impl SegmentName {
    pub fn new(boot: BootCounter, base: Micros) -> Self {
        Self { boot, base }
    }

    /// Разобрать имя файла. `None` — файл не наш (чужой мусор в каталоге).
    ///
    /// Принимается ровно то, что порождает `Display`, и ничего сверх: строгая
    /// ширина, только строчные шестнадцатеричные цифры. `from_str_radix` сам
    /// по себе глотает знак (`+000002a`), а прописные буквы дают ещё одно
    /// написание того же числа — и один сегмент оказался бы в учёте дважды,
    /// под разными именами, тогда как на диске файл один.
    pub fn parse(name: &str) -> Option<Self> {
        fn is_lower_hex(s: &str, width: usize) -> bool {
            s.len() == width
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }

        let stem = name.strip_suffix(".seg")?;
        let (boot, base) = stem.split_once('-')?;
        if !is_lower_hex(boot, 8) || !is_lower_hex(base, 16) {
            return None;
        }
        Some(Self {
            boot: BootCounter(u32::from_str_radix(boot, 16).ok()?),
            base: Micros(u64::from_str_radix(base, 16).ok()?),
        })
    }
}

impl fmt::Display for SegmentName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:08x}-{:016x}.{SEGMENT_EXT}", self.boot.0, self.base.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> SegmentHeader {
        SegmentHeader {
            protocol_version: ProtocolVersion(3),
            boot: BootCounter(42),
            base: Micros(1_000_000),
            store_id: 0,
        }
    }

    #[test]
    fn header_roundtrip() {
        let h = header();
        let bytes = h.to_bytes();
        assert_eq!(bytes.len(), SegmentHeader::SIZE);
        assert_eq!(&bytes[0..4], b"DSEG");
        assert_eq!(SegmentHeader::parse(&bytes).unwrap(), h);
    }

    #[test]
    fn rejects_foreign_and_corrupt() {
        let mut bytes = header().to_bytes();
        bytes[0] = b'X';
        assert!(matches!(
            SegmentHeader::parse(&bytes),
            Err(Error::BadMagic { .. })
        ));

        // Порча полезного поля ловится CRC.
        let mut bytes = header().to_bytes();
        bytes[9] ^= 0xFF;
        assert!(matches!(
            SegmentHeader::parse(&bytes),
            Err(Error::CrcMismatch { .. })
        ));

        assert_eq!(SegmentHeader::parse(&[0u8; 10]), Err(Error::Truncated));
    }

    #[test]
    fn rejects_other_container_version() {
        let mut bytes = header().to_bytes();
        bytes[4] = 2;
        // CRC пересчитан — имитируем честно записанный сегмент версии 2.
        let crc = crc32c::crc32c(&bytes[..28]);
        bytes[28..32].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(
            SegmentHeader::parse(&bytes),
            Err(Error::UnsupportedContainerVersion(2, 1))
        );
    }

    #[test]
    fn name_roundtrip_and_ordering() {
        let n = SegmentName::new(BootCounter(42), Micros(0x3b9aca00));
        assert_eq!(n.to_string(), "0000002a-000000003b9aca00.seg");
        assert_eq!(SegmentName::parse(&n.to_string()), Some(n));
        assert_eq!(
            header().file_name().to_string(),
            "0000002a-00000000000f4240.seg"
        );

        // Лексикографический порядок имён = временной порядок.
        let mut names: Vec<String> = vec![
            SegmentName::new(BootCounter(2), Micros(5)).to_string(),
            SegmentName::new(BootCounter(1), Micros(500)).to_string(),
            SegmentName::new(BootCounter(1), Micros(20)).to_string(),
            SegmentName::new(BootCounter(10), Micros(1)).to_string(),
        ];
        names.sort();
        let parsed: Vec<_> = names
            .iter()
            .map(|n| SegmentName::parse(n).unwrap())
            .collect();
        let mut expected = parsed.clone();
        expected.sort();
        assert_eq!(
            parsed, expected,
            "порядок строк совпадает с порядком (boot, время)"
        );
    }

    #[test]
    fn name_parse_rejects_junk() {
        assert_eq!(SegmentName::parse("readme.txt"), None);
        assert_eq!(SegmentName::parse("0000002a.seg"), None);
        // Нестрогая ширина ломала бы сортировку — отвергаем.
        assert_eq!(SegmentName::parse("2a-3b9aca00.seg"), None);
        assert_eq!(SegmentName::parse("0000002a-000000003b9aca0.seg"), None);
        assert_eq!(SegmentName::parse("zzzzzzzz-000000003b9aca00.seg"), None);
    }

    #[test]
    fn parse_accepts_only_what_display_produces() {
        // Каждое из этих имён `from_str_radix` разобрал бы, и сегмент получил
        // бы второе написание: в инвентаре он оказался бы двумя записями, а
        // на диске остался бы одним файлом.
        for junk in [
            "+000002a-000000003b9aca00.seg", // знак вместо цифры
            "0000002A-000000003b9aca00.seg", // прописные
            "0000002a-000000003B9ACA00.seg", // прописные во второй половине
            "0000002a-+00000003b9aca00.seg", // знак во второй половине
            "0000002a-000000003b9aca00.SEG", // расширение не то
            " 000002a-000000003b9aca00.seg", // пробел
        ] {
            assert_eq!(
                SegmentName::parse(junk),
                None,
                "{junk:?} не порождается Display и не должно разбираться"
            );
        }

        // А то, что Display породил, — обязано разбираться обратно.
        for (boot, base) in [(0u32, 0u64), (u32::MAX, u64::MAX), (0xabcdef, 0x123456789)] {
            let n = SegmentName::new(BootCounter(boot), Micros(base));
            assert_eq!(SegmentName::parse(&n.to_string()), Some(n));
        }
    }
}
