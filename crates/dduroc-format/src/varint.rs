//! LEB128 varint: 7 бит полезной нагрузки на байт, старший бит — признак
//! продолжения. Знаковые значения кодируются zigzag'ом.
//!
//! Значения < 128 занимают один байт — на это опирается вся экономия формата
//! (типичные `event_id`, `series_local`, `Δt` малы).

use crate::error::{Error, Result};

/// Максимум байт в varint для u64: ceil(64 / 7) = 10.
pub const MAX_LEN_U64: usize = 10;

/// Дописать `value` в конец `out`. Возвращает число записанных байт.
pub fn write_u64(out: &mut Vec<u8>, mut value: u64) -> usize {
    let mut n = 0;
    loop {
        let byte = (value as u8) & 0x7F;
        value >>= 7;
        n += 1;
        if value == 0 {
            out.push(byte);
            return n;
        }
        out.push(byte | 0x80);
    }
}

/// Дописать знаковое значение (zigzag + varint).
pub fn write_i64(out: &mut Vec<u8>, value: i64) -> usize {
    write_u64(out, zigzag_encode(value))
}

/// Прочитать varint из начала `input`. Возвращает `(значение, число байт)`.
///
/// Ошибки: [`Error::Truncated`] — данные кончились до терминирующего байта;
/// [`Error::VarintOverflow`] — кодировка не влезает в u64 или содержит
/// избыточные (non-canonical) старшие биты.
pub fn read_u64(input: &[u8]) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;

    for (i, &byte) in input.iter().take(MAX_LEN_U64).enumerate() {
        let payload = u64::from(byte & 0x7F);

        // На 10-м байте (shift == 63) в u64 остаётся место только под 1 бит.
        if shift == 63 && payload > 1 {
            return Err(Error::VarintOverflow);
        }
        value |= payload << shift;

        if byte & 0x80 == 0 {
            // Канонический: терминирующий байт не может быть нулевым padding'ом
            // (кроме единственного байта, кодирующего 0).
            if byte == 0 && i > 0 {
                return Err(Error::VarintOverflow);
            }
            return Ok((value, i + 1));
        }
        shift += 7;
    }

    if input.len() >= MAX_LEN_U64 {
        // 10 байт с выставленным continuation-битом — заведомо не u64.
        Err(Error::VarintOverflow)
    } else {
        Err(Error::Truncated)
    }
}

/// Прочитать знаковое значение (varint + zigzag).
pub fn read_i64(input: &[u8]) -> Result<(i64, usize)> {
    let (raw, n) = read_u64(input)?;
    Ok((zigzag_decode(raw), n))
}

/// Число байт, которое займёт `value`. Для расчёта размеров без записи.
pub fn len_u64(value: u64) -> usize {
    // 1 + число полных 7-битных групп сверх младшей.
    let bits = 64 - value.leading_zeros();
    match bits {
        0 => 1,
        n => n.div_ceil(7) as usize,
    }
}

#[inline]
pub const fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
pub const fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: u64) {
        let mut buf = Vec::new();
        let written = write_u64(&mut buf, v);
        assert_eq!(written, buf.len(), "возвращённая длина расходится с buf");
        assert_eq!(
            written,
            len_u64(v),
            "len_u64 расходится с write_u64 для {v}"
        );
        let (got, read) = read_u64(&buf).expect("чтение");
        assert_eq!(got, v);
        assert_eq!(read, written);
    }

    #[test]
    fn single_byte_range() {
        for v in 0..=127u64 {
            roundtrip(v);
            let mut buf = Vec::new();
            write_u64(&mut buf, v);
            assert_eq!(buf.len(), 1, "значения < 128 обязаны занимать 1 байт");
        }
    }

    #[test]
    fn boundaries() {
        for v in [
            0,
            1,
            127,
            128,
            255,
            256,
            16_383,
            16_384,
            u32::MAX as u64,
            u64::MAX - 1,
            u64::MAX,
        ] {
            roundtrip(v);
        }
        // Границы длин: 2^(7k) — первое значение, требующее k+1 байт.
        for k in 1..10u32 {
            let v = 1u64 << (7 * k);
            roundtrip(v);
            roundtrip(v - 1);
            assert_eq!(len_u64(v - 1), k as usize);
            assert_eq!(len_u64(v), k as usize + 1);
        }
    }

    #[test]
    fn u64_max_uses_ten_bytes() {
        let mut buf = Vec::new();
        assert_eq!(write_u64(&mut buf, u64::MAX), MAX_LEN_U64);
    }

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, -1, 1, -2, 2, i64::MIN, i64::MAX, -12345, 12345] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
            let mut buf = Vec::new();
            write_i64(&mut buf, v);
            let (got, n) = read_i64(&buf).unwrap();
            assert_eq!(got, v);
            assert_eq!(n, buf.len());
        }
        // Малые по модулю значения — один байт (главное свойство zigzag).
        for v in -63i64..=63 {
            let mut buf = Vec::new();
            write_i64(&mut buf, v);
            assert_eq!(buf.len(), 1, "малое {v} должно занимать 1 байт");
        }
    }

    #[test]
    fn truncated_input() {
        assert!(matches!(read_u64(&[]), Err(Error::Truncated)));
        // Все байты с continuation-битом, но поток кончился.
        assert!(matches!(read_u64(&[0x80]), Err(Error::Truncated)));
        assert!(matches!(
            read_u64(&[0x80, 0x80, 0x80]),
            Err(Error::Truncated)
        ));
    }

    #[test]
    fn overflow_rejected() {
        // 10 байт с continuation → нет терминатора и уже вне u64.
        let all_cont = [0x80u8; MAX_LEN_U64];
        assert!(matches!(read_u64(&all_cont), Err(Error::VarintOverflow)));

        // 10-й байт с payload > 1 не влезает в u64.
        let mut buf = vec![0x80u8; 9];
        buf.push(0x02);
        assert!(matches!(read_u64(&buf), Err(Error::VarintOverflow)));

        // Non-canonical: избыточный нулевой терминатор.
        assert!(matches!(
            read_u64(&[0x81, 0x00]),
            Err(Error::VarintOverflow)
        ));
    }

    #[test]
    fn trailing_bytes_ignored() {
        // Читатель обязан вернуть длину, а не съесть хвост.
        let mut buf = Vec::new();
        write_u64(&mut buf, 300);
        buf.extend_from_slice(&[0xAA, 0xBB]);
        let (v, n) = read_u64(&buf).unwrap();
        assert_eq!(v, 300);
        assert_eq!(n, 2);
    }
}
