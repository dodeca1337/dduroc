//! LEB128 varint: seven payload bits per byte, the top bit marking
//! continuation. Signed values are zigzag-encoded.
//!
//! Values below 128 occupy one byte, and the whole economy of the format
//! rests on that (a typical `event_id`, `series_local` or `Δt` is small).

use crate::error::{Error, Result};

/// The most bytes a u64 varint can take: ceil(64 / 7) = 10.
pub const MAX_LEN_U64: usize = 10;

/// Append `value` to `out`. Returns the number of bytes written.
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

/// Append a signed value (zigzag, then varint).
pub fn write_i64(out: &mut Vec<u8>, value: i64) -> usize {
    write_u64(out, zigzag_encode(value))
}

/// Read a varint from the start of `input`. Returns `(value, byte count)`.
///
/// Errors: [`Error::Truncated`] — the data ran out before the terminating
/// byte; [`Error::VarintOverflow`] — the encoding does not fit in a u64 or
/// carries redundant (non-canonical) high bits.
pub fn read_u64(input: &[u8]) -> Result<(u64, usize)> {
    let mut value: u64 = 0;
    let mut shift = 0u32;

    for (i, &byte) in input.iter().take(MAX_LEN_U64).enumerate() {
        let payload = u64::from(byte & 0x7F);

        // On the tenth byte (shift == 63) only one bit of room is left in the
        // u64.
        if shift == 63 && payload > 1 {
            return Err(Error::VarintOverflow);
        }
        value |= payload << shift;

        if byte & 0x80 == 0 {
            // Canonical: the terminating byte cannot be zero padding (except
            // for the single byte that encodes 0).
            if byte == 0 && i > 0 {
                return Err(Error::VarintOverflow);
            }
            return Ok((value, i + 1));
        }
        shift += 7;
    }

    if input.len() >= MAX_LEN_U64 {
        // Ten bytes all carrying the continuation bit are knowably not a u64.
        Err(Error::VarintOverflow)
    } else {
        Err(Error::Truncated)
    }
}

/// Read a signed value (varint, then zigzag).
pub fn read_i64(input: &[u8]) -> Result<(i64, usize)> {
    let (raw, n) = read_u64(input)?;
    Ok((zigzag_decode(raw), n))
}

/// How many bytes `value` will take. For sizing without writing.
pub fn len_u64(value: u64) -> usize {
    // 1 plus the number of full 7-bit groups above the lowest one.
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
        assert_eq!(written, buf.len(), "the returned length disagrees with buf");
        assert_eq!(
            written,
            len_u64(v),
            "len_u64 disagrees with write_u64 for {v}"
        );
        let (got, read) = read_u64(&buf).expect("a read");
        assert_eq!(got, v);
        assert_eq!(read, written);
    }

    #[test]
    fn single_byte_range() {
        for v in 0..=127u64 {
            roundtrip(v);
            let mut buf = Vec::new();
            write_u64(&mut buf, v);
            assert_eq!(buf.len(), 1, "values below 128 must take 1 byte");
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
        // Length boundaries: 2^(7k) is the first value that needs k+1 bytes.
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
        // Values small in absolute terms take one byte — the point of zigzag.
        for v in -63i64..=63 {
            let mut buf = Vec::new();
            write_i64(&mut buf, v);
            assert_eq!(buf.len(), 1, "a small {v} must take 1 byte");
        }
    }

    #[test]
    fn truncated_input() {
        assert!(matches!(read_u64(&[]), Err(Error::Truncated)));
        // Every byte carries the continuation bit, but the stream ended.
        assert!(matches!(read_u64(&[0x80]), Err(Error::Truncated)));
        assert!(matches!(
            read_u64(&[0x80, 0x80, 0x80]),
            Err(Error::Truncated)
        ));
    }

    #[test]
    fn overflow_rejected() {
        // Ten continuation bytes: no terminator, and already outside u64.
        let all_cont = [0x80u8; MAX_LEN_U64];
        assert!(matches!(read_u64(&all_cont), Err(Error::VarintOverflow)));

        // A tenth byte with a payload above 1 does not fit in a u64.
        let mut buf = vec![0x80u8; 9];
        buf.push(0x02);
        assert!(matches!(read_u64(&buf), Err(Error::VarintOverflow)));

        // Non-canonical: a redundant zero terminator.
        assert!(matches!(
            read_u64(&[0x81, 0x00]),
            Err(Error::VarintOverflow)
        ));
    }

    #[test]
    fn trailing_bytes_ignored() {
        // The reader must return the length, not swallow the tail.
        let mut buf = Vec::new();
        write_u64(&mut buf, 300);
        buf.extend_from_slice(&[0xAA, 0xBB]);
        let (v, n) = read_u64(&buf).unwrap();
        assert_eq!(v, 300);
        assert_eq!(n, 2);
    }
}
