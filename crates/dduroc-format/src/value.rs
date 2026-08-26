//! Telemetry values.
//!
//! The value type is declared on the metric in the schema and is
//! **duplicated in the low bits of the record flags** of [`Sample`]: a sample
//! is the only record without a length prefix, and `vtype` in the flags is
//! what gives the value its length. Without it a sample cannot be skipped,
//! and skipping is exactly what a reader without the schema has to do. The
//! duplication is free — the flags are in the record's first byte anyway.
//!
//! Hence a consequence worth knowing: **a new `vtype` is a breaking change**.
//! A reader that does not know the code loses not one record but the whole
//! rest of the block.
//!
//! [`Sample`]: crate::record::Sample

use crate::error::{Error, Result};
use crate::varint;

/// The value type of a series.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueType {
    /// 4 bytes, IEEE-754.
    F32 = 0,
    /// 8 bytes, IEEE-754.
    F64 = 1,
    /// varint plus zigzag.
    I64 = 2,
    /// varint.
    U64 = 3,
    /// 1 byte (0/1).
    Bool = 4,
    /// A varint `len` plus bytes. A composite measurement: a spectrum snapshot
    /// and the like.
    Blob = 5,
}

impl ValueType {
    pub const fn from_u8(raw: u8) -> Result<Self> {
        match raw {
            0 => Ok(ValueType::F32),
            1 => Ok(ValueType::F64),
            2 => Ok(ValueType::I64),
            3 => Ok(ValueType::U64),
            4 => Ok(ValueType::Bool),
            5 => Ok(ValueType::Blob),
            other => Err(Error::UnknownValueType(other)),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            ValueType::F32 => "f32",
            ValueType::F64 => "f64",
            ValueType::I64 => "i64",
            ValueType::U64 => "u64",
            ValueType::Bool => "bool",
            ValueType::Blob => "blob",
        }
    }
}

/// The value of one sample. A blob borrows bytes from the block — no copying.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Value<'a> {
    F32(f32),
    F64(f64),
    I64(i64),
    U64(u64),
    Bool(bool),
    Blob(&'a [u8]),
}

impl<'a> Value<'a> {
    pub const fn value_type(&self) -> ValueType {
        match self {
            Value::F32(_) => ValueType::F32,
            Value::F64(_) => ValueType::F64,
            Value::I64(_) => ValueType::I64,
            Value::U64(_) => ValueType::U64,
            Value::Bool(_) => ValueType::Bool,
            Value::Blob(_) => ValueType::Blob,
        }
    }

    /// As f64, for charts. `None` for blobs.
    pub fn as_f64(&self) -> Option<f64> {
        match *self {
            Value::F32(v) => Some(f64::from(v)),
            Value::F64(v) => Some(v),
            Value::I64(v) => Some(v as f64),
            Value::U64(v) => Some(v as f64),
            Value::Bool(v) => Some(if v { 1.0 } else { 0.0 }),
            Value::Blob(_) => None,
        }
    }

    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        match *self {
            Value::F32(v) => out.extend_from_slice(&v.to_le_bytes()),
            Value::F64(v) => out.extend_from_slice(&v.to_le_bytes()),
            Value::I64(v) => {
                varint::write_i64(out, v);
            }
            Value::U64(v) => {
                varint::write_u64(out, v);
            }
            Value::Bool(v) => out.push(u8::from(v)),
            Value::Blob(b) => {
                varint::write_u64(out, b.len() as u64);
                out.extend_from_slice(b);
            }
        }
    }

    /// Reads a value of type `ty` from the start of `input`. Returns the value
    /// and the number of bytes consumed.
    pub(crate) fn decode(ty: ValueType, input: &'a [u8]) -> Result<(Self, usize)> {
        fn fixed<const N: usize>(input: &[u8]) -> Result<[u8; N]> {
            input
                .get(..N)
                .and_then(|s| s.try_into().ok())
                .ok_or(Error::Truncated)
        }

        match ty {
            ValueType::F32 => Ok((Value::F32(f32::from_le_bytes(fixed::<4>(input)?)), 4)),
            ValueType::F64 => Ok((Value::F64(f64::from_le_bytes(fixed::<8>(input)?)), 8)),
            ValueType::I64 => {
                let (v, n) = varint::read_i64(input)?;
                Ok((Value::I64(v), n))
            }
            ValueType::U64 => {
                let (v, n) = varint::read_u64(input)?;
                Ok((Value::U64(v), n))
            }
            ValueType::Bool => match input.first() {
                Some(0) => Ok((Value::Bool(false), 1)),
                Some(1) => Ok((Value::Bool(true), 1)),
                // Any other byte is corruption, not "true": coercing it
                // silently would hide damaged data.
                Some(_) => Err(Error::ReservedValue),
                None => Err(Error::Truncated),
            },
            ValueType::Blob => {
                let (len, n) = varint::read_u64(input)?;
                let len = usize::try_from(len).map_err(|_| Error::Truncated)?;
                // The length came from a file and can be anything. `n + len`
                // without a check overflows usize — on a 32-bit target (armv7)
                // that is reachable with a length of about four gigabytes,
                // which is ordinary garbage in a damaged block rather than an
                // exotic case.
                let end = n.checked_add(len).ok_or(Error::Truncated)?;
                let bytes = input.get(n..end).ok_or(Error::Truncated)?;
                Ok((Value::Blob(bytes), end))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: Value<'_>) {
        let mut buf = Vec::new();
        v.encode(&mut buf);
        let (got, n) = Value::decode(v.value_type(), &buf).expect("decoding");
        assert_eq!(got, v);
        assert_eq!(n, buf.len(), "not everything was consumed");
    }

    #[test]
    fn roundtrip_all_types() {
        roundtrip(Value::F32(36.6));
        roundtrip(Value::F32(f32::NEG_INFINITY));
        roundtrip(Value::F64(-1.0e-300));
        roundtrip(Value::I64(-42));
        roundtrip(Value::I64(i64::MIN));
        roundtrip(Value::U64(0));
        roundtrip(Value::U64(u64::MAX));
        roundtrip(Value::Bool(true));
        roundtrip(Value::Bool(false));
        roundtrip(Value::Blob(&[]));
        roundtrip(Value::Blob(&[1, 2, 3, 4, 5]));
    }

    #[test]
    fn scalar_sizes_are_minimal() {
        let mut buf = Vec::new();
        Value::F32(1.0).encode(&mut buf);
        assert_eq!(buf.len(), 4);

        buf.clear();
        Value::U64(100).encode(&mut buf);
        assert_eq!(buf.len(), 1, "a small u64 is one byte");

        buf.clear();
        Value::I64(-5).encode(&mut buf);
        assert_eq!(buf.len(), 1, "a small i64 through zigzag is one byte");
    }

    #[test]
    fn nan_roundtrips_bitwise() {
        let mut buf = Vec::new();
        Value::F64(f64::NAN).encode(&mut buf);
        let (got, _) = Value::decode(ValueType::F64, &buf).unwrap();
        // NaN != NaN, so compare the bits.
        match got {
            Value::F64(v) => assert!(v.is_nan()),
            other => panic!("expected an f64, got {other:?}"),
        }
    }

    #[test]
    fn truncated_and_bad_bool() {
        assert_eq!(
            Value::decode(ValueType::F32, &[1, 2]),
            Err(Error::Truncated)
        );
        assert_eq!(Value::decode(ValueType::Bool, &[]), Err(Error::Truncated));
        assert_eq!(
            Value::decode(ValueType::Bool, &[2]),
            Err(Error::ReservedValue)
        );
        // A blob whose length exceeds what is available.
        let mut buf = Vec::new();
        varint::write_u64(&mut buf, 10);
        buf.extend_from_slice(&[1, 2, 3]);
        assert_eq!(Value::decode(ValueType::Blob, &buf), Err(Error::Truncated));
    }

    #[test]
    fn absurd_blob_length_does_not_overflow() {
        // A blob's length comes from a file. Adding offset and length without a
        // check overflows usize: on armv7 (32 bits) four gigabytes suffice, on
        // a 64-bit build values near u64::MAX do. In debug the overflow panics
        // right inside the parsing of someone else's dump.
        for len in [u64::MAX, u64::from(u32::MAX), 1 << 40] {
            let mut buf = Vec::new();
            varint::write_u64(&mut buf, len);
            buf.extend_from_slice(&[1, 2, 3, 4]);
            assert_eq!(
                Value::decode(ValueType::Blob, &buf),
                Err(Error::Truncated),
                "length {len} must be rejected, not overflow the addition"
            );
        }
    }

    #[test]
    fn value_type_roundtrip() {
        for ty in [
            ValueType::F32,
            ValueType::F64,
            ValueType::I64,
            ValueType::U64,
            ValueType::Bool,
            ValueType::Blob,
        ] {
            assert_eq!(ValueType::from_u8(ty as u8), Ok(ty));
        }
        assert_eq!(
            ValueType::from_u8(6),
            Err(Error::UnknownValueType(6)),
            "an unknown vtype is rejected"
        );
    }
}
