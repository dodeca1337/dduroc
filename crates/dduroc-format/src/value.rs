//! Значения телеметрии.
//!
//! Тип значения объявлен у метрики в схеме и **дублируется в младших битах
//! флагов записи** [`Sample`]: сэмпл — единственная запись без префикса длины,
//! и `vtype` во флагах и есть источник длины значения. Без него сэмпл нечем
//! пропустить, а пропускать нужно тому, кто не знает схемы. Дублирование
//! бесплатно — флаги всё равно есть в первом байте записи.
//!
//! Отсюда следствие, которое надо знать: **новый `vtype` — ломающее
//! изменение**. Читатель, не знающий кода, теряет не одну запись, а весь
//! остаток блока.
//!
//! [`Sample`]: crate::record::Sample

use crate::error::{Error, Result};
use crate::varint;

/// Тип значений серии.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum ValueType {
    /// 4 байта, IEEE-754.
    F32 = 0,
    /// 8 байт, IEEE-754.
    F64 = 1,
    /// varint + zigzag.
    I64 = 2,
    /// varint.
    U64 = 3,
    /// 1 байт (0/1).
    Bool = 4,
    /// `len` varint + байты. Составное измерение: снимок спектра и т.п.
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

/// Значение одного сэмпла. Blob заимствует байты из блока — без копирования.
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

    /// Как f64 — для графиков. `None` для blob'ов.
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

    /// Читает значение типа `ty` из начала `input`.
    /// Возвращает значение и число потреблённых байт.
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
                // Любой другой байт — повреждение, а не «истина»: тихое
                // приведение скрывало бы порчу данных.
                Some(_) => Err(Error::ReservedNotZero),
                None => Err(Error::Truncated),
            },
            ValueType::Blob => {
                let (len, n) = varint::read_u64(input)?;
                let len = usize::try_from(len).map_err(|_| Error::Truncated)?;
                // Длина пришла из файла и может быть любой. `n + len` без
                // проверки переполняет usize — на 32-битной цели (armv7) это
                // достижимо длиной около четырёх гигабайт, то есть обычным
                // мусором в повреждённом блоке, а не экзотикой.
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
        let (got, n) = Value::decode(v.value_type(), &buf).expect("декод");
        assert_eq!(got, v);
        assert_eq!(n, buf.len(), "потреблено не всё");
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
        assert_eq!(buf.len(), 1, "малое u64 — один байт");

        buf.clear();
        Value::I64(-5).encode(&mut buf);
        assert_eq!(buf.len(), 1, "малое i64 через zigzag — один байт");
    }

    #[test]
    fn nan_roundtrips_bitwise() {
        let mut buf = Vec::new();
        Value::F64(f64::NAN).encode(&mut buf);
        let (got, _) = Value::decode(ValueType::F64, &buf).unwrap();
        // NaN != NaN, сравниваем биты.
        match got {
            Value::F64(v) => assert!(v.is_nan()),
            other => panic!("ожидался f64, получен {other:?}"),
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
            Err(Error::ReservedNotZero)
        );
        // Blob с длиной больше доступного.
        let mut buf = Vec::new();
        varint::write_u64(&mut buf, 10);
        buf.extend_from_slice(&[1, 2, 3]);
        assert_eq!(Value::decode(ValueType::Blob, &buf), Err(Error::Truncated));
    }

    #[test]
    fn absurd_blob_length_does_not_overflow() {
        // Длина blob'а приходит из файла. Сложение «смещение + длина» без
        // проверки переполняет usize: на armv7 (32 бита) хватает четырёх
        // гигабайт, на 64-битной сборке — значений около u64::MAX. Результат
        // переполнения в debug — паника прямо в разборе чужого дампа.
        for len in [u64::MAX, u64::from(u32::MAX), 1 << 40] {
            let mut buf = Vec::new();
            varint::write_u64(&mut buf, len);
            buf.extend_from_slice(&[1, 2, 3, 4]);
            assert_eq!(
                Value::decode(ValueType::Blob, &buf),
                Err(Error::Truncated),
                "длина {len} обязана быть отвергнута, а не переполнить сложение"
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
            "неизвестный vtype отвергается"
        );
    }
}
