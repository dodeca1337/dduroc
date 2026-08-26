//! A sequential read cursor. An internal helper for the codecs: it removes
//! hand-written offset arithmetic and bounds-checks every read.

use crate::error::{Error, Result};
use crate::varint;

#[derive(Debug, Clone)]
pub(crate) struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    #[inline]
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    /// How many bytes have been consumed.
    #[inline]
    pub(crate) fn pos(&self) -> usize {
        self.pos
    }

    #[inline]
    pub(crate) fn u8(&mut self) -> Result<u8> {
        let b = *self.data.get(self.pos).ok_or(Error::Truncated)?;
        self.pos += 1;
        Ok(b)
    }

    #[inline]
    pub(crate) fn varint(&mut self) -> Result<u64> {
        let (v, n) = varint::read_u64(&self.data[self.pos..])?;
        self.pos += n;
        Ok(v)
    }

    /// A varint narrowed to u32. Overflow is an error — otherwise corrupt data
    /// would silently turn into a valid small id.
    #[inline]
    pub(crate) fn varint_u32(&mut self, what: &'static str) -> Result<u32> {
        let v = self.varint()?;
        u32::try_from(v).map_err(|_| Error::LimitExceeded {
            what,
            value: v,
            max: u64::from(u32::MAX),
        })
    }

    #[inline]
    pub(crate) fn varint_u16(&mut self, what: &'static str) -> Result<u16> {
        let v = self.varint()?;
        u16::try_from(v).map_err(|_| Error::LimitExceeded {
            what,
            value: v,
            max: u64::from(u16::MAX),
        })
    }

    /// A varint length narrowed to usize (on armv7 usize is 32 bits).
    #[inline]
    pub(crate) fn varint_len(&mut self, what: &'static str) -> Result<usize> {
        let v = self.varint()?;
        usize::try_from(v).map_err(|_| Error::LimitExceeded {
            what,
            value: v,
            max: usize::MAX as u64,
        })
    }

    #[inline]
    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(Error::Truncated)?;
        let slice = self.data.get(self.pos..end).ok_or(Error::Truncated)?;
        self.pos = end;
        Ok(slice)
    }

    /// A string with a varint length prefix.
    #[inline]
    pub(crate) fn str(&mut self, what: &'static str) -> Result<&'a str> {
        let len = self.varint_len(what)?;
        let bytes = self.take(len)?;
        core::str::from_utf8(bytes).map_err(|_| Error::BadUtf8)
    }

    /// The unread tail, without moving the cursor.
    #[inline]
    pub(crate) fn rest(&self) -> &'a [u8] {
        &self.data[self.pos.min(self.data.len())..]
    }

    /// Decode a telemetry value of the given type.
    #[inline]
    pub(crate) fn value(&mut self, ty: crate::ValueType) -> Result<crate::Value<'a>> {
        let (v, n) = crate::Value::decode(ty, self.rest())?;
        self.pos += n;
        Ok(v)
    }
}

/// Write a string with a varint length prefix.
pub(crate) fn write_str(out: &mut Vec<u8>, s: &str) {
    varint::write_u64(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_sequentially() {
        let mut buf = vec![0xAB];
        varint::write_u64(&mut buf, 300);
        write_str(&mut buf, "hello");
        buf.extend_from_slice(&[1, 2, 3]);

        let mut c = Cursor::new(&buf);
        assert_eq!(c.u8().unwrap(), 0xAB);
        assert_eq!(c.varint().unwrap(), 300);
        assert_eq!(c.str("s").unwrap(), "hello");
        assert_eq!(c.take(3).unwrap(), &[1, 2, 3]);
        assert_eq!(c.pos(), buf.len());
        assert!(c.rest().is_empty());
        assert_eq!(c.u8(), Err(Error::Truncated));
    }

    #[test]
    fn narrowing_rejects_overflow() {
        let mut buf = Vec::new();
        varint::write_u64(&mut buf, u64::from(u32::MAX) + 1);
        let mut c = Cursor::new(&buf);
        assert!(matches!(
            c.varint_u32("id"),
            Err(Error::LimitExceeded { what: "id", .. })
        ));

        let mut buf = Vec::new();
        varint::write_u64(&mut buf, 70_000);
        let mut c = Cursor::new(&buf);
        assert!(matches!(
            c.varint_u16("id"),
            Err(Error::LimitExceeded { .. })
        ));
    }

    #[test]
    fn take_beyond_end_is_truncated() {
        let buf = [1u8, 2, 3];
        let mut c = Cursor::new(&buf);
        assert_eq!(c.take(4), Err(Error::Truncated));
        // The cursor did not move — the state is intact.
        assert_eq!(c.pos(), 0);
        assert_eq!(c.take(3).unwrap(), &[1, 2, 3]);
    }

    #[test]
    fn bad_utf8_rejected() {
        let mut buf = Vec::new();
        varint::write_u64(&mut buf, 2);
        buf.extend_from_slice(&[0xFF, 0xFE]);
        let mut c = Cursor::new(&buf);
        assert_eq!(c.str("s"), Err(Error::BadUtf8));
    }
}
