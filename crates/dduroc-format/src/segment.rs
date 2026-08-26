//! A segment — the file that contains blocks.
//!
//! ```text
//! <boot:08x>-<micros:016x>.seg
//! [SegmentHeader 32B] [Block]* [Footer]?
//! ```
//!
//! The name is fixed-width hex, so lexicographic order of names coincides with
//! chronological order: picking segments by time range is string sorting, with
//! no need to read any content.
//!
//! A segment never crosses a run boundary: `boot_counter` lives in the header
//! rather than in every record, and a change of run always means a new segment.

use crate::error::{Error, Result};
use crate::ids::{BootCounter, Micros, ProtocolVersion};
use core::fmt;

/// The segment header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    /// Version of the namespace schema protocol at the time the segment was
    /// written. A migration compares it against the current schema version.
    pub protocol_version: ProtocolVersion,
    pub boot: BootCounter,
    /// Time of the segment's first record (matches the file name).
    pub base: Micros,
    /// Identity of the store the segment was created in.
    ///
    /// Without it, files copied from another device would blend seamlessly
    /// into the local ones: their `boot_counter` follows its own numbering,
    /// and foreign events would inherit the local UTC anchor — that is, a
    /// knowably wrong absolute time.
    pub store_id: u64,
}

/// The segment file signature.
pub const SEGMENT_MAGIC: [u8; 4] = *b"DSEG";

impl SegmentHeader {
    pub const SIZE: usize = 32;

    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut b = [0u8; Self::SIZE];
        b[0..4].copy_from_slice(&SEGMENT_MAGIC);
        b[4] = crate::CONTAINER_VERSION;
        b[5] = 0; // flags, reserved
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

        let magic: [u8; 4] = raw[0..4].try_into().expect("a 4-byte slice");
        if magic != SEGMENT_MAGIC {
            return Err(Error::BadMagic {
                expected: SEGMENT_MAGIC,
                actual: magic,
            });
        }

        let expected_crc = u32::from_le_bytes(raw[28..32].try_into().expect("a 4-byte slice"));
        let actual_crc = crc32c::crc32c(&raw[..28]);
        if expected_crc != actual_crc {
            return Err(Error::CrcMismatch {
                expected: expected_crc,
                actual: actual_crc,
            });
        }

        // The CRC is checked before the version: otherwise a garbage version
        // byte would pass for "a format from the future", masking corruption.
        if raw[4] != crate::CONTAINER_VERSION {
            return Err(Error::UnsupportedContainerVersion(
                raw[4],
                crate::CONTAINER_VERSION,
            ));
        }
        if raw[5] != 0 {
            return Err(Error::ReservedValue);
        }

        Ok(Self {
            protocol_version: ProtocolVersion(u16::from_le_bytes([raw[6], raw[7]])),
            boot: BootCounter(u32::from_le_bytes(
                raw[8..12].try_into().expect("a 4-byte slice"),
            )),
            base: Micros(u64::from_le_bytes(
                raw[12..20].try_into().expect("an 8-byte slice"),
            )),
            store_id: u64::from_le_bytes(raw[20..28].try_into().expect("an 8-byte slice")),
        })
    }

    /// The file name of this segment.
    pub fn file_name(&self) -> SegmentName {
        SegmentName {
            boot: self.boot,
            base: self.base,
        }
    }
}

/// A segment file name: `<boot:08x>-<micros:016x>.seg`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SegmentName {
    pub boot: BootCounter,
    pub base: Micros,
}

/// The extension of segment files.
pub const SEGMENT_EXT: &str = "seg";

impl SegmentName {
    pub fn new(boot: BootCounter, base: Micros) -> Self {
        Self { boot, base }
    }

    /// The moment of the segment's first record as a single value.
    ///
    /// A segment name is a [`crate::BootTime`] written in fixed-width
    /// hexadecimal, so comparing names and comparing moments are the same
    /// operation.
    pub const fn start(&self) -> crate::BootTime {
        crate::BootTime::new(self.boot, self.base)
    }

    /// Parse a file name. `None` means the file is not ours (foreign litter in
    /// the directory).
    ///
    /// Exactly what `Display` produces is accepted and nothing beyond it: a
    /// strict width, lowercase hex digits only. `from_str_radix` on its own
    /// swallows a sign (`+000002a`), and uppercase letters give a second
    /// spelling of the same number — one segment would then be accounted for
    /// twice under different names while a single file sits on disk.
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

        // Corruption of a payload field is caught by the CRC.
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
        // Version 1 is the previous layout, where a sample referred to a local
        // series number and a separate record tied that number to a metric.
        // This build cannot read such a segment, and "try to parse it anyway"
        // is not an option either: the bytes would pass the CRC and yield the
        // wrong metrics.
        for other in [1u8, 3, 255] {
            let mut bytes = header().to_bytes();
            bytes[4] = other;
            // The CRC is recomputed — an honestly written segment of that
            // version.
            let crc = crc32c::crc32c(&bytes[..28]);
            bytes[28..32].copy_from_slice(&crc.to_le_bytes());
            assert_eq!(
                SegmentHeader::parse(&bytes),
                Err(Error::UnsupportedContainerVersion(
                    other,
                    crate::CONTAINER_VERSION
                )),
                "container version {other}"
            );
        }
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

        // Lexicographic order of names equals chronological order.
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
        assert_eq!(parsed, expected, "string order matches (boot, time) order");
    }

    #[test]
    fn name_parse_rejects_junk() {
        assert_eq!(SegmentName::parse("readme.txt"), None);
        assert_eq!(SegmentName::parse("0000002a.seg"), None);
        // A loose width would break the sorting, so it is rejected.
        assert_eq!(SegmentName::parse("2a-3b9aca00.seg"), None);
        assert_eq!(SegmentName::parse("0000002a-000000003b9aca0.seg"), None);
        assert_eq!(SegmentName::parse("zzzzzzzz-000000003b9aca00.seg"), None);
    }

    #[test]
    fn parse_accepts_only_what_display_produces() {
        // `from_str_radix` would parse every one of these names, giving the
        // segment a second spelling: the inventory would hold two entries while
        // the disk holds one file.
        for junk in [
            "+000002a-000000003b9aca00.seg", // a sign instead of a digit
            "0000002A-000000003b9aca00.seg", // uppercase
            "0000002a-000000003B9ACA00.seg", // uppercase in the second half
            "0000002a-+00000003b9aca00.seg", // a sign in the second half
            "0000002a-000000003b9aca00.SEG", // the wrong extension
            " 000002a-000000003b9aca00.seg", // a space
        ] {
            assert_eq!(
                SegmentName::parse(junk),
                None,
                "{junk:?} is not produced by Display and must not parse"
            );
        }

        // And what Display produced must parse back.
        for (boot, base) in [(0u32, 0u64), (u32::MAX, u64::MAX), (0xabcdef, 0x123456789)] {
            let n = SegmentName::new(BootCounter(boot), Micros(base));
            assert_eq!(SegmentName::parse(&n.to_string()), Some(n));
        }
    }
}
