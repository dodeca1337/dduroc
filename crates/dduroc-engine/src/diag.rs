//! The engine's own notices in the record stream.
//!
//! A lost record is announced in the stream itself, as a text record with the
//! [`TARGET`] target: a hole nobody mentions is indistinguishable from
//! silence. The text of a notice is a contract between the writer and the
//! reader, and it is assembled and parsed HERE, in one place: application code
//! that parsed the prose by its prefix would break with every rewording.

/// The target of the engine's own records. It does not belong to an
/// application's free text: it is how a reader tells an engine notice from a
/// word that merely matched.
pub const TARGET: &str = "dduroc";

/// The text of a notice about `count` records lost to queue overflow.
pub fn drop_notice(count: u64) -> String {
    format!("records lost: {count} (the queue overflowed)")
}

/// Parse a loss notice back into a number.
///
/// `None` means this is not a loss notice (a foreign target, or other text).
pub fn parse_drop_notice(target: &str, text: &str) -> Option<u64> {
    if target != TARGET {
        return None;
    }
    text.strip_prefix("records lost: ")?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_notice_survives_the_round_trip() {
        // The format is assembled and parsed by one pair of functions, so there
        // is nothing for them to drift apart on — and that is all there is to
        // check here.
        assert_eq!(parse_drop_notice(TARGET, &drop_notice(42)), Some(42));
        assert_eq!(
            parse_drop_notice("app", &drop_notice(42)),
            None,
            "a foreign target is not an engine notice, whatever word happened to match"
        );
        assert_eq!(parse_drop_notice(TARGET, "everything lost"), None);
    }
}
