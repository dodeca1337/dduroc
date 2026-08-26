//! Parsing message templates.
//!
//! A template is written with named placeholders (`"мощность {dbm} дБм"`) and
//! expands into a positional `format!`: the field names are checked at compile
//! time and the runtime does no lookup by name.
//!
//! Format specifiers are preserved: `{dbm:.1}` becomes `{:.1}`.

/// A template's placeholder names in order of appearance.
pub fn placeholders(template: &str) -> Vec<String> {
    let mut out = Vec::new();
    scan(template, |name, _| out.push(name.to_owned()));
    out
}

/// Rewrite a template into positional form.
///
/// Returns the format string and the order of the fields to substitute.
pub fn rewrite(template: &str) -> (String, Vec<String>) {
    let mut fmt = String::with_capacity(template.len());
    let mut order = Vec::new();
    let bytes = template.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        match bytes[i] {
            b'{' if i + 1 < bytes.len() && bytes[i + 1] == b'{' => {
                fmt.push_str("{{");
                i += 2;
            }
            b'}' if i + 1 < bytes.len() && bytes[i + 1] == b'}' => {
                fmt.push_str("}}");
                i += 2;
            }
            b'{' => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j] != b'}' && bytes[j] != b':' {
                    j += 1;
                }
                // The segment boundaries are ASCII separators, so a slice
                // always lands on a character boundary: non-ASCII templates do
                // not break.
                order.push(template[start..j].to_owned());
                fmt.push('{');
                if j < bytes.len() && bytes[j] == b':' {
                    while j < bytes.len() && bytes[j] != b'}' {
                        fmt.push(bytes[j] as char);
                        j += 1;
                    }
                }
                fmt.push('}');
                i = if j < bytes.len() { j + 1 } else { j };
            }
            _ => {
                // The byte is copied as it is: multi-byte characters pass
                // straight through, because non-ASCII is never interpreted
                // here.
                let ch_len = utf8_len(bytes[i]);
                fmt.push_str(&template[i..i + ch_len]);
                i += ch_len;
            }
        }
    }
    (fmt, order)
}

fn scan(template: &str, mut on_placeholder: impl FnMut(&str, Option<&str>)) {
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'{' if i + 1 < bytes.len() && bytes[i + 1] == b'{' => i += 2,
            b'}' if i + 1 < bytes.len() && bytes[i + 1] == b'}' => i += 2,
            b'{' => {
                let start = i + 1;
                let mut j = start;
                while j < bytes.len() && bytes[j] != b'}' && bytes[j] != b':' {
                    j += 1;
                }
                let name = &template[start..j];
                let spec_start = j;
                while j < bytes.len() && bytes[j] != b'}' {
                    j += 1;
                }
                let spec = (spec_start < j).then(|| &template[spec_start..j]);
                on_placeholder(name, spec);
                i = if j < bytes.len() { j + 1 } else { j };
            }
            _ => i += utf8_len(bytes[i]),
        }
    }
}

/// The length of a UTF-8 character from its first byte.
const fn utf8_len(first: u8) -> usize {
    match first {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_names() {
        assert_eq!(placeholders("нет плейсхолдеров"), Vec::<String>::new());
        assert_eq!(placeholders("power {dbm} dBm"), vec!["dbm"]);
        assert_eq!(
            placeholders("{a} и {b:.2} и снова {a}"),
            vec!["a", "b", "a"]
        );
        assert_eq!(placeholders("{{экранировано}}"), Vec::<String>::new());
    }

    #[test]
    fn rewrites_to_positional() {
        let (fmt, order) = rewrite("power {dbm} dBm");
        assert_eq!(fmt, "power {} dBm");
        assert_eq!(order, vec!["dbm"]);

        let (fmt, order) = rewrite("{a} + {b} = {c}");
        assert_eq!(fmt, "{} + {} = {}");
        assert_eq!(order, vec!["a", "b", "c"]);
    }

    #[test]
    fn preserves_format_spec() {
        let (fmt, order) = rewrite("мощность {dbm:.1} дБм");
        assert_eq!(fmt, "мощность {:.1} дБм");
        assert_eq!(order, vec!["dbm"]);

        let (fmt, _) = rewrite("{v:>8}");
        assert_eq!(fmt, "{:>8}");
    }

    #[test]
    fn cyrillic_text_survives() {
        // Casting byte by byte to char would turn every non-ASCII byte into its
        // own latin-1 character — exactly the bug the prototype had.
        let (fmt, order) = rewrite("температура {t} °C, канал {ch}");
        assert_eq!(fmt, "температура {} °C, канал {}");
        assert_eq!(order, vec!["t", "ch"]);
    }

    #[test]
    fn escaped_braces_pass_through() {
        let (fmt, order) = rewrite("{{не плейсхолдер}} и {real}");
        assert_eq!(fmt, "{{не плейсхолдер}} и {}");
        assert_eq!(order, vec!["real"]);
    }

    #[test]
    fn unterminated_placeholder_does_not_panic() {
        let (fmt, order) = rewrite("оборвано {name");
        assert_eq!(order, vec!["name"]);
        assert_eq!(fmt, "оборвано {}");
    }
}
