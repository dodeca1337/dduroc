//! Ordered preferences for selecting a schema's message language.
use dduroc_engine::schema::Schema;

/// At most this many language codes may be supplied in one preference list.
const MAX_LANGUAGE_PREFERENCES: usize = 8;
/// Maximum size in bytes of one language code.
const MAX_LANGUAGE_CODE_BYTES: usize = 64;

/// An invalid language preference list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidLanguagePreferences;
impl std::fmt::Display for InvalidLanguagePreferences {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid language preferences")
    }
}
impl std::error::Error for InvalidLanguagePreferences {}

/// Ordered language codes. Selection does not infer a parent from a region.
/// Codes match case-insensitively, with `_` and `-` treated alike. If none
/// matches, the schema's first declared language is used. The default is `en`.
/// Empty lists, invalid codes and equivalent duplicates are rejected. Within
/// each preference an exact match takes precedence over an equivalent match;
/// preference order takes precedence over a later exact match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LanguagePreferences(Vec<String>);
impl LanguagePreferences {
    /// Maximum number of language codes in a list.
    pub const MAX_CODES: usize = MAX_LANGUAGE_PREFERENCES;
    /// Maximum size in bytes of one language code.
    pub const MAX_CODE_BYTES: usize = MAX_LANGUAGE_CODE_BYTES;
    /// Validate and retain the requested order.
    pub fn new<I, S>(codes: I) -> Result<Self, InvalidLanguagePreferences>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut values: Vec<String> = Vec::new();
        for code in codes {
            let code = code.into();
            if values.len() == MAX_LANGUAGE_PREFERENCES
                || code.is_empty()
                || code.len() > MAX_LANGUAGE_CODE_BYTES
                || !code
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
            {
                return Err(InvalidLanguagePreferences);
            }
            if values.iter().any(|previous| equivalent(previous, &code)) {
                return Err(InvalidLanguagePreferences);
            }
            values.push(code);
        }
        if values.is_empty() {
            return Err(InvalidLanguagePreferences);
        }
        Ok(Self(values))
    }
    /// The original codes in preference order.
    pub fn codes(&self) -> &[String] {
        &self.0
    }
    /// Return the chosen index, or `None` when the schema declares no languages.
    pub fn select(&self, schema: &Schema) -> Option<LanguageSelection> {
        for (preference_index, code) in self.0.iter().enumerate() {
            if let Some(index) = schema.languages.iter().position(|lang| lang.0 == code) {
                return Some(LanguageSelection {
                    language_index: index,
                    preference_index: Some(preference_index),
                });
            }
            if let Some(index) = schema
                .languages
                .iter()
                .position(|lang| equivalent(lang.0, code))
            {
                return Some(LanguageSelection {
                    language_index: index,
                    preference_index: Some(preference_index),
                });
            }
        }
        (!schema.languages.is_empty()).then_some(LanguageSelection {
            language_index: 0,
            preference_index: None,
        })
    }
}
/// The selected schema language and the matching preference, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LanguageSelection {
    /// Index into the schema language list.
    pub language_index: usize,
    /// Matching preference index, or `None` for first-language fallback.
    pub preference_index: Option<usize>,
}
fn equivalent(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes().zip(b.bytes()).all(|(a, b)| {
            let normalize = |c: u8| {
                if c == b'_' {
                    b'-'
                } else {
                    c.to_ascii_lowercase()
                }
            };
            normalize(a) == normalize(b)
        })
}
impl Default for LanguagePreferences {
    fn default() -> Self {
        Self(vec!["en".into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_bounded_codes() {
        assert!(LanguagePreferences::new(Vec::<String>::new()).is_err());
        assert!(LanguagePreferences::new([""]).is_err());
        assert!(LanguagePreferences::new(["ru RU"]).is_err());
        assert!(LanguagePreferences::new(vec!["en"; 9]).is_err());
        assert!(LanguagePreferences::new(["x".repeat(65)]).is_err());
        assert!(LanguagePreferences::new(["ru-RU", "en"]).is_ok());
        assert!(LanguagePreferences::new(["en-US", "EN_us"]).is_err());
        assert!(equivalent("RU_ru", "ru-RU"));
    }
}
