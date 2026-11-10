//! Validated requests. Validation happens before work is admitted.
use crate::error::ServiceError;
use crate::types::{Class, Level, UInt64};
use async_graphql::{Enum, InputObject};
use dduroc_read::{
    BootCounter, BootTime, Filter, KindFilter, Micros, NsSelect, Query, ReadLimits, Timestamp,
};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct InputLimits {
    pub max_items: usize,
    pub max_string_bytes: usize,
    pub max_total_string_bytes: usize,
}
impl Default for InputLimits {
    fn default() -> Self {
        Self {
            max_items: 256,
            max_string_bytes: 512,
            max_total_string_bytes: 64 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, Enum, Eq, PartialEq, Default)]
pub enum Order {
    Oldest,
    #[default]
    Newest,
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct KindsInput {
    pub messages: Option<bool>,
    pub text: Option<bool>,
    pub samples: Option<bool>,
    pub spans: Option<bool>,
}

/// Exactly one clock scale must be supplied. UTC is an RFC3339 timestamp.
#[derive(Debug, Clone, Default, InputObject)]
pub struct TimeInput {
    pub boot: Option<UInt64>,
    pub micros: Option<UInt64>,
    pub utc: Option<String>,
}
impl TimeInput {
    fn native(&self) -> Result<Timestamp, ServiceError> {
        match (&self.utc, self.boot, self.micros) {
            (Some(utc), None, None) if utc.len() <= 128 => {
                dduroc_read::chrono::DateTime::parse_from_rfc3339(utc)
                    .map(|time| Timestamp::Utc(time.to_utc()))
                    .map_err(|_| invalid("invalid RFC3339 timestamp"))
            }
            (None, Some(boot), Some(micros)) => Ok(Timestamp::Boot(BootTime::new(
                BootCounter(as_u32(boot, "boot")?),
                Micros(micros.0),
            ))),
            _ => Err(invalid("time requires UTC or both boot and micros")),
        }
    }
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct FilterInput {
    pub min_level: Option<Level>,
    pub any_tags: Option<Vec<String>>,
    pub event_ids: Option<Vec<UInt64>>,
    pub event_names: Option<Vec<String>>,
    pub span_ids: Option<Vec<UInt64>>,
    pub kinds: Option<KindsInput>,
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct QueryInput {
    /// Exact names. Omit names and prefix to select all namespaces.
    pub namespaces: Option<Vec<String>>,
    pub namespace_prefix: Option<String>,
    pub classes: Option<Vec<Class>>,
    pub boot: Option<UInt64>,
    pub from: Option<TimeInput>,
    pub to: Option<TimeInput>,
    pub filter: Option<FilterInput>,
    pub order: Option<Order>,
    pub limit: Option<UInt64>,
    pub seed_states: Option<bool>,
}

/// Catalog selection has no event/time window: it describes channels, not records.
#[derive(Debug, Clone, Default, InputObject)]
pub struct CatalogInput {
    pub namespaces: Option<Vec<String>>,
    pub namespace_prefix: Option<String>,
    pub classes: Option<Vec<Class>>,
}
impl CatalogInput {
    pub fn native(&self, limits: &InputLimits) -> Result<Query, ServiceError> {
        QueryInput {
            namespaces: self.namespaces.clone(),
            namespace_prefix: self.namespace_prefix.clone(),
            classes: self.classes.clone(),
            ..QueryInput::default()
        }
        .native(limits, false)
    }
}

fn invalid(message: &str) -> ServiceError {
    ServiceError::InvalidInput(message.to_owned())
}
fn as_u32(value: UInt64, field: &str) -> Result<u32, ServiceError> {
    u32::try_from(value.0).map_err(|_| invalid(&format!("{field} exceeds u32")))
}
fn as_usize(value: UInt64, field: &str) -> Result<usize, ServiceError> {
    usize::try_from(value.0).map_err(|_| invalid(&format!("{field} exceeds platform capacity")))
}

impl QueryInput {
    pub fn native(&self, limits: &InputLimits, follow: bool) -> Result<Query, ServiceError> {
        let filter = self.filter.clone().unwrap_or_default();
        let lists = [
            self.namespaces.as_deref().unwrap_or_default(),
            filter.any_tags.as_deref().unwrap_or_default(),
            filter.event_names.as_deref().unwrap_or_default(),
        ];
        let strings = lists
            .iter()
            .flat_map(|items| items.iter())
            .chain(self.namespace_prefix.iter());
        let mut total = 0usize;
        for text in strings {
            total = total
                .checked_add(text.len())
                .ok_or_else(|| invalid("filter text overflow"))?;
            if text.is_empty()
                || text.len() > limits.max_string_bytes
                || total > limits.max_total_string_bytes
            {
                return Err(invalid("filter text exceeds limits"));
            }
        }
        let count = lists.iter().map(|list| list.len()).sum::<usize>()
            + filter.event_ids.as_ref().map_or(0, Vec::len)
            + filter.span_ids.as_ref().map_or(0, Vec::len)
            + self.classes.as_ref().map_or(0, Vec::len);
        if count > limits.max_items {
            return Err(invalid("too many filter items"));
        }
        if self.namespaces.is_some() && self.namespace_prefix.is_some() {
            return Err(invalid(
                "namespaces and namespacePrefix are mutually exclusive",
            ));
        }
        let order = self
            .order
            .unwrap_or(if follow { Order::Oldest } else { Order::Newest });
        if follow && (self.to.is_some() || order == Order::Newest) {
            return Err(invalid(
                "follow requires oldest order without an upper time bound",
            ));
        }
        let from = self.from.as_ref().map(TimeInput::native).transpose()?;
        let to = self.to.as_ref().map(TimeInput::native).transpose()?;
        match (from, to) {
            (Some(Timestamp::Boot(a)), Some(Timestamp::Boot(b))) if a > b => {
                return Err(invalid("from exceeds to"));
            }
            (Some(Timestamp::Utc(a)), Some(Timestamp::Utc(b))) if a > b => {
                return Err(invalid("from exceeds to"));
            }
            (Some(Timestamp::Boot(_)), Some(Timestamp::Utc(_)))
            | (Some(Timestamp::Utc(_)), Some(Timestamp::Boot(_))) => {
                return Err(invalid("time bounds must use the same clock scale"));
            }
            _ => {}
        }
        let event_ids = filter
            .event_ids
            .as_ref()
            .map(|ids| {
                ids.iter()
                    .map(|id| {
                        u16::try_from(id.0)
                            .map(dduroc_format::EventId)
                            .map_err(|_| invalid("event id exceeds u16"))
                    })
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        let spans = filter
            .span_ids
            .as_ref()
            .map(|ids| {
                ids.iter()
                    .map(|id| as_u32(*id, "span id").map(dduroc_format::SpanId))
                    .collect::<Result<HashSet<_>, _>>()
            })
            .transpose()?;
        let kinds = filter.kinds.unwrap_or_default();
        Ok(Query {
            namespaces: match (&self.namespaces, &self.namespace_prefix) {
                (Some(names), _) => NsSelect::Names(names.clone()),
                (_, Some(prefix)) => NsSelect::Group(prefix.clone()),
                _ => NsSelect::All,
            },
            channels: self
                .classes
                .as_ref()
                .map(|items| items.iter().copied().map(Into::into).collect())
                .unwrap_or_default(),
            boot: self
                .boot
                .map(|value| as_u32(value, "boot").map(BootCounter))
                .transpose()?,
            from,
            to,
            filter: Filter {
                min_level: filter.min_level.map(Into::into),
                any_tags: filter.any_tags.unwrap_or_default(),
                events: event_ids,
                event_names: filter.event_names.unwrap_or_default(),
                spans,
                kinds: KindFilter {
                    messages: kinds.messages.unwrap_or(true),
                    text: kinds.text.unwrap_or(true),
                    samples: kinds.samples.unwrap_or(true),
                    spans: kinds.spans.unwrap_or(true),
                },
            },
            order: match order {
                Order::Oldest => dduroc_read::Order::Oldest,
                Order::Newest => dduroc_read::Order::Newest,
            },
            limit: self
                .limit
                .map(|value| as_usize(value, "query limit"))
                .transpose()?,
            seed_states: self.seed_states.unwrap_or(false),
        })
    }
}

/// Every supplied ceiling is intersected with the service ceiling.
#[derive(Debug, Clone, Default, InputObject)]
pub struct LimitsInput {
    pub epoch_runs: Option<UInt64>,
    pub hardware_boots: Option<UInt64>,
    pub roster_entries: Option<UInt64>,
    pub channels: Option<UInt64>,
    pub blocks: Option<UInt64>,
    pub compressed_bytes: Option<UInt64>,
    pub decoded_bytes: Option<UInt64>,
    pub examined_records: Option<UInt64>,
    pub returned_bytes: Option<UInt64>,
    /// Cumulative deadline for the read, session or follow created by this call.
    /// It is measured from this call and is not reset when requesting another page.
    pub timeout_millis: Option<UInt64>,
}
impl LimitsInput {
    pub fn native(
        &self,
        server: &ReadLimits,
        max_duration: Duration,
    ) -> Result<ReadLimits, ServiceError> {
        let mut out = server.clone();
        macro_rules! count {
            ($field:ident) => {
                if let Some(value) = self.$field {
                    out.$field = out
                        .$field
                        .min(usize::try_from(value.0).unwrap_or(usize::MAX));
                }
            };
        }
        macro_rules! bytes {
            ($field:ident) => {
                if let Some(value) = self.$field {
                    out.$field = out.$field.min(value.0);
                }
            };
        }
        count!(epoch_runs);
        count!(hardware_boots);
        count!(roster_entries);
        count!(channels);
        count!(blocks);
        bytes!(compressed_bytes);
        bytes!(decoded_bytes);
        bytes!(examined_records);
        bytes!(returned_bytes);
        let timeout = self.timeout_millis.map_or(max_duration, |ms| {
            Duration::from_millis(ms.0).min(max_duration)
        });
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| invalid("deadline overflow"))?;
        out.deadline = Some(
            out.deadline
                .map_or(deadline, |existing| existing.min(deadline)),
        );
        Ok(out)
    }
}

#[derive(Debug, Clone, Default, InputObject)]
pub struct PageInput {
    pub max_records: Option<UInt64>,
    pub max_bytes: Option<UInt64>,
}
impl PageInput {
    pub fn native(
        &self,
        records: usize,
        bytes: usize,
    ) -> Result<dduroc_read::PageLimits, ServiceError> {
        let records = self
            .max_records
            .map(|value| as_usize(value, "page records"))
            .transpose()?
            .map_or(records, |n| records.min(n));
        let bytes = self
            .max_bytes
            .map(|value| as_usize(value, "page bytes"))
            .transpose()?
            .map_or(bytes, |n| bytes.min(n));
        if records == 0 || bytes == 0 {
            return Err(invalid("page limits must be positive"));
        }
        dduroc_read::PageLimits::new(records, bytes).map_err(ServiceError::Native)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn client_limits_never_expand_server_work_or_deadline() {
        let server = ReadLimits {
            channels: 2,
            blocks: 3,
            decoded_bytes: 512,
            deadline: Some(Instant::now()),
            ..ReadLimits::default()
        };
        let request = LimitsInput {
            channels: Some(UInt64(u64::MAX)),
            blocks: Some(UInt64(1)),
            decoded_bytes: Some(UInt64(1024)),
            timeout_millis: Some(UInt64(u64::MAX)),
            ..LimitsInput::default()
        };
        let actual = request.native(&server, Duration::from_secs(1));
        let actual = actual.unwrap();
        assert_eq!(actual.channels, 2);
        assert_eq!(actual.blocks, 1);
        assert_eq!(actual.decoded_bytes, 512);
        assert!(actual.deadline <= server.deadline);
    }
    #[test]
    fn ambiguous_time_scales_and_out_of_domain_ids_are_refused() {
        let query = QueryInput {
            from: Some(TimeInput {
                boot: Some(UInt64(1)),
                micros: Some(UInt64(1)),
                utc: Some("2026-11-10T00:00:00Z".into()),
            }),
            ..QueryInput::default()
        };
        assert!(query.native(&InputLimits::default(), false).is_err());
        let query = QueryInput {
            boot: Some(UInt64(u64::from(u32::MAX) + 1)),
            ..QueryInput::default()
        };
        assert!(query.native(&InputLimits::default(), false).is_err());
        let query = QueryInput {
            filter: Some(FilterInput {
                event_ids: Some(vec![UInt64(65536)]),
                ..FilterInput::default()
            }),
            ..QueryInput::default()
        };
        assert!(query.native(&InputLimits::default(), false).is_err());
    }
}
