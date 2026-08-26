//! Metric value limits: the schema's defaults plus runtime overrides.
//!
//! # Why this is not written to disk
//!
//! A limit is a property of the **installation**, not of the measurement. One
//! and the same temperature is normal for one amplifier and critical for
//! another; the quantity itself is the same in both. Writing a limit into
//! every sample would mean paying bytes for a setting that changes without the
//! data changing — and, worse, the history would end up marked up by stale
//! thresholds nobody considers right any more.
//!
//! So limits live in memory and a value's severity is computed at read time.
//! The price is honest and worth knowing: **an offline viewer sees only the
//! schema's defaults**. Runtime overrides belong to the running process and
//! are not in a dump carried off for analysis.
//!
//! # Why at the namespace level
//!
//! A namespace is an instance of a microservice, that is, a particular piece
//! of hardware. The limits are set by the external system that determined what
//! is actually connected: `orc-radio-0` may drive a different amplifier model
//! than `orc-radio-1`, and process-wide limits would be wrong for both.

use crate::error::{Error, Result};
#[cfg(test)]
use crate::schema::Range;
use crate::schema::{MetricDesc, MetricKind, Schema, Severity, StateDesc, Thresholds};
use dduroc_format::MetricId;
use std::sync::RwLock;

/// An override of one metric's limits.
///
/// The fields are independent: one may set only the numeric bounds, only the
/// severity of states, or both. What is not set stays as in the schema.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricLimits {
    /// Numeric bounds. `None` keeps the ones declared in the schema.
    pub thresholds: Option<Thresholds>,
    /// The severity of individual state codes. Empty keeps the schema's.
    pub states: Vec<(u64, Severity)>,
}

impl MetricLimits {
    /// Numeric bounds only.
    pub fn numeric(thresholds: Thresholds) -> Self {
        Self {
            thresholds: Some(thresholds),
            states: Vec::new(),
        }
    }

    /// State severities only.
    pub fn states(states: impl IntoIterator<Item = (u64, Severity)>) -> Self {
        Self {
            thresholds: None,
            states: states.into_iter().collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.thresholds.is_none() && self.states.is_empty()
    }

    /// A value's severity, taking the override into account.
    fn severity_of(&self, desc: &MetricDesc, value: &dduroc_format::Value<'_>) -> Severity {
        use dduroc_format::Value;
        if !desc.states.is_empty() {
            let code = match value {
                Value::U64(v) => Some(*v),
                Value::I64(v) if *v >= 0 => Some(*v as u64),
                Value::Bool(b) => Some(u64::from(*b)),
                _ => None,
            };
            let Some(code) = code else {
                return Severity::Normal;
            };
            // An override takes precedence over the schema; a code neither of
            // them mentions is left without a diagnosis.
            if let Some((_, s)) = self.states.iter().find(|(c, _)| *c == code) {
                return *s;
            }
            return desc.state(code).map_or(Severity::Normal, |s| s.severity);
        }
        // Only the ranges are overridden; the schema's predicates keep applying
        // — they are different axes of one diagnosis.
        let thresholds = self.thresholds.unwrap_or(desc.thresholds);
        value
            .as_f64()
            .map_or(Severity::Normal, |v| desc.numeric_severity(&thresholds, v))
    }
}

/// A metric state with its effective severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStatus {
    pub code: u64,
    pub name: &'static str,
    pub severity: Severity,
}

/// A metric's limits as they stand now: the schema plus the overrides.
///
/// This is what is handed out — to application code and (in future) to the web
/// layer.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct EffectiveLimits {
    pub metric: MetricId,
    pub name: &'static str,
    pub unit: &'static str,
    pub kind: MetricKind,
    pub thresholds: Thresholds,
    /// The states in schema declaration order. Empty means not an enum.
    pub states: Vec<StateStatus>,
    /// Whether what is in effect differs from what the schema declared.
    pub overridden: bool,
    /// The diagnosis is taken over entirely by a runtime closure
    /// ([`crate::namespace::Namespace::set_severity_fn`]): the numbers above
    /// describe what it overrides, and bands must not be drawn from them.
    pub has_severity_fn: bool,
}

/// A closure that takes over a metric's diagnosis entirely.
///
/// It receives the value as a number (a state code as a number too) and answers
/// with a severity; it beats both the schema and the data overrides. This is
/// the hatch for rules the data cannot express: hysteresis, or a dependence on
/// captured context.
pub type SeverityFn = Box<dyn Fn(f64) -> Severity + Send + Sync>;

/// The limits of all a namespace's metrics.
///
/// Indexed by a metric's position in [`Schema::metrics`] rather than by a hash
/// table: there are between a handful and a hundred metrics, the position is
/// already found by binary search over the schema, and an extra hash table
/// here would cost more than the access itself.
#[derive(Default)]
pub struct LimitsRegistry {
    slots: RwLock<Vec<Option<MetricLimits>>>,
    /// Runtime closures live apart from the data: they have a different life
    /// cycle (they can be neither compared nor shown) and a different priority.
    fns: RwLock<Vec<Option<SeverityFn>>>,
}

impl std::fmt::Debug for LimitsRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let slots = self.slots.read().unwrap_or_else(|e| e.into_inner());
        let fns = self.fns.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("LimitsRegistry")
            .field("overrides", &slots.iter().flatten().count())
            .field("fns", &fns.iter().flatten().count())
            .finish()
    }
}

impl LimitsRegistry {
    /// A registry with not a single slot allocated.
    ///
    /// Slots appear on the first override (`set` grows the vector itself): the
    /// vast majority of namespaces have no overrides at all, and a dense
    /// `vec![None; metric_count]` would cost roughly a hundred bytes per metric
    /// — with a hundred metrics and the tens of thousands of namespaces
    /// claimed, that is hundreds of megabytes allocated for emptiness. Reading
    /// a non-existent index already answers "no override".
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the limits. `None` removes the override.
    ///
    /// It checks that they make sense: numeric bounds on an enum metric, and a
    /// severity for a code the schema does not have, are refused. Swallowing
    /// such a setting silently is not an option — the operator would believe
    /// the limit was in force.
    pub fn set(
        &self,
        schema: &Schema,
        metric: MetricId,
        limits: Option<MetricLimits>,
    ) -> Result<()> {
        let (index, desc) = resolve(schema, metric)?;

        if let Some(l) = &limits {
            if l.thresholds.is_some_and(|t| !t.is_unset()) && !desc.states.is_empty() {
                return Err(Error::BadLimits {
                    metric_name: desc.name,
                    reason: "numeric bounds on an enum metric: its values are not \
                             ordered, set the severity of the states instead",
                });
            }
            if !l.states.is_empty() && desc.states.is_empty() {
                return Err(Error::BadLimits {
                    metric_name: desc.name,
                    reason: "state severities on a metric that is not declared as \
                             an enum",
                });
            }
            for (code, _) in &l.states {
                if desc.state(*code).is_none() {
                    return Err(Error::BadLimits {
                        metric_name: desc.name,
                        reason: "the severity of a state code the schema does not \
                                 have: there would be nothing to label it with",
                    });
                }
            }
            if let Some(t) = l.thresholds {
                check_nesting(desc.name, &t)?;
            }
        }

        let mut slots = self.slots.write().unwrap_or_else(|e| e.into_inner());
        // A namespace has one schema and it does not change, but guarding is
        // cheaper than panicking on an index.
        if slots.len() < schema.metrics.len() {
            slots.resize(schema.metrics.len(), None);
        }
        slots[index] = limits.filter(|l| !l.is_empty());
        Ok(())
    }

    /// Set the runtime diagnosis closure. `None` removes it.
    ///
    /// A blob is refused: it cannot be reduced to a number, so the closure
    /// would never be called — accepting such a setting silently would be lying
    /// to the operator.
    pub fn set_fn(
        &self,
        schema: &Schema,
        metric: MetricId,
        check: Option<SeverityFn>,
    ) -> Result<()> {
        let (index, desc) = resolve(schema, metric)?;
        if check.is_some() && desc.value_type == dduroc_format::ValueType::Blob {
            return Err(Error::BadLimits {
                metric_name: desc.name,
                reason: "a diagnosis closure on a blob metric: the value cannot be \
                         reduced to a number, so there would be nothing to call it with",
            });
        }
        let mut fns = self.fns.write().unwrap_or_else(|e| e.into_inner());
        if fns.len() < schema.metrics.len() {
            fns.resize_with(schema.metrics.len(), || None);
        }
        fns[index] = check;
        Ok(())
    }

    /// A metric's limits as they stand.
    pub fn effective(&self, schema: &Schema, metric: MetricId) -> Result<EffectiveLimits> {
        let (index, desc) = resolve(schema, metric)?;
        let slots = self.slots.read().unwrap_or_else(|e| e.into_inner());
        let over = slots.get(index).and_then(|o| o.as_ref());
        let has_severity_fn = self
            .fns
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(index)
            .is_some_and(Option::is_some);

        Ok(EffectiveLimits {
            has_severity_fn,
            metric: desc.id,
            name: desc.name,
            unit: desc.unit,
            kind: desc.kind,
            thresholds: over.and_then(|o| o.thresholds).unwrap_or(desc.thresholds),
            states: desc
                .states
                .iter()
                .map(|s: &StateDesc| StateStatus {
                    code: s.code,
                    name: s.name,
                    severity: over
                        .and_then(|o| o.states.iter().find(|(c, _)| *c == s.code))
                        .map_or(s.severity, |(_, sev)| *sev),
                })
                .collect(),
            overridden: over.is_some(),
        })
    }

    /// A value's severity, taking the overrides into account.
    ///
    /// A hot path only for whoever decided to check values on write; writing
    /// itself does not touch the limits.
    pub fn severity_of(
        &self,
        schema: &Schema,
        metric: MetricId,
        value: &dduroc_format::Value<'_>,
    ) -> Severity {
        let Ok((index, desc)) = resolve(schema, metric) else {
            return Severity::Normal;
        };
        // The closure beats everything: taking the diagnosis over entirely is
        // the whole reason it is set.
        {
            let fns = self.fns.read().unwrap_or_else(|e| e.into_inner());
            if let Some(check) = fns.get(index).and_then(|o| o.as_ref())
                && let Some(v) = value.as_f64()
            {
                return check(v);
            }
        }
        let slots = self.slots.read().unwrap_or_else(|e| e.into_inner());
        match slots.get(index).and_then(|o| o.as_ref()) {
            Some(over) => over.severity_of(desc, value),
            None => desc.severity_of(value),
        }
    }
}

fn resolve(schema: &Schema, metric: MetricId) -> Result<(usize, &'static MetricDesc)> {
    schema.metric_index(metric).ok_or(Error::UnknownMetric {
        metric_id: metric.0,
    })
}

/// `alarm` has to contain `warn`: a quantity first leaves what is normal and
/// only then what is permissible.
pub(crate) fn check_nesting(metric_name: &'static str, t: &Thresholds) -> Result<()> {
    let bad_low = matches!((t.warn.min, t.alarm.min), (Some(w), Some(c)) if c > w);
    let bad_high = matches!((t.warn.max, t.alarm.max), (Some(w), Some(c)) if c < w);
    if bad_low || bad_high {
        return Err(Error::BadLimits {
            metric_name,
            reason: "the alarm range has to contain the warning one: otherwise \
                     a value would be alarming without being a warning",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{Language, StorageClass};
    use dduroc_format::{ProtocolVersion, Value, ValueType};

    static LANGS: &[Language] = &[Language("en")];

    static STATES: &[StateDesc] = &[
        StateDesc {
            code: 0,
            name: "Los",
            severity: Severity::Alarm,
        },
        StateDesc {
            code: 1,
            name: "Sync",
            severity: Severity::Warn,
        },
        StateDesc {
            code: 2,
            name: "Lock",
            severity: Severity::Normal,
        },
    ];

    static METRICS: &[MetricDesc] = &[
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "temp",
            value_type: ValueType::F32,
            class: StorageClass::Telemetry,
            unit: "°C",
            tags: &["thermal"],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: Thresholds {
                warn: Range {
                    min: Some(0.0),
                    max: Some(70.0),
                },
                alarm: Range {
                    min: Some(-10.0),
                    max: Some(85.0),
                },
            },
        },
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(2),
            name: "link",
            value_type: ValueType::U64,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind: MetricKind::State,
            states: STATES,
            thresholds: Thresholds::NONE,
        },
    ];

    fn schema() -> Schema {
        Schema {
            name: "radio",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: &[],
            metrics: METRICS,
            spans: &[],
            migrations: &[],
        }
    }

    fn registry() -> LimitsRegistry {
        LimitsRegistry::new()
    }

    #[test]
    fn schema_defaults_classify_numeric_values() {
        let (s, r) = (schema(), registry());
        let sev = |v: f32| r.severity_of(&s, MetricId(1), &Value::F32(v));
        assert_eq!(sev(36.6), Severity::Normal);
        assert_eq!(sev(0.0), Severity::Normal, "the bound is inclusive");
        assert_eq!(sev(70.0), Severity::Normal);
        assert_eq!(sev(71.0), Severity::Warn);
        assert_eq!(sev(-5.0), Severity::Warn, "below normal is a warning too");
        assert_eq!(sev(90.0), Severity::Alarm);
        assert_eq!(sev(-50.0), Severity::Alarm);
        assert_eq!(
            sev(f32::NAN),
            Severity::Alarm,
            "an unknown value cannot count as normal"
        );
    }

    #[test]
    fn state_severity_comes_from_the_state_not_from_ranges() {
        let (s, r) = (schema(), registry());
        let sev = |code: u64| r.severity_of(&s, MetricId(2), &Value::U64(code));
        assert_eq!(sev(0), Severity::Alarm, "Los");
        assert_eq!(sev(1), Severity::Warn, "Sync");
        assert_eq!(sev(2), Severity::Normal, "Lock");
        assert_eq!(
            sev(99),
            Severity::Normal,
            "an unfamiliar code has nothing to match against, so nothing to warn about"
        );
    }

    #[test]
    fn runtime_override_wins_over_schema() {
        // The external system determined the amplifier model and narrowed the
        // limits.
        let (s, r) = (schema(), registry());
        r.set(
            &s,
            MetricId(1),
            Some(MetricLimits::numeric(Thresholds {
                warn: Range {
                    min: None,
                    max: Some(40.0),
                },
                alarm: Range {
                    min: None,
                    max: Some(50.0),
                },
            })),
        )
        .unwrap();

        let sev = |v: f32| r.severity_of(&s, MetricId(1), &Value::F32(v));
        assert_eq!(sev(36.6), Severity::Normal);
        assert_eq!(
            sev(45.0),
            Severity::Warn,
            "the schema would have said Normal"
        );
        assert_eq!(sev(60.0), Severity::Alarm);

        let eff = r.effective(&s, MetricId(1)).unwrap();
        assert!(eff.overridden);
        assert_eq!(eff.thresholds.warn.max, Some(40.0));
        assert_eq!(eff.unit, "°C", "the metric's statics are untouched");

        // Removing it brings the schema's back.
        r.set(&s, MetricId(1), None).unwrap();
        assert_eq!(sev(45.0), Severity::Normal);
        assert!(!r.effective(&s, MetricId(1)).unwrap().overridden);
    }

    #[test]
    fn state_severity_can_be_reassigned_at_runtime() {
        // On this installation a loss of sync is tolerable while Lock is
        // mandatory.
        let (s, r) = (schema(), registry());
        r.set(
            &s,
            MetricId(2),
            Some(MetricLimits::states([(1, Severity::Normal)])),
        )
        .unwrap();

        assert_eq!(
            r.severity_of(&s, MetricId(2), &Value::U64(1)),
            Severity::Normal
        );
        assert_eq!(
            r.severity_of(&s, MetricId(2), &Value::U64(0)),
            Severity::Alarm,
            "states that were not overridden stay as the schema has them"
        );

        let eff = r.effective(&s, MetricId(2)).unwrap();
        assert_eq!(eff.states.len(), 3);
        assert_eq!(eff.states[1].name, "Sync");
        assert_eq!(eff.states[1].severity, Severity::Normal);
        assert_eq!(eff.states[0].severity, Severity::Alarm);
        assert_eq!(eff.kind, MetricKind::State);
    }

    #[test]
    fn nonsensical_limits_are_refused_not_ignored() {
        // Swallowing a setting silently is worse than refusing it: the operator
        // would believe the limit was in force.
        let (s, r) = (schema(), registry());

        let err = r
            .set(
                &s,
                MetricId(2),
                Some(MetricLimits::numeric(Thresholds {
                    warn: Range {
                        min: Some(0.0),
                        max: Some(1.0),
                    },
                    alarm: Range::NONE,
                })),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "got {err}");

        let err = r
            .set(
                &s,
                MetricId(1),
                Some(MetricLimits::states([(0, Severity::Warn)])),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "got {err}");

        let err = r
            .set(
                &s,
                MetricId(2),
                Some(MetricLimits::states([(42, Severity::Warn)])),
            )
            .unwrap_err();
        assert!(
            matches!(err, Error::BadLimits { .. }),
            "a code outside the schema"
        );

        // A critical range that does not contain the warning one.
        let err = r
            .set(
                &s,
                MetricId(1),
                Some(MetricLimits::numeric(Thresholds {
                    warn: Range {
                        min: None,
                        max: Some(80.0),
                    },
                    alarm: Range {
                        min: None,
                        max: Some(50.0),
                    },
                })),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "nesting");

        let err = r.set(&s, MetricId(99), None).unwrap_err();
        assert!(
            matches!(err, Error::UnknownMetric { metric_id: 99 }),
            "{err}"
        );
    }

    #[test]
    fn schema_predicates_join_ranges_by_the_heavier_diagnosis() {
        // A shape a normal range cannot express: VSWR is critical both above
        // and below one, but only alarming above. A predicate is a trigger
        // condition, with the polarity opposite to a range.
        fn warn_hit(v: f64) -> bool {
            v > 1.5
        }
        #[allow(clippy::manual_range_contains)]
        fn alarm_hit(v: f64) -> bool {
            v > 3.0 || v < 1.0
        }
        static VSWR: &[MetricDesc] = &[MetricDesc {
            warn_if: Some(warn_hit),
            alarm_if: Some(alarm_hit),
            id: MetricId(1),
            name: "vswr",
            value_type: ValueType::F32,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: Thresholds::NONE,
        }];
        let s = Schema {
            metrics: VSWR,
            ..schema()
        };
        let r = registry();
        let sev = |v: f32| r.severity_of(&s, MetricId(1), &Value::F32(v));
        assert_eq!(sev(1.2), Severity::Normal);
        assert_eq!(sev(2.0), Severity::Warn);
        assert_eq!(sev(3.5), Severity::Alarm);
        assert_eq!(
            sev(0.5),
            Severity::Alarm,
            "triggering from below is the whole point"
        );

        // A runtime data override replaces the ranges, but the schema's
        // predicates keep applying: they are different axes of one diagnosis.
        r.set(
            &s,
            MetricId(1),
            Some(MetricLimits::numeric(Thresholds {
                warn: Range {
                    min: Some(1.05),
                    max: None,
                },
                alarm: Range::NONE,
            })),
        )
        .unwrap();
        assert_eq!(sev(1.02), Severity::Warn, "the new range is in force");
        assert_eq!(
            sev(0.5),
            Severity::Alarm,
            "the schema predicate is not cancelled"
        );
    }

    #[test]
    fn severity_fn_takes_the_diagnosis_over_entirely() {
        let (s, r) = (schema(), registry());
        // Context that is in neither the schema nor the data: a limit from the
        // hardware model plus a latched state after the first fault
        // (hysteresis).
        let latched = std::sync::atomic::AtomicBool::new(false);
        let hw_max = 42.0;
        r.set_fn(
            &s,
            MetricId(1),
            Some(Box::new(move |v| {
                use std::sync::atomic::Ordering;
                if v > hw_max {
                    latched.store(true, Ordering::Relaxed);
                }
                if latched.load(Ordering::Relaxed) {
                    Severity::Alarm
                } else {
                    Severity::Normal
                }
            })),
        )
        .unwrap();

        let sev = |v: f32| r.severity_of(&s, MetricId(1), &Value::F32(v));
        assert_eq!(
            sev(90.0),
            Severity::Alarm,
            "the schema's 85 is no longer relevant"
        );
        assert_eq!(
            sev(20.0),
            Severity::Alarm,
            "the closure remembers the context: the fault is latched"
        );
        assert!(r.effective(&s, MetricId(1)).unwrap().has_severity_fn);

        // A state is available to the closure too: the code arrives as a
        // number.
        r.set_fn(
            &s,
            MetricId(2),
            Some(Box::new(|code| {
                if code == 0.0 {
                    Severity::Warn
                } else {
                    Severity::Normal
                }
            })),
        )
        .unwrap();
        assert_eq!(
            r.severity_of(&s, MetricId(2), &Value::U64(0)),
            Severity::Warn,
            "the schema treated Los as a fault"
        );

        // Removing it restores the usual order: the data and the schema.
        r.set_fn(&s, MetricId(1), None).unwrap();
        assert_eq!(sev(90.0), Severity::Alarm, "now by the schema ranges");
        assert_eq!(sev(20.0), Severity::Normal);
        assert!(!r.effective(&s, MetricId(1)).unwrap().has_severity_fn);
    }

    #[test]
    fn severity_fn_on_a_blob_metric_is_refused() {
        static BLOBBY: &[MetricDesc] = &[MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "spectrum",
            value_type: ValueType::Blob,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: Thresholds::NONE,
        }];
        let s = Schema {
            metrics: BLOBBY,
            ..schema()
        };
        let r = registry();
        let err = r
            .set_fn(&s, MetricId(1), Some(Box::new(|_| Severity::Alarm)))
            .unwrap_err();
        assert!(
            matches!(err, Error::BadLimits { .. }),
            "a blob cannot be reduced to a number, so the closure would never be called: {err}"
        );
    }

    #[test]
    fn metric_without_limits_is_always_normal() {
        static PLAIN: &[MetricDesc] = &[MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "count",
            value_type: ValueType::U64,
            class: StorageClass::Telemetry,
            unit: "",
            tags: &[],
            kind: MetricKind::Counter,
            states: &[],
            thresholds: Thresholds::NONE,
        }];
        let s = Schema {
            metrics: PLAIN,
            ..schema()
        };
        let r = LimitsRegistry::new();
        for v in [0u64, 1, u64::MAX] {
            assert_eq!(
                r.severity_of(&s, MetricId(1), &Value::U64(v)),
                Severity::Normal
            );
        }
        assert!(!r.effective(&s, MetricId(1)).unwrap().overridden);
    }
}
