//! Пределы значений метрик: дефолты схемы плюс рантайм-переопределения.
//!
//! # Почему это не пишется на диск
//!
//! Предел — свойство **установки**, а не измерения. Одна и та же температура
//! нормальна для одного усилителя и аварийна для другого; сама величина при
//! этом одна и та же. Записывать предел в каждый отсчёт значило бы платить
//! байтами за настройку, которая меняется без изменения данных — и хуже того,
//! история оказалась бы размечена по устаревшим порогам, которые уже никто не
//! считает верными.
//!
//! Поэтому пределы живут в памяти, а важность значения вычисляется при
//! чтении. Цена честная и её надо знать: **офлайн-вьюер видит только дефолты
//! схемы**. Рантайм-переопределения принадлежат работающему процессу, и в
//! унесённом на анализ дампе их нет.
//!
//! # Почему на уровне неймспейса
//!
//! Неймспейс — это экземпляр микросервиса, то есть конкретное железо.
//! Пределы задаёт внешняя система, определившая, что именно подключено:
//! `orc-radio-0` может управлять другой моделью усилителя, чем `orc-radio-1`,
//! и общие на процесс пределы были бы неверны для обоих.

use crate::error::{Error, Result};
#[cfg(test)]
use crate::schema::Range;
use crate::schema::{MetricDesc, MetricKind, Schema, Severity, StateDesc, Thresholds};
use dduroc_format::MetricId;
use std::sync::RwLock;

/// Переопределение пределов одной метрики.
///
/// Поля независимы: можно задать только числовые границы, только важность
/// состояний, или и то и другое. Незаданное остаётся как в схеме.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricLimits {
    /// Числовые границы. `None` — оставить объявленные в схеме.
    pub thresholds: Option<Thresholds>,
    /// Важность отдельных кодов состояния. Пусто — оставить схемные.
    pub states: Vec<(u64, Severity)>,
}

impl MetricLimits {
    /// Только числовые границы.
    pub fn numeric(thresholds: Thresholds) -> Self {
        Self {
            thresholds: Some(thresholds),
            states: Vec::new(),
        }
    }

    /// Только важность состояний.
    pub fn states(states: impl IntoIterator<Item = (u64, Severity)>) -> Self {
        Self {
            thresholds: None,
            states: states.into_iter().collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.thresholds.is_none() && self.states.is_empty()
    }

    /// Важность значения с учётом переопределения.
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
            // Переопределение имеет приоритет над схемой; код, о котором не
            // сказано ни там, ни там, остаётся без диагноза.
            if let Some((_, s)) = self.states.iter().find(|(c, _)| *c == code) {
                return *s;
            }
            return desc.state(code).map_or(Severity::Normal, |s| s.severity);
        }
        let thresholds = self.thresholds.unwrap_or(desc.thresholds);
        value
            .as_f64()
            .map_or(Severity::Normal, |v| thresholds.severity_of(v))
    }
}

/// Состояние метрики с действующей важностью.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateStatus {
    pub code: u64,
    pub name: &'static str,
    pub severity: Severity,
}

/// Пределы метрики, действующие сейчас: схема плюс переопределения.
///
/// То, что отдаётся наружу — прикладному коду и (в будущем) веб-слою.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectiveLimits {
    pub metric: MetricId,
    pub name: &'static str,
    pub unit: &'static str,
    pub kind: MetricKind,
    pub thresholds: Thresholds,
    /// Состояния в порядке объявления в схеме. Пусто — не перечисление.
    pub states: Vec<StateStatus>,
    /// Отличается ли действующее от объявленного в схеме.
    pub overridden: bool,
}

/// Пределы всех метрик неймспейса.
///
/// Индексируется позицией метрики в [`Schema::metrics`], а не хеш-таблицей:
/// метрик единицы-сотни, позиция уже находится бинарным поиском по схеме, и
/// лишняя хеш-таблица тут была бы дороже самого доступа.
#[derive(Debug)]
pub struct LimitsRegistry {
    slots: RwLock<Vec<Option<MetricLimits>>>,
}

impl LimitsRegistry {
    pub fn new(metric_count: usize) -> Self {
        Self {
            slots: RwLock::new(vec![None; metric_count]),
        }
    }

    /// Выставить пределы. `None` — снять переопределение.
    ///
    /// Проверяет осмысленность: числовые границы у метрики-перечисления и
    /// важность кода, которого нет в схеме, отвергаются. Молча проглотить
    /// такую настройку нельзя — оператор считал бы, что предел действует.
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
                    metric: desc.name,
                    reason: "числовые границы у метрики-перечисления: её значения \
                             не упорядочены, задавайте важность состояний",
                });
            }
            if !l.states.is_empty() && desc.states.is_empty() {
                return Err(Error::BadLimits {
                    metric: desc.name,
                    reason: "важность состояний у метрики, которая не объявлена \
                             перечислением",
                });
            }
            for (code, _) in &l.states {
                if desc.state(*code).is_none() {
                    return Err(Error::BadLimits {
                        metric: desc.name,
                        reason: "важность кода состояния, которого нет в схеме: \
                                 подписать его будет нечем",
                    });
                }
            }
            if let Some(t) = l.thresholds {
                check_nesting(desc.name, &t)?;
            }
        }

        let mut slots = self.slots.write().unwrap_or_else(|e| e.into_inner());
        // Схема одна на неймспейс и неизменна, но подстраховаться дешевле,
        // чем паниковать на индексе.
        if slots.len() < schema.metrics.len() {
            slots.resize(schema.metrics.len(), None);
        }
        slots[index] = limits.filter(|l| !l.is_empty());
        Ok(())
    }

    /// Действующие пределы метрики.
    pub fn effective(&self, schema: &Schema, metric: MetricId) -> Result<EffectiveLimits> {
        let (index, desc) = resolve(schema, metric)?;
        let slots = self.slots.read().unwrap_or_else(|e| e.into_inner());
        let over = slots.get(index).and_then(|o| o.as_ref());

        Ok(EffectiveLimits {
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

    /// Важность значения с учётом переопределений.
    ///
    /// Горячий путь только для того, кто сам решил проверять значения на
    /// записи; сама запись пределов не касается.
    pub fn severity_of(
        &self,
        schema: &Schema,
        metric: MetricId,
        value: &dduroc_format::Value<'_>,
    ) -> Severity {
        let Ok((index, desc)) = resolve(schema, metric) else {
            return Severity::Normal;
        };
        let slots = self.slots.read().unwrap_or_else(|e| e.into_inner());
        match slots.get(index).and_then(|o| o.as_ref()) {
            Some(over) => over.severity_of(desc, value),
            None => desc.severity_of(value),
        }
    }
}

fn resolve(schema: &Schema, metric: MetricId) -> Result<(usize, &'static MetricDesc)> {
    schema
        .metric_index(metric)
        .ok_or(Error::UnknownMetric { id: metric.0 })
}

/// `critical` обязан включать `warn`: величина сначала выходит из нормы и
/// только потом из допустимого.
pub(crate) fn check_nesting(metric: &'static str, t: &Thresholds) -> Result<()> {
    let bad_low = matches!((t.warn.min, t.critical.min), (Some(w), Some(c)) if c > w);
    let bad_high = matches!((t.warn.max, t.critical.max), (Some(w), Some(c)) if c < w);
    if bad_low || bad_high {
        return Err(Error::BadLimits {
            metric,
            reason: "критический диапазон обязан включать тревожный: иначе \
                     значение оказалось бы критическим, не будучи тревожным",
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
            severity: Severity::Critical,
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
            id: MetricId(1),
            name: "temp",
            value_type: ValueType::F32,
            class: StorageClass::TELEMETRY,
            unit: "°C",
            tags: &["thermal"],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: Thresholds {
                warn: Range {
                    min: Some(0.0),
                    max: Some(70.0),
                },
                critical: Range {
                    min: Some(-10.0),
                    max: Some(85.0),
                },
            },
        },
        MetricDesc {
            id: MetricId(2),
            name: "link",
            value_type: ValueType::U64,
            class: StorageClass::TELEMETRY,
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
        LimitsRegistry::new(METRICS.len())
    }

    #[test]
    fn schema_defaults_classify_numeric_values() {
        let (s, r) = (schema(), registry());
        let sev = |v: f32| r.severity_of(&s, MetricId(1), &Value::F32(v));
        assert_eq!(sev(36.6), Severity::Normal);
        assert_eq!(sev(0.0), Severity::Normal, "граница включительно");
        assert_eq!(sev(70.0), Severity::Normal);
        assert_eq!(sev(71.0), Severity::Warn);
        assert_eq!(sev(-5.0), Severity::Warn, "ниже нормы — тоже тревога");
        assert_eq!(sev(90.0), Severity::Critical);
        assert_eq!(sev(-50.0), Severity::Critical);
        assert_eq!(
            sev(f32::NAN),
            Severity::Critical,
            "неизвестное значение не может считаться нормальным"
        );
    }

    #[test]
    fn state_severity_comes_from_the_state_not_from_ranges() {
        let (s, r) = (schema(), registry());
        let sev = |code: u64| r.severity_of(&s, MetricId(2), &Value::U64(code));
        assert_eq!(sev(0), Severity::Critical, "Los");
        assert_eq!(sev(1), Severity::Warn, "Sync");
        assert_eq!(sev(2), Severity::Normal, "Lock");
        assert_eq!(
            sev(99),
            Severity::Normal,
            "незнакомый код не с чем сопоставить — тревожить нечем"
        );
    }

    #[test]
    fn runtime_override_wins_over_schema() {
        // Внешняя система определила модель усилителя и сузила пределы.
        let (s, r) = (schema(), registry());
        r.set(
            &s,
            MetricId(1),
            Some(MetricLimits::numeric(Thresholds {
                warn: Range {
                    min: None,
                    max: Some(40.0),
                },
                critical: Range {
                    min: None,
                    max: Some(50.0),
                },
            })),
        )
        .unwrap();

        let sev = |v: f32| r.severity_of(&s, MetricId(1), &Value::F32(v));
        assert_eq!(sev(36.6), Severity::Normal);
        assert_eq!(sev(45.0), Severity::Warn, "схема сказала бы Normal");
        assert_eq!(sev(60.0), Severity::Critical);

        let eff = r.effective(&s, MetricId(1)).unwrap();
        assert!(eff.overridden);
        assert_eq!(eff.thresholds.warn.max, Some(40.0));
        assert_eq!(eff.unit, "°C", "статика метрики не тронута");

        // Снятие возвращает схемные.
        r.set(&s, MetricId(1), None).unwrap();
        assert_eq!(sev(45.0), Severity::Normal);
        assert!(!r.effective(&s, MetricId(1)).unwrap().overridden);
    }

    #[test]
    fn state_severity_can_be_reassigned_at_runtime() {
        // На этой установке потеря синхронизации терпима, а Lock обязателен.
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
            Severity::Critical,
            "непереопределённые состояния остаются схемными"
        );

        let eff = r.effective(&s, MetricId(2)).unwrap();
        assert_eq!(eff.states.len(), 3);
        assert_eq!(eff.states[1].name, "Sync");
        assert_eq!(eff.states[1].severity, Severity::Normal);
        assert_eq!(eff.states[0].severity, Severity::Critical);
        assert_eq!(eff.kind, MetricKind::State);
    }

    #[test]
    fn nonsensical_limits_are_refused_not_ignored() {
        // Молча проглотить настройку хуже, чем отказать: оператор считал бы,
        // что предел действует.
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
                    critical: Range::NONE,
                })),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "получено {err}");

        let err = r
            .set(
                &s,
                MetricId(1),
                Some(MetricLimits::states([(0, Severity::Warn)])),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "получено {err}");

        let err = r
            .set(
                &s,
                MetricId(2),
                Some(MetricLimits::states([(42, Severity::Warn)])),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "код вне схемы");

        // Критический диапазон, не включающий тревожный.
        let err = r
            .set(
                &s,
                MetricId(1),
                Some(MetricLimits::numeric(Thresholds {
                    warn: Range {
                        min: None,
                        max: Some(80.0),
                    },
                    critical: Range {
                        min: None,
                        max: Some(50.0),
                    },
                })),
            )
            .unwrap_err();
        assert!(matches!(err, Error::BadLimits { .. }), "вложенность");

        let err = r.set(&s, MetricId(99), None).unwrap_err();
        assert!(matches!(err, Error::UnknownMetric { id: 99 }), "{err}");
    }

    #[test]
    fn metric_without_limits_is_always_normal() {
        static PLAIN: &[MetricDesc] = &[MetricDesc {
            id: MetricId(1),
            name: "count",
            value_type: ValueType::U64,
            class: StorageClass::TELEMETRY,
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
        let r = LimitsRegistry::new(PLAIN.len());
        for v in [0u64, 1, u64::MAX] {
            assert_eq!(
                r.severity_of(&s, MetricId(1), &Value::U64(v)),
                Severity::Normal
            );
        }
        assert!(!r.effective(&s, MetricId(1)).unwrap().overridden);
    }
}
