//! Independent accounting limits for owned data held by the two write queues.
use crate::{Error, Result};

/// Queue-owned data accounting, separately from queue item capacities.
///
/// A queued item is charged its inline envelope, allocated capacity of spilled
/// payloads, and owned text. Shared text is conservatively charged per record.
/// Channel slot allocations, allocator overhead, encoding buffers, backend
/// indexes and application-owned values are additional costs: this is not an
/// RSS limit. A record larger than its pool is rejected without waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueByteLimits {
    /// Accounting limit for ordinary records.
    pub normal: usize,
    /// Independent accounting limit for critical records.
    pub critical: usize,
}

impl Default for QueueByteLimits {
    fn default() -> Self {
        Self {
            normal: 16 * 1024 * 1024,
            critical: 4 * 1024 * 1024,
        }
    }
}

impl QueueByteLimits {
    /// Validate before opening or modifying the store.
    pub fn validate(self) -> Result<()> {
        for (setting, limit) in [
            ("queue_byte_limits.normal", self.normal),
            ("queue_byte_limits.critical", self.critical),
        ] {
            if limit == 0 || limit > isize::MAX as usize {
                return Err(Error::BadStore {
                    setting,
                    reason: "must be positive and fit within isize::MAX",
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn defaults_preserve_separate_priority_pools() {
        let limits = QueueByteLimits::default();
        assert_eq!(limits.normal, 16 * 1024 * 1024);
        assert_eq!(limits.critical, 4 * 1024 * 1024);
        limits.validate().unwrap();
    }
    #[test]
    fn invalid_limits_are_rejected_for_each_pool() {
        for limits in [
            QueueByteLimits {
                normal: 0,
                critical: 1,
            },
            QueueByteLimits {
                normal: 1,
                critical: 0,
            },
            QueueByteLimits {
                normal: usize::MAX,
                critical: 1,
            },
            QueueByteLimits {
                normal: 1,
                critical: usize::MAX,
            },
        ] {
            assert!(matches!(limits.validate(), Err(Error::BadStore { .. })));
        }
    }
}
