//! Resource ceilings shared by all requests using a service.
use dduroc_read::ReadLimits;
use std::time::Duration;

/// Server-owned ceilings. Client requests may only lower these limits.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub input_limits: crate::input::InputLimits,
    pub conversion_limits: crate::types::ConversionLimits,
    pub max_sessions: usize,
    pub max_jobs: usize,
    pub max_subscriptions: usize,
    pub idle_ttl: Duration,
    pub session_lifetime: Duration,
    pub operation_timeout: Duration,
    pub subscription_lifetime: Duration,
    pub subscription_queue_items: usize,
    pub subscription_queue_bytes: usize,
    pub slow_consumer_timeout: Duration,
    pub max_seek_pages_per_call: usize,
    pub max_page_records: usize,
    pub max_page_bytes: usize,
    pub read_limits: ReadLimits,
}
impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            input_limits: crate::input::InputLimits::default(),
            conversion_limits: crate::types::ConversionLimits::default(),
            max_sessions: 4,
            max_jobs: 4,
            max_subscriptions: 2,
            idle_ttl: Duration::from_secs(60),
            session_lifetime: Duration::from_secs(300),
            operation_timeout: Duration::from_secs(10),
            subscription_lifetime: Duration::from_secs(300),
            subscription_queue_items: 32,
            subscription_queue_bytes: 1024 * 1024,
            slow_consumer_timeout: Duration::from_secs(2),
            max_seek_pages_per_call: 8,
            max_page_records: 128,
            max_page_bytes: 256 * 1024,
            read_limits: ReadLimits {
                compressed_bytes: 8 * 1024 * 1024,
                decoded_bytes: 8 * 1024 * 1024,
                blocks: 2048,
                examined_records: 50_000,
                returned_bytes: 2 * 1024 * 1024,
                ..ReadLimits::default()
            },
        }
    }
}
impl ServiceConfig {
    pub(crate) fn valid(&self) -> bool {
        self.max_sessions > 0
            && self.max_sessions <= tokio::sync::Semaphore::MAX_PERMITS
            && self.max_jobs > 0
            && self.max_jobs <= tokio::sync::Semaphore::MAX_PERMITS
            && self.max_subscriptions <= tokio::sync::Semaphore::MAX_PERMITS
            && self.max_subscriptions < self.max_jobs
            && self.subscription_queue_bytes
                <= (u32::MAX as usize).min(tokio::sync::Semaphore::MAX_PERMITS)
            && self.subscription_queue_items <= tokio::sync::Semaphore::MAX_PERMITS
            && self.subscription_queue_items > 0
            && self.subscription_queue_bytes > 0
            && self.max_seek_pages_per_call > 0
            && self.max_page_records > 0
            && self.max_page_bytes > 0
            && !self.idle_ttl.is_zero()
            && !self.session_lifetime.is_zero()
            && !self.operation_timeout.is_zero()
            && !self.subscription_lifetime.is_zero()
            && !self.slow_consumer_timeout.is_zero()
            && [
                self.idle_ttl,
                self.session_lifetime,
                self.operation_timeout,
                self.subscription_lifetime,
                self.slow_consumer_timeout,
            ]
            .into_iter()
            .all(|duration| std::time::Instant::now().checked_add(duration).is_some())
    }
    pub(crate) fn clamp(
        &self,
        mut requested: ReadLimits,
        deadline: std::time::Instant,
    ) -> ReadLimits {
        let max = &self.read_limits;
        requested.epoch_runs = requested.epoch_runs.min(max.epoch_runs);
        requested.hardware_boots = requested.hardware_boots.min(max.hardware_boots);
        requested.roster_entries = requested.roster_entries.min(max.roster_entries);
        requested.channels = requested.channels.min(max.channels);
        requested.blocks = requested.blocks.min(max.blocks);
        requested.compressed_bytes = requested.compressed_bytes.min(max.compressed_bytes);
        requested.decoded_bytes = requested.decoded_bytes.min(max.decoded_bytes);
        requested.examined_records = requested.examined_records.min(max.examined_records);
        requested.returned_bytes = requested.returned_bytes.min(max.returned_bytes);
        requested.deadline = Some(
            requested
                .deadline
                .map_or(deadline, |value| value.min(deadline))
                .min(max.deadline.unwrap_or(deadline)),
        );
        requested
    }
}
