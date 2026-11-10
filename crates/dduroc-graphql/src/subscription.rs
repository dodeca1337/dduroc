//! Bounded per-subscription native follow workers.
use crate::{
    error::ServiceError,
    service::{Inner, Service},
    session::random_id,
};
use dduroc_read::{Cancellation, FollowItem, Query, ReadLimits};
use futures_core::Stream;
use std::{
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

struct Envelope {
    value: crate::types::FollowBatchView,
    _bytes: OwnedSemaphorePermit,
}
/// A stream whose drop cancels its native worker.
pub struct FollowStream {
    receiver: mpsc::Receiver<Envelope>,
    terminal: Arc<Mutex<Option<ServiceError>>>,
    cancellation: Cancellation,
    delivered_terminal: bool,
    capacity: Option<Arc<OwnedSemaphorePermit>>,
}
impl std::fmt::Debug for FollowStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FollowStream").finish_non_exhaustive()
    }
}
impl Drop for FollowStream {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}
impl Stream for FollowStream {
    type Item = Result<crate::types::FollowBatchView, ServiceError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(envelope)) => Poll::Ready(Some(Ok(envelope.value))),
            Poll::Ready(None) if !self.delivered_terminal => {
                self.delivered_terminal = true;
                self.capacity.take();
                let error = self.terminal.lock().ok().and_then(|mut value| value.take());
                Poll::Ready(error.map(Err))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}
struct WorkerCleanup {
    inner: Weak<Inner>,
    id: String,
    cancellation: Cancellation,
}
impl Drop for WorkerCleanup {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(inner) = self.inner.upgrade()
            && let Ok(mut items) = inner.subscriptions.lock()
        {
            items.remove(&self.id);
        }
    }
}
impl Service {
    /// Start a dedicated bounded native follow worker. A slow consumer gets a
    /// terminal error and must create a new subscription; no records are dropped.
    /// Before conversion the worker reserves the full per-message ceiling, so
    /// small messages may wait for earlier queued bytes to be consumed.
    pub fn subscribe(
        &self,
        query: Query,
        limits: ReadLimits,
        presentation: crate::types::Presentation,
    ) -> Result<FollowStream, ServiceError> {
        self.validate_request(&query, &presentation)?;
        let capacity = Arc::clone(&self.inner.subscription_capacity)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Capacity)?;
        let capacity = Arc::new(capacity);
        let worker_capacity = Arc::clone(&capacity);
        let job = self.job()?;
        let id = random_id()?;
        let token = Cancellation::new();
        let deadline = self.deadline(self.inner.config.subscription_lifetime)?;
        let id_for_cleanup = id.clone();
        {
            let mut registry = self
                .inner
                .subscriptions
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if registry.contains_key(&id) {
                return Err(ServiceError::WorkerUnavailable);
            }
            registry.insert(id.clone(), token.clone());
        }
        let limits = self.inner.config.clamp(limits, deadline);
        let output_limit = limits.returned_bytes;
        let (sender, receiver) = mpsc::channel(self.inner.config.subscription_queue_items);
        let byte_budget = Arc::new(Semaphore::new(self.inner.config.subscription_queue_bytes));
        let terminal = Arc::new(Mutex::new(None));
        let worker_terminal = Arc::clone(&terminal);
        let worker_token = token.clone();
        let reader = Arc::clone(&self.inner.reader);
        let weak = Arc::downgrade(&self.inner);
        let slow_timeout = self.inner.config.slow_consumer_timeout;
        let byte_limit = self.inner.config.subscription_queue_bytes;
        let conversion = self.inner.config.conversion_limits.clone();
        let worker = std::thread::Builder::new()
            .name("dduroc-graphql-follow".into())
            .spawn(move || {
                let _capacity = worker_capacity;
                let _job = job;
                let _cleanup = WorkerCleanup {
                    inner: weak,
                    id,
                    cancellation: worker_token.clone(),
                };
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || -> Result<(), ServiceError> {
                        let mut follow = reader
                            .follow_controlled(query, limits, worker_token.clone())
                            .map_err(ServiceError::Native)?;
                        let mut previous_unanchored = Vec::new();
                        let mut delivered_bytes = 0u64;
                        loop {
                            if worker_token.is_cancelled() {
                                return Ok(());
                            }
                            let tail = follow
                                .next_item(Duration::from_millis(100))
                                .map_err(ServiceError::Native)?;
                            let (entries, seeds, ended) = match tail {
                                FollowItem::Entry { entry, seed: false } => {
                                    (vec![*entry], Vec::new(), false)
                                }
                                FollowItem::Entry { entry, seed: true } => {
                                    (Vec::new(), vec![*entry], false)
                                }
                                FollowItem::Idle => (Vec::new(), Vec::new(), false),
                                FollowItem::Ended => (Vec::new(), Vec::new(), true),
                            };
                            let damaged = follow.take_damage();
                            let unanchored = follow.unanchored().map_err(ServiceError::Native)?;
                            if entries.is_empty()
                                && seeds.is_empty()
                                && damaged.is_empty()
                                && !ended
                                && unanchored == previous_unanchored
                            {
                                continue;
                            }
                            previous_unanchored = unanchored.clone();
                            let until = Instant::now()
                                .checked_add(slow_timeout)
                                .ok_or(ServiceError::WorkerUnavailable)?;
                            let reserve = conversion.max_output_bytes.min(byte_limit).max(1);
                            let mut permit = loop {
                                if worker_token.is_cancelled() || sender.is_closed() {
                                    return Ok(());
                                }
                                if let Ok(permit) =
                                    Arc::clone(&byte_budget).try_acquire_many_owned(reserve as u32)
                                {
                                    break permit;
                                }
                                if Instant::now() >= until {
                                    return Err(ServiceError::SlowConsumer);
                                }
                                std::thread::sleep(Duration::from_millis(10));
                            };
                            let mut bounded = conversion.clone();
                            bounded.max_output_bytes = bounded.max_output_bytes.min(reserve);
                            let (view, bytes) = crate::types::convert_follow(
                                &reader,
                                crate::types::FollowRecords {
                                    entries,
                                    seeds,
                                    damaged,
                                    unanchored,
                                    ended,
                                },
                                &presentation,
                                &bounded,
                            )?;
                            let bytes = bytes.max(1);
                            delivered_bytes = delivered_bytes.checked_add(bytes as u64).ok_or(
                                ServiceError::Native(dduroc_read::ReadError::LimitExceeded(
                                    dduroc_read::ReadResource::ReturnedBytes,
                                )),
                            )?;
                            if delivered_bytes > output_limit {
                                return Err(ServiceError::Native(
                                    dduroc_read::ReadError::LimitExceeded(
                                        dduroc_read::ReadResource::ReturnedBytes,
                                    ),
                                ));
                            }
                            let used =
                                permit.split(bytes).ok_or(ServiceError::WorkerUnavailable)?;
                            drop(permit);
                            let mut envelope = Envelope {
                                value: view,
                                _bytes: used,
                            };
                            loop {
                                if worker_token.is_cancelled() {
                                    return Ok(());
                                }
                                match sender.try_send(envelope) {
                                    Ok(()) => break,
                                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                                    Err(mpsc::error::TrySendError::Full(value)) => envelope = value,
                                }
                                if Instant::now() >= until {
                                    return Err(ServiceError::SlowConsumer);
                                }
                                std::thread::sleep(Duration::from_millis(10));
                            }
                            if ended {
                                return Ok(());
                            }
                        }
                    },
                ));
                let error = match result {
                    Ok(Ok(())) => None,
                    Ok(Err(error)) => Some(error),
                    Err(_) => Some(ServiceError::WorkerUnavailable),
                };
                if let Some(error) = error
                    && let Ok(mut terminal) = worker_terminal.lock()
                {
                    *terminal = Some(error);
                }
            });
        if worker.is_err() {
            if let Ok(mut registry) = self.inner.subscriptions.lock() {
                registry.remove(&id_for_cleanup);
            }
            token.cancel();
            return Err(ServiceError::WorkerUnavailable);
        }
        Ok(FollowStream {
            receiver,
            terminal,
            cancellation: token,
            delivered_terminal: false,
            capacity: Some(capacity),
        })
    }
}
