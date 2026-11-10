//! Bounded runtime over the synchronous native reader.
use crate::{
    config::ServiceConfig,
    error::ServiceError,
    session::{CountedSessionOpened, SessionOpened, SessionPage, Slot, random_id},
};
use dduroc_read::{Cancellation, Query, ReadLimits, Reader};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct Service {
    pub(crate) inner: Arc<Inner>,
}
impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service").finish_non_exhaustive()
    }
}
pub(crate) struct Inner {
    pub reader: Arc<Reader>,
    pub config: ServiceConfig,
    pub sessions: Mutex<HashMap<String, Arc<Slot>>>,
    pub jobs: Arc<Semaphore>,
    pub session_capacity: Arc<Semaphore>,
    pub subscription_capacity: Arc<Semaphore>,
    pub subscriptions: Mutex<HashMap<String, Cancellation>>,
}
impl Drop for Inner {
    fn drop(&mut self) {
        if let Ok(sessions) = self.sessions.get_mut() {
            for slot in sessions.values() {
                slot.invalidate();
            }
            sessions.clear();
        }
        if let Ok(subscriptions) = self.subscriptions.get_mut() {
            for token in subscriptions.values() {
                token.cancel();
            }
            subscriptions.clear();
        }
    }
}
/// Cancels a native operation if its request future is dropped or times out.
struct RequestGuard {
    inner: Weak<Inner>,
    id: String,
    slot: Arc<Slot>,
    armed: bool,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        self.slot.invalidate();
        if let Some(inner) = self.inner.upgrade()
            && let Ok(mut sessions) = inner.sessions.lock()
            && sessions
                .get(&self.id)
                .is_some_and(|value| Arc::ptr_eq(value, &self.slot))
        {
            sessions.remove(&self.id);
        }
    }
}
impl Service {
    fn validate_query(&self, query: &Query) -> Result<(), ServiceError> {
        let limits = &self.inner.config.input_limits;
        let names: &[String] = match &query.namespaces {
            dduroc_read::NsSelect::Names(names) => names,
            _ => &[],
        };
        let group = match &query.namespaces {
            dduroc_read::NsSelect::Group(group) => Some(group),
            _ => None,
        };
        let mut count = query
            .channels
            .len()
            .saturating_add(names.len())
            .saturating_add(query.filter.any_tags.len())
            .saturating_add(query.filter.event_names.len())
            .saturating_add(query.filter.events.as_ref().map_or(0, |items| items.len()))
            .saturating_add(query.filter.spans.as_ref().map_or(0, |items| items.len()));
        if group.is_some() {
            count = count.saturating_add(1);
        }
        if count > limits.max_items {
            return Err(ServiceError::InvalidInput("too many filter items".into()));
        }
        let mut total = 0usize;
        for value in names
            .iter()
            .chain(query.filter.any_tags.iter())
            .chain(query.filter.event_names.iter())
            .chain(group)
        {
            total = total
                .checked_add(value.len())
                .ok_or_else(|| ServiceError::InvalidInput("filter text overflow".into()))?;
            if value.is_empty()
                || value.len() > limits.max_string_bytes
                || total > limits.max_total_string_bytes
            {
                return Err(ServiceError::InvalidInput(
                    "filter text exceeds limits".into(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn validate_request(
        &self,
        query: &Query,
        presentation: &crate::types::Presentation,
    ) -> Result<(), ServiceError> {
        self.validate_query(query)?;
        let preferences = presentation
            .languages
            .as_ref()
            .unwrap_or_else(|| self.language_preferences());
        let limits = &self.inner.config.input_limits;
        let mut total = 0usize;
        if preferences.codes().len() > limits.max_items {
            return Err(ServiceError::InvalidInput("too many languages".into()));
        }
        for language in preferences.codes() {
            total = total.saturating_add(language.len());
            if language.len() > limits.max_string_bytes || total > limits.max_total_string_bytes {
                return Err(ServiceError::InvalidInput(
                    "language codes exceed limits".into(),
                ));
            }
        }
        Ok(())
    }
    /// Construct and use this service inside a Tokio runtime with its time
    /// driver enabled (`enable_time` or `enable_all`). A weak janitor owns no
    /// service lifetime. The constructor checks that a runtime is entered; it
    /// cannot check whether that runtime's time driver is enabled.
    pub fn new(reader: Arc<Reader>, config: ServiceConfig) -> Result<Self, ServiceError> {
        if !config.valid() || tokio::runtime::Handle::try_current().is_err() {
            return Err(ServiceError::InvalidInput(
                "invalid service configuration or no Tokio runtime".into(),
            ));
        }
        let inner = Arc::new(Inner {
            jobs: Arc::new(Semaphore::new(config.max_jobs)),
            session_capacity: Arc::new(Semaphore::new(config.max_sessions)),
            subscription_capacity: Arc::new(Semaphore::new(config.max_subscriptions)),
            sessions: Mutex::new(HashMap::new()),
            subscriptions: Mutex::new(HashMap::new()),
            reader,
            config,
        });
        let weak = Arc::downgrade(&inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                let now = Instant::now();
                if let Ok(mut sessions) = inner.sessions.lock() {
                    sessions.retain(|_, slot| {
                        let expired = slot
                            .created
                            .checked_add(inner.config.session_lifetime)
                            .is_none_or(|at| now >= at)
                            || slot.state.lock().map_or(true, |state| {
                                state.closed
                                    || (!state.busy
                                        && state
                                            .touched
                                            .checked_add(inner.config.idle_ttl)
                                            .is_none_or(|at| now >= at))
                            });
                        if expired {
                            slot.invalidate();
                        }
                        !expired
                    });
                }
            }
        });
        Ok(Self { inner })
    }
    pub(crate) fn deadline(&self, lifetime: Duration) -> Result<Instant, ServiceError> {
        Instant::now()
            .checked_add(lifetime)
            .ok_or_else(|| ServiceError::InvalidInput("deadline is out of range".into()))
    }
    pub(crate) fn job(&self) -> Result<OwnedSemaphorePermit, ServiceError> {
        Arc::clone(&self.inner.jobs)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Capacity)
    }
    fn guard(&self, id: String, slot: Arc<Slot>) -> RequestGuard {
        RequestGuard {
            inner: Arc::downgrade(&self.inner),
            id,
            slot,
            armed: true,
        }
    }
    fn remove(&self, id: &str, slot: &Arc<Slot>) {
        slot.invalidate();
        if let Ok(mut sessions) = self.inner.sessions.lock()
            && sessions
                .get(id)
                .is_some_and(|current| Arc::ptr_eq(current, slot))
        {
            sessions.remove(id);
        }
    }
    pub async fn open_session(
        &self,
        query: Query,
        limits: ReadLimits,
        presentation: crate::types::Presentation,
    ) -> Result<SessionOpened, ServiceError> {
        self.open_session_impl(query, limits, presentation, None)
            .await
            .map(|(opened, _)| opened)
    }
    pub async fn open_counted_session(
        &self,
        query: Query,
        limits: ReadLimits,
        presentation: crate::types::Presentation,
        page: dduroc_read::PageLimits,
    ) -> Result<CountedSessionOpened, ServiceError> {
        let records = page.max_records();
        let bytes = page.max_bytes();
        if records > self.inner.config.max_page_records || bytes > self.inner.config.max_page_bytes
        {
            return Err(ServiceError::InvalidInput("invalid page size".into()));
        }
        let (opened, totals) = self
            .open_session_impl(query, limits, presentation, Some(page))
            .await?;
        Ok(CountedSessionOpened {
            id: opened.id,
            revision: opened.revision,
            totals: totals.expect("counted session has totals"),
        })
    }
    async fn open_session_impl(
        &self,
        query: Query,
        limits: ReadLimits,
        presentation: crate::types::Presentation,
        dimensions: Option<dduroc_read::PageLimits>,
    ) -> Result<(SessionOpened, Option<dduroc_read::QueryTotals>), ServiceError> {
        self.validate_request(&query, &presentation)?;
        let capacity = Arc::clone(&self.inner.session_capacity)
            .try_acquire_owned()
            .map_err(|_| ServiceError::Capacity)?;
        let job = self.job()?;
        let deadline = self.deadline(self.inner.config.session_lifetime)?;
        let requested = self.inner.config.clamp(limits, deadline);
        let slot = Slot::new(capacity, presentation);
        let id = random_id()?;
        {
            let mut sessions = self
                .inner
                .sessions
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if sessions.contains_key(&id) {
                return Err(ServiceError::WorkerUnavailable);
            }
            sessions.insert(id.clone(), Arc::clone(&slot));
        }
        let mut guard = self.guard(id.clone(), Arc::clone(&slot));
        let reader = Arc::clone(&self.inner.reader);
        let worker_slot = Arc::clone(&slot);
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            let result = match dimensions {
                Some(page) => reader
                    .open_counted_session(query, requested, worker_slot.cancellation.clone(), page)
                    .map(|(session, totals)| (session, Some(totals))),
                None => reader
                    .open_session(query, requested, worker_slot.cancellation.clone())
                    .map(|session| (session, None)),
            };
            (worker_slot, result)
        });
        let (_, result) = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)?;
        let (session, totals) = result.map_err(ServiceError::Native)?;
        {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if state.closed
                || slot.cancellation.is_cancelled()
                || Instant::now().saturating_duration_since(slot.created)
                    >= self.inner.config.session_lifetime
            {
                return Err(ServiceError::Expired);
            }
            state.session = Some(session);
            state.busy = false;
            state.touched = Instant::now();
        }
        guard.armed = false;
        Ok((SessionOpened { id, revision: 0 }, totals))
    }
    pub async fn next_page(
        &self,
        id: String,
        expected_revision: u64,
        page: dduroc_read::PageLimits,
        projection: crate::types::Projection,
    ) -> Result<SessionPage, ServiceError> {
        if id.len() != 32 {
            return Err(ServiceError::InvalidInput(
                "invalid session or page size".into(),
            ));
        }
        let slot = self
            .inner
            .sessions
            .lock()
            .map_err(|_| ServiceError::WorkerUnavailable)?
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        let (session, job) = {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if state.closed {
                return Err(ServiceError::Expired);
            }
            if Instant::now().saturating_duration_since(slot.created)
                >= self.inner.config.session_lifetime
                || (!state.busy
                    && Instant::now().saturating_duration_since(state.touched)
                        >= self.inner.config.idle_ttl)
            {
                state.closed = true;
                slot.cancellation.cancel();
                return Err(ServiceError::Expired);
            }
            if state.busy {
                return Err(ServiceError::Busy);
            }
            if state.revision != expected_revision {
                return Err(ServiceError::RevisionConflict {
                    actual: state.revision,
                });
            }
            let job = self.job()?;
            let session = state
                .session
                .take()
                .ok_or(ServiceError::WorkerUnavailable)?;
            state.busy = true;
            (session, job)
        };
        let mut guard = self.guard(id.clone(), Arc::clone(&slot));
        let page = dduroc_read::PageLimits::new(
            page.max_records().min(self.inner.config.max_page_records),
            page.max_bytes().min(self.inner.config.max_page_bytes),
        )
        .expect("configured page limits are nonzero");
        let worker_slot = Arc::clone(&slot);
        let reader = Arc::clone(&self.inner.reader);
        let conversion = self.inner.config.conversion_limits.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            let mut session = session;
            let mut presentation = worker_slot.presentation.clone();
            presentation.projection = projection;
            let page = session
                .next_page(page)
                .map_err(ServiceError::Native)
                .and_then(|page| {
                    if worker_slot.cancellation.is_cancelled() {
                        return Err(ServiceError::Cancelled);
                    }
                    let result =
                        crate::types::convert_page(&reader, page, &presentation, &conversion)?;
                    if worker_slot.cancellation.is_cancelled() {
                        return Err(ServiceError::Cancelled);
                    }
                    Ok(result)
                });
            (worker_slot, session, page)
        });
        let (_, session, page) = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)?;
        let page = match page {
            Ok(page) => page,
            Err(ServiceError::Native(
                error @ (dduroc_read::ReadError::PageTooSmall
                | dduroc_read::ReadError::InvalidNavigation),
            )) => {
                let mut state = slot
                    .state
                    .lock()
                    .map_err(|_| ServiceError::WorkerUnavailable)?;
                if state.closed
                    || slot.cancellation.is_cancelled()
                    || Instant::now().saturating_duration_since(slot.created)
                        >= self.inner.config.session_lifetime
                {
                    return Err(ServiceError::Expired);
                }
                state.session = Some(session);
                state.busy = false;
                state.touched = Instant::now();
                guard.armed = false;
                return Err(ServiceError::Native(error));
            }
            Err(error) => return Err(error),
        };
        let revision = {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if state.closed
                || slot.cancellation.is_cancelled()
                || Instant::now().saturating_duration_since(slot.created)
                    >= self.inner.config.session_lifetime
            {
                return Err(ServiceError::Expired);
            }
            let revision = state
                .revision
                .checked_add(1)
                .ok_or(ServiceError::WorkerUnavailable)?;
            state.session = Some(session);
            state.revision = revision;
            state.busy = false;
            state.touched = Instant::now();
            revision
        };
        guard.armed = false;
        Ok(SessionPage { revision, page })
    }
    pub async fn seek_page(
        &self,
        id: String,
        expected_revision: u64,
        target: u64,
        presentation: crate::types::Presentation,
        page: dduroc_read::PageLimits,
    ) -> Result<crate::session::SessionSeekPage, ServiceError> {
        if id.len() != 32 || target == 0 {
            return Err(ServiceError::InvalidInput(
                "invalid session or page size".into(),
            ));
        }
        self.validate_request(&Query::default(), &presentation)?;
        let slot = self
            .inner
            .sessions
            .lock()
            .map_err(|_| ServiceError::WorkerUnavailable)?
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        let (session, job) = {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if state.closed {
                return Err(ServiceError::Expired);
            }
            if Instant::now().saturating_duration_since(slot.created)
                >= self.inner.config.session_lifetime
                || (!state.busy
                    && Instant::now().saturating_duration_since(state.touched)
                        >= self.inner.config.idle_ttl)
            {
                state.closed = true;
                slot.cancellation.cancel();
                return Err(ServiceError::Expired);
            }
            if state.busy {
                return Err(ServiceError::Busy);
            }
            if state.revision != expected_revision {
                return Err(ServiceError::RevisionConflict {
                    actual: state.revision,
                });
            }
            let job = self.job()?;
            let session = state
                .session
                .take()
                .ok_or(ServiceError::WorkerUnavailable)?;
            state.busy = true;
            (session, job)
        };
        let mut guard = self.guard(id.clone(), Arc::clone(&slot));
        let page = dduroc_read::PageLimits::new(
            page.max_records().min(self.inner.config.max_page_records),
            page.max_bytes().min(self.inner.config.max_page_bytes),
        )
        .expect("configured page limits are nonzero");
        let worker_slot = Arc::clone(&slot);
        let reader = Arc::clone(&self.inner.reader);
        let conversion = self.inner.config.conversion_limits.clone();
        let max_pages = self.inner.config.max_seek_pages_per_call;
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            let mut session = session;
            let page = session
                .seek_page(target, page, max_pages)
                .map_err(ServiceError::Native)
                .and_then(|page| {
                    if worker_slot.cancellation.is_cancelled() {
                        return Err(ServiceError::Cancelled);
                    }
                    let result = crate::session::SessionSeekResult {
                        reached_page: page.reached_page,
                        target_reached: page.target_reached,
                        exhausted: page.exhausted,
                        page: page
                            .page
                            .map(|page| {
                                crate::types::convert_page(
                                    &reader,
                                    page,
                                    &presentation,
                                    &conversion,
                                )
                            })
                            .transpose()?,
                    };
                    if worker_slot.cancellation.is_cancelled() {
                        return Err(ServiceError::Cancelled);
                    }
                    Ok(result)
                });
            (worker_slot, session, page)
        });
        let (_, session, page) = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)?;
        let page = match page {
            Ok(page) => page,
            Err(ServiceError::Native(error @ dduroc_read::ReadError::InvalidNavigation)) => {
                let mut state = slot
                    .state
                    .lock()
                    .map_err(|_| ServiceError::WorkerUnavailable)?;
                if state.closed
                    || slot.cancellation.is_cancelled()
                    || Instant::now().saturating_duration_since(slot.created)
                        >= self.inner.config.session_lifetime
                {
                    return Err(ServiceError::Expired);
                }
                state.session = Some(session);
                state.busy = false;
                state.touched = Instant::now();
                guard.armed = false;
                return Err(ServiceError::Native(error));
            }
            Err(error) => return Err(error),
        };
        let revision = {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if state.closed
                || slot.cancellation.is_cancelled()
                || Instant::now().saturating_duration_since(slot.created)
                    >= self.inner.config.session_lifetime
            {
                return Err(ServiceError::Expired);
            }
            let revision = state
                .revision
                .checked_add(1)
                .ok_or(ServiceError::WorkerUnavailable)?;
            state.session = Some(session);
            state.revision = revision;
            state.busy = false;
            state.touched = Instant::now();
            revision
        };
        guard.armed = false;
        Ok(crate::session::SessionSeekPage {
            revision,
            result: page,
        })
    }
    pub async fn close(&self, id: String, expected_revision: u64) -> Result<(), ServiceError> {
        if id.len() != 32 {
            return Err(ServiceError::NotFound);
        }
        let slot = self
            .inner
            .sessions
            .lock()
            .map_err(|_| ServiceError::WorkerUnavailable)?
            .get(&id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        {
            let mut state = slot
                .state
                .lock()
                .map_err(|_| ServiceError::WorkerUnavailable)?;
            if state.closed {
                return Err(ServiceError::Expired);
            }
            if Instant::now().saturating_duration_since(slot.created)
                >= self.inner.config.session_lifetime
                || (!state.busy
                    && Instant::now().saturating_duration_since(state.touched)
                        >= self.inner.config.idle_ttl)
            {
                state.closed = true;
                slot.cancellation.cancel();
                return Err(ServiceError::Expired);
            }
            if state.busy {
                return Err(ServiceError::Busy);
            }
            if state.revision != expected_revision {
                return Err(ServiceError::RevisionConflict {
                    actual: state.revision,
                });
            }
            // Close under the same lock as revision validation, so a page
            // cannot advance between the check and retirement.
            state.closed = true;
            state.session.take();
            slot.cancellation.cancel();
        }
        self.remove(&id, &slot);
        Ok(())
    }
    /// Cancel a session, including one currently executing. Capacity remains
    /// held until the native worker actually stops.
    pub async fn cancel(&self, id: String) -> Result<(), ServiceError> {
        self.cancel_now(&id)
    }
    pub(crate) fn cancel_now(&self, id: &str) -> Result<(), ServiceError> {
        if id.len() != 32 {
            return Err(ServiceError::NotFound);
        }
        let slot = self
            .inner
            .sessions
            .lock()
            .map_err(|_| ServiceError::WorkerUnavailable)?
            .get(id)
            .cloned()
            .ok_or(ServiceError::NotFound)?;
        self.remove(id, &slot);
        Ok(())
    }
    pub fn language_preferences(&self) -> &dduroc_read::LanguagePreferences {
        self.inner.reader.language_preferences()
    }
    pub fn config(&self) -> &ServiceConfig {
        &self.inner.config
    }
    /// Global statistics are metadata-only; optional root allocation is a
    /// bounded separate sample and is computed on a blocking worker.
    pub async fn statistics(
        &self,
        limits: ReadLimits,
        include_roots: bool,
    ) -> Result<crate::types::StatisticsView, ServiceError> {
        let job = self.job()?;
        let cancellation = Cancellation::new();
        struct Cancel(Cancellation, bool);
        impl Drop for Cancel {
            fn drop(&mut self) {
                if self.1 {
                    self.0.cancel();
                }
            }
        }
        let mut guard = Cancel(cancellation.clone(), true);
        let deadline = self.deadline(self.inner.config.operation_timeout)?;
        let limits = self.inner.config.clamp(limits, deadline);
        let reader = Arc::clone(&self.inner.reader);
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            reader
                .statistics_controlled(limits, cancellation, include_roots)
                .map(Into::into)
                .map_err(ServiceError::Native)
        });
        let result = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)??;
        guard.1 = false;
        Ok(result)
    }

    /// Execute one bounded, explicitly requested index-maintenance step.
    pub async fn backfill_statistics(
        &self,
        limits: ReadLimits,
    ) -> Result<crate::types::BackfillStatisticsView, ServiceError> {
        let job = self.job()?;
        let cancellation = Cancellation::new();
        struct Cancel(Cancellation, bool);
        impl Drop for Cancel {
            fn drop(&mut self) {
                if self.1 {
                    self.0.cancel();
                }
            }
        }
        let mut guard = Cancel(cancellation.clone(), true);
        let deadline = self.deadline(self.inner.config.operation_timeout)?;
        let limits = self.inner.config.clamp(limits, deadline);
        let reader = Arc::clone(&self.inner.reader);
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            reader
                .backfill_statistics_step(limits, cancellation)
                .map(|more| crate::types::BackfillStatisticsView { more })
                .map_err(ServiceError::Native)
        });
        let result = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)??;
        guard.1 = false;
        Ok(result)
    }

    /// Bounded catalog discovery and descriptor conversion on the worker pool.
    pub async fn catalog(
        &self,
        query: Query,
        limits: ReadLimits,
    ) -> Result<crate::types::CatalogView, ServiceError> {
        self.validate_query(&query)?;
        let job = self.job()?;
        let cancellation = Cancellation::new();
        struct Cancel(Cancellation, bool);
        impl Drop for Cancel {
            fn drop(&mut self) {
                if self.1 {
                    self.0.cancel();
                }
            }
        }
        let mut guard = Cancel(cancellation.clone(), true);
        let deadline = self.deadline(self.inner.config.operation_timeout)?;
        let limits = self.inner.config.clamp(limits, deadline);
        let reader = Arc::clone(&self.inner.reader);
        let conversion = self.inner.config.conversion_limits.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            let rows = reader
                .catalog_controlled(&query, limits, cancellation.clone())
                .map_err(ServiceError::Native)?;
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            let result =
                crate::types::convert_catalog(rows, &conversion, reader.language_preferences())?;
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            Ok(result)
        });
        let result = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)??;
        guard.1 = false;
        Ok(result)
    }
    pub async fn query(
        &self,
        query: Query,
        limits: ReadLimits,
        page: dduroc_read::PageLimits,
        presentation: crate::types::Presentation,
    ) -> Result<crate::types::PageView, ServiceError> {
        self.validate_request(&query, &presentation)?;
        let job = self.job()?;
        let cancellation = Cancellation::new();
        struct Cancel(Cancellation, bool);
        impl Drop for Cancel {
            fn drop(&mut self) {
                if self.1 {
                    self.0.cancel();
                }
            }
        }
        let mut guard = Cancel(cancellation.clone(), true);
        let deadline = self.deadline(self.inner.config.operation_timeout)?;
        let limits = self.inner.config.clamp(limits, deadline);
        let reader = Arc::clone(&self.inner.reader);
        let conversion = self.inner.config.conversion_limits.clone();
        let page = dduroc_read::PageLimits::new(
            page.max_records().min(self.inner.config.max_page_records),
            page.max_bytes().min(self.inner.config.max_page_bytes),
        )
        .expect("configured page limits are nonzero");
        let task = tokio::task::spawn_blocking(move || {
            let _job = job;
            let mut session = reader
                .open_session(query, limits, cancellation.clone())
                .map_err(ServiceError::Native)?;
            let page = session.next_page(page).map_err(ServiceError::Native)?;
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            let value = crate::types::convert_page(&reader, page, &presentation, &conversion)?;
            if cancellation.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            Ok(value)
        });
        let result = tokio::time::timeout(self.inner.config.operation_timeout, task)
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| ServiceError::WorkerUnavailable)??;
        guard.1 = false;
        Ok(result)
    }
}
