//! Owned query sessions and their admission state.
use dduroc_read::{Cancellation, QuerySession};
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::OwnedSemaphorePermit;

/// A newly opened opaque session capability and its first revision.
#[derive(Debug, Clone)]
pub struct SessionOpened {
    pub id: String,
    pub revision: u64,
}
/// A counted snapshot and its exact totals for fixed page dimensions.
#[derive(Debug, Clone)]
pub struct CountedSessionOpened {
    pub id: String,
    pub revision: u64,
    pub totals: dduroc_read::QueryTotals,
}
/// A page and the revision required by the next operation.
#[derive(Debug)]
pub struct SessionPage {
    pub revision: u64,
    pub page: crate::types::PageView,
}

#[derive(Debug)]
pub struct SessionSeekResult {
    pub reached_page: u64,
    pub target_reached: bool,
    pub exhausted: bool,
    pub page: Option<crate::types::PageView>,
}
#[derive(Debug)]
pub struct SessionSeekPage {
    pub revision: u64,
    pub result: SessionSeekResult,
}

pub(crate) struct Slot {
    pub state: Mutex<SlotState>,
    pub cancellation: Cancellation,
    pub created: Instant,
    pub presentation: crate::types::Presentation,
    // This lease is retained by the worker's Arc even if the registry entry is
    // removed while the native operation is still executing.
    pub _capacity: OwnedSemaphorePermit,
}
pub(crate) struct SlotState {
    pub session: Option<QuerySession>,
    pub revision: u64,
    pub busy: bool,
    pub closed: bool,
    pub touched: Instant,
}
impl Slot {
    pub fn new(
        capacity: OwnedSemaphorePermit,
        presentation: crate::types::Presentation,
    ) -> Arc<Self> {
        let now = Instant::now();
        Arc::new(Self {
            state: Mutex::new(SlotState {
                session: None,
                revision: 0,
                busy: true,
                closed: false,
                touched: now,
            }),
            cancellation: Cancellation::new(),
            created: now,
            presentation,
            _capacity: capacity,
        })
    }
    pub fn invalidate(&self) {
        self.cancellation.cancel();
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
            state.session.take();
        }
    }
}

pub(crate) fn random_id() -> Result<String, crate::error::ServiceError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|_| crate::error::ServiceError::WorkerUnavailable)?;
    let mut out = String::with_capacity(32);
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(out, "{byte:02x}");
    }
    Ok(out)
}
