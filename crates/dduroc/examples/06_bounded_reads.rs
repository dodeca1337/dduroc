//! Bounded in-process pages and cooperative cancellation over a live reader.
//!
//! Run: `cargo run -p dduroc --example 06_bounded_reads`.
//! The session owns a fixed snapshot and its position. Its position does not
//! survive dropping the session or restarting this process.

#[cfg(feature = "read")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use dduroc::prelude::*;
    use dduroc::read::{Cancellation, Order, Query, ReadError, ReadLimits};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    dduroc::schema! {
        name: page_demo,
        version: 1,
        languages: [en],
        events {
            Reading = 1 { level: Info, en: "reading {sequence}", sequence: u32 }
        }
    }

    let directory = tempfile::tempdir()?;
    let store = Store::open(StoreConfig::new(directory.path()))?;
    // Explicitly trust the host clock for this demonstration. The UTC value
    // must describe the current time when record_sync is called.
    let start = Utc::now() - dduroc::chrono::Duration::seconds(1);
    store.record_sync(Utc::now(), SyncSource::User)?;
    let namespace = store.namespace("device", page_demo::SCHEMA)?;
    for sequence in 0..6 {
        namespace.try_log(page_demo::events::Reading { sequence })?;
    }
    // Ordinary success acknowledges queue admission. Flush before querying.
    namespace.sync()?;
    let reader = Arc::new(store.reader());
    let query = Query::new()
        .order(Order::Oldest)
        .time_window(start, Utc::now() + dduroc::chrono::Duration::seconds(1));
    let limits = ReadLimits {
        channels: 4,
        blocks: 8,
        compressed_bytes: 4 * 1024 * 1024,
        decoded_bytes: 4 * 1024 * 1024,
        examined_records: 64,
        returned_bytes: 1024 * 1024,
        deadline: Some(Instant::now() + Duration::from_secs(5)),
        ..ReadLimits::default()
    };

    let mut session = reader.open_session(query.clone(), limits.clone(), Cancellation::new())?;
    loop {
        // Page limits apply in addition to the session's cumulative limits.
        let page = session.next_page(dduroc::read::PageLimits::new(2, 64 * 1024)?)?;
        for entry in &page.entries {
            println!("{}", reader.render(entry, "en").unwrap_or_default());
        }
        // Report damage and missing UTC anchors alongside the entries. An
        // empty page alone is not a declaration of a complete result.
        println!(
            "damage: {}, unanchored boots: {}, truncated: {}",
            page.damaged.len(),
            page.unanchored.len(),
            page.truncated
        );
        if page.exhausted {
            break;
        }
    }

    let cancellation = Cancellation::new();
    let mut session = reader.open_session(query, limits, cancellation.clone())?;
    let _first_page = session.next_page(dduroc::read::PageLimits::new(2, 64 * 1024)?)?;
    // A request handler can signal its worker when the client disconnects.
    // Cancellation is cooperative; it does not interrupt an operating-system I/O call.
    cancellation.cancel();
    match session.next_page(dduroc::read::PageLimits::new(2, 64 * 1024)?) {
        Err(ReadError::Cancelled) => println!("cancelled without restarting the query"),
        Err(error) => return Err(error.into()),
        Ok(_) => return Err("cancelled session unexpectedly returned another page".into()),
    }
    Ok(())
}

#[cfg(not(feature = "read"))]
fn main() {
    eprintln!("Enable the dduroc read feature to run this example.");
}
