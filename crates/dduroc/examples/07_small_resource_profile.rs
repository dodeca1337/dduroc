//! An illustrative small configuration, not a measured board/RSS guarantee.
//!
//! Queue counts, queue-owned byte accounting, writer buffers, retained class
//! budgets and cumulative read limits are independent. Backend indexes, schema
//! metadata, allocator overhead and application-owned data are additional.
//! Run: `cargo run -p dduroc --example 07_small_resource_profile`.

#[cfg(feature = "read")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use dduroc::prelude::*;
    use dduroc::read::{Cancellation, Order, Query, ReadLimits};
    use dduroc::{ChannelConfig, QueueByteLimits, QueueSizes, StorageClass};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    dduroc::schema! {
        name: small_profile,
        version: 1,
        languages: [en],
        events {
            Reading = 1 { level: Info, en: "reading {sequence}", sequence: u32 },
            Fault = 2 { level: Error, store: critical, en: "fault {code}", code: u16 }
        }
    }

    let directory = tempfile::tempdir()?;
    let mut config = StoreConfig::new(directory.path())
        .with_budget_per_class(4 * 1024 * 1024)
        .with_queues(QueueSizes {
            normal: 128,
            critical: 16,
        })
        .with_queue_byte_limits(QueueByteLimits {
            normal: 64 * 1024,
            critical: 16 * 1024,
        })
        .with_buffer_ceiling(64 * 1024);
    for class in StorageClass::ALL {
        let mut channel = if class == StorageClass::Critical {
            ChannelConfig::critical(4 * 1024 * 1024)
        } else {
            ChannelConfig::new(4 * 1024 * 1024)
        };
        channel.segment_bytes = 1024 * 1024; // Shared raft file target, not a namespace reservation.
        channel.block_max_bytes = 4096;
        channel.flush_interval = Duration::from_millis(50);
        if class != StorageClass::Critical {
            channel.sync_interval = Duration::from_millis(100);
        }
        config.channels.insert(class, channel);
    }
    // Each of the three classes has a 4 MiB logical retained budget. The
    // backend derives a separate physical root limit with rotation/rewrite
    // headroom; inspect its reported limit rather than treating 12 MiB as disk usage.
    let store = Store::open(config)?;
    let namespace = store.namespace("device", small_profile::SCHEMA)?;
    for sequence in 0..12 {
        namespace.try_log(small_profile::events::Reading { sequence })?;
    }
    namespace.try_log(small_profile::events::Fault { code: 7 })?; // Durable critical ACK.
    namespace.sync()?; // Ordinary admission alone was not a durable ACK.

    let reader = Arc::new(store.reader());
    let limits = ReadLimits {
        epoch_runs: 8,
        hardware_boots: 8,
        roster_entries: 16,
        channels: 4,
        blocks: 32,
        compressed_bytes: 256 * 1024,
        decoded_bytes: 512 * 1024,
        examined_records: 1024,
        returned_bytes: 256 * 1024,
        deadline: Some(Instant::now() + Duration::from_secs(5)),
    };
    let mut session = reader.open_session(
        Query::new().order(Order::Oldest),
        limits,
        Cancellation::new(),
    )?;
    loop {
        let page = session.next_page(dduroc::read::PageLimits::new(16, 32 * 1024)?)?;
        println!(
            "{} records, {} damaged blocks, truncated={}",
            page.entries.len(),
            page.damaged.len(),
            page.truncated
        );
        if page.exhausted {
            break;
        }
    }
    for usage in store.read_backend().usage()? {
        println!(
            "physical root limit={} bytes, currently allocated={} bytes",
            usage.physical_limit_bytes, usage.allocated_bytes
        );
    }
    Ok(())
}

#[cfg(not(feature = "read"))]
fn main() {
    eprintln!("Enable the dduroc read feature to run this example.");
}
