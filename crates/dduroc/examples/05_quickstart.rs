use dduroc::prelude::*;
use dduroc::{NsQuota, StorageClass};

dduroc::schema! {
    name: radio,
    version: 1,
    languages: [en],
    events {
        PowerSet = 1 {
            level: Info,
            store: critical,
            en: "power set to {dbm} dBm",
            dbm: f32,
        },
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::args_os().nth(1).ok_or("pass a log directory")?;
    let store = Store::open(StoreConfig::new(root).with_budget_per_class(16 << 20))?;
    let radio = store.namespace_with_quota(
        "radio-0",
        radio::SCHEMA,
        NsQuota::new().limit_bytes(StorageClass::Critical, 1 << 20),
    )?;
    radio.try_log(radio::events::PowerSet { dbm: 27.5 })?;
    // Critical success means the synchronous storage write completed.
    // Ordinary events need Store::sync() or their configured periodic barrier.
    store.sync()?;
    Ok(())
}
