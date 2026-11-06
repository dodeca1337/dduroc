# dduroc

Compact typed event logs and telemetry for embedded Linux, written in Rust.
Events store stable IDs, relative timestamps and binary fields. Text templates,
field names, severity and static tags live in your compiled schema and are
resolved when reading. The store uses Raft Engine 0.4.2 with bounded block
queues, namespace quotas and explicit retention.

## Getting started

Requires Rust 1.88 or newer and Linux. ARMv7 cross-compilation is checked; actual
flash latency and power-loss behavior must be validated on your hardware.

```toml
[dependencies]
dduroc = "0.1.0"
```

The example takes a storage directory as its first argument:

```rust
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
```

From the repository, run
`cargo run -p dduroc --example 05_quickstart -- /path/to/journal`.
The compiled examples also cover [reading](https://github.com/dodeca1337/dduroc/blob/v0.1.0/crates/dduroc/examples/02_viewer_reads.rs),
schema migrations, spans, metrics and channel policies.

The `read` feature is enabled by default. Firmware that only writes can use
`dduroc = { version = "0.1.0", default-features = false }`.

## Durability and limits

- A successful critical `try_log` waits for a synchronous storage write.
  Ordinary calls acknowledge queue admission and use periodic synchronization
  (10 seconds by default), or an explicit `Store::sync()`.
- Storage errors are returned; callers that require confirmation must handle
  them. An unconfirmed write may still appear after restart, so retries are
  not automatically exactly-once. Hardware must honor filesystem sync.
- Class budgets are shared across namespaces; personal quotas limit each
  namespace within its class. Closed namespaces remain accounted for. Retention
  deliberately expires old records, including critical records.
- Logical retained bytes differ from allocated disk space. Each physical root
  has explicit admission headroom and a class-budget sum limit of 1 GiB. RAM
  includes bounded queues, active buffers, retained indexes and reader pages.
  A broad query holds a decoded block per selected channel.
- Roots are exclusively owned while open and must not overlap. Live readers
  share the backend; offline Raft readers need exclusive engine access.
- Recovery is strict: an incomplete or corrupt tail can prevent opening rather
  than trigger destructive repair. Legacy segment stores are refused for
  writing and need explicit export/import into a new root.

The integration compensates for pinned upstream empty-sync and cross-engine
cache behavior. The tests cover process crashes, injected I/O faults, quota
persistence, retention, concurrent readers and resource churn. These tests do
not establish physical power-cut safety on an SD card or eMMC device.

## Workspace

| Crate | Purpose |
|---|---|
| `dduroc` | Application facade, schema macro and optional reader |
| `dduroc-engine` | Writer, storage, durability, namespaces and retention |
| `dduroc-format` | Compact record/block codec and legacy container codec |
| `dduroc-read` | Queries, stream merging, schema decoding and time resolution |
| `dduroc-macros` | Compile-time schema declaration |

See the repository's [design](https://github.com/dodeca1337/dduroc/blob/v0.1.0/DESIGN.md),
[format specification](https://github.com/dodeca1337/dduroc/blob/v0.1.0/SPEC.md)
and [measurements](https://github.com/dodeca1337/dduroc/blob/v0.1.0/BENCHMARKS.md).

## License

Licensed under either Apache License 2.0 or MIT, at your option.
