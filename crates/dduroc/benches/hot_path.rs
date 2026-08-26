//! Benchmarks of the hot paths.
//!
//! What is measured is what runs for every event on an application thread and
//! what runs for every block in the writer. The numbers from this machine
//! (x86-64) do not carry over to armv7 directly, but the ratios between the
//! variants do, and it is those that show where the bottleneck is.
//!
//! The store is written to `/dev/shm` where it is available: the goal is to
//! measure the cost of the code, not the speed of a developer's medium. The
//! durability tests live apart and measure exactly the medium.

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass};
use dduroc_format::block::{BlockBuilder, Compression};
use dduroc_format::record::{Message, Sample};
use dduroc_format::{EventId, MetricId, Micros, Record, Value, varint};
use std::hint::black_box;
use std::path::PathBuf;

dduroc::schema! {
    name: bench,
    version: 1,
    languages: [en],

    events {
        Tick = 0x01 {
            level: Info,
            en: "tick {n}",
            n: u32,
        },
        Measured = 0x02 {
            level: Info,
            en: "power {dbm} dBm at {ch}",
            dbm: f32,
            ch: u8,
        },
        Alarm = 0x03 {
            level: Error,
            store: critical,
            en: "alarm {code}",
            code: u16,
        },
    }

    metrics {
        Temp = 0x01 { type: f32, unit: "°C", tags: [thermal] },
    }

    spans {
        Work = 0x01,
    }
}

/// The directory for benchmarks: in memory where possible.
fn bench_root() -> PathBuf {
    let base = if PathBuf::from("/dev/shm").is_dir() {
        PathBuf::from("/dev/shm")
    } else {
        std::env::temp_dir()
    };
    let dir = base.join(format!("dduroc-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the benchmark directory");
    dir
}

fn store_config(root: &std::path::Path) -> StoreConfig {
    StoreConfig::new(root)
        .with_budget_per_class(256 * 1024 * 1024)
        // The ordinary channel's durability is turned off deliberately:
        // otherwise the benchmark would measure the medium's fdatasync rather
        // than the cost of our own code.
        .channel(
            StorageClass::Default,
            ChannelConfig {
                sync_interval: std::time::Duration::from_secs(3600),
                ..ChannelConfig::new(256 * 1024 * 1024)
            },
        )
        // The critical one is left as it is: immediacy is not a setting but its
        // definition, and the store refuses such a substitution at open time.
        // An application thread gains nothing from it anyway: the `fdatasync`
        // is the writer's, and what is measured here is the enqueueing.
        .channel(
            StorageClass::Critical,
            ChannelConfig::critical(64 * 1024 * 1024),
        )
}

// ════════════════════════════════════════════════════════════════════════════
// The format's codecs: what runs for every record
// ════════════════════════════════════════════════════════════════════════════

fn bench_format(c: &mut Criterion) {
    let mut g = c.benchmark_group("format");
    g.throughput(Throughput::Elements(1));

    g.bench_function("varint/write_small", |b| {
        let mut buf = Vec::with_capacity(16);
        b.iter(|| {
            buf.clear();
            varint::write_u64(&mut buf, black_box(127));
            black_box(&buf);
        });
    });

    g.bench_function("varint/read_small", |b| {
        let mut buf = Vec::new();
        varint::write_u64(&mut buf, 127);
        b.iter(|| varint::read_u64(black_box(&buf)).unwrap());
    });

    let payload = [0xABu8; 8];
    g.bench_function("record/encode_message", |b| {
        let rec = Record::Message(Message {
            event: EventId(1),
            span: None,
            payload: &payload,
        });
        let mut buf = Vec::with_capacity(64);
        b.iter(|| {
            buf.clear();
            dduroc_format::record::encode(black_box(&rec), 100, &mut buf).unwrap();
            black_box(&buf);
        });
    });

    g.bench_function("record/decode_message", |b| {
        let rec = Record::Message(Message {
            event: EventId(1),
            span: None,
            payload: &payload,
        });
        let mut buf = Vec::new();
        dduroc_format::record::encode(&rec, 100, &mut buf).unwrap();
        b.iter(|| dduroc_format::record::decode(black_box(&buf)).unwrap());
    });

    // Serializing an event's fields: the only part of the hot path an
    // application thread runs before enqueueing.
    g.bench_function("payload/encode_two_fields", |b| {
        b.iter(|| {
            let payload: dduroc::Payload = dduroc::postcard::to_extend(
                &bench::events::Measured {
                    dbm: black_box(27.5),
                    ch: black_box(3),
                },
                dduroc::Payload::new(),
            )
            .unwrap();
            black_box(payload)
        });
    });

    g.bench_function("payload/encode_one_field", |b| {
        b.iter(|| {
            let payload: dduroc::Payload = dduroc::postcard::to_extend(
                &bench::events::Tick { n: black_box(42) },
                dduroc::Payload::new(),
            )
            .unwrap();
            black_box(payload)
        });
    });

    g.bench_function("record/encode_sample", |b| {
        let rec = Record::Sample(Sample {
            metric: MetricId(3),
            value: Value::F32(36.6),
        });
        let mut buf = Vec::with_capacity(32);
        b.iter(|| {
            buf.clear();
            dduroc_format::record::encode(black_box(&rec), 250, &mut buf).unwrap();
            black_box(&buf);
        });
    });

    g.finish();
}

/// Assembling a whole block: the CRC and the compression are included.
fn bench_block(c: &mut Criterion) {
    let mut g = c.benchmark_group("block");
    const RECORDS: usize = 1000;
    g.throughput(Throughput::Elements(RECORDS as u64));

    let payload = [0xABu8; 8];
    let rec = Record::Message(Message {
        event: EventId(1),
        span: None,
        payload: &payload,
    });

    for (name, compression) in [
        ("build/none", Compression::None),
        ("build/lz4", Compression::Lz4),
    ] {
        g.bench_function(name, |b| {
            b.iter_batched(
                || {
                    (
                        BlockBuilder::with_capacity(64 * 1024),
                        Vec::with_capacity(64 * 1024),
                    )
                },
                |(mut builder, mut out)| {
                    for i in 0..RECORDS {
                        builder.push(Micros(i as u64 * 100), &rec).unwrap();
                    }
                    builder.finish(0, compression, &mut out).unwrap();
                    black_box(out)
                },
                BatchSize::SmallInput,
            );
        });
    }

    // The writer's steady-state path: the accumulator lives between flushes and
    // the buffers (the body and the LZ4 output) are reused. That is how a
    // channel works in a process; the cold-accumulator variant above is the
    // first flush after waking from idleness, which pays an allocation and an
    // initialization.
    g.bench_function("build/lz4_steady", |b| {
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        let mut seq = 0u32;
        b.iter(|| {
            out.clear();
            for i in 0..RECORDS {
                builder.push(Micros(i as u64 * 100), &rec).unwrap();
            }
            let h = builder.finish(seq, Compression::Lz4, &mut out).unwrap();
            seq = seq.wrapping_add(1);
            black_box(h.body_len)
        });
    });

    // Reading a block: decompression plus parsing every record.
    let mut builder = BlockBuilder::with_capacity(64 * 1024);
    for i in 0..RECORDS {
        builder.push(Micros(i as u64 * 100), &rec).unwrap();
    }
    let mut bytes = Vec::new();
    builder.finish(0, Compression::Lz4, &mut bytes).unwrap();

    g.bench_function("parse_and_iterate/lz4", |b| {
        b.iter(|| {
            let block = dduroc_format::Block::parse(black_box(&bytes))
                .unwrap()
                .unwrap();
            let mut n = 0usize;
            for item in block.records() {
                black_box(item.unwrap());
                n += 1;
            }
            assert_eq!(n, RECORDS);
        });
    });

    g.finish();
}

// ════════════════════════════════════════════════════════════════════════════
// The hot write path: what an application thread sees
// ════════════════════════════════════════════════════════════════════════════

fn bench_write(c: &mut Criterion) {
    let root = bench_root();
    let config = store_config(&root);
    let store = Store::open(config.clone()).expect("the store");
    let ns = store
        .namespace("bench-0", bench::SCHEMA)
        .expect("the namespace");
    let series = ns.series(bench::metrics::Temp).expect("the series");

    let mut g = c.benchmark_group("write");
    g.throughput(Throughput::Elements(1));

    // IMPORTANT about these numbers: criterion drives the calls faster than the
    // writer can drain the queue, so what is measured is the SATURATED mode — a
    // mixture of successful enqueues and fast refusals when the queue is full.
    // The numbers are useful for comparing the variants with one another but
    // not as "the cost of a write in production": there the queue is usually
    // empty. The meaningful throughput figure is the `throughput` group.
    g.bench_function("log/simple", |b| {
        let mut n = 0u32;
        b.iter(|| {
            n = n.wrapping_add(1);
            ns.log(bench::events::Tick { n: black_box(n) });
        });
    });

    g.bench_function("log/two_fields", |b| {
        b.iter(|| {
            ns.log(bench::events::Measured {
                dbm: black_box(27.5),
                ch: black_box(3),
            });
        });
    });

    g.bench_function("sample/f32", |b| {
        b.iter(|| {
            series.sample(black_box(36.6));
        });
    });

    g.bench_function("log/critical", |b| {
        b.iter(|| {
            ns.log(bench::events::Alarm { code: black_box(7) });
        });
    });

    // The unsaturated mode — what an application sees in real work: the queue
    // is empty and the writer keeps up. The batch is knowably smaller than the
    // queue, and the (unmeasured) preparation empties it.
    const BURST: usize = 1000;
    g.throughput(Throughput::Elements(BURST as u64));
    g.bench_function("burst/unsaturated_1k", |b| {
        b.iter_batched(
            || {
                ns.sync()
                    .expect("the queue is emptied before the measurement")
            },
            |()| {
                for i in 0..BURST {
                    ns.try_log(bench::events::Tick { n: i as u32 })
                        .expect("the queue must not overflow");
                }
            },
            BatchSize::PerIteration,
        );
    });

    g.throughput(Throughput::Elements(BURST as u64));
    g.bench_function("burst/samples_1k", |b| {
        b.iter_batched(
            || {
                ns.sync()
                    .expect("the queue is emptied before the measurement")
            },
            |()| {
                for i in 0..BURST {
                    series
                        .try_sample(20.0 + i as f32)
                        .expect("the queue is free");
                }
            },
            BatchSize::PerIteration,
        );
    });

    g.throughput(Throughput::Elements(1));
    g.bench_function("span/open_close", |b| {
        b.iter(|| {
            // The mode here is saturated, as in the rest of the group: a full
            // queue's refusal is normal and is part of the mixture being
            // measured. It is not visible at the call site either — the guard
            // is always handed out, and the loss is counted.
            let span = ns.span(bench::spans::Work);
            black_box(span.id());
        });
    });

    g.finish();
    store.shutdown();
    let _ = std::fs::remove_dir_all(&root);
}

/// End-to-end throughput: how many events a second reach the files, the
/// writer's work included.
fn bench_throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("throughput");
    const BATCH: usize = 10_000;
    g.throughput(Throughput::Elements(BATCH as u64));
    g.sample_size(20);

    g.bench_function("end_to_end/10k_events", |b| {
        b.iter_batched(
            || {
                let root = bench_root();
                let config = store_config(&root);
                let store = Store::open(config.clone()).unwrap();
                let ns = store.namespace("bench-0", bench::SCHEMA).unwrap();
                (root, store, ns)
            },
            |(root, store, ns)| {
                for i in 0..BATCH {
                    ns.log(bench::events::Tick { n: i as u32 });
                }
                ns.sync().unwrap();
                store.shutdown();
                let _ = std::fs::remove_dir_all(&root);
            },
            BatchSize::PerIteration,
        );
    });

    // Telemetry separately: it is the most frequent record there is, and it has
    // to be measured where everything else is — end to end. The `write` group's
    // numbers are deceptive for it: they depend on the application thread
    // contending with the writer for the queue buffer, and speeding the writer
    // up looks like a slowdown there.
    g.bench_function("end_to_end/10k_samples", |b| {
        b.iter_batched(
            || {
                let root = bench_root();
                let config = store_config(&root);
                let store = Store::open(config.clone()).unwrap();
                let ns = store.namespace("bench-0", bench::SCHEMA).unwrap();
                let series = ns.series(bench::metrics::Temp).unwrap();
                (root, store, series)
            },
            |(root, store, series)| {
                for i in 0..BATCH {
                    series.sample(20.0 + i as f32);
                }
                store.sync().unwrap();
                store.shutdown();
                let _ = std::fs::remove_dir_all(&root);
            },
            BatchSize::PerIteration,
        );
    });

    g.finish();
}

/// Scale: the cost of bringing up many namespaces.
///
/// The limit claimed is up to 24 thousand; a hundred is taken here to see the
/// cost of one and the linearity.
fn bench_scale(c: &mut Criterion) {
    let mut g = c.benchmark_group("scale");
    g.sample_size(10);

    for count in [10usize, 100] {
        g.throughput(Throughput::Elements(count as u64));
        g.bench_function(format!("open_namespaces/{count}"), |b| {
            b.iter_batched(
                bench_root,
                |root| {
                    let config = store_config(&root);
                    let store = Store::open(config.clone()).unwrap();
                    let mut namespaces = Vec::with_capacity(count);
                    for i in 0..count {
                        namespaces.push(
                            store
                                .namespace(&format!("orc-svc-{i}"), bench::SCHEMA)
                                .unwrap(),
                        );
                    }
                    // Empty namespaces must write nothing: a segment is created
                    // only by the first record.
                    black_box(&namespaces);
                    store.shutdown();
                    let _ = std::fs::remove_dir_all(&root);
                },
                BatchSize::PerIteration,
            );
        });
    }

    g.finish();
}

/// Reading: what it costs to bring up and merge records.
fn bench_read(c: &mut Criterion) {
    use dduroc_read::{KindFilter, Order, Query, Reader};

    let root = bench_root();
    let config = store_config(&root);
    let written = {
        let store = Store::open(config.clone()).unwrap();
        let mut written = 0u64;
        for inst in 0..4 {
            let ns = store
                .namespace(&format!("orc-radio-{inst}"), bench::SCHEMA)
                .unwrap();
            for i in 0..25_000u32 {
                // The filling goes with retries: an ordinary channel may lose
                // records under pressure, but the set to be read has to be
                // predictable.
                while ns.try_log(bench::events::Tick { n: i }).is_err() {
                    std::thread::yield_now();
                }
                written += 1;
            }
            ns.sync().unwrap();
        }
        store.shutdown();
        written
    };

    let mut g = c.benchmark_group("read");
    g.sample_size(10);
    g.throughput(Throughput::Elements(100_000));

    g.bench_function("query/all_100k", |b| {
        b.iter(|| {
            let reader = Reader::open_dump([&root], &[bench::SCHEMA]).unwrap();
            let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
            // There may be more read: failed try_sends leave a loss notice in
            // the stream. Fewer means records disappeared.
            assert!(
                result.entries.len() as u64 >= written,
                "{} read, {written} written",
                result.entries.len()
            );
            black_box(result.entries.len())
        });
    });

    g.throughput(Throughput::Elements(100));
    g.bench_function("query/newest_100", |b| {
        b.iter(|| {
            let reader = Reader::open_dump([&root], &[bench::SCHEMA]).unwrap();
            let result = reader
                .query(&Query::new().order(Order::Newest).limit(100))
                .unwrap();
            black_box(result.entries.len())
        });
    });

    g.throughput(Throughput::Elements(1));
    g.bench_function("query/errors_only", |b| {
        b.iter(|| {
            let reader = Reader::open_dump([&root], &[bench::SCHEMA]).unwrap();
            let result = reader
                .query(
                    &Query::new()
                        .min_level(dduroc::Level::Error)
                        .kinds(KindFilter::LOGS)
                        .order(Order::Oldest),
                )
                .unwrap();
            black_box(result.entries.len())
        });
    });

    g.finish();
    let _ = std::fs::remove_dir_all(&root);
}

criterion_group!(
    benches,
    bench_format,
    bench_block,
    bench_write,
    bench_throughput,
    bench_scale,
    bench_read
);
criterion_main!(benches);
