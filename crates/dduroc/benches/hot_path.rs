//! Бенчмарки горячих путей.
//!
//! Меряется то, что выполняется на каждое событие в прикладном потоке, и то,
//! что выполняется на каждый блок в writer'е. Числа с этой машины (x86-64)
//! не переносятся на armv7 напрямую, но соотношения между вариантами —
//! переносятся, и именно они показывают, где узкое место.
//!
//! Хранилище пишется в `/dev/shm`, если он доступен: цель — измерить
//! стоимость кода, а не скорость носителя разработчика. Тесты долговечности
//! живут отдельно и меряют как раз носитель.

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

/// Каталог для бенчмарков: по возможности в памяти.
fn bench_root() -> PathBuf {
    let base = if PathBuf::from("/dev/shm").is_dir() {
        PathBuf::from("/dev/shm")
    } else {
        std::env::temp_dir()
    };
    let dir = base.join(format!("dduroc-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("каталог для бенчмарков");
    dir
}

fn store_config(root: &std::path::Path) -> StoreConfig {
    StoreConfig::new(root)
        .with_budget_per_class(256 * 1024 * 1024)
        // Долговечность обычного канала отключена намеренно: иначе бенчмарк
        // мерил бы fdatasync носителя, а не стоимость собственного кода.
        .channel(
            StorageClass::Default,
            ChannelConfig {
                sync_interval: std::time::Duration::from_secs(3600),
                ..ChannelConfig::new(256 * 1024 * 1024)
            },
        )
        // А критический — как есть: немедленность не настройка, а его
        // определение, и хранилище такую подмену отвергает при открытии.
        // Прикладной поток от неё всё равно не выигрывает: `fdatasync` делает
        // writer, а измеряется здесь постановка в очередь.
        .channel(
            StorageClass::Critical,
            ChannelConfig::critical(64 * 1024 * 1024),
        )
}

// ════════════════════════════════════════════════════════════════════════════
// Кодеки формата: то, что выполняется на каждую запись
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

    // Сериализация полей события: единственная часть горячего пути, которую
    // выполняет прикладной поток до постановки в очередь.
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

/// Сборка блока целиком: сюда входят CRC и сжатие.
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

    // Стационарный путь writer'а: накопитель живёт между flush'ами, буферы
    // (тело и выход LZ4) переиспользуются. Именно так работает канал в
    // процессе; вариант с холодным накопителем выше — это первый flush после
    // пробуждения из бездействия, он платит аллокацию и инициализацию.
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

    // Чтение блока: распаковка + разбор всех записей.
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
// Горячий путь записи: то, что видит прикладной поток
// ════════════════════════════════════════════════════════════════════════════

fn bench_write(c: &mut Criterion) {
    let root = bench_root();
    let config = store_config(&root);
    let store = Store::open(config.clone()).expect("хранилище");
    let ns = store
        .namespace("bench-0", bench::SCHEMA)
        .expect("неймспейс");
    let series = ns.series(bench::metrics::Temp).expect("серия");

    let mut g = c.benchmark_group("write");
    g.throughput(Throughput::Elements(1));

    // ВАЖНО про эти числа: криterion гонит вызовы плотнее, чем writer
    // успевает разбирать очередь, поэтому измеряется НАСЫЩЕННЫЙ режим —
    // смесь удачных постановок в очередь и быстрых отказов при её
    // переполнении. Числа полезны для сравнения вариантов между собой, но
    // не как «стоимость записи в проде»: там очередь обычно пуста.
    // Осмысленная величина пропускной способности — группа `throughput`.
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

    // Ненасыщённый режим — то, что видит приложение в реальной работе:
    // очередь пуста, writer успевает. Пачка заведомо меньше очереди, а
    // подготовка (не измеряемая) её опустошает.
    const BURST: usize = 1000;
    g.throughput(Throughput::Elements(BURST as u64));
    g.bench_function("burst/unsaturated_1k", |b| {
        b.iter_batched(
            || ns.sync().expect("очередь опустошена перед замером"),
            |()| {
                for i in 0..BURST {
                    ns.try_log(bench::events::Tick { n: i as u32 })
                        .expect("очередь не должна переполняться");
                }
            },
            BatchSize::PerIteration,
        );
    });

    g.throughput(Throughput::Elements(BURST as u64));
    g.bench_function("burst/samples_1k", |b| {
        b.iter_batched(
            || ns.sync().expect("очередь опустошена перед замером"),
            |()| {
                for i in 0..BURST {
                    series
                        .try_sample(20.0 + i as f32)
                        .expect("очередь свободна");
                }
            },
            BatchSize::PerIteration,
        );
    });

    g.throughput(Throughput::Elements(1));
    g.bench_function("span/open_close", |b| {
        b.iter(|| {
            // Режим здесь насыщенный, как и у остальных замеров группы:
            // отказ переполненной очереди штатен и входит в измеряемую смесь.
            // Он и не виден на месте вызова — страж выдаётся всегда, а потеря
            // учтена счётчиком.
            let span = ns.span(bench::spans::Work);
            black_box(span.id());
        });
    });

    g.finish();
    store.shutdown();
    let _ = std::fs::remove_dir_all(&root);
}

/// Сквозная пропускная способность: сколько событий в секунду доходит до
/// файлов, включая работу writer'а.
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

    // Телеметрия отдельно: это самая частая запись, и мерить её надо там же,
    // где меряется всё остальное — в сквозном режиме. Числа группы `write`
    // для неё обманчивы: они зависят от состязания прикладного потока с
    // writer'ом за буфер очереди, и ускорение writer'а выглядит там
    // замедлением.
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

/// Масштаб: стоимость подъёма многих неймспейсов.
///
/// Заявленный предел — до 24 тысяч; здесь берётся сотня, чтобы увидеть
/// стоимость одного и линейность.
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
                    // Пустые неймспейсы не должны ничего писать: сегмент
                    // создаётся только при первой записи.
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

/// Чтение: сколько стоит поднять и слить записи.
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
                // Наполнение идёт с ретраями: обычный канал вправе терять
                // записи под давлением, но набор для чтения должен быть
                // предсказуемым.
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
            // Прочитанного может быть больше: неудачные try_send оставляют
            // в потоке отметку о потере. Меньше — значит записи исчезли.
            assert!(
                result.entries.len() as u64 >= written,
                "прочитано {}, записано {written}",
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
