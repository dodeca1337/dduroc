//! Deterministic independent tests of priority/drain and diagnostic boundaries.
use super::*;

fn fixture() -> (
    tempfile::TempDir,
    WriterLoop,
    Arc<Counters>,
    Arc<DropCounters>,
    NsId,
) {
    let directory = tempfile::tempdir().unwrap();
    let backend =
        ReadBackend::open_live(&[directory.path().to_owned()], &[64 << 20], &[4 << 20]).unwrap();
    let counters = Arc::new(Counters::default());
    let drops = Arc::new(DropCounters::new(2));
    let mut worker = WriterLoop {
        namespaces: HashMap::new(),
        next_id: 0,
        drops_seen: 0,
        buffered_bytes: 0,
        counters: counters.clone(),
        backend,
        pulse: Arc::new(crate::Pulse::new()),
        buffer_ceiling: None,
        active: HashSet::new(),
    };
    let ns = worker
        .register(NsSetup {
            name: "audit".into(),
            schema_name: "audit".into(),
            protocol_version: ProtocolVersion(1),
            store_id: 1,
            boot: BootCounter(1),
            drops: drops.clone(),
            channels: [StorageClass::Default, StorageClass::Critical]
                .into_iter()
                .map(|class| ChannelSpec {
                    dir: directory.path().join("audit"),
                    group: 0,
                    class,
                    root_index: 0,
                    quota_bytes: None,
                    config: ChannelConfig {
                        block_max_bytes: 1 << 20,
                        flush_interval: Duration::from_secs(3600),
                        ..if class == StorageClass::Critical {
                            ChannelConfig::critical(64 << 20)
                        } else {
                            ChannelConfig::new(64 << 20)
                        }
                    },
                })
                .collect(),
        })
        .unwrap();
    (directory, worker, counters, drops, ns)
}

fn queued(ns: NsId, channel: u16) -> Queued {
    Queued {
        item: Staged {
            ns,
            channel: ChannelIdx(channel),
            at: Micros(1),
            record: StagedRecord::Message {
                event: dduroc_format::EventId(1),
                span: None,
                payload: Default::default(),
            },
        },
        reply: None,
        _permit: QueuePermit {
            held: Arc::new(AtomicUsize::new(1)),
            bytes: 1,
        },
    }
}

#[test]
fn drop_notice_at_maximum_block_count_preserves_writer_and_records() {
    let (_directory, mut worker, counters, drops, ns) = fixture();
    for _ in 0..u16::MAX {
        worker.apply(queued(ns, 0)).unwrap();
    }
    assert_eq!(
        worker.namespaces[&(ns.0 as usize)][0].builder.count(),
        u16::MAX
    );
    drops.record(ChannelIdx(0));
    Counters::publish(&counters.dropped);
    worker
        .tick()
        .expect("a diagnostic must flush a full block before appending its notice");
    worker.barrier().unwrap();
    assert_eq!(counters.snapshot().records_written, u64::from(u16::MAX) + 1);
}

#[test]
fn release_does_not_abandon_normal_records_behind_a_long_critical_burst() {
    let (_directory, mut worker, counters, _drops, ns) = fixture();
    let other = worker
        .register(NsSetup {
            name: "priority".into(),
            schema_name: "audit".into(),
            protocol_version: ProtocolVersion(1),
            store_id: 1,
            boot: BootCounter(1),
            drops: Arc::new(DropCounters::new(1)),
            channels: vec![ChannelSpec {
                dir: PathBuf::new(),
                group: 0,
                class: StorageClass::Critical,
                root_index: 0,
                quota_bytes: None,
                config: ChannelConfig {
                    block_max_bytes: 1 << 20,
                    ..ChannelConfig::critical(64 << 20)
                },
            }],
        })
        .unwrap();
    let (normal_tx, normal) = crossbeam_channel::bounded(1);
    normal_tx.send(queued(ns, 0)).unwrap();
    // Preload the same traffic that continuously replenished bounded critical
    // producers can supply during Release. This removes scheduler timing from
    // the priority-starvation regression; production queue capacity is smaller.
    let burst = DRAIN_LIMIT * (DRAIN_ROUNDS + 1) + 1;
    let (critical_tx, critical) = crossbeam_channel::unbounded();
    for _ in 0..burst {
        critical_tx.send(queued(other, 0)).unwrap();
    }
    let (control_tx, control) = crossbeam_channel::bounded(2);
    control_tx.send(Control::Release(ns)).unwrap();
    let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded(1);
    control_tx.send(Control::Shutdown(shutdown_tx)).unwrap();
    worker.run(normal, critical, control);
    assert!(
        shutdown_rx.try_recv().is_ok(),
        "Release must leave the worker able to finish shutdown"
    );
    assert_eq!(counters.snapshot().io_errors, 0);
    assert_eq!(counters.snapshot().dropped, 0);
    assert_eq!(counters.snapshot().records_written, burst as u64 + 1);
}
