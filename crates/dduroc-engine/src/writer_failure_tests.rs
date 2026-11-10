//! Independent deterministic accounting at the worker's terminal boundary.
//! Existing public Store fault tests separately exercise ACK and reopen behavior.
use super::*;

#[derive(Clone, Copy)]
enum FailurePoint {
    CriticalFlush,
    BufferCeiling,
    FullChannel,
}
fn queued(channel: u16, reply: Option<Sender<Result<()>>>) -> Queued {
    Queued {
        item: Staged {
            ns: NsId(0),
            channel: ChannelIdx(channel),
            at: Micros(1),
            record: StagedRecord::Text {
                level: dduroc_format::Level::Info,
                span: None,
                target: "test".into(),
                text: "accepted".into(),
            },
        },
        reply,
        _permit: QueuePermit {
            held: Arc::new(AtomicUsize::new(1)),
            bytes: 1,
        },
    }
}
fn terminal_accounting(point: FailurePoint, after_sync: bool) {
    let (completed, done) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let directory = tempfile::tempdir().unwrap();
        let backend =
            ReadBackend::open_live(&[directory.path().to_owned()], &[64 << 20], &[4 << 20])
                .unwrap();
        let counters = Arc::new(Counters::default());
        let drops = Arc::new(DropCounters::new(3));
        let mut worker = WriterLoop {
            namespaces: HashMap::new(),
            next_id: 0,
            drops_seen: 0,
            buffered_bytes: 0,
            counters: counters.clone(),
            backend: backend.clone(),
            pulse: Arc::new(crate::pulse::Pulse::new()),
            buffer_ceiling: None,
            active: HashSet::new(),
        };
        let channels = [
            StorageClass::Default,
            StorageClass::Critical,
            StorageClass::Telemetry,
        ]
        .into_iter()
        .map(|class| ChannelSpec {
            dir: directory.path().join("radio"),
            group: 0,
            class,
            root_index: 0,
            quota_bytes: None,
            config: if class == StorageClass::Critical {
                ChannelConfig::critical(16 << 20)
            } else {
                ChannelConfig::new(16 << 20)
            },
        })
        .collect();
        assert_eq!(
            worker
                .register(NsSetup {
                    name: "radio".into(),
                    schema_name: "accounting".into(),
                    protocol_version: ProtocolVersion(1),
                    store_id: 1,
                    boot: BootCounter(1),
                    channels,
                    drops: drops.clone(),
                })
                .unwrap(),
            NsId(0)
        );
        for _ in 0..2 {
            worker.apply(queued(0, None)).unwrap();
        }
        for _ in 0..3 {
            worker.apply(queued(2, None)).unwrap();
        }
        // Five records have left the queues and now occupy two builders.
        assert_eq!(counters.snapshot().dropped, 0);
        assert_eq!(counters.snapshot().records_written, 0);
        match point {
            FailurePoint::BufferCeiling => {
                worker.buffer_ceiling = Some(1);
                for channel in worker.namespaces.get_mut(&0).unwrap() {
                    channel.config.sync_interval = Duration::ZERO;
                }
            }
            FailurePoint::FullChannel => {
                worker.namespaces.get_mut(&0).unwrap()[0]
                    .config
                    .block_max_bytes = 1;
                worker.namespaces.get_mut(&0).unwrap()[0]
                    .config
                    .sync_interval = Duration::ZERO;
            }
            FailurePoint::CriticalFlush => {}
        }
        backend.inject_commit_fault(if after_sync {
            crate::backend::CommitFault::AfterEio
        } else {
            crate::backend::CommitFault::BeforeEio
        });
        let (normal_send, normal) = crossbeam_channel::bounded(4);
        for _ in 0..4 {
            normal_send.send(queued(0, None)).unwrap();
        }
        let (critical_send, critical) = crossbeam_channel::bounded(1);
        let (reply, ack) = crossbeam_channel::bounded(1);
        let current_channel = if matches!(point, FailurePoint::FullChannel) {
            0
        } else {
            1
        };
        critical_send
            .send(queued(current_channel, Some(reply)))
            .unwrap();
        let (_control_send, control) = crossbeam_channel::bounded(1);
        worker.run(normal, critical, control);
        // 5 builder records + 1 current/failed-block record + 4 queued records.
        // An after-sync error is counted as unconfirmed, not proven absent.
        assert_eq!(counters.snapshot().dropped, 10);
        assert_eq!(counters.snapshot().io_errors, 1);
        assert_eq!(
            drops.take(ChannelIdx(0)),
            if current_channel == 0 { 7 } else { 6 }
        );
        assert_eq!(
            drops.take(ChannelIdx(1)),
            if current_channel == 1 { 1 } else { 0 }
        );
        assert_eq!(drops.take(ChannelIdx(2)), 3);
        assert!(
            !matches!(ack.try_recv(), Ok(Ok(()))),
            "terminal failure must never acknowledge the current record"
        );
        assert!(backend.cached_statistics().is_err());
        completed.send(()).unwrap();
    });
    done.recv_timeout(Duration::from_secs(10))
        .expect("worker did not terminate after its injected failure");
    worker.join().unwrap();
}
#[test]
fn terminal_failure_counts_queued_failed_block_and_other_builders_once() {
    terminal_accounting(FailurePoint::CriticalFlush, false);
}
#[test]
fn ambiguous_sync_counts_failed_block_once_without_claiming_it_is_absent() {
    terminal_accounting(FailurePoint::CriticalFlush, true);
}
#[test]
fn buffer_ceiling_failure_also_counts_the_dequeued_unencoded_record() {
    terminal_accounting(FailurePoint::BufferCeiling, false);
}
#[test]
fn full_channel_preflush_failure_also_counts_the_dequeued_unencoded_record() {
    terminal_accounting(FailurePoint::FullChannel, false);
}
