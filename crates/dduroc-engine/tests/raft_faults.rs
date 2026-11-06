//! EXP-0042: independent fault probes for the pinned raft-engine 0.4.2 backend.
//! These exercise disposable host files, not physical power-loss behavior.

use std::collections::BTreeMap;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission};
use raft_engine::{Config, Engine, LogBatch, ReadableSize, RecoveryMode};

type NativeHandle = <DefaultFileSystem as FileSystem>::Handle;
type NativeReader = <DefaultFileSystem as FileSystem>::Reader;

#[derive(Default)]
struct Faults {
    sync_calls: AtomicUsize,
    fail_sync: AtomicBool,
    fail_body_read_once: AtomicBool,
}

struct CountedHandle {
    inner: Arc<NativeHandle>,
    faults: Arc<Faults>,
}

impl Handle for CountedHandle {
    fn truncate(&self, offset: usize) -> io::Result<()> {
        self.inner.truncate(offset)
    }

    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }

    fn sync(&self) -> io::Result<()> {
        self.faults.sync_calls.fetch_add(1, Ordering::SeqCst);
        if self.faults.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::from_raw_os_error(5)); // EIO, Linux targets.
        }
        self.inner.sync()
    }
}

struct FaultReader {
    inner: NativeReader,
    faults: Arc<Faults>,
    offset: u64,
}

impl Read for FaultReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        // Let the intact file header through. Fail exactly one subsequent body
        // read, not EOF and not a fabricated checksum mismatch.
        if self.offset > 0
            && !buffer.is_empty()
            && self
                .faults
                .fail_body_read_once
                .swap(false, Ordering::SeqCst)
        {
            return Err(io::Error::from_raw_os_error(5));
        }
        let count = self.inner.read(buffer)?;
        self.offset += count as u64;
        Ok(count)
    }
}

impl Seek for FaultReader {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.offset = self.inner.seek(position)?;
        Ok(self.offset)
    }
}

struct FaultFileSystem(Arc<Faults>);

impl FileSystem for FaultFileSystem {
    type Handle = CountedHandle;
    type Reader = FaultReader;
    type Writer = <DefaultFileSystem as FileSystem>::Writer;

    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::Handle> {
        Ok(CountedHandle {
            inner: Arc::new(DefaultFileSystem.create(path)?),
            faults: self.0.clone(),
        })
    }

    fn open<P: AsRef<Path>>(&self, path: P, permission: Permission) -> io::Result<Self::Handle> {
        Ok(CountedHandle {
            inner: Arc::new(DefaultFileSystem.open(path, permission)?),
            faults: self.0.clone(),
        })
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        DefaultFileSystem.delete(path)
    }

    fn rename<P: AsRef<Path>>(&self, from: P, to: P) -> io::Result<()> {
        DefaultFileSystem.rename(from, to)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Reader> {
        Ok(FaultReader {
            inner: DefaultFileSystem.new_reader(handle.inner.clone())?,
            faults: self.0.clone(),
            offset: 0,
        })
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> io::Result<Self::Writer> {
        DefaultFileSystem.new_writer(handle.inner.clone())
    }
}

fn config(path: &Path) -> Config {
    Config {
        dir: path.to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::mb(1),
        purge_threshold: ReadableSize::mb(4),
        enable_log_recycle: false,
        recovery_mode: RecoveryMode::AbsoluteConsistency,
        ..Config::default()
    }
}

fn batch(key: &[u8], value: &[u8]) -> LogBatch {
    let mut batch = LogBatch::default();
    batch.put(1, key.to_vec(), value.to_vec()).unwrap();
    batch
}

fn file_bytes(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

#[test]
fn a_nonempty_barrier_syncs_but_the_pinned_empty_sync_does_not() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("engine");
    let faults = Arc::new(Faults::default());
    let engine =
        Engine::open_with_file_system(config(&path), Arc::new(FaultFileSystem(faults.clone())))
            .unwrap();
    let before = faults.sync_calls.load(Ordering::SeqCst);
    engine
        .write(&mut batch(b"ordinary", b"payload"), false)
        .unwrap();
    engine.sync().unwrap();
    assert_eq!(faults.sync_calls.load(Ordering::SeqCst), before);
    engine.write(&mut batch(b"barrier", b"1"), true).unwrap();
    assert!(faults.sync_calls.load(Ordering::SeqCst) > before);
    drop(engine);
    let reopened = Engine::open(config(&path)).unwrap();
    assert_eq!(reopened.get(1, b"ordinary"), Some(b"payload".to_vec()));
}

#[test]
fn strict_recovery_propagates_body_eio_without_changing_any_file() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("engine");
    let engine = Engine::open(config(&path)).unwrap();
    engine
        .write(&mut batch(b"acked", b"durable"), true)
        .unwrap();
    drop(engine);
    let before = file_bytes(&path);
    let faults = Arc::new(Faults::default());
    faults.fail_body_read_once.store(true, Ordering::SeqCst);
    let opened =
        Engine::open_with_file_system(config(&path), Arc::new(FaultFileSystem(faults.clone())));
    assert!(matches!(opened, Err(raft_engine::Error::Io(_))));
    assert!(!faults.fail_body_read_once.load(Ordering::SeqCst));
    assert_eq!(file_bytes(&path), before);
    let reopened = Engine::open(config(&path)).unwrap();
    assert_eq!(reopened.get(1, b"acked"), Some(b"durable".to_vec()));
}

#[test]
fn strict_recovery_refuses_a_torn_tail_without_repairing_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("engine");
    let engine = Engine::open(config(&path)).unwrap();
    engine
        .write(&mut batch(b"acked", b"durable"), true)
        .unwrap();
    engine
        .write(&mut batch(b"tail", b"ordinary"), false)
        .unwrap();
    drop(engine);
    let mut logs: Vec<_> = file_bytes(&path)
        .into_keys()
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "raftlog")
        })
        .collect();
    logs.sort();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(logs.last().unwrap())
        .unwrap();
    file.set_len(file.metadata().unwrap().len() - 3).unwrap();
    file.sync_all().unwrap();
    let damaged = file_bytes(&path);
    assert!(matches!(
        Engine::open(config(&path)),
        Err(raft_engine::Error::Corruption(_))
    ));
    assert_eq!(file_bytes(&path), damaged);
}

const FAULT_CHILD: &str = "DDUROC_RAFT_SYNC_FAULT_CHILD";

#[test]
fn sync_failure_child() {
    let Some(path) = std::env::var_os(FAULT_CHILD) else {
        return;
    };
    let faults = Arc::new(Faults::default());
    let engine = Engine::open_with_file_system(
        config(Path::new(&path)),
        Arc::new(FaultFileSystem(faults.clone())),
    )
    .unwrap();
    engine
        .write(&mut batch(b"prior", b"durable"), true)
        .unwrap();
    faults.fail_sync.store(true, Ordering::SeqCst);
    let result = engine.write(&mut batch(b"critical", b"pending"), true);
    assert!(result.is_ok());
    println!("CRITICAL_DURABLE_ACK");
}

#[test]
fn sync_failure_terminates_without_acknowledging_the_critical_write() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("engine");
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "sync_failure_child", "--nocapture"])
        .env(FAULT_CHILD, &path)
        .current_dir(directory.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("CRITICAL_DURABLE_ACK"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("Input/output error"));
    let reopened = Engine::open(config(&path)).unwrap();
    assert_eq!(reopened.get(1, b"prior"), Some(b"durable".to_vec()));
}

#[derive(Debug)]
struct Entry;
impl raft_engine::MessageExt for Entry {
    type Entry = protobuf::well_known_types::BytesValue;
    fn index(entry: &Self::Entry) -> u64 {
        u64::from_le_bytes(entry.value[..8].try_into().unwrap())
    }
}

fn append_equal_geometry(engine: &Engine, byte: u8) {
    let mut entry = protobuf::well_known_types::BytesValue::new();
    entry.value.extend_from_slice(&1u64.to_le_bytes());
    entry.value.extend_from_slice(&[byte; 32]);
    let mut batch = LogBatch::default();
    batch.add_entries::<Entry>(1, &[entry]).unwrap();
    engine.write(&mut batch, true).unwrap();
}

#[test]
fn pinned_upstream_cache_aliases_equal_geometry_across_roots() {
    let directory = tempfile::tempdir().unwrap();
    let first = Engine::open(config(&directory.path().join("first"))).unwrap();
    let second = Engine::open(config(&directory.path().join("second"))).unwrap();
    append_equal_geometry(&first, b'A');
    append_equal_geometry(&second, b'B');
    let a = first.get_entry::<Entry>(1, 1).unwrap().unwrap();
    let b = second.get_entry::<Entry>(1, 1).unwrap().unwrap();
    // Negative control: the pinned released dependency is NOT corrected.
    // Its TLS key omits engine identity. Public dduroc reads must isolate roots
    // on fresh dedicated workers; migration_acceptance checks that boundary.
    assert_eq!(&a.value[8..], &[b'A'; 32]);
    assert_eq!(&b.value[8..], &[b'A'; 32]);
    let actual_b = std::thread::spawn(move || second.get_entry::<Entry>(1, 1).unwrap().unwrap())
        .join()
        .unwrap();
    assert_eq!(&actual_b.value[8..], &[b'B'; 32]);
}
