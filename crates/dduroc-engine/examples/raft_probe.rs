//! Reproducible raft-engine 0.4.2 suitability probe (EXP-0042).
//! Run only against a new, empty disposable directory:
//! `cargo run -p dduroc-engine --example raft_probe -- suite <fixture-directory>`.
//! The suite deliberately kills child processes and corrupts its own fixtures.
//! It characterizes both the unsafe upstream default recovery and explicit
//! write barriers. Production uses AbsoluteConsistency, covered by raft_faults.
//! This is process-crash evidence, not physical power-loss qualification.
use protobuf::well_known_types::BytesValue;
use raft_engine::env::{DefaultFileSystem, FileSystem, Handle, Permission, WriteExt};
use raft_engine::{Command as RaftCommand, Config, Engine, LogBatch, MessageExt, ReadableSize};
use serde_json::{Value, json};
use std::{
    fs,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
type DefaultEngine = Engine;
struct Block;
impl MessageExt for Block {
    type Entry = BytesValue;
    fn index(entry: &BytesValue) -> u64 {
        u64::from_le_bytes(entry.value[..8].try_into().unwrap())
    }
}
fn entry(index: u64, size: usize) -> BytesValue {
    let mut e = BytesValue::new();
    e.value = (0..size.max(8))
        .map(|n| ((n as u64 * 17 + index * 31) % 251) as u8)
        .collect();
    e.value[..8].copy_from_slice(&index.to_le_bytes());
    e
}
fn config(path: &Path) -> Config {
    Config {
        dir: path.to_string_lossy().into(),
        target_file_size: ReadableSize::kb(64),
        purge_threshold: ReadableSize::kb(256),
        purge_rewrite_threshold: Some(ReadableSize::kb(128)),
        batch_compression_threshold: ReadableSize(0),
        enable_log_recycle: false,
        recovery_threads: 1,
        ..Config::default()
    }
}
fn append(engine: &DefaultEngine, group: u64, index: u64, size: usize, sync: bool) -> Result<()> {
    let mut batch = LogBatch::default();
    batch.add_entries::<Block>(group, &[entry(index, size)])?;
    engine.write(&mut batch, sync)?;
    Ok(())
}
fn check(engine: &DefaultEngine, group: u64, start: u64, end: u64, size: usize) -> Result<usize> {
    let mut found = vec![];
    engine.fetch_entries_to::<Block>(group, start, end, None, &mut found)?;
    assert_eq!(found.len() as u64, end - start, "group {group} entry count");
    for (n, e) in found.iter().enumerate() {
        assert_eq!(e, &entry(start + n as u64, size), "group {group} payload");
    }
    Ok(found.len())
}
fn files(path: &Path) -> Vec<PathBuf> {
    let mut out: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|p| p.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("raftlog"))
        .collect();
    out.sort();
    out
}
fn disk(path: &Path) -> Value {
    let mut bytes = 0;
    let mut allocated = 0;
    let mut count = 0;
    for f in fs::read_dir(path).unwrap() {
        let m = f.unwrap().metadata().unwrap();
        if m.is_file() {
            bytes += m.len();
            allocated += m.blocks() * 512;
            count += 1;
        }
    }
    json!({"logical_bytes":bytes,"allocated_bytes":allocated,"files":count})
}
fn rss() -> usize {
    fs::read_to_string("/proc/self/status")
        .unwrap()
        .lines()
        .find(|l| l.starts_with("VmRSS:"))
        .unwrap()
        .split_whitespace()
        .nth(1)
        .unwrap()
        .parse()
        .unwrap()
}
fn emit(v: Value) {
    println!("{v}");
    io::stdout().flush().unwrap();
}
fn child_writer(path: &Path, sync: bool) -> Result<()> {
    let engine = Engine::open(config(path))?;
    let start = engine.last_index(1).unwrap_or(0) + 1;
    for i in start..start + 10000 {
        append(&engine, 1, i, 1024, sync)?;
        println!("ACK {i}");
        io::stdout().flush()?;
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    Ok(())
}
fn kill_reopen(root: &Path, sync: bool) -> Result<Value> {
    let path = root.join(if sync { "durable" } else { "unsynced" });
    fs::create_dir(&path)?;
    let mut acked = 0;
    let mut recovered = 0;
    let mut rounds = vec![];
    for _ in 0..5 {
        let mut child = Command::new(std::env::current_exe()?)
            .args([
                "writer",
                path.to_str().unwrap(),
                if sync { "true" } else { "false" },
            ])
            .stdout(Stdio::piped())
            .spawn()?;
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut last = 0;
        for _ in 0..32 {
            let l = lines.next().ok_or("child ended before ACK")??;
            assert!(l.starts_with("ACK "));
            last = l[4..].parse()?;
            acked += 1;
        }
        child.kill()?;
        let status = child.wait()?;
        assert!(!status.success());
        let engine = Engine::open(config(&path))?;
        recovered = engine.last_index(1).unwrap_or(0);
        assert!(recovered >= last, "acknowledged entry missing");
        check(&engine, 1, 1, recovered + 1, 1024)?;
        rounds.push(json!({"last_observed_ack":last,"recovered_last":recovered}));
    }
    Ok(
        json!({"scenario":if sync{"sync_ack_sigkill_reopen"}else{"unsynced_sigkill_observation"},"acknowledged_count":acked,"recovered_last":recovered,"rounds":rounds,"disk":disk(&path),"powerloss_simulated":false}),
    )
}
fn retention(root: &Path) -> Result<Value> {
    let path = root.join("retention");
    fs::create_dir(&path)?;
    let engine = Engine::open(config(&path))?;
    let groups = [(1, 16, 1024), (2, 96, 2048), (3, 96, 4096)];
    let mut last = [0u64; 3];
    let mut samples = vec![];
    let mut written = 0usize;
    let mut maxrss = rss();
    let start = Instant::now();
    for cycle in 0..100 {
        for (slot, (group, keep, size)) in groups.iter().enumerate() {
            if *group == 3 && cycle > 0 {
                continue;
            }
            for _ in 0..if *group == 3 { 192 } else { 48 } {
                last[slot] += 1;
                append(&engine, *group, last[slot], *size, false)?;
                written += *size;
            }
            let first = last[slot].saturating_sub(*keep - 1).max(1);
            let mut batch = LogBatch::default();
            batch.add_command(*group, RaftCommand::Compact { index: first });
            engine.write(&mut batch, true)?;
        }
        let hints = engine.purge_expired_files()?;
        maxrss = maxrss.max(rss());
        if cycle % 10 == 9 {
            samples.push(json!({"cycle":cycle+1,"disk":disk(&path),"engine_used_bytes":engine.get_used_size(),"rss_kib":rss(),"gc_hint_groups":hints}));
        }
    }
    for (slot, (group, keep, size)) in groups.iter().enumerate() {
        let first = last[slot].saturating_sub(*keep - 1).max(1);
        assert_eq!(engine.first_index(*group), Some(first));
        check(&engine, *group, first, last[slot] + 1, *size)?;
    }
    let retained_before: Vec<_> = groups
        .iter()
        .map(|(g, _, _)| (*g, engine.first_index(*g), engine.last_index(*g)))
        .collect();
    drop(engine);
    let reopened = Engine::open(config(&path))?;
    for (g, first, last) in &retained_before {
        assert_eq!(reopened.first_index(*g), *first);
        assert_eq!(reopened.last_index(*g), *last);
    }
    let sizes: Vec<u64> = samples
        .iter()
        .skip(3)
        .map(|v| v["disk"]["allocated_bytes"].as_u64().unwrap())
        .collect();
    let min = *sizes.iter().min().unwrap();
    let max = *sizes.iter().max().unwrap();
    assert!(
        max < 4 * 1024 * 1024,
        "physical footprint failed bounded plateau"
    );
    Ok(
        json!({"scenario":"mixed_group_retention_purge_with_dormant_group","payload_written_bytes":written,"retained_group_ranges":retained_before,"samples":samples,"steady_allocated_min":min,"steady_allocated_max":max,"peak_rss_kib":maxrss,"elapsed_ms":start.elapsed().as_millis()}),
    )
}
fn resource(path: &Path, size: usize) -> Result<Value> {
    fs::create_dir(path)?;
    let engine = Engine::open(config(path))?;
    let baseline = rss();
    let count = 8 * 1024 * 1024 / size;
    let start = Instant::now();
    for i in 1..=count {
        append(&engine, 1, i as u64, size, false)?;
    }
    let peak = rss();
    let mut b = LogBatch::default();
    b.put(0, b"barrier".to_vec(), vec![1])?;
    engine.write(&mut b, true)?;
    let before = disk(path);
    let mut b = LogBatch::default();
    b.add_command(
        1,
        RaftCommand::Compact {
            index: (count + 1) as u64,
        },
    );
    engine.write(&mut b, true)?;
    for _ in 0..4 {
        engine.purge_expired_files()?;
    }
    Ok(
        json!({"scenario":"block_size_index_resource","block_bytes":size,"entries":count,"payload_bytes":size*count,"baseline_rss_kib":baseline,"retained_rss_kib":peak,"rss_delta_kib":peak.saturating_sub(baseline),"after_compact_rss_kib":rss(),"before_compact_disk":before,"after_compact_disk":disk(path),"elapsed_ms":start.elapsed().as_millis()}),
    )
}
fn corruption(root: &Path) -> Result<Vec<Value>> {
    let mut reports = vec![];
    for case in ["tail", "middle", "middle-index"] {
        let path = root.join(format!("corrupt-{case}"));
        fs::create_dir(&path)?;
        {
            let e = Engine::open(config(&path))?;
            for i in 1..=160 {
                append(&e, 1, i, 2048, true)?;
            }
        }
        let ff = files(&path);
        assert!(ff.len() > 2);
        let target = if case == "tail" {
            ff.last().unwrap()
        } else {
            ff.first().unwrap()
        };
        let len = fs::metadata(target)?.len();
        let mut file = fs::OpenOptions::new().write(true).read(true).open(target)?;
        if case == "tail" {
            file.set_len(len - 17)?;
        } else {
            let offset = if case == "middle-index" { len - 8 } else { 100 };
            file.seek(SeekFrom::Start(offset))?;
            let mut byte = [0];
            file.read_exact(&mut byte)?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&[byte[0] ^ 0x7f])?;
        }
        file.sync_all()?;
        drop(file);
        let result = Engine::open(config(&path));
        if case != "tail" {
            match result {
                Err(err) => reports.push(
                    json!({"scenario":format!("corruption_{case}"),"open_error":err.to_string()}),
                ),
                Ok(e) => {
                    let mut entries = vec![];
                    let read = e.fetch_entries_to::<Block>(1, 1, 2, None, &mut entries);
                    assert!(read.is_err(), "middle corrupted payload silently read");
                    reports.push(json!({"scenario":format!("corruption_{case}"),"open_succeeded":true,"read_error":read.err().unwrap().to_string()}));
                }
            }
        } else {
            let e = result?;
            let last = e.last_index(1).unwrap_or(0);
            assert!(last < 160);
            check(&e, 1, 1, last + 1, 2048)?;
            reports.push(json!({"scenario":"truncated_tail","synced_entries_before_external_damage":160,"recovered_prefix":last,"lost_by_manual_truncation":160-last,"recovery_mode":"TolerateTailCorruption"}));
        }
    }
    Ok(reports)
}

// Faults affect only this wrapper's files after explicitly arming the state.
type DH = <DefaultFileSystem as FileSystem>::Handle;
type DR = <DefaultFileSystem as FileSystem>::Reader;
type DW = <DefaultFileSystem as FileSystem>::Writer;
#[derive(Default)]
struct FaultState {
    mode: AtomicUsize,
    syncs: AtomicUsize,
}
struct FH {
    inner: Arc<DH>,
    state: Arc<FaultState>,
}
impl Handle for FH {
    fn truncate(&self, n: usize) -> io::Result<()> {
        self.inner.truncate(n)
    }
    fn file_size(&self) -> io::Result<usize> {
        self.inner.file_size()
    }
    fn sync(&self) -> io::Result<()> {
        self.state.syncs.fetch_add(1, Ordering::SeqCst);
        if self.state.mode.load(Ordering::SeqCst) == 2 {
            return Err(io::Error::from_raw_os_error(5));
        }
        self.inner.sync()
    }
}
struct FW {
    inner: DW,
    state: Arc<FaultState>,
}
impl Write for FW {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if self.state.mode.load(Ordering::SeqCst) == 1 {
            return Err(io::Error::from_raw_os_error(28));
        }
        self.inner.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl Seek for FW {
    fn seek(&mut self, p: SeekFrom) -> io::Result<u64> {
        self.inner.seek(p)
    }
}
impl WriteExt for FW {
    fn truncate(&mut self, n: usize) -> io::Result<()> {
        self.inner.truncate(n)
    }
    fn allocate(&mut self, o: usize, n: usize) -> io::Result<()> {
        self.inner.allocate(o, n)
    }
}
struct FaultFs {
    state: Arc<FaultState>,
}
impl FileSystem for FaultFs {
    type Handle = FH;
    type Reader = DR;
    type Writer = FW;
    fn create<P: AsRef<Path>>(&self, p: P) -> io::Result<FH> {
        Ok(FH {
            inner: Arc::new(DefaultFileSystem.create(p)?),
            state: self.state.clone(),
        })
    }
    fn open<P: AsRef<Path>>(&self, p: P, perm: Permission) -> io::Result<FH> {
        Ok(FH {
            inner: Arc::new(DefaultFileSystem.open(p, perm)?),
            state: self.state.clone(),
        })
    }
    fn delete<P: AsRef<Path>>(&self, p: P) -> io::Result<()> {
        DefaultFileSystem.delete(p)
    }
    fn rename<P: AsRef<Path>>(&self, a: P, b: P) -> io::Result<()> {
        DefaultFileSystem.rename(a, b)
    }
    fn new_reader(&self, h: Arc<FH>) -> io::Result<DR> {
        DefaultFileSystem.new_reader(h.inner.clone())
    }
    fn new_writer(&self, h: Arc<FH>) -> io::Result<FW> {
        Ok(FW {
            inner: DefaultFileSystem.new_writer(h.inner.clone())?,
            state: self.state.clone(),
        })
    }
}
fn fault_child(path: &Path, mode: usize) -> Result<()> {
    let state = Arc::new(FaultState::default());
    let e = Engine::open_with_file_system(
        config(path),
        Arc::new(FaultFs {
            state: state.clone(),
        }),
    )?;
    let mut b = LogBatch::default();
    b.add_entries::<Block>(1, &[entry(1, 1024)])?;
    e.write(&mut b, true)?;
    state.mode.store(mode, Ordering::SeqCst);
    let mut b = LogBatch::default();
    b.add_entries::<Block>(1, &[entry(2, 1024)])?;
    let result = e.write(&mut b, mode == 2);
    match result {
        Ok(_) => emit(json!({"unexpected_ack":true})),
        Err(err) => emit(json!({"write_error":err.to_string()})),
    }
    Ok(())
}
fn sync_count(path: &Path) -> Result<Value> {
    fs::create_dir(path)?;
    let state = Arc::new(FaultState::default());
    let e = Engine::open_with_file_system(
        config(path),
        Arc::new(FaultFs {
            state: state.clone(),
        }),
    )?;
    let mut b = LogBatch::default();
    b.add_entries::<Block>(1, &[entry(1, 1024)])?;
    e.write(&mut b, false)?;
    let before = state.syncs.load(Ordering::SeqCst);
    e.sync()?;
    let after = state.syncs.load(Ordering::SeqCst);
    let mut b = LogBatch::default();
    b.put(0, b"barrier".to_vec(), 1u64.to_le_bytes().to_vec())?;
    e.write(&mut b, true)?;
    let barrier = state.syncs.load(Ordering::SeqCst);
    assert_eq!(after, before);
    assert!(barrier > after);
    Ok(
        json!({"scenario":"sync_syscall_oracle","public_sync_handle_sync_delta":after-before,"nonempty_barrier_handle_sync_delta":barrier-after}),
    )
}
fn suite(root: &Path) -> Result<()> {
    assert!(root.is_dir());
    emit(
        json!({"probe":"raft-engine=0.4.2","upstream_commit":"084c5ca0e4f8291ab67afbfe8b9df02747838c86","pid":std::process::id(),"fixture_root":root,"seed":"deterministic(index*31+offset*17)%251","compression":false,"log_recycle":false}),
    );
    emit(kill_reopen(root, true)?);
    emit(kill_reopen(root, false)?);
    emit(retention(root)?);
    for size in [128, 4096, 65536] {
        let p = root.join(format!("resource-{size}"));
        let out = Command::new(std::env::current_exe()?)
            .args(["resource", p.to_str().unwrap(), &size.to_string()])
            .output()?;
        assert!(
            out.status.success(),
            "resource child: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        print!("{}", String::from_utf8_lossy(&out.stdout));
    }
    for report in corruption(root)? {
        emit(report);
    }
    emit(sync_count(&root.join("sync-count"))?);
    for mode in [1, 2] {
        let path = root.join(format!("fault-{mode}"));
        fs::create_dir(&path)?;
        let out = Command::new(std::env::current_exe()?)
            .args(["fault", path.to_str().unwrap(), &mode.to_string()])
            .output()?;
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(!stdout.contains("unexpected_ack"));
        if mode == 1 {
            assert!(stdout.contains("write_error"));
        } else {
            assert!(!out.status.success());
        }
        let e = Engine::open(config(&path))?;
        check(&e, 1, 1, 2, 1024)?;
        emit(
            json!({"scenario":if mode==1{"injected_enospc"}else{"injected_sync_eio"},"child_exit":out.status.code(),"stdout":stdout,"stderr":String::from_utf8_lossy(&out.stderr[..out.stderr.len().min(512)]),"recovered_first_ack":true,"recovered_last":e.last_index(1)}),
        );
    }
    emit(json!({"suite":"PASS"}));
    Ok(())
}
fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let mode = args.get(1).ok_or("mode")?;
    let path = Path::new(args.get(2).ok_or("path")?);
    match mode.as_str() {
        "suite" => suite(path),
        "writer" => child_writer(path, args[3] == "true"),
        "resource" => {
            emit(resource(path, args[3].parse()?)?);
            Ok(())
        }
        "fault" => fault_child(path, args[3].parse()?),
        _ => Err("unknown mode".into()),
    }
}
