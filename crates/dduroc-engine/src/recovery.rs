//! Offline recovery on copies (`recovery` feature). Stop every writer first.
//!
//! Existing source locks are acquired without creating source files. Evidence
//! is never opened by raft-engine; only the separate candidate is inspected or
//! explicitly repaired. Nothing activates or replaces a live store.
//!
//! Limits bound input files/bytes, not upstream recovery RAM or temporary disk
//! use. Metadata decode is capped at 8 MiB for epochs and 256 KiB per ns-meta;
//! epoch collections also obey max_entries. Two full copies need twice the
//! input size plus manifests and upstream overhead. Cancellation/deadlines are
//! cooperative: they cannot interrupt an OS read or Engine::open. Use a process
//! supervisor for a hard deadline.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::{StoreConfig, backend::ReadBackend, schema::StorageClass};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Repair is never inferred from an inspection failure.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryMode {
    #[default]
    StrictInspect,
    /// Can discard ACKed data, including after transient read errors.
    RepairCopyAllowDataLoss,
}
#[derive(Debug, Clone)]
pub struct RecoveryOptions {
    pub mode: RecoveryMode,
    /// Sum of input file lengths across all roots; two full copies are made.
    pub max_source_bytes: u64,
    /// All directory entries, including directories, across all roots.
    pub max_entries: usize,
    pub deadline: Option<Instant>,
}
impl Default for RecoveryOptions {
    fn default() -> Self {
        Self {
            mode: RecoveryMode::StrictInspect,
            max_source_bytes: 4 * 1024 * 1024 * 1024,
            max_entries: 65_536,
            deadline: None,
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum RecoveryError {
    #[error("recovery refused: {0}")]
    Refused(&'static str),
    #[error("source root {0} is busy")]
    Busy(usize),
    #[error("recovery cancelled or deadline elapsed")]
    Cancelled,
    #[error("recovery I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("recovery JSON: {0}")]
    Json(#[from] serde_json::Error),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootMapping {
    pub root: usize,
    /// Root zero also owns store metadata. No original paths are disclosed.
    pub classes: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestEntry {
    pub root: usize,
    /// Relative to evidence/root-N or candidate/root-N.
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub version: u8,
    pub mode: RecoveryMode,
    /// Always true in repair mode, even if inspection later succeeds.
    pub loss_possible: bool,
    pub roots: Vec<RootMapping>,
    pub evidence_complete: bool,
    pub source_bytes: u64,
    pub strict_inspection_succeeded: bool,
    pub initial_strict_error: Option<String>,
    pub repair_attempted: bool,
    pub inspected_blocks: u64,
    pub inspection_error: Option<String>,
    /// Paths relative to candidate/, including added/deleted files.
    pub changed_candidate_files: Vec<PathBuf>,
    pub complete: bool,
}
struct Control<'a> {
    options: &'a RecoveryOptions,
    cancel: &'a AtomicBool,
}
impl Control<'_> {
    fn check(&self) -> Result<(), RecoveryError> {
        if self.cancel.load(Ordering::Relaxed)
            || self.options.deadline.is_some_and(|d| Instant::now() >= d)
        {
            Err(RecoveryError::Cancelled)
        } else {
            Ok(())
        }
    }
}
struct Entry {
    root: usize,
    path: PathBuf,
    directory: bool,
    bytes: u64,
}

/// Copies all roots configured in StoreConfig. A damaged candidate is returned
/// as a report with strict_inspection_succeeded=false. The destination parent
/// must exist; an existing destination is refused. Failures preserve partial
/// artifacts with complete=false when report.json remains writable.
///
/// Supply the original configuration: old store metadata does not persist a
/// complete root roster, so an omitted custom root cannot be discovered.
/// Strict inspection validates epochs and namespace metadata referenced by the
/// channel catalog. Unreferenced namespace metadata is preserved in evidence
/// and manifests but is not required for interpreting the retained catalog.
/// This operation does not establish that every previously ACKed record exists.
pub fn recover_copy(
    config: &StoreConfig,
    destination: &Path,
    options: &RecoveryOptions,
    cancel: &AtomicBool,
) -> Result<RecoveryReport, RecoveryError> {
    let control = Control { options, cancel };
    control.check()?;
    if options.max_entries == 0 || options.max_source_bytes == 0 {
        return Err(RecoveryError::Refused("limits must be positive"));
    }
    let (roots, mappings) = configured_roots(config)?;
    let name = destination
        .file_name()
        .ok_or(RecoveryError::Refused("invalid destination"))?;
    let parent = destination
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let destination = fs::canonicalize(parent)?.join(name);
    if roots
        .iter()
        .any(|r| destination.starts_with(r) || r.starts_with(&destination))
    {
        return Err(RecoveryError::Refused("source and destination overlap"));
    }
    match fs::symlink_metadata(&destination) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => return Err(e.into()),
        Ok(_) => return Err(RecoveryError::Refused("destination already exists")),
    }
    let _locks = source_locks(&roots)?;
    let entries = inventory(&roots, &control)?;
    for root in &roots {
        let marker = read_small(&root.join("dduroc-raft-v1"))?;
        if marker != b"dduroc raft store v1\n" && marker != b"dduroc raft store v2 stats-writer\n" {
            return Err(RecoveryError::Refused("unknown source format marker"));
        }
    }
    let meta: crate::store::StoreMeta =
        match postcard::take_from_bytes(&read_small(&roots[0].join("store-meta"))?) {
            Ok((value, [])) => value,
            _ => return Err(RecoveryError::Refused("invalid source metadata")),
        };
    if !(1..=3).contains(&meta.container_version) {
        return Err(RecoveryError::Refused(
            "unsupported source metadata version",
        ));
    }
    create_dir(&destination)?;
    File::open(destination.parent().unwrap())?.sync_all()?;
    let mut report = RecoveryReport {
        version: 1,
        mode: options.mode,
        loss_possible: options.mode == RecoveryMode::RepairCopyAllowDataLoss,
        roots: mappings,
        evidence_complete: false,
        source_bytes: 0,
        strict_inspection_succeeded: false,
        initial_strict_error: None,
        repair_attempted: false,
        inspected_blocks: 0,
        inspection_error: None,
        changed_candidate_files: Vec::new(),
        complete: false,
    };
    write_json(&destination.join("report.json"), &report)?;
    let result = run_copy(&roots, &destination, &entries, &control, &mut report);
    if let Err(error) = &result {
        report.inspection_error = Some(error.to_string());
    }
    write_json(&destination.join("report.json"), &report)?;
    result?;
    Ok(report)
}
fn run_copy(
    roots: &[PathBuf],
    destination: &Path,
    entries: &[Entry],
    control: &Control<'_>,
    report: &mut RecoveryReport,
) -> Result<(), RecoveryError> {
    let evidence = destination.join("evidence");
    let candidate = destination.join("candidate");
    let before = copy_roots(roots, &evidence, entries, control)?;
    report.source_bytes = before.iter().map(|e| e.bytes).sum();
    write_json(&destination.join("evidence-manifest.json"), &before)?;
    report.evidence_complete = true;
    write_json(&destination.join("report.json"), report)?;
    let evidence_roots = numbered_roots(&evidence, roots.len());
    if before != copy_roots(&evidence_roots, &candidate, entries, control)? {
        return Err(RecoveryError::Refused(
            "evidence changed while copying candidate",
        ));
    }
    let candidate_roots = numbered_roots(&candidate, roots.len());
    let mut inspection = inspect(
        &candidate_roots,
        &report.roots,
        control,
        &mut report.inspected_blocks,
    );
    if let Err(error) = &inspection {
        report.initial_strict_error =
            Some(error.replace(candidate.to_string_lossy().as_ref(), "<candidate>"));
        control.check()?;
        if control.options.mode == RecoveryMode::RepairCopyAllowDataLoss {
            report.repair_attempted = true;
            // Record this before the only lossy operation, including if a
            // process is killed inside upstream recovery.
            write_json(&destination.join("report.json"), report)?;
            report.inspected_blocks = 0;
            inspection = repair_candidate(&candidate_roots, control).and_then(|()| {
                inspect(
                    &candidate_roots,
                    &report.roots,
                    control,
                    &mut report.inspected_blocks,
                )
            });
        }
    }
    match inspection {
        Ok(()) => report.strict_inspection_succeeded = true,
        Err(error) => {
            report.inspection_error =
                Some(error.replace(candidate.to_string_lossy().as_ref(), "<candidate>"))
        }
    }
    control.check()?;
    let after_entries = inventory(&candidate_roots, control)?;
    let mut after = Vec::new();
    for entry in after_entries.iter().filter(|e| !e.directory) {
        after.push(ManifestEntry {
            root: entry.root,
            path: entry.path.clone(),
            bytes: entry.bytes,
            sha256: hash_file(
                &candidate_roots[entry.root].join(&entry.path),
                entry.bytes,
                control,
                None,
            )?,
        });
    }
    let keyed = |items: &[ManifestEntry]| {
        items
            .iter()
            .map(|e| ((e.root, e.path.clone()), (e.bytes, e.sha256.clone())))
            .collect::<BTreeMap<_, _>>()
    };
    let old = keyed(&before);
    let new = keyed(&after);
    for key in old.keys().chain(new.keys()) {
        if old.get(key) != new.get(key) {
            report
                .changed_candidate_files
                .push(PathBuf::from(format!("root-{}", key.0)).join(&key.1));
        }
    }
    report.changed_candidate_files.sort();
    report.changed_candidate_files.dedup();
    write_json(&destination.join("candidate-manifest.json"), &after)?;
    report.complete = true;
    Ok(())
}
fn configured_roots(
    config: &StoreConfig,
) -> Result<(Vec<PathBuf>, Vec<RootMapping>), RecoveryError> {
    let mut roots = vec![canonical_root(&config.root)?];
    let mut mappings = vec![RootMapping {
        root: 0,
        classes: Vec::new(),
    }];
    for class in StorageClass::ALL {
        let path = config
            .channels
            .get(&class)
            .and_then(|c| c.custom_root.as_deref())
            .unwrap_or(&config.root);
        let path = canonical_root(path)?;
        let index = match roots.iter().position(|r| *r == path) {
            Some(index) => index,
            None => {
                if roots
                    .iter()
                    .any(|r| r.starts_with(&path) || path.starts_with(r))
                {
                    return Err(RecoveryError::Refused("source roots overlap"));
                }
                let index = roots.len();
                roots.push(path);
                mappings.push(RootMapping {
                    root: index,
                    classes: Vec::new(),
                });
                index
            }
        };
        mappings[index].classes.push(class.as_str().to_owned());
    }
    Ok((roots, mappings))
}
fn canonical_root(path: &Path) -> Result<PathBuf, RecoveryError> {
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err(RecoveryError::Refused(
            "source root is not a directory or is a symlink",
        ));
    }
    Ok(fs::canonicalize(path)?)
}
fn source_locks(roots: &[PathBuf]) -> Result<Vec<File>, RecoveryError> {
    let mut locks = Vec::new();
    for (index, root) in roots.iter().enumerate() {
        let mut paths = vec![root.join("raft-engine/LOCK")];
        if index == 0 {
            paths.insert(0, root.join(".lock"));
        }
        for path in paths {
            // Same Linux flock primitive as Store and raft-engine's fs2 lock.
            // Missing locks are refused, never created in source.
            let file = open_regular(&path, true)?;
            rustix::fs::flock(&file, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                .map_err(|e| {
                    if e == rustix::io::Errno::WOULDBLOCK {
                        RecoveryError::Busy(index)
                    } else {
                        RecoveryError::Io(e.into())
                    }
                })?;
            locks.push(file);
        }
    }
    Ok(locks)
}
// Resolve every component using directory descriptors and O_NOFOLLOW. A
// symlink swapped in after inventory must not redirect a source read or lock.
fn open_source(path: &Path, writable: bool, directory: bool) -> Result<File, RecoveryError> {
    use rustix::fs::{Mode, OFlags, openat};
    let mut parent = File::open("/")?;
    let parts: Vec<_> = path.components().collect();
    for (index, component) in parts.iter().enumerate() {
        let std::path::Component::Normal(name) = component else {
            if matches!(component, std::path::Component::RootDir) {
                continue;
            }
            return Err(RecoveryError::Refused("noncanonical source path"));
        };
        let last = index + 1 == parts.len();
        let mut flags = OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK;
        if !last || directory {
            flags |= OFlags::DIRECTORY;
        }
        flags |= if last && writable {
            OFlags::RDWR
        } else {
            OFlags::RDONLY
        };
        parent =
            File::from(openat(&parent, *name, flags, Mode::empty()).map_err(std::io::Error::from)?);
    }
    Ok(parent)
}
fn open_regular(path: &Path, writable: bool) -> Result<File, RecoveryError> {
    let file = open_source(path, writable, false)?;
    if !file.metadata()?.is_file() {
        return Err(RecoveryError::Refused("nonregular source file"));
    }
    Ok(file)
}
fn read_small(path: &Path) -> Result<Vec<u8>, RecoveryError> {
    read_bounded_metadata(path, crate::fsutil::CONTROL_METADATA_MAX_BYTES as u64)
}
fn read_bounded_metadata(path: &Path, limit: u64) -> Result<Vec<u8>, RecoveryError> {
    let mut bytes = Vec::new();
    open_regular(path, false)?
        .take(limit + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        return Err(RecoveryError::Refused("metadata byte limit"));
    }
    Ok(bytes)
}
// Reader fallback deliberately tolerates lost UTC metadata. Recovery reports
// must distinguish that degraded state from a completely inspected copy.
fn strict_metadata<T: serde::de::DeserializeOwned>(
    path: &Path,
    limit: u64,
    label: &str,
) -> Result<T, String> {
    let bytes = read_bounded_metadata(path, limit).map_err(|error| format!("{label}: {error}"))?;
    let (value, trailing) = postcard::take_from_bytes(&bytes)
        .map_err(|error| format!("{label}: invalid metadata: {error}"))?;
    if !trailing.is_empty() {
        return Err(format!("{label}: trailing metadata bytes"));
    }
    Ok(value)
}
fn strict_epochs(root: &Path, control: &Control<'_>) -> Result<BTreeSet<u32>, String> {
    let epochs: crate::Epochs = strict_metadata(
        &root.join(crate::epochs::EPOCHS_FILE),
        crate::epochs::MAX_EPOCHS_BYTES as u64,
        "epochs.bin",
    )?;
    if epochs.runs.len() > control.options.max_entries
        || epochs.hw_boots.len() > control.options.max_entries
    {
        return Err("epochs.bin: metadata entry limit".into());
    }
    epochs
        .validate_with(|| control.check().map_err(|error| error.to_string()))
        .map_err(|error| format!("epochs.bin: {error}"))?;
    let mut runs = BTreeSet::new();
    for run in &epochs.runs {
        control.check().map_err(|error| error.to_string())?;
        runs.insert(run.boot_counter);
    }
    Ok(runs)
}
fn numbered_roots(parent: &Path, count: usize) -> Vec<PathBuf> {
    (0..count)
        .map(|i| parent.join(format!("root-{i}")))
        .collect()
}
fn inventory(roots: &[PathBuf], control: &Control<'_>) -> Result<Vec<Entry>, RecoveryError> {
    let mut entries = Vec::new();
    let (mut bytes, mut path_bytes) = (0u64, 0usize);
    for (root_index, root) in roots.iter().enumerate() {
        let mut pending = vec![PathBuf::new()];
        while let Some(relative) = pending.pop() {
            control.check()?;
            let directory = open_source(&root.join(&relative), false, true)?;
            let items = rustix::fs::Dir::read_from(&directory).map_err(std::io::Error::from)?;
            for item in items {
                control.check()?;
                let item = item.map_err(std::io::Error::from)?;
                let name = item.file_name();
                if name.to_bytes() == b"." || name.to_bytes() == b".." {
                    continue;
                }
                let path = relative.join(std::ffi::OsString::from_vec(name.to_bytes().to_vec()));
                let text = path
                    .to_str()
                    .ok_or(RecoveryError::Refused("non-UTF8 source name"))?;
                path_bytes = path_bytes
                    .checked_add(text.len())
                    .ok_or(RecoveryError::Refused("path byte limit"))?;
                if entries.len() >= control.options.max_entries || path_bytes > 16 * 1024 * 1024 {
                    return Err(RecoveryError::Refused("entry or path byte limit"));
                }
                let meta =
                    rustix::fs::statat(&directory, name, rustix::fs::AtFlags::SYMLINK_NOFOLLOW)
                        .map_err(std::io::Error::from)?;
                let kind = rustix::fs::FileType::from_raw_mode(meta.st_mode);
                let is_dir = kind == rustix::fs::FileType::Directory;
                if !is_dir && kind != rustix::fs::FileType::RegularFile {
                    return Err(RecoveryError::Refused("symlink or nonregular source entry"));
                }
                let length = if is_dir {
                    0
                } else {
                    u64::try_from(meta.st_size)
                        .map_err(|_| RecoveryError::Refused("negative source size"))?
                };
                bytes = bytes
                    .checked_add(length)
                    .ok_or(RecoveryError::Refused("byte limit"))?;
                if bytes > control.options.max_source_bytes {
                    return Err(RecoveryError::Refused("source byte limit"));
                }
                if is_dir {
                    pending.push(path.clone());
                }
                entries.push(Entry {
                    root: root_index,
                    path,
                    directory: is_dir,
                    bytes: length,
                });
            }
        }
    }
    entries.sort_by(|a, b| (a.root, &a.path).cmp(&(b.root, &b.path)));
    Ok(entries)
}
fn create_dir(path: &Path) -> Result<(), RecoveryError> {
    fs::DirBuilder::new()
        .mode(crate::fsutil::DIR_MODE)
        .create(path)?;
    Ok(())
}
fn copy_roots(
    roots: &[PathBuf],
    parent: &Path,
    entries: &[Entry],
    control: &Control<'_>,
) -> Result<Vec<ManifestEntry>, RecoveryError> {
    create_dir(parent)?;
    let targets = numbered_roots(parent, roots.len());
    for target in &targets {
        create_dir(target)?;
    }
    let mut manifest = Vec::new();
    for entry in entries {
        control.check()?;
        let target = targets[entry.root].join(&entry.path);
        if entry.directory {
            create_dir(&target)?;
            continue;
        }
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(crate::fsutil::FILE_MODE)
            .open(target)?;
        let sha256 = hash_file(
            &roots[entry.root].join(&entry.path),
            entry.bytes,
            control,
            Some(&mut output),
        )?;
        output.sync_all()?;
        manifest.push(ManifestEntry {
            root: entry.root,
            path: entry.path.clone(),
            bytes: entry.bytes,
            sha256,
        });
    }
    for entry in entries.iter().rev().filter(|e| e.directory) {
        File::open(targets[entry.root].join(&entry.path))?.sync_all()?;
    }
    for target in &targets {
        File::open(target)?.sync_all()?;
    }
    File::open(parent)?.sync_all()?;
    Ok(manifest)
}
fn hash_file(
    path: &Path,
    length: u64,
    control: &Control<'_>,
    mut output: Option<&mut File>,
) -> Result<String, RecoveryError> {
    let mut input = open_regular(path, false)?;
    let before = input.metadata()?;
    if before.len() != length {
        return Err(RecoveryError::Refused("source size changed"));
    }
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut remaining = length;
    while remaining > 0 {
        control.check()?;
        let amount = remaining.min(buffer.len() as u64) as usize;
        input.read_exact(&mut buffer[..amount])?;
        if let Some(writer) = &mut output {
            writer.write_all(&buffer[..amount])?;
        }
        hash.update(&buffer[..amount]);
        remaining -= amount as u64;
    }
    let after = input.metadata()?;
    if before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
        || before.ctime() != after.ctime()
        || before.ctime_nsec() != after.ctime_nsec()
    {
        return Err(RecoveryError::Refused("source changed while copying"));
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn repair_candidate(roots: &[PathBuf], control: &Control<'_>) -> Result<(), String> {
    let check = || control.check().map_err(|e| e.to_string());
    for root in roots {
        check()?;
        crate::backend::validate_file_spans(&root.join("raft-engine"), 4 * 1024 * 1024)
            .map_err(|error| error.to_string())?;
        let config = raft_engine::Config {
            dir: root
                .join("raft-engine")
                .to_str()
                .ok_or("non-UTF8 candidate")?
                .into(),
            target_file_size: raft_engine::ReadableSize(4 * 1024 * 1024),
            recovery_threads: 1,
            recovery_mode: raft_engine::RecoveryMode::TolerateTailCorruption,
            enable_log_recycle: false,
            prefill_for_recycle: false,
            ..raft_engine::Config::default()
        };
        drop(raft_engine::Engine::open(config).map_err(|e| e.to_string())?);
    }
    Ok(())
}
fn inspect(
    roots: &[PathBuf],
    mappings: &[RootMapping],
    control: &Control<'_>,
    blocks: &mut u64,
) -> Result<(), String> {
    let check = || control.check().map_err(|e| e.to_string());
    check()?;
    let backend = ReadBackend::open_dump(roots).map_err(|e| e.to_string())?;
    let known_runs = strict_epochs(&roots[0], control)?;
    let snapshots = backend
        .snapshot_channels(|_| Ok(true))
        .map_err(|e| e.to_string())?;
    let mut namespaces = BTreeMap::new();
    for snapshot in &snapshots {
        check()?;
        let channel = &snapshot.info;
        if channel.protocol_version == 0 {
            return Err("ns-meta: invalid catalog protocol version".into());
        }
        crate::channel::validate_component(&channel.key.namespace)
            .map_err(|_| "ns-meta: invalid namespace component")?;
        let (schema, latest) = namespaces
            .entry(channel.key.namespace.as_str())
            .or_insert((channel.schema_name.as_str(), channel.protocol_version));
        if *schema != channel.schema_name {
            return Err("ns-meta: channel schema identities disagree".into());
        }
        *latest = (*latest).max(channel.protocol_version);
    }
    for (namespace, (schema, latest)) in namespaces {
        check()?;
        let meta: crate::namespace::NsMeta = strict_metadata(
            &roots[0].join(namespace).join(crate::namespace::NS_META),
            crate::fsutil::CONTROL_METADATA_MAX_BYTES as u64,
            "ns-meta",
        )?;
        if meta.schema_name != schema {
            return Err("ns-meta: schema identity differs from channel catalog".into());
        }
        // This stamp means last COMPLETED namespace migration, whereas each
        // active channel catalog advances on open. Removed/closed classes can
        // retain older catalog versions; untouched blocks can remain older
        // even after migration. Only a stamp beyond EVERY channel is invalid.
        if meta.protocol_version == 0 || meta.protocol_version > latest {
            return Err(
                "ns-meta: invalid completed protocol version relative to channel catalog".into(),
            );
        }
    }
    for snapshot in &snapshots {
        let channel = &snapshot.info;
        if !mappings[snapshot.generation.0]
            .classes
            .iter()
            .any(|class| class == channel.key.class.as_str())
        {
            return Err("configured class root does not match channel placement".into());
        }
        let (Some(mut next), Some(last)) = (channel.first_index, channel.last_index) else {
            continue;
        };
        let end = last.checked_add(1).ok_or("block index exhausted")?;
        while next < end {
            check()?;
            let page = backend
                .read_blocks(&channel.key, next, end, 1, 8 * 1024 * 1024, false)
                .map_err(|e| e.to_string())?;
            for block in &page.blocks {
                if !known_runs.contains(&block.boot.0) {
                    return Err("epochs.bin: retained block references missing run".into());
                }
                if block.protocol_version.0 == 0
                    || block.protocol_version.0 > channel.protocol_version
                {
                    return Err(
                        "ns-meta: invalid block protocol version relative to channel catalog"
                            .into(),
                    );
                }
                let parsed = dduroc_format::Block::parse(&block.bytes)
                    .map_err(|e| e.to_string())?
                    .ok_or("empty block")?;
                if parsed.header.total_len() != block.bytes.len() as u64 {
                    return Err("trailing block bytes".into());
                }
                let mut count = 0u64;
                for record in parsed.records() {
                    check()?;
                    record.map_err(|e| e.to_string())?;
                    count += 1;
                }
                if count != u64::from(parsed.header.count) {
                    return Err("record count mismatch".into());
                }
                *blocks = blocks.checked_add(1).ok_or("block count exhausted")?;
            }
            match page.next_index {
                Some(index) if index > next => next = index,
                Some(_) => return Err("inspection made no progress".into()),
                None => break,
            }
        }
    }
    Ok(())
}
fn write_json(path: &Path, value: &impl Serialize) -> Result<(), RecoveryError> {
    let bytes = serde_json::to_vec_pretty(value)?;
    crate::fsutil::write_atomic(path, &bytes)
        .map_err(|_| RecoveryError::Refused("durable report write failed"))
}
