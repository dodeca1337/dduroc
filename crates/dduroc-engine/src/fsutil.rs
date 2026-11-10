//! File primitives with explicit durability semantics.
//!
//! These helpers use `fdatasync` for file contents and `fsync` for parent
//! directories after rename. The live raft backend uses upstream sync behavior,
//! including `sync_all`; these rules describe this module's helpers only.
//!
use crate::error::{IoContext, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

// Synthetic boundary failures are private to unit tests and the calling thread.
// They do not intercept raft-engine I/O or model a physical power loss.
#[cfg(test)]
mod fault {
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum FaultStage {
        Create,
        Write,
        FileSync,
        Rename,
        DirectorySync,
    }

    struct Armed {
        stage: FaultStage,
        errno: i32,
        consumed: Rc<Cell<bool>>,
    }

    thread_local! {
        static NEXT: RefCell<Option<Armed>> = const { RefCell::new(None) };
    }

    // Rc deliberately keeps a guard on its originating thread.
    #[derive(Debug)]
    pub(crate) struct FaultGuard {
        consumed: Rc<Cell<bool>>,
    }

    impl FaultGuard {
        pub(crate) fn consumed(&self) -> bool {
            self.consumed.get()
        }
    }

    impl Drop for FaultGuard {
        fn drop(&mut self) {
            NEXT.with(|slot| {
                let mut pending = slot.borrow_mut();
                if pending
                    .as_ref()
                    .is_some_and(|fault| Rc::ptr_eq(&fault.consumed, &self.consumed))
                {
                    pending.take();
                }
            });
        }
    }

    pub(crate) fn fail_next(stage: FaultStage, errno: i32) -> FaultGuard {
        assert!(
            matches!(errno, 5 | 28),
            "only synthetic EIO and ENOSPC are supported"
        );
        let consumed = Rc::new(Cell::new(false));
        NEXT.with(|slot| {
            let mut pending = slot.borrow_mut();
            assert!(
                pending.is_none(),
                "a filesystem fault is already armed on this thread"
            );
            *pending = Some(Armed {
                stage,
                errno,
                consumed: consumed.clone(),
            });
        });
        FaultGuard { consumed }
    }

    pub(super) fn check(stage: FaultStage) -> std::io::Result<()> {
        let armed = NEXT.with(|slot| {
            let mut pending = slot.borrow_mut();
            if pending.as_ref().is_some_and(|fault| fault.stage == stage) {
                pending.take()
            } else {
                None
            }
        });
        match armed {
            Some(fault) => {
                fault.consumed.set(true);
                Err(std::io::Error::from_raw_os_error(fault.errno))
            }
            None => Ok(()),
        }
    }
}

#[cfg(test)]
pub(crate) use fault::{FaultGuard, FaultStage, fail_next};

/// Permissions on created files: read and write for the owner, read for the
/// group, **nothing for anyone else**.
///
/// Set explicitly rather than left to the process umask: a device's journal is
/// diagnostics of a working installation (addresses, modes, tuning
/// parameters), and with the usual umask of 022 it would be readable by every
/// user on the system. The kernel can only narrow these permissions with its
/// umask, never widen them.
///
/// The group is left readable deliberately: a device's web interface and the
/// dump-collection utility often run as a separate user in a shared group, and
/// taking their access away would force them to be root.
pub const FILE_MODE: u32 = 0o640;

/// Permissions on store directories: the same plus traversal for owner and
/// group.
pub const DIR_MODE: u32 = 0o750;

/// Sync a directory: this makes operations on **names** durable (creating,
/// renaming and deleting files inside it).
pub fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(test)]
    fault::check(FaultStage::DirectorySync).ctx_path("fsync of a directory", dir)?;
    let f = File::open(dir).ctx_path("opening a directory", dir)?;
    // Some filesystems do not support fsync on a directory (certain network
    // ones, for example); EINVAL there means "nothing to sync", not a failure.
    match rustix::fs::fsync(&f) {
        Ok(()) | Err(rustix::io::Errno::INVAL) => Ok(()),
        Err(e) => Err(e).ctx_path("fsync of a directory", dir),
    }
}

/// `fdatasync` a file.
pub fn sync_data(file: &File, path: &Path) -> Result<()> {
    #[cfg(test)]
    fault::check(FaultStage::FileSync).ctx_path("fdatasync", path)?;
    rustix::fs::fdatasync(file).ctx_path("fdatasync", path)
}

/// Create a directory with all its parents and make the names durable.
pub fn create_dir_all_synced(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(DIR_MODE)
        .create(dir)
        .ctx_path("creating a directory", dir)?;
    // The name of every link created has to become durable, so the chain of
    // parents is synced from the bottom up.
    let mut cur = Some(dir);
    while let Some(d) = cur {
        if let Some(parent) = d.parent() {
            if parent.as_os_str().is_empty() {
                break;
            }
            sync_dir(parent)?;
            cur = Some(parent);
        } else {
            break;
        }
    }
    Ok(())
}

/// Replace a file's contents atomically.
///
/// The order: write to a temporary file → `fdatasync` → `rename` → `fsync` of
/// the directory. A power loss at any point leaves either the whole old
/// content on disk or the whole new one.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp = tmp_path(path);

    {
        #[cfg(test)]
        fault::check(FaultStage::Create).ctx_path("creating a temporary file", &tmp)?;
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(FILE_MODE)
            .open(&tmp)
            .ctx_path("creating a temporary file", &tmp)?;
        #[cfg(test)]
        fault::check(FaultStage::Write).ctx_path("writing", &tmp)?;
        f.write_all(bytes).ctx_path("writing", &tmp)?;
        sync_data(&f, &tmp)?;
    }

    // A rename error must not be swallowed: the temporary file would stay
    // behind as litter while the caller believed the data was written.
    #[cfg(test)]
    let renamed = fault::check(FaultStage::Rename).and_then(|()| std::fs::rename(&tmp, path));
    #[cfg(not(test))]
    let renamed = std::fs::rename(&tmp, path);
    if let Err(e) = renamed {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).ctx_path("renaming into", path);
    }
    sync_dir(dir)
}

/// The path of a temporary file next to the target: `rename` works only
/// within one filesystem, so /tmp will not do.
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Read a whole file; `Ok(None)` if it does not exist.
#[allow(dead_code)] // Kept for general callers; control metadata uses the bounded variant.
pub fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).ctx_path("reading", path),
    }
}

/// Shared ceiling for ordinary control metadata and format markers.
pub(crate) const CONTROL_METADATA_MAX_BYTES: usize = 256 * 1024;

/// Read no more than the control metadata ceiling; missing files remain absent.
pub(crate) fn read_optional_bounded(path: &Path) -> Result<Option<Vec<u8>>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error).ctx_path("opening", path),
    };
    let mut bytes = Vec::new();
    file.take((CONTROL_METADATA_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .ctx_path("reading", path)?;
    if bytes.len() > CONTROL_METADATA_MAX_BYTES {
        return Err(crate::Error::Corrupt {
            path: path.to_owned(),
            reason: "control metadata exceeds byte limit".into(),
        });
    }
    Ok(Some(bytes))
}

pub(crate) fn read_required_bounded(path: &Path) -> Result<Vec<u8>> {
    read_optional_bounded(path)?.ok_or_else(|| crate::Error::Io {
        context: format!("reading {}", path.display()),
        source: std::io::Error::from(std::io::ErrorKind::NotFound),
    })
}

/// Decode exactly one complete metadata value, rejecting trailing bytes.
pub(crate) fn decode_metadata<T: serde::de::DeserializeOwned>(
    path: &Path,
    bytes: &[u8],
) -> Result<T> {
    match postcard::take_from_bytes(bytes) {
        Ok((value, [])) => Ok(value),
        _ => Err(crate::Error::Corrupt {
            path: path.to_owned(),
            reason: "control metadata does not parse completely".into(),
        }),
    }
}

/// Delete a file if it exists and make the deletion durable.
#[cfg(test)]
pub fn remove_synced(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).ctx_path("deleting", path),
    }
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// Clean up `*.tmp` files left behind by interrupted operations in a
/// directory.
///
/// Such a file is the trace of a power loss between creating the temporary
/// file and the `rename`: its content is knowably incomplete and nothing
/// addresses it.
pub fn sweep_tmp(dir: &Path) -> Result<usize> {
    let mut removed = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).ctx_path("reading a directory", dir),
    };
    for entry in entries {
        let entry = entry.ctx_path("walking a directory", dir)?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "tmp") && path.is_file() {
            std::fs::remove_file(&path).ctx_path("deleting a temporary file", &path)?;
            removed += 1;
        }
    }
    if removed > 0 {
        sync_dir(dir)?;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_fault_is_one_shot_and_local_to_the_arming_thread() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta");
        let guard: FaultGuard = fail_next(FaultStage::Create, 5);
        let other = dir.path().join("other-thread");
        std::thread::spawn(move || write_atomic(&other, b"unaffected"))
            .join()
            .unwrap()
            .unwrap();
        assert!(!guard.consumed());
        let error = write_atomic(&path, b"rejected").unwrap_err();
        assert!(
            matches!(error, crate::Error::Io { source, .. } if source.raw_os_error() == Some(5))
        );
        assert!(guard.consumed());
        write_atomic(&path, b"accepted").unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"accepted");
    }

    #[test]
    fn dropping_an_unused_fault_guard_disarms_only_that_fault() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta");
        drop(fail_next(FaultStage::Write, 28));
        write_atomic(&path, b"accepted").unwrap();
        let first = fail_next(FaultStage::Create, 5);
        assert!(write_atomic(&path, b"rejected").is_err());
        let second = fail_next(FaultStage::Create, 28);
        drop(first);
        let error = write_atomic(&path, b"still-rejected").unwrap_err();
        assert!(
            matches!(error, crate::Error::Io { source, .. } if source.raw_os_error() == Some(28))
        );
        assert!(second.consumed());
    }

    #[test]
    fn bounded_control_read_handles_missing_exact_limit_and_oversize() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dduroc-raft-v1");
        assert!(read_optional_bounded(&path).unwrap().is_none());
        let file = std::fs::File::create(&path).unwrap();
        file.set_len(CONTROL_METADATA_MAX_BYTES as u64).unwrap();
        assert_eq!(
            read_optional_bounded(&path).unwrap().unwrap().len(),
            CONTROL_METADATA_MAX_BYTES
        );
        file.set_len((CONTROL_METADATA_MAX_BYTES + 1) as u64)
            .unwrap();
        assert!(matches!(
            read_optional_bounded(&path),
            Err(crate::Error::Corrupt { .. })
        ));
    }

    #[test]
    fn atomic_write_replaces_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta");

        assert_eq!(read_optional(&path).unwrap(), None);
        write_atomic(&path, b"first").unwrap();
        assert_eq!(
            read_optional(&path).unwrap().as_deref(),
            Some(&b"first"[..])
        );
        write_atomic(&path, b"second-longer").unwrap();
        assert_eq!(
            read_optional(&path).unwrap().as_deref(),
            Some(&b"second-longer"[..])
        );

        // The temporary file does not stay behind as litter.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .collect();
        assert!(leftovers.is_empty(), "a temporary file was left behind");
    }

    #[test]
    fn tmp_is_a_sibling_for_same_filesystem_rename() {
        let p = Path::new("/data/logs/ns/epochs.bin");
        assert_eq!(tmp_path(p), Path::new("/data/logs/ns/epochs.bin.tmp"));
    }

    #[test]
    fn sweep_removes_only_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.seg"), b"keep").unwrap();
        std::fs::write(dir.path().join("a.seg.tmp"), b"drop").unwrap();
        std::fs::write(dir.path().join("meta.tmp"), b"drop").unwrap();

        assert_eq!(sweep_tmp(dir.path()).unwrap(), 2);
        assert!(dir.path().join("a.seg").exists());
        assert!(!dir.path().join("a.seg.tmp").exists());
        assert!(!dir.path().join("meta.tmp").exists());

        // Idempotence: a second pass finds nothing.
        assert_eq!(sweep_tmp(dir.path()).unwrap(), 0);
        // A missing directory is not an error.
        assert_eq!(sweep_tmp(&dir.path().join("nope")).unwrap(), 0);
    }

    #[test]
    fn create_dir_all_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        create_dir_all_synced(&nested).unwrap();
        assert!(nested.is_dir());
        create_dir_all_synced(&nested).unwrap();
    }

    #[test]
    fn created_files_and_dirs_are_not_world_readable() {
        // A device's journal is diagnostics of a working installation. With the
        // usual umask of 022 it would be readable by every user on the system,
        // so the permissions are set explicitly. What is checked is precisely
        // the absence of permissions for others: a umask can only narrow ours,
        // and the exact value depends on it.
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("ns/default");
        create_dir_all_synced(&nested).unwrap();
        let path = nested.join("meta");
        write_atomic(&path, b"secret").unwrap();

        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(
            mode & 0o007,
            0,
            "others must not read the journal: {mode:#o}"
        );
        assert_eq!(
            mode & 0o600,
            0o600,
            "the owner must be able to read and write"
        );

        let dir_mode = std::fs::metadata(&nested).unwrap().permissions().mode();
        assert_eq!(
            dir_mode & 0o007,
            0,
            "the directory must not be open to others: {dir_mode:#o}"
        );
    }

    #[test]
    fn remove_missing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        remove_synced(&dir.path().join("absent")).unwrap();
    }
}
