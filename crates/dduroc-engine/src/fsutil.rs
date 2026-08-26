//! File primitives with explicit durability semantics.
//!
//! The rules the whole engine obeys:
//!
//! - **`fdatasync`, not `fsync`.** Segments reserve their space up front
//!   (`fallocate`), so an append does not change the file size and there is no
//!   reason to sync inode metadata. The exception is files that grow
//!   (metadata): `fdatasync` is enough there too — it syncs the metadata
//!   needed to read the data, the size included.
//! - **A file name is durable only after an `fsync` of the directory.**
//!   Without it, a file may exist after a power loss but have no name.
//! - **Replacing a file goes through `rename` only.** Overwriting in place
//!   leaves a window in which the file is half old and half new.

use crate::error::{IoContext, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

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
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(FILE_MODE)
            .open(&tmp)
            .ctx_path("creating a temporary file", &tmp)?;
        f.write_all(bytes).ctx_path("writing", &tmp)?;
        sync_data(&f, &tmp)?;
    }

    // A rename error must not be swallowed: the temporary file would stay
    // behind as litter while the caller believed the data was written.
    if let Err(e) = std::fs::rename(&tmp, path) {
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
pub fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).ctx_path("reading", path),
    }
}

/// Delete a file if it exists and make the deletion durable.
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
