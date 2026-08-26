//! A channel's segment inventory and rotation by budget.
//!
//! Rotation is an `unlink` of the oldest segment once the total size exceeds
//! the budget. Nothing is rewritten: deleting a whole file on eMMC/SD hands
//! the FTL whole erase blocks at once rather than smearing the work across the
//! entire address translation.
//!
//! What counts is the **allocated** size of the files, not the volume of
//! useful data: the flash is taken by the reserve window, and the budget has
//! to reflect real occupancy.

use crate::error::{IoContext, Result};
use crate::fsutil;
use dduroc_format::segment::SegmentName;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

/// A segment in the inventory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentEntry {
    pub name: SegmentName,
    /// The file's size on disk (its reserve window included).
    pub size_bytes: u64,
}

impl SegmentEntry {
    pub fn path(&self, dir: &Path) -> PathBuf {
        dir.join(self.name.to_string())
    }
}

/// A channel's segment list, ordered by time.
#[derive(Debug, Default)]
pub struct Inventory {
    /// Sorted by ascending `(boot, base)` — the rotation order.
    segments: VecDeque<SegmentEntry>,
    total: u64,
}

impl Inventory {
    /// Read a channel directory.
    ///
    /// Foreign files are ignored: a directory may contain anything, and the
    /// engine must not fall over someone else's `README`.
    pub fn scan(dir: &Path) -> Result<Self> {
        let mut segments = Vec::new();
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(e) => return Err(e).ctx_path("reading a channel directory", dir),
        };

        for entry in entries {
            let entry = entry.ctx_path("walking a directory", dir)?;
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str().and_then(SegmentName::parse) else {
                continue;
            };
            // A symlink to a foreign file must not fall under rotation: the
            // deletion would follow the link, and the size accounting would be
            // wrong.
            let meta = entry.metadata().ctx_path("stat", &entry.path())?;
            if !meta.is_file() {
                continue;
            }
            segments.push(SegmentEntry {
                name,
                size_bytes: meta.len(),
            });
        }

        segments.sort_by_key(|s| s.name);
        let total = segments.iter().map(|s| s.size_bytes).sum();
        Ok(Self {
            segments: segments.into(),
            total,
        })
    }

    /// The segment names alone, in time order.
    ///
    /// Separate from [`Inventory::scan`] because the sizes cost a `stat` per
    /// file and a reader does not need them at all: selecting segments by
    /// window goes by name. With hundreds of segments per channel and thousands
    /// of channels, that is the difference between one `readdir` and hundreds
    /// of thousands of system calls per query.
    pub fn scan_names(dir: &Path) -> Result<Vec<SegmentName>> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).ctx_path("reading a channel directory", dir),
        };
        let mut names: Vec<SegmentName> = entries
            .flatten()
            .filter_map(|e| e.file_name().to_str().and_then(SegmentName::parse))
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// The total size of the segments.
    pub fn total_bytes(&self) -> u64 {
        self.total
    }

    pub fn iter(&self) -> impl Iterator<Item = &SegmentEntry> {
        self.segments.iter()
    }

    pub fn oldest(&self) -> Option<&SegmentEntry> {
        self.segments.front()
    }

    pub fn newest(&self) -> Option<&SegmentEntry> {
        self.segments.back()
    }

    /// Add a just-created segment (it is the newest).
    pub fn push_newest(&mut self, entry: SegmentEntry) {
        self.total += entry.size_bytes;
        self.segments.push_back(entry);
    }

    /// Correct a segment's size (sealing truncates the file).
    pub fn update_size_bytes(&mut self, name: SegmentName, size_bytes: u64) {
        if let Some(e) = self.segments.iter_mut().find(|e| e.name == name) {
            self.total = self.total - e.size_bytes + size_bytes;
            e.size_bytes = size_bytes;
        }
    }

    /// Delete the oldest segments while the total exceeds the budget.
    ///
    /// `live` is the segment the channel writes to or will go on writing to
    /// (see `WriterLoop::live_segment`): it must not be deleted. Returns the
    /// number deleted.
    pub fn enforce_budget(
        &mut self,
        dir: &Path,
        budget: u64,
        live: Option<SegmentName>,
    ) -> Result<usize> {
        let mut removed = 0;
        while self.total > budget {
            let Some(front) = self.segments.front() else {
                break;
            };
            // The only segment left is the active one: there is nothing to
            // delete, or the write would lose the file out from under itself.
            if Some(front.name) == live {
                break;
            }
            let path = front.path(dir);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                // The file is already gone (manual cleanup, a race) — simply
                // forget it.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).ctx_path("deleting a segment", &path),
            }
            let entry = self
                .segments
                .pop_front()
                .expect("the front was checked above");
            self.total = self.total.saturating_sub(entry.size_bytes);
            removed += 1;
        }
        if removed > 0 {
            fsutil::sync_dir(dir)?;
        }
        Ok(removed)
    }

    /// Forget a segment (after a migration or a hand deleted it).
    pub fn remove(&mut self, name: SegmentName) {
        if let Some(pos) = self.segments.iter().position(|e| e.name == name) {
            let entry = self.segments.remove(pos).expect("the position was found");
            self.total = self.total.saturating_sub(entry.size_bytes);
        }
    }

    /// The smallest `boot_counter` among the segments — the boundary for
    /// cleaning up epoch entries.
    pub fn min_boot(&self) -> Option<u32> {
        self.segments.front().map(|s| s.name.boot.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::{BootCounter, Micros};

    fn make(dir: &Path, boot: u32, base: u64, size_bytes: usize) {
        let name = SegmentName::new(BootCounter(boot), Micros(base));
        std::fs::write(dir.join(name.to_string()), vec![0u8; size_bytes]).unwrap();
    }

    #[test]
    fn scan_orders_by_time_and_ignores_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 2, 100, 10);
        make(dir.path(), 1, 500, 20);
        make(dir.path(), 1, 100, 30);
        std::fs::write(dir.path().join("README"), "not a segment".as_bytes()).unwrap();
        std::fs::write(dir.path().join("junk.seg"), "a wrong name".as_bytes()).unwrap();
        std::fs::create_dir(dir.path().join("00000009-000000000000000f.seg")).unwrap();

        let inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.len(), 3, "foreign files and directories are skipped");
        assert_eq!(inv.total_bytes(), 60);

        let order: Vec<_> = inv.iter().map(|e| (e.name.boot.0, e.name.base.0)).collect();
        assert_eq!(
            order,
            vec![(1, 100), (1, 500), (2, 100)],
            "ordered by (boot, time)"
        );
        assert_eq!(inv.oldest().unwrap().name.base, Micros(100));
        assert_eq!(inv.newest().unwrap().name.boot, BootCounter(2));
        assert_eq!(inv.min_boot(), Some(1));
    }

    #[test]
    fn missing_dir_is_empty_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let inv = Inventory::scan(&dir.path().join("absent")).unwrap();
        assert!(inv.is_empty());
        assert_eq!(inv.total_bytes(), 0);
    }

    #[test]
    fn budget_removes_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..5u64 {
            make(dir.path(), 1, i * 100, 100);
        }
        let mut inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.total_bytes(), 500);

        let removed = inv.enforce_budget(dir.path(), 250, None).unwrap();
        assert_eq!(removed, 3, "exactly enough was deleted to fit the budget");
        assert_eq!(inv.total_bytes(), 200);
        assert_eq!(inv.oldest().unwrap().name.base, Micros(300));
        assert!(
            !dir.path()
                .join(SegmentName::new(BootCounter(1), Micros(0)).to_string())
                .exists(),
            "the oldest file really is deleted"
        );
    }

    #[test]
    fn active_segment_is_never_removed() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 1000);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        let active = inv.newest().unwrap().name;

        // The budget is knowably exceeded, but there is nothing to delete
        // except the active one.
        let removed = inv.enforce_budget(dir.path(), 10, Some(active)).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(inv.len(), 1, "the active segment survived");

        // With a second segment the old one is deleted and the active one
        // stays.
        make(dir.path(), 1, 500, 1000);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        let active = inv.newest().unwrap().name;
        assert_eq!(inv.enforce_budget(dir.path(), 10, Some(active)).unwrap(), 1);
        assert_eq!(inv.len(), 1);
        assert_eq!(inv.newest().unwrap().name, active);
    }

    #[test]
    fn budget_within_limit_removes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 100);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.enforce_budget(dir.path(), 1000, None).unwrap(), 0);
        assert_eq!(inv.len(), 1);
    }

    #[test]
    fn size_updates_after_seal_are_accounted() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 1000);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.total_bytes(), 1000);

        // Sealing trims the tail of the reserve window.
        let name = inv.newest().unwrap().name;
        inv.update_size_bytes(name, 200);
        assert_eq!(inv.total_bytes(), 200, "the budget counts the real size");

        inv.remove(name);
        assert_eq!(inv.total_bytes(), 0);
        assert!(inv.is_empty());
    }

    #[test]
    fn vanished_file_does_not_break_rotation() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 100);
        make(dir.path(), 1, 100, 100);
        let mut inv = Inventory::scan(dir.path()).unwrap();

        // Someone deleted the file behind the engine's back.
        std::fs::remove_file(
            dir.path()
                .join(SegmentName::new(BootCounter(1), Micros(0)).to_string()),
        )
        .unwrap();

        let removed = inv.enforce_budget(dir.path(), 50, None).unwrap();
        assert_eq!(removed, 2, "a vanished file is simply forgotten");
        assert!(inv.is_empty());
    }
}
