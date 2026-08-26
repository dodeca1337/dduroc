//! Epochs: what ties relative time to absolute time.
//!
//! There are two levels of identity:
//!
//! - a **run** (`boot_counter`) — one execution of the process. It grows on
//!   every start.
//! - a **hardware boot** (`hw_boot_id`) — one boot of the hardware. Determined
//!   from `/proc/sys/kernel/random/boot_id`: the kernel generates that UUID at
//!   boot, and it does not depend on when the software started. (The prototype
//!   told boots apart by `CLOCK_BOOTTIME` increasing, which got it wrong on a
//!   quick restart after a reboot.)
//!
//! **The UTC anchor is stored per hardware boot** — it is the UTC time
//! corresponding to `CLOCK_BOOTTIME == 0`. One synchronization gives absolute
//! time to every event of that boot, including those recorded **before** the
//! synchronization: the conversion happens at read time.
//!
//! The anchor is **updatable, with source priority**: `User < Ntp < Gps`. An
//! operator may have entered the time by hand first and GPS arrived later — the
//! anchor is refined. The other way round (by hand over GPS) it is not.

use crate::clock::boottime_us;
use crate::error::{Error, IoContext, Result};
use crate::fsutil;
use chrono::{DateTime, Utc};
use dduroc_format::{BootTime, Micros};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The name of the epochs file in the store root.
pub const EPOCHS_FILE: &str = "epochs.bin";

/// The source of a time synchronization. The order is the priority: a more
/// trustworthy source overwrites a less trustworthy one, but not the reverse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum SyncSource {
    /// Entered by an operator.
    User = 1,
    Ntp = 2,
    Gps = 3,
}

impl SyncSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            SyncSource::User => "user",
            SyncSource::Ntp => "ntp",
            SyncSource::Gps => "gps",
        }
    }
}

/// One execution of the process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub boot_counter: u32,
    pub hw_boot_id: u32,
    /// `CLOCK_BOOTTIME` at the moment the run was registered. The same value
    /// serves as the base of [`crate::clock::Clock`], so
    /// `boottime_at_init_us + event_micros` is the event's exact BOOTTIME.
    pub boottime_at_init_us: u64,
}

/// One boot of the hardware.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HwBoot {
    pub hw_boot_id: u32,
    /// The UUID from `/proc/sys/kernel/random/boot_id`.
    pub kernel_boot_id: [u8; 16],
    /// The UTC (ms) corresponding to `CLOCK_BOOTTIME == 0`. `None` means there
    /// was no synchronization: the events of this boot have relative time only.
    ///
    /// On disk this is milliseconds as an integer rather than a parsed date:
    /// eight bytes against twelve for a serialized `DateTime`, and no
    /// dependence of the file format on how some other crate represents a date.
    /// What is handed out is a normal type — [`HwBoot::utc_anchor`].
    pub utc_anchor_ms: Option<i64>,
    pub anchor_source: Option<SyncSource>,
    /// `CLOCK_BOOTTIME` at the moment the anchor was recorded.
    pub anchor_captured_us: Option<u64>,
}

impl HwBoot {
    /// The anchor as a moment in time. `None` means there was no
    /// synchronization.
    pub fn utc_anchor(&self) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.utc_anchor_ms?)
    }
}

/// Where a wall-clock moment falls in a run's relative scale.
///
/// Three outcomes are warranted here: "earlier than the start" and "there is
/// no anchor" are different things. The first is ordinary for the lower bound
/// of a window (the whole run lies inside it); the second means there is
/// nothing to compare with, the run drops out of the selection, and the caller
/// has to be told.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOffset {
    /// A moment earlier than the run started — not expressible in its scale.
    BeforeStart,
    /// Microseconds since the run started.
    At(Micros),
    /// The run is unknown, or its boot was never synchronized.
    Unanchored,
}

/// The contents of `epochs.bin`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Epochs {
    pub runs: Vec<Run>,
    pub hw_boots: Vec<HwBoot>,
}

impl Epochs {
    /// A run by its number.
    ///
    /// The search is binary: `to_utc` is called on **every** record of an
    /// answer, and the longer a device lives the more runs pile up in the file
    /// (twenty restarts a day over five years is thirty-six thousand). A linear
    /// walk would mean thirty-six thousand comparisons per journal line.
    /// Entries are appended with an increasing `boot_counter`, and cleanup
    /// preserves the order; on a file edited by hand there is an honest
    /// fallback.
    pub fn run(&self, boot_counter: u32) -> Option<&Run> {
        match self
            .runs
            .binary_search_by_key(&boot_counter, |r| r.boot_counter)
        {
            Ok(i) => Some(&self.runs[i]),
            Err(_) => self.runs.iter().find(|r| r.boot_counter == boot_counter),
        }
    }

    pub fn hw_boot(&self, id: u32) -> Option<&HwBoot> {
        match self.hw_boots.binary_search_by_key(&id, |b| b.hw_boot_id) {
            Ok(i) => Some(&self.hw_boots[i]),
            Err(_) => self.hw_boots.iter().find(|b| b.hw_boot_id == id),
        }
    }

    /// Convert relative time into wall-clock time. `None` means the run's
    /// hardware boot has no anchor and the record has no absolute time.
    ///
    /// The anchor is stored to the millisecond, but the offset within the run
    /// is added in microseconds: there is no reason to round relative time to
    /// milliseconds — that is exactly the part that was measured precisely.
    pub fn to_utc(&self, at: BootTime) -> Option<DateTime<Utc>> {
        let run = self.run(at.boot.0)?;
        let hw = self.hw_boot(run.hw_boot_id)?;
        let anchor_ms = hw.utc_anchor_ms?;
        let event_boottime_us = run.boottime_at_init_us.checked_add(at.at.0)?;
        let total_us = i128::from(anchor_ms) * 1_000 + i128::from(event_boottime_us);
        DateTime::from_timestamp_micros(i64::try_from(total_us).ok()?)
    }

    /// The reverse conversion: where a wall-clock moment lies in a given run's
    /// scale.
    ///
    /// Needed by a query with wall-clock bounds: records cannot be compared
    /// against it directly — every run has its own scale and its own anchor.
    pub fn from_utc(&self, boot_counter: u32, utc: DateTime<Utc>) -> RunOffset {
        let Some(run) = self.run(boot_counter) else {
            return RunOffset::Unanchored;
        };
        let Some(anchor_ms) = self.hw_boot(run.hw_boot_id).and_then(|hw| hw.utc_anchor_ms) else {
            return RunOffset::Unanchored;
        };
        let boottime_us = i128::from(utc.timestamp_micros()) - i128::from(anchor_ms) * 1_000;
        let from_start = boottime_us - i128::from(run.boottime_at_init_us);
        if from_start < 0 {
            return RunOffset::BeforeStart;
        }
        // Saturating rather than erroring: 2^64 µs is 584 thousand years, which
        // relative time never reaches, and there is even less reason to panic
        // on the arithmetic of a query bound.
        RunOffset::At(Micros(u64::try_from(from_start).unwrap_or(u64::MAX)))
    }

    /// Whether a run has an anchor: whether its records can be matched against
    /// a wall clock at all.
    pub fn is_anchored(&self, boot_counter: u32) -> bool {
        self.run(boot_counter)
            .and_then(|r| self.hw_boot(r.hw_boot_id))
            .is_some_and(|hw| hw.utc_anchor_ms.is_some())
    }

    /// The runs in chronological order of registration.
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }

    /// Update a boot's anchor. Returns `true` if the anchor was accepted.
    ///
    /// The rule: the source must be no lower than the current one. Equal
    /// priority is allowed — a fresh GPS fix refines an old one (clock drift
    /// between synchronizations is real, and the refinement must not be thrown
    /// away).
    pub fn set_anchor(
        &mut self,
        hw_boot_id: u32,
        utc: DateTime<Utc>,
        source: SyncSource,
        now_boottime_us: u64,
    ) -> bool {
        let Some(hw) = self
            .hw_boots
            .iter_mut()
            .find(|b| b.hw_boot_id == hw_boot_id)
        else {
            return false;
        };
        if let Some(current) = hw.anchor_source
            && source < current
        {
            return false;
        }
        // The anchor is the UTC of the moment BOOTTIME was zero.
        hw.utc_anchor_ms = Some(
            utc.timestamp_millis()
                .saturating_sub((now_boottime_us / 1_000) as i64),
        );
        hw.anchor_source = Some(source);
        hw.anchor_captured_us = Some(now_boottime_us);
        true
    }

    /// Forget runs that are not among `alive`, and boots left orphaned.
    ///
    /// Without this the file would grow forever: twenty restarts a day over
    /// five years is thirty-six thousand entries, read and rewritten whole on
    /// every start.
    pub fn retain_runs(&mut self, alive: &dyn Fn(u32) -> bool) {
        self.runs.retain(|r| alive(r.boot_counter));
        let used: std::collections::BTreeSet<u32> =
            self.runs.iter().map(|r| r.hw_boot_id).collect();
        self.hw_boots.retain(|b| used.contains(&b.hw_boot_id));
    }
}

/// The outcome of reading the epochs file.
enum Loaded {
    Missing,
    Corrupt,
    Ok(Epochs),
}

/// The epochs file: reading, registering a run, updating an anchor.
#[derive(Debug)]
pub struct EpochStore {
    path: PathBuf,
    epochs: Epochs,
    current: Run,
}

impl EpochStore {
    /// Open the file and register a new run.
    ///
    /// `boottime_at_init_us` is passed in from outside so that it matches the
    /// clock's base to the microsecond.
    ///
    /// `floor_boot` is the largest run number written on the names of segments
    /// already on disk. It is asked for **only** if the epochs file did not
    /// survive the previous run (lost, or quarantined after corruption): with
    /// an intact file the maximum over `runs` already covers everything on
    /// disk, and walking directories with thousands of namespaces costs more
    /// than it saves — which is exactly why epoch cleanup runs on a threshold
    /// rather than at every start.
    ///
    /// Without that floor, losing `epochs.bin` would restart the numbering on
    /// top of runs whose segments are still there. A segment's name is `(boot,
    /// µs)` and its lexicographic order is declared to be chronological: a
    /// number handed out twice would place new segments **before** the old ones
    /// in the history. Rotation, deleting the oldest, would start on the fresh
    /// records, and a reader would hand back the history jumbled.
    pub fn open_and_register(
        root: &Path,
        boottime_at_init_us: u64,
        floor_boot: &dyn Fn() -> Result<Option<u32>>,
    ) -> Result<Self> {
        let path = root.join(EPOCHS_FILE);
        let (mut epochs, lost) = Self::load_for_write(&path)?;
        let floor_boot = if lost { floor_boot()? } else { None };
        let kernel_boot_id = read_kernel_boot_id()?;

        // The same hardware boot as the previous run? Compare the kernel UUID.
        let hw_boot_id = match epochs
            .hw_boots
            .iter()
            .find(|b| b.kernel_boot_id == kernel_boot_id)
        {
            Some(b) => b.hw_boot_id,
            None => {
                let id = epochs
                    .hw_boots
                    .iter()
                    .map(|b| b.hw_boot_id)
                    .max()
                    .map_or(0, |m| m.saturating_add(1));
                epochs.hw_boots.push(HwBoot {
                    hw_boot_id: id,
                    kernel_boot_id,
                    utc_anchor_ms: None,
                    anchor_source: None,
                    anchor_captured_us: None,
                });
                id
            }
        };

        let boot_counter = epochs
            .runs
            .iter()
            .map(|r| r.boot_counter)
            .chain(floor_boot)
            .max()
            .map_or(0, |m| m.saturating_add(1));

        let current = Run {
            boot_counter,
            hw_boot_id,
            boottime_at_init_us,
        };
        epochs.runs.push(current);

        let store = Self {
            path,
            epochs,
            current,
        };
        store.persist()?;
        Ok(store)
    }

    /// Open read-only (a viewer, offline analysis) — no run is registered.
    ///
    /// **It writes nothing.** A dump brought in for analysis may sit on read-only
    /// media, belong to another device, or be material evidence in the incident
    /// being investigated: quarantining a damaged file is a write, and in this
    /// mode a write is not allowed.
    pub fn open_read_only(root: &Path) -> Result<Epochs> {
        Ok(match Self::read(&root.join(EPOCHS_FILE))? {
            Loaded::Ok(e) => e,
            // Relative time is self-sufficient; without epochs only the
            // conversion to UTC is lost.
            Loaded::Missing | Loaded::Corrupt => Epochs::default(),
        })
    }

    /// Read the file without touching it.
    fn read(path: &Path) -> Result<Loaded> {
        let Some(bytes) = fsutil::read_optional(path)? else {
            return Ok(Loaded::Missing);
        };
        Ok(match postcard::from_bytes(&bytes) {
            Ok(e) => Loaded::Ok(e),
            Err(_) => Loaded::Corrupt,
        })
    }

    /// The same for the writing side: a damaged file is moved to quarantine.
    ///
    /// Overwriting it silently is not an option — it is what one examines to
    /// work out what happened to the anchoring of time.
    ///
    /// The second element means "no previous state survived": either the file
    /// was not there or it turned out unreadable. That is the only case in
    /// which run numbering has to be reconstructed from the disk.
    fn load_for_write(path: &Path) -> Result<(Epochs, bool)> {
        match Self::read(path)? {
            Loaded::Ok(e) => Ok((e, false)),
            Loaded::Missing => Ok((Epochs::default(), true)),
            Loaded::Corrupt => {
                let backup = path.with_extension("corrupt");
                let _ = std::fs::rename(path, &backup);
                Ok((Epochs::default(), true))
            }
        }
    }

    pub fn current_run(&self) -> Run {
        self.current
    }

    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    /// Record a time synchronization for the current hardware boot. Returns
    /// `false` if the source is less trustworthy than the current anchor.
    pub fn record_sync(&mut self, utc: DateTime<Utc>, source: SyncSource) -> Result<bool> {
        let accepted = self
            .epochs
            .set_anchor(self.current.hw_boot_id, utc, source, boottime_us());
        if accepted {
            self.persist()?;
        }
        Ok(accepted)
    }

    /// Remove entries for runs of which no segments are left.
    pub fn retain_runs(&mut self, alive: &dyn Fn(u32) -> bool) -> Result<()> {
        let current = self.current.boot_counter;
        let before = self.epochs.runs.len();
        // The current run must never be removed: it is still writing.
        self.epochs
            .retain_runs(&|boot| boot == current || alive(boot));
        if self.epochs.runs.len() != before {
            self.persist()?;
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let bytes = postcard::to_allocvec(&self.epochs)?;
        fsutil::write_atomic(&self.path, &bytes)
    }
}

/// Read the kernel's boot UUID.
fn read_kernel_boot_id() -> Result<[u8; 16]> {
    const PATH: &str = "/proc/sys/kernel/random/boot_id";
    let raw = std::fs::read_to_string(PATH).ctx("reading /proc/sys/kernel/random/boot_id")?;
    parse_uuid(raw.trim()).ok_or_else(|| Error::Corrupt {
        path: PathBuf::from(PATH),
        reason: "not a UUID".to_owned(),
    })
}

/// Parsing a UUID of the form `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` without
/// depending on the uuid crate: exactly one place needs it and exactly one
/// format.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = s.bytes().filter(|&b| b != b'-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks_exact(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &Path, base_us: u64) -> EpochStore {
        EpochStore::open_and_register(dir, base_us, &|| Ok(None)).unwrap()
    }

    /// The same, but with a known lower bound on the run number: this is how a
    /// store whose segments outlived its epochs file is opened.
    fn store_with_floor(dir: &Path, base_us: u64, floor: u32) -> EpochStore {
        EpochStore::open_and_register(dir, base_us, &|| Ok(Some(floor))).unwrap()
    }

    #[test]
    fn a_lost_epochs_file_does_not_restart_run_numbering() {
        // `boot_counter` goes into the name of every segment, and the order of
        // names is declared to be chronological. Restarting the numbering on
        // top of segments that survived the loss of the epochs file would place
        // new records BEFORE the old ones in the history: rotation would start
        // on the fresh ones and a reader would hand the history back jumbled.
        let dir = tempfile::tempdir().unwrap();

        // The first run: number zero, and the epochs file is created.
        assert_eq!(store(dir.path(), 1_000).current_run().boot_counter, 0);
        // The second remembers the first.
        assert_eq!(store(dir.path(), 2_000).current_run().boot_counter, 1);

        // The epochs file is lost (corruption would quarantine it — the same
        // outcome).
        std::fs::remove_file(dir.path().join(EPOCHS_FILE)).unwrap();

        // Without the floor the numbering would start over; with it, it
        // continues past the number visible on the segment names.
        assert_eq!(store(dir.path(), 3_000).current_run().boot_counter, 0);
        std::fs::remove_file(dir.path().join(EPOCHS_FILE)).unwrap();
        assert_eq!(
            store_with_floor(dir.path(), 4_000, 1)
                .current_run()
                .boot_counter,
            2,
            "the numbering continues past the last run whose segments are alive"
        );
    }

    #[test]
    fn an_intact_epochs_file_is_never_second_guessed_by_the_disk() {
        // The floor is asked for lazily: walking the names with thousands of
        // namespaces costs more than it saves, and an intact epochs file does
        // not need it — its maximum already covers everything on disk.
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), 1_000);

        let asked = std::cell::Cell::new(false);
        let next = EpochStore::open_and_register(dir.path(), 2_000, &|| {
            asked.set(true);
            Ok(Some(1_000_000))
        })
        .unwrap();
        assert!(
            !asked.get(),
            "an intact epochs file needs no walk of the disk"
        );
        assert_eq!(next.current_run().boot_counter, 1);
    }

    /// A moment in time from epoch milliseconds — shorter than parsing a date.
    fn utc(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).expect("the milliseconds are within the epoch")
    }

    #[test]
    fn registers_runs_incrementally_within_one_hw_boot() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path(), 1_000);
        assert_eq!(a.current_run().boot_counter, 0);
        drop(a);

        let b = store(dir.path(), 2_000);
        assert_eq!(b.current_run().boot_counter, 1);
        // The same kernel_boot_id means the same hardware boot.
        assert_eq!(b.current_run().hw_boot_id, 0);
        assert_eq!(b.epochs().hw_boots.len(), 1, "no new boot was invented");
    }

    #[test]
    fn anchor_is_retroactive_for_events_before_sync() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 5_000_000); // the run started at the 5th second of BOOTTIME
        let at = BootTime::from_raw(0, 1_000_000); // BOOTTIME 6 s

        assert_eq!(s.epochs().to_utc(at), None, "there is no anchor yet");

        // Synchronization: BOOTTIME is now ~T, UTC = 1_700_000_000_000.
        let now_boottime = boottime_us();
        assert!(
            s.epochs
                .set_anchor(0, utc(1_700_000_000_000), SyncSource::Ntp, now_boottime)
        );

        let got = s.epochs().to_utc(at).expect("there is an anchor");
        let expected = 1_700_000_000_000 - (now_boottime / 1_000) as i64 + 6_000;
        assert_eq!(
            got.timestamp_millis(),
            expected,
            "an event from BEFORE the synchronization got a UTC"
        );

        // The reverse conversion returns the same relative time: rounding the
        // anchor to milliseconds must not move a query bound.
        assert_eq!(
            s.epochs().from_utc(0, got),
            RunOffset::At(Micros(1_000_000))
        );
    }

    #[test]
    fn from_utc_distinguishes_before_start_from_no_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 5_000_000);

        // Without an anchor there is nothing to compare with — which is not the
        // same as "earlier".
        assert_eq!(
            s.epochs().from_utc(0, utc(1_700_000_000_000)),
            RunOffset::Unanchored
        );

        s.epochs
            .set_anchor(0, utc(1_700_000_000_000), SyncSource::Gps, 5_000_000);
        // The anchor is the UTC at BOOTTIME 0; the run started at the 5th
        // second.
        let anchor = s.epochs().hw_boot(0).unwrap().utc_anchor().unwrap();
        assert_eq!(
            s.epochs().from_utc(0, anchor),
            RunOffset::BeforeStart,
            "the moment of zero BOOTTIME is earlier than the run started"
        );
        assert_eq!(
            s.epochs()
                .from_utc(0, anchor + chrono::TimeDelta::seconds(5)),
            RunOffset::At(Micros(0)),
            "exactly the run's start"
        );
        assert_eq!(
            s.epochs()
                .from_utc(0, anchor + chrono::TimeDelta::seconds(7)),
            RunOffset::At(Micros(2_000_000))
        );
        assert_eq!(
            s.epochs().from_utc(42, utc(1_700_000_000_000)),
            RunOffset::Unanchored
        );
    }

    #[test]
    fn anchored_runs_are_distinguishable() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 1_000);
        assert!(!s.epochs().is_anchored(0));
        assert!(!s.epochs().is_anchored(7), "an unknown run");
        s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        assert!(s.epochs().is_anchored(0));
        assert_eq!(s.epochs().runs().len(), 1);
    }

    #[test]
    fn anchor_priority_rules() {
        let mut e = Epochs {
            runs: vec![Run {
                boot_counter: 0,
                hw_boot_id: 0,
                boottime_at_init_us: 0,
            }],
            hw_boots: vec![HwBoot {
                hw_boot_id: 0,
                kernel_boot_id: [0; 16],
                utc_anchor_ms: None,
                anchor_source: None,
                anchor_captured_us: None,
            }],
        };

        assert!(
            e.set_anchor(0, utc(1_000_000), SyncSource::User, 0),
            "the first anchor"
        );
        assert!(
            e.set_anchor(0, utc(2_000_000), SyncSource::Gps, 0),
            "GPS over a manual entry"
        );
        assert_eq!(e.hw_boots[0].utc_anchor_ms, Some(2_000_000));

        assert!(
            !e.set_anchor(0, utc(3_000_000), SyncSource::User, 0),
            "a manual entry over GPS: no"
        );
        assert!(
            !e.set_anchor(0, utc(3_000_000), SyncSource::Ntp, 0),
            "NTP over GPS: no"
        );
        assert_eq!(
            e.hw_boots[0].utc_anchor_ms,
            Some(2_000_000),
            "the anchor is untouched"
        );

        assert!(
            e.set_anchor(0, utc(4_000_000), SyncSource::Gps, 0),
            "a fresh GPS fix refines an old one"
        );
        assert_eq!(e.hw_boots[0].utc_anchor_ms, Some(4_000_000));
        assert_eq!(e.hw_boots[0].utc_anchor(), Some(utc(4_000_000)));

        assert!(
            !e.set_anchor(99, utc(1), SyncSource::Gps, 0),
            "an unknown boot"
        );
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = store(dir.path(), 1_000);
            s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
                .unwrap();
        }
        let s = store(dir.path(), 2_000);
        assert_eq!(s.current_run().boot_counter, 1);
        let hw = s.epochs().hw_boot(0).unwrap();
        assert!(
            hw.utc_anchor_ms.is_some(),
            "the anchor survived the restart"
        );
        assert_eq!(hw.anchor_source, Some(SyncSource::Gps));
        // A new run inherits the anchor of its hardware boot.
        assert!(s.epochs().to_utc(BootTime::from_raw(1, 0)).is_some());
    }

    #[test]
    fn corrupt_file_is_quarantined_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(EPOCHS_FILE), b"\xff\xff\xff not postcard").unwrap();

        let s = store(dir.path(), 1_000);
        assert_eq!(
            s.current_run().boot_counter,
            0,
            "we started from a clean slate"
        );
        assert!(
            dir.path().join("epochs.corrupt").exists(),
            "the damaged file is kept for examination rather than overwritten"
        );
    }

    #[test]
    fn read_only_open_never_writes() {
        // A dump for analysis arrives from another device, sometimes on
        // read-only media, sometimes as evidence in the incident being
        // investigated. A reader has no right to change it — not even to
        // quarantine a damaged file.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(EPOCHS_FILE);
        std::fs::write(&path, b"\xff\xff\xff not postcard").unwrap();

        let epochs = EpochStore::open_read_only(dir.path()).unwrap();
        assert_eq!(
            epochs,
            Epochs::default(),
            "without epochs there is only relative time"
        );
        assert!(path.exists(), "the file must stay where it is");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"\xff\xff\xff not postcard",
            "the content is untouched"
        );
        assert!(
            !dir.path().join("epochs.corrupt").exists(),
            "quarantine is a write, and a reader is not allowed one"
        );

        // An intact file reads as usual.
        let mut s = store(dir.path(), 1_000);
        s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        let epochs = EpochStore::open_read_only(dir.path()).unwrap();
        assert_eq!(epochs.runs.len(), 1);
    }

    #[test]
    fn retain_keeps_current_run_and_drops_orphan_hw_boots() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 1_000);
        s.epochs.runs.push(Run {
            boot_counter: 100,
            hw_boot_id: 7,
            boottime_at_init_us: 0,
        });
        s.epochs.hw_boots.push(HwBoot {
            hw_boot_id: 7,
            kernel_boot_id: [9; 16],
            utc_anchor_ms: None,
            anchor_source: None,
            anchor_captured_us: None,
        });

        // Nobody has live segments — but the current run must survive.
        s.retain_runs(&|_| false).unwrap();
        assert_eq!(s.epochs().runs.len(), 1);
        assert_eq!(s.epochs().runs[0].boot_counter, 0);
        assert_eq!(
            s.epochs().hw_boots.len(),
            1,
            "the orphaned boot was removed"
        );
    }

    #[test]
    fn uuid_parsing() {
        let id = parse_uuid("0f8fad5b-d9cb-469f-a165-70867728950e").unwrap();
        assert_eq!(id[0], 0x0f);
        assert_eq!(id[15], 0x0e);
        assert_eq!(parse_uuid("not a uuid"), None);
        assert_eq!(parse_uuid(""), None);
        assert_eq!(parse_uuid("0f8fad5b-d9cb-469f-a165-70867728950"), None);
        assert_eq!(parse_uuid("zf8fad5b-d9cb-469f-a165-70867728950e"), None);
    }

    #[test]
    fn real_kernel_boot_id_is_readable() {
        // On Linux the file must exist; the test catches a parser regression
        // against the real kernel format.
        let id = read_kernel_boot_id().expect("/proc is available");
        assert_ne!(id, [0u8; 16], "a kernel boot_id is never all zeros");
    }

    #[test]
    fn utc_conversion_handles_missing_data() {
        let e = Epochs::default();
        assert_eq!(e.to_utc(BootTime::from_raw(0, 0)), None, "there is no run");
        assert_eq!(e.from_utc(0, utc(1_700_000_000_000)), RunOffset::Unanchored);
    }

    #[test]
    fn absurd_relative_time_does_not_panic() {
        // A corrupt segment can yield any time. The conversion has to return
        // `None` rather than overflow.
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 1_000);
        s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        assert_eq!(s.epochs().to_utc(BootTime::from_raw(0, u64::MAX)), None);
    }
}
