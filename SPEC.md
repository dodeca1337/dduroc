# dduroc — the byte specification of the storage format

Container version: **2**. Status: draft.

This document describes the layout the code implements. Everything written
here is covered by the format's tests (property tests for resilience to
arbitrary garbage included) — a divergence between the specification and the
code counts as a defect in the specification.

### The container's version history

| version | what changed |
|---|---|
| 1 | the first layout: a sample referred to a segment-local series number, a separate `SeriesDef` record tied that number to a metric and to runtime tags, and the series↔metric table was duplicated in the footer |
| 2 | no runtime tags: a series is identified by its metric, a sample carries `metric_id` directly, and both the `SeriesDef` record (kind `0x3`) and the series table in the footer are gone |

A reader accepts **only** the current version: a mismatch is a clear error
rather than an attempt to guess the layout. A store written by an earlier
version does open (the `store-meta` file is rewritten and the identity is
preserved), but its segments are not read by this build and will go with
rotation; the fact that it came up from an older version is reported to the
caller.

## 0. Conventions

- Every fixed-width integer is **little-endian** (inside a file the byte order
  does not affect sorting; sorting is needed only for segment names, and those
  are hex).
- A **varint** is LEB128 (as in postcard): 7 bits per byte, the top bit
  marking continuation. Signed values are zigzag-encoded. Values below 128
  take 1 byte.
- **CRC32C** (Castagnoli; hardware-accelerated on ARMv8 and x86).
- The widths of the identifiers:

| Identifier | Space | On disk | Rationale |
|---|---|---|---|
| `boot_counter` | u32 | fixed u32 (the segment header) | one width everywhere (this cures the prototype's u16/u32 mismatch) |
| µs since the run started | **a full u64** | u64 fixed / varint | the boot is no longer packed into the same 64 bits (it lives in the segment header), so the prototype's 48-bit ceiling is gone |
| `event_id` | u16 | varint | ≤150 types per unit: usually 1 byte, with ×400 of headroom |
| `metric_id` | u16 | varint | the identity of a telemetry series; ≤160 metrics means usually 1 byte |
| `span_kind_id` | u16 | varint | — |
| `span_id` | (boot u32, local u32) | varint local | a span lives strictly inside one run, so the boot is implicit from the segment; 0 means "outside any span" |
| the schema protocol version | u16 | fixed u16 | — |
| `store_id` | u64 | fixed u64 (the segment header) | it tells devices apart: foreign files have their own run numbering and their own anchoring to time |

In the macro, `event_id`/`metric_id`/`span_kind_id` are **explicit only**
(auto-numbering is forbidden: positional ids were the source of the
prototype's silent remapping; remaps are done by migrations).

**Number metrics densely from 1.** `metric_id` goes into every sample as a
varint, so values below 128 cost one byte and values from 128 up cost two.
With dense numbering that changes nothing; a hole above `0x7F` costs +1 byte
on every sample (an f32 sample: 7 → 8 bytes, +14%). The format does not break
from it, but telemetry is the most frequent record there is, and there is no
reason to pay for arbitrary numbers.

### The design limits (the scale claimed plus a little headroom)

| Quantity | Claimed | Design limit |
|---|---|---|
| Microservice types | ~200 | **256** |
| Instances per type | ≤64 | **96** |
| Namespaces in total | ~12.8k | **~24k** |
| Message types per unit | ~150 | **256** |
| Metrics per unit | ~100 | **160** |
| Span kinds per unit | — | **64** |

The limits are targets for validation and for sizing RAM (block buffers,
segment inventories). The wire format does not change with them: an id is a
varint in a u16 space, so holes left by migrations and growth past the limit
do not break the format but merely lengthen an id to 2 bytes (values ≥128).

The namespace and the channel are encoded nowhere on disk — they are implicit
in the file path: `<root>/<namespace>/<channel>/<segment>`.

## 1. The directory tree

```
<root>/
  .lock                         the root lock (flock, exclusive)
  store-meta                    the container version plus store_id
  epochs.bin                    epochs: runs, hw boots, UTC anchors (see §6)
  <namespace>/                  e.g. orc-radio-0
    ns-meta                     the schema name, the directory's protocol version
    <channel>/                  e.g. critical/, default/, telemetry/
      <boot:08x>-<micros:016x>.seg
```

A segment's name is fixed-width hex: `boot_counter` (8 chars) plus `-` plus
the µs of the first record (16 chars, a full u64) plus `.seg`. Lexicographic
order of names equals chronological order; the strict width is mandatory, or
string sorting would diverge from sorting by time.

Parsing a name accepts **exactly** what formatting produces: a strict width
and lowercase hex digits only. A sign (`+000002a`) or uppercase letters would
give a second spelling of the same segment, and it would be accounted for
twice against one file on disk.

An example: `0000002a-000000003b9aca00.seg`.

**Permissions are set explicitly** rather than left to the process umask:
files `0640`, directories `0750` — read and write for the owner, read for the
group, nothing for anyone else. A device's journal is diagnostics of a working
installation (modes, tuning parameters, addresses), and with the usual umask
of 022 it would be readable by every user on the system. The group is left
readable deliberately: a web interface and the dump-collection utility often
run as a separate user in a shared group. The kernel can only narrow these
permissions with its umask, never widen them.

## 2. The segment

```
[SegmentHeader 32B] [Block]* [Footer]?   // a Footer only on sealed segments
```

Space for a file is reserved (`fallocate`) as a **window** rather than for the
whole `segment_bytes` at once: that sets a growth limit — the rotation
boundary — not the file's size. The tail of zeros inside the window is the
search terminator (see §3.1).

### The reserve window

- at creation **64 KiB** is taken (or `segment_bytes`, if that is smaller);
- before every block the window cannot hold, it is extended by **one eighth of
  the limit** — reaching `segment_bytes` in eight extensions whatever its
  value;
- the window is raised above the limit only for a particular block and only
  where there is no limit as a rotation boundary: when a migration rewrites a
  segment (§8), where the limit equals the size of the original and only the
  chain of steps knows how much the records will swell;
- sealing and releasing for idleness (§5) truncate the file to the end of the
  data, that is, give the unwritten tail of the window back;
- opening a released segment asks for **no space**: the window is collapsed to
  the data and the first block will extend it.

The window preserves the properties the reserve exists for: an `fdatasync`
between extensions does not touch the inode's metadata, and the window's tail
is zero. One thing changes: **ENOSPC arrives when the window is extended** as
well, not only when the file is created. The reaction is the same as at
creation: the channel gives up its oldest and retries once (an eviction frees
a whole segment while an eighth of one is asked for); if that did not help,
the block is lost with an accounting entry and a notice, and the medium is
marked full for a general eviction (§5).

The price of a whole segment per channel is not paid by the store but by every
channel: at the 24,000 namespaces claimed that is 192 GiB of reserved space
holding a few kilobytes of writes. Measured over 2000 channels: closing 40 s
and syncing 2.2 s, against 17 ms and 141 ms with a window.

### 2.1 SegmentHeader — 32 bytes

| off | size | field |
|---|---|---|
| 0 | 4 | magic `"DSEG"` (44 53 45 47) |
| 4 | 1 | container_version = 2 |
| 5 | 1 | flags (reserved, 0) |
| 6 | 2 | protocol_version — the namespace schema's version at write time |
| 8 | 4 | boot_counter |
| 12 | 8 | base_micros — the µs since the run started of the first record (= the file name) |
| 20 | 8 | store_id — the store's identity |
| 28 | 4 | CRC32C of bytes 0..28 |

The CRC is checked **before** the container version: otherwise a garbage
version byte would pass for "a format from the future", masking corruption.

A segment never crosses a boot boundary: a change of run means a new segment.

## 3. The block

A block is the unit of a flush (a writer batch). A 32-byte header plus a body.

### 3.1 BlockHeader — 32 bytes

| off | size | field |
|---|---|---|
| 0 | 2 | magic `"DB"` (44 42) |
| 2 | 1 | flags: bits 0–1 compression (0 none, 1 LZ4, 2 zstd); the rest 0 |
| 3 | 1 | reserved (0) |
| 4 | 4 | body_len — the body's length on disk (compressed); 0 is not allowed |
| 8 | 4 | raw_len — the body's length before compression (= body_len without compression) |
| 12 | 4 | seq — the block's number within the segment, from zero |
| 16 | 8 | base_micros — the µs of the block's first record |
| 24 | 2 | count — the number of records |
| 26 | 2 | reserved (0) |
| 28 | 4 | CRC32C: header bytes 0..28 plus the body (as it lies on disk) |

#### Three distinguishable states of the tail

A segment reserves its space, so the unwritten tail is filled with zeros. The
header is arranged so that a normal end of data, a lost piece and corruption
are **different** diagnoses:

| state | sign |
|---|---|
| end of data | all 32 header bytes are zero |
| corruption | the magic, the CRC or the reserved bits do not agree |
| a hole | `seq` is not one greater than the previous |

The naive rule "`body_len == 0` means the end" is dangerous: a single flipped
bit in a length field is indistinguishable from the end of the log, and blocks
already confirmed by `fdatasync` would silently disappear. So a `body_len ==
0` with a non-empty header is an error rather than a terminator.

#### Length bounds

The lengths are checked **before** anything is allocated from them:

- `body_len` and `raw_len` ≤ 64 MiB (`MAX_BODY`);
- `raw_len` is checked against `body_len`: without compression they are equal,
  and with LZ4 the ratio is no more than 255:1 (the format's expansion limit).
  Otherwise a thirty-byte body could demand a sixty-four-megabyte
  decompression buffer.

Compression is applied only when it actually shrinks the body: on short blocks
LZ4 often grows them, and there is no reason to store a bloated body — the
writer then marks the block as uncompressed. `zstd` is recognized by the
format but the codec is not built in (a C dependency complicates
cross-building for armv7) — reading such a block gives a clear error rather
than garbage.

Recovery of an active segment: a sequential walk over the blocks, stopping at
a zero header (the normal case), at running past the file, at a CRC that does
not agree, or at a gap in `seq` → the file's logical length is the end of the
last valid block.

The walk is performed when a channel comes up, for a segment of a **foreign**
run left unsealed: the file is truncated to the length found and sealed (§4).
The footer is assembled in the same walk — it reads the block bodies anyway to
verify their CRCs. The point is not the convenience of reading but the budget:
an unsealed segment is counted in it together with the unwritten tail of its
reserve window. A segment of the current run is not touched — the live state
of this same process may be writing into it.

### 3.2 The block body

A sequence of records. Every record:

```
[b0: kind(4 bits) | kflags(4 bits)] [Δt varint] [fields by kind]
```

`Δt` is the µs since the **previous record in the block** (for the first one,
since `base_micros`, so 0). A delta from the neighbour rather than from the
base: smaller numbers, shorter varints.

| kind | record | fields after Δt |
|---|---|---|
| 0x0 | Message | `event_id v` · `span_local v` (only when kflags.bit0=SPAN) · `payload_len v` · `payload` (postcard) |
| 0x1 | SpanStart | `span_local v` · `span_kind_id v` · `parent_local v` (0 = root) |
| 0x2 | SpanEnd | `span_local v` |
| 0x3 | — | **withdrawn**: in version 1 this was `SeriesDef`. The code is not reused; on meeting it the decoder answers with a distinguishable "a version 1 record" rather than "an unknown kind" |
| 0x4 | Sample | `metric_id v` · the value; **`vtype` is in the low 3 bits of kflags** (not in the body) |
| 0x5 | Text | `level u8` · `span_local v` (when kflags.bit0=SPAN) · `target_len v` · `target utf8` · `text_len v` · `text utf8` — "foreign" free text (the tracing/log bridge, a panic handler). The level is stored in the record — foreign text has no schema |
| 0xF | Ext | `len v` · `len` bytes — forward compatibility: a reader skips what it does not know |

The remaining kinds are reserved; their appearance is a format error
(introducing a new kind requires bumping container_version, or using Ext).

A Message's `payload_len` is stored explicitly even though postcard is
self-describing given the schema: that makes it possible to skip records
without a decoder (a foreign build, a migration, corruption).

**A Sample is the only record without a length.** The `vtype` in the flags is
what gives the value its length: without it there is nothing to skip a sample
by. Hence a consequence worth knowing: **a new `vtype` is a breaking change**.
A reader that does not know the code loses not one record but the whole rest
of the block (the record iterator stops at the error). Three mask bits give 8
codes, of which 6 are taken — two are free, and they should not be spent on
what the schema settles.

**vtype** (the value type; declared on the metric in the schema and duplicated
in kflags):

| vtype | the value in a Sample |
|---|---|
| 0 | f32 — 4 bytes |
| 1 | f64 — 8 bytes |
| 2 | i64 — a zigzag varint |
| 3 | u64 — a varint |
| 4 | bool — 1 byte |
| 5 | blob — `len v` plus bytes (a composite measurement: a spectrum snapshot and the like) |
| 6, 7 | free |

A Sample's top kflags bit has to be zero. It is the point of return for series
dimensions should the requirement ever change: `bit3 = 0` means "the id field
is a `metric_id`".

A telemetry series is identified **by its metric and by nothing else**. There
are no runtime dimensions in the system: several sensors of one kind are
expressed as different schema metrics. So a sample is self-sufficient — no
bookkeeping record, no table in the footer, and no reconstruction of identity
when reading from the middle or in reverse.

The other record invariants the decoder checks:

- a `span_local == 0` with the SPAN flag set is the reserved value (span
  numbering starts at 1); in a SpanStart's `parent` a `0` normally means
  "root". It is checked **on encoding too**: a codec has no right to produce
  bytes it stops at itself — otherwise one such record would cost a reader the
  whole rest of the block.
- the kflags bits that are zero in this version have to be zero: ignoring them
  silently would hide corruption.
- time inside a block is monotonic; a step backwards (data from another
  thread) collapses into a zero delta — losing a record is worse than losing
  microseconds of resolution.
- a blob's length is added to an offset with an overflow check: on a 32-bit
  target a garbage length would otherwise overflow the address arithmetic.

### 3.3 Typical sizes (before block compression)

| record | bytes |
|---|---|
| a Message, event_id<128, no span, a 4B payload | 1+1+1+1+4 = **8** |
| a Message inside a span | +1–3 (the span_local varint) |
| an f32 Sample, metric_id<128 | 1+1+1+4 = **7** |
| a small-u64 Sample (a state code included) | 1+1+1+1 = **4** |
| the same with metric_id ≥ 128 | +1 |
| SpanStart / SpanEnd | **5–7** / **3–5** |

A state machine's state costs as much as a small integer: only the code
reaches the disk, while its label and severity live in the schema.

## 4. The footer (on sealed segments only)

Written at seal time: `ftruncate` to the actual end of the data → append the
footer → `fdatasync`. The sign of sealedness: the file's last 4 bytes are the
magic `"DFTR"`.

```
[entries] [event_id_set] [metric_id_set] [FooterTrailer 32B]
```

- **entries** — one per block: `offset_delta v` · `base_micros_delta v` ·
  `count v` (deltas from the previous entry; the first offset is from the end
  of the `SegmentHeader`, the first time from 0).
- **event_id_set / metric_id_set** — the sets of type ids occurring in the
  segment: `n v`, then ascending varint deltas (a zero delta after the first
  element is a duplicate and is rejected). They answer two questions without
  reading any blocks: for migrations, "is this segment affected" (an
  unaffected one is not rewritten, which saves flash wear); for a reader,
  "what telemetry is in here" (a series is identified by its metric, so
  listing the metrics is listing the series).

  A type reaches the set **together with the block it was met in** rather than
  as the records are gathered. A block need not land in the segment it was
  assembled over: one that does not fit moves to a fresh one (§5), and its
  types have to move with it. Otherwise the old segment would declare types it
  does not hold while the new one stayed silent about types it does — and a
  migration would walk past exactly the history it was written for. A
  discarded block's types remain nowhere.

  Parsing the sets does not allocate from a counter in the file: the
  preallocation ceiling is fixed and the vector grows on its own. "This many
  elements would physically fit in the sections" is not enough on its own — on
  disk an element takes a byte and in memory eight.
- **FooterTrailer** (32 bytes):

| off | size | field |
|---|---|---|
| 0 | 4 | sections_len — the bytes of entries plus sets (before the trailer; the footer's full length is 32 bytes more) |
| 4 | 4 | block_count |
| 8 | 8 | min_micros — the actual minimum over the blocks |
| 16 | 8 | max_micros — the actual maximum |
| 24 | 4 | CRC32C of the footer (entries + sets + trailer bytes 0..24) |
| 28 | 4 | magic `"DFTR"` |

Reading a sealed segment: the trailer from the end → the footer → a binary
search for a block by time. A broken footer means degrading to a scan of the
block headers (as with an active segment).

Blocks are selected by **both** window bounds and in **both** walk directions.
The lower cuts off blocks lying entirely in the past; the upper cuts off blocks
whose base is later than the end of the window (the base is the time of the
block's first record, so the rest are later still). A block whose base falls
into the window at one edge is read whole: what lies inside it cannot be known
without reading, and discarding it would lose records rather than save a read.
The direction does not affect the bounds — a query's default order is reversed
(the fresh first), and a one-sided skip would not work in the commonest
scenario at all.

Parsing the footer does not allocate from counters in the file: CRC32C is not
a signature, and anyone can recompute it. Capacity is bounded by what would
physically fit in the sections. Surplus bytes after the parsed sections are
rejected: the length in the trailer is covered by the CRC, so a discrepancy is
a sign of forgery rather than of slack.

## 5. The write rules

- The order of bytes to disk: append only; a block is written with one
  `pwrite` (header plus body), then the channel's policy (`fdatasync` for
  Immediate). **Visibility and durability are different moments**: records
  become readable right after the `pwrite` (the page cache is coherent) and
  survive a power loss after the `fdatasync`. Hence the different attitudes to
  an unfinished tail at read time: `pwrite` is not atomic, and a reader sees a
  block whose header is already there and whose body is not (§3, "three
  distinguishable states of the tail"). A reader of a live store passes such a
  tail over, a subscription defers it until it arrives whole, and a dump
  declares it corruption.
- A block is closed by: reaching `block_max_bytes`, the `flush_interval`
  timer, a record arriving in an Immediate channel (a group commit — one
  `fdatasync` per batch rather than per record), sealing a segment, shutdown.
- A segment is sealed by: not enough room for a whole block, a change of boot,
  closing a namespace, shutdown.
- A block that did not fit the current segment moves to a new one with its
  `seq` and CRC recomputed: block numbering restarts in every segment. A block
  that does not fit even a fresh segment (one record larger than a whole
  segment — an incompressible blob) is **discarded**: growing a segment past
  its limit would cancel the rotation boundary — a segment would grow for one
  record without end, while a class budget evicts only whole segments. The
  loss is counted and announced by a notice in the stream.
- **Buffer memory**: a block buffer is allocated lazily, on a channel's first
  record, and is **given back to the allocator whole** once the channel has
  nothing left to service (no open block and no sync deadline) — but no sooner
  than a couple of seconds of idleness. A buffer's capacity is the imprint of
  the largest block: without a return, one megabyte blob would pin ~2× its own
  size to a channel forever, and a steady 64–128 KiB per channel at ~24k
  namespaces would add up to gigabytes.

  The delay is mandatory. A channel with immediate durability turns out to be
  "idle" after **every** group commit — the block is flushed and there is
  nothing to sync — and an instant return would mean freeing and reallocating
  the block buffer and its scratch on every critical record, that is, on
  exactly the path the channel exists to make fast. Genuine idleness will not
  escape the pause.

  The LZ4 output is reused between flushes (in the steady state neither an
  allocation nor a memset), and the footer's block index gives its slack back
  after sealing.

  The optional **ceiling on the total bytes of channels writing at once**
  (`StoreConfig::buffer_ceiling_bytes`) is for where "only a handful write"
  stops being true: a class with hundreds of channels writing at once at 64
  KiB per block is tens of megabytes, and armv7 may not have them. It is
  honoured **between turns of the loop** rather than on every record, and it
  cannot be hard: the block-closing threshold is checked AFTER a record is
  added, and one record can be larger than any reasonable ceiling. An excess
  is removed by flushing a block and giving the buffers of the largest holders
  back — in that order, because a loaded accumulator does not shrink; a
  channel with no open segment is skipped (a flush would discard its block).
  What could not be returned is announced by the `buffer_overruns` counter
  rather than by discarded records: losing data for the sake of memory
  accounting is the worst outcome there is. The footer's block index is not
  counted against the ceiling: it describes an open segment, is not returned
  while its entries are live and goes with sealing.
- **An idle channel's segment is released** (not sealed) once the channel has
  had nothing to service for noticeably longer than it takes to give the
  buffers back (minutes against a couple of seconds): `fdatasync` →
  `ftruncate` to the end of the data → closing the file. An open segment costs
  a file descriptor and is counted in the budget together with the unwritten
  tail of its reserve window. At the tens of thousands of channels claimed,
  quiet ones would hold tens of thousands of descriptors while only a handful
  write at any moment.

  A released segment stays **unsealed and continuable**: the next record opens
  it again and restores the position by walking the blocks; the reserve window
  is not restored in the process — the first block will extend it. Sealing a
  segment would mean starting a new file on every wake-up — a channel writing
  once an hour would leave eight thousand tiny segments a year, and byte-based
  rotation would remove none of them because there are hardly any bytes in
  them: the files would run out before the space did.

  If the process stopped before the wake-up, the segment stays unsealed — it
  reads by scanning, and the next start appends its footer in the same walk it
  uses to find the end of the data (see the recovery in §2). Opening every
  quiet channel for a footer at shutdown is not an option: at the scale
  claimed that is tens of thousands of opens.

  The pause is longer than the buffer one because the price is different:
  buffers cost a couple of allocations, a segment an `fdatasync`, an
  `ftruncate` and a walk over the file when it comes back. A channel writing at
  least once every few minutes never gets here.
- **The budget is a property of a storage class, shared across the whole
  store.** "All telemetry gets this much, all logs get that much": the
  channels of every namespace of a class draw on one budget, and when it is
  exceeded the **class's** oldest segment is deleted whoever's namespace it
  lies in — a quiet service does not hold space a noisy one lacks. The number
  of namespaces does not affect the budget: there are thousands of them and it
  is not known in advance. The sum of the class budgets is the occupancy
  ceiling; there is no separate "store ceiling" knob — classes may live on
  different media (a class has its own `custom_root:`, with critical data on a
  protected partition), and a shared ceiling would mean nothing.

  The optional **personal quota of a namespace** (`NsQuota` at open time) is a
  limit inside the shared budget: such a namespace's channels rotate within it
  without waiting for the class to hit its budget. Without a quota there is no
  per-channel rotation at all. A quota (and the other channel settings apart
  from the budget and the medium, which belong to the class) can be set for a
  whole **group** of namespaces at once, that is, for a shared prefix of their
  names; on disk that is no different from a quota named at each one's open.

  The eviction order is global by construction: a segment's name is the pair
  `(run number, microseconds)`, and the run number is one for the whole store.
  A segment a channel writes to or will go on writing to (active, or released
  for idleness) is never evicted. Hence a limit: a class budget has to exceed
  what the live segments have **taken** — and they take their reserve window,
  which grows along with what was written, rather than `segment_bytes` (the
  segment size is fixed rather than derived from the budget, so that one noisy
  channel does not gain the right to take the whole budget before the first
  rotation); an unmeetable budget is not broken silently but counted by a
  counter of its own.
- **Running out of space on the medium** is cured across the whole medium
  rather than only in the channel that hit it: first the segments of quiet
  channels are released (giving back the unwritten tail of their windows),
  then the oldest history **on that same medium** is evicted — down to a
  volume enough for a whole segment. Rotation within one channel is useless
  here: the space is taken by someone else; eviction on another partition is
  useless too — the bytes are not there.
- **Draining the queue before `sync` and `shutdown`** is bounded by a number
  of passes: without a bound neither would return until the application
  threads fell silent. The remainder is **discarded only on `shutdown`** —
  after it there will be nobody to write it. `sync` leaves the remainder in
  the queue (the ordinary course of the loop will write it) and tells the
  caller that the promise "everything accumulated is on the medium" was not
  kept in full. Releasing a namespace discards nothing either: the queue is
  shared by the process and holds records of other, living namespaces.

## 6. epochs.bin

A small file (kilobytes), updated atomically: temp plus `fdatasync` plus
`rename`. A postcard-serialized structure:

```
Epochs {
  runs:     Vec<Run>,
  hw_boots: Vec<HwBoot>,
}

Run    { boot_counter u32, hw_boot_id u32, boottime_at_init_us u64 }
HwBoot { hw_boot_id u32,
         kernel_boot_id [u8;16],          // /proc/sys/kernel/random/boot_id
         utc_anchor_ms Option<i64>,       // the UTC corresponding to BOOTTIME == 0
         anchor_source Option<u8>,        // 1 User, 2 NTP, 3 GPS
         anchor_captured_us Option<u64> }
```

- A run is registered at start; the hardware boot is determined from the
  kernel's `kernel_boot_id` rather than from `BOOTTIME` increasing (the latter
  got it wrong on a quick restart after a reboot).
- A run number is one greater than the largest in the file. If no file is left
  (a quarantine after corruption, a cleanup of the medium), the lower bound is
  taken from the **segment names**: they outlived the file, and `boot_counter`
  is written into every one of them. Starting the numbering over would place
  new segments before the old ones in the history — the order of names is
  declared chronological — and rotation, deleting the oldest, would start on
  the fresh records. The walk over names is done only in that case: with an
  intact file the maximum over `runs` already covers everything on disk.
- `utc_anchor_ms` is **milliseconds as an integer** rather than a serialized
  date: eight bytes against twelve, and the file format does not depend on how
  some other crate represents a date. The API hands out a `DateTime<Utc>`
  (chrono).
- The conversion both ways is **to the microsecond**: the anchor is quantized
  to milliseconds, but the offset within a run is added as it is, so a
  wall-clock query bound and the records it finds match exactly (rounding to
  milliseconds would move it by ±0.5 ms).
- Updating the anchor: it is accepted if `new.source ≥ current.source` (GPS
  over a manual entry yes; a manual entry over GPS no; a fresh GPS fix refines
  an old one). The conversion to UTC happens at read time and is retroactive:
  one synchronization gives absolute time to events recorded before it too.
- A knowably implausible moment in time (outside 2001-09-09 … 2100-01-01) is
  rejected: the anchor is retroactive, and one call with garbage would distort
  the UTC of that boot's whole history.
- A damaged file is moved to quarantine (`epochs.corrupt`) **only by the
  writing side**. A reader writes nothing: a dump brought in for analysis may
  sit on read-only media or be material evidence in the incident being
  investigated.
- Retention: runs of which no segments are left are cleaned out
  (`Store::compact_epochs`). The live runs are determined by a walk over
  segment **names** — a `readdir` per channel, without opening a single file;
  the current run is never deleted. The cleanup is called on its own when the
  store comes up, but only once the file really has grown (of the order of a
  thousand entries): walking directories with thousands of namespaces is not
  free, while a stale entry costs tens of bytes. A run of which segments
  remain is not touched — otherwise its records would lose their anchoring to
  UTC.

## 7. What is deliberately NOT on disk

Resolved from the binary's schema at read time:

- the text and the templates of every configured language, the `level`, the
  event names;
- the metric names, the units, the static tags;
- **the labels and severities of the states** of an enum metric: only the
  state code reaches the disk;
- **the metric's kind** (`gauge` / `state` / `counter`) — a hint for whoever
  draws it: a state is held as a step, a continuous quantity is interpolated;
- **the value limits** (the ranges of what is normal, outside which a value is
  a warning — `warn:` — or a fault — `alarm:`; for shapes a range cannot
  express there are the trigger predicates `warn_if:`/`alarm_if:` with the
  opposite polarity; the word `critical` is taken by the storage class and is
  not used for severity). A limit is a property of the installation, not of
  the measurement: one and the same temperature is normal for one amplifier
  and critical for another. Limits are set in the schema and are **overridden
  at runtime** per namespace — by data (`set_thresholds`) or by a closure with
  captured context (`set_severity_fn`, which beats everything); none of it is
  written to disk. A consequence worth knowing: an offline viewer sees only
  the schema's defaults — its predicates included, but not the runtime
  settings.

Records of types absent from the current schema are read as "an unknown type"
(skipped by `payload_len`) — until a migration is run.

## 8. Migrations

Two places carry a protocol version, and they mean different things:

- **a segment's header** — the schema's version at write time; every segment
  has its own, and a mixed state of a directory is legitimate;
- **`ns-meta`** — the version of the last **completed** migration. It is
  stamped when a namespace is first created and at the end of a successful
  run — and never on a plain bring-up: metadata that declared a migration
  complete too early would make a future run walk past segments that were
  never rewritten.

The step `N => …` brings records of version N up to version N+1; the chain has
to be unbroken from 1 to the current version (checked by schema validation).
The `events`/`metrics`/`spans` sets a step declares are a **binding filter**:
they decide both "is this segment worth rewriting" and "which records the step
will see" — records outside the sets pass the step byte for byte. A step
without sets (`touches_all`) sees every record, text and spans included.

Saving segments works only for events and metrics: their sets lie in the
footer, and an intersection answers "there is nothing like this here" without
reading any blocks. There is **no** set of span kinds in the footer, so a step
that declared `spans` rewrites every segment. Deciding "no" on a guess would
leave those records in the earlier layout forever — silently at that: the run
would report success and stamp the metadata, and the next pass would skip
those segments.

A step's outcomes: leave as is; replace a message's type and payload; change a
sample's metric (the value is not copied in the process); replace a sample's
value; rename a span kind; delete the record. A sample's value is
self-describing — the type lies in the record itself — so transforming a value
needs no declared old layout: what a rule **reads** it declares itself. What it
**returns**, though, is held by the schema: an ordinary write does not let
through a sample whose type contradicts the schema, and a migration has no
right to be a gap in that check — for a typed rule it is a compile error. Only
a span's kind changes: its number and parent are the record's identity,
referred to by its end, its messages and its children, and a chain has nothing
to rewrite those references with; for the same reason a rule does not delete a
span's start.

**Reading** applies the chain on the fly: a segment of version V < the current
one is read through the steps V → … → current, so the answer is correct
without a physical run. A sealed segment whose footer sets intersect no step
is read with no overhead at all. A segment of a version **newer** than the
reader's schema is not parsed at all and is declared damage: there is nothing
to decode a layout from the future with, and silence would look like "the
device wrote nothing".

**A physical run** (`Namespace::migrate`) is an explicit call by the
application rather than something automatic: it reads and rewrites the history
and burns flash wear. Segment by segment:

- a segment of a version ≥ the current one is skipped;
- one that is sealed and touched by no step is skipped; its header **keeps the
  earlier version**, and that is legitimate: being untouched is precisely what
  makes the current decoders read it correctly;
- otherwise it is rewritten: `<name>.seg.tmp` next to the original → the
  blocks are carried over one for one (emptied ones drop out; compression
  follows the source block's flag), the records go through the chain, the
  footer is assembled anew and the header gets the current version →
  `fdatasync` → an atomic `rename` over the old name → an `fsync` of the
  directory. A segment emptied entirely is deleted.

A segment's name and `base` are **preserved** even if a step deleted the
leading records: `base` is then earlier than the first record, and selecting
segments by name only becomes more conservative (a segment may be read a
needless extra time — but never skipped). A power loss at any step is safe:
before the `rename` the original is untouched and the `*.tmp` is swept up at
the next open; after it the segment is already at the new version and a repeat
run will skip it. A run is idempotent and resumable from any point.

A run's heavy work happens on the calling thread; **committing** each segment
(the rename and the inventory edit) goes as a command through the writer, the
sole owner of the inventory and of rotation: a segment rotated during the
rewrite does not come back — the commit answers "it is gone" and the result is
thrown away.

**Space.** While a rewrite goes on, the original and the temporary file lie on
the medium together, so a run needs roughly **one segment** of free space on
top of what is occupied — and no more: the capacity of the `*.tmp` is taken
from the original's actual data and grows only if a step really did swell the
records. That file does not count towards the class budget, and the engine
frees no space for it by eviction: an ENOSPC means the run is deferred, the
original is untouched and the metadata is not stamped.

Segments with an unreadable header and segments of a foreign store are not
touched by a run and do not stand in the way of a migration completing: no
schema version would have read them.
