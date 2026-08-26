# dduroc — a logging system for embedded Linux

A design document. The implementation: the crates `dduroc-format`,
`dduroc-engine`, `dduroc-read`, `dduroc-macros`, `dduroc`. The byte format is
in [SPEC.md](SPEC.md), the measurements in [BENCHMARKS.md](BENCHMARKS.md).

Not implemented (deliberately deferred):

- **the web layer** (`dduroc-graphql`, `dduroc-web`, `dduroc-viewer`) and the
  frontend;
- **the bridge from tracing/log** — the `Text` record kind exists in the
  format, the bridge does not;
- **continuing to write into a segment of a previous run**:
  `SegmentWriter::reopen` is implemented and tested but unused — by the format
  a segment does not cross a run boundary, and at start `boot_counter` is
  always new. A torn segment is sealed instead (see below), and `reopen`
  remains a groundwork for recovery within one run.

## Goals and constraints

- Rust, embedded Linux (no RTC), the media are eMMC and SD cards.
- A relative time system; conversion to UTC is a best-effort layer on top.
- The minimum volume of stored data and the minimum flash wear.
- The storage budget is custom: the design reference point is ~20 GB, scaling
  from 2–8 GB to hundreds of GB. The library is reused across projects.
- Durability within one engine: critical messages are synced at once — that is
  the definition of their class — while the rest only have their `fdatasync`
  interval configured. The storage class is stated per message.
- Scale: up to ~200 microservice types × up to 64 instances (up to ~12.8k
  namespaces); up to ~150 message types and ~100 metrics per unit.
- The target architecture is **armv7** (32-bit), cross-compiled with
  **cargo-zigbuild**. The consequences: file sizes and offsets are always
  `u64`, never `usize`; large segments are not mmapped (pread — already
  decided); CRC32C on armv7 has no hardware instruction (it appeared in
  ARMv8) — the `crc32c` crate's software fallback is enough at our speeds.

## Decisions (settled)

### Rejecting LSM

The prototype (radio-hog: fjall plus redb) was rejected: log keys are
monotonic and LSM's sorting is not needed; a write amplification of ≥2×
(a WAL plus a memtable flush) is bad for flash; the caches and background
workers are superfluous. fjall's FIFO compaction is the same deletion of old
files, but with overhead on top.

### A segmented append-only log

- **A segment** is a file `<boot>-<µs of the first record>.log` in a channel's
  directory. Append only. It is sealed on reaching the size limit or on a
  change of boot. Rotation is an `unlink` of the oldest segment when a
  channel's budget is exceeded.
- **A block** is the unit of writing and of a flush (= a writer batch):
  `header{base_time, count, uncompressed_len, compressed_len, crc32c}` plus
  the records, optionally LZ4/zstd per block. A block may hold a single record
  (the critical channel).
- **A record inside a block**: `varint Δµs from base_time` plus an event
  header plus a postcard payload. Delta-encoded time: 1–3 bytes instead of
  LSM's 10-byte key.
- **Recovery**: no WAL is needed — the log is the WAL. A scan of the last
  segment by block CRCs, stopping at the first broken or zero header, and a
  truncation of the tail. It is done when a channel comes up: a torn segment
  of a previous run is truncated to the end of the intact data and **sealed** —
  the footer is assembled in the same walk that looks for that end, so no
  second pass is needed. The point is not the convenience of reading: while a
  segment is unsealed, the unwritten tail of its reserve window is counted
  against it, and several crash stops in a row eat the channel's budget with
  emptiness, after which rotation starts on live history. Only a segment of a
  foreign run is touched — the live state of this same process may be writing
  into one of the current run.
- **Reading**: a binary search over the segment names → the footer's block
  index for a sealed segment (or a scan of the headers for an active one) →
  decompressing a block.
- **Spans** — with no keyspace of their own and no update semantics: ordinary
  `SpanStart`/`SpanEnd` events in the shared stream, and the tree is assembled
  at read time. Spans rotate along with the events — there are no dangling
  span ids.

### The data model

One OS process; the "microservices" are tasks and threads inside it. One
writer task, with the span context in a task_local.

**Namespaces instead of an origin.** Two notions are kept apart:
- **A schema** is compile-time: the declarations of events, spans and metrics
  (`register_events!`), the protocol version, the migration chain. It belongs
  to the microservice's code.
- **A namespace** is runtime: a named storage area a microservice brings up at
  start, binding it to a schema. Physically a subdirectory
  `<root>/<namespace>/<channel>/…` with its own segments and metadata (the
  schema name, the protocol version).

Records **do not store an origin at all** — belonging is implicit from where
they lie. Every microservice has a unique name and id: four instances of an
amplifier service are four namespaces `orc-radio-0` … `orc-radio-3` sharing a
schema. There is no separate notion of an instance anywhere.

**Namespace groups** go by name prefix: `orc-*` for orchestrators, `apt-*` for
adapters. A group is used for queries ("all the orchestrators' logs") and as a
level of default configuration (channels and budgets are inherited group →
namespace).

**Rotation budgets** are per storage class, shared across the whole store:
"all telemetry gets this much". The class's oldest segment is evicted across
namespace boundaries; a namespace's personal quota is an optional limit inside
the shared one (`NsQuota` at open time). A class may have a root of its own
(`custom_root:`) — critical data on a protected partition.

Reading "everything over a period" is a k-way merge of streams (namespaces ×
channels) by time. The price of the model: an active block buffer per
(namespace, channel); the group commit of Immediate channels works per
namespace.

**A message**:
- the event type — an id within the namespace's schema (`register_events!`);
- the span_id (from the runtime context);
- **the data** — the event's typed fields, postcard on disk; the text is not
  stored but rendered at read time from a template in the binary.

**Template languages are configurable**: en/ru is one project's particular
case; another may need en+ja+zh. An application declares the list of languages
once (a const), the macro demands a template in every language for every
event, and the read API accepts a language code. The library is agnostic to
the set.

Only what varies is written to disk. Every static property of a type lives in
the binary's registry and takes not a byte on disk:
- **level** — a given event type has a given level, known at compile time; it
  is resolved at read time through the registry;
- **tags** — static only, in the event type's declaration; there are no
  dynamic (runtime) tags.

Old records stay readable not through self-description but through
**migrations** (see below): the store is always brought up to the schema of
the current build.

**A span** marks a stretch of the system's work (an example: an amplifier
calibration → a child span "power ramp" → messages attached to it). Two
records in the shared stream: `SpanStart{span_id, kind, parent}` and
`SpanEnd{span_id}`. One and the same message types attach to different spans
at runtime — the context propagates through a task_local, as in the prototype
(a drop guard records the end on cancellation of a future). A span_id is
global to the process (one allocator): the `SpanStart` lies in the initiator's
namespace, and other namespaces' messages refer to the span_id — resolved
during a merged read.

**Telemetry** is the history of how values change. **A series is a metric and
nothing else.** There are no runtime dimensions: several sensors of one kind
are expressed as different schema metrics (`temp_pa`, `temp_lna`) rather than
as one metric with a tag. The reason is the one messages have no runtime tags
for: what tells them apart is known at compile time, and there is no reason to
pay bytes for it in every sample.

A sample is `[Δt varint][metric_id 1–2B][the value]`, 4–8 bytes before
compression. No bookkeeping record, no interning, no table in the footer: a
sample is self-sufficient and there is nothing to reconstruct from it.

> It was not always so. In container version 1 a series was the pair
> `(metric, tags)`, a sample referred to a segment-local number, and a
> `SeriesDef` record tied them together. That gave exactly one critical
> defect — a series' identity was lost when reading in reverse (the default
> mode), after a jump in time and across a segment boundary — and a whole
> mechanism for restoring it, a duplicate table in the footer included.
> Dropping runtime tags removed both the defect and the mechanism.

**What lives in the schema rather than on disk** (beyond the general rule
about statics):

- **the labels and severities of the states** of an enum metric;
- **the metric's kind**: a `gauge` is interpolated, a `state` is held as a
  step, a `counter` is monotonic. Joining states with a straight line means
  showing values that never were — that is not cosmetic but a lie on a chart;
- **the value limits**: the range of what is normal, outside which a value is
  a warning (`warn:`), and the range outside which it is a fault (`alarm:`);
  for shapes a range cannot express there are the trigger predicates
  `warn_if:`/`alarm_if:`.

**Enum metrics.** A state machine (`los | sync | lock`) is a time series, a
band of states on a chart. The format did not change for it: a state code is
an ordinary integer, one byte for codes below 128. The codes are declared
**explicitly**: positional numbering would shift when a state was inserted
into the middle of the list, and segments already written would start reading
wrongly. The macro generates a Rust type, so the call site reads
`link.sample_state(LinkState::Lock)` rather than a bare number.

**Value limits.** The severity is called `warn` / `alarm` rather than
`warn` / `critical`: the word `critical` is taken by the storage class
(`store: critical` — resilience to a power loss), and in a metric declaration
the two stand side by side. They are different axes: the storage class says
*how to write*, the severity *what the value means*.

There are three forms of limit, and their roles differ deliberately:

- **ranges of what is normal** (`warn: -40.0..=70.0`) are data: the bands on
  charts are drawn from them and the runtime overrides them
  (`set_thresholds`). A range describes what is normal precisely because a
  two-sided condition is expressed by a single range;
- **trigger predicates** (`alarm_if: v > 3.0 || v < 1.0`) are arbitrary logic
  a range cannot express. A predicate's polarity is the opposite of a range's,
  hence a different key — the forms cannot be confused silently. A predicate
  compiles into a descriptor fn and works for any reader of this schema, but
  it is opaque: no bands on a chart come from it. It composes with ranges by
  the rule "the heaviest diagnosis wins";
- **a runtime closure** (`ns.set_severity_fn(metric, |v| …)`) is the extreme
  form: captured context (the hardware model, hysteresis) and full authority —
  it beats both the schema and the data overrides until it is removed.

A limit is a property of the **installation**, not of the measurement: one and
the same temperature is normal for one amplifier and critical for another.
Hence three consequences:

- limits are **never written to disk**. Otherwise the history would be marked
  up by thresholds nobody considers right any more;
- the defaults are declared in the schema and are **overridden at runtime** —
  an external system, having determined the hardware model, sets its own. An
  override lives in memory at the **namespace** level: every instance has
  hardware of its own, and process-wide limits would be wrong for all of them
  at once;
- an offline viewer sees only the schema's defaults. That is a price, not an
  oversight: runtime settings belong to a running process.

The engine does **not apply** the limits itself: it does not decide for the
application what to do about a bound being crossed, and it raises no events.
It only answers the question "how much does this value call for attention" —
to the application and to the future web layer.

**The sparseness of state series.** States are written on change, or there is
no point to them. So a query's window may hold not one sample of a series that
held one value the whole time. On request the reader carries the last sample
of every state series through to **before** the window's start — as a separate
field of the answer, so as not to break the promise that everything is inside
the range. The search is bounded to two segments back and discards segments
without the metrics wanted by the set of identifiers in the footer; a series
unchanged for a very long time will be left without a seed — which is more
honest than reading the whole history for one value.

### The protocol version and migrations

A message schema can change between firmware versions. The answer is a system
of **migrations** (rather than self-describing segments):

- **The protocol version is per schema and applied per namespace**: a
  monotonic number in the schema declaration; every namespace migrates
  independently (updating one service migrates only its namespaces). The
  library provides the mechanism, while the number and the steps belong to the
  microservice's code.
- Every **segment** carries a version (as of write time) in its header, while
  a namespace's metadata carries the version of the last completed migration.
  A mixed state is legitimate, and a migration is idempotent ("rewrite the
  segments whose version is below the current one").
- **Migration steps** `vN → vN+1` are written by the developer along with the
  schema change: renaming or removing an event, adding a field with a default,
  remapping an id. A step is declared by its **source key**: `1 =>
  migrate_v1` is the step from version 1 to version 2, and the chain has to be
  unbroken from 1 to the current version. A chain of steps covers a jump
  across several versions. Remapping an id by a migration takes the edge off
  the prototype's positional auto-id problem.
- **Crash safety**: a migration goes segment by segment — rewrite into a
  temporary file → fdatasync → an atomic rename → unlink the old one. A power
  loss means continuing at the next start.
- **Saving wear**: a segment's footer holds the sets of event and metric type
  ids occurring in the segment. A migration rewrites only the segments with
  affected types; the rest are not touched (the version is held by the
  namespace's metadata, and an in-place stamp on a segment is not needed). For
  a raw fn the affected types are declared **explicitly** — `1 => migrate_v1 {
  events: [PowerSet] }` — and their absence means "it affects everything": a
  forgotten list must not silently turn into "we touch nothing". The sets are
  **binding** at the same time: a step sees only records of the declared
  types, so what is declared and what is done do not diverge.
- **Typed rules** are a step's main form. The old layout is declared in
  `history {}` (the macro generates `v1::PowerSet` with `Deserialize` and the
  old id), and a step is written as rules keyed by it:
  `v1::PowerSet: |old| events::PowerSet { dbm: f32::from(old.dbm) }` (the
  decoding, the transformation and the encoding are generated),
  `event(0x05): drop` (a removed type, by its bare id), `metric(0x07):
  metrics::TempPa` (a remap). The affected types are **inferred** from the
  keys, and an unused history entry is a compile error. The raw fn remains as
  a hatch.
- **Sample values and span kinds** migrate by the same rules:
  `metrics::Level: |v: u64| v as f32 / 10.0` (a quantity is now written in its
  own units), `span(0x01): spans::Calibration` (a kind was renamed). A value
  needs no `history`: it is self-describing — the type lies in the record
  itself — and the type is named by the closure's parameter; the check is
  strict and no conversion happens silently. The return, meanwhile, is held by
  the schema: a rule that yields a type the metric does not declare will not
  compile — otherwise a migration would become a gap in the very check `sample`
  is typed for. For the same reason a value is transformed only on a metric
  named by name: a bare id has no declared type. Only a span's **kind**
  changes: its number and parent are the record's identity, referred to by its
  end, its messages and its children. For the same reason a rule does not
  delete a span's start at all. Spans save no segments: there is no set of
  kinds in the footer and nothing to answer "there are no such spans here"
  with — a step with spans rewrites every segment. Deciding "no" on a guess
  would leave those records in the earlier layout forever: the run would report
  success and stamp the metadata.
- **Reading applies the chain on the fly**: a segment of an old version reads
  correctly even before a physical run — a run changes the medium, not the
  answer. When to call it is the application's decision
  (`Namespace::migrate`); the debt is visible in
  `Namespace::pending_migration()`.

### Channels (storage classes)

The engine is a set of named **channels**. A channel is a directory of
segments plus a configuration:

```
ChannelConfig {
    budget_bytes,        // the budget of the CLASS across the whole store
    segment_bytes,       // the growth LIMIT, which is also the rotation
                         // boundary (8 MiB; 4 MiB for critical). Not the
                         // file's size: space is taken as a window from
                         // 64 KiB, by eighths of the limit
    root,                // the class's own medium (critical data on jffs)
    block_max_bytes,     // the block buffer's size (64 KiB, say)
    flush_interval,      // the longest delay before flushing a block
    sync_interval,       // see below
    compression,         // None | Lz4 | Zstd(level)
}
```

Syncing is per channel with one interval (`sync_interval`): an `fdatasync` no
more often than that, plus always on sealing a segment and at shutdown. Zero
means right after every group commit (a group commit takes everything that has
piled up); that is how the critical channel works, and for it this is not a
setting but a **definition** — `Store::open` refuses a critical channel with a
non-zero interval. There are no separate "Immediate"/"Relaxed" states:
immediacy is a zero interval, and "on sealing only" is a large one.

A class's settings are shared by the whole store, and that holds exactly as
long as the namespaces are uniform. Twenty-four thousand never are, and the
difference is expressed by a **group** — a name prefix, the very one
`Query::group` selects by: "the orchestrators' journals" and "the
orchestrators' settings" have to denote one set, so the selection rule is one
and the same for reading and for writing (`in_group`).

```rust
StoreConfig::new("/data/logs").group("orc-", GroupPolicy::new()
    .channel(StorageClass::Telemetry, ChannelOverride::new().segment_bytes(2 << 20))
    .limit_bytes(StorageClass::Telemetry, 1 << 30))
```

When several prefixes match they **lay over** one another from the general to
the specific: `orc-radio-` refines `orc-` rather than replacing it, and a
setting the refining group did not mention comes from the general one.
Otherwise refining one segment size would silently remove the quota, the
compression and the intervals set for every orchestrator. Settings named when
a namespace is opened beat the group's. A group sets only what belongs to
**each writing channel separately**: the segment and block sizes, the
intervals, the compression, the personal quota. The class budget and its
medium are not available to it, and not as an oversight: the budget is shared
by the class and is the occupancy ceiling ("all telemetry gets this much"),
and a group with a budget of its own would either raise that ceiling above
what was declared or spread one class across two media, where a shared budget
would stop meaning anything. A group's quota is a limit **inside** the class
budget and is assigned to every namespace of the group separately: "no
orchestrator takes more than a gigabyte", not "all of them together". A group
is validated by the same rules as a class: a setting unfit for a class does
not become fit by being given to a group.

Routing works by **significance declared on the type**: message types, metrics
(and span kinds) all declare a storage class in `register_events!` (`store:
critical`, for instance). What matters is written to the critical channel (an
fdatasync per burst), what does not to the ordinary one (batches, deferred
syncing). The default is the ordinary channel. One block and record format in
every channel — only the policies differ.

There are exactly three classes: `default`, `critical`, `telemetry`. An
unfamiliar name in `store:` is a **compile error** rather than a new class:
otherwise a typo in `critical` would give a channel with a different name, a
different durability policy and a different budget — that is, exactly what a
storage class protects against, and with no sign at all. A namespace
**always** has the ordinary channel, even if no type declared it: free text
(the bridge from foreign logs, the panic handler, the one-off announcement of
a build defect) has nowhere to get a class of its own, and it has to be
written.

#### The queues

There are two queues — ordinary and critical — and both are allocated whole
when a store is opened. The capacities are configurable
(`StoreConfig::with_queues`): the defaults (8192 / 1024) cost about three
quarters of a megabyte per process, which is not always appropriate on armv7.
A smaller queue saves memory but starts losing records sooner on bursts.

Waiting for room in the critical queue is the only place where writing blocks
the caller, and there is an exception to it: **`SpanGuard::drop` never
waits**. A guard is dropped during stack unwinding after a panic too, and
there a five-second wait would turn an emergency shutdown into a hang, with
nested guards stacking their timeouts on top of one another. The price is a
span's end lost under pressure; it is counted and noted in the stream, and a
reader has to be able to show an unclosed span in any case: a span cut short
by a process crash looks the same. Whoever needs the guarantee calls
`SpanGuard::close()` explicitly, where the channel's ordinary policy applies.

### Flash specifics (eMMC/SD)

- The appends are sequential; the block size is roughly a multiple of the
  FTL's page (4–64 KiB).
- Reserving a segment's space up front plus `fdatasync` instead of `fsync`: an
  append does not touch the filesystem's metadata, so the sync is cheaper. The
  tail of zeros in a reserved file naturally terminates a recovery scan.
- "Permanent" durability is implemented as a group commit — one fdatasync per
  burst of critical records (~1–10 ms on eMMC), not per record.

### Scaling

- RAM per channel: the active block buffer plus the segment inventory (a name
  → the first key), hundreds of bytes per segment. 200 GB at 256 MiB segments
  is about 800 inventory entries.
- A segment's block index is loaded only when that segment is read.
- **Descriptors and space are counted by the writers rather than by the
  channels that exist.** An open segment is a descriptor and a reserve window;
  both come back once a channel goes quiet (SPEC §5). So both quantities are
  bounded by the number of channels that wrote in the last few minutes rather
  than by their total number: at ~24k namespaces × 2–3 channels, the
  difference between "tens of thousands" and "a handful" is the difference
  between a working device and one that hit its `ulimit`. A released segment
  is **continued** rather than closed: otherwise a rarely writing channel
  would pay for the saving with a file per wake-up, and the inodes would run
  out before the space did.
- **The buffer memory ceiling** (`StoreConfig::buffer_ceiling_bytes`) is
  optional and absent by default: only a handful write at any moment. It is
  for where that stops being true, and it bounds **RAM** rather than space:
  the budget belongs to a class and a medium, the ceiling to the process. An
  excess is removed by giving buffers back rather than by discarding records;
  an unmeetable ceiling is visible in `Stats::buffer_overruns`.
- **The scale limit is set by what was written, not by what was
  configured.** An active segment cannot be evicted, so a class budget has to
  cover the sum of the live segments — but they are counted by their reserve
  window rather than by `segment_bytes` (SPEC §2). A channel that wrote a
  hundred bytes costs 64 KiB rather than 8 MiB: the same 8-gigabyte budget
  holds not ~1000 channels writing at once but the whole 24k fleet claimed, as
  long as they write a little at a time. An excess is still visible in
  `Stats::budget_overruns` rather than in the medium running out.

  A fleet measurement (one record per namespace, then closing):

  | Namespaces | Bring-up | Taken under load | Would be with a whole-segment reserve |
  |---|---|---|---|
  | 1,000 | 0.19 s | 66 MB | 8 GiB |
  | 4,000 | 0.51 s | 266 MB | 32 GiB |
  | 24,000 | 2.80 s | 1.1 GB | 192 GiB |

## Inherited from the prototype (proven decisions)

- The time model: `BootTime` = `(boot_counter, µs since the run started)` as
  **one type** (apart, those numbers are not a moment: `Micros` of different
  runs are not comparable), the source is CLOCK_BOOTTIME; the identity of a
  hardware boot comes from `/proc/sys/kernel/random/boot_id`; the UTC anchor
  is retroactive and per hardware boot. Wall-clock time is handed out as a
  `chrono::DateTime<Utc>`. **The anchor is updatable, with source priority**:
  User/Manual < NTP < GPS. A new synchronization overwrites the anchor only if
  its priority is ≥ the current one (GPS over a manual entry yes, a manual
  entry over GPS no; a fresh GPS fix refines an old one). The conversion
  happens at read time, so refining an anchor retroactively improves the UTC
  of every event of that boot.
- A postcard payload plus decoders and templates (en/ru) in the binary, not on
  disk.
- A declarative macro for registering events with explicit IDs.
- A single writer task with batching.
- A drop guard for ending spans (recorded on cancellation of a future).

## Known prototype problems the new design has to solve

- [ ] The width of boot_counter: u16 in Time against u32 in the epochs —
      unify it.
- [ ] Positional auto-ids for events → a silent remapping of historical logs.
- [ ] The schema only in the binary: an old firmware's logs are unreadable;
      there is no versioning of the payload's shape.
- [ ] Silent losses: events before the writer starts, channel overflow.
- [ ] Global singletons — untestability.

## The crates and the API

Decisions: the web interface is **on the device plus an offline dump viewer**;
the protocol is **GraphQL** (as in the prototype); the ergonomics are an
**explicit handle** for a namespace. The frontend SPA is **TypeScript plus
Svelte** (charts with uPlot/ECharts). The project name and crate prefix are
**dduroc**.

**The writer's queue**: the ordinary channel drops plus counts an overflow (as
the prototype did); the critical channel applies **back pressure**:
`ns.log()` of a critical event blocks (with a timeout) until room frees up —
critical events are rare, so blocking is all but unreachable, but the
guarantee is honest.

**The tracing/log bridge** (intercepting third-party crates' logs and panics)
comes second; the Text record kind (0x5) is reserved in the format from the
start.

**Off-the-shelf crates**: postcard/serde (the payload), crc32c (a hardware
CRC), lz4_flex, zstd (optional), rustix (syscalls), crossbeam-channel (the
writer's queue), globset, chrono (the read layer), thiserror, syn/quote,
axum plus async-graphql plus tokio (the web), proptest (the format's tests).
What is our own is only the engine (there is no alternative in the ecosystem
without LSM or B-tree overhead), the format and the schema macro (tracing does
not do as a frontend: no stable ids, no schema, no i18n, no migrations).

### The workspace

| Crate | Contents | Dependencies |
|---|---|---|
| `dduroc-format` | the byte format: segments, blocks, records, varint, CRC | minimal, sync, no tokio |
| `dduroc-engine` | the engine: Store, namespaces, channels, the writer thread, rotation, recovery, migrations, epochs | format; **no tokio** — the writer is a dedicated OS thread |
| `dduroc-macros` | the proc-macro for the schema declaration | syn/quote |
| `dduroc` | the facade: re-exports, the Namespace/Span/Series handles, the LogEvent trait | engine, macros |
| `dduroc-read` | the reader: the k-way merge, filters, schema resolution, UTC | format, engine (or a standalone open of a directory) |
| `dduroc-graphql` | a GraphQL schema over read; a subscription stub for live | async-graphql |
| `dduroc-web` | an axum router: the GraphQL endpoint plus the embedded SPA statics | graphql, tokio |
| `dduroc-viewer` | the offline viewer library: open a dump and bring the same dduroc-web up locally | read, web |

The core (format plus engine) is synchronous and independent of tokio: it can
be reused in projects without async, and the writer is simple (an OS thread,
an mpsc). tokio appears only in the web layer. The facade's span context: the
explicit handles below, and for async code an optional `tokio` feature with
task_local propagation.

**The viewer and the schema**: decoding requires a schema, so there is no
universal viewer — every project builds its own viewer binary: it links its
schema crates plus `dduroc-viewer` (about 10 lines of main). Records of
unknown types are shown as "unknown" (skipped by payload_len).

### A sketch of the API

```rust
// A service's crate — the schema declaration:
dduroc::schema! {
    name: radio, version: 3, languages: [en, ru],
    events {
        PowerSet = 0x01 { level: Info, store: critical,
            en: "power set to {dbm:.1} dBm", ru: "мощность {dbm:.1} дБм",
            dbm: f32 },
    }
    metrics {
        Temp = 0x01 { type: f32, unit: "°C", tags: [sensor] },
        Spectrum = 0x02 { type: blob, store: critical },
    }
    spans { Calibration = 0x01 }
    // The key is the version being migrated FROM; the chain is unbroken from 1.
    // The affected types are optional, but their absence means "everything".
    migrations {
        1 => migrate_v1 { events: [PowerSet], metrics: [Temp] },
        2 => migrate_v2,
    }
}

// main:
let store = Store::open(StoreConfig::new("/data/logs"))?; // epochs, boot registration

// a service instance brings its namespace up (the channel policies come from
// the store's settings and from the group the namespace belongs to by name
// prefix):
let ns = store.namespace("orc-radio-0", radio::SCHEMA)?;

// an unfinished migration is named rather than implied: the directory holds
// segments of an earlier layout, and that is worth writing to the journal.
if let Some((from, to)) = ns.pending_migration() { /* … */ }

// messages return nothing: logging does not influence control flow, and
// losses are counted in store.stats() and announced in the stream itself:
ns.log(radio::events::PowerSet { dbm: 27.5 });

// telemetry: a sample's type comes from the metric constant (Metric<f32>), so
// `sample(36u64)` is a compile error rather than a runtime refusal:
let temp = ns.series(radio::metrics::Temp)?;
temp.sample(36.6);

// a state is the same sample; another metric's enum fails the type check:
ns.series(radio::metrics::LinkState)?.sample(radio::metrics::LinkState::Lock);

// spans are a guard, with SpanEnd on drop (and on cancellation of a future):
let cal = ns.span(radio::spans::Calibration);              // a root span
let sub = cal.child(radio::spans::PowerRamp);              // a child
ns.log_in(&sub, radio::events::PowerSet { dbm: 30.0 });    // attached to the span
// sub.id() : SpanId — Copy, passed to other services for a cross-namespace link

// whoever needs a verdict at the call site has the paired try_*:
if let Err(e) = ns.try_log(radio::events::Overheat { t: 91.0 }) {
    if e.loses_record() { /* the disk is behind */ } else { /* a build defect */ }
}
```

Value bounds known only at runtime (an external system determined the hardware
model) — with the same range expressions as in the schema; never written to
disk:

```rust
ns.set_thresholds(radio::metrics::Temp, ..=60.0, ..=75.0)?;
ns.clear_limits(radio::metrics::Temp)?;   // the schema applies again
```

The bounds are written with the same type as the samples (`NumericValue`), so
they cannot be set on an enum metric or on `type: blob`: such a call used to
compile and then fail with `BadLimits` on the device. A metric known only at
runtime is served by `set_thresholds_raw` — there the compiler has nothing to
lean on and the runtime check remains. The question of severity is typed the
same way: `ns.severity_of(metrics::Temp, 65.0)`,
`link.severity_of(LinkState::Los)` — an `OwnedValue` need not be assembled by
hand anywhere except the `*_raw` paths.

Reading (the same API on a device and in a viewer):

```rust
// A live reader: parallel to writing by construction. Created once; the roots
// (the media of classes moved out included), the schemas of the namespaces
// that came up and the time anchors are asked of the store on every query —
// there is nothing for them to go stale from. Rotation underfoot and a
// segment's growing tail are ordinary events rather than damage.
let reader = store.reader();
// A foreign dump goes by hand, with EVERY root at once: a `Store` must not be
// opened there — it takes a lock on the root and sweeps temporary files. A
// dump missing some class's tree is refused at open time; showing part of the
// history silently is not allowed. The snapshot is frozen at open time, and
// anything unfinished is honestly reported as damage.
let reader = Reader::open_dump([path, vault], &[radio::SCHEMA])?;
let q = Query::new()
    .group("orc-")                             // every orchestrator instance
    .since(Utc::now() - TimeDelta::hours(2))   // or .since(BootTime)
    .min_level(Level::Warn)
    .order(Order::Newest)
    .limit(500);

let result = reader.query(&q)?;
// result.entries       — a time-merged stream of Message | Span | Sample | Text
// result.damaged       — what did not read; empty means the answer is complete
// result.unanchored    — the runs that dropped out of a wall-clock window: no anchor
println!("{:?}", reader.render(&result.entries[0], "en"));  // the text from the record's schema

// the same lazily: memory is bounded by one block per channel rather than by
// the answer's size. The only way to read a lot: an answer without a `limit`
// over 200 GB will not fit in memory.
for entry in reader.stream(&q)? {
    if done() { break; }                        // the walk can be broken off
}
// No descriptors are held meanwhile: a cursor is created per (namespace,
// channel) pair and the merge needs a head from each of them, so a permanently
// open file for each would mean tens of thousands of descriptors per query.
// What has been parsed (the header, the footer, the block bounds) lives in
// memory, and the file is opened for the duration of reading a batch.

// a subscription: the same window, but the stream does not end at the end of
// the data — it waits for more. The reader sleeps while there is nothing to
// write and wakes on the very first block that lands in a file, so no polling
// on a timer is needed.
let mut tail = reader.follow(&Query::new().since(ns.now()).order(Order::Oldest))?;
loop {
    match tail.next(Duration::from_millis(200)) {
        Tail::Entry(e) => draw(&e),
        Tail::Idle     => if stopping() { break; },  // silence, not the end
        Tail::Ended    => break,                     // nobody left to write
    }
}
```

A subscription refuses what it cannot promise instead of adjusting the query
silently: reverse order (`Order::Newest` — "the last hundred" of a stream with
no last record means nothing), an upper window bound (it reads what is not
there yet) and a dump (nobody appends to it). The window's bounds are computed
once at open time: a wall-clock bound is converted into a run's scale by an
anchor, and time synchronization is retroactive — recomputing would move the
window under a subscription that has already walked part of the stream and
cannot go back. The anchors, meanwhile, are refreshed: a record's UTC comes
from a synchronization that happened after the record itself.

The order between channels is by time within what is visible at the moment of
waking, and that is all that is honestly promised: channels sync by their own
policies (the critical one at once, the ordinary one once a second), so an
ordinary channel's record may become visible later than a critical one that
happened after it. Within one channel the order is exact — it is the order on
disk.

There are three seams a subscription must not lose anything at, and all three
set it apart from a one-off query: an **unfinished tail** of a block is
deferred rather than skipped (a query has a next query, a subscription does
not); **a change of segment** — the directory is listed anew, but only when
the store has announced that the segments changed, or a subscription does not
do a single `readdir`; **a namespace brought up later** than the subscription
is picked up — a viewer that started before a service is the ordinary order on
a device.

That is why there are three marks, and they differ in the cost of the answer:
"a block landed" means reading on through the tail of an open segment; "the
segment changed" means listing the directory of **that** channel; "a namespace
came up" means walking the root, reading `ns-meta` in every matching directory
and opening cursors for the newcomers. The last is the only work proportional
to the whole store, and only a namespace coming up triggers it: fused with
rotation it would mean walking twenty-four thousand directories every half
second for the sake of a segment that changed in one channel. The rest of the
reading-on is one cursor per channel per wake-up, and that is the price of a
mark shared by the store: it does not say which channel wrote. Whoever
narrowed the query pays the least — a subscription to a group pays for its
group.

A subscription hands out damage as a difference (`take_damage`) rather than as
an accumulated list: it lives a long time, and a list that only grows would
repeat one and the same damage in every batch.

The window's bounds are one type, `Timestamp`: either a `BootTime` (a run plus
µs since it started) or a `DateTime<Utc>`. A wall-clock bound is converted
into every run's scale by its anchor **before** scanning; a run with no anchor
cannot be matched against a wall clock, drops out of the selection and is
named in `result.unanchored` — silence would look like "the device wrote
nothing in those hours".

GraphQL is a thin wrapper over `dduroc-read`: the queries `logs / spans /
series / namespaces / storageStats`, cursor pagination (base64 positions) and
`subscription { tail }` over `Reader::follow`.

## Open questions

- The byte-level format of a record header (the event type id, the span_id) —
  the origin/instance is not stored; the level and the tags are not stored on
  disk.
- The span_id: its width and its uniqueness across boots (in the prototype a
  persistent monotonic u32).
- Storing the epochs and the configs: a file with an atomic rename against
  redb.
- Composite telemetry values (a spectrum): the payload's format, and whether a
  separate block type for high-frequency series is worth it.
- The behaviour on a writer queue overflow for the critical channel (back
  pressure against dropping).
- **A pagination cursor for the web layer.** `stream` makes it possible to
  read lazily and break the walk off, but there is nothing to continue it with
  in the next HTTP request: resuming "from the last record" is imprecise,
  because the clock is monotonic but not strictly increasing — several records
  land in one microsecond, and a `since(last.at)` bound either repeats them or
  loses them. What is needed is a token of the form (moment, ordinal within
  the moment); it is not designed yet.

  A subscription (`Reader::follow`) does not close this question and should
  not: it holds its own place, in the process's memory, and lives exactly as
  long as its object does. A token is needed where the place outlives the
  connection.
