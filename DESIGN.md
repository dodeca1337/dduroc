# dduroc — a logging system for embedded Linux

A design document. The implementation: the crates `dduroc-format`,
`dduroc-engine`, `dduroc-read`, `dduroc-macros`, `dduroc`. The byte format is
in [SPEC.md](SPEC.md), the measurements in [BENCHMARKS.md](BENCHMARKS.md).

The writer uses the pinned Raft Engine backend described below. The compact
record/block codec is independent of that backend. Legacy segment readers are
retained for explicit offline inspection; an old segment store is not opened
for writing or silently converted.

The web layer and a tracing/log bridge remain separate work.

## Goals and constraints

- Rust, embedded Linux (no RTC), the media are eMMC and SD cards.
- A relative time system; conversion to UTC is a best-effort layer on top.
- The minimum volume of stored data and the minimum flash wear.
- Storage budgets are configurable per class and namespace. The current backend
  imposes explicit capacity limits for its 32-bit indexes; the former 20 GB
  design target is not a claim of support for this backend on ARMv7.
- Durability within one engine: critical messages are synced at once — that is
  the definition of their class — while the rest only have their `fdatasync`
  interval configured. The storage class is stated per message.
- Historical sizing target: ~200 service types × 64 instances. This is not a
  measured capacity claim; current scenarios and limits are in BENCHMARKS.md.
- The target architecture is **armv7** (32-bit). Application file sizes and
  offsets use `u64`; the dependency still has internal `usize` accounting,
  so capacity and file-sequence spans are checked explicitly. Target CPU and
  storage performance require measurement on the device.

## Decisions (settled)

### Rejecting LSM

The prototype (radio-hog: fjall plus redb) was rejected: log keys are
monotonic and LSM's sorting is not needed; a write amplification of ≥2×
(a WAL plus a memtable flush) is bad for flash; the caches and background
workers are superfluous. fjall's FIFO compaction is the same deletion of old
files, but with overhead on top.

### Raft Engine block storage

The storage dependency is pinned to `raft-engine = 0.4.2`, with default features
disabled and `std_fs` enabled. `std_fs` is also used on the host so tests exercise
the backend used on ARMv7. The default Unix backend of this release fails to
compile for ARMv7 because of its `off_t` assumptions.

Each distinct configured root has one Engine, shared with live readers.
Distinct roots must not overlap or nest. Payload reads and purge execute on a
bounded worker dedicated to that engine
lifetime. This is required because the pinned upstream thread-local block
cache identifies file positions without identifying the engine. Reusing a
caller thread across roots can otherwise return another root's payload. The
worker is closed and joined rather than reused when its engine is dropped. Each
(namespace, storage class) has a persistent Raft group. Entries contain compact
dduroc blocks; metadata values contain identities, indexes and retention
accounting, not event payloads. Keeping multiple events in a block amortizes
the engine's per-entry memory index. Raft batch compression is disabled because
dduroc already compresses blocks.

The backend's files are opaque: dduroc does not implement a second WAL or
truncate them itself. Prefix removal is logged in an error-returning batch,
followed by explicit garbage collection. Bounded page reads exclude concurrent
prefix compaction and schema group replacement so normal retention cannot
appear as a missing/corrupt block halfway through a page. Logical retained bytes and allocated
filesystem bytes are different measurements. Released namespaces remain in
retention accounting, including after reopening the store.

**Durability:** a critical `try_log*` result succeeds only after a nonempty
synchronous engine write. Ordinary success means acceptance into a bounded
queue; its eventual persistence follows the configured flush/sync intervals
while the writer and storage are making progress. These intervals do not
guarantee a deadline under stalled I/O.
Public synchronization uses a nonempty metadata barrier on every root: the
published dependency's `Engine::sync()` is a no-op for an empty batch. An
unconfirmed write can nevertheless appear after restart; errors are not a
promise that retrying cannot duplicate an event.

**Recovery:** the backend selects `AbsoluteConsistency`. The dependency's
default tolerant recovery can classify a body-read EIO as a corrupt tail and
truncate acknowledged data. Strict recovery preserves files and returns an
error instead, including for a genuinely torn tail. Automatic repair is not
provided. Upstream I/O panics can terminate an aborting application; no critical
success is emitted after such a failure. The dependency tests in
`crates/dduroc-engine/tests/raft_faults.rs` pin these limitations.

**Compatibility:** the root marker is `dduroc-raft-v1`. Existing segment stores
are rejected before modification; moving their history needs an explicit
export/import into a different root. Changing a storage backend is distinct
from migrating an event schema.

### The data model

One OS process; the "microservices" are tasks and threads inside it. One
writer task, with the span context in a task_local.

**Namespaces instead of an origin.** Two notions are kept apart:
- **A schema** is compile-time: the declarations of events, spans and metrics
  (`register_events!`), the protocol version, the migration chain. It belongs
  to the microservice's code.
- **A namespace** is runtime: a named storage area a microservice brings up at
  start, binding it to a schema. It is bound to persistent backend groups with schema and protocol metadata;
  records do not repeat the namespace name.

Records **do not store an origin at all** — belonging is implicit from the
backend channel that contains them. Every microservice has a unique name and id: four instances of an
amplifier service are four namespaces `orc-radio-0` … `orc-radio-3` sharing a
schema. There is no separate notion of an instance anywhere.

**Namespace groups** go by name prefix: `orc-*` for orchestrators, `apt-*` for
adapters. A group is used for queries ("all the orchestrators' logs") and as a
level of default configuration (channels and budgets are inherited group →
namespace).

**Retention budgets** are per storage class, shared across the whole store:
"all telemetry gets this much". The class's oldest blocks are evicted across
namespace boundaries; a namespace's personal quota is an optional limit inside
the shared one (`NsQuota` at open time). A class may have a root of its own
(`custom_root:`) — critical data on a protected partition.

Reading "everything over a period" is a k-way merge of streams (namespaces ×
channels) by time. The price of the model includes an active block buffer per writing channel
and an in-memory backend index per retained block.

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

Old records stay readable through declared **migrations** (see below), applied
by the reader or explicitly persisted by a namespace migration. Opening a
store does not silently rewrite all historical payloads.

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
the range. The Raft reader searches at most 256 blocks with records in the seed window
per selected channel. Blocks newer than that window may also need scanning;
there is no time index, so this is not a constant-I/O promise. The legacy reader
uses two eligible segments and footer filtering. A state older than the search
horizon can be absent; neither mode claims a complete snapshot of all states.

### The protocol version and migrations

Event schemas keep independent monotonically increasing protocol versions.
Each block carries its original version; readers apply the declared migration
chain on demand. Missing steps are errors, never guessed conversions. Typed
rules still use the old layouts declared in `history {}`; raw migration
functions retain their binding event/metric filters. Event text and severity
remain properties of the current decoder rather than duplicated log payloads.

`Namespace::migrate` physically transforms a channel in one streaming pass.
It copies into a staging Raft group and publishes the replacement through an
atomic metadata switch after successful transformation. It does not overwrite
an old Raft index in place, which could truncate the remaining suffix. Failed
transformation leaves the published history unchanged; tracked staging groups
are cleaned up on retry/startup. Dropped blocks become explicit tombstones so
index positions remain meaningful. Reading skips tombstones while advancing
its bounded cursor.

Schema migration is distinct from converting a legacy segment store. No
in-place legacy backend conversion is performed by opening a Store.

### Channels (storage classes)

There are three storage classes: `default`, `critical`, and `telemetry`.
Class budgets are shared across namespaces; optional namespace quotas are
additional limits inside them. Channel policy controls block batching,
compression, flush intervals, sync intervals and the destination root.

The critical class requires a zero sync interval. A successful critical
`try_log*` call waits for its synchronous batch. Concurrent calls may share a
group commit; sequential calls that each wait for durability necessarily pay
separate barriers. Ordinary calls return after enqueueing, and explicit
`Store::sync`/namespace synchronization waits for persistence. A void logging
wrapper still invokes the fallible path and reports failures through counters;
it does not turn a failed write into a durable acknowledgement.

A class's settings are shared by the whole store, and that holds exactly as
long as the namespaces are uniform. Twenty-four thousand never are, and the
difference is expressed by a **group** — a name prefix, the very one
`Query::group` selects by: "the orchestrators' journals" and "the
orchestrators' settings" have to denote one set, so the selection rule is one
and the same for reading and for writing (`in_group`).

```rust
StoreConfig::new("/data/logs").group("orc-", GroupPolicy::new()
    .channel(StorageClass::Telemetry, ChannelOverride::new().block_max_bytes(16 << 10))
    .limit_bytes(StorageClass::Telemetry, 1 << 20))
```

When several prefixes match they **lay over** one another from the general to
the specific: `orc-radio-` refines `orc-` rather than replacing it, and a
setting the refining group did not mention comes from the general one.
Otherwise refining one block size would silently remove the quota, the
compression and the intervals set for every orchestrator. Settings named when
a namespace is opened beat the group's. A group sets only what belongs to
**each writing channel separately**: block size, intervals, compression and
personal quota. A per-group segment-size override is rejected because the
physical Raft log files are shared. The smallest class-level `segment_bytes`
on a root determines that root's file target size. The class budget and its
medium are not available to it, and not as an oversight: the budget is shared
by the class and is the occupancy ceiling ("all telemetry gets this much"),
and a group with a budget of its own would either raise that ceiling above
what was declared or spread one class across two media, where a shared budget
would stop meaning anything. A group's quota is a limit **inside** the class
budget and is assigned to every namespace of the group separately: "no
orchestrator takes more than a megabyte", not "all of them together". A group
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
Payload bytes have separate bounds: 16 MiB for the ordinary queue and 4 MiB
reserved for the critical queue; permits are released on dequeue or rejection.
Dynamic record data is limited to 1 MiB before enqueueing. The default active
builder ceiling is 16 MiB, with one bounded record of transient headroom; a
configured buffer ceiling can tighten it.

Critical calls can wait both for queue capacity and for durable completion.
There is an exception to waiting: **`SpanGuard::drop` never
waits**. A guard is dropped during stack unwinding after a panic too, and
there a five-second wait would turn an emergency shutdown into a hang, with
nested guards stacking their timeouts on top of one another. The price is a
span's end lost under pressure; it is counted and noted in the stream, and a
reader has to be able to show an unclosed span in any case: a span cut short
by a process crash looks the same. Whoever needs the guarantee calls
`SpanGuard::close()` explicitly, where the channel's ordinary policy applies.

### Flash and resource limits

Appends are sequential. The portable backend uses `sync_all()` and does not
preallocate filesystem space. A configured reserve is an admission budget, not
a physical reservation against other processes filling the same filesystem.
Host throughput and sync timings do not predict eMMC wear or power-loss safety.

Memory includes queued payloads, active block buffers, the engine's retained
entry index, catalog metadata and reader pages. Count limits alone do not bound
queued payload bytes. Readers bound both entry counts and returned bytes;
reading a single entry can still load its upstream log batch, so the writer
also bounds batch size. A merged query retains a decoded head block per
selected channel: RAM grows with selected channels and block size. A result
count limit alone does not bound the merge-head memory. Select namespaces and
time windows appropriately for the target memory budget. Local, correctly checksummed store files are the input
model; adversarial files with forged valid checksums are not claimed safe.

Configured class/namespace budgets bound retained payloads; root accounting
includes allocated log, rewrite and metadata files. An open store exclusively
owns its directory trees: callers must not write unrelated files there.
Managed control-file allocations are tracked on mutation; journal allocations
are measured from the filesystem before admission. A failed late namespace
registration can leave small control metadata, which remains counted against
physical and catalog limits; it is not an automatic rollback. The current per-root
class-budget sum is limited to 1 GiB. Its allocated-space admission ceiling is
the budget plus the larger of half the budget or 8 MiB, capped at 1.5 GiB.
These conservative limits avoid the pinned engine's internal 32-bit size
arithmetic overflowing; they are restrictions of this backend, not record
format limits. Garbage collection needs
working space and must propagate failures. Limits are checked rather than
silently wrapped on 32-bit machines. This is an admission ceiling with rewrite
headroom, not a proven hard bound on every upstream GC allocation. The channel
catalog and retained block index also have explicit count limits. See current
configuration validation and
`BENCHMARKS.md` for measured scenarios and their limits.

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
`ns.try_log()` of a critical event waits for queue capacity and then durable
completion. Failure must be handled by callers that require acknowledgement;
critical retention still expires old records according to its configured quota.

**The tracing/log bridge** (intercepting third-party crates' logs and panics)
comes second; the Text record kind (0x5) is reserved in the format from the
start.

**Off-the-shelf crates**: postcard/serde (the payload), crc32c (a hardware
CRC), lz4_flex, zstd (optional), rustix (syscalls), crossbeam-channel (the
writer's queue), globset, chrono (the read layer), thiserror, syn/quote,
axum plus async-graphql plus tokio (the web), proptest (the format's tests).
Raft Engine owns journal storage. Our code supplies the compact format,
schema macro, retention policy, durability acknowledgements and query layer.
The GraphQL/web/viewer crates below remain proposed integration layers.

### The workspace

| Crate | Contents | Dependencies |
|---|---|---|
| `dduroc-format` | the byte format: segments, blocks, records, varint, CRC | minimal, sync, no tokio |
| `dduroc-engine` | the engine: Store, namespaces, channels, the writer thread, Raft storage integration, retention, migrations, epochs | format; **no tokio** — the writer is a dedicated OS thread |
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

### A historical API sketch

The examples below illustrate the model; the compiled examples under
`crates/dduroc/examples/` define the current callable API.

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

// This convenience wrapper reports errors via counters. Critical calls still
// wait for storage; use try_log when the caller requires a durable verdict:
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
// there is nothing for them to go stale from. New blocks and retention
// are ordinary events rather than damage.
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
// Raft cursors share the live backend; they do not open a file per namespace.
// A merged stream keeps one decoded head block per selected channel. Narrow
// the query to control this memory cost; a result limit alone does not do so.

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

A subscription follows committed blocks, retention changes and newly registered
namespaces. Raft cursors retain bounded block pages and advance persistent
indexes; fully drained channels with no retained history are pruned. Dormant
channels with history retain their watermark so a wake-up cannot replay old
records. Namespace schema presentation caching is bounded separately.

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
