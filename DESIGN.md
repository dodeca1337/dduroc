# dduroc — a logging system for embedded Linux

A design document. The implementation: the crates `dduroc-format`,
`dduroc-engine`, `dduroc-read`, `dduroc-macros`, `dduroc`, `dduroc-graphql` and `dduroc-web`. The byte format is
in [SPEC.md](SPEC.md), the measurements in [BENCHMARKS.md](BENCHMARKS.md).

The writer uses the pinned Raft Engine backend described below. The compact
record/block codec is independent of that backend. Legacy segment readers are
retained for explicit offline inspection; an old segment store is not opened
for writing or silently converted.

The transport-neutral GraphQL adapter uses the controlled native API. An HTTP
host and tracing/log bridge remain separate work. The embedded Carbon frontend
and host-configured asset router are implemented in dduroc-web.

## Goals and constraints

- Rust, embedded Linux (no RTC), the media are eMMC and SD cards.
- A relative time system; conversion to UTC is a best-effort layer on top.
- The minimum volume of stored data and the minimum flash wear.
- Storage budgets are configurable per class and namespace. The current backend
  imposes explicit capacity limits for its 32-bit indexes; the former 20 GB
  design target is not a claim of support for this backend on ARMv7.
- Durability within one engine: critical calls wait for a synchronous Raft write,
  while ordinary channels have configurable flush/sync intervals. The storage class is stated per message.
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

**Compatibility:** `dduroc-raft-v1` is the marker filename. The 0.2 writer
upgrades its content to `dduroc raft store v2 stats-writer` and uses independent
store-metadata writer version 3; the record/block container codec remains 2.
Read-only 0.2 opens accept both marker variants. All engines are owned and all
existing store-id bindings are validated before markers change or any new
binding is written. Ordinary 0.1 opens reject the upgraded marker. A stale
pre-lock old check can still reach engine open, but metadata version 3 prevents
old logical writes from bypassing maintained counts. Upstream engine open may
create internal files, so no universal zero-filesystem-mutation claim is made.
Existing custom segment stores remain refused; import needs a different root.
Backend migration, writer compatibility and event-schema migration are separate.

### The data model

One OS process; the "microservices" are tasks and threads inside it. One
writer thread per Store, with explicit namespace and span handles.

**Namespaces instead of an origin.** Two notions are kept apart:
- **A schema** is compile-time: the declarations of events, spans and metrics
  (`schema!`), the protocol version, the migration chain. It belongs
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
- the event type — an id within the namespace's schema (`schema!`);
- the span_id (from the runtime context);
- **the data** — the event's typed fields, postcard on disk; the text is not
  stored but rendered at read time from a template in the binary.

**Template languages are configurable**: en/ru is one project's particular
case; another may need en+ja+zh. An application declares the list of languages
once (a const), the macro demands a template in every language for every
event. The library is agnostic to the set; ordered read-time preferences
select among each schema's own templates without storing translated strings.

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
at runtime through explicit guards (dropping a guard attempts a nonblocking
end record, including when a future is cancelled). A span_id is
allocated once within one Store activation: the `SpanStart` lies in the initiator's
namespace, and other namespaces' messages refer to the span_id — resolved
during a merged read.

**Telemetry** is the history of how values change. **A series is a metric and
nothing else.** There are no runtime dimensions: several sensors of one kind
are expressed as different schema metrics (`temp_pa`, `temp_lna`) rather than
as one metric with a tag. The reason is the one messages have no runtime tags
for: what tells them apart is known at compile time, and there is no reason to
pay bytes for it in every sample.

A sample stores `[kind/flags][Δt varint][metric_id varint][value]`. A small
state code can use four bytes; an f64 sample with small IDs/deltas uses eleven
bytes before compression. SPEC.md gives the byte layout and typical sizes. No bookkeeping record, no interning, no table in the footer: a
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
`link.sample(LinkState::Lock)` rather than a bare number.

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
the range. Classic convenience reads search at most 256 Raft blocks with records in the
seed window per selected channel. Controlled sessions instead scan under their
explicit cumulative ReadLimits without a separate hidden seed horizon. Blocks newer than that window may also need scanning;
there is no time index, so this is not a constant-I/O promise. The legacy reader
uses two eligible segments and footer filtering. A state older than the search
horizon can be absent; neither mode claims a complete snapshot of all states.

### Native API boundary

The native API targets version 0.2. Generated event,
metric and span-kind handles carry a compiled schema identity. A typed operation
must match the namespace's identity before serialization or queue admission;
matching a numeric ID, display name or compatible payload shape is insufficient.
A manual descriptor without compiled identity remains usable through explicit
raw APIs. These identities are process-local type information, not persisted
Rust TypeIds or a replacement for schema name/protocol version on disk. A live
Store rejects conflicting compiled declarations/versions under one schema name;
offline readers reject ambiguous registries before opening their backend.

Typed decoding must use the effective schema provenance attached to the entry
when it was read, including declared read-time migrations. It cannot infer a
foreign entry's schema merely from a matching namespace in another reader.
Cross-namespace span attachment within the same active store remains supported;
a span guard from a different store activation is rejected.

The bounded read API owns its session state and can be retained independently
of a borrowed reader. A session freezes its query, selected channel roster,
block watermarks and time resolution, then continues the same merge across
pages. Its cursor is an in-process handle, not a serialized restart token.
New appends and namespaces do not enter an already-open finite session.
Retention of unread history or a physical schema replacement causes explicit
expiration instead of silent skipping. Empty channels participate in validation.

Cancellation and cumulative work limits are part of native reading, including
initial channel discovery, block loading/decompression and filtered scanning.
Page output bounds are separate from total session work. At a zero total result
limit, no block scan is performed: `truncated` conservatively reports retained
selected blocks rather than proving that a record would pass the filters.
Cancellation is
cooperative: the reader checks between work units and bounded waits, but cannot
interrupt one already-running synchronous filesystem operation or application
decoder. One bounded upstream log batch can be fetched before the encoded
payload budget is charged; decoded lengths are checked before decompression.
A deadline or
resource stop must remain distinguishable from EOF, corruption and missing UTC.
Controlled follow reports a retention gap or generation replacement for already
observed channels, including an interval whose every record was evicted before
the next poll. It cannot reconstruct history lost before a new namespace was
first observed.

The bounded session API explicitly rejects the legacy segment backend.
Legacy unconstrained convenience APIs retain their documented compatibility;
future transports use the controlled API rather than pretending that a result
count alone limits scan cost.

### Indexed snapshot navigation

`QuerySession::seek_page` is an alternative to sequential `next_page`, with
one-based page numbers and immutable row/byte dimensions. Mixing modes or
changing those dimensions is invalid and does not advance the revision. Each
step reads at most its supplied page count and returns reached position,
target/EOF flags and optional requested/last page data. Unknown filtered totals
remain unknown until an actual terminal scan; a full page followed only by
filtered records does not create an imaginary empty last page.

Forward navigation continues the existing cursors. Backward navigation rebuilds
cursors from the original channel snapshots, compiled schemas, frozen Epochs,
resolved bounds and query. It does not rediscover channels, adopt new appends or
reset the cumulative Budget. Seeds are recomputed inside that budget. Retention
of the original range or replacement permanently poisons the session, even when
the failed rewind found EOF in its former forward position. Invalid navigation
arguments are recoverable; indexed PageTooSmall is terminal because replay may
already have advanced and page dimensions cannot be enlarged. Legacy sequential
PageTooSmall remains retryable.

Only one bounded raw QueryPage is retained server-side. A repeat seek to the
current page can rerender it without another block scan; its returned entries
and seeds are charged again to the cumulative returned-byte budget. Fresh pages
are not double-charged and progress-only responses do not re-charge cached data.
Bounded conversion and session liveness checks still apply. The GraphQL service serializes cursor
ownership through the existing busy/revision/guard machinery, applies
`ServiceConfig.max_seek_pages_per_call` and preserves admission while work runs.
Successful progress steps advance CAS revisions, including calls that return no
page yet. Indexed sessions remain present at EOF, subject to existing idle and
absolute lifetimes. Cancellation, expiry and uncertain advancing outcomes do not
silently resume or reopen a query.

### Exact filtered totals

`Reader::open_counted_session` runs the native query/page machinery against one
frozen snapshot before returning a session and `QueryTotals`. `total_matches`
counts matching entries across all selected record kinds; `total_seeds` counts
initial state separately. `total_pages` follows the fixed record/byte page
layout, including seeds and the existing empty-terminal-page normalization.
An empty complete result has one empty page. Totals are not derived from global
statistics, timestamps or a fresh second query.

The count keeps only one bounded page at a time. Scan, compressed/decoded bytes,
blocks, roster, epochs and examination work remain cumulative through counting,
rewind and subsequent navigation. Count-only returned bytes are suppressed;
prepared browse seeds are charged exactly once after rewind, and regular page
output/cache replay remains charged. The count reuses the original roster,
watermarks, schemas, epochs and bounds. Original-range retention/replacement
before rewind refuses the result rather than publishing a false exact count.

Truncation, damage or excluded unanchored history yields `IncompleteCount`.
A query limit that actually truncates is not a complete filtered count. Work
ceilings, cancellation and deadlines retain their existing explicit outcomes;
there is no unlimited scan fallback. Legacy sequential/indexed opening remains
available for clients intentionally accepting partial results.

`openCountedReadSession` returns ID, revision and exact UInt64 totals in one
successful response. Its mandatory normalized page dimensions are fixed for
future `seekReadSession` calls. Count work runs under existing blocking-job and
session admission/abort guards. The viewer waits for counted opening, then
reads page one, retaining totals across navigation and presentation changes.
Live is a separate changing stream and does not reuse a static snapshot total.
Independent export sessions retain their existing APIs.

### Message language policy

`LanguagePreferences` is a validated immutable ordered list, owned by each
Reader. Its default is English; `with_language_preferences` configures the
application-wide choice for that reader before sharing it. Separate readers
and request overrides remain independent; no mutable process-global state or
writer setting participates. `render_preferred` uses the default;
`render_with_preferences` explicitly overrides it for one entry. The classic
`render(entry, language)` retains its original exact single-code fallback.

For each preference, first seek its exact spelling, then ASCII case-insensitive
hyphen/underscore equivalence. Finish those attempts before trying the next
preference. If none match, choose the first declared schema language. Do not
infer regional parents or translate arbitrary text. Exact spelling wins when a
legacy schema has equivalent language aliases. Schemas still require a template
per declared language for every event; missing-language fallback usually arises
between schemas. Invalid or equivalent duplicate preferences are rejected.
Associated limits are eight codes and 64 bytes per code; they bound presentation
input rather than the number of languages an application may declare in schemas.

Native preferred rendering returns actual language and matched preference index.
An absent index with a language means schema fallback; absent language on free
text means it is not translated. Schema identity checks remain in force, and an
unknown schema never acquires a plausible translation from another declaration.
The new preferred methods require matching present compiled identities; raw/manual
schemas without one return no rendered result. An unknown-entry/unrelated manual
decoder regression reproduced false text before this check. The legacy explicit
renderer and GraphQL manual/raw contract keep their existing trust boundary;
manual decoding remains an application-owned explicit low-level operation.
GraphQL conversion borrows the resolved policy and continues using bounded
renderers, without cloning the preference list for each record.

GraphQL resolves `Presentation.languages: None` from the service's Reader for
native Rust callers and exposes the same default as `catalog.defaultLanguages`.
The wire input accepts ordered `languages` or legacy `language`, never both;
request overrides cannot mutate the reader or another client. Sequential sessions
and live subscriptions capture their own policy; indexed seek accepts presentation
per call, with omission resolving the Reader default. `languagePreferenceIndex` is
zero-based and nullable; `language` describes selected schema metadata while
`renderStatus` reports whether rendering was requested and succeeded.

The viewer's primary/fallback preference is browser-local and separate from
interface localization. Service-default mode sends no override. Explicit
preferences survive reload, including a code absent from the current catalog;
the union of advertised schema/default codes supplies normal choices. A change
rerenders the current indexed snapshot page automatically and replaces translated
cache entries. In-flight advancing acknowledgements are serialized before the
latest preference is applied; stale translations cannot be republished. The active
query is captured independently of unsaved filter drafts. Live restarts automatically
with an explicit resynchronization notice; stopped/expired cursors still need a new
explicit query. Prepared exports are invalidated and running exports freeze their
own policy. CSV/JSONL metadata includes the requested policy; entries retain actual language,
matched preference and render status. Stored payloads and wire format do not change.

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
(and span kinds) all declare a storage class in `schema!` (`store:
critical`, for instance). What matters is written to the critical channel (a
synchronous backend write per burst), what does not to the ordinary one (batches, deferred
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
`QueueByteLimits` configures these independent pools. Accounting includes the
inline queue envelope, spilled payload capacity and owned text (shared text is
conservatively charged per record). It does not include every allocator or
backend cost. An item exceeding its own pool is rejected immediately rather
than waiting for capacity it can never fit. Configuration is validated before
store I/O. Dynamic record data is limited to 1 MiB before enqueueing. The default active
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

### Retained statistics and count maintenance

Global statistics distinguish retained wire records, nonempty retained blocks,
logical encoded data bytes, allocated filesystem bytes and process-lifetime
writer counters. Wire records include messages, text, samples, span boundaries
and extensions; they are not a filtered result count. Reopening resets runtime
activity, not persisted retained counts. Offline policy/runtime fields remain
unavailable rather than being fabricated.

Each channel owns a versioned `dduroc.counts.v1` key. Append, rewrite and
prefix eviction update counts in the same Raft batch as their data/metadata
change; tombstones contribute zero records and zero nonempty blocks. Global
aggregates and the set of incomplete channels are cached under the backend
mutation discipline. A statistics request does not enumerate channels or scan
payloads. Optional physical allocation uses a separate bounded filesystem
sample; its configured limit is admission headroom, not filesystem capacity.

Older channels without counts remain explicitly `BackfillRequired`. Bounded
maintenance verifies header/body CRC over encoded bytes and persists progress;
new appends, retention and rewrite preserve the covered-prefix invariant.
Cancellation or interruption never turns an unfinished index into an exact
zero. `Reader::backfill_statistics_step` is explicit and live-store-only, with
block/byte ceilings and cooperative cancellation; its result says whether work
remains. GraphQL uses bounded worker admission and `backfillStatistics { more }`.
The browser repeats steps sequentially and stops after the current bounded
step, without claiming an HTTP abort has synchronously stopped a worker.

`Reader::statistics_controlled` exposes the cached snapshot and optional root
allocation. `Store::stats()` remains runtime activity. Metadata snapshots and
filesystem samples describe separate instants; `captured_at` is collection
host wall time, not a persisted event UTC anchor. A fenced backend refuses
exact statistics and maintenance rather than publishing an uncertain cache.

### Failure publication and offline recovery

Ordinary format markers, store metadata and namespace metadata are bounded to
256 KiB and require complete postcard decoding where applicable. Namespace
metadata writes reject oversize output before persistence. Epochs have their
separate 8 MiB cap, uniqueness/reference/UTC validation and explicit exhaustion
errors. A retained boot floor is checked on every live startup, including with
parseable but incomplete epochs. Decode corruption keeps writer quarantine and
reader boot-only fallback; semantic ambiguity and oversize are refused unchanged.

Epoch changes are persisted as candidates. Live time anchors are published
only after persistence and allocation accounting succeed. Pre-rename failures
keep the old file; post-rename errors may have changed disk authority, so the
backend refuses later writes until reopen. Error does not imply rollback.
Writer reply waits detect worker exit without a timeout/retry of a living
operation; buffered reply senders cannot hang shutdown after a fatal failure.
The drop counter records known or unconfirmed delivery failures across queued,
current, failed-block and other active-builder records exactly once. It is not
a proof that each counted record is absent from disk after a sync error.

The optional `recovery` feature provides copy-only offline operations and the
`recover_copy` example. Source roots use existing main/engine locks without
creating source files. The original configuration must identify every custom
root; older metadata does not contain a complete discoverable root roster.
New output must not overlap any source or already exist. Copying rejects
symlinks/nonregular entries, bounds entries and bytes, and checks cancellation
and deadline between files/chunks.

An unopened `evidence/` copy carries SHA256 manifests; a separate `candidate/`
is opened by the engine. Initial strict inspection validates direct bounded
epoch and referenced namespace metadata, root identity, block framing and
record counts. Normal reader fallbacks for missing schemas/anchors remain
unchanged. Orphan metadata is preserved in manifests, outside referenced
metadata validation.

Only explicit `RepairCopyAllowDataLoss` permits tolerant recovery after failed
initial strict inspection. Candidate changes and final strict inspection are
reported, and `loss_possible` remains true in that mode even on clean input.
Nothing replaces or activates the source. A failure preserves partial outputs
with `complete=false` when the report can be written. Input limits bound copied
content; two copies, manifests and upstream engine working space are separate
resource costs. Cooperative deadlines cannot interrupt engine open or an OS
call, and recovery does not establish preservation of external critical ACKs.

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

## The crates and the API

Decisions: the web interface is **on the device plus an offline dump viewer**;
the protocol is **GraphQL** (as in the prototype); the ergonomics are an
**explicit handle** for a namespace. The frontend SPA is **TypeScript plus
Svelte** with Carbon components. The project name and crate prefix are
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
CRC), lz4_flex, rustix (syscalls), crossbeam-channel (the
writer's queue), chrono (the read layer), thiserror, syn/quote,
axum plus async-graphql plus tokio (the web), proptest (the format's tests).
Raft Engine owns journal storage. Our code supplies the compact format,
schema macro, retention policy, durability acknowledgements and query layer.
GraphQL is implemented as a transport-neutral adapter. The embedded web package is implemented in the development workspace; the
standalone viewer helper remains a proposed integration layer.

### The workspace

| Crate | Contents | Dependencies |
|---|---|---|
| `dduroc-format` | the byte format: segments, blocks, records, varint, CRC | minimal, sync, no tokio |
| `dduroc-engine` | the engine: Store, namespaces, channels, the writer thread, Raft storage integration, retention, migrations, epochs | format; **no tokio** — the writer is a dedicated OS thread |
| `dduroc-macros` | the proc-macro for the schema declaration | syn/quote |
| `dduroc` | the facade: re-exports, the Namespace/Span/Series handles, the Event trait | engine, macros |
| `dduroc-read` | the reader: the k-way merge, filters, schema resolution, UTC | format, engine (or a standalone open of a directory) |
| `dduroc-graphql` | bounded catalog/read, revisioned read sessions and live follow | read, async-graphql, tokio |
| `dduroc-web` | embedded Svelte/Carbon assets, host-configured Axum router, explicit source-build feature | axum; GraphQL transport only in examples |

The core (format plus engine) is synchronous and independent of tokio: it can
be reused in projects without async, and the writer is simple (an OS thread,
an mpsc). Tokio appears in the GraphQL adapter and host examples. The facade propagates
span context through explicit guards; it has no Tokio feature or implicit
task-local span context.

**The viewer and the schema**: the embedding host links its compiled schema
crates for payload decoding and migrations. The same browser assets display
its schema descriptors and tagged values. Unknown types remain explicit, with
raw payload access where available. A standalone offline host remains planned
as `dduroc-viewer`; the current web package supplies the asset router.

### Frontend delivery requirement

GraphQL is an adapter over the native API; schema binding, cursor semantics,
cancellation and scan budgets are not reimplemented in resolvers. The Svelte
frontend receives schema descriptors and structured decoded values, so new event
schemas do not require a frontend rebuild. Server-side compiled decoders remain
responsible for payload versions and migrations.

Frontend development uses its own build tools. Published web assets are already
compiled and included in the Rust package; an ordinary application `cargo build`
must not invoke npm, install Node or fetch frontend dependencies. Frontend
maintenance/release preparation and explicit consumer `build-ui` builds rebuild
those assets with the documented toolchain.

### Embedded web delivery

The development `dduroc-web` package contains a Svelte/TypeScript application
using Carbon Components Svelte, its locked frontend dependencies, prebuilt
assets and integrity metadata. Its Axum asset router owns neither the storage
reader nor GraphQL transport/listeners. Hosts configure browser-visible base
paths and HTTP/WebSocket endpoints and attach their own authentication.
The executable example supplies a complete local transport for development.
Demo transport is available only in development previews; release assets use
the configured real endpoints.

Default Cargo builds verify and embed the packaged prebuilt assets and never
invoke Node/npm. The explicit `build-ui` feature installs locked dependencies
with lifecycle scripts disabled, builds in Cargo output storage, and embeds
that rebuilt output. Temporary source/dependency/cache directories are removed
afterward, including command failures. Source and asset hashes support auditing
and an exact rebuild comparison; hashes are not independent proof of dependency
trust. Rebuilding requires the documented frontend toolchain and dependency
availability.

Both `CARBON_TELEMETRY_DISABLED=1` and `IBM_TELEMETRY_DISABLED=true` are forced
for project installation/build commands. npm audit/funding calls and dependency
lifecycle scripts are disabled. The upstream mechanism analyzes source during
installation; it is not a browser runtime API. See the [Carbon telemetry FAQ](https://www.carbondesignsystem.com/support/faqs)
and [IBM opt-out documentation](https://github.com/ibm-telemetry/telemetry-js#opting-out-of-ibm-telemetry-data-collection).
The application registers complete local IBM Plex font files through the
[documented self-hosted setup](https://github.com/carbon-design-system/carbon/blob/main/docs/guides/ibm-plex.md),
using the recommended per-family font packages. Carbon Svelte theme CSS is
imported once and its official Vite preprocessors/optimization plugin are used.
The documented [PostCSS URL plugin](https://github.com/postcss/postcss-url)
rebases retained font sources onto those installed complete font files while
preserving component selectors, font declarations and Unicode ranges. This
also avoids an external fallback request when a local Mono face lacks a glyph.
Runtime network checks cover the local fonts actually used and configured
journal endpoints. Upstream licenses accompany the distributed assets.

### Carbon guideline inventory and review rules

The review covers the complete relevant guideline pages below, including their
variants, behavior and accessibility sections. The [pattern index](https://www.carbondesignsystem.com/building-blocks/core/patterns/overview)
was used to identify the applicable workflows. Login, editing tools and AI
presence are outside this viewer's task. Svelte component APIs come from the
[Carbon Svelte documentation](https://svelte.carbondesignsystem.com/llms.txt);
framework availability is community-maintained, as described in the Carbon FAQ.

| Rules | Guideline and sections | Checkable application |
|---|---|---|
| R1-R3 | [Grid overview](https://www.carbondesignsystem.com/building-blocks/foundations/2x-grid/overview) and [guidelines](https://www.carbondesignsystem.com/building-blocks/foundations/2x-grid/guidelines): geometry, responsive behavior, hierarchy, gutter modes | Give journal data the main width; keep type aligned to shared key lines; reflow controls at smaller widths without hiding actions |
| R4-R6 | [Color](https://www.carbondesignsystem.com/building-blocks/foundations/color/overview) and [themes](https://www.carbondesignsystem.com/building-blocks/foundations/themes/overview): layering, tokens, interaction states, contrast | Use role tokens and neutral layers; reserve emphasis for actions/status; retain visible keyboard focus and text/icon status cues |
| R7-R8 | [Typography](https://www.carbondesignsystem.com/building-blocks/foundations/typography/overview): productive styles, Plex, scale, weights, type color | Use productive text hierarchy and local Plex; use monospace for exact engineering values without changing their digits |
| R9-R12 | [Data table](https://www.carbondesignsystem.com/building-blocks/core/components/data-table/guidelines): formatting, content, expansion, toolbar, pagination, loading, modifiers | Match header/row/toolbar density; use short sentence-case headings; progressively disclose details; keep page controls below data and avoid expand-all by default |
| R13-R14 | [Table accessibility](https://www.carbondesignsystem.com/building-blocks/core/components/data-table/accessibility) and [foundation accessibility](https://www.carbondesignsystem.com/building-blocks/foundations/accessibility/overview): keyboard, naming, adaptive use | Provide a table name, keyboard-operated expansion and logical focus order; do not claim screen-reader certification from an automated audit |
| R15-R17 | [Filtering](https://www.carbondesignsystem.com/building-blocks/core/patterns/filtering): selection methods, states, reset | Apply related filter changes together; expose applied state and a clear/reset action; distinguish server filtering from any loaded-page search |
| R18-R20 | [Forms](https://www.carbondesignsystem.com/building-blocks/core/patterns/forms): labels/help, defaults, buttons, validation, layout, accessibility | Keep visible labels and essential format guidance; group related controls with actions after inputs; present inline validation and prevent duplicate submission |
| R21-R22 | [Loading](https://www.carbondesignsystem.com/building-blocks/core/patterns/loading): skeleton, inline feedback, progressive batches, accessibility | Use a table skeleton initially; announce subsequent loading while preserving context and working controls |
| R23-R24 | [Empty states](https://www.carbondesignsystem.com/building-blocks/core/patterns/empty-states): anatomy, types, alternatives, accessibility | Replace empty tables, headers and footer with contextual guidance; distinguish no matching records, unavailable source and failed/incomplete reads |
| R25-R26 | [Notifications](https://www.carbondesignsystem.com/building-blocks/core/patterns/notifications): status, type, priority, actions, accessibility | Keep relevant errors/loss notices near the results with a next action; never auto-dismiss critical gaps or expiry |

Additional operator-review rules:

| Rule | Source | Check |
|---|---|---|
| R27 | [UI shell header](https://www.carbondesignsystem.com/building-blocks/core/components/ui-shell-header/guidelines) | Place global theme action at the right edge, full header height, with accessible name/state and tooltip |
| R28 | [Combo box](https://www.carbondesignsystem.com/building-blocks/core/components/dropdown/guidelines) | Use the real typeahead component, a visible label and keyboard/custom-value behavior; selecting stages filters rather than submitting them |
| R29 | [Pagination](https://www.carbondesignsystem.com/building-blocks/core/components/pagination/guidelines) | Attach native pagination below the table with the paired row height and keep navigation state bounded |
| R30 | Developer operator requirements | Keep namespace available, secondary filters compact, retained/global statistics separate from query results, and physical/logical bytes separately labelled |
| R31 | Developer counted-opening requirement | Return exact filtered matches, seeds and page totals on the same snapshot before page one; use native Carbon pagination without a parallel Go form |

### Viewer read lifecycles

Cursor capabilities, revisions, wide integers and timestamps remain strings in
the browser. Page advances are serialized; replacing filters or leaving a view
retires the old session and ignores stale responses. Merely aborting a fetch
does not retire a capability whose ID was already returned.

Snapshot browsing and live follow are distinct reads. There is no portable
append cursor joining them, so the UI must not promise seamless replay or
silently reconnect after a gap. Server errors, retention loss, browser buffer
limits and lost connections require an explicit restart/resynchronization.
Browser retention has both row and byte ceilings; it is not an unbounded log
history. Schema discovery and tagged values let the same assets display new
compiled event schemas without a frontend rebuild. Limited field projection
must be identified as incomplete, with raw payload access available instead of
invented empty values.

The display clock is independent of the query clock. Calendar UTC is the
initial presentation; switching to boot time only reformats retained entries.
It does not issue a query or alter cursor/order. Missing `Entry.utc` falls back
to boot time with an explicit unavailable label, while details retain both.
The native API accepts `DateTime<Utc>` bounds directly and GraphQL accepts
RFC3339 offsets. Both endpoints are inclusive on the microsecond event grid:
a lower bound between ticks rounds upward, an upper bound downward. UTC
selection excludes unanchored runs and reports them; presentation fallback must
never invent an absolute time or silently change that selection. The host
explicitly records a current trusted reading using `Store::record_sync`; the
persisted per-hardware-boot anchor also resolves earlier records. Anchor
precision and snapshot behavior are described under the time model and finite
read contract above.

The table always uses Carbon Pagination's native `pageInputType="input"` for
finite snapshots. Counted opening supplies exact page and filtered-record totals
before page one; the UI shows a counting phase while the bounded native scan
runs. Its UI units are snapshot pages (`pageSize=1`, `totalItems` equal to the
exact page count), with truthful loaded/matching-record labels; backend page
dimensions remain unchanged. User `change` events navigate, while reactive
`update` events do not issue requests. Numeric mode creates no option list for
large counts and remains accessible on compact bars through a results-scoped
wrapper style. No separate Go form or unknown-total paging branch is retained.
One current page is rendered; a sparse five-page cache is bounded additionally
by rows/bytes and lists its exact indices. Cached navigation needs no RPC;
uncached selection uses bounded indexed steps in the same snapshot, with
progress and cancellation. Previous/Next select adjacent page numbers. They do
not deduplicate tied records or reopen the query. Observed expiry or retention
failure invalidates cached navigation and exact totals, requiring resync.

The table-content wrapper keeps its natural content height and horizontal
scrolling. A results-scoped `overflow-y:hidden` prevents a transient nested
vertical scrollbar from fractional row heights during expand/collapse; no
fixed table height or document overflow suppression is introduced. Long decoded
values and raw payload areas retain their own scrolling. Browser regression
checks sample transition frames and ensure expanded rows fit the wrapper.

Namespace uses Carbon ComboBox, including keyboard selection and custom
exact/prefix input. Empty input selects all namespaces; choosing a value stages
filters without submitting a query. Global statistics appear separately from
table selection, showing allocated disk and logical retained data distinctly;
root and writer details are expandable.

### Analysis downloads

The browser prepares CSV and JSONL through a separate bounded finite GraphQL
session. Its filter form is an independent editable copy of the viewer's
filters; both use one shared form and query conversion. Export never uses the
table's row buffer as the history source. Read progress, cancellation and
cleanup acknowledgement are distinct from file download. Closing or leaving
the dialog cancels active work; a late open acknowledgement is observed and
retired. If a transport failure hides the open acknowledgement and its session
ID, only the host's expiry can retire that unknown capability. An unconfirmed
cleanup prevents another export in that view; the UI explains expiry/reload.

Export format version 1 uses typed rows: `metadata`, `entry`, `seed`, `damage`,
`unanchored`, `summary`. JSONL has one `{type,data}` object per line. CSV has
one header and the fixed columns `record_type,namespace,class,utc,boot,micros,kind,data_json`; every row has the same column count. `data_json` preserves the
original GraphQL object, including tagged values, decimal integers, float bits
and available raw payloads. CSV display columns prefix spreadsheet formula-like
strings and decimal integers of at least 16 digits with an apostrophe; the
original strings remain in `data_json`. Both formats use UTF-8.

Metadata records the query, metadata-only scope, output ceilings and field
projection. Structured fields retain the API's bounded projection and explicit
status; an omitted subtree is not an empty value. Schema provenance is not an
exported durable identity. The file includes available rendered names, fields,
statuses and raw payloads rather than inventing a decoder identity.

The final summary distinguishes history completeness from cursor cleanup.
Damage, excluded unanchored runs, truncation, request failures or client limits
make the read partial. Included records without UTC keep their relative time
and an informational warning; this alone does not mean history was lost.
Incomplete output has a `.partial` filename and requires an explicit download
action after its warning. No page is retried automatically.

Output uses bounded byte chunks, reserves summary space and applies both record
and UTF-8 byte ceilings. The defaults are 10,000 records and 16 MiB; the browser
ceilings are 100,000 records and 32 MiB. These bound serialized file content,
not total browser RSS. The host's scan/session/output ceilings remain in force.

Dynamic theming follows the official Carbon Svelte
[Theme setup](https://github.com/carbon-design-system/carbon-components-svelte/blob/master/docs/src/pages/components/Theme.md):
import `all.css` once and use `Theme` with `persist` and a viewer-specific key.
A right-aligned header icon toggles White and Gray 100, with an accessible
name/state and a persisted manual preference. Earlier light/dark selections
map to these two choices without a system-mode override. All themes use the
same bundled font pipeline and semantic Carbon tokens. The header control mounts after persistence restoration so its initial value
cannot overwrite the saved theme; storage errors retain a usable default.

### Application examples

The compiled `crates/dduroc/examples/` cover writing, reading, schema migrations,
channel policy, bounded pages and small resource profiles. They define the
current callable API; this document describes its mechanisms instead of keeping
an independent API sketch. `dduroc-graphql/examples/embedded_schema.rs` shows
direct execution without a listener; `dduroc-web/examples/server.rs` is an
application-owned HTTP/WebSocket host.

Follow accepts oldest-first, open-ended live queries. Its window is resolved
at open while anchors refresh for output; a late block in another channel may
appear after a newer record, so cross-channel delivery is ordered within the
currently visible data rather than globally across future flushes. Damage is
drained with `take_damage` rather than accumulating forever. On writer exit,
follow adopts and drains final committed channels before reporting `Ended`.

### GraphQL read contract

`dduroc-graphql::build_schema` builds a ready `LogSchema`; it does not start a
server. The host supplies a Reader and explicit server-owned ceilings.
`apiVersion`, `catalog` and `read` are queries. `openReadSession`,
`openCountedReadSession`, `readSession`, `seekReadSession`, `closeReadSession`
and `cancelReadSession` mutate reading resources;
`follow` is a live subscription. No client filesystem paths or writer controls
are exposed. The exported SDL defines the callable contract.

A random session capability identifies an owned native cursor. Advancing it
requires the expected revision and rejects concurrent or stale advancement.
Idle and absolute deadlines retire sessions. Per-call worker timeouts do not
shorten the default cumulative session deadline. A client's `timeoutMillis`
sets a tighter cumulative deadline and is not refreshed on each page.
If execution is aborted before
returning an advancing operation's result, the session is invalidated rather
than replayed from a timestamp. Construction of a response is not a transport
delivery acknowledgement.
Capabilities are process-local and do not implement user authentication or
restart/reconnect tokens. The host protects access to the schema and transport.

Finite reads and catalog work use bounded blocking jobs. Long-lived follow
uses dedicated threads with separate subscription admission; some job capacity
is reserved for finite operations. Cancellation propagates on request/stream
drop, but a worker retains its permits until it actually stops. Follow queues
bound both items and converted bytes and report slow-consumer termination;
clients must open a new subscription and resynchronize.
Subscription capacity remains charged while either its worker or its undrained
stream owns resources, including an unread terminal stream. Byte reservations
use the full per-message conversion ceiling, so small items may wait for prior
queued bytes to be consumed.

Native scan budgets and GraphQL conversion/projection budgets are separate.
The schema also bounds source/variables, selection expansion, aliases, depth,
complexity, root calls, resolved fields and aggregate response presentation.
These counters are not total RSS or exact serialized HTTP-byte guarantees;
body and batch admission remain the transport host's responsibility.
The GraphQL framework may allocate a list's resolver futures before individual
field counters run; native page/tree bounds and selection caps bound that
admitted work, with framework-dependent transient overhead.

Generated schema decoders expose bounded tagged fields and bounded rendering.
Fields/rendering are performed only when selected; legacy manual callbacks
without bounded support are explicitly unsupported for that presentation.
Decoded values use tagged shapes, decimal integer strings and floating-point
bits/classification, preserving wide integers, non-finite floats, maps and
variants without JSON precision loss. This is a presentation tree, not a full
Rust-type roundtrip: transparent newtypes and nested Option distinctions may
collapse. Raw payloads remain the source of truth. Arbitrary application callbacks and OS
I/O cannot be forcibly interrupted by these cooperative controls.

## Deferred work

Portable continuation across process restart or source reopening remains future
work. Current native and GraphQL sessions retain exact cursor state only within
the live process; timestamps alone are not continuation tokens. A standalone
offline host and tracing/log bridges remain separate integrations. Physical
power-cut acceptance, target throughput and long-duration resource measurements
remain hardware validation, as bounded in BENCHMARKS.md.
