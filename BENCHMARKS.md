# Measurements

## Pinned Raft Engine characterization

These are synthetic host tests of raft-engine 0.4.2 with `std_fs`, on a
verified disk-backed temporary filesystem. They are not ARM/eMMC acceptance,
and SIGKILL does not remove the kernel page cache. The reusable harness is:

```sh
cargo run -p dduroc-engine --example raft_probe -- suite "$FIXTURE_ROOT"
cargo test -p dduroc-engine --test raft_faults
```

`FIXTURE_ROOT` must be a new disposable directory on the intended filesystem.
The example intentionally tests unsafe upstream defaults as controls; the
production backend uses strict recovery.

| Scenario | Observation | Limit |
|---|---|---|
| 5 child-process kills/reopens, 32 synchronous records each | 160/160 exact acknowledged payloads recovered | Process crash, not physical power loss |
| 100 retention cycles, two active groups and one dormant group | 15,532,032 payload bytes written; steady allocated range 688,128–897,024 bytes | Group retention 16/96/96 blocks; explicit compaction and purge |
| 8 MiB payload in 128 B / 4 KiB / 64 KiB entries | RSS increase 3256 / 204 / 280 KiB | Independent process baselines; allocator retention affects RSS |
| Default recovery, one injected body-read EIO | Previously acknowledged value lost; file truncated | Reproduced upstream defect; forbidden production policy |
| Strict recovery, read EIO or truncated tail | Open fails; files unchanged | Requires explicit intervention for a genuinely torn tail |
| Empty sync vs nonempty synchronous barrier | 0 vs 1 filesystem sync calls | Pinned-version defect; use nonempty barriers |

An independent release-build sample with 16 groups and 512 synchronous 4 KiB
blocks measured p50/p95/p99 at 1.265/1.446/1.686 ms. A separate 128 MiB run with
64 KiB blocks, four per batch and a barrier every 8 MiB measured 319 MiB/s,
including payload generation. RSS rose from 3352 to 4428 KiB; allocated storage
ended at 136,990,720 bytes. That throughput run performed no retention and did
not test the application's time-based synchronization scheduler. These are
raw-engine figures, not migrated dduroc end-to-end throughput promises.

## Migrated public Store acceptance

Run `cargo test -p dduroc-engine --test migration_acceptance -- --nocapture`.
Set `DDUROC_TEST_DISK_ROOT` to a disposable disk-backed parent directory for
resource characterization; other unit fixtures use the system temporary area.
These samples use the default unoptimized test profile and `std_fs` on the host.
They are neither release throughput claims nor target flash measurements.

| Scenario | Observation |
|---|---|
| Critical ACK, explicit ordinary sync, periodic ordinary sync | Each mode preserved 48 exact payloads through three SIGKILL/reopen cycles |
| Different personal quotas, 128 / 256 KiB | 3 / 6 payloads retained after handle release and Store reopen |
| 160 closed namespaces, 40 KiB payload each, 256 KiB class budget | Logical retention remained within budget; allocated bytes below 4 MiB |
| 600 repeated open/write/drop cycles of one namespace | All 600 records recovered; sampled warmed RSS 8964 → 9020 KiB |
| Equal-geometry independent roots and fresh offline reopen cycles | Correct root-specific payloads; raw upstream negative control reproduces aliasing |

The periodic test uses 10 ms flush / 60 ms sync intervals to exercise the real
scheduler quickly. It is not a 10-second wall-clock soak or a power-cut test.
Repeated offline opens and a 530-namespace follower separately exercise reader
resource bounds. A broad merge still retains one decoded block per selected
channel; the number of returned records does not cap that memory.

End-to-end characterization writes 256 critical records of 2 KiB, then 512
ordinary records with 128 explicit barriers, after registering the indicated
number of namespaces. Both cases finish with 252,930 retained payload bytes.

| Registered namespaces | Critical total | Critical p50 / p95 / p99 | Ordinary total | RSS baseline → end | Allocated baseline → end |
|---|---|---|---|---|---|
| 100 | 399 ms | 1497 / 2067 / 2889 us | 259 ms | 9512 → 9584 KiB | 1,032,192 → 2,789,376 B |
| 1000 | 397 ms | 1482 / 1987 / 2834 us | 262 ms | 14768 → 14848 KiB | 8,617,984 → 10,350,592 B |

An earlier version recursively measured all namespace files on every commit:
its critical p50 rose from 2565 to 13235 us between 100 and 1000 namespaces.
The current implementation tracks managed metadata allocation on mutation and
measures the shared journal files before admission. The repeat sample removes
that observed namespace-count penalty. Metadata/catalog memory and disk space
still grow with distinct namespaces; this is bounded by explicit admission
and count limits, not evidence of zero cost or unlimited namespace churn.

## Published 0.1 verification environment

The final workspace gate passed 423 tests, with two existing ignored doctests.
Formatting, Clippy on all targets with warnings denied, and Rustdoc with
warnings denied passed. `cargo check --workspace --all-targets --target
armv7-unknown-linux-gnueabihf` passed using `arm-linux-gnueabihf-gcc` for C code.
These checks used rustc/Cargo 1.97.1. Release preparation also passed
`cargo +1.88.0 check --workspace --all-targets --locked` on the declared minimum
Rust 1.88; the previous 1.85 declaration did not support the code's let-chains.

The concurrent-page/retention regression reproduced a missing-block error
before page/append locking and passed after the fix. No physical board,
filesystem power-cut campaign or target throughput measurement was performed.

## Development checkpoints before the release audit

These incremental checkpoints precede the final 0.2 release audit below. Their
test counts describe earlier development states, not the current workspace.

### Native API verification

The development API passed 463 workspace tests, including 31 independently
authored public API regressions; two existing doctests remain ignored. The
public-contract test lives in `crates/dduroc/tests/public_api_contract.rs`.
These are correctness checks, not fresh latency or target hardware benchmarks.

| Contract | Verified cases |
|---|---|
| Schema ownership | Foreign events/metrics/limits/span kinds rejected before serialization; inert rejected spans; same-store cross-namespace parents; foreign-store rejection; typed migration output scope |
| Decode provenance | Same names, IDs and compatible fields cannot reinterpret another reader's generated schema; conflicting live/offline registries rejected |
| Finite cursor | Both orders, real identical timestamps, page-size retries, fixed roster/watermarks/time anchors, owned dump lock, strict pending/seed expiry |
| Read budgets | Initial roster/epoch admission, filtered-out records, compressed and decoded bytes, cumulative output and state-seed work |
| Cancellation and live | Stops during scanning, seed work and long waits; tracked empty-channel loss, unseen evicted intervals, generation change before cached output and after EOF |

The equal-time fixture writes ordinary production blocks, then uses the pinned
engine on a fresh isolated thread to re-encode their timestamps with the public
format codec. Assertions preserve lengths, payloads, envelopes and metadata.
It needs no production clock override or writable backend testing API.

Formatting, Clippy and Rustdoc with warnings denied passed, as did all-target
checks on Rust 1.88 and ARMv7 GNU and the writer-only library configuration.
`06_bounded_reads` compiled and ran, paging six messages and reporting explicit
cancellation. These gates do not change the physical acceptance limits above.

### GraphQL verification

The workspace including GraphQL passed 494 tests; two existing doctests remain
ignored. The independent `crates/dduroc-graphql/tests/public_graphql.rs` suite
passed all 23 public-execution scenarios through `execute`/`execute_stream`.
This is host correctness evidence, not a target throughput or memory benchmark.

| Contract | Verified cases |
|---|---|
| Reading and schema presentation | Multiple schemas, offline/unknown/manual schemas, exact wide integers and float bits, nested values, raw payloads, seeds, unanchored boots and truncation |
| Cursor lifecycle | CAS revisions, busy/stale operations, page-size retry, idle/absolute lifetimes, explicit cancellation, operation timeout distinct from cursor lifetime |
| Resource retirement | Pending-open admission, worker permits retained after timeout/abort, failed-operation capability cleanup, stream-drop cancellation, unread terminal queues retaining subscription capacity |
| Bounded work | Client ceilings only tighten server limits, typed scan/retention/snapshot errors, bounded decode/render, selection/directive projection, aggregate alias budgets and reuse after rejection |

The direct `embedded_schema` example executed both a query and a finite follow
subscription. Its `--sdl` mode exported the checked-in GraphQL schema. No HTTP
listener or frontend was exercised.

Formatting, all-target Clippy and Rustdoc with warnings denied passed, as did
the actual Rust 1.88 and ARMv7 GNU all-target workspace checks. The writer-only
library configuration passed and its normal dependency graph excludes Tokio
and GraphQL. The final example ran successfully and its SDL export exactly
matched the checked-in schema. These gates do not establish physical target
latency, flash behavior or a total RSS bound.

### Embedded viewer and calendar-time verification

The final Rust workspace passed 507 tests with two existing ignored doctests.
Formatting, all-target Clippy and Rustdoc with warnings denied passed, along
with actual Rust 1.88 and ARMv7 GNU all-target checks. The writer-only library
and normal dependency graph still exclude Tokio, GraphQL and web hosting.
The six router/asset cases passed again against the final generated assets.

The frontend passed 62 tests with both opt-in real-browser cases enabled,
using Node 22.23.2, npm 12.0.2 and the system Chrome executable. Svelte checking
reported zero errors and warnings. The independent acceptance files exercised
actual downloads, HTTP and modern WebSocket transport; the parent repeated the
full suite. These are host correctness checks, not hardware throughput or a
process-RSS guarantee.

| Contract | Verified cases |
|---|---|
| Calendar time | Persisted anchor/reopen, live reader anchor refresh, RFC3339 offset equivalence, inclusive microsecond-grid bounds, nanosecond state seeds, leap seconds and minimum UTC saturation |
| Time presentation | Calendar UTC by default, exact fractions, boot fallback, mixed anchored/unanchored rows, display switches without RPC or row reordering |
| Export | Actual CSV/JSONL bytes, filters and exact UTC point, wide integers/float bits/Unicode/CSV escaping, metadata/diagnostics/summary, explicit partial download, output ceilings and cancellation/late acknowledgements |
| Theme and layout | All four Carbon themes, stored choice across reload and OS preference changes, denied storage, no page/modal horizontal overflow at 320/477/698/1280 pixels |
| Asset delivery | Nested mounts and proxy paths, exact bytes/MIME/cache, traversal rejection, production demo disabled, cold-cache local fonts across scripts/weights without unexpected network requests |

The final generated distribution contains eight manifest-listed assets and
956,581 bytes including its manifest. Its 25 build inputs fingerprint to
`a47e93fad3b38e6e762b9642416697d847d1fb155f45e31bece0d423257b6e17`.
The source-build path matched the shipped asset manifest and bytes exactly;
temporary npm dependencies/cache were removed. Cargo package listing includes
source, lockfile, fonts, licenses and generated assets without `node_modules`.
A fresh default Cargo build succeeded with failing Node/npm/npx sentinels at
the front of PATH and invoked none of them.

Anchor storage resolution is milliseconds, while event offsets and returned
UTC retain microseconds. This does not establish microsecond absolute clock
accuracy. A host must explicitly supply a trusted current clock reading.
Unknown session IDs after a lost open acknowledgement remain subject to host
expiry; navigation cancellation is best effort. Physical flash/power-cut
acceptance and target latency remain as limited in the earlier sections.

### Retained statistics and operational verification

The updated workspace passed 573 Rust tests with two existing ignored
doctests. The optional recovery feature separately passed 18 recovery and
metadata-acceptance cases. The final frontend passed 112 tests with all six
real-browser cases enabled, serialized against the shared example server; Svelte checking reported zero errors/warnings.
The parent repeated full gates after the independent acceptance work. The counted-opening change was checked through native, GraphQL and packaged viewer tests; count/rewind retain work ceilings while output quotas and snapshot identity are verified separately.

| Contract | Verified cases |
|---|---|
| Exact retained statistics | Append, class/personal retention, rewrite tombstones, closed namespaces/reopen, CRC-checked incremental backfill and unchanged entries; cached global summary with 301 channels beyond ordinary read-channel limits |
| Native/GraphQL semantics | Retained versus process-lifetime counters, offline null policy/runtime, exact wide scalars, optional physical root samples, bounded explicit indexing and real unknown-to-complete progression |
| Metadata and critical failures | ENOSPC/EIO injections at five atomic metadata stages and critical adapter boundaries; no false ACK, old live UTC publication, post-rename/post-sync ambiguity, sticky refusal and prompt worker-exit handling |
| Ownership and compatibility | Busy and inactive foreign custom-root markers remain unchanged on refusal; all existing bindings checked before writes; old writer guard upgraded independently of block codec |
| Failure accounting | Four independently authored cases count queued, current, failed-block and other active-builder records exactly once as known/unconfirmed delivery failures |
| Recovery copies | Existing source locks, overlap/symlink/limits/cancellation, immutable evidence manifests, initial/final strict inspection, explicit tolerant candidate only, corrupt/missing metadata and valid older closed classes/migrations |
| Large-history UI | 5,030 real records over 79 pages through one cursor, no duplicates, at most five cached pages/320 records and 64 rendered record rows; cached back/forward adds no RPC |
| Ergonomics | Header light/dark icon and persistence, actual Carbon ComboBox keyboard/custom values, separate exact allocated/retained values, downloads, local fonts and responsive layouts |
| Carbon paging and row overflow | Native numeric picker from counted opening, no separate Go form or page-option list; 713×932 and 1440×932 Chromium transition sampling verifies natural table height and preserved horizontal/detail/page scrolling |
| Exact filtered opening | Nine independent Rust cases cover filter families, byte-limited page counts, seeds, limits/zero results, incomplete/damaged/unknown data, retention, cancellation/deadline and prepared-seed quota; 5,000 matches traverse 79 pages on the original snapshot |
| Initial native pagination | Actual first counted response exposes 5,030 matches/79 pages before first seek; radio-1 yields 3/1 while global retained stats stay 5,030; All namespace reset restores 5,030/79 and no Go form is present |
| Message languages | Seven independent public Rust cases: ordered exact/equivalent choice, fallback, isolated readers and concurrent clients, service default inheritance, query/session/follow metadata, unknown schemas and confirmed compiled identity; new preferred manual-decoder refusal reproduced red then green |
| Viewer language lifecycle | Persisted primary/secondary, exact schema alias choices, automatic current-page rendering without another snapshot, serialized rapid changes and pending acknowledgements, unchanged draft filters, explicit live resync, frozen export preferences and actual JSONL language metadata; raw text stays unchanged |
| Indexed navigation | Nine independent public cases cover bounded progress, append/new-channel exclusion, forward/back/EOF replay, exact and filtered-tail page boundaries, CAS/dimension/mode errors, terminal retention/PageTooSmall, cumulative replay budgets and cached returned-byte accounting without repeated block scans |
| Direct controls | Actual Go-to-page selection with one open snapshot/document, cached and uncached navigation after eviction/EOF; parent separately verified page 1 → 75 → 5 and automatic English-to-Russian rendering at page 5 |
| Namespace reset | Regression reproduced before the fix: All namespaces must clear both ComboBox text and selected ID; native query and inherited/edited export scopes are checked, including real JSONL query metadata |

The small compiled resource-profile example read 13 records with zero damaged
blocks and no truncation. Its configured logical budgets derive a 20,971,520 B
physical admission ceiling; its disposable host root occupied 24,576 B. These
are a functional example, not a measured board profile or total-RSS bound.

All-target Clippy and Rustdoc passed with warnings denied for the normal
workspace and optional recovery code. Actual Rust 1.88 and ARMv7 GNU all-target
checks passed, including recovery. The writer-only graph excludes Tokio,
GraphQL, Axum and the optional SHA256 recovery dependency. The facade's optional
recovery configuration and checked-in SDL were checked separately.

Final UI inputs fingerprint to
`81bea8eb6ea9e78a61d41d1ded1ff7675f6fb1ecf06e9ed8b58f873fccd509c2`.
The 28 inputs produced eight manifest-listed assets; distribution size including
the manifest is 1,077,883 B. Individual changed files are below 1 MB. Rebuilt
assets match prebuilt bytes, npm scratch is removed, and default Cargo builds
still succeed with failing Node/npm/npx sentinels without invoking them.

Fault injection here models errors at host filesystem/adapter boundaries.
It is not a physical power cut, actual full-flash/inode exhaustion, target
throughput benchmark or multi-day target soak. Broader release automation remains deferred. The existing gate now includes
recovery, writer-only tests and denied Rustdoc warnings. Recovery candidates may have lost data;
preserving original/evidence bytes does not establish external ACK survival.

## 0.2 release audit

The final host gate passes 621 Rust tests with recovery enabled. The only
ignored test is the opt-in disk characterization below; no doctests remain
ignored. Formatting, all-target Clippy and Rustdoc with warnings denied,
writer-only tests and actual Rust 1.88/ARMv7 all-target checks pass.

Independent regressions cover failed-first-record migration/reopen, exhausted
run/hardware/span identities, malformed or oversized metadata, final follow
tails/new namespaces, release under critical traffic, full diagnostic blocks,
invalid runtime thresholds, extreme channel configuration, Unicode templates,
state migration wire types and malformed block counts/deltas. Production
modules across all seven crates were reviewed; this is not a measured line or
branch coverage percentage, and large test modules were sampled by risk.

`cargo package --workspace --allow-dirty --locked -j 2` creates and builds all
seven isolated archives. Failing Node/npm/npx sentinels were not invoked by
this default path. All seven facade examples and the direct GraphQL example run successfully;
the exported SDL matches the checked-in contract. The frontend passes all
112 tests, with all six real-browser cases enabled against the rebuilt host;
Svelte reports zero errors/warnings. Light/dark screenshots are actual Chromium
captures under docs/images. Packaged source rebuilding with
`--features dduroc-web/build-ui` and `DDUROC_VERIFY_PREBUILT=1` matches the asset
manifest; scratch dependencies/cache are removed. Publication, commit/tag and push are separate steps.

### Transient rewrite disk use

Run the ignored fixture explicitly on a disposable disk-backed parent:

```sh
DDUROC_TEST_DISK_ROOT=/path/to/parent cargo test -p dduroc-engine --test rewrite_headroom -- --ignored --nocapture --test-threads=1
```

The ext4 host fixture uses a 32 MiB critical class budget, a 1 MiB file target,
48 cold records of 512 KiB and a hot namespace with an 8 MiB quota. Compression
is disabled. The sampler checks stable file identities in two inventories and
discards scans crossing unlink/replacement, so its accepted file-byte samples
are conservative lower bounds; directories are excluded.

| Measurement | Result |
|---|---|
| Physical admission ceiling | 50,331,648 B (48 MiB) |
| Sampled transient allocated file peak | 64,217,088 B (61.242 MiB) |
| Final allocated root / retained logical data | 47,382,528 / 33,032,538 B |
| Hot writes before explicit admission refusal | 52 acknowledged; next result unconfirmed |
| Accepted / discarded inventory samples | 7,643 / 47 |
| Cold payloads after reopen / hot quota | All 48 exact / at most 8 MiB |

This confirms that admission headroom is not a hard transient rewrite ceiling.
A partition needs additional GC working space; this single peak is not a
universal sizing multiplier or maximum. The fixture verifies cold payloads,
not the exact retained hot suffix, physical power cuts or long-duration wear.
Stop producers and obtain a successful explicit sync when ordinary delivery
needs a verdict; concurrent shutdown admission is not such a verdict.

## Scope of the historical tables

The tables below describe the former custom segment backend. Historical
end-to-end fixtures also used quiet writes without confirming every delivery;
the current benchmark retries only QueueFull and checks completed writer counts.
Those old throughput figures must not be treated as confirmed delivered-event
rates under queue pressure. They are not
performance evidence for the Raft Engine backend. Codec-only measurements
remain useful for the unchanged record encoding; end-to-end and retention
figures must be measured again for the new storage path.

`cargo bench -p dduroc --bench hot_path`

The numbers were taken on x86-64 (Linux, `/dev/shm`, a release build). Neither absolute timings nor ratios have been validated on ARMv7. CPU,
filesystem and synchronization costs must be measured on the target device.

The absolute numbers in the tables below are tied to the machine they were
taken on and are not comparable with numbers from another: the same
`read/query/all_100k` gives 16.4 ms here and 24 ms on the machine the audit
review was done on. That is why the section
[What the audit review cost](#what-the-audit-review-cost) is arranged
differently — as before/after pairs from one sitting.

The store is written to memory and durability is switched off in the
benchmark's configuration: the goal is to measure the cost of our own code,
not the speed of a developer's medium. The cost of `fdatasync` on eMMC
(1–10 ms) does not depend on the code, but it does depend on how many times
the code calls it — and that is checked by the test
`critical_burst_is_one_group_commit`.

## Which number to trust

There are three groups and they measure different things:

- **`format/*`, `block/*`** — pure functions over bytes. The numbers are
  reliable and comparable between runs.
- **`throughput/*`** — the end-to-end cost: enqueueing, the writer's work,
  syncing, sealing. **This is the reference point for "what a write costs".**
- **`write/*`** — the cost of enqueueing, but it is **not isolated**: the
  writer runs in parallel, and the number depends on how many times the
  application thread woke a sleeping consumer. Good for comparing variants
  within one run; as an absolute "price of a write" it is deceptive (the
  analysis is below).

## The format's codecs

| Operation | Time | Throughput |
|---|---|---|
| varint, writing a small value | 1.12 ns | 894 M/s |
| varint, reading | 4.82 ns | 207 M/s |
| encoding a message | 7.67 ns | 130 M/s |
| decoding a message | 18.3 ns | 55 M/s |
| encoding a sample | 7.29 ns | 137 M/s |
| serializing fields (1 field) | 11.4 ns | — |
| serializing fields (2 fields) | 22.9 ns | — |

## Blocks

| Operation | Per 1000 records | Per record |
|---|---|---|
| assembling a block without compression | 17.0 µs | 17.0 ns |
| assembling a block with LZ4 | 12.9 µs | 12.9 ns |
| parsing and walking a block | 30.8 µs | 30.8 ns |

LZ4 is faster than "no compression": the body is copied into the output
buffer already compressed, and on well-compressible logs there is less to
copy.

## End-to-end throughput

Opening a store, 10,000 records, syncing, sealing, deleting — all of it
together.

| Scenario | Time | Per record | Throughput |
|---|---|---|---|
| 10,000 events | 1.99 ms | 199 ns | 5.0 M/s |
| 10,000 telemetry samples | 2.12 ms | 212 ns | 4.7 M/s |

## Enqueueing

| Operation | Per record |
|---|---|
| an event (batch of 1000) | 312 ns |
| a telemetry sample (batch of 1000) | 97 ns |
| opening and closing a span | 331 ns |

The gap between an event and a sample is neither serialization (11–23 ns) nor
encoding (7 ns) but **waking the writer thread**: when the consumer keeps up
with the queue, every enqueue wakes a sleeper.

### Why a telemetry sample "grew dearer" from 53 to 97 ns

After runtime tags were dropped (container version 2) this measurement
doubled, and it was worth working out why: the figure looks like a regression
of the hot path.

What was checked:

- **the application thread's work did not grow**: `Staged` stayed 72 bytes
  (the test `hot_path_structs_do_not_grow_unnoticed`), while checking the
  value's type stopped dereferencing a hundred-and-fifty-byte descriptor —
  which gave −13%;
- **the writer's work strictly decreased**: reading the series registry's
  `RwLock` and cloning the series description left the sample path — that is,
  **an allocation on every sample**;
- **the only new work the writer does per sample** (noting the metric in the
  footer's set) has nothing to do with the number: with the noting switched
  off the measurement did not improve but got worse;
- **the end-to-end cost did not suffer**: events got faster (2.37 → 1.99 ms
  per 10,000) and reading got 5–26% faster.

What remains is an explanation consistent with all four observations: a writer
that finishes its work sooner falls asleep sooner, and every enqueue into an
empty queue has to wake it. There are more wake-ups, and by this measurement
they cost more than the allocation saved. End-to-end throughput, where the
wake-ups are amortized over a stream of records, shows no difference.

The claim remains a conclusion rather than a direct measurement: there is no
end-to-end telemetry measurement from before the rework — the benchmark
appeared along with the analysis. The figure for 10,000 samples (2.12 ms) is
worth keeping as a reference point.

The consequence for further work: **the bottleneck of writing is neither
serialization nor the format but waking the consumer.** That is what is worth
optimizing (by not signalling until the queue reaches a threshold, say),
rather than the bytes.

## Scale

There used to be two measurements here — at 10 and at 100 namespaces — and a
linear extrapolation to 24 thousand: "about 3.4 s". The extrapolation was
wrong, and not by percentages: a fleet measurement showed 26.8 s at 4 thousand
already. What paid the price was not registration but **closing**, and it grew
superlinearly because every writing channel reserved a whole segment.

Below is a direct measurement rather than a recomputation. The fleet comes up,
every namespace writes one record, then `sync` and closing; `/dev/shm`,
release.

| Namespaces | Registration | sync | Closing | **Total** | Taken under load | RSS |
|---|---|---|---|---|---|---|
| 1,000 | 71 ms | 107 ms | 8 ms | **0.19 s** | 66 MB | +3.9 MB |
| 2,000 | 140 ms | 144 ms | 19 ms | **0.30 s** | 133 MB | +6.6 MB |
| 4,000 | 285 ms | 184 ms | 40 ms | **0.51 s** | 266 MB | +12.2 MB |
| 24,000 | 1.95 s | 375 ms | 463 ms | **2.80 s** | 1.1 GB | +57 MB |

Linear in all four columns: 74–81 µs per namespace on registration, 17–19 µs
on closing, ~45 KB of space taken.

Before the move to a reserve window (SPEC §2) the same 2000 namespaces gave a
closing time of 39.3 s and a `sync` of 2.57 s — 2100 and 18 times worse — and
would have taken 16 GiB against today's 133 MB. The price was paid by an
8 MiB `fallocate` and its unwritten extents, which `fdatasync` has to push
through on the very first block.

An empty namespace creates no segment and takes neither a file descriptor nor
a reserved byte.

## Reading

The store: 100,000 messages in 4 namespaces.

| Query | Time | Throughput |
|---|---|---|
| every record | 16.4 ms | 6.1 M records/s |
| the last 100 | 1.39 ms | — |
| errors only (there are none) | 4.15 ms | — |

## What the changes gave

Cumulatively, from the first working version:

| Measure | Was | Is | Gain |
|---|---|---|---|
| reading all 100k records | 531 ms | 16.4 ms | **32×** |
| selecting errors only | 506 ms | 4.15 ms | **122×** |
| the last 100 records | 1.88 ms | 1.39 ms | **26%** |
| end-to-end write of 10k events | 2.37 ms | 1.99 ms | **16%** |
| a telemetry sample (enqueueing) | 53 ns | 97 ns | −83% ¹ |

¹ Analysed above: the code's work decreased while the number of consumer
wake-ups grew.

What was fixed along the way:

- **Resolving a schema read `ns-meta` from disk for every record** — a file
  operation per record. A schema is now resolved once per namespace.
- **Filtered-out records were materialized** — a payload allocation for each
  of hundreds of thousands discarded. The predicate is applied before the
  owning copy.
- **The namespace and channel names were copied into every answer record** —
  two allocations per record, replaced by an `Arc<str>`.
- **Logging allocated an intermediate `Vec`** — postcard now writes straight
  into the buffer that goes into the queue.
- **Loss accounting went through a mutex and a hash table** — on the hot path
  exactly when the system is under pressure. Replaced by an array of atomics.
- **The descriptor search over a schema was linear** — it is binary now.
- **Every channel was walked on every turn of the writer's loop** — replaced
  by a list of active ones: at 24 thousand namespaces a full pass would eat
  the CPU for nothing, while only a handful write at any moment.
- **A telemetry series' identity was reconstructed at read time** — a separate
  definition record, a table in the footer, a preliminary pass over the
  segment on a reverse walk and a clone of the series description for every
  sample in the writer. All of it went along with runtime tags: a series is a
  metric.
- **Sorting a batch allocated a temporary buffer on every go** — it is now
  skipped when the batch is already ordered (the ordinary case).
- **The type sets in the footer were trees** — `insert` is called on every
  record; they were replaced by a flat sorted vector with a latch on the last
  insert.

## What the audit review cost

A/B measurements in one sitting on one machine: an unmodified `cde39c6` in a
separate worktree against the current one. The absolute values here are lower
than in the tables above — a different machine; only the pairs mean anything.

**The significance threshold is about 5%** for measurements that bring a store
up and about 1% for pure functions. That is the spread between two runs of
**one and the same code**: `read/query/all_100k` gave 23.3 and 24.5 ms,
`block/parse` gave 28.9 and 30.2 µs. Everything inside that range is called
unchanged below rather than improved: claiming someone else's noise is the
easiest thing there is.

| Measurement | Before | After | |
|---|---|---|---|
| `format/record/encode_message` | 8.62 ns | 9.34 ns | **+8%** |
| `format/record/encode_sample` | 6.77 ns | 7.81 ns | **+15%** |
| `format/record/decode_message` | 19.2 ns | 19.1 ns | unchanged |
| `format/varint/*`, `format/payload/*` | | | unchanged |
| `block/build/*`, `block/parse_and_iterate/lz4` | | | within the noise |
| `write/*` | | | within the noise |
| `throughput/end_to_end/10k_events` | 1.426 ms | 1.430 ms | unchanged |
| `throughput/end_to_end/10k_samples` | 1.405 ms | 1.412 ms | unchanged |
| `scale/open_namespaces/100` | 8.00 ms | 8.26 ms | +3%, see below |
| `read/query/*` | | | within the noise |

### Encoding a record grew dearer, and that was deliberate

`encode` stopped being infallible: `SpanId(0)` is a reserved value, the decoder
rejects it, and a codec has no right to produce bytes it stops at itself. Such
a record used to be reachable through the public
`Namespace::log_raw(.., Some(SpanId(0)))`, and a reader lost **the whole rest
of the block** on it rather than one record.

The price is about a nanosecond: a `Result` appeared that the caller has to
check. Moving the check to the call boundary or folding it into one variant
dispatch was tried — it does not help; what costs is the possibility of
failure itself, not where it sits. A nanosecond against an end-to-end write
cost of about 140 ns is invisible: `throughput/*` did not move.

### What these measurements do not check

Half of the audit's changes address a scale the benchmark does not have.
Merging streams moved from scanning every cursor to a binary heap, walking the
writer's deadlines moved to a list of active channels, and selecting segments
at read time stopped doing a `stat` per file. With four namespaces and eight
cursors all of that is indistinguishable — and the table above honestly says
"within the noise". The gain appears at the twenty-four thousand namespaces
claimed; such a measurement now exists — see "Scale".

`scale/open_namespaces/100` grew by 3% (on the edge of the noise) for a plain
reason: bringing a channel up gained a read of four bytes — a check for
whether a segment of the previous run was left unsealed. One read per channel
per bring-up against a returned reserve-window tail is an obvious trade.

## The memory of the write path

An RSS measurement (`/proc/self/status`), the store in `/dev/shm`, release:

| Checkpoint | Before the changes | After |
|---|---|---|
| opening a Store plus a namespace | +1.0 MiB | +1.0 MiB |
| 10,000 small events plus a sync | +1.4 MiB | +1.5 MiB |
| **one 8 MiB blob, then a sync** | **+16.3 MiB forever** | **+0** |
| 3 seconds of idleness | never returned | +0 |

The +16 MiB was the block buffer and the scratch: both grew to the size of the
largest block and were never shrunk. Now a channel with nothing to service
gives its buffers back whole; the price is a reallocation on waking, no more
often than the channel's sync period.

A pair of LZ4-assembly numbers corresponds to it: `block/build/lz4` (a cold
accumulator — the first flush after waking from idleness) grew dearer from
13.8 to 15.7 µs per 1000 records, because `compress_into` initializes the
reused buffer; `block/build/lz4_steady` (the accumulator lives between
flushes — how a channel works all its life), meanwhile, costs 12.1 µs, faster
than the old cold path and without a single allocation per flush. The numbers
were taken under unrelated load (LA ~3–5) and are comparable only with each
other.

The fixed part of the footprint is worth knowing: the writer's queues are
allocated whole when a store is opened (8192 + 1024 slots × 72 bytes ≈
660 KiB, configurable with `StoreConfig::with_queues`), and the writer's batch
buffer holds up to ~290 KiB after the first peak. The registry of runtime
limits no longer allocates slots while there are no overrides (it used to be
~10 KiB per namespace at a hundred metrics — hundreds of megabytes for 24k
namespaces of emptiness).

## Data volume: the density of metric numbering

A separate measurement, not part of the criterion set: `metric_id` goes into
every sample as a varint, so the layout of the identifiers affects the volume.

| metric id layout | raw bytes | after LZ4 |
|---|---|---|
| dense from 1 | baseline | baseline |
| grouped (`0x0101`, `0x0201`…) | **+10%** | **+15%** |
| large (`0x4000`+) | **+23%** | **+29…33%** |

150 metrics, a one-second scan, a minute of data. Compression does not rescue
it: on noisy values the gap widens. The conclusion is to number metrics
densely from 1; more in the docstring of `dduroc_format::ids`.

## What these measurements do not measure

- **The medium.** Durability is switched off in the benchmark; an `fdatasync`
  on eMMC costs 1–10 ms and is not in the numbers.
- **armv7.** Every number is from x86-64. Cross-building is checked, measuring
  on the target is not.
- **Compressing real data.** `block/build/lz4` works on a uniform set; the
  compression ratio of real telemetry is different.
- **Long operation.** Rotation, directory fragmentation and the growth of
  `epochs.bin` over thousands of restarts are out of scope.
