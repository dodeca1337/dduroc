# Measurements

`cargo bench -p dduroc --bench hot_path`

The numbers were taken on x86-64 (Linux, `/dev/shm`, a release build). On
armv7 they will be several times worse in absolute terms, but **the ratios
between the variants carry over** — and those are what the measurements exist
for.

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
