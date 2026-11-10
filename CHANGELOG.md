# Changelog

## 0.2.0

### Application API

- Bind generated events, metrics and span kinds to compiled schema identity;
  reject foreign declarations before serialization or queue admission. Typed
  decoding respects effective schema provenance and declared migrations.
- Accept borrowed events without cloning owned fields. Checked span operations
  report foreign-store, invalid-guard and exhausted-ID errors.
- Add controlled owned read sessions with cancellation, deadlines, cumulative
  work/output budgets, validated `PageLimits`, indexed seeks and exact filtered
  counts from the same snapshot. Portable/restart cursors are not provided.
- Accept calendar UTC bounds and expose resolved UTC alongside boot time.
  Persist trusted clock anchors, including their synchronization source.
- Add ordered per-reader message language preferences and per-read overrides.
- Add retained record/block/byte statistics distinct from runtime writer
  counters, with explicit bounded count-index backfill for older roots.

### Adapters and viewer

- Add transport-neutral `dduroc-graphql`: bounded catalog/statistics/reads,
  counted/sequential sessions, revision-checked navigation and live follow.
- Add `dduroc-web`: an Axum asset router and embedded Svelte/Carbon viewer,
  with no listener in the library. HTTP/WebSocket hosting is an example.
- Include namespace typeahead, native Carbon page selection, UTC/boot display,
  primary/fallback message languages, light/dark themes and CSV/JSONL exports.
- Ship prebuilt assets, source and lockfile. Default Cargo builds need no npm;
  opt-in `build-ui` rebuilds with telemetry/lifecycle scripts disabled and
  supports exact prebuilt verification. Fonts and notices are bundled locally.

### Storage and correctness

- Add queue-owned byte budgets independently of queue slots and writer buffers.
- Publish clock changes only after successful persistence/accounting; fence
  uncertain post-rename state until reopen. Propagate writer exit to waiters.
- Bound ordinary control metadata to 256 KiB with full decoding, and reject
  oversized namespace metadata before persistence. Bound and validate epoch metadata; preserve decode-corruption evidence and
  existing boot-only reader fallback, while rejecting semantic ambiguity and
  oversized files. Consult retained boots even for parseable metadata and
  refuse exhausted run/hardware/span identities instead of reusing them.
- Close writer publication on every exit; follow drains final committed tails
  and newly registered channels before ending. Namespace release drains each
  captured FIFO prefix, preventing sustained critical traffic from stranding
  another namespace's admitted ordinary records.
- Clean marker-only migration staging reservations before group-ID reuse.
  Failed first-record transforms preserve later registration and reopen.
- Validate runtime threshold overrides before mutation; reject invalid ranges,
  NaN bounds and numeric Blob bounds. Avoid extreme configuration arithmetic
  overflow and zero-target division.
- Preserve Unicode formatting fill, reject malformed templates/migration keys,
  and use declared Bool/I64 types in state migrations.
- Validate block record counts, zero first delta, direct decompression bounds
  and fused iterator failure without changing the valid wire codec.
- Add opt-in copy-only offline recovery with preserved evidence, strict metadata
  inspection, bounded copying and explicit candidate-only lossy repair.

### Upgrade notes

- All seven workspace crates use exact matching internal `0.2.0` versions.
  The minimum supported Rust version remains 1.88 on Linux.
- Source changes: `Reader::with_schema` returns `Result`; manual `Schema`
  descriptors include `identity: None` and use explicit raw-ID APIs. Typed
  severity/span operations are checked. New session page methods accept
  `PageLimits`; counted GraphQL service opening returns `CountedSessionOpened`
  with mandatory totals. The GraphQL wire inputs remain compatible with the
  initial 0.2 adapter.
- The writer upgrades recognized Raft roots to metadata writer version 3 and
  new marker content. Record/block codec remains version 2. 0.2 readers accept
  old roots; published 0.1 readers/writers refuse upgraded roots. Preserve a
  copy for rollback; editing markers is unsupported. Legacy segment roots are
  read offline and require explicit export/import into a new root.
- Epoch metadata is limited to 8 MiB encoded. Missing/decode-corrupt metadata
  keeps documented fallback/evidence behavior; duplicate identities, invalid
  references/UTC and oversized metadata fail explicitly without resetting it.

### Verification boundaries

Host crash/fault/resource regressions, UI browser acceptance, MSRV and ARMv7
compilation are described in BENCHMARKS.md. They do not prove physical power-cut
safety, target eMMC/SD latency, a hard transient GC disk ceiling, total-RSS
bounds or unlimited-duration retention. Historical custom-backend throughput
tables are not measurements of the current Raft backend. The benchmark now
counts accepted delivery and fails on unexpected enqueue errors.
