//! Applying migration steps to records.
//!
//! One core serving two callers: both reading old segments and the physical
//! run (`Namespace::migrate`) push records through **the same** chain —
//! otherwise "how it reads" and "how it is rewritten" could drift apart, and
//! those are two answers to one question.
//!
//! # The semantics of the declared sets
//!
//! A step's `events`/`metrics` are not a hint but a **binding filter**: the
//! step is called only for records whose identifiers they name. So the sets
//! decide both "is this segment worth rewriting" (from the footer) and "is
//! this record shown to the step" — a discrepancy between what is declared and
//! what is done does not exist by construction. A step with `touches_all` sees
//! every record, text and spans included.
//!
//! Records outside a step's sets pass it without a call: their bytes are
//! neither copied nor re-encoded.

use crate::error::{Error, Result};
use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome, Schema};
use crate::segment::{SegmentReader, SegmentWriter};
use crate::staged::{ChannelIdx, NsId};
use crate::writer::{MigrationCommit, Writer};
use dduroc_format::block::{BlockBuilder, BlockHeader};
use dduroc_format::record::{Message, Sample};
use dduroc_format::segment::SegmentHeader;
use dduroc_format::{EventId, FooterBuilder, MetricId, ProtocolVersion, Record, SpanId};
use std::path::Path;

/// The chain of steps that brings records of version `seg_version` up to the
/// current one.
///
/// An empty chain means the segment is already at the current version (or
/// newer: that case the caller has to rule out itself — a chain has no steps
/// leading forward).
///
/// An error means the chain has a hole. For a schema that passed
/// [`Schema::validate`] that is unreachable, but the schema is handed to the
/// reader by the application, and skipping a step silently would mean reading
/// records in a foreign layout.
pub fn chain(
    schema: &Schema,
    seg_version: u16,
) -> std::result::Result<Vec<&'static Migration>, ChainError> {
    chain_between(schema.migrations, seg_version, schema.version.0)
}

/// The same from a bare list of steps — for a reader that has the schema only
/// in pieces ([`Schema`] is not dragged through cursors against its will).
pub fn chain_between(
    migrations: &'static [Migration],
    from: u16,
    to: u16,
) -> std::result::Result<Vec<&'static Migration>, ChainError> {
    let mut steps = Vec::new();
    for v in from..to {
        steps.push(migrations.iter().find(|m| m.from == v).ok_or(ChainError {
            step_from: v,
            kind: ChainErrorKind::MissingStep,
        })?);
    }
    Ok(steps)
}

/// Whether any step of the chain touches a segment with this footer.
///
/// `false` means the segment holds not one record any step would take up, so
/// it can be read with the current decoders and need not be rewritten. This is
/// one and the same rule for reading and for a run.
pub fn chain_touches(steps: &[&Migration], footer: &dduroc_format::Footer) -> bool {
    steps.iter().any(|m| m.touches(footer))
}

/// The outcome of applying the chain to one record.
#[derive(Debug)]
pub enum Chained<'a> {
    /// No step touched the record: the bytes are the originals.
    Same(Record<'a>),
    /// The record was transformed — the payload or the identifiers are new.
    Owned(OwnedChained<'a>),
    /// Some step deleted the record.
    Dropped,
}

impl<'a> Chained<'a> {
    /// The record, if it survived the chain.
    pub fn record(&self) -> Option<Record<'_>> {
        match self {
            Chained::Same(r) => Some(*r),
            Chained::Owned(o) => Some(o.as_record()),
            Chained::Dropped => None,
        }
    }
}

/// A transformed record: it owns what a step re-encoded and borrows what no
/// step touched (a sample's value lives in the block buffer).
#[derive(Debug)]
pub enum OwnedChained<'a> {
    Message {
        event: EventId,
        /// The original record's span: a step changes the type and the payload
        /// but not the attachment — it has no way to express one.
        span: Option<SpanId>,
        payload: Vec<u8>,
    },
    /// A span with its kind renamed.
    ///
    /// A step does not touch `span` and `parent`: they are the record's
    /// identity, referred to by its messages and its child spans, and a chain
    /// has no way to rewrite those references.
    SpanStart(dduroc_format::record::SpanStart),
    Sample {
        metric: MetricId,
        value: ChainedValue<'a>,
    },
}

/// A sample's value partway through the chain.
///
/// Remapping an identifier does not touch the value, and there is no reason to
/// copy it — a megabyte spectrum would cost a copy on every record.
/// Transforming a value, by contrast, has to own the result: a step's outcome
/// has no lifetime tying it to the input record.
#[derive(Debug)]
pub enum ChainedValue<'a> {
    /// The original record's value: the bytes were not copied.
    Same(dduroc_format::Value<'a>),
    /// The value computed by a step.
    Owned(crate::staged::OwnedValue),
}

impl ChainedValue<'_> {
    pub fn as_value(&self) -> dduroc_format::Value<'_> {
        match self {
            ChainedValue::Same(v) => *v,
            ChainedValue::Owned(v) => v.as_value(),
        }
    }
}

impl OwnedChained<'_> {
    pub fn as_record(&self) -> Record<'_> {
        match self {
            OwnedChained::Message {
                event,
                span,
                payload,
            } => Record::Message(Message {
                event: *event,
                span: *span,
                payload,
            }),
            OwnedChained::SpanStart(s) => Record::SpanStart(*s),
            OwnedChained::Sample { metric, value } => Record::Sample(Sample {
                metric: *metric,
                value: value.as_value(),
            }),
        }
    }
}

/// A failure of the chain at a particular step.
///
/// For reading this is damage (the record drops out of the answer with an
/// announcement); for a run it is a refusal to rewrite the segment: the
/// original is left untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("migration step {step_from} → {}: {kind}", step_from + 1)]
pub struct ChainError {
    /// The version the failing step migrated from.
    pub step_from: u16,
    pub kind: ChainErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChainErrorKind {
    /// The step could not parse the payload: the record is not in the layout it
    /// expected — either corruption or a mistake in the history declaration.
    #[error("the payload did not parse")]
    Decode,
    /// The step returned an outcome that does not apply to the record kind:
    /// `SampleMetric` for a message, `Message` for text, `SpanKind` for a
    /// sample. A defect in the step's code, not in the data.
    #[error("the step's outcome does not apply to the record kind")]
    WrongOutcome,
    /// The chain has no step from this version: the schema was never validated.
    #[error("the step is missing from the schema")]
    MissingStep,
}

/// Whether a step sees this record.
///
/// A binding filter over the declared sets; `touches_all` sees everything —
/// text, spans and extensions included, which sets cannot express.
fn step_applies(step: &Migration, record: &Record<'_>) -> bool {
    if step.touches_all {
        return true;
    }
    match record {
        Record::Message(m) => step.events.contains(&m.event),
        Record::Sample(s) => step.metrics.contains(&s.metric),
        Record::SpanStart(s) => step.spans.contains(&s.kind),
        // A span's end carries no kind — only its start does — and is never
        // selected by a set of kinds under any circumstances.
        Record::SpanEnd { .. } | Record::Text(_) | Record::Ext { .. } => false,
    }
}

/// Push a record through the chain of steps.
///
/// The steps are applied in order, and each sees the record in the layout the
/// previous one left — so a jump across several versions is made of the same
/// steps as a jump across one.
pub fn apply<'a>(
    steps: &[&Migration],
    record: Record<'a>,
) -> std::result::Result<Chained<'a>, ChainError> {
    let mut current = Chained::Same(record);
    for step in steps {
        let viewed = match &current {
            Chained::Same(r) => *r,
            Chained::Owned(o) => o.as_record(),
            Chained::Dropped => unreachable!("the loop breaks after a deletion"),
        };
        if !step_applies(step, &viewed) {
            continue;
        }
        let failed = |kind| ChainError {
            step_from: step.from,
            kind,
        };
        let outcome = (step.migrate)(MigrationInput { record: viewed })
            .map_err(|DecodeError| failed(ChainErrorKind::Decode))?;
        current = match outcome {
            None => return Ok(Chained::Dropped),
            Some(MigrationOutcome::AsIs) => current,
            Some(MigrationOutcome::Message { event, payload }) => {
                let span = match viewed {
                    Record::Message(m) => m.span,
                    Record::Text(t) => t.span,
                    _ => return Err(failed(ChainErrorKind::WrongOutcome)),
                };
                Chained::Owned(OwnedChained::Message {
                    event,
                    span,
                    payload,
                })
            }
            Some(MigrationOutcome::SampleMetric(metric)) => match current {
                // A remap on top of anything: the value stays as the previous
                // step left it, only the identifier changes.
                Chained::Owned(OwnedChained::Sample { value, .. }) => {
                    Chained::Owned(OwnedChained::Sample { metric, value })
                }
                Chained::Same(Record::Sample(original)) => Chained::Owned(OwnedChained::Sample {
                    metric,
                    value: ChainedValue::Same(original.value),
                }),
                _ => return Err(failed(ChainErrorKind::WrongOutcome)),
            },
            Some(MigrationOutcome::Sample { metric, value }) => {
                if !matches!(viewed, Record::Sample(_)) {
                    return Err(failed(ChainErrorKind::WrongOutcome));
                }
                Chained::Owned(OwnedChained::Sample {
                    metric,
                    value: ChainedValue::Owned(value),
                })
            }
            Some(MigrationOutcome::SpanKind(kind)) => {
                let Record::SpanStart(start) = viewed else {
                    return Err(failed(ChainErrorKind::WrongOutcome));
                };
                Chained::Owned(OwnedChained::SpanStart(dduroc_format::record::SpanStart {
                    kind,
                    ..start
                }))
            }
        };
    }
    Ok(current)
}

// ════════════════════════════════════════════════════════════════════════════
// The physical run
// ════════════════════════════════════════════════════════════════════════════

/// The outcome of a physical migration run.
///
/// Segments: every one encountered during the run fell into exactly one column.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MigrationReport {
    /// Rewritten into the current layout.
    pub rewritten: usize,
    /// Skipped by their footer: no step touches them. Their header keeps the
    /// earlier version — which is legal, and the current decoders read them
    /// correctly precisely because none of the affected types are in them.
    pub skipped_untouched: usize,
    /// Already at the current version (or a newer one).
    pub already_current: usize,
    /// Deleted whole: the steps removed all of their records.
    pub emptied: usize,
    /// Vanished during the run: rotation got there first. Not a loss — their
    /// history would have been thrown out by the same rotation without a
    /// migration.
    pub rotated_away: usize,
    /// Did not parse (a broken header) or belong to a foreign store. The run
    /// does not touch them; the reader knows them as damage or as foreigners
    /// anyway — they do not stand in the way of stamping the metadata, because
    /// no schema version would have read them.
    pub corrupt_or_foreign: usize,
    /// Records that landed in the rewritten segments.
    pub records_rewritten: u64,
    /// Records deleted by the steps.
    pub records_dropped: u64,
}

/// Run every channel of a namespace. Returns the combined report; stamping
/// the metadata is the caller's job (`Namespace::migrate`), which holds both
/// the file and the atomic.
pub(crate) fn run_namespace(
    schema: &Schema,
    store_id: u64,
    writer: &Writer,
    ns: NsId,
    channel_dirs: &[std::path::PathBuf],
) -> Result<MigrationReport> {
    let mut report = MigrationReport::default();
    for (idx, channel_dir) in channel_dirs.iter().enumerate() {
        run_channel(
            channel_dir,
            schema,
            store_id,
            writer,
            ns,
            ChannelIdx(idx as u16),
            &mut report,
        )?;
    }
    Ok(report)
}

fn run_channel(
    dir: &Path,
    schema: &Schema,
    store_id: u64,
    writer: &Writer,
    ns: NsId,
    channel: ChannelIdx,
    report: &mut MigrationReport,
) -> Result<()> {
    // The trace of an earlier interrupted run: nothing addresses the contents
    // of a `*.tmp`, and the name it occupies would get in the way of a new
    // attempt.
    crate::fsutil::sweep_tmp(dir)?;

    for name in crate::rotation::Inventory::scan_names(dir)? {
        let path = dir.join(name.to_string());
        let mut reader = match SegmentReader::open(&path) {
            Ok(r) => r,
            // The file vanished underfoot — rotation runs in parallel.
            Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                report.rotated_away += 1;
                continue;
            }
            // No schema version will read a broken header: there is nothing to
            // rewrite, and leaving the metadata unstamped because of it would
            // make migration impossible forever.
            Err(Error::Corrupt { .. }) => {
                report.corrupt_or_foreign += 1;
                continue;
            }
            Err(e) => return Err(e),
        };
        if reader.header().store_id != store_id {
            // A foreign dump planted in the directory: not ours, so not
            // touched.
            report.corrupt_or_foreign += 1;
            continue;
        }
        let seg_version = reader.header().protocol_version.0;
        if seg_version >= schema.version.0 {
            report.already_current += 1;
            continue;
        }
        let steps = chain(schema, seg_version).map_err(|e| Error::Corrupt {
            path: path.clone(),
            reason: e.to_string(),
        })?;
        if reader.is_sealed()
            && let Some(footer) = reader.footer()
            && !chain_touches(&steps, &footer)
        {
            report.skipped_untouched += 1;
            continue;
        }

        let rewrite = match rewrite_segment(&mut reader, &path, schema.version, &steps) {
            Ok(r) => r,
            Err(e) => {
                // The original is untouched; the next attempt will sweep up the
                // unfinished tmp. The run breaks off: carrying on after a
                // failure would mean stamping the metadata over a segment that
                // was never rewritten.
                let _ = std::fs::remove_file(tmp_path(&path));
                return Err(e);
            }
        };
        let emptied = matches!(rewrite.commit, MigrationCommit::Remove);
        if writer.commit_migration(ns, channel, name, rewrite.commit)? {
            if emptied {
                report.emptied += 1;
            } else {
                report.rewritten += 1;
            }
            report.records_rewritten += rewrite.records_rewritten;
            report.records_dropped += rewrite.records_dropped;
        } else {
            // Rotation got there first: nobody needs the result, and it is not
            // in the report either — those records were thrown out, not
            // rewritten.
            let _ = std::fs::remove_file(tmp_path(&path));
            report.rotated_away += 1;
        }
    }
    Ok(())
}

fn tmp_path(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    std::path::PathBuf::from(s)
}

/// The outcome of rewriting one segment — before the commit.
struct Rewrite {
    commit: MigrationCommit,
    records_rewritten: u64,
    records_dropped: u64,
}

/// Rewrite a segment into a temporary file. The file is synced; committing is
/// the writer's job.
///
/// Block boundaries are preserved: a block in is a block out (an emptied one
/// drops out). So are the name and the `base`: even if the leading records are
/// deleted, selecting segments by name only becomes more conservative — see
/// SPEC.
fn rewrite_segment(
    reader: &mut SegmentReader,
    path: &Path,
    to: ProtocolVersion,
    steps: &[&Migration],
) -> Result<Rewrite> {
    let src = reader.header();
    let header = SegmentHeader {
        protocol_version: to,
        boot: src.boot,
        base: src.base,
        store_id: src.store_id,
    };
    // The growth limit comes from the original's actual data, and the window
    // grows as needed. Slack "just in case" is not free here: the temporary
    // file lies next to the original, does not count towards the class budget
    // and presses on the medium for real. The former double slack meant a run
    // demanded three times the space the segment takes — on a device laid out
    // to its own budget, that is an ENOSPC out of nowhere. Only the chain of
    // steps knows how much the records will swell, so the only honest answer is
    // to grow by actual need and refuse exactly when the space really did run
    // out. Seal trims the tail. Exactly the original's data plus room for the
    // zero terminator header: with the record size unchanged the file never
    // grows at all.
    let limit = reader.data_end() + BlockHeader::SIZE as u64;
    let tmp = tmp_path(path);
    let mut seg = SegmentWriter::create_at(&tmp, header, limit)?;

    let offsets: Vec<u64> = match reader.footer() {
        Some(f) => f.blocks.iter().map(|b| b.offset).collect(),
        // An unsealed segment (or one with a broken footer) is walked by
        // scanning; a damaged tail is discarded in the process — exactly as
        // when reading.
        None => reader.scan_block_offsets().0,
    };

    let mut footer = FooterBuilder::new();
    let mut builder = BlockBuilder::new();
    let mut buf = Vec::new();
    let mut out = Vec::new();
    let mut rewritten = 0u64;
    let mut dropped = 0u64;

    for offset in offsets {
        match reader.read_block_at(offset, &mut buf) {
            Ok(Some(_)) => {}
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
        let Some(block) = crate::segment::parse_block(&buf)? else {
            continue;
        };
        let compression = block.header.compression;
        let mut last = block.header.base;
        for item in block.records() {
            let (at, record) = item.map_err(|e| Error::Corrupt {
                path: path.to_owned(),
                reason: format!("a segment record did not parse: {e}"),
            })?;
            let chained = apply(steps, record).map_err(|e| Error::Corrupt {
                path: path.to_owned(),
                reason: format!(
                    "{e}: the record cannot be brought to the current version — the history \
                     and the actual layout have drifted apart; the original is untouched"
                ),
            })?;
            let Some(rec) = chained.record() else {
                dropped += 1;
                continue;
            };
            match rec {
                Record::Message(m) => footer.add_event(m.event),
                Record::Sample(s) => footer.add_metric(s.metric),
                _ => {}
            }
            builder.push(at, &rec)?;
            last = at;
            rewritten += 1;
        }
        if builder.is_empty() {
            footer.discard_pending();
            continue;
        }
        out.clear();
        let h = builder.finish(seg.next_seq(), compression, &mut out)?;
        let at = seg.append_block(&out)?;
        footer.add_block(at, &h, last);
    }

    if footer.is_empty() {
        // Every record was deleted: the segment is no more.
        drop(seg);
        std::fs::remove_file(&tmp).ok();
        return Ok(Rewrite {
            commit: MigrationCommit::Remove,
            records_rewritten: rewritten,
            records_dropped: dropped,
        });
    }
    let size = seg.data_end() + {
        let bytes = footer.build();
        let len = bytes.len() as u64;
        seg.seal(&bytes)?;
        len
    };
    Ok(Rewrite {
        commit: MigrationCommit::Replace { tmp, size },
        records_rewritten: rewritten,
        records_dropped: dropped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::record::Text;
    use dduroc_format::{Level, Value};
    use std::result::Result;

    fn msg(event: u16, payload: &[u8]) -> Record<'_> {
        Record::Message(Message {
            event: EventId(event),
            span: Some(SpanId(7)),
            payload,
        })
    }

    fn sample(metric: u16, v: u64) -> Record<'static> {
        Record::Sample(Sample {
            metric: MetricId(metric),
            value: Value::U64(v),
        })
    }

    fn step(
        from: u16,
        events: &'static [EventId],
        metrics: &'static [MetricId],
        migrate: fn(MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError>,
    ) -> Migration {
        step_of(from, events, metrics, &[], migrate)
    }

    #[allow(unused_imports)]
    use dduroc_format::SpanKindId;

    fn step_of(
        from: u16,
        events: &'static [EventId],
        metrics: &'static [MetricId],
        spans: &'static [SpanKindId],
        migrate: fn(MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError>,
    ) -> Migration {
        Migration {
            from,
            touches_all: events.is_empty() && metrics.is_empty() && spans.is_empty(),
            events,
            metrics,
            spans,
            migrate,
        }
    }

    #[test]
    fn declared_sets_are_binding_not_advisory() {
        // The step declared events [1] — it does not see records of other types
        // at all. Otherwise "declares one thing, transforms another" would live
        // on silently: segments would be selected by what was declared while
        // what was rewritten was something else.
        fn poison(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::Message {
                event: EventId(0xEE),
                payload: vec![0xEE],
            }))
        }
        let s = step(1, &[EventId(1)], &[], poison);
        let steps = [&s];

        // What was declared is transformed.
        match apply(&steps, msg(1, &[1])).unwrap() {
            Chained::Owned(OwnedChained::Message { event, span, .. }) => {
                assert_eq!(event, EventId(0xEE));
                assert_eq!(
                    span,
                    Some(SpanId(7)),
                    "the attachment to a span survived the replacement"
                );
            }
            other => panic!("expected a replacement, got {other:?}"),
        }

        // What was not declared passes the step with its original bytes.
        for rec in [
            msg(2, &[2]),
            sample(9, 42),
            Record::Text(Text {
                level: Level::Info,
                span: None,
                target: "t",
                text: "x",
            }),
        ] {
            match apply(&steps, rec).unwrap() {
                Chained::Same(r) => assert_eq!(r, rec, "the bytes must be the originals"),
                other => panic!("{rec:?} should not have changed: {other:?}"),
            }
        }
    }

    #[test]
    fn touches_all_sees_even_what_sets_cannot_name() {
        // Text and spans cannot be expressed by sets of types — but a step with
        // touches_all has to see them: it is the only way to, say, scrub free
        // text of sensitive data.
        fn drop_text(r: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(match r.record {
                Record::Text(_) => None,
                _ => Some(MigrationOutcome::AsIs),
            })
        }
        let s = step(1, &[], &[], drop_text);
        let steps = [&s];

        let text = Record::Text(Text {
            level: Level::Info,
            span: None,
            target: "t",
            text: "a secret",
        });
        assert!(matches!(apply(&steps, text).unwrap(), Chained::Dropped));
        assert!(matches!(
            apply(&steps, msg(1, &[1])).unwrap(),
            Chained::Same(_)
        ));
    }

    #[test]
    fn steps_compose_in_order_and_each_sees_the_previous_layout() {
        // A jump v1 → v3: step 1 re-encodes the payload and changes the type,
        // and step 2 has to see the NEW type already — that is how one and the
        // same step works both alone and in a chain.
        fn one(r: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            let old: (u8,) = r.decode()?;
            Ok(Some(MigrationOutcome::Message {
                event: EventId(2),
                payload: postcard::to_allocvec(&(u16::from(old.0) * 2,)).unwrap(),
            }))
        }
        fn two(r: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            let mid: (u16,) = r.decode()?;
            Ok(Some(MigrationOutcome::Message {
                event: EventId(3),
                payload: postcard::to_allocvec(&(u32::from(mid.0) + 1,)).unwrap(),
            }))
        }
        let s1 = step(1, &[EventId(1)], &[], one);
        let s2 = step(2, &[EventId(2)], &[], two);
        let steps = [&s1, &s2];

        let payload = postcard::to_allocvec(&(21u8,)).unwrap();
        let out = apply(&steps, msg(1, &payload)).unwrap();
        match out {
            Chained::Owned(OwnedChained::Message { event, payload, .. }) => {
                assert_eq!(event, EventId(3));
                let v: (u32,) = postcard::from_bytes(&payload).unwrap();
                assert_eq!(v.0, 43, "21 * 2 + 1: the steps composed in order");
            }
            other => panic!("{other:?}"),
        }

        // A record that appeared already in the v2 layout passes only step 2.
        let payload = postcard::to_allocvec(&(5u16,)).unwrap();
        let out = apply(&steps[1..], msg(2, &payload)).unwrap();
        match out {
            Chained::Owned(OwnedChained::Message { event, .. }) => assert_eq!(event, EventId(3)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn a_span_kind_is_renamed_and_its_identity_is_left_alone() {
        // A span's kind is about all it has beyond its identity. The number and
        // the parent are that identity: the span's end, its messages and its
        // children refer to them, and a chain cannot rewrite those references.
        fn rename(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::SpanKind(SpanKindId(0x42))))
        }
        let s = step_of(1, &[], &[], &[SpanKindId(1)], rename);
        let steps = [&s];

        let start = Record::SpanStart(dduroc_format::record::SpanStart {
            span: SpanId(11),
            kind: SpanKindId(1),
            parent: Some(SpanId(3)),
        });
        match apply(&steps, start).unwrap() {
            Chained::Owned(OwnedChained::SpanStart(s)) => {
                assert_eq!(s.kind, SpanKindId(0x42), "the kind was renamed");
                assert_eq!(s.span, SpanId(11), "the number is the record's identity");
                assert_eq!(s.parent, Some(SpanId(3)), "and so is the parent");
            }
            other => panic!("{other:?}"),
        }

        // A span's end carries no kind and is not selected by a set of kinds:
        // it refers by number to a span that has already begun.
        let end = Record::SpanEnd { span: SpanId(11) };
        assert!(matches!(apply(&steps, end).unwrap(), Chained::Same(_)));

        // A step does not see a span of another kind — the set binds.
        let other_kind = Record::SpanStart(dduroc_format::record::SpanStart {
            span: SpanId(12),
            kind: SpanKindId(9),
            parent: None,
        });
        assert!(matches!(
            apply(&steps, other_kind).unwrap(),
            Chained::Same(_)
        ));
    }

    #[test]
    fn a_transformed_value_survives_a_remap_on_top_of_it() {
        // Transforming a value and remapping an identifier are different steps,
        // and they have to compose in either order: otherwise a legitimate
        // chain would be declared a defect in a step's code.
        fn scale(r: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            let Some(Value::U64(v)) = r.value() else {
                return Err(DecodeError);
            };
            Ok(Some(MigrationOutcome::Sample {
                metric: MetricId(0x10),
                value: crate::staged::OwnedValue::F64(v as f64 / 10.0),
            }))
        }
        fn rename(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::SampleMetric(MetricId(0x20))))
        }
        let s1 = step(1, &[], &[MetricId(0x10)], scale);
        let s2 = step(2, &[], &[MetricId(0x10)], rename);

        let out = apply(&[&s1, &s2], sample(0x10, 365)).unwrap();
        match out.record() {
            Some(Record::Sample(s)) => {
                assert_eq!(
                    s.metric,
                    MetricId(0x20),
                    "a remap on top of a transformation"
                );
                assert_eq!(s.value, Value::F64(36.5), "the value survived the remap");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn an_outcome_that_does_not_fit_the_record_is_a_defect_not_a_no_op() {
        // An outcome that does not apply to the record kind is a defect in the
        // step's code. Staying silent about it is not an option: the record
        // would stay in the earlier layout while the run reported success and
        // stamped the metadata.
        fn span_kind(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::SpanKind(SpanKindId(1))))
        }
        let s = step(1, &[], &[MetricId(1)], span_kind);
        let e = apply(&[&s], sample(1, 5)).unwrap_err();
        assert_eq!(e.kind, ChainErrorKind::WrongOutcome);

        fn value(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::Sample {
                metric: MetricId(1),
                value: crate::staged::OwnedValue::U64(1),
            }))
        }
        let s = step(1, &[EventId(1)], &[], value);
        let e = apply(&[&s], msg(1, &[1])).unwrap_err();
        assert_eq!(e.kind, ChainErrorKind::WrongOutcome);
    }

    #[test]
    fn a_step_that_touches_spans_rewrites_every_segment() {
        // There is no set of span kinds in the footer — there is nobody to ask
        // "is there such a span here". Deciding "no" on a guess would leave
        // those records in the earlier layout forever, and silently at that:
        // the run reports success and stamps the metadata, and the next pass
        // skips those segments.
        fn noop(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::AsIs))
        }
        let footer = dduroc_format::Footer {
            blocks: Vec::new(),
            events: vec![EventId(7)],
            metrics: vec![MetricId(7)],
            min: dduroc_format::Micros(0),
            max: dduroc_format::Micros(0),
        };

        let spans_step = step_of(1, &[], &[], &[SpanKindId(1)], noop);
        assert!(
            spans_step.touches(&footer),
            "a step with spans must rewrite the segment whatever the footer holds"
        );

        // And without spans the rule is as before: not one declared type was
        // found, so the segment is not touched.
        let events_step = step(1, &[EventId(1)], &[], noop);
        assert!(!events_step.touches(&footer));
    }

    #[test]
    fn metric_remap_keeps_the_value_bytes() {
        fn remap(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::SampleMetric(MetricId(0x20))))
        }
        let s = step(1, &[], &[MetricId(0x10)], remap);
        let steps = [&s];

        match apply(&steps, sample(0x10, 777)).unwrap() {
            Chained::Owned(o @ OwnedChained::Sample { metric, .. }) => {
                assert_eq!(metric, MetricId(0x20));
                match o.as_record() {
                    Record::Sample(s) => assert_eq!(s.value, Value::U64(777)),
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
        // A foreign metric is untouched.
        assert!(matches!(
            apply(&steps, sample(0x11, 1)).unwrap(),
            Chained::Same(_)
        ));
    }

    #[test]
    fn a_failing_step_names_itself() {
        fn broken(r: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            let _: (u64, u64, u64, u64) = r.decode()?; // the payload is knowably shorter
            Ok(Some(MigrationOutcome::AsIs))
        }
        let s1 = step(1, &[EventId(1)], &[], |_| Ok(Some(MigrationOutcome::AsIs)));
        let s2 = step(2, &[EventId(1)], &[], broken);
        let steps = [&s1, &s2];

        let err = apply(&steps, msg(1, &[1])).unwrap_err();
        assert_eq!(
            err.step_from, 2,
            "the culprit is named rather than \"somewhere in the chain\""
        );
        assert_eq!(err.kind, ChainErrorKind::Decode);
    }

    #[test]
    fn an_outcome_that_does_not_fit_the_record_is_a_step_bug() {
        // A SampleMetric for a message is a defect in the step's code. Passing
        // it over silently would leave the record in the old layout and declare
        // success.
        fn wrong(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::SampleMetric(MetricId(1))))
        }
        let s = step(1, &[EventId(1)], &[], wrong);
        let err = apply(&[&s], msg(1, &[1])).unwrap_err();
        assert_eq!(err.kind, ChainErrorKind::WrongOutcome);
    }

    #[test]
    fn chain_is_resolved_or_refused_never_guessed() {
        use crate::schema::tests_support::minimal_schema_with_migrations;
        fn asis(_: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::AsIs))
        }
        static STEPS: &[Migration] = &[
            Migration {
                from: 1,
                touches_all: true,
                events: &[],
                metrics: &[],
                spans: &[],
                migrate: asis,
            },
            Migration {
                from: 2,
                touches_all: true,
                events: &[],
                metrics: &[],
                spans: &[],
                migrate: asis,
            },
        ];
        let schema = minimal_schema_with_migrations(3, STEPS);

        assert_eq!(chain(&schema, 3).unwrap().len(), 0, "the current version");
        assert_eq!(chain(&schema, 2).unwrap().len(), 1);
        let full = chain(&schema, 1).unwrap();
        assert_eq!(full.len(), 2);
        assert_eq!(full[0].from, 1);
        assert_eq!(full[1].from, 2);

        // A hole in the chain is a refusal, not a silently skipped step.
        static GAPPY: &[Migration] = &[Migration {
            from: 2,
            touches_all: true,
            events: &[],
            metrics: &[],
            spans: &[],
            migrate: asis,
        }];
        let schema = minimal_schema_with_migrations(3, GAPPY);
        let err = chain(&schema, 1).unwrap_err();
        assert_eq!(err.step_from, 1);
        assert_eq!(err.kind, ChainErrorKind::MissingStep);
    }
    /// Write a single-segment file of version `version` with `count` records.
    fn segment_of(dir: &Path, version: u16, count: usize) -> (std::path::PathBuf, u64) {
        use dduroc_format::Compression;
        use dduroc_format::segment::SegmentName;

        let header = SegmentHeader {
            protocol_version: ProtocolVersion(version),
            boot: dduroc_format::BootCounter(0),
            base: dduroc_format::Micros(0),
            store_id: 0,
        };
        let mut seg = SegmentWriter::create(dir, header, 1 << 20).unwrap();
        let mut footer = FooterBuilder::new();
        let mut builder = BlockBuilder::new();
        let mut out = Vec::new();
        let payload = vec![0xABu8; 200];
        for i in 0..count {
            builder
                .push(dduroc_format::Micros(i as u64), &msg(1, &payload))
                .unwrap();
            if builder.raw_len() >= 4096 || i + 1 == count {
                out.clear();
                let h = builder
                    .finish(seg.next_seq(), Compression::None, &mut out)
                    .unwrap();
                footer.add_event(EventId(1));
                let at = seg.append_block(&out).unwrap();
                footer.add_block(at, &h, dduroc_format::Micros(i as u64));
            }
        }
        seg.seal(&footer.build()).unwrap();
        let path = dir.join(
            SegmentName::new(dduroc_format::BootCounter(0), dduroc_format::Micros(0)).to_string(),
        );
        let size = std::fs::metadata(&path).unwrap().len();
        (path, size)
    }

    #[test]
    fn a_rewrite_asks_for_the_room_it_needs_not_for_twice_it() {
        // A run's temporary file lies next to the original, does not count
        // towards the class budget and presses on the medium for real. While
        // the capacity was taken at double "just in case", a run demanded three
        // times the space the segment takes — on a device laid out to its own
        // budget that is an ENOSPC out of nowhere, while on a developer's roomy
        // machine it would pass unnoticed. Hence the free-space ceiling in this
        // test.
        fn widen(r: MigrationInput<'_>) -> Result<Option<MigrationOutcome>, DecodeError> {
            let _ = r;
            Ok(Some(MigrationOutcome::AsIs))
        }
        let dir = tempfile::tempdir().unwrap();
        let (path, size) = segment_of(dir.path(), 1, 400);
        let steps = [&step_of(1, &[EventId(1)], &[], &[], widen)];

        // The medium has exactly as much free as a copy takes plus a little: a
        // rewritten segment of the same size as the original has to fit.
        crate::segment::fault::free_space(size + size / 8);
        let outcome = rewrite_segment(
            &mut SegmentReader::open(&path).unwrap(),
            &path,
            ProtocolVersion(2),
            &steps,
        );
        crate::segment::fault::unlimited_space();
        let rewrite = outcome.expect("there is room for a copy of the segment");

        let MigrationCommit::Replace { tmp, size: out } = rewrite.commit else {
            panic!("the segment was rewritten, not deleted");
        };
        assert_eq!(rewrite.records_rewritten, 400);
        assert!(
            out <= size + size / 8,
            "the rewritten segment is {out} against the original {size}"
        );
        assert!(tmp.exists(), "the result awaits its commit");
        let _ = std::fs::remove_file(&tmp);
    }
}
