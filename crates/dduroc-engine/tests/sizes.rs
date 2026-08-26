//! The sizes of the hot path's structs.
//!
//! The writer's queues are allocated whole when a store is opened, so the size
//! of [`Staged`] sets the memory cost directly: with the defaults that is 9216
//! slots. On armv7 with hundreds of megabytes of memory, a record growing by
//! ten bytes is noticeable, and there is no other way to notice it — the
//! compiler will not say so.
//!
//! The test is not about a "correct" size but about a change of size being
//! **deliberate**: if it grew, it is worth looking at whether the new field was
//! worth it.

use dduroc_engine::staged::{INLINE_PAYLOAD, OwnedValue, Payload, Staged, StagedRecord};

#[test]
fn hot_path_structs_do_not_grow_unnoticed() {
    let staged = std::mem::size_of::<Staged>();
    let record = std::mem::size_of::<StagedRecord>();
    let value = std::mem::size_of::<OwnedValue>();
    let payload = std::mem::size_of::<Payload>();

    // The reference points were taken on x86-64; on armv7 pointers are half as
    // long, so a ceiling is checked rather than an exact equality.
    assert!(
        staged <= 72,
        "Staged grew to {staged} bytes: the default queue got dearer by \
         {} KB. Is the new field worth it?",
        staged.saturating_sub(72) * 9216 / 1024
    );
    assert!(record <= 56, "StagedRecord = {record}");
    assert!(value <= 48, "OwnedValue = {value}");

    // Payload holds an inline buffer: an event with a couple of numbers must
    // not go to the heap, or the hot path allocates on every call.
    assert!(
        payload > INLINE_PAYLOAD,
        "Payload = {payload}, inline capacity = {INLINE_PAYLOAD}"
    );

    // The size is set by the Message variant with its payload rather than by a
    // telemetry sample: saving on a sample is pointless while Message is
    // larger.
    assert!(
        record >= payload,
        "StagedRecord ({record}) is smaller than Payload ({payload}) — the \
         layout changed and the reference points need recomputing"
    );
}
