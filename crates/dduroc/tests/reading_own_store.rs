//! A reader built over one's own store.
//!
//! Until now the link between a `Store` and a `Reader` rested on the caller's
//! memory, and it had a silent outcome: a class moved to a medium of its own is
//! a second tree, and a reader told only the main root showed the history
//! without it. With no error, no damage notice, not a single sign that anything
//! was missing — just shorter.

use dduroc::prelude::*;
use dduroc::{ChannelConfig, StorageClass, StoreConfig, StoreExt};
use dduroc_read::{KindFilter, Order, Query, Reader};

dduroc::schema! {
    name: split,
    version: 1,
    languages: [en],

    events {
        Ping = 0x01 { level: Info, en: "ping {seq}", seq: u32 },
        Fault = 0x02 { level: Error, store: critical, en: "fault {code}", code: u8 },
    }
}

/// The texts of the journal records, oldest to newest.
fn lines(reader: &Reader) -> Vec<String> {
    let q = Query::new().kinds(KindFilter::LOGS).order(Order::Oldest);
    let result = reader.query(&q).expect("the query");
    assert!(result.damaged.is_empty(), "{:?}", result.damaged);
    result
        .entries
        .iter()
        .filter_map(|e| reader.render(e, "ru"))
        .collect()
}

#[test]
fn a_reader_of_the_store_sees_the_class_that_lives_on_another_medium() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("data");
    let vault = dir.path().join("vault");

    let store = Store::open(
        StoreConfig::new(&root)
            .with_budget_per_class(16 << 20)
            .channel(
                StorageClass::Critical,
                ChannelConfig {
                    custom_root: Some(vault.clone()),
                    ..ChannelConfig::critical(16 << 20)
                },
            ),
    )
    .unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.log(split::events::Fault { code: 3 });
    ns.sync().unwrap();

    // The store knows both of its roots and the schema of the namespace that
    // came up — there is nowhere and no reason to name them a second time.
    assert_eq!(store.roots().len(), 2, "{:?}", store.roots());
    let reader = store.reader();
    assert_eq!(lines(&reader), ["ping 1", "fault 3"]);

    // A dump that was not told every root is not a "short answer" but a
    // refusal: completeness is checked against the schema at open time. Such a
    // reader used to show the history without the critical part, giving nothing
    // away.
    let e = Reader::open_dump([&root], &[split::SCHEMA]).unwrap_err();
    assert!(
        matches!(
            &e,
            dduroc_read::ReadError::IncompleteDump { namespace, class }
                if namespace == "orc-probe-0" && *class == StorageClass::Critical
        ),
        "{e}"
    );
    // With every root the dump reads whole.
    assert_eq!(
        lines(&Reader::open_dump([&root, &vault], &[split::SCHEMA]).unwrap()),
        ["ping 1", "fault 3"]
    );

    store.shutdown();
}

mod other {
    // A foreign schema with THE SAME id 0x01 but a different type under it: a
    // collision of identifiers between schemas is normal — an id is unique only
    // within its own schema.
    dduroc::schema! {
        name: other,
        version: 1,
        languages: [en],
        events {
            Boom = 0x01 { level: Info, en: "boom {code}", code: u8 },
        }
    }
}

#[test]
fn an_entry_decodes_back_into_the_type_it_was_written_as() {
    // `render` gives the text; here is the way back to the FIELDS. The type is
    // checked against the schema of the record's namespace: a matching id is
    // not yet a matching type.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 7 });
    // The same id from a FOREIGN schema — in the neighbouring namespace.
    let foreign = store.namespace("other-0", other::other::SCHEMA).unwrap();
    foreign.log(other::other::events::Boom { code: 3 });
    // A record that declares itself a Ping but whose payload does not parse.
    ns.try_log_raw(dduroc::EventId(0x01), &[0xFF], None)
        .unwrap();
    ns.sync().unwrap();
    foreign.sync().unwrap();

    let reader = store.reader();
    let got = reader
        .query(&Query::new().kinds(KindFilter::LOGS).order(Order::Oldest))
        .unwrap();

    let pings: Vec<_> = got
        .entries
        .iter()
        .filter_map(|e| reader.decode::<split::events::Ping>(e))
        .collect();
    assert_eq!(
        pings.len(),
        2,
        "a Ping from its own namespace yes; a Boom with the same id no"
    );
    assert_eq!(pings[0], Ok(split::events::Ping { seq: 7 }));
    assert_eq!(
        pings[1],
        Err(dduroc::DecodeError),
        "an unparsable payload is an error, not silence"
    );
    // A foreign Boom parses with its own type — in its own namespace.
    assert_eq!(
        got.entries
            .iter()
            .filter_map(|e| reader.decode::<other::other::events::Boom>(e))
            .collect::<Vec<_>>(),
        [Ok(other::other::events::Boom { code: 3 })]
    );
    store.shutdown();
}

#[test]
fn an_unknown_directory_under_a_namespace_is_reported_not_hidden() {
    // A channel is a storage class, and the listing of channels is typed. A
    // directory that is the channel of no class this build knows (a foreign
    // directory, a dump from a future version with a new class) is not parsed —
    // but neither does it drop out silently: it is declared damage.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.sync().unwrap();
    std::fs::create_dir(dir.path().join("orc-probe-0").join("scratch")).unwrap();

    let listing = store.reader().namespaces().unwrap();
    assert_eq!(
        listing.namespaces[0].channels,
        [StorageClass::Default, StorageClass::Critical],
        "the known channels are there and typed"
    );
    assert_eq!(
        listing.damaged.len(),
        1,
        "an unknown directory must be announced: {:?}",
        listing.damaged
    );
    assert!(listing.damaged[0].path.ends_with("scratch"));
    store.shutdown();
}

#[test]
fn schemas_outlive_the_namespace_handle() {
    // A namespace handle is released as soon as the service has done its work;
    // the process does not thereby forget how to decode its records. Otherwise
    // a reader taken from the store later would show bare identifiers.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    {
        let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
        ns.log(split::events::Ping { seq: 7 });
        ns.sync().unwrap();
    }

    assert_eq!(store.schemas().len(), 1);
    assert_eq!(lines(&store.reader()), ["ping 7"]);
    store.shutdown();
}

#[test]
fn a_live_reader_stays_current_without_being_rebuilt() {
    // A live reader is created once at start and lives in parallel with
    // writing. Everything that appears in the store after it was created it has
    // to see without being recreated: the truth is asked of the store on every
    // query rather than frozen at creation.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let reader = store.reader(); // before the namespace, the records and the time anchor

    assert!(lines(&reader).is_empty(), "the store is still empty");

    // The namespace came up AFTER the reader was created: the schema has to be
    // found.
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.sync().unwrap();
    let first = reader.query(&Query::new().kinds(KindFilter::LOGS)).unwrap();
    assert_eq!(
        reader.render(&first.entries[0], "ru").as_deref(),
        Some("ping 1"),
        "the schema of a service that started after the reader was created is visible"
    );
    assert!(
        first.entries[0].utc.is_none(),
        "there is no anchor yet — there is nowhere to take wall-clock time from"
    );

    // A time synchronization AFTER the reader was created is retroactive, and
    // that same reader has to see it on its very next query.
    store.record_sync(Utc::now(), SyncSource::Ntp).unwrap();
    let second = reader.query(&Query::new().kinds(KindFilter::LOGS)).unwrap();
    assert!(
        second.entries[0].utc.is_some(),
        "the anchor is retroactive: a record made before the synchronization got a UTC"
    );
    store.shutdown();
}

#[test]
fn a_torn_active_tail_is_data_not_yet_for_live_and_damage_for_dump() {
    // The writer lays a block down with one write, but a reader sees the pages
    // with no guarantee of being whole: an active segment may have a tail that
    // has not arrived yet. For a live reader that is "the data is not ready
    // yet", for a dump honest corruption: nobody appends to a dump.
    use dduroc_engine::segment::SegmentReader;
    use std::os::unix::fs::FileExt;

    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 1 });
    ns.sync().unwrap();

    // The channel's only segment is the active one. We append garbage to it
    // where the writer would have gone on writing: that is what a block whose
    // write has not become visible whole looks like.
    let ch_dir = dir
        .path()
        .join("orc-probe-0")
        .join(StorageClass::Default.as_str());
    let seg_path = std::fs::read_dir(&ch_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "seg"))
        .expect("the active segment");
    let mut seg = SegmentReader::open(&seg_path).unwrap();
    let (offsets, stopped) = seg.scan_block_offsets();
    assert!(
        stopped.is_none(),
        "before the interference the segment is intact"
    );
    let mut buf = Vec::new();
    let end = seg
        .read_block_at(*offsets.last().unwrap(), &mut buf)
        .unwrap()
        .expect("the last block is intact");
    drop(seg);
    std::fs::OpenOptions::new()
        .write(true)
        .open(&seg_path)
        .unwrap()
        .write_all_at(&[0xEE; 16], end)
        .unwrap();

    let live = store
        .reader()
        .query(&Query::new().kinds(KindFilter::LOGS))
        .unwrap();
    assert_eq!(live.entries.len(), 1, "the intact blocks are read");
    assert!(
        live.damaged.is_empty(),
        "an unfinished tail is not corruption: {:?}",
        live.damaged
    );

    let dump = Reader::open_dump([dir.path()], &[split::SCHEMA])
        .unwrap()
        .query(&Query::new().kinds(KindFilter::LOGS))
        .unwrap();
    assert_eq!(
        dump.damaged.len(),
        1,
        "in a dump the same tail is damage, and silence about it is not allowed"
    );
    store.shutdown();
}

#[test]
fn rotation_and_writes_under_a_live_reader_never_look_like_damage() {
    // A smoke check of real concurrency: the writer hammers records through a
    // tight quota (constant rotation) while the reader keeps asking. No answer
    // has a right to declare damage: eviction and appending are the store's
    // ordinary life, not corruption.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(
        StoreConfig::new(dir.path())
            .with_budget_per_class(16 << 20)
            .channel(StorageClass::Default, {
                let mut c = ChannelConfig::new(16 << 20);
                c.segment_bytes = 128 * 1024;
                c.block_max_bytes = 8 * 1024;
                c
            }),
    )
    .unwrap();
    let ns = store
        .namespace_with_quota(
            "orc-probe-0",
            split::SCHEMA,
            dduroc::NsQuota::new().limit_bytes(StorageClass::Default, 256 * 1024),
        )
        .unwrap();
    let reader = store.reader();

    let writer = std::thread::spawn(move || {
        for i in 0..4000u32 {
            ns.log(split::events::Ping { seq: i });
            if i % 256 == 0 {
                let _ = ns.sync();
            }
        }
        let _ = ns.sync();
    });

    // At least three queries in any case: on a fast machine the writer may
    // finish before the main thread reaches the first query.
    let mut total_queries = 0;
    while !writer.is_finished() || total_queries < 3 {
        let got = reader
            .query(&Query::new().kinds(KindFilter::LOGS).limit(64))
            .unwrap();
        assert!(
            got.damaged.is_empty(),
            "live reading declared corruption: {:?}",
            got.damaged
        );
        total_queries += 1;
    }
    writer.join().unwrap();
    let last = reader.query(&Query::new().kinds(KindFilter::LOGS)).unwrap();
    assert!(last.damaged.is_empty(), "{:?}", last.damaged);
    assert!(
        !last.entries.is_empty() && total_queries > 0,
        "the check had to catch both the writes and the queries"
    );
    store.shutdown();
}

#[test]
fn a_foreign_namespace_needs_its_schema_named() {
    // The store holds another service's namespace: this build has no schema for
    // it, and its records stay identifiers — until the schema is named.
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    let ns = store.namespace("orc-probe-0", split::SCHEMA).unwrap();
    ns.log(split::events::Ping { seq: 2 });
    ns.sync().unwrap();
    store.shutdown();
    drop(ns);
    drop(store);

    // The second process knows nothing of the schema.
    let store = Store::open(StoreConfig::new(dir.path()).with_budget_per_class(16 << 20)).unwrap();
    assert!(store.schemas().is_empty());
    assert!(
        lines(&store.reader()).is_empty(),
        "without a schema there is nothing to render with — and a reader will not invent a text"
    );
    assert_eq!(
        lines(&store.reader().with_schema(split::SCHEMA)),
        ["ping 2"]
    );
    store.shutdown();
}
