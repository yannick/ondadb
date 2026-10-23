//! A frozen set of WAL segments in both ondaDB layouts, and what
//! `Wal::replay` makes of each.
//!
//! `tests/fixtures/epoch1-wal/` is committed to git and vendored by wavesdb,
//! whose layout-1/2 reader (yoloDB plan C step 2c) must decode the same
//! records from the same bytes; `EXPECTED` is the dump below. The segments
//! cover:
//! - layout 1 (per family): legacy flag-byte frames (put, delete,
//!   single-delete, TTL), an empty frame, a schema-1 envelope with a merge
//!   operand and a range delete, a second stripe, and a torn tail;
//! - layout 2 (unified): cf-id-prefixed legacy frames, a schema-2 envelope
//!   with prefixed range bounds, and the 3.2 control frames — prepares
//!   committed, aborted and undecided, a decision whose prepare is gone, a
//!   prepare and its decision in different stripes, and a decision retried
//!   in a later generation.
//!
//! Regenerate with `cargo test --test epoch1_wal_corpus -- --ignored` for a
//! reviewed format change only.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ondadb::config::SyncMode;
use ondadb::format::{
    KIND_DELETE, KIND_MERGE, KIND_PUT, KIND_SINGLE_DELETE,
};
use ondadb::wal::{
    EnvelopeRecord, RangeRef, RecordRef, ReplayRecord, SegmentId, Wal, ENVELOPE_SCHEMA_PER_CF,
    ENVELOPE_SCHEMA_UNIFIED,
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/epoch1-wal")
}

/// Every segment file, in replay order: generation, then stripe.
const SEGMENTS: &[(&str, Layout, u64)] = &[
    ("cf-x/wal-3.log", Layout::PerCf, 3),
    ("cf-x/wal-3.log.s2", Layout::PerCf, 3),
    ("unified-wal-7.log", Layout::Unified, 7),
    ("unified-wal-7.log.s1", Layout::Unified, 7),
    ("unified-wal-8.log", Layout::Unified, 8),
];

#[derive(Clone, Copy)]
enum Layout {
    PerCf,
    Unified,
}

fn seg(layout: Layout, generation: u64) -> SegmentId {
    match layout {
        Layout::PerCf => SegmentId::per_cf(generation),
        Layout::Unified => SegmentId::unified(generation),
    }
}

fn put(key: &[u8], value: &'static [u8], seq: u64) -> RecordRef<'static> {
    RecordRef {
        key: Box::leak(key.to_vec().into_boxed_slice()),
        value,
        seq,
        ttl: 0,
        kind: KIND_PUT,
    }
}

fn kind(key: &[u8], seq: u64, kind: u64) -> RecordRef<'static> {
    RecordRef {
        kind,
        ..put(key, b"", seq)
    }
}

/// `key` behind family `id`'s 8-byte big-endian unified prefix.
fn pk(id: u64, key: &[u8]) -> Vec<u8> {
    let mut k = id.to_be_bytes().to_vec();
    k.extend_from_slice(key);
    k
}

const CF_A: u64 = 0x1111_2222_3333_4444;
const CF_B: u64 = 0x5555_6666_7777_8888;

fn id(n: u8) -> [u8; 16] {
    [n; 16]
}

/// Write one stripe file at `dir/name` holding `frames`, as segment
/// `layout`/`generation`. Each stripe is written through its own `Wal` under
/// `SyncMode::Full` (a single stripe file), then renamed into place: the
/// header names no stripe, so a stripe file is a stripe-0 file renamed.
fn write_stripe(dir: &Path, name: &str, layout: Layout, generation: u64, frames: impl FnOnce(&Wal)) {
    let tmp = dir.join(".tmp-wal");
    let w = Wal::open(&tmp, SyncMode::Full, Duration::ZERO, seg(layout, generation)).unwrap();
    frames(&w);
    w.close().unwrap();
    let dst = dir.join(name);
    std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
    std::fs::rename(&tmp, &dst).unwrap();
}

fn append_bytes(path: &Path, tail: &[u8]) {
    let mut b = std::fs::read(path).unwrap();
    b.extend_from_slice(tail);
    std::fs::write(path, b).unwrap();
}

fn generate(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    write_stripe(dir, "cf-x/wal-3.log", Layout::PerCf, 3, |w| {
        w.append_batch(&[put(b"a", b"v1", 1), kind(b"b", 2, KIND_DELETE)])
            .unwrap();
        w.append_batch(&[]).unwrap();
        w.append_batch(&[
            kind(b"c", 3, KIND_SINGLE_DELETE),
            RecordRef {
                ttl: 1_900_000_000_000_000_000,
                ..put(b"t", b"ttl", 4)
            },
        ])
        .unwrap();
        w.append_batch_envelope(
            ENVELOPE_SCHEMA_PER_CF,
            &[
                EnvelopeRecord::Point(RecordRef {
                    kind: KIND_MERGE,
                    ..put(b"m", b"+1", 5)
                }),
                EnvelopeRecord::Range(RangeRef {
                    start: b"d",
                    end: b"f",
                    seq: 6,
                }),
                EnvelopeRecord::Point(put(b"g", b"v7", 7)),
            ],
        )
        .unwrap();
    });
    // A torn tail: a frame header promising more than the file holds.
    append_bytes(&dir.join("cf-x/wal-3.log"), &[100, 0, 0, 0, 1, 2, 3, 4, 9, 9]);
    write_stripe(dir, "cf-x/wal-3.log.s2", Layout::PerCf, 3, |w| {
        w.append_batch(&[put(b"s", b"stripe2", 10)]).unwrap();
    });
    // A complete frame whose CRC fails: also a clean end of the stripe.
    append_bytes(
        &dir.join("cf-x/wal-3.log.s2"),
        &[3, 0, 0, 0, 0xde, 0xad, 0xbe, 0xef, 0, 1, 2],
    );

    write_stripe(dir, "unified-wal-7.log", Layout::Unified, 7, |w| {
        w.append_batch(&[put(&pk(CF_A, b"a"), b"ua", 20), put(&pk(CF_B, b"b"), b"ub", 21)])
            .unwrap();
        let (start, end) = (pk(CF_A, b"c"), pk(CF_A, b"e"));
        w.append_batch_envelope(
            ENVELOPE_SCHEMA_UNIFIED,
            &[
                EnvelopeRecord::Point(RecordRef {
                    kind: KIND_MERGE,
                    ..put(&pk(CF_B, b"m"), b"+2", 22)
                }),
                EnvelopeRecord::Range(RangeRef {
                    start: &start,
                    end: &end,
                    seq: 23,
                }),
            ],
        )
        .unwrap();
        // Committed (by its newest decision, in generation 8).
        w.append_prepare(
            ENVELOPE_SCHEMA_UNIFIED,
            &id(0xa),
            &[CF_A, CF_B],
            &[put(&pk(CF_A, b"pa"), b"A1", 0), kind(&pk(CF_B, b"pb"), 0, KIND_DELETE)],
        )
        .unwrap();
        w.append_decision(ENVELOPE_SCHEMA_UNIFIED, &id(0xa), Some((100, 2)))
            .unwrap();
        // Aborted.
        w.append_prepare(
            ENVELOPE_SCHEMA_UNIFIED,
            &id(0xb),
            &[CF_A],
            &[put(&pk(CF_A, b"pb"), b"B1", 0)],
        )
        .unwrap();
        w.append_decision(ENVELOPE_SCHEMA_UNIFIED, &id(0xb), None)
            .unwrap();
        // Undecided.
        w.append_prepare(
            ENVELOPE_SCHEMA_UNIFIED,
            &id(0xc),
            &[CF_B],
            &[RecordRef {
                ttl: 1_900_000_000_000_000_000,
                ..put(&pk(CF_B, b"pc"), b"C1", 0)
            }],
        )
        .unwrap();
        // Its decision is in stripe 1.
        w.append_prepare(
            ENVELOPE_SCHEMA_UNIFIED,
            &id(0xe),
            &[CF_A],
            &[RecordRef {
                kind: KIND_MERGE,
                ..put(&pk(CF_A, b"pe"), b"+3", 0)
            }],
        )
        .unwrap();
        // A decision whose prepare was retired: watermark only.
        w.append_decision(ENVELOPE_SCHEMA_UNIFIED, &id(0xd), Some((200, 3)))
            .unwrap();
    });
    write_stripe(dir, "unified-wal-7.log.s1", Layout::Unified, 7, |w| {
        w.append_decision(ENVELOPE_SCHEMA_UNIFIED, &id(0xe), Some((150, 1)))
            .unwrap();
        w.append_batch(&[put(&pk(CF_A, b"z"), b"uz", 24)]).unwrap();
    });
    write_stripe(dir, "unified-wal-8.log", Layout::Unified, 8, |w| {
        // The retried commit of 0xa: the newer generation's decision wins.
        w.append_decision(ENVELOPE_SCHEMA_UNIFIED, &id(0xa), Some((300, 2)))
            .unwrap();
        w.append_batch(&[put(&pk(CF_B, b"y"), b"uy", 25)]).unwrap();
    });
    std::fs::write(dir.join("EXPECTED"), dump(dir)).unwrap();
}

fn hex(b: &[u8]) -> String {
    b.iter().fold(String::new(), |mut s, x| {
        let _ = write!(s, "{x:02x}");
        s
    })
}

/// Each segment's records as `Wal::replay` hands them over, then the
/// high-water mark it returns. Each stripe file is replayed alone (through a
/// copy at a stripe-0 path), so the dump says which file holds what.
fn dump(dir: &Path) -> String {
    let mut s = String::new();
    let tmp = tempfile::tempdir().unwrap();
    for (name, layout, generation) in SEGMENTS {
        let alone = tmp.path().join("alone.log");
        std::fs::copy(dir.join(name), &alone).unwrap();
        let _ = writeln!(s, "segment {name}");
        let last = Wal::replay(&alone, seg(*layout, *generation), |rec| {
            let _ = match rec {
                ReplayRecord::Point(r) => writeln!(
                    s,
                    "  point kind {} seq {} ttl {} key {} value {}",
                    r.kind,
                    r.seq,
                    r.ttl,
                    hex(&r.key),
                    hex(&r.value)
                ),
                ReplayRecord::RangeDelete { start, end, seq } => {
                    writeln!(s, "  range seq {seq} start {} end {}", hex(&start), hex(&end))
                }
                ReplayRecord::Prepare {
                    id,
                    cf_ids,
                    records,
                } => {
                    let ids: Vec<String> = cf_ids.iter().map(|c| format!("{c:x}")).collect();
                    let _ = writeln!(s, "  prepare id {} cfs {}", hex(&id), ids.join(","));
                    for r in records {
                        let _ = writeln!(
                            s,
                            "    record kind {} ttl {} key {} value {}",
                            r.kind,
                            r.ttl,
                            hex(&r.key),
                            hex(&r.value)
                        );
                    }
                    Ok(())
                }
                ReplayRecord::Decision { id, commit } => match commit {
                    Some((seq, count)) => {
                        writeln!(s, "  commit id {} seq {seq} count {count}", hex(&id))
                    }
                    None => writeln!(s, "  abort id {}", hex(&id)),
                },
            };
            Ok(())
        })
        .unwrap();
        let _ = writeln!(s, "  last_seq {last}");
        std::fs::remove_file(&alone).unwrap();
    }
    s
}

fn files() -> Vec<String> {
    let mut v: Vec<String> = SEGMENTS.iter().map(|(n, _, _)| n.to_string()).collect();
    v.push("EXPECTED".into());
    v
}

#[test]
#[ignore = "regenerates committed fixtures; run manually for a reviewed format change"]
fn regenerate_wal_corpus() {
    let tmp = tempfile::tempdir().unwrap();
    generate(tmp.path());
    for name in files() {
        let dst = fixture_dir().join(&name);
        std::fs::create_dir_all(dst.parent().unwrap()).unwrap();
        std::fs::copy(tmp.path().join(&name), dst).unwrap();
    }
}

/// The live encoder reproduces every committed byte, and replay the dump.
#[test]
fn the_wal_corpus_is_reproduced() {
    let tmp = tempfile::tempdir().unwrap();
    generate(tmp.path());
    for name in files() {
        let produced = std::fs::read(tmp.path().join(&name)).unwrap();
        let committed = std::fs::read(fixture_dir().join(&name)).unwrap();
        assert!(
            produced == committed,
            "{name} moved; if this is a deliberate format change, run \
             `cargo test --test epoch1_wal_corpus -- --ignored`"
        );
    }
}

/// The committed segments replay to the committed dump, and the stripes of
/// one generation replay together through the stripe paths.
#[test]
fn the_committed_segments_replay_as_documented() {
    let dir = fixture_dir();
    assert_eq!(
        std::fs::read_to_string(dir.join("EXPECTED")).unwrap(),
        dump(&dir)
    );
    let mut n = 0;
    let last = Wal::replay(dir.join("cf-x/wal-3.log"), SegmentId::per_cf(3), |_| {
        n += 1;
        Ok(())
    })
    .unwrap();
    // Seven records in stripe 0 (the empty frame holds none), one in .s2.
    assert_eq!((n, last), (8, 10));
    let mut decisions = 0;
    let last = Wal::replay(dir.join("unified-wal-7.log"), SegmentId::unified(7), |r| {
        decisions += usize::from(matches!(r, ReplayRecord::Decision { .. }));
        Ok(())
    })
    .unwrap();
    // Control frames raise nothing: the newest data record is seq 24.
    assert_eq!((decisions, last), (4, 24));
}
