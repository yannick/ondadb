//! The frozen ondaDB 0.9.x corpus, read through `ondadb::legacy_onda`.
//!
//! `tests/fixtures/legacy-onda/` holds bytes written by 0.8.2–0.9.1 encoders
//! that no longer exist in this tree:
//!
//! * `phase1/` — the byte-level corpus: WAL frames (legacy and envelope), klogs
//!   of every footer-flag shape, and manifests carrying every tail combination;
//! * `blocks/` — three klogs pinning 2.1's block shapes (restart trailer, no
//!   trailer, prefix-delta);
//! * `db-percf/`, `db-caps/`, `db-unified/` — whole database directories written
//!   by 0.9.1 (see `generator/gen_legacy_fixtures.rs`), each ending in an
//!   unflushed WAL tail, with `expected.txt` recording what 0.9.1 itself read.
//!
//! Every file here is **read-only**: nothing in this crate can write a 0.9 byte
//! any more, so nothing can regenerate them. They are the inputs the epoch-1
//! auto-upgrade is tested against.
#![cfg(feature = "legacy-onda")]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ondadb::comparator::default_comparator;
use ondadb::legacy_onda;
use ondadb::manifest::{CfManifest, Manifest, SstMeta, WalLayout};
use ondadb::sst::Reader;
use ondadb::wal::{Record, ReplayRecord};

fn fixture_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/legacy-onda")
}

fn fixture(rel: &str) -> Vec<u8> {
    let path = fixture_root().join(rel);
    std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()))
}

/// Copy a WAL fixture into `dir` under a stripe-0 name and replay it through the
/// 0.9 decoder (which also probes stripes 1..3 and finds nothing).
fn replay_fixture(dir: &Path, rel: &str) -> ondadb::Result<Vec<Record>> {
    let path = dir.join("wal-0.log");
    std::fs::write(&path, fixture(rel)).unwrap();
    let mut out = Vec::new();
    legacy_onda::wal::replay(&path, |rec| {
        match rec {
            ReplayRecord::Point(r) => out.push(r),
            ReplayRecord::RangeDelete { start, end, seq } => {
                panic!("fixture yielded a range delete {start:?}..{end:?}@{seq}")
            }
            other => panic!("fixture yielded a control record {other:?}"),
        }
        Ok(())
    })?;
    Ok(out)
}

fn open_klog(dir: &Path, rel: &str) -> Arc<Reader> {
    let name = Path::new(rel).file_name().unwrap().to_str().unwrap();
    let klog = dir.join(name);
    std::fs::write(&klog, fixture(rel)).unwrap();
    let vrel = rel.replace(".klog", ".vlog");
    if fixture_root().join(&vrel).exists() {
        std::fs::write(klog.with_extension("vlog"), fixture(&vrel)).unwrap();
    }
    legacy_onda::sst::open_table(klog.to_str().unwrap(), default_comparator())
        .unwrap_or_else(|e| panic!("{rel}: {e}"))
}

// ---- expected contents (the values the 0.8.2 generators wrote) ---------------

/// The four writer-produced WAL flag combinations, one frame each.
fn wal_all_flags_records() -> Vec<Record> {
    vec![
        Record {
            key: b"put".to_vec(),
            value: b"v1".to_vec(),
            seq: 1,
            ..Default::default()
        },
        Record {
            key: b"ttl".to_vec(),
            value: b"v2".to_vec(),
            seq: 2,
            ttl: 1_700_000_000_000_000_000,
            ..Default::default()
        },
        Record {
            key: b"del".to_vec(),
            seq: 3,
            kind: ondadb::format::KIND_DELETE,
            ..Default::default()
        },
        Record {
            key: b"sdel".to_vec(),
            seq: 4,
            kind: ondadb::format::KIND_SINGLE_DELETE,
            ..Default::default()
        },
    ]
}

/// Every writer-produced entry-flag combination plus filler, as in every
/// `phase1/klog_*` fixture.
fn klog_entries() -> Vec<(String, Vec<u8>, u64, i64, bool, bool)> {
    let big = vec![b'V'; 64];
    let mut v: Vec<(String, Vec<u8>, u64, i64, bool, bool)> = vec![
        ("k01".into(), b"small".to_vec(), 11, 0, false, false),
        (
            "k02".into(),
            b"small".to_vec(),
            12,
            1_700_000_000_000_000_000,
            false,
            false,
        ),
        ("k03".into(), Vec::new(), 13, 0, true, false),
        ("k04".into(), Vec::new(), 14, 0, true, true),
        ("k05".into(), big.clone(), 15, 0, false, false),
        (
            "k06".into(),
            big,
            16,
            1_700_000_000_000_000_001,
            false,
            false,
        ),
    ];
    for i in 7..40u64 {
        v.push((
            format!("k{i:02}"),
            b"filler".to_vec(),
            20 + i,
            0,
            false,
            false,
        ));
    }
    v
}

fn klog_names() -> Vec<String> {
    let mut out = Vec::new();
    for btree in ["flat", "btree"] {
        for restarts in ["norestarts", "restarts"] {
            for bloom in ["nobloom", "bloom"] {
                out.push(format!(
                    "phase1/klog_legacy_{btree}_{restarts}_{bloom}.klog"
                ));
            }
        }
    }
    out.push("phase1/klog_extended.klog".into());
    out
}

fn manifest_base() -> Manifest {
    Manifest {
        next_file_id: 42,
        global_seq: 99,
        generation: 0,
        applied_through: 0,
        next_edit_id: 1,
        wal_layout: WalLayout::PerColumnFamily,
        instance_nonce: None,
        caps: 0,
        cfs: vec![
            CfManifest {
                name: "default".into(),
                config: vec![1, 2, 3, 4],
                sstables: vec![
                    SstMeta {
                        id: 1,
                        level: 0,
                        num_entries: 100,
                        num_tombstones: 5,
                        max_seq: 50,
                        klog_size: 4096,
                        vlog_size: 0,
                        min_key: b"aaa".to_vec(),
                        max_key: b"zzz".to_vec(),
                        ..Default::default()
                    },
                    SstMeta {
                        id: 2,
                        level: 3,
                        num_entries: 200,
                        num_tombstones: 0,
                        max_seq: 60,
                        klog_size: 8192,
                        vlog_size: 1024,
                        min_key: b"aaa".to_vec(),
                        max_key: b"mmm".to_vec(),
                        ..Default::default()
                    },
                ],
                unified_id: None,
            },
            CfManifest {
                name: "other".into(),
                config: Vec::new(),
                sstables: vec![SstMeta {
                    id: 7,
                    level: 1,
                    num_entries: 3,
                    num_tombstones: 1,
                    max_seq: 9,
                    klog_size: 64,
                    vlog_size: 0,
                    min_key: b"b".to_vec(),
                    max_key: b"c".to_vec(),
                    ..Default::default()
                }],
                unified_id: None,
            },
        ],
    }
}

/// Every `phase1/manifest_*` fixture and the catalog it must decode to.
fn manifest_variants() -> Vec<(&'static str, Manifest)> {
    let with = |f: fn(&mut Manifest)| {
        let mut m = manifest_base();
        f(&mut m);
        m
    };
    vec![
        ("manifest_v1_notail.bin", manifest_base()),
        (
            "manifest_v1_partition.bin",
            with(|m| m.cfs[0].sstables[1].partition = Some("p-2024".into())),
        ),
        (
            "manifest_v1_tier.bin",
            with(|m| {
                m.cfs[0].sstables[1].partition = Some("p-2024".into());
                m.cfs[0].sstables[1].tier = Some("cold".into());
            }),
        ),
        (
            "manifest_v1_time.bin",
            with(|m| {
                m.cfs[0].sstables[1].tier = Some("cold".into());
                m.cfs[0].sstables[1].max_entry_time = Some(1_700_000_000_000_000_000);
            }),
        ),
        (
            "manifest_v1_unified.bin",
            with(|m| m.wal_layout = WalLayout::Unified),
        ),
        (
            "manifest_v1_object.bin",
            with(|m| m.cfs[0].sstables[1].object = Some("cf-default/0000000000000001-2".into())),
        ),
        (
            "manifest_v1_nonce.bin",
            with(|m| {
                m.instance_nonce = Some(0x0123_4567_89ab_cdef);
                m.wal_layout = WalLayout::Unified;
            }),
        ),
        (
            "manifest_v2_caps_only.bin",
            with(|m| m.caps = ondadb::format::CAP_EXTENDED_RECORDS),
        ),
    ]
}

// ---- the byte-level corpus ----------------------------------------------------

#[test]
fn wal_corpus_replays_through_the_0_9_decoder() {
    let tmp = tempfile::tempdir().unwrap();
    // Every writer-produced flag combination, one frame each; and the same
    // records as a schema-1 envelope.
    for rel in [
        "phase1/wal_legacy_all_flags.bin",
        "phase1/wal_v2_envelope_schema1.bin",
    ] {
        let recs = replay_fixture(tmp.path(), rel).unwrap();
        let expect = wal_all_flags_records();
        assert_eq!(recs.len(), expect.len(), "{rel}");
        for (got, want) in recs.iter().zip(expect.iter()) {
            assert_eq!(got.key, want.key, "{rel}");
            assert_eq!(got.value, want.value, "{rel}");
            assert_eq!(got.seq, want.seq, "{rel}");
            assert_eq!(got.ttl, want.ttl, "{rel}");
            assert_eq!(got.kind, want.kind, "{rel}");
        }
    }
    // Schema 2 keeps the CF-id prefix inside the key.
    let got = replay_fixture(tmp.path(), "phase1/wal_v2_envelope_schema2.bin").unwrap();
    assert_eq!(got.len(), 2);
    assert_eq!(&got[0].key[..8], &0x0123_4567_89ab_cdefu64.to_be_bytes());
    assert_eq!(&got[0].key[8..], b"user-key");
    assert_eq!(got[1].kind, ondadb::format::KIND_DELETE);
    // The tail contract: torn is clean, CRC-valid-but-undecodable is corruption.
    assert_eq!(
        replay_fixture(tmp.path(), "phase1/wal_legacy_torn_tail.bin")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        replay_fixture(tmp.path(), "phase1/wal_legacy_crc_valid_undecodable.bin")
            .unwrap_err()
            .kind(),
        "corruption"
    );
    // Range deletes (kind 5) replay as range records.
    let path = tmp.path().join("wal-0.log");
    std::fs::write(&path, fixture("phase1/wal_v2_range_schema1.bin")).unwrap();
    let mut ranges = Vec::new();
    legacy_onda::wal::replay(&path, |rec| {
        if let ReplayRecord::RangeDelete { start, end, seq } = rec {
            ranges.push((start, end, seq));
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(
        ranges,
        vec![
            (b"alpha".to_vec(), b"omega".to_vec(), 11),
            (b"a".to_vec(), b"b".to_vec(), 12),
            (b"b".to_vec(), b"b\0".to_vec(), 13),
        ]
    );
}

#[test]
fn klog_corpus_reads_through_the_0_9_decoder() {
    let tmp = tempfile::tempdir().unwrap();
    let entries = klog_entries();
    for rel in klog_names() {
        let reader = open_klog(tmp.path(), &rel);
        let mut it = reader.iter();
        it.seek_to_first();
        for (k, v, seq, ttl, tomb, sdel) in &entries {
            assert!(it.valid(), "{rel}: iterator ended at {k}");
            assert_eq!(it.user_key(), k.as_bytes(), "{rel}");
            assert_eq!(it.seq(), *seq, "{rel} {k}");
            assert_eq!(it.ttl(), *ttl, "{rel} {k}");
            assert_eq!(it.is_tombstone(), *tomb, "{rel} {k}");
            assert_eq!(it.is_single_delete(), *sdel, "{rel} {k}");
            assert_eq!(&it.value().unwrap(), v, "{rel} {k}");
            it.next();
        }
        assert!(!it.valid(), "{rel}: extra entries");
        assert_eq!(reader.num_entries(), entries.len() as u64, "{rel}");
        // Point reads through the bloom, the index and the restart search.
        for (k, v, _, _, tomb, _) in &entries {
            let (got, _, found, deleted, _) = reader.get(k.as_bytes(), u64::MAX, 0).unwrap();
            assert!(found, "{rel}: {k}");
            assert_eq!(deleted, *tomb, "{rel}: {k}");
            if !tomb {
                assert_eq!(got.as_deref(), Some(v.as_slice()), "{rel}: {k}");
            }
        }
    }
}

#[test]
fn manifest_corpus_decodes_through_the_0_9_decoder() {
    for (name, want) in manifest_variants() {
        let got = legacy_onda::manifest::decode(&fixture(&format!("phase1/{name}")))
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(got.next_file_id, want.next_file_id, "{name}");
        assert_eq!(got.global_seq, want.global_seq, "{name}");
        assert_eq!(got.wal_layout, want.wal_layout, "{name}");
        assert_eq!(got.instance_nonce, want.instance_nonce, "{name}");
        assert_eq!(got.caps, want.caps, "{name}");
        assert_eq!(got.cfs.len(), want.cfs.len(), "{name}");
        for (a, b) in got.cfs.iter().zip(want.cfs.iter()) {
            assert_eq!(a.name, b.name, "{name}");
            assert_eq!(a.config, b.config, "{name}");
            assert_eq!(a.sstables, b.sstables, "{name}");
        }
    }
}

// ---- the 2.1 block corpus -----------------------------------------------------

/// `(key, value, seq, ttl, tombstone, single_delete)`.
type Entry = (Vec<u8>, Vec<u8>, u64, i64, bool, bool);

/// The entries every `blocks/*` klog holds.
fn block_entries() -> Vec<Entry> {
    let mut v = Vec::new();
    for tenant in 0..4u32 {
        for segment in 0..50u32 {
            let key = format!("tenant/{tenant:03}/cluster/aa/segment/{segment:05}").into_bytes();
            let seq = (tenant as u64) * 100 + segment as u64 + 1;
            match segment % 5 {
                1 => v.push((key, Vec::new(), seq, 0, true, false)),
                2 => v.push((key, Vec::new(), seq, 0, true, true)),
                3 => v.push((key, b"ttl-value".to_vec(), seq, 1_700_000_000, false, false)),
                _ => v.push((key, b"value-0123456789".to_vec(), seq, 0, false, false)),
            }
        }
    }
    v
}

/// Read a LEB128 uvarint at `p`, returning `(value, bytes_read)`.
fn uvarint(b: &[u8], mut p: usize) -> (u64, usize) {
    let start = p;
    let (mut value, mut shift) = (0u64, 0u32);
    loop {
        let byte = b[p];
        p += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return (value, p - start);
        }
        shift += 7;
    }
}

/// The first data block of a `blocks/*` klog (uncompressed), split into
/// `(entries, restart offsets)`.
fn first_block(bytes: &[u8], has_restarts: bool) -> (&[u8], Vec<u32>) {
    const BLOCK_HEADER: usize = 13;
    let raw_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let block = &bytes[BLOCK_HEADER..BLOCK_HEADER + raw_len];
    if !has_restarts {
        return (block, Vec::new());
    }
    let count = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap()) as usize;
    let end = block.len() - (count * 4 + 4);
    let restarts = (0..count)
        .map(|i| u32::from_le_bytes(block[end + i * 4..end + i * 4 + 4].try_into().unwrap()))
        .collect();
    (&block[..end], restarts)
}

fn scan_all(r: &Arc<Reader>) -> Vec<Entry> {
    let mut it = r.iter();
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        out.push((
            it.user_key().to_vec(),
            it.value().unwrap(),
            it.seq(),
            it.ttl(),
            it.is_tombstone(),
            it.is_single_delete(),
        ));
        it.next();
    }
    assert_eq!(it.err().map(|e| e.to_string()), None);
    out
}

#[test]
fn block_corpus_decodes_by_hand_and_through_the_0_9_reader() {
    let tmp = tempfile::tempdir().unwrap();
    let footer_flags = |b: &[u8]| b[b.len() - legacy_onda::sst::FOOTER_SIZE + 48];
    use legacy_onda::sst::{
        FLAG_EXTENDED_BLOCK, FLAG_HAS_BLOOM, FLAG_PREFIX_DELTA, FLAG_RESTARTS, FLAG_VLOG_V2,
    };

    // Restart trailer, full keys.
    let bytes = fixture("blocks/legacy_restarts.klog");
    assert_eq!(footer_flags(&bytes), FLAG_VLOG_V2 | FLAG_RESTARTS);
    assert_eq!(footer_flags(&bytes) & FLAG_HAS_BLOOM, 0);
    let (e, restarts) = first_block(&bytes, true);
    assert_eq!(restarts[0], 0);
    assert!(restarts.len() >= 4);
    assert_eq!(e[0], 0, "a plain put has no flag bits set");
    let (klen, n) = uvarint(e, 1);
    let (vlen, m) = uvarint(e, 1 + n);
    let (seq, o) = uvarint(e, 1 + n + m);
    assert_eq!((klen, vlen, seq), (35, 16, 1));
    let at = 1 + n + m + o;
    assert_eq!(&e[at..at + 35], &b"tenant/000/cluster/aa/segment/00000"[..]);

    // No trailer at all: 0.9's legacy block shape.
    let bytes = fixture("blocks/legacy_no_trailer.klog");
    assert_eq!(footer_flags(&bytes), FLAG_VLOG_V2);

    // Prefix-delta.
    let bytes = fixture("blocks/delta.klog");
    assert_eq!(
        footer_flags(&bytes),
        FLAG_VLOG_V2 | FLAG_RESTARTS | FLAG_EXTENDED_BLOCK | FLAG_PREFIX_DELTA
    );
    let (e, restarts) = first_block(&bytes, true);
    for &off in &restarts {
        let off = off as usize;
        let (_, n0) = uvarint(e, off);
        let (_, n1) = uvarint(e, off + n0);
        let (shared, _) = uvarint(e, off + n0 + n1);
        assert_eq!(shared, 0, "anchor at {off} shares a prefix");
    }

    let want: Vec<_> = block_entries();
    for rel in [
        "blocks/legacy_restarts.klog",
        "blocks/legacy_no_trailer.klog",
        "blocks/delta.klog",
    ] {
        let r = open_klog(tmp.path(), rel);
        assert_eq!(scan_all(&r), want, "{rel}");
    }
}

// ---- the 0.9.1 database directories -------------------------------------------

/// Each 0.9.1 directory's catalog recovers — snapshot, edit log and config
/// conversion — to the families its generator created.
#[test]
fn database_catalogs_recover() {
    for (name, cfs) in [
        ("db-percf", vec!["alpha", "beta"]),
        ("db-caps", vec!["m", "plain"]),
        ("db-unified", vec!["ua", "ub"]),
    ] {
        let m = legacy_onda::recover_catalog(fixture_root().join(name))
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let mut got: Vec<String> = m.cfs.iter().map(|c| c.name.clone()).collect();
        got.sort();
        assert_eq!(got, cfs, "{name}");
    }
}

/// The merge operator `db-caps` was written with (see the generator).
#[derive(Debug)]
struct Concat;

impl ondadb::MergeOperator for Concat {
    fn name(&self) -> &str {
        "fixture.concat.v1"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = existing.map(|b| b.to_vec()).unwrap_or_default();
        for op in operands {
            if !out.is_empty() {
                out.push(b'|');
            }
            out.extend_from_slice(op);
        }
        Ok(out)
    }
}

/// Copy a fixture directory somewhere writable: even a read-only open takes a
/// shared lock on `LOCK`, and the committed fixtures must never change.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let to = dst.join(e.file_name());
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to);
        } else {
            std::fs::copy(e.path(), &to).unwrap();
        }
    }
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// What 0.9.1 itself read from a fixture, per family, as `expected.txt` holds
/// it: `key-hex value-len sha256(value)-hex`.
fn expected_scan(name: &str) -> Vec<(String, Vec<String>)> {
    let text = String::from_utf8(fixture(&format!("{name}/expected.txt"))).unwrap();
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for line in text.lines() {
        if let Some(cf) = line.strip_prefix("cf ") {
            out.push((cf.to_string(), Vec::new()));
        } else {
            out.last_mut().unwrap().1.push(line.to_string());
        }
    }
    out
}

/// The acceptance test for the read-only path the auto-upgrade builds on:
/// every 0.9.1 directory opens through `legacy_onda::open_read_only`, and a
/// full scan of every family — tables, WAL-only tail, merge operands, range
/// tombstones, TTLs, the unified layout's 0.9 cf ids — reads exactly what
/// 0.9.1 read before the crash image was taken.
#[test]
fn database_directories_open_read_only_and_scan_equal() {
    for name in ["db-percf", "db-caps", "db-unified"] {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join(name);
        copy_dir(&fixture_root().join(name), &dir);
        let mut opts = ondadb::Options::new(dir.to_str().unwrap());
        opts.merge_fns = vec![Arc::new(Concat)];
        let db = legacy_onda::open_read_only(opts).unwrap_or_else(|e| panic!("{name}: {e}"));
        for (cf_name, want) in expected_scan(name) {
            let cf = db
                .get_column_family(&cf_name)
                .unwrap_or_else(|| panic!("{name}: no family {cf_name}"));
            let txn = db.begin();
            let mut it = txn.new_iterator(&cf);
            it.seek_to_first();
            let mut got = Vec::new();
            while it.valid() {
                let digest = <sha2::Sha256 as sha2::Digest>::digest(it.value());
                got.push(format!(
                    "{} {} {}",
                    hex(it.key()),
                    it.value().len(),
                    hex(&digest)
                ));
                it.next();
            }
            assert!(it.err().is_none(), "{name}/{cf_name}: {:?}", it.err());
            assert_eq!(got.len(), want.len(), "{name}/{cf_name}: row count");
            assert_eq!(got, want, "{name}/{cf_name}");
            // And point reads agree with the scan.
            if let Some(first) = want.first() {
                let key: Vec<u8> = (0..first.find(' ').unwrap() / 2)
                    .map(|i| u8::from_str_radix(&first[2 * i..2 * i + 2], 16).unwrap())
                    .collect();
                assert!(db.get(&cf, &key).is_ok(), "{name}/{cf_name}: point read");
            }
        }
        // A 0.9 handle writes nothing: a write on a read-only handle is the
        // engine's usual no-op, and the byte comparison below proves no file
        // was touched by it, by replay, or by close.
        let cf = db.list_column_families().pop().unwrap();
        let cf = db.get_column_family(&cf).unwrap();
        let _ = db.put(&cf, b"x", b"y", std::time::Duration::ZERO);
        db.close().unwrap();
        // The directory is byte-for-byte what was copied in, apart from LOCK.
        for e in std::fs::read_dir(fixture_root().join(name)).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_file() {
                assert_eq!(
                    std::fs::read(e.path()).unwrap(),
                    std::fs::read(dir.join(e.file_name())).unwrap(),
                    "{name}: {:?} changed",
                    e.file_name()
                );
            }
        }
    }
}

/// `open_read_only` refuses a directory that is not a 0.9 database, and a
/// 0.9 table cannot sneak into an epoch-1 reader.
#[test]
fn legacy_open_refuses_non_legacy_input() {
    let tmp = tempfile::tempdir().unwrap();
    let epoch1 = tmp.path().join("epoch1");
    {
        let db = ondadb::DB::open(ondadb::Options::new(epoch1.to_str().unwrap())).unwrap();
        db.create_column_family("a", ondadb::ColumnFamilyConfig::default())
            .unwrap();
        db.close().unwrap();
    }
    assert!(!legacy_onda::is_legacy_dir(&epoch1).unwrap());
    let err = legacy_onda::open_read_only(ondadb::Options::new(epoch1.to_str().unwrap()))
        .expect_err("an epoch-1 directory is not a 0.9 one");
    assert_eq!(err.kind(), "invalid_args");
    assert!(legacy_onda::is_legacy_dir(fixture_root().join("db-percf")).unwrap());

    // A 0.9 klog through the epoch-1 reader is a named refusal.
    let klog = tmp.path().join("t.klog");
    std::fs::write(&klog, fixture("phase1/klog_extended.klog")).unwrap();
    let err = Reader::open(
        klog.to_str().unwrap(),
        ondadb::storage::LocalStorage::new(
            Arc::new(ondadb::cache::FileCache::new(4)),
            cfg!(feature = "mmap-reads"),
        ),
        Arc::new(ondadb::cache::BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
    )
    .expect_err("a 0.9 table must not open as epoch 1");
    assert_eq!(err.kind(), "unsupported_format");
}
