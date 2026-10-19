//! The frozen yoloDB epoch-1 corpus: what this binary writes, byte for byte.
//!
//! `tests/fixtures/epoch1/` is committed to git. Each test below regenerates
//! its artifact into a temp dir with the live encoder and requires it to be
//! **identical** to the committed bytes — so a format change has to be a
//! deliberate, reviewed regeneration (`cargo test --test epoch1_golden --
//! --ignored`) rather than a silent shift — and then decodes the committed
//! bytes **by hand**, from the layout documented in `docs/formats.md` and
//! `src/format.rs`, rather than through the production decoder: a golden test
//! that used the decoder would only prove encoder and decoder agree with each
//! other.
//!
//! The 0.9 corpus these replace lives in `tests/fixtures/legacy-onda/` and is
//! read only through `legacy_onda` (`tests/legacy_onda.rs`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::{Compression, SyncMode};
use ondadb::encoding::checksum;
use ondadb::manifest::{CfManifest, Manifest, SstMeta, WalLayout};
use ondadb::manifest_edit::{EditLog, EditLogHeader, Op, VersionEdit};
use ondadb::range_tombstone::Fragment;
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;
use ondadb::wal::{Record, SegmentId, Wal};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/epoch1")
}

fn fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()))
}

fn u32_at(b: &[u8], o: usize) -> u32 {
    u32::from_le_bytes(b[o..o + 4].try_into().unwrap())
}

fn u64_at(b: &[u8], o: usize) -> u64 {
    u64::from_le_bytes(b[o..o + 8].try_into().unwrap())
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

// ---- generators ---------------------------------------------------------------

/// `(key, value, seq, ttl, kind)`.
type Entry = (Vec<u8>, Vec<u8>, u64, i64, u64);

/// Prefix-heavy keys and every point kind, spanning several 1 KiB blocks.
fn entries() -> Vec<Entry> {
    let mut v = Vec::new();
    for tenant in 0..3u32 {
        for segment in 0..40u32 {
            let key = format!("tenant/{tenant:03}/segment/{segment:05}").into_bytes();
            let seq = u64::from(tenant) * 100 + u64::from(segment) + 1;
            let (value, ttl, kind) = match segment % 5 {
                1 => (Vec::new(), 0, ondadb::format::KIND_DELETE),
                2 => (Vec::new(), 0, ondadb::format::KIND_SINGLE_DELETE),
                3 => (
                    b"ttl-value".to_vec(),
                    1_700_000_000_000_000_000,
                    ondadb::format::KIND_PUT,
                ),
                4 if segment % 10 == 4 => (vec![b'V'; 80], 0, ondadb::format::KIND_PUT),
                _ => (b"value-0123456789".to_vec(), 0, ondadb::format::KIND_PUT),
            };
            v.push((key, value, seq, ttl, kind));
        }
    }
    v
}

fn options(extended: bool, prefix_delta: bool, bloom: bool) -> WriterOptions {
    WriterOptions {
        // No compression: the corpus must not move when a codec dependency
        // changes its output.
        compression: Compression::None,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: bloom,
        bloom_fpr: bloom.then_some(0.01),
        klog_value_threshold: 64, // the 80-byte values go to the vlog
        block_size: 1024,
        expected_entries: 128,
        use_btree: false,
        restart_interval: 8,
        extended_entries: extended,
        prefix_delta,
    }
}

fn klog_variants() -> Vec<(&'static str, WriterOptions, Vec<Fragment>)> {
    vec![
        ("base.klog", options(false, false, true), Vec::new()),
        ("delta.klog", options(true, true, false), Vec::new()),
        (
            "ranges.klog",
            options(true, false, false),
            vec![
                Fragment {
                    start: b"tenant/000/segment/00010".to_vec(),
                    end: b"tenant/000/segment/00020".to_vec(),
                    seqs: vec![900, 12],
                },
                Fragment {
                    start: b"tenant/002/segment/00000".to_vec(),
                    end: b"tenant/002/segment/00005".to_vec(),
                    seqs: vec![950],
                },
            ],
        ),
    ]
}

fn write_klog(path: &Path, opts: WriterOptions, frags: Vec<Fragment>) {
    let mut w = Writer::new(path.to_str().unwrap(), opts).unwrap();
    if !frags.is_empty() {
        w.set_range_fragments(frags);
    }
    for (k, v, seq, ttl, kind) in entries() {
        w.add(&k, &v, seq, ttl, kind).unwrap();
    }
    w.finish().unwrap();
}

fn sample_manifest() -> Manifest {
    Manifest {
        next_file_id: 42,
        global_seq: 9_999,
        generation: 3,
        applied_through: 17,
        next_edit_id: 18,
        wal_layout: WalLayout::Unified,
        instance_nonce: Some(0x0123_4567_89ab_cdef),
        caps: ondadb::format::CAP_EXTENDED_RECORDS
            | ondadb::format::CAP_RANGE_DELETES
            | ondadb::format::CAP_MANIFEST_EDITS
            | ondadb::format::CAP_PERIODIC_AGE,
        cfs: vec![CfManifest {
            name: "photos".into(),
            config: ondadb::ColumnFamilyConfig {
                compression: Compression::Lz4,
                // Explicit: the corpus predates the graduated default (P10)
                // and pins the config bytes, which must not move with it.
                compression_per_level: Vec::new(),
                merge_operator_name: Some("counter.v1".into()),
                sync_mode: SyncMode::Full,
                ..Default::default()
            }
            .encode(),
            sstables: vec![SstMeta {
                id: 7,
                level: 6,
                num_entries: 100,
                num_tombstones: 5,
                max_seq: 9_000,
                klog_size: 65_536,
                vlog_size: 1_024,
                min_key: b"a".to_vec(),
                max_key: b"z".to_vec(),
                partition: Some("p1".into()),
                tier: Some("cold".into()),
                object: Some("cf-photos/0123456789abcdef-7".into()),
                max_entry_time: Some(1_700_000_000_000_000_000),
                last_compaction_time: Some(1_700_000_000_000_000_001),
                range_count: 2,
                range_min_seq: 10,
                range_max_seq: 20,
                range_min_key: Some(b"b".to_vec()),
                range_max_key: Some(b"y".to_vec()),
            }],
            unified_id: Some(0x1111_2222_3333_4444),
        }],
    }
}

fn wal_records() -> Vec<Record> {
    vec![
        Record {
            key: b"put".to_vec(),
            value: b"v1".to_vec(),
            seq: 1,
            ..Default::default()
        },
        Record {
            key: b"del".to_vec(),
            seq: 2,
            kind: ondadb::format::KIND_DELETE,
            ..Default::default()
        },
    ]
}

/// Write every artifact of the corpus into `dir`.
fn generate(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    for (name, opts, frags) in klog_variants() {
        write_klog(&dir.join(name), opts, frags);
    }
    // The manifest.
    sample_manifest().save(dir.join("MANIFEST")).unwrap();
    // The edit log: header, then two records.
    {
        let mut log = EditLog::create(
            dir,
            EditLogHeader {
                base_applied_through: 17,
                snapshot_generation: 3,
            },
        )
        .unwrap();
        log.append(18, &VersionEdit::new(vec![Op::SetGlobalSeq(10_000)]))
            .unwrap();
        log.append(19, &VersionEdit::new(vec![Op::SetNextFileId(43)]))
            .unwrap();
    }
    // One WAL segment (SyncMode::Full: one stripe file).
    {
        let wal = Wal::open(
            dir.join("wal-5.log"),
            SyncMode::Full,
            Duration::ZERO,
            SegmentId::per_cf(5),
        )
        .unwrap();
        for r in wal_records() {
            wal.append(r).unwrap();
        }
        wal.close().unwrap();
    }
}

/// Every file of the corpus, by name.
const FILES: &[&str] = &[
    "base.klog",
    "base.vlog",
    "delta.klog",
    "delta.vlog",
    "ranges.klog",
    "ranges.vlog",
    "MANIFEST",
    "MANIFEST-EDITS",
    "wal-5.log",
];

#[test]
#[ignore = "regenerates committed fixtures; run manually for a reviewed format change"]
fn regenerate_epoch1_fixtures() {
    let tmp = tempfile::tempdir().unwrap();
    generate(tmp.path());
    std::fs::create_dir_all(fixture_dir()).unwrap();
    for name in FILES {
        std::fs::copy(tmp.path().join(name), fixture_dir().join(name)).unwrap();
    }
}

/// The live encoders reproduce every committed byte.
#[test]
fn the_encoders_reproduce_the_corpus() {
    let tmp = tempfile::tempdir().unwrap();
    generate(tmp.path());
    for name in FILES {
        let produced = std::fs::read(tmp.path().join(name)).unwrap();
        let committed = fixture(name);
        assert_eq!(
            produced.len(),
            committed.len(),
            "{name}: length moved; if this is a deliberate format change, run \
             `cargo test --test epoch1_golden -- --ignored`"
        );
        let diff = produced.iter().zip(&committed).position(|(a, b)| a != b);
        assert_eq!(diff, None, "{name}: bytes diverge at offset {diff:?}");
    }
}

// ---- hand decoding --------------------------------------------------------------

/// The 96-byte footer of every committed klog, field by field.
#[test]
fn sst_footers_decode_by_hand() {
    for (name, caps, bloom) in [
        ("base.klog", 0x00u64, true),
        ("delta.klog", 0x09, false),  // EXTENDED | PREFIX_DELTA
        ("ranges.klog", 0x05, false), // EXTENDED | RANGE_DELETES
    ] {
        let b = fixture(name);
        let f = &b[b.len() - 96..];
        assert_eq!(&f[88..96], b"YOLOST01", "{name}");
        assert_eq!(u32_at(f, 52), 1, "{name}: format_version");
        assert_eq!(u32_at(f, 84), 0, "{name}: reserved");
        assert_eq!(
            u32_at(f, 80),
            checksum(&f[..80]),
            "{name}: crc32c over 0..80"
        );
        assert_eq!(u64_at(f, 56), caps, "{name}: capability word");
        assert_eq!(
            u32_at(f, 48),
            u32::from(bloom),
            "{name}: flags (bloom, no btree)"
        );
        assert_eq!(u64_at(f, 32), entries().len() as u64, "{name}: num_entries");
        assert_eq!(u64_at(f, 40), 240, "{name}: max_seq");
        let (bloom_off, bloom_len) = (u64_at(f, 16), u64_at(f, 24));
        assert_eq!(bloom_len > 0, bloom, "{name}: bloom handle");
        if bloom {
            // The bloom block: frame header, then `hash=1 | m | k | words`.
            let block = &b[bloom_off as usize..(bloom_off + bloom_len) as usize];
            assert_eq!(block[0], 0, "meta blocks are stored raw");
            assert_eq!(block[13], 1, "{name}: the xxh3 id leads the bloom block");
        }
        let (aux_off, aux_len) = (u64_at(f, 64), u64_at(f, 72));
        assert_eq!(aux_len > 0, name == "ranges.klog", "{name}: aux handle");
        let _ = aux_off;
        // The index block ends where the footer starts.
        assert_eq!(u64_at(f, 0) + u64_at(f, 8), (b.len() - 96) as u64, "{name}");
    }
}

/// The first data block of each layout: codec byte, restart trailer, and the
/// first entry, decoded by hand.
#[test]
fn data_blocks_decode_by_hand() {
    // Base layout: flags | klen | vlen | seq | key | value.
    let b = fixture("base.klog");
    assert_eq!(b[0], 0, "codec 0: stored raw");
    let raw_len = u32_at(&b, 5) as usize;
    assert_eq!(u32_at(&b, 9), checksum(&b[13..13 + u32_at(&b, 1) as usize]));
    let block = &b[13..13 + raw_len];
    let count = u32_at(block, block.len() - 4) as usize;
    assert!(count >= 2, "every block carries a restart trailer");
    let entries_end = block.len() - 4 - 4 * count;
    assert_eq!(
        u32_at(block, entries_end),
        0,
        "the first anchor is at offset 0"
    );
    let e = &block[..entries_end];
    assert_eq!(e[0], 0, "a plain put sets no flag bit");
    let (klen, n) = uvarint(e, 1);
    let (vlen, m) = uvarint(e, 1 + n);
    let (seq, o) = uvarint(e, 1 + n + m);
    assert_eq!((klen, vlen, seq), (24, 16, 1));
    let at = 1 + n + m + o;
    assert_eq!(&e[at..at + 24], b"tenant/000/segment/00000");

    // Prefix-delta layout: kind | mods | shared | suffix_len | vlen | seq.
    let b = fixture("delta.klog");
    let raw_len = u32_at(&b, 5) as usize;
    let block = &b[13..13 + raw_len];
    let count = u32_at(block, block.len() - 4) as usize;
    let e = &block[..block.len() - 4 - 4 * count];
    let (kind, a) = uvarint(e, 0);
    let (mods, c) = uvarint(e, a);
    let (shared, d) = uvarint(e, a + c);
    let (suffix, _) = uvarint(e, a + c + d);
    assert_eq!(
        (kind, mods, shared, suffix),
        (1, 0, 0, 24),
        "an anchor stores its whole key"
    );
}

/// Every vlog starts with the 32-byte header; the first frame follows it.
#[test]
fn vlog_headers_decode_by_hand() {
    for name in ["base.vlog", "delta.vlog", "ranges.vlog"] {
        let v = fixture(name);
        assert_eq!(&v[..8], b"YOLODBVL", "{name}");
        assert_eq!(u32_at(&v, 8), 1, "{name}: version");
        assert_eq!(u32_at(&v, 12), 0, "{name}: flags");
        assert_eq!(&v[16..28], &[0u8; 12], "{name}: reserved");
        assert_eq!(u32_at(&v, 28), checksum(&v[..28]), "{name}: crc32c");
        // First frame at 32: crc | codec | stored_len | stored.
        let stored_len = u32_at(&v, 37) as usize;
        assert_eq!(v[36], 0, "{name}: codec 0 (compression disabled)");
        assert_eq!(stored_len, 80, "{name}");
        assert_eq!(u32_at(&v, 32), checksum(&v[41..41 + stored_len]), "{name}");
        assert_eq!(&v[41..41 + stored_len], &[b'V'; 80][..], "{name}");
    }
}

/// The corpus reads back through the production reader.
#[test]
fn klogs_read_back() {
    let tmp = tempfile::tempdir().unwrap();
    for (name, _, frags) in klog_variants() {
        let klog = tmp.path().join(name);
        std::fs::write(&klog, fixture(name)).unwrap();
        std::fs::write(
            klog.with_extension("vlog"),
            fixture(&name.replace(".klog", ".vlog")),
        )
        .unwrap();
        let r = Reader::open(
            klog.to_str().unwrap(),
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            1,
            default_comparator(),
            0,
        )
        .unwrap();
        assert_eq!(r.range_fragments(), frags.as_slice(), "{name}");
        let mut it = r.iter();
        it.seek_to_first();
        for (k, v, seq, ttl, kind) in entries() {
            assert!(it.valid(), "{name}");
            assert_eq!(it.user_key(), k.as_slice(), "{name}");
            assert_eq!(it.seq(), seq, "{name}");
            assert_eq!(it.ttl(), ttl, "{name}");
            assert_eq!(
                it.is_tombstone(),
                kind != ondadb::format::KIND_PUT,
                "{name}"
            );
            assert_eq!(it.value().unwrap(), v, "{name}");
            it.next();
        }
        assert!(!it.valid(), "{name}: extra entries");
    }
}

/// The manifest header and trailer by hand, and the whole catalog back
/// through the decoder.
#[test]
fn manifest_decodes_by_hand_and_back() {
    let m = fixture("MANIFEST");
    assert_eq!(&m[..8], b"YOLODBMF");
    assert_eq!(u32_at(&m, 8), 1, "version");
    assert_eq!(u64_at(&m, 12), 0x35, "caps");
    assert_eq!(u32_at(&m, 20), 0x07, "db flags: unified | nonce | edit log");
    assert_eq!(u64_at(&m, 24), 42, "next_file_id");
    assert_eq!(u64_at(&m, 32), 9_999, "global_seq");
    assert_eq!(u64_at(&m, 40), 0x0123_4567_89ab_cdef, "nonce section");
    assert_eq!(
        (u64_at(&m, 48), u64_at(&m, 56), u64_at(&m, 64)),
        (3, 17, 18)
    );
    assert_eq!(m[72], 1, "cf_count");
    assert_eq!(m[73], 0x01, "cf flags: CF_UNIFIED_ID");
    let n = m.len();
    assert_eq!(u32_at(&m, n - 4), checksum(&m[..n - 4]));

    let tmp = tempfile::tempdir().unwrap();
    std::fs::write(tmp.path().join("MANIFEST"), &m).unwrap();
    let d = Manifest::load(tmp.path().join("MANIFEST")).unwrap();
    let want = sample_manifest();
    assert_eq!(d.cfs[0].sstables, want.cfs[0].sstables);
    assert_eq!(d.cfs[0].unified_id, want.cfs[0].unified_id);
    let cfg = ondadb::ColumnFamilyConfig::decode(&d.cfs[0].config).unwrap();
    assert_eq!(cfg.compression, Compression::Lz4);
    assert_eq!(cfg.merge_operator_name.as_deref(), Some("counter.v1"));
}

/// The edit-log header and record frames by hand.
#[test]
fn edit_log_decodes_by_hand() {
    let e = fixture("MANIFEST-EDITS");
    assert_eq!(&e[..8], b"YOLODBED");
    assert_eq!(u32_at(&e, 8), 1, "schema");
    assert_eq!(u64_at(&e, 12), 17, "base_applied_through");
    assert_eq!(u64_at(&e, 20), 3, "snapshot_generation");
    assert_eq!(u32_at(&e, 28), checksum(&e[..28]));
    let len = u32_at(&e, 32) as usize;
    let payload = &e[40..40 + len];
    assert_eq!(u32_at(&e, 36), checksum(payload), "record crc32c");
    assert_eq!(u64_at(payload, 0), 18, "edit id");
    assert_eq!(
        &payload[8..],
        &[0x01, 0x08, 0x90, 0x4E],
        "SetGlobalSeq(10_000)"
    );
}

/// The WAL segment header and its first frame by hand, and a replay.
#[test]
fn wal_segment_decodes_by_hand_and_replays() {
    let w = fixture("wal-5.log");
    assert_eq!(&w[..8], b"YOLODBWL");
    assert_eq!(u32_at(&w, 8), 1, "version");
    assert_eq!(w[12], 1, "per-CF layout");
    assert_eq!(&w[13..16], &[0, 0, 0]);
    assert_eq!(u64_at(&w, 16), 5, "generation");
    assert_eq!(u32_at(&w, 24), 0, "reserved");
    assert_eq!(u32_at(&w, 28), checksum(&w[..28]));
    let plen = u32_at(&w, 32) as usize;
    assert_eq!(u32_at(&w, 36), checksum(&w[40..40 + plen]), "frame crc32c");
    assert_eq!(
        &w[40..40 + plen],
        &[0x00, 3, 2, 1, b'p', b'u', b't', b'v', b'1']
    );

    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("wal-5.log");
    std::fs::write(&path, &w).unwrap();
    let mut got = Vec::new();
    Wal::replay(&path, SegmentId::per_cf(5), |r| {
        if let ondadb::wal::ReplayRecord::Point(r) = r {
            got.push((r.key, r.seq));
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(got, vec![(b"put".to_vec(), 1), (b"del".to_vec(), 2)]);
}
