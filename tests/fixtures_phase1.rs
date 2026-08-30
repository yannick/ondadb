//! Frozen phase-1 legacy corpus.
//!
//! The bytes in `tests/fixtures/phase1/` were produced by the 0.8.2 encoders
//! *before* any strict-decoding work landed, and are committed to git. They are
//! the gate for feature 1.0: every strictness check added afterwards must leave
//! the decoded outcome of every valid legacy fixture unchanged.
//!
//! [`regenerate_phase1_fixtures`] is `#[ignore]`d on purpose — it is run by
//! hand, only for an explicit and reviewed format change. Every other test in
//! the corpus *reads* the committed files and never writes them.
//!
//! **The klog regenerator no longer reproduces the committed `*_restarts_*`
//! bytes.** Feature 2.1 made block-size accounting trailer-inclusive
//! (`src/sst/writer.rs`), which moves where a restart-bearing block is cut. The
//! committed bytes stay the record of what 0.8.2 wrote — and are still the gate
//! for "every valid legacy fixture decodes unchanged" — but running the
//! regenerator would silently replace them with 2.1's boundaries. The frozen
//! record of 2.1's own output lives in `tests/golden_blocks.rs`.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::{Compression, SyncMode};
use ondadb::encoding::{checksum, put_u32};
use ondadb::manifest::{CfManifest, Manifest, SstMeta, WalLayout};
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;
use ondadb::wal::{
    EnvelopeRecord, RangeRef, Record, ReplayRecord, Wal, ENVELOPE_SCHEMA_PER_CF,
    ENVELOPE_SCHEMA_UNIFIED,
};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/phase1")
}

fn fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()))
}

/// Copy a WAL fixture into `dir` under the stripe-0 name and replay it, so the
/// bytes go through the real `Wal::replay` (which also probes stripes 1..3 and
/// finds nothing).
fn replay_fixture(dir: &Path, name: &str) -> ondadb::Result<Vec<Record>> {
    let path = dir.join("wal");
    std::fs::write(&path, fixture(name)).unwrap();
    let mut out = Vec::new();
    Wal::replay(&path, |rec| {
        match rec {
            ReplayRecord::Point(r) => out.push(r),
            ReplayRecord::RangeDelete { start, end, seq } => {
                panic!("legacy fixture yielded a range delete {start:?}..{end:?}@{seq}")
            }
        }
        Ok(())
    })?;
    Ok(out)
}

/// The 1.2 range-delete records frozen in `wal_v2_range_schema1.bin`, mirroring
/// `wal::tests::range_records`.
fn range_delete_records() -> Vec<(Vec<u8>, Vec<u8>, u64)> {
    vec![
        (b"alpha".to_vec(), b"omega".to_vec(), 11),
        (b"a".to_vec(), b"b".to_vec(), 12),
        (b"b".to_vec(), b"b\0".to_vec(), 13),
    ]
}

// ---- generator --------------------------------------------------------------

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
            tombstone: true,
            ..Default::default()
        },
        Record {
            key: b"sdel".to_vec(),
            seq: 4,
            tombstone: true,
            single_delete: true,
            ..Default::default()
        },
    ]
}

/// Build a WAL through the real writer and return the bytes of the single
/// stripe a one-threaded writer used (stripe assignment is sticky per thread,
/// so exactly one stripe file is non-empty).
fn wal_bytes(build: impl FnOnce(&Wal)) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wal");
    {
        let wal = Wal::open(&path, SyncMode::None, Duration::ZERO).unwrap();
        build(&wal);
        wal.close().unwrap();
    }
    for k in 0..4 {
        let p = if k == 0 {
            path.clone()
        } else {
            PathBuf::from(format!("{}.s{k}", path.display()))
        };
        match std::fs::read(&p) {
            Ok(b) if !b.is_empty() => return b,
            _ => {}
        }
    }
    panic!("no stripe holds the frames");
}

/// Frame `payload` exactly as `Wal::append_batch` does.
fn frame(payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; 8];
    put_u32(&mut out[0..], payload.len() as u32);
    put_u32(&mut out[4..], checksum(payload));
    out.extend_from_slice(payload);
    out
}

fn klog_options(use_btree: bool, restarts: bool, bloom: bool) -> WriterOptions {
    WriterOptions {
        // No compression: the fixture bytes must not move when a compression
        // dependency changes its output.
        compression: Compression::None,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: bloom,
        bloom_fpr: Some(0.01),
        klog_value_threshold: 32,
        block_size: 256,
        expected_entries: 64,
        use_btree,
        restart_interval: if restarts { 8 } else { 0 },
        extended_entries: false,
        prefix_delta: false,
    }
}

/// Every writer-produced entry-flag combination, plus filler so the table spans
/// several blocks (and, with `use_btree`, more than one index level's worth of
/// entries to exercise the tree walk).
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

fn write_klog(path: &Path, opts: WriterOptions) {
    let mut w = Writer::new(path.to_str().unwrap(), opts).unwrap();
    for (k, v, seq, ttl, tomb, sdel) in klog_entries() {
        w.add(k.as_bytes(), &v, seq, ttl, tomb, sdel).unwrap();
    }
    w.finish().unwrap();
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
            },
        ],
    }
}

/// Every `manifest_v1_*` variant, keyed by fixture basename.
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
    ]
}

/// Names of every committed klog fixture, paired with its writer options.
fn klog_variants() -> Vec<(String, WriterOptions)> {
    let mut out = Vec::new();
    for (idx, btree) in ["flat", "btree"].iter().enumerate() {
        for (ridx, restarts) in ["norestarts", "restarts"].iter().enumerate() {
            for (bidx, bloom) in ["nobloom", "bloom"].iter().enumerate() {
                out.push((
                    format!("klog_legacy_{btree}_{restarts}_{bloom}.klog"),
                    klog_options(idx == 1, ridx == 1, bidx == 1),
                ));
            }
        }
    }
    out
}

#[test]
#[ignore = "regenerates committed fixtures; run manually"]
fn regenerate_phase1_fixtures() {
    let dir = fixture_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let write = |name: &str, bytes: &[u8]| std::fs::write(dir.join(name), bytes).unwrap();

    // --- WAL ---
    let all_flags = wal_bytes(|w| {
        for r in wal_all_flags_records() {
            w.append(r).unwrap();
        }
    });
    write("wal_legacy_all_flags.bin", &all_flags);

    // `append_batch(&[])` is public API and writes a zero-length frame; replay
    // must skip it and keep going.
    let empty = wal_bytes(|w| {
        w.append_batch(&[]).unwrap();
        w.append(Record {
            key: b"after".to_vec(),
            value: b"v".to_vec(),
            seq: 9,
            ..Default::default()
        })
        .unwrap();
    });
    write("wal_legacy_empty_frame.bin", &empty);

    let mut torn = wal_bytes(|w| {
        w.append(Record {
            key: b"good".to_vec(),
            value: b"v".to_vec(),
            seq: 1,
            ..Default::default()
        })
        .unwrap();
    });
    // A header claiming 32 payload bytes followed by only 3: the crash residue
    // replay must treat as a clean end of stripe.
    torn.extend_from_slice(&[32, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3]);
    write("wal_legacy_torn_tail.bin", &torn);

    let mut undecodable = wal_bytes(|w| {
        w.append(Record {
            key: b"good".to_vec(),
            value: b"v".to_vec(),
            seq: 1,
            ..Default::default()
        })
        .unwrap();
    });
    // flags 0, klen 5, vlen 0, seq 7 — but only two key bytes follow. The frame
    // CRC is valid, so this is not crash residue: the payload itself lies.
    undecodable.extend_from_slice(&frame(&[0x00, 0x05, 0x00, 0x07, b'a', b'b']));
    write("wal_legacy_crc_valid_undecodable.bin", &undecodable);

    // --- klogs ---
    let tmp = tempfile::tempdir().unwrap();
    for (name, opts) in klog_variants() {
        let path = tmp.path().join(&name);
        write_klog(&path, opts);
        write(&name, &std::fs::read(&path).unwrap());
        let vlog = path.with_extension("vlog");
        let vname = name.replace(".klog", ".vlog");
        write(&vname, &std::fs::read(&vlog).unwrap());
    }

    // --- manifests ---
    for (name, m) in manifest_variants() {
        let path = tmp.path().join(name);
        m.save(&path).unwrap();
        write(name, &std::fs::read(&path).unwrap());
    }
}

// ---- 1.0-B generator (manifest v2, envelopes, extended klog) ----------------

/// The point records the schema-1 envelope fixture holds: one of every writer
/// shape (put, TTL, delete, single-delete).
fn envelope_schema1_records() -> Vec<Record> {
    wal_all_flags_records()
}

/// The schema-2 fixture's records: keys carry the 8-byte big-endian CF-id
/// prefix inside the key, exactly as the unified layout writes them.
fn envelope_schema2_records() -> Vec<Record> {
    let cf_id = 0x0123_4567_89ab_cdefu64;
    let with_prefix = |suffix: &[u8]| {
        let mut k = cf_id.to_be_bytes().to_vec();
        k.extend_from_slice(suffix);
        k
    };
    vec![
        Record {
            key: with_prefix(b"user-key"),
            value: b"v".to_vec(),
            seq: 7,
            ..Default::default()
        },
        Record {
            key: with_prefix(b"gone"),
            seq: 8,
            tombstone: true,
            ..Default::default()
        },
    ]
}

/// Extended-layout klog writer options: the legacy flat/restarts/bloom shape
/// with [`FOOTER_EXTENDED_BLOCK`] set, so the fixture differs from
/// `klog_legacy_flat_restarts_bloom.klog` in exactly the entry layout, the
/// footer bit and the 16-byte aux prefix.
fn klog_extended_options() -> WriterOptions {
    WriterOptions {
        extended_entries: true,
        prefix_delta: false,
        ..klog_options(false, true, true)
    }
}

/// The 1.0-B fixtures, written by the *new* encoders and frozen on the same
/// terms as the legacy corpus: committed once, read-only thereafter.
#[test]
#[ignore = "regenerates committed fixtures; run manually"]
fn regenerate_phase1b_fixtures() {
    let dir = fixture_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let write = |name: &str, bytes: &[u8]| std::fs::write(dir.join(name), bytes).unwrap();

    // --- WAL envelopes (one frame each) ---
    for (name, schema, recs) in [
        (
            "wal_v2_envelope_schema1.bin",
            ENVELOPE_SCHEMA_PER_CF,
            envelope_schema1_records(),
        ),
        (
            "wal_v2_envelope_schema2.bin",
            ENVELOPE_SCHEMA_UNIFIED,
            envelope_schema2_records(),
        ),
    ] {
        let bytes = wal_bytes(|w| {
            let refs: Vec<_> = recs.iter().map(|r| r.as_ref()).collect();
            w.append_batch_enveloped(schema, &refs).unwrap();
        });
        write(name, &bytes);
    }

    // --- WAL range deletes (kind 5, schema 1) ---
    let ranges = range_delete_records();
    let bytes = wal_bytes(|w| {
        let recs: Vec<EnvelopeRecord<'_>> = ranges
            .iter()
            .map(|(start, end, seq)| {
                EnvelopeRecord::Range(RangeRef {
                    start,
                    end,
                    seq: *seq,
                })
            })
            .collect();
        w.append_batch_envelope(ENVELOPE_SCHEMA_PER_CF, &recs)
            .unwrap();
    });
    write("wal_v2_range_schema1.bin", &bytes);

    // --- manifest v2: caps and nothing else ---
    let tmp = tempfile::tempdir().unwrap();
    let mut m = manifest_base();
    m.caps = ondadb::format::CAP_EXTENDED_RECORDS;
    let path = tmp.path().join("manifest_v2_caps_only.bin");
    m.save(&path).unwrap();
    write("manifest_v2_caps_only.bin", &std::fs::read(&path).unwrap());

    // --- extended klog ---
    let path = tmp.path().join("klog_extended.klog");
    write_klog(&path, klog_extended_options());
    write("klog_extended.klog", &std::fs::read(&path).unwrap());
    write(
        "klog_extended.vlog",
        &std::fs::read(path.with_extension("vlog")).unwrap(),
    );
}

// ---- the pinning tests ------------------------------------------------------

/// The gate for feature 1.0: every strictness check added later must leave all
/// of these outcomes exactly as the 0.8.2 decoders produced them.
#[test]
fn legacy_corpus_decodes_unchanged() {
    let tmp = tempfile::tempdir().unwrap();

    // --- WAL: every writer-produced flag combination, one frame each ---
    let recs = replay_fixture(tmp.path(), "wal_legacy_all_flags.bin").unwrap();
    let expect = wal_all_flags_records();
    assert_eq!(recs.len(), expect.len());
    for (got, want) in recs.iter().zip(expect.iter()) {
        assert_eq!(got.key, want.key);
        assert_eq!(got.value, want.value);
        assert_eq!(got.seq, want.seq);
        assert_eq!(got.ttl, want.ttl);
        assert_eq!(got.tombstone, want.tombstone);
        assert_eq!(got.single_delete, want.single_delete);
    }

    // --- klogs: entry flags, both index shapes, with and without restarts ---
    let entries = klog_entries();
    for (name, _) in klog_variants() {
        let klog = tmp.path().join(&name);
        std::fs::write(&klog, fixture(&name)).unwrap();
        let vname = name.replace(".klog", ".vlog");
        std::fs::write(klog.with_extension("vlog"), fixture(&vname)).unwrap();

        let reader = Reader::open(
            klog.to_str().unwrap(),
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            1,
            default_comparator(),
            0,
        )
        .unwrap_or_else(|e| panic!("{name}: {e}"));

        let mut it = reader.iter();
        it.seek_to_first();
        for (k, v, seq, ttl, tomb, sdel) in &entries {
            assert!(it.valid(), "{name}: iterator ended at {k}");
            assert_eq!(it.user_key(), k.as_bytes(), "{name}");
            assert_eq!(it.seq(), *seq, "{name} {k}");
            assert_eq!(it.ttl(), *ttl, "{name} {k}");
            assert_eq!(it.is_tombstone(), *tomb, "{name} {k}");
            assert_eq!(it.is_single_delete(), *sdel, "{name} {k}");
            assert_eq!(&it.value().unwrap(), v, "{name} {k}");
            it.next();
        }
        assert!(!it.valid(), "{name}: extra entries");
        assert_eq!(reader.num_entries(), entries.len() as u64, "{name}");
    }

    // --- manifests: every tail combination, in emission order ---
    for (name, want) in manifest_variants() {
        let bytes = fixture(name);
        let path = tmp.path().join(name);
        std::fs::write(&path, &bytes).unwrap();
        let got = Manifest::load(&path).unwrap();
        assert_eq!(got.next_file_id, want.next_file_id, "{name}");
        assert_eq!(got.global_seq, want.global_seq, "{name}");
        assert_eq!(got.wal_layout, want.wal_layout, "{name}");
        assert_eq!(got.instance_nonce, want.instance_nonce, "{name}");
        assert_eq!(got.cfs.len(), want.cfs.len(), "{name}");
        for (a, b) in got.cfs.iter().zip(want.cfs.iter()) {
            assert_eq!(a.name, b.name, "{name}");
            assert_eq!(a.config, b.config, "{name}");
            assert_eq!(a.sstables, b.sstables, "{name}");
        }
        // Re-encoding a decoded manifest must reproduce the frozen bytes.
        let round = tmp.path().join(format!("{name}.round"));
        got.save(&round).unwrap();
        assert_eq!(std::fs::read(&round).unwrap(), bytes, "{name}: re-encode");
    }
}

/// The 1.0-B corpus: the bytes the new encoders produce are frozen on the same
/// terms as the legacy ones, and each decodes back to the value that built it.
#[test]
fn phase1b_corpus_decodes_unchanged() {
    let tmp = tempfile::tempdir().unwrap();

    // --- WAL envelopes: both schemas replay as ordinary point records ---
    for (name, recs) in [
        ("wal_v2_envelope_schema1.bin", envelope_schema1_records()),
        ("wal_v2_envelope_schema2.bin", envelope_schema2_records()),
    ] {
        let got = replay_fixture(tmp.path(), name).unwrap();
        assert_eq!(got.len(), recs.len(), "{name}");
        for (g, w) in got.iter().zip(recs.iter()) {
            assert_eq!(g.key, w.key, "{name}");
            assert_eq!(g.value, w.value, "{name}");
            assert_eq!(g.seq, w.seq, "{name}");
            assert_eq!(g.ttl, w.ttl, "{name}");
            assert_eq!(g.tombstone, w.tombstone, "{name}");
            assert_eq!(g.single_delete, w.single_delete, "{name}");
        }
    }
    // Schema 2 keeps the CF-id prefix inside the key; replay hands it back whole.
    let got = replay_fixture(tmp.path(), "wal_v2_envelope_schema2.bin").unwrap();
    assert_eq!(&got[0].key[..8], &0x0123_4567_89ab_cdefu64.to_be_bytes());

    // --- manifest v2: caps and nothing else ---
    let bytes = fixture("manifest_v2_caps_only.bin");
    assert_eq!(&bytes[4..8], &2u32.to_le_bytes(), "version field must be 2");
    let path = tmp.path().join("manifest_v2_caps_only.bin");
    std::fs::write(&path, &bytes).unwrap();
    let m = Manifest::load(&path).unwrap();
    assert_eq!(m.caps, ondadb::format::CAP_EXTENDED_RECORDS);
    let round = tmp.path().join("manifest_v2.round");
    m.save(&round).unwrap();
    assert_eq!(std::fs::read(&round).unwrap(), bytes, "re-encode");

    // --- extended klog: the same entries as the legacy corpus, new layout ---
    let klog = tmp.path().join("klog_extended.klog");
    std::fs::write(&klog, fixture("klog_extended.klog")).unwrap();
    std::fs::write(klog.with_extension("vlog"), fixture("klog_extended.vlog")).unwrap();
    let reader = Reader::open(
        klog.to_str().unwrap(),
        LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
    )
    .unwrap();
    // 1.0 defines the aux container and writes it empty; 1.2 is the first producer.
    assert_eq!(reader.aux_block_handle(), Some((0, 0)));
    let mut it = reader.iter();
    it.seek_to_first();
    for (k, v, seq, ttl, tomb, sdel) in klog_entries() {
        assert!(it.valid(), "iterator ended at {k}");
        assert_eq!(it.user_key(), k.as_bytes());
        assert_eq!(it.seq(), seq, "{k}");
        assert_eq!(it.ttl(), ttl, "{k}");
        assert_eq!(it.is_tombstone(), tomb, "{k}");
        assert_eq!(it.is_single_delete(), sdel, "{k}");
        assert_eq!(it.value().unwrap(), v, "{k}");
        it.next();
    }
    assert!(!it.valid(), "extra entries");
}
