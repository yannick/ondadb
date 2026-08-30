//! SSTable integration tests

use std::sync::Arc;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::Compression;
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;

fn opts(alg: Compression, n: usize, klog_threshold: usize, block_size: usize) -> WriterOptions {
    WriterOptions {
        compression: alg,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: true,
        bloom_fpr: Some(0.01),
        klog_value_threshold: klog_threshold,
        block_size,
        expected_entries: n,
        use_btree: false,
        restart_interval: 8,
        extended_entries: false,
    }
}

fn build_sst(
    dir: &std::path::Path,
    alg: Compression,
    n: usize,
    val_size: usize,
) -> (Arc<Reader>, Vec<String>) {
    let klog = dir.join("1.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(alg, n, 512, 1024)).unwrap();
    let val = vec![b'v'; val_size];
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("key{i:06}");
        w.add(k.as_bytes(), &val, (i + 1) as u64, 0, false, false)
            .unwrap();
        keys.push(k);
    }
    w.finish().unwrap();
    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        1,
        default_comparator(),
        0,
    )
    .unwrap();
    (r, keys)
}

#[test]
fn get_present_absent() {
    for alg in [Compression::None, Compression::Snappy, Compression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let (r, keys) = build_sst(dir.path(), alg, 500, 50);
        for k in &keys {
            let (v, _seq, found, deleted) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
            assert!(found && !deleted, "alg {alg:?} get {k}");
            assert_eq!(v.unwrap().len(), 50);
        }
        let (_, _, found, _) = r.get(b"key999999", u64::MAX, 0).unwrap();
        assert!(!found, "alg {alg:?}: unexpected find of absent key");
        let (_, _, found, _) = r.get(b"aaa", u64::MAX, 0).unwrap();
        assert!(!found, "alg {alg:?}: unexpected find of key before min");
    }
}

#[test]
fn large_value_vlog() {
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("2.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(Compression::None, 10, 64, 1024)).unwrap();
    let big = vec![b'X'; 4096];
    w.add(b"a", b"tiny", 1, 0, false, false).unwrap();
    w.add(b"b", &big, 2, 0, false, false).unwrap();
    let meta = w.finish().unwrap();
    assert!(meta.vlog_size > 0, "expected vlog for large value");

    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        2,
        default_comparator(),
        0,
    )
    .unwrap();
    let (v, _, found, _) = r.get(b"b", u64::MAX, 0).unwrap();
    assert!(found && v.as_deref() == Some(big.as_slice()));
    let (v, _, found, _) = r.get(b"a", u64::MAX, 0).unwrap();
    assert!(found && v.as_deref() == Some(b"tiny".as_slice()));
}

#[test]
fn corrupt_vlog_value_is_detected() {
    // A bit-flip in the vlog (large value) region must be caught by the per-value
    // CRC, not silently returned as a wrong value.
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("vc.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(Compression::None, 4, 64, 1024)).unwrap();
    let big = vec![b'Z'; 4096];
    w.add(b"a", &big, 1, 0, false, false).unwrap();
    w.finish().unwrap();

    // Flip a byte in the vlog value region (past the 4-byte CRC prefix).
    let vlog = dir.path().join("vc.vlog");
    let mut bytes = std::fs::read(&vlog).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 0xFF;
    std::fs::write(&vlog, &bytes).unwrap();

    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        42,
        default_comparator(),
        0,
    )
    .unwrap();
    let res = r.get(b"a", u64::MAX, 0);
    assert!(
        res.is_err(),
        "corrupted vlog value must be rejected, got {res:?}"
    );
}

/// A vlog frame's CRC is verified once per open reader, not once per read
/// (`Reader::vlog_verified`). The mark must never turn a corrupt frame into a
/// readable one, so a frame that fails has to keep failing — on every read, on
/// every thread, for the life of the reader.
#[test]
fn corrupt_vlog_value_is_detected_on_every_read() {
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("vr.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(Compression::None, 4, 64, 1024)).unwrap();
    let good = vec![b'G'; 8192];
    let bad = vec![b'B'; 8192];
    w.add(b"good", &good, 1, 0, false, false).unwrap();
    w.add(b"zbad", &bad, 2, 0, false, false).unwrap();
    w.finish().unwrap();

    // Corrupt only the second frame; the first must stay readable.
    let vlog = dir.path().join("vr.vlog");
    let mut bytes = std::fs::read(&vlog).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 0xFF;
    std::fs::write(&vlog, &bytes).unwrap();

    let r = Reader::open(
        klog,
        LocalStorage::new(Arc::new(FileCache::new(16)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        43,
        default_comparator(),
        0,
    )
    .unwrap();

    for i in 0..5 {
        let res = r.get(b"zbad", u64::MAX, 0);
        assert!(
            res.is_err(),
            "read {i} of a corrupt frame succeeded: {res:?}"
        );
        let (v, _, found, _) = r.get(b"good", u64::MAX, 0).unwrap();
        assert!(found && v.as_deref() == Some(good.as_slice()), "read {i}");
    }

    // Same from several threads at once: the failing frame must never be
    // marked verified by a racing reader of the frame beside it.
    let r = Arc::new(r);
    let good = Arc::new(good);
    let mut hs = Vec::new();
    for _ in 0..4 {
        let (r, good) = (r.clone(), good.clone());
        hs.push(std::thread::spawn(move || {
            for _ in 0..200 {
                assert!(r.get(b"zbad", u64::MAX, 0).is_err());
                let (v, _, found, _) = r.get(b"good", u64::MAX, 0).unwrap();
                assert!(found && v.as_deref() == Some(good.as_slice()));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
}

/// Repeat reads of vlog values must return identical bytes whether or not the
/// frame's checksum is recomputed — including the compressed frame layout,
/// where skipping the CRC must not skip the decompression length checks.
#[test]
fn vlog_values_stable_across_repeat_reads() {
    for alg in [Compression::None, Compression::Snappy, Compression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("vs.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(klog, opts(alg, 8, 64, 1024)).unwrap();
        let vals: Vec<Vec<u8>> = (0..8u8)
            .map(|i| (0..9000u32).map(|j| (j as u8) ^ i).collect())
            .collect();
        for (i, v) in vals.iter().enumerate() {
            w.add(
                format!("k{i}").as_bytes(),
                v,
                (i + 1) as u64,
                0,
                false,
                false,
            )
            .unwrap();
        }
        w.finish().unwrap();
        let r = Reader::open(
            klog,
            LocalStorage::new(Arc::new(FileCache::new(16)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            44,
            default_comparator(),
            0,
        )
        .unwrap();
        for round in 0..4 {
            for (i, want) in vals.iter().enumerate() {
                let (v, _, found, _) = r.get(format!("k{i}").as_bytes(), u64::MAX, 0).unwrap();
                assert!(found, "{alg:?} round {round} key k{i} missing");
                assert_eq!(v.as_ref(), Some(want), "{alg:?} round {round} key k{i}");
            }
        }
    }
}

#[test]
fn tombstone_and_mvcc() {
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("3.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(Compression::None, 10, 512, 4096)).unwrap();
    // Two versions of "k": newer tombstone (seq 5), older value (seq 3).
    w.add(b"k", b"", 5, 0, true, false).unwrap();
    w.add(b"k", b"old", 3, 0, false, false).unwrap();
    w.finish().unwrap();

    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        3,
        default_comparator(),
        0,
    )
    .unwrap();

    let (_, _, found, deleted) = r.get(b"k", 100, 0).unwrap();
    assert!(found && deleted, "latest version should be tombstone");
    let (v, _, found, deleted) = r.get(b"k", 4, 0).unwrap();
    assert!(found && !deleted && v.as_deref() == Some(b"old".as_slice()));
}

#[test]
fn iterator_forward_backward() {
    let dir = tempfile::tempdir().unwrap();
    let (r, keys) = build_sst(dir.path(), Compression::None, 300, 20);

    let mut it = r.iter();
    let mut got = Vec::new();
    it.seek_to_first();
    while it.valid() {
        got.push(String::from_utf8(it.user_key().to_vec()).unwrap());
        it.next();
    }
    assert!(it.err().is_none());
    assert_eq!(got, keys);

    got.clear();
    it.seek_to_last();
    while it.valid() {
        got.push(String::from_utf8(it.user_key().to_vec()).unwrap());
        it.prev();
    }
    let mut rev = keys.clone();
    rev.reverse();
    assert_eq!(got, rev);
}

#[test]
fn iterator_seek() {
    let dir = tempfile::tempdir().unwrap();
    let (r, _) = build_sst(dir.path(), Compression::None, 1000, 10);
    let mut it = r.iter();
    it.seek(b"key000500", u64::MAX);
    assert!(it.valid() && it.user_key() == b"key000500");
    // Seek to a key between entries lands on the next one.
    it.seek(b"key0005005", u64::MAX);
    assert!(
        it.valid() && it.user_key() == b"key000501",
        "got {:?}",
        String::from_utf8_lossy(it.user_key())
    );
}

#[test]
fn btree_hybrid_klog_round_trip() {
    // Small blocks + many entries force many data blocks, hence a multi-leaf
    // (multi-level) B+tree index.
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("bt.klog");
    let klog = klog.to_str().unwrap();
    let n = 20_000usize;
    let mut wopts = opts(Compression::None, n, 512, 256); // tiny 256B blocks
    wopts.use_btree = true;
    let mut w = Writer::new(klog, wopts).unwrap();
    let val = vec![b'v'; 40];
    let mut keys = Vec::with_capacity(n);
    for i in 0..n {
        let k = format!("key{i:08}");
        w.add(k.as_bytes(), &val, (i + 1) as u64, 0, false, false)
            .unwrap();
        keys.push(k);
    }
    let meta = w.finish().unwrap();
    assert_eq!(meta.num_entries, n as u64);

    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        7,
        default_comparator(),
        0,
    )
    .unwrap();

    // Point reads (exercises find_block over the reconstructed index).
    for k in keys.iter().step_by(97) {
        let (v, _, found, deleted) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found && !deleted, "btree get {k}");
        assert_eq!(v.unwrap().len(), 40);
    }
    assert!(!r.get(b"key99999999", u64::MAX, 0).unwrap().2);
    assert_eq!(r.min_key(), b"key00000000");

    // Full forward iteration yields every key in order.
    let mut it = r.iter();
    let mut count = 0usize;
    it.seek_to_first();
    while it.valid() {
        assert_eq!(it.user_key(), keys[count].as_bytes());
        it.next();
        count += 1;
    }
    assert_eq!(count, n);

    // Seek lands precisely.
    it.seek(b"key00012345", u64::MAX);
    assert!(it.valid() && it.user_key() == b"key00012345");
}

#[test]
fn value_round_trip_via_iterator() {
    let dir = tempfile::tempdir().unwrap();
    let (r, keys) = build_sst(dir.path(), Compression::Zstd, 100, 30);
    let mut it = r.iter();
    it.seek_to_first();
    let mut i = 0;
    while it.valid() {
        assert_eq!(it.user_key(), keys[i].as_bytes());
        assert_eq!(it.value().unwrap(), vec![b'v'; 30]);
        it.next();
        i += 1;
    }
    assert_eq!(i, keys.len());
}

// ---- vlog compression (v2 frames) + per-prefix rules -----------------------

/// Large compressible values must shrink the vlog and round-trip intact.
#[test]
fn vlog_compression_roundtrip_and_shrinks() {
    let n = 200;
    let val: Vec<u8> = (0..4096u32).map(|i| (i % 13) as u8).collect(); // highly compressible
    let mut sizes = std::collections::HashMap::new();
    for alg in [Compression::None, Compression::Lz4, Compression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("1.klog");
        let klog = klog.to_str().unwrap();
        let mut w = Writer::new(klog, opts(alg, n, 512, 4096)).unwrap();
        for i in 0..n {
            let k = format!("key{i:06}");
            w.add(k.as_bytes(), &val, (i + 1) as u64, 0, false, false)
                .unwrap();
        }
        w.finish().unwrap();
        let vlog_size = std::fs::metadata(dir.path().join("1.vlog")).unwrap().len();
        sizes.insert(alg, vlog_size);
        let fc = Arc::new(FileCache::new(16));
        let bc = Arc::new(BlockCache::new(1 << 20));
        let r = Reader::open(
            klog,
            LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
            bc,
            1,
            default_comparator(),
            0,
        )
        .unwrap();
        for i in 0..n {
            let k = format!("key{i:06}");
            let (v, _seq, found, deleted) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
            assert!(found && !deleted, "alg {alg:?} get {k}");
            assert_eq!(v.unwrap(), val, "alg {alg:?} value mismatch for {k}");
        }
        // Scan path reads vlog values too.
        let r = Arc::new(r);
        let mut it = r.iter();
        it.seek_to_first();
        let mut cnt = 0;
        while it.valid() {
            let mut out = Vec::new();
            it.value_into(&mut out).unwrap();
            assert_eq!(out, val);
            cnt += 1;
            it.next();
        }
        assert_eq!(cnt, n);
    }
    let raw = sizes[&Compression::None];
    assert!(
        sizes[&Compression::Lz4] < raw / 2,
        "lz4 vlog {} not < half of raw {}",
        sizes[&Compression::Lz4],
        raw
    );
    assert!(sizes[&Compression::Zstd] < raw / 2);
}

/// Incompressible values fall back to raw storage (alg=None per frame) and
/// still round-trip.
#[test]
fn vlog_incompressible_stored_raw() {
    let n = 50;
    // Pseudo-random bytes: xorshift, effectively incompressible.
    let mut x = 0x12345678u32;
    let val: Vec<u8> = (0..2048)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            (x & 0xff) as u8
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("1.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(Compression::Lz4, n, 512, 4096)).unwrap();
    for i in 0..n {
        let k = format!("key{i:06}");
        w.add(k.as_bytes(), &val, (i + 1) as u64, 0, false, false)
            .unwrap();
    }
    w.finish().unwrap();
    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        1,
        default_comparator(),
        0,
    )
    .unwrap();
    for i in 0..n {
        let k = format!("key{i:06}");
        let (v, _s, found, _d) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found);
        assert_eq!(v.unwrap(), val);
    }
}

/// Per-prefix rules: klog blocks are cut at rule boundaries, every key stays
/// readable, and the rule algorithm is applied to vlog values.
#[test]
fn per_prefix_compression_rules() {
    use ondadb::config::CompressionRule;
    let compressible: Vec<u8> = (0..4096u32).map(|i| (i % 7) as u8).collect();
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("1.klog");
    let klog = klog.to_str().unwrap();
    let mut o = opts(Compression::None, 300, 512, 4096);
    o.compression_rules = vec![
        CompressionRule {
            prefix: b"az".to_vec(), // longer prefix beats "a"
            compression: Compression::Zstd,
        },
        CompressionRule {
            prefix: b"a".to_vec(),
            compression: Compression::Lz4,
        },
    ];
    let mut w = Writer::new(klog, o).unwrap();
    // Interleave rule regions in sorted order: a..., az..., b... (no rule).
    let mut keys = Vec::new();
    for i in 0..100 {
        keys.push(format!("a{i:04}"));
    }
    for i in 0..100 {
        keys.push(format!("az{i:04}"));
    }
    for i in 0..100 {
        keys.push(format!("b{i:04}"));
    }
    keys.sort();
    for (i, k) in keys.iter().enumerate() {
        w.add(k.as_bytes(), &compressible, (i + 1) as u64, 0, false, false)
            .unwrap();
    }
    w.finish().unwrap();
    // Vlog must be far smaller than raw (200 of 300 values compressed).
    let vlog_size = std::fs::metadata(dir.path().join("1.vlog")).unwrap().len();
    assert!(
        (vlog_size as usize) < 300 * compressible.len() / 2,
        "vlog {} not compressed",
        vlog_size
    );
    let fc = Arc::new(FileCache::new(16));
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = Reader::open(
        klog,
        LocalStorage::new(fc, cfg!(feature = "mmap-reads")),
        bc,
        1,
        default_comparator(),
        0,
    )
    .unwrap();
    for k in &keys {
        let (v, _s, found, _d) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found, "missing {k}");
        assert_eq!(v.unwrap(), compressible, "value mismatch for {k}");
    }
    // Full scan sees every key in order.
    let r = Arc::new(r);
    let mut it = r.iter();
    it.seek_to_first();
    let mut seen = Vec::new();
    while it.valid() {
        seen.push(String::from_utf8(it.user_key().to_vec()).unwrap());
        it.next();
    }
    assert_eq!(seen, keys);
}

/// A footer flag bit this binary does not implement is `UnsupportedFormat`, not
/// `Corruption`: the bytes are well-formed, they just name a feature we lack.
#[test]
fn footer_unknown_flag_bit_is_unsupported_format() {
    const FOOTER_SIZE: usize = 64;
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phase1/klog_legacy_flat_restarts_bloom.klog");
    let mut bytes = std::fs::read(&src).unwrap();
    let flags_at = bytes.len() - FOOTER_SIZE + 48;
    // 0x20 is above every assigned footer bit (0x10 is reserved for 1.0B's
    // extended-block flag), and the footer carries no checksum of its own.
    bytes[flags_at] |= 0x20;

    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("1.klog");
    std::fs::write(&klog, &bytes).unwrap();
    std::fs::write(
        klog.with_extension("vlog"),
        std::fs::read(src.with_extension("vlog")).unwrap(),
    )
    .unwrap();

    let err = Reader::open(
        klog.to_str().unwrap(),
        LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
    )
    .expect_err("an unknown footer flag must be refused");
    assert_eq!(err.kind(), "unsupported_format");
}

/// The footer decoder must be total over its flags byte: every value either
/// opens the table or returns an error, and none panics.
#[test]
fn fuzz_footer_flags_never_panic() {
    const FOOTER_SIZE: usize = 64;
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phase1/klog_legacy_flat_restarts_bloom.klog");
    let original = std::fs::read(&src).unwrap();
    let vlog = std::fs::read(src.with_extension("vlog")).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("1.klog");
    std::fs::write(klog.with_extension("vlog"), &vlog).unwrap();

    for value in 0u8..=255 {
        let mut bytes = original.clone();
        let flags_at = bytes.len() - FOOTER_SIZE + 48;
        bytes[flags_at] = value;
        std::fs::write(&klog, &bytes).unwrap();
        let res = Reader::open(
            klog.to_str().unwrap(),
            LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
            Arc::new(BlockCache::new(1 << 20)),
            1,
            default_comparator(),
            0,
        );
        if let Err(e) = res {
            // Only the two fail-closed taxonomies are acceptable here.
            assert!(
                matches!(e.kind(), "corruption" | "unsupported_format"),
                "flags {value:#04x}: unexpected {}",
                e.kind()
            );
        }
    }
}
// ---------------------------------------------------------------------------
// Vlog value cache (feature 0.5)
// ---------------------------------------------------------------------------

/// Open a reader over `klog` with the vlog value cache limited to `limit`
/// bytes, sharing `bc` so the test can inspect admissions.
fn open_reader(klog: &str, bc: Arc<BlockCache>, file_id: u64, limit: usize) -> Arc<Reader> {
    Reader::open(
        klog,
        LocalStorage::new(Arc::new(FileCache::new(16)), cfg!(feature = "mmap-reads")),
        bc,
        file_id,
        default_comparator(),
        limit,
    )
    .unwrap()
}

/// Write a one-key table whose value is separated into the vlog.
fn build_vlog_table(dir: &std::path::Path, name: &str, key: &[u8], value: &[u8]) -> String {
    build_vlog_table_with(dir, name, &[(key, value)], Compression::None)
}

fn build_vlog_table_with(
    dir: &std::path::Path,
    name: &str,
    entries: &[(&[u8], &[u8])],
    alg: Compression,
) -> String {
    let klog = dir.join(format!("{name}.klog"));
    let klog = klog.to_str().unwrap().to_string();
    // Threshold 64 separates every value used here; 1 KiB blocks.
    let mut w = Writer::new(&klog, opts(alg, entries.len(), 64, 1024)).unwrap();
    for (i, (k, v)) in entries.iter().enumerate() {
        w.add(k, v, (i + 1) as u64, 0, false, false).unwrap();
    }
    let meta = w.finish().unwrap();
    assert!(meta.vlog_size > 0, "expected a vlog for {name}");
    klog
}

/// `(file_id, 0)` names both the first klog data block and the first vlog
/// frame. Without the `BlockDomain` tag on the cache key, one would be handed
/// out where the other was asked for. Reading in both orders pins that.
#[test]
fn klog_block_and_vlog_frame_at_same_offset_do_not_alias() {
    for reversed in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        // "a" is inline (below the threshold), so the first data block holds it
        // at klog offset 0; "b" is separated, so its frame sits at vlog offset 0.
        let big = vec![b'X'; 4096];
        let klog = build_vlog_table_with(
            dir.path(),
            "alias",
            &[(b"a", b"tiny"), (b"b", big.as_slice())],
            Compression::None,
        );
        let bc = Arc::new(BlockCache::new(1 << 20));
        let r = open_reader(&klog, bc, 7, 1 << 20);

        let read_value = || {
            let (v, _, found, _) = r.get(b"b", u64::MAX, 0).unwrap();
            assert!(found, "separated value missing");
            assert_eq!(v.as_deref(), Some(big.as_slice()), "vlog frame aliased");
        };
        let read_block = || {
            let (v, _, found, _) = r.get(b"a", u64::MAX, 0).unwrap();
            assert!(found, "inline value missing");
            assert_eq!(v.as_deref(), Some(b"tiny".as_slice()), "data block aliased");
        };

        if reversed {
            read_block();
            read_value();
        } else {
            read_value();
            read_block();
        }
        // ...and again, now that both are resident.
        read_value();
        read_block();
    }
}

/// The second read of a hot frame must do no vlog I/O and no decompression —
/// in both feature configs. Under `mmap-reads` this is the assertion that the
/// cache is consulted *before* the mmap path, which would otherwise decompress
/// a v2 frame on every read.
#[test]
fn hot_vlog_frame_is_served_from_cache() {
    for alg in [Compression::None, Compression::Zstd] {
        let dir = tempfile::tempdir().unwrap();
        let big: Vec<u8> = (0..64_000u32).map(|i| (i % 251) as u8).collect();
        let klog = build_vlog_table_with(dir.path(), "hot", &[(b"k", big.as_slice())], alg);
        let bc = Arc::new(BlockCache::new(1 << 20));
        let r = open_reader(&klog, bc, 11, 1 << 20);

        let cold = ondadb::perf::enter();
        let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
        let cold = cold.finish();
        assert!(found && v.as_deref() == Some(big.as_slice()));
        assert_eq!(cold.vlog_reads, 1, "{alg:?} cold read");
        assert_eq!(cold.vlog_cache_hits, 0, "{alg:?} cold read");

        let warm = ondadb::perf::enter();
        let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
        let warm = warm.finish();
        assert!(found && v.as_deref() == Some(big.as_slice()));
        assert_eq!(warm.vlog_reads, 0, "{alg:?}: warm read touched the vlog");
        assert_eq!(warm.vlog_cache_hits, 1, "{alg:?}: warm read was not a hit");
        assert_eq!(
            warm.bytes_decompressed, 0,
            "{alg:?}: warm read decompressed"
        );
    }
}

/// Admission is bounded by the configured limit, and a limit of 0 admits
/// nothing at all.
#[test]
fn vlog_admission_respects_limit() {
    const LIMIT: usize = 4096;
    for (len, want_entry) in [(LIMIT - 1, true), (LIMIT, true), (LIMIT + 1, false)] {
        let dir = tempfile::tempdir().unwrap();
        let val = vec![b'q'; len];
        let klog = build_vlog_table(dir.path(), "lim", b"k", &val);
        let bc = Arc::new(BlockCache::new(1 << 20));
        let r = open_reader(&klog, bc.clone(), 21, LIMIT);

        let before = bc.stats().vlog_entries;
        let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
        assert!(found && v.as_deref() == Some(val.as_slice()));
        let added = bc.stats().vlog_entries - before;
        assert_eq!(
            added,
            usize::from(want_entry),
            "len {len} vs limit {LIMIT}: entries added {added}"
        );
    }

    // Disabled: nothing is ever admitted, however small the value.
    let dir = tempfile::tempdir().unwrap();
    let val = vec![b'q'; 100];
    let klog = build_vlog_table(dir.path(), "off", b"k", &val);
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = open_reader(&klog, bc.clone(), 22, 0);
    let before = bc.stats().vlog_entries;
    for _ in 0..3 {
        let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
        assert!(found && v.as_deref() == Some(val.as_slice()));
    }
    assert_eq!(
        bc.stats().vlog_entries,
        before,
        "a disabled vlog cache admitted an entry"
    );
    let ctx = ondadb::perf::enter();
    let _ = r.get(b"k", u64::MAX, 0).unwrap();
    assert_eq!(ctx.finish().vlog_cache_hits, 0);
}

/// A frame that fails its CRC must never be admitted — a cache that memoized a
/// corrupt decode would turn a detected corruption into a silent wrong answer
/// for the life of the process.
#[test]
fn corrupt_vlog_frame_is_not_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let big = vec![b'Z'; 4096];
    let klog = build_vlog_table(dir.path(), "bad", b"a", &big);
    let vlog = dir.path().join("bad.vlog");

    let good = std::fs::read(&vlog).unwrap();
    let mut bytes = good.clone();
    let n = bytes.len();
    bytes[n - 1] ^= 0xFF;
    std::fs::write(&vlog, &bytes).unwrap();

    let bc = Arc::new(BlockCache::new(1 << 20));
    {
        let r = open_reader(&klog, bc.clone(), 31, 1 << 20);
        let before = bc.stats().vlog_entries;
        assert!(
            r.get(b"a", u64::MAX, 0).is_err(),
            "a corrupt frame must be rejected"
        );
        assert_eq!(
            bc.stats().vlog_entries,
            before,
            "a corrupt decode was admitted to the cache"
        );
        // Still an error on the retry: nothing memoized the bad bytes.
        assert!(r.get(b"a", u64::MAX, 0).is_err());
        assert_eq!(bc.stats().vlog_entries, before);
    }

    // Repair and reopen: the same frame now decodes and is admitted.
    std::fs::write(&vlog, &good).unwrap();
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = open_reader(&klog, bc.clone(), 32, 1 << 20);
    let before = bc.stats().vlog_entries;
    let (v, _, found, _) = r.get(b"a", u64::MAX, 0).unwrap();
    assert!(found && v.as_deref() == Some(big.as_slice()));
    assert_eq!(
        bc.stats().vlog_entries,
        before + 1,
        "repaired frame not admitted"
    );
}

/// Two threads racing on the same cold frame both decode correctly, and the
/// duplicate insert leaves exactly one entry (there is no singleflight in v1;
/// `put` keeps the resident value).
#[test]
fn concurrent_vlog_misses_both_return_correct_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let big: Vec<u8> = (0..48_000u32).map(|i| (i % 253) as u8).collect();
    let klog = build_vlog_table(dir.path(), "race", b"k", &big);
    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = open_reader(&klog, bc.clone(), 41, 1 << 20);

    let before = bc.stats().vlog_entries;
    let start = Arc::new(std::sync::Barrier::new(2));
    let big = Arc::new(big);
    let mut hs = Vec::new();
    for _ in 0..2 {
        let (r, start, big) = (r.clone(), start.clone(), big.clone());
        hs.push(std::thread::spawn(move || {
            start.wait();
            let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
            assert!(found && v.as_deref() == Some(big.as_slice()));
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    assert_eq!(
        bc.stats().vlog_entries,
        before + 1,
        "a duplicate insert must not add a second entry"
    );
}

/// Tables written before the v2 vlog frame layout (`FOOTER_VLOG_V2` clear) must
/// cache and serve identically — the cache keys on the frame offset, not on the
/// frame's shape. Today's writer only emits v2, so the v1 table is synthesised:
/// one value, so its frame is at vlog offset 0 either way.
#[test]
fn legacy_v1_vlog_frames_are_cached() {
    const FOOTER_SIZE: usize = 64;
    const FOOTER_VLOG_V2: u8 = 0x08;

    let dir = tempfile::tempdir().unwrap();
    let big = vec![b'L'; 8192];
    let klog = build_vlog_table(dir.path(), "v1", b"k", &big);

    // v1 frame: CRC32-C of the value, then the raw value.
    let vlog = dir.path().join("v1.vlog");
    let mut frame = ondadb::encoding::checksum(&big).to_le_bytes().to_vec();
    frame.extend_from_slice(&big);
    std::fs::write(&vlog, &frame).unwrap();

    // Clear the v2 flag in the klog footer (the footer carries no checksum of
    // its own, only the trailing magic).
    let mut klog_bytes = std::fs::read(&klog).unwrap();
    let flags = klog_bytes.len() - FOOTER_SIZE + 48;
    assert!(
        klog_bytes[flags] & FOOTER_VLOG_V2 != 0,
        "expected a v2 table"
    );
    klog_bytes[flags] &= !FOOTER_VLOG_V2;
    std::fs::write(&klog, &klog_bytes).unwrap();

    let bc = Arc::new(BlockCache::new(1 << 20));
    let r = open_reader(&klog, bc.clone(), 51, 1 << 20);
    let before = bc.stats().vlog_entries;

    let cold = ondadb::perf::enter();
    let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
    let cold = cold.finish();
    assert!(found && v.as_deref() == Some(big.as_slice()));
    assert_eq!(cold.vlog_reads, 1);
    assert_eq!(bc.stats().vlog_entries, before + 1, "v1 frame not admitted");

    let warm = ondadb::perf::enter();
    let (v, _, found, _) = r.get(b"k", u64::MAX, 0).unwrap();
    let warm = warm.finish();
    assert!(found && v.as_deref() == Some(big.as_slice()));
    assert_eq!(warm.vlog_reads, 0);
    assert_eq!(warm.vlog_cache_hits, 1);
}

// ---------------------------------------------------------------------------
// 0.6-A: what the SSTable reader and writer charge the IO limiter.
// ---------------------------------------------------------------------------

use ondadb::ioctrl::{IoClass, IoLimiter, RecordingLimiter, MAX_CHARGE_CHUNK};

#[test]
fn cached_block_reads_are_not_charged() {
    // A cache hit — or, under `mmap-reads`, a block this reader has already
    // faulted in and verified — costs no device IO, so it must cost no
    // bandwidth either. Only the fetch is charged.
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("1.klog");
    let klog = klog.to_str().unwrap();
    // One block, so every key below resolves through the same fetch.
    let mut w = Writer::new(klog, opts(Compression::None, 4, 1 << 20, 1 << 20)).unwrap();
    for i in 0..4u32 {
        let k = format!("key{i:06}");
        w.add(k.as_bytes(), b"value", (i + 1) as u64, 0, false, false)
            .unwrap();
    }
    w.finish().unwrap();

    let recorder = Arc::new(RecordingLimiter::default());
    let limiter: Option<Arc<dyn IoLimiter>> = Some(recorder.clone());
    let r = Reader::open_with_limiter(
        klog,
        LocalStorage::new(Arc::new(FileCache::new(16)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
        limiter,
    )
    .unwrap();

    assert_eq!(
        r.get(b"key000000", u64::MAX, 0).unwrap().0.unwrap(),
        b"value"
    );
    let first = recorder.charges();
    assert_eq!(first.len(), 1, "the fetch is one charge: {first:?}");
    assert_eq!(first[0].0, IoClass::Foreground);
    assert!(first[0].1 > 0, "the framed block length must be charged");

    for _ in 0..8 {
        assert_eq!(
            r.get(b"key000001", u64::MAX, 0).unwrap().0.unwrap(),
            b"value"
        );
    }
    assert_eq!(
        recorder.charges(),
        first,
        "re-reading a resident block must charge nothing"
    );
}

#[test]
fn written_bytes_are_charged_once() {
    // Everything the writer puts on the device is charged exactly once, and
    // the total matches the files it produced (the footer and the trailing
    // index/bloom are the documented metadata allowance).
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("2.klog");
    let klog = klog.to_str().unwrap();
    let recorder = Arc::new(RecordingLimiter::default());
    let limiter: Option<Arc<dyn IoLimiter>> = Some(recorder.clone());
    let mut w = Writer::new(klog, opts(Compression::None, 2000, 64, 1024))
        .unwrap()
        .with_limiter(limiter);
    let value = vec![b'v'; 200]; // over the klog threshold: every value goes to the vlog
    for i in 0..2000u32 {
        w.add(
            format!("key{i:06}").as_bytes(),
            &value,
            (i + 1) as u64,
            0,
            false,
            false,
        )
        .unwrap();
    }
    w.finish().unwrap();

    let charged: u64 = recorder.charges().iter().map(|(_, b)| b).sum();
    let on_disk = std::fs::metadata(klog).unwrap().len()
        + std::fs::metadata(dir.path().join("2.vlog")).unwrap().len();
    assert!(charged > 0, "the writer charged nothing");
    assert!(
        charged <= on_disk,
        "charged {charged} must not exceed the {on_disk} bytes written"
    );
    // The unaccounted remainder is the footer plus the index and bloom blocks;
    // it must be a small fraction, not most of the file.
    assert!(
        charged * 100 >= on_disk * 90,
        "charged {charged} of {on_disk} written bytes — the write path is \
         missing a charge point"
    );
}

#[test]
fn large_write_charges_in_bounded_chunks() {
    // A value far larger than any plausible bucket capacity must still be
    // admitted, in pieces, rather than deadlocking on a charge nothing can pay.
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("3.klog");
    let klog = klog.to_str().unwrap();
    let recorder = Arc::new(RecordingLimiter::default());
    let limiter: Option<Arc<dyn IoLimiter>> = Some(recorder.clone());
    let mut w = Writer::new(klog, opts(Compression::None, 1, 64, 1024))
        .unwrap()
        .with_limiter(limiter);
    let huge = vec![b'x'; (MAX_CHARGE_CHUNK as usize) * 3 + 4096];
    w.add(b"big", &huge, 1, 0, false, false).unwrap();
    w.finish().unwrap();

    let charges = recorder.charges();
    assert!(
        charges.iter().all(|(_, b)| *b <= MAX_CHARGE_CHUNK),
        "no single charge may exceed MAX_CHARGE_CHUNK: {charges:?}"
    );
    let charged: u64 = charges.iter().map(|(_, b)| b).sum();
    assert!(
        charged >= huge.len() as u64,
        "the whole value must be charged: {charged} < {}",
        huge.len()
    );
}

#[test]
fn vlog_reads_are_charged() {
    // Separated values are read straight from the vlog, outside the block
    // cache on the buffered path; that IO must be paced too.
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("4.klog");
    let klog = klog.to_str().unwrap();
    let mut w = Writer::new(klog, opts(Compression::None, 8, 64, 1024)).unwrap();
    let value = vec![b'v'; 4096];
    for i in 0..8u32 {
        w.add(
            format!("key{i:06}").as_bytes(),
            &value,
            (i + 1) as u64,
            0,
            false,
            false,
        )
        .unwrap();
    }
    w.finish().unwrap();

    let recorder = Arc::new(RecordingLimiter::default());
    let limiter: Option<Arc<dyn IoLimiter>> = Some(recorder.clone());
    let r = Reader::open_with_limiter(
        klog,
        LocalStorage::new(Arc::new(FileCache::new(16)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        4,
        default_comparator(),
        0,
        limiter,
    )
    .unwrap();
    assert_eq!(
        r.get(b"key000003", u64::MAX, 0).unwrap().0.unwrap().len(),
        4096
    );
    let charged: u64 = recorder.charges().iter().map(|(_, b)| b).sum();
    assert!(
        charged >= 4096,
        "the vlog frame must be charged: {charged} bytes"
    );
}

/// `bloom_fpr: None` means "write no filter block" — distinct from
/// `enable_bloom: false` (the family-wide switch) only in where the decision
/// comes from, but identical in the bytes it produces.
#[test]
fn writer_omits_bloom_block_when_fpr_is_none() {
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("1.klog");
    let klog = klog.to_str().unwrap();

    let mut options = opts(Compression::None, 2048, 512, 1024);
    options.bloom_fpr = None;
    let mut w = Writer::new(klog, options).unwrap();
    for i in 0..2048u32 {
        let k = format!("key{i:06}");
        w.add(k.as_bytes(), b"v", (i + 1) as u64, 0, false, false)
            .unwrap();
    }
    w.finish().unwrap();

    let reader = Reader::open(
        klog,
        LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
    )
    .unwrap();

    // The filter is ABSENT, not empty: an empty filter still occupies resident
    // bytes, and would answer "no" for the absent key below.
    let (_index, bloom, _entries) = reader.resident_breakdown();
    assert_eq!(bloom, 0, "no filter block should have been written");

    // A key that was never written is still admitted — a missing filter means
    // "may contain", which is what keeps a filterless table readable.
    let (value, _seq, found, deleted) = reader.get(b"absent-key", u64::MAX, 0).unwrap();
    assert!(!found, "the absent key must not resolve");
    assert!(!deleted);
    assert!(value.is_none());

    // And every written key still reads back.
    for i in 0..2048u32 {
        let k = format!("key{i:06}");
        let (value, _seq, found, deleted) = reader.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found && !deleted, "{k} lost");
        assert_eq!(value.as_deref(), Some(&b"v"[..]));
    }
}

/// The counterpart: with a rate set, the block is there and it rules keys out.
#[test]
fn writer_writes_a_bloom_block_when_fpr_is_some() {
    let dir = tempfile::tempdir().unwrap();
    let (reader, keys) = build_sst(dir.path(), Compression::None, 2048, 8);
    let (_index, bloom, _entries) = reader.resident_breakdown();
    assert!(bloom > 0, "a filter block should have been written");
    for k in &keys {
        let (_v, _seq, found, _d) = reader.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found, "{k} lost");
    }
}

// ---- extended entry layout (FOOTER_EXTENDED_BLOCK, 1.0-B) -------------------

/// Path of a frozen phase-1 fixture.
fn phase1_fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phase1")
        .join(name)
}

/// Every entry shape a writer produces, so the extended layout is exercised on
/// tombstones, TTLs and vlog-separated values alike.
fn extended_entries() -> Vec<(String, Vec<u8>, u64, i64, bool, bool)> {
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
        ("k05".into(), big, 15, 0, false, false),
    ];
    for i in 6..40u64 {
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

fn extended_opts(use_btree: bool, restarts: bool) -> WriterOptions {
    WriterOptions {
        compression: Compression::None,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: true,
        bloom_fpr: Some(0.01),
        klog_value_threshold: 32,
        block_size: 256,
        expected_entries: 64,
        use_btree,
        restart_interval: if restarts { 8 } else { 0 },
        extended_entries: true,
    }
}

fn open_klog(klog: &std::path::Path) -> ondadb::Result<Arc<Reader>> {
    Reader::open(
        klog.to_str().unwrap(),
        LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
    )
}

fn write_extended(klog: &std::path::Path, opts: WriterOptions) {
    let mut w = Writer::new(klog.to_str().unwrap(), opts).unwrap();
    for (k, v, seq, ttl, tomb, sdel) in extended_entries() {
        w.add(k.as_bytes(), &v, seq, ttl, tomb, sdel).unwrap();
    }
    w.finish().unwrap();
}

/// The extended layout must carry exactly the same information as the legacy
/// one, through both index shapes and with and without the restart trailer.
#[test]
fn extended_table_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    for (i, (btree, restarts)) in [(false, false), (false, true), (true, false), (true, true)]
        .into_iter()
        .enumerate()
    {
        let klog = dir.path().join(format!("{i}.klog"));
        write_extended(&klog, extended_opts(btree, restarts));
        let r =
            open_klog(&klog).unwrap_or_else(|e| panic!("btree={btree} restarts={restarts}: {e}"));
        let mut it = r.iter();
        it.seek_to_first();
        for (k, v, seq, ttl, tomb, sdel) in extended_entries() {
            assert!(it.valid(), "ended at {k}");
            assert_eq!(it.user_key(), k.as_bytes());
            assert_eq!(it.seq(), seq, "{k}");
            assert_eq!(it.ttl(), ttl, "{k}");
            assert_eq!(it.is_tombstone(), tomb, "{k}");
            assert_eq!(it.is_single_delete(), sdel, "{k}");
            assert_eq!(it.value().unwrap(), v, "{k}");
            it.next();
        }
        assert!(!it.valid());
        assert_eq!(r.num_entries(), extended_entries().len() as u64);
    }
}

/// The aux-block handle is the 16 bytes immediately before the footer, and 1.0
/// writes it empty. Asserted against the raw file so the *position* is pinned,
/// not just the value the reader reports.
#[test]
fn extended_footer_prefix_is_16_bytes() {
    const FOOTER_SIZE: usize = 64;
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("ext.klog");
    write_extended(&klog, extended_opts(false, true));
    let bytes = std::fs::read(&klog).unwrap();
    let at = bytes.len() - FOOTER_SIZE - 16;
    assert_eq!(&bytes[at..at + 8], &[0u8; 8], "aux_off");
    assert_eq!(&bytes[at + 8..at + 16], &[0u8; 8], "aux_len");
    // Footer flag 0x10 is set, and the reader reports the handle it read.
    assert_eq!(bytes[bytes.len() - FOOTER_SIZE + 48] & 0x10, 0x10);
    assert_eq!(open_klog(&klog).unwrap().aux_block_handle(), Some((0, 0)));

    // A legacy table has no such prefix at all.
    let legacy = dir.path().join("legacy.klog");
    write_extended(
        &legacy,
        WriterOptions {
            extended_entries: false,
            ..extended_opts(false, true)
        },
    );
    assert_eq!(open_klog(&legacy).unwrap().aux_block_handle(), None);
}

/// The restart trailer indexes entry *offsets*, which still come from
/// `decode_entry`'s returned `next` — so in-block binary search must land on
/// exactly what a linear scan finds.
#[test]
fn extended_table_restart_search_matches_scan() {
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("ext.klog");
    write_extended(&klog, extended_opts(false, true));
    let r = open_klog(&klog).unwrap();
    for (k, v, seq, _ttl, tomb, _sdel) in extended_entries() {
        // Point read: bloom, index, restart binary search, then the scan.
        let (value, got_seq, found, deleted) = r.get(k.as_bytes(), u64::MAX, 0).unwrap();
        assert!(found, "{k}");
        assert_eq!(got_seq, seq, "{k}");
        assert_eq!(deleted, tomb, "{k}");
        if !tomb {
            assert_eq!(value.unwrap(), v, "{k}");
        }
        // Iterator seek: the same entry, reached through the offsets index.
        let mut it = r.iter();
        it.seek(k.as_bytes(), u64::MAX);
        assert!(it.valid(), "{k}");
        assert_eq!(it.user_key(), k.as_bytes());
        assert_eq!(it.seq(), seq, "{k}");
    }
}

/// The frozen extended klog pins the wire bytes: a change to the entry layout,
/// the footer bit or the aux prefix is a test failure, not a silent break.
#[test]
fn extended_golden_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let klog = dir.path().join("ext.klog");
    let mut w = Writer::new(klog.to_str().unwrap(), extended_opts(false, true)).unwrap();
    // Same entry set as the frozen legacy corpus (`klog_entries` there).
    let big = vec![b'V'; 64];
    let mut entries: Vec<(String, Vec<u8>, u64, i64, bool, bool)> = vec![
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
        entries.push((
            format!("k{i:02}"),
            b"filler".to_vec(),
            20 + i,
            0,
            false,
            false,
        ));
    }
    for (k, v, seq, ttl, tomb, sdel) in &entries {
        w.add(k.as_bytes(), v, *seq, *ttl, *tomb, *sdel).unwrap();
    }
    w.finish().unwrap();
    assert_eq!(
        std::fs::read(&klog).unwrap(),
        std::fs::read(phase1_fixture("klog_extended.klog")).unwrap(),
        "the extended klog bytes are frozen"
    );
    assert_eq!(
        std::fs::read(klog.with_extension("vlog")).unwrap(),
        std::fs::read(phase1_fixture("klog_extended.vlog")).unwrap(),
    );
}

/// Compiling the extended layout in must not change how a legacy table reads:
/// the flag is table-level and absent, so every frozen legacy klog still
/// decodes through the legacy path.
#[test]
fn legacy_table_still_decodes_with_extended_support_compiled() {
    let dir = tempfile::tempdir().unwrap();
    for name in [
        "klog_legacy_flat_restarts_bloom.klog",
        "klog_legacy_btree_norestarts_nobloom.klog",
    ] {
        let klog = dir.path().join(name);
        std::fs::write(&klog, std::fs::read(phase1_fixture(name)).unwrap()).unwrap();
        std::fs::write(
            klog.with_extension("vlog"),
            std::fs::read(phase1_fixture(&name.replace(".klog", ".vlog"))).unwrap(),
        )
        .unwrap();
        let r = open_klog(&klog).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(r.aux_block_handle(), None, "{name}: no aux prefix");
        let mut it = r.iter();
        it.seek_to_first();
        let mut n = 0;
        while it.valid() {
            n += 1;
            it.next();
        }
        assert_eq!(n, r.num_entries(), "{name}");
    }
}
