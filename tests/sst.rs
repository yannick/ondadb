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
        bloom_fpr: 0.01,
        klog_value_threshold: klog_threshold,
        block_size,
        expected_entries: n,
        use_btree: false,
        restart_interval: 8,
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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 1, default_comparator()).unwrap();
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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 2, default_comparator()).unwrap();
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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 42, default_comparator()).unwrap();
    let res = r.get(b"a", u64::MAX, 0);
    assert!(
        res.is_err(),
        "corrupted vlog value must be rejected, got {res:?}"
    );
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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 3, default_comparator()).unwrap();

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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 7, default_comparator()).unwrap();

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
        let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 1, default_comparator()).unwrap();
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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 1, default_comparator()).unwrap();
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
    let r = Reader::open(klog, LocalStorage::new(fc, cfg!(feature = "mmap-reads")), bc, 1, default_comparator()).unwrap();
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
