//! Frozen data-block corpus: legacy and prefix-delta twins, byte for byte.
//!
//! The three klogs in `tests/fixtures/blocks/` are committed to git. They pin
//! what the block writer emits — footer flags, restart trailer, entry framing,
//! `shared_len`/`suffix_len` — so a format change has to be a deliberate,
//! reviewed regeneration rather than a silent shift.
//!
//! The tests below decode the fixtures **by hand**, from the layout documented
//! in `docs/formats.md`, rather than through `sst::decode_entry`. A golden test
//! that used the production decoder would only prove the encoder and decoder
//! agree with each other; parsing the bytes independently is what pins the
//! bytes.
//!
//! `regenerate_block_fixtures` is `#[ignore]`d on purpose — it is run by hand,
//! only for an explicit format change.
//!
//! The pre-2.1 legacy bytes are pinned separately, in `tests/fixtures/phase1/`
//! (`fixtures_phase1.rs`): 2.1 makes block-size accounting trailer-inclusive,
//! which shifts where restart-bearing blocks are cut, so the phase-1 corpus is
//! the record of what 0.8.2 wrote and this one is the record of what 2.1 does.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::default_comparator;
use ondadb::config::Compression;
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;

const FOOTER_SIZE: usize = 64;
const FOOTER_HAS_BLOOM: u8 = 0x01;
const FOOTER_RESTARTS: u8 = 0x04;
const FOOTER_VLOG_V2: u8 = 0x08;
const FOOTER_EXTENDED_BLOCK: u8 = 0x10;
const FOOTER_PREFIX_DELTA: u8 = 0x20;
/// `[alg u8][comp_len u32 LE][raw_len u32 LE][crc32c u32 LE]` (see `block.rs`).
const BLOCK_HEADER: usize = 13;

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/blocks")
}

fn fixture(name: &str) -> Vec<u8> {
    let path = fixture_dir().join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()))
}

/// `(key, value, seq, ttl, tombstone, single_delete)`.
type Entry = (Vec<u8>, Vec<u8>, u64, i64, bool, bool);
/// One entry as a full scan reports it.
type Scanned = (Vec<u8>, u64, bool, bool, i64, Vec<u8>);

/// The entries every fixture holds: prefix-heavy keys (the documented spada
/// shape), every writer-produced flag combination, and enough filler to span
/// several 4 KiB blocks.
fn entries() -> Vec<Entry> {
    let mut v: Vec<Entry> = Vec::new();
    for tenant in 0..4u32 {
        for segment in 0..50u32 {
            let key = format!("tenant/{tenant:03}/cluster/aa/segment/{segment:05}").into_bytes();
            let seq = (tenant as u64) * 100 + segment as u64 + 1;
            match segment % 5 {
                1 => v.push((key, Vec::new(), seq, 0, true, false)), // tombstone
                2 => v.push((key, Vec::new(), seq, 0, true, true)),  // single delete
                3 => v.push((key, b"ttl-value".to_vec(), seq, 1_700_000_000, false, false)),
                _ => v.push((key, b"value-0123456789".to_vec(), seq, 0, false, false)),
            }
        }
    }
    v
}

fn options(restart_interval: usize, prefix_delta: bool) -> WriterOptions {
    WriterOptions {
        // No compression: the fixture bytes must not move when a compression
        // dependency changes its output.
        compression: Compression::None,
        compression_rules: Vec::new(),
        cmp: default_comparator(),
        enable_bloom: false,
        bloom_fpr: None,
        klog_value_threshold: 1 << 20, // every value stays inline; no vlog
        block_size: 4096,
        expected_entries: 200,
        use_btree: false,
        restart_interval,
        extended_entries: false,
        prefix_delta,
    }
}

fn variants() -> [(&'static str, WriterOptions); 3] {
    [
        ("legacy_restarts.klog", options(8, false)),
        ("legacy_no_trailer.klog", options(0, false)),
        ("delta.klog", options(8, true)),
    ]
}

fn write_klog(path: &Path, opts: WriterOptions) {
    let mut w = Writer::new(path.to_str().unwrap(), opts).unwrap();
    for (k, v, seq, ttl, tomb, sdel) in entries() {
        w.add(&k, &v, seq, ttl, ondadb::format::point_kind(tomb, sdel))
            .unwrap();
    }
    w.finish().unwrap();
}

#[test]
#[ignore = "regenerates committed fixtures; run manually"]
fn regenerate_block_fixtures() {
    let dir = fixture_dir();
    std::fs::create_dir_all(&dir).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    for (name, opts) in variants() {
        let path = tmp.path().join(name);
        write_klog(&path, opts);
        std::fs::copy(&path, dir.join(name)).unwrap();
    }
}

/// Regenerate into a temp dir and compare with the committed bytes.
fn assert_frozen(name: &str, opts: WriterOptions) -> Vec<u8> {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join(name);
    write_klog(&path, opts);
    let produced = std::fs::read(&path).unwrap();
    let committed = fixture(name);
    assert_eq!(
        produced.len(),
        committed.len(),
        "{name}: length moved ({} -> {}); if this is a deliberate format \
         change, run `cargo test --test golden_blocks -- --ignored`",
        committed.len(),
        produced.len()
    );
    let diff = produced.iter().zip(&committed).position(|(a, b)| a != b);
    assert_eq!(diff, None, "{name}: bytes diverge at offset {diff:?}");
    committed
}

fn open(name: &str) -> Arc<Reader> {
    Reader::open(
        fixture_dir().join(name).to_str().unwrap(),
        LocalStorage::new(Arc::new(FileCache::new(4)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(1 << 20)),
        1,
        default_comparator(),
        0,
    )
    .unwrap()
}

fn footer_flags(bytes: &[u8]) -> u8 {
    bytes[bytes.len() - FOOTER_SIZE + 48]
}

/// Every entry of the table, read back through the public reader.
fn scanned(name: &str) -> Vec<Scanned> {
    let r = open(name);
    let mut it = r.iter();
    it.seek_to_first();
    let mut out = Vec::new();
    while it.valid() {
        out.push((
            it.user_key().to_vec(),
            it.seq(),
            it.is_tombstone(),
            it.is_single_delete(),
            it.ttl(),
            it.value().unwrap(),
        ));
        it.next();
    }
    assert_eq!(it.err().map(|e| e.to_string()), None, "{name}: scan error");
    out
}

fn expected_scan() -> Vec<Scanned> {
    entries()
        .into_iter()
        .map(|(k, v, seq, ttl, tomb, sdel)| (k, seq, tomb, sdel, ttl, v))
        .collect()
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

/// The first data block of `bytes`, split into `(entries, restart offsets)`.
/// The block is uncompressed in every fixture, so the payload is the raw block.
fn first_block(bytes: &[u8], has_restarts: bool) -> (&[u8], Vec<u32>) {
    let raw_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let block = &bytes[BLOCK_HEADER..BLOCK_HEADER + raw_len];
    if !has_restarts {
        return (block, Vec::new());
    }
    let count = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap()) as usize;
    let trailer = count * 4 + 4;
    let end = block.len() - trailer;
    let restarts = (0..count)
        .map(|i| u32::from_le_bytes(block[end + i * 4..end + i * 4 + 4].try_into().unwrap()))
        .collect();
    (&block[..end], restarts)
}

#[test]
fn legacy_restart_block_bytes_are_frozen() {
    let bytes = assert_frozen("legacy_restarts.klog", options(8, false));
    assert_eq!(
        footer_flags(&bytes),
        FOOTER_VLOG_V2 | FOOTER_RESTARTS,
        "legacy restart table: no bloom, no btree, no extended, no delta"
    );
    assert_eq!(footer_flags(&bytes) & FOOTER_HAS_BLOOM, 0);

    let (entries_region, restarts) = first_block(&bytes, true);
    assert_eq!(restarts[0], 0, "the first anchor is at offset 0");
    assert!(restarts.len() >= 4, "anchors: {restarts:?}");
    for pair in restarts.windows(2) {
        assert!(pair[0] < pair[1], "anchors must increase: {restarts:?}");
    }
    assert!((*restarts.last().unwrap() as usize) < entries_region.len());

    // Entry 0, decoded by hand: flags | key_len | val_len | seq | key | value.
    let e = entries_region;
    assert_eq!(e[0], 0, "a plain put has no flag bits set");
    let (klen, n) = uvarint(e, 1);
    let (vlen, m) = uvarint(e, 1 + n);
    let (seq, o) = uvarint(e, 1 + n + m);
    assert_eq!(klen, 35, "the full user key is stored");
    assert_eq!(vlen, 16);
    assert_eq!(seq, 1);
    let key_at = 1 + n + m + o;
    assert_eq!(
        &e[key_at..key_at + klen as usize],
        &b"tenant/000/cluster/aa/segment/00000"[..],
        "entry 0 stores its whole key"
    );

    assert_eq!(scanned("legacy_restarts.klog"), expected_scan());
}

#[test]
fn legacy_no_trailer_block_bytes_are_frozen() {
    let bytes = assert_frozen("legacy_no_trailer.klog", options(0, false));
    assert_eq!(
        footer_flags(&bytes),
        FOOTER_VLOG_V2,
        "no restart trailer means no FOOTER_RESTARTS"
    );
    let (entries_region, restarts) = first_block(&bytes, false);
    assert!(restarts.is_empty());
    assert_eq!(entries_region[0], 0, "entries start immediately");
    assert_eq!(scanned("legacy_no_trailer.klog"), expected_scan());
}

#[test]
fn delta_block_bytes_are_frozen() {
    let bytes = assert_frozen("delta.klog", options(8, true));
    assert_eq!(
        footer_flags(&bytes),
        FOOTER_VLOG_V2 | FOOTER_RESTARTS | FOOTER_EXTENDED_BLOCK | FOOTER_PREFIX_DELTA,
        "a delta table is an extended table with restarts"
    );

    let (e, restarts) = first_block(&bytes, true);
    assert_eq!(restarts[0], 0);
    assert!(restarts.len() >= 4, "anchors: {restarts:?}");

    // Entry 0 (an anchor): kind | modifiers | shared_len | suffix_len |
    // val_len | seq | suffix | value.
    let (kind, n0) = uvarint(e, 0);
    let (mods, n1) = uvarint(e, n0);
    let (shared, n2) = uvarint(e, n0 + n1);
    let (suffix_len, n3) = uvarint(e, n0 + n1 + n2);
    let (vlen, n4) = uvarint(e, n0 + n1 + n2 + n3);
    let (seq, n5) = uvarint(e, n0 + n1 + n2 + n3 + n4);
    assert_eq!(kind, 1, "KIND_PUT");
    assert_eq!(mods, 0);
    assert_eq!(shared, 0, "a restart anchor shares nothing");
    assert_eq!(suffix_len, 35, "so it stores the whole key");
    assert_eq!(vlen, 16);
    assert_eq!(seq, 1);
    let at = n0 + n1 + n2 + n3 + n4 + n5;
    assert_eq!(&e[at..at + 35], &b"tenant/000/cluster/aa/segment/00000"[..]);

    // Entry 1 shares the whole key but its last digit.
    let next = at + 35 + 16;
    let (kind, n0) = uvarint(e, next);
    let (_, n1) = uvarint(e, next + n0);
    let (shared, n2) = uvarint(e, next + n0 + n1);
    let (suffix_len, _) = uvarint(e, next + n0 + n1 + n2);
    assert_eq!(kind, 2, "KIND_DELETE: segment 1 is a tombstone");
    assert_eq!(shared, 34, "34 of 35 key bytes are shared");
    assert_eq!(suffix_len, 1, "and only the diverging byte is stored");

    // Every offset the restart array names is self-contained.
    for &off in &restarts {
        let off = off as usize;
        let (_, n0) = uvarint(e, off);
        let (_, n1) = uvarint(e, off + n0);
        let (shared, _) = uvarint(e, off + n0 + n1);
        assert_eq!(shared, 0, "anchor at {off} shares a prefix");
    }

    assert_eq!(scanned("delta.klog"), expected_scan());
}

/// The whole point of the encoding: the same entries, materially fewer key
/// bytes on disk.
#[test]
fn delta_blocks_are_smaller_than_their_legacy_twin() {
    let legacy = fixture("legacy_restarts.klog").len();
    let delta = fixture("delta.klog").len();
    assert!(
        delta < legacy,
        "delta table ({delta} B) is not smaller than its legacy twin ({legacy} B)"
    );
}
