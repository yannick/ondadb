//! Feature 2.1 — restart-based prefix-delta data blocks.
//!
//! The centrepiece is a round-trip oracle: every table is written twice, once
//! legacy and once delta, from the same entries, and every read path is asserted
//! to agree entry for entry. Legacy is the oracle because it is the format in
//! production — a delta table that answers differently is wrong by definition,
//! whichever of the two the bug is in.
//!
//! Key shapes are deliberate: prefix-heavy (`tenant/cluster/segment`, the
//! documented spada shape), random (where sharing buys nothing and the +1
//! byte/entry floor cost is paid), and adversarial — long shared prefixes,
//! `0xFF`-heavy bytes, keys **shorter than 8 bytes** so `key_prefix8`'s
//! zero padding is exercised on a reconstructed key, and keys differing only in
//! their last byte.

use std::sync::Arc;
use std::time::Duration;

use ondadb::cache::{BlockCache, FileCache};
use ondadb::comparator::{default_comparator, Comparator, ComparatorRef};
use ondadb::config::Compression;
use ondadb::format::{CAP_EXTENDED_RECORDS, CAP_PREFIX_DELTA};
use ondadb::sst::{Reader, Writer, WriterOptions};
use ondadb::storage::LocalStorage;
use ondadb::{ColumnFamilyConfig, Options, DB};

const FOOTER_SIZE: usize = 64;
const FOOTER_PREFIX_DELTA: u8 = 0x20;
const FOOTER_EXTENDED_BLOCK: u8 = 0x10;
/// `[alg u8][comp_len u32 LE][raw_len u32 LE][crc32c u32 LE]`.
const BLOCK_HEADER: usize = 13;

// ---- key corpora ------------------------------------------------------------

fn prefix_heavy() -> Vec<Vec<u8>> {
    let mut v = Vec::new();
    for tenant in 0..6u32 {
        for cluster in 0..4u32 {
            for segment in 0..30u32 {
                v.push(
                    format!("tenant/{tenant:04}/cluster/{cluster:04}/segment/{segment:06}")
                        .into_bytes(),
                );
            }
        }
    }
    v
}

fn random_keys() -> Vec<Vec<u8>> {
    // A deterministic xorshift, so a failure reproduces exactly.
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut v: Vec<Vec<u8>> = (0..600)
        .map(|_| {
            let n = 4 + (next() % 28) as usize;
            (0..n).map(|_| next() as u8).collect()
        })
        .collect();
    v.sort();
    v.dedup();
    v
}

fn adversarial_keys() -> Vec<Vec<u8>> {
    let mut v: Vec<Vec<u8>> = Vec::new();
    // Keys shorter than the 8-byte `key_prefix8` window, including the empty
    // key: reconstruction must pad the prefix exactly as a stored key does.
    v.push(Vec::new());
    for b in b'a'..=b'z' {
        v.push(vec![b]);
        v.push(vec![b, b]);
        v.push(vec![b, b, b, b, b, b, b]);
    }
    // A long shared prefix with only the last byte differing.
    for i in 0..=255u8 {
        let mut k = vec![b'p'; 200];
        k.push(i);
        v.push(k);
    }
    // 0xFF-heavy, where a naive "increment the diverging byte" would overflow.
    for i in 0..64u8 {
        let mut k = vec![0xffu8; 12];
        k.push(i);
        k.extend_from_slice(&[0xff; 4]);
        v.push(k);
    }
    // A key that is a strict prefix of the next, in both directions.
    v.push(b"prefix".to_vec());
    v.push(b"prefixed".to_vec());
    v.push(b"prefixed-further".to_vec());
    v.sort();
    v.dedup();
    v
}

/// `(key, value, seq, ttl, tombstone, single_delete)` for a key corpus, cycling
/// through every writer-producible entry shape.
type Entry = (Vec<u8>, Vec<u8>, u64, i64, bool, bool);
/// One entry as a full forward scan reports it.
type Scanned = (Vec<u8>, u64, bool, bool, i64, Vec<u8>);

fn entries_for(keys: &[Vec<u8>], big_values: bool) -> Vec<Entry> {
    keys.iter()
        .enumerate()
        .map(|(i, k)| {
            let seq = i as u64 + 1;
            match i % 6 {
                1 => (k.clone(), Vec::new(), seq, 0, true, false),
                2 => (k.clone(), Vec::new(), seq, 0, true, true),
                3 => (k.clone(), b"ttl".to_vec(), seq, 1_900_000_000, false, false),
                4 if big_values => (k.clone(), vec![b'B'; 4096], seq, 0, false, false),
                _ => (
                    k.clone(),
                    format!("value-{i:06}").into_bytes(),
                    seq,
                    0,
                    false,
                    false,
                ),
            }
        })
        .collect()
}

// ---- table construction -----------------------------------------------------

fn opts(
    prefix_delta: bool,
    restart_interval: usize,
    block_size: usize,
    cmp: ComparatorRef,
) -> WriterOptions {
    WriterOptions {
        compression: Compression::None,
        compression_rules: Vec::new(),
        cmp,
        enable_bloom: true,
        bloom_fpr: Some(0.01),
        klog_value_threshold: 512, // the big-value shape lands in the vlog
        block_size,
        expected_entries: 1024,
        use_btree: false,
        restart_interval,
        extended_entries: false,
        prefix_delta,
    }
}

fn build(path: &str, entries: &[Entry], o: WriterOptions) {
    let mut w = Writer::new(path, o).unwrap();
    for (k, v, seq, ttl, tomb, sdel) in entries {
        w.add(k, v, *seq, *ttl, *tomb, *sdel).unwrap();
    }
    w.finish().unwrap();
}

fn open_table(path: &str, cmp: ComparatorRef) -> Arc<Reader> {
    Reader::open(
        path,
        LocalStorage::new(Arc::new(FileCache::new(8)), cfg!(feature = "mmap-reads")),
        Arc::new(BlockCache::new(4 << 20)),
        7,
        cmp,
        0,
    )
    .unwrap()
}

/// A legacy/delta twin pair over the same entries.
struct Twins {
    _dir: tempfile::TempDir,
    legacy: Arc<Reader>,
    delta: Arc<Reader>,
    entries: Vec<Entry>,
}

fn twins(keys: &[Vec<u8>], restart_interval: usize, block_size: usize) -> Twins {
    twins_with(keys, restart_interval, block_size, default_comparator(), true)
}

fn twins_with(
    keys: &[Vec<u8>],
    restart_interval: usize,
    block_size: usize,
    cmp: ComparatorRef,
    big_values: bool,
) -> Twins {
    let dir = tempfile::tempdir().unwrap();
    let entries = entries_for(keys, big_values);
    let legacy_path = dir.path().join("legacy.klog");
    let delta_path = dir.path().join("delta.klog");
    build(
        legacy_path.to_str().unwrap(),
        &entries,
        opts(false, restart_interval, block_size, cmp.clone()),
    );
    build(
        delta_path.to_str().unwrap(),
        &entries,
        opts(true, restart_interval, block_size, cmp.clone()),
    );
    let legacy = open_table(legacy_path.to_str().unwrap(), cmp.clone());
    let delta = open_table(delta_path.to_str().unwrap(), cmp);
    Twins {
        _dir: dir,
        legacy,
        delta,
        entries,
    }
}

/// Every (interval, block size) shape the round-trip oracle runs at. Small
/// blocks force many boundaries; a large interval makes runs long.
const SHAPES: [(usize, usize); 4] = [(1, 256), (4, 512), (8, 4096), (32, 1024)];

fn corpora() -> Vec<(&'static str, Vec<Vec<u8>>)> {
    vec![
        ("prefix-heavy", prefix_heavy()),
        ("random", random_keys()),
        ("adversarial", adversarial_keys()),
    ]
}

// ---- oracle helpers ---------------------------------------------------------

fn scan_forward(r: &Arc<Reader>) -> Vec<Scanned> {
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
    assert_eq!(it.err().map(|e| e.to_string()), None, "forward scan error");
    out
}

fn scan_reverse(r: &Arc<Reader>) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
    let mut it = r.iter();
    it.seek_to_last();
    let mut out = Vec::new();
    while it.valid() {
        out.push((it.user_key().to_vec(), it.seq(), it.value().unwrap()));
        it.prev();
    }
    assert_eq!(it.err().map(|e| e.to_string()), None, "reverse scan error");
    out
}

/// `(key, seq)` at the position `seek(key, seq)` lands on, or `None` when it
/// runs off the end.
fn seek_at(r: &Arc<Reader>, key: &[u8], seq: u64) -> Option<(Vec<u8>, u64)> {
    let mut it = r.iter();
    it.seek(key, seq);
    assert_eq!(it.err().map(|e| e.to_string()), None, "seek error");
    it.valid().then(|| (it.user_key().to_vec(), it.seq()))
}

fn seek_for_prev_at(r: &Arc<Reader>, key: &[u8], seq: u64) -> Option<(Vec<u8>, u64)> {
    let mut it = r.iter();
    it.seek_for_prev(key, seq);
    assert_eq!(it.err().map(|e| e.to_string()), None, "seek_for_prev error");
    it.valid().then(|| (it.user_key().to_vec(), it.seq()))
}

// ---- task 7: point reads ----------------------------------------------------

#[test]
fn delta_point_get_matches_legacy_twin() {
    for (name, keys) in corpora() {
        for (interval, block_size) in SHAPES {
            let t = twins(&keys, interval, block_size);
            assert!(t.delta.is_prefix_delta());
            assert!(!t.legacy.is_prefix_delta());
            for (k, ..) in &t.entries {
                for seq in [u64::MAX, 1, 50] {
                    let want = t.legacy.get(k, seq, 0).unwrap();
                    let got = t.delta.get(k, seq, 0).unwrap();
                    assert_eq!(
                        got, want,
                        "{name} r={interval} b={block_size}: get({:?}, {seq})",
                        String::from_utf8_lossy(k)
                    );
                }
            }
        }
    }
}

/// Every position within a restart run has a different decode path length —
/// the anchor is self-contained, the rest are reconstructed by walking to them.
#[test]
fn delta_point_get_finds_key_at_every_run_position() {
    let keys = prefix_heavy();
    for interval in [1usize, 2, 4, 8, 32] {
        let t = twins(&keys, interval, 1024);
        for (k, v, seq, ttl, tomb, _) in &t.entries {
            let (value, got_seq, found, deleted) = t.delta.get(k, u64::MAX, 0).unwrap();
            assert!(found, "r={interval}: missing {:?}", String::from_utf8_lossy(k));
            assert_eq!(got_seq, *seq);
            // `now = 0` is before every TTL in the corpus, so only the
            // tombstones read as deleted.
            let _ = ttl;
            assert_eq!(deleted, *tomb, "r={interval}");
            if !*tomb {
                assert_eq!(value.as_deref(), Some(v.as_slice()), "r={interval}");
            }
        }
    }
}

#[test]
fn delta_point_get_misses_between_keys() {
    let keys = prefix_heavy();
    let t = twins(&keys, 8, 1024);
    let mut probes: Vec<Vec<u8>> = Vec::new();
    for k in keys.iter().take(80) {
        // Just before and just after each key, plus a same-prefix sibling that
        // was never written.
        let mut before = k.clone();
        before.pop();
        probes.push(before);
        let mut after = k.clone();
        after.push(0);
        probes.push(after);
    }
    probes.push(b"aaaaa".to_vec());
    probes.push(b"zzzzz".to_vec());
    probes.push(Vec::new());
    for probe in &probes {
        let want = t.legacy.get(probe, u64::MAX, 0).unwrap();
        let got = t.delta.get(probe, u64::MAX, 0).unwrap();
        assert_eq!(got, want, "probe {:?}", String::from_utf8_lossy(probe));
    }
}

// ---- task 8: seek -----------------------------------------------------------

#[test]
fn delta_seek_matches_legacy_twin_at_every_key() {
    for (name, keys) in corpora() {
        for (interval, block_size) in SHAPES {
            let t = twins(&keys, interval, block_size);
            for (k, _, seq, ..) in &t.entries {
                for probe_seq in [u64::MAX, *seq, seq.saturating_sub(1), 0] {
                    assert_eq!(
                        seek_at(&t.delta, k, probe_seq),
                        seek_at(&t.legacy, k, probe_seq),
                        "{name} r={interval} b={block_size}: seek({:?}, {probe_seq})",
                        String::from_utf8_lossy(k)
                    );
                    assert_eq!(
                        seek_for_prev_at(&t.delta, k, probe_seq),
                        seek_for_prev_at(&t.legacy, k, probe_seq),
                        "{name} r={interval} b={block_size}: seek_for_prev({:?}, {probe_seq})",
                        String::from_utf8_lossy(k)
                    );
                }
            }
        }
    }
}

/// A target past the last entry of its block must land on the first entry of
/// the next one, not go invalid.
#[test]
fn delta_seek_past_block_end_advances_to_next_block() {
    let keys = prefix_heavy();
    let t = twins(&keys, 4, 256);
    assert!(
        t.delta.data_block_count() > 8,
        "the table must span many blocks"
    );
    // Probe strictly between consecutive keys, which is where a block-end
    // overrun shows up.
    let mut crossings = 0usize;
    for pair in keys.windows(2) {
        let mut probe = pair[0].clone();
        probe.push(0xff);
        let got = seek_at(&t.delta, &probe, u64::MAX);
        let want = seek_at(&t.legacy, &probe, u64::MAX);
        assert_eq!(got, want, "probe {:?}", String::from_utf8_lossy(&probe));
        if got.as_ref().map(|(k, _)| k.as_slice()) == Some(pair[1].as_slice()) {
            crossings += 1;
        }
    }
    assert!(crossings > 100, "the probes never crossed a key: {crossings}");
    // Past the very last key: both go invalid.
    let mut past = keys.last().unwrap().clone();
    past.push(0xff);
    assert_eq!(seek_at(&t.delta, &past, u64::MAX), None);
    assert_eq!(seek_at(&t.legacy, &past, u64::MAX), None);
}

#[test]
fn delta_seek_to_last_matches_legacy_twin() {
    for (name, keys) in corpora() {
        for (interval, block_size) in SHAPES {
            let t = twins(&keys, interval, block_size);
            let mut a = t.delta.iter();
            let mut b = t.legacy.iter();
            a.seek_to_last();
            b.seek_to_last();
            assert!(a.valid() && b.valid(), "{name}");
            assert_eq!(
                (a.user_key().to_vec(), a.seq(), a.value().unwrap()),
                (b.user_key().to_vec(), b.seq(), b.value().unwrap()),
                "{name} r={interval} b={block_size}"
            );
        }
    }
}

// ---- task 9: forward and reverse iteration ----------------------------------

#[test]
fn delta_forward_scan_matches_legacy_twin() {
    for (name, keys) in corpora() {
        for (interval, block_size) in SHAPES {
            let t = twins(&keys, interval, block_size);
            let want = scan_forward(&t.legacy);
            assert_eq!(want.len(), t.entries.len(), "{name}: oracle is complete");
            assert_eq!(
                scan_forward(&t.delta),
                want,
                "{name} r={interval} b={block_size}"
            );
        }
    }
}

#[test]
fn delta_reverse_scan_matches_legacy_twin() {
    for (name, keys) in corpora() {
        for (interval, block_size) in SHAPES {
            let t = twins(&keys, interval, block_size);
            let want = scan_reverse(&t.legacy);
            assert_eq!(want.len(), t.entries.len(), "{name}: oracle is complete");
            assert_eq!(
                scan_reverse(&t.delta),
                want,
                "{name} r={interval} b={block_size}"
            );
        }
    }
}

/// Reverse stepping is where the run cursor earns its keep: within a run, back
/// across a run boundary, and back across a block boundary into the *last* run
/// of the previous block.
#[test]
fn delta_reverse_scan_crosses_run_and_block_boundaries() {
    let keys = prefix_heavy();
    // interval 4 with 256-byte blocks gives many runs and many blocks.
    let t = twins(&keys, 4, 256);
    assert!(t.delta.data_block_count() > 10, "many blocks");
    let forward = scan_forward(&t.delta);
    let reverse = scan_reverse(&t.delta);
    let mut want = forward.clone();
    want.reverse();
    assert_eq!(
        reverse
            .iter()
            .map(|(k, s, v)| (k.clone(), *s, v.clone()))
            .collect::<Vec<_>>(),
        want.iter()
            .map(|(k, s, _, _, _, v)| (k.clone(), *s, v.clone()))
            .collect::<Vec<_>>()
    );
    // Enter each block by seeking to its first key, then step back one: that
    // is the block-boundary path taken deliberately rather than incidentally.
    let mut crossed = 0usize;
    for (i, (k, ..)) in forward.iter().enumerate().skip(1) {
        let mut it = t.delta.iter();
        it.seek(k, u64::MAX);
        assert!(it.valid());
        it.prev();
        assert!(it.valid(), "prev from {:?}", String::from_utf8_lossy(k));
        assert_eq!(it.user_key(), forward[i - 1].0.as_slice());
        assert_eq!(it.seq(), forward[i - 1].1);
        crossed += 1;
    }
    assert_eq!(crossed, forward.len() - 1);
}

#[test]
fn delta_alternating_next_prev_is_stable() {
    let keys = adversarial_keys();
    let t = twins(&keys, 4, 512);
    let forward = scan_forward(&t.delta);
    let mut it = t.delta.iter();
    it.seek_to_first();
    let mut i = 0usize;
    // next, next, prev, repeat: every step must land where the forward scan says.
    while i + 2 < forward.len() {
        it.next();
        it.next();
        it.prev();
        i += 1;
        assert!(it.valid(), "went invalid at {i}");
        assert_eq!(it.user_key(), forward[i].0.as_slice(), "at {i}");
        assert_eq!(it.seq(), forward[i].1, "at {i}");
    }
    // ...and walking all the way back reaches the first entry.
    while i > 0 {
        it.prev();
        i -= 1;
        assert!(it.valid());
        assert_eq!(it.user_key(), forward[i].0.as_slice(), "back at {i}");
    }
    it.prev();
    assert!(!it.valid(), "stepping before the first entry ends the walk");
}

// ---- comparator agnosticism -------------------------------------------------

/// Prefix sharing is a representation choice; order comes from the comparator.
/// AGENTS.md invariant 7 is untouched, and this pins it.
#[derive(Debug)]
struct ReverseComparator;

impl Comparator for ReverseComparator {
    fn name(&self) -> &str {
        "test.reverse"
    }
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        b.cmp(a)
    }
    fn is_bytewise(&self) -> bool {
        false
    }
}

#[test]
fn delta_round_trips_under_a_non_bytewise_comparator() {
    let cmp: ComparatorRef = Arc::new(ReverseComparator);
    let mut keys = prefix_heavy();
    // The writer requires the CF's own order.
    keys.sort_by(|a, b| cmp.compare(a, b));
    for (interval, block_size) in SHAPES {
        let t = twins_with(&keys, interval, block_size, cmp.clone(), false);
        assert_eq!(scan_forward(&t.delta), scan_forward(&t.legacy));
        assert_eq!(scan_reverse(&t.delta), scan_reverse(&t.legacy));
        for (k, ..) in t.entries.iter().take(120) {
            assert_eq!(
                t.delta.get(k, u64::MAX, 0).unwrap(),
                t.legacy.get(k, u64::MAX, 0).unwrap()
            );
            assert_eq!(seek_at(&t.delta, k, u64::MAX), seek_at(&t.legacy, k, u64::MAX));
        }
    }
}

// ---- task 11: corruption matrix ---------------------------------------------

/// Read a delta klog, hand `mutate` its first data block's `(entries,
/// restarts)` regions, rewrite the block with a matching CRC, and return the
/// error a full read produces. The CRC is recomputed on purpose: the point is
/// to test the *decoder's* validation rules, not the checksum that already
/// covers them.
fn mutated_delta_error(mutate: impl FnOnce(&mut Vec<u8>, usize) -> usize) -> (String, String) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d.klog");
    let path = path.to_str().unwrap();
    let entries = entries_for(&prefix_heavy(), false);
    build(path, &entries, opts(true, 8, 4096, default_comparator()));

    let mut file = std::fs::read(path).unwrap();
    let raw_len = u32::from_le_bytes(file[5..9].try_into().unwrap()) as usize;
    let mut block = file[BLOCK_HEADER..BLOCK_HEADER + raw_len].to_vec();
    let new_len = mutate(&mut block, raw_len);
    assert_eq!(new_len, block.len(), "mutation must report its new length");
    assert_eq!(
        block.len(),
        raw_len,
        "mutations keep the block length so the framing stays valid"
    );
    file.splice(
        BLOCK_HEADER..BLOCK_HEADER + raw_len,
        block.iter().copied(),
    );
    // Re-checksum so the block framing accepts it and the entry decoder is
    // what refuses.
    let crc = ondadb::encoding::checksum(&file[BLOCK_HEADER..BLOCK_HEADER + raw_len]);
    file[9..13].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(path, &file).unwrap();

    let r = open_table(path, default_comparator());
    // Any of the read paths must refuse; take the first error one produces.
    let mut it = r.iter();
    it.seek_to_first();
    while it.valid() {
        it.next();
    }
    if let Some(e) = it.err() {
        return (e.kind().to_string(), e.to_string());
    }
    let mut it = r.iter();
    it.seek_to_last();
    while it.valid() {
        it.prev();
    }
    if let Some(e) = it.err() {
        return (e.kind().to_string(), e.to_string());
    }
    for (k, ..) in &entries {
        if let Err(e) = r.get(k, u64::MAX, 0) {
            return (e.kind().to_string(), e.to_string());
        }
    }
    panic!("the mutation was accepted by every read path");
}

/// The restart array of a block: `(entries_end, count)`.
fn split_trailer(block: &[u8]) -> (usize, usize) {
    let count = u32::from_le_bytes(block[block.len() - 4..].try_into().unwrap()) as usize;
    (block.len() - (count * 4 + 4), count)
}

/// Rule 1: `shared_len` may not exceed the previous key's length.
#[test]
fn delta_rejects_shared_longer_than_prev_key() {
    let e = mutated_delta_error(|block, len| {
        let (end, _) = split_trailer(block);
        // Walk to the second entry (the first non-anchor) and blow up its
        // shared_len. Entry 0 is at offset 0: kind, mods, shared(0), suffix_len.
        let mut p = 0usize;
        // kind + mods + shared + suffix_len + val_len + seq of entry 0
        let mut fields = Vec::new();
        for _ in 0..6 {
            let (v, n) = read_uvarint(block, p);
            fields.push(v);
            p += n;
        }
        let next = p + fields[3] as usize + fields[4] as usize;
        assert!(next < end);
        // Entry 1's shared_len is its third uvarint; it is single-byte for a
        // key under 128 bytes, so raising it keeps the block length.
        let mut q = next;
        for _ in 0..2 {
            q += read_uvarint(block, q).1;
        }
        assert_eq!(read_uvarint(block, q).1, 1, "single-byte shared_len");
        block[q] = 120; // far past any predecessor in this corpus
        len
    });
    assert_eq!(e.0, "corruption", "{}", e.1);
}

/// Rule 2: every offset the restart array names must be self-contained.
#[test]
fn delta_rejects_nonzero_shared_at_restart() {
    let e = mutated_delta_error(|block, len| {
        let (end, count) = split_trailer(block);
        assert!(count >= 2);
        // The second anchor, so the first run still materializes and the
        // corrupt anchor is reached by an ordinary forward walk.
        let at = u32::from_le_bytes(block[end + 4..end + 8].try_into().unwrap()) as usize;
        let mut q = at;
        for _ in 0..2 {
            q += read_uvarint(block, q).1;
        }
        assert_eq!(read_uvarint(block, q), (0, 1), "an anchor shares nothing");
        block[q] = 3; // <= the predecessor's length, so rule 1 does not fire
        len
    });
    assert_eq!(e.0, "corruption", "{}", e.1);
}

/// Rule 3: restart offsets strictly increase.
#[test]
fn delta_rejects_unsorted_restart_offsets() {
    let e = mutated_delta_error(|block, len| {
        let (end, count) = split_trailer(block);
        assert!(count >= 3);
        // Swap two adjacent anchors.
        for i in 0..4 {
            block.swap(end + 4 + i, end + 8 + i);
        }
        len
    });
    assert_eq!(e.0, "corruption", "{}", e.1);
    assert!(e.1.contains("restart"), "{}", e.1);
}

/// Rule 3: a non-empty block must have at least one anchor.
#[test]
fn delta_rejects_zero_restart_count_on_nonempty_block() {
    let e = mutated_delta_error(|block, len| {
        let n = block.len();
        // Claim zero anchors; the array bytes stay, becoming trailing entries
        // bytes, which the walk must also refuse.
        block[n - 4..].copy_from_slice(&0u32.to_le_bytes());
        len
    });
    assert_eq!(e.0, "corruption", "{}", e.1);
}

/// Rule 5: the entries region is consumed exactly — nothing may sit between
/// the last entry and the trailer.
#[test]
fn delta_rejects_trailing_bytes_before_trailer() {
    let e = mutated_delta_error(|block, len| {
        let (end, count) = split_trailer(block);
        // Shrink the last entry's value by one byte and claim one more anchor,
        // so the entries region ends one byte short of the trailer. Keeping the
        // total length fixed is what isolates "trailing bytes" from "truncated".
        let mut anchors: Vec<u32> = (0..count)
            .map(|i| {
                u32::from_le_bytes(block[end + i * 4..end + i * 4 + 4].try_into().unwrap())
            })
            .collect();
        // Point a new final anchor at the byte just before the trailer: the
        // walk from the previous anchor then cannot land on it.
        anchors.push((end - 1) as u32);
        let mut tail = Vec::new();
        for a in &anchors {
            tail.extend_from_slice(&a.to_le_bytes());
        }
        tail.extend_from_slice(&(anchors.len() as u32).to_le_bytes());
        let keep = block.len() - tail.len();
        block.truncate(keep);
        block.extend_from_slice(&tail);
        len
    });
    assert_eq!(e.0, "corruption", "{}", e.1);
}

/// Rule 7: reconstructed keys are non-decreasing within a block.
#[test]
fn delta_rejects_out_of_order_reconstructed_keys() {
    let e = mutated_delta_error(|block, len| {
        let (end, _) = split_trailer(block);
        // Entry 1's suffix byte, lowered below its predecessor's. The keys are
        // `tenant/0000/cluster/0000/segment/00000N`, so entry 1's suffix is the
        // final digit `1`; making it `0` makes the key equal-then-smaller.
        let mut p = 0usize;
        let mut fields = Vec::new();
        for _ in 0..6 {
            let (v, n) = read_uvarint(block, p);
            fields.push(v);
            p += n;
        }
        let next = p + fields[3] as usize + fields[4] as usize;
        assert!(next < end);
        let mut q = next;
        let mut f1 = Vec::new();
        for _ in 0..6 {
            let (v, n) = read_uvarint(block, q);
            f1.push(v);
            q += n;
        }
        assert_eq!(f1[3], 1, "entry 1 stores one suffix byte");
        block[q] = b'0'.wrapping_sub(1); // strictly below entry 0's byte
        len
    });
    assert_eq!(e.0, "corruption", "{}", e.1);
}

fn read_uvarint(b: &[u8], mut p: usize) -> (u64, usize) {
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

// ---- tasks 13-15: engine wiring ---------------------------------------------

fn delta_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        enable_prefix_delta_keys: true,
        block_restart_interval: 8,
        // Flush and compact eagerly so the tests observe real tables.
        write_buffer_size: 1 << 16,
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    }
}

/// Footer flags of every `.klog` under `dir`, recursively.
fn klog_footer_flags(dir: &std::path::Path) -> Vec<(String, u8)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "klog") {
                let bytes = std::fs::read(&p).unwrap();
                // A background compaction may be part-way through a new table;
                // an unfinished file has no footer to read yet.
                if bytes.len() < FOOTER_SIZE {
                    continue;
                }
                out.push((
                    p.file_name().unwrap().to_string_lossy().into_owned(),
                    bytes[bytes.len() - FOOTER_SIZE + 48],
                ));
            }
        }
    }
    out.sort();
    out
}

/// Copy every `.klog`/`.vlog` under `dir` into a flat directory — the shape
/// `attach_part` reads, and the shape that proves the source manifest is not
/// consulted.
fn klog_dir(dir: &std::path::Path) -> std::path::PathBuf {
    let staging = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "attach-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&staging).unwrap();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "klog" || e == "vlog") {
                std::fs::copy(&p, staging.join(p.file_name().unwrap())).unwrap();
            }
        }
    }
    staging
}

fn assert_all_delta(dir: &std::path::Path, what: &str) {
    let flags = klog_footer_flags(dir);
    assert!(!flags.is_empty(), "{what}: no tables were written");
    for (name, f) in &flags {
        assert_eq!(
            f & (FOOTER_PREFIX_DELTA | FOOTER_EXTENDED_BLOCK),
            FOOTER_PREFIX_DELTA | FOOTER_EXTENDED_BLOCK,
            "{what}: {name} is not a delta table (flags {f:#04x})"
        );
    }
}

fn fill(db: &DB, cf: &Arc<ondadb::ColumnFamily>, range: std::ops::Range<u32>) {
    for i in range {
        db.put(
            cf,
            format!("tenant/0001/cluster/0002/segment/{i:06}").as_bytes(),
            format!("value-{i:06}").as_bytes(),
            Duration::ZERO,
        )
        .unwrap();
    }
}

fn check(db: &DB, cf: &Arc<ondadb::ColumnFamily>, range: std::ops::Range<u32>) {
    for i in range {
        assert_eq!(
            db.get(cf, format!("tenant/0001/cluster/0002/segment/{i:06}").as_bytes())
                .unwrap(),
            format!("value-{i:06}").into_bytes(),
            "key {i}"
        );
    }
}

#[test]
fn flush_honours_the_prefix_delta_option() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    fill(&db, &cf, 0..2000);
    db.flush_memtable(&cf).unwrap();
    assert_all_delta(dir.path(), "flush");
    check(&db, &cf, 0..2000);
    db.close().unwrap();
}

#[test]
fn ingest_honours_the_prefix_delta_option() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    let mut ing = db.start_ingestion(&cf).unwrap();
    for i in 0..3000u32 {
        ing.write(
            format!("tenant/0001/cluster/0002/segment/{i:06}").as_bytes(),
            format!("value-{i:06}").as_bytes(),
            Duration::ZERO,
        )
        .unwrap();
    }
    ing.finish().unwrap();
    assert_all_delta(dir.path(), "ingest");
    check(&db, &cf, 0..3000);
    db.close().unwrap();
}

#[test]
fn compaction_output_honours_the_prefix_delta_option() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    let cf = db.create_column_family("d", delta_cfg()).unwrap();
    for chunk in 0..4u32 {
        fill(&db, &cf, chunk * 500..(chunk + 1) * 500);
        db.flush_memtable(&cf).unwrap();
    }
    db.compact(&cf).unwrap();
    assert_all_delta(dir.path(), "compaction");
    check(&db, &cf, 0..2000);
    db.close().unwrap();
}

#[test]
fn option_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
            .unwrap();
        let cf = db
            .create_column_family(
                "d",
                ColumnFamilyConfig {
                    block_restart_interval: 16,
                    ..delta_cfg()
                },
            )
            .unwrap();
        fill(&db, &cf, 0..200);
        db.close().unwrap();
    }
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("d").expect("cf survives reopen");
    let persisted = db.column_family_config("d").unwrap();
    assert!(persisted.enable_prefix_delta_keys);
    assert_eq!(persisted.block_restart_interval, 16);
    assert_eq!(
        db.format_capabilities() & CAP_PREFIX_DELTA,
        CAP_PREFIX_DELTA,
        "the capability is durable too"
    );
    // ...and it still governs new output after the reopen.
    fill(&db, &cf, 200..2500);
    db.flush_memtable(&cf).unwrap();
    assert_all_delta(dir.path(), "reopen");
    check(&db, &cf, 0..2500);
    db.close().unwrap();
}

// ---- task 14: capability gate -----------------------------------------------

/// The option alone writes nothing new: a table this binary writes must be
/// refused outright by one too old to decode its footer flag, and the manifest
/// capability word is what makes that refusal happen.
#[test]
fn delta_writer_requires_the_capability() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                // No background compaction, so the footer scan below sees a
                // settled directory.
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    fill(&db, &cf, 0..2000);
    db.flush_memtable(&cf).unwrap();
    for (name, f) in klog_footer_flags(dir.path()) {
        assert_eq!(
            f & FOOTER_PREFIX_DELTA,
            0,
            "{name}: a delta table was written without the capability"
        );
    }
    check(&db, &cf, 0..2000);
    // CAP_PREFIX_DELTA alone is not enough — a delta block IS an extended
    // block, so the extended-record capability is required too.
    db.enable_format_capabilities(CAP_PREFIX_DELTA).unwrap();
    fill(&db, &cf, 2000..4000);
    db.flush_memtable(&cf).unwrap();
    for (name, f) in klog_footer_flags(dir.path()) {
        assert_eq!(f & FOOTER_PREFIX_DELTA, 0, "{name}: half a capability sufficed");
    }
    db.close().unwrap();
}

#[test]
fn capability_persists_before_the_first_delta_table() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    // The bit is durable at this instant, and no delta table exists yet.
    assert!(
        klog_footer_flags(dir.path()).is_empty(),
        "no table may exist yet"
    );
    drop(cf);
    db.close().unwrap();

    // A separate open — the only way to read what actually reached disk —
    // sees the capability with still no table written.
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    assert_eq!(
        db.format_capabilities() & (CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA),
        CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA,
        "the capability must reach the manifest before the first delta byte"
    );
    assert!(klog_footer_flags(dir.path()).is_empty());
    let cf = db.get_column_family("d").unwrap();
    fill(&db, &cf, 0..2000);
    db.flush_memtable(&cf).unwrap();
    assert_all_delta(dir.path(), "after enable");
    db.close().unwrap();
}

// ---- task 15: mixed and standalone ------------------------------------------

#[test]
fn mixed_legacy_and_delta_tables_in_one_level_scan_identically() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    // No capability yet: this half of L0 is legacy.
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                // Never compact, so both formats stay side by side in L0.
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    fill(&db, &cf, 0..800);
    db.flush_memtable(&cf).unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    fill(&db, &cf, 800..1600);
    db.flush_memtable(&cf).unwrap();

    let flags = klog_footer_flags(dir.path());
    assert!(
        flags.iter().any(|(_, f)| f & FOOTER_PREFIX_DELTA != 0)
            && flags.iter().any(|(_, f)| f & FOOTER_PREFIX_DELTA == 0),
        "the level must actually hold both formats: {flags:?}"
    );
    check(&db, &cf, 0..1600);
    // A full scan spans both formats through one merge.
    let mut txn = db.begin();
    let mut n = 0usize;
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut previous: Option<Vec<u8>> = None;
    while it.valid() {
        if let Some(p) = &previous {
            assert!(p.as_slice() < it.key(), "merge order broke at {n}");
        }
        previous = Some(it.key().to_vec());
        n += 1;
        it.next();
    }
    assert_eq!(n, 1600);
    drop(it);
    txn.rollback().unwrap();
    db.close().unwrap();
}

#[test]
fn compaction_reads_legacy_writes_delta() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    for chunk in 0..3u32 {
        fill(&db, &cf, chunk * 500..(chunk + 1) * 500);
        db.flush_memtable(&cf).unwrap();
    }
    for (name, f) in klog_footer_flags(dir.path()) {
        assert_eq!(f & FOOTER_PREFIX_DELTA, 0, "{name} should still be legacy");
    }
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    db.compact(&cf).unwrap();
    assert_all_delta(dir.path(), "legacy inputs -> delta output");
    check(&db, &cf, 0..1500);
    db.close().unwrap();
}

/// A column family whose option is **off** must keep reading delta tables it
/// inherits, and compact them into legacy output. That is the rollback
/// contract: turning the option off stops new delta blocks and strands nothing.
///
/// Column-family options are fixed at creation (hot reconfiguration is a
/// documented non-goal), so the two policies are two families and the delta
/// tables cross over through `freeze_part` + `attach_part`.
#[test]
fn compaction_reads_delta_writes_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let frozen = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    let part_rule = vec![ondadb::PartitionRule {
        prefix: b"tenant/".to_vec(),
        name: "tenant".into(),
    }];
    let source = db
        .create_column_family(
            "delta",
            ColumnFamilyConfig {
                partition_rules: part_rule.clone(),
                ..delta_cfg()
            },
        )
        .unwrap();
    fill(&db, &source, 0..1200);
    db.flush_memtable(&source).unwrap();
    db.compact(&source).unwrap();
    assert_all_delta(&dir.path().join("cf-delta"), "source part");
    db.freeze_part(&source, "tenant", frozen.path()).unwrap();

    // The destination family never enables the option.
    let dest = db
        .create_column_family(
            "legacy",
            ColumnFamilyConfig {
                enable_prefix_delta_keys: false,
                partition_rules: part_rule,
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    // Overlapping keys, so the attached delta table lands in L0 and the
    // compaction below is forced to read it.
    fill(&db, &dest, 0..1200);
    db.flush_memtable(&dest).unwrap();
    db.attach_part(&dest, klog_dir(frozen.path())).unwrap();
    let flags = klog_footer_flags(&dir.path().join("cf-legacy"));
    assert!(
        flags.iter().any(|(_, f)| f & FOOTER_PREFIX_DELTA != 0),
        "the delta table must have been attached: {flags:?}"
    );
    check(&db, &dest, 0..1200);

    db.compact(&dest).unwrap();
    for (name, f) in klog_footer_flags(&dir.path().join("cf-legacy")) {
        assert_eq!(
            f & FOOTER_PREFIX_DELTA,
            0,
            "{name}: the destination family never enabled the option"
        );
    }
    check(&db, &dest, 0..1200);
    db.close().unwrap();
}

#[test]
fn frozen_delta_part_reopens_standalone() {
    let dir = tempfile::tempdir().unwrap();
    let frozen = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
            .unwrap();
        let cf = db
            .create_column_family(
                "d",
                ColumnFamilyConfig {
                    partition_rules: vec![ondadb::PartitionRule {
                        prefix: b"tenant/".to_vec(),
                        name: "tenant".into(),
                    }],
                    ..delta_cfg()
                },
            )
            .unwrap();
        fill(&db, &cf, 0..1200);
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        db.freeze_part(&cf, "tenant", frozen.path()).unwrap();
        db.close().unwrap();
    }
    // A frozen part is self-describing: the footer says how to decode it, so
    // the source manifest is never consulted.
    assert_all_delta(frozen.path(), "frozen part");
    let db = DB::open(Options::new(frozen.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("d").expect("cf in the frozen slice");
    check(&db, &cf, 0..1200);
    db.close().unwrap();
}

#[test]
fn attached_delta_part_reads_without_the_source_manifest() {
    let source = tempfile::tempdir().unwrap();
    let frozen = tempfile::tempdir().unwrap();
    {
        let db = DB::open(Options::new(source.path().to_str().unwrap())).unwrap();
        db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
            .unwrap();
        let cf = db
            .create_column_family(
                "d",
                ColumnFamilyConfig {
                    partition_rules: vec![ondadb::PartitionRule {
                        prefix: b"tenant/".to_vec(),
                        name: "tenant".into(),
                    }],
                    ..delta_cfg()
                },
            )
            .unwrap();
        fill(&db, &cf, 0..1200);
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        db.freeze_part(&cf, "tenant", frozen.path()).unwrap();
        db.close().unwrap();
    }
    // Keep only the klog/vlog files; the source manifest is deliberately gone.
    let staging = klog_dir(frozen.path());
    // A fresh database, sharing nothing with the source but the bytes. Its own
    // option is *off*: a delta table must read regardless of the write policy.
    let dest = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dest.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("d", ColumnFamilyConfig::default())
        .unwrap();
    // `attach_part` refuses a table whose max_seq is ahead of the destination's
    // visible sequence, so give the destination a lineage of its own first.
    for i in 0..1400u32 {
        db.put(&cf, format!("zzz/{i:06}").as_bytes(), b"local", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.attach_part(&cf, &staging).unwrap();
    check(&db, &cf, 0..1200);
    assert_eq!(db.get(&cf, b"zzz/000001").unwrap(), b"local");
    db.close().unwrap();
}

// ---- task 10: merge-iterator fallback ---------------------------------------

/// A delta child serves a buffered key while a legacy child serves a pinned
/// one; the merge must produce the same stream either way.
#[test]
fn delta_and_legacy_children_merge_identically() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    let mut streams = Vec::new();
    for (dir, delta) in [(&a, false), (&b, true)] {
        let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
        if delta {
            db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
                .unwrap();
        }
        let cf = db
            .create_column_family(
                "d",
                ColumnFamilyConfig {
                    l1_file_count_trigger: 1 << 20,
                    ..delta_cfg()
                },
            )
            .unwrap();
        // Several overlapping L0 tables plus a live memtable, so the merge has
        // real work: the same key at different sequences across children.
        for round in 0..3u32 {
            for i in 0..400u32 {
                db.put(
                    &cf,
                    format!("tenant/0001/cluster/0002/segment/{i:06}").as_bytes(),
                    format!("round-{round}-{i:06}").as_bytes(),
                    Duration::ZERO,
                )
                .unwrap();
            }
            if round < 2 {
                db.flush_memtable(&cf).unwrap();
            }
        }
        let mut txn = db.begin();
        let mut it = txn.new_iterator(&cf);
        it.seek_to_first();
        let mut out = Vec::new();
        while it.valid() {
            out.push((it.key().to_vec(), it.value().to_vec()));
            it.next();
        }
        assert!(it.err().is_none());
        streams.push(out);
        drop(it);
        txn.rollback().unwrap();
        db.close().unwrap();
    }
    assert_eq!(streams[0].len(), 400);
    assert_eq!(streams[0], streams[1], "delta merge diverged from legacy");
}

/// The merge iterator advances a child past the group key while still serving
/// it, so a borrow of the child's own reconstruction buffer would dangle. The
/// copy into `CurKey::Buffered` is what makes this hold — a duplicate key
/// across children is exactly the case that forces the advance.
#[test]
fn delta_scan_survives_child_advance_past_group_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    db.enable_format_capabilities(CAP_EXTENDED_RECORDS | CAP_PREFIX_DELTA)
        .unwrap();
    let cf = db
        .create_column_family(
            "d",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                ..delta_cfg()
            },
        )
        .unwrap();
    // Every key exists in four L0 tables, so every group spans four children.
    for round in 0..4u32 {
        for i in 0..500u32 {
            db.put(
                &cf,
                format!("tenant/0001/cluster/0002/segment/{i:06}").as_bytes(),
                format!("round-{round}").as_bytes(),
                Duration::ZERO,
            )
            .unwrap();
        }
        db.flush_memtable(&cf).unwrap();
    }
    assert_all_delta(dir.path(), "duplicated keys");
    let mut txn = db.begin();
    let mut it = txn.new_iterator(&cf);
    it.seek_to_first();
    let mut n = 0usize;
    while it.valid() {
        let key = it.key().to_vec();
        // The newest version wins, and the key served must still be intact
        // after the merge advanced every older child past it.
        assert_eq!(it.value(), b"round-3", "at {n}");
        assert_eq!(
            key,
            format!("tenant/0001/cluster/0002/segment/{n:06}").into_bytes()
        );
        n += 1;
        it.next();
    }
    assert!(it.err().is_none());
    assert_eq!(n, 500);
    drop(it);
    txn.rollback().unwrap();
    db.close().unwrap();
}
