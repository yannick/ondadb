//! wavesdb 0.8 data blocks, normalized into the layout ondaDB's reader walks.
//!
//! A wavesdb table's blocks describe themselves, one by one, by their first
//! bytes; ondaDB's reader picks one entry layout per table from its footer.
//! So every wavesdb data block is rewritten once, when it is loaded (before it
//! enters the block cache), into one shape — [`EntryLayout::Waves08`] entries
//! with a restart trailer — and from then on every point read, seek and scan
//! runs the reader's ordinary code.
//!
//! The three wavesdb encodings:
//!
//! ```text
//! legacy        entry* until the end of the block, each
//!               flags u8 | klen uv | vlen uv | seq uv | ttl varint? | key | value-or-pointer
//! full-key      0xff | schema uv = 1 | key_encoding uv = 0 | count uv (may be padded) |
//!               (kind uv | mods uv | klen uv | vlen uv | seq uv | ttl varint? | key | value-or-pointer)*
//! prefix-delta  0xff | 1 | key_encoding uv = 1 | count uv |
//!               (shared uv | suffix_len uv | suffix | kind uv | mods uv | vlen uv | seq uv
//!                | ttl varint? | value-or-pointer)*
//!               | restart_off u32 × R | R u32 | magic u32 "WDBL"
//! pointer       blob_id u64 | blob_off u64 | in_group_off uv (only with VLOG_GROUPED)
//! ```
//!
//! The normalized block:
//!
//! ```text
//! (kind uv | mods uv | klen uv | vlen uv | seq uv | ttl varint? | key
//!  | value-or-(blob_id u64 | blob_off u64 | uv((in_group_off << 1) | grouped)))*
//! | restart_off u32 × R | R u32
//! ```
//!
//! The grouped bit moves out of the modifiers into the pointer's last field,
//! so the normalized pointer has one fixed shape and `DecEntry` needs no new
//! flag. It is an in-memory form only: nothing writes it to disk.
//!
//! Decoding is held to wavesdb's own rules (`decodeEntry`,
//! `decodeExtendedEntry`, `decodeDeltaRaw`, `buildDeltaIndex`): a flag or
//! modifier outside the SST mask, a grouped pointer without a pointer, a
//! tombstone with one, a merge operand with a TTL, a range-delete or unknown
//! kind, a count that disagrees with the entries, a restart that does not
//! decode on its own — all `Corruption`.
//!
//! [`EntryLayout::Waves08`]: crate::sst::EntryLayout::Waves08

use crate::encoding::{append_u32, append_u64, append_uvarint, append_varint, read_u32, read_u64};
use crate::encoding::{uvarint, varint};
use crate::error::{OndaError, Result};
use crate::format::flags;

/// Restart interval of a normalized block. It never reaches the disk, so it
/// is a read-cost choice only: the same default ondaDB writes.
pub(crate) const RESTART_INTERVAL: usize = 8;

/// The extended-block marker.
const EXTENDED_MARKER: u8 = 0xff;
/// The only extended block schema wavesdb writes.
const BLOCK_SCHEMA: u8 = 1;
const KEY_ENCODING_FULL: u8 = 0;
const KEY_ENCODING_DELTA: u8 = 1;
/// `"WDBL"` as a big-endian word, stored little-endian.
const DELTA_TRAILER_MAGIC: u32 = 0x5744_424c;

/// wavesdb's SST modifier bits (`format.SSTModifierMask`).
const MOD_HAS_TTL: u64 = 0x02;
const MOD_HAS_VLOG: u64 = 0x04;
const MOD_VLOG_GROUPED: u64 = crate::format::wavesdb_reserved::MOD_VLOG_GROUPED;
const MOD_KNOWN: u64 = MOD_HAS_TTL | MOD_HAS_VLOG | MOD_VLOG_GROUPED;

/// wavesdb's legacy flag bits: ondaDB's four plus `VLOG_GROUPED`.
const FLAG_VLOG_GROUPED: u8 = crate::format::wavesdb_reserved::FLAG_VLOG_GROUPED;
const FLAGS_KNOWN: u8 = crate::format::KNOWN_ENTRY_FLAGS | FLAG_VLOG_GROUPED;

fn corrupt(what: impl std::fmt::Display) -> OndaError {
    OndaError::Corruption(format!("wavesdb block: {what}"))
}

/// One decoded wavesdb entry, borrowing its key and value from the block
/// (the key from the reconstruction buffer for a delta entry).
struct Entry<'a> {
    kind: u64,
    ttl: Option<i64>,
    val_len: u64,
    seq: u64,
    value: Value<'a>,
}

enum Value<'a> {
    Inline(&'a [u8]),
    Pointer {
        blob_id: u64,
        blob_off: u64,
        group: Option<u64>,
    },
}

struct Cursor<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Cursor<'a> {
    fn uvar(&mut self, what: &str) -> Result<u64> {
        let (v, n) = uvarint(&self.b[self.p..]).ok_or_else(|| corrupt(what))?;
        self.p += n;
        Ok(v)
    }

    fn varint(&mut self, what: &str) -> Result<i64> {
        let (v, n) = varint(&self.b[self.p..]).ok_or_else(|| corrupt(what))?;
        self.p += n;
        Ok(v)
    }

    fn take(&mut self, n: u64, what: &str) -> Result<&'a [u8]> {
        let n = usize::try_from(n).map_err(|_| corrupt(format!("{what} length")))?;
        if n > self.b.len() - self.p {
            return Err(corrupt(format!("{what} runs past the entries")));
        }
        let v = &self.b[self.p..self.p + n];
        self.p += n;
        Ok(v)
    }

    fn u64(&mut self, what: &str) -> Result<u64> {
        Ok(read_u64(self.take(8, what)?))
    }
}

/// What an entry's modifiers (or legacy flags) say about its value.
#[derive(Clone, Copy)]
struct Mods {
    ttl: bool,
    vlog: bool,
    grouped: bool,
}

/// Check a kind and modifier word the way `decodeExtendedEntry` does.
fn extended_mods(kind: u64, mods: u64) -> Result<Mods> {
    // A range tombstone is an interval in the aux section, never a data-block
    // entry; reading one as a point record would mask one key.
    crate::format::check_point_kind(kind)?;
    if mods & !MOD_KNOWN != 0 {
        return Err(corrupt(format!("modifiers {mods:#x} outside the SST mask")));
    }
    let m = Mods {
        ttl: mods & MOD_HAS_TTL != 0,
        vlog: mods & MOD_HAS_VLOG != 0,
        grouped: mods & MOD_VLOG_GROUPED != 0,
    };
    let tombstone = kind == crate::format::KIND_DELETE || kind == crate::format::KIND_SINGLE_DELETE;
    if m.grouped && !m.vlog {
        return Err(corrupt("a grouped pointer without a pointer"));
    }
    if tombstone && m.vlog {
        return Err(corrupt("a tombstone with a value pointer"));
    }
    if kind == crate::format::KIND_MERGE && m.ttl {
        return Err(corrupt("a merge operand with a TTL"));
    }
    Ok(m)
}

/// Check a legacy flags byte the way `format.ValidSSTFlags` does, and map it
/// to its kind.
fn legacy_flags(fl: u8) -> Result<(u64, Mods)> {
    if fl & !FLAGS_KNOWN != 0 {
        return Err(corrupt(format!("flags {fl:#04x} outside the SST mask")));
    }
    let tombstone = fl & flags::TOMBSTONE != 0;
    let single = fl & flags::SINGLE_DELETE != 0;
    let m = Mods {
        ttl: fl & flags::HAS_TTL != 0,
        vlog: fl & flags::HAS_VLOG != 0,
        grouped: fl & FLAG_VLOG_GROUPED != 0,
    };
    if single && !tombstone {
        return Err(corrupt("single-delete without the tombstone bit"));
    }
    if tombstone && m.vlog {
        return Err(corrupt("a tombstone with a value pointer"));
    }
    if m.grouped && !m.vlog {
        return Err(corrupt("a grouped pointer without a pointer"));
    }
    Ok((crate::format::point_kind(tombstone, single), m))
}

/// The value slot every encoding shares: an inline value of `val_len` bytes,
/// or the pointer.
fn value<'a>(c: &mut Cursor<'a>, m: Mods, val_len: u64) -> Result<Value<'a>> {
    if !m.vlog {
        return Ok(Value::Inline(c.take(val_len, "value")?));
    }
    let blob_id = c.u64("blob id")?;
    let blob_off = c.u64("blob offset")?;
    let group = if m.grouped {
        let g = c.uvar("in-group offset")?;
        if g >> 63 != 0 {
            return Err(corrupt("in-group offset overflows"));
        }
        Some(g)
    } else {
        None
    };
    Ok(Value::Pointer { blob_id, blob_off, group })
}

/// `klen | vlen | seq | ttl? | key | value` — the tail legacy and full-key
/// entries share.
fn full_key_tail<'a>(c: &mut Cursor<'a>, kind: u64, m: Mods) -> Result<(&'a [u8], Entry<'a>)> {
    let klen = c.uvar("key length")?;
    let val_len = c.uvar("value length")?;
    let seq = c.uvar("sequence")?;
    let ttl = if m.ttl { Some(c.varint("ttl")?) } else { None };
    let key = c.take(klen, "key")?;
    let value = value(c, m, val_len)?;
    Ok((key, Entry { kind, ttl, val_len, seq, value }))
}

/// Append one entry in the normalized layout.
fn emit(out: &mut Vec<u8>, key: &[u8], e: &Entry<'_>) {
    append_uvarint(out, e.kind);
    let mut mods = 0u64;
    if e.ttl.is_some() {
        mods |= u64::from(flags::HAS_TTL);
    }
    if matches!(e.value, Value::Pointer { .. }) {
        mods |= u64::from(flags::HAS_VLOG);
    }
    append_uvarint(out, mods);
    append_uvarint(out, key.len() as u64);
    append_uvarint(out, e.val_len);
    append_uvarint(out, e.seq);
    if let Some(t) = e.ttl {
        append_varint(out, t);
    }
    out.extend_from_slice(key);
    match e.value {
        Value::Inline(v) => out.extend_from_slice(v),
        Value::Pointer { blob_id, blob_off, group } => {
            append_u64(out, blob_id);
            append_u64(out, blob_off);
            append_uvarint(out, group.map_or(0, |g| (g << 1) | 1));
        }
    }
}

/// Decode a normalized pointer at `raw[at..]`: `(blob_id, blob_off,
/// in_group_off)`. Only ever applied to bytes [`normalize`] wrote.
#[cfg_attr(not(test), allow(dead_code))] // read by the shared-blob reader (next 2c step)
pub(crate) fn decode_pointer(raw: &[u8], at: usize) -> Result<(u64, u64, Option<u64>)> {
    let bad = || OndaError::Corruption("wavesdb block: normalized pointer".into());
    let p = raw.get(at..at + 16).ok_or_else(bad)?;
    let (g, _) = uvarint(&raw[at + 16..]).ok_or_else(bad)?;
    Ok((read_u64(p), read_u64(&p[8..]), (g & 1 != 0).then_some(g >> 1)))
}

/// Builds the normalized block: entries, then the restart trailer.
struct Out {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    count: usize,
    prev_key: Vec<u8>,
    bytewise: bool,
}

impl Out {
    fn push(&mut self, key: &[u8], e: &Entry<'_>) -> Result<()> {
        // Keys never decrease within a block. Exact under the bytewise
        // comparator, which is where it is checked: a custom comparator's
        // order is its own (AGENTS.md invariant 7).
        if self.bytewise && self.count > 0 && key < self.prev_key.as_slice() {
            return Err(corrupt("keys are not in order"));
        }
        if self.count.is_multiple_of(RESTART_INTERVAL) {
            let off = u32::try_from(self.buf.len()).map_err(|_| corrupt("block exceeds 4 GiB"))?;
            self.restarts.push(off);
        }
        emit(&mut self.buf, key, e);
        self.count += 1;
        if self.bytewise {
            self.prev_key.clear();
            self.prev_key.extend_from_slice(key);
        }
        Ok(())
    }

    fn finish(mut self) -> Vec<u8> {
        for &r in &self.restarts {
            append_u32(&mut self.buf, r);
        }
        append_u32(&mut self.buf, self.restarts.len() as u32);
        self.buf
    }
}

/// Rewrite one decompressed wavesdb data block into the normalized layout.
/// `bytewise` turns on the key-order check.
pub(crate) fn normalize(raw: &[u8], bytewise: bool) -> Result<Vec<u8>> {
    if raw.is_empty() {
        return Err(corrupt("empty data block"));
    }
    let mut out = Out {
        buf: Vec::with_capacity(raw.len() + raw.len() / 4),
        restarts: Vec::new(),
        count: 0,
        prev_key: Vec::new(),
        bytewise,
    };
    if raw[0] != EXTENDED_MARKER {
        let mut c = Cursor { b: raw, p: 0 };
        while c.p < raw.len() {
            let fl = raw[c.p];
            c.p += 1;
            let (kind, m) = legacy_flags(fl)?;
            let (key, e) = full_key_tail(&mut c, kind, m)?;
            out.push(key, &e)?;
        }
        return Ok(out.finish());
    }
    // marker | schema | key encoding: single-byte uvarints, a padded form
    // refused (the strict rule that lets one byte each identify the block).
    if raw.len() < 4 || raw[1] != BLOCK_SCHEMA {
        return Err(corrupt("extended block schema"));
    }
    let key_encoding = raw[2];
    let mut c = Cursor { b: raw, p: 3 };
    let count = c.uvar("entry count")?;
    if count == 0 || count > (raw.len() - c.p) as u64 {
        return Err(corrupt(format!("entry count {count}")));
    }
    match key_encoding {
        KEY_ENCODING_FULL => {
            while c.p < raw.len() {
                let kind = c.uvar("kind")?;
                let mods = c.uvar("modifiers")?;
                let m = extended_mods(kind, mods)?;
                let (key, e) = full_key_tail(&mut c, kind, m)?;
                out.push(key, &e)?;
            }
        }
        KEY_ENCODING_DELTA => delta(raw, c.p, count, &mut out)?,
        other => return Err(corrupt(format!("key encoding {other}"))),
    }
    if out.count as u64 != count {
        return Err(corrupt(format!(
            "header declares {count} entries, the block holds {}",
            out.count
        )));
    }
    Ok(out.finish())
}

/// A prefix-delta block's entries, from `start` (just past the header).
fn delta(raw: &[u8], start: usize, count: u64, out: &mut Out) -> Result<()> {
    let n = raw.len();
    if n < start + 8 || read_u32(&raw[n - 4..]) != DELTA_TRAILER_MAGIC {
        return Err(corrupt("prefix-delta trailer magic"));
    }
    let restarts = u64::from(read_u32(&raw[n - 8..n - 4]));
    if restarts == 0 || restarts > count || restarts * 4 + 8 > (n - start) as u64 {
        return Err(corrupt(format!("prefix-delta restart count {restarts}")));
    }
    let end = n - 8 - restarts as usize * 4;
    let offsets: Vec<usize> = (0..restarts as usize)
        .map(|i| read_u32(&raw[end + 4 * i..]) as usize)
        .collect();
    if offsets[0] != start || offsets.windows(2).any(|w| w[1] <= w[0]) || offsets[offsets.len() - 1] >= end {
        return Err(corrupt("prefix-delta restart offsets"));
    }
    let entries = &raw[..end];
    let mut c = Cursor { b: entries, p: start };
    let mut key: Vec<u8> = Vec::new();
    let mut next_restart = 0;
    while c.p < end {
        let at_restart = next_restart < offsets.len() && offsets[next_restart] == c.p;
        if at_restart {
            next_restart += 1;
        } else if next_restart < offsets.len() && offsets[next_restart] < c.p {
            // The walk stepped over an anchor: it does not sit on an entry.
            return Err(corrupt("a restart offset inside an entry"));
        }
        let shared = c.uvar("shared length")?;
        if at_restart && shared != 0 {
            return Err(corrupt("a restart entry does not decode on its own"));
        }
        if shared > key.len() as u64 {
            return Err(corrupt("shared length exceeds the previous key"));
        }
        let suffix_len = c.uvar("suffix length")?;
        let suffix = c.take(suffix_len, "suffix")?;
        key.truncate(shared as usize);
        key.extend_from_slice(suffix);
        let kind = c.uvar("kind")?;
        let mods = c.uvar("modifiers")?;
        let m = extended_mods(kind, mods)?;
        let val_len = c.uvar("value length")?;
        let seq = c.uvar("sequence")?;
        let ttl = if m.ttl { Some(c.varint("ttl")?) } else { None };
        let value = value(&mut c, m, val_len)?;
        out.push(&key, &Entry { kind, ttl, val_len, seq, value })?;
    }
    if next_restart != offsets.len() {
        return Err(corrupt("a restart offset inside an entry"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::{decode_entry, EntryLayout};

    /// One logical entry, the oracle every encoding is checked against.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct E {
        key: Vec<u8>,
        kind: u64,
        seq: u64,
        ttl: Option<i64>,
        /// `Ok` inline, `Err((blob_id, blob_off, in_group_off, len))` a pointer.
        value: std::result::Result<Vec<u8>, (u64, u64, Option<u64>, u64)>,
    }

    fn sample() -> Vec<E> {
        let mut v = Vec::new();
        for i in 0..21u64 {
            let key = format!("key{:03}", i / 2).into_bytes();
            let (kind, value) = match i % 5 {
                0 => (1, Ok(format!("v{i}").into_bytes())),
                1 => (2, Ok(Vec::new())),
                2 => (1, Err((7 + i, 32 + 100 * i, None, 900))),
                3 => (1, Err((9, 4096 * i, Some(i * 3), 77))),
                _ => (4, Ok(b"+op".to_vec())),
            };
            let ttl = (kind == 1 && i % 3 == 0).then_some(1_700_000_000_000 + i as i64);
            v.push(E { key, kind, seq: 1000 - i, ttl, value });
        }
        v
    }

    fn mods(e: &E) -> u64 {
        let mut m = 0;
        if e.ttl.is_some() {
            m |= MOD_HAS_TTL;
        }
        if let Err((_, _, g, _)) = &e.value {
            m |= MOD_HAS_VLOG;
            if g.is_some() {
                m |= MOD_VLOG_GROUPED;
            }
        }
        m
    }

    fn val_len(e: &E) -> u64 {
        match &e.value {
            Ok(v) => v.len() as u64,
            Err((_, _, _, len)) => *len,
        }
    }

    fn put_value(b: &mut Vec<u8>, e: &E) {
        match &e.value {
            Ok(v) => b.extend_from_slice(v),
            Err((id, off, g, _)) => {
                append_u64(b, *id);
                append_u64(b, *off);
                if let Some(g) = g {
                    append_uvarint(b, *g);
                }
            }
        }
    }

    fn tail(b: &mut Vec<u8>, e: &E) {
        append_uvarint(b, e.key.len() as u64);
        append_uvarint(b, val_len(e));
        append_uvarint(b, e.seq);
        if let Some(t) = e.ttl {
            append_varint(b, t);
        }
        b.extend_from_slice(&e.key);
        put_value(b, e);
    }

    /// wavesdb's legacy encoding, written from its spec. Merge operands have
    /// no legacy spelling, so the caller passes point kinds only.
    fn legacy(es: &[E]) -> Vec<u8> {
        let mut b = Vec::new();
        for e in es {
            let mut fl = mods(e) as u8;
            if e.kind == 2 {
                fl |= flags::TOMBSTONE;
            }
            b.push(fl);
            tail(&mut b, e);
        }
        b
    }

    fn full(es: &[E], padded_count: bool) -> Vec<u8> {
        let mut b = vec![EXTENDED_MARKER, BLOCK_SCHEMA, KEY_ENCODING_FULL];
        if padded_count {
            // wavesdb reserves five bytes and patches the count in place.
            let n = es.len() as u8;
            b.extend_from_slice(&[n | 0x80, 0x80, 0x80, 0x80, 0x00]);
        } else {
            append_uvarint(&mut b, es.len() as u64);
        }
        for e in es {
            append_uvarint(&mut b, e.kind);
            append_uvarint(&mut b, mods(e));
            tail(&mut b, e);
        }
        b
    }

    fn delta_block(es: &[E], interval: usize) -> Vec<u8> {
        let mut b = vec![EXTENDED_MARKER, BLOCK_SCHEMA, KEY_ENCODING_DELTA];
        append_uvarint(&mut b, es.len() as u64);
        let mut restarts = Vec::new();
        let mut prev: &[u8] = &[];
        for (i, e) in es.iter().enumerate() {
            let shared = if i % interval == 0 {
                restarts.push(b.len() as u32);
                0
            } else {
                prev.iter().zip(&e.key).take_while(|(a, b)| a == b).count()
            };
            append_uvarint(&mut b, shared as u64);
            append_uvarint(&mut b, (e.key.len() - shared) as u64);
            b.extend_from_slice(&e.key[shared..]);
            append_uvarint(&mut b, e.kind);
            append_uvarint(&mut b, mods(e));
            append_uvarint(&mut b, val_len(e));
            append_uvarint(&mut b, e.seq);
            if let Some(t) = e.ttl {
                append_varint(&mut b, t);
            }
            put_value(&mut b, e);
            prev = &e.key;
        }
        for r in &restarts {
            append_u32(&mut b, *r);
        }
        append_u32(&mut b, restarts.len() as u32);
        append_u32(&mut b, DELTA_TRAILER_MAGIC);
        b
    }

    /// Walk a normalized block with ondaDB's own decoder.
    fn read_back(norm: &[u8]) -> Vec<E> {
        let r = read_u32(&norm[norm.len() - 4..]) as usize;
        let end = norm.len() - 4 - 4 * r;
        let entries = &norm[..end];
        let anchors: Vec<usize> = (0..r).map(|i| read_u32(&norm[end + 4 * i..]) as usize).collect();
        let mut out = Vec::new();
        let mut off = 0;
        while off < entries.len() {
            if out.len() % RESTART_INTERVAL == 0 {
                assert_eq!(anchors[out.len() / RESTART_INTERVAL], off, "anchor");
            }
            let (d, next) = decode_entry(entries, EntryLayout::Waves08, off).unwrap();
            let value = if d.flags & flags::HAS_VLOG != 0 {
                let (id, boff, g) = decode_pointer(entries, d.val_start).unwrap();
                assert_eq!(boff, d.vlog_off);
                Err((id, boff, g, d.val_len as u64))
            } else {
                Ok(entries[d.val_start..d.val_start + d.val_len].to_vec())
            };
            out.push(E {
                key: entries[d.key_start..d.key_start + d.key_len].to_vec(),
                kind: u64::from(d.kind),
                seq: d.seq,
                ttl: (d.flags & flags::HAS_TTL != 0).then_some(d.ttl),
                value,
            });
            off = next;
        }
        assert_eq!(anchors.len(), out.len().div_ceil(RESTART_INTERVAL));
        out
    }

    #[test]
    fn every_encoding_normalizes_to_the_same_entries() {
        let all = sample();
        let points: Vec<E> = all.iter().filter(|e| e.kind != 4).cloned().collect();
        assert_eq!(read_back(&normalize(&legacy(&points), true).unwrap()), points);
        assert_eq!(read_back(&normalize(&full(&all, false), true).unwrap()), all);
        assert_eq!(read_back(&normalize(&full(&all, true), true).unwrap()), all);
        for interval in [1, 3, 16] {
            assert_eq!(read_back(&normalize(&delta_block(&all, interval), true).unwrap()), all);
        }
    }

    #[test]
    fn malformed_blocks_are_corruption() {
        let all = sample();
        let points: Vec<E> = all.iter().filter(|e| e.kind != 4).cloned().collect();
        let mut cases: Vec<(&str, Vec<u8>)> = Vec::new();
        cases.push(("empty", Vec::new()));
        let mut b = full(&all, false);
        b[1] = 2;
        cases.push(("schema", b));
        let mut b = full(&all, false);
        b[2] = 7;
        cases.push(("key encoding", b));
        let mut b = full(&all, false);
        b[3] += 1;
        cases.push(("count", b));
        let mut b = legacy(&points);
        b[0] |= 0x20;
        cases.push(("legacy flag", b));
        let mut b = legacy(&points);
        b[0] = flags::SINGLE_DELETE;
        cases.push(("single delete alone", b));
        let mut b = delta_block(&all, 4);
        let n = b.len();
        b[n - 1] ^= 1;
        cases.push(("trailer magic", b));
        let mut b = delta_block(&all, 4);
        let n = b.len();
        b[n - 12] = b[n - 12].wrapping_add(1);
        cases.push(("restart offset", b));
        let mut b = legacy(&points);
        b.truncate(b.len() - 1);
        cases.push(("truncated", b));
        let mut unordered = points.clone();
        unordered.swap(0, 6);
        cases.push(("order", legacy(&unordered)));
        let mut grouped_alone = all[..1].to_vec();
        grouped_alone[0].ttl = None;
        let mut b = full(&grouped_alone, false);
        b[5] = MOD_VLOG_GROUPED as u8;
        cases.push(("grouped without pointer", b));
        for (name, b) in cases {
            match normalize(&b, true) {
                Err(OndaError::Corruption(_)) => {}
                other => panic!("{name}: {other:?}"),
            }
        }
        // Range deletes live in the aux section; an entry claiming one is not
        // a point record.
        let mut b = full(&all[..1], false);
        b[4] = 5;
        assert!(normalize(&b, true).is_err());
    }

    /// The fixture generator's values carry their coordinates: a large or
    /// grouped value starts `{round}.{i}:`, a small one is
    /// `value-{round}-{i}`, and under a `key{i:05}` key `i` must match. A
    /// value that resolved to the wrong frame fails this; one in neither shape
    /// (merge operands, entities, the int64-keyed family) is not checked.
    /// Returns whether it was.
    fn fixture_value_matches(key: &[u8], v: &[u8], what: &str) -> bool {
        let Some(want) = key
            .strip_prefix(b"key")
            .filter(|n| n.len() == 5)
            .and_then(|n| std::str::from_utf8(n).ok()?.parse::<u64>().ok())
        else {
            return false;
        };
        let text = String::from_utf8_lossy(v);
        let coords = if let Some(rest) = text.strip_prefix("value-") {
            rest.split_once('-').map(|(r, i)| (r.to_string(), i.to_string()))
        } else {
            text.split_once(':')
                .and_then(|(head, _)| head.split_once('.'))
                .map(|(r, i)| (r.to_string(), i.to_string()))
        };
        let Some((round, i)) = coords else {
            return false;
        };
        assert!(round.parse::<u64>().is_ok_and(|r| r < 3), "{what}: {text:.40}");
        assert_eq!(i.parse::<u64>().ok(), Some(want), "{what}: {text:.40}");
        true
    }

    /// Every table in every wavesdb fixture, through the reader: each data
    /// block decodes and normalizes, the entries are the footer's count in
    /// internal order within the catalog's key range, the index finds every
    /// entry in the block that holds it, and a point read of each returns
    /// that entry with its value — resolved from the family's blob files
    /// when it is separated. Nothing is refused.
    #[test]
    fn every_fixture_table_reads_through_the_normalizer() {
        use crate::cache::{BlockCache, FileCache};
        use crate::format::FormatProfile;
        use crate::sst::{cmp_internal, Reader};
        use crate::storage::LocalStorage;
        use std::sync::Arc;

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/wavesdb-epoch1");
        let storage = LocalStorage::new(Arc::new(FileCache::new(8)), cfg!(feature = "mmap-reads"));
        let (mut tables, mut entries, mut delta_tables, mut blob_values) = (0, 0u64, 0, 0);
        let (mut grouped_values, mut checked_contents) = (0, 0u64);
        let mut fixtures: Vec<_> = std::fs::read_dir(&root)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.is_dir())
            .collect();
        fixtures.sort();
        assert_eq!(fixtures.len(), 5);
        for fixture in fixtures {
            let dir = fixture.join("db");
            let catalog = super::super::recover_catalog(&dir).unwrap();
            for cf in &catalog.cfs {
                let cfg = super::super::config::decode(&cf.config).unwrap();
                let cmp = crate::comparator::comparator_by_name(&cfg.comparator_name)
                    .unwrap_or_else(|| panic!("comparator {:?}", cfg.comparator_name));
                for t in &cf.tables {
                    let what = format!("{}/{}/{}", fixture.display(), cf.name, t.meta.id);
                    let klog = dir.join(format!("cf_{}/{}.klog", cf.name, t.meta.id));
                    let r = Reader::open_profiled(
                        klog.to_str().unwrap(),
                        storage.clone(),
                        Arc::new(BlockCache::new(1 << 20)),
                        t.meta.id,
                        cmp.clone(),
                        0,
                        None,
                        FormatProfile::Waves08,
                    )
                    .unwrap_or_else(|e| panic!("{what}: {e}"));
                    assert!(r.normalizes_blocks(), "{what}");
                    assert_eq!(r.num_entries(), t.meta.num_entries, "{what}");
                    if r.table_caps() & crate::format::CAP_PREFIX_DELTA != 0 {
                        delta_tables += 1;
                    }
                    let mut seen = 0u64;
                    let mut prev: Option<(Vec<u8>, u64)> = None;
                    for i in 0..r.data_block_count() {
                        let block = r
                            .read_data_block(i)
                            .unwrap_or_else(|e| panic!("{what} block {i}: {e}"));
                        let (raw, _) = r.split_block(block.bytes()).unwrap();
                        let mut off = 0;
                        while off < raw.len() {
                            let (d, next) = decode_entry(raw, EntryLayout::Waves08, off)
                                .unwrap_or_else(|e| panic!("{what} block {i}: {e}"));
                            let key = d.user_key(raw).to_vec();
                            if let Some((pk, ps)) = &prev {
                                assert!(
                                    cmp_internal(&cmp, pk, *ps, &key, d.seq).is_lt(),
                                    "{what}: entries out of order"
                                );
                            }
                            assert_eq!(r.find_block(&key, d.seq), i, "{what}: separator");
                            match r.get(&key, d.seq, 0) {
                                Ok((v, seq, found, deleted, kind)) => {
                                    assert!(found && seq == d.seq, "{what}");
                                    assert_eq!(kind, u64::from(d.kind), "{what}");
                                    if !deleted {
                                        let v = v.unwrap();
                                        if d.has_vlog() {
                                            assert_eq!(v.len(), d.val_len, "{what}");
                                            blob_values += 1;
                                            if decode_pointer(raw, d.val_start).unwrap().2.is_some() {
                                                grouped_values += 1;
                                            }
                                        } else {
                                            assert_eq!(v, d.inline_value(raw), "{what}");
                                        }
                                        checked_contents += u64::from(fixture_value_matches(&key, &v, &what));
                                    }
                                }
                                Err(e) => panic!("{what}: {e}"),
                            }
                            prev = Some((key, d.seq));
                            seen += 1;
                            off = next;
                        }
                    }
                    assert_eq!(seen, t.meta.num_entries, "{what}");
                    let (first, last) = (r.min_key().to_vec(), r.max_key().to_vec());
                    assert_eq!((first, last), (t.meta.min_key.clone(), t.meta.max_key.clone()), "{what}");
                    tables += 1;
                    entries += seen;
                }
            }
        }
        // The corpus is frozen (SHA256SUMS), so its shape is pinned exactly:
        // a table, block or value that silently stopped being read would
        // change a count.
        assert_eq!((tables, entries), (36, 6568));
        assert_eq!(delta_tables, 5);
        assert_eq!((blob_values, grouped_values), (3068, 1000));
        assert_eq!(checked_contents, 3940);
    }
}
