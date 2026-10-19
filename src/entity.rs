//! Wide-column entities (wavesdb 4.2 `entity.go`, plan C §1.4 F11).
//!
//! An *entity* is a small set of named byte columns stored under one key as
//! **one ordinary value**: a self-describing frame whose layout is the public
//! wavesdb entity frame, version 1, reproduced here byte for byte so that an
//! entity written by either engine (or by any process implementing the
//! specification) is read by the other. The frame is registered in
//! `docs/format-registry.md` as a value-level format shared with wavesdb.
//!
//! The engine never looks inside a value: there is no record kind, no
//! capability bit and no on-disk change. Transactions, conflict detection,
//! snapshots, TTL, value separation, compaction and checkpoints treat an
//! entity exactly like any other value, at whole-key granularity. To
//! [`DB::get`](crate::DB::get) or an iterator the frame is opaque bytes;
//! [`decode_entity`] (or [`DB::get_entity`](crate::DB::get_entity)) is how a
//! caller opts in.
//!
//! # Frame, version 1
//!
//! All integers little-endian; varints are unsigned LEB128, at most five bytes
//! and at most `u32::MAX` in value.
//!
//! ```text
//! offset  size  field
//! 0       4     magic "WVE1"
//! 4       1     version (1)
//! 5       1     flags (0)
//! 6       4     header length H = 14 + directory bytes
//! 10      4     column count N
//! 14      var   directory: N × (nameOff u32, nameLen uvarint, valOff u32, valLen uvarint)
//! H       var   payload: name₀ value₀ name₁ value₁ … packed with no gaps
//! len-4   4     CRC32-C over [0, len-4)
//! ```
//!
//! Offsets are relative to the payload start. The directory ends exactly at
//! `H` and the payload is covered in order with no gaps, so a frame is
//! canonical: decoding and re-encoding reproduces it byte for byte.
//!
//! # Error classes
//!
//! | Condition | Error |
//! | --- | --- |
//! | Key absent, deleted or expired | [`OndaError::NotFound`] (from `get_entity`) |
//! | Wrong length, magic, version or flags; structure does not hold; empty or duplicate name | [`OndaError::NotEntity`] |
//! | Everything holds but the CRC | [`OndaError::Corruption`] |
//!
//! The CRC is consulted **last** on purpose: a plain value that merely starts
//! with `WVE1` is almost always rejected structurally, and `Corruption` is kept
//! for a frame that is an entity in every respect except its checksum.

use crate::encoding::{append_u32, append_uvarint, checksum, put_u32, read_u32, uvarint_len};
use crate::error::{OndaError, Result};

/// Largest number of columns one entity may carry.
pub const MAX_ENTITY_COLUMNS: usize = 4096;
/// Longest column name, in bytes.
pub const MAX_ENTITY_NAME_LEN: usize = 4096;
/// Largest encoded frame, header and checksum included.
///
/// Package constants in wavesdb, constants here, never options: an entity
/// written by one process must always be readable by another.
pub const MAX_ENTITY_BYTES: usize = 64 << 20;

const MAGIC: &[u8; 4] = b"WVE1";
const VERSION: u8 = 1;
const FIXED_HEADER: usize = 14;
/// The empty entity: fixed header plus checksum.
const MIN_FRAME: usize = FIXED_HEADER + 4;
/// Two `u32` offsets and two one-byte varints.
const MIN_DIR_ENTRY: u64 = 4 + 1 + 4 + 1;
/// A `u32` needs at most five LEB128 bytes.
const MAX_VARINT: usize = 5;
/// Column count up to which duplicate names are found pairwise; above it one
/// open-addressing table is built (wavesdb's threshold).
const LINEAR_SCAN_MAX: usize = 32;

/// Anything that can be written as an entity column: a name and a value.
///
/// Implemented for [`EntityColumn`], [`EntityColumnRef`] and any
/// `(name, value)` pair of byte-slice-likes, so
/// `db.put_entity(&cf, key, &[("name", "Ada"), ("email", "ada@example.com")], ttl)`
/// works without building columns first.
pub trait EntityColumnLike {
    /// The column name. Must be non-empty and unique within the entity.
    fn name(&self) -> &[u8];
    /// The column value. Any bytes, empty included.
    fn value(&self) -> &[u8];
}

/// One owned column of an entity, as [`DB::get_entity`](crate::DB::get_entity)
/// returns it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct EntityColumn {
    /// Column name: non-empty, unique within its entity, compared as bytes.
    pub name: Vec<u8>,
    /// Column value: any bytes.
    pub value: Vec<u8>,
}

impl EntityColumn {
    /// A column from anything convertible to bytes.
    pub fn new(name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> EntityColumn {
        EntityColumn {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// One column of a decoded frame, **borrowing** the frame it came from — valid
/// exactly as long as that buffer is (for an iterator, until it moves), like
/// [`Iterator::value`](crate::Iterator::value) itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct EntityColumnRef<'a> {
    /// Column name.
    pub name: &'a [u8],
    /// Column value.
    pub value: &'a [u8],
}

impl EntityColumnRef<'_> {
    /// Copy the column out of the frame it borrows.
    pub fn into_owned(self) -> EntityColumn {
        EntityColumn {
            name: self.name.to_vec(),
            value: self.value.to_vec(),
        }
    }
}

impl EntityColumnLike for EntityColumn {
    fn name(&self) -> &[u8] {
        &self.name
    }
    fn value(&self) -> &[u8] {
        &self.value
    }
}

impl EntityColumnLike for EntityColumnRef<'_> {
    fn name(&self) -> &[u8] {
        self.name
    }
    fn value(&self) -> &[u8] {
        self.value
    }
}

impl<N: AsRef<[u8]>, V: AsRef<[u8]>> EntityColumnLike for (N, V) {
    fn name(&self) -> &[u8] {
        self.0.as_ref()
    }
    fn value(&self) -> &[u8] {
        self.1.as_ref()
    }
}

impl<C: EntityColumnLike + ?Sized> EntityColumnLike for &C {
    fn name(&self) -> &[u8] {
        (**self).name()
    }
    fn value(&self) -> &[u8] {
        (**self).value()
    }
}

/// Build the entity frame for `cols`, in the order given.
///
/// Validates the column set before allocating the frame: an empty or
/// duplicate name, more than [`MAX_ENTITY_COLUMNS`] columns or a name over
/// [`MAX_ENTITY_NAME_LEN`] bytes is [`OndaError::InvalidArgs`]; a frame that
/// would exceed [`MAX_ENTITY_BYTES`] is [`OndaError::TooLarge`]. Nothing is
/// sorted or deduplicated (see [`sort_columns`] for a canonical order). An
/// empty `cols` encodes the empty entity, which is a value, distinct from an
/// absent key.
pub fn encode_entity<C: EntityColumnLike>(cols: &[C]) -> Result<Vec<u8>> {
    if cols.len() > MAX_ENTITY_COLUMNS {
        return Err(OndaError::InvalidArgs(format!(
            "entity has {} columns, limit is {MAX_ENTITY_COLUMNS}",
            cols.len()
        )));
    }
    // Sized in u64 so the running total cannot wrap before it is compared.
    let mut total = MIN_FRAME as u64;
    for (i, c) in cols.iter().enumerate() {
        let n = c.name().len();
        if n == 0 {
            return Err(OndaError::InvalidArgs(format!(
                "entity column {i} has an empty name"
            )));
        }
        if n > MAX_ENTITY_NAME_LEN {
            return Err(OndaError::InvalidArgs(format!(
                "entity column {i} name is {n} bytes, limit is {MAX_ENTITY_NAME_LEN}"
            )));
        }
        let v = c.value().len() as u64;
        total += 4 + uvarint_len(n as u64) as u64 + 4 + uvarint_len(v) as u64;
        total += n as u64 + v;
        if total > MAX_ENTITY_BYTES as u64 {
            return Err(OndaError::TooLarge(format!(
                "entity frame exceeds {MAX_ENTITY_BYTES} bytes"
            )));
        }
    }
    if let Some((i, j)) = find_duplicate(cols) {
        return Err(OndaError::InvalidArgs(dup_message(i, j)));
    }

    let mut buf = Vec::with_capacity(total as usize);
    buf.extend_from_slice(MAGIC);
    buf.push(VERSION);
    buf.push(0);
    buf.extend_from_slice(&[0; 8]); // header length and count, patched below
    // Bounded by MAX_ENTITY_BYTES above, so u32 offsets cannot overflow.
    let mut pay_len: u32 = 0;
    for c in cols {
        append_u32(&mut buf, pay_len);
        append_uvarint(&mut buf, c.name().len() as u64);
        pay_len += c.name().len() as u32;
        append_u32(&mut buf, pay_len);
        append_uvarint(&mut buf, c.value().len() as u64);
        pay_len += c.value().len() as u32;
    }
    let header_len = buf.len() as u32;
    put_u32(&mut buf[6..10], header_len);
    put_u32(&mut buf[10..14], cols.len() as u32);
    for c in cols {
        buf.extend_from_slice(c.name());
        buf.extend_from_slice(c.value());
    }
    let crc = checksum(&buf);
    append_u32(&mut buf, crc);
    debug_assert_eq!(buf.len() as u64, total);
    Ok(buf)
}

/// Parse an entity frame and return its columns in stored order, borrowing
/// `value`.
///
/// Not an entity (a plain value, an unknown version, a frame whose structure
/// does not hold) is [`OndaError::NotEntity`]; a structurally sound frame with
/// a bad checksum is [`OndaError::Corruption`]. The frame is validated
/// completely before the result is allocated, and every count and length is
/// bounded by the frame's own size first, so a hostile value cannot make the
/// decoder allocate by its claims.
pub fn decode_entity(value: &[u8]) -> Result<Vec<EntityColumnRef<'_>>> {
    let mut out = Vec::new();
    decode_entity_into(&mut out, value)?;
    Ok(out)
}

/// [`decode_entity`], **appending** to `dst` so a scan can reuse one result
/// vector across rows (`dst.clear()` between them): once `dst` has grown to
/// the widest row, decoding allocates nothing. On error `dst` is exactly as it
/// was.
pub fn decode_entity_into<'a>(
    dst: &mut Vec<EntityColumnRef<'a>>,
    value: &'a [u8],
) -> Result<()> {
    let n = value.len();
    if !(MIN_FRAME..=MAX_ENTITY_BYTES).contains(&n) {
        return Err(not_entity(format_args!("frame length {n}")));
    }
    if &value[..4] != MAGIC {
        return Err(OndaError::NotEntity("bad magic".into()));
    }
    if value[4] != VERSION {
        return Err(not_entity(format_args!("version {}", value[4])));
    }
    if value[5] != 0 {
        return Err(not_entity(format_args!("flags {:#04x}", value[5])));
    }
    let header_len = read_u32(&value[6..10]) as u64;
    let count = read_u32(&value[10..14]) as u64;
    // The header holds the fixed part plus at least the smallest directory
    // entry per column and leaves room for the checksum. All 64-bit
    // arithmetic on values bounded by n <= MAX_ENTITY_BYTES.
    if header_len < FIXED_HEADER as u64 || header_len > n as u64 - 4 {
        return Err(not_entity(format_args!(
            "header length {header_len} in frame of {n} bytes"
        )));
    }
    if count > MAX_ENTITY_COLUMNS as u64 {
        return Err(not_entity(format_args!(
            "column count {count} exceeds limit {MAX_ENTITY_COLUMNS}"
        )));
    }
    let dir_len = header_len - FIXED_HEADER as u64;
    if count * MIN_DIR_ENTRY > dir_len {
        return Err(not_entity(format_args!(
            "column count {count} does not fit a {dir_len}-byte directory"
        )));
    }
    let header_len = header_len as usize;
    let count = count as usize;
    let dir = &value[FIXED_HEADER..header_len];
    let payload = &value[header_len..n - 4];
    let pay_len = payload.len() as u64;

    // First pass: validate every region before anything is allocated. Regions
    // must be packed in order from payload offset 0, with no gaps or
    // overlaps, and must cover the whole payload.
    let mut cursor: u64 = 0;
    let mut pos = 0usize;
    for i in 0..count {
        let (name_off, name_len, val_off, val_len) = dir_entry(dir, &mut pos)
            .ok_or_else(|| not_entity(format_args!("directory entry {i} is malformed")))?;
        if name_len == 0 || name_len > MAX_ENTITY_NAME_LEN as u64 {
            return Err(not_entity(format_args!("name length {name_len} at column {i}")));
        }
        if name_off != cursor || name_off + name_len > pay_len {
            return Err(not_entity(format_args!(
                "name region [{name_off},+{name_len}) at column {i}"
            )));
        }
        cursor = name_off + name_len;
        if val_off != cursor || val_off + val_len > pay_len {
            return Err(not_entity(format_args!(
                "value region [{val_off},+{val_len}) at column {i}"
            )));
        }
        cursor = val_off + val_len;
    }
    if pos != dir.len() {
        return Err(not_entity(format_args!(
            "directory has {} trailing bytes",
            dir.len() - pos
        )));
    }
    if cursor != pay_len {
        return Err(not_entity(format_args!(
            "payload has {} bytes not covered by any column",
            pay_len - cursor
        )));
    }

    // Second pass builds the result: the layout is proven, so no slice here
    // can go out of bounds. Uniqueness is checked on the views before the CRC
    // so a structurally invalid frame is "not an entity", not corruption.
    let start = dst.len();
    dst.reserve(count);
    pos = 0;
    for _ in 0..count {
        let (name_off, name_len, val_off, val_len) =
            dir_entry(dir, &mut pos).expect("directory validated by the first pass");
        let (no, nl, vo, vl) = (
            name_off as usize,
            name_len as usize,
            val_off as usize,
            val_len as usize,
        );
        dst.push(EntityColumnRef {
            name: &payload[no..no + nl],
            value: &payload[vo..vo + vl],
        });
    }
    if let Some((i, j)) = find_duplicate(&dst[start..]) {
        dst.truncate(start);
        return Err(OndaError::NotEntity(dup_message(i, j)));
    }
    let stored = read_u32(&value[n - 4..]);
    let computed = checksum(&value[..n - 4]);
    if stored != computed {
        dst.truncate(start);
        return Err(OndaError::Corruption(format!(
            "entity checksum mismatch (stored {stored:#010x}, computed {computed:#010x})"
        )));
    }
    Ok(())
}

/// Order `cols` by name in byte order, in place (stable).
///
/// Encoding preserves the caller's order, so the same columns in another order
/// are different bytes; sort first when a canonical frame is wanted, e.g. to
/// compare entities by their encoding.
pub fn sort_columns<C: EntityColumnLike>(cols: &mut [C]) {
    cols.sort_by(|a, b| a.name().cmp(b.name()));
}

/// Decode `value` and copy out the columns named in `names`, in `names` order:
/// `None` for a name the entity does not carry. The projection helper behind
/// [`DB::get_columns`](crate::DB::get_columns).
pub fn project_columns(value: &[u8], names: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
    let cols = decode_entity(value)?;
    Ok(names
        .iter()
        .map(|want| {
            cols.iter()
                .find(|c| c.name == *want)
                .map(|c| c.value.to_vec())
        })
        .collect())
}

impl crate::DB {
    /// Write `cols` as an entity under `key`, in its own transaction — the
    /// entity counterpart of [`put`](crate::DB::put). `ttl` (zero for none)
    /// applies to the whole entity. The column set is validated before
    /// anything is written (see [`encode_entity`]).
    pub fn put_entity<C: EntityColumnLike>(
        &self,
        cf: &std::sync::Arc<crate::ColumnFamily>,
        key: &[u8],
        cols: &[C],
        ttl: std::time::Duration,
    ) -> Result<()> {
        let frame = encode_entity(cols)?;
        self.put(cf, key, &frame, ttl)
    }

    /// Read `key` and decode it as an entity, the entity counterpart of
    /// [`get`](crate::DB::get). `NotFound` for an absent, deleted or expired
    /// key; [`OndaError::NotEntity`] for a plain value. The columns are owned
    /// copies, in the order they were written.
    pub fn get_entity(
        &self,
        cf: &std::sync::Arc<crate::ColumnFamily>,
        key: &[u8],
    ) -> Result<Vec<EntityColumn>> {
        let value = self.get(cf, key)?;
        owned_columns(&value)
    }

    /// Read `key` as an entity and return only the columns named in `names`,
    /// in `names` order — `None` for a name the entity does not carry.
    ///
    /// An ondaDB convenience with no wavesdb counterpart: the whole frame is
    /// still read and validated (checksum included); only the copying is
    /// narrowed. Errors as [`get_entity`](Self::get_entity).
    pub fn get_columns(
        &self,
        cf: &std::sync::Arc<crate::ColumnFamily>,
        key: &[u8],
        names: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let value = self.get(cf, key)?;
        project_columns(&value, names)
    }
}

impl crate::Txn {
    /// Write `cols` as an entity under `key` in this transaction, exactly as
    /// [`put`](crate::Txn::put) writes any value: buffered until commit, and
    /// conflicting at whole-key granularity. The frame is built first, so an
    /// invalid column set leaves the transaction untouched.
    pub fn put_entity<C: EntityColumnLike>(
        &mut self,
        cf: &std::sync::Arc<crate::ColumnFamily>,
        key: &[u8],
        cols: &[C],
        ttl: std::time::Duration,
    ) -> Result<()> {
        let frame = encode_entity(cols)?;
        self.put(cf, key, &frame, ttl)
    }

    /// Read `key` through this transaction (its own writes, savepoints and
    /// snapshot included) and decode it as an entity. Errors as
    /// [`DB::get_entity`](crate::DB::get_entity).
    pub fn get_entity(
        &mut self,
        cf: &std::sync::Arc<crate::ColumnFamily>,
        key: &[u8],
    ) -> Result<Vec<EntityColumn>> {
        let value = self.get(cf, key)?;
        owned_columns(&value)
    }

    /// [`DB::get_columns`](crate::DB::get_columns) through this transaction.
    pub fn get_columns(
        &mut self,
        cf: &std::sync::Arc<crate::ColumnFamily>,
        key: &[u8],
        names: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>> {
        let value = self.get(cf, key)?;
        project_columns(&value, names)
    }
}

fn owned_columns(value: &[u8]) -> Result<Vec<EntityColumn>> {
    Ok(decode_entity(value)?
        .into_iter()
        .map(EntityColumnRef::into_owned)
        .collect())
}

/// Read one directory entry at `*pos`, advancing it. `None` if the directory
/// is truncated or a varint is malformed.
fn dir_entry(dir: &[u8], pos: &mut usize) -> Option<(u64, u64, u64, u64)> {
    let name_off = read_u32(dir.get(*pos..*pos + 4)?) as u64;
    *pos += 4;
    let (name_len, w) = bounded_varint(&dir[*pos..])?;
    *pos += w;
    let val_off = read_u32(dir.get(*pos..*pos + 4)?) as u64;
    *pos += 4;
    let (val_len, w) = bounded_varint(&dir[*pos..])?;
    *pos += w;
    Some((name_off, name_len, val_off, val_len))
}

/// A LEB128 length of at most five bytes and at most `u32::MAX` in value.
/// Deliberately not `encoding::uvarint`, which accepts ten bytes and the whole
/// `u64` range; the spec bounds both.
fn bounded_varint(b: &[u8]) -> Option<(u64, usize)> {
    let mut x: u64 = 0;
    let mut shift = 0u32;
    for (i, &c) in b.iter().take(MAX_VARINT).enumerate() {
        x |= u64::from(c & 0x7f) << shift;
        if c < 0x80 {
            return (x <= u64::from(u32::MAX)).then_some((x, i + 1));
        }
        shift += 7;
    }
    None
}

/// The first pair `(i, j)`, `j < i`, of columns sharing a name.
///
/// Small sets compare pairwise; larger ones use one power-of-two
/// open-addressing table at most half full (one allocation, no per-name
/// keys), so validating a wide entity stays linear.
fn find_duplicate<C: EntityColumnLike>(cols: &[C]) -> Option<(usize, usize)> {
    if cols.len() <= LINEAR_SCAN_MAX {
        for i in 1..cols.len() {
            for j in 0..i {
                if cols[i].name() == cols[j].name() {
                    return Some((i, j));
                }
            }
        }
        return None;
    }
    let size = (2 * cols.len()).next_power_of_two().max(64);
    let mask = (size - 1) as u64;
    // Slots hold index + 1; 0 is empty.
    let mut table = vec![0u32; size];
    for (i, c) in cols.iter().enumerate() {
        let mut h = name_hash(c.name()) & mask;
        loop {
            let slot = table[h as usize];
            if slot == 0 {
                table[h as usize] = i as u32 + 1;
                break;
            }
            let j = slot as usize - 1;
            if cols[j].name() == c.name() {
                return Some((i, j));
            }
            h = (h + 1) & mask;
        }
    }
    None
}

/// FNV-1a over the name; only buckets names, so quality beyond avoiding
/// trivial collisions does not matter.
fn name_hash(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &c in b {
        h ^= u64::from(c);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

fn dup_message(i: usize, j: usize) -> String {
    format!("entity column {i} duplicates the name of column {j}")
}

fn not_entity(detail: std::fmt::Arguments<'_>) -> OndaError {
    OndaError::NotEntity(detail.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(pairs: &[(&str, &str)]) -> Vec<EntityColumn> {
        pairs.iter().map(|(n, v)| EntityColumn::new(*n, *v)).collect()
    }

    /// Recompute the trailing CRC so an altered frame is judged on structure.
    fn refix(mut b: Vec<u8>) -> Vec<u8> {
        let n = b.len();
        let crc = checksum(&b[..n - 4]);
        put_u32(&mut b[n - 4..], crc);
        b
    }

    fn is_not_entity<T: std::fmt::Debug>(r: Result<T>) -> bool {
        matches!(r, Err(OndaError::NotEntity(_)))
    }

    #[test]
    fn bounded_varint_limits() {
        assert_eq!(bounded_varint(&[0x00]), Some((0, 1)));
        assert_eq!(bounded_varint(&[0xac, 0x02]), Some((300, 2)));
        assert_eq!(
            bounded_varint(&[0xff, 0xff, 0xff, 0xff, 0x0f]),
            Some((u64::from(u32::MAX), 5))
        );
        // u32::MAX + 1 in five bytes: out of range.
        assert_eq!(bounded_varint(&[0x80, 0x80, 0x80, 0x80, 0x10]), None);
        // Six bytes, even if present.
        assert_eq!(bounded_varint(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]), None);
        assert_eq!(bounded_varint(&[0x81]), None);
        assert_eq!(bounded_varint(&[]), None);
    }

    #[test]
    fn duplicate_detection_both_paths() {
        let mut many: Vec<EntityColumn> = (0..200)
            .map(|i| EntityColumn::new(format!("column-{i}"), ""))
            .collect();
        assert_eq!(find_duplicate(&many), None);
        many[199].name = b"column-0".to_vec();
        assert_eq!(find_duplicate(&many), Some((199, 0)));
        let few = cols(&[("a", "1"), ("b", "2"), ("a", "3")]);
        assert_eq!(find_duplicate(&few), Some((2, 0)));
    }

    #[test]
    fn huge_count_is_rejected_by_arithmetic() {
        let mut b = encode_entity(&cols(&[("a", "b")])).unwrap();
        put_u32(&mut b[10..14], u32::MAX);
        assert!(is_not_entity(decode_entity(&refix(b))));
    }

    #[test]
    fn decode_into_preserves_dst_on_every_error() {
        let good = encode_entity(&cols(&[("a", "1"), ("b", "2")])).unwrap();
        let mut dup = encode_entity(&cols(&[("a", "1"), ("b", "2")])).unwrap();
        let h = read_u32(&dup[6..10]) as usize;
        dup[h + 2] = b'a'; // second name "b" -> "a"
        let dup = refix(dup);
        let mut bad_crc = good.clone();
        *bad_crc.last_mut().unwrap() ^= 0xff;
        let mut prefix = Vec::new();
        decode_entity_into(&mut prefix, &good).unwrap();
        for frame in [&dup, &bad_crc, &good[..17].to_vec()] {
            let mut dst = prefix.clone();
            assert!(decode_entity_into(&mut dst, frame).is_err());
            assert_eq!(dst, prefix);
        }
    }
}
