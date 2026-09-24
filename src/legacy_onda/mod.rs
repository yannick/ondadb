//! Read-only decoders for the ondaDB **0.9.x** on-disk formats.
//!
//! yoloDB format epoch 1 replaced every persisted identifier ondaDB 0.9 wrote:
//! new magics and versions, CRC32-C instead of IEEE CRC-32, a checksummed
//! 96-byte SST footer, a sectioned manifest, headers on WAL segments and value
//! logs, and a tagged config blob. A 0.9 directory is therefore unreadable by
//! the epoch-1 engine, and this module is the only place the 0.9 formats are
//! still understood. It exists so a 0.9 database can be opened **read-only**
//! and, later, rebuilt into epoch 1 (the auto-upgrade of plan C §1.3).
//!
//! Everything here is **decode-only**. No production path writes a 0.9 byte;
//! the few encoders that remain are `#[cfg(test)]` fixture builders. The whole
//! module sits behind the default-on `legacy-onda` cargo feature — building
//! without it makes a 0.9 directory a hard `UnsupportedFormat` refusal.
//!
//! | Artifact | 0.9 container | Decoder |
//! |---|---|---|
//! | `MANIFEST` | `WVMF` u32, version 1/2, positional body + `ONDA*` tails, IEEE CRC | [`manifest`] |
//! | `MANIFEST-EDITS` | `ONDE` u32 header (28 B), IEEE-framed records | [`edit_log`] |
//! | CF config blob | positional fields + `ONDA*` tails, durations in µs | [`config`] |
//! | WAL | headerless IEEE frames, 4 stripes | [`wal`] |
//! | SST footer | `WAVESST1`, 64 B, unchecksummed, flag byte | [`sst`] |
//! | Bloom | trailing hash tag, FNV (truncated basis) or xxh3 | [`sst::decode_bloom`] |
//! | Blocks / vlog frames | IEEE CRC, codec ids 0–5 (`2`/`4` = LZ4) | [`block_payload`], [`codec`] |
//! | Unified CF id | FNV-1a-64 with the truncated basis | [`cf_id_09`] |
//!
//! The frozen fixtures these decoders are pinned against live in
//! `tests/fixtures/legacy-onda/`: the byte-level corpus (`phase1/`, `blocks/`)
//! and three whole database directories written by 0.9.1 itself (`db-*`).

pub mod config;
pub mod edit_log;
pub mod manifest;
pub mod sst;
pub mod wal;

use crate::config::Compression;
use crate::encoding::read_u32;
use crate::error::{OndaError, Result};

/// CRC-32/IEEE (polynomial `0xEDB88320`) — what every 0.9 artifact carries.
///
/// 0.9's documentation called its checksum CRC32-C; the code always computed
/// IEEE through `crc32fast`. Epoch 1 made the documentation true, which is why
/// the IEEE implementation now lives only here.
#[inline]
pub fn checksum_ieee(b: &[u8]) -> u32 {
    let mut h = crc32fast::Hasher::new();
    h.update(b);
    h.finalize()
}

/// 0.9's FNV-1a-64 **offset basis**, missing its last digit. The correct
/// basis is `14695981039346656037`; 0.9's unified CF ids and its oldest bloom
/// filters were computed with this one, so reading them needs it bit-exact.
pub const FNV_OFFSET_BASIS_09: u64 = 1469598103934665603;
const FNV_PRIME: u64 = 1099511628211;

/// FNV-1a-64 under [`FNV_OFFSET_BASIS_09`].
pub fn fnv1a64_09(bytes: &[u8]) -> u64 {
    let mut h = FNV_OFFSET_BASIS_09;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// The unified-layout column-family id 0.9 derived from a name — the 8-byte
/// big-endian prefix on every key in a 0.9 unified WAL.
pub fn cf_id_09(name: &str) -> u64 {
    fnv1a64_09(name.as_bytes())
}

/// Map a 0.9 codec id to its codec: `0` none, `1` snappy, `2` LZ4, `3` zstd,
/// `4` LZ4 (the "fast" alias — the same raw LZ4 block bytes), `5` raw deflate.
pub fn codec(id: u8) -> Option<Compression> {
    Some(match id {
        0 => Compression::None,
        1 => Compression::Snappy,
        2 => Compression::Lz4,
        3 => Compression::Zstd,
        4 => Compression::Lz4Fast,
        5 => Compression::Flate,
        _ => return None,
    })
}

/// The 0.9 id of `c` (test-only: nothing writes a 0.9 byte).
#[cfg(test)]
pub(crate) fn codec_id(c: Compression) -> u8 {
    match c {
        Compression::None => 0,
        Compression::Snappy => 1,
        Compression::Lz4 => 2,
        Compression::Zstd => 3,
        Compression::Lz4Fast => 4,
        Compression::Flate => 5,
    }
}

/// Fixed 0.9 block-frame header: `alg u8 | comp_len u32 | raw_len u32 | crc u32`.
pub const BLOCK_HEADER: usize = 13;

/// Parse a 0.9 framed block in place: validate the header and (when `verify`)
/// the IEEE CRC over the stored payload, and return
/// `(codec, payload, raw_len, framed_len)` without decompressing.
pub fn block_payload(buf: &[u8], verify: bool) -> Result<(Compression, &[u8], usize, usize)> {
    if buf.len() < BLOCK_HEADER {
        return Err(OndaError::Corruption("0.9 block: short header".into()));
    }
    let alg = codec(buf[0])
        .ok_or_else(|| OndaError::Corruption(format!("0.9 block: bad algorithm {}", buf[0])))?;
    let comp_len = read_u32(&buf[1..]) as usize;
    let raw_len = read_u32(&buf[5..]) as usize;
    let want = read_u32(&buf[9..]);
    let total = BLOCK_HEADER
        .checked_add(comp_len)
        .ok_or_else(|| OndaError::Corruption("0.9 block: length overflow".into()))?;
    if buf.len() < total {
        return Err(OndaError::Corruption("0.9 block: truncated payload".into()));
    }
    let payload = &buf[BLOCK_HEADER..total];
    if verify && checksum_ieee(payload) != want {
        return Err(OndaError::Corruption("0.9 block: checksum mismatch".into()));
    }
    Ok((alg, payload, raw_len, total))
}

/// Load a 0.9 directory's whole catalog: the `MANIFEST` snapshot with its
/// `MANIFEST-EDITS` log replayed onto it, every CF config converted from the
/// 0.9 positional blob to the one [`ColumnFamilyConfig::encode`] writes, and —
/// under the unified WAL layout — every CF pinned to the 0.9 unified id its WAL
/// keys carry.
///
/// [`ColumnFamilyConfig::encode`]: crate::ColumnFamilyConfig::encode
pub fn recover_catalog(dir: impl AsRef<std::path::Path>) -> Result<crate::manifest::Manifest> {
    let mut m = edit_log::recover_raw(dir.as_ref())?;
    for cf in &mut m.cfs {
        cf.config = config::decode(&cf.config).encode();
        // Every 0.9 family's id is the truncated-basis hash, and a 0.9 unified
        // WAL's keys carry exactly that prefix; pinning it here is what lets
        // the epoch-1 engine route those records without translating them.
        cf.unified_id = Some(cf_id_09(&cf.name));
    }
    Ok(m)
}

/// Whether `dir` holds an ondaDB 0.9 database: a `MANIFEST` whose first four
/// bytes are 0.9's `WVMF` magic. `false` for a missing manifest (an empty
/// directory is nobody's format) and for an epoch-1 one.
pub fn is_legacy_dir(dir: impl AsRef<std::path::Path>) -> Result<bool> {
    use std::io::Read;
    let mut f = match std::fs::File::open(crate::manifest::manifest_path(dir)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let mut magic = [0u8; 4];
    match f.read_exact(&mut magic) {
        Ok(()) => Ok(read_u32(&magic) == manifest::MAGIC),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// Open an ondaDB 0.9 database **read-only**, through the engine.
///
/// The returned [`DB`](crate::DB) is the ordinary engine over a 0.9 directory:
/// its catalog comes from [`recover_catalog`], its WAL generations replay
/// through [`wal::replay`] into memtables (per-CF or unified, whichever the
/// manifest records), and its tables open with the 0.9 decoding profile —
/// the `WAVESST1` footer, IEEE checksums, 0.9 codec ids and blooms. Gets,
/// scans, merge folding and range tombstones all work as they did in 0.9.
///
/// Nothing is ever written: `opts.read_only` is forced on, and the WAL layout
/// is taken from the manifest (`opts.unified_memtable` is set to match). The
/// caller still supplies what 0.9 needed at open — merge operators and
/// partition functions by name. This is the source side of the epoch-1
/// auto-upgrade: stream every family out of this handle into an epoch-1
/// writer.
///
/// Refuses a directory that is not a 0.9 database with `InvalidArgs`.
pub fn open_read_only(mut opts: crate::Options) -> Result<crate::DB> {
    let dir = std::path::PathBuf::from(&opts.path);
    if !is_legacy_dir(&dir)? {
        return Err(OndaError::InvalidArgs(format!(
            "{}: not an ondaDB 0.9 database (no WVMF MANIFEST)",
            dir.display()
        )));
    }
    // The layout comes from the recovered catalog, not the bare snapshot: an
    // edit-log `SetWalLayout` may have flipped it since the last snapshot.
    let catalog = recover_catalog(&dir)?;
    opts.read_only = true;
    opts.migrate_to_unified = false;
    opts.unified_memtable = catalog.wal_layout == crate::manifest::WalLayout::Unified;
    crate::DB::open_with_format(opts, crate::format::FormatProfile::Onda09)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The standard CRC-32/IEEE check value.
    #[test]
    fn ieee_check_value_is_pinned() {
        assert_eq!(checksum_ieee(b"123456789"), 0xcbf4_3926);
    }

    /// The 0.9 unified id is FNV-1a over the truncated basis — not FNV-1a.
    #[test]
    fn cf_id_09_uses_the_truncated_basis() {
        assert_eq!(fnv1a64_09(b""), 1469598103934665603);
        assert_ne!(fnv1a64_09(b"default"), {
            let mut h: u64 = 14695981039346656037;
            for &b in b"default" {
                h ^= u64::from(b);
                h = h.wrapping_mul(FNV_PRIME);
            }
            h
        });
    }

    #[test]
    fn codec_ids_are_the_0_9_table() {
        for id in 0..=5u8 {
            assert_eq!(codec_id(codec(id).unwrap()), id);
        }
        for id in [6u8, 7, 8, 255] {
            assert!(codec(id).is_none());
        }
    }

    /// Each 0.9.1-written database directory decodes into a catalog whose
    /// configs re-decode under the current encoding.
    #[test]
    fn fixture_catalogs_recover() {
        for (name, cfs, unified) in [
            ("db-percf", &["alpha", "beta"][..], false),
            ("db-caps", &["m", "plain"][..], false),
            ("db-unified", &["ua", "ub"][..], true),
        ] {
            let m = recover_catalog(crate::util::legacy_fixture_path(name))
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let mut got: Vec<&str> = m.cfs.iter().map(|c| c.name.as_str()).collect();
            got.sort_unstable();
            assert_eq!(got, cfs, "{name}");
            assert_eq!(
                m.wal_layout == crate::manifest::WalLayout::Unified,
                unified,
                "{name}"
            );
            for cf in &m.cfs {
                crate::ColumnFamilyConfig::decode(&cf.config)
                    .unwrap_or_else(|e| panic!("{name}/{}: {e}", cf.name));
                assert_eq!(cf.unified_id, Some(cf_id_09(&cf.name)));
            }
        }
    }
}
