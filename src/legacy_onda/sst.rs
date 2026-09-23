//! 0.9.x SSTable decoders: the `WAVESST1` footer, its flag byte, the pre-footer
//! aux handle, the trailing-tag bloom encoding (with the FNV hash 0.9 still
//! read), and the vlog frame versions.
//!
//! ```text
//! klog: [data blocks] [bloom?] [aux?] [index] [aux_off u64 | aux_len u64]? [footer 64B]
//!
//! footer: 0 index off u64 | 8 index len u64 | 16 bloom off u64 | 24 bloom len u64
//!         | 32 num_entries u64 | 40 max_seq u64 | 48 flags u8 | 49..56 unused
//!         | 56 magic u64 = 0x5741_5645_5353_5431 ("WAVESST1" read as a LE u64)
//! ```
//!
//! The footer is **not** checksummed in 0.9 — one of the gaps epoch 1 closed.
//! Blocks are framed exactly as epoch 1 frames them (`[alg][comp_len][raw_len]
//! [crc][payload]`) but under an IEEE CRC and 0.9's codec ids.
//!
//! The flag byte is table-level and carries format meaning epoch 1 moved
//! elsewhere: restart trailers were optional (`0x04`), vlog frames came in two
//! versions (`0x08`), the extended entry layout and the 16-byte aux handle
//! ahead of the footer were a flag (`0x10`), and so was prefix-delta (`0x20`).

use std::sync::Arc;

use crate::bloom::{Bloom, HashKind};
use crate::sst::Reader;
use crate::encoding::{read_u64, uvarint};
use crate::error::{OndaError, Result};

/// Fixed footer width.
pub const FOOTER_SIZE: usize = 64;
/// `WAVESST1`, as 0.9 stored it (a little-endian `u64`).
pub const FOOTER_MAGIC: u64 = 0x5741_5645_5353_5431;
/// A bloom block is present.
pub const FLAG_HAS_BLOOM: u8 = 0x01;
/// The index is a B+tree root.
pub const FLAG_BTREE: u8 = 0x02;
/// Data blocks carry a restart trailer.
pub const FLAG_RESTARTS: u8 = 0x04;
/// Vlog frames are v2 (`crc | alg | stored_len | stored`); otherwise v1
/// (`crc | raw value`).
pub const FLAG_VLOG_V2: u8 = 0x08;
/// Extended (kind-bearing) entries, and the 16-byte aux handle before the footer.
pub const FLAG_EXTENDED_BLOCK: u8 = 0x10;
/// Prefix-delta entries (requires extended and restarts).
pub const FLAG_PREFIX_DELTA: u8 = 0x20;
/// Every flag bit 0.9.x implemented.
pub const KNOWN_FLAGS: u8 = FLAG_HAS_BLOOM
    | FLAG_BTREE
    | FLAG_RESTARTS
    | FLAG_VLOG_V2
    | FLAG_EXTENDED_BLOCK
    | FLAG_PREFIX_DELTA;
/// Width of the pre-footer aux handle of an extended table.
pub const AUX_HANDLE_LEN: usize = 16;
/// v1 vlog frame header: the CRC only.
pub const VLOG_V1_HDR_LEN: usize = 4;
/// v2 vlog frame header: crc(4) + alg(1) + stored_len(4).
pub const VLOG_V2_HDR_LEN: usize = 9;

/// A decoded 0.9 footer, plus the aux handle when the table is extended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Footer {
    pub index: (u64, u64),
    pub bloom: (u64, u64),
    pub num_entries: u64,
    pub max_seq: u64,
    pub flags: u8,
    /// `Some` iff [`FLAG_EXTENDED_BLOCK`]; `(0, 0)` means no aux block.
    pub aux: Option<(u64, u64)>,
}

impl Footer {
    pub fn has_bloom(&self) -> bool {
        self.flags & FLAG_HAS_BLOOM != 0 && self.bloom.1 > 0
    }
    pub fn btree(&self) -> bool {
        self.flags & FLAG_BTREE != 0
    }
    pub fn restarts(&self) -> bool {
        self.flags & FLAG_RESTARTS != 0
    }
    pub fn vlog_v2(&self) -> bool {
        self.flags & FLAG_VLOG_V2 != 0
    }
    pub fn extended(&self) -> bool {
        self.flags & FLAG_EXTENDED_BLOCK != 0
    }
    pub fn prefix_delta(&self) -> bool {
        self.flags & FLAG_PREFIX_DELTA != 0
    }
}

fn corrupt() -> OndaError {
    OndaError::Corruption("0.9 sst: corruption detected".into())
}

/// How many trailing bytes [`parse_footer`] may need: the footer plus the aux
/// handle ahead of it.
pub const TAIL_LEN: usize = FOOTER_SIZE + AUX_HANDLE_LEN;

/// Decode the footer from `tail`, the **last** `min(file_len, TAIL_LEN)` bytes of
/// a klog of `file_len` bytes, with 0.9's rules: wrong magic is `Corruption`,
/// an unknown flag bit `UnsupportedFormat`, prefix-delta without extended or
/// restarts `Corruption`, and an aux handle must address bytes ahead of itself.
pub fn parse_footer(tail: &[u8], file_len: u64) -> Result<Footer> {
    if (file_len as usize) < FOOTER_SIZE || tail.len() < FOOTER_SIZE {
        return Err(corrupt());
    }
    let footer = &tail[tail.len() - FOOTER_SIZE..];
    if read_u64(&footer[56..64]) != FOOTER_MAGIC {
        return Err(corrupt());
    }
    let flags = footer[48];
    if flags & !KNOWN_FLAGS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "0.9 sst footer flags {flags:#04x} outside known mask {KNOWN_FLAGS:#04x}"
        )));
    }
    if flags & FLAG_PREFIX_DELTA != 0 {
        if flags & FLAG_EXTENDED_BLOCK == 0 {
            return Err(OndaError::Corruption(
                "0.9 sst: FOOTER_PREFIX_DELTA without FOOTER_EXTENDED_BLOCK".into(),
            ));
        }
        if flags & FLAG_RESTARTS == 0 {
            return Err(OndaError::Corruption(
                "0.9 sst: FOOTER_PREFIX_DELTA without FOOTER_RESTARTS".into(),
            ));
        }
    }
    let aux = if flags & FLAG_EXTENDED_BLOCK != 0 {
        if (file_len as usize) < TAIL_LEN || tail.len() < TAIL_LEN {
            return Err(corrupt());
        }
        let a = &tail[tail.len() - TAIL_LEN..tail.len() - FOOTER_SIZE];
        let (off, len) = (read_u64(&a[0..8]), read_u64(&a[8..16]));
        let limit = file_len - TAIL_LEN as u64;
        if off > limit || len > limit - off {
            return Err(corrupt());
        }
        Some((off, len))
    } else {
        None
    };
    Ok(Footer {
        index: (read_u64(&footer[0..8]), read_u64(&footer[8..16])),
        bloom: (read_u64(&footer[16..24]), read_u64(&footer[24..32])),
        num_entries: read_u64(&footer[32..40]),
        max_seq: read_u64(&footer[40..48]),
        flags,
        aux,
    })
}

/// Open a 0.9 klog (and the vlog beside it) for reading, on local storage with
/// a private block cache.
///
/// The table decodes through the shared [`Reader`] with the 0.9 profile: the
/// 64-byte footer and its flag byte, IEEE block and vlog checksums, 0.9 codec
/// ids, and the trailing-tag bloom.
pub fn open_table(klog_path: &str, cmp: crate::comparator::ComparatorRef) -> Result<Arc<Reader>> {
    Reader::open_profiled(
        klog_path,
        crate::storage::LocalStorage::new(
            Arc::new(crate::cache::FileCache::new(4)),
            cfg!(feature = "mmap-reads"),
        ),
        Arc::new(crate::cache::BlockCache::new(1 << 20)),
        1,
        cmp,
        0,
        None,
        crate::format::FormatProfile::Onda09,
    )
}

/// 0.9's bloom hash for filters written before the xxh3 tag existed: FNV-1a-64
/// with the **truncated** offset basis `1469598103934665603` (the real basis is
/// `14695981039346656037`). Kept bit-exact: a filter built with it answers only
/// to this function.
pub fn fnv09(key: &[u8]) -> u64 {
    super::fnv1a64_09(key)
}

/// Decode a 0.9 dense bloom block: `m uvarint | k uvarint | words u64 LE |
/// [hash tag u8]`. The **trailing** tag is absent on the oldest filters, which
/// means FNV; `0` is FNV, `1` xxh3, anything else `Corruption`.
pub fn decode_bloom(mut p: &[u8]) -> Result<Bloom> {
    let corrupt = || OndaError::Corruption("0.9 bloom: truncated".into());
    let (m, n) = uvarint(p).ok_or_else(corrupt)?;
    p = &p[n..];
    let (k, n) = uvarint(p).ok_or_else(corrupt)?;
    p = &p[n..];
    let words = usize::try_from(m.div_ceil(64)).map_err(|_| corrupt())?;
    let need = words.checked_mul(8).ok_or_else(corrupt)?;
    if p.len() < need {
        return Err(corrupt());
    }
    let bits = (0..words).map(|i| read_u64(&p[i * 8..])).collect();
    let hash = match p.get(need).copied() {
        None | Some(0) => HashKind::Fnv09,
        Some(1) => HashKind::Xxh3,
        Some(other) => {
            return Err(OndaError::Corruption(format!(
                "0.9 bloom: unknown hash id {other}"
            )))
        }
    };
    let k = u32::try_from(k).map_err(|_| corrupt())?;
    if m == 0 || k == 0 {
        return Err(corrupt());
    }
    Ok(Bloom::from_parts(bits, m, k, hash))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Vec<u8> {
        std::fs::read(crate::util::legacy_fixture(name)).unwrap()
    }

    fn tail(bytes: &[u8]) -> &[u8] {
        &bytes[bytes.len().saturating_sub(TAIL_LEN)..]
    }

    const KLOGS: &[&str] = &[
        "klog_legacy_flat_norestarts_nobloom.klog",
        "klog_legacy_flat_norestarts_bloom.klog",
        "klog_legacy_flat_restarts_nobloom.klog",
        "klog_legacy_flat_restarts_bloom.klog",
        "klog_legacy_btree_norestarts_nobloom.klog",
        "klog_legacy_btree_norestarts_bloom.klog",
        "klog_legacy_btree_restarts_nobloom.klog",
        "klog_legacy_btree_restarts_bloom.klog",
        "klog_extended.klog",
    ];

    /// Every frozen klog's footer decodes, and its flags say what its name says.
    #[test]
    fn fixture_footers_decode_to_their_names() {
        for name in KLOGS {
            let b = fixture(name);
            let f = parse_footer(tail(&b), b.len() as u64).unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(f.btree(), name.contains("btree"), "{name}");
            if name.contains("legacy") {
                assert_eq!(f.restarts(), !name.contains("norestarts"), "{name}");
                assert_eq!(f.has_bloom(), !name.contains("nobloom"), "{name}");
                assert!(!f.extended(), "{name}");
                assert_eq!(f.aux, None, "{name}");
            } else {
                assert!(f.extended() && f.restarts() && f.has_bloom(), "{name}");
                assert_eq!(f.aux, Some((0, 0)), "{name}");
            }
            assert!(f.vlog_v2(), "{name}");
            assert_eq!(f.num_entries, 39, "{name}");
        }
    }

    #[test]
    fn footer_corruption_rows_fail_closed() {
        let b = fixture("klog_extended.klog");
        let len = b.len() as u64;
        // Wrong magic.
        let mut t = tail(&b).to_vec();
        let n = t.len();
        t[n - 1] ^= 0xFF;
        assert_eq!(parse_footer(&t, len).unwrap_err().kind(), "corruption");
        // Unknown flag bit: a newer format, not a damaged one.
        let mut t = tail(&b).to_vec();
        t[n - FOOTER_SIZE + 48] |= 0x40;
        assert_eq!(parse_footer(&t, len).unwrap_err().kind(), "unsupported_format");
        // Prefix-delta without restarts.
        let mut t = tail(&b).to_vec();
        t[n - FOOTER_SIZE + 48] = FLAG_EXTENDED_BLOCK | FLAG_PREFIX_DELTA | FLAG_VLOG_V2;
        assert_eq!(parse_footer(&t, len).unwrap_err().kind(), "corruption");
        // An aux handle past the end of the file.
        let mut t = tail(&b).to_vec();
        t[0..8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert_eq!(parse_footer(&t, len).unwrap_err().kind(), "corruption");
        // Truncated below one footer.
        assert_eq!(parse_footer(&b[..10], 10).unwrap_err().kind(), "corruption");
    }

    /// The bloom block of a frozen klog decodes and admits every key it holds.
    #[test]
    fn fixture_bloom_decodes_and_admits_its_keys() {
        let b = fixture("klog_legacy_flat_restarts_bloom.klog");
        let f = parse_footer(tail(&b), b.len() as u64).unwrap();
        let (off, len) = f.bloom;
        let block = &b[off as usize..(off + len) as usize];
        let (_, payload, _, _) = crate::legacy_onda::block_payload(block, true).unwrap();
        let bloom = decode_bloom(payload).unwrap();
        for i in 1..40u64 {
            let k = format!("k{i:02}");
            assert!(bloom.may_contain(k.as_bytes()), "{k}");
        }
    }

    /// `restart_scan_offset` runs through the shared `restart_lower_bound`, and
    /// must return byte-for-byte the offsets an open-coded binary search
    /// returns over a frozen 0.9 table — the reader's restart search is shared
    /// by both format families.
    #[test]
    fn restart_lower_bound_matches_open_coded_scan_offset() {
        use crate::encoding::read_u32;
        use crate::sst::{cmp_internal, decode_entry};
        let dir = tempfile::tempdir().unwrap();
        let klog = dir.path().join("t.klog");
        std::fs::write(&klog, fixture("klog_legacy_flat_restarts_bloom.klog")).unwrap();
        std::fs::write(
            klog.with_extension("vlog"),
            fixture("klog_legacy_flat_restarts_bloom.vlog"),
        )
        .unwrap();
        let r = open_table(klog.to_str().unwrap(), crate::comparator::default_comparator()).unwrap();
        assert!(!r.prefix_delta());
        let open_coded = |raw: &[u8], restarts: &[u8], key: &[u8], seq: u64| -> usize {
            if restarts.len() < 8 {
                return 0;
            }
            let restart_off = |i: usize| read_u32(&restarts[i * 4..]) as usize;
            let (mut lo, mut hi) = (0usize, restarts.len() / 4);
            while lo < hi {
                let mid = (lo + hi) / 2;
                let (entry, _) = decode_entry(raw, r.entry_layout(), restart_off(mid)).unwrap();
                if cmp_internal(r.comparator(), entry.user_key(raw), entry.seq, key, seq).is_lt() {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            if lo > 0 {
                restart_off(lo - 1)
            } else {
                0
            }
        };
        let mut probes: Vec<String> = (1..45u64).map(|i| format!("k{i:02}")).collect();
        probes.extend(["a".into(), "k00".into(), "k99".into(), "zzz".into()]);
        let mut checked = 0;
        for bi in 0..r.data_block_count() {
            let block = r.read_data_block_local(bi).unwrap();
            let (raw, restarts) = r.split_block(block.bytes()).unwrap();
            assert!(!restarts.is_empty(), "a restarts fixture has anchors");
            for probe in &probes {
                for seq in [0u64, 25, u64::MAX] {
                    let want = open_coded(raw, restarts, probe.as_bytes(), seq);
                    let got = r
                        .restart_scan_offset(raw, restarts, probe.as_bytes(), seq)
                        .unwrap();
                    assert_eq!(got, want, "block {bi}, probe {probe}, seq {seq}");
                    checked += 1;
                }
            }
        }
        assert!(checked > 100, "the fixture must exercise the search");
    }

    /// A filter with no trailing tag is FNV (0.9's oldest encoding), and it
    /// answers only to the truncated-basis hash.
    #[test]
    fn an_untagged_bloom_is_fnv09() {
        let mut enc = Vec::new();
        crate::encoding::append_uvarint(&mut enc, 64);
        crate::encoding::append_uvarint(&mut enc, 3);
        let mut word = 0u64;
        let h = fnv09(b"key");
        let (h1, h2) = (h as u32, (h >> 32) as u32);
        for i in 0..3u32 {
            word |= 1 << ((h1.wrapping_add(i.wrapping_mul(h2)) % 64) as u64);
        }
        enc.extend_from_slice(&word.to_le_bytes());
        let b = decode_bloom(&enc).unwrap();
        assert!(b.may_contain(b"key"));
        // Tag 2 does not exist in 0.9.
        enc.push(2);
        assert_eq!(decode_bloom(&enc).unwrap_err().kind(), "corruption");
        // Truncated inside the words.
        assert_eq!(decode_bloom(&enc[..4]).unwrap_err().kind(), "corruption");
    }
}
