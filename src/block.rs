//! SSTable block framing.
//!
//! Each block (data, index, or bloom) is independently compressed and
//! checksummed.
//!
//! ```text
//! [codec u8][comp_len u32 LE][raw_len u32 LE][crc32c u32 LE][compressed bytes]
//! ```
//!
//! The checksum covers the compressed bytes.  If compression does not shrink the
//! block it is stored raw (codec `0`), so reads never expand pathologically.
//! The codec byte is an epoch-1 codec id ([`crate::format::codec`]): LZ4 is
//! `6`, and the burned ids `2`/`4` are refused as `UnsupportedFormat`.

use crate::config::Compression;
use crate::encoding::{append_u32, checksum, read_u32};
use crate::error::{OndaError, Result};
use crate::{compress, compress::compress as do_compress};

/// Fixed block-frame header size in bytes.
pub const BLOCK_HEADER: usize = 1 + 4 + 4 + 4;

/// Compress and frame `raw` with `alg`, appending the framed block to `out` and
/// returning the number of bytes written.  Falls back to storing raw bytes when
/// compression would not shrink the block.
pub fn write_block(out: &mut Vec<u8>, alg: Compression, raw: &[u8]) -> Result<usize> {
    let start = out.len();
    let (used_alg, payload) = if alg == Compression::None {
        (Compression::None, raw.to_vec())
    } else {
        let c = do_compress(alg, raw)?;
        if c.len() < raw.len() {
            (alg, c)
        } else {
            (Compression::None, raw.to_vec())
        }
    };
    out.push(used_alg.codec_id());
    append_u32(out, payload.len() as u32);
    append_u32(out, raw.len() as u32);
    append_u32(out, checksum(&payload));
    out.extend_from_slice(&payload);
    Ok(out.len() - start)
}

/// Decode a framed block from the front of `buf`, returning the decompressed
/// bytes and the total framed length consumed.
pub fn read_block(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    if buf.len() < BLOCK_HEADER {
        return Err(OndaError::Corruption("block: short header".into()));
    }
    let (alg, payload, raw_len, total) = block_payload_inner(buf, true)?;
    let raw = compress::decompress(alg, payload, raw_len)?;
    if raw.len() != raw_len {
        return Err(OndaError::Corruption("block: raw length mismatch".into()));
    }
    Ok((raw, total))
}

/// Inspect a framed block in place: validate the header + checksum and return
/// `(algorithm, payload_slice, raw_len, total_framed_len)` **without**
/// decompressing.  For `Compression::None` the payload slice *is* the raw block,
/// enabling zero-copy reads from an mmap.
pub fn block_payload(buf: &[u8]) -> Result<(Compression, &[u8], usize, usize)> {
    block_payload_inner(buf, true)
}

/// Like [`block_payload`] but skips the checksum, for callers that have already
/// verified this exact block once (the bytes are immutable afterwards).
pub fn block_payload_preverified(buf: &[u8]) -> Result<(Compression, &[u8], usize, usize)> {
    block_payload_inner(buf, false)
}

/// [`block_payload`] / [`block_payload_preverified`] for a table of format
/// `profile`: an epoch-1 table takes the fast path below; a 0.9 table (read
/// through `legacy_onda`) is parsed by the frozen 0.9 decoder.
#[inline]
pub(crate) fn block_payload_for(
    profile: crate::format::FormatProfile,
    buf: &[u8],
    verify: bool,
) -> Result<(Compression, &[u8], usize, usize)> {
    match profile {
        crate::format::FormatProfile::Epoch1 => block_payload_inner(buf, verify),
        #[cfg(feature = "legacy-onda")]
        crate::format::FormatProfile::Onda09 => crate::legacy_onda::block_payload(buf, verify),
    }
}

#[inline]
fn block_payload_inner(buf: &[u8], verify: bool) -> Result<(Compression, &[u8], usize, usize)> {
    if buf.len() < BLOCK_HEADER {
        return Err(OndaError::Corruption("block: short header".into()));
    }
    let comp_len = read_u32(&buf[1..]) as usize;
    let raw_len = read_u32(&buf[5..]) as usize;
    let want_crc = read_u32(&buf[9..]);
    let total = BLOCK_HEADER
        .checked_add(comp_len)
        .ok_or_else(|| OndaError::Corruption("block: length overflow".into()))?;
    if buf.len() < total {
        return Err(OndaError::Corruption("block: truncated payload".into()));
    }
    let payload = &buf[BLOCK_HEADER..total];
    // The CRC before the codec: a flipped codec byte is damage (`Corruption`),
    // and only an intact frame naming an unimplemented codec is a newer format.
    if verify && checksum(payload) != want_crc {
        return Err(OndaError::Corruption("block: checksum mismatch".into()));
    }
    let alg = Compression::from_codec_id(buf[0])?;
    Ok((alg, payload, raw_len, total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_all_codecs() {
        let raw = b"the quick brown fox ".repeat(50);
        for alg in [
            Compression::None,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Flate,
        ] {
            let mut out = Vec::new();
            let n = write_block(&mut out, alg, &raw).unwrap();
            assert_eq!(n, out.len());
            let (got, consumed) = read_block(&out).unwrap();
            assert_eq!(got, raw, "alg {alg:?}");
            assert_eq!(consumed, out.len());
        }
    }

    #[test]
    fn incompressible_stored_raw() {
        let mut raw = vec![0u8; 256];
        for (i, b) in raw.iter_mut().enumerate() {
            *b = (i * 7 + 3) as u8;
        }
        let mut out = Vec::new();
        write_block(&mut out, Compression::Zstd, &raw).unwrap();
        // header byte should record None when compression didn't help
        assert_eq!(out[0], Compression::None.codec_id());
        let (got, _) = read_block(&out).unwrap();
        assert_eq!(got, raw);
    }

    #[test]
    fn detects_corruption() {
        let raw = b"hello world".to_vec();
        let mut out = Vec::new();
        write_block(&mut out, Compression::None, &raw).unwrap();
        let n = out.len();
        out[n - 1] ^= 0xFF; // flip a payload byte
        assert!(read_block(&out).is_err());
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(read_block(&[0u8; 4]).is_err());
    }

    /// LZ4 is stored as codec 6 — the same raw LZ4 block bytes 0.9 stored
    /// under id 2 — and the burned ids 2 and 4 are refused, as are the
    /// reserved 7 and 8.
    #[test]
    fn codec_byte_is_the_epoch1_registry() {
        let raw = b"the quick brown fox ".repeat(50);
        let mut out = Vec::new();
        write_block(&mut out, Compression::Lz4, &raw).unwrap();
        assert_eq!(out[0], 6);
        assert_eq!(&out[BLOCK_HEADER..], lz4_flex::compress(&raw).as_slice());
        let mut out2 = Vec::new();
        write_block(&mut out2, Compression::Lz4Fast, &raw).unwrap();
        assert_eq!(out, out2, "LZ4 and LZ4-fast are the same bytes");
        for id in [2u8, 4, 7, 8, 99] {
            let mut bad = out.clone();
            bad[0] = id;
            assert_eq!(
                read_block(&bad).unwrap_err().kind(),
                "unsupported_format",
                "id {id}"
            );
        }
    }
}
