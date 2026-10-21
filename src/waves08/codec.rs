//! The codecs only wavesdb writes: brotli (codec 8), in data blocks and value
//! frames, and zstd with a trained dictionary (codec 7), in the value frames
//! of a blob file whose header carries one.
//!
//! They live here rather than in [`Compression`] because ondaDB never writes
//! either: a variant there would be a codec every config, writer and
//! `match` in the engine has to account for, for the sake of a read-only
//! path. Every other codec id goes through [`Compression::from_codec_id`]
//! unchanged.

use std::io::Read;

use crate::config::Compression;
use crate::error::{OndaError, Result};
use crate::format::codec::{BROTLI, NONE, ZSTD_DICT};

/// The largest dictionary any wavesdb writer embeds (`compress.MaxDictBytes`);
/// it bounds the allocation a corrupt header can ask for.
pub(crate) const MAX_DICT_BYTES: u32 = 64 << 20;

/// A blob file's dictionary, prepared once for decoding.
pub(crate) type Dict = zstd::dict::DecoderDictionary<'static>;

fn corrupt(what: impl std::fmt::Display) -> OndaError {
    OndaError::Corruption(format!("wavesdb codec: {what}"))
}

/// Decode `stored` — already checksummed — into exactly `raw_len` bytes.
///
/// `dict` is the blob file's dictionary, for a value frame; `None` where no
/// dictionary can apply (a data block, a file without one), which makes a
/// codec-7 frame corruption: no writer emits one there.
pub(crate) fn decode(
    codec: u8,
    stored: &[u8],
    raw_len: usize,
    dict: Option<&Dict>,
) -> Result<Vec<u8>> {
    if codec == NONE {
        if stored.len() != raw_len {
            return Err(corrupt("raw frame length disagrees with its record"));
        }
        return Ok(stored.to_vec());
    }
    // A writer keeps compressed bytes only when they are not longer than the
    // raw ones, and never frames an empty compressed payload.
    if stored.is_empty() || stored.len() > raw_len {
        return Err(corrupt(format!("compressed frame of {} bytes for {raw_len}", stored.len())));
    }
    let raw = match codec {
        BROTLI => brotli(stored, raw_len)?,
        ZSTD_DICT => {
            let dict = dict.ok_or_else(|| corrupt("dictionary frame with no dictionary"))?;
            zstd::bulk::Decompressor::with_prepared_dictionary(dict)
                .and_then(|mut d| d.decompress(stored, raw_len))
                .map_err(|e| corrupt(format!("zstd-dictionary frame: {e}")))?
        }
        other => crate::compress::decompress(Compression::from_codec_id(other)?, stored, raw_len)?,
    };
    if raw.len() != raw_len {
        return Err(corrupt(format!("frame decoded to {} bytes, not {raw_len}", raw.len())));
    }
    Ok(raw)
}

/// Brotli, with the output bounded at `raw_len`: a streaming decoder would
/// expand a corrupt frame without limit, so it reads at most `raw_len` bytes
/// and then probes for one more — which tells "longer than it claims" from
/// "exactly `raw_len`" (wavesdb `decompressBrotli`).
fn brotli(stored: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    let mut r = brotli_decompressor::Decompressor::new(stored, 4096);
    let mut out = vec![0u8; raw_len];
    let mut n = 0;
    while n < raw_len {
        match r.read(&mut out[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(corrupt(format!("brotli: {e}"))),
        }
    }
    let mut probe = [0u8; 1];
    if matches!(r.read(&mut probe), Ok(1)) {
        return Err(corrupt("brotli frame decodes past its recorded length"));
    }
    out.truncate(n);
    Ok(out)
}

/// Parse and decode one wavesdb block frame (`codec u8 | stored_len u32 |
/// raw_len u32 | crc32c u32 | stored`, the epoch-1 frame): the checksum first,
/// then the codec, as for every epoch-1 frame. The second value is
/// `Compression::None` for a stored-raw block and otherwise only says that
/// the block was compressed — brotli has no `Compression` to name it.
pub(crate) fn read_block(buf: &[u8]) -> Result<(Vec<u8>, Compression)> {
    use crate::block::BLOCK_HEADER;
    use crate::encoding::read_u32;
    if buf.len() < BLOCK_HEADER {
        return Err(corrupt("block: short header"));
    }
    let stored_len = read_u32(&buf[1..]) as usize;
    let raw_len = read_u32(&buf[5..]) as usize;
    let stored = buf
        .get(BLOCK_HEADER..BLOCK_HEADER.saturating_add(stored_len))
        .ok_or_else(|| corrupt("block: truncated payload"))?;
    if crate::encoding::checksum(stored) != read_u32(&buf[9..]) {
        return Err(corrupt("block: checksum mismatch"));
    }
    let codec = buf[0];
    // Dictionaries belong to blob files; a block never names one.
    let raw = decode(codec, stored, raw_len, None)?;
    let alg = if codec == NONE { Compression::None } else { Compression::Zstd };
    Ok((raw, alg))
}

/// Prepare a dictionary frame's bytes (`crc32c(dict) u32 | dict_len u32 |
/// dict`, wavesdb `decodeBlobDict`): the length must be the header's, and
/// the checksum covers the dictionary.
pub(crate) fn load_dict(frame: &[u8], want_len: u32) -> Result<Dict> {
    if frame.len() < 8 {
        return Err(corrupt("dictionary frame is truncated"));
    }
    let n = crate::encoding::read_u32(&frame[4..]);
    if n != want_len || 8 + n as usize > frame.len() {
        return Err(corrupt(format!("dictionary frame length {n}, header says {want_len}")));
    }
    let dict = &frame[8..8 + n as usize];
    if crate::encoding::checksum(dict) != crate::encoding::read_u32(frame) {
        return Err(corrupt("dictionary checksum mismatch"));
    }
    Ok(Dict::copy(dict))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A brotli stream for "hello hello hello hello" — produced by Go's
    /// `andybalholm/brotli` (quality 5, lgwin 24, as wavesdb writes), so the
    /// decoder is checked against the writer's bytes, not its own encoder.
    const HELLO_BR: &[u8] = &[
        0x1f, 0x16, 0x00, 0x00, 0x24, 0x40, 0x6a, 0x10, 0x65, 0xea, 0xf0, 0x9c, 0x3e,
    ];
    const HELLO: &[u8] = b"hello hello hello hello";

    #[test]
    fn brotli_is_bounded_both_ways() {
        assert_eq!(decode(BROTLI, HELLO_BR, HELLO.len(), None).unwrap(), HELLO);
        // A recorded length shorter than the stream: refused, not truncated.
        assert!(decode(BROTLI, HELLO_BR, HELLO.len() - 1, None).is_err());
        // Longer: the stream ends early.
        assert!(decode(BROTLI, HELLO_BR, HELLO.len() + 4, None).is_err());
        let mut bad = HELLO_BR.to_vec();
        bad[3] ^= 0xff;
        assert!(decode(BROTLI, &bad, HELLO.len(), None).is_err());
    }

    #[test]
    fn dictionary_frames_need_their_dictionary() {
        let samples: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("{{\"user\":{i},\"name\":\"name-{i}\",\"tags\":[\"a\",\"b\"]}}").into_bytes())
            .collect();
        let dict = zstd::dict::from_samples(&samples, 1024).unwrap();
        let value = samples[7].clone();
        let stored = zstd::bulk::Compressor::with_dictionary(3, &dict)
            .unwrap()
            .compress(&value)
            .unwrap();
        let mut frame = Vec::new();
        crate::encoding::append_u32(&mut frame, crate::encoding::checksum(&dict));
        crate::encoding::append_u32(&mut frame, dict.len() as u32);
        frame.extend_from_slice(&dict);
        let d = load_dict(&frame, dict.len() as u32).unwrap();
        assert_eq!(decode(ZSTD_DICT, &stored, value.len(), Some(&d)).unwrap(), value);
        assert!(decode(ZSTD_DICT, &stored, value.len(), None).is_err());
        assert!(load_dict(&frame, dict.len() as u32 - 1).is_err());
        frame[9] ^= 1;
        assert!(load_dict(&frame, dict.len() as u32).is_err());
    }

    #[test]
    fn plain_codecs_and_lengths() {
        assert_eq!(decode(NONE, b"abc", 3, None).unwrap(), b"abc");
        assert!(decode(NONE, b"abc", 4, None).is_err());
        let z = zstd::bulk::compress(&HELLO.repeat(4), 3).unwrap();
        assert_eq!(decode(crate::format::codec::ZSTD, &z, HELLO.len() * 4, None).unwrap(), HELLO.repeat(4));
        assert!(decode(crate::format::codec::ZSTD, &[], 0, None).is_err());
        assert!(matches!(decode(2, b"x", 1, None), Err(OndaError::UnsupportedFormat(_))));
    }
}
