//! Block compression codecs for SSTable klog/vlog blocks (the WAL is never
//! compressed).  Codecs are stateless and safe for concurrent use.
//!
//! dependency — ondaDB ships a real LZ4 codec (`lz4_flex`),
//! algorithm set: none / snappy / lz4 / zstd / lz4fast / flate.

use std::io::{Read, Write};

use crate::config::Compression;
use crate::error::{OndaError, Result};

const ZSTD_LEVEL: i32 = 3;
const ZSTD_FAST_LEVEL: i32 = 1;

/// Every codec is compiled in, so all algorithms are always available.
pub fn is_available(_alg: Compression) -> bool {
    true
}

/// Compress `src`, returning a freshly allocated buffer. `Compression::None`
/// returns a verbatim copy.
pub fn compress(alg: Compression, src: &[u8]) -> Result<Vec<u8>> {
    Ok(match alg {
        Compression::None => src.to_vec(),
        Compression::Snappy => snap::raw::Encoder::new()
            .compress_vec(src)
            .map_err(|e| OndaError::Corruption(format!("snappy compress: {e}")))?,
        Compression::Lz4 | Compression::Lz4Fast => lz4_flex::compress(src),
        Compression::Zstd => zstd::bulk::compress(src, ZSTD_LEVEL)
            .map_err(|e| OndaError::Corruption(format!("zstd compress: {e}")))?,
        Compression::Flate => {
            let mut enc =
                flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(src)?;
            enc.finish()?
        }
    })
}

/// Compress with the fastest setting for the codec (used by [`Compression::Zstd`]
/// fast path / `Lz4Fast`); other codecs behave like [`compress`].
pub fn compress_fast(alg: Compression, src: &[u8]) -> Result<Vec<u8>> {
    match alg {
        Compression::Zstd => zstd::bulk::compress(src, ZSTD_FAST_LEVEL)
            .map_err(|e| OndaError::Corruption(format!("zstd compress: {e}"))),
        _ => compress(alg, src),
    }
}

/// Decompress `src` whose original (uncompressed) length is `raw_len`.
pub fn decompress(alg: Compression, src: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    Ok(match alg {
        Compression::None => src.to_vec(),
        Compression::Snappy => snap::raw::Decoder::new()
            .decompress_vec(src)
            .map_err(|e| OndaError::Corruption(format!("snappy decompress: {e}")))?,
        Compression::Lz4 | Compression::Lz4Fast => lz4_flex::decompress(src, raw_len)
            .map_err(|e| OndaError::Corruption(format!("lz4 decompress: {e}")))?,
        Compression::Zstd => zstd::bulk::decompress(src, raw_len)
            .map_err(|e| OndaError::Corruption(format!("zstd decompress: {e}")))?,
        Compression::Flate => {
            let mut out = Vec::with_capacity(raw_len);
            flate2::read::DeflateDecoder::new(src).read_to_end(&mut out)?;
            out
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALGS: [Compression; 6] = [
        Compression::None,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
        Compression::Lz4Fast,
        Compression::Flate,
    ];

    #[test]
    fn round_trip_compressible() {
        let src = b"the quick brown fox jumps over the lazy dog ".repeat(64);
        for alg in ALGS {
            let c = compress(alg, &src).unwrap();
            let d = decompress(alg, &c, src.len()).unwrap();
            assert_eq!(d, src, "alg {alg:?}");
        }
    }

    #[test]
    fn round_trip_empty() {
        for alg in ALGS {
            let c = compress(alg, b"").unwrap();
            let d = decompress(alg, &c, 0).unwrap();
            assert_eq!(d, b"", "alg {alg:?}");
        }
    }

    #[test]
    fn round_trip_incompressible() {
        // Pseudo-random, hard-to-compress data.
        let mut src = vec![0u8; 4096];
        let mut x: u32 = 0x1234_5678;
        for b in src.iter_mut() {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            *b = x as u8;
        }
        for alg in ALGS {
            let c = compress(alg, &src).unwrap();
            let d = decompress(alg, &c, src.len()).unwrap();
            assert_eq!(d, src, "alg {alg:?}");
        }
    }

    #[test]
    fn compressible_data_shrinks() {
        let src = vec![7u8; 8192];
        for alg in [
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
            Compression::Flate,
        ] {
            let c = compress(alg, &src).unwrap();
            assert!(c.len() < src.len(), "alg {alg:?} did not shrink");
        }
    }

    #[test]
    fn fast_path_round_trips() {
        let src = b"abcabcabc".repeat(100);
        for alg in ALGS {
            let c = compress_fast(alg, &src).unwrap();
            let d = decompress(alg, &c, src.len()).unwrap();
            assert_eq!(d, src, "alg {alg:?}");
        }
    }
}
