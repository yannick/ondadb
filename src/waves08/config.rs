//! wavesdb's column-family config: a JSON object (wavesdb `persistedConfig`).
//!
//! A wavesdb directory is only ever opened read-only, so what matters is what
//! changes how stored bytes **read**: the comparator (the order every table is
//! sorted in) and the merge operator (how operand chains fold). Those two are
//! translated exactly, and a value of the wrong JSON type is `Corruption`.
//! The write-side settings both engines share under the same meaning are
//! carried over so the family reports its own config; everything else is
//! wavesdb's write tuning (blob dictionaries, b-tree memtables, compaction
//! pickers) and has no effect on a read, so it is ignored rather than
//! refused.
//!
//! A comparator or merge operator this binary does not know is not refused
//! here: the open path's `preflight_catalog_names` refuses it before anything
//! reads a table, with the same message it gives for an ondaDB directory.

use serde_json::{Map, Value};

use super::corrupt;
use crate::config::{ColumnFamilyConfig, Compression};
use crate::error::Result;

/// wavesdb's `Compression` enum as its config stores it. Ids 0-5 are the
/// codecs ondaDB 0.9 numbered the same way; 8 (brotli) has no ondaDB
/// equivalent and leaves the setting at its default — a read-only family
/// never compresses anything.
fn compression(id: u64) -> Option<Compression> {
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

struct Fields<'a>(&'a Map<String, Value>);

impl Fields<'_> {
    fn str(&self, key: &str) -> Result<Option<&str>> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => Ok(Some(s)),
            Some(v) => Err(corrupt(format!("config field {key:?} is not a string: {v}"))),
        }
    }

    fn u64(&self, key: &str) -> Result<Option<u64>> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v.as_u64().map(Some).ok_or_else(|| {
                corrupt(format!("config field {key:?} is not an unsigned integer: {v}"))
            }),
        }
    }

    fn usize(&self, key: &str) -> Result<Option<usize>> {
        self.u64(key)?
            .map(|v| {
                usize::try_from(v)
                    .map_err(|_| corrupt(format!("config field {key:?} overflows usize")))
            })
            .transpose()
    }

    fn f64(&self, key: &str) -> Result<Option<f64>> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(v) => v
                .as_f64()
                .map(Some)
                .ok_or_else(|| corrupt(format!("config field {key:?} is not a number: {v}"))),
        }
    }

    fn bool(&self, key: &str) -> Result<Option<bool>> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::Bool(b)) => Ok(Some(*b)),
            Some(v) => Err(corrupt(format!("config field {key:?} is not a boolean: {v}"))),
        }
    }

    /// A list of small integers. wavesdb declares its per-level codec list
    /// `[]uint8`, which Go's `encoding/json` writes as a base64 string, not
    /// an array; both spellings are accepted, as Go's decoder accepts both.
    fn u64_list(&self, key: &str) -> Result<Option<Vec<u64>>> {
        match self.0.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(s)) => base64_std(s)
                .map(|b| Some(b.into_iter().map(u64::from).collect()))
                .ok_or_else(|| corrupt(format!("config field {key:?} is not base64: {s:?}"))),
            Some(Value::Array(a)) => a
                .iter()
                .map(|v| {
                    v.as_u64().ok_or_else(|| {
                        corrupt(format!("config field {key:?} holds a non-integer: {v}"))
                    })
                })
                .collect::<Result<Vec<_>>>()
                .map(Some),
            Some(v) => Err(corrupt(format!("config field {key:?} is not an array: {v}"))),
        }
    }
}

/// Decode RFC 4648 standard base64 with padding, strictly — Go's
/// `base64.StdEncoding`, which is what `encoding/json` writes for a byte slice.
/// `None` for anything else, non-zero trailing bits included.
fn base64_std(s: &str) -> Option<Vec<u8>> {
    fn sextet(c: u8) -> Option<u32> {
        Some(u32::from(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        }))
    }
    let b = s.as_bytes();
    if !b.len().is_multiple_of(4) {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() / 4 * 3);
    for (i, quad) in b.as_chunks::<4>().0.iter().enumerate() {
        let last = i + 1 == b.len() / 4;
        let pad = quad.iter().rev().take_while(|&&c| c == b'=').count();
        if pad > 2 || (pad > 0 && !last) {
            return None;
        }
        let mut word = 0u32;
        for &c in &quad[..4 - pad] {
            word = (word << 6) | sextet(c)?;
        }
        word <<= 6 * pad as u32;
        let bytes = [(word >> 16) as u8, (word >> 8) as u8, word as u8];
        // Bits past the last whole byte must be zero (Go rejects them too).
        if bytes[3 - pad..].iter().any(|&x| x != 0) {
            return None;
        }
        out.extend_from_slice(&bytes[..3 - pad]);
    }
    Some(out)
}

/// Translate a wavesdb config blob.
pub fn decode(blob: &[u8]) -> Result<ColumnFamilyConfig> {
    let v: Value = serde_json::from_slice(blob)
        .map_err(|e| corrupt(format!("config is not JSON: {e}")))?;
    let Value::Object(obj) = v else {
        return Err(corrupt("config is not a JSON object"));
    };
    let f = Fields(&obj);
    let mut cfg = ColumnFamilyConfig::default();

    // What reads depend on.
    if let Some(name) = f.str("comparator")? {
        // wavesdb writes "" for its default, which is bytewise.
        if !name.is_empty() {
            cfg.comparator_name = name.to_string();
        }
    }
    if let Some(name) = f.str("merge_operator")? {
        if !name.is_empty() {
            cfg.merge_operator_name = Some(name.to_string());
        }
    }

    // Shared write-side settings, same meaning in both engines.
    if let Some(v) = f.usize("write_buffer_size")? {
        cfg.write_buffer_size = v;
    }
    if let Some(v) = f.u64("level_size_ratio")? {
        cfg.level_size_ratio = v;
    }
    if let Some(v) = f.u64("min_levels")? {
        cfg.min_levels =
            u32::try_from(v).map_err(|_| corrupt("config field \"min_levels\" overflows u32"))?;
    }
    if let Some(v) = f.usize("klog_value_threshold")? {
        cfg.klog_value_threshold = v;
    }
    if let Some(v) = f.usize("block_size")? {
        cfg.data_block_size = v;
    }
    if let Some(v) = f.usize("block_restart_interval")? {
        cfg.block_restart_interval = v;
    }
    if let Some(v) = f.bool("enable_prefix_delta_keys")? {
        cfg.enable_prefix_delta_keys = v;
    }
    if let Some(v) = f.bool("enable_bloom_filter")? {
        cfg.enable_bloom_filter = v;
    }
    if let Some(v) = f.f64("bloom_fpr")? {
        cfg.bloom_fpr = v;
    }
    if let Some(c) = f.u64("compression")?.and_then(compression) {
        cfg.compression = c;
    }
    if let Some(ids) = f.u64_list("compression_per_level")? {
        // All or nothing: a list with a codec ondaDB lacks would shift every
        // later level onto the wrong entry if the gap were dropped.
        if let Some(list) = ids.into_iter().map(compression).collect::<Option<Vec<_>>>() {
            cfg.compression_per_level = list;
        }
    }
    Ok(cfg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::OndaError;

    #[test]
    fn read_semantics_translate() {
        let c = decode(br#"{"comparator":"reverse","merge_operator":"append","compression":2}"#)
            .unwrap();
        assert_eq!(c.comparator_name, "reverse");
        assert_eq!(c.merge_operator_name.as_deref(), Some("append"));
        assert_eq!(c.compression, Compression::Lz4);
        let d = decode(br#"{"comparator":"","compression":8}"#).unwrap();
        assert_eq!(d.comparator_name, ColumnFamilyConfig::default().comparator_name);
        assert_eq!(d.compression, ColumnFamilyConfig::default().compression);
        // Go's encoding of []uint8{0, 2, 3}, as the percf-caps fixture holds it.
        let e = decode(br#"{"compression_per_level":"AAID"}"#).unwrap();
        assert_eq!(
            e.compression_per_level,
            vec![Compression::None, Compression::Lz4, Compression::Zstd]
        );
        let a = decode(br#"{"compression_per_level":[0,2,3]}"#).unwrap();
        assert_eq!(a.compression_per_level, e.compression_per_level);
    }

    #[test]
    fn base64_is_strict() {
        assert_eq!(base64_std("").unwrap(), b"");
        assert_eq!(base64_std("AQ==").unwrap(), [1]);
        assert_eq!(base64_std("AQI=").unwrap(), [1, 2]);
        assert_eq!(base64_std("AQID").unwrap(), [1, 2, 3]);
        assert_eq!(base64_std("+/8=").unwrap(), [0xfb, 0xff]);
        for bad in ["A", "AQ=", "AQ", "AR==", "A===", "AQ==AQID", "AQ-_", "AQ\n="] {
            assert!(base64_std(bad).is_none(), "{bad}");
        }
    }

    #[test]
    fn malformed_configs_are_corruption() {
        for bad in [
            &b"not json"[..],
            b"[1,2]",
            br#"{"comparator":5}"#,
            br#"{"write_buffer_size":-1}"#,
            br#"{"compression_per_level":"zst"}"#,
            br#"{"compression_per_level":{"a":1}}"#,
        ] {
            assert!(
                matches!(decode(bad), Err(OndaError::Corruption(_))),
                "{}",
                String::from_utf8_lossy(bad)
            );
        }
    }
}
