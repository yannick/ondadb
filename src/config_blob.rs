//! The epoch-1 column-family config blob: a tagged TLV.
//!
//! ```text
//! blob  := magic "YOLODBCF" | version u32 LE = 1 | entry*
//! entry := tag uvarint | len uvarint | value[len]
//! ```
//!
//! One tag per durable [`ColumnFamilyConfig`] field
//! ([`crate::format::cf_config::tag`], registered in
//! `docs/format-registry.md`). Entries appear in **strictly ascending** tag
//! order; a field at its default is **elided**, so a family that never changes
//! a setting stores no bytes for it. Durations are **nanoseconds**.
//!
//! A tag this binary does not know is **preserved**: it is kept, bytes and all,
//! on the decoded config and written back by the next encode. Plan C step 2
//! needs that, so one engine rewriting a config never strips another engine's
//! options. What is *not* tolerated is a malformed entry for a tag this binary
//! does know: the blob lives inside a CRC-verified manifest or edit record, so
//! bytes that contradict a known tag are `Corruption`, and a known tag naming an
//! enum value this binary lacks (a codec, a sync mode) is `UnsupportedFormat`.
//!
//! This replaces 0.9's positional encoding, whose lenient decoder read
//! wavesdb's JSON config as a 123-byte comparator name; that decoder survives
//! only in `legacy_onda::config`.

use std::time::Duration;

use crate::config::{
    ColumnFamilyConfig, CompactionStyle, Compression, CompressionRule, PartitionRule,
    PartitionScheme, SyncMode, TierRule,
};
use crate::encoding::{
    append_u32, append_u64, append_uvarint, read_u32, read_u64, uvarint, uvarint_len,
};
use crate::error::{OndaError, Result};
use crate::format::cf_config::{tag, HEADER_LEN, MAGIC, VERSION};

fn corrupt(msg: impl std::fmt::Display) -> OndaError {
    OndaError::Corruption(format!("config blob: {msg}"))
}

/// Duration as the stored `u64` nanoseconds. A duration past `u64::MAX`
/// nanoseconds (584 years) saturates; nothing configures one.
fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

fn put(out: &mut Vec<u8>, t: u64, value: &[u8]) {
    append_uvarint(out, t);
    append_uvarint(out, value.len() as u64);
    out.extend_from_slice(value);
}

fn put_uvar(out: &mut Vec<u8>, t: u64, v: u64) {
    let mut b = Vec::with_capacity(10);
    append_uvarint(&mut b, v);
    put(out, t, &b);
}

fn put_bytes_field(b: &mut Vec<u8>, v: &[u8]) {
    append_uvarint(b, v.len() as u64);
    b.extend_from_slice(v);
}

/// Encode `cfg` as an epoch-1 config blob.
pub(crate) fn encode(cfg: &ColumnFamilyConfig) -> Vec<u8> {
    let d = ColumnFamilyConfig::default();
    let mut out = Vec::with_capacity(64);
    out.extend_from_slice(&MAGIC);
    append_u32(&mut out, VERSION);

    // Ascending tag order is the encoding's canonical form, and the decoder
    // enforces it; every arm below is in tag order.
    if cfg.comparator_name != d.comparator_name {
        put(
            &mut out,
            tag::COMPARATOR_NAME,
            cfg.comparator_name.as_bytes(),
        );
    }
    if cfg.compression != d.compression {
        put(&mut out, tag::COMPRESSION, &[cfg.compression.codec_id()]);
    }
    if cfg.write_buffer_size != d.write_buffer_size {
        put_uvar(
            &mut out,
            tag::WRITE_BUFFER_SIZE,
            cfg.write_buffer_size as u64,
        );
    }
    if cfg.level_size_ratio != d.level_size_ratio {
        put_uvar(&mut out, tag::LEVEL_SIZE_RATIO, cfg.level_size_ratio);
    }
    if cfg.klog_value_threshold != d.klog_value_threshold {
        put_uvar(
            &mut out,
            tag::KLOG_VALUE_THRESHOLD,
            cfg.klog_value_threshold as u64,
        );
    }
    if cfg.enable_bloom_filter != d.enable_bloom_filter {
        put(
            &mut out,
            tag::ENABLE_BLOOM_FILTER,
            &[u8::from(cfg.enable_bloom_filter)],
        );
    }
    if cfg.bloom_fpr.to_bits() != d.bloom_fpr.to_bits() {
        put(
            &mut out,
            tag::BLOOM_FPR,
            &cfg.bloom_fpr.to_bits().to_le_bytes(),
        );
    }
    if cfg.l1_file_count_trigger != d.l1_file_count_trigger {
        put_uvar(
            &mut out,
            tag::L1_FILE_COUNT_TRIGGER,
            u64::from(cfg.l1_file_count_trigger),
        );
    }
    if cfg.l0_queue_stall_threshold != d.l0_queue_stall_threshold {
        put_uvar(
            &mut out,
            tag::L0_QUEUE_STALL_THRESHOLD,
            u64::from(cfg.l0_queue_stall_threshold),
        );
    }
    if cfg.use_btree != d.use_btree {
        put(&mut out, tag::USE_BTREE, &[u8::from(cfg.use_btree)]);
    }
    if cfg.sync_mode != d.sync_mode {
        put(&mut out, tag::SYNC_MODE, &[sync_mode_id(cfg.sync_mode)]);
    }
    if cfg.sync_interval != d.sync_interval {
        put_uvar(&mut out, tag::SYNC_INTERVAL, nanos(cfg.sync_interval));
    }
    if !cfg.compression_per_level.is_empty() {
        let ids: Vec<u8> = cfg
            .compression_per_level
            .iter()
            .map(|c| c.codec_id())
            .collect();
        put(&mut out, tag::COMPRESSION_PER_LEVEL, &ids);
    }
    if cfg.compaction_style != d.compaction_style {
        put(
            &mut out,
            tag::COMPACTION_STYLE,
            &[cfg.compaction_style as u8],
        );
    }
    if cfg.fifo_max_bytes != d.fifo_max_bytes {
        put_uvar(&mut out, tag::FIFO_MAX_BYTES, cfg.fifo_max_bytes);
    }
    if cfg.fifo_ttl != d.fifo_ttl {
        put_uvar(&mut out, tag::FIFO_TTL, nanos(cfg.fifo_ttl));
    }
    if !cfg.compression_rules.is_empty() {
        let mut v = Vec::new();
        append_uvarint(&mut v, cfg.compression_rules.len() as u64);
        for r in &cfg.compression_rules {
            put_bytes_field(&mut v, &r.prefix);
            v.push(r.compression.codec_id());
        }
        put(&mut out, tag::COMPRESSION_RULES, &v);
    }
    if !cfg.partition_rules.is_empty() {
        let mut v = Vec::new();
        append_uvarint(&mut v, cfg.partition_rules.len() as u64);
        for r in &cfg.partition_rules {
            put_bytes_field(&mut v, &r.prefix);
            put_bytes_field(&mut v, r.name.as_bytes());
        }
        put(&mut out, tag::PARTITION_RULES, &v);
    }
    if !cfg.tier_rules.is_empty() {
        let mut v = Vec::new();
        append_uvarint(&mut v, cfg.tier_rules.len() as u64);
        for r in &cfg.tier_rules {
            put_bytes_field(&mut v, &r.prefix);
            put_bytes_field(&mut v, r.tier.as_bytes());
            append_uvarint(&mut v, nanos(r.min_age));
        }
        put(&mut out, tag::TIER_RULES, &v);
    }
    // The scheme *name*: from a live derived partitioner, or the one read from
    // a blob that has not been resolved yet — dropping an unresolved name would
    // silently demote the family to rule-based partitioning.
    match &cfg.partition_scheme {
        PartitionScheme::Rules => {}
        PartitionScheme::Derived(f) => {
            put(&mut out, tag::PARTITION_SCHEME, f.scheme_name().as_bytes())
        }
        PartitionScheme::Unresolved(n) => put(&mut out, tag::PARTITION_SCHEME, n.as_bytes()),
    }
    if cfg.target_file_size != d.target_file_size {
        put_uvar(&mut out, tag::TARGET_FILE_SIZE, cfg.target_file_size as u64);
    }
    if cfg.l1_base_bytes != d.l1_base_bytes {
        put_uvar(&mut out, tag::L1_BASE_BYTES, cfg.l1_base_bytes);
    }
    if cfg.soft_pending_compaction_bytes != d.soft_pending_compaction_bytes {
        put_uvar(
            &mut out,
            tag::SOFT_PENDING_COMPACTION_BYTES,
            cfg.soft_pending_compaction_bytes,
        );
    }
    if cfg.hard_pending_compaction_bytes != d.hard_pending_compaction_bytes {
        put_uvar(
            &mut out,
            tag::HARD_PENDING_COMPACTION_BYTES,
            cfg.hard_pending_compaction_bytes,
        );
    }
    if cfg.data_block_size != d.data_block_size {
        put_uvar(&mut out, tag::DATA_BLOCK_SIZE, cfg.data_block_size as u64);
    }
    if cfg.max_cached_vlog_value_bytes != d.max_cached_vlog_value_bytes {
        put_uvar(
            &mut out,
            tag::MAX_CACHED_VLOG_VALUE_BYTES,
            cfg.max_cached_vlog_value_bytes as u64,
        );
    }
    if !cfg.bloom_fpr_per_level.is_empty() {
        let mut v = Vec::with_capacity(cfg.bloom_fpr_per_level.len() * 8);
        for fpr in &cfg.bloom_fpr_per_level {
            append_u64(&mut v, fpr.to_bits());
        }
        put(&mut out, tag::BLOOM_FPR_PER_LEVEL, &v);
    }
    if cfg.optimize_filters_for_hits != d.optimize_filters_for_hits {
        put(
            &mut out,
            tag::OPTIMIZE_FILTERS_FOR_HITS,
            &[u8::from(cfg.optimize_filters_for_hits)],
        );
    }
    if cfg.periodic_compaction_interval != d.periodic_compaction_interval {
        put_uvar(
            &mut out,
            tag::PERIODIC_COMPACTION_INTERVAL,
            nanos(cfg.periodic_compaction_interval),
        );
    }
    if cfg.enable_prefix_delta_keys != d.enable_prefix_delta_keys {
        put(
            &mut out,
            tag::ENABLE_PREFIX_DELTA_KEYS,
            &[u8::from(cfg.enable_prefix_delta_keys)],
        );
    }
    if cfg.block_restart_interval != d.block_restart_interval {
        put_uvar(
            &mut out,
            tag::BLOCK_RESTART_INTERVAL,
            cfg.block_restart_interval as u64,
        );
    }
    if let Some(name) = &cfg.merge_operator_name {
        put(&mut out, tag::MERGE_OPERATOR_NAME, name.as_bytes());
    }
    // Preserved unknown entries last: every tag this binary does not know is
    // above every tag it does (`decode` refuses tag 0, the only other gap).
    for (t, v) in &cfg.unknown_config_tags {
        debug_assert!(*t > tag::MAX_KNOWN);
        put(&mut out, *t, v);
    }
    out
}

fn sync_mode_id(m: SyncMode) -> u8 {
    match m {
        SyncMode::None => 0,
        SyncMode::Full => 1,
        SyncMode::Interval => 2,
    }
}

/// A value slice with the checked readers the tags need.
struct Val<'a> {
    tag: u64,
    b: &'a [u8],
}

impl<'a> Val<'a> {
    fn bad(&self, what: &str) -> OndaError {
        corrupt(format!("tag {}: {what}", self.tag))
    }

    /// The whole value as one canonical uvarint.
    fn uvar(&self) -> Result<u64> {
        let (v, n) = uvarint(self.b).ok_or_else(|| self.bad("malformed uvarint"))?;
        if n != self.b.len() || n != uvarint_len(v) {
            return Err(self.bad("not exactly one minimal uvarint"));
        }
        Ok(v)
    }

    fn usize(&self) -> Result<usize> {
        usize::try_from(self.uvar()?).map_err(|_| self.bad("value exceeds usize"))
    }

    fn u32(&self) -> Result<u32> {
        u32::try_from(self.uvar()?).map_err(|_| self.bad("value exceeds u32"))
    }

    fn byte(&self) -> Result<u8> {
        match self.b {
            [x] => Ok(*x),
            _ => Err(self.bad("expected exactly one byte")),
        }
    }

    fn bool(&self) -> Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(self.bad("boolean is neither 0 nor 1")),
        }
    }

    fn string(&self) -> Result<String> {
        String::from_utf8(self.b.to_vec()).map_err(|_| self.bad("invalid UTF-8"))
    }

    fn duration(&self) -> Result<Duration> {
        Ok(Duration::from_nanos(self.uvar()?))
    }

    fn f64(&self) -> Result<f64> {
        if self.b.len() != 8 {
            return Err(self.bad("expected 8 bytes"));
        }
        Ok(f64::from_bits(read_u64(self.b)))
    }
}

/// A cursor over a list value (`count uvarint | item*`).
struct List<'a> {
    tag: u64,
    b: &'a [u8],
}

impl<'a> List<'a> {
    fn bad(&self) -> OndaError {
        corrupt(format!("tag {}: malformed list", self.tag))
    }

    fn uvar(&mut self) -> Result<u64> {
        let (v, n) = uvarint(self.b).ok_or_else(|| self.bad())?;
        self.b = &self.b[n..];
        Ok(v)
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = usize::try_from(self.uvar()?).map_err(|_| self.bad())?;
        if self.b.len() < len {
            return Err(self.bad());
        }
        let (v, rest) = self.b.split_at(len);
        self.b = rest;
        Ok(v.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).map_err(|_| self.bad())
    }

    fn byte(&mut self) -> Result<u8> {
        let (&x, rest) = self.b.split_first().ok_or_else(|| self.bad())?;
        self.b = rest;
        Ok(x)
    }

    /// The declared item count, bounded by the bytes left so a lying count
    /// cannot reserve memory it has no data for.
    fn count(&mut self) -> Result<usize> {
        let n = usize::try_from(self.uvar()?).map_err(|_| self.bad())?;
        if n > self.b.len() {
            return Err(self.bad());
        }
        Ok(n)
    }

    fn finish(self) -> Result<()> {
        if self.b.is_empty() {
            Ok(())
        } else {
            Err(self.bad())
        }
    }
}

/// Decode an epoch-1 config blob.
///
/// `Corruption` for anything malformed — a wrong magic, a short entry, a tag
/// out of order or repeated, a known tag whose value does not parse; and
/// `UnsupportedFormat` for an unknown version or a known tag naming a value
/// this binary does not implement. Unknown tags are kept on
/// [`ColumnFamilyConfig::unknown_config_tags`].
pub(crate) fn decode(blob: &[u8]) -> Result<ColumnFamilyConfig> {
    if blob.len() < HEADER_LEN {
        return Err(corrupt(format!(
            "{} bytes is shorter than its header",
            blob.len()
        )));
    }
    if blob[..8] != MAGIC {
        return Err(corrupt("magic is not YOLODBCF"));
    }
    let version = read_u32(&blob[8..12]);
    if version != VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "config blob version {version} is not implemented by this binary"
        )));
    }
    let mut cfg = ColumnFamilyConfig::default();
    let mut p = &blob[HEADER_LEN..];
    let mut last: Option<u64> = None;
    while !p.is_empty() {
        let (t, n) = uvarint(p).ok_or_else(|| corrupt("truncated tag"))?;
        p = &p[n..];
        let (len, n) = uvarint(p).ok_or_else(|| corrupt("truncated length"))?;
        p = &p[n..];
        let len = usize::try_from(len).map_err(|_| corrupt("length exceeds usize"))?;
        if p.len() < len {
            return Err(corrupt(format!("tag {t}: value runs past the blob")));
        }
        let (value, rest) = p.split_at(len);
        p = rest;
        if t == 0 {
            return Err(corrupt("tag 0 is never assigned"));
        }
        if last.is_some_and(|l| t <= l) {
            return Err(corrupt(format!("tag {t} is out of order or repeated")));
        }
        last = Some(t);
        apply(&mut cfg, Val { tag: t, b: value })?;
    }
    Ok(cfg)
}

fn apply(cfg: &mut ColumnFamilyConfig, v: Val<'_>) -> Result<()> {
    match v.tag {
        tag::COMPARATOR_NAME => cfg.comparator_name = v.string()?,
        tag::COMPRESSION => cfg.compression = Compression::from_codec_id(v.byte()?)?,
        tag::WRITE_BUFFER_SIZE => cfg.write_buffer_size = v.usize()?,
        tag::LEVEL_SIZE_RATIO => cfg.level_size_ratio = v.uvar()?,
        tag::KLOG_VALUE_THRESHOLD => cfg.klog_value_threshold = v.usize()?,
        tag::ENABLE_BLOOM_FILTER => cfg.enable_bloom_filter = v.bool()?,
        tag::BLOOM_FPR => cfg.bloom_fpr = v.f64()?,
        tag::L1_FILE_COUNT_TRIGGER => cfg.l1_file_count_trigger = v.u32()?,
        tag::L0_QUEUE_STALL_THRESHOLD => cfg.l0_queue_stall_threshold = v.u32()?,
        tag::USE_BTREE => cfg.use_btree = v.bool()?,
        tag::SYNC_MODE => {
            let id = v.byte()?;
            cfg.sync_mode = SyncMode::from_u8(id).ok_or_else(|| {
                OndaError::UnsupportedFormat(format!("config: sync mode {id} is not implemented"))
            })?;
        }
        tag::SYNC_INTERVAL => cfg.sync_interval = v.duration()?,
        tag::COMPRESSION_PER_LEVEL => {
            cfg.compression_per_level =
                v.b.iter()
                    .map(|&id| Compression::from_codec_id(id))
                    .collect::<Result<_>>()?;
        }
        tag::COMPACTION_STYLE => {
            let id = v.byte()?;
            cfg.compaction_style = CompactionStyle::from_u8(id).ok_or_else(|| {
                OndaError::UnsupportedFormat(format!(
                    "config: compaction style {id} is not implemented"
                ))
            })?;
        }
        tag::FIFO_MAX_BYTES => cfg.fifo_max_bytes = v.uvar()?,
        tag::FIFO_TTL => cfg.fifo_ttl = v.duration()?,
        tag::COMPRESSION_RULES => {
            let mut l = List { tag: v.tag, b: v.b };
            let n = l.count()?;
            let mut rules = Vec::with_capacity(n);
            for _ in 0..n {
                let prefix = l.bytes()?;
                let compression = Compression::from_codec_id(l.byte()?)?;
                rules.push(CompressionRule {
                    prefix,
                    compression,
                });
            }
            l.finish()?;
            cfg.compression_rules = rules;
        }
        tag::PARTITION_RULES => {
            let mut l = List { tag: v.tag, b: v.b };
            let n = l.count()?;
            let mut rules = Vec::with_capacity(n);
            for _ in 0..n {
                let prefix = l.bytes()?;
                let name = l.string()?;
                rules.push(PartitionRule { prefix, name });
            }
            l.finish()?;
            cfg.partition_rules = rules;
        }
        tag::TIER_RULES => {
            let mut l = List { tag: v.tag, b: v.b };
            let n = l.count()?;
            let mut rules = Vec::with_capacity(n);
            for _ in 0..n {
                let prefix = l.bytes()?;
                let tier = l.string()?;
                let min_age = Duration::from_nanos(l.uvar()?);
                rules.push(TierRule {
                    prefix,
                    tier,
                    min_age,
                });
            }
            l.finish()?;
            cfg.tier_rules = rules;
        }
        tag::PARTITION_SCHEME => {
            cfg.partition_scheme = PartitionScheme::Unresolved(v.string()?);
        }
        tag::TARGET_FILE_SIZE => cfg.target_file_size = v.usize()?,
        tag::L1_BASE_BYTES => cfg.l1_base_bytes = v.uvar()?,
        tag::SOFT_PENDING_COMPACTION_BYTES => cfg.soft_pending_compaction_bytes = v.uvar()?,
        tag::HARD_PENDING_COMPACTION_BYTES => cfg.hard_pending_compaction_bytes = v.uvar()?,
        tag::DATA_BLOCK_SIZE => {
            cfg.data_block_size = v.usize()?;
            if cfg.data_block_size == 0 {
                return Err(v.bad("data_block_size is zero"));
            }
        }
        tag::MAX_CACHED_VLOG_VALUE_BYTES => cfg.max_cached_vlog_value_bytes = v.usize()?,
        tag::BLOOM_FPR_PER_LEVEL => {
            if !v.b.len().is_multiple_of(8) {
                return Err(v.bad("per-level rates are not whole f64s"));
            }
            let mut rates = Vec::with_capacity(v.b.len() / 8);
            for c in v.b.chunks_exact(8) {
                let fpr = f64::from_bits(read_u64(c));
                // A rate no filter can realize was never written by an encoder
                // fed a config that passed `validate`.
                if !fpr.is_finite() || fpr <= 0.0 || fpr >= 1.0 {
                    return Err(v.bad("per-level rate outside (0, 1)"));
                }
                rates.push(fpr);
            }
            cfg.bloom_fpr_per_level = rates;
        }
        tag::OPTIMIZE_FILTERS_FOR_HITS => cfg.optimize_filters_for_hits = v.bool()?,
        tag::PERIODIC_COMPACTION_INTERVAL => cfg.periodic_compaction_interval = v.duration()?,
        tag::ENABLE_PREFIX_DELTA_KEYS => cfg.enable_prefix_delta_keys = v.bool()?,
        tag::BLOCK_RESTART_INTERVAL => {
            let i = v.usize()?;
            if !(1..=1024).contains(&i) {
                return Err(v.bad("block_restart_interval outside [1, 1024]"));
            }
            cfg.block_restart_interval = i;
        }
        tag::MERGE_OPERATOR_NAME => cfg.merge_operator_name = Some(v.string()?),
        // Unknown (including reserved-but-unimplemented): kept verbatim.
        t => cfg.unknown_config_tags.push((t, v.b.to_vec())),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::cf_config::tag;

    fn header() -> Vec<u8> {
        let mut b = MAGIC.to_vec();
        append_u32(&mut b, VERSION);
        b
    }

    fn entry(b: &mut Vec<u8>, t: u64, v: &[u8]) {
        put(b, t, v);
    }

    /// A config with every durable field away from its default.
    fn everything() -> ColumnFamilyConfig {
        ColumnFamilyConfig {
            comparator_name: "uint64".into(),
            compression: Compression::Lz4,
            write_buffer_size: 3 << 20,
            level_size_ratio: 7,
            klog_value_threshold: 1024,
            enable_bloom_filter: false,
            bloom_fpr: 0.05,
            l1_file_count_trigger: 9,
            l0_queue_stall_threshold: 33,
            use_btree: true,
            sync_mode: SyncMode::Interval,
            sync_interval: Duration::from_micros(250_000),
            compression_per_level: vec![Compression::None, Compression::Zstd],
            compaction_style: CompactionStyle::Fifo,
            fifo_max_bytes: 1 << 30,
            fifo_ttl: Duration::from_secs(3600),
            compression_rules: vec![CompressionRule {
                prefix: b"img/".to_vec(),
                compression: Compression::Snappy,
            }],
            partition_rules: vec![PartitionRule {
                prefix: b"t1/".to_vec(),
                name: "tenant-1".into(),
            }],
            tier_rules: vec![TierRule {
                prefix: b"t1/".to_vec(),
                tier: "cold".into(),
                min_age: Duration::from_secs(86_400),
            }],
            partition_scheme: PartitionScheme::Unresolved("by-tenant".into()),
            target_file_size: 8 << 20,
            l1_base_bytes: 1 << 30,
            soft_pending_compaction_bytes: 1 << 31,
            hard_pending_compaction_bytes: 1 << 33,
            data_block_size: 16 << 10,
            max_cached_vlog_value_bytes: 1 << 20,
            bloom_fpr_per_level: vec![0.01, 0.1],
            optimize_filters_for_hits: true,
            periodic_compaction_interval: Duration::from_secs(7200),
            enable_prefix_delta_keys: true,
            block_restart_interval: 16,
            merge_operator_name: Some("counter.v1".into()),
            ..ColumnFamilyConfig::default()
        }
    }

    fn same(a: &ColumnFamilyConfig, b: &ColumnFamilyConfig) {
        assert_eq!(encode(a), encode(b));
        assert_eq!(a.comparator_name, b.comparator_name);
        assert_eq!(a.compression, b.compression);
        assert_eq!(a.sync_interval, b.sync_interval);
        assert_eq!(a.tier_rules, b.tier_rules);
        assert_eq!(a.partition_rules, b.partition_rules);
        assert_eq!(a.compression_rules, b.compression_rules);
        assert_eq!(a.bloom_fpr_per_level, b.bloom_fpr_per_level);
        assert_eq!(a.merge_operator_name, b.merge_operator_name);
        assert_eq!(a.unknown_config_tags, b.unknown_config_tags);
    }

    #[test]
    fn a_default_config_is_just_the_header() {
        let b = encode(&ColumnFamilyConfig::default());
        assert_eq!(b, header(), "every default is elided");
        let d = decode(&b).unwrap();
        same(&d, &ColumnFamilyConfig::default());
    }

    #[test]
    fn every_field_round_trips() {
        let cfg = everything();
        let b = encode(&cfg);
        let d = decode(&b).unwrap();
        same(&d, &cfg);
        assert_eq!(d.write_buffer_size, 3 << 20);
        assert!(!d.enable_bloom_filter);
        assert_eq!(d.bloom_fpr, 0.05);
        assert!(d.use_btree);
        assert_eq!(d.sync_mode, SyncMode::Interval);
        assert_eq!(d.compaction_style, CompactionStyle::Fifo);
        assert_eq!(d.fifo_ttl, Duration::from_secs(3600));
        assert_eq!(d.target_file_size, 8 << 20);
        assert_eq!(d.hard_pending_compaction_bytes, 1 << 33);
        assert_eq!(d.data_block_size, 16 << 10);
        assert!(d.optimize_filters_for_hits);
        assert_eq!(d.periodic_compaction_interval, Duration::from_secs(7200));
        assert!(d.enable_prefix_delta_keys);
        assert_eq!(d.block_restart_interval, 16);
        assert!(
            matches!(d.partition_scheme, PartitionScheme::Unresolved(ref n) if n == "by-tenant")
        );
    }

    /// Byte-exact encoding of a small config, decoded by hand: the header,
    /// then `tag | len | value` in ascending tag order, durations in ns and
    /// LZ4 as codec 6.
    #[test]
    fn golden_bytes() {
        let cfg = ColumnFamilyConfig {
            compression: Compression::Lz4,
            sync_interval: Duration::from_micros(1),
            merge_operator_name: Some("m".into()),
            ..ColumnFamilyConfig::default()
        };
        let mut want = b"YOLODBCF".to_vec();
        want.extend_from_slice(&[1, 0, 0, 0]);
        want.extend_from_slice(&[2, 1, 6]); // COMPRESSION: LZ4 = 6
        want.extend_from_slice(&[12, 2, 0xE8, 0x07]); // SYNC_INTERVAL: 1000 ns
        want.extend_from_slice(&[32, 1, b'm']); // MERGE_OPERATOR_NAME
        assert_eq!(encode(&cfg), want);
    }

    /// An unknown tag survives decode → encode byte for byte, beside the known
    /// ones — the property plan C step 2 relies on so one engine never strips
    /// another's options.
    #[test]
    fn unknown_tags_are_preserved() {
        let mut b = header();
        entry(&mut b, tag::COMPARATOR_NAME, b"reverse");
        entry(&mut b, tag::RESERVED_BLOOM_AUTO_ALLOCATE, &[1, 2, 3]);
        entry(&mut b, 900, b"wavesdb-option");
        let d = decode(&b).unwrap();
        assert_eq!(d.comparator_name, "reverse");
        assert_eq!(
            d.unknown_config_tags,
            vec![(33, vec![1, 2, 3]), (900, b"wavesdb-option".to_vec())]
        );
        assert_eq!(encode(&d), b, "re-encode must preserve unknown tags");
        // And a changed known field does not disturb them.
        let mut changed = d.clone();
        changed.use_btree = true;
        let again = decode(&encode(&changed)).unwrap();
        assert!(again.use_btree);
        assert_eq!(again.unknown_config_tags, d.unknown_config_tags);
    }

    #[test]
    fn corruption_rows_fail_closed() {
        let good = encode(&everything());
        // Wrong magic.
        let mut b = good.clone();
        b[0] ^= 0x01;
        assert_eq!(decode(&b).unwrap_err().kind(), "corruption");
        // Unknown version.
        let mut b = good.clone();
        b[8] = 2;
        assert_eq!(decode(&b).unwrap_err().kind(), "unsupported_format");
        // Every truncation below the full blob either fails or (at an entry
        // boundary) decodes a shorter config; never panics.
        for n in 0..good.len() {
            let _ = decode(&good[..n]);
        }
        assert_eq!(decode(&good[..5]).unwrap_err().kind(), "corruption");
        // Tag out of order.
        let mut b = header();
        entry(&mut b, tag::USE_BTREE, &[1]);
        entry(&mut b, tag::COMPARATOR_NAME, b"x");
        assert_eq!(decode(&b).unwrap_err().kind(), "corruption");
        // Repeated tag.
        let mut b = header();
        entry(&mut b, tag::USE_BTREE, &[1]);
        entry(&mut b, tag::USE_BTREE, &[0]);
        assert_eq!(decode(&b).unwrap_err().kind(), "corruption");
        // Tag 0.
        let mut b = header();
        entry(&mut b, 0, &[]);
        assert_eq!(decode(&b).unwrap_err().kind(), "corruption");
        // A value running past the blob.
        let mut b = header();
        b.extend_from_slice(&[tag::USE_BTREE as u8, 5, 1]);
        assert_eq!(decode(&b).unwrap_err().kind(), "corruption");
        // Known tags with malformed values.
        for (t, v) in [
            (tag::USE_BTREE, vec![2u8]),                // bool not 0/1
            (tag::USE_BTREE, vec![]),                   // empty
            (tag::WRITE_BUFFER_SIZE, vec![0x80, 0x00]), // non-minimal uvarint
            (tag::WRITE_BUFFER_SIZE, vec![1, 1]),       // trailing byte
            (tag::BLOOM_FPR, vec![0; 7]),               // short f64
            (tag::BLOOM_FPR_PER_LEVEL, vec![0; 9]),     // not whole f64s
            (
                tag::BLOOM_FPR_PER_LEVEL,
                2.0f64.to_bits().to_le_bytes().to_vec(),
            ),
            (tag::BLOCK_RESTART_INTERVAL, vec![0]), // outside [1, 1024]
            (tag::DATA_BLOCK_SIZE, vec![0]),
            (tag::COMPARATOR_NAME, vec![0xFF, 0xFE]), // invalid UTF-8
            (tag::COMPRESSION_RULES, vec![5]),        // count past the bytes
            (tag::PARTITION_RULES, vec![1, 1, b'a', 1]), // truncated item
            (tag::TIER_RULES, vec![0, 0]),            // trailing bytes
        ] {
            let mut b = header();
            entry(&mut b, t, &v);
            assert_eq!(
                decode(&b).unwrap_err().kind(),
                "corruption",
                "tag {t} value {v:?}"
            );
        }
    }

    /// A known tag naming a value this binary does not implement is a newer
    /// format, not a damaged one — including the burned codec ids 2 and 4.
    #[test]
    fn unknown_enum_values_are_unsupported_format() {
        for (t, v) in [
            (tag::COMPRESSION, vec![2u8]),
            (tag::COMPRESSION, vec![4]),
            (tag::COMPRESSION, vec![7]),
            (tag::COMPRESSION_PER_LEVEL, vec![0, 8]),
            (tag::COMPRESSION_RULES, vec![1, 1, b'a', 2]),
            (tag::SYNC_MODE, vec![9]),
            (tag::COMPACTION_STYLE, vec![9]),
        ] {
            let mut b = header();
            entry(&mut b, t, &v);
            assert_eq!(
                decode(&b).unwrap_err().kind(),
                "unsupported_format",
                "tag {t} value {v:?}"
            );
        }
    }

    /// The decoder is total over arbitrary bytes.
    #[test]
    fn fuzz_decode_never_panics() {
        let seeds = [
            encode(&everything()),
            encode(&ColumnFamilyConfig::default()),
        ];
        let mut rng = crate::util::FuzzRng::new(0xC0F1_6B10_B5EE_D001);
        for seed in &seeds {
            for _ in 0..4000 {
                let case = crate::util::fuzz_mutate(&mut rng, seed);
                let _ = decode(&case);
            }
        }
    }

    /// Lists far past 0.9's 255-entry `u8` counts round-trip whole.
    #[test]
    fn long_rule_lists_round_trip() {
        let n = 1000;
        let cfg = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; n],
            partition_rules: (0..n)
                .map(|i| PartitionRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    name: format!("p{i:04}"),
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&encode(&cfg)).unwrap();
        assert_eq!(d.compression_per_level.len(), n);
        assert_eq!(d.partition_rules.len(), n);
        assert_eq!(d.partition_rules[999].name, "p0999");
    }
}
