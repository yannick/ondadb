//! 0.9.x column-family config blob decoder.
//!
//! 0.9 stored a family's config as a **positional** prefix followed by
//! append-tolerant tails and 8-byte tagged tails (`ONDAOVF1`, `ONDAPFN1`,
//! `ONDACMP1`, `ONDABLK1`, `ONDAVVC1`, `ONDABLM1`, `ONDAPRD1`, `ONDAPFX1`,
//! `ONDAMRG1`), all inside the manifest's CRC. Decoding is lenient exactly as
//! 0.9's was: a short or damaged tail leaves the struct defaults in place.
//!
//! Durations were stored in **microseconds**; epoch 1's TLV blob stores
//! nanoseconds. [`super::recover_catalog`] decodes here into a
//! [`ColumnFamilyConfig`] and re-encodes it, so the unit change happens by going
//! through the struct rather than by translating bytes. Compression ids are
//! 0.9's (`2` = LZ4, `4` = LZ4-fast), mapped by [`super::codec`].

use crate::config::{
    ColumnFamilyConfig, CompactionStyle, Compression, CompressionRule, PartitionRule,
    PartitionScheme, SyncMode, TierRule,
};

/// Decode a 0.9 config blob. Never fails: 0.9's decoder fell back to defaults
/// on anything short or unrecognized, and a 0.9 database was opened that way.
pub fn decode(blob: &[u8]) -> ColumnFamilyConfig {
    let mut cfg = ColumnFamilyConfig::default();
    decode_into(blob, &mut cfg);
    cfg
}

/// Test-only 0.9 encoder, kept to build decoder inputs; nothing in production
/// writes a 0.9 config blob.
#[cfg(test)]
pub(crate) fn encode(cfg: &ColumnFamilyConfig) -> Vec<u8> {
    let mut b = Vec::new();
    encode_base_config(&mut b, cfg);
    let counts = encode_legacy_policies(&mut b, cfg);
    encode_overflow_policies(&mut b, cfg, counts);
    encode_partition_scheme(&mut b, cfg);
    encode_compaction_geometry(&mut b, cfg);
    encode_block_size(&mut b, cfg);
    encode_vlog_cache(&mut b, cfg);
    encode_bloom_policy(&mut b, cfg);
    encode_periodic_interval(&mut b, cfg);
    encode_prefix_delta(&mut b, cfg);
    encode_merge_operator(&mut b, cfg);
    b
}

/// `cfg.enc09()` in the tests below: the 0.9 blob, spelled like the method
/// the tests were written against.
#[cfg(test)]
trait Encode09 {
    fn enc09(&self) -> Vec<u8>;
}

#[cfg(test)]
impl Encode09 for ColumnFamilyConfig {
    fn enc09(&self) -> Vec<u8> {
        encode(self)
    }
}

#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
fn derived_scheme_name(cfg: &ColumnFamilyConfig) -> Option<&str> {
    match &cfg.partition_scheme {
        PartitionScheme::Derived(f) => Some(f.scheme_name()),
        PartitionScheme::Unresolved(n) => Some(n.as_str()),
        PartitionScheme::Rules => None,
    }
}

const CONFIG_OVERFLOW_MAGIC: &[u8; 8] = b"ONDAOVF1";
/// Tag introducing the derived-partitioner tail (scheme name only).
const CONFIG_PARTITION_FN_MAGIC: &[u8; 8] = b"ONDAPFN1";
/// Tag introducing the 0.8.0 compaction-geometry tail.
const CONFIG_COMPACTION_MAGIC: &[u8; 8] = b"ONDACMP1";
/// Tag introducing the 0.8.1 per-family data-block-size tail.
const CONFIG_BLOCK_SIZE_MAGIC: &[u8; 8] = b"ONDABLK1";
/// Tag introducing the vlog-value-cache tail (feature 0.5).
const CONFIG_VLOG_CACHE_MAGIC: &[u8; 8] = b"ONDAVVC1";
/// Tag introducing the per-level bloom-policy tail (0.1).
const CONFIG_BLOOM_POLICY_MAGIC: &[u8; 8] = b"ONDABLM1";
/// Tag introducing the periodic-compaction interval tail (0.3).
const CONFIG_PERIODIC_MAGIC: &[u8; 8] = b"ONDAPRD1";
/// Tag introducing the prefix-delta key-encoding tail (2.1).
const CONFIG_PREFIX_DELTA_MAGIC: &[u8; 8] = b"ONDAPFX1";
/// Tag introducing the merge-operator-name tail (1.1).
const CONFIG_MERGE_OP_MAGIC: &[u8; 8] = b"ONDAMRG1";
/// Reserved for a future geometric (Monkey-style) auto-allocation policy. It is
/// mutually exclusive with the explicit `bloom_fpr_per_level` vector, so the tag
/// is claimed here to keep the two from ever sharing one; nothing writes or
/// reads it yet.
#[allow(dead_code)]
const CONFIG_BLOOM_AUTO_MAGIC: &[u8; 8] = b"ONDABLM2";

#[cfg(test)]
#[derive(Clone, Copy)]
struct LegacyPolicyCounts {
    levels: usize,
    compression: usize,
    partitions: usize,
    tiers: usize,
}

#[cfg(test)]
fn encode_base_config(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::{append_u32, append_u64, append_uvarint};

    append_uvarint(b, cfg.comparator_name.len() as u64);
    b.extend_from_slice(cfg.comparator_name.as_bytes());
    b.push(super::codec_id(cfg.compression));
    append_u64(b, cfg.write_buffer_size as u64);
    append_u64(b, cfg.level_size_ratio);
    append_u64(b, cfg.klog_value_threshold as u64);
    b.push(u8::from(cfg.enable_bloom_filter));
    append_u64(b, cfg.bloom_fpr.to_bits());
    append_u32(b, cfg.l1_file_count_trigger);
    append_u32(b, cfg.l0_queue_stall_threshold);
    b.push(u8::from(cfg.use_btree));

    // This is the first append-tolerant tail. A legacy blob ending above keeps
    // the defaults because decoding stops before assigning these fields.
    b.push(cfg.sync_mode as u8);
    append_u64(b, cfg.sync_interval.as_micros() as u64);
}

#[cfg(test)]
fn encode_legacy_policies(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) -> LegacyPolicyCounts {
    use crate::encoding::{append_u64, append_uvarint};

    let counts = LegacyPolicyCounts {
        levels: cfg.compression_per_level.len().min(u8::MAX as usize),
        compression: cfg.compression_rules.len().min(u8::MAX as usize),
        partitions: cfg.partition_rules.len().min(u8::MAX as usize),
        tiers: cfg.tier_rules.len().min(u8::MAX as usize),
    };
    b.push(counts.levels as u8);
    b.extend(
        cfg.compression_per_level
            .iter()
            .take(counts.levels)
            .map(|c| super::codec_id(*c)),
    );
    b.push(cfg.compaction_style as u8);
    append_u64(b, cfg.fifo_max_bytes);
    append_u64(b, cfg.fifo_ttl.as_micros() as u64);

    b.push(counts.compression as u8);
    for rule in cfg.compression_rules.iter().take(counts.compression) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        b.push(super::codec_id(rule.compression));
    }
    b.push(counts.partitions as u8);
    for rule in cfg.partition_rules.iter().take(counts.partitions) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.name.len() as u64);
        b.extend_from_slice(rule.name.as_bytes());
    }
    b.push(counts.tiers as u8);
    for rule in cfg.tier_rules.iter().take(counts.tiers) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.tier.len() as u64);
        b.extend_from_slice(rule.tier.as_bytes());
        append_u64(b, rule.min_age.as_micros() as u64);
    }
    counts
}

#[cfg(test)]
fn has_overflow_policies(cfg: &ColumnFamilyConfig) -> bool {
    cfg.compression_per_level.len() > u8::MAX as usize
        || cfg.compression_rules.len() > u8::MAX as usize
        || cfg.partition_rules.len() > u8::MAX as usize
        || cfg.tier_rules.len() > u8::MAX as usize
}

#[cfg(test)]
fn encode_overflow_policies(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig, counts: LegacyPolicyCounts) {
    use crate::encoding::{append_u64, append_uvarint};

    if !has_overflow_policies(cfg) {
        return;
    }
    b.extend_from_slice(CONFIG_OVERFLOW_MAGIC);
    append_uvarint(
        b,
        cfg.compression_per_level
            .len()
            .saturating_sub(counts.levels) as u64,
    );
    b.extend(
        cfg.compression_per_level
            .iter()
            .skip(counts.levels)
            .map(|c| super::codec_id(*c)),
    );

    append_uvarint(
        b,
        cfg.compression_rules
            .len()
            .saturating_sub(counts.compression) as u64,
    );
    for rule in cfg.compression_rules.iter().skip(counts.compression) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        b.push(super::codec_id(rule.compression));
    }
    append_uvarint(
        b,
        cfg.partition_rules.len().saturating_sub(counts.partitions) as u64,
    );
    for rule in cfg.partition_rules.iter().skip(counts.partitions) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.name.len() as u64);
        b.extend_from_slice(rule.name.as_bytes());
    }
    append_uvarint(b, cfg.tier_rules.len().saturating_sub(counts.tiers) as u64);
    for rule in cfg.tier_rules.iter().skip(counts.tiers) {
        append_uvarint(b, rule.prefix.len() as u64);
        b.extend_from_slice(&rule.prefix);
        append_uvarint(b, rule.tier.len() as u64);
        b.extend_from_slice(rule.tier.as_bytes());
        append_u64(b, rule.min_age.as_micros() as u64);
    }
}

#[cfg(test)]
fn encode_partition_scheme(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_uvarint;

    let Some(name) = derived_scheme_name(cfg) else {
        return;
    };
    b.extend_from_slice(CONFIG_PARTITION_FN_MAGIC);
    append_uvarint(b, name.len() as u64);
    b.extend_from_slice(name.as_bytes());
}

#[cfg(test)]
fn encode_compaction_geometry(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    let defaults = ColumnFamilyConfig::default();
    if cfg.target_file_size == defaults.target_file_size
        && cfg.l1_base_bytes == defaults.l1_base_bytes
        && cfg.soft_pending_compaction_bytes == defaults.soft_pending_compaction_bytes
        && cfg.hard_pending_compaction_bytes == defaults.hard_pending_compaction_bytes
    {
        return;
    }
    b.extend_from_slice(CONFIG_COMPACTION_MAGIC);
    append_u64(b, cfg.target_file_size as u64);
    append_u64(b, cfg.l1_base_bytes);
    append_u64(b, cfg.soft_pending_compaction_bytes);
    append_u64(b, cfg.hard_pending_compaction_bytes);
}

#[cfg(test)]
fn encode_block_size(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    if cfg.data_block_size == ColumnFamilyConfig::default().data_block_size {
        return;
    }
    b.extend_from_slice(CONFIG_BLOCK_SIZE_MAGIC);
    append_u64(b, cfg.data_block_size as u64);
}

#[cfg(test)]
fn encode_vlog_cache(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    // Eliding the default keeps an untouched family's blob byte-identical to
    // what a pre-0.5 binary wrote.
    if cfg.max_cached_vlog_value_bytes == ColumnFamilyConfig::default().max_cached_vlog_value_bytes
    {
        return;
    }
    b.extend_from_slice(CONFIG_VLOG_CACHE_MAGIC);
    append_u64(b, cfg.max_cached_vlog_value_bytes as u64);
}

#[cfg(test)]
/// The per-level bloom-policy tail: `count` levels of IEEE-754 bits, then the
/// `optimize_filters_for_hits` byte. Elided at the defaults so a family that
/// never touches the policy encodes byte-for-byte as earlier releases wrote it.
fn encode_bloom_policy(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    if cfg.bloom_fpr_per_level.is_empty() && !cfg.optimize_filters_for_hits {
        return;
    }
    b.extend_from_slice(CONFIG_BLOOM_POLICY_MAGIC);
    // A fixed-width count, matching the other tails: the vector holds one entry
    // per level and is never large, but a varint here would buy nothing and
    // make the truncation check below less obvious.
    append_u64(b, cfg.bloom_fpr_per_level.len() as u64);
    for fpr in &cfg.bloom_fpr_per_level {
        append_u64(b, fpr.to_bits());
    }
    b.push(u8::from(cfg.optimize_filters_for_hits));
}

#[derive(Clone, Copy)]
struct ConfigCursor<'a> {
    remaining: &'a [u8],
}

impl<'a> ConfigCursor<'a> {
    fn new(remaining: &'a [u8]) -> Self {
        Self { remaining }
    }

    fn byte(&mut self) -> Option<u8> {
        Some(self.bytes(1)?[0])
    }

    fn u32(&mut self) -> Option<u32> {
        Some(crate::encoding::read_u32(self.bytes(4)?))
    }

    fn u64(&mut self) -> Option<u64> {
        Some(crate::encoding::read_u64(self.bytes(8)?))
    }

    fn uvar(&mut self) -> Option<u64> {
        let (value, used) = crate::encoding::uvarint(self.remaining)?;
        self.remaining = &self.remaining[used..];
        Some(value)
    }

    fn bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        if self.remaining.len() < len {
            return None;
        }
        let (value, remaining) = self.remaining.split_at(len);
        self.remaining = remaining;
        Some(value)
    }

    fn consume_prefix(&mut self, prefix: &[u8]) -> bool {
        let Some(remaining) = self.remaining.strip_prefix(prefix) else {
            return false;
        };
        self.remaining = remaining;
        true
    }

    fn remaining_len(&self) -> usize {
        self.remaining.len()
    }

    fn into_remaining(self) -> &'a [u8] {
        self.remaining
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }
}

fn decode_into(p: &[u8], cfg: &mut ColumnFamilyConfig) -> Option<()> {
    let mut cursor = ConfigCursor::new(p);
    decode_base_config(&mut cursor, cfg)?;
    decode_legacy_policies(&mut cursor, cfg)?;
    if cursor.consume_prefix(CONFIG_OVERFLOW_MAGIC) {
        decode_overflow_policies(&mut cursor, cfg)?;
    }
    let p = read_partition_fn_tail(cursor.into_remaining(), cfg);
    let p = read_compaction_tail(p, cfg);
    let p = read_block_size_tail(p, cfg);
    let p = read_vlog_cache_tail(p, cfg);
    let p = read_bloom_policy_tail(p, cfg);
    let p = read_periodic_interval_tail(p, cfg);
    let p = read_prefix_delta_tail(p, cfg);
    read_merge_operator_tail(p, cfg);
    Some(())
}

fn decode_base_config(cursor: &mut ConfigCursor<'_>, cfg: &mut ColumnFamilyConfig) -> Option<()> {
    let name_len = cursor.uvar()? as usize;
    cfg.comparator_name = String::from_utf8_lossy(cursor.bytes(name_len)?).into_owned();
    if let Some(compression) = super::codec(cursor.byte()?) {
        cfg.compression = compression;
    }
    cfg.write_buffer_size = cursor.u64()? as usize;
    cfg.level_size_ratio = cursor.u64()?;
    cfg.klog_value_threshold = cursor.u64()? as usize;
    cfg.enable_bloom_filter = cursor.byte()? != 0;
    cfg.bloom_fpr = f64::from_bits(cursor.u64()?);
    cfg.l1_file_count_trigger = cursor.u32()?;
    cfg.l0_queue_stall_threshold = cursor.u32()?;
    cfg.use_btree = cursor.byte()? != 0;

    // All remaining fields were appended after the original durable subset.
    // A short legacy blob returns here and leaves their defaults in place.
    if let Some(sync_mode) = SyncMode::from_u8(cursor.byte()?) {
        cfg.sync_mode = sync_mode;
    }
    cfg.sync_interval = std::time::Duration::from_micros(cursor.u64()?);
    Some(())
}

fn decode_legacy_policies(
    cursor: &mut ConfigCursor<'_>,
    cfg: &mut ColumnFamilyConfig,
) -> Option<()> {
    let level_count = cursor.byte()? as usize;
    cfg.compression_per_level = decode_compression_levels(cursor, level_count)?;
    if let Some(style) = CompactionStyle::from_u8(cursor.byte()?) {
        cfg.compaction_style = style;
    }
    cfg.fifo_max_bytes = cursor.u64()?;
    cfg.fifo_ttl = std::time::Duration::from_micros(cursor.u64()?);

    let compression_count = cursor.byte()? as usize;
    cfg.compression_rules = decode_compression_rules(cursor, compression_count)?;
    let partition_count = cursor.byte()? as usize;
    cfg.partition_rules = decode_partition_rules(cursor, partition_count)?;
    let tier_count = cursor.byte()? as usize;
    cfg.tier_rules = decode_tier_rules(cursor, tier_count)?;
    Some(())
}

fn decode_compression_levels(
    cursor: &mut ConfigCursor<'_>,
    count: usize,
) -> Option<Vec<Compression>> {
    let mut levels = Vec::with_capacity(count);
    for _ in 0..count {
        levels.push(super::codec(cursor.byte()?)?);
    }
    Some(levels)
}

fn decode_compression_rules(
    cursor: &mut ConfigCursor<'_>,
    count: usize,
) -> Option<Vec<CompressionRule>> {
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        rules.push(decode_compression_rule(cursor)?);
    }
    Some(rules)
}

fn decode_compression_rule(cursor: &mut ConfigCursor<'_>) -> Option<CompressionRule> {
    let prefix_len = cursor.uvar()? as usize;
    let prefix = cursor.bytes(prefix_len)?.to_vec();
    let compression = super::codec(cursor.byte()?)?;
    Some(CompressionRule {
        prefix,
        compression,
    })
}

fn decode_partition_rules(
    cursor: &mut ConfigCursor<'_>,
    count: usize,
) -> Option<Vec<PartitionRule>> {
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        rules.push(decode_partition_rule(cursor)?);
    }
    Some(rules)
}

fn decode_partition_rule(cursor: &mut ConfigCursor<'_>) -> Option<PartitionRule> {
    let prefix_len = cursor.uvar()? as usize;
    let prefix = cursor.bytes(prefix_len)?.to_vec();
    let name_len = cursor.uvar()? as usize;
    let name = String::from_utf8_lossy(cursor.bytes(name_len)?).into_owned();
    Some(PartitionRule { prefix, name })
}

fn decode_tier_rules(cursor: &mut ConfigCursor<'_>, count: usize) -> Option<Vec<TierRule>> {
    let mut rules = Vec::with_capacity(count);
    for _ in 0..count {
        rules.push(decode_tier_rule(cursor)?);
    }
    Some(rules)
}

fn decode_tier_rule(cursor: &mut ConfigCursor<'_>) -> Option<TierRule> {
    let prefix_len = cursor.uvar()? as usize;
    let prefix = cursor.bytes(prefix_len)?.to_vec();
    let tier_len = cursor.uvar()? as usize;
    let tier = String::from_utf8_lossy(cursor.bytes(tier_len)?).into_owned();
    let min_age = std::time::Duration::from_micros(cursor.u64()?);
    Some(TierRule {
        prefix,
        tier,
        min_age,
    })
}

fn decode_overflow_policies(
    cursor: &mut ConfigCursor<'_>,
    cfg: &mut ColumnFamilyConfig,
) -> Option<()> {
    let extra_levels = cursor.uvar()? as usize;
    cfg.compression_per_level
        .reserve(extra_levels.min(cursor.remaining_len()));
    for _ in 0..extra_levels {
        cfg.compression_per_level
            .push(super::codec(cursor.byte()?)?);
    }

    let extra_compression = cursor.uvar()? as usize;
    cfg.compression_rules
        .reserve(extra_compression.min(cursor.remaining_len()));
    for _ in 0..extra_compression {
        cfg.compression_rules.push(decode_compression_rule(cursor)?);
    }

    let extra_partitions = cursor.uvar()? as usize;
    cfg.partition_rules
        .reserve(extra_partitions.min(cursor.remaining_len()));
    for _ in 0..extra_partitions {
        cfg.partition_rules.push(decode_partition_rule(cursor)?);
    }

    let extra_tiers = cursor.uvar()? as usize;
    cfg.tier_rules
        .reserve(extra_tiers.min(cursor.remaining_len()));
    for _ in 0..extra_tiers {
        cfg.tier_rules.push(decode_tier_rule(cursor)?);
    }
    Some(())
}

/// Read the optional derived-partitioner tail, recording the scheme name for
/// `DB::open` to resolve.
///
/// Absent tail ⇒ rule-based partitioning, which is what every config written
/// before derived schemes existed decodes to. A malformed tail is ignored
/// rather than fatal, matching how the rest of this decoder treats a truncated
/// blob; the consequence is a column family that opens as rule-partitioned,
/// and `DB::open` cannot then mis-resolve it because there is no name to
/// resolve.
/// Consume the derived-partitioner tail if present, returning what follows it
/// so later tails can be read in turn.
fn read_partition_fn_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::uvarint;
    let Some(rest) = p.strip_prefix(CONFIG_PARTITION_FN_MAGIC) else {
        return p;
    };
    let Some((len, n)) = uvarint(rest) else {
        return p;
    };
    let rest = &rest[n..];
    let len = len as usize;
    if rest.len() < len {
        return p;
    }
    cfg.partition_scheme =
        PartitionScheme::Unresolved(String::from_utf8_lossy(&rest[..len]).into_owned());
    &rest[len..]
}

/// Consume the 0.8.0 compaction-geometry tail if present. Absent (every
/// manifest written before 0.8.0, and any config left at the defaults), the
/// struct defaults stand.
fn read_compaction_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::read_u64;
    let Some(mut rest) = p.strip_prefix(CONFIG_COMPACTION_MAGIC) else {
        return p;
    };
    let mut next = || -> Option<u64> {
        if rest.len() < 8 {
            return None;
        }
        let v = read_u64(rest);
        rest = &rest[8..];
        Some(v)
    };
    // All four or none: a truncated tail leaves every field at its default
    // rather than applying a half-read geometry.
    let (Some(tfs), Some(l1), Some(soft), Some(hard)) = (next(), next(), next(), next()) else {
        return p;
    };
    cfg.target_file_size = tfs as usize;
    cfg.l1_base_bytes = l1;
    cfg.soft_pending_compaction_bytes = soft;
    cfg.hard_pending_compaction_bytes = hard;
    rest
}

fn read_block_size_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_BLOCK_SIZE_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    let value = crate::encoding::read_u64(rest) as usize;
    if value != 0 {
        cfg.data_block_size = value;
    }
    &rest[8..]
}

fn read_vlog_cache_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_VLOG_CACHE_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    // Unlike the block size, 0 is a meaningful value here (disabled) — but the
    // encoder elides it, so a stored 0 can only come from a truncated or
    // hand-edited blob. Take it at face value: it is also the default.
    cfg.max_cached_vlog_value_bytes = crate::encoding::read_u64(rest) as usize;
    &rest[8..]
}

/// Consume the per-level bloom-policy tail if present. Absent (every manifest
/// written before 0.1, and any family left at the defaults), the struct
/// defaults stand — an empty vector and `optimize_filters_for_hits == false`,
/// which is exactly the uniform behaviour of earlier releases.
///
/// All-or-nothing, like the compaction tail: a truncated tail leaves both
/// fields at their defaults rather than applying a half-read policy that would
/// silently filter some levels and not others.
///
/// Returns the unconsumed remainder so later tails can be chained behind it. A
/// rejected (absent, short or invalid) tail returns `p` untouched — the next
/// reader then fails its own `strip_prefix` and also falls back to defaults,
/// which is the intended all-or-nothing behaviour for a damaged blob.
fn read_bloom_policy_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::read_u64;

    let Some(rest) = p.strip_prefix(CONFIG_BLOOM_POLICY_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    let count = read_u64(rest) as usize;
    let rest = &rest[8..];
    // `count` comes off disk, so the size it implies is computed with checked
    // arithmetic — a lying count must fail the bounds check, not wrap past it —
    // and the bytes must actually be present before anything is reserved.
    let Some(needed) = count.checked_mul(8).and_then(|n| n.checked_add(1)) else {
        return p;
    };
    if rest.len() < needed {
        return p;
    }
    let mut per_level = Vec::with_capacity(count);
    for i in 0..count {
        let fpr = f64::from_bits(read_u64(&rest[i * 8..]));
        // A blob whose rates would not `validate` is not made valid by having
        // been written: fall back to uniform rather than hand a NaN to the
        // filter sizer.
        if !fpr.is_finite() || fpr <= 0.0 || fpr >= 1.0 {
            return p;
        }
        per_level.push(fpr);
    }
    cfg.bloom_fpr_per_level = per_level;
    cfg.optimize_filters_for_hits = rest[count * 8] != 0;
    &rest[needed..]
}

#[cfg(test)]
/// The 0.3 periodic-compaction interval tail: one `u64` of microseconds.
/// Elided at the default (zero, disabled) so a family that never sets it
/// encodes byte-for-byte as earlier releases wrote it.
fn encode_periodic_interval(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    if cfg.periodic_compaction_interval.is_zero() {
        return;
    }
    b.extend_from_slice(CONFIG_PERIODIC_MAGIC);
    append_u64(b, cfg.periodic_compaction_interval.as_micros() as u64);
}

/// Consume the periodic-compaction tail if present. Absent (every blob written
/// before 0.3, and any family that left the option at zero), the default stands
/// — `Duration::ZERO`, which disables the trigger.
/// Returns the unconsumed remainder so later tails can be chained behind it.
fn read_periodic_interval_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_PERIODIC_MAGIC) else {
        return p;
    };
    if rest.len() < 8 {
        return p;
    }
    cfg.periodic_compaction_interval =
        std::time::Duration::from_micros(crate::encoding::read_u64(rest));
    &rest[8..]
}

#[cfg(test)]
/// The 2.1 prefix-delta tail: `enabled u8 | block_restart_interval u64 LE`.
/// Elided when both fields are at their defaults, so a family that never sets
/// them encodes byte-for-byte as earlier releases wrote it.
fn encode_prefix_delta(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_u64;

    let default = ColumnFamilyConfig::default();
    if !cfg.enable_prefix_delta_keys && cfg.block_restart_interval == default.block_restart_interval
    {
        return;
    }
    b.extend_from_slice(CONFIG_PREFIX_DELTA_MAGIC);
    b.push(u8::from(cfg.enable_prefix_delta_keys));
    append_u64(b, cfg.block_restart_interval as u64);
}

/// Consume the prefix-delta tail if present. All-or-nothing, like the tails
/// before it: a truncated or out-of-range tail leaves both fields at their
/// defaults rather than applying half a policy — and an interval outside
/// `[1, 1024]` would not survive `validate`, so it is not made valid by having
/// been written.
fn read_prefix_delta_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    let Some(rest) = p.strip_prefix(CONFIG_PREFIX_DELTA_MAGIC) else {
        return p;
    };
    if rest.len() < 9 {
        return p;
    }
    let interval = crate::encoding::read_u64(&rest[1..]) as usize;
    if !(1..=1024).contains(&interval) {
        return p;
    }
    cfg.enable_prefix_delta_keys = rest[0] != 0;
    cfg.block_restart_interval = interval;
    &rest[9..]
}

#[cfg(test)]
/// The 1.1 merge-operator tail: `name_len uvarint | name`. Elided entirely for
/// a family with no operator, so a family that never sets one encodes
/// byte-for-byte as earlier releases wrote it.
fn encode_merge_operator(b: &mut Vec<u8>, cfg: &ColumnFamilyConfig) {
    use crate::encoding::append_uvarint;

    let Some(name) = cfg.merge_operator_name.as_deref() else {
        return;
    };
    b.extend_from_slice(CONFIG_MERGE_OP_MAGIC);
    append_uvarint(b, name.len() as u64);
    b.extend_from_slice(name.as_bytes());
}

/// Consume the merge-operator tail if present. All-or-nothing, like the tails
/// before it: a truncated tail leaves `merge_operator_name` at `None`, which is
/// how a pre-1.1 blob decodes and is the only safe default — the resolver then
/// simply has nothing to look up.
fn read_merge_operator_tail<'a>(p: &'a [u8], cfg: &mut ColumnFamilyConfig) -> &'a [u8] {
    use crate::encoding::uvarint;
    let Some(rest) = p.strip_prefix(CONFIG_MERGE_OP_MAGIC) else {
        return p;
    };
    let Some((len, n)) = uvarint(rest) else {
        return p;
    };
    let rest = &rest[n..];
    let len = len as usize;
    if rest.len() < len {
        return p;
    }
    cfg.merge_operator_name = Some(String::from_utf8_lossy(&rest[..len]).into_owned());
    &rest[len..]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_config_emits_no_prefix_delta_tail() {
        let blob = ColumnFamilyConfig::default().enc09();
        assert!(
            !blob
                .windows(CONFIG_PREFIX_DELTA_MAGIC.len())
                .any(|w| w == CONFIG_PREFIX_DELTA_MAGIC),
            "a family at the defaults must encode as earlier releases wrote it"
        );
        let decoded = decode(&blob);
        assert!(!decoded.enable_prefix_delta_keys);
        assert_eq!(decoded.block_restart_interval, crate::sst::RESTART_INTERVAL);
    }

    #[test]
    fn prefix_delta_settings_round_trip() {
        for (enabled, interval) in [(true, 8usize), (false, 32), (true, 1), (true, 1024)] {
            let config = ColumnFamilyConfig {
                enable_prefix_delta_keys: enabled,
                block_restart_interval: interval,
                ..Default::default()
            };
            config.validate().unwrap();
            let decoded = decode(&config.enc09());
            assert_eq!(decoded.enable_prefix_delta_keys, enabled);
            assert_eq!(decoded.block_restart_interval, interval);
        }
    }

    /// The blob tails are positional, so the new one must survive behind every
    /// tail that already existed — including the two it directly follows.
    #[test]
    fn the_prefix_delta_tail_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            data_block_size: 16 << 10,
            max_cached_vlog_value_bytes: 1 << 20,
            bloom_fpr_per_level: vec![0.02, 0.05],
            optimize_filters_for_hits: true,
            periodic_compaction_interval: std::time::Duration::from_secs(3600),
            enable_prefix_delta_keys: true,
            block_restart_interval: 16,
            ..Default::default()
        };
        config.validate().unwrap();
        let blob = config.enc09();
        let block_at = blob
            .windows(8)
            .position(|w| w == CONFIG_BLOCK_SIZE_MAGIC)
            .expect("block-size tail");
        let delta_at = blob
            .windows(8)
            .position(|w| w == CONFIG_PREFIX_DELTA_MAGIC)
            .expect("prefix-delta tail");
        assert!(block_at < delta_at, "the new tail must come last");
        let decoded = decode(&blob);
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.max_cached_vlog_value_bytes, 1 << 20);
        assert_eq!(decoded.bloom_fpr_per_level, vec![0.02, 0.05]);
        assert!(decoded.optimize_filters_for_hits);
        assert_eq!(
            decoded.periodic_compaction_interval,
            std::time::Duration::from_secs(3600)
        );
        assert!(decoded.enable_prefix_delta_keys);
        assert_eq!(decoded.block_restart_interval, 16);
    }

    #[test]
    fn config_cursor_reads_checked_little_endian_values() {
        let bytes = [
            0x7f, 0x78, 0x56, 0x34, 0x12, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0xac,
            0x02, b'o', b'k',
        ];
        let mut cursor = ConfigCursor::new(&bytes);

        assert_eq!(cursor.byte(), Some(0x7f));
        assert_eq!(cursor.u32(), Some(0x1234_5678));
        assert_eq!(cursor.u64(), Some(0x0102_0304_0506_0708));
        assert_eq!(cursor.uvar(), Some(300));
        assert_eq!(cursor.bytes(2), Some(&b"ok"[..]));
        assert!(cursor.is_empty());
    }

    #[test]
    fn config_cursor_does_not_advance_after_a_short_fixed_width_read() {
        let mut cursor = ConfigCursor::new(&[1, 2, 3]);

        assert_eq!(cursor.u64(), None);
        assert_eq!(cursor.bytes(3), Some(&[1, 2, 3][..]));
        assert!(cursor.is_empty());
    }

    #[test]
    fn cf_config_encode_decode() {
        let c = ColumnFamilyConfig {
            comparator_name: "uint64".into(),
            compression: Compression::Zstd,
            write_buffer_size: 123456,
            enable_bloom_filter: false,
            compression_rules: vec![
                CompressionRule {
                    prefix: b"img/".to_vec(),
                    compression: Compression::Zstd,
                },
                CompressionRule {
                    prefix: b"hot/".to_vec(),
                    compression: Compression::None,
                },
            ],
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&c.enc09());
        assert_eq!(d.comparator_name, "uint64");
        assert!(d.compression_per_level.is_empty());
        assert_eq!(d.compression, Compression::Zstd);
        assert_eq!(d.write_buffer_size, 123456);
        assert!(!d.enable_bloom_filter);
        assert_eq!(d.compression_rules, c.compression_rules);
    }

    #[test]
    fn partition_rules_survive_manifest_round_trip() {
        let c = ColumnFamilyConfig {
            partition_rules: vec![
                PartitionRule {
                    prefix: b"a/".to_vec(),
                    name: "alpha".into(),
                },
                PartitionRule {
                    prefix: b"b/".to_vec(),
                    name: "beta".into(),
                },
            ],
            // Coexists with compression_rules (both are appended tails).
            compression_rules: vec![CompressionRule {
                prefix: b"a/".to_vec(),
                compression: Compression::Zstd,
            }],
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&c.enc09());
        assert_eq!(d.partition_rules, c.partition_rules);
        assert_eq!(d.compression_rules, c.compression_rules);
    }

    #[test]
    fn legacy_config_without_partition_tail_decodes_to_empty() {
        // A config encoded before partition_rules / tier_rules existed ends right
        // after the compression_rules section. The encoding now appends a 1-byte
        // partition-count then a 1-byte tier-count; dropping both trailing count
        // bytes simulates that older, shorter blob and both lists fall back empty.
        let c = ColumnFamilyConfig {
            comparator_name: "uint64".into(),
            ..ColumnFamilyConfig::default()
        };
        let full = c.enc09();
        let legacy = &full[..full.len() - 2];
        let d = decode(legacy);
        assert_eq!(d.comparator_name, "uint64");
        assert!(d.partition_rules.is_empty());
        assert!(d.tier_rules.is_empty());
    }

    #[test]
    fn tier_rules_survive_manifest_round_trip() {
        let c = ColumnFamilyConfig {
            tier_rules: vec![
                TierRule {
                    prefix: b"img/".to_vec(),
                    tier: "hdd".into(),
                    min_age: Duration::from_secs(30 * 24 * 3600),
                },
                TierRule {
                    prefix: b"log/".to_vec(),
                    tier: "cold".into(),
                    min_age: Duration::from_secs(3600),
                },
            ],
            // Coexists with partition_rules (both are appended tails).
            partition_rules: vec![PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            }],
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&c.enc09());
        assert_eq!(d.tier_rules, c.tier_rules);
        assert_eq!(d.partition_rules, c.partition_rules);
    }

    #[test]
    fn legacy_config_with_partition_but_no_tier_tail_decodes_tiers_empty() {
        // A P1-era blob carried the partition tail but no tier tail. Encode with
        // a partition rule, drop only the trailing tier-count byte, and confirm
        // the partition rule still decodes while tier_rules falls back empty.
        let c = ColumnFamilyConfig {
            partition_rules: vec![PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            }],
            ..ColumnFamilyConfig::default()
        };
        let full = c.enc09();
        let legacy = &full[..full.len() - 1];
        let d = decode(legacy);
        assert_eq!(d.partition_rules, c.partition_rules);
        assert!(d.tier_rules.is_empty());
    }

    #[test]
    fn cf_config_persists_sync_mode_and_interval() {
        for sm in [SyncMode::Full, SyncMode::Interval] {
            let c = ColumnFamilyConfig {
                sync_mode: sm,
                sync_interval: Duration::from_micros(250_000),
                ..ColumnFamilyConfig::default()
            };
            let d = decode(&c.enc09());
            assert_eq!(
                d.sync_mode, sm,
                "sync_mode must survive a manifest round-trip"
            );
            assert_eq!(d.sync_interval, Duration::from_micros(250_000));
        }
    }

    #[test]
    fn legacy_blob_without_sync_fields_decodes_to_defaults() {
        // Simulate a manifest written before the appended-tail fields
        // (sync_mode/sync_interval, compression_per_level, FIFO settings,
        // compression_rules, partition_rules, tier_rules) were persisted:
        // encode, then truncate the whole tail (9 bytes sync + 1 byte per-level
        // count + 17 bytes FIFO + 1 byte compression-rules count + 1 byte
        // partition-rules count + 1 byte tier-rules count).
        let c = ColumnFamilyConfig {
            sync_mode: SyncMode::Full,
            comparator_name: "uint64".into(),
            ..ColumnFamilyConfig::default()
        };
        let full = c.enc09();
        let legacy = &full[..full.len() - 30];
        let d = decode(legacy);
        // Older fields still decode; the missing sync fields fall back to default.
        assert_eq!(d.comparator_name, "uint64");
        assert_eq!(d.sync_mode, SyncMode::None);
        assert_eq!(d.sync_interval, ColumnFamilyConfig::default().sync_interval);
    }
}

#[cfg(test)]
mod per_level_tests {
    use super::*;

    #[test]
    fn compression_per_level_roundtrip_and_selection() {
        let c = ColumnFamilyConfig {
            compression: Compression::Snappy,
            compression_per_level: vec![Compression::None, Compression::None, Compression::Zstd],
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&c.enc09());
        assert_eq!(d.compression_per_level, c.compression_per_level);
        assert_eq!(d.compression_for_level(0), Compression::None);
        assert_eq!(d.compression_for_level(1), Compression::None);
        assert_eq!(d.compression_for_level(2), Compression::Zstd);
        assert_eq!(d.compression_for_level(9), Compression::Zstd); // last repeats

        // Empty policy falls back to the uniform setting.
        let u = ColumnFamilyConfig {
            compression: Compression::Lz4,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(u.compression_for_level(0), Compression::Lz4);
        assert_eq!(u.compression_for_level(5), Compression::Lz4);
    }

    /// Counts above 255 use the compatible overflow tail instead of silently
    /// dropping policies. This is realistic for prefix-per-tenant layouts.
    #[test]
    fn rule_counts_are_not_truncated() {
        let n = 1000;
        let c = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; n],
            partition_rules: (0..n)
                .map(|i| PartitionRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    name: format!("p{i:04}"),
                })
                .collect(),
            tier_rules: (0..n)
                .map(|i| TierRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    tier: format!("t{i:04}"),
                    min_age: Duration::from_secs(i as u64),
                })
                .collect(),
            compression_rules: (0..n)
                .map(|i| CompressionRule {
                    prefix: format!("ns{i:04}/").into_bytes(),
                    compression: Compression::Zstd,
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&c.enc09());
        assert_eq!(d.compression_per_level.len(), n, "level policy truncated");
        assert_eq!(d.partition_rules.len(), n, "partition rules truncated");
        assert_eq!(d.tier_rules.len(), n, "tier rules truncated");
        assert_eq!(d.compression_rules.len(), n, "compression rules truncated");
        assert_eq!(d.partition_rules[999].name, "p0999");
        assert_eq!(d.tier_rules[999].tier, "t0999");
        assert_eq!(d.compression_per_level[999], Compression::Zstd);
    }

    #[test]
    fn representable_rule_counts_keep_the_legacy_encoding() {
        let c = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; 255],
            partition_rules: (0..255)
                .map(|i| PartitionRule {
                    prefix: format!("p{i}/").into_bytes(),
                    name: format!("p{i}"),
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let encoded = c.enc09();
        assert!(!encoded
            .windows(CONFIG_OVERFLOW_MAGIC.len())
            .any(|w| w == CONFIG_OVERFLOW_MAGIC));
        let d = decode(&encoded);
        assert_eq!(d.compression_per_level.len(), 255);
        assert_eq!(d.partition_rules.len(), 255);
    }

    #[test]
    fn old_reader_can_ignore_the_overflow_tail() {
        let c = ColumnFamilyConfig {
            partition_rules: (0..300)
                .map(|i| PartitionRule {
                    prefix: format!("p{i}/").into_bytes(),
                    name: format!("p{i}"),
                })
                .collect(),
            ..ColumnFamilyConfig::default()
        };
        let encoded = c.enc09();
        let tail = encoded
            .windows(CONFIG_OVERFLOW_MAGIC.len())
            .position(|w| w == CONFIG_OVERFLOW_MAGIC)
            .expect("oversized policy must have an overflow tail");

        // A 0.3.0 reader ignores bytes after its four base lists. Decoding the
        // base alone models that behavior and must preserve its first 255 rules.
        let old_view = decode(&encoded[..tail]);
        assert_eq!(old_view.partition_rules.len(), 255);
        assert_eq!(old_view.partition_rules[254].name, "p254");
    }

    #[test]
    fn legacy_u8_count_128_decodes_without_losing_policy() {
        use crate::encoding::uvarint;

        let c127 = ColumnFamilyConfig {
            compression_per_level: vec![Compression::Zstd; 127],
            ..ColumnFamilyConfig::default()
        };
        let mut legacy = c127.enc09();

        // Locate the first variable-count field after the fixed config prefix.
        let (name_len, name_len_bytes) = uvarint(&legacy).unwrap();
        let count_offset = name_len_bytes
            + name_len as usize
            + 1 // compression
            + 8 // write_buffer_size
            + 8 // level_size_ratio
            + 8 // klog_value_threshold
            + 1 // enable_bloom_filter
            + 8 // bloom_fpr
            + 4 // l1_file_count_trigger
            + 4 // l0_queue_stall_threshold
            + 1 // use_btree
            + 1 // sync_mode
            + 8; // sync_interval

        // Counts through 127 have always been byte-identical. Turn that blob
        // into the exact 0.3.0 representation of 128 entries: one count byte
        // followed immediately by all 128 compression bytes.
        assert_eq!(legacy[count_offset], 127);
        legacy[count_offset] = 128;
        legacy.insert(count_offset + 1, super::super::codec_id(Compression::Zstd));

        let decoded = decode(&legacy);
        assert_eq!(decoded.compression_per_level, vec![Compression::Zstd; 128]);
    }

    /// The 0.8.0 geometry survives a manifest round-trip, and a config left at
    /// the defaults still encodes exactly as earlier releases wrote it.
    #[test]
    fn compaction_geometry_roundtrip_and_default_is_byte_identical() {
        let tuned = ColumnFamilyConfig {
            target_file_size: 4 << 20,
            l1_base_bytes: 1 << 30,
            soft_pending_compaction_bytes: 7 << 30,
            hard_pending_compaction_bytes: 9 << 30,
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&tuned.enc09());
        assert_eq!(d.target_file_size, 4 << 20);
        assert_eq!(d.l1_base_bytes, 1 << 30);
        assert_eq!(d.soft_pending_compaction_bytes, 7 << 30);
        assert_eq!(d.hard_pending_compaction_bytes, 9 << 30);

        // Defaults carry no tail at all.
        let base = ColumnFamilyConfig::default().enc09();
        assert!(
            !base
                .windows(CONFIG_COMPACTION_MAGIC.len())
                .any(|w| w == CONFIG_COMPACTION_MAGIC),
            "a default config must not emit the compaction tail"
        );
    }

    /// A pre-0.8.0 manifest (no compaction tail) decodes to the new defaults
    /// rather than to zeroes, which would divide by zero when sizing levels.
    #[test]
    fn pre_080_manifest_decodes_to_compaction_defaults() {
        let legacy = ColumnFamilyConfig {
            compression: Compression::Zstd,
            ..ColumnFamilyConfig::default()
        }
        .enc09();
        let d = decode(&legacy);
        let def = ColumnFamilyConfig::default();
        assert_eq!(d.target_file_size, def.target_file_size);
        assert_eq!(d.l1_base_bytes, def.l1_base_bytes);
        assert_eq!(
            d.hard_pending_compaction_bytes,
            def.hard_pending_compaction_bytes
        );
    }

    /// The geometry tail must survive alongside the tails that precede it.
    #[test]
    fn compaction_tail_coexists_with_partition_fn_tail() {
        let cfg = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            ..ColumnFamilyConfig::default()
        };
        let d = decode(&cfg.enc09());
        assert_eq!(d.target_file_size, 2 << 20);
        match d.partition_scheme {
            PartitionScheme::Unresolved(n) => assert_eq!(n, "byhash"),
            other => panic!("partition scheme lost: {other:?}"),
        }
    }

}

#[cfg(test)]
mod block_size_tests {
    use super::*;

    #[test]
    fn a_default_config_emits_no_block_size_tail() {
        let blob = ColumnFamilyConfig {
            compression: Compression::Zstd,
            ..ColumnFamilyConfig::default()
        }
        .enc09();
        assert!(!blob
            .windows(CONFIG_BLOCK_SIZE_MAGIC.len())
            .any(|window| window == CONFIG_BLOCK_SIZE_MAGIC));
    }

    #[test]
    fn a_set_block_size_round_trips() {
        let config = ColumnFamilyConfig {
            data_block_size: 64 << 10,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(
            decode(&config.enc09()).data_block_size,
            64 << 10
        );
    }

    #[test]
    fn the_block_size_tail_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            data_block_size: 16 << 10,
            ..ColumnFamilyConfig::default()
        };
        let decoded = decode(&config.enc09());
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.target_file_size, 2 << 20);
        assert!(matches!(
            decoded.partition_scheme,
            PartitionScheme::Unresolved(ref name) if name == "byhash"
        ));
    }

    #[test]
    fn vlog_cache_blob_omits_default() {
        // The default (0, disabled) must add no bytes: old readers decode new
        // blobs, and an untouched family's blob does not change shape.
        let blob = ColumnFamilyConfig {
            compression: Compression::Zstd,
            data_block_size: 16 << 10,
            ..ColumnFamilyConfig::default()
        }
        .enc09();
        assert!(!blob
            .windows(CONFIG_VLOG_CACHE_MAGIC.len())
            .any(|window| window == CONFIG_VLOG_CACHE_MAGIC));
    }

    #[test]
    fn a_set_vlog_cache_limit_round_trips() {
        let config = ColumnFamilyConfig {
            max_cached_vlog_value_bytes: 1 << 20,
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(
            decode(&config.enc09()).max_cached_vlog_value_bytes,
            1 << 20
        );
    }

    #[test]
    fn the_vlog_cache_tail_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            data_block_size: 16 << 10,
            max_cached_vlog_value_bytes: 4 << 20,
            ..ColumnFamilyConfig::default()
        };
        let decoded = decode(&config.enc09());
        assert_eq!(decoded.max_cached_vlog_value_bytes, 4 << 20);
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.target_file_size, 2 << 20);
        assert!(matches!(
            decoded.partition_scheme,
            PartitionScheme::Unresolved(ref name) if name == "byhash"
        ));
    }

}

#[cfg(test)]
mod bloom_policy_tests {
    use super::*;

    #[test]
    fn bloom_policy_blob_omits_defaults() {
        // A config that differs only elsewhere must encode exactly as it did
        // before this tail existed, so an older binary keeps decoding it.
        let blob = ColumnFamilyConfig {
            compression: Compression::Zstd,
            data_block_size: 16 << 10,
            ..ColumnFamilyConfig::default()
        }
        .enc09();
        assert!(!blob
            .windows(CONFIG_BLOOM_POLICY_MAGIC.len())
            .any(|window| window == CONFIG_BLOOM_POLICY_MAGIC));
    }

    #[test]
    fn a_set_bloom_policy_round_trips_and_coexists_with_preceding_tails() {
        let config = ColumnFamilyConfig {
            partition_scheme: PartitionScheme::Unresolved("byhash".into()),
            target_file_size: 2 << 20,
            data_block_size: 16 << 10,
            bloom_fpr_per_level: vec![0.001, 0.01, 0.05],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        let decoded = decode(&config.enc09());
        assert_eq!(decoded.bloom_fpr_per_level, vec![0.001, 0.01, 0.05]);
        assert!(decoded.optimize_filters_for_hits);
        assert_eq!(decoded.data_block_size, 16 << 10);
        assert_eq!(decoded.target_file_size, 2 << 20);
        assert!(matches!(
            decoded.partition_scheme,
            PartitionScheme::Unresolved(ref name) if name == "byhash"
        ));
    }

    /// A truncated tail leaves both fields at their defaults rather than
    /// applying a half-read policy (the compaction tail's rule).
    #[test]
    fn a_truncated_bloom_policy_tail_is_ignored() {
        let config = ColumnFamilyConfig {
            bloom_fpr_per_level: vec![0.001, 0.01],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        let mut blob = config.enc09();
        blob.truncate(blob.len() - 4);
        let decoded = decode(&blob);
        assert!(decoded.bloom_fpr_per_level.is_empty());
        assert!(!decoded.optimize_filters_for_hits);
    }
}

#[cfg(test)]
mod periodic_tests {
    use super::*;

    /// 1.1: the operator name is the durable half of the merge feature, and it
    /// is decoded from the remainder of the prefix-delta tail — so the two must
    /// chain, in both orders of being set.
    #[test]
    fn merge_operator_name_round_trips() {
        let cfg = ColumnFamilyConfig {
            merge_operator_name: Some("example.counter.i64.v1".to_string()),
            ..ColumnFamilyConfig::default()
        };
        let decoded = decode(&cfg.enc09());
        assert_eq!(
            decoded.merge_operator_name.as_deref(),
            Some("example.counter.i64.v1")
        );
        // The resolved implementation is not persisted; only the name is.
        assert!(decoded.merge_operator.is_none());

        // Behind every other tail this release writes.
        let chained = ColumnFamilyConfig {
            merge_operator_name: Some("m".to_string()),
            enable_prefix_delta_keys: true,
            block_restart_interval: 16,
            periodic_compaction_interval: Duration::from_secs(60),
            bloom_fpr_per_level: vec![0.001, 0.01],
            data_block_size: 8192,
            ..ColumnFamilyConfig::default()
        };
        let decoded = decode(&chained.enc09());
        assert_eq!(decoded.merge_operator_name.as_deref(), Some("m"));
        assert!(decoded.enable_prefix_delta_keys);
        assert_eq!(decoded.block_restart_interval, 16);
        assert_eq!(decoded.data_block_size, 8192);
    }

    /// A blob written before 1.1 has no merge tail, so the family decodes as
    /// having no operator rather than reading garbage off the end — and a
    /// family that sets none must still encode byte-for-byte as 0.8.2 wrote it.
    #[test]
    fn config_blob_without_operator_decodes_none() {
        let default = ColumnFamilyConfig::default();
        let blob = default.enc09();
        assert!(
            !blob
                .windows(CONFIG_MERGE_OP_MAGIC.len())
                .any(|w| w == CONFIG_MERGE_OP_MAGIC),
            "a family with no operator must stay byte-identical to a pre-1.1 blob"
        );
        assert!(decode(&blob).merge_operator_name.is_none());

        // A truncated tail is all-or-nothing: no name rather than half a name.
        let cfg = ColumnFamilyConfig {
            merge_operator_name: Some("truncated".to_string()),
            ..ColumnFamilyConfig::default()
        };
        let full = cfg.enc09();
        let cut = &full[..full.len() - 3];
        assert!(decode(cut).merge_operator_name.is_none());
    }

    #[test]
    fn periodic_interval_blob_omits_default_and_round_trips() {
        let default = ColumnFamilyConfig::default();
        assert!(
            !default
                .enc09()
                .windows(CONFIG_PERIODIC_MAGIC.len())
                .any(|w| w == CONFIG_PERIODIC_MAGIC),
            "the default must stay byte-identical to a pre-0.3 blob"
        );

        let cfg = ColumnFamilyConfig {
            periodic_compaction_interval: Duration::from_secs(7 * 24 * 3600),
            // Set alongside the bloom tail so the two chain correctly: the
            // periodic tail is decoded from the bloom tail's remainder.
            bloom_fpr_per_level: vec![0.001, 0.01],
            optimize_filters_for_hits: true,
            ..ColumnFamilyConfig::default()
        };
        let decoded = decode(&cfg.enc09());
        assert_eq!(
            decoded.periodic_compaction_interval,
            Duration::from_secs(7 * 24 * 3600)
        );
        assert_eq!(decoded.bloom_fpr_per_level, vec![0.001, 0.01]);
        assert!(decoded.optimize_filters_for_hits);

        // And without the bloom tail ahead of it.
        let alone = ColumnFamilyConfig {
            periodic_compaction_interval: Duration::from_secs(60),
            ..ColumnFamilyConfig::default()
        };
        assert_eq!(
            decode(&alone.enc09()).periodic_compaction_interval,
            Duration::from_secs(60)
        );
    }

    /// A blob written before 0.3 has no tail, so the option decodes to its
    /// disabled default rather than to garbage read off the end.
    #[test]
    fn legacy_blob_decodes_periodic_interval_as_disabled() {
        let legacy = ColumnFamilyConfig {
            write_buffer_size: 7 << 20,
            ..ColumnFamilyConfig::default()
        };
        let blob = legacy.enc09();
        let decoded = decode(&blob);
        assert_eq!(decoded.write_buffer_size, 7 << 20);
        assert!(decoded.periodic_compaction_interval.is_zero());
    }
}
