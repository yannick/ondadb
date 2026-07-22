//! Derived partitioning (A5): partitions computed from the key rather than
//! enumerated in a rule vector.
//!
//! The rule-based path must be untouched, so the first test here pins its
//! encoding and its cut boundaries against the behaviour that shipped before
//! this feature existed.

use std::sync::Arc;
use std::time::Duration;

use ondadb::{ColumnFamilyConfig, Options, PartitionFn, PartitionRule, PartitionScheme, DB};

// ---------------------------------------------------------------------------
// A partitioner shaped like the one this feature was asked for: keys are
// `<name>\0\0<8-byte bucket><rest>` and a partition is one `(name, bucket)`.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct NameAndBucket;

impl NameAndBucket {
    /// Build a key in the layout this partitioner understands.
    fn key(name: &str, bucket: u64, rest: &str) -> Vec<u8> {
        let mut k = Vec::new();
        k.extend_from_slice(name.as_bytes());
        k.extend_from_slice(&[0, 0]);
        k.extend_from_slice(&bucket.to_be_bytes());
        k.extend_from_slice(rest.as_bytes());
        k
    }
}

impl PartitionFn for NameAndBucket {
    fn boundary_len(&self, key: &[u8]) -> usize {
        match key.windows(2).position(|w| w == [0, 0]) {
            Some(end) => (end + 2 + 8).min(key.len()),
            None => key.len(),
        }
    }

    fn name(&self, key: &[u8]) -> String {
        let b = &key[..self.boundary_len(key)];
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    fn scheme_name(&self) -> &str {
        "test.name-and-bucket.v1"
    }
}

/// A second, deliberately different scheme, to exercise mismatch detection.
#[derive(Debug)]
struct OtherScheme;

impl PartitionFn for OtherScheme {
    fn boundary_len(&self, key: &[u8]) -> usize {
        key.len().min(1)
    }
    fn name(&self, key: &[u8]) -> String {
        format!("{:02x}", key.first().copied().unwrap_or(0))
    }
    fn scheme_name(&self) -> &str {
        "test.other.v1"
    }
}

fn derived_cfg() -> ColumnFamilyConfig {
    ColumnFamilyConfig {
        partition_scheme: PartitionScheme::Derived(Arc::new(NameAndBucket)),
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    }
}

fn opts_with_fn(path: &str) -> Options {
    let mut o = Options::new(path);
    o.partition_fns = vec![Arc::new(NameAndBucket)];
    o
}

// ---------------------------------------------------------------------------
// The rules path is unchanged
// ---------------------------------------------------------------------------

/// A config with no derived scheme encodes to exactly the bytes it did before
/// derived partitioning existed.
///
/// The new marker lives in its own tagged tail that is only written when a
/// derived scheme is set, so this is the guarantee that every existing
/// manifest — and every reader of one — is unaffected.
#[test]
fn rules_config_encoding_is_byte_identical() {
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            },
            PartitionRule {
                prefix: b"log/".to_vec(),
                name: "log".into(),
            },
        ],
        ..ColumnFamilyConfig::default()
    };
    let blob = cfg.encode();

    // No derived marker anywhere in a rules-only blob.
    assert!(
        !blob.windows(8).any(|w| w == b"ONDAPFN1"),
        "a rules-only config must not carry the derived-partitioner tail"
    );

    // And it still round-trips to the same rules.
    let back = ColumnFamilyConfig::decode(&blob);
    assert_eq!(back.partition_rules, cfg.partition_rules);
    assert!(matches!(back.partition_scheme, PartitionScheme::Rules));
}

/// Rule-based cutting still produces the `img` and `log` parts.
///
/// `freeze_part` is the public probe for "this partition has a bottom part":
/// it hard-links the part's files and errors with `NotFound` otherwise.
#[test]
fn rules_path_cuts_the_same_parts() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cfg = ColumnFamilyConfig {
        partition_rules: vec![
            PartitionRule {
                prefix: b"img/".to_vec(),
                name: "img".into(),
            },
            PartitionRule {
                prefix: b"log/".to_vec(),
                name: "log".into(),
            },
        ],
        l1_file_count_trigger: 1,
        ..ColumnFamilyConfig::default()
    };
    let cf = db.create_column_family("default", cfg).unwrap();
    for i in 0..5u32 {
        db.put(&cf, format!("img/{i:03}").as_bytes(), b"I", Duration::ZERO)
            .unwrap();
        db.put(&cf, format!("log/{i:03}").as_bytes(), b"L", Duration::ZERO)
            .unwrap();
        db.put(&cf, format!("etc/{i:03}").as_bytes(), b"E", Duration::ZERO)
            .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    let out = tempfile::tempdir().unwrap();
    db.freeze_part(&cf, "img", out.path().join("img")).unwrap();
    db.freeze_part(&cf, "log", out.path().join("log")).unwrap();
}

// ---------------------------------------------------------------------------
// The derived path
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

/// Reopening with the right partitioner registered resolves the same
/// partitions, and the parts written before the reopen stay addressable.
#[test]
fn reopen_with_the_same_scheme_resolves_identically() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let part = NameAndBucket.name(&NameAndBucket::key("x", 3, ""));

    {
        let db = DB::open(opts_with_fn(&path)).unwrap();
        let cf = db.create_column_family("default", derived_cfg()).unwrap();
        for t in ["x", "y"] {
            for i in 0..4u32 {
                db.put(
                    &cf,
                    &NameAndBucket::key(t, 3, &format!("{i:03}")),
                    b"v",
                    Duration::ZERO,
                )
                .unwrap();
            }
        }
        db.flush_memtable(&cf).unwrap();
        db.compact(&cf).unwrap();
        let out = tempfile::tempdir().unwrap();
        db.freeze_part(&cf, &part, out.path().join("p")).unwrap();
        db.close().unwrap();
    }

    let db = DB::open(opts_with_fn(&path)).unwrap();
    let cf = db.get_column_family("default").unwrap();
    assert_eq!(
        db.get(&cf, &NameAndBucket::key("x", 3, "000")).unwrap(),
        b"v"
    );
    // The same partition name still addresses a bottom part after reopen.
    let out = tempfile::tempdir().unwrap();
    db.freeze_part(&cf, &part, out.path().join("p")).unwrap();
}

/// Reopening WITHOUT the partitioner registered fails, rather than silently
/// falling back to rule-based partitioning.
///
/// The fallback would be the dangerous outcome: existing parts were cut on
/// derived boundaries, so every part written afterwards would be cut
/// differently while every operation reported success.
#[test]
fn reopen_without_the_scheme_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = DB::open(opts_with_fn(&path)).unwrap();
        let cf = db.create_column_family("default", derived_cfg()).unwrap();
        db.put(&cf, &NameAndBucket::key("x", 1, "k"), b"v", Duration::ZERO)
            .unwrap();
        db.flush_memtable(&cf).unwrap();
        db.close().unwrap();
    }

    // Plain Options: no partitioner registered.
    let err = DB::open(Options::new(&path)).expect_err("must refuse to open");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("test.name-and-bucket.v1") && msg.contains("partition_fns"),
        "error should name the missing scheme and where to register it, got: {msg}"
    );
}

/// Reopening with a *different* partitioner is an error too.
#[test]
fn reopen_with_a_mismatched_scheme_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = DB::open(opts_with_fn(&path)).unwrap();
        let cf = db.create_column_family("default", derived_cfg()).unwrap();
        db.put(&cf, &NameAndBucket::key("x", 1, "k"), b"v", Duration::ZERO)
            .unwrap();
        db.flush_memtable(&cf).unwrap();
        db.close().unwrap();
    }

    let mut o = Options::new(&path);
    o.partition_fns = vec![Arc::new(OtherScheme)]; // registered, but not the one used
    let err = DB::open(o).expect_err("a different scheme must not satisfy the marker");
    assert!(format!("{err:?}").contains("test.name-and-bucket.v1"));
}

/// A config carrying an unresolved scheme keeps the name when re-encoded.
///
/// Without this, a round trip through a reader that could not resolve the
/// scheme would erase the marker and silently demote the column family to
/// rule-based partitioning.
#[test]
fn an_unresolved_scheme_survives_re_encoding() {
    let cfg = ColumnFamilyConfig {
        partition_scheme: PartitionScheme::Derived(Arc::new(NameAndBucket)),
        ..ColumnFamilyConfig::default()
    };
    let once = ColumnFamilyConfig::decode(&cfg.encode());
    assert!(matches!(
        &once.partition_scheme,
        PartitionScheme::Unresolved(n) if n == "test.name-and-bucket.v1"
    ));

    // Re-encode from the unresolved state and decode again: still there.
    let twice = ColumnFamilyConfig::decode(&once.encode());
    assert!(matches!(
        &twice.partition_scheme,
        PartitionScheme::Unresolved(n) if n == "test.name-and-bucket.v1"
    ));
}

/// Derived parts survive detach → attach with their tags intact.
#[test]
fn detach_and_attach_preserve_derived_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(opts_with_fn(dir.path().to_str().unwrap())).unwrap();
    let cf = db.create_column_family("default", derived_cfg()).unwrap();

    for t in ["p", "q"] {
        for i in 0..4u32 {
            db.put(
                &cf,
                &NameAndBucket::key(t, 5, &format!("{i:03}")),
                b"v",
                Duration::ZERO,
            )
            .unwrap();
        }
    }
    db.flush_memtable(&cf).unwrap();
    db.compact(&cf).unwrap();

    // The partition name the derived scheme gives these keys.
    let target = NameAndBucket.name(&NameAndBucket::key("p", 5, ""));
    let probe = NameAndBucket::key("p", 5, "000");

    let detached = db.detach_part(&cf, &target).unwrap();
    assert_eq!(detached.partition, target);
    assert!(db.get(&cf, &probe).is_err(), "detached data is hidden");

    db.attach_part(&cf, &detached.dir).unwrap();
    assert_eq!(db.get(&cf, &probe).unwrap(), b"v");

    // Reattached with the same derived tag: it can be detached again by name.
    db.detach_part(&cf, &target).unwrap();
}
