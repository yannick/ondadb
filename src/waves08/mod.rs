//! Read-only decoders for **wavesdb** directories (plan C step 2c).
//!
//! Since step 2b wavesdb writes yoloDB epoch-1 *containers* — the same magics,
//! versions, CRC32-C and section layout this crate writes — but keeps its 0.8
//! *bodies* inside them, which it declares with
//! [`CAP_WAVES08_BODIES`].
//! Step 2c makes either engine able to open the other's directory read-only;
//! this module is ondaDB's half. Its decoders are the only place wavesdb's
//! bodies are understood, behind the default-on `waves08` feature — building
//! without it makes a wavesdb directory a hard `UnsupportedFormat` refusal, as
//! it was before 2c.
//!
//! The dispatch rule is the **whole body profile**: a catalog carrying
//! `CAP_WAVES08_BODIES` is read entirely through this module, never one
//! section or one bit at a time through the epoch-1 decoders.
//!
//! | Artifact | wavesdb body | Decoder |
//! |---|---|---|
//! | `MANIFEST` | epoch-1 layout plus db section `0x8` (managed state), cf `0x2` (shared blobs), sst `0x80` (merge stats), `0x100` (blob refs) | [`manifest`] |
//! | `MANIFEST-EDITS` | `YOLODBED` header, schema 2: wavesdb's 18-op table, canonical payloads | [`edit_log`] |
//! | CF config | JSON (wavesdb's `persistedConfig`) | [`config`] |
//! | SSTable data blocks | legacy, extended full-key and prefix-delta, chosen per block | `block` (normalized once, before the block cache) |
//!
//! Everything here is **decode-only**: nothing writes a wavesdb byte, and a
//! wavesdb directory is never opened read-write.
//!
//! The frozen fixtures these decoders are checked against live in
//! `tests/fixtures/wavesdb-epoch1/`, written by wavesdb itself.

pub(crate) mod block;
pub mod config;
pub mod edit_log;
pub mod manifest;

use std::collections::BTreeMap;
use std::path::Path;

use crate::error::{OndaError, Result};
use crate::format::wavesdb_reserved::{
    CAP_MANAGED_MODE, CAP_VALUES_SHARED_BLOBS, CAP_VLOG_GROUPING, CAP_WAVES08_BODIES,
};
use crate::format::{
    CAP_EXTENDED_RECORDS, CAP_MANIFEST_EDITS, CAP_MERGE_OPERANDS, CAP_PERIODIC_AGE,
    CAP_PREFIX_DELTA, CAP_RANGE_DELETES, CAP_TXN_DECISIONS,
};
use crate::manifest::{CfManifest, Manifest, SstMeta, WalLayout};

/// Every capability bit a wavesdb epoch-1 catalog may carry (wavesdb
/// `format.Epoch1KnownCaps`): ondaDB's seven, wavesdb's managed mode and value
/// grouping, and the two container bits.
pub const KNOWN_CAPS: u64 = crate::format::KNOWN_CAPS
    | CAP_MANAGED_MODE
    | CAP_VLOG_GROUPING
    | CAP_VALUES_SHARED_BLOBS
    | CAP_WAVES08_BODIES;

/// The bits a table's footer may declare (wavesdb `format.Epoch1TableCaps`);
/// a database-level bit in a table word is corruption.
pub const TABLE_CAPS: u64 = CAP_EXTENDED_RECORDS
    | CAP_MERGE_OPERANDS
    | CAP_RANGE_DELETES
    | CAP_PREFIX_DELTA
    | CAP_VLOG_GROUPING
    | CAP_VALUES_SHARED_BLOBS
    | CAP_WAVES08_BODIES;

/// Table bits describing extended entries, which need
/// `CAP_EXTENDED_RECORDS` in the same word unless it carries
/// `CAP_WAVES08_BODIES` (wavesdb `format.Epoch1RecordCaps`): 0.8 blocks
/// declare their own encoding per block.
const RECORD_CAPS: u64 = CAP_MERGE_OPERANDS | CAP_RANGE_DELETES | CAP_PREFIX_DELTA;

const _: () = assert!(KNOWN_CAPS == 0x7FF);
const _: () = assert!(TABLE_CAPS & !KNOWN_CAPS == 0);
const _: () = assert!(
    TABLE_CAPS & (CAP_MANIFEST_EDITS | CAP_PERIODIC_AGE | CAP_TXN_DECISIONS | CAP_MANAGED_MODE)
        == 0
);

/// wavesdb's capability dependency rule (`format.capabilityDeps`): merge
/// operands, range deletes and transaction decisions each need extended
/// records. A word violating it is corruption — no wavesdb writer produces
/// one.
pub fn check_dependencies(caps: u64) -> Result<()> {
    let needs_ext = caps & (CAP_MERGE_OPERANDS | CAP_RANGE_DELETES | CAP_TXN_DECISIONS) != 0;
    if needs_ext && caps & CAP_EXTENDED_RECORDS == 0 {
        return Err(corrupt(format!(
            "capability word {caps:#x} needs CAP_EXTENDED_RECORDS"
        )));
    }
    Ok(())
}

/// Check a table capability word the way wavesdb's `ValidTableCaps` does:
/// an unknown bit is a newer format, a word no writer produces is corruption.
pub fn check_table_caps(caps: u64) -> Result<()> {
    if caps & !KNOWN_CAPS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb table capability word {caps:#x} outside known mask {KNOWN_CAPS:#x}"
        )));
    }
    let impossible = caps & !TABLE_CAPS != 0
        || (caps & RECORD_CAPS != 0 && caps & (CAP_EXTENDED_RECORDS | CAP_WAVES08_BODIES) == 0)
        || (caps & CAP_VLOG_GROUPING != 0 && caps & CAP_VALUES_SHARED_BLOBS == 0);
    if impossible {
        return Err(corrupt(format!(
            "table capability word {caps:#x} is not one any writer produces"
        )));
    }
    Ok(())
}

pub(crate) fn corrupt(msg: impl std::fmt::Display) -> OndaError {
    OndaError::Corruption(format!("wavesdb catalog: {msg}"))
}

/// One shared value file (`<id>.blob`) a family's tables point into.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Blob {
    pub id: u64,
    pub size: u64,
    pub live_bytes: u64,
}

/// One table, as wavesdb catalogs it: ondaDB's [`SstMeta`] plus the two
/// sections ondaDB's catalog has no field for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Table {
    /// Every field the two engines share. `table_caps` is the catalog's
    /// declaration of the footer word (`None` for zero).
    pub meta: SstMeta,
    /// Merge operands in the table, and the longest operand chain.
    pub merge_count: u64,
    pub merge_chain_max: u64,
    /// Bytes this table references in each shared value file, by blob id.
    pub blob_bytes: BTreeMap<u64, u64>,
}

/// One column family in a wavesdb catalog.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Family {
    pub name: String,
    /// The config exactly as stored: wavesdb's JSON.
    pub config: Vec<u8>,
    /// The stored unified id, when it is not the one the name derives.
    pub unified_id: Option<u64>,
    /// Shared value files, ascending by id.
    pub blobs: Vec<Blob>,
    pub tables: Vec<Table>,
}

/// A wavesdb catalog: the snapshot with its edit log replayed onto it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Catalog {
    pub caps: u64,
    pub next_file_id: u64,
    pub global_seq: u64,
    pub wal_layout: WalLayout,
    pub instance_nonce: Option<u64>,
    pub generation: u64,
    pub applied_through: u64,
    pub next_edit_id: u64,
    /// Managed-sequence state (`CAP_MANAGED_MODE`): the discard floor and the
    /// last caller-assigned commit; both zero outside managed mode.
    pub discard_seq: u64,
    pub last_managed_commit: u64,
    pub cfs: Vec<Family>,
}

impl Default for Catalog {
    fn default() -> Self {
        Catalog {
            caps: 0,
            next_file_id: 1,
            global_seq: 0,
            wal_layout: WalLayout::PerColumnFamily,
            instance_nonce: None,
            generation: 0,
            applied_through: 0,
            next_edit_id: 1,
            discard_seq: 0,
            last_managed_commit: 0,
            cfs: Vec::new(),
        }
    }
}

impl Catalog {
    pub(crate) fn family_mut(&mut self, name: &str) -> Option<&mut Family> {
        self.cfs.iter_mut().find(|cf| cf.name == name)
    }

    /// The catalog as ondaDB's [`Manifest`]: each config converted from JSON
    /// to the blob [`ColumnFamilyConfig::encode`] writes, everything ondaDB
    /// has no field for left on `self`.
    ///
    /// [`ColumnFamilyConfig::encode`]: crate::ColumnFamilyConfig::encode
    pub fn to_manifest(&self) -> Result<Manifest> {
        let mut cfs = Vec::with_capacity(self.cfs.len());
        for cf in &self.cfs {
            let cfg = config::decode(&cf.config)
                .map_err(|e| prefix(e, &format!("column family {:?}", cf.name)))?;
            cfs.push(CfManifest {
                name: cf.name.clone(),
                config: cfg.encode(),
                sstables: cf.tables.iter().map(|t| t.meta.clone()).collect(),
                unified_id: cf.unified_id,
            });
        }
        Ok(Manifest {
            next_file_id: self.next_file_id,
            global_seq: self.global_seq,
            cfs,
            wal_layout: self.wal_layout,
            instance_nonce: self.instance_nonce,
            caps: self.caps,
            generation: self.generation,
            applied_through: self.applied_through,
            next_edit_id: self.next_edit_id,
        })
    }
}

/// Prefix a decode error's message with where it happened, keeping its class.
pub(crate) fn prefix(e: OndaError, at: &str) -> OndaError {
    match e {
        OndaError::Corruption(m) => OndaError::Corruption(format!("{at}: {m}")),
        OndaError::UnsupportedFormat(m) => OndaError::UnsupportedFormat(format!("{at}: {m}")),
        other => other,
    }
}

/// Load a wavesdb directory's whole catalog: the `MANIFEST` snapshot with its
/// `MANIFEST-EDITS` log replayed onto it, under wavesdb's recovery rules
/// ([`edit_log::replay`]).
pub fn recover_catalog(dir: impl AsRef<Path>) -> Result<Catalog> {
    let dir = dir.as_ref();
    let data = std::fs::read(crate::manifest::manifest_path(dir))?;
    let snapshot = manifest::decode(&data)?;
    match std::fs::read(crate::manifest_edit::edit_log_path(dir)) {
        Ok(log) => edit_log::replay(snapshot, &log),
        // wavesdb admits a missing log when the snapshot has nothing unfolded
        // (`IsFreshEditLog`), which an epoch-1 snapshot always satisfies: its
        // decoder already demands next_edit_id == applied_through + 1.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(snapshot),
        Err(e) => Err(e.into()),
    }
}

/// Whether `dir` holds a wavesdb database: an epoch-1 `MANIFEST` whose
/// capability word carries `CAP_WAVES08_BODIES`. Reads the 20-byte prefix
/// only; the full decode (and its checksum) is [`recover_catalog`]'s job.
pub fn is_waves_dir(dir: impl AsRef<Path>) -> Result<bool> {
    use std::io::Read;
    let mut f = match std::fs::File::open(crate::manifest::manifest_path(dir)) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e.into()),
    };
    let mut head = [0u8; 20];
    match f.read_exact(&mut head) {
        Ok(()) => Ok(head[..8] == crate::format::manifest_file::MAGIC
            && crate::encoding::read_u64(&head[12..20]) & CAP_WAVES08_BODIES != 0),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(e.into()),
    }
}

/// A checked reader over a decoded body. `canonical` additionally refuses a
/// uvarint encoded in more bytes than it needs — the edit log's rule, which
/// makes its payload bytes a function of the edit; the snapshot decoder does
/// not demand it, and neither does wavesdb's.
pub(crate) struct Reader<'a> {
    pub(crate) p: &'a [u8],
    canonical: bool,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(p: &'a [u8], canonical: bool) -> Self {
        Reader { p, canonical }
    }

    pub(crate) fn take(&mut self, n: usize, what: &str) -> Result<&'a [u8]> {
        if self.p.len() < n {
            return Err(corrupt(format!("{what} is truncated")));
        }
        let (v, rest) = self.p.split_at(n);
        self.p = rest;
        Ok(v)
    }

    pub(crate) fn u32(&mut self, what: &str) -> Result<u32> {
        Ok(crate::encoding::read_u32(self.take(4, what)?))
    }

    pub(crate) fn u64(&mut self, what: &str) -> Result<u64> {
        Ok(crate::encoding::read_u64(self.take(8, what)?))
    }

    pub(crate) fn uvar(&mut self, what: &str) -> Result<u64> {
        let (v, n) = crate::encoding::uvarint(self.p)
            .ok_or_else(|| corrupt(format!("{what} is not a uvarint")))?;
        if self.canonical && n > 1 && self.p[n - 1] == 0 {
            return Err(corrupt(format!("{what} is not a minimal uvarint")));
        }
        self.p = &self.p[n..];
        Ok(v)
    }

    pub(crate) fn bytes(&mut self, what: &str) -> Result<Vec<u8>> {
        let len = self.uvar(what)?;
        let len = usize::try_from(len).map_err(|_| corrupt(format!("{what} length overflows")))?;
        Ok(self.take(len, what)?.to_vec())
    }

    pub(crate) fn string(&mut self, what: &str) -> Result<String> {
        String::from_utf8(self.bytes(what)?).map_err(|_| corrupt(format!("{what} is not UTF-8")))
    }

    /// A level, which both engines bound to `u32`.
    pub(crate) fn level(&mut self) -> Result<u32> {
        let l = self.uvar("table level")?;
        u32::try_from(l).map_err(|_| corrupt(format!("table level {l} exceeds u32")))
    }

    /// A count about to drive a loop, bounded by the bytes left (every element
    /// is at least one byte) so a lying count cannot ask for an allocation.
    pub(crate) fn count(&mut self, what: &str) -> Result<u64> {
        let n = self.uvar(what)?;
        if n > self.p.len() as u64 {
            return Err(corrupt(format!("{what} {n} exceeds the bytes left")));
        }
        Ok(n)
    }
}
