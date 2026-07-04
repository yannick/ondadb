//! Bulk ingestion: stream pre-sorted entries directly into L0 SSTables,
//! bypassing the WAL and memtable.
//!
//! The fast path for loading large sorted datasets (migrations, restores,
//! bench fills): no WAL write amplification, no memtable churn, no flush.
//! Entries must arrive in strictly ascending key order (per the CF's
//! comparator); files roll at the CF's `write_buffer_size`.
//!
//! Durability and visibility:
//! - Nothing is visible until [`Ingestion::finish`] installs every finished
//!   SSTable atomically and persists the manifest. On `Ok`, the ingested data
//!   is durable (each SSTable is fsynced by `Writer::finish` before the
//!   manifest references it — the same ordering the flush path uses).
//! - All entries share one commit sequence, reserved (and published) at
//!   `start_ingestion`. A snapshot taken *during* the ingestion therefore has
//!   a read sequence past it and will see the data once installed — the same
//!   visibility quirk fjall's ingestion has. Take snapshots before or after.
//! - Dropping an `Ingestion` without `finish` aborts: the partially written
//!   files are deleted (they were never referenced by the manifest).

use std::sync::Arc;
use std::time::Duration;

use crate::column_family::{ColumnFamily, SstHandle};
use crate::db::{DbInner, DB};
use crate::error::{OndaError, Result};
use crate::sst::Writer;
use crate::util::now_nanos;

/// Streaming bulk loader for one column family. Create with
/// [`DB::start_ingestion`].
#[derive(Debug)]
pub struct Ingestion {
    db: Arc<DbInner>,
    cf: Arc<ColumnFamily>,
    seq: u64,
    writer: Option<(Writer, u64)>, // (writer, file_id)
    /// Finished-but-not-installed tables, installed together in `finish`.
    done: Vec<Arc<SstHandle>>,
    /// klog paths of every file written so far, for abort cleanup.
    pending_files: Vec<String>,
    last_key: Option<Vec<u8>>,
    written: u64,
    cur_bytes: u64,
    roll_bytes: u64,
    finished: bool,
}

impl DB {
    /// Begin a bulk ingestion into `cf`. See the [`ingest`](crate::ingest) module
    /// docs for ordering, durability, and snapshot-visibility semantics.
    pub fn start_ingestion(&self, cf: &Arc<ColumnFamily>) -> Result<Ingestion> {
        if self.inner.opts.read_only {
            return Err(OndaError::ReadOnly("database is read-only".into()));
        }
        self.inner.poison.check()?;
        // One commit sequence for the whole ingestion, published immediately so
        // the gap-free visibility cursor never stalls behind a long load.
        let seq = self.inner.reserve_seq(1);
        self.inner.publish_range(seq, seq + 1);
        Ok(Ingestion {
            db: self.inner.clone(),
            cf: cf.clone(),
            seq,
            writer: None,
            done: Vec::new(),
            pending_files: Vec::new(),
            last_key: None,
            written: 0,
            cur_bytes: 0,
            roll_bytes: (cf.opts.write_buffer_size as u64).max(1 << 20),
            finished: false,
        })
    }
}

impl Ingestion {
    /// Append one key/value. Keys must be strictly ascending in the CF's
    /// comparator order. `ttl` of zero means no expiry.
    pub fn write(&mut self, key: &[u8], value: &[u8], ttl: Duration) -> Result<()> {
        self.add(key, value, ttl, false)
    }

    /// Append a tombstone, shadowing any older version of `key` already in the
    /// tree. Same ordering rule as [`write`](Self::write).
    pub fn write_tombstone(&mut self, key: &[u8]) -> Result<()> {
        self.add(key, &[], Duration::ZERO, true)
    }

    fn add(&mut self, key: &[u8], value: &[u8], ttl: Duration, tombstone: bool) -> Result<()> {
        if self.finished {
            return Err(OndaError::InvalidArgs("ingestion already finished".into()));
        }
        if let Some(last) = &self.last_key {
            if self.cf.comparator().compare(last, key) != std::cmp::Ordering::Less {
                return Err(OndaError::InvalidArgs(
                    "ingestion keys must be strictly ascending".into(),
                ));
            }
        }
        if self.writer.is_none() {
            let file_id = self.db.next_file_id();
            let expected = (self.roll_bytes / 64).clamp(1024, 1 << 22) as usize;
            let w = self.cf.new_sst_writer(file_id, expected)?;
            self.pending_files.push(self.cf.klog_path(file_id));
            self.writer = Some((w, file_id));
        }
        let ttl_abs = if ttl.is_zero() {
            0
        } else {
            now_nanos().saturating_add(ttl.as_nanos() as i64)
        };
        let (w, _) = self.writer.as_mut().expect("writer created above");
        w.add(key, value, self.seq, ttl_abs, tombstone, false)?;
        self.last_key = Some(key.to_vec());
        self.written += 1;
        self.cur_bytes += (key.len() + value.len()) as u64;
        if self.cur_bytes >= self.roll_bytes {
            let (w, file_id) = self.writer.take().expect("current writer");
            self.done.push(self.cf.finish_writer_to_handle(w, file_id)?);
            self.cur_bytes = 0;
        }
        Ok(())
    }

    /// Finish the ingestion: fsync the last table, install every table into
    /// L0 atomically, and persist the manifest. Returns the entry count.
    pub fn finish(mut self) -> Result<u64> {
        self.finished = true;
        if let Some((w, file_id)) = self.writer.take() {
            self.done.push(self.cf.finish_writer_to_handle(w, file_id)?);
        }
        if !self.done.is_empty() {
            self.cf.install_handles_l0(std::mem::take(&mut self.done));
            self.db.persist_manifest()?;
        }
        self.pending_files.clear(); // referenced by the manifest now
        Ok(self.written)
    }
}

impl Drop for Ingestion {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // Abort: none of these files ever entered the manifest, so plain
        // unlink is safe (checkpoint/backup only copy manifest-listed files).
        self.writer = None;
        self.done.clear();
        for klog in &self.pending_files {
            let _ = std::fs::remove_file(klog);
            let _ = std::fs::remove_file(klog.replace(".klog", ".vlog"));
        }
    }
}
