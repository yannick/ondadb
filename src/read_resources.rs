//! Process-wide read resources shared by read-only databases (wavesdb
//! `ReadResources`, `23648c8` / `f6b3def`).
//!
//! Every [`DB::open`](crate::DB::open) builds its own block cache, file-handle
//! cache and reader cache, each bounded on its own. A process serving N
//! immutable databases (a search node holding N published segments, say) then
//! holds N budgets: N × `block_cache_size` of decoded blocks, N ×
//! `max_open_sstables` descriptors, N × `max_open_readers` resident indexes.
//! [`ReadResources`] is one set of those three caches that any number of
//! **read-only** opens lease instead ([`Options::read_resources`]), so the
//! process has one block-cache budget, one file-handle budget and one reader
//! budget however many databases it opens.
//!
//! # Namespacing
//!
//! Both cache keys used to be a bare file id, which is unique only within one
//! database: two databases each have a table `7`. Every lease therefore gets a
//! **namespace id**, and the block and reader caches key on `(namespace, file
//! id)` — two databases with the same table ids and different contents can
//! never be served each other's blocks or readers. The namespace comes from
//! [`Options::read_cache_namespace`]; when unset it is the database directory's
//! canonical path, so two read-only opens of the *same* directory share cached
//! readers and blocks (the files are immutable while any read-only open holds
//! the shared directory lock) and two different directories never do. A
//! caller-supplied namespace is a promise that every database opened under it
//! holds byte-identical tables under the same ids — e.g. copies of one
//! published checkpoint — and is how such copies share one cached copy.
//!
//! # Lifetime
//!
//! [`close`](ReadResources::close) stops new leases immediately; databases
//! already open keep working. The caches are emptied when the last leased
//! database closes (or at `close` if none is open). When the last lease of one
//! *namespace* ends, that namespace's readers and blocks are purged at once, so
//! a closed database's descriptors and memory do not linger until eviction.
//!
//! [`Options::read_resources`]: crate::Options::read_resources
//! [`Options::read_cache_namespace`]: crate::Options::read_cache_namespace

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::cache::{BlockCache, CacheStats, FileCache};
use crate::error::{OndaError, Result};
use crate::table_cache::TableCache;

/// Limits for one [`ReadResources`]. Each field has exactly the meaning of the
/// [`Options`](crate::Options) field it replaces for a leased database, and the
/// defaults are the same.
#[derive(Debug, Clone)]
pub struct ReadResourceOptions {
    /// Decoded-block cache capacity in bytes; `0` disables the cache
    /// ([`Options::block_cache_size`](crate::Options::block_cache_size)).
    pub block_cache_bytes: usize,
    /// Idle SSTable file handles kept open (at least one)
    /// ([`Options::max_open_sstables`](crate::Options::max_open_sstables)).
    pub max_open_files: usize,
    /// Open SSTable readers (at least one)
    /// ([`Options::max_open_readers`](crate::Options::max_open_readers)).
    pub max_open_readers: usize,
    /// Resident index + bloom bytes of those readers; `0` means no byte bound
    /// ([`Options::max_open_reader_bytes`](crate::Options::max_open_reader_bytes)).
    pub max_reader_bytes: usize,
}

impl Default for ReadResourceOptions {
    fn default() -> Self {
        let o = crate::Options::default();
        ReadResourceOptions {
            block_cache_bytes: o.block_cache_size,
            max_open_files: o.max_open_sstables,
            max_open_readers: o.max_open_readers,
            max_reader_bytes: o.max_open_reader_bytes,
        }
    }
}

/// Reader-cache counters of a [`ReadResources`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TableCacheStats {
    /// Readers currently open.
    pub open_readers: usize,
    /// Reader opens (cache misses) so far.
    pub misses: u64,
    /// Lookups served by an already-open reader.
    pub hits: u64,
    /// Readers closed by the count or byte bound.
    pub evictions: u64,
    /// Resident index + bloom bytes of the open readers.
    pub bytes: usize,
}

/// A point-in-time view of a [`ReadResources`].
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadResourceStats {
    /// The shared block cache: hits, misses, evictions, entries, bytes.
    pub block: CacheStats,
    /// The shared reader cache.
    pub table: TableCacheStats,
    /// Idle file handles currently cached.
    pub open_files: usize,
    /// Databases currently holding a lease.
    pub leases: usize,
    /// Whether [`ReadResources::close`] has been called.
    pub closing: bool,
}

#[derive(Debug, Default)]
struct LeaseState {
    leases: usize,
    closing: bool,
    closed: bool,
    /// Live namespaces: name → (namespace id, leases under it).
    namespaces: HashMap<String, (u64, usize)>,
    /// Next namespace id. Starts at 1: id 0 is every private cache's.
    next_ns: u64,
}

/// One block cache, file-handle cache and reader cache shared by any number of
/// read-only databases. See the [module docs](self).
pub struct ReadResources {
    block: BlockCache,
    files: Arc<FileCache>,
    tables: TableCache,
    state: Mutex<LeaseState>,
}

impl std::fmt::Debug for ReadResources {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.state.lock();
        f.debug_struct("ReadResources")
            .field("leases", &s.leases)
            .field("namespaces", &s.namespaces.len())
            .field("closing", &s.closing)
            .finish()
    }
}

impl ReadResources {
    /// A new, independently bounded set of shared read caches.
    pub fn new(opts: ReadResourceOptions) -> Arc<ReadResources> {
        Arc::new(ReadResources {
            block: BlockCache::new(opts.block_cache_bytes as i64),
            files: Arc::new(FileCache::new(opts.max_open_files.max(1))),
            tables: TableCache::with_byte_budget(opts.max_open_readers, opts.max_reader_bytes),
            state: Mutex::new(LeaseState {
                next_ns: 1,
                ..LeaseState::default()
            }),
        })
    }

    /// Stop new leases. Databases already open keep working; the caches are
    /// emptied now if none is, or when the last one closes. Idempotent.
    pub fn close(&self) {
        let finish = {
            let mut s = self.state.lock();
            if s.closing {
                return;
            }
            s.closing = true;
            let finish = s.leases == 0 && !s.closed;
            if finish {
                s.closed = true;
            }
            finish
        };
        if finish {
            self.clear_caches();
        }
    }

    /// Aggregate counters. Safe to call at any time, including after `close`.
    pub fn stats(&self) -> ReadResourceStats {
        let (leases, closing) = {
            let s = self.state.lock();
            (s.leases, s.closing)
        };
        let (open_readers, misses, hits, evictions) = self.tables.stats();
        ReadResourceStats {
            block: self.block.stats(),
            table: TableCacheStats {
                open_readers,
                misses,
                hits,
                evictions,
                bytes: self.tables.byte_stats().0,
            },
            open_files: self.files.num_open(),
            leases,
            closing,
        }
    }

    /// Take a lease for a database opening under `namespace`.
    pub(crate) fn lease(self: &Arc<Self>, namespace: String) -> Result<ReadLease> {
        let ns = {
            let mut s = self.state.lock();
            if s.closing {
                return Err(OndaError::InvalidArgs("ReadResources is closed".into()));
            }
            s.leases += 1;
            let next = s.next_ns;
            let entry = s.namespaces.entry(namespace.clone()).or_insert((next, 0));
            entry.1 += 1;
            let ns = entry.0;
            if ns == next {
                s.next_ns += 1;
            }
            ns
        };
        Ok(ReadLease {
            owner: self.clone(),
            namespace,
            ns,
        })
    }

    fn release(&self, namespace: &str, ns: u64) {
        let (purge_ns, finish) = {
            let mut s = self.state.lock();
            s.leases = s.leases.saturating_sub(1);
            let mut purge_ns = false;
            if let Some(entry) = s.namespaces.get_mut(namespace) {
                entry.1 = entry.1.saturating_sub(1);
                if entry.1 == 0 {
                    s.namespaces.remove(namespace);
                    purge_ns = true;
                }
            }
            let finish = s.closing && s.leases == 0 && !s.closed;
            if finish {
                s.closed = true;
            }
            (purge_ns, finish)
        };
        if finish {
            self.clear_caches();
        } else if purge_ns {
            // Nobody reads this namespace any more: free its readers (and their
            // file handles) and blocks now rather than when eviction gets there.
            for reader in self.tables.purge_namespace(ns) {
                reader.close();
            }
            self.block.purge_namespace(ns);
        }
    }

    fn clear_caches(&self) {
        for reader in self.tables.purge_all() {
            reader.close();
        }
        self.block.clear();
        self.files.clear();
    }

    /// The shared file-handle cache, for the leased database's local storage.
    pub(crate) fn file_cache(&self) -> Arc<FileCache> {
        self.files.clone()
    }
}

/// A database's hold on a [`ReadResources`]: its namespaced views of the shared
/// caches. Dropping it releases the lease.
pub(crate) struct ReadLease {
    owner: Arc<ReadResources>,
    namespace: String,
    ns: u64,
}

impl std::fmt::Debug for ReadLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadLease")
            .field("namespace", &self.namespace)
            .field("ns", &self.ns)
            .finish()
    }
}

impl ReadLease {
    pub(crate) fn file_cache(&self) -> Arc<FileCache> {
        self.owner.file_cache()
    }

    /// This lease's namespaced view of the shared block cache.
    pub(crate) fn block_cache(&self) -> BlockCache {
        self.owner.block.namespaced(self.ns)
    }

    /// This lease's namespaced view of the shared reader cache.
    pub(crate) fn table_cache(&self) -> TableCache {
        self.owner.tables.namespaced(self.ns)
    }
}

impl Drop for ReadLease {
    fn drop(&mut self) {
        self.owner.release(&self.namespace, self.ns);
    }
}
