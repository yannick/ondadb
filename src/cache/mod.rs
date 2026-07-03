//! Shared caches used by the SSTable layer: a byte-bounded LRU [`BlockCache`] of
//! decompressed blocks, and an [`FileCache`] that bounds the number of open
//! SSTable file descriptors.

mod block;
mod file;

pub use block::{BlockCache, CacheStats};
pub use file::FileCache;
