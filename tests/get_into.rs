//! Caller-buffer point reads (`get_into`).
//!
//! The binary installs a counting global allocator, so "no allocation on a
//! hit" is asserted, not assumed.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::time::Duration;

use ondadb::format::CAP_RANGE_DELETES;
use ondadb::{ColumnFamily, ColumnFamilyConfig, MergeOperator, OndaError, Options, DB};

struct Counting;

thread_local! {
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.with(|c| c.set(c.get() + 1));
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Allocations `f` performs on this thread.
fn allocs<R>(f: impl FnOnce() -> R) -> (R, u64) {
    let before = ALLOCS.with(Cell::get);
    let r = f();
    (r, ALLOCS.with(Cell::get) - before)
}

#[derive(Debug)]
struct Concat;

impl MergeOperator for Concat {
    fn name(&self) -> &str {
        "test.get-into.concat"
    }
    fn full_merge(
        &self,
        _key: &[u8],
        existing: Option<&[u8]>,
        operands: &[&[u8]],
    ) -> Result<Vec<u8>, String> {
        let mut out = existing.map(<[u8]>::to_vec).unwrap_or_default();
        for op in operands {
            out.push(b'+');
            out.extend_from_slice(op);
        }
        Ok(out)
    }
}

fn open(dir: &std::path::Path, unified: bool) -> (DB, Arc<ColumnFamily>) {
    let mut opts = Options::new(dir.to_str().unwrap());
    opts.unified_memtable = unified;
    opts.merge_fns = vec![Arc::new(Concat)];
    let db = DB::open(opts).unwrap();
    db.enable_format_capabilities(CAP_RANGE_DELETES).unwrap();
    let cf = db
        .create_column_family(
            "c",
            ColumnFamilyConfig {
                l1_file_count_trigger: 1 << 20,
                klog_value_threshold: 64,
                merge_operator_name: Some("test.get-into.concat".into()),
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    (db, cf)
}

/// `get_into` answers exactly what `get` answers, from every kind of source,
/// appending to whatever the buffer already holds and leaving it untouched on
/// a miss.
#[test]
fn get_into_agrees_with_get_everywhere() {
    for unified in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (db, cf) = open(dir.path(), unified);
        let big = vec![b'v'; 300]; // above klog_value_threshold: a vlog value
        db.put(&cf, b"old", b"table-0", Duration::ZERO).unwrap();
        db.put(&cf, b"shadowed", b"stale", Duration::ZERO).unwrap();
        db.put(&cf, b"vlog", &big, Duration::ZERO).unwrap();
        db.put(&cf, b"gone", b"x", Duration::ZERO).unwrap();
        db.put(&cf, b"ranged", b"x", Duration::ZERO).unwrap();
        db.merge(&cf, b"m", b"a").unwrap();
        db.flush_memtable(&cf).unwrap();
        db.put(&cf, b"shadowed", b"fresh", Duration::ZERO).unwrap();
        db.flush_memtable(&cf).unwrap();
        db.delete(&cf, b"gone").unwrap();
        db.delete_range(&cf, b"ranged", b"ranged\0").unwrap();
        db.put(&cf, b"expired", b"x", Duration::from_nanos(1)).unwrap();
        db.merge(&cf, b"m", b"b").unwrap();
        db.put(&cf, b"mem", b"in-memtable", Duration::ZERO).unwrap();
        std::thread::sleep(Duration::from_millis(2));

        let keys: [&[u8]; 10] = [
            b"old", b"shadowed", b"vlog", b"gone", b"ranged", b"expired", b"m", b"mem",
            b"absent", b"zzz",
        ];
        for key in keys {
            let want = db.get(&cf, key);
            let mut buf = b"prefix:".to_vec();
            let got = db.get_into(&cf, key, &mut buf);
            match want {
                Ok(v) => {
                    assert_eq!(got.unwrap(), v.len(), "{key:?} unified={unified}");
                    assert_eq!(&buf[..7], b"prefix:");
                    assert_eq!(&buf[7..], &v[..], "{key:?} unified={unified}");
                }
                Err(OndaError::NotFound) => {
                    assert!(matches!(got, Err(OndaError::NotFound)), "{key:?}");
                    assert_eq!(buf, b"prefix:", "a miss changed the buffer");
                }
                Err(e) => panic!("{key:?}: {e}"),
            }
        }
        assert_eq!(db.get(&cf, b"m").unwrap(), b"+a+b");
        db.close().unwrap();
    }
}

#[test]
fn transaction_and_snapshot_get_into() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), false);
    db.put(&cf, b"k", b"v1", Duration::ZERO).unwrap();
    let snap = db.snapshot();
    db.put(&cf, b"k", b"v2", Duration::ZERO).unwrap();

    let mut buf = Vec::new();
    assert_eq!(snap.get_into(&cf, b"k", &mut buf).unwrap(), 2);
    assert_eq!(buf, b"v1");

    let mut txn = db.begin();
    txn.put(&cf, b"own", b"buffered", Duration::ZERO).unwrap();
    txn.delete(&cf, b"k").unwrap();
    buf.clear();
    assert_eq!(txn.get_into(&cf, b"own", &mut buf).unwrap(), 8);
    assert_eq!(buf, b"buffered");
    assert!(matches!(txn.get_into(&cf, b"k", &mut buf), Err(OndaError::NotFound)));
    assert_eq!(buf, b"buffered");
    txn.merge(&cf, b"m", b"z").unwrap();
    buf.clear();
    txn.get_into(&cf, b"m", &mut buf).unwrap();
    assert_eq!(buf, txn.get(&cf, b"m").unwrap());
    txn.rollback().unwrap();
    buf.clear();
    db.begin().get_into(&cf, b"k", &mut buf).unwrap();
    assert_eq!(buf, b"v2");
    drop(snap);
    db.close().unwrap();
}

/// The point of the API: a hit into a buffer with room allocates nothing for
/// the value — from the memtable, and from a table whose block is already
/// cached — where `get` allocates the value it returns.
#[test]
fn a_hit_into_a_buffer_with_room_allocates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path(), false);
    db.put(&cf, b"t", b"from-a-table", Duration::ZERO).unwrap();
    db.flush_memtable(&cf).unwrap();
    db.put(&cf, b"k", b"from-the-memtable", Duration::ZERO).unwrap();
    let mut buf = Vec::with_capacity(64);
    // Warm the reader and the block cache: a cold read does I/O and allocates
    // the block it decodes, which is not what this measures.
    db.get_into(&cf, b"t", &mut buf).unwrap();

    for (key, want) in [(&b"k"[..], &b"from-the-memtable"[..]), (b"t", b"from-a-table")] {
        buf.clear();
        let (r, n) = allocs(|| db.get_into(&cf, key, &mut buf));
        assert_eq!(r.unwrap(), want.len());
        assert_eq!(buf, want);
        // The default (crossbeam skiplist) memtable builds an owned probe key
        // for every lookup, `get` and `get_into` alike — one allocation that
        // is the memtable's, not the value's. The arena memtable probes with
        // the borrowed key and allocates nothing.
        let probe = u64::from(key == b"k" && !cfg!(feature = "arena-memtable"));
        assert_eq!(n, probe, "get_into of {key:?} allocated {n} times");
        let (v, m) = allocs(|| db.get(&cf, key).unwrap());
        assert_eq!(v, want);
        assert!(m > n, "get is expected to allocate its value ({m} vs {n})");
    }
    db.close().unwrap();
}
