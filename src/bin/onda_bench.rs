//! Standalone ondaDB benchmark, .
//!  16-byte keys, 100-byte values, random keys, 8
//! worker threads by default; Put / cold Get / Delete run as batched
//! transactions (1000 ops/txn) partitioned across threads, Forward/Backward
//! scans run `threads` concurrent full iterations.
//!
//! Output lines match the Go bench format exactly (`<phase> … <ops/sec> ops/sec`)
//! so `bench/bench_graphs.sh`'s `parse_go` awk handles them verbatim with the
//! engine label `ondadb`.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use ondadb::{ColumnFamily, ColumnFamilyConfig, Compression, IsolationLevel, Options, DB};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Put,
    Get,
    Forward,
    Backward,
    Delete,
}

impl Phase {
    const ALL: [Self; 5] = [
        Self::Put,
        Self::Get,
        Self::Forward,
        Self::Backward,
        Self::Delete,
    ];

    fn parse(name: &str) -> Option<Self> {
        match name {
            "put" => Some(Self::Put),
            "get" => Some(Self::Get),
            "forward" => Some(Self::Forward),
            "backward" => Some(Self::Backward),
            "delete" => Some(Self::Delete),
            _ => None,
        }
    }

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PhaseSet(u8);

impl PhaseSet {
    fn all() -> Self {
        Self(
            Phase::ALL
                .into_iter()
                .fold(0, |bits, phase| bits | phase.bit()),
        )
    }

    fn parse_list(value: &str) -> Result<Self, String> {
        if value.is_empty() {
            return Err("-phases requires at least one phase".into());
        }
        let mut bits = 0;
        for name in value.split(',') {
            let phase =
                Phase::parse(name).ok_or_else(|| format!("unknown benchmark phase: {name}"))?;
            bits |= phase.bit();
        }
        Ok(Self(bits))
    }

    const fn contains(self, phase: Phase) -> bool {
        self.0 & phase.bit() != 0
    }

    const fn needs_population(self) -> bool {
        self.0 != 0
    }

    const fn needs_reopen(self) -> bool {
        self.contains(Phase::Get)
            || self.contains(Phase::Forward)
            || self.contains(Phase::Backward)
            || self.contains(Phase::Delete)
    }

    #[cfg(test)]
    fn selected(self) -> Vec<Phase> {
        Phase::ALL
            .into_iter()
            .filter(|phase| self.contains(*phase))
            .collect()
    }
}

struct Args {
    ops: usize,
    key_size: usize,
    value_size: usize,
    threads: usize,
    pattern: String,
    compression: String,
    batch: usize,
    db_path: String,
    keep: bool,
    phases: PhaseSet,
    perf_scope: PerfScope,
}

/// How the Get phase exercises [`ondadb::perf`]. The nil-path acceptance for
/// 0.10 is `Off` versus `Thread`: same reads, the only difference being whether
/// every `perf::bump` in the read path has a frame to write into.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PerfScope {
    /// No scope open: every bump is the thread-local nil check.
    Off,
    /// One scope per worker thread, open across the whole phase.
    Thread,
    /// `get_with_perf` per operation: also pays scope push/pop per read.
    Op,
}

impl PerfScope {
    fn parse(name: &str) -> Result<PerfScope, String> {
        match name {
            "off" => Ok(PerfScope::Off),
            "thread" => Ok(PerfScope::Thread),
            "op" => Ok(PerfScope::Op),
            other => Err(format!("-perf_scope must be off|thread|op: {other}")),
        }
    }
}

fn parse_args_from<I, S>(args: I) -> Result<Args, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut a = Args {
        ops: 1_000_000,
        key_size: 16,
        value_size: 100,
        threads: 8,
        pattern: "random".into(),
        compression: "none".into(),
        batch: 1000,
        db_path: String::new(),
        keep: false,
        phases: PhaseSet::all(),
        perf_scope: PerfScope::Off,
    };
    let argv: Vec<String> = args
        .into_iter()
        .map(|value| value.as_ref().to_owned())
        .collect();
    let mut i = 1;
    while i < argv.len() {
        let flag = argv[i].clone();
        let val = |i: &mut usize| -> Result<String, String> {
            *i += 1;
            argv.get(*i)
                .cloned()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        let positive = |name: &str, value: String| -> Result<usize, String> {
            let parsed = value
                .parse::<usize>()
                .map_err(|_| format!("{name} must be a positive integer: {value}"))?;
            if parsed == 0 {
                return Err(format!("{name} must be greater than zero"));
            }
            Ok(parsed)
        };
        match flag.as_str() {
            "-ops" => a.ops = positive("-ops", val(&mut i)?)?,
            "-key_size" => a.key_size = positive("-key_size", val(&mut i)?)?,
            "-value_size" => a.value_size = positive("-value_size", val(&mut i)?)?,
            "-threads" => a.threads = positive("-threads", val(&mut i)?)?,
            "-pattern" => a.pattern = val(&mut i)?,
            "-compression" => a.compression = val(&mut i)?,
            "-batch" => a.batch = positive("-batch", val(&mut i)?)?,
            "-db" => a.db_path = val(&mut i)?,
            "-phases" => a.phases = PhaseSet::parse_list(&val(&mut i)?)?,
            "-perf_scope" => a.perf_scope = PerfScope::parse(&val(&mut i)?)?,
            "-keep" => a.keep = true,
            "-engine" => {
                let _ = val(&mut i)?; // accepted for CLI compatibility
            }
            _ => {}
        }
        i += 1;
    }
    Ok(a)
}

fn gen_keys(a: &Args) -> Vec<Vec<u8>> {
    let mut keys = Vec::with_capacity(a.ops);
    // Simple deterministic xorshift PRNG (no external rng needed in the binary).
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = || {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        x
    };
    for idx in 0..a.ops {
        let mut k = vec![0u8; a.key_size];
        if a.pattern == "sequential" {
            let be = (idx as u64).to_be_bytes();
            let n = be.len().min(a.key_size);
            k[..n].copy_from_slice(&be[..n]);
        } else {
            for b in k.iter_mut() {
                *b = next() as u8;
            }
        }
        keys.push(k);
    }
    keys
}

fn compression(name: &str) -> Compression {
    Compression::parse(name).unwrap_or(Compression::None)
}

/// Run `n` items across `threads`, calling `f(lo, hi)` per partition.
fn run_threaded<F>(n: usize, threads: usize, f: F)
where
    F: Fn(usize, usize) + Sync,
{
    let per = n.div_ceil(threads);
    thread::scope(|s| {
        for t in 0..threads {
            let lo = t * per;
            let hi = ((t + 1) * per).min(n);
            if lo >= hi {
                continue;
            }
            let f = &f;
            s.spawn(move || f(lo, hi));
        }
    });
}

fn report(phase: &str, ops: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64().max(1e-9);
    let ops_per_sec = ops as f64 / secs;
    println!(
        "{:<28} {} ops    {:.2} ms    {:.0} ops/sec",
        phase,
        ops,
        elapsed.as_secs_f64() * 1000.0,
        ops_per_sec
    );
}

fn populate(db: &DB, cf: &Arc<ColumnFamily>, keys: &[Vec<u8>], value: &[u8], a: &Args) -> Duration {
    let start = Instant::now();
    run_threaded(keys.len(), a.threads, |lo, hi| {
        let mut i = lo;
        while i < hi {
            let be = (i + a.batch).min(hi);
            let mut txn = db.begin_with_isolation(IsolationLevel::ReadCommitted);
            for key in &keys[i..be] {
                txn.put(cf, key, value, Duration::ZERO).unwrap();
            }
            txn.commit().unwrap();
            i = be;
        }
    });
    start.elapsed()
}

fn main() {
    let a = match parse_args_from(std::env::args()) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("onda_bench: {error}");
            std::process::exit(2);
        }
    };
    let db_path = if a.db_path.is_empty() {
        "ondadb_bench_data".to_string()
    } else {
        a.db_path.clone()
    };
    let _ = std::fs::remove_dir_all(&db_path);

    eprintln!(
        "ondadb benchmark: ops={} threads={} key={} value={} pattern={} batch={} compression={}",
        a.ops, a.threads, a.key_size, a.value_size, a.pattern, a.batch, a.compression
    );

    let keys = gen_keys(&a);
    let value = vec![b'v'; a.value_size];

    let cfg = ColumnFamilyConfig {
        compression: compression(&a.compression),
        ..ColumnFamilyConfig::default()
    };

    // ---- Put -----------------------------------------------------------------
    let initial_db = Arc::new(DB::open(Options::new(&db_path)).expect("open"));
    let initial_cf = initial_db
        .create_column_family("bench", cfg.clone())
        .expect("create cf");
    let put_elapsed = if a.phases.needs_population() {
        populate(&initial_db, &initial_cf, &keys, &value, &a)
    } else {
        Duration::ZERO
    };
    if a.phases.contains(Phase::Put) {
        report("Put", a.ops, put_elapsed);
    }

    let (db, cf) = if a.phases.needs_reopen() {
        // Keep every post-put phase comparable with the full workload: its
        // keys live in SSTables, even when Put itself was only setup.
        initial_db.close().expect("close before post-put phases");
        drop(initial_cf);
        drop(initial_db);
        let db = Arc::new(DB::open(Options::new(&db_path)).expect("reopen"));
        let cf = db.get_column_family("bench").expect("cf after reopen");
        (db, cf)
    } else {
        (initial_db, initial_cf)
    };

    // ---- Get (cold) ----------------------------------------------------------
    if a.phases.contains(Phase::Get) {
        let start = Instant::now();
        {
            let db = &db;
            let cf = &cf;
            let keys = &keys;
            let vsize = a.value_size;
            let mode = a.perf_scope;
            run_threaded(a.ops, a.threads, |lo, hi| {
                // Held for the whole slice in `Thread` mode, so every bump in
                // the read path writes into a real frame.
                let _scope = (mode == PerfScope::Thread).then(ondadb::perf::enter);
                for k in &keys[lo..hi] {
                    let got = if mode == PerfScope::Op {
                        db.get_with_perf(cf, k).0
                    } else {
                        db.get(cf, k)
                    };
                    match got {
                        Ok(v) if v.len() == vsize => {}
                        _ => { /* count silently; random keys may collide/miss */ }
                    }
                }
            });
        }
        report("Get (cold)", a.ops, start.elapsed());
    }

    // ---- Forward / Backward scan (threads concurrent full iterations) --------
    // Run BEFORE Delete and straight after the reopen, matching the Go/C harness
    // phase order — so scans read from SSTables (the on-disk path), not a hot
    // memtable.
    let scan = |reverse: bool| -> Duration {
        let start = Instant::now();
        {
            let db = &db;
            let cf = &cf;
            thread::scope(|s| {
                for _ in 0..a.threads {
                    s.spawn(move || {
                        let mut txn = db.begin();
                        let mut it = txn.new_iterator(cf);
                        let mut count = 0u64;
                        if reverse {
                            it.seek_to_last();
                            while it.valid() {
                                count += 1;
                                it.prev();
                            }
                        } else {
                            it.seek_to_first();
                            while it.valid() {
                                count += 1;
                                it.next();
                            }
                        }
                        std::hint::black_box(count);
                        let _ = txn.rollback();
                    });
                }
            });
        }
        start.elapsed()
    };
    if a.phases.contains(Phase::Forward) {
        report("Forward Scan", a.ops, scan(false));
    }
    if a.phases.contains(Phase::Backward) {
        report("Backward Scan", a.ops, scan(true));
    }

    // ---- Delete --------------------------------------------------------------
    if a.phases.contains(Phase::Delete) {
        let start = Instant::now();
        {
            let db = &db;
            let cf = &cf;
            let keys = &keys;
            run_threaded(a.ops, a.threads, |lo, hi| {
                let mut i = lo;
                while i < hi {
                    let be = (i + a.batch).min(hi);
                    let mut txn = db.begin_with_isolation(IsolationLevel::ReadCommitted);
                    for key in &keys[i..be] {
                        txn.delete(cf, key).unwrap();
                    }
                    txn.commit().unwrap();
                    i = be;
                }
            });
        }
        report("Delete", a.ops, start.elapsed());
    }

    db.close().expect("final close");
    if !a.keep {
        let _ = std::fs::remove_dir_all(&db_path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phases_default_to_all() {
        let args = parse_args_from(["onda_bench"]).unwrap();
        assert!(Phase::ALL
            .into_iter()
            .all(|phase| args.phases.contains(phase)));
    }

    #[test]
    fn phases_accept_a_comma_separated_subset() {
        let args = parse_args_from(["onda_bench", "-phases", "get,forward"]).unwrap();
        assert!(args.phases.contains(Phase::Get));
        assert!(args.phases.contains(Phase::Forward));
        assert!(!args.phases.contains(Phase::Put));
    }

    #[test]
    fn perf_scope_defaults_to_off_and_parses_its_modes() {
        assert_eq!(
            parse_args_from(["onda_bench"]).unwrap().perf_scope,
            PerfScope::Off
        );
        for (name, want) in [("thread", PerfScope::Thread), ("op", PerfScope::Op)] {
            let args = parse_args_from(["onda_bench", "-perf_scope", name]).unwrap();
            assert_eq!(args.perf_scope, want);
        }
        assert!(parse_args_from(["onda_bench", "-perf_scope", "bogus"]).is_err());
    }

    #[test]
    fn phases_reject_unknown_and_empty_values() {
        assert!(parse_args_from(["onda_bench", "-phases", "bogus"]).is_err());
        assert!(parse_args_from(["onda_bench", "-phases", ""]).is_err());
    }

    #[test]
    fn numeric_arguments_reject_invalid_values() {
        assert!(parse_args_from(["onda_bench", "-ops", "zero"]).is_err());
        assert!(parse_args_from(["onda_bench", "-threads", "0"]).is_err());
    }

    #[test]
    fn post_put_phases_require_population_and_reopen() {
        for name in ["get", "forward", "backward", "delete"] {
            let phases = PhaseSet::parse_list(name).unwrap();
            assert!(phases.needs_population());
            assert!(phases.needs_reopen());
        }
    }

    #[test]
    fn put_only_does_not_require_reopen() {
        let phases = PhaseSet::parse_list("put").unwrap();
        assert!(phases.needs_population());
        assert!(!phases.needs_reopen());
    }

    #[test]
    fn populate_writes_every_key() {
        let db_path =
            std::env::temp_dir().join(format!("ondadb-onda-bench-populate-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&db_path);
        let db = DB::open(Options::new(db_path.to_string_lossy())).unwrap();
        let cf = db
            .create_column_family("bench", ColumnFamilyConfig::default())
            .unwrap();
        let keys = vec![b"first".to_vec(), b"second".to_vec()];
        let value = b"value";
        let args = Args {
            ops: keys.len(),
            key_size: 5,
            value_size: value.len(),
            threads: 1,
            pattern: "sequential".into(),
            compression: "none".into(),
            batch: 1,
            db_path: String::new(),
            keep: false,
            phases: PhaseSet::all(),
            perf_scope: PerfScope::Off,
        };

        let _ = populate(&db, &cf, &keys, value, &args);

        for key in &keys {
            assert_eq!(db.get(&cf, key).unwrap(), value);
        }
        db.close().unwrap();
        std::fs::remove_dir_all(db_path).unwrap();
    }

    #[test]
    fn selected_phases_keep_canonical_order() {
        let phases = PhaseSet::parse_list("delete,get,put").unwrap();
        assert_eq!(
            phases.selected(),
            vec![Phase::Put, Phase::Get, Phase::Delete]
        );
    }
}
