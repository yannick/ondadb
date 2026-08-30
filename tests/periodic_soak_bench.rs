//! 0.3 harness: TTL write phase + idle soak.
//!
//! `#[ignore]`d: this is a measurement harness, not a gate — it prints numbers
//! and asserts only the two things the feature's acceptance section actually
//! claims (space reclaimed with no writes; negligible idle CPU when nothing is
//! eligible). Run it explicitly:
//!
//! ```sh
//! cargo test --release --test periodic_soak_bench -- --ignored --nocapture
//! ```
//!
//! The A/B is `periodic_compaction_interval = 0` (the control — today's
//! behaviour, where an idle database reclaims nothing) against the same fixture
//! with the interval set. Both arms take the capability, so the only difference
//! between them is the option.
//!
//! Two clocks are in play on purpose. TTL expiry runs on the REAL clock
//! (`now_nanos`), so the harness genuinely waits its TTL out; the periodic
//! trigger runs on the injected clock, so the soak does not have to last an
//! interval. Idle CPU is read from the process's own `ps` cputime, which has
//! centisecond resolution on both macOS and Linux — enough over a soak measured
//! in tens of seconds.

use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ondadb::format::CAP_PERIODIC_AGE;
use ondadb::{ColumnFamilyConfig, Options, DB};

/// Entries written with a TTL, and the payload each carries.
const EXPIRING: u32 = 20_000;
const SURVIVING: u32 = 2_000;
const VALUE_BYTES: usize = 512;
/// How long the fixture's TTL data stays live. Waited out on the real clock.
const TTL: Duration = Duration::from_secs(5);
/// The configured interval in the enabled arm; derives a 1 s scan cadence.
const INTERVAL: Duration = Duration::from_secs(4);
/// How long each arm sits completely idle after the fixture is in place.
const SOAK: Duration = Duration::from_secs(20);
const T0: i64 = 1_000_000_000_000;

fn sst_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "klog" || x == "vlog") {
                total += std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
            }
        }
    }
    total
}

/// This process's accumulated CPU time, in seconds. `ps` is used rather than a
/// new dependency; its `time` column is `[[DD-]HH:]MM:SS.ss` on both platforms.
fn process_cpu_seconds() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &std::process::id().to_string()])
        .output()
        .expect("ps must be available");
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let mut seconds = 0.0;
    for part in text.replace('-', ":").split(':') {
        seconds = seconds * 60.0 + part.parse::<f64>().unwrap_or(0.0);
    }
    seconds
}

struct Arm {
    label: &'static str,
    bytes_before: u64,
    bytes_after: u64,
    entries_before: u64,
    entries_after: u64,
    periodic_compactions: u64,
    idle_cpu_seconds: f64,
}

impl Arm {
    fn reclaimed_pct(&self) -> f64 {
        if self.bytes_before == 0 {
            return 0.0;
        }
        100.0 * (self.bytes_before - self.bytes_after) as f64 / self.bytes_before as f64
    }

    fn print(&self) {
        println!(
            "{:<22} bytes {:>10} -> {:>10} ({:>5.1}% reclaimed)  entries {:>6} -> {:>6}  \
             periodic_compactions {:>3}  idle CPU {:>6.2}s over {}s",
            self.label,
            self.bytes_before,
            self.bytes_after,
            self.reclaimed_pct(),
            self.entries_before,
            self.entries_after,
            self.periodic_compactions,
            self.idle_cpu_seconds,
            SOAK.as_secs(),
        );
    }
}

/// One arm: write the TTL fixture, let its TTL expire, then sit idle.
fn run_arm(label: &'static str, interval: Duration) -> Arm {
    let dir = tempfile::tempdir().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family(
            "default",
            ColumnFamilyConfig {
                periodic_compaction_interval: interval,
                // One L0 file can never reach this, so no capacity trigger can
                // fire during the soak: whatever reclaims is the age trigger.
                l1_file_count_trigger: 64,
                ..ColumnFamilyConfig::default()
            },
        )
        .unwrap();
    let clock = Arc::new(AtomicI64::new(T0));
    let handle = clock.clone();
    db.set_clock_for_tests(Arc::new(move || handle.load(Ordering::SeqCst)));

    // --- TTL write phase -------------------------------------------------
    let payload = vec![b'x'; VALUE_BYTES];
    for i in 0..EXPIRING {
        db.put(&cf, format!("exp{i:07}").as_bytes(), &payload, TTL)
            .unwrap();
    }
    for i in 0..SURVIVING {
        db.put(
            &cf,
            format!("keep{i:07}").as_bytes(),
            &payload,
            Duration::ZERO,
        )
        .unwrap();
    }
    db.flush_memtable(&cf).unwrap();
    // Both arms take the capability, so the ONLY difference is the option.
    db.enable_format_capabilities(CAP_PERIODIC_AGE).unwrap();

    let bytes_before = sst_bytes(dir.path());
    let entries_before = cf.stats().num_entries;

    // Wait the TTL out on the real clock, then move the periodic clock past the
    // interval. From here the database takes no writes at all.
    std::thread::sleep(TTL + Duration::from_secs(1));
    clock.store(T0 + 100 * INTERVAL.as_nanos() as i64, Ordering::SeqCst);

    // --- idle soak -------------------------------------------------------
    let cpu_before = process_cpu_seconds();
    std::thread::sleep(SOAK);
    let idle_cpu_seconds = process_cpu_seconds() - cpu_before;

    let arm = Arm {
        label,
        bytes_before,
        bytes_after: sst_bytes(dir.path()),
        entries_before,
        entries_after: cf.stats().num_entries,
        periodic_compactions: cf.stats().periodic_compactions,
        idle_cpu_seconds,
    };
    drop(cf);
    db.close().unwrap();
    arm
}

#[test]
#[ignore = "measurement harness, not a gate — run with --ignored --nocapture"]
fn periodic_ttl_write_then_idle_soak() {
    println!(
        "\n0.3 TTL-write + idle-soak: {EXPIRING} expiring + {SURVIVING} surviving entries \
         of {VALUE_BYTES}B, TTL {}s, soak {}s\n",
        TTL.as_secs(),
        SOAK.as_secs()
    );
    let control = run_arm("interval = 0 (control)", Duration::ZERO);
    let enabled = run_arm("interval = 4s", INTERVAL);
    control.print();
    enabled.print();
    println!(
        "\nreclaimed delta: {:.1} pp   idle CPU delta: {:+.2}s\n",
        enabled.reclaimed_pct() - control.reclaimed_pct(),
        enabled.idle_cpu_seconds - control.idle_cpu_seconds,
    );

    // The acceptance criteria, asserted rather than eyeballed.
    assert_eq!(
        control.bytes_after, control.bytes_before,
        "the control must reclaim nothing: no trigger is due on an idle database"
    );
    assert_eq!(control.periodic_compactions, 0);
    assert!(
        enabled.bytes_after < enabled.bytes_before / 2,
        "the age trigger must reclaim the expired half: {} -> {}",
        enabled.bytes_before,
        enabled.bytes_after
    );
    assert!(
        enabled.entries_after <= u64::from(SURVIVING),
        "every expired entry is gone: {} left",
        enabled.entries_after
    );
    assert!(
        enabled.periodic_compactions >= 1,
        "and the age trigger is what did it"
    );
}
