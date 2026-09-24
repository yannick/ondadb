//! `yolodb` — offline maintenance for yoloDB-format databases.
//!
//! ```text
//! yolodb upgrade <path> [--verify scan|counts] [--no-backup]
//! ```
//!
//! `upgrade` runs the automatic 0.9.x → epoch-1 upgrade (plan C §1.3) without
//! opening the database for use, with progress on stderr — for operators who
//! would rather not upgrade inside the application's first open. It needs none
//! of the application's merge operators or partitioners (operands are copied,
//! never folded), takes the WAL layout from the 0.9 catalog, and completes a
//! swap an earlier crash interrupted. Exit status: `0` upgraded or already
//! epoch 1, `1` failed (the 0.9 directory is untouched unless the failure came
//! mid-swap, which the next open or run resolves), `2` usage.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use ondadb::upgrade::{UpgradeObserver, UpgradePhase};
use ondadb::{FormatUpgradeVerify, Options};

const USAGE: &str = "usage: yolodb upgrade <path> [--verify scan|counts] [--no-backup]";

/// Progress on stderr: one line per protocol step, one per family's tables.
struct Progress {
    start: Instant,
    tables: AtomicUsize,
}

impl UpgradeObserver for Progress {
    fn on_phase(&self, phase: UpgradePhase) -> ondadb::Result<()> {
        let what = match phase {
            UpgradePhase::Preflighted => "preflight passed (lock held, space checked)",
            UpgradePhase::UpgradeDirCreated => "upgrade directory created",
            UpgradePhase::TablesWritten => "tables written",
            UpgradePhase::ManifestWritten => "epoch-1 manifest written",
            UpgradePhase::Verified => "verified against the source",
            UpgradePhase::JournalWritten => "swap journal written",
            UpgradePhase::SourceRenamed => "0.9 directory moved to its backup name",
            UpgradePhase::UpgradeRenamed => "upgraded directory moved into place",
            UpgradePhase::JournalDone => "swap complete",
        };
        eprintln!("[{:>7.2}s] {what}", self.start.elapsed().as_secs_f64());
        Ok(())
    }

    fn on_table(&self, cf: &str, done: usize, total: usize) {
        let n = self.tables.fetch_add(1, Ordering::Relaxed) + 1;
        if done <= total {
            eprintln!(
                "[{:>7.2}s]   {cf}: table {done}/{total} ({n} written)",
                self.start.elapsed().as_secs_f64()
            );
        } else {
            eprintln!(
                "[{:>7.2}s]   {cf}: replayed WAL data written ({n} written)",
                self.start.elapsed().as_secs_f64()
            );
        }
    }
}

fn usage() -> ! {
    eprintln!("{USAGE}");
    std::process::exit(2);
}

fn main() {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("upgrade") {
        usage();
    }
    let mut path = None;
    let mut opts_verify = FormatUpgradeVerify::Scan;
    let mut keep_backup = true;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--verify" => {
                opts_verify = match args.next().as_deref() {
                    Some("scan") => FormatUpgradeVerify::Scan,
                    Some("counts") => FormatUpgradeVerify::Counts,
                    _ => usage(),
                }
            }
            "--no-backup" => keep_backup = false,
            "-h" | "--help" => {
                println!("{USAGE}");
                return;
            }
            other if other.starts_with('-') => usage(),
            other if path.is_none() => path = Some(other.to_string()),
            _ => usage(),
        }
    }
    let Some(path) = path else { usage() };
    let mut opts = Options::new(path.clone());
    opts.format_upgrade_verify = opts_verify;
    opts.format_upgrade_keep_backup = keep_backup;
    let progress = Progress {
        start: Instant::now(),
        tables: AtomicUsize::new(0),
    };
    match ondadb::upgrade::upgrade_observed(opts, &progress) {
        Ok(None) => println!("{path}: already yoloDB format epoch 1; nothing to do"),
        Ok(Some(r)) => {
            let secs = r.duration.as_secs_f64();
            println!(
                "{path}: {} to yoloDB format epoch 1 in {secs:.2}s",
                if r.resumed {
                    "completed an interrupted upgrade"
                } else {
                    "upgraded"
                },
            );
            println!(
                "  {} column families, {} tables, {} entries, max sequence {}",
                r.column_families, r.tables, r.entries, r.max_seq
            );
            println!(
                "  {} bytes (0.9) -> {} bytes (epoch 1); verification: {:?}",
                r.source_bytes, r.upgraded_bytes, r.verify
            );
            if !r.resumed && secs > 0.0 {
                println!(
                    "  throughput {:.1} MiB/s of source data",
                    r.source_bytes as f64 / (1 << 20) as f64 / secs
                );
            }
            match &r.backup_path {
                Some(b) => println!(
                    "  the 0.9 directory is kept at {} — delete it once satisfied",
                    b.display()
                ),
                None => println!("  the 0.9 directory was deleted (--no-backup)"),
            }
        }
        Err(e) => {
            eprintln!("{path}: upgrade failed: {e}");
            std::process::exit(1);
        }
    }
}
