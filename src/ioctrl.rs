//! Background IO classification and rate limiting.
//!
//! Flush, compaction and obsolete-file cleanup can saturate a device and
//! inflate foreground p99. ondaDB already paces *writers* by compaction debt
//! (how much work is owed); this module adds the missing dimension — how fast
//! background work is allowed to consume IO.
//!
//! **Why a thread-local class.** ondaDB's scheduled background IO runs on
//! dedicated, stable worker threads (`onda-flush`, `onda-compact-{n}`), so the
//! class can live in a thread-local instead of being threaded through every
//! read/write signature. Three paths run background-sized IO on the *caller's*
//! thread — manual compaction ([`crate::compaction::run_manual`]), the
//! caller-side flush rotate, and bulk ingest — and those set the class with an
//! explicit [`scoped`] guard at entry. A spawn-time default alone would leave
//! them classified `Foreground` and never charged, and manual compaction is the
//! largest burst the engine produces.
//!
//! **The limiter object itself is not thread-local.** It is DB-scoped: readers
//! and writers carry an `Option<Arc<dyn IoLimiter>>` from their column family,
//! so two databases in one process pace independently and the default costs one
//! nil check. Only the *class* comes from the thread.
//!
//! Foreground IO is never made to wait — the whole point is to protect it — so
//! [`TokenBucket::charge`] returns immediately for [`IoClass::Foreground`]. WAL
//! writes and fsyncs are foreground durability and are never charged at all.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

/// What a thread's IO is for.
///
/// The class is a property of the *work*, not of the device: a compaction
/// worker reading an input block and a user thread serving a `get` issue the
/// same syscall, and only the class separates them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoClass {
    /// User-facing reads and write commits. Never delayed by the limiter.
    Foreground,
    /// Memtable flush and bulk-ingest output (both land in L0).
    Flush,
    /// Compaction jobs, and the part mover that shares the compaction worker.
    Compaction,
    /// Unlinking obsolete SSTables (paced in review B of this feature).
    ObsoleteDelete,
}

thread_local! {
    /// This thread's current class. A `const`-initialized [`Cell`] so the
    /// foreground fast path is a plain TLS load with no lazy-init call and no
    /// destructor to register — the same reasoning as `perf::HOT`.
    static CLASS: Cell<IoClass> = const { Cell::new(IoClass::Foreground) };
}

/// This thread's current IO class.
#[inline]
pub fn current() -> IoClass {
    CLASS.with(|c| c.get())
}

/// Set this thread's class, returning the previous one.
///
/// Used at worker spawn, where the thread is dedicated and never needs
/// restoring. Anywhere else use [`scoped`], which restores on drop.
pub fn set_class(class: IoClass) -> IoClass {
    CLASS.with(|c| c.replace(class))
}

/// Restores the enclosing IO class when dropped. See [`scoped`].
#[derive(Debug)]
pub struct ClassGuard {
    prev: IoClass,
}

impl Drop for ClassGuard {
    fn drop(&mut self) {
        set_class(self.prev);
    }
}

/// Classify this thread's IO until the returned guard drops.
///
/// Nesting is safe and restores in LIFO order, so a caller thread that already
/// *is* a worker thread is left exactly as it was found. The restore also runs
/// while unwinding: a panic mid-compaction must not leave the thread that
/// caught it permanently labelled as background.
pub fn scoped(class: IoClass) -> ClassGuard {
    ClassGuard {
        prev: set_class(class),
    }
}

/// Admission control for background IO.
///
/// `Debug` is required because [`crate::Options`] derives it and carries an
/// optional limiter — the injection point for tests and for embedders with
/// their own policy.
pub trait IoLimiter: Send + Sync + std::fmt::Debug {
    /// Block until `bytes` of `class` may proceed.
    ///
    /// Called *before* issuing known-size IO, so a job cancelled while waiting
    /// never consumes the bandwidth it was queued for. Implementations must
    /// return immediately for [`IoClass::Foreground`].
    fn charge(&self, class: IoClass, bytes: u64);

    /// Release every waiter and stop delaying, permanently.
    ///
    /// Called on close and on fail-stop: a waiter must never outlive the
    /// database that would have refilled its bucket. The default is a no-op,
    /// for limiters that never block.
    fn cancel(&self) {}
}

/// Largest single charge issued by [`charge`].
///
/// A charge bigger than the bucket's capacity could never be paid in one go,
/// and a multi-megabyte value charged as a single unit would also stall the
/// pacing loop past the point where cancellation is checked. Splitting bounds
/// both. 1 MiB is far above any data block and far below any plausible burst.
pub const MAX_CHARGE_CHUNK: u64 = 1 << 20;

/// Charge `bytes` against `limiter` under this thread's class, in chunks of at
/// most [`MAX_CHARGE_CHUNK`].
///
/// The `None` case — the default configuration — is one nil check.
#[inline]
pub fn charge(limiter: &Option<Arc<dyn IoLimiter>>, bytes: u64) {
    let Some(limiter) = limiter else {
        return;
    };
    let class = current();
    let mut left = bytes;
    loop {
        let chunk = left.min(MAX_CHARGE_CHUNK);
        limiter.charge(class, chunk);
        left -= chunk;
        if left == 0 {
            return;
        }
    }
}

/// The time source *and* the blocking primitive a [`TokenBucket`] uses.
///
/// Both live behind one trait because a bucket that reads an injectable clock
/// but sleeps on the real one is not testable: the test would have to spend the
/// wall-clock time it is trying to simulate. A fake clock answers `wait` by
/// advancing its own notion of now, so refill behaviour is asserted exactly and
/// instantly.
pub trait Clock: Send + Sync + std::fmt::Debug {
    /// Current instant.
    fn now(&self) -> Instant;
    /// Block for at most `dur`. May return early — spuriously, or because
    /// [`wake`](Self::wake) was called — so callers re-check their condition.
    fn wait(&self, dur: Duration);
    /// Release every thread blocked in [`wait`], now and in future. Terminal:
    /// once woken, a clock stays woken.
    fn wake(&self) {}
}

/// A single `wait` never blocks longer than this before the bucket re-checks
/// its cancellation flag. [`SystemClock::wake`] releases waiters promptly; this
/// bounds the residual race where a thread reads the flag and then parks.
const MAX_PARK: Duration = Duration::from_millis(50);

/// Real time, with an interruptible park.
#[derive(Debug)]
pub struct SystemClock {
    woken: Mutex<bool>,
    cv: Condvar,
}

impl Default for SystemClock {
    fn default() -> SystemClock {
        SystemClock {
            woken: Mutex::new(false),
            cv: Condvar::new(),
        }
    }
}

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn wait(&self, dur: Duration) {
        let mut woken = self.woken.lock();
        if *woken {
            return;
        }
        self.cv.wait_for(&mut woken, dur.min(MAX_PARK));
    }

    fn wake(&self) {
        *self.woken.lock() = true;
        self.cv.notify_all();
    }
}

/// Mutable bucket state, behind one mutex.
#[derive(Debug)]
struct BucketState {
    /// Bytes available to spend right now.
    tokens: u64,
    /// When `tokens` was last refilled.
    last: Instant,
}

/// Work-conserving token bucket: background IO proceeds at `bytes_per_second`,
/// may burst up to `burst_bytes` of credit accrued while idle, and never gets
/// ahead of the rate beyond that.
///
/// Charges are satisfied **greedily**: a caller spends whatever tokens are
/// present and waits only for the remainder. That makes progress independent of
/// how a charge compares to the bucket capacity — a value larger than the whole
/// burst still completes, it just takes proportionally longer — and keeps the
/// device busy rather than idle until a large charge can be paid in one piece.
#[derive(Debug)]
pub struct TokenBucket {
    /// Bytes per second; `0` means unlimited (every charge returns at once).
    rate: u64,
    /// Maximum accrued credit, in bytes.
    capacity: u64,
    state: Mutex<BucketState>,
    clock: Arc<dyn Clock>,
    /// Set by [`cancel`](IoLimiter::cancel); makes every charge free.
    cancelled: AtomicBool,
}

impl TokenBucket {
    /// A bucket on the real clock. A `burst_bytes` of `0` derives one second of
    /// rate, the smallest burst that lets a steady producer reach the
    /// configured rate at all.
    pub fn new(bytes_per_second: u64, burst_bytes: u64) -> TokenBucket {
        TokenBucket::with_clock(
            bytes_per_second,
            burst_bytes,
            Arc::new(SystemClock::default()),
        )
    }

    /// A bucket on `clock`. The clock is injectable so pacing can be asserted
    /// against simulated time instead of slept through.
    pub fn with_clock(
        bytes_per_second: u64,
        burst_bytes: u64,
        clock: Arc<dyn Clock>,
    ) -> TokenBucket {
        let capacity = if burst_bytes == 0 {
            bytes_per_second
        } else {
            burst_bytes
        };
        let now = clock.now();
        TokenBucket {
            rate: bytes_per_second,
            capacity,
            // Start full: a database that has just opened has not been using
            // the device, so its first background job may spend the burst.
            state: Mutex::new(BucketState {
                tokens: capacity,
                last: now,
            }),
            clock,
            cancelled: AtomicBool::new(false),
        }
    }

    /// Add the tokens `now` has earned since the last refill, capped at
    /// `capacity` — that cap is what bounds the burst.
    fn refill(&self, state: &mut BucketState, now: Instant) {
        let elapsed = now.saturating_duration_since(state.last);
        if elapsed.is_zero() {
            return;
        }
        state.last = now;
        // `u128` because rate * nanos overflows a u64 for any realistic rate
        // after a few seconds of idling.
        let earned = (self.rate as u128).saturating_mul(elapsed.as_nanos()) / 1_000_000_000u128;
        let earned = u64::try_from(earned).unwrap_or(u64::MAX);
        state.tokens = state.tokens.saturating_add(earned).min(self.capacity);
    }

    /// How long `deficit` bytes take to accrue at the configured rate.
    fn refill_time(&self, deficit: u64) -> Duration {
        debug_assert!(self.rate > 0);
        let nanos = (deficit as u128 * 1_000_000_000u128).div_ceil(self.rate as u128);
        Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
    }
}

impl IoLimiter for TokenBucket {
    fn charge(&self, class: IoClass, bytes: u64) {
        // Protecting foreground latency is the entire purpose; a foreground
        // read is never made to wait for background credit.
        if class == IoClass::Foreground || self.rate == 0 || bytes == 0 {
            return;
        }
        let mut left = bytes;
        while left > 0 {
            if self.cancelled.load(Ordering::Relaxed) {
                return;
            }
            let wait = {
                let mut state = self.state.lock();
                self.refill(&mut state, self.clock.now());
                let take = left.min(state.tokens);
                state.tokens -= take;
                left -= take;
                if left == 0 {
                    return;
                }
                // `tokens` is 0 here — `take` was the whole balance. Wait for
                // the smaller of what is still owed and a full bucket, so a
                // huge charge wakes repeatedly and stays cancellable.
                self.refill_time(left.min(self.capacity).max(1))
            };
            self.clock.wait(wait);
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
        self.clock.wake();
    }
}

/// An [`IoLimiter`] that records every charge and never waits.
///
/// Exists so the engine's IO accounting is observable: the tests use it to pin
/// which thread class each path runs under, and an embedder can use it to see
/// what a workload actually charges before choosing a rate. It never delays
/// anything, so it is safe to leave installed.
#[derive(Debug, Default)]
pub struct RecordingLimiter {
    charges: Mutex<Vec<(IoClass, u64)>>,
}

impl RecordingLimiter {
    /// Every charge so far, in order.
    pub fn charges(&self) -> Vec<(IoClass, u64)> {
        self.charges.lock().clone()
    }

    /// Total bytes charged under `class`.
    pub fn bytes_for(&self, class: IoClass) -> u64 {
        self.charges
            .lock()
            .iter()
            .filter(|(c, _)| *c == class)
            .map(|(_, b)| b)
            .sum()
    }

    /// Number of charges recorded under `class`.
    pub fn count_for(&self, class: IoClass) -> usize {
        self.charges
            .lock()
            .iter()
            .filter(|(c, _)| *c == class)
            .count()
    }

    /// Forget everything recorded so far.
    pub fn clear(&self) {
        self.charges.lock().clear();
    }
}

impl IoLimiter for RecordingLimiter {
    fn charge(&self, class: IoClass, bytes: u64) {
        self.charges.lock().push((class, bytes));
    }
}

/// Build the DB-wide limiter for a configuration, or `None` when background IO
/// is unlimited. `None` is the default: no allocation, no thread, one nil check
/// at each charge point.
pub(crate) fn limiter_for(
    bytes_per_second: u64,
    burst_bytes: u64,
    injected: Option<Arc<dyn IoLimiter>>,
) -> Option<Arc<dyn IoLimiter>> {
    if injected.is_some() {
        return injected;
    }
    if bytes_per_second == 0 {
        return None;
    }
    Some(Arc::new(TokenBucket::new(bytes_per_second, burst_bytes)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// A clock the test drives: `now` never moves on its own, and a wait
    /// advances it by exactly the requested amount instead of sleeping.
    #[derive(Debug)]
    struct FakeClock {
        base: Instant,
        elapsed_nanos: AtomicU64,
        waits: AtomicU64,
    }

    impl FakeClock {
        fn new() -> Arc<FakeClock> {
            Arc::new(FakeClock {
                base: Instant::now(),
                elapsed_nanos: AtomicU64::new(0),
                waits: AtomicU64::new(0),
            })
        }
        fn advance(&self, d: Duration) {
            self.elapsed_nanos
                .fetch_add(d.as_nanos() as u64, Ordering::SeqCst);
        }
        fn elapsed(&self) -> Duration {
            Duration::from_nanos(self.elapsed_nanos.load(Ordering::SeqCst))
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            self.base + self.elapsed()
        }
        fn wait(&self, dur: Duration) {
            self.waits.fetch_add(1, Ordering::SeqCst);
            self.advance(dur);
        }
    }

    #[test]
    fn default_class_is_foreground() {
        // A fresh thread must start unclassified; only an explicit set or a
        // scope guard makes IO background.
        let observed = std::thread::spawn(current).join().unwrap();
        assert_eq!(observed, IoClass::Foreground);
    }

    #[test]
    fn scoped_restores_previous_class() {
        assert_eq!(current(), IoClass::Foreground);
        {
            let _outer = scoped(IoClass::Compaction);
            assert_eq!(current(), IoClass::Compaction);
            {
                let _inner = scoped(IoClass::Flush);
                assert_eq!(current(), IoClass::Flush);
            }
            assert_eq!(current(), IoClass::Compaction);
        }
        assert_eq!(current(), IoClass::Foreground);

        // A panic inside the scope must still restore: the guard's `Drop` runs
        // while unwinding, and a leaked class would silently mislabel every
        // later operation on this thread.
        let _ = std::panic::catch_unwind(|| {
            let _g = scoped(IoClass::ObsoleteDelete);
            assert_eq!(current(), IoClass::ObsoleteDelete);
            panic!("unwind through the guard");
        });
        assert_eq!(current(), IoClass::Foreground);
    }

    #[test]
    fn bucket_refills_at_configured_rate() {
        let clock = FakeClock::new();
        let b = TokenBucket::with_clock(1000, 1000, clock.clone());
        // The bucket starts full, so the first 1000 bytes cost no time.
        b.charge(IoClass::Compaction, 1000);
        assert_eq!(clock.elapsed(), Duration::ZERO);
        // The next 1000 must wait exactly one second of refill.
        b.charge(IoClass::Compaction, 1000);
        assert_eq!(clock.elapsed(), Duration::from_secs(1));
    }

    #[test]
    fn bucket_is_work_conserving() {
        let clock = FakeClock::new();
        let b = TokenBucket::with_clock(1000, 2000, clock.clone());
        b.charge(IoClass::Compaction, 2000); // drains the initial burst
        assert_eq!(clock.elapsed(), Duration::ZERO);
        // Ten idle seconds accrue 10,000 bytes of credit, but the bucket caps
        // at `burst_bytes`: 2000 spendable, and not one byte more for free.
        clock.advance(Duration::from_secs(10));
        let idle_mark = clock.elapsed();
        b.charge(IoClass::Compaction, 2000);
        assert_eq!(clock.elapsed(), idle_mark, "accrued burst must be free");
        b.charge(IoClass::Compaction, 500);
        assert_eq!(
            clock.elapsed() - idle_mark,
            Duration::from_millis(500),
            "past the burst the rate applies again"
        );
    }

    #[test]
    fn foreground_never_waits() {
        let clock = FakeClock::new();
        let b = TokenBucket::with_clock(1000, 1000, clock.clone());
        b.charge(IoClass::Compaction, 1000); // exhaust
        let mark = clock.elapsed();
        b.charge(IoClass::Foreground, 1 << 30);
        assert_eq!(clock.elapsed(), mark, "foreground must not wait");
        assert_eq!(clock.waits.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn zero_rate_is_unlimited() {
        let clock = FakeClock::new();
        let b = TokenBucket::with_clock(0, 0, clock.clone());
        b.charge(IoClass::Compaction, 1 << 40);
        assert_eq!(clock.elapsed(), Duration::ZERO);
    }

    #[test]
    fn cancel_wakes_a_blocked_waiter() {
        // A real clock, so the waiter genuinely blocks; `cancel` (close or
        // poison) must release it rather than let it sit out the refill.
        let b = Arc::new(TokenBucket::new(1, 1));
        b.charge(IoClass::Compaction, 1); // drain; the next byte needs a second
        let waiter = {
            let b = b.clone();
            std::thread::spawn(move || {
                let t0 = Instant::now();
                b.charge(IoClass::Compaction, 10_000);
                t0.elapsed()
            })
        };
        std::thread::sleep(Duration::from_millis(30));
        b.cancel();
        let waited = waiter.join().unwrap();
        assert!(
            waited < Duration::from_secs(5),
            "cancel must wake the waiter, waited {waited:?}"
        );
    }

    #[test]
    fn charges_are_split_into_bounded_chunks() {
        let rec = Arc::new(RecordingLimiter::default());
        let limiter: Option<Arc<dyn IoLimiter>> = Some(rec.clone());
        charge(&limiter, MAX_CHARGE_CHUNK * 3 + 7);
        let calls = rec.charges();
        assert_eq!(calls.len(), 4);
        assert!(calls.iter().all(|(_, b)| *b <= MAX_CHARGE_CHUNK));
        assert_eq!(
            calls.iter().map(|(_, b)| b).sum::<u64>(),
            MAX_CHARGE_CHUNK * 3 + 7
        );
    }
}
