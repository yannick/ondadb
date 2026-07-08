# ondaDB read/write gap closure — implementation plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the benchmarked cold-Get gap vs RocksDB (0.83× at 16B keys → 0.50× at 2KiB keys) and the Put gap vs TidesDB (0.5× at 512B values → 0.35× at 1–4KiB) with eight targeted changes to the get and put pipelines.

**Architecture:** Get side: make per-get key work O(short) instead of O(key len) — shortened index separators (bytewise-gated), one fast key hash instead of three slow ones, restart-point binary search inside data blocks, and allocation/refcount shaves. Put side: remove the two realloc-growth payload copies (exact-size WAL frame, pooled txn buffer), make the WAL append lock-free (atomic offset + `pwrite`), and raise flush parallelism.

**Tech stack:** Rust, existing deps (`xxhash-rust`, `parking_lot`, `memmap2`, `crc32fast`) + `smallvec` (new, tiny). Repo: `/Volumes/HOME/code/storage-engines/ondadb`, branch `perf/read-write-gaps` off current main.

**Verified baseline (Apple M2 Ultra, remote, mean of 2 runs, `results_matrix.csv`):**
Get: 16B/100B onda 2.70M vs rocks 3.25M; 1KiB/256B 983k vs 1.55M; 2KiB/256B 544k vs 1.09M.
Put: 16B/100B onda 6.61M vs tides 6.15M; 64B/512B 2.62M vs 5.24M; 16B/4KiB 402k vs 1.08M; 2KiB/256B 723k vs 2.12M.

**Test placement convention (applies to every task):** white-box tests that touch `pub(crate)` items (`get_unfiltered`, `shortest_separator`, `reader.index`, footer flags, txn buffer capacity) go in `#[cfg(test)] mod tests` blocks INSIDE the respective `src/` module — `tests/*.rs` integration tests compile as separate crates and only see the public API. Behavior-level tests (scans, gets vs oracle) may stay in `tests/sst.rs`.

**Compatibility invariants (hold for every task):**
- Old SSTables/WALs must stay readable (new footer/encoding bits are additive; absent bit ⇒ legacy path).
- Non-bytewise comparators must behave exactly as today (shortening gated on `Comparator::is_bytewise()`, `src/comparator.rs:25`).
- `cargo test --release` and `cargo test --release --features unsafe-fastpath` green after every task.

---

## Task 1: Bloom — single fast hash, tagged encoding

**Files:** Modify `src/bloom.rs`; callers in `src/sst/reader.rs`, `src/column_family.rs` adjusted in Task 2.

The filter currently hashes with byte-at-a-time FNV-1a (`src/bloom.rs:36-45`) and is consulted twice per get (`column_family.rs:722` + `sst/reader.rs:366`). Move to xxh3 for new filters, keep FNV for legacy ones, and expose a hash-once API.

- [ ] **Step 1: failing tests** in `src/bloom.rs` `mod tests`:
  - `xxh3_round_trip`: build with `Bloom::new` (which now uses xxh3), encode, decode, `may_contain` all inserted keys.
  - `legacy_decode_uses_fnv`: hand-encode a filter with the OLD format (no trailing hash tag — reuse `encode()` output from a `Bloom { hash: HashKind::Fnv, .. }` constructed directly), decode, verify keys inserted under FNV are still found (no false negatives).
  - `hash_once_api`: `let h = b.hash_of(key); assert_eq!(b.may_contain_hash(h), b.may_contain(key));`
- [ ] **Step 2: implement.**
  - Add `enum HashKind { Fnv, Xxh3 }` field. `hash_of(&self, key) -> u64` dispatches (`xxhash_rust::xxh3::xxh3_64` for new). `positions` takes the precomputed `h` (same double-hashing split h1/h2). `add`/`may_contain` call `hash_of` internally; add `add_hash`/`may_contain_hash`.
  - Encoding: `encode()` appends one trailing byte `hash_id` (0=Fnv, 1=Xxh3) AFTER the words. `decode()`: if bytes remain past `words*8`, read the tag; else `Fnv` (legacy). Same for `encode_sparse`/`decode_sparse` (tag after the last pair).
  - `Bloom::new` constructs `Xxh3`.
- [ ] **Step 3:** UPDATE the existing `decode_rejects_truncation` test (`src/bloom.rs:244-248`): truncating exactly 1 byte now strips the hash tag and yields a valid legacy filter — truncate into the words region instead (e.g. `enc.len() - 9`) and additionally assert that a 1-byte truncation decodes as `HashKind::Fnv`. Then `cargo test --release bloom` → PASS.
- [ ] **Step 4:** commit `perf(bloom): xxh3 hashing with tagged encoding + hash-once API`.

## Task 2: Get plumbing — hash once, check bloom once, skip empty memtable

**Files:** Modify `src/sst/reader.rs:360-371`, `src/column_family.rs:667-745`, `src/memtable.rs:358`.

- [ ] **Step 1: failing test** in `tests/sst.rs`: `get_after_bloom_equivalent` — build a small SSTable, assert `reader.get(..)` and the new no-bloom entry point return identical results for present and absent keys.
- [ ] **Step 2: implement.**
  - `Reader::get` keeps its signature but its bloom check moves to a thin wrapper: add `pub(crate) fn get_unfiltered(&self, user_key, read_seq, now)` containing today's body minus the `self.bloom` check (`reader.rs:366-370`); `get` = bloom check + `get_unfiltered`.
  - `Reader::bloom_may_contain_hash(&self, h: u64) -> bool` and `bloom_hash(&self, key) -> Option<u64>` passthroughs.
  - `ColumnFamily::get` (`column_family.rs:719-726`): per candidate table compute `th.reader.bloom_hash(key)` once, check `bloom_may_contain_hash`, then call `get_unfiltered`. (Per-table hash because legacy tables may be FNV; in steady state all tables are xxh3.) Same in `peek_seq`'s table loop.
  - `Memtable::get` (`memtable.rs:358`): first line `if self.num_entries.load(Relaxed) == 0 { return Lookup::default(); }` (verify `Lookup` has a "not found" default; otherwise construct explicitly) — skips the xxh3 shard hash on empty memtables (the entire cold-get phase). Do the same in the arena memtable if it has a separate `get` (`memtable_arena.rs`).
- [ ] **Step 3:** `cargo test --release --features unsafe-fastpath` → PASS.
- [ ] **Step 4:** commit `perf(get): hash key once per table, drop duplicate bloom check, skip empty memtable`.

## Task 3: Shortened index separators (bytewise only)

**Files:** Modify `src/sst/writer.rs` (`add`, `flush_block`, `finish`); test in `tests/sst.rs`.

Today `flush_block` (`writer.rs:195-218`) pushes the block's full last key as the index separator. Instead, defer the index push until the next key is known and store the shortest separator: `last_key <= sep < next_first_key`. The final block keeps the FULL last key (Reader derives `max_key` from `index.last()`, `reader.rs:186-190`).

Correctness argument (why get/seek still work): `find_block` (`reader.rs:339`) returns the first block whose separator ≥ target. For a target in the gap `(last_key_i, sep_i]` the key cannot exist (it's < first_key_{i+1} because sep_i < first_key_{i+1}), so block *i*'s in-block miss correctly reports absent, and `SstIterator::seek`'s in-block miss already rolls into block *i+1* (`iter.rs:179-188`). When shortening is impossible the separator equals the last key — exactly today's behavior. Non-bytewise comparators always take the unshortened path.

- [ ] **Step 1: failing tests** in `tests/sst.rs`:
  - `separator_properties`: unit-test `shortest_separator(a, b)` directly (make it `pub(crate)`): for pairs incl. (`"abcXYZ"`, `"abd000"`) → `"abcY"`-class result; (`"abc"`, `"abcd"`) → `"abc"` (prefix case, unshortened); (`[0xff,0xff]`, …) fallback; property loop over random pairs asserting `a <= sep && sep < b`, or `sep == a` when returned unshortened.
  - `large_key_index_shrinks`: write an SSTable with 2KiB keys / 100B values (≥100 blocks); assert the sum of `reader.index` key lengths is far below `num_blocks * 2048` (e.g. < 10%).
  - `get_and_scan_with_shortened_index`: 10k random 2KiB keys through Writer → Reader; every key found via `get`; forward and backward full scans return all entries in order; `seek` to 100 random present and 100 absent keys lands correctly (compare against a BTreeMap oracle).
- [ ] **Step 2: implement** in `writer.rs`:
  ```rust
  /// Shortest bytewise separator s with a <= s < b (a < b). Returns a.to_vec()
  /// when no shorter separator exists (a is a prefix of b, or increment overflows).
  pub(crate) fn shortest_separator(a: &[u8], b: &[u8]) -> Vec<u8> {
      let n = a.len().min(b.len());
      let mut i = 0;
      while i < n && a[i] == b[i] { i += 1; }
      if i >= n { return a.to_vec(); }           // a is a prefix of b
      if a[i] < 0xff && a[i] + 1 < b[i] {
          let mut s = a[..=i].to_vec();
          s[i] += 1;                              // a < s < b, len i+1
          return s;
      }
      // a[i]+1 == b[i]: s = a[..=i] with byte+1 equals b[..=i]; s < b only if b
      // extends past i (checked); s > a because s[i] > a[i].
      if a[i] < 0xff && a[i] + 1 == b[i] && b.len() > i + 1 {
          let mut s = a[..=i].to_vec();
          s[i] += 1;
          return s;
      }
      a.to_vec()
  }
  ```
  Writer: add field `pending_index: Option<(Vec<u8> /*last_key*/, u64 /*last_seq*/, BlockHandle)>`. `flush_block` sets it instead of pushing. At the top of `add()`: if `pending_index` is `Some` — `let sep = if self.opts.cmp.is_bytewise() { shortest_separator(&last_key, user_key) } else { last_key.clone() };` push `IndexEntry { user_key: sep, seq: if sep == last_key { last_seq } else { 0 }, handle }` (seq 0 is inert: `cmp_internal` only consults seq on exact key equality, and a strictly-greater separator never equals a stored key). In `finish()`: flush pending with the full last key + `last_seq`.
  - `seq: 0` + `cmp_internal` note: for a synthetic separator equal to the *target* key, `find_block` selects block *i* and the in-block scan reports absent — correct, since a key strictly between blocks cannot exist.
- [ ] **Step 3:** full `cargo test --release --features unsafe-fastpath` (compaction and iterator suites exercise the new index shape) → PASS.
- [ ] **Step 4:** commit `perf(sst): shortest-separator index keys (bytewise), full key kept for final block`.

## Task 4: Restart offsets + in-block binary search

**Files:** Modify `src/sst/mod.rs` (footer flag), `src/sst/writer.rs` (`flush_block`), `src/sst/reader.rs` (`get_unfiltered`, block-region helper), `src/sst/iter.rs` (entry bounds).

Additive data-block trailer, gated by a new footer flag so old files use the old path:
`[entries..... | restart_off u32 LE × R | R u32 LE]` where restart offsets mark every 8th entry's start (entry 0 always included). No prefix compression (backward iteration and entry format unchanged).

- [ ] **Step 1: failing tests** in `tests/sst.rs`:
  - `restart_trailer_roundtrip`: build a table, assert footer flag set, `get` finds every key, absent probes return absent.
  - `iter_ignores_trailer`: full forward + backward scans and seeks return exactly the inserted entries (bounds must exclude the trailer).
  - `legacy_block_without_flag_still_reads`: construct a Writer variant with the flag suppressed (test hook or feature off switch) OR keep a checked-in tiny legacy fixture; simplest: write with `restart_interval = 0` (writer omits trailer + flag) and assert reads work — also gives an escape hatch.
- [ ] **Step 2: implement.**
  - `sst/mod.rs`: `pub(crate) const FOOTER_RESTARTS: u8 = 0x04;` `const RESTART_INTERVAL: usize = 8;`
  - `WriterOptions` gains `restart_interval: usize` (0 = no trailer, the legacy/escape-hatch path). ALL struct-literal constructors must be updated: `tests/sst.rs:11`, `src/compaction.rs:307-321` (`cf_writer_opts`), `src/column_family.rs:626-637` (`writer_opts`).
  - Writer: track `entry_starts: Vec<u32>` per block (push `cur_block.len()` before each `encode_entry` when `entries_in_block % RESTART_INTERVAL == 0`). In `flush_block`, before framing: append each restart u32 LE then the count u32 LE to `cur_block`. Set `FOOTER_RESTARTS` in `finish`'s footer flags.
  - Reader: store `has_restarts: bool` from footer. Helper on the block bytes:
    ```rust
    /// (entries_region, restarts) — restarts empty for legacy blocks.
    fn split_block<'a>(raw: &'a [u8], has_restarts: bool) -> Result<(&'a [u8], &'a [u8])>
    ```
    parsing count from the last 4 bytes with bounds checks (corrupt() on inconsistency).
  - `get_unfiltered`: with restarts, binary-search the restart array (decode the entry at each probed restart offset, `cmp_internal` on its key) for the last restart whose key ≤ target, then linear-scan ≤ 8 entries from there within the entries region. Without restarts: today's loop, bounded by entries region.
  - `iter.rs`: everywhere `bytes.len()` / `raw.len()` bounds the entry walk (`load_block` full decode `iter.rs:84`, `next()`'s `cur_next >= raw_len` `iter.rs:220-221`), use the entries-region length instead (thread `entries_len` through — simplest: `load_block` computes and stores `self.entries_len` once per block via `Reader::split_block`).
- [ ] **Step 3:** full test suite both feature sets → PASS. Run `tests/fjall_suite.rs`, `tests/surrealkv_suite.rs` (iterator-heavy) specifically.
- [ ] **Step 4:** commit `perf(sst): restart-offset trailer + in-block binary search (footer-flag gated)`.

## Task 5: Get micro — no per-get mmap Arc bump, SmallVec candidates

**Files:** Modify `src/sst/reader.rs`, `src/column_family.rs:672`, `Cargo.toml` (+`smallvec = "1"`).

- [ ] **Step 1:** add `read_data_block_local(&self, i) -> Result<BlockRef<'_>>` where `enum BlockRef<'a> { Owned(Arc<[u8]>), Mapped(&'a [u8]) }` — the `Mapped` arm borrows `self.klog_mmap` (no `Arc` clone; the reader outlives the call) and is `#[cfg(feature = "unsafe-fastpath")]`-gated exactly like `read_data_block`'s mmap branch. Use it from `get_unfiltered` only (the iterator keeps owning `Block`).
- [ ] **Step 2:** `ColumnFamily::get`/`peek_seq`: `let mut tables: SmallVec<[Arc<SstHandle>; 4]> = SmallVec::new();` (typical candidate count ≤ 2).
- [ ] **Step 3:** tests green (behavioral no-op), `cargo clippy --features unsafe-fastpath` clean.
- [ ] **Step 4:** commit `perf(get): borrow mmap block in point reads, SmallVec table candidates`.

## Task 6: WAL — exact frame size + lock-free positional append

**Files:** Modify `src/wal.rs`; tests in `src/wal.rs` `mod tests` (check existing ones at file bottom).

- [ ] **Step 1: failing tests**:
  - `frame_presized_exactly`: expose `encoded_frame_len(recs) -> usize` and assert `append_batch`'s buffer never reallocates (assert `buf.capacity() == exact` after encode, via a `#[cfg(test)]` hook or by unit-testing the length fn against `encode_record_body` output).
  - `concurrent_append_replay_complete`: 8 threads × 500 batches of 10 records each through one `Wal` (SyncMode::None), then `Wal::replay` and count 40,000 records with all seqs present.
- [ ] **Step 2: implement.**
  - Exact pre-size (`wal.rs:277`): `let body: usize = recs.iter().map(|r| 1 + uvarint_len(r.key.len()) + uvarint_len(r.value.len()) + uvarint_len(r.seq) + ttl_len(r) + r.key.len() + r.value.len()).sum(); Vec::with_capacity(HEADER_SIZE + body)`. Add tiny `uvarint_len(u64) -> usize` in `src/encoding.rs`.
  - Lock-free append: replace `files: Vec<Mutex<Option<File>>>` with `struct Stripe { file: parking_lot::RwLock<Option<File>>, off: AtomicU64 }`. Open files WITHOUT `.append(true)` (pwrite on an O_APPEND fd ignores the offset on Linux); initialize `off` from `metadata().len()`. Non-Full append path (`wal.rs:291-307`): `let g = stripe.file.read(); let f = g.as_ref().ok_or(closed)?; let pos = stripe.off.fetch_add(buf.len() as u64, Ordering::Relaxed); f.write_all_at(&buf, pos)?;` (`std::os::unix::fs::FileExt::write_all_at` takes `&File` — concurrent callers proceed under the shared read lock). `Full`-mode group commit: same `write_all_at` + offset reservation per request, then one `sync_data` (keep fdatasync as today, `wal.rs:363` — not `sync_all`); `interval_sync`/`sync`/`close` take the write lock to sync/take the file. `Wal::open` must add `.write(true)` when `.append(true)` is dropped. Document the crash-window trade: a crash between offset reservation and write leaves a zeroed hole; replay decodes a zeroed region as empty frames (plen=0, crc-of-empty=0) and may continue past it — content is still CRC-guarded, but under None/Interval a hole no longer implies "end of log" (acceptable: no durability contract there; `Full` still fdatasyncs before acking, contract unchanged).
  - Keep `WAL_STRIPES = 4` (replay `wal.rs:425-435` and `remove_wal_files` iterate it; the win is the lock removal, not more files).
- [ ] **Step 3:** `cargo test --release wal` and full suite → PASS.
- [ ] **Step 4:** commit `perf(wal): exact frame pre-size; lock-free positional appends`.

## Task 7: Txn buffer pool

**Files:** Modify `src/txn.rs` (`begin_with_isolation:124`, `release`, `Drop`).

- [ ] **Step 1: failing test** in `tests/db.rs`: `txn_buf_reused` — needs observability; simplest honest test: commit two identical 1000-op batches on one thread and assert the second `begin`'s buffer arrives with non-zero capacity (expose `#[cfg(test)] pub(crate) fn buf_capacity(&self)`).
- [ ] **Step 2: implement.** Thread-local pool:
  ```rust
  thread_local! {
      static BUF_POOL: RefCell<Vec<Vec<u8>>> = const { RefCell::new(Vec::new()) };
  }
  fn take_buf() -> Vec<u8> { BUF_POOL.with(|p| p.borrow_mut().pop()).map(|mut b| { b.clear(); b }).unwrap_or_default() }
  fn put_buf(buf: Vec<u8>) { if buf.capacity() > 0 && buf.capacity() <= 32 << 20 { BUF_POOL.with(|p| { let mut g = p.borrow_mut(); if g.len() < 4 { g.push(buf); } }) } }
  ```
  `begin_with_isolation`: `buf: take_buf()`. Return the buffer in `release()` is wrong (commit still borrows it) — return it at the *end* of `commit`, in `rollback`, and in `Drop` (`std::mem::take(&mut self.buf)` then `put_buf`). Cap prevents a giant one-off batch from pinning memory per thread.
- [ ] **Step 3:** tests → PASS (run `read_your_writes.rs` suite: savepoint truncation interacts with `buf`, must stay correct — truncate semantics unchanged since pool only swaps the allocation).
- [ ] **Step 4:** commit `perf(txn): thread-local write-buffer pool (no cold-growth copies per batch)`.

## Task 8: Flush parallelism default

**Files:** Modify `src/config.rs:174` (`num_flush_threads: 2` → `4`), update the default-assert test (`config.rs:423`).

- [ ] Bump, adjust test, full suite, commit `perf(flush): default flush threads 2 -> 4`.

## Task 9: Verify + benchmark

- [ ] `cargo test --release` and `cargo test --release --features unsafe-fastpath` → all green; `cargo clippy --features unsafe-fastpath` no new warnings.
- [ ] Local A/B (same machine, back-to-back): `cargo build --release --features unsafe-fastpath --bin onda_bench`, run configs `16/100@1M`, `16/4096@30k`, `2048/256@100k` on the new binary; compare against `git stash`-built old binary or the checked-in remote numbers directionally. Success bar: Put ≥ +50% at 4KiB values; Get ≥ +30% at 2KiB keys; **no regression >5% at 16/100** (scans especially — Task 4 touches the iterator).
- [ ] Full suite via bench harness: `cd ../bench && ./make_dist.sh`, scp + extract on `alphons@192.168.98.31`, rerun `bench_matrix.sh` there (nohup, as before), fetch `results_matrix.csv`, regenerate `bench_matrix.html` + `summarize_matrix.py` locally. Compare scoreboard vs the pre-change remote run (kept at `results_matrix.csv` current state — snapshot it to `results_matrix_m2ultra_before.csv` before overwriting).
- [ ] Write up per-config before/after in the final report.

## Explicit non-goals (YAGNI)
- No prefix compression of data-block keys (backward iteration complexity; separators + restarts capture most of the win).
- No index-in-block-cache restructuring (separator shortening removes the memory blow-up that motivated it).
- No WAL stripe-count change, no bench-driver changes (methodology stays comparable across engines).
