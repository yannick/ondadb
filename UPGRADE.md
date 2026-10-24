# Upgrading ondaDB

This guide covers moving an application and its databases from ondaDB
**0.9.x** to **0.20.x**, the yoloDB parity line. It is about what you have to
do; `CHANGELOG.md` has the full list of what changed and why.

| From | To | On-disk format | What you do |
|---|---|---|---|
| 0.9.x | 0.20.x | 0.9 → yoloDB epoch 1 (breaking) | Plan the one-time upgrade below; fix the API breaks |
| 0.10.x | 0.20.x | unchanged, plus one capability bit taken at open | Read [Rollback](#rollback-and-mixed-versions) before you deploy |
| 0.20.x | 0.20.y | unchanged | Nothing |

## In short

1. A 0.9.x database is **rebuilt into the new format the first time 0.20
   opens it read-write**. It is verified first, and the old directory is kept
   as a backup. Budget the free space and time it needs.
2. After that first open, **no 0.9.x binary can read the database**. Keep the
   backup until you are sure you will not roll back.
3. A database with prepared transactions takes `CAP_WAL_FLOOR` at its first
   0.20 read-write open, after which **0.10.x binaries refuse it too**.
4. A handful of public APIs changed shape; the compiler finds all of them.
5. ondaDB 0.20 and wavesdb 0.20 share format epoch 1; which directories each
   opens read-write is in [wavesdb interoperability](#wavesdb-interoperability).

## 1. The format change (0.9.x → epoch 1)

0.10.0 moved ondaDB onto **yoloDB format epoch 1**, the format ondaDB and
wavesdb share. Every magic, checksum and header changed:

- CRC32-C everywhere. 0.9 documented CRC32-C but computed CRC-32/IEEE.
- New magics: `YOLOST01`, `YOLODBMF`, `YOLODBED`, `YOLODBWL`, `YOLODBVL`,
  `YOLODBCF`.
- A checksummed SST footer, headers on WAL segments and value logs, and a TLV
  config blob.

Nothing in a 0.9 directory can be read in place. `docs/formats.md` is the
byte-level reference.

### What happens on first open

With the default `Options::format_upgrade = FormatUpgrade::Auto`,
`DB::open` on a 0.9 directory:

1. takes the source's `LOCK` exclusively and **never writes to the source**;
2. rebuilds it in a sibling directory, `.<name>.yolo-upgrade-<nonce>/`.
   Every table is transcoded one-for-one (same id, level, partition and age
   stamps; tombstones, merge operands, TTLs and range deletes copied as they
   are), and the WAL tail becomes L0 tables;
3. verifies the rebuild against the source (`FormatUpgradeVerify::Scan` by
   default: entry counts, maximum sequence and a streaming compare);
4. swaps it into place with two renames under a durable journal, keeping the
   old directory as `.<name>.pre-yolo-<nonce>/`;
5. opens the result normally. `DB::last_format_upgrade()` returns an
   `UpgradeReport` (bytes, duration, backup path, counts).

A crash at any step is finished or rolled back by the next open. Any failure
before the swap leaves the 0.9 directory byte-identical.

### Before you deploy

- **Free space.** The rebuild needs roughly the database's size again, next
  to it, on the same filesystem. Too little is `Io(StorageFull)`, raised
  before anything is written.
- **Time.** The rebuild reads and writes every byte, and the default verify
  reads both copies again. On a large database, run it offline:

  ```sh
  yolodb upgrade /var/lib/app/db               # same protocol, with progress
  yolodb upgrade /var/lib/app/db --verify counts   # faster, weaker verify
  ```

  `yolodb` needs none of your merge operators, because operands are copied,
  never folded.
- **Directory layout.** The swap renames the database directory inside its
  parent, so it refuses a directory that is a **mount point** or a
  **symlink**. If your container mounts a volume *at* the database path, move
  the database into a subdirectory first (for example `<volume>/db`) and
  point the application there.
- **Refused cases** (`OndaError::FormatUpgradeUnsupported`, nothing
  written):
  - tables on a named tier or an object store. Demote them to the default
    tier with 0.9 first;
  - unresolved prepared transactions. Commit or abort them with 0.9 first.

### Choosing the behaviour

| `Options::format_upgrade` | Effect on a 0.9 directory |
|---|---|
| `Auto` (default) | Upgrade as above, then open |
| `Forbid` | `UnsupportedFormat`, nothing written. Use it where an unattended upgrade is not acceptable |
| `ReadOnlyLegacy` | Open it read-only as it is, through the frozen 0.9 decoders |

A read-only open never upgrades. `Options::format_upgrade_keep_backup = false`
deletes the backup once the upgraded database has opened. Leave it `true`
until you have stopped needing a rollback.

The 0.9 decoders live behind the default-on cargo feature `legacy-onda`. If
you build with `default-features = false`, a 0.9 directory is a hard
`UnsupportedFormat`, with no upgrade and no read-only open. Keep the feature on
until every database has been upgraded.

## 2. API changes

All of these are compile errors or `Result`s the compiler makes you handle.
None changes behaviour silently.

| Was (0.9.x) | Now | Since |
|---|---|---|
| `Compression::from_u8(u8) -> Option<_>` | `Compression::from_codec_id(u8) -> Result<_>` and `Compression::codec_id()`. LZ4 is stored as id **6**; ids 2 and 4 are refused | 0.10.0 |
| `ColumnFamilyConfig::decode(&[u8]) -> ColumnFamilyConfig` | returns `Result<ColumnFamilyConfig>` (strict decode) | 0.10.0 |
| `Wal::open` / `Wal::replay` by generation number | take a `wal::SegmentId` | 0.10.0 |
| — | new error variant `OndaError::FormatUpgradeUnsupported` (code `-17`). An exhaustive `match` on `OndaError` needs an arm | 0.10.0 |
| a family could name only a built-in comparator | `Options::comparators` registers custom ones by name. A name that is neither built-in nor registered refuses the open (`InvalidArgs`), before anything is written | 0.10.2 |
| `flush_memtable` under the unified layout flushed nothing | it seals the shared memtable and waits for its flush | 0.20.0 |
| `close` on a read-only handle with unresolved prepares returned `Busy` | it closes; the prepares stay on disk for the next writer | 0.20.0 |

A family with a custom comparator must register it in `Options::comparators`
under the same name on **every** open, as with `Options::merge_fns`.

## 3. Behaviour changes worth checking

- **Comparators (0.10.1).** `case_insensitive` now breaks ties on the raw
  bytes, so `"K10"` and `"k10"` are two adjacent keys, no longer one.
  `int64` is now a total order when key lengths are mixed. The byte format did
  not change; only data that was already misordered is affected:
  - a `case_insensitive` family holding two spellings of one key;
  - an `int64` family mixing negative 8-byte keys with keys of other lengths.

  If you have either, scan the family into a fresh one.
- **Clear under the unified layout (0.10.0)** now works (it was refused). The
  cleared family takes a fresh unified id. It is refused with `Busy` while a
  prepared transaction names the family.
- **Dropping or clearing a family** now waits for, and fences off, its
  in-flight rotation, flush and compaction (0.10.1). `flush_memtable` through
  a dropped family's handle returns `NotFound`.

## 4. Capabilities: one-way switches

Optional format features are capability bits in the manifest
(`AGENTS.md` § Optional format capabilities). Each is **opt-in and one-way**:
once a database has a bit, a binary that does not implement it refuses the
database as `UnsupportedFormat`. The upgrade itself takes no new bits beyond
what the 0.9 database already had, with one exception, below.

**`CAP_WAL_FLOOR` (bit 11, 0.20.0).** The unified WAL's replay floor. It
fixes data that a prepare-pinned WAL generation brought back after
compaction had deleted it. A database with `CAP_TXN_DECISIONS`, i.e. one that
has used prepared transactions, takes this bit **automatically at its first
read-write open by 0.20**. Until then it replays as before and logs a warning
if it recovers a prepare from an older generation. After that, 0.10.x
binaries refuse the database. A database that never used prepared
transactions is not affected.

## Rollback and mixed versions

- **Back to 0.9.x after an upgrade.** Stop the application, move the upgraded
  directory aside, rename `.<name>.pre-yolo-<nonce>/` back to `<name>`, and
  start the 0.9 binary. Writes made after the upgrade are not in the backup.
  Without the backup (`format_upgrade_keep_backup = false` or
  `--no-backup`), there is no way back: 0.9 cannot read epoch 1.
- **Back to 0.10.x from 0.20.** Possible only for a database that has not
  taken `CAP_WAL_FLOOR`, i.e. one that never used prepared transactions.
- **Never run two versions against one directory at once.** The `LOCK` file
  prevents it within one host; do not share a directory across hosts.
- **Rolling deploys.** Upgrade the binary everywhere before you rely on a
  0.20-only capability. Where an unattended upgrade is not acceptable, deploy
  with `FormatUpgrade::Forbid`, then run `yolodb upgrade` per database in a
  maintenance window.

## wavesdb interoperability

From 0.20.0, ondaDB and wavesdb 0.20.0 write the same epoch-1 format and agree
on every cell of the `yolodb-format` cross-engine harness, handoffs included:

- an ondaDB directory opens read-write in wavesdb 0.20;
- a wavesdb 0.20 database created with `FormatCompat` (epoch 1 as specified,
  ondaDB's bodies) opens read-write in ondaDB;
- a wavesdb 0.20 database in its default format (epoch-1 containers with
  wavesdb's own bodies, `CAP_WAVES08_BODIES`) opens in ondaDB **read-only
  only**, via `Options::read_only` (decoders behind the default-on feature
  `waves08`). A read-write open refuses it as `UnsupportedFormat` naming
  wavesdb;
- a wavesdb directory still in the legacy 0.8 containers does not open in
  ondaDB at all. Let wavesdb 0.20 convert it first;
- prepared transactions resolve across engines: either can commit or abort a
  prepare the other wrote.

## Fixes after 0.20.0

- **Named and S3 tiers no longer leak retired tables.** Compaction inputs,
  FIFO evictions and the tables of a dropped or cleared family on a named
  tier used to be deleted at their default-tier path, which missed. On S3 the
  objects stayed for good. The startup orphan sweep now also lists S3 tiers.
  Objects already leaked before the fix are not reclaimed automatically; see
  `docs/parts-and-tiers.md` § Crash residue and table retirement for the
  bucket audit.
