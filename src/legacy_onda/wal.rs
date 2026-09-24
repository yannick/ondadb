//! 0.9.x WAL replay.
//!
//! A 0.9 WAL file has **no header**: it is frames from byte 0,
//! `[payload_len u32 LE][crc32-ieee(payload) u32 LE][payload]`, one per
//! committed batch, in up to four stripe files (`<base>`, `<base>.s1..s3`).
//! The payload forms — the flags-byte record stream and the `0xFF` envelope,
//! schema 1 (per-CF) or 2 (unified) — are the ones epoch 1 still writes, so
//! they are decoded by the shared [`crate::wal`] payload decoder; only the
//! container differs.
//!
//! Tail contract, unchanged from 0.9: a short header, a short payload or a CRC
//! mismatch ends a stripe cleanly (crash residue); a record that fails to decode
//! inside a CRC-valid frame is `Corruption`. A zero-length file — a generation
//! created but never written — replays as empty.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::Result;
use crate::wal::ReplayRecord;

/// Stripes 0.9 could write per generation.
const STRIPES: usize = 4;

fn stripe_path(base: &Path, k: usize) -> std::path::PathBuf {
    if k == 0 {
        base.to_path_buf()
    } else {
        std::path::PathBuf::from(format!("{}.s{k}", base.display()))
    }
}

/// Replay every stripe of the 0.9 WAL generation based at `base`, invoking `f`
/// per record. Returns the highest sequence seen; missing files are empty.
pub fn replay<F>(base: impl AsRef<Path>, mut f: F) -> Result<u64>
where
    F: FnMut(ReplayRecord) -> Result<()>,
{
    let mut last = 0u64;
    for k in 0..STRIPES {
        let file = match File::open(stripe_path(base.as_ref(), k)) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        let mut r = BufReader::with_capacity(64 << 10, file);
        let seq = crate::wal::replay_frames(&mut r, super::checksum_ieee, &mut f)?;
        last = last.max(seq);
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::Record;

    /// Copy a WAL fixture under a stripe-0 name and replay it.
    fn replay_fixture(name: &str) -> (tempfile::TempDir, Result<(Vec<Record>, u64)>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal-0.log");
        std::fs::copy(crate::util::legacy_fixture(name), &path).unwrap();
        let mut out = Vec::new();
        let res = replay(&path, |rec| {
            match rec {
                ReplayRecord::Point(r) => out.push(r),
                other => panic!("unexpected {other:?}"),
            }
            Ok(())
        })
        .map(|seq| (std::mem::take(&mut out), seq));
        (dir, res)
    }

    #[test]
    fn torn_payload_stops_replay_cleanly() {
        let (_d, res) = replay_fixture("wal_legacy_torn_tail.bin");
        let (recs, seq) = res.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key, b"good");
        assert_eq!(seq, 1);
    }

    #[test]
    fn crc_valid_undecodable_record_is_corruption() {
        let (_d, res) = replay_fixture("wal_legacy_crc_valid_undecodable.bin");
        assert_eq!(res.unwrap_err().kind(), "corruption");
    }

    #[test]
    fn empty_frame_is_skipped_and_replay_continues() {
        let (_d, res) = replay_fixture("wal_legacy_empty_frame.bin");
        let (recs, seq) = res.unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key, b"after");
        assert_eq!(seq, 9);
    }

    /// Every stripe of a 0.9 generation is read, and a zero-length stripe (a
    /// generation created but never written) is empty, not an error.
    #[test]
    fn stripes_are_all_replayed_and_empty_files_are_empty() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("wal-3.log");
        std::fs::write(&base, b"").unwrap();
        std::fs::copy(
            crate::util::legacy_fixture("wal_legacy_all_flags.bin"),
            stripe_path(&base, 2),
        )
        .unwrap();
        let mut n = 0;
        let seq = replay(&base, |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 4);
        assert_eq!(seq, 4);
    }

    /// A frame whose CRC is CRC32-C rather than IEEE is a checksum mismatch —
    /// under 0.9's contract, a torn tail that ends the stripe.
    #[test]
    fn a_crc32c_frame_ends_the_stripe() {
        let bytes = std::fs::read(crate::util::legacy_fixture("wal_legacy_all_flags.bin")).unwrap();
        let len = crate::encoding::read_u32(&bytes[0..4]) as usize;
        let mut first = bytes[..8 + len].to_vec();
        let mut c = !0u32;
        for &x in &first[8..] {
            c ^= u32::from(x);
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    (c >> 1) ^ 0x82F6_3B78
                } else {
                    c >> 1
                };
            }
        }
        first[4..8].copy_from_slice(&(!c).to_le_bytes());
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("wal-0.log");
        std::fs::write(&base, &first).unwrap();
        let mut n = 0;
        replay(&base, |_| {
            n += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 0);
    }
}
