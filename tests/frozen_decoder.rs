//! Old-binary refusal, proven by a decoder rather than by a constant.
//!
//! This file vendors a *copy* of the 0.8.2 manifest header decode path — the
//! one that checks `VERSION` by exact equality against `1`. It is deliberately
//! not a re-export and not parameterized: the point of the test is that a
//! binary compiled before manifest v2 existed refuses a v2 manifest, and that
//! claim would be worth nothing if the "old" decoder could be made to accept v2
//! by flipping a constant this repository still owns.
//!
//! Nothing here may be changed to follow a format change. If a future version
//! makes this file fail, the fix is in the new format, not in this copy.

use std::path::{Path, PathBuf};

/// Verbatim 0.8.2 constants.
const MAGIC: u32 = 0x5756_4D46; // "WVMF"
const VERSION: u32 = 1;

fn fixture(name: &str) -> Vec<u8> {
    let path: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phase1")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("missing fixture {}: {e}", path.display()))
}

fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b[..4].try_into().unwrap())
}

/// Verbatim 0.8.2 whole-file CRC check plus header decode, down to the
/// `!= VERSION` equality that makes every version bump a fail-closed boundary.
/// `Err(&'static str)` stands in for `OndaError::Corruption`.
fn frozen_v1_decode_header(data: &[u8]) -> Result<(u64, u64), &'static str> {
    if data.len() < 4 {
        return Err("corrupt");
    }
    let (body, stored_crc) = data.split_at(data.len() - 4);
    // The CRC algorithm is not what this test freezes — the version check is —
    // so the engine's own CRC32-C is used rather than a second copy of it.
    if read_u32(stored_crc) != ondadb::encoding::checksum(body) {
        return Err("corrupt");
    }
    if body.len() < 24 {
        return Err("corrupt");
    }
    if read_u32(&body[0..4]) != MAGIC || read_u32(&body[4..8]) != VERSION {
        return Err("corrupt");
    }
    let next_file_id = u64::from_le_bytes(body[8..16].try_into().unwrap());
    let global_seq = u64::from_le_bytes(body[16..24].try_into().unwrap());
    Ok((next_file_id, global_seq))
}

/// The proof: a manifest carrying a capability word is refused by the frozen
/// VERSION-1 decoder, while every frozen VERSION-1 fixture still decodes.
#[test]
fn frozen_v1_decoder_refuses_v2_manifest() {
    // The v1 corpus is still readable by the old decoder — the refusal below is
    // about v2, not about this test's copy being broken.
    for name in [
        "manifest_v1_notail.bin",
        "manifest_v1_partition.bin",
        "manifest_v1_tier.bin",
        "manifest_v1_time.bin",
        "manifest_v1_unified.bin",
        "manifest_v1_object.bin",
        "manifest_v1_nonce.bin",
    ] {
        let got = frozen_v1_decode_header(&fixture(name));
        assert!(got.is_ok(), "{name}: the frozen decoder must still read v1");
    }

    let v2 = fixture("manifest_v2_caps_only.bin");
    assert_eq!(&v2[4..8], &2u32.to_le_bytes(), "fixture must be VERSION 2");
    assert!(
        frozen_v1_decode_header(&v2).is_err(),
        "a pre-1.0 binary must refuse a manifest that announces capabilities"
    );

    // And the current decoder reads the same bytes: the refusal is the old
    // binary's, not a property of the file being broken.
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("MANIFEST");
    std::fs::write(&path, &v2).unwrap();
    let m = ondadb::manifest::Manifest::load(&path).unwrap();
    assert_eq!(m.caps, ondadb::format::CAP_EXTENDED_RECORDS);
}
