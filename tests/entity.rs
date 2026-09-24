//! Wide-column entities (plan C F11, wavesdb `entity.go`).
//!
//! The golden frames below were produced by **wavesdb's own**
//! `EncodeEntity` (wavesdb at `340cd87` and later, via a throwaway Go program
//! importing the package), so they pin interoperability, not just
//! self-consistency: ondaDB must encode these column sets to exactly these
//! bytes and decode these bytes to exactly these columns.

use std::sync::Arc;
use std::time::Duration;

use ondadb::entity::{
    decode_entity, decode_entity_into, encode_entity, sort_columns, EntityColumn,
    EntityColumnRef, MAX_ENTITY_BYTES, MAX_ENTITY_COLUMNS, MAX_ENTITY_NAME_LEN,
};
use ondadb::{ColumnFamily, ColumnFamilyConfig, IsolationLevel, OndaError, Options, DB};

fn hex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

fn cols(pairs: &[(&[u8], &[u8])]) -> Vec<EntityColumn> {
    pairs.iter().map(|(n, v)| EntityColumn::new(*n, *v)).collect()
}

fn owned(refs: &[EntityColumnRef<'_>]) -> Vec<EntityColumn> {
    refs.iter().map(|c| c.into_owned()).collect()
}

/// Recompute the trailing CRC so an altered frame is judged on its structure.
fn refix(mut b: Vec<u8>) -> Vec<u8> {
    let n = b.len();
    let crc = ondadb::encoding::checksum(&b[..n - 4]);
    b[n - 4..].copy_from_slice(&crc.to_le_bytes());
    b
}

fn golden() -> Vec<(&'static str, Vec<EntityColumn>, Vec<u8>)> {
    let long = vec![b'v'; 300];
    let mut long_frame = hex("5756453101001900000001000000000000000101000000ac0261");
    long_frame.extend_from_slice(&long);
    long_frame.extend_from_slice(&hex("aaa216d1"));
    vec![
        ("empty", vec![], hex("5756453101000e00000000000000a9ab8ab6")),
        (
            "ab_xyz",
            cols(&[(b"ab", b"xyz")]),
            hex("575645310100180000000100000000000000020200000003616278797abfb7a4e9"),
        ),
        (
            "a_empty",
            cols(&[(b"a", b"")]),
            hex("575645310100180000000100000000000000010100000000611c2e7213"),
        ),
        (
            "order preserved",
            cols(&[(b"z", b"1"), (b"a", b"2"), (b"m", b"3")]),
            hex(concat!(
                "5756453101002c000000030000000000000001010000000102000000010300000001",
                "040000000105000000017a3161326d339be559bc"
            )),
        ),
        (
            "binary",
            vec![
                EntityColumn::new(vec![0, 0xff, b'W', b'V', b'E', b'1'], vec![0x80, 0x00, 0xff]),
                EntityColumn::new(vec![0], vec![0xaa; 10]),
            ],
            hex(concat!(
                "57564531010022000000020000000000000006060000000309000000010a0000000a",
                "00ff575645318000ff00aaaaaaaaaaaaaaaaaaaab2fc3a24"
            )),
        ),
        ("two-byte varint", cols(&[(b"a", &long)]), long_frame),
        (
            "user",
            cols(&[(b"name", b"Ada"), (b"email", b"ada@example.com")]),
            hex(concat!(
                "57564531010022000000020000000000000004040000000307000000050c0000000f",
                "6e616d65416461656d61696c616461406578616d706c652e636f6d7012415c"
            )),
        ),
    ]
}

#[test]
fn frames_match_wavesdb_byte_for_byte() {
    for (name, columns, frame) in golden() {
        assert_eq!(encode_entity(&columns).unwrap(), frame, "{name}: encode");
        assert_eq!(owned(&decode_entity(&frame).unwrap()), columns, "{name}: decode");
    }
}

/// The spec's own worked layout (wavesdb `TestEntityFrameLayout`), assembled
/// field by field rather than from a captured hex string.
#[test]
fn frame_layout_matches_the_specification() {
    let mut want = b"WVE1".to_vec();
    want.extend_from_slice(&[1, 0]);
    want.extend_from_slice(&24u32.to_le_bytes()); // 14 + (4+1+4+1)
    want.extend_from_slice(&1u32.to_le_bytes());
    want.extend_from_slice(&0u32.to_le_bytes()); // name offset
    want.push(2); // name length
    want.extend_from_slice(&2u32.to_le_bytes()); // value offset
    want.push(3); // value length
    want.extend_from_slice(b"abxyz");
    let crc = ondadb::encoding::checksum(&want);
    want.extend_from_slice(&crc.to_le_bytes());
    assert_eq!(encode_entity(&[("ab", "xyz")]).unwrap(), want);
}

#[test]
fn round_trip_is_canonical() {
    let cases: Vec<Vec<EntityColumn>> = vec![
        vec![],
        cols(&[(b"a", b"1")]),
        cols(&[(b"a", b"")]),
        vec![EntityColumn::new(vec![b'n'; MAX_ENTITY_NAME_LEN], "v")],
        (0..MAX_ENTITY_COLUMNS)
            .map(|i| EntityColumn::new(format!("col-{i}"), vec![i as u8]))
            .collect(),
    ];
    for c in cases {
        let b = encode_entity(&c).unwrap();
        let got = decode_entity(&b).unwrap();
        assert_eq!(owned(&got), c);
        assert_eq!(encode_entity(&got).unwrap(), b, "re-encode differs");
    }
}

#[test]
fn encode_rejects_invalid_column_sets() {
    let too_many: Vec<EntityColumn> = (0..=MAX_ENTITY_COLUMNS)
        .map(|i| EntityColumn::new(format!("c{i}"), ""))
        .collect();
    let mut dup_many: Vec<EntityColumn> =
        (0..40).map(|i| EntityColumn::new(format!("c{i}"), "")).collect();
    dup_many[39].name = b"c3".to_vec();
    for (name, c) in [
        ("empty name", cols(&[(b"", b"v")])),
        ("duplicate", cols(&[(b"a", b"1"), (b"b", b"2"), (b"a", b"3")])),
        ("duplicate, table path", dup_many),
        ("name too long", vec![EntityColumn::new(vec![b'n'; MAX_ENTITY_NAME_LEN + 1], "")]),
        ("too many columns", too_many),
    ] {
        assert!(
            matches!(encode_entity(&c), Err(OndaError::InvalidArgs(_))),
            "{name}"
        );
    }
    let big = vec![0u8; MAX_ENTITY_BYTES / 2];
    assert!(matches!(
        encode_entity(&[("a", &big), ("b", &big)]),
        Err(OndaError::TooLarge(_))
    ));
}

#[test]
fn decode_rejects_malformed_frames() {
    let valid = encode_entity(&[("name", "value"), ("k", "v")]).unwrap();
    let h = u32::from_le_bytes(valid[6..10].try_into().unwrap()) as usize;
    let mutate = |f: &dyn Fn(&mut Vec<u8>)| {
        let mut b = valid.clone();
        f(&mut b);
        refix(b)
    };
    let put = |b: &mut Vec<u8>, at: usize, x: u32| b[at..at + 4].copy_from_slice(&x.to_le_bytes());
    let not_entity: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("short raw", b"hello".to_vec()),
        ("raw of frame length", vec![b'x'; valid.len()]),
        ("17 bytes", valid[..17].to_vec()),
        ("wrong magic", mutate(&|b| b[0] = b'X')),
        ("unknown version", mutate(&|b| b[4] = 2)),
        ("version zero", mutate(&|b| b[4] = 0)),
        ("nonzero flags", mutate(&|b| b[5] = 1)),
        ("header below minimum", mutate(&|b| put(b, 6, 13))),
        ("header past frame", mutate(&|b| {
            let n = b.len() as u32;
            put(b, 6, n - 3)
        })),
        ("header huge", mutate(&|b| put(b, 6, u32::MAX))),
        ("header shorter than directory", mutate(&|b| put(b, 6, 24))),
        ("count too large", mutate(&|b| put(b, 10, 3))),
        ("count too small", mutate(&|b| put(b, 10, 1))),
        ("count over limit", mutate(&|b| put(b, 10, MAX_ENTITY_COLUMNS as u32 + 1))),
        ("second name overlaps", mutate(&|b| put(b, 24, 0))),
        ("truncated payload", refix(valid[..valid.len() - 6].to_vec())),
        ("slack between directory and payload", {
            let mut b = valid[..h].to_vec();
            b.push(0);
            b.extend_from_slice(&valid[h..]);
            put(&mut b, 6, h as u32 + 1);
            refix(b)
        }),
        ("above MAX_ENTITY_BYTES", vec![0; MAX_ENTITY_BYTES + 1]),
        ("magic and garbage", {
            let mut b = b"WVE1\x01\x00".to_vec();
            b.extend_from_slice(&[7; 40]);
            b
        }),
    ];
    for (name, frame) in not_entity {
        assert!(
            matches!(decode_entity(&frame), Err(OndaError::NotEntity(_))),
            "{name}: {:?}",
            decode_entity(&frame)
        );
    }
    // Duplicate and empty names in hand-built frames: not an entity.
    let dup = {
        let mut b = encode_entity(&[("ab", "1"), ("cd", "2")]).unwrap();
        let h = u32::from_le_bytes(b[6..10].try_into().unwrap()) as usize;
        b[h + 3] = b'a';
        b[h + 4] = b'b';
        refix(b)
    };
    assert!(matches!(decode_entity(&dup), Err(OndaError::NotEntity(_))));

    // Structure holds, CRC does not: corruption, never "not an entity".
    let mut bad_crc = valid.clone();
    *bad_crc.last_mut().unwrap() ^= 0xff;
    assert!(matches!(decode_entity(&bad_crc), Err(OndaError::Corruption(_))));
    let mut flipped = valid.clone();
    flipped[h] ^= 1;
    assert!(matches!(decode_entity(&flipped), Err(OndaError::Corruption(_))));
}

/// wavesdb `TestEntityOverlapRejected`: directory entries pointing at bad
/// regions of a well-sized payload.
#[test]
fn decode_rejects_bad_regions() {
    fn build(entries: &[(u32, u64, u32, u64)], payload: &[u8]) -> Vec<u8> {
        let mut dir = Vec::new();
        for &(no, nl, vo, vl) in entries {
            dir.extend_from_slice(&no.to_le_bytes());
            ondadb::encoding::append_uvarint(&mut dir, nl);
            dir.extend_from_slice(&vo.to_le_bytes());
            ondadb::encoding::append_uvarint(&mut dir, vl);
        }
        let mut b = b"WVE1\x01\x00".to_vec();
        b.extend_from_slice(&(14 + dir.len() as u32).to_le_bytes());
        b.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        b.extend_from_slice(&dir);
        b.extend_from_slice(payload);
        b.extend_from_slice(&[0; 4]);
        refix(b)
    }
    let pay = b"ab12cd34";
    let good = build(&[(0, 2, 2, 2), (4, 2, 6, 2)], pay);
    assert_eq!(
        owned(&decode_entity(&good).unwrap()),
        cols(&[(b"ab", b"12"), (b"cd", b"34")])
    );
    for (name, entries) in [
        ("value overlaps name", [(0, 2, 1, 2), (4, 2, 6, 2)]),
        ("second name overlaps first value", [(0, 2, 2, 2), (3, 2, 6, 2)]),
        ("descending offsets", [(4, 2, 6, 2), (0, 2, 2, 2)]),
        ("value before name", [(2, 2, 0, 2), (4, 2, 6, 2)]),
        ("gap before first name", [(1, 1, 2, 2), (4, 2, 6, 2)]),
        ("gap between columns", [(0, 2, 2, 1), (4, 2, 6, 2)]),
        ("payload not covered", [(0, 2, 2, 2), (4, 2, 6, 1)]),
        ("past payload", [(0, 2, 2, 2), (4, 2, 6, 3)]),
        ("offset past payload", [(0, 2, 2, 2), (4, 2, 100, 0)]),
        ("length overflow", [(0, 2, 2, 2), (4, 2, 6, 1 << 40)]),
        ("offset+length wraps u32", [(0, 2, 2, 2), (4, 2, u32::MAX, 1)]),
        ("empty name", [(0, 0, 0, 4), (4, 2, 6, 2)]),
    ] {
        assert!(
            matches!(decode_entity(&build(&entries, pay)), Err(OndaError::NotEntity(_))),
            "{name}"
        );
    }
}

#[test]
fn decode_borrows_and_into_reuses() {
    let mut b = encode_entity(&[("a", "hello")]).unwrap();
    let h = u32::from_le_bytes(b[6..10].try_into().unwrap()) as usize;
    b[h + 1] = b'H';
    let b = refix(b);
    let got = decode_entity(&b).unwrap();
    assert_eq!(got[0].value, b"Hello");
    assert!(std::ptr::eq(got[0].value.as_ptr(), b[h + 1..].as_ptr()));

    let frame = encode_entity(&[("x", "1"), ("y", "2")]).unwrap();
    let mut dst = Vec::with_capacity(8);
    decode_entity_into(&mut dst, &frame).unwrap();
    assert_eq!(dst.len(), 2);
    let cap = dst.capacity();
    dst.clear();
    decode_entity_into(&mut dst, &frame).unwrap();
    assert_eq!(dst.capacity(), cap, "reused vector regrew");

    // An empty entity is a value: an empty vector, not an error.
    assert!(decode_entity(&encode_entity::<EntityColumn>(&[]).unwrap())
        .unwrap()
        .is_empty());
}

#[test]
fn sort_columns_is_bytewise() {
    let mut c = cols(&[(b"b", b"1"), (b"a", b"2"), (b"B", b"3")]);
    sort_columns(&mut c);
    let names: Vec<&[u8]> = c.iter().map(|c| c.name.as_slice()).collect();
    assert_eq!(names, vec![&b"B"[..], b"a", b"b"]);
}

fn open(dir: &std::path::Path) -> (DB, Arc<ColumnFamily>) {
    let db = DB::open(Options::new(dir.to_str().unwrap())).unwrap();
    let cf = db
        .create_column_family("c", ColumnFamilyConfig::default())
        .unwrap();
    (db, cf)
}

#[test]
fn db_put_get_entity() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    let user = [("name", "Ada"), ("email", "ada@example.com")];
    db.put_entity(&cf, b"user:1", &user, Duration::ZERO).unwrap();
    assert_eq!(
        db.get_entity(&cf, b"user:1").unwrap(),
        cols(&[(b"name", b"Ada"), (b"email", b"ada@example.com")])
    );
    // The stored value is exactly the frame: `get` sees opaque bytes.
    assert_eq!(db.get(&cf, b"user:1").unwrap(), encode_entity(&user).unwrap());

    assert_eq!(
        db.get_columns(&cf, b"user:1", &[b"email", b"missing", b"name"]).unwrap(),
        vec![Some(b"ada@example.com".to_vec()), None, Some(b"Ada".to_vec())]
    );

    db.put(&cf, b"plain", b"raw", Duration::ZERO).unwrap();
    assert!(matches!(db.get_entity(&cf, b"plain"), Err(OndaError::NotEntity(_))));
    assert!(matches!(db.get_columns(&cf, b"plain", &[b"a"]), Err(OndaError::NotEntity(_))));
    assert!(matches!(db.get_entity(&cf, b"absent"), Err(OndaError::NotFound)));

    // A frame written with plain `put` is an entity to `get_entity`.
    db.put(&cf, b"raw-frame", &encode_entity(&[("k", "v")]).unwrap(), Duration::ZERO)
        .unwrap();
    assert_eq!(db.get_entity(&cf, b"raw-frame").unwrap(), cols(&[(b"k", b"v")]));

    // Invalid column sets write nothing.
    assert!(matches!(
        db.put_entity(&cf, b"bad", &[("a", "1"), ("a", "2")], Duration::ZERO),
        Err(OndaError::InvalidArgs(_))
    ));
    assert!(matches!(db.get(&cf, b"bad"), Err(OndaError::NotFound)));

    // Survives flush and reopen: it is an ordinary value.
    db.flush_memtable(&cf).unwrap();
    db.close().unwrap();
    let db = DB::open(Options::new(dir.path().to_str().unwrap())).unwrap();
    let cf = db.get_column_family("c").unwrap();
    assert_eq!(db.get_entity(&cf, b"user:1").unwrap().len(), 2);
    db.close().unwrap();
}

#[test]
fn txn_entities_are_whole_key_values() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put_entity(&cf, b"e", &[("a", "1"), ("b", "2")], Duration::ZERO)
        .unwrap();

    // Read-your-writes through the buffer.
    let mut t = db.begin_with_isolation(IsolationLevel::Snapshot);
    t.put_entity(&cf, b"e2", &[("x", "y")], Duration::ZERO).unwrap();
    assert_eq!(t.get_entity(&cf, b"e2").unwrap(), cols(&[(b"x", b"y")]));
    assert_eq!(
        t.get_columns(&cf, b"e", &[b"b"]).unwrap(),
        vec![Some(b"2".to_vec())]
    );
    // A validation failure leaves the transaction untouched.
    assert!(matches!(
        t.put_entity(&cf, b"e3", &[("", "v")], Duration::ZERO),
        Err(OndaError::InvalidArgs(_))
    ));
    assert!(matches!(t.get(&cf, b"e3"), Err(OndaError::NotFound)));
    t.commit().unwrap();
    assert_eq!(db.get_entity(&cf, b"e2").unwrap(), cols(&[(b"x", b"y")]));

    // Two transactions updating *different* columns of one key conflict:
    // entities are whole-key values.
    let mut t1 = db.begin_with_isolation(IsolationLevel::Snapshot);
    let mut t2 = db.begin_with_isolation(IsolationLevel::Snapshot);
    let mut c1 = t1.get_entity(&cf, b"e").unwrap();
    c1[0].value = b"10".to_vec();
    t1.put_entity(&cf, b"e", &c1, Duration::ZERO).unwrap();
    let mut c2 = t2.get_entity(&cf, b"e").unwrap();
    c2[1].value = b"20".to_vec();
    t2.put_entity(&cf, b"e", &c2, Duration::ZERO).unwrap();
    t1.commit().unwrap();
    assert!(matches!(t2.commit(), Err(OndaError::Conflict(_))));
    assert_eq!(
        db.get_entity(&cf, b"e").unwrap(),
        cols(&[(b"a", b"10"), (b"b", b"2")])
    );
    db.close().unwrap();
}

#[test]
fn entity_ttl_applies_to_the_whole_entity() {
    let dir = tempfile::tempdir().unwrap();
    let (db, cf) = open(dir.path());
    db.put_entity(&cf, b"e", &[("a", "1")], Duration::from_millis(1))
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(matches!(db.get_entity(&cf, b"e"), Err(OndaError::NotFound)));
    db.close().unwrap();
}
