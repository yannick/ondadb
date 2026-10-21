//! wavesdb shared value files (`cf_<name>/<id>.blob`), read-only.
//!
//! A wavesdb table does not own a vlog: a separated value lives in one of its
//! family's shared blob files, and the entry names it by `(blob_id, off)` —
//! plus an in-group offset when the value sits inside a compression group.
//! The file is an epoch-1 value file:
//!
//! ```text
//! header   "YOLODBVL" | version u32 = 1 | flags u32 | dict_off u64 | dict_len u32 | crc32c u32
//!          flags: 0x1 a trained zstd dictionary (dict_off/dict_len locate its frame)
//!                 0x2 the file holds compression-group frames
//! value    crc32c(stored) u32 | codec u8 | stored_len u32 | stored        (= ondaDB's vlog frame)
//! group    crc32c(stored) u32 | codec u8 | stored_len u32 | raw_len u32 | stored
//! ```
//!
//! Decoding follows wavesdb's reader (`Reader.readBlob`,
//! `decodeVlogValue`, `decodeBlobGroupHeader`, `decodeBlobGroup`): the
//! header is validated before any frame is served, a frame may not start
//! inside it, a stored length never exceeds the raw one, the checksum is
//! checked before the codec byte is trusted, and a value must lie wholly
//! inside the group it names.
//!
//! Dictionary frames (codec 7) and brotli (codec 8) are `UnsupportedFormat`
//! until their 2c step; the header that announces a dictionary is accepted
//! now, so a file mixing dictionary and plain frames serves the plain ones.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::cache::{BlockCache, BlockDomain};
use crate::config::Compression;
use crate::encoding::{checksum, read_u32, read_u64};
use crate::error::{OndaError, Result};
use crate::format::vlog_header::{FLAG_DICTIONARY, FLAG_GROUPED, HEADER_LEN, MAGIC, VERSION};
use crate::storage::{ReadHandle, Storage};

/// `crc | codec | stored_len`, the value frame's header.
const VALUE_FRAME_HDR: usize = 9;
/// `crc | codec | stored_len | raw_len`, the group frame's header.
const GROUP_FRAME_HDR: usize = 13;
/// The largest group any wavesdb writer frames (`MaxBlobGroupBytes`); it bounds
/// the allocation a corrupt group header can ask for.
const MAX_GROUP_BYTES: u64 = 64 << 20;
const KNOWN_FLAGS: u32 = FLAG_DICTIONARY | FLAG_GROUPED;

fn corrupt(id: u64, what: impl std::fmt::Display) -> OndaError {
    OndaError::Corruption(format!("wavesdb blob file {id}: {what}"))
}

/// A decoded value-file header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Header {
    pub flags: u32,
    pub dict_off: u64,
    pub dict_len: u32,
}

/// Validate a value-file header the way wavesdb's `decodeValueFileHeader`
/// does: magic and checksum are corruption, the version (checked before the
/// checksum — a later version may checksum differently) and unknown flags are
/// `UnsupportedFormat`, reserved bytes and an empty dictionary corruption.
pub(crate) fn decode_header(id: u64, b: &[u8]) -> Result<Header> {
    if b.len() < HEADER_LEN || b[..8] != MAGIC {
        return Err(corrupt(id, "not an epoch-1 value file"));
    }
    let version = read_u32(&b[8..]);
    if version != VERSION {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb blob file {id}: value-file version {version}"
        )));
    }
    if checksum(&b[..28]) != read_u32(&b[28..]) {
        return Err(corrupt(id, "header checksum mismatch"));
    }
    let flags = read_u32(&b[12..]);
    if flags & !KNOWN_FLAGS != 0 {
        return Err(OndaError::UnsupportedFormat(format!(
            "wavesdb blob file {id}: value-file flags {flags:#x}"
        )));
    }
    if flags & FLAG_DICTIONARY == 0 {
        if b[16..28].iter().any(|&x| x != 0) {
            return Err(corrupt(id, "reserved header bytes are set"));
        }
        return Ok(Header { flags, dict_off: 0, dict_len: 0 });
    }
    let h = Header { flags, dict_off: read_u64(&b[16..]), dict_len: read_u32(&b[24..]) };
    if h.dict_off < HEADER_LEN as u64 || h.dict_len == 0 {
        return Err(corrupt(id, format!("dictionary at {} length {}", h.dict_off, h.dict_len)));
    }
    Ok(h)
}

/// The codec a value or group frame names, once its checksum has passed.
fn frame_codec(id: u64, codec: u8) -> Result<Compression> {
    match codec {
        crate::format::codec::ZSTD_DICT => Err(OndaError::UnsupportedFormat(format!(
            "wavesdb blob file {id}: zstd-dictionary frames (codec 7) are not readable yet"
        ))),
        other => Compression::from_codec_id(other),
    }
}

/// Decode `stored` (already checksummed) into exactly `raw_len` bytes.
fn decode_stored(id: u64, codec: Compression, stored: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    if codec == Compression::None {
        if stored.len() != raw_len {
            return Err(corrupt(id, "raw frame length disagrees with the value"));
        }
        return Ok(stored.to_vec());
    }
    if stored.is_empty() {
        return Err(corrupt(id, "empty compressed frame"));
    }
    let raw = crate::compress::decompress(codec, stored, raw_len)?;
    if raw.len() != raw_len {
        return Err(corrupt(id, "frame decompressed to the wrong length"));
    }
    Ok(raw)
}

struct Open {
    file: Arc<dyn ReadHandle>,
    header: Header,
}

/// One table's view of its family's blob files: opened (and header-checked)
/// on first use, then held for the reader's life. Blob files are immutable
/// once written, so a handle never goes stale while the table that points
/// into it is live.
pub(crate) struct Blobs {
    dir: PathBuf,
    storage: Arc<dyn Storage>,
    open: parking_lot::Mutex<HashMap<u64, Arc<Open>>>,
}

impl std::fmt::Debug for Blobs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Blobs").field("dir", &self.dir).finish_non_exhaustive()
    }
}

/// What a read needs from the table that asks for it.
pub(crate) struct ReadCtx<'a> {
    pub bc: &'a BlockCache,
    /// Largest single value admitted to the block cache; `0` disables it.
    pub value_cache_limit: usize,
    pub limiter: &'a Option<Arc<dyn crate::ioctrl::IoLimiter>>,
}

impl Blobs {
    /// The blob files beside the klog at `klog_path` (a family's tables and
    /// blob files share its `cf_<name>` directory).
    pub(crate) fn beside(klog_path: &str, storage: Arc<dyn Storage>) -> Blobs {
        let dir = std::path::Path::new(klog_path)
            .parent()
            .map(PathBuf::from)
            .unwrap_or_default();
        Blobs { dir, storage, open: parking_lot::Mutex::new(HashMap::new()) }
    }

    fn handle(&self, id: u64) -> Result<Arc<Open>> {
        if let Some(h) = self.open.lock().get(&id) {
            return Ok(h.clone());
        }
        let path = self.dir.join(format!("{id}.blob"));
        let file = self.storage.open_read(&path.to_string_lossy())?;
        let mut hdr = vec![0u8; HEADER_LEN.min(file.size()? as usize)];
        file.read_exact_at(&mut hdr, 0)?;
        // A failed header is not remembered: every read of the file fails
        // the same way, which is the point.
        let header = decode_header(id, &hdr)?;
        let open = Arc::new(Open { file, header });
        Ok(self.open.lock().entry(id).or_insert(open).clone())
    }

    /// Append the value `(id, off[, in_group_off])` of `raw_len` bytes to
    /// `out`.
    pub(crate) fn read_into(
        &self,
        ctx: &ReadCtx<'_>,
        id: u64,
        off: u64,
        group: Option<u64>,
        raw_len: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        let h = self.handle(id)?;
        // Every frame follows the header; a pointer into it is not a frame.
        if off < HEADER_LEN as u64 {
            return Err(corrupt(id, format!("frame offset {off} is inside the header")));
        }
        match group {
            None => self.read_value(ctx, &h, id, off, raw_len, out),
            Some(in_group) => self.read_grouped(ctx, &h, id, off, in_group, raw_len, out),
        }
    }

    fn read_value(
        &self,
        ctx: &ReadCtx<'_>,
        h: &Open,
        id: u64,
        off: u64,
        raw_len: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        // Blob ids come from the same id space as table ids, so the Vlog
        // domain keyed by blob id can collide with nothing: a wavesdb table
        // has no vlog of its own. A single value and a group never share an
        // offset within one file.
        let cacheable = ctx.value_cache_limit > 0 && raw_len <= ctx.value_cache_limit;
        let admit = ctx.bc.admits_current_thread();
        if cacheable {
            let hit = if admit {
                ctx.bc.get(id, off, BlockDomain::Vlog)
            } else {
                ctx.bc.peek(id, off, BlockDomain::Vlog)
            };
            if let Some(v) = hit {
                if v.len() != raw_len {
                    ctx.bc.remove(id, off, BlockDomain::Vlog);
                    return Err(corrupt(id, "cached value length disagrees with the pointer"));
                }
                out.extend_from_slice(&v);
                crate::perf::bump(|p| p.vlog_cache_hits += 1);
                return Ok(());
            }
        }
        crate::ioctrl::charge(ctx.limiter, (raw_len + VALUE_FRAME_HDR) as u64);
        let mut hdr = [0u8; VALUE_FRAME_HDR];
        h.file.read_exact_at(&mut hdr, off)?;
        let stored_len = read_u32(&hdr[5..]) as usize;
        // Stored bytes are kept only when smaller than the raw value, so a
        // longer frame is corrupt — and the bound keeps a bad header from
        // asking for a 4 GiB buffer.
        if stored_len > raw_len {
            return Err(corrupt(id, format!("frame at {off} is longer than its value")));
        }
        let mut stored = vec![0u8; stored_len];
        h.file.read_exact_at(&mut stored, off + VALUE_FRAME_HDR as u64)?;
        if checksum(&stored) != read_u32(&hdr) {
            return Err(corrupt(id, format!("frame at {off} checksum mismatch")));
        }
        let codec = frame_codec(id, hdr[4])?;
        let raw = decode_stored(id, codec, &stored, raw_len)?;
        crate::perf::bump(|p| {
            p.vlog_reads += 1;
            p.vlog_read_bytes += raw_len as u64;
        });
        if cacheable && admit && ctx.bc.enabled() {
            ctx.bc.put(id, off, BlockDomain::Vlog, Arc::from(raw.as_slice()));
        }
        out.extend_from_slice(&raw);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn read_grouped(
        &self,
        ctx: &ReadCtx<'_>,
        h: &Open,
        id: u64,
        off: u64,
        in_group: u64,
        raw_len: usize,
        out: &mut Vec<u8>,
    ) -> Result<()> {
        if h.header.flags & FLAG_GROUPED == 0 {
            return Err(corrupt(id, "a grouped pointer into a file with no groups"));
        }
        // The whole group is decoded once and cached, so a scan over its
        // members pays one decompression, not one per value.
        let admit = ctx.bc.admits_current_thread();
        let cached = if admit {
            ctx.bc.get(id, off, BlockDomain::Vlog)
        } else {
            ctx.bc.peek(id, off, BlockDomain::Vlog)
        };
        let group: Arc<[u8]> = match cached {
            Some(g) => {
                crate::perf::bump(|p| p.vlog_cache_hits += 1);
                g
            }
            None => {
                let mut hdr = [0u8; GROUP_FRAME_HDR];
                h.file.read_exact_at(&mut hdr, off)?;
                let stored_len = u64::from(read_u32(&hdr[5..]));
                let group_len = u64::from(read_u32(&hdr[9..]));
                if stored_len == 0
                    || group_len == 0
                    || stored_len > group_len
                    || group_len > MAX_GROUP_BYTES
                {
                    return Err(corrupt(id, format!("group header at {off}")));
                }
                // Checked against the group's own length before anything is
                // read or decompressed: a pointer that drifted onto the wrong
                // frame is caught here.
                if in_group.checked_add(raw_len as u64).is_none_or(|end| end > group_len) {
                    return Err(corrupt(id, format!("value outside the group at {off}")));
                }
                crate::ioctrl::charge(ctx.limiter, stored_len + GROUP_FRAME_HDR as u64);
                let mut stored = vec![0u8; stored_len as usize];
                h.file.read_exact_at(&mut stored, off + GROUP_FRAME_HDR as u64)?;
                if checksum(&stored) != read_u32(&hdr) {
                    return Err(corrupt(id, format!("group at {off} checksum mismatch")));
                }
                let codec = frame_codec(id, hdr[4])?;
                let raw = decode_stored(id, codec, &stored, group_len as usize)?;
                crate::perf::bump(|p| {
                    p.vlog_reads += 1;
                    p.vlog_read_bytes += group_len;
                });
                let g: Arc<[u8]> = Arc::from(raw.into_boxed_slice());
                if admit && ctx.bc.enabled() {
                    ctx.bc.put(id, off, BlockDomain::Vlog, g.clone());
                }
                g
            }
        };
        let start = usize::try_from(in_group).map_err(|_| corrupt(id, "in-group offset"))?;
        let v = start
            .checked_add(raw_len)
            .and_then(|end| group.get(start..end))
            .ok_or_else(|| corrupt(id, format!("value outside the group at {off}")))?;
        out.extend_from_slice(v);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::FileCache;
    use crate::encoding::{append_u32, append_u64};
    use crate::storage::LocalStorage;

    fn header(flags: u32, dict_off: u64, dict_len: u32) -> Vec<u8> {
        let mut b = MAGIC.to_vec();
        append_u32(&mut b, VERSION);
        append_u32(&mut b, flags);
        append_u64(&mut b, dict_off);
        append_u32(&mut b, dict_len);
        let crc = checksum(&b);
        append_u32(&mut b, crc);
        b
    }

    fn value_frame(codec: u8, stored: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        append_u32(&mut b, checksum(stored));
        b.push(codec);
        append_u32(&mut b, stored.len() as u32);
        b.extend_from_slice(stored);
        b
    }

    fn group_frame(codec: u8, stored: &[u8], raw_len: u32) -> Vec<u8> {
        let mut b = Vec::new();
        append_u32(&mut b, checksum(stored));
        b.push(codec);
        append_u32(&mut b, stored.len() as u32);
        append_u32(&mut b, raw_len);
        b.extend_from_slice(stored);
        b
    }

    #[test]
    fn headers_follow_wavesdb_rules() {
        assert_eq!(decode_header(1, &header(0, 0, 0)).unwrap().flags, 0);
        assert_eq!(decode_header(1, &header(3, 32, 9)).unwrap().dict_len, 9);
        let mut bad_magic = header(0, 0, 0);
        bad_magic[0] ^= 1;
        let mut bad_crc = header(0, 0, 0);
        bad_crc[29] ^= 1;
        let mut version = header(0, 0, 0);
        version[8] = 2;
        for (b, unsupported) in [
            (bad_magic, false),
            (bad_crc, false),
            (version, true),
            (header(4, 0, 0), true),
            (header(0, 32, 0), false), // reserved bytes without the flag
            (header(1, 31, 9), false), // dictionary inside the header
            (header(1, 32, 0), false), // empty dictionary
            (header(0, 0, 0)[..31].to_vec(), false),
        ] {
            match decode_header(1, &b) {
                Err(OndaError::UnsupportedFormat(_)) if unsupported => {}
                Err(OndaError::Corruption(_)) if !unsupported => {}
                other => panic!("{other:?}"),
            }
        }
    }

    /// One file with a raw value, a zstd value, a zstd group of three and a
    /// dictionary-coded frame, read through [`Blobs`].
    #[test]
    fn values_and_groups_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let big = b"wavesdb shared blob value ".repeat(40);
        let members: Vec<Vec<u8>> =
            (0..3).map(|i| format!("member {i} ").repeat(20 + i).into_bytes()).collect();
        let group_raw = members.concat();
        let zstd = crate::format::codec::ZSTD;

        let mut f = header(FLAG_GROUPED, 0, 0);
        let raw_off = f.len() as u64;
        f.extend(value_frame(0, b"small raw value"));
        let z_off = f.len() as u64;
        let z = crate::compress::compress(Compression::Zstd, &big).unwrap();
        assert!(z.len() < big.len());
        f.extend(value_frame(zstd, &z));
        let g_off = f.len() as u64;
        let gz = crate::compress::compress(Compression::Zstd, &group_raw).unwrap();
        f.extend(group_frame(zstd, &gz, group_raw.len() as u32));
        let dict_off = f.len() as u64;
        f.extend(value_frame(crate::format::codec::ZSTD_DICT, b"x"));
        std::fs::write(dir.path().join("7.blob"), &f).unwrap();

        let storage = LocalStorage::new(Arc::new(FileCache::new(4)), false);
        let blobs = Blobs::beside(dir.path().join("3.klog").to_str().unwrap(), storage);
        let bc = BlockCache::new(1 << 20);
        let ctx = ReadCtx { bc: &bc, value_cache_limit: 1 << 16, limiter: &None };
        let read = |off, group, len| {
            let mut out = b"prefix".to_vec();
            blobs.read_into(&ctx, 7, off, group, len, &mut out).map(|()| out[6..].to_vec())
        };
        assert_eq!(read(raw_off, None, 15).unwrap(), b"small raw value");
        for _ in 0..2 {
            // The second pass is served from the cache.
            assert_eq!(read(z_off, None, big.len()).unwrap(), big);
            let mut at = 0u64;
            for m in &members {
                assert_eq!(&read(g_off, Some(at), m.len()).unwrap(), m);
                at += m.len() as u64;
            }
        }
        let err = |r: Result<Vec<u8>>| match r {
            Err(OndaError::Corruption(m)) => m,
            other => panic!("{other:?}"),
        };
        err(read(raw_off, None, 14)); // raw length disagrees
        err(read(z_off, None, z.len() - 1)); // stored longer than the value
        err(read(g_off, Some(group_raw.len() as u64 - 1), 2)); // past the group
        err(read(4, None, 1)); // inside the header
        assert!(matches!(
            read(dict_off, None, 1),
            Err(OndaError::UnsupportedFormat(_))
        ));
        // A flipped stored byte is a checksum failure, never a decode.
        f[z_off as usize + VALUE_FRAME_HDR] ^= 1;
        std::fs::write(dir.path().join("8.blob"), &f).unwrap();
        let mut out = Vec::new();
        assert!(matches!(
            blobs.read_into(&ctx, 8, z_off, None, big.len(), &mut out),
            Err(OndaError::Corruption(m)) if m.contains("checksum")
        ));
        // A grouped pointer into a file whose header never declared groups.
        let mut plain = header(0, 0, 0);
        plain.extend(group_frame(0, b"abc", 3));
        std::fs::write(dir.path().join("9.blob"), &plain).unwrap();
        assert!(blobs.read_into(&ctx, 9, 32, Some(0), 3, &mut out).is_err());
    }
}
