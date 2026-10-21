//! Minimal SQLite WAL file reader: header parsing, frame checksum chaining
//! and collection of the pages touched by committed transactions.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const HEADER_SIZE: u64 = 32;
pub const FRAME_HEADER_SIZE: u64 = 24;

const MAGIC_LE: u32 = 0x377f_0682;
const MAGIC_BE: u32 = 0x377f_0683;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalHeader {
    /// Checksums are computed over big-endian words when true.
    pub big_endian: bool,
    pub page_size: u32,
    pub checkpoint_seq: u32,
    pub salt1: u32,
    pub salt2: u32,
    pub checksum1: u32,
    pub checksum2: u32,
}

impl WalHeader {
    pub fn frame_size(&self) -> u64 {
        FRAME_HEADER_SIZE + self.page_size as u64
    }
}

fn be32(b: &[u8], i: usize) -> u32 {
    u32::from_be_bytes(b[i..i + 4].try_into().unwrap())
}

pub fn checksum(big_endian: bool, mut s0: u32, mut s1: u32, b: &[u8]) -> (u32, u32) {
    debug_assert!(b.len().is_multiple_of(8));
    for chunk in b.as_chunks::<8>().0 {
        let (x, y) = if big_endian {
            (be32(chunk, 0), be32(chunk, 4))
        } else {
            (u32::from_le_bytes(chunk[0..4].try_into().unwrap()), u32::from_le_bytes(chunk[4..8].try_into().unwrap()))
        };
        s0 = s0.wrapping_add(x).wrapping_add(s1);
        s1 = s1.wrapping_add(y).wrapping_add(s0);
    }
    (s0, s1)
}

/// Reads the WAL header. Returns `None` if the file is missing, short, or its
/// header is not valid (SQLite treats all of those as an empty WAL).
pub fn read_header(path: &Path) -> io::Result<Option<WalHeader>> {
    let f = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut b = [0u8; HEADER_SIZE as usize];
    match f.read_exact_at(&mut b, 0) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let big_endian = match be32(&b, 0) {
        MAGIC_LE => false,
        MAGIC_BE => true,
        _ => return Ok(None),
    };
    let (c1, c2) = (be32(&b, 24), be32(&b, 28));
    if checksum(big_endian, 0, 0, &b[..24]) != (c1, c2) {
        return Ok(None);
    }
    if be32(&b, 4) != 3_007_000 {
        return Err(io::Error::other(format!("unsupported wal version: {}", be32(&b, 4))));
    }
    Ok(Some(WalHeader {
        big_endian,
        page_size: be32(&b, 8),
        checkpoint_seq: be32(&b, 12),
        salt1: be32(&b, 16),
        salt2: be32(&b, 20),
        checksum1: c1,
        checksum2: c2,
    }))
}

/// Result of scanning WAL frames from a starting offset.
#[derive(Debug, Default)]
pub struct Scan {
    /// Latest committed frame offset for each page, limited to `commit`.
    pub pages: BTreeMap<u32, u64>,
    /// Database size in pages after the last committed transaction; 0 if none.
    pub commit: u32,
    /// Offset just past the last commit frame; 0 if no commit was found.
    pub end: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ScanError {
    /// The frame before `start` no longer belongs to this WAL, so the WAL was
    /// rewritten since we last read it.
    #[error("previous wal frame does not match")]
    PrevFrameMismatch,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Walks frames starting at `start` (a frame boundary) and collects the pages
/// of fully committed transactions. Stops at the first frame that is short,
/// carries other salts, or breaks the checksum chain.
pub fn scan(f: &File, hdr: &WalHeader, start: u64) -> Result<Scan, ScanError> {
    let frame_size = hdr.frame_size();
    if start < HEADER_SIZE || !(start - HEADER_SIZE).is_multiple_of(frame_size) {
        return Err(io::Error::other(format!("unaligned wal offset {start}")).into());
    }
    let mut fh = [0u8; FRAME_HEADER_SIZE as usize];
    let (mut c1, mut c2) = if start == HEADER_SIZE {
        (hdr.checksum1, hdr.checksum2)
    } else {
        // Resume the checksum chain from the frame just before `start`.
        if f.read_exact_at(&mut fh, start - frame_size).is_err() {
            return Err(ScanError::PrevFrameMismatch);
        }
        if be32(&fh, 8) != hdr.salt1 || be32(&fh, 12) != hdr.salt2 {
            return Err(ScanError::PrevFrameMismatch);
        }
        (be32(&fh, 16), be32(&fh, 20))
    };

    let mut out = Scan::default();
    let mut pending: Vec<(u32, u64)> = Vec::new();
    let mut data = vec![0u8; hdr.page_size as usize];
    let mut offset = start;
    loop {
        if f.read_exact_at(&mut fh, offset).is_err() || f.read_exact_at(&mut data, offset + FRAME_HEADER_SIZE).is_err()
        {
            break;
        }
        if be32(&fh, 8) != hdr.salt1 || be32(&fh, 12) != hdr.salt2 {
            break;
        }
        let (n1, n2) = checksum(hdr.big_endian, c1, c2, &fh[..8]);
        let (n1, n2) = checksum(hdr.big_endian, n1, n2, &data);
        if (n1, n2) != (be32(&fh, 16), be32(&fh, 20)) {
            break;
        }
        (c1, c2) = (n1, n2);

        let pgno = be32(&fh, 0);
        let commit = be32(&fh, 4);
        pending.push((pgno, offset));
        offset += frame_size;
        if commit != 0 {
            out.pages.extend(pending.drain(..));
            out.commit = commit;
            out.end = offset;
        }
    }
    let commit = out.commit;
    out.pages.retain(|&pgno, _| pgno <= commit);
    Ok(out)
}

/// Reads the page stored in the frame at `frame_offset`.
pub fn read_frame_page(f: &File, frame_offset: u64, buf: &mut [u8]) -> io::Result<()> {
    f.read_exact_at(buf, frame_offset + FRAME_HEADER_SIZE)
}
