//! Reader and writer for LTX (Liteserver Transaction) files, the on-disk
//! format used by Litestream v0.5+. Mirrors github.com/superfly/ltx so files
//! are interchangeable with the Go implementation.

use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};

use crc::{CRC_64_GO_ISO, Crc, Digest};

pub const MAGIC: &[u8; 4] = b"LTX1";
pub const VERSION: u32 = 3;
pub const HEADER_SIZE: usize = 100;
pub const PAGE_HEADER_SIZE: usize = 6;
pub const TRAILER_SIZE: usize = 16;

pub const HEADER_FLAG_NO_CHECKSUM: u32 = 1 << 1;
pub const HEADER_FLAG_MASK: u32 = HEADER_FLAG_NO_CHECKSUM;

/// Set on a page header when a 4-byte size and an LZ4 *block* follow.
pub const PAGE_HEADER_FLAG_SIZE: u16 = 1 << 0;

/// Set on every checksum so that a valid checksum is never zero.
pub const CHECKSUM_FLAG: u64 = 1 << 63;

const PENDING_BYTE: u64 = 0x4000_0000;

static CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

pub fn hasher() -> Digest<'static, u64> {
    CRC64.digest()
}

/// Page number that holds SQLite's PENDING_BYTE; it is never stored.
pub fn lock_pgno(page_size: u32) -> u32 {
    (PENDING_BYTE / page_size as u64) as u32 + 1
}

pub fn is_valid_page_size(sz: u32) -> bool {
    (512..=65536).contains(&sz) && sz.is_power_of_two()
}

/// CRC-64 of a page combined with its page number.
pub fn checksum_page(pgno: u32, data: &[u8]) -> u64 {
    let mut h = hasher();
    h.update(&pgno.to_be_bytes());
    h.update(data);
    CHECKSUM_FLAG | h.finalize()
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid LTX file")]
    InvalidFile,
    #[error("file checksum mismatch")]
    ChecksumMismatch,
    #[error("invalid ltx header: {0}")]
    InvalidHeader(String),
    #[error("{0}")]
    Format(String),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

fn format_err(msg: impl Into<String>) -> Error {
    Error::Format(msg.into())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Header {
    pub flags: u32,
    pub page_size: u32,
    /// Database size in pages after this file is applied.
    pub commit: u32,
    pub min_txid: u64,
    pub max_txid: u64,
    /// Milliseconds since the unix epoch.
    pub timestamp: i64,
    pub pre_apply_checksum: u64,
    pub wal_offset: i64,
    pub wal_size: i64,
    pub wal_salt1: u32,
    pub wal_salt2: u32,
    pub node_id: u64,
}

impl Header {
    /// Snapshots start at TXID 1 and contain every page in the database.
    pub fn is_snapshot(&self) -> bool {
        self.min_txid == 1
    }

    pub fn no_checksum(&self) -> bool {
        self.flags & HEADER_FLAG_NO_CHECKSUM != 0
    }

    pub fn validate(&self) -> Result<()> {
        let bad = |m: String| Err(Error::InvalidHeader(m));
        if self.flags & !HEADER_FLAG_MASK != 0 {
            return bad(format!("invalid flags: 0x{:08x}", self.flags));
        }
        if !is_valid_page_size(self.page_size) {
            return bad(format!("invalid page size: {}", self.page_size));
        }
        if self.min_txid == 0 {
            return bad("minimum transaction id required".into());
        }
        if self.max_txid == 0 {
            return bad("maximum transaction id required".into());
        }
        if self.min_txid > self.max_txid {
            return bad(format!("transaction ids out of order: ({},{})", self.min_txid, self.max_txid));
        }
        if self.wal_offset < 0 || self.wal_size < 0 {
            return bad("wal offset and size cannot be negative".into());
        }
        if (self.wal_salt1 != 0 || self.wal_salt2 != 0) && self.wal_offset == 0 {
            return bad("wal offset required if salt exists".into());
        }
        if self.wal_offset == 0 && self.wal_size != 0 {
            return bad("wal offset required if wal size exists".into());
        }
        if self.is_snapshot() || self.no_checksum() {
            if self.pre_apply_checksum != 0 {
                return bad("pre-apply checksum must be zero".into());
            }
        } else if self.pre_apply_checksum & CHECKSUM_FLAG == 0 {
            return bad("pre-apply checksum required on non-snapshot files".into());
        }
        Ok(())
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut b = [0u8; HEADER_SIZE];
        b[0..4].copy_from_slice(MAGIC);
        b[4..8].copy_from_slice(&self.flags.to_be_bytes());
        b[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        b[12..16].copy_from_slice(&self.commit.to_be_bytes());
        b[16..24].copy_from_slice(&self.min_txid.to_be_bytes());
        b[24..32].copy_from_slice(&self.max_txid.to_be_bytes());
        b[32..40].copy_from_slice(&self.timestamp.to_be_bytes());
        b[40..48].copy_from_slice(&self.pre_apply_checksum.to_be_bytes());
        b[48..56].copy_from_slice(&self.wal_offset.to_be_bytes());
        b[56..64].copy_from_slice(&self.wal_size.to_be_bytes());
        b[64..68].copy_from_slice(&self.wal_salt1.to_be_bytes());
        b[68..72].copy_from_slice(&self.wal_salt2.to_be_bytes());
        b[72..80].copy_from_slice(&self.node_id.to_be_bytes());
        b
    }

    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        if b.len() < HEADER_SIZE || &b[0..4] != MAGIC {
            return Err(Error::InvalidFile);
        }
        let u32_at = |i: usize| u32::from_be_bytes(b[i..i + 4].try_into().unwrap());
        let u64_at = |i: usize| u64::from_be_bytes(b[i..i + 8].try_into().unwrap());
        Ok(Header {
            flags: u32_at(4),
            page_size: u32_at(8),
            commit: u32_at(12),
            min_txid: u64_at(16),
            max_txid: u64_at(24),
            timestamp: u64_at(32) as i64,
            pre_apply_checksum: u64_at(40),
            wal_offset: u64_at(48) as i64,
            wal_size: u64_at(56) as i64,
            wal_salt1: u32_at(64),
            wal_salt2: u32_at(68),
            node_id: u64_at(72),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Trailer {
    pub post_apply_checksum: u64,
    pub file_checksum: u64,
}

/// Formats a TXID the way Litestream does: 16 lowercase hex digits.
pub struct Txid(pub u64);

impl fmt::Display for Txid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

pub fn format_filename(min_txid: u64, max_txid: u64) -> String {
    format!("{}-{}.ltx", Txid(min_txid), Txid(max_txid))
}

/// Parses `<min>-<max>.ltx` where both are 16 lowercase hex digits.
pub fn parse_filename(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".ltx")?;
    let (a, b) = stem.split_once('-')?;
    let is_hex = |s: &str| s.len() == 16 && s.bytes().all(|c| matches!(c, b'0'..=b'9' | b'a'..=b'f'));
    if !is_hex(a) || !is_hex(b) {
        return None;
    }
    Some((u64::from_str_radix(a, 16).ok()?, u64::from_str_radix(b, 16).ok()?))
}

pub fn parse_txid(s: &str) -> Option<u64> {
    if s.is_empty() || s.len() > 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

fn append_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

/// Streaming LTX writer. Pages must be written in ascending page order.
pub struct Encoder<W: Write> {
    w: W,
    header: Header,
    hash: Digest<'static, u64>,
    /// (pgno, offset, size) of each page frame, in write order.
    index: Vec<(u32, u64, u64)>,
    n: u64,
    prev_pgno: u32,
    compress_buf: Vec<u8>,
}

impl<W: Write> Encoder<W> {
    pub fn new(mut w: W, header: Header) -> Result<Self> {
        header.validate()?;
        let b = header.to_bytes();
        w.write_all(&b)?;
        let mut hash = hasher();
        hash.update(&b);
        let compress_buf = vec![0u8; lz4_flex::block::get_maximum_output_size(header.page_size as usize)];
        Ok(Encoder { w, header, hash, index: Vec::new(), n: HEADER_SIZE as u64, prev_pgno: 0, compress_buf })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    /// Bytes written so far.
    pub fn len(&self) -> u64 {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    fn write_hashed(&mut self, b: &[u8]) -> Result<()> {
        self.w.write_all(b)?;
        self.hash.update(b);
        self.n += b.len() as u64;
        Ok(())
    }

    pub fn encode_page(&mut self, pgno: u32, data: &[u8]) -> Result<()> {
        let hdr = &self.header;
        if pgno == 0 {
            return Err(format_err("page number required"));
        }
        if pgno > hdr.commit {
            return Err(format_err(format!("page number {pgno} out-of-bounds for commit size {}", hdr.commit)));
        }
        if data.len() != hdr.page_size as usize {
            return Err(format_err(format!("invalid page buffer size: {}, expecting {}", data.len(), hdr.page_size)));
        }
        let lock = lock_pgno(hdr.page_size);
        if pgno == lock {
            return Err(format_err(format!("cannot encode lock page: pgno={pgno}")));
        }
        if hdr.is_snapshot() {
            let expected = match self.prev_pgno {
                0 => 1,
                p if p + 1 == lock => p + 2,
                p => p + 1,
            };
            if pgno != expected {
                return Err(format_err(format!("nonsequential page numbers in snapshot: {},{pgno}", self.prev_pgno)));
            }
        } else if self.prev_pgno >= pgno {
            return Err(format_err(format!("out-of-order page numbers: {},{pgno}", self.prev_pgno)));
        }

        let offset = self.n;
        let size = lz4_flex::block::compress_into(data, &mut self.compress_buf)
            .map_err(|e| format_err(format!("compress page: {e}")))?;

        let mut ph = [0u8; PAGE_HEADER_SIZE + 4];
        ph[0..4].copy_from_slice(&pgno.to_be_bytes());
        ph[4..6].copy_from_slice(&PAGE_HEADER_FLAG_SIZE.to_be_bytes());
        ph[6..10].copy_from_slice(&(size as u32).to_be_bytes());
        self.write_hashed(&ph)?;

        // The compressed bytes are written but the *uncompressed* page is hashed.
        self.w.write_all(&self.compress_buf[..size])?;
        self.hash.update(data);
        self.n += size as u64;

        self.index.push((pgno, offset, self.n - offset));
        self.prev_pgno = pgno;
        Ok(())
    }

    /// Writes the page index and trailer and returns the underlying writer.
    pub fn finish(mut self, post_apply_checksum: u64) -> Result<(W, Trailer)> {
        if self.header.no_checksum() {
            if post_apply_checksum != 0 {
                return Err(format_err("post-apply checksum not allowed"));
            }
            if self.header.commit == 0 {
                return Err(format_err("cannot encode a zero-length database without checksums"));
            }
        } else if post_apply_checksum & CHECKSUM_FLAG == 0 {
            return Err(format_err("post-apply checksum required"));
        }

        // An empty page header ends the page block.
        self.write_hashed(&[0u8; PAGE_HEADER_SIZE])?;

        let index_start = self.n;
        let mut buf = Vec::with_capacity(self.index.len() * 8 + 16);
        for &(pgno, offset, size) in &self.index {
            append_uvarint(&mut buf, pgno as u64);
            append_uvarint(&mut buf, offset);
            append_uvarint(&mut buf, size);
        }
        append_uvarint(&mut buf, 0);
        let index_size = buf.len() as u64;
        self.write_hashed(&buf)?;
        debug_assert_eq!(self.n - index_start, index_size);
        self.write_hashed(&index_size.to_be_bytes())?;

        let post = post_apply_checksum.to_be_bytes();
        self.write_hashed(&post)?;
        let file_checksum = CHECKSUM_FLAG | self.hash.clone().finalize();
        self.w.write_all(&file_checksum.to_be_bytes())?;
        self.n += 8;
        self.w.flush()?;

        Ok((self.w, Trailer { post_apply_checksum, file_checksum }))
    }
}

/// Streaming LTX reader that verifies the file checksum on `finish`.
pub struct Decoder<R: Read> {
    r: BufReader<R>,
    header: Header,
    hash: Digest<'static, u64>,
    compressed: Vec<u8>,
    pages_read: usize,
    done_pages: bool,
    /// Bytes consumed through `read_hashed`.
    n: u64,
    /// Rolling database checksum, tracked for snapshots with checksums.
    chksum: u64,
}

impl<R: Read> Decoder<R> {
    pub fn new(r: R) -> Result<Self> {
        let mut r = BufReader::with_capacity(256 * 1024, r);
        let mut b = [0u8; HEADER_SIZE];
        r.read_exact(&mut b)?;
        let header = Header::from_bytes(&b)?;
        header.validate()?;
        let mut hash = hasher();
        hash.update(&b);
        let chksum = if header.no_checksum() { 0 } else { CHECKSUM_FLAG };
        Ok(Decoder {
            r,
            header,
            hash,
            compressed: Vec::new(),
            pages_read: 0,
            done_pages: false,
            n: HEADER_SIZE as u64,
            chksum,
        })
    }

    pub fn header(&self) -> &Header {
        &self.header
    }

    fn read_hashed(&mut self, buf: &mut [u8]) -> Result<()> {
        self.r.read_exact(buf)?;
        self.hash.update(buf);
        self.n += buf.len() as u64;
        Ok(())
    }

    /// Reads the next page into `data`. Returns `None` at the end of the page block.
    pub fn next_page(&mut self, data: &mut [u8]) -> Result<Option<u32>> {
        if self.done_pages {
            return Ok(None);
        }
        if data.len() != self.header.page_size as usize {
            return Err(format_err("invalid page buffer size"));
        }
        let mut ph = [0u8; PAGE_HEADER_SIZE];
        self.read_hashed(&mut ph)?;
        let pgno = u32::from_be_bytes(ph[0..4].try_into().unwrap());
        let flags = u16::from_be_bytes(ph[4..6].try_into().unwrap());
        if pgno == 0 && flags == 0 {
            self.done_pages = true;
            return Ok(None);
        }
        if pgno == 0 {
            return Err(format_err("page number required"));
        }
        if flags & PAGE_HEADER_FLAG_SIZE == 0 {
            // Pre-release LTX files used one LZ4 frame per page.
            return Err(format_err("unsupported legacy LZ4-frame page encoding"));
        }
        let mut sz = [0u8; 4];
        self.read_hashed(&mut sz)?;
        let size = u32::from_be_bytes(sz) as usize;
        self.compressed.resize(size, 0);
        self.r.read_exact(&mut self.compressed)?;
        let n = lz4_flex::block::decompress_into(&self.compressed, data)
            .map_err(|e| format_err(format!("decompress page {pgno}: {e}")))?;
        if n != data.len() {
            return Err(format_err(format!("page {pgno} decompressed to {n} bytes")));
        }
        self.hash.update(data);
        self.pages_read += 1;
        if self.header.is_snapshot() && !self.header.no_checksum() && pgno != lock_pgno(self.header.page_size) {
            self.chksum = CHECKSUM_FLAG | (self.chksum ^ checksum_page(pgno, data));
        }
        Ok(Some(pgno))
    }

    fn read_uvarint(&mut self) -> Result<u64> {
        let mut v: u64 = 0;
        for shift in (0..64).step_by(7) {
            let mut b = [0u8; 1];
            self.read_hashed(&mut b)?;
            v |= ((b[0] & 0x7f) as u64) << shift;
            if b[0] & 0x80 == 0 {
                return Ok(v);
            }
        }
        Err(format_err("varint overflow"))
    }

    /// Drains any remaining pages, reads the index and trailer, and verifies
    /// the file checksum.
    pub fn finish(mut self) -> Result<Trailer> {
        let mut scratch = vec![0u8; self.header.page_size as usize];
        while self.next_page(&mut scratch)?.is_some() {}

        let index_start = self.n;
        let mut entries = 0usize;
        while self.read_uvarint()? != 0 {
            self.read_uvarint()?;
            self.read_uvarint()?;
            entries += 1;
        }
        let index_len = self.n - index_start;
        let mut size = [0u8; 8];
        self.read_hashed(&mut size)?;
        if u64::from_be_bytes(size) != index_len {
            return Err(format_err("page index size mismatch"));
        }
        if entries != self.pages_read {
            return Err(format_err(format!(
                "page index has {entries} entries but {} pages were decoded",
                self.pages_read
            )));
        }

        let mut tb = [0u8; TRAILER_SIZE];
        self.r.read_exact(&mut tb)?;
        self.hash.update(&tb[0..8]);
        let trailer = Trailer {
            post_apply_checksum: u64::from_be_bytes(tb[0..8].try_into().unwrap()),
            file_checksum: u64::from_be_bytes(tb[8..16].try_into().unwrap()),
        };
        if !self.r.fill_buf()?.is_empty() {
            return Err(format_err("unexpected data after trailer"));
        }
        if CHECKSUM_FLAG | self.hash.finalize() != trailer.file_checksum {
            return Err(Error::ChecksumMismatch);
        }
        if self.header.is_snapshot() && !self.header.no_checksum() && trailer.post_apply_checksum != self.chksum {
            return Err(format_err("post-apply checksum does not match calculated checksum"));
        }
        Ok(trailer)
    }
}

/// Merges contiguous LTX files into one. For each page the newest input wins,
/// and pages beyond the final commit are dropped.
pub fn compact<R: Read, W: Write>(mut inputs: Vec<Decoder<R>>, w: W) -> Result<(W, Header, Trailer)> {
    if inputs.is_empty() {
        return Err(format_err("at least one input required"));
    }
    for pair in inputs.windows(2) {
        let (a, b) = (pair[0].header(), pair[1].header());
        if a.page_size != b.page_size {
            return Err(format_err("input files have mismatched page sizes"));
        }
        if !(b.min_txid <= a.max_txid + 1 && b.max_txid > a.max_txid) {
            return Err(format_err(format!(
                "non-contiguous transaction ids in input files: ({},{}) -> ({},{})",
                Txid(a.min_txid),
                Txid(a.max_txid),
                Txid(b.min_txid),
                Txid(b.max_txid)
            )));
        }
    }
    let first = inputs[0].header().clone();
    let last = inputs[inputs.len() - 1].header().clone();
    let header = Header {
        flags: HEADER_FLAG_NO_CHECKSUM,
        page_size: first.page_size,
        commit: last.commit,
        min_txid: first.min_txid,
        max_txid: last.max_txid,
        timestamp: last.timestamp,
        ..Default::default()
    };
    let mut enc = Encoder::new(w, header.clone())?;

    let page_size = first.page_size as usize;
    // Current buffered page for each input: (pgno, data); pgno 0 = exhausted.
    let mut bufs: Vec<(u32, Vec<u8>)> = Vec::with_capacity(inputs.len());
    for dec in inputs.iter_mut() {
        let mut data = vec![0u8; page_size];
        let pgno = dec.next_page(&mut data)?.unwrap_or(0);
        bufs.push((pgno, data));
    }
    while let Some(pgno) = bufs.iter().map(|b| b.0).filter(|&p| p != 0).min() {
        // Newest input holding this page wins.
        let winner = bufs.iter().rposition(|b| b.0 == pgno).unwrap();
        if pgno <= header.commit {
            enc.encode_page(pgno, &bufs[winner].1)?;
        }
        for (i, dec) in inputs.iter_mut().enumerate() {
            if bufs[i].0 == pgno {
                bufs[i].0 = dec.next_page(&mut bufs[i].1)?.unwrap_or(0);
            }
        }
    }
    for dec in inputs {
        dec.finish()?;
    }
    let (w, trailer) = enc.finish(0)?;
    Ok((w, header, trailer))
}

/// Convenience: fully reads and verifies an LTX file, returning its header,
/// trailer and number of pages.
pub fn verify<R: Read>(r: R) -> Result<(Header, Trailer, usize)> {
    let mut dec = Decoder::new(r)?;
    let mut data = vec![0u8; dec.header().page_size as usize];
    let mut n = 0;
    while dec.next_page(&mut data)?.is_some() {
        n += 1;
    }
    let header = dec.header().clone();
    let trailer = dec.finish()?;
    Ok((header, trailer, n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crc64_matches_go_iso() {
        let mut h = hasher();
        h.update(b"123456789");
        assert_eq!(h.finalize(), 0xb90956c775a41001);
    }

    #[test]
    fn filename_roundtrip() {
        let name = format_filename(1, 0x2a);
        assert_eq!(name, "0000000000000001-000000000000002a.ltx");
        assert_eq!(parse_filename(&name), Some((1, 0x2a)));
        assert_eq!(parse_filename("0000000000000001-000000000000002A.ltx"), None);
        assert_eq!(parse_filename("0000000000000001-000000000000002a.ltx.tmp"), None);
    }

    #[test]
    fn lock_page() {
        assert_eq!(lock_pgno(4096), 262145);
    }

    fn header(min: u64, max: u64, commit: u32) -> Header {
        Header {
            flags: HEADER_FLAG_NO_CHECKSUM,
            page_size: 512,
            commit,
            min_txid: min,
            max_txid: max,
            timestamp: 1_700_000_000_000,
            ..Default::default()
        }
    }

    fn encode(h: Header, pages: &[(u32, u8)]) -> Vec<u8> {
        let mut enc = Encoder::new(Vec::new(), h).unwrap();
        for &(pgno, fill) in pages {
            enc.encode_page(pgno, &[fill; 512]).unwrap();
        }
        enc.finish(0).unwrap().0
    }

    #[test]
    fn roundtrip() {
        let buf = encode(header(1, 1, 3), &[(1, 1), (2, 2), (3, 3)]);
        let (h, t, n) = verify(&buf[..]).unwrap();
        assert_eq!(h, header(1, 1, 3));
        assert_eq!(n, 3);
        assert_eq!(t.post_apply_checksum, 0);
        assert!(t.file_checksum & CHECKSUM_FLAG != 0);
    }

    #[test]
    fn corruption_detected() {
        let mut buf = encode(header(1, 1, 2), &[(1, 1), (2, 2)]);
        let i = buf.len() - 20;
        buf[i] ^= 0xff;
        assert!(verify(&buf[..]).is_err());
    }

    #[test]
    fn snapshot_requires_sequential_pages() {
        let mut enc = Encoder::new(Vec::new(), header(1, 1, 3)).unwrap();
        enc.encode_page(1, &[0; 512]).unwrap();
        assert!(enc.encode_page(3, &[0; 512]).is_err());
    }

    #[test]
    fn compaction_newest_wins_and_truncates() {
        let a = encode(header(1, 1, 3), &[(1, 1), (2, 1), (3, 1)]);
        let b = encode(header(2, 2, 3), &[(2, 2)]);
        let c = encode(header(3, 3, 2), &[(1, 3)]);
        let inputs = vec![Decoder::new(&a[..]).unwrap(), Decoder::new(&b[..]).unwrap(), Decoder::new(&c[..]).unwrap()];
        let (out, h, _) = compact(inputs, Vec::new()).unwrap();
        assert_eq!((h.min_txid, h.max_txid, h.commit), (1, 3, 2));
        let mut dec = Decoder::new(&out[..]).unwrap();
        let mut data = vec![0u8; 512];
        assert_eq!(dec.next_page(&mut data).unwrap(), Some(1));
        assert_eq!(data[0], 3);
        assert_eq!(dec.next_page(&mut data).unwrap(), Some(2));
        assert_eq!(data[0], 2);
        assert_eq!(dec.next_page(&mut data).unwrap(), None);
        dec.finish().unwrap();
    }

    #[test]
    fn compaction_rejects_gaps() {
        let a = encode(header(1, 1, 1), &[(1, 1)]);
        let b = encode(header(3, 3, 1), &[(1, 2)]);
        let inputs = vec![Decoder::new(&a[..]).unwrap(), Decoder::new(&b[..]).unwrap()];
        assert!(compact(inputs, Vec::new()).is_err());
    }
}
