//! Immutable on-disk sorted string table.
//!
//! Layout: `[data blocks...][index block][bloom block][footer]`.
//! A data block packs sorted entries up to roughly `block_size` bytes, each
//! `[varint klen][key][u64 seqno][u8 type][varint vlen][value]`. The index
//! block maps the first key of every data block to its offset and length. The
//! bloom block is the serialized filter over all keys. The fixed 40 byte footer
//! is `[u64 index_off][u64 index_len][u64 bloom_off][u64 bloom_len][u64 magic]`.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::bloom::Bloom;
use crate::error::{Error, Result};
use crate::types::{Entry, ValueType};
use crate::varint;

const MAGIC: u64 = 0x4B45_5953_544F_4E45; // "KEYSTONE"
const FOOTER_LEN: u64 = 40;

/// Summary statistics produced when a table is finished.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Smallest user key in the table.
    pub smallest_key: Vec<u8>,
    /// Largest user key in the table.
    pub largest_key: Vec<u8>,
    /// Smallest sequence number in the table.
    pub smallest_seqno: u64,
    /// Largest sequence number in the table.
    pub largest_seqno: u64,
    /// Total file size in bytes.
    pub file_size: u64,
    /// Number of entries written.
    pub count: u64,
}

fn encode_entry(e: &Entry, out: &mut Vec<u8>) {
    varint::encode_bytes(&e.key, out);
    out.extend_from_slice(&e.seqno.to_le_bytes());
    out.push(e.kind.as_u8());
    varint::encode_bytes(&e.value, out);
}

fn decode_entry(buf: &[u8], pos: &mut usize) -> Result<Entry> {
    let key = varint::decode_bytes(buf, pos)?.to_vec();
    let seq_bytes = buf
        .get(*pos..*pos + 8)
        .ok_or_else(|| Error::corruption("sst entry seqno truncated"))?;
    let seqno = u64::from_le_bytes(seq_bytes.try_into().unwrap());
    *pos += 8;
    let kind_byte = *buf
        .get(*pos)
        .ok_or_else(|| Error::corruption("sst entry type truncated"))?;
    *pos += 1;
    let kind =
        ValueType::from_u8(kind_byte).ok_or_else(|| Error::corruption("sst bad type byte"))?;
    let value = varint::decode_bytes(buf, pos)?.to_vec();
    Ok(Entry {
        key,
        seqno,
        kind,
        value,
    })
}

/// Streaming writer that consumes entries in ascending key order.
pub struct SsTableWriter {
    writer: BufWriter<File>,
    offset: u64,
    block_buf: Vec<u8>,
    block_first_key: Option<Vec<u8>>,
    index: Vec<(Vec<u8>, u64, u64)>,
    keys: Vec<Vec<u8>>,
    block_size: usize,
    bloom_bits_per_key: usize,
    smallest_key: Option<Vec<u8>>,
    largest_key: Vec<u8>,
    min_seq: u64,
    max_seq: u64,
    count: u64,
}

impl SsTableWriter {
    /// Create a new table file at `path`.
    pub fn create(path: &Path, block_size: usize, bloom_bits_per_key: usize) -> Result<Self> {
        let file = File::create(path)?;
        Ok(SsTableWriter {
            writer: BufWriter::new(file),
            offset: 0,
            block_buf: Vec::new(),
            block_first_key: None,
            index: Vec::new(),
            keys: Vec::new(),
            block_size,
            bloom_bits_per_key,
            smallest_key: None,
            largest_key: Vec::new(),
            min_seq: u64::MAX,
            max_seq: 0,
            count: 0,
        })
    }

    /// Add one entry. Callers must supply entries in ascending key order.
    pub fn add(&mut self, e: &Entry) -> Result<()> {
        if self.smallest_key.is_none() {
            self.smallest_key = Some(e.key.clone());
        }
        self.largest_key = e.key.clone();
        self.min_seq = self.min_seq.min(e.seqno);
        self.max_seq = self.max_seq.max(e.seqno);
        self.count += 1;
        self.keys.push(e.key.clone());

        if self.block_first_key.is_none() {
            self.block_first_key = Some(e.key.clone());
        }
        encode_entry(e, &mut self.block_buf);
        if self.block_buf.len() >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<()> {
        if self.block_buf.is_empty() {
            return Ok(());
        }
        let first = self.block_first_key.take().unwrap();
        let len = self.block_buf.len() as u64;
        self.writer.write_all(&self.block_buf)?;
        self.index.push((first, self.offset, len));
        self.offset += len;
        self.block_buf.clear();
        Ok(())
    }

    /// Finish the table, writing index, bloom and footer.
    pub fn finish(mut self) -> Result<TableStats> {
        self.flush_block()?;

        let index_off = self.offset;
        let mut index_buf = Vec::new();
        varint::encode_u64(self.index.len() as u64, &mut index_buf);
        for (key, off, len) in &self.index {
            varint::encode_bytes(key, &mut index_buf);
            index_buf.extend_from_slice(&off.to_le_bytes());
            index_buf.extend_from_slice(&len.to_le_bytes());
        }
        self.writer.write_all(&index_buf)?;
        let index_len = index_buf.len() as u64;
        self.offset += index_len;

        let mut bloom = Bloom::new(self.keys.len(), self.bloom_bits_per_key);
        for k in &self.keys {
            bloom.add(k);
        }
        let bloom_buf = bloom.to_bytes();
        let bloom_off = self.offset;
        self.writer.write_all(&bloom_buf)?;
        let bloom_len = bloom_buf.len() as u64;
        self.offset += bloom_len;

        let mut footer = Vec::with_capacity(FOOTER_LEN as usize);
        footer.extend_from_slice(&index_off.to_le_bytes());
        footer.extend_from_slice(&index_len.to_le_bytes());
        footer.extend_from_slice(&bloom_off.to_le_bytes());
        footer.extend_from_slice(&bloom_len.to_le_bytes());
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        self.writer.write_all(&footer)?;
        self.offset += FOOTER_LEN;

        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        Ok(TableStats {
            smallest_key: self.smallest_key.unwrap_or_default(),
            largest_key: self.largest_key,
            smallest_seqno: if self.count == 0 { 0 } else { self.min_seq },
            largest_seqno: self.max_seq,
            file_size: self.offset,
            count: self.count,
        })
    }
}

/// Reader over a finished table. Loads footer, index and bloom eagerly.
pub struct SsTableReader {
    file: File,
    index: Vec<(Vec<u8>, u64, u64)>,
    bloom: Bloom,
}

impl SsTableReader {
    /// Open and parse the table at `path`.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < FOOTER_LEN {
            return Err(Error::corruption("sst smaller than footer"));
        }
        file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer = [0u8; FOOTER_LEN as usize];
        file.read_exact(&mut footer)?;
        let index_off = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_len = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let bloom_off = u64::from_le_bytes(footer[16..24].try_into().unwrap());
        let bloom_len = u64::from_le_bytes(footer[24..32].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[32..40].try_into().unwrap());
        if magic != MAGIC {
            return Err(Error::corruption("sst bad magic"));
        }

        let index_raw = read_at(&mut file, index_off, index_len as usize)?;
        let mut pos = 0;
        let n = varint::decode_u64(&index_raw, &mut pos)? as usize;
        let mut index = Vec::with_capacity(n);
        for _ in 0..n {
            let key = varint::decode_bytes(&index_raw, &mut pos)?.to_vec();
            let off_bytes = index_raw
                .get(pos..pos + 8)
                .ok_or_else(|| Error::corruption("index off truncated"))?;
            let off = u64::from_le_bytes(off_bytes.try_into().unwrap());
            pos += 8;
            let len_bytes = index_raw
                .get(pos..pos + 8)
                .ok_or_else(|| Error::corruption("index len truncated"))?;
            let len = u64::from_le_bytes(len_bytes.try_into().unwrap());
            pos += 8;
            index.push((key, off, len));
        }

        let bloom_raw = read_at(&mut file, bloom_off, bloom_len as usize)?;
        let bloom = Bloom::from_bytes(&bloom_raw)?;

        Ok(SsTableReader { file, index, bloom })
    }

    /// Point lookup honoring the bloom filter and block index.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Entry>> {
        if !self.bloom.may_contain(key) {
            return Ok(None);
        }
        let block_idx = match self.find_block(key) {
            Some(i) => i,
            None => return Ok(None),
        };
        let (_, off, len) = self.index[block_idx].clone();
        let block = read_at(&mut self.file, off, len as usize)?;
        let mut pos = 0;
        while pos < block.len() {
            let e = decode_entry(&block, &mut pos)?;
            match e.key.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(e)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    // Index of the last block whose first key is <= key.
    fn find_block(&self, key: &[u8]) -> Option<usize> {
        if self.index.is_empty() || key < self.index[0].0.as_slice() {
            return None;
        }
        let mut lo = 0usize;
        let mut hi = self.index.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.index[mid].0.as_slice() <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Some(lo - 1)
    }

    /// Iterate all entries in ascending key order.
    pub fn iter(&self) -> Result<SsTableIter> {
        let file = self.file.try_clone()?;
        Ok(SsTableIter {
            file,
            index: self.index.clone(),
            block_idx: 0,
            block: Vec::new(),
            block_pos: 0,
            loaded: false,
        })
    }
}

/// Forward iterator over every entry in a table.
pub struct SsTableIter {
    file: File,
    index: Vec<(Vec<u8>, u64, u64)>,
    block_idx: usize,
    block: Vec<u8>,
    block_pos: usize,
    loaded: bool,
}

impl SsTableIter {
    fn load_block(&mut self) -> Result<bool> {
        while self.block_idx < self.index.len() {
            let (_, off, len) = self.index[self.block_idx].clone();
            self.block = read_at(&mut self.file, off, len as usize)?;
            self.block_pos = 0;
            self.block_idx += 1;
            if !self.block.is_empty() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl Iterator for SsTableIter {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if !self.loaded || self.block_pos >= self.block.len() {
                match self.load_block() {
                    Ok(true) => self.loaded = true,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            }
            if self.block_pos < self.block.len() {
                return Some(decode_entry(&self.block, &mut self.block_pos));
            }
        }
    }
}

fn read_at(file: &mut File, off: u64, len: usize) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(off))?;
    let mut buf = vec![0u8; len];
    file.read_exact(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("keystone-sst-{}-{}", std::process::id(), name));
        p
    }

    fn entry(k: &str, seq: u64, v: Option<&str>) -> Entry {
        match v {
            Some(v) => Entry {
                key: k.as_bytes().to_vec(),
                seqno: seq,
                kind: ValueType::Put,
                value: v.as_bytes().to_vec(),
            },
            None => Entry {
                key: k.as_bytes().to_vec(),
                seqno: seq,
                kind: ValueType::Delete,
                value: Vec::new(),
            },
        }
    }

    #[test]
    fn write_read_iter_round_trip() {
        let path = tmp("rt");
        let _ = std::fs::remove_file(&path);
        let mut entries: Vec<Entry> = (0..500u32)
            .map(|i| entry(&format!("key{i:04}"), i as u64, Some(&format!("val{i}"))))
            .collect();
        entries.push(entry("key0250", 999, None));
        entries.sort_by(|a, b| a.key.cmp(&b.key));
        entries.dedup_by(|a, b| a.key == b.key);

        let mut w = SsTableWriter::create(&path, 256, 10).unwrap();
        for e in &entries {
            w.add(e).unwrap();
        }
        let stats = w.finish().unwrap();
        assert_eq!(stats.count as usize, entries.len());
        assert_eq!(stats.smallest_key, b"key0000".to_vec());

        let mut r = SsTableReader::open(&path).unwrap();
        for e in &entries {
            let got = r.get(&e.key).unwrap().unwrap();
            assert_eq!(got.key, e.key);
            assert_eq!(got.kind, e.kind);
            assert_eq!(got.value, e.value);
        }
        assert!(r.get(b"missing").unwrap().is_none());

        let collected: Vec<Entry> = r.iter().unwrap().map(|e| e.unwrap()).collect();
        assert_eq!(collected, entries);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn bad_magic_rejected() {
        let path = tmp("magic");
        let _ = std::fs::remove_file(&path);
        let mut w = SsTableWriter::create(&path, 256, 10).unwrap();
        w.add(&entry("a", 1, Some("1"))).unwrap();
        w.finish().unwrap();
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            f.seek(SeekFrom::End(-8)).unwrap();
            f.write_all(&[0u8; 8]).unwrap();
        }
        assert!(SsTableReader::open(&path).is_err());
        std::fs::remove_file(&path).unwrap();
    }
}
