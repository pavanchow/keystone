//! The Keystone engine: memtable, WAL, `SSTables`, manifest and compaction wired
//! into a durable ordered key value store.

use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};

use crate::compaction;
use crate::error::Result;
use crate::iter::MergeIterator;
use crate::manifest::{Manifest, TableMeta};
use crate::memtable::MemTable;
use crate::options::Options;
use crate::sstable::{SsTableReader, SsTableWriter};
use crate::types::{Entry, ValueType, WriteOp};
use crate::wal::{WalReader, WalRecord, WalWriter};

/// A durable, ordered, embedded LSM-tree key value store.
pub struct Db {
    dir: PathBuf,
    opts: Options,
    mem: MemTable,
    wal: WalWriter,
    manifest: Manifest,
    next_seqno: u64,
    readers: HashMap<u64, SsTableReader>,
}

/// Per-level statistics for inspection.
#[derive(Debug, Clone)]
pub struct LevelStat {
    /// Level number.
    pub level: u32,
    /// Number of files at this level.
    pub files: usize,
    /// Total bytes across those files.
    pub bytes: u64,
}

/// Result of a full integrity scan over every live table.
#[derive(Debug, Clone, Copy)]
pub struct VerifyReport {
    /// Number of tables read end to end.
    pub tables: usize,
    /// Number of entries decoded across all tables.
    pub entries: u64,
}

/// A snapshot of engine state for the CLI `stats` command.
#[derive(Debug, Clone)]
pub struct Stats {
    /// Per-level file counts and sizes.
    pub levels: Vec<LevelStat>,
    /// Total live `SSTable` files.
    pub total_files: usize,
    /// Total live `SSTable` bytes.
    pub total_bytes: u64,
    /// Next sequence number to be assigned.
    pub next_seqno: u64,
    /// Approximate memtable footprint in bytes.
    pub memtable_bytes: usize,
    /// Keys currently buffered in the memtable.
    pub memtable_keys: usize,
}

fn wal_path(dir: &Path) -> PathBuf {
    dir.join("wal.log")
}

impl Db {
    /// Open (or create) a database rooted at `path`.
    pub fn open(path: impl AsRef<Path>, opts: Options) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let manifest = Manifest::load(&dir)?;

        let mut mem = MemTable::new();
        let mut max_seq = 0u64;
        let wpath = wal_path(&dir);
        if wpath.exists() {
            let mut reader = WalReader::open(&wpath)?;
            for rec in reader.read_all()? {
                max_seq = max_seq.max(rec.seqno);
                match rec.kind {
                    ValueType::Put => mem.put(&rec.key, &rec.value, rec.seqno),
                    ValueType::Delete => mem.delete(&rec.key, rec.seqno),
                }
            }
        }
        let next_seqno = manifest.next_seqno.max(max_seq + 1);
        let wal = WalWriter::open(&wpath, opts.sync_on_write)?;

        Ok(Db {
            dir,
            opts,
            mem,
            wal,
            manifest,
            next_seqno,
            readers: HashMap::new(),
        })
    }

    fn take_seqno(&mut self) -> u64 {
        let s = self.next_seqno;
        self.next_seqno += 1;
        s
    }

    /// Insert or overwrite `key` with `value`.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let seqno = self.take_seqno();
        self.wal.append(&WalRecord {
            kind: ValueType::Put,
            seqno,
            key: key.to_vec(),
            value: value.to_vec(),
        })?;
        self.mem.put(key, value, seqno);
        self.maybe_flush()?;
        Ok(())
    }

    /// Apply a batch of writes as one unit.
    ///
    /// Every record is appended to the WAL first, the whole batch is made
    /// durable with a single fsync, and only then are the records applied to
    /// the memtable. A concurrent reader therefore never observes a partial
    /// batch, and a batch of N writes costs one fsync instead of N. The last
    /// write to a key inside the batch wins. Sequence numbers are consumed
    /// for the whole batch even if the log append fails, because the records
    /// already written stay in the log and replay on recovery.
    pub fn write_batch(&mut self, ops: &[WriteOp]) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        let first = self.next_seqno;
        let records: Vec<WalRecord> = ops
            .iter()
            .enumerate()
            .map(|(i, op)| match op {
                WriteOp::Put { key, value } => WalRecord {
                    kind: ValueType::Put,
                    seqno: first + i as u64,
                    key: key.clone(),
                    value: value.clone(),
                },
                WriteOp::Delete { key } => WalRecord {
                    kind: ValueType::Delete,
                    seqno: first + i as u64,
                    key: key.clone(),
                    value: Vec::new(),
                },
            })
            .collect();
        self.next_seqno = first + records.len() as u64;
        self.wal.append_batch(&records, self.opts.sync_on_write)?;
        for rec in &records {
            match rec.kind {
                ValueType::Put => self.mem.put(&rec.key, &rec.value, rec.seqno),
                ValueType::Delete => self.mem.delete(&rec.key, rec.seqno),
            }
        }
        self.maybe_flush()?;
        Ok(())
    }

    /// Delete `key`, writing a tombstone.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let seqno = self.take_seqno();
        self.wal.append(&WalRecord {
            kind: ValueType::Delete,
            seqno,
            key: key.to_vec(),
            value: Vec::new(),
        })?;
        self.mem.delete(key, seqno);
        self.maybe_flush()?;
        Ok(())
    }

    /// Read the current value of `key`, or None if absent or deleted.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.mem.get(key) {
            return Ok(v);
        }
        // L0 newest first (higher file id is a later flush), then deeper levels.
        let mut candidates: Vec<TableMeta> = self
            .manifest
            .tables
            .iter()
            .filter(|t| key >= t.smallest_key.as_slice() && key <= t.largest_key.as_slice())
            .cloned()
            .collect();
        candidates.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| b.file_id.cmp(&a.file_id))
        });
        for meta in candidates {
            let reader = self.reader_for(meta.file_id)?;
            if let Some(entry) = reader.get(key)? {
                return match entry.kind {
                    ValueType::Put => Ok(Some(entry.value)),
                    ValueType::Delete => Ok(None),
                };
            }
        }
        Ok(None)
    }

    fn reader_for(&mut self, id: u64) -> Result<&mut SsTableReader> {
        if !self.readers.contains_key(&id) {
            let r = SsTableReader::open(&compaction::sst_path(&self.dir, id))?;
            self.readers.insert(id, r);
        }
        Ok(self.readers.get_mut(&id).unwrap())
    }

    fn maybe_flush(&mut self) -> Result<()> {
        if self.mem.approx_size() >= self.opts.memtable_size_bytes {
            self.flush()?;
            self.compact()?;
        }
        Ok(())
    }

    /// Write the memtable to a new L0 `SSTable`, commit the manifest and rotate
    /// the WAL. A no-op when the memtable is empty.
    pub fn flush(&mut self) -> Result<()> {
        if self.mem.is_empty() {
            return Ok(());
        }
        let id = self.manifest.next_file_id;
        self.manifest.next_file_id += 1;
        let path = compaction::sst_path(&self.dir, id);
        let mut w =
            SsTableWriter::create(&path, self.opts.block_size, self.opts.bloom_bits_per_key)?;
        for e in self.mem.iter() {
            w.add(&e)?;
        }
        let stats = w.finish()?;

        self.manifest.tables.push(TableMeta {
            level: 0,
            file_id: id,
            smallest_key: stats.smallest_key,
            largest_key: stats.largest_key,
            smallest_seqno: stats.smallest_seqno,
            largest_seqno: stats.largest_seqno,
            file_size: stats.file_size,
        });
        self.manifest.next_seqno = self.next_seqno;
        self.manifest.save(&self.dir)?;

        // Rotate the WAL only after the flush is durably recorded.
        self.mem = MemTable::new();
        let wpath = wal_path(&self.dir);
        let _ = std::fs::remove_file(&wpath);
        self.wal = WalWriter::open(&wpath, self.opts.sync_on_write)?;
        Ok(())
    }

    /// Run all pending compactions to keep the level shape in budget.
    pub fn compact(&mut self) -> Result<()> {
        let live_before: Vec<u64> = self.manifest.tables.iter().map(|t| t.file_id).collect();
        compaction::compact(&self.dir, &mut self.manifest, &self.opts)?;
        // Drop cached readers for files that no longer exist.
        let live_after: Vec<u64> = self.manifest.tables.iter().map(|t| t.file_id).collect();
        for id in live_before {
            if !live_after.contains(&id) {
                self.readers.remove(&id);
            }
        }
        Ok(())
    }

    /// Ordered scan over a key range, skipping deleted keys.
    pub fn scan<R: RangeBounds<Vec<u8>>>(&mut self, range: R) -> Result<Scan> {
        let lo = clone_bound(range.start_bound());
        let hi = clone_bound(range.end_bound());

        let mut sources: Vec<Box<dyn Iterator<Item = Result<Entry>>>> = Vec::new();
        let mem_entries: Vec<Entry> = self.mem.iter().collect();
        sources.push(Box::new(mem_entries.into_iter().map(Ok)));

        let ids: Vec<u64> = self.manifest.tables.iter().map(|t| t.file_id).collect();
        for id in ids {
            let reader = SsTableReader::open(&compaction::sst_path(&self.dir, id))?;
            sources.push(Box::new(reader.iter()?));
        }
        let merged = MergeIterator::new(sources)?;
        Ok(Scan {
            inner: merged,
            lo,
            hi,
            done: false,
        })
    }

    /// Flush any buffered writes and shut down cleanly.
    pub fn close(mut self) -> Result<()> {
        self.flush()?;
        self.wal.sync()?;
        Ok(())
    }

    /// Verify on-disk integrity by reading every block of every live table.
    ///
    /// Each data, index and bloom block carries a CRC32 that is checked as it
    /// is read, so any silent corruption surfaces here as an error rather than
    /// as a wrong answer at query time.
    pub fn verify(&self) -> Result<VerifyReport> {
        let mut tables = 0usize;
        let mut entries = 0u64;
        for meta in &self.manifest.tables {
            let reader = SsTableReader::open(&compaction::sst_path(&self.dir, meta.file_id))?;
            for item in reader.iter()? {
                item?;
                entries += 1;
            }
            tables += 1;
        }
        Ok(VerifyReport { tables, entries })
    }

    /// Snapshot of engine state for inspection.
    #[must_use]
    pub fn stats(&self) -> Stats {
        let max_level = self.manifest.max_level();
        let mut levels = Vec::new();
        for level in 0..=max_level {
            let tables = self.manifest.tables_at(level);
            if tables.is_empty() && level != 0 {
                continue;
            }
            levels.push(LevelStat {
                level,
                files: tables.len(),
                bytes: tables.iter().map(|t| t.file_size).sum(),
            });
        }
        Stats {
            levels,
            total_files: self.manifest.tables.len(),
            total_bytes: self.manifest.tables.iter().map(|t| t.file_size).sum(),
            next_seqno: self.next_seqno,
            memtable_bytes: self.mem.approx_size(),
            memtable_keys: self.mem.len(),
        }
    }
}

fn clone_bound(b: Bound<&Vec<u8>>) -> Bound<Vec<u8>> {
    match b {
        Bound::Included(v) => Bound::Included(v.clone()),
        Bound::Excluded(v) => Bound::Excluded(v.clone()),
        Bound::Unbounded => Bound::Unbounded,
    }
}


fn below_lower(key: &[u8], lo: &Bound<Vec<u8>>) -> bool {
    match lo {
        Bound::Included(v) => key < v.as_slice(),
        Bound::Excluded(v) => key <= v.as_slice(),
        Bound::Unbounded => false,
    }
}

fn above_upper(key: &[u8], hi: &Bound<Vec<u8>>) -> bool {
    match hi {
        Bound::Included(v) => key > v.as_slice(),
        Bound::Excluded(v) => key >= v.as_slice(),
        Bound::Unbounded => false,
    }
}

/// Ordered iterator over live key value pairs in a range.
pub struct Scan {
    inner: MergeIterator,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    done: bool,
}

impl Iterator for Scan {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            match self.inner.next() {
                None => return None,
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(e));
                }
                Some(Ok(entry)) => {
                    if below_lower(&entry.key, &self.lo) {
                        continue;
                    }
                    if above_upper(&entry.key, &self.hi) {
                        self.done = true;
                        return None;
                    }
                    if entry.kind == ValueType::Delete {
                        continue;
                    }
                    return Some(Ok((entry.key, entry.value)));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn tmp(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("keystone-batch-{}-{}", std::process::id(), name))
    }

    fn small_opts() -> Options {
        Options::new()
            .memtable_size_bytes(2 * 1024)
            .block_size(256)
            .l0_compaction_trigger(3)
    }

    fn oracle_of(db: &mut Db) -> BTreeMap<Vec<u8>, Vec<u8>> {
        db.scan(..).unwrap().map(|r| r.unwrap()).collect()
    }

    #[test]
    fn batch_is_last_write_wins_and_survives_reopen() {
        let dir = tmp("atomic");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Db::open(&dir, small_opts()).unwrap();
        db.put(b"a", b"old").unwrap();
        db.put(b"b", b"keep").unwrap();
        db.write_batch(&[]).unwrap();
        let batch = vec![
            WriteOp::Put {
                key: b"a".to_vec(),
                value: b"new".to_vec(),
            },
            WriteOp::Delete {
                key: b"b".to_vec(),
            },
            WriteOp::Put {
                key: b"c".to_vec(),
                value: b"three".to_vec(),
            },
            WriteOp::Put {
                key: b"a".to_vec(),
                value: b"newest".to_vec(),
            },
        ];
        db.write_batch(&batch).unwrap();
        assert_eq!(db.get(b"a").unwrap(), Some(b"newest".to_vec()));
        assert_eq!(db.get(b"b").unwrap(), None);
        assert_eq!(db.get(b"c").unwrap(), Some(b"three".to_vec()));
        let oracle = oracle_of(&mut db);
        // Crash: drop the handle with no clean shutdown, then reopen.
        drop(db);
        let mut db = Db::open(&dir, small_opts()).unwrap();
        assert_eq!(oracle_of(&mut db), oracle, "batch lost or torn on reopen");
        // Sequence numbers continued past the batch.
        db.put(b"d", b"four").unwrap();
        assert_eq!(db.get(b"d").unwrap(), Some(b"four".to_vec()));
        db.close().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn large_batch_flushes_and_matches_oracle() {
        let dir = tmp("large");
        let _ = std::fs::remove_dir_all(&dir);
        let mut db = Db::open(&dir, small_opts()).unwrap();
        let mut oracle = BTreeMap::new();
        let batch: Vec<WriteOp> = (0..500u32)
            .map(|i| {
                let key = format!("k{i:04}").into_bytes();
                let value = format!("v{i}").into_bytes();
                oracle.insert(key.clone(), value.clone());
                WriteOp::Put { key, value }
            })
            .collect();
        db.write_batch(&batch).unwrap();
        assert_eq!(oracle_of(&mut db), oracle);
        assert!(
            db.stats().total_files > 0,
            "a batch over the memtable budget must flush"
        );
        db.close().unwrap();
        let mut db = Db::open(&dir, small_opts()).unwrap();
        assert_eq!(oracle_of(&mut db), oracle);
        db.close().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
