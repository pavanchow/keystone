//! Durable catalog of live SSTables, written with an atomic temp-then-rename.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::crc::crc32;
use crate::error::{Error, Result};
use crate::varint;

const MAGIC: u64 = 0x4D41_4E49_4645_5354; // "MANIFEST"

/// Metadata describing one on-disk table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// Level the table lives at (0 is newest, higher is older/larger).
    pub level: u32,
    /// Unique file id, mapped to `<id>.sst` on disk.
    pub file_id: u64,
    /// Smallest user key in the table.
    pub smallest_key: Vec<u8>,
    /// Largest user key in the table.
    pub largest_key: Vec<u8>,
    /// Smallest sequence number in the table.
    pub smallest_seqno: u64,
    /// Largest sequence number in the table.
    pub largest_seqno: u64,
    /// Table file size in bytes.
    pub file_size: u64,
}

/// The complete durable state catalog.
#[derive(Debug, Clone)]
pub struct Manifest {
    /// Next file id to allocate.
    pub next_file_id: u64,
    /// Next sequence number to assign.
    pub next_seqno: u64,
    /// All live tables across all levels.
    pub tables: Vec<TableMeta>,
}

impl Manifest {
    /// Create an empty manifest for a fresh database.
    pub fn empty() -> Self {
        Manifest {
            next_file_id: 1,
            next_seqno: 1,
            tables: Vec::new(),
        }
    }

    fn manifest_path(dir: &Path) -> PathBuf {
        dir.join("MANIFEST")
    }

    fn tmp_path(dir: &Path) -> PathBuf {
        dir.join("MANIFEST.tmp")
    }

    fn serialize(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&MAGIC.to_le_bytes());
        body.extend_from_slice(&self.next_file_id.to_le_bytes());
        body.extend_from_slice(&self.next_seqno.to_le_bytes());
        varint::encode_u64(self.tables.len() as u64, &mut body);
        for t in &self.tables {
            varint::encode_u64(t.level as u64, &mut body);
            body.extend_from_slice(&t.file_id.to_le_bytes());
            varint::encode_bytes(&t.smallest_key, &mut body);
            varint::encode_bytes(&t.largest_key, &mut body);
            body.extend_from_slice(&t.smallest_seqno.to_le_bytes());
            body.extend_from_slice(&t.largest_seqno.to_le_bytes());
            body.extend_from_slice(&t.file_size.to_le_bytes());
        }
        let mut out = body.clone();
        out.extend_from_slice(&crc32(&body).to_le_bytes());
        out
    }

    fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 4 {
            return Err(Error::corruption("manifest too short"));
        }
        let body = &buf[..buf.len() - 4];
        let want_crc = u32::from_le_bytes(buf[buf.len() - 4..].try_into().unwrap());
        if crc32(body) != want_crc {
            return Err(Error::corruption("manifest crc mismatch"));
        }
        let mut pos = 0;
        let read_u64 = |b: &[u8], p: &mut usize| -> Result<u64> {
            let s = b
                .get(*p..*p + 8)
                .ok_or_else(|| Error::corruption("manifest u64 truncated"))?;
            *p += 8;
            Ok(u64::from_le_bytes(s.try_into().unwrap()))
        };
        let magic = read_u64(body, &mut pos)?;
        if magic != MAGIC {
            return Err(Error::corruption("manifest bad magic"));
        }
        let next_file_id = read_u64(body, &mut pos)?;
        let next_seqno = read_u64(body, &mut pos)?;
        let n = varint::decode_u64(body, &mut pos)? as usize;
        let mut tables = Vec::with_capacity(n);
        for _ in 0..n {
            let level = varint::decode_u64(body, &mut pos)? as u32;
            let file_id = read_u64(body, &mut pos)?;
            let smallest_key = varint::decode_bytes(body, &mut pos)?.to_vec();
            let largest_key = varint::decode_bytes(body, &mut pos)?.to_vec();
            let smallest_seqno = read_u64(body, &mut pos)?;
            let largest_seqno = read_u64(body, &mut pos)?;
            let file_size = read_u64(body, &mut pos)?;
            tables.push(TableMeta {
                level,
                file_id,
                smallest_key,
                largest_key,
                smallest_seqno,
                largest_seqno,
                file_size,
            });
        }
        Ok(Manifest {
            next_file_id,
            next_seqno,
            tables,
        })
    }

    /// Load the manifest from `dir`, or return an empty one if none exists.
    pub fn load(dir: &Path) -> Result<Self> {
        let path = Self::manifest_path(dir);
        match fs::read(&path) {
            Ok(buf) => Self::deserialize(&buf),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Manifest::empty()),
            Err(e) => Err(Error::Io(e)),
        }
    }

    /// Atomically persist by writing a temp file, fsyncing, then renaming.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let tmp = Self::tmp_path(dir);
        let final_path = Self::manifest_path(dir);
        let bytes = self.serialize();
        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &final_path)?;
        // Fsync the directory so the rename itself is durable.
        if let Ok(dirf) = fs::File::open(dir) {
            let _ = dirf.sync_all();
        }
        Ok(())
    }

    /// Tables at a given level.
    pub fn tables_at(&self, level: u32) -> Vec<&TableMeta> {
        self.tables.iter().filter(|t| t.level == level).collect()
    }

    /// Highest level that currently holds any table.
    pub fn max_level(&self) -> u32 {
        self.tables.iter().map(|t| t.level).max().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("keystone-man-{}-{}", std::process::id(), name));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        p
    }

    fn sample() -> Manifest {
        Manifest {
            next_file_id: 7,
            next_seqno: 42,
            tables: vec![
                TableMeta {
                    level: 0,
                    file_id: 1,
                    smallest_key: b"a".to_vec(),
                    largest_key: b"m".to_vec(),
                    smallest_seqno: 1,
                    largest_seqno: 10,
                    file_size: 1234,
                },
                TableMeta {
                    level: 1,
                    file_id: 2,
                    smallest_key: b"n".to_vec(),
                    largest_key: b"z".to_vec(),
                    smallest_seqno: 11,
                    largest_seqno: 20,
                    file_size: 5678,
                },
            ],
        }
    }

    #[test]
    fn save_load_round_trip() {
        let dir = tmpdir("rt");
        let m = sample();
        m.save(&dir).unwrap();
        let loaded = Manifest::load(&dir).unwrap();
        assert_eq!(loaded.next_file_id, 7);
        assert_eq!(loaded.next_seqno, 42);
        assert_eq!(loaded.tables.len(), 2);
        assert_eq!(loaded.tables[1].largest_key, b"z".to_vec());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn missing_manifest_is_empty() {
        let dir = tmpdir("empty");
        let m = Manifest::load(&dir).unwrap();
        assert_eq!(m.next_file_id, 1);
        assert_eq!(m.next_seqno, 1);
        assert!(m.tables.is_empty());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn crash_between_tmp_and_rename_keeps_old() {
        let dir = tmpdir("crash");
        // Commit a good manifest first.
        let old = sample();
        old.save(&dir).unwrap();
        // Simulate a crash: a new temp is written but the rename never happens.
        let mut newer = sample();
        newer.next_seqno = 999;
        let bytes = newer.serialize();
        let tmp = Manifest::tmp_path(&dir);
        {
            let mut f = fs::File::create(&tmp).unwrap();
            f.write_all(&bytes).unwrap();
            f.sync_all().unwrap();
        }
        // Recovery loads the committed MANIFEST, never the dangling temp.
        let loaded = Manifest::load(&dir).unwrap();
        assert_eq!(loaded.next_seqno, 42);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn corrupt_crc_rejected() {
        let dir = tmpdir("corrupt");
        let m = sample();
        m.save(&dir).unwrap();
        let path = Manifest::manifest_path(&dir);
        let mut bytes = fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n / 2] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        assert!(Manifest::load(&dir).is_err());
        fs::remove_dir_all(&dir).unwrap();
    }
}
