//! Write-ahead log for crash durability.
//!
//! Record framing on disk is `[u32 payload_len][u32 crc32(payload)][payload]`.
//! The payload is `[u8 type][u64 seqno][varint klen][key][varint vlen][value]`
//! where the value is absent for a delete.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::Path;

use crate::crc::crc32;
use crate::error::{Error, Result};
use crate::types::ValueType;
use crate::varint;

// A record larger than this in the length prefix is treated as a corrupt or
// torn tail rather than trusted, so a bit-flipped length cannot trigger a
// multi-gigabyte allocation before the CRC is ever checked.
const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;

/// A logical operation recovered from the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    /// Put or delete.
    pub kind: ValueType,
    /// Sequence number assigned at write time.
    pub seqno: u64,
    /// User key.
    pub key: Vec<u8>,
    /// Value bytes, empty for a delete.
    pub value: Vec<u8>,
}

fn encode_payload(rec: &WalRecord) -> Vec<u8> {
    let mut p = Vec::with_capacity(1 + 8 + rec.key.len() + rec.value.len() + 8);
    p.push(rec.kind.as_u8());
    p.extend_from_slice(&rec.seqno.to_le_bytes());
    varint::encode_bytes(&rec.key, &mut p);
    if rec.kind == ValueType::Put {
        varint::encode_bytes(&rec.value, &mut p);
    }
    p
}

fn decode_payload(payload: &[u8]) -> Result<WalRecord> {
    let mut pos = 0;
    let kind_byte = *payload
        .get(pos)
        .ok_or_else(|| Error::corruption("wal payload empty"))?;
    pos += 1;
    let kind =
        ValueType::from_u8(kind_byte).ok_or_else(|| Error::corruption("wal bad type byte"))?;
    let seq_bytes = payload
        .get(pos..pos + 8)
        .ok_or_else(|| Error::corruption("wal seqno truncated"))?;
    let seqno = u64::from_le_bytes(seq_bytes.try_into().unwrap());
    pos += 8;
    let key = varint::decode_bytes(payload, &mut pos)?.to_vec();
    let value = if kind == ValueType::Put {
        varint::decode_bytes(payload, &mut pos)?.to_vec()
    } else {
        Vec::new()
    };
    Ok(WalRecord {
        kind,
        seqno,
        key,
        value,
    })
}

/// Appends records to a log file.
pub struct WalWriter {
    file: File,
    sync: bool,
}

impl WalWriter {
    /// Open (creating if needed) a log for appending.
    pub fn open(path: &Path, sync: bool) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(WalWriter { file, sync })
    }

    /// Append a single record, optionally fsyncing.
    pub fn append(&mut self, rec: &WalRecord) -> Result<()> {
        let payload = encode_payload(rec);
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc32(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        self.file.write_all(&frame)?;
        if self.sync {
            self.file.sync_all()?;
        }
        Ok(())
    }

    /// Force any buffered data to stable storage.
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }
}

/// Replays records from a log file, discarding a torn or corrupt tail.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    /// Open a log for replay.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        Ok(WalReader {
            reader: BufReader::new(file),
        })
    }

    /// Read every intact record in order.
    ///
    /// A short read on the header or payload, or a checksum mismatch, is
    /// treated as a torn tail from an interrupted write and stops replay
    /// cleanly without error so earlier records survive.
    pub fn read_all(&mut self) -> Result<Vec<WalRecord>> {
        let mut out = Vec::new();
        loop {
            let mut header = [0u8; 8];
            match read_exact_or_eof(&mut self.reader, &mut header)? {
                ReadState::Eof | ReadState::Short => break,
                ReadState::Full => {}
            }
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
            // A corrupt length prefix must not drive a huge allocation: an
            // implausible length is a torn or corrupt tail, so stop cleanly.
            if len > MAX_RECORD_LEN {
                break;
            }
            let mut payload = vec![0u8; len];
            match read_exact_or_eof(&mut self.reader, &mut payload)? {
                ReadState::Full => {}
                _ => break,
            }
            if crc32(&payload) != expected_crc {
                break;
            }
            match decode_payload(&payload) {
                Ok(rec) => out.push(rec),
                Err(_) => break,
            }
        }
        Ok(out)
    }
}

enum ReadState {
    Full,
    Short,
    Eof,
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<ReadState> {
    let mut filled = 0;
    while filled < buf.len() {
        match r.read(&mut buf[filled..]) {
            Ok(0) => {
                return Ok(if filled == 0 {
                    ReadState::Eof
                } else {
                    ReadState::Short
                });
            }
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(Error::Io(e)),
        }
    }
    Ok(ReadState::Full)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};

    fn tmp(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("keystone-wal-{}-{}", std::process::id(), name));
        p
    }

    #[test]
    fn append_and_replay() {
        let path = tmp("replay");
        let _ = std::fs::remove_file(&path);
        {
            let mut w = WalWriter::open(&path, true).unwrap();
            w.append(&WalRecord {
                kind: ValueType::Put,
                seqno: 1,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord {
                kind: ValueType::Delete,
                seqno: 2,
                key: b"a".to_vec(),
                value: Vec::new(),
            })
            .unwrap();
        }
        let mut r = WalReader::open(&path).unwrap();
        let recs = r.read_all().unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[0].kind, ValueType::Put);
        assert_eq!(recs[1].kind, ValueType::Delete);
        assert_eq!(recs[1].seqno, 2);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn bad_crc_tail_discarded() {
        let path = tmp("badcrc");
        let _ = std::fs::remove_file(&path);
        {
            let mut w = WalWriter::open(&path, true).unwrap();
            w.append(&WalRecord {
                kind: ValueType::Put,
                seqno: 1,
                key: b"good".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord {
                kind: ValueType::Put,
                seqno: 2,
                key: b"corruptme".to_vec(),
                value: b"v".to_vec(),
            })
            .unwrap();
        }
        // Flip a byte inside the last record's payload.
        {
            let mut f = OpenOptions::new().write(true).open(&path).unwrap();
            let end = f.seek(SeekFrom::End(-1)).unwrap();
            let _ = end;
            f.write_all(&[0xff]).unwrap();
        }
        let mut r = WalReader::open(&path).unwrap();
        let recs = r.read_all().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key, b"good".to_vec());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn torn_short_tail_discarded() {
        let path = tmp("torn");
        let _ = std::fs::remove_file(&path);
        {
            let mut w = WalWriter::open(&path, true).unwrap();
            w.append(&WalRecord {
                kind: ValueType::Put,
                seqno: 1,
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            })
            .unwrap();
            w.append(&WalRecord {
                kind: ValueType::Put,
                seqno: 2,
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            })
            .unwrap();
        }
        // Truncate mid-way through the second record.
        let full = std::fs::metadata(&path).unwrap().len();
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(full - 3).unwrap();
        drop(f);
        let mut r = WalReader::open(&path).unwrap();
        let recs = r.read_all().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].key, b"a".to_vec());
        std::fs::remove_file(&path).unwrap();
    }
}
