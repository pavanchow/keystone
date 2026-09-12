//! The keystone-server wire protocol codec.
//!
//! The protocol is a sequence of frames over any reliable byte stream, usually
//! a TCP connection. A frame is
//!
//! ```text
//! [u32 payload_len][payload]
//! ```
//!
//! with the length little endian. A request payload starts with an operation
//! tag followed by an operation specific body. A response payload starts with
//! a status tag followed by a status specific body. Keys and values are
//! length prefixed with LEB128 varints, the same encoding the on-disk formats
//! use. The full specification, including a hex worked example and the caps,
//! lives in `DESIGN.md`.
//!
//! The exact bytes of a put request and its response:
//!
//! ```
//! use keystone::wire;
//!
//! // put key "k" with value "v"
//! let body = wire::encode_put_body(b"k", b"v");
//! let frame = wire::encode_request_frame(wire::OP_PUT, &body);
//! assert_eq!(frame, [0x05, 0x00, 0x00, 0x00, 0x02, 0x01, 0x6b, 0x01, 0x76]);
//!
//! // the server answers one ok frame
//! assert_eq!(wire::frame_ok(), [0x01, 0x00, 0x00, 0x00, 0x00]);
//! ```

use std::io::{ErrorKind, Read, Write};
use std::ops::Bound;

use crate::db::{LevelStat, Stats};
use crate::error::{Error, Result};
use crate::varint;

/// Ping the server for liveness.
pub const OP_PING: u8 = 1;
/// Store a key and a value.
pub const OP_PUT: u8 = 2;
/// Read the value of a key.
pub const OP_GET: u8 = 3;
/// Delete a key.
pub const OP_DELETE: u8 = 4;
/// Read one ordered page of a key range.
pub const OP_SCAN: u8 = 5;
/// Open a per connection write transaction.
pub const OP_BEGIN: u8 = 6;
/// Apply the buffered writes of the open transaction as one batch.
pub const OP_COMMIT: u8 = 7;
/// Discard the buffered writes of the open transaction.
pub const OP_ABORT: u8 = 8;
/// Read a snapshot of engine statistics.
pub const OP_STATS: u8 = 9;

/// The request succeeded.
pub const STATUS_OK: u8 = 0;
/// A get found no live value for the key.
pub const STATUS_NOT_FOUND: u8 = 1;
/// The request was refused, the body carries a UTF-8 message.
pub const STATUS_ERROR: u8 = 2;

/// A scan range start or end that is unbounded.
pub const BOUND_UNBOUNDED: u8 = 0;
/// A scan range bound that includes its key.
pub const BOUND_INCLUDED: u8 = 1;
/// A scan range bound that excludes its key.
pub const BOUND_EXCLUDED: u8 = 2;

/// Default cap on a single frame payload, enforced by client and server alike
/// so a hostile peer length prefix can never drive a large allocation.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// Human readable name of an operation tag, for error messages.
#[must_use]
pub fn op_name(op: u8) -> &'static str {
    match op {
        OP_PING => "ping",
        OP_PUT => "put",
        OP_GET => "get",
        OP_DELETE => "delete",
        OP_SCAN => "scan",
        OP_BEGIN => "begin",
        OP_COMMIT => "commit",
        OP_ABORT => "abort",
        OP_STATS => "stats",
        _ => "unknown",
    }
}

/// A decoded request payload.
#[derive(Debug)]
pub struct Request {
    /// The operation tag.
    pub op: u8,
    /// The operation body, parsed further with [`Body`].
    pub body: Vec<u8>,
}

/// Decode a request payload into the operation tag and its body.
pub fn decode_request(payload: &[u8]) -> Result<Request> {
    let (&op, body) = payload
        .split_first()
        .ok_or_else(|| Error::Protocol("empty request payload".to_string()))?;
    Ok(Request {
        op,
        body: body.to_vec(),
    })
}

/// Incremental decoder for a request or response body.
pub struct Body<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Body<'a> {
    /// Wrap a body buffer.
    #[must_use]
    pub fn new(buf: &'a [u8]) -> Self {
        Body { buf, pos: 0 }
    }

    /// Take one byte.
    pub fn take_u8(&mut self) -> Result<u8> {
        let b = *self
            .buf
            .get(self.pos)
            .ok_or_else(|| Error::Protocol("body truncated at u8".to_string()))?;
        self.pos += 1;
        Ok(b)
    }

    /// Take a little endian u32.
    pub fn take_u32(&mut self) -> Result<u32> {
        let end = self
            .pos
            .checked_add(4)
            .ok_or_else(|| Error::Protocol("body length overflow".to_string()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Error::Protocol("body truncated at u32".to_string()))?;
        self.pos = end;
        let arr: [u8; 4] = bytes
            .try_into()
            .expect("slice of exactly 4 bytes by construction");
        Ok(u32::from_le_bytes(arr))
    }

    /// Take a little endian u64.
    pub fn take_u64(&mut self) -> Result<u64> {
        let end = self
            .pos
            .checked_add(8)
            .ok_or_else(|| Error::Protocol("body length overflow".to_string()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Error::Protocol("body truncated at u64".to_string()))?;
        self.pos = end;
        let arr: [u8; 8] = bytes
            .try_into()
            .expect("slice of exactly 8 bytes by construction");
        Ok(u64::from_le_bytes(arr))
    }

    /// Take a LEB128 varint.
    pub fn take_varint(&mut self) -> Result<u64> {
        let v = varint::decode_u64(self.buf, &mut self.pos)
            .map_err(|e| Error::Protocol(format!("bad varint in body: {e}")))?;
        Ok(v)
    }

    /// Take a varint length prefixed byte slice.
    pub fn take_bytes(&mut self) -> Result<&'a [u8]> {
        let len = self.take_varint()?;
        let len = usize::try_from(len)
            .map_err(|_| Error::Protocol("varint length exceeds address space".to_string()))?;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Error::Protocol("body length overflow".to_string()))?;
        let bytes = self
            .buf
            .get(self.pos..end)
            .ok_or_else(|| Error::Protocol("body truncated at length prefixed bytes".to_string()))?;
        self.pos = end;
        Ok(bytes)
    }

    /// Take a scan range bound: a kind byte followed, for bounded kinds, by a
    /// varint length prefixed key.
    pub fn take_bound(&mut self) -> Result<Bound<Vec<u8>>> {
        let kind = self.take_u8()?;
        match kind {
            BOUND_UNBOUNDED => Ok(Bound::Unbounded),
            BOUND_INCLUDED => Ok(Bound::Included(self.take_bytes()?.to_vec())),
            BOUND_EXCLUDED => Ok(Bound::Excluded(self.take_bytes()?.to_vec())),
            other => Err(Error::Protocol(format!("bad bound kind {other}"))),
        }
    }

    /// Succeed only when the whole body has been consumed.
    pub fn finish(&self) -> Result<()> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(Error::Protocol(format!(
                "{} trailing bytes after request body",
                self.buf.len() - self.pos
            )))
        }
    }
}

/// Read one frame. Returns `None` for a clean connection close at a frame
/// boundary. A length over `max_frame` is a protocol error, never an
/// allocation.
pub fn read_frame<R: Read>(r: &mut R, max_frame: usize) -> Result<Option<Vec<u8>>> {
    let mut first = [0u8; 1];
    let n = r.read(&mut first)?;
    if n == 0 {
        return Ok(None);
    }
    let mut rest = [0u8; 3];
    read_exact(r, &mut rest)?;
    let len = u32::from_le_bytes([first[0], rest[0], rest[1], rest[2]]) as usize;
    if len > max_frame {
        return Err(Error::Protocol(format!(
            "frame of {len} bytes exceeds cap of {max_frame}"
        )));
    }
    let mut payload = vec![0u8; len];
    read_exact(r, &mut payload)?;
    Ok(Some(payload))
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    match r.read_exact(buf) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => Err(Error::Protocol(
            "connection closed mid-frame".to_string(),
        )),
        Err(e) => Err(Error::Io(e)),
    }
}

/// Write one frame and flush the stream.
pub fn write_frame<W: Write>(w: &mut W, payload: &[u8]) -> Result<()> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    w.write_all(&frame)?;
    w.flush()?;
    Ok(())
}

fn frame_of(payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    frame
}

/// Build a full request frame from an operation tag and a body.
#[must_use]
pub fn encode_request_frame(op: u8, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + body.len());
    payload.push(op);
    payload.extend_from_slice(body);
    frame_of(&payload)
}

/// Body of a put request: length prefixed key then length prefixed value.
#[must_use]
pub fn encode_put_body(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(
        varint::encoded_len(key.len() as u64)
            + key.len()
            + varint::encoded_len(value.len() as u64)
            + value.len(),
    );
    varint::encode_bytes(key, &mut b);
    varint::encode_bytes(value, &mut b);
    b
}

/// Body of a get or delete request: a length prefixed key.
#[must_use]
pub fn encode_key_body(key: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(varint::encoded_len(key.len() as u64) + key.len());
    varint::encode_bytes(key, &mut b);
    b
}

/// Body of a scan request: two range bounds and a page limit.
#[must_use]
pub fn encode_scan_body(
    lo: &Bound<Vec<u8>>,
    hi: &Bound<Vec<u8>>,
    limit: u32,
) -> Vec<u8> {
    let mut b = Vec::new();
    push_bound(&mut b, lo);
    push_bound(&mut b, hi);
    b.extend_from_slice(&limit.to_le_bytes());
    b
}

fn push_bound(b: &mut Vec<u8>, bound: &Bound<Vec<u8>>) {
    match bound {
        Bound::Unbounded => b.push(BOUND_UNBOUNDED),
        Bound::Included(k) => {
            b.push(BOUND_INCLUDED);
            varint::encode_bytes(k, b);
        }
        Bound::Excluded(k) => {
            b.push(BOUND_EXCLUDED);
            varint::encode_bytes(k, b);
        }
    }
}

/// Frame of a plain ok response.
#[must_use]
pub fn frame_ok() -> Vec<u8> {
    frame_of(&[STATUS_OK])
}

/// Frame of a get miss response.
#[must_use]
pub fn frame_not_found() -> Vec<u8> {
    frame_of(&[STATUS_NOT_FOUND])
}

/// Frame of an error response carrying a UTF-8 message.
#[must_use]
pub fn frame_error(msg: &str) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + varint::encoded_len(msg.len() as u64) + msg.len());
    payload.push(STATUS_ERROR);
    varint::encode_bytes(msg.as_bytes(), &mut payload);
    frame_of(&payload)
}

/// Frame of a get hit response carrying a length prefixed value.
#[must_use]
pub fn frame_value(value: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(1 + varint::encoded_len(value.len() as u64) + value.len());
    payload.push(STATUS_OK);
    varint::encode_bytes(value, &mut payload);
    frame_of(&payload)
}

/// Frame of one scan page: ok status, a has-more byte, a varint count, then
/// that many length prefixed key value pairs.
pub fn frame_scan_page(has_more: bool, pairs: &[(Vec<u8>, Vec<u8>)], out: &mut Vec<u8>) {
    let mut payload = Vec::new();
    payload.push(STATUS_OK);
    payload.push(u8::from(has_more));
    varint::encode_u64(pairs.len() as u64, &mut payload);
    for (k, v) in pairs {
        varint::encode_bytes(k, &mut payload);
        varint::encode_bytes(v, &mut payload);
    }
    out.clear();
    out.extend_from_slice(&frame_of(&payload));
}

/// Frame of a stats response.
#[must_use]
pub fn frame_stats(stats: &Stats) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.push(STATUS_OK);
    payload.extend_from_slice(&stats.next_seqno.to_le_bytes());
    payload.extend_from_slice(&(stats.memtable_keys as u64).to_le_bytes());
    payload.extend_from_slice(&(stats.memtable_bytes as u64).to_le_bytes());
    payload.extend_from_slice(&(stats.total_files as u64).to_le_bytes());
    payload.extend_from_slice(&stats.total_bytes.to_le_bytes());
    payload.extend_from_slice(&(stats.levels.len() as u32).to_le_bytes());
    for l in &stats.levels {
        payload.extend_from_slice(&l.level.to_le_bytes());
        payload.extend_from_slice(&(l.files as u64).to_le_bytes());
        payload.extend_from_slice(&l.bytes.to_le_bytes());
    }
    frame_of(&payload)
}

/// Decode a stats response body into a [`Stats`] snapshot.
pub fn decode_stats(body: &[u8]) -> Result<Stats> {
    let mut b = Body::new(body);
    let next_seqno = b.take_u64()?;
    let memtable_keys = b.take_u64()?;
    let memtable_bytes = b.take_u64()?;
    let total_files = b.take_u64()?;
    let total_bytes = b.take_u64()?;
    let n_levels = b.take_u32()?;
    let mut levels = Vec::with_capacity(n_levels.min(1024) as usize);
    for _ in 0..n_levels {
        let level = b.take_u32()?;
        let files = b.take_u64()?;
        let bytes = b.take_u64()?;
        levels.push(LevelStat {
            level,
            files: usize::try_from(files)
                .map_err(|_| Error::Protocol("file count exceeds address space".to_string()))?,
            bytes,
        });
    }
    b.finish()?;
    Ok(Stats {
        levels,
        total_files: usize::try_from(total_files)
            .map_err(|_| Error::Protocol("file count exceeds address space".to_string()))?,
        total_bytes,
        next_seqno,
        memtable_bytes: usize::try_from(memtable_bytes)
            .map_err(|_| Error::Protocol("memtable size exceeds address space".to_string()))?,
        memtable_keys: usize::try_from(memtable_keys)
            .map_err(|_| Error::Protocol("memtable keys exceed address space".to_string()))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn round_trip_frames() {
        for payload in [&b""[..], b"a", b"hello world", &[0u8; 1000][..]] {
            let mut buf = Vec::new();
            write_frame(&mut buf, payload).unwrap();
            let mut cur = Cursor::new(&buf);
            let got = read_frame(&mut cur, DEFAULT_MAX_FRAME_BYTES)
                .unwrap()
                .expect("frame should be present");
            assert_eq!(got, payload);
        }
    }

    #[test]
    fn empty_stream_is_clean_close() {
        let mut cur = Cursor::new(Vec::<u8>::new());
        assert!(read_frame(&mut cur, 1024).unwrap().is_none());
    }

    #[test]
    fn oversized_frame_is_rejected_not_allocated() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        let mut cur = Cursor::new(buf);
        let err = read_frame(&mut cur, 1024).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err}");
    }

    #[test]
    fn truncated_frame_is_error_not_none() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.extend_from_slice(b"abc");
        let mut cur = Cursor::new(buf);
        let err = read_frame(&mut cur, 1024).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err}");
    }

    #[test]
    fn bounds_round_trip() {
        let cases = [
            Bound::Unbounded,
            Bound::Included(b"abc".to_vec()),
            Bound::Excluded(b"zzz".to_vec()),
        ];
        for bound in &cases {
            let mut body = Vec::new();
            push_bound(&mut body, bound);
            let mut b = Body::new(&body);
            match bound {
                Bound::Unbounded => assert!(matches!(b.take_bound().unwrap(), Bound::Unbounded)),
                Bound::Included(k) => assert_eq!(b.take_bound().unwrap(), Bound::Included(k.clone())),
                Bound::Excluded(k) => assert_eq!(b.take_bound().unwrap(), Bound::Excluded(k.clone())),
            }
            b.finish().unwrap();
        }
    }

    #[test]
    fn bad_bound_kind_is_rejected() {
        let body = [7u8];
        let mut b = Body::new(&body);
        let err = b.take_bound().unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err}");
    }

    #[test]
    fn stats_round_trip() {
        let stats = Stats {
            levels: vec![LevelStat { level: 0, files: 3, bytes: 4096 }, LevelStat { level: 1, files: 1, bytes: 512 }],
            total_files: 4,
            total_bytes: 4608,
            next_seqno: 42,
            memtable_bytes: 128,
            memtable_keys: 7,
        };
        let frame = frame_stats(&stats);
        let got = decode_stats(&frame[5..]).unwrap();
        assert_eq!(got.next_seqno, 42);
        assert_eq!(got.total_files, 4);
        assert_eq!(got.total_bytes, 4608);
        assert_eq!(got.memtable_keys, 7);
        assert_eq!(got.memtable_bytes, 128);
        assert_eq!(got.levels.len(), 2);
        assert_eq!(got.levels[0].level, 0);
        assert_eq!(got.levels[0].files, 3);
        assert_eq!(got.levels[1].bytes, 512);
    }

    #[test]
    fn truncated_stats_is_error() {
        let frame = frame_stats(&Stats {
            levels: Vec::new(),
            total_files: 0,
            total_bytes: 0,
            next_seqno: 1,
            memtable_bytes: 0,
            memtable_keys: 0,
        });
        let body = &frame[5..];
        let err = decode_stats(&body[..body.len() - 1]).unwrap_err();
        assert!(matches!(err, Error::Protocol(_)), "got {err}");
    }

    #[test]
    fn error_frame_round_trip_message() {
        let frame = frame_error("no such transaction");
        let mut cur = Cursor::new(frame);
        let payload = read_frame(&mut cur, 1024).unwrap().unwrap();
        assert_eq!(payload[0], STATUS_ERROR);
        let mut b = Body::new(&payload[1..]);
        let msg = b.take_bytes().unwrap();
        assert_eq!(msg, b"no such transaction");
        b.finish().unwrap();
    }
}
