//! Blocking client for the keystone-server wire protocol.
//!
//! `Client` speaks the same framed binary protocol the server implements, so
//! tests and tools exercise the real bytes over real sockets. Every method
//! sends one request frame and reads one response frame, except [`Client::scan`]
//! which walks the range one page at a time and stitches the pages into one
//! ordered result.
//!
//! ```
//! # fn main() -> keystone::Result<()> {
//! // See tests/server.rs for end to end examples against a live server.
//! let body = keystone::wire::encode_put_body(b"user:1", b"alice");
//! assert_eq!(body.len(), 1 + 6 + 1 + 5);
//! # Ok(())
//! # }
//! ```

use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::ops::{Bound, RangeBounds};

use crate::db::Stats;
use crate::error::{Error, Result};
use crate::wire::{self, Body};

/// How many pairs one scan request asks for. The server clamps this to its
/// own page cap, so a page is always bounded by server configuration.
const SCAN_PAGE_LIMIT: u32 = 512;

/// A connection to a keystone server.
pub struct Client {
    stream: TcpStream,
    max_frame: usize,
}

impl Client {
    /// Connect to a server listening on `addr`, with the default frame cap.
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Client> {
        let stream = TcpStream::connect(addr)?;
        Ok(Client {
            stream,
            max_frame: wire::DEFAULT_MAX_FRAME_BYTES,
        })
    }

    /// Override the cap this client enforces on response frames.
    #[must_use]
    pub fn with_max_frame_bytes(mut self, max_frame: usize) -> Client {
        self.max_frame = max_frame;
        self
    }

    /// Address of the connected server.
    pub fn peer_addr(&self) -> Result<std::net::SocketAddr> {
        Ok(self.stream.peer_addr()?)
    }

    /// Check server liveness.
    pub fn ping(&mut self) -> Result<()> {
        self.exchange(wire::OP_PING, &[])?;
        Ok(())
    }

    /// Insert or overwrite a key with a value.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let body = wire::encode_put_body(key, value);
        self.exchange(wire::OP_PUT, &body)?;
        Ok(())
    }

    /// Read the value of a key, or `None` when the key is absent or deleted.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let body = wire::encode_key_body(key);
        match self.exchange(wire::OP_GET, &body)? {
            Some(payload) => {
                let mut b = Body::new(&payload);
                let value = b.take_bytes()?.to_vec();
                b.finish()?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Delete a key. Deleting an absent key still succeeds.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let body = wire::encode_key_body(key);
        self.exchange(wire::OP_DELETE, &body)?;
        Ok(())
    }

    /// Ordered scan over a range, assembled from protocol pages.
    ///
    /// The client issues scan requests until the server reports that no more
    /// pairs exist, advancing the lower bound past the last pair of each page.
    /// The returned vector is in ascending key order.
    pub fn scan<R: RangeBounds<Vec<u8>>>(
        &mut self,
        range: R,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let hi = clone_bound(range.end_bound());
        let mut lo = clone_bound(range.start_bound());
        let mut out = Vec::new();
        loop {
            let body = wire::encode_scan_body(&lo, &hi, SCAN_PAGE_LIMIT);
            let payload = self
                .expect_ok(wire::OP_SCAN, &body)?
                .ok_or_else(|| Error::Protocol("scan answered not-found".to_string()))?;
            let mut b = Body::new(&payload);
            let has_more = b.take_u8()? == 1;
            let count = b.take_varint()?;
            let count =
                usize::try_from(count).map_err(|_| Error::Protocol("count exceeds address space".to_string()))?;
            let mut page = Vec::with_capacity(count);
            for _ in 0..count {
                let k = b.take_bytes()?.to_vec();
                let v = b.take_bytes()?.to_vec();
                page.push((k, v));
            }
            b.finish()?;
            let last = page.last().map(|(k, _)| k.clone());
            out.append(&mut page);
            match last {
                Some(k) if has_more => lo = Bound::Excluded(k),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Open a write transaction on this connection. Later puts and deletes
    /// are buffered until [`Client::commit`], which applies them as one
    /// durable batch. Only one transaction may be open per connection.
    pub fn begin(&mut self) -> Result<()> {
        self.exchange(wire::OP_BEGIN, &[])?;
        Ok(())
    }

    /// Apply the buffered writes of the open transaction as one batch.
    pub fn commit(&mut self) -> Result<()> {
        self.exchange(wire::OP_COMMIT, &[])?;
        Ok(())
    }

    /// Discard the buffered writes of the open transaction.
    pub fn abort(&mut self) -> Result<()> {
        self.exchange(wire::OP_ABORT, &[])?;
        Ok(())
    }

    /// Snapshot of server engine statistics.
    pub fn stats(&mut self) -> Result<Stats> {
        let payload = self
            .expect_ok(wire::OP_STATS, &[])?
            .ok_or_else(|| Error::Protocol("stats answered not-found".to_string()))?;
        wire::decode_stats(&payload)
    }

    /// Send a request and read the ok body. `Ok(None)` means the server
    /// answered not-found, which only get interprets as a valid answer.
    fn exchange(&mut self, op: u8, body: &[u8]) -> Result<Option<Vec<u8>>> {
        let frame = wire::encode_request_frame(op, body);
        self.stream.write_all(&frame)?;
        self.stream.flush()?;
        let payload = wire::read_frame(&mut self.stream, self.max_frame)?
            .ok_or_else(|| Error::Protocol("server closed the connection".to_string()))?;
        let (&status, rest) = payload
            .split_first()
            .ok_or_else(|| Error::Protocol("empty response payload".to_string()))?;
        match status {
            wire::STATUS_OK => Ok(Some(rest.to_vec())),
            wire::STATUS_NOT_FOUND => Ok(None),
            wire::STATUS_ERROR => {
                let mut b = Body::new(rest);
                let msg = String::from_utf8_lossy(b.take_bytes()?);
                b.finish()?;
                Err(Error::Protocol(msg.into_owned()))
            }
            other => Err(Error::Protocol(format!("bad response status {other}"))),
        }
    }

    /// Like [`Client::exchange`] but treats a not-found answer as a protocol
    /// violation, since only get may legitimately answer that way.
    fn expect_ok(&mut self, op: u8, body: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.exchange(op, body)? {
            Some(payload) => Ok(Some(payload)),
            None => Err(Error::Protocol(format!(
                "server answered not-found to {}",
                wire::op_name(op)
            ))),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_range_scan_body() {
        let body = wire::encode_scan_body(&Bound::Unbounded, &Bound::Unbounded, SCAN_PAGE_LIMIT);
        let mut b = Body::new(&body);
        assert!(matches!(b.take_bound().unwrap(), Bound::Unbounded));
        assert!(matches!(b.take_bound().unwrap(), Bound::Unbounded));
        assert_eq!(b.take_u32().unwrap(), SCAN_PAGE_LIMIT);
        b.finish().unwrap();
    }

    #[test]
    fn bounded_scan_body_round_trip() {
        let body = wire::encode_scan_body(
            &Bound::Included(b"a".to_vec()),
            &Bound::Excluded(b"m".to_vec()),
            7,
        );
        let mut b = Body::new(&body);
        assert_eq!(b.take_bound().unwrap(), Bound::Included(b"a".to_vec()));
        assert_eq!(b.take_bound().unwrap(), Bound::Excluded(b"m".to_vec()));
        assert_eq!(b.take_u32().unwrap(), 7);
        b.finish().unwrap();
    }
}
