//! The keystone server: an embeddable TCP key-value server in pure std.
//!
//! The server opens a [`Db`] on a directory and exposes it to any number of
//! TCP clients over the framed protocol in [`crate::wire`]. Each accepted
//! connection gets its own thread, and all connections share one engine
//! behind a `std::sync::Mutex`. That choice is deliberate and documented in
//! `DESIGN.md`: the workload is bounded by WAL fsyncs, so serializing engine
//! access costs little, and a threads-plus-mutex design needs no runtime,
//! no external crates, and no lock-free subtlety to be correct.
//!
//! Shutdown has three shapes, in increasing roughness.
//!
//! - Programmatic, by calling [`Server::shutdown`] or dropping in a shutdown
//!   file, which stops the accept loop, closes idle connections, joins every
//!   worker, and flushes the memtable before returning.
//! - Hard kill, such as Ctrl-C sending SIGINT to the process. Pure std Rust
//!   cannot intercept signals, so the process dies where it stands. That is
//!   safe by the durability contract: an acknowledged put is already fsynced
//!   to the WAL before the ack, so a reopen replays it exactly.
//! - A crash anywhere in between, covered by the same WAL guarantees.
//!
//! ```
//! use keystone::client::Client;
//! use keystone::server::{Server, ServerConfig};
//!
//! let dir = std::env::temp_dir().join("keystone-server-doctest");
//! let _ = std::fs::remove_dir_all(&dir);
//! let config = ServerConfig::new()
//!     .path(&dir)
//!     .bind("127.0.0.1:0");
//! let mut server = Server::bind(config).unwrap();
//! let addr = server.local_addr().unwrap();
//! let shutdown = server.shutdown_flag();
//! let _handle = std::thread::spawn(move || server.run());
//! let mut client = Client::connect(addr).unwrap();
//! client.put(b"greeting", b"hello").unwrap();
//! assert_eq!(client.get(b"greeting").unwrap(), Some(b"hello".to_vec()));
//! shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
//! # Ok::<(), keystone::Error>(())
//! ```

use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::db::Db;
use crate::error::Result;
use crate::options::Options;
use crate::types::WriteOp;
use crate::wire::{self, Body};

/// How long a connection read waits before re-checking the shutdown flag.
/// Idle connections therefore notice shutdown within roughly this interval.
const READ_TICK: Duration = Duration::from_millis(250);

/// Fixed per record cost charged against the transaction buffer cap, on top
/// of key and value bytes, so batches of many tiny writes still count.
const TX_RECORD_OVERHEAD: usize = 16;

/// Configuration for a [`Server`].
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Directory of the engine the server exposes.
    pub path: PathBuf,
    /// Address to listen on, for example `127.0.0.1:7373`. Port `0` picks a
    /// free port, readable through [`Server::local_addr`].
    pub bind: String,
    /// Engine options, passed straight to [`Db::open`].
    pub options: Options,
    /// Largest frame payload accepted from a client. Larger frames are
    /// refused and the connection is closed.
    pub max_frame_bytes: usize,
    /// Largest scan page, in pairs, the server will return.
    pub max_scan_page: usize,
    /// Largest transaction buffer, in approximate bytes, before the
    /// transaction is poisoned and can no longer commit.
    pub max_tx_bytes: usize,
    /// When set, the server shuts down gracefully once this file exists.
    pub shutdown_file: Option<PathBuf>,
    /// How often the accept loop re-checks the shutdown flag and file.
    pub poll_interval: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            path: PathBuf::from("keystone-data"),
            bind: String::from("127.0.0.1:7373"),
            options: Options::new(),
            max_frame_bytes: wire::DEFAULT_MAX_FRAME_BYTES,
            max_scan_page: 10_000,
            max_tx_bytes: 64 * 1024 * 1024,
            shutdown_file: None,
            poll_interval: Duration::from_millis(25),
        }
    }
}

impl ServerConfig {
    /// Start from the defaults.
    #[must_use]
    pub fn new() -> Self {
        ServerConfig::default()
    }

    /// Set the engine directory.
    #[must_use]
    pub fn path<P: AsRef<Path>>(mut self, path: P) -> Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Set the bind address.
    #[must_use]
    pub fn bind(mut self, bind: impl Into<String>) -> Self {
        self.bind = bind.into();
        self
    }

    /// Set the engine options.
    #[must_use]
    pub fn options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Set the frame payload cap.
    #[must_use]
    pub fn max_frame_bytes(mut self, v: usize) -> Self {
        self.max_frame_bytes = v;
        self
    }

    /// Set the scan page cap.
    #[must_use]
    pub fn max_scan_page(mut self, v: usize) -> Self {
        self.max_scan_page = v;
        self
    }

    /// Set the transaction buffer cap.
    #[must_use]
    pub fn max_tx_bytes(mut self, v: usize) -> Self {
        self.max_tx_bytes = v;
        self
    }

    /// Set the shutdown file path.
    #[must_use]
    pub fn shutdown_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.shutdown_file = Some(path.into());
        self
    }

    /// Set the accept loop poll interval.
    #[must_use]
    pub fn poll_interval(mut self, v: Duration) -> Self {
        self.poll_interval = v;
        self
    }
}

/// An embeddable TCP key-value server over a [`Db`].
pub struct Server {
    config: ServerConfig,
    listener: TcpListener,
    engine: Arc<Mutex<Db>>,
    shutdown: Arc<AtomicBool>,
    panics: Arc<AtomicUsize>,
    workers: Vec<JoinHandle<()>>,
}

impl Server {
    /// Bind the listener and open the engine. Does not accept connections
    /// until [`Server::run`] is called.
    pub fn bind(config: ServerConfig) -> Result<Server> {
        let listener = TcpListener::bind(&config.bind)?;
        let db = Db::open(&config.path, config.options.clone())?;
        Ok(Server {
            config,
            listener,
            engine: Arc::new(Mutex::new(db)),
            shutdown: Arc::new(AtomicBool::new(false)),
            panics: Arc::new(AtomicUsize::new(0)),
            workers: Vec::new(),
        })
    }

    /// The address the listener bound to, useful when the port was `0`.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Request a graceful shutdown. The accept loop and every idle
    /// connection stop within about one poll interval plus one read tick.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Number of connection handler panics observed so far. The invariant
    /// the server gate checks is that this stays zero across adversarial
    /// client input.
    #[must_use]
    pub fn panics(&self) -> usize {
        self.panics.load(Ordering::Relaxed)
    }

    /// Shared handle to the panic counter, for reading after `run` has
    /// consumed the server.
    #[must_use]
    pub fn panic_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.panics)
    }

    /// Shared handle to the shutdown flag, for signalling shutdown after
    /// `run` has consumed the server into its worker thread.
    #[must_use]
    pub fn shutdown_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.shutdown)
    }

    /// Accept connections until shutdown is requested, then drain workers
    /// and flush the engine before returning.
    pub fn run(mut self) -> Result<()> {
        if let Some(f) = &self.config.shutdown_file {
            let _ = std::fs::remove_file(f);
        }
        self.listener.set_nonblocking(true)?;
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            if let Some(f) = &self.config.shutdown_file {
                if f.exists() {
                    break;
                }
            }
            match self.listener.accept() {
                Ok((stream, _peer)) => self.spawn_worker(stream),
                Err(e) if e.kind() == ErrorKind::WouldBlock => {
                    std::thread::sleep(self.config.poll_interval);
                }
                Err(e) => {
                    eprintln!("keystone-server: accept error: {e}");
                    std::thread::sleep(self.config.poll_interval);
                }
            }
        }
        if let Some(f) = &self.config.shutdown_file {
            let _ = std::fs::remove_file(f);
        }
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
        {
            let mut engine = lock_engine(&self.engine);
            engine.flush()?;
        }
        Ok(())
    }

    fn spawn_worker(&mut self, stream: TcpStream) {
        let engine = Arc::clone(&self.engine);
        let shutdown = Arc::clone(&self.shutdown);
        let panics = Arc::clone(&self.panics);
        let limits = ConnLimits {
            frame_bytes: self.config.max_frame_bytes,
            scan_page: self.config.max_scan_page,
            tx_bytes: self.config.max_tx_bytes,
        };
        let handle = std::thread::Builder::new()
            .name(String::from("keystone-conn"))
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    serve_connection(stream, &engine, &shutdown, limits);
                }));
                if result.is_err() {
                    panics.fetch_add(1, Ordering::Relaxed);
                    eprintln!("keystone-server: connection handler panicked");
                }
            });
        match handle {
            Ok(h) => self.workers.push(h),
            Err(e) => eprintln!("keystone-server: failed to spawn worker: {e}"),
        }
    }
}

#[derive(Clone, Copy)]
struct ConnLimits {
    frame_bytes: usize,
    scan_page: usize,
    tx_bytes: usize,
}

/// Buffered writes of a per connection transaction.
#[derive(Default)]
struct Tx {
    ops: Vec<WriteOp>,
    bytes: usize,
    poisoned: bool,
}

fn serve_connection(
    mut stream: TcpStream,
    engine: &Arc<Mutex<Db>>,
    shutdown: &Arc<AtomicBool>,
    limits: ConnLimits,
) {
    let _ = stream.set_read_timeout(Some(READ_TICK));
    let _ = stream.set_nodelay(true);
    let mut tx: Option<Tx> = None;
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return;
        }
        match read_frame_tick(&mut stream, limits.frame_bytes, shutdown) {
            Tick::Frame(payload) => {
                if handle_request(&mut stream, engine, &payload, &mut tx, &limits) {
                    return;
                }
            }
            Tick::Closed | Tick::Stopped => return,
        }
    }
}

enum Tick {
    Frame(Vec<u8>),
    Closed,
    Stopped,
}

/// Read one frame from a connection with a read timeout, re-checking the
/// shutdown flag on every timeout so an idle connection cannot delay a
/// graceful shutdown. A clean EOF, a torn frame, or an I/O error all close
/// the connection without a response.
fn read_frame_tick(
    stream: &mut TcpStream,
    max_frame: usize,
    shutdown: &AtomicBool,
) -> Tick {
    let mut header = [0u8; 4];
    match read_exact_tick(stream, &mut header, shutdown) {
        ReadTick::Filled => {}
        ReadTick::Stopped => return Tick::Stopped,
        ReadTick::Closed => return Tick::Closed,
    }
    let len = u32::from_le_bytes(header) as usize;
    if len > max_frame {
        let msg = format!("frame of {len} bytes exceeds cap of {max_frame}");
        let _ = stream.write_all(&wire::frame_error(&msg));
        let _ = stream.flush();
        return Tick::Closed;
    }
    let mut payload = vec![0u8; len];
    match read_exact_tick(stream, &mut payload, shutdown) {
        ReadTick::Filled => Tick::Frame(payload),
        _ => Tick::Closed,
    }
}

enum ReadTick {
    Filled,
    Closed,
    Stopped,
}

/// Fill `buf` completely, tolerating read timeouts, which is why this cannot
/// use `Read::read_exact` directly: a timed-out `read_exact` loses the bytes
/// already consumed, while this loop tracks the filled prefix itself.
fn read_exact_tick(
    stream: &mut TcpStream,
    buf: &mut [u8],
    shutdown: &AtomicBool,
) -> ReadTick {
    let mut filled = 0;
    while filled < buf.len() {
        if shutdown.load(Ordering::Relaxed) {
            return ReadTick::Stopped;
        }
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return ReadTick::Closed,
            Ok(n) => filled += n,
            Err(ref e)
                if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == ErrorKind::Interrupted => {}
            Err(_) => return ReadTick::Closed,
        }
    }
    ReadTick::Filled
}

/// Handle one request frame. Returns `true` when the connection must close.
///
/// Two error classes behave differently. A frame that cannot be parsed at
/// all, unknown operation, malformed body, oversized length, gets one error
/// frame and a close, because a client that emits unparseable bytes cannot
/// be trusted to stay in sync. A semantic refusal, no transaction open,
/// buffer overflow, engine error, gets an error frame and keeps the
/// connection open.
fn handle_request(
    stream: &mut TcpStream,
    engine: &Arc<Mutex<Db>>,
    payload: &[u8],
    tx: &mut Option<Tx>,
    limits: &ConnLimits,
) -> bool {
    let request = match wire::decode_request(payload) {
        Ok(r) => r,
        Err(e) => return fail_and_close(stream, &e.to_string()),
    };
    match request.op {
        wire::OP_PING => respond(stream, &wire::frame_ok()),
        wire::OP_PUT => match parse_put(&request.body) {
            Ok((key, value)) => apply_put(stream, engine, key, value, tx, limits),
            Err(e) => fail_and_close(stream, &e.to_string()),
        },
        wire::OP_GET => match parse_key(&request.body) {
            Ok(key) => {
                let answer = {
                    let mut db = lock_engine(engine);
                    match db.get(&key) {
                        Ok(Some(value)) => wire::frame_value(&value),
                        Ok(None) => wire::frame_not_found(),
                        Err(e) => wire::frame_error(&e.to_string()),
                    }
                };
                respond(stream, &answer)
            }
            Err(e) => fail_and_close(stream, &e.to_string()),
        },
        wire::OP_DELETE => match parse_key(&request.body) {
            Ok(key) => apply_delete(stream, engine, key, tx, limits),
            Err(e) => fail_and_close(stream, &e.to_string()),
        },
        wire::OP_SCAN => match parse_scan(&request.body) {
            Ok((lo, hi, limit)) => apply_scan(stream, engine, lo, hi, limit, limits),
            Err(e) => fail_and_close(stream, &e.to_string()),
        },
        wire::OP_BEGIN => {
            if tx.is_some() {
                respond(stream, &wire::frame_error("transaction already open"))
            } else {
                *tx = Some(Tx::default());
                respond(stream, &wire::frame_ok())
            }
        }
        wire::OP_COMMIT => match tx.take() {
            None => respond(stream, &wire::frame_error("no open transaction")),
            Some(t) if t.poisoned => respond(
                stream,
                &wire::frame_error("transaction overflowed its buffer and cannot commit"),
            ),
            Some(t) => {
                let answer = {
                    let mut db = lock_engine(engine);
                    match db.write_batch(&t.ops) {
                        Ok(()) => wire::frame_ok(),
                        Err(e) => wire::frame_error(&e.to_string()),
                    }
                };
                respond(stream, &answer)
            }
        },
        wire::OP_ABORT => match tx.take() {
            None => respond(stream, &wire::frame_error("no open transaction")),
            Some(_) => respond(stream, &wire::frame_ok()),
        },
        wire::OP_STATS => {
            let answer = {
                let db = lock_engine(engine);
                wire::frame_stats(&db.stats())
            };
            respond(stream, &answer)
        }
        other => fail_and_close(stream, &format!("unknown operation tag {other}")),
    }
}

fn parse_put(body: &[u8]) -> std::result::Result<(Vec<u8>, Vec<u8>), crate::error::Error> {
    let mut b = Body::new(body);
    let key = b.take_bytes()?.to_vec();
    let value = b.take_bytes()?.to_vec();
    b.finish()?;
    Ok((key, value))
}

fn parse_key(body: &[u8]) -> std::result::Result<Vec<u8>, crate::error::Error> {
    let mut b = Body::new(body);
    let key = b.take_bytes()?.to_vec();
    b.finish()?;
    Ok(key)
}

type ScanParse = (Bound<Vec<u8>>, Bound<Vec<u8>>, usize);

fn parse_scan(body: &[u8]) -> std::result::Result<ScanParse, crate::error::Error> {
    let mut b = Body::new(body);
    let lo = b.take_bound()?;
    let hi = b.take_bound()?;
    let limit = b.take_u32()?;
    b.finish()?;
    Ok((lo, hi, limit as usize))
}

fn apply_put(
    stream: &mut TcpStream,
    engine: &Arc<Mutex<Db>>,
    key: Vec<u8>,
    value: Vec<u8>,
    tx: &mut Option<Tx>,
    limits: &ConnLimits,
) -> bool {
    if let Some(t) = tx.as_mut() {
        if t.poisoned {
            return respond(
                stream,
                &wire::frame_error("transaction overflowed, commit refused, abort to recover"),
            );
        }
        let cost = key.len() + value.len() + TX_RECORD_OVERHEAD;
        if t.bytes.saturating_add(cost) > limits.tx_bytes {
            t.poisoned = true;
            return respond(
                stream,
                &wire::frame_error(&format!(
                    "transaction buffer cap of {} bytes exceeded",
                    limits.tx_bytes
                )),
            );
        }
        t.bytes += cost;
        t.ops.push(WriteOp::Put { key, value });
        return respond(stream, &wire::frame_ok());
    }
    let answer = {
        let mut db = lock_engine(engine);
        match db.put(&key, &value) {
            Ok(()) => wire::frame_ok(),
            Err(e) => wire::frame_error(&e.to_string()),
        }
    };
    respond(stream, &answer)
}

fn apply_delete(
    stream: &mut TcpStream,
    engine: &Arc<Mutex<Db>>,
    key: Vec<u8>,
    tx: &mut Option<Tx>,
    limits: &ConnLimits,
) -> bool {
    if let Some(t) = tx.as_mut() {
        if t.poisoned {
            return respond(
                stream,
                &wire::frame_error("transaction overflowed, commit refused, abort to recover"),
            );
        }
        let cost = key.len() + TX_RECORD_OVERHEAD;
        if t.bytes.saturating_add(cost) > limits.tx_bytes {
            t.poisoned = true;
            return respond(
                stream,
                &wire::frame_error(&format!(
                    "transaction buffer cap of {} bytes exceeded",
                    limits.tx_bytes
                )),
            );
        }
        t.bytes += cost;
        t.ops.push(WriteOp::Delete { key });
        return respond(stream, &wire::frame_ok());
    }
    let answer = {
        let mut db = lock_engine(engine);
        match db.delete(&key) {
            Ok(()) => wire::frame_ok(),
            Err(e) => wire::frame_error(&e.to_string()),
        }
    };
    respond(stream, &answer)
}

/// Serve one scan page. The engine lock is held only while pulling at most
/// `limit` plus one entries, so a page bounds the lock hold as well as the
/// response size. The extra entry is consumed but not sent, and its presence
/// becomes the has-more flag.
fn apply_scan(
    stream: &mut TcpStream,
    engine: &Arc<Mutex<Db>>,
    lo: Bound<Vec<u8>>,
    hi: Bound<Vec<u8>>,
    limit: usize,
    limits: &ConnLimits,
) -> bool {
    let page_limit = if limit == 0 {
        limits.scan_page
    } else {
        limit.min(limits.scan_page)
    };
    let mut pairs = Vec::new();
    let mut has_more = false;
    let mut engine_error = None;
    {
        let mut db = lock_engine(engine);
        let mut scan = match db.scan((lo, hi)) {
            Ok(s) => s,
            Err(e) => return respond(stream, &wire::frame_error(&e.to_string())),
        };
        for item in scan.by_ref() {
            if pairs.len() == page_limit {
                has_more = true;
                break;
            }
            match item {
                Ok((k, v)) => pairs.push((k, v)),
                Err(e) => {
                    engine_error = Some(e);
                    break;
                }
            }
        }
        drop(scan);
    }
    if let Some(e) = engine_error {
        return respond(stream, &wire::frame_error(&e.to_string()));
    }
    let mut out = Vec::new();
    wire::frame_scan_page(has_more, &pairs, &mut out);
    respond(stream, &out)
}

fn respond(stream: &mut TcpStream, frame: &[u8]) -> bool {
    stream.write_all(frame).is_err()
}

fn fail_and_close(stream: &mut TcpStream, msg: &str) -> bool {
    let _ = stream.write_all(&wire::frame_error(msg));
    let _ = stream.flush();
    true
}

/// Lock the engine, recovering from a poisoned mutex by carrying on. A
/// poison means a connection handler panicked while holding the lock, which
/// is counted in the server panic counter and asserted to be zero by the
/// server gate. The engine state itself is WAL first, so serving after a
/// recovered poison is preferable to locking the store shut forever.
fn lock_engine(engine: &Arc<Mutex<Db>>) -> MutexGuard<'_, Db> {
    match engine.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::Client;

    fn test_config(dir: &Path) -> ServerConfig {
        ServerConfig::new()
            .path(dir)
            .bind("127.0.0.1:0")
            .max_frame_bytes(64 * 1024)
            .max_scan_page(64)
    }

    fn wait_for_client(addr: SocketAddr) -> Client {
        for _ in 0..200 {
            if let Ok(c) = Client::connect(addr) {
                return c;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        panic!("server never accepted a connection on {addr}");
    }

    #[test]
    fn server_serves_round_trip_and_shuts_down() {
        let dir = std::env::temp_dir().join(format!("keystone-srv-{}-smoke", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let server = Server::bind(test_config(&dir)).unwrap();
        let addr = server.local_addr().unwrap();
        let panics = server.panic_counter();
        let shutdown = server.shutdown_flag();
        let handle = std::thread::spawn(move || server.run());
        let mut client = wait_for_client(addr);
        client.put(b"a", b"1").unwrap();
        client.put(b"b", b"2").unwrap();
        assert_eq!(client.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(client.get(b"missing").unwrap(), None);
        client.delete(b"a").unwrap();
        assert_eq!(client.get(b"a").unwrap(), None);
        let pairs = client.scan(..).unwrap();
        assert_eq!(pairs, vec![(b"b".to_vec(), b"2".to_vec())]);
        drop(client);
        let stats = {
            let mut c = wait_for_client(addr);
            let s = c.stats().unwrap();
            drop(c);
            s
        };
        // put a, put b, delete a: three writes, so the next seqno is 4.
        assert_eq!(stats.next_seqno, 4);
        shutdown.store(true, Ordering::Relaxed);
        handle.join().unwrap().unwrap();
        assert_eq!(panics.load(Ordering::Relaxed), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
