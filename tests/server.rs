//! Gate 4: the network server.
//!
//! Everything here runs over real 127.0.0.1 sockets against the real framed
//! protocol, either against the embeddable `Server` or against the actual
//! `keystone serve` binary. The gate proves four things: protocol round
//! trips are exact, concurrent clients interleave safely against one shared
//! engine, malformed input closes connections without ever panicking the
//! server, and an acknowledged write survives a hard kill of the server
//! process. All sizes are env scaleable with small defaults. No test is
//! ignored.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use keystone::client::Client;
use keystone::error::Error;
use keystone::server::{Server, ServerConfig};
use keystone::{Db, Options, Rng};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default)
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "keystone-srv-{}-{}",
        std::process::id(),
        name
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn engine_opts() -> Options {
    Options::new()
        .memtable_size_bytes(4 * 1024)
        .block_size(256)
        .l0_compaction_trigger(3)
}

/// An in-process server on a free port, plus the handles a test needs to
/// signal shutdown and inspect the panic counter.
struct TestServer {
    addr: SocketAddr,
    shutdown: Arc<AtomicBool>,
    panics: Arc<AtomicUsize>,
    handle: thread::JoinHandle<keystone::Result<()>>,
    dir: PathBuf,
}

fn start_server(dir: &Path, max_frame: usize, max_scan_page: usize) -> TestServer {
    let config = ServerConfig::new()
        .path(dir)
        .bind("127.0.0.1:0")
        .options(engine_opts())
        .max_frame_bytes(max_frame)
        .max_scan_page(max_scan_page)
        .poll_interval(Duration::from_millis(2));
    let server = Server::bind(config).expect("server bind");
    let addr = server.local_addr().expect("local addr");
    let shutdown = server.shutdown_flag();
    let panics = server.panic_counter();
    let owned = dir.to_path_buf();
    let handle = thread::spawn(move || server.run());
    TestServer {
        addr,
        shutdown,
        panics,
        handle,
        dir: owned,
    }
}

impl TestServer {
    fn client(&self) -> Client {
        for _ in 0..400 {
            if let Ok(c) = Client::connect(self.addr) {
                return c;
            }
            thread::sleep(Duration::from_millis(5));
        }
        panic!("server never accepted a connection");
    }

    fn stop(self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.handle.join().expect("server thread panicked").unwrap();
        assert_eq!(
            self.panics.load(Ordering::Relaxed),
            0,
            "a connection handler panicked under test load"
        );
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

// Raw protocol helpers for tests that speak bytes, not client calls.

fn raw_frame(payload: &[u8]) -> Vec<u8> {
    let mut f = Vec::with_capacity(4 + payload.len());
    f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    f.extend_from_slice(payload);
    f
}

fn raw_read_frame(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut lenb = [0u8; 4];
    stream.read_exact(&mut lenb).ok()?;
    let len = u32::from_le_bytes(lenb) as usize;
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).ok()?;
    Some(payload)
}

fn raw_expect_error_then_close(stream: &mut TcpStream) {
    let payload = raw_read_frame(stream).expect("server should answer with an error frame");
    assert_eq!(payload[0], 2, "expected STATUS_ERROR, got {payload:?}");
    let mut tail = Vec::new();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.read_to_end(&mut tail).expect("read to eof");
    assert_eq!(tail, b"", "connection should close after the error frame");
}

#[test]
fn protocol_round_trip_over_real_socket() {
    let dir = fresh_dir("round-trip");
    let server = start_server(&dir, 64 * 1024, 64);
    let mut c = server.client();

    c.ping().unwrap();

    // get of a missing key, before anything is written
    assert_eq!(c.get(b"missing").unwrap(), None);

    c.put(b"a", b"one").unwrap();
    c.put(b"b", b"two").unwrap();
    c.put(b"c", b"three").unwrap();
    assert_eq!(c.get(b"a").unwrap(), Some(b"one".to_vec()));
    assert_eq!(c.get(b"b").unwrap(), Some(b"two".to_vec()));
    assert_eq!(c.get(b"c").unwrap(), Some(b"three".to_vec()));
    assert_eq!(c.get(b"missing").unwrap(), None);

    c.delete(b"b").unwrap();
    assert_eq!(c.get(b"b").unwrap(), None);

    let all = c.scan(..).unwrap();
    assert_eq!(
        all,
        vec![
            (b"a".to_vec(), b"one".to_vec()),
            (b"c".to_vec(), b"three".to_vec())
        ]
    );

    // bounded ranges, both bound kinds
    let bounded = c.scan(b"a".to_vec()..b"c".to_vec()).unwrap();
    assert_eq!(bounded, vec![(b"a".to_vec(), b"one".to_vec())]);
    let inclusive = c.scan(b"b".to_vec()..=b"c".to_vec()).unwrap();
    assert_eq!(inclusive, vec![(b"c".to_vec(), b"three".to_vec())]);

    // stats reflect the four writes: put a, put b, put c, delete b
    let s = c.stats().unwrap();
    assert_eq!(s.next_seqno, 5);
    // a, c, and the tombstone for b all live in the memtable before a flush
    assert_eq!(s.memtable_keys, 3);

    // transactions: buffered writes are invisible until commit
    c.begin().unwrap();
    c.put(b"x", b"1").unwrap();
    c.put(b"y", b"2").unwrap();
    assert_eq!(c.get(b"x").unwrap(), None, "buffered put visible early");
    let s2 = c.stats().unwrap();
    assert_eq!(s2.next_seqno, 5, "commit must not have happened yet");
    c.commit().unwrap();
    assert_eq!(c.get(b"x").unwrap(), Some(b"1".to_vec()));
    assert_eq!(c.get(b"y").unwrap(), Some(b"2".to_vec()));

    // abort discards buffered writes
    c.begin().unwrap();
    c.put(b"z", b"3").unwrap();
    c.delete(b"a").unwrap();
    c.abort().unwrap();
    assert_eq!(c.get(b"z").unwrap(), None);
    assert_eq!(c.get(b"a").unwrap(), Some(b"one".to_vec()));

    // semantic errors answer with a message and keep the connection
    let err = c.commit().unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "got {err}");
    c.ping().unwrap();
    c.begin().unwrap();
    let err = c.begin().unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "got {err}");
    c.abort().unwrap();

    c.put(b"after", b"errors").unwrap();
    assert_eq!(c.get(b"after").unwrap(), Some(b"errors".to_vec()));

    drop(c);
    server.stop();
}

#[test]
fn concurrent_clients_interleave_against_one_engine() {
    let clients = env_usize("KEYSTONE_SERVER_TEST_CLIENTS", 6);
    let ops = env_usize("KEYSTONE_SERVER_TEST_OPS", 150);
    let dir = fresh_dir("concurrent");
    let server = start_server(&dir, 64 * 1024, 128);

    let barrier = Arc::new(Barrier::new(clients));
    let mut handles = Vec::new();
    for i in 0..clients {
        let addr = server.addr;
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let mut c = loop {
                if let Ok(c) = Client::connect(addr) {
                    break c;
                }
                thread::sleep(Duration::from_millis(5));
            };
            let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            let mut rng = Rng::new(1000 + i as u64);
            // Everyone connects first, then all threads start writing at
            // once, so the writes genuinely interleave on the server.
            barrier.wait();
            for j in 0..ops {
                let key = format!("c{i:02}-k{j:04}");
                if rng.below(100) < 75 {
                    let value = format!("v{i}-{j}");
                    c.put(key.as_bytes(), value.as_bytes()).unwrap();
                    oracle.insert(key.into_bytes(), value.into_bytes());
                } else if j >= 3 {
                    let victim = format!("c{i:02}-k{:04}", j - 2);
                    c.delete(victim.as_bytes()).unwrap();
                    oracle.remove(victim.as_bytes());
                }
                if j % 25 == 24 {
                    // page the own range back through the wire
                    let lo = format!("c{i:02}-").into_bytes();
                    let hi = format!("c{:02}", i + 1).into_bytes();
                    let pairs = c.scan(lo..hi).unwrap();
                    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    assert_eq!(pairs, want, "client {i} scan diverged at op {j}");
                }
            }
            oracle
        }));
    }

    let mut merged: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for h in handles {
        for (k, v) in h.join().expect("client thread panicked") {
            merged.insert(k, v);
        }
    }

    // One client verifies the whole store through the wire.
    let mut c = server.client();
    let all = c.scan(..).unwrap();
    let want: Vec<(Vec<u8>, Vec<u8>)> = merged.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(all, want, "full scan over the wire diverged from the oracle");
    let mut rng = Rng::new(42);
    for _ in 0..50 {
        let idx = rng.below(merged.len() as u64) as usize;
        let (k, v) = merged.iter().nth(idx).unwrap();
        assert_eq!(c.get(k).unwrap().as_ref(), Some(v));
    }

    drop(c);
    server.stop();
}

#[test]
fn malformed_frames_close_cleanly_and_never_panic_the_server() {
    let dir = fresh_dir("malformed");
    let server = start_server(&dir, 1024, 64);

    // 1: a frame length prefix over the cap
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&u32::MAX.to_le_bytes()).unwrap();
    raw_expect_error_then_close(&mut s);

    // 2: a frame that promises bytes it never sends
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&100u32.to_le_bytes()).unwrap();
    s.write_all(b"abc").unwrap();
    drop(s);

    // 3: an unknown operation tag
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&raw_frame(&[0xEE])).unwrap();
    raw_expect_error_then_close(&mut s);

    // 4: a put whose body claims a longer key than the frame holds
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&raw_frame(&[2, 50])).unwrap();
    raw_expect_error_then_close(&mut s);

    // 5: a scan with a bogus bound kind
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&raw_frame(&[5, 9])).unwrap();
    raw_expect_error_then_close(&mut s);

    // 6: an empty frame, no operation tag at all
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&raw_frame(&[])).unwrap();
    raw_expect_error_then_close(&mut s);

    // 7: pure garbage, no valid framing anywhere
    let mut s = TcpStream::connect(server.addr).unwrap();
    s.write_all(&[0xFF, 0xFF, 0xFF, 0x7F]).unwrap();
    let _ = raw_read_frame(&mut s);
    drop(s);

    // The server must still be fully alive for well behaved clients.
    let mut c = server.client();
    c.put(b"still", b"alive").unwrap();
    assert_eq!(c.get(b"still").unwrap(), Some(b"alive".to_vec()));
    drop(c);
    server.stop();
}

#[test]
fn scan_pages_span_the_network_boundary() {
    let keys = env_usize("KEYSTONE_SERVER_TEST_KEYS", 3000);
    let page = env_usize("KEYSTONE_SERVER_TEST_PAGE", 128);
    let dir = fresh_dir("pages");
    let server = start_server(&dir, 1024 * 1024, page);
    let mut c = server.client();

    for i in 0..keys {
        let k = format!("key_{i:05}");
        let v = format!("val-{i}");
        c.put(k.as_bytes(), v.as_bytes()).unwrap();
    }

    // A full scan of `keys` pairs over a server capped at `page` pairs per
    // frame issues many scan requests, so the result only exists if
    // pagination, ordering, and the has-more flag all work across the wire.
    let all = c.scan(..).unwrap();
    assert_eq!(all.len(), keys, "scan must return every key");
    for (i, (k, v)) in all.iter().enumerate() {
        assert_eq!(k, format!("key_{i:05}").as_bytes(), "key out of order at {i}");
        assert_eq!(v, format!("val-{i}").as_bytes());
    }

    // A bounded range crossing many pages too, with both bound kinds set.
    let mid = c
        .scan(b"key_01000".to_vec()..b"key_02000".to_vec())
        .unwrap();
    assert_eq!(mid.len(), 1000);
    assert_eq!(mid[0].0, b"key_01000".to_vec());
    assert_eq!(mid[999].0, b"key_01999".to_vec());

    // An empty range scans to nothing without hanging.
    assert!(c.scan(b"zzz".to_vec()..b"zzzz".to_vec()).unwrap().is_empty());

    drop(c);
    server.stop();
}

fn free_addr() -> String {
    let l = TcpListener::bind("127.0.0.1:0").expect("probe bind");
    l.local_addr().unwrap().to_string()
}

fn wait_for_child(port_addr: &str) -> Option<Client> {
    for _ in 0..500 {
        if let Ok(c) = Client::connect(port_addr) {
            return Some(c);
        }
        thread::sleep(Duration::from_millis(10));
    }
    None
}

#[test]
fn kill_server_process_midrun_and_reopen() {
    let keys = env_usize("KEYSTONE_SERVER_TEST_KILL_KEYS", 300);
    let dir = fresh_dir("kill");
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut child = None;
    let mut c = None;
    for _ in 0..3 {
        let addr = free_addr();
        let spawned = Command::new(env!("CARGO_BIN_EXE_keystone"))
            .args(["--path", dir.to_str().unwrap(), "serve", "--bind", &addr])
            .env("KEYSTONE_SERVER_POLL_MS", "2")
            .env("KEYSTONE_SERVER_MEMTABLE", "2048")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
        match spawned {
            Ok(mut ch) => match wait_for_child(&addr) {
                Some(client) => {
                    child = Some(ch);
                    c = Some(client);
                    break;
                }
                None => {
                    let _ = ch.kill();
                    let _ = ch.wait();
                }
            },
            Err(e) => panic!("failed to spawn keystone serve: {e}"),
        }
    }
    let mut child = child.expect("no spawned server became connectable");
    let mut c = c.unwrap();

    // Every acked put is a durability promise. Small memtable on the child
    // forces several flushes, so the oracle spans both SSTables and the WAL
    // when the kill lands.
    let mut rng = Rng::new(7);
    for i in 0..keys {
        let k = format!("kill_{i:04}");
        if rng.below(100) < 80 {
            let v = format!("survivor-{i}");
            c.put(k.as_bytes(), v.as_bytes()).unwrap();
            oracle.insert(k.into_bytes(), v.into_bytes());
        } else {
            c.delete(k.as_bytes()).unwrap();
            oracle.remove(k.as_bytes());
        }
    }
    // The connection was active up to the last ack: kill without any
    // shutdown path, the harshest stop there is.
    drop(c);
    child.kill().expect("kill -9 the server");
    child.wait().unwrap();

    let mut db = Db::open(&dir, engine_opts()).unwrap();
    let scanned: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).unwrap().map(|r| r.unwrap()).collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scanned, want, "acked writes lost across a server kill");
    for (k, v) in &oracle {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn graceful_shutdown_file_stops_binary_and_data_reopens() {
    let dir = fresh_dir("shutdown");
    let stop_file = dir.join("please-stop");
    let addr = free_addr();
    let mut child = Command::new(env!("CARGO_BIN_EXE_keystone"))
        .args(["--path", dir.to_str().unwrap(), "serve", "--bind", &addr])
        .env("KEYSTONE_SERVER_POLL_MS", "2")
        .env("KEYSTONE_SERVER_SHUTDOWN_FILE", stop_file.to_str().unwrap())
        .env("KEYSTONE_SERVER_MEMTABLE", "2048")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn keystone serve");
    let mut c = wait_for_child(&addr).expect("spawned server never became connectable");

    c.put(b"shutdown", b"clean").unwrap();
    c.put(b"before", b"exit").unwrap();
    drop(c);

    std::fs::write(&stop_file, b"stop").unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(s) = child.try_wait().unwrap() {
            break s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server ignored the shutdown file"
        );
        thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "graceful shutdown must exit 0, got {status}");
    assert!(!stop_file.exists(), "server should remove the shutdown file");

    let mut db = Db::open(&dir, engine_opts()).unwrap();
    assert_eq!(db.get(b"shutdown").unwrap(), Some(b"clean".to_vec()));
    assert_eq!(db.get(b"before").unwrap(), Some(b"exit".to_vec()));
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn transaction_commit_is_invisible_until_it_lands() {
    let dir = fresh_dir("tx");
    let server = start_server(&dir, 64 * 1024, 64);
    let mut a = server.client();
    let mut b = server.client();

    a.put(b"base", b"0").unwrap();

    // While a is buffering, b sees only committed state.
    a.begin().unwrap();
    for i in 0..50u32 {
        a.put(format!("tx_{i:03}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    a.delete(b"base").unwrap();
    assert_eq!(b.get(b"tx_000").unwrap(), None, "uncommitted write leaked");
    assert_eq!(b.get(b"base").unwrap(), Some(b"0".to_vec()));
    let s = b.stats().unwrap();
    assert_eq!(s.next_seqno, 2, "commit must not have happened yet");

    a.commit().unwrap();

    assert_eq!(b.get(b"base").unwrap(), None, "tombstone lost");
    for i in 0..50u32 {
        let k = format!("tx_{i:03}");
        assert_eq!(
            b.get(k.as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "batch member {k} missing after commit"
        );
    }
    let s = b.stats().unwrap();
    assert_eq!(s.next_seqno, 2 + 51);

    // An oversized transaction is poisoned: further writes are refused,
    // commit refuses, and abort restores the connection.
    let config = ServerConfig::new()
        .path(dir.join("poison"))
        .bind("127.0.0.1:0")
        .options(engine_opts())
        .max_frame_bytes(64 * 1024)
        .max_scan_page(64)
        .max_tx_bytes(300)
        .poll_interval(Duration::from_millis(2));
    let poison = Server::bind(config).unwrap();
    let paddr = poison.local_addr().unwrap();
    let pshutdown = poison.shutdown_flag();
    let ppanics = poison.panic_counter();
    let phandle = thread::spawn(move || poison.run());
    let mut c = loop {
        if let Ok(c) = Client::connect(paddr) {
            break c;
        }
        thread::sleep(Duration::from_millis(5));
    };
    c.begin().unwrap();
    let mut refused = false;
    for i in 0..100u32 {
        if c.put(format!("big_{i:03}").as_bytes(), &[0xAB; 64]).is_err() {
            refused = true;
            break;
        }
    }
    assert!(refused, "buffer cap never tripped");
    // Committing a poisoned transaction refuses and closes the transaction,
    // so the connection is clean again without an abort.
    let err = c.commit().unwrap_err();
    assert!(matches!(err, Error::Protocol(_)), "got {err}");
    c.ping().unwrap();
    c.put(b"recovered", b"yes").unwrap();
    assert_eq!(c.get(b"recovered").unwrap(), Some(b"yes".to_vec()));
    drop(c);
    pshutdown.store(true, Ordering::Relaxed);
    phandle.join().unwrap().unwrap();
    assert_eq!(ppanics.load(Ordering::Relaxed), 0);

    drop(b);
    drop(a);
    server.stop();
    let _ = std::fs::remove_dir_all(dir.join("poison"));
}
