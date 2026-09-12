//! Command line interface for the Keystone key value store.
#![warn(clippy::pedantic)]

use std::io::Write;
use std::process::exit;
use std::time::Duration;

use keystone::rng::Rng;
use keystone::server::{Server, ServerConfig};
use keystone::{Db, Options};

fn usage() -> ! {
    eprintln!(
        "keystone --path <dir> <command> [args]\n\
\n\
commands:\n\
  put <key> <value>     store a value\n\
  get <key>             print a value or (nil)\n\
  del <key>             delete a key\n\
  scan [prefix]         print sorted key=value pairs, optionally by prefix\n\
  compact               run pending compactions\n\
  stats                 print level layout, file counts, sizes, seqno\n\
  verify                check every sstable block CRC and report integrity\n\
  demo                  run a scripted workload and print the LSM state\n\
  serve [--bind addr]   expose the store over TCP on a framed binary\n\
                        protocol, default bind 127.0.0.1:7373, graceful\n\
                        shutdown via KEYSTONE_SERVER_SHUTDOWN_FILE, see\n\
                        DESIGN.md for the wire protocol and env knobs"
    );
    exit(2);
}

struct Args {
    path: String,
    rest: Vec<String>,
}

fn parse_args() -> Args {
    let mut path = String::from("keystone-data");
    let mut rest = Vec::new();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--path" | "-p" => {
                path = it.next().unwrap_or_else(|| usage());
            }
            "-h" | "--help" => usage(),
            _ => rest.push(a),
        }
    }
    Args { path, rest }
}

fn main() {
    let args = parse_args();
    if args.rest.is_empty() {
        usage();
    }
    if let Err(e) = run(&args) {
        eprintln!("error: {e}");
        exit(1);
    }
}

fn run(args: &Args) -> keystone::Result<()> {
    let cmd = args.rest[0].as_str();
    let rest = &args.rest[1..];
    let opts = Options::new();

    match cmd {
        "put" => {
            if rest.len() != 2 {
                usage();
            }
            let mut db = Db::open(&args.path, opts)?;
            db.put(rest[0].as_bytes(), rest[1].as_bytes())?;
            db.close()?;
            println!("ok");
        }
        "get" => {
            if rest.len() != 1 {
                usage();
            }
            let mut db = Db::open(&args.path, opts)?;
            match db.get(rest[0].as_bytes())? {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(nil)"),
            }
        }
        "del" => {
            if rest.len() != 1 {
                usage();
            }
            let mut db = Db::open(&args.path, opts)?;
            db.delete(rest[0].as_bytes())?;
            db.close()?;
            println!("ok");
        }
        "scan" => {
            let mut db = Db::open(&args.path, opts)?;
            let prefix = rest.first().map(|s| s.as_bytes().to_vec());
            let mut n = 0;
            for item in db.scan(..)? {
                let (k, v) = item?;
                if let Some(p) = &prefix {
                    if !k.starts_with(p) {
                        continue;
                    }
                }
                println!(
                    "{}={}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
                n += 1;
            }
            eprintln!("{n} pairs");
        }
        "compact" => {
            let mut db = Db::open(&args.path, opts)?;
            db.compact()?;
            db.close()?;
            print_stats(&args.path)?;
        }
        "stats" => {
            print_stats(&args.path)?;
        }
        "verify" => {
            let db = Db::open(&args.path, opts)?;
            let report = db.verify()?;
            println!(
                "ok: {} tables, {} entries verified",
                report.tables, report.entries
            );
        }
        "demo" => {
            run_demo(&args.path)?;
        }
        "serve" => {
            run_serve(&args.path, rest)?;
        }
        _ => usage(),
    }
    Ok(())
}

/// Size knob read from the environment, silently ignored when absent or not
/// a plain number so a bad value can never stop the server from starting.
fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok().and_then(|v| v.parse::<usize>().ok())
}

fn run_serve(path: &str, args: &[String]) -> keystone::Result<()> {
    let mut bind: Option<String> = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--bind" | "-b" => bind = Some(it.next().unwrap_or_else(|| usage()).clone()),
            _ => usage(),
        }
    }
    let mut config = ServerConfig::new().path(path);
    if let Some(b) = bind {
        config = config.bind(b);
    }
    if let Some(v) = env_usize("KEYSTONE_SERVER_MAX_FRAME") {
        config = config.max_frame_bytes(v);
    }
    if let Some(v) = env_usize("KEYSTONE_SERVER_MAX_SCAN") {
        config = config.max_scan_page(v);
    }
    if let Some(v) = env_usize("KEYSTONE_SERVER_MAX_TX") {
        config = config.max_tx_bytes(v);
    }
    if let Some(v) = env_usize("KEYSTONE_SERVER_POLL_MS") {
        config = config.poll_interval(Duration::from_millis(v.min(1000) as u64));
    }
    if let Some(v) = env_usize("KEYSTONE_SERVER_MEMTABLE") {
        config = config.options(keystone::Options::new().memtable_size_bytes(v));
    }
    if let Ok(f) = std::env::var("KEYSTONE_SERVER_SHUTDOWN_FILE") {
        config = config.shutdown_file(f);
    }
    let shutdown_hint = match &config.shutdown_file {
        Some(f) => format!("touch {} to stop", f.display()),
        None => String::from("set KEYSTONE_SERVER_SHUTDOWN_FILE to a path, then touch it, to stop"),
    };
    let server = Server::bind(config)?;
    let addr = server.local_addr()?;
    println!("keystone serve listening on {addr} path {path}");
    println!("{shutdown_hint}, killing the process is always safe for acknowledged writes");
    std::io::stdout().flush()?;
    server.run()?;
    println!("keystone serve stopped cleanly");
    Ok(())
}

fn print_stats(path: &str) -> keystone::Result<()> {
    let db = Db::open(path, Options::new())?;
    let s = db.stats();
    println!("Keystone at {path}");
    println!("  next seqno:      {}", s.next_seqno);
    println!("  memtable:        {} keys, {} bytes", s.memtable_keys, s.memtable_bytes);
    println!("  total sstables:  {} files, {} bytes", s.total_files, s.total_bytes);
    println!("  levels:");
    for l in &s.levels {
        println!("    L{}: {} files, {} bytes", l.level, l.files, l.bytes);
    }
    Ok(())
}

fn run_demo(path: &str) -> keystone::Result<()> {
    let _ = std::fs::remove_dir_all(path);
    // Small options so the workload actually builds several levels.
    let opts = Options::new()
        .memtable_size_bytes(4 * 1024)
        .block_size(512)
        .l0_compaction_trigger(3)
        .level_size_multiplier(4);
    let mut db = Db::open(path, opts)?;

    let mut rng = Rng::new(0xC1FE_B00C);
    println!("== writing 4000 random puts over 400 keys ==");
    for _ in 0..4000 {
        let k = format!("user:{:04}", rng.below(400));
        let v = format!("balance={}", rng.below(1_000_000));
        db.put(k.as_bytes(), v.as_bytes())?;
    }
    println!("== deleting every 7th key ==");
    for i in (0..400).step_by(7) {
        db.delete(format!("user:{i:04}").as_bytes()).unwrap();
    }
    db.flush()?;
    db.compact()?;

    let s = db.stats();
    println!("\n== LSM state after workload ==");
    println!("next seqno:     {}", s.next_seqno);
    println!("total sstables: {} files, {} bytes", s.total_files, s.total_bytes);
    for l in &s.levels {
        println!("  L{}: {} files, {} bytes", l.level, l.files, l.bytes);
    }

    let live: Vec<_> = db.scan(..)?.collect::<keystone::Result<Vec<_>>>()?;
    println!("\nlive keys: {}", live.len());
    println!("first 5 live pairs:");
    for (k, v) in live.iter().take(5) {
        println!(
            "  {} -> {}",
            String::from_utf8_lossy(k),
            String::from_utf8_lossy(v)
        );
    }
    let probe = b"user:0001";
    println!(
        "\npoint get {} -> {:?}",
        String::from_utf8_lossy(probe),
        db.get(probe)?.map(|v| String::from_utf8_lossy(&v).into_owned())
    );
    db.close()?;
    Ok(())
}
