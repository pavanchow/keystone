//! Gate 3: on-disk decoder corruption resistance.
//!
//! Every persisted structure (WAL, `SSTable`, manifest) is built valid, then
//! corrupted byte by byte and by truncation. Reading corrupt bytes must never
//! panic, hang, overflow, over-allocate, or return wrong data. It must either
//! reproduce the original bytes exactly or fail with a clean error (for the WAL,
//! drop the torn tail). Garbage files are opened as each reader for good measure.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};

use keystone::manifest::{Manifest, TableMeta};
use keystone::sstable::{SsTableReader, SsTableWriter};
use keystone::types::{Entry, ValueType};
use keystone::wal::{WalReader, WalRecord, WalWriter};
use keystone::{Db, Options, Rng};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("keystone-corrupt-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    dir.join(name)
}

#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    Panicked,
    Errored,
    OkCorrect,
    OkWrong,
}

// Byte mutations applied to a copy of a valid file: xor 0xff at each offset,
// plus a truncation at each length. Bounded and deterministic.
fn mutations(len: usize) -> Vec<(usize, Option<u8>)> {
    let mut out = Vec::new();
    for i in 0..len {
        out.push((i, Some(0xff))); // xor this offset with 0xff
    }
    for cut in 0..len {
        out.push((cut, None)); // truncate to `cut` bytes
    }
    out
}

fn apply(base: &[u8], m: &(usize, Option<u8>)) -> Vec<u8> {
    match m.1 {
        Some(x) => {
            let mut v = base.to_vec();
            v[m.0] ^= x;
            v
        }
        None => base[..m.0].to_vec(),
    }
}

// ---- SSTable -------------------------------------------------------------

fn build_sstable(path: &Path) -> Vec<Entry> {
    let _ = std::fs::remove_file(path);
    let mut entries: Vec<Entry> = (0..60u32)
        .map(|i| Entry {
            key: format!("key{i:04}").into_bytes(),
            seqno: u64::from(i) + 1,
            kind: ValueType::Put,
            value: format!("value-{i}-payload").into_bytes(),
        })
        .collect();
    entries[13].kind = ValueType::Delete;
    entries[13].value = Vec::new();
    let mut w = SsTableWriter::create(path, 128, 10).unwrap();
    for e in &entries {
        w.add(e).unwrap();
    }
    w.finish().unwrap();
    entries
}

fn probe_sstable(bytes: &[u8], expected: &[Entry], path: &Path) -> Outcome {
    std::fs::write(path, bytes).unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<bool, ()> {
        let mut r = SsTableReader::open(path).map_err(|_| ())?;
        // Point lookups must match exactly.
        for e in expected {
            match r.get(&e.key).map_err(|_| ())? {
                Some(got) => {
                    if got.key != e.key || got.kind != e.kind || got.value != e.value {
                        return Ok(false);
                    }
                }
                None => return Ok(false),
            }
        }
        // Full scan must reproduce the table exactly.
        let fresh = SsTableReader::open(path).map_err(|_| ())?;
        let iter = fresh.iter().map_err(|_| ())?;
        let mut collected = Vec::new();
        for item in iter {
            collected.push(item.map_err(|_| ())?);
        }
        Ok(collected == expected)
    }));
    match result {
        Err(_) => Outcome::Panicked,
        Ok(Err(())) => Outcome::Errored,
        Ok(Ok(true)) => Outcome::OkCorrect,
        Ok(Ok(false)) => Outcome::OkWrong,
    }
}

#[test]
fn sstable_corruption_is_detected_or_clean() {
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let build_path = scratch("sst-build");
    let expected = build_sstable(&build_path);
    let base = std::fs::read(&build_path).unwrap();

    let probe_path = scratch("sst-probe");
    let mut panicked = 0u64;
    let mut wrong = 0u64;
    let mut errored = 0u64;
    let mut ok = 0u64;
    let mut total = 0u64;

    for m in mutations(base.len()) {
        let mutated = apply(&base, &m);
        total += 1;
        match probe_sstable(&mutated, &expected, &probe_path) {
            Outcome::Panicked => panicked += 1,
            Outcome::OkWrong => wrong += 1,
            Outcome::Errored => errored += 1,
            Outcome::OkCorrect => ok += 1,
        }
    }

    std::panic::set_hook(orig_hook);
    eprintln!(
        "SSTABLE sweep: total={total} panicked={panicked} wrong_data={wrong} clean_error={errored} ok_correct={ok}"
    );
    let _ = std::fs::remove_file(&build_path);
    let _ = std::fs::remove_file(&probe_path);
    assert_eq!(panicked, 0, "corrupt sstable caused a panic");
    assert_eq!(wrong, 0, "corrupt sstable returned wrong data");
}

#[test]
fn sstable_data_block_bitflip_detected() {
    // A single flipped byte inside the first data block must be caught by the
    // block CRC on read, never returned as a wrong value.
    let path = scratch("sst-datablock");
    let expected = build_sstable(&path);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[4] ^= 0x01; // well inside the first data block
    std::fs::write(&path, &bytes).unwrap();
    let mut r = SsTableReader::open(&path).unwrap();
    let mut saw_error = false;
    for e in &expected {
        match r.get(&e.key) {
            Ok(Some(got)) => assert_eq!(got.value, e.value, "returned corrupted value"),
            Ok(None) => {}
            Err(_) => saw_error = true,
        }
    }
    assert!(saw_error, "flipped data block byte went undetected");
    let _ = std::fs::remove_file(&path);
}

#[test]
fn sstable_giant_footer_length_no_oom() {
    // The exact abort vector: a huge index length in the footer must fail with
    // a clean error, not attempt a multi-terabyte allocation.
    let path = scratch("sst-footer");
    build_sstable(&path);
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    // index_len occupies footer bytes [8..16], i.e. file bytes [n-44+8 .. n-44+16].
    let base = n - 44 + 8;
    bytes[base..base + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    assert!(SsTableReader::open(&path).is_err());
    let _ = std::fs::remove_file(&path);
}

// ---- WAL -----------------------------------------------------------------

fn build_wal(path: &Path) -> Vec<WalRecord> {
    let _ = std::fs::remove_file(path);
    let recs: Vec<WalRecord> = (0..40u32)
        .map(|i| WalRecord {
            kind: if i % 5 == 0 {
                ValueType::Delete
            } else {
                ValueType::Put
            },
            seqno: u64::from(i) + 1,
            key: format!("k{i:03}").into_bytes(),
            value: if i % 5 == 0 {
                Vec::new()
            } else {
                format!("v{i}").into_bytes()
            },
        })
        .collect();
    let mut w = WalWriter::open(path, true).unwrap();
    for r in &recs {
        w.append(r).unwrap();
    }
    recs
}

// Read must yield a prefix of the original records, never garbage, never OOM.
fn probe_wal(bytes: &[u8], expected: &[WalRecord], path: &Path) -> Outcome {
    std::fs::write(path, bytes).unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<bool, ()> {
        let mut r = WalReader::open(path).map_err(|_| ())?;
        let recs = r.read_all().map_err(|_| ())?;
        if recs.len() > expected.len() {
            return Ok(false);
        }
        for (got, want) in recs.iter().zip(expected.iter()) {
            if got != want {
                return Ok(false);
            }
        }
        Ok(true)
    }));
    match result {
        Err(_) => Outcome::Panicked,
        Ok(Err(())) => Outcome::Errored,
        Ok(Ok(true)) => Outcome::OkCorrect,
        Ok(Ok(false)) => Outcome::OkWrong,
    }
}

#[test]
fn wal_corruption_is_prefix_or_clean() {
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let build_path = scratch("wal-build");
    let expected = build_wal(&build_path);
    let base = std::fs::read(&build_path).unwrap();

    let probe_path = scratch("wal-probe");
    let mut panicked = 0u64;
    let mut wrong = 0u64;
    let mut total = 0u64;

    for m in &mutations(base.len()) {
        let mutated = apply(&base, m);
        total += 1;
        match probe_wal(&mutated, &expected, &probe_path) {
            Outcome::Panicked => panicked += 1,
            Outcome::OkWrong => wrong += 1,
            _ => {}
        }
    }

    // Adversarial huge length prefixes (the OOM vector).
    for val in [u32::MAX, 0x7fff_ffff, 0x0fff_ffff, 0x00ff_ffff] {
        let mut v = base.clone();
        v[0..4].copy_from_slice(&val.to_le_bytes());
        total += 1;
        match probe_wal(&v, &expected, &probe_path) {
            Outcome::Panicked => panicked += 1,
            Outcome::OkWrong => wrong += 1,
            _ => {}
        }
    }

    std::panic::set_hook(orig_hook);
    eprintln!("WAL sweep: total={total} panicked={panicked} wrong_data={wrong}");
    let _ = std::fs::remove_file(&build_path);
    let _ = std::fs::remove_file(&probe_path);
    assert_eq!(panicked, 0, "corrupt wal caused a panic");
    assert_eq!(wrong, 0, "corrupt wal returned non-prefix/garbled data");
}

#[test]
fn wal_giant_length_prefix_no_oom() {
    // A bit-flipped length prefix must be treated as a torn tail: earlier
    // records survive and no huge allocation is attempted.
    let path = scratch("wal-giant");
    let expected = build_wal(&path);
    let mut bytes = std::fs::read(&path).unwrap();
    // Corrupt the length prefix of the SECOND record so the first survives.
    let first_len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    let second = 8 + first_len;
    bytes[second..second + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    std::fs::write(&path, &bytes).unwrap();
    let mut r = WalReader::open(&path).unwrap();
    let recs = r.read_all().unwrap();
    assert_eq!(recs.len(), 1, "torn tail should leave exactly the first record");
    assert_eq!(recs[0], expected[0]);
    let _ = std::fs::remove_file(&path);
}

// ---- Manifest ------------------------------------------------------------

fn build_manifest(dir: &Path) -> Manifest {
    let _ = std::fs::create_dir_all(dir);
    let m = Manifest {
        next_file_id: 9,
        next_seqno: 77,
        tables: vec![
            TableMeta {
                level: 0,
                file_id: 1,
                smallest_key: b"aaa".to_vec(),
                largest_key: b"mmm".to_vec(),
                smallest_seqno: 1,
                largest_seqno: 30,
                file_size: 4096,
            },
            TableMeta {
                level: 2,
                file_id: 5,
                smallest_key: b"nnn".to_vec(),
                largest_key: b"zzz".to_vec(),
                smallest_seqno: 31,
                largest_seqno: 60,
                file_size: 8192,
            },
        ],
    };
    m.save(dir).unwrap();
    m
}

fn probe_manifest(bytes: &[u8], dir: &Path) -> Outcome {
    std::fs::write(dir.join("MANIFEST"), bytes).unwrap();
    let result = catch_unwind(AssertUnwindSafe(|| Manifest::load(dir).is_ok()));
    match result {
        Err(_) => Outcome::Panicked,
        Ok(true) => Outcome::OkCorrect,
        Ok(false) => Outcome::Errored,
    }
}

#[test]
fn manifest_corruption_is_detected_or_clean() {
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let dir = scratch("manifest-dir");
    let _ = std::fs::remove_dir_all(&dir);
    build_manifest(&dir);
    let base = std::fs::read(dir.join("MANIFEST")).unwrap();

    let mut panicked = 0u64;
    let mut total = 0u64;
    for m in mutations(base.len()) {
        let mutated = apply(&base, &m);
        total += 1;
        if probe_manifest(&mutated, &dir) == Outcome::Panicked {
            panicked += 1;
        }
    }

    std::panic::set_hook(orig_hook);
    eprintln!("MANIFEST sweep: total={total} panicked={panicked}");
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(panicked, 0, "corrupt manifest caused a panic");
}

// ---- Whole-DB verify -----------------------------------------------------

#[test]
fn db_verify_passes_clean_and_catches_corruption() {
    let dir = scratch("verify-db");
    let _ = std::fs::remove_dir_all(&dir);
    let opts = Options::new()
        .memtable_size_bytes(2 * 1024)
        .block_size(256)
        .l0_compaction_trigger(3)
        .sync_on_write(false);
    {
        let mut db = Db::open(&dir, opts.clone()).unwrap();
        for i in 0..400u32 {
            db.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
        db.compact().unwrap();
        let report = db.verify().unwrap();
        assert!(report.tables >= 1, "expected at least one table");
        assert!(report.entries > 0, "expected verified entries");
        db.close().unwrap();
    }

    // Corrupt one byte in the first sstable file, then verify must fail.
    let sst = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(std::result::Result::ok)
        .map(|e| e.path())
        .find(|p| p.extension().is_some_and(|x| x == "sst"))
        .expect("an sstable file");
    let mut bytes = std::fs::read(&sst).unwrap();
    bytes[4] ^= 0xff;
    std::fs::write(&sst, &bytes).unwrap();

    let db = Db::open(&dir, opts).unwrap();
    assert!(db.verify().is_err(), "verify missed a corrupted block");
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- Garbage files -------------------------------------------------------

#[test]
fn garbage_files_never_panic() {
    let orig_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    let dir = scratch("garbage");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut rng = Rng::new(0xDEAD_BEEF);
    let mut panicked = 0u64;
    let mut total = 0u64;

    for trial in 0..200 {
        let len = (rng.below(4096) + 1) as usize;
        let mut bytes = vec![0u8; len];
        for b in &mut bytes {
            *b = (rng.below(256)) as u8;
        }
        let sst = dir.join("g.sst");
        let wal = dir.join("g.wal");
        std::fs::write(&sst, &bytes).unwrap();
        std::fs::write(&wal, &bytes).unwrap();
        std::fs::write(dir.join("MANIFEST"), &bytes).unwrap();

        total += 3;
        let r1 = catch_unwind(AssertUnwindSafe(|| {
            let _ = SsTableReader::open(&sst).map(|mut r| {
                let _ = r.get(b"anything");
            });
        }));
        let r2 = catch_unwind(AssertUnwindSafe(|| {
            if let Ok(mut r) = WalReader::open(&wal) {
                let _ = r.read_all();
            }
        }));
        let r3 = catch_unwind(AssertUnwindSafe(|| {
            let _ = Manifest::load(&dir);
        }));
        for r in [r1, r2, r3] {
            if r.is_err() {
                panicked += 1;
            }
        }
        let _ = trial;
    }

    std::panic::set_hook(orig_hook);
    eprintln!("GARBAGE trials: total_opens={total} panicked={panicked}");
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(panicked, 0, "a garbage file caused a panic");
}
