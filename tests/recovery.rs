//! Gate 2: crash recovery and torn-write survival.

use std::collections::BTreeMap;

use keystone::{Db, Options, Rng};

fn fresh_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("keystone-rec-{}-{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn opts() -> Options {
    Options::new()
        .memtable_size_bytes(4 * 1024)
        .block_size(256)
        .l0_compaction_trigger(3)
        .sync_on_write(true)
}

fn assert_matches_oracle(db: &mut Db, oracle: &BTreeMap<Vec<u8>, Vec<u8>>) {
    let scanned: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).unwrap().map(|r| r.unwrap()).collect();
    let want: Vec<(Vec<u8>, Vec<u8>)> = oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    assert_eq!(scanned, want, "scan does not match oracle after reopen");
    for (k, v) in oracle {
        assert_eq!(db.get(k).unwrap().as_ref(), Some(v));
    }
}

#[test]
fn durability_round_trip_after_hard_drop() {
    let dir = fresh_dir("hard-drop");
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    {
        let mut db = Db::open(&dir, opts()).unwrap();
        let mut rng = Rng::new(99);
        for step in 0..1500 {
            let k = format!("k{:03}", rng.below(200)).into_bytes();
            if rng.below(100) < 70 {
                let v = format!("val{step}").into_bytes();
                db.put(&k, &v).unwrap();
                oracle.insert(k, v);
            } else {
                db.delete(&k).unwrap();
                oracle.remove(&k);
            }
            if step == 500 || step == 1000 {
                db.flush().unwrap();
            }
        }
        // Simulate a crash: drop the handle without calling close().
        drop(db);
    }
    let mut db = Db::open(&dir, opts()).unwrap();
    assert_matches_oracle(&mut db, &oracle);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn torn_write_tail_discarded_on_reopen() {
    let dir = fresh_dir("torn");
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    {
        let mut db = Db::open(&dir, opts()).unwrap();
        for i in 0..50u32 {
            let k = format!("k{i:03}").into_bytes();
            let v = format!("v{i}").into_bytes();
            db.put(&k, &v).unwrap();
            oracle.insert(k, v);
        }
        // Do NOT flush: everything above the last flush lives only in the WAL.
        drop(db);
    }

    let wal = dir.join("wal.log");
    let full = std::fs::metadata(&wal).unwrap().len();
    assert!(full > 0, "wal should hold unflushed writes");

    // Record the last written op so we know which key may be lost.
    let last_key = b"k049".to_vec();
    let last_val = oracle.get(&last_key).cloned().unwrap();

    // Truncate somewhere inside the last record to simulate a torn write.
    let mut rng = Rng::new(5);
    let cut = full - 1 - rng.below(6).min(full - 1);
    let f = std::fs::OpenOptions::new().write(true).open(&wal).unwrap();
    f.set_len(cut).unwrap();
    drop(f);

    let mut db = Db::open(&dir, opts()).unwrap();

    // Every key other than possibly the last must survive intact.
    for (k, v) in &oracle {
        if k == &last_key {
            continue;
        }
        assert_eq!(
            db.get(k).unwrap().as_ref(),
            Some(v),
            "earlier record corrupted by torn tail"
        );
    }

    // The last op is present with its exact value or cleanly absent, never garbage.
    if let Some(v) = db.get(&last_key).unwrap() {
        assert_eq!(v, last_val, "torn record produced a garbled value");
    }

    // The reopened store must still be fully consistent and scannable.
    let scanned: Vec<(Vec<u8>, Vec<u8>)> = db.scan(..).unwrap().map(|r| r.unwrap()).collect();
    for w in scanned.windows(2) {
        assert!(w[0].0 < w[1].0, "scan not strictly ordered after recovery");
    }
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn reopen_after_clean_flush_empty_wal() {
    let dir = fresh_dir("clean-flush");
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    {
        let mut db = Db::open(&dir, opts()).unwrap();
        for i in 0..300u32 {
            let k = format!("k{i:04}").into_bytes();
            let v = format!("v{i}").into_bytes();
            db.put(&k, &v).unwrap();
            oracle.insert(k, v);
        }
        db.flush().unwrap();
        db.compact().unwrap();
        // After a flush the WAL is rotated and empty on disk.
        let wal_len = std::fs::metadata(dir.join("wal.log")).map(|m| m.len()).unwrap_or(0);
        assert_eq!(wal_len, 0, "wal should be empty after flush");
        db.close().unwrap();
    }
    let mut db = Db::open(&dir, opts()).unwrap();
    assert_matches_oracle(&mut db, &oracle);
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
