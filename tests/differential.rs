//! Gate 1: a random op stream checked against a BTreeMap oracle after every op.

use std::collections::BTreeMap;

use keystone::{Db, Options, Rng};

fn oracle_pairs(oracle: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    oracle.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

fn run_seed(seed: u64, ops: usize) {
    let dir = std::env::temp_dir().join(format!(
        "keystone-diff-{}-{}-{}",
        std::process::id(),
        seed,
        ops
    ));
    let _ = std::fs::remove_dir_all(&dir);

    // Small thresholds so flushes and compactions actually fire during the run.
    let opts = Options::new()
        .memtable_size_bytes(2 * 1024)
        .block_size(256)
        .bloom_bits_per_key(10)
        .l0_compaction_trigger(3)
        .level_size_multiplier(3)
        .sync_on_write(false);
    let mut db = Db::open(&dir, opts).unwrap();
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

    let mut rng = Rng::new(seed);
    // Colliding key space so overwrites and deletes actually hit.
    let key_space = 40u64;

    for step in 0..ops {
        let choice = rng.below(100);
        let k = format!("key{:02}", rng.below(key_space)).into_bytes();

        if choice < 55 {
            let v = format!("v{}-{}", step, rng.below(1_000_000)).into_bytes();
            db.put(&k, &v).unwrap();
            oracle.insert(k.clone(), v);
        } else if choice < 80 {
            db.delete(&k).unwrap();
            oracle.remove(&k);
        } else if choice < 90 {
            // Explicit maintenance to interleave flush and compaction.
            db.flush().unwrap();
            db.compact().unwrap();
        } else {
            // A read only op, still validated below.
            let _ = db.get(&k).unwrap();
        }

        // Sample point gets against the oracle.
        for _ in 0..6 {
            let sk = format!("key{:02}", rng.below(key_space)).into_bytes();
            let got = db.get(&sk).unwrap();
            let want = oracle.get(&sk).cloned();
            assert_eq!(got, want, "seed {seed} step {step} get mismatch for {sk:?}");
        }

        // Full ordered scan must equal the oracle exactly.
        let scanned: Vec<(Vec<u8>, Vec<u8>)> =
            db.scan(..).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(
            scanned,
            oracle_pairs(&oracle),
            "seed {seed} step {step} scan mismatch"
        );
    }

    // A bounded range scan matches the oracle's equivalent slice.
    let lo = b"key10".to_vec();
    let hi = b"key30".to_vec();
    let ranged: Vec<(Vec<u8>, Vec<u8>)> =
        db.scan(lo.clone()..hi.clone()).unwrap().map(|r| r.unwrap()).collect();
    let want_ranged: Vec<(Vec<u8>, Vec<u8>)> = oracle
        .range(lo..hi)
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(ranged, want_ranged, "seed {seed} ranged scan mismatch");

    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn differential_against_btreemap() {
    let ops: usize = std::env::var("KEYSTONE_FUZZ_OPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2500);
    for seed in [1u64, 2, 7, 42, 12345] {
        run_seed(seed, ops);
    }
}
