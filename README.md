# Keystone

Keystone is a durable, ordered, embedded key value store written from scratch in pure Rust with zero external dependencies. It is a real log-structured merge tree (LSM), not a hashmap with save and load. Writes go to a write-ahead log and an in-memory sorted table, flush to immutable on-disk sorted string tables, and merge downward through leveled compaction. It recovers exactly across a crash.

Playground: https://pavanchow.github.io/keystone/

## The gap it fills

Most embedded stores in the Rust ecosystem pull in large dependency trees. Most from-scratch toy stores are an in-memory map serialized to a file, which loses ordering, loses crash safety, and rewrites the whole dataset on every change.

Keystone sits in the space between. It is a single crate, standard library only, that still gives you the properties a real workload needs.

- Durable. Every write is logged before it is acknowledged, and the log survives a crash mid-write.
- Ordered. Keys are stored sorted, so range scans and prefix scans are first class.
- Embedded. No server, no network, no background daemon. It is a library you open on a directory.
- Compact on disk. Data is written once to sorted tables and merged in the background shape of an LSM, not rewritten wholesale on each put.

A person reaches for it when they want a small, auditable, dependency-free ordered store they can read end to end. An AI agent reaches for it when it needs durable ordered local state across steps or sessions without standing up infrastructure, and wants a store whose correctness is proven by tests it can run.

## Quickstart

```rust
use keystone::{Db, Options};

let mut db = Db::open("mydata", Options::new())?;
db.put(b"user:1", b"alice")?;
db.put(b"user:2", b"bob")?;
db.delete(b"user:1")?;

assert_eq!(db.get(b"user:1")?, None);
assert_eq!(db.get(b"user:2")?, Some(b"bob".to_vec()));

for pair in db.scan(..)? {
    let (k, v) = pair?;
    println!("{} = {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
}

db.close()?;
# Ok::<(), keystone::Error>(())
```

## CLI

The crate ships a `keystone` binary that operates over a `--path` directory.

```
keystone --path ./data put apple red
keystone --path ./data get apple
keystone --path ./data del apple
keystone --path ./data scan            # all pairs in key order
keystone --path ./data scan user:      # only keys with a prefix
keystone --path ./data compact         # run pending compactions
keystone --path ./data stats           # print levels, file counts, sizes, seqno
keystone --path ./data verify          # read every sstable block and check its CRC
keystone --path ./data demo            # scripted workload that builds several levels
```

## API

- `Db::open(path, Options)` creates the directory if needed, loads the manifest, replays the WAL into a fresh memtable, and resumes the sequence number.
- `put(&[u8], &[u8])` and `delete(&[u8])` write through the WAL, then the memtable.
- `get(&[u8]) -> Option<Vec<u8>>` reads newest first: memtable, then L0 newest to oldest, then deeper levels. A tombstone reads back as `None`.
- `scan(range) -> impl Iterator` yields live pairs in ascending key order over any range.
- `flush()` writes the memtable to a new L0 table, commits the manifest atomically, and rotates the WAL.
- `compact()` runs all pending leveled compactions.
- `verify() -> VerifyReport` reads every block of every live table and checks its CRC, returning the number of tables and entries verified or a corruption error.
- `close()` flushes and shuts down cleanly.

Options are `memtable_size_bytes`, `block_size`, `bloom_bits_per_key`, `l0_compaction_trigger`, `level_size_multiplier`, and `sync_on_write`, each with a chainable setter.

## Correctness gates

Three gates carry the weight of the claim that Keystone is correct, durable, and robust against corrupt input. See `DESIGN.md` for why they prove it.

1. Differential fuzz. `tests/differential.rs` runs a random stream of put, delete, get, and scan over a small colliding key space against a `BTreeMap` oracle, interleaving forced flushes and compactions. After every single op it checks sampled point reads and a full ordered scan against the oracle, across several deterministic seeds. Raise the op count with `KEYSTONE_FUZZ_OPS`, for example `KEYSTONE_FUZZ_OPS=200000 cargo test --release differential`.

2. Crash recovery. `tests/recovery.rs` covers a durability round trip where the handle is dropped without a clean shutdown then reopened, a torn-write case where the WAL is truncated inside its last record and the store must lose at most that one trailing op with no corruption of earlier records, and a clean-flush reopen with an empty rotated WAL.

3. Corruption resistance. `tests/corruption.rs` builds a valid WAL, SSTable, and manifest, then sweeps every byte offset with a bit flip and every length with a truncation, plus adversarial length prefixes and random garbage files. Reading corrupt bytes must never panic, hang, over-allocate, or return wrong data. It must reproduce the original bytes exactly or fail with a clean error, and for the WAL drop the torn tail. Because every SSTable block now carries a CRC, the sweep confirms that all corruptions are detected rather than trusted.

Unit tests cover CRC vectors, varint round trips including full u64 values, bloom filters with no false negatives and rejection of malformed headers, SSTable write read and iterate round trips, WAL replay with bad-CRC tail discard, and the atomic manifest swap surviving a simulated crash between temp write and rename.

## Integrity

Every on-disk structure is checksummed. WAL records are framed with a CRC32, the manifest carries a trailing CRC32 over its whole body, and each SSTable block (data, index, and bloom) plus the SSTable footer carries its own CRC32 that is verified as it is read. A single flipped bit anywhere in a table surfaces as a clean corruption error rather than a wrong answer. Every decoder bounds any length it reads against the file size before allocating, so a corrupt length prefix cannot drive a huge allocation. The `verify` API and CLI command read every block of every live table and report the tables and entries checked.

## Build and test

```
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
cargo build --release
```

## License

MIT.
