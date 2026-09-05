//! # Keystone
//!
//! Keystone is a durable, ordered, embedded key value store built on a
//! log-structured merge tree in pure standard-library Rust with no external
//! dependencies.
//!
//! Writes land in a write-ahead log and an in-memory sorted memtable. When the
//! memtable fills it is flushed to an immutable on-disk sorted string table
//! (SSTable) at level 0. Leveled compaction merges tables downward, dropping
//! shadowed versions and tombstones at the bottom level. A durable manifest,
//! committed with an atomic temp-then-rename, records the live tables and the
//! monotonic sequence number so the store recovers exactly across a crash.
//!
//! ```
//! use keystone::{Db, Options};
//!
//! let dir = std::env::temp_dir().join("keystone-doctest");
//! let _ = std::fs::remove_dir_all(&dir);
//! let mut db = Db::open(&dir, Options::new()).unwrap();
//! db.put(b"alpha", b"one").unwrap();
//! db.put(b"beta", b"two").unwrap();
//! db.delete(b"alpha").unwrap();
//! assert_eq!(db.get(b"alpha").unwrap(), None);
//! assert_eq!(db.get(b"beta").unwrap(), Some(b"two".to_vec()));
//! let pairs: Vec<_> = db.scan(..).unwrap().map(|r| r.unwrap()).collect();
//! assert_eq!(pairs, vec![(b"beta".to_vec(), b"two".to_vec())]);
//! db.close().unwrap();
//! # let _ = std::fs::remove_dir_all(&dir);
//! ```

pub mod bloom;
pub mod compaction;
pub mod crc;
pub mod db;
pub mod error;
pub mod iter;
pub mod manifest;
pub mod memtable;
pub mod options;
pub mod rng;
pub mod sstable;
pub mod types;
pub mod varint;
pub mod wal;

pub use db::{Db, LevelStat, Scan, Stats};
pub use error::{Error, Result};
pub use options::Options;
pub use rng::Rng;
pub use types::{Entry, ValueType};

/// Alias for the primary engine type.
pub type Keystone = Db;
