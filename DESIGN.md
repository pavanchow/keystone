# Keystone design

Keystone is a log-structured merge tree. This document describes the components, the exact on-disk byte layouts, the read path, the compaction strategy, the durability model, and an argument for why the two correctness gates actually prove correctness and durability.

## Components

- `error` defines the `Error` enum and `Result` alias used everywhere.
- `varint` is LEB128 encoding for u64 plus length-prefixed byte slices.
- `crc` is a table-based CRC32 using the IEEE reflected polynomial 0xEDB88320.
- `bloom` is a bloom filter using double hashing over two FNV-1a base hashes.
- `memtable` is an in-memory sorted table over a `BTreeMap`, tracking approximate byte size.
- `wal` is the write-ahead log with framed, checksummed records and torn-tail discard.
- `sstable` is the immutable on-disk sorted string table with per-block CRC checked data blocks, an index block, a bloom block, and a checksummed footer.
- `manifest` is the durable catalog of live tables, committed with an atomic temp-then-rename.
- `iter` is a k-way merge that yields the newest version per user key.
- `compaction` is the leveled compaction driver.
- `db` wires it all together into the engine.

## Sequence numbers and MVCC

Every write is stamped with a strictly increasing 64-bit sequence number. A record is identified by its user key plus its sequence number. When the same user key has several versions across the memtable and the levels, the version with the highest sequence number is the current one. A delete is a tombstone, a versioned record that marks the key absent. Sequence numbers give a total order over all writes, which is what makes newest-wins deterministic and what makes recovery exact. The next sequence number is persisted in the manifest and also recoverable from the WAL.

## On-disk byte layouts

### WAL record

Each record is a frame:

```
[u32 payload_len][u32 crc32(payload)][payload]
```

The payload is:

```
[u8 type: 0 put, 1 delete][u64 seqno][varint klen][key][varint vlen][value]
```

The value is absent for a delete. All fixed integers are little endian. The length prefix and the checksum are what make a torn tail detectable. A short read of either the header or the payload, or a checksum mismatch, means the record was never fully committed, so replay stops and discards from that point.

### SSTable

```
[data block 0][data block 1]...[index block][bloom block][footer]
```

Every block on disk, whether a data block, the index block, or the bloom block, is stored as its payload followed by a 4 byte CRC32 trailer over that payload:

```
[block payload][u32 crc32(payload)]
```

A data block payload packs sorted entries up to roughly `block_size` bytes. Each entry is:

```
[varint klen][key][u64 seqno][u8 type][varint vlen][value]
```

The index block payload starts with a varint count, then one record per data block. The stored length is the payload length, and the reader adds the CRC trailer:

```
[varint klen][first_key][u64 block_off][u64 block_payload_len]
```

The bloom block payload is the serialized filter over every key in the table, laid out as `[u64 num_bits][u32 k][bit bytes]`.

The footer is a fixed 44 bytes at the very end:

```
[u64 index_off][u64 index_len][u64 bloom_off][u64 bloom_len][u32 crc32(first 32 bytes)][u64 magic]
```

Reading starts at the footer. The magic and the footer CRC are checked first, so a corrupt set of offsets is rejected before any of them is used. The four offsets and lengths are the payload offset and payload length of the index and bloom blocks. Each block read bounds the offset and length against the file size before allocating, then verifies the block CRC, so a corrupt or truncated file fails with a clean error instead of over-allocating, panicking, or returning a wrong value. The reader loads the index and bloom, then serves point lookups and iteration from the data blocks.

### Manifest

```
[u64 magic][u64 next_file_id][u64 next_seqno][varint num_tables]
  per table: [varint level][u64 file_id][varint klen][smallest_key]
             [varint klen][largest_key][u64 smallest_seqno]
             [u64 largest_seqno][u64 file_size]
[u32 crc32(all preceding bytes)]
```

The trailing checksum makes a partially written or corrupted manifest detectable on load.

## Read path

A point read resolves newest to oldest.

1. Check the memtable. If the key is present it is authoritative, whether it is a value or a tombstone.
2. Otherwise gather the tables whose key range covers the key, ordered by level ascending and, within L0, by file id descending. L0 tables can overlap, and a higher file id is a later flush, so file id descending is newest first. Deeper levels are non-overlapping, so at most one table per level covers the key.
3. For each candidate, the bloom filter rejects most misses without touching disk. On a possible hit, binary search the index for the one block that could hold the key, read that block, and scan it. The first table that returns the key wins. A tombstone resolves to `None`.

A scan builds a k-way merge over the memtable and every table. The merge yields the newest version per user key in ascending key order. The scan layer skips tombstones and clips to the requested range.

## Leveled compaction

L0 holds tables written directly by memtable flushes, so L0 tables may overlap in key range. When the L0 file count reaches `l0_compaction_trigger`, all of L0 plus the overlapping L1 tables merge into fresh non-overlapping L1 tables.

For a deeper level that exceeds its byte budget, one table from that level merges with the overlapping tables one level down, producing new non-overlapping tables at the lower level. Level budgets grow by `level_size_multiplier` per level.

Every merge uses the k-way merge iterator, so for each user key only the newest version flows through. When the output level is the bottom-most populated level, nothing below can be shadowed, so tombstones and older shadowed versions are dropped there. Above the bottom level, tombstones are kept, because an older value for the same key may still live further down and the tombstone is what shadows it.

Compaction is triggered synchronously after a flush, so behavior is deterministic and testable without background threads.

## Durability model

Durability rests on four mechanisms.

- Write-ahead logging. A put or delete is appended to the WAL before it is applied. With `sync_on_write` the append is fsynced before the call returns, so an acknowledged write is on stable storage.
- Framed checksummed records. The length prefix plus CRC32 make an incomplete trailing record detectable, so a crash mid-write costs at most the one op that was in flight.
- Atomic manifest commit. A new catalog is written to a temp file, fsynced, then renamed over MANIFEST. Rename is atomic on a single filesystem, so a reader always sees either the whole old manifest or the whole new one, never a mix. A crash between the temp write and the rename leaves the committed manifest untouched.
- Flush ordering. On flush the new SSTable is written and fsynced, then the manifest is committed, and only then is the WAL rotated. A crash at any point leaves either the WAL still holding the data or the SSTable already committed, so the data is never in neither place.

## Integrity and corruption resistance

Durability protects against a crash. Integrity protects against on-disk bytes that are wrong, whether from a torn write, a failing disk, a stray bit flip, or a hand-edited file. Keystone treats every persisted structure as untrusted input on read.

- Checksums everywhere. WAL records are framed with a CRC32, the manifest carries a trailing CRC32 over its whole body, and each SSTable block plus the SSTable footer carries its own CRC32. Any single-byte corruption in a table breaks exactly one CRC and is caught on read, so a bit flip surfaces as a corruption error rather than a wrong answer.
- Bounded allocation. Every decoder validates any length it reads against the file size or a fixed cap before it allocates. A corrupt SSTable footer whose index length has been flipped to a huge value is rejected by the footer CRC and the bounds check, not by an attempt to allocate terabytes. A corrupt WAL length prefix is capped and treated as a torn tail rather than a giant buffer.
- Clean failure. A corrupt or truncated structure returns a `Corruption` error. The WAL is the one place that recovers by design, dropping the torn or corrupt tail so every intact earlier record survives. No decoder panics, overflows, hangs, or reads out of bounds on adversarial input.
- Whole-store verify. `Db::verify`, exposed as the `verify` CLI command, opens every live table and reads every block end to end, which forces every block CRC and every entry decode, and reports the tables and entries checked or the first corruption found.

## Why the gates prove it

### The differential gate proves functional correctness

A `BTreeMap` is an obviously correct model of an ordered key value map. The differential test drives Keystone and the model with the same random op stream over a deliberately small colliding key space, so overwrites and deletes actually land on existing keys and exercise version shadowing and tombstones. It forces flushes and compactions in the middle of the stream, so reads are served from every source: the memtable, multiple L0 tables, and compacted lower levels, including the tombstone-dropping bottom level.

After every single op it asserts two things. Sampled point reads match the model, which pins down the newest-wins resolution across all sources. A full ordered scan equals the model exactly, which pins down ordering, deduplication, tombstone hiding, and range clipping over the whole key space at once. Because the check runs after every op rather than only at the end, any divergence is caught at the exact op that introduced it, across several deterministic seeds. The op count is env controllable, so the same test scales from a fast CI run to a long soak.

### The recovery gate proves durability

The durability round trip applies a large op stream with syncing on, forces flushes so state is split between committed SSTables and the live WAL, then drops the handle with no clean shutdown, which is the software equivalent of a crash. Reopening from the same directory and matching the model proves that recovery reconstructs the exact state from the manifest plus the replayed WAL, and that the resumed sequence number keeps newest-wins intact.

The torn-write case writes records that live only in the WAL, then truncates the file at a random byte inside the last record, which is exactly what a crash mid-append leaves behind. On reopen the store must contain every earlier record intact and either the last op with its exact value or nothing at all, never garbage, and it must still be strictly ordered and scannable. That proves the framing and checksum actually isolate a torn tail rather than corrupting the log.

The clean-flush case flushes, confirms the WAL is empty, closes, and reopens to the exact flushed state, which proves the flush-then-rotate ordering leaves a consistent store with data served entirely from SSTables.

### The corruption gate proves robustness against bad bytes

The corruption gate builds a valid WAL, SSTable, and manifest, then mutates each one exhaustively. It flips a bit at every byte offset, truncates at every length, injects adversarial length prefixes, and throws random garbage files at every reader. For each mutation it runs the reader under a panic guard and classifies the outcome. The invariant is that reading corrupt bytes never panics and never returns wrong data. It either reproduces the original bytes exactly or fails with a clean error, and for the WAL it yields a prefix of the original records. Because every SSTable byte lives inside a CRC covered block or the checksummed footer, the sweep shows every SSTable mutation being detected rather than served as a wrong answer, which is exactly the property the block checksums exist to provide.

Together the three gates cover what an LSM must get right. The differential gate proves the query semantics over the full layered structure, the recovery gate proves the on-disk state survives a crash and reloads to the same logical contents, and the corruption gate proves that damaged on-disk bytes fail loudly and safely instead of silently corrupting a read.
