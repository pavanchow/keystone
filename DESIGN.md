# Keystone design

Keystone is a log-structured merge tree. This document describes the components, the exact on-disk byte layouts, the read path, the compaction strategy, the durability model, and an argument for why the two correctness gates actually prove correctness and durability.

## Components

- `error` defines the `Error` enum and `Result` alias used everywhere.
- `varint` is LEB128 encoding for u64 plus length-prefixed byte slices.
- `crc` is a table-based CRC32 using the IEEE reflected polynomial 0xEDB88320.
- `bloom` is a bloom filter using double hashing over two FNV-1a base hashes.
- `memtable` is an in-memory sorted table over a `BTreeMap`, tracking approximate byte size.
- `wal` is the write-ahead log with framed, checksummed records and torn-tail discard.
- `sstable` is the immutable on-disk sorted string table with data blocks, an index block, a bloom block, and a footer.
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

A data block packs sorted entries up to roughly `block_size` bytes. Each entry is:

```
[varint klen][key][u64 seqno][u8 type][varint vlen][value]
```

The index block starts with a varint count, then one record per data block:

```
[varint klen][first_key][u64 block_off][u64 block_len]
```

The bloom block is the serialized filter over every key in the table, laid out as `[u64 num_bits][u32 k][bit bytes]`.

The footer is a fixed 40 bytes at the very end:

```
[u64 index_off][u64 index_len][u64 bloom_off][u64 bloom_len][u64 magic]
```

Reading starts at the footer, which locates the index and bloom, so the reader loads those two structures and then serves point lookups and iteration from the data blocks.

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

## Why the gates prove it

### The differential gate proves functional correctness

A `BTreeMap` is an obviously correct model of an ordered key value map. The differential test drives Keystone and the model with the same random op stream over a deliberately small colliding key space, so overwrites and deletes actually land on existing keys and exercise version shadowing and tombstones. It forces flushes and compactions in the middle of the stream, so reads are served from every source: the memtable, multiple L0 tables, and compacted lower levels, including the tombstone-dropping bottom level.

After every single op it asserts two things. Sampled point reads match the model, which pins down the newest-wins resolution across all sources. A full ordered scan equals the model exactly, which pins down ordering, deduplication, tombstone hiding, and range clipping over the whole key space at once. Because the check runs after every op rather than only at the end, any divergence is caught at the exact op that introduced it, across several deterministic seeds. The op count is env controllable, so the same test scales from a fast CI run to a long soak.

### The recovery gate proves durability

The durability round trip applies a large op stream with syncing on, forces flushes so state is split between committed SSTables and the live WAL, then drops the handle with no clean shutdown, which is the software equivalent of a crash. Reopening from the same directory and matching the model proves that recovery reconstructs the exact state from the manifest plus the replayed WAL, and that the resumed sequence number keeps newest-wins intact.

The torn-write case writes records that live only in the WAL, then truncates the file at a random byte inside the last record, which is exactly what a crash mid-append leaves behind. On reopen the store must contain every earlier record intact and either the last op with its exact value or nothing at all, never garbage, and it must still be strictly ordered and scannable. That proves the framing and checksum actually isolate a torn tail rather than corrupting the log.

The clean-flush case flushes, confirms the WAL is empty, closes, and reopens to the exact flushed state, which proves the flush-then-rotate ordering leaves a consistent store with data served entirely from SSTables.

Together the two gates cover the two things an LSM must get right. The differential gate proves the query semantics over the full layered structure, and the recovery gate proves the on-disk state survives a crash and reloads to the same logical contents.
