# Keystone design

Keystone is a log-structured merge tree. This document describes the components, the exact on-disk byte layouts, the read path, the compaction strategy, the network server and its wire protocol, the durability model, and an argument for why the correctness gates actually prove correctness, durability, and robustness.

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
- `wire` is the framed binary protocol codec shared by the network client and server.
- `client` is a blocking client that speaks the wire protocol over a `TcpStream`.
- `server` is an embeddable TCP key-value server exposing one engine to many connections.

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

## Serving over the network

The engine can be exposed to many processes at once through `keystone serve` or through the embeddable `keystone::server::Server`. The network surface uses only `std::net` and threads, no external crates, and it speaks a fully specified framed binary protocol.

### Wire protocol

Communication is a sequence of frames over a TCP connection. A frame is a little endian `u32` payload length followed by exactly that many payload bytes. A request payload is one operation tag byte followed by an operation specific body. A response payload is one status tag byte followed by a status specific body. Keys and values are length prefixed with LEB128 varints, the same encoding the on-disk formats use. All fixed width integers are little endian.

Operations:

| tag | name   | request body                          | ok response body |
|-----|--------|---------------------------------------|------------------|
| 1   | ping   | empty                                 | empty            |
| 2   | put    | key, value                            | empty            |
| 3   | get    | key                                   | value, or status 1 |
| 4   | delete | key                                   | empty            |
| 5   | scan   | low bound, high bound, u32 page limit | has-more, pairs  |
| 6   | begin  | empty                                 | empty            |
| 7   | commit | empty                                 | empty            |
| 8   | abort  | empty                                 | empty            |
| 9   | stats  | empty                                 | engine snapshot  |

A key is a varint length followed by that many bytes. A value is a varint length followed by that many bytes. A bound is a kind byte, `0` unbounded, `1` included, `2` excluded, followed for kinds `1` and `2` by a key. A scan request carries the low bound, the high bound, then a `u32` page limit that the server clamps to its own cap. A scan answer carries a `u8` has-more flag, a varint pair count, then that many key value pairs. A scan page never exceeds the limit, plus one internal lookahead entry that is consumed but not sent, and its presence is what the has-more flag reports. A stats answer carries five `u64` fields, next sequence number, memtable keys, memtable bytes, total table files, total table bytes, then a `u32` level count and per level a `u32` level number with `u64` file count and `u64` bytes.

Statuses are `0` ok, `1` not found, used only by get, and `2` error, whose body is a varint length prefixed UTF-8 message.

The exact bytes of the smallest interesting exchange, a put of key `k` with value `v`:

```
02                put tag
01 6b             key: length 1, "k"
01 76             value: length 1, "v"
payload:          02 01 6b 01 76
full frame:       05 00 00 00 02 01 6b 01 76

answer payload:   00
answer frame:     01 00 00 00 00
```

Errors come in two classes with different consequences. A frame the server cannot parse at all, an empty payload, an unknown operation tag, a malformed body, a length prefix over the frame cap, gets one error frame and then the connection closes, because a client that emits unparseable bytes cannot be trusted to stay in sync. A semantic refusal, a commit with no open transaction, a transaction that overflowed its buffer, an engine error, gets an error frame and keeps the connection open. Every connection handler runs under a panic guard, a panic is counted on a shared counter rather than taking the server down, and the test gate asserts that adversarial input drives that counter to zero while the server keeps serving well behaved clients.

Caps bound every allocation the wire can drive, the same discipline the on-disk readers apply. The default frame payload cap is 8 MiB, enforced by client and server alike before either allocates. The default scan page cap is 10,000 pairs. The default transaction buffer cap is 64 MiB. The `serve` subcommand reads `KEYSTONE_SERVER_MAX_FRAME`, `KEYSTONE_SERVER_MAX_SCAN`, `KEYSTONE_SERVER_MAX_TX`, `KEYSTONE_SERVER_POLL_MS`, `KEYSTONE_SERVER_MEMTABLE`, and `KEYSTONE_SERVER_SHUTDOWN_FILE` from the environment to override these.

### Concurrency model

The server spawns one thread per accepted connection, and every connection shares one engine through a `std::sync::Mutex`. The choice is deliberate. Engine access is bounded by WAL fsyncs, so serializing it costs little throughput. A threads plus mutex design needs no async runtime, no external crates, and no lock-free reasoning to be correct. And the engine API already takes `&mut self` for writes, which a mutex models exactly. A scan page is served under one lock hold bounded by the page limit, so a large scan never stalls writers for long.

Each connection can open one write transaction with begin. While it is open, that connection's puts and deletes are buffered in memory, capped, and are invisible to every other client, because they have not touched the engine. Reads on the same connection see committed state only. Commit hands the buffer to `Db::write_batch` under a single lock acquisition. The engine appends every record to the WAL, fsyncs once, and only then applies the batch to the memtable, so no other client can ever observe a partial batch, and the whole batch costs one fsync instead of one per op. The last write to a key inside the batch wins. A transaction that exceeds its buffer cap is poisoned, its commit is refused, and the connection recovers by starting fresh.

### Server durability contract

Durability over the network is defined by one sentence: an ok response means the write is already fsynced to the write-ahead log, so the server can be killed at any instant and every acknowledged write survives.

- A put or delete answered ok is durable before the answer was sent, because the engine fsyncs the WAL record before acknowledging when `sync_on_write` is on, which is the default. A hard kill, Ctrl-C, or a power cut costs at most the one op that was in flight and not yet acknowledged.
- Writes buffered inside an open transaction are not yet durable, their per op ok means accepted, not committed. The commit ok is the durability point for the whole batch. A crash mid-commit can leave a prefix of the batch in the log, exactly like a sequence of independent puts cut short, and replay reconstructs that prefix record by record, never a torn value.
- A graceful shutdown, through the shutdown file or the programmatic flag, stops accepting connections, lets idle connections close within about one read tick, joins every worker, and flushes the memtable before the process exits with status 0. Pure std Rust cannot intercept SIGINT, so Ctrl-C kills the process outright, and that is safe by the first bullet, the durability lives in the WAL, not in the shutdown path.

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
- Bounded wire input. The server caps a frame length before allocating, decodes every operation body with checked bounds, refuses scan pages beyond its cap, and caps transaction buffers. Adversarial client bytes are handled exactly like adversarial on-disk bytes, rejected or answered with an error, never a panic or a huge allocation.

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

### The server gate proves network robustness and kill durability

The server gate runs everything over real 127.0.0.1 sockets against the real protocol, never a mocked transport. The round trip case pins the exact request and response bodies for every operation, including the missing key answer, bounded and unbounded scans, stats counters, and the transaction state machine with its semantic refusals. The concurrency case starts several real threads through a barrier so their writes interleave on the server, gives each thread a distinct key range and a local oracle, and then verifies the whole store through one client scan against the merged oracle, which exercises flushes and compactions happening underneath live network traffic.

The malformed input case writes a length prefix over the cap, a frame that promises bytes it never sends, an unknown operation tag, truncated bodies, a bogus bound kind, an empty frame, and pure garbage. Every case must end with either an error frame or a clean close, the server must keep serving well behaved clients afterward, and the shared panic counter must read zero, which is the network mirror of the corruption gate's never panic invariant.

The kill case spawns the actual `keystone serve` binary, writes a mixed put and delete stream with a small memtable so state spans SSTables and the live WAL, then sends SIGKILL with no shutdown path at all. Reopening the directory must reproduce the exact acknowledged state, which proves the durability contract, ack means fsynced, survives the harshest stop a supervisor or an operator can produce. A companion case stops the binary through the shutdown file and asserts a clean exit status, shutdown file removal, and a correct reopen.

Together the four gates cover what a durable ordered store must get right, embedded or served. The differential gate proves the query semantics over the full layered structure, the recovery gate proves the on-disk state survives a crash and reloads to the same logical contents, the corruption gate proves that damaged on-disk bytes fail loudly and safely instead of silently corrupting a read, and the server gate proves that the network surface holds the same two properties while many clients hit one engine over real sockets.
