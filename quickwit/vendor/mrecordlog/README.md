# What is it?

This crate implements a solution to efficiently handle several record logs.
Each recordlog has its own "local" notion of position.
It is possible to truncate each of the queues individually.

# Goals

- be durable, offer some flexibility on `fsync` strategies.
- offer a way to truncate a queue after a specific position
- handle an arbitrary number of queues
- have limited IO
- be fast
- offer the possibility to implement push back

```rust
pub struct MultiRecordLog {
    pub fn create_queue(&mut self, queue: &str) -> Result<(), CreateQueueError>;
    pub fn delete_queue(&mut self, queue: &str) -> Result<(), DeleteQueueError>;
    pub fn queue_exists(&self, queue: &str) -> bool;
    pub fn list_queues(&self) -> impl Iterator<Item = &str> {
    pub fn append_record(
        &mut self,
        queue: &str,
        position_opt: Option<u64>,
        payload: &[u8],
    );
    pub fn truncate(&mut self, queue: &str, position: u64) -> Result<(), TruncateError>;
    pub fn range<R>(
        &self,
        queue: &str,
        range: R,
    ) -> Option<impl Iterator<Item = (u64, &[u8])> + '_>;
}
```

# Non-goals

This is not Kafka. This recordlog is designed for a "small amount of data".
All retained data can fit in RAM.

In the context of Quickwit, this queue is used in the ingest API and is meant to contain
1 minute worth of data. (At 60MB/s, means 3.6 GB of RAM)

Reading the recordlog files only happens on startup.
High-performance when reading the recordlog files is not a goal.
Writing fast on the other hand is important.

# Implementation details.

`mrecordlog` is multiplexing several independent queues into the same record log.
This approach has the merit of limiting the number of file descriptors necessary,
and more importantly, to limit the number of `fsync`.

It also offers the possibility to truncate the queue for a given record log.
The actual deletion of the data happens when a file only contains deleted records.
Then, and only then, the entire file is deleted.

That recordlog emits a new file every 1GB.
A recordlog file is deleted once all queues have been truncated after the
last record of a  of a file.

There is no compaction logic.

# On-disk format & compatibility

A record is serialized and split into one or more frames. Each frame starts with a fixed
7-byte header:

```
[ crc32(payload + type_byte):4 LE ][ payload_len:2 LE ][ type_byte:1 ]
```

`type_byte` is `FrameType as u8` in the low 7 bits (`Full = 1`, `First = 2`, `Middle = 3`,
`Last = 4`, a strict whitelist) plus a high **compressed flag** (`0b1000_0000`). When the
flag is set the frame payload is a zstd-compressed chunk of the record, prefixed by the
record's uncompressed length as a `u32`. The flag is part of the CRC-covered type byte, so
it cannot be flipped silently. Records written without compression have the flag clear and
decode unchanged.

Record compression is **opt-in**. `open_with_prefs` (and `open`) disable it; use
`open_with_prefs_and_compression(path, policy, true)` to enable it. Reading is
*unconditional*: a reader always accepts both compressed and uncompressed frames,
regardless of how the writer was configured.

Compatibility matrix:

| reader \ writer | uncompressed frames | compressed frames |
|---|---|---|
| pre-compression binary | reads normally | `FrameType::from_u8` returns `None` → `Corruption`; the block is skipped during recovery (silent partial loss, no mis-decode) |
| compression-aware binary | reads normally | reads normally |

Because an old binary **cannot** read compressed frames, compression must never be enabled
on a WAL that an older binary may still need to open. In particular, a new binary that has
compressed a WAL must not be rolled back to a pre-compression binary without draining
first:

> **No-downgrade rule.** While `compression_enabled` is `true`, do not downgrade the node
to a build that predates frame compression. Drain the WAL (truncate all queues and let the
files be deleted) before downgrading, or keep compression disabled on WALs that may be
opened by older binaries.

# TODO

- add backpressure.
- add fsync policy
- better testing.
- non auto-inc position
- less Arc
