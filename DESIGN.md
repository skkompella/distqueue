# Design Notes

## Delivery semantics: at-least-once, by construction

Every mutation that changes what the queue *owes* the world — enqueue, ack,
nack, dead-letter — is appended to the WAL and fsync'd **before** it is
applied in memory. One mutation is deliberately *not* logged: **dequeue**.

A dequeue only moves a task from the pending heap to the in-flight tracker.
If the broker crashes at that moment, replay sees an `ENQUEUE` with no
terminal record and recovers the task as pending — so it gets delivered
again. That is the at-least-once contract: a task is never lost, but a
handler may see it twice (crash after handling but before the ack lands, or
a handler that outlives its lease). **Handlers must be idempotent.**

The alternative (logging dequeues and leases for exactly-once-ish behavior)
buys little on a single node and is superseded entirely by Phase 2, where
the replicated log becomes the source of truth.

## WAL format

```
| Magic 'DQWL' (4) | EntryType (1) | PayloadLen (4) | Payload (N) | CRC32 (4) |
```

- `ENQUEUE (0x01)` carries the gob-encoded task. `ACK (0x02)`,
  `NACK (0x03)`, `DEAD (0x04)` carry only the task ID — that's all replay
  needs, and it keeps the hot ack path writing ~30 bytes instead of
  re-serializing the task.
- CRC32 (IEEE) covers the entry type and payload. On replay, a bad magic,
  bad CRC, or short read means a torn write from a crash: the file is
  truncated at the last valid entry and recovery proceeds with the valid
  prefix. Nothing fsync-acknowledged is ever in the torn region, so nothing
  acknowledged is lost.
- `PayloadLen` is sanity-capped (64 MiB) so a corrupt length can't OOM
  recovery.

## Replay fold

Recovery folds the log into live state in one pass:

| record  | effect on state                          |
|---------|------------------------------------------|
| ENQUEUE | task becomes pending                     |
| ACK     | task removed                             |
| NACK    | retry count incremented (stays pending)  |
| DEAD    | task moved to the dead-letter queue      |

## Compaction

The log grows with every operation, so the broker rewrites it every
`CompactEvery` appends (and after every recovery): the new log contains one
`ENQUEUE` per live task (pending and in-flight — in-flight tasks are live
because they'd be redelivered) plus `ENQUEUE`+`DEAD` pairs for the DLQ.
The rewrite goes to a temp file, fsync, `rename(2)` over the original,
fsync the directory — atomic on POSIX, so a crash mid-compaction leaves
either the old log or the new one, never a hybrid.

## Group commit

An fsync on a consumer NVMe drive costs ~0.5–1ms, so naive
fsync-per-operation caps the broker at ~1–2k ops/sec regardless of CPU.
The WAL instead **group-commits**, the same technique PostgreSQL and etcd
use:

1. An operation writes its record to the file (cheap, buffered) under the
   file lock and gets a sequence number.
2. It applies its in-memory state change, releases the broker lock, and
   calls `WaitSync(seq)`.
3. The first waiter to find no fsync in progress becomes the *sync
   leader*: it fsyncs once, and that single fsync covers every record
   written before it started. Everyone else waits for the broadcast.

N concurrent producers share ~1 fsync instead of paying N. On this
machine that is the difference between ~1.7k tasks/sec (single producer)
and ~61k tasks/sec (160 producers) on the same ext4 disk.

Two consequences worth knowing:

- **Visibility before durability, response after.** A task is visible to
  `Dequeue` as soon as its record is written, possibly before the fsync
  lands — but the producer's `Enqueue` call doesn't return until it is
  durable. If the broker crashes in that window the task may vanish; the
  producer never got an ack, so it retries. At-least-once holds for every
  acknowledged operation. (Record order in the file matches apply order,
  so a later ack's fsync also covers the enqueue it depends on.)
- **A failed fsync poisons the WAL.** After a sync error, the durability
  of already-written records is unknown, so every subsequent operation
  fails until the process restarts and replays the last consistent
  prefix. Never report an fsync that failed as eventually-succeeded.

## Timeouts and retries

In-flight tasks carry a deadline (`now + TaskTimeout` at dequeue). A scanner
goroutine sweeps every `ScanInterval` and routes expired tasks through the
same path as an explicit nack: retry count++, then re-enqueue or
dead-letter at `MaxRetries`. One code path for both means the WAL records
and retry accounting can't diverge.

A late ack for a task that already timed out returns `NotFound` — the task
has been handed to someone else, and the first completion to ack wins.

## Locking

The broker serializes all state changes behind one mutex; the WAL has its
own for the file handle. Lock order is always broker → tracker, and the
tracker's expiry scan calls back into the broker *without* holding the
tracker lock, which is what makes the order acyclic. A single coarse lock
is deliberate: correctness first. The expensive part — the fsync — happens
*outside* the broker lock via group commit, so the coarse lock guards only
cheap in-memory operations and a buffered write.

## Phase 2 direction

The Raft log replaces the WAL: each queue mutation becomes a replicated log
entry, and the broker becomes the state machine that committed entries are
applied to. The replay fold above *is* that state machine's apply function,
which is why mutations are already separated from durability here.
