# distqueue

A fault-tolerant distributed task queue in Go: priority scheduling, a
CRC-checked write-ahead log with crash recovery, group-committed fsyncs,
at-least-once delivery with retries and a dead-letter queue, a gRPC API,
and a worker SDK.

This is **Phase 1** of a two-phase project. Phase 2 replaces the WAL with
a hand-rolled Raft log and turns the broker into a replicated state
machine across a 3-node cluster.

```
Client (producer)
    |
    v  gRPC
 [Broker]
    | WAL (disk, fsync + group commit)
    | In-memory priority queue (min-heap)
    |
    +----> Worker A  --ack/nack--> Broker
    +----> Worker B  --ack/nack--> Broker
    +----> Worker C  --ack/nack--> Broker

    Unacked tasks ── timeout ──> re-enqueued
    Failed tasks (N retries) ──> Dead Letter Queue
```

## Quickstart

```bash
go build -o bin/ ./cmd/...

# 1. Start the broker
./bin/broker --port 9000 --wal /tmp/demo.wal --task-timeout 10s

# 2. Enqueue some work
./bin/produce --broker localhost:9000 --count 100

# 3. Start a worker (4 concurrent handlers, 10% simulated failures)
./bin/worker --broker localhost:9000 --concurrency 4 --fail-rate 0.1

# 4. Watch the queue drain
./bin/produce --broker localhost:9000 --stats
# pending=37 in_flight=4 dlq=0 acked=59

# 5. Inspect anything that exhausted its retries
./bin/produce --broker localhost:9000 --dlq
```

## Crash recovery demo

Every mutation is fsync'd to the WAL before it is acknowledged, so
`kill -9` loses nothing that was acknowledged. Actual transcript:

```
$ ./bin/produce --count 200            # 200 tasks in, worker chewing through them
$ ./bin/produce --stats
pending=110 in_flight=2 dlq=0 acked=88
$ kill -9 <broker pid>                 # hard kill, no shutdown hook

$ ./bin/broker --wal demo/demo.wal     # restart on the same WAL
recovered from WAL: 112 pending, 0 dead-lettered

$ ./bin/produce --stats                # worker drains the recovered queue
pending=0 in_flight=0 dlq=0 acked=112
```

88 acked before the crash + 112 recovered = all 200 accounted for. The
2 tasks in flight at crash time were redelivered — the worker processed
202 tasks total for 200 enqueued, which is **at-least-once delivery**
working as specified: nothing lost, duplicates possible, so handlers
must be idempotent. See [DESIGN.md](DESIGN.md) for why dequeues are
deliberately not logged.

## Benchmarks

`go test -bench . ./bench/` — Linux, 20-core CPU. Two storage targets,
because fsync cost dominates and honesty matters:

**ext4 on NVMe** (real fsync, ~0.6ms each):

| Benchmark | Throughput | Notes |
|---|---|---|
| Enqueue, 1 producer | 1.7k tasks/sec | fsync-bound: 1 sync per op |
| Enqueue, 160 producers | **61k tasks/sec** | group commit: many ops share 1 sync |
| Dequeue | 711k tasks/sec | pure in-memory |
| Enqueue→Dequeue→Ack round trip | P50 1.09ms / P99 1.58ms | 2 fsyncs per cycle |

**tmpfs** (fsync ~free; shows the CPU-side ceiling):

| Benchmark | Throughput |
|---|---|
| Enqueue, 1 producer | 214k tasks/sec |
| Dequeue | 700k tasks/sec |
| Enqueue→Dequeue→Ack round trip | P50 8.7µs / P99 28.5µs |

The 35× gap between 1 and 160 producers on ext4 is **group commit**: a
writer appends its record, then the first waiter fsyncs once for every
record written before the sync started. Same approach as PostgreSQL and
etcd; details in [DESIGN.md](DESIGN.md).

## Semantics

- **At-least-once delivery.** Acknowledged enqueues survive `kill -9`.
  A task is redelivered if its worker dies, stalls past the task
  timeout, or the broker crashes while it is in flight. Handlers must
  be idempotent.
- **Priority scheduling.** Lower number = higher priority; FIFO within
  a priority level.
- **Bounded retries.** A task that is nacked or times out `MaxRetries`
  times moves to the dead-letter queue, inspectable over the API.
- **Torn-write safety.** Every WAL record carries a CRC32; a corrupt
  tail (crash mid-write) is truncated at the last valid record on
  recovery.
- **Compaction.** The WAL is periodically rewritten to just live state
  (atomic temp-file + rename), so it doesn't grow without bound.

## Repository layout

```
broker/     core: priority queue, in-flight tracker, WAL, broker state machine
proto/      gRPC API definition
gen/        generated protobuf/gRPC code (checked in)
server/     gRPC server adapter
worker/     worker SDK (handler loop, backoff, concurrency)
cmd/        broker / worker / produce binaries
tests/      chaos + end-to-end tests
bench/      benchmarks
```

## Tests

```bash
go test -race ./...
```

Unit tests cover heap ordering, WAL roundtrip/replay/corruption, timeout
redelivery, and retry→DLQ promotion. Chaos tests crash the broker with
tasks pending and in flight, crash workers mid-execution, and run the
full gRPC stack with injected handler failures — all under the race
detector.

## Roadmap (Phase 2)

- Raft consensus (leader election, log replication, snapshots) — the WAL
  becomes the replicated log; the broker becomes the state machine
- 3-node cluster with leader redirects
- Linearizability checking (Porcupine) under chaos
- Prometheus metrics + Grafana dashboard, Docker Compose deploy
