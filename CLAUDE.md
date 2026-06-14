# CLAUDE.md

Guidance for working in this repo. For the narrative/marketing overview see
[README.md](README.md); for design rationale and trade-offs see
[DESIGN.md](DESIGN.md). This file is the operational map.

## What this is

`distqueue` — a distributed task queue in Go, built in two phases:

- **Phase 1 (single node):** a broker with a CRC-checked write-ahead log,
  priority scheduling, at-least-once delivery, retries + dead-letter queue,
  group-committed fsyncs, a gRPC API, and a worker SDK.
- **Phase 2 (cluster):** a **from-scratch Raft** (election, log
  replication, snapshots — no etcd/raft library) that replaces the WAL and
  turns the broker into a replicated state machine across 3 nodes.

Module: `github.com/skkompella/distqueue` · `go 1.25` (toolchain 1.26
installed).

## Environment / toolchain gotchas (READ FIRST)

These bite every fresh shell — the Bash tool runs non-interactive, which
does **not** source `~/.bashrc`:

- **Go, protoc, and plugins are not on the default PATH.** Prefix Go
  commands with:
  ```bash
  export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin:$HOME/.local/bin
  ```
  (`go` → `/usr/local/go/bin`, `protoc` → `~/.local/bin`,
  `protoc-gen-go*` → `~/go/bin`.)
- **`/tmp` is tmpfs.** Benchmarks there report inflated fsync numbers. Run
  disk benchmarks with `TMPDIR=$PWD/.benchtmp` (ext4 on `$HOME`).
- **`pkill -f`/`pgrep -f` match the Bash tool's own wrapper script** (its
  argv contains the whole command string), so they kill the shell running
  them. Use `pkill -x broker` / `pkill -x worker`, or track PIDs in files.
- **Docker is not installed.** The `deploy/` Compose + Grafana stack is
  written but unrun — don't claim it works; verify the cluster with the
  manual multi-process commands below instead.

## Build, test, run

```bash
export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin:$HOME/.local/bin

go build -o bin/ ./cmd/...          # broker, worker, produce
go vet ./...
go test -race ./...                 # full suite; always use -race

# Raft tests are timing-sensitive — flush flakes before trusting a change:
go test -race -count 8 ./raft/
go test -race -count 3 ./tests/

# Benchmarks (real disk, not tmpfs):
mkdir -p .benchtmp && TMPDIR=$PWD/.benchtmp go test -bench . ./bench/

# Regenerate protobufs after editing proto/*.proto (code is checked in):
protoc --proto_path=proto --proto_path=$HOME/.local/include \
  --go_out=gen --go_opt=module=github.com/skkompella/distqueue/gen \
  --go-grpc_out=gen --go-grpc_opt=module=github.com/skkompella/distqueue/gen \
  proto/queue.proto proto/raft.proto
```

Run a 3-node cluster locally (no Docker): see the README "Quickstart
(3-node Raft cluster)" block — three `broker --cluster` processes with
`--peers id=raftAddr=clientAddr,...`, then clients with comma-separated
`--broker` addresses.

## Code map

| Path | Role |
|---|---|
| `broker/` | Phase 1 core: `task`, `priorityqueue`, `tracker`, `wal`, `broker`; **plus** `statemachine.go` (the replicated, deterministic queue used in cluster mode) |
| `raft/` | from-scratch Raft: `raft` (types/state), `election`, `replication`, `snapshot`, `persist`, `grpc` (transport + server) |
| `server/` | `grpc.go` (single-node adapter), `cluster.go` (Raft-backed TaskQueue: propose-and-wait, leader redirect, timeout redelivery), `metrics.go` (Prometheus) |
| `worker/` | consumer SDK: dequeue→handle→ack loop, backoff, concurrency |
| `client/` | `failover.go` — `queuepb.TaskQueueClient` that follows `leader=` hints and rotates on failure (drop-in for the worker SDK) |
| `cmd/` | `broker` (single-node + `--cluster`), `worker`, `produce` |
| `proto/`, `gen/` | gRPC schema and **checked-in** generated code |
| `tests/` | cross-package chaos + end-to-end (single-node and cluster) |
| `bench/` | throughput/latency benchmarks |
| `deploy/` | Dockerfile, 3-node Compose, Prometheus + Grafana (unrun) |

## Invariants — do not break these

- **Durability before acknowledgement.** Every state mutation
  (enqueue/ack/nack; Raft term/vote/log) is fsync'd *before* the operation
  is acknowledged or an RPC is answered. A failed fsync is fatal, never
  swallowed.
- **Dequeue is never logged or replicated.** It moves a task pending →
  in-flight only. This is what makes delivery **at-least-once** across
  crashes and leader failover; handlers must be idempotent. In cluster
  mode the in-flight set is a **leader-local overlay** (`statemachine.go` /
  `cluster.go`) — followers keep tasks pending, so a new leader redelivers.
- **Group commit (WAL).** Concurrent writers share one fsync; don't revert
  to fsync-per-op (it's a ~35× throughput cliff).
- **Raft from the paper.** Election restriction (§5.4.1), current-term-only
  commit (§5.4.2), leader no-op on election (§8). Changes here need the
  `raft/` suite green under `-race -count 8` and the linearizability test
  (`TestLinearizability`) passing.
- **Generated code is committed.** Re-run `protoc` and commit `gen/` when
  protos change; don't hand-edit `gen/`.
- **gRPC reconnect backoff is capped at 1s** (`raft/grpc.go`,
  `client/failover.go`). The default 120s cap causes rejoin term-storms —
  don't remove the `fastReconnect` connect params.

## Metrics

### Project metrics (current)

| | |
|---|---|
| Production Go (non-test, non-generated) | **~3,600 LOC** |
| Test Go | **~2,460 LOC** (test:prod ≈ 0.68) |
| Test + benchmark functions | **47** (broker 20, raft 14, tests 8, bench 5) |
| Largest packages | `raft/` ~1,290 · `broker/` ~1,150 · `server/` ~550 |
| Raft suite stability | green under `-race -count 8` |
| Linearizability | Porcupine-verified, ~143 ops/run with leader kills |

Per-package LOC and exact test inventory drift as code changes — regenerate
with the commands in "Build, test, run" rather than trusting these numbers
blindly after large edits.

### Runtime metrics (Prometheus)

Exposed on `--metrics-port` (e.g. `:7000/metrics`), defined in
`server/metrics.go`. All carry a `node` label.

Queue (single-node and cluster):

| Metric | Meaning |
|---|---|
| `queue_pending` | tasks awaiting delivery |
| `queue_in_flight` | delivered, not yet acked |
| `queue_dlq_depth` | dead-lettered tasks |
| `queue_acked_total` | acks since startup (use `rate()` for throughput) |

Raft (cluster mode only; absent single-node):

| Metric | Meaning |
|---|---|
| `raft_is_leader` | 1 on the leader, else 0 |
| `raft_term` | current term |
| `raft_commit_index` / `raft_last_applied` | replication/apply progress |
| `raft_log_entries` | live log length (post-compaction) |
| `raft_snapshot_index` | index covered by the latest snapshot |
| `raft_elections_started_total` | elections this node started (term-storm signal) |

When adding a metric: register it as a `GaugeFunc` in `ServeMetrics`, label
it via the shared `labels`, and add a row to the table above. If it's a
counter-like quantity, name it `_total` and read it with `rate()` in the
Grafana dashboard (`deploy/grafana/dashboards/distqueue.json`).

## Performance posture (so you don't "fix" a documented trade-off)

Replicated writes are intentionally modest (~165 tasks/sec sequential,
~350 concurrent): persistence rewrites the **whole** Raft log per write.
This is a deliberate correctness-first starting point. The optimization
path (incremental entry log + batched group persist — Phase 1's group
commit applied to Raft) is documented in `DESIGN.md`; pursue that rather
than ad-hoc patches if asked to speed up the cluster.

## Git

Default branch `main` holds Phase 1; Phase 2 lives on `phase-2-raft`
(pushed, PR pending). Branch off `main` for new work. `gh` is not installed
locally — open PRs via the GitHub compare URL or install/auth `gh` first.
End commit messages with the `Co-Authored-By: Claude` trailer.
