# context.md — project state snapshot

Quick-orientation doc for anyone (human or agent) picking this repo up.
Deeper docs: [README.md](README.md) (results + narrative),
[CLAUDE.md](CLAUDE.md) (operational map + invariants),
[DESIGN.md](DESIGN.md) (rationale + trade-offs).

## What this is

`distqueue` — a distributed task queue built as a recruiter-facing
portfolio project (GitHub: **skkompella/distqueue**), in three phases:

1. **Single-node broker (Go)** — priority queue, CRC-checked WAL with
   crash recovery, **group-committed fsyncs**, at-least-once delivery,
   retries + DLQ, gRPC API, worker SDK.
2. **Raft cluster (Go)** — consensus **from scratch** (election, log
   replication, snapshots — no etcd/raft): the broker becomes a replicated
   state machine across 3 nodes; Porcupine-verified linearizable under
   leader kills.
3. **Adaptive control plane (Python, stdlib-only core)** — scrapes
   Prometheus metrics, tunes timeout/retries/workers via EMA/heuristic
   controllers, hot-reloads the broker through `broker.conf` + SIGHUP.
   Optional scikit-learn SGD timeout model behind a factory seam.

## Current state (June 2026)

**All three phases complete, tested, and pushed.** Branches are *stacked*:

| Branch | Contents | Status |
|---|---|---|
| `main` | Phase 1 | pushed |
| `phase-2-raft` | Phase 2 (on main) | pushed, **PR not yet opened** |
| `phase-3-ml` | Phase 3 + benchmarks/charts (on phase-2-raft) | pushed, current branch |

Merge order matters: `main` ← `phase-2-raft` ← `phase-3-ml`. `gh` is not
installed — open PRs via compare URLs. A prepared Phase-2 PR body exists at
`/tmp/pr-body.md` (may have been cleaned up; regenerate if gone).

## Headline numbers (all measured, reproducible)

- Enqueue: **1.6k → 48k tasks/sec** scaling 1→128 producers on ext4
  (group commit); dequeue ~740k/s; round trip P50 1.10ms / P99 1.68ms.
- Cluster: 672/s seq, 873/s concurrent; **~200ms leader failover**, ~2s
  rejoin. Intentionally modest (whole-log persist per write — documented
  trade-off, don't "fix" ad hoc; see DESIGN.md for the optimization path).
- Ablation: EMA controller **2.8% requeue @ 21.9s avg timeout** vs fixed
  30s (1.1%/30s) and fixed 10s (17.9%/10s). **SGD ties conservative
  (2.4%/31s) but does NOT beat EMA** — deliberate honest finding, don't
  "fix" it.
- Regenerate: `bash bench/run.sh && python3 bench/plot.py` (stdlib SVG
  charts → `docs/img/`).

## Key decisions (why things are the way they are)

- **Dequeue is never logged/replicated** → at-least-once across crashes
  and failover; in-flight is a leader-local overlay.
- **Failed fsync poisons the WAL**; durability always precedes ack.
- **gRPC reconnect backoff capped at 1s** (`fastReconnect`) — default 120s
  cap caused rejoin term-storms.
- **`ml/` core is stdlib-only**; sklearn lives only behind
  `timeout_factory` + `ml/.venv` (`requirements-sgd.txt`).
- Control plane is **single-node v1**; worker count is **actuated**
  (broker relays it via `Stats.advised_worker_count`; worker SDK polls and
  live-resizes, graceful scale-down); per-type timeouts deferred.

## Environment gotchas (bite every fresh shell)

- `export PATH=$PATH:/usr/local/go/bin:$HOME/go/bin:$HOME/.local/bin`
  (go / protoc / plugins are off the default non-interactive PATH).
- `/tmp` is tmpfs — run disk benchmarks with `TMPDIR=$PWD/.benchtmp`.
- `pkill -f` matches the shell's own wrapper script → use `pkill -x broker`.
- Docker **not installed**: `deploy/` compose+Grafana is written but unrun.
- Raft tests are timing-sensitive: trust changes only after
  `go test -race -count 8 ./raft/`.
- Python 3.14; sklearn pickles don't survive version bumps — retrain after
  upgrades.

## Open items / natural next steps

- Pre-Vote (§9.6), incremental Raft log persistence + batched group
  persist, client session dedup, cluster-mode hot-reload (config via
  Raft), run the Docker stack once Docker exists.
