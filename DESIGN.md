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

---

# Phase 2: the Raft-replicated broker

In cluster mode the Raft log replaces the WAL: every queue mutation is a
replicated log entry, and the broker becomes the state machine committed
entries are applied to. The replay fold above *is* the apply function.

## Raft implementation choices

Built from the paper (Ongaro & Ousterhout), not ported from etcd:

- **Single mutex per node.** All Raft state behind one lock; RPC sends and
  applyCh delivery happen outside it. Same philosophy as the broker:
  correctness first, contention profiled later.
- **Election restriction (§5.4.1)** in RequestVote and the
  **current-term-only commit rule (§5.4.2)** in commit advancement — the
  two safety rules that make leader changes lossless.
- **Leader no-op on election (§8).** A new leader can't count replicas for
  old-term entries, so it commits a no-op of its own term immediately;
  otherwise the commit index stalls until client traffic arrives.
- **Conflict-term fast backtracking.** Followers reject inconsistent
  AppendEntries with (ConflictTerm, ConflictIndex), letting the leader
  skip a whole term per round trip instead of decrementing one entry at a
  time.
- **Snapshot sentinel.** `log[0]` always holds the snapshot boundary's
  (index, term), so index arithmetic is uniform before and after
  compaction. Followers too far behind get the full snapshot via
  InstallSnapshot.
- **Persistence is a whole-state atomic rewrite** (gob → temp file →
  fsync → rename), synchronous before any RPC reply or self-vote-count.
  Simple and obviously correct, but it makes log length the dominant cost
  of every replicated write — so the cluster snapshots aggressively
  (default: every 1000 applied entries) to keep the log short. The next
  optimization, deliberately not built yet, is an incremental entry log +
  batched group persist — the same idea as the Phase 1 WAL group commit,
  applied to Raft.
- **Capped reconnect backoff (1s) in the gRPC transport.** Found the hard
  way: gRPC's default backoff grows to 120s, so a node returning from a
  long outage stayed unreachable *inbound* (the leader's old channel was
  still backing off) while its own *outbound* votes worked — it kept
  deposing the leader with ever-rising terms and never heard the
  heartbeats that would have calmed it down. A ~1s cap bounds rejoin
  disruption to a couple of seconds. (Pre-Vote (§9.6) would remove the
  term inflation entirely; future work.)

## Delivery semantics across failover

Replicated through the log: **enqueue, ack, nack** (and via nack,
dead-lettering — the retry counter is replicated state, so every node
reaches the same DLQ verdict deterministically).

Deliberately *not* replicated: **dequeue**. Delivery is a leader-local
overlay — followers keep delivered-but-unacked tasks as pending. The
consequences, all consistent with Phase 1's at-least-once contract:

- Leader dies → new leader sees in-flight tasks as pending → redelivers.
- A node that loses leadership requeues its overlay locally (the
  leadership watcher), since those deliveries are now someone else's job.
- A worker's ack can land on a *different* leader than the one that
  delivered the task: acks validate against replicated state, not the
  delivery overlay, so completed work counts across failover.
- Task timeouts are detected by the leader but *recorded* as replicated
  nacks, keeping retry counts identical on every replica.

Exactly-once would require replicating delivery leases and client session
dedup; that buys little when handlers must be idempotent anyway.

## What the tests prove

- `raft/` unit suite: elections converge and stay stable; minority
  partitions can't commit; uncommitted entries from deposed leaders are
  overwritten and never applied; full-cluster power loss recovers from
  disk; snapshots trim, install, and survive restarts. State Machine
  Safety is asserted by index-aligned cross-node comparison.
- `raft/linearizability_test.go`: a single-register KV over the same raft
  package, 5 concurrent clients, leader killed repeatedly; the full
  operation history (including ambiguous timeouts, kept as open-ended
  operations) is verified linearizable with **Porcupine**.
- `tests/cluster_test.go`: the real stack — gRPC transport, cluster
  server, failover client — under leader kills and node restarts: no
  acknowledged enqueue lost, no acked task redelivered, restarted nodes
  catch up.

## Adaptive control plane

The broker's tuning knobs (timeout, retries, worker count) are guesses
baked in at startup. The control plane (`ml/`) makes them a feedback loop:
observe the broker's own metrics, decide, and push the decision back.

**Why SIGHUP + a config file, not a control RPC.** Reconfiguration is
rare, low-throughput, and operationally familiar — the nginx/Postgres
model. A file is greppable, hand-editable in a pinch, and survives a broker
restart; SIGHUP is a one-line handler. An RPC would mean a new proto
method, auth, and an always-on surface for something that fires every few
seconds at most. The file is written atomically (temp → `rename`) so the
broker, reading on SIGHUP, never sees a half-written TOML, and
`ApplyTuning` only touches the two genuinely live-reloadable fields under
the broker lock — `ScanInterval` and `WALPath` are bound to the tracker
ticker and WAL handle at construction and are deliberately left alone.

**Fail-static, never fail-open.** A malformed or absent config is logged
and the running config is kept. A bad push from the controller must never
be able to take the broker down — the worst case is "no change."

**Why the signal is `nack_rate / ack_rate`.** The broker can't report
per-task execution times without logging every dequeue (which the
at-least-once design deliberately avoids). But the *rate of redeliveries
relative to completions* is a faithful proxy for "timeout too tight": when
it climbs, workers are being interrupted before they finish. That single
ratio drives all three controllers, which is why Phase 3 first had to add a
`queue_nacked_total` counter — the broker tracked acks but not nacks.

**EMA, not "ML".** The controllers are exponential moving averages and
heuristics, chosen for interpretability and zero dependencies (the core
runs on the Python standard library, important on a possibly-offline box).
An EMA resists yanking the timeout around on one noisy scrape while still
adapting over a minute. A scikit-learn online-SGD variant slots in behind
the same `OnlineModel` interface, but it is an optional upgrade, not load
bearing — the honest framing is "adaptive control loop," not deep learning.

**What the ablation does and doesn't claim.** `eval/replay.py` is a
closed-loop *simulation* with known ground-truth execution times — the
controller reacts to the requeues its own timeout choice produces. It shows
the EMA *policy* beats both a fixed-conservative and a fixed-aggressive
timeout (conservative's low requeue rate at ~27% lower average timeout). It
is a policy comparison, not a measurement of a live cluster; a true live A/B
would need two clusters under identical load.

## The SGD upgrade — and why the heuristic still wins

The optional `SGDTimeoutModel` is the spec's "real ML" step, done properly
rather than for show:

- **Trained on labeled data the live system can't see.** In the simulation
  the true p90 execution time is known, so `eval/train_sgd.py` labels each
  observed (symptom → ideal-timeout) pair with `p90 · 1.2` and fits an
  `SGDRegressor`. The live broker has no per-task times — this is exactly
  why training happens against the simulation.
- **Honest train/test split.** Training uses a p90 grid and seeds *disjoint*
  from the held-out evaluation load, so `replay.py --with-sgd` measures
  generalization (~5s MAE on unseen loads), not memorization.
- **The feature that makes it learnable.** Raw observable state is
  under-determined: the same `nack_pressure` can mean a light load under a
  tight timeout or a heavy load under a loose one — different ideal
  timeouts. Feeding the *current setpoint* (the timeout in effect, which the
  controller always knows) as a feature resolves the ambiguity and roughly
  halved MAE. A small but real modeling insight.

**The result: SGD ties the conservative policy but does not beat EMA.** It
converges to a correct-but-cautious timeout because at a loose setpoint
almost nothing requeues — the signal that would justify tightening is
absent, so a model that only acts on evidence won't tighten. EMA's blind
ratchet (nudge down whenever it's quiet) is not justified by any
observation, yet that very inductive bias is what wins the latency/requeue
tradeoff. The lesson, kept in the repo precisely because it's the honest
one: a supervised model is bounded by the information in its features, and a
one-line heuristic can encode a useful prior the data cannot supply.

The SGD path is also strictly optional — lazy-imported behind a factory
with EMA fallback — so the control-plane core keeps its zero-dependency,
runs-anywhere property.

## The worker-count actuator

`worker_count` started life advisory; closing the loop needed a last hop
from the broker to a process the controller can't signal. Three choices
were on the table, and **advice-over-Stats** won:

- **Poll, not push.** The broker relays the recommendation in its existing
  Stats RPC (`advised_worker_count`); workers poll every few seconds. No
  new RPC surface, no broker→worker connection tracking, and it works
  through the failover client for any number of workers on any host —
  unlike a `worker.conf` + SIGHUP scheme, which assumes a shared filesystem
  and pid management.
- **`0` means "no advice."** Cluster nodes advertise 0 until config is
  replicated through Raft, so a worker pointed at a cluster simply keeps
  its configured size — no false signal, no special-casing.
- **Scale-down never kills in-flight work.** Each pool goroutine has a
  private stop channel checked only *between* iterations; a retiring worker
  finishes its current task first (the handler keeps the parent context).
  The stop channel also wakes idle workers out of their backoff sleep, so
  shrinking doesn't wait on a timer.
- The broker stays policy-free: it stores and serves the number, nothing
  else. The controller owns *what* to recommend; the worker owns *how* to
  resize. Live behavior: 2→7 under a produce flood, then a staircase back
  to 1 as the backlog drained (10 resizes, 57 config reloads, zero dropped
  tasks).
