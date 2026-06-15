#!/usr/bin/env bash
# Measure replicated throughput on a local 3-node Raft cluster: sequential
# (one client, one op at a time) and concurrent (N parallel producers).
# Prints `cluster seq_tasks_per_sec=<x>` and `cluster conc_tasks_per_sec=<x>`.
#
# A replicated write costs two fsyncs + an RPC round trip and is serialized
# by the whole-log persistence the design starts with — so these are
# intentionally far below single-node. See DESIGN.md.
set -uo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:/usr/local/go/bin"

go build -o bin/ ./cmd/... 2>/dev/null
D=$(mktemp -d)
trap 'pkill -x broker 2>/dev/null; rm -rf "$D"' EXIT

P1="node2=localhost:18002=localhost:19002,node3=localhost:18003=localhost:19003"
P2="node1=localhost:18001=localhost:19001,node3=localhost:18003=localhost:19003"
P3="node1=localhost:18001=localhost:19001,node2=localhost:18002=localhost:19002"
for n in 1 2 3; do
  eval "peers=\$P$n"
  ./bin/broker --cluster --node-id node$n --port 1900$n --raft-port 1800$n \
    --metrics-port 1700$n --data-dir "$D/node$n" --peers "$peers" \
    >"$D/node$n.log" 2>&1 &
done
ADDRS=localhost:19001,localhost:19002,localhost:19003
sleep 3  # let an election settle

acked() { curl -s "localhost:1700$1/metrics" 2>/dev/null | grep '^queue_acked_total' | awk '{print $2}'; }

# Sequential: one producer, fixed count, wall-clock timed.
SEQ_N=400
t0=$(date +%s.%N)
./bin/produce --broker "$ADDRS" --count $SEQ_N >/dev/null 2>&1
t1=$(date +%s.%N)
seq_rate=$(echo "$SEQ_N / ($t1 - $t0)" | bc -l)
printf 'cluster seq_tasks_per_sec=%.0f\n' "$seq_rate"

# Concurrent: 8 producers in parallel. Wait only on the producer PIDs —
# bare `wait` would also block on the never-exiting broker children.
CONC_N=400; PROCS=8
pids=()
t0=$(date +%s.%N)
for _ in $(seq 1 $PROCS); do
  ./bin/produce --broker "$ADDRS" --count $((CONC_N / PROCS)) >/dev/null 2>&1 &
  pids+=($!)
done
wait "${pids[@]}"
t1=$(date +%s.%N)
conc_rate=$(echo "$CONC_N / ($t1 - $t0)" | bc -l)
printf 'cluster conc_tasks_per_sec=%.0f\n' "$conc_rate"
