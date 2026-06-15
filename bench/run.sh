#!/usr/bin/env bash
# Measure every benchmark and write raw output to docs/img/bench_raw.txt.
# Then `python3 bench/plot.py --parse` turns it into bench_data.json and
# `python3 bench/plot.py` renders the SVG figures.
#
#   bash bench/run.sh && python3 bench/plot.py
#
# ext4 numbers use TMPDIR on the (real-disk) repo; tmpfs uses /tmp. The
# difference is the fsync floor, which is the whole point of group commit.
set -euo pipefail
cd "$(dirname "$0")/.."

export PATH="$PATH:/usr/local/go/bin:$HOME/go/bin:$HOME/.local/bin"
OUT=docs/img/bench_raw.txt
mkdir -p docs/img .benchtmp
: > "$OUT"

section() { echo "=== SECTION $1 ===" >> "$OUT"; }

echo "[run] go benchmarks on ext4 (real fsync)..."
section ext4_bench
TMPDIR="$PWD/.benchtmp" go test -bench . -run '^$' -benchtime 2s ./bench/ >> "$OUT" 2>&1

echo "[run] go benchmarks on tmpfs (fsync ~free)..."
section tmpfs_bench
go test -bench . -run '^$' -benchtime 2s ./bench/ >> "$OUT" 2>&1

echo "[run] producer-scaling sweep (ext4)..."
section scaling
TMPDIR="$PWD/.benchtmp" go test -run TestEnqueueScaling -v ./bench/ 2>&1 | grep '^scaling' >> "$OUT"

echo "[run] latency distribution (ext4)..."
section latency_ext4
TMPDIR="$PWD/.benchtmp" go test -run TestLatencyDistribution -v ./bench/ 2>&1 | grep '^round-trip' >> "$OUT"

echo "[run] latency distribution (tmpfs)..."
section latency_tmpfs
go test -run TestLatencyDistribution -v ./bench/ 2>&1 | grep '^round-trip' >> "$OUT"

echo "[run] control-plane ablation (EMA)..."
section ablation
python3 ml/eval/replay.py >> "$OUT" 2>&1
if [ -x ml/.venv/bin/python ]; then
  echo "[run] ablation with SGD..."
  ml/.venv/bin/python ml/eval/train_sgd.py >/dev/null 2>&1 || true
  section ablation_sgd
  ml/.venv/bin/python ml/eval/replay.py --with-sgd >> "$OUT" 2>&1
fi

echo "[run] 3-node cluster throughput..."
section cluster
bash bench/cluster_throughput.sh >> "$OUT" 2>&1 || echo "cluster: measurement failed (skipped)" >> "$OUT"

rm -rf .benchtmp
echo "[run] done → $OUT"
