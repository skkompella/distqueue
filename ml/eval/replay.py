#!/usr/bin/env python3
"""Offline ablation: compare timeout-tuning policies on a workload with
KNOWN ground-truth execution times.

This is a *policy comparison in simulation*, not a measurement of a live
run. Why simulation: the broker exposes only aggregate rates, so a live
A/B would need two real clusters under identical load. Here we generate a
workload with phases of known p90 execution time, then score three
policies on the same stream:

  - fixed-conservative: timeout pinned high (safe but slow to detect
    genuinely stuck tasks → high time-to-redeliver)
  - fixed-aggressive:   timeout pinned low (fast detection but requeues
    legitimate slow tasks when load rises)
  - controller:         the real TimeoutModel, reacting in closed loop to
    the requeue rate its own timeout produces

The headline: the controller approaches the aggressive policy's low
average timeout while keeping the conservative policy's low requeue rate.

A recorded JSONL (`--data`) is used only to size the run (tick count); the
workload phases are synthetic and deterministic (fixed seed).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import QueueSnapshot  # noqa: E402
from models.timeout_model import TimeoutModel  # noqa: E402

SEED = 1234
TASKS_PER_TICK = 200
SIGMA = 0.5  # lognormal spread of execution times

# (ticks, true p90 execution seconds). A calm start, a load spike where
# tasks legitimately run long, then recovery. Lengths reflect that a
# control loop runs continuously — it earns its keep over many ticks of
# steady load, not in a handful.
DEFAULT_PHASES = [(50, 8.0), (25, 26.0), (50, 8.0)]


def _exec_times(rng, p90, n):
    # lognormal with median chosen so the 90th percentile ≈ p90.
    median = p90 / math.exp(1.2816 * SIGMA)
    mu = math.log(median)
    return [rng.lognormvariate(mu, SIGMA) for _ in range(n)]


def _score_tick(exec_times, timeout):
    """Return (acked, requeued) for one tick under a given timeout."""
    requeued = sum(1 for t in exec_times if t > timeout)
    return len(exec_times) - requeued, requeued


def simulate(phases=DEFAULT_PHASES, extra_adaptive=None):
    """Score fixed and adaptive timeout policies on one shared workload.

    extra_adaptive: optional (name, model) for a second adaptive controller
    (e.g. the SGD model) evaluated on the SAME held-out load as EMA. Each
    adaptive controller runs in closed loop on its OWN previous-tick outcome
    snapshot, predicting then adapting.
    """
    rng = random.Random(SEED)

    fixed = {"fixed-conservative (30s)": 30.0, "fixed-aggressive (10s)": 10.0}
    adaptives = [("controller (EMA)", TimeoutModel(default_seconds=30.0))]
    if extra_adaptive is not None:
        adaptives.append(extra_adaptive)

    names = list(fixed) + [n for n, _ in adaptives]
    totals = {n: {"acked": 0, "requeued": 0, "timeout_sum": 0.0, "ticks": 0}
              for n in names}
    last_snap = {n: _neutral_snap() for n, _ in adaptives}

    for ticks, p90 in phases:
        for _ in range(ticks):
            exec_times = _exec_times(rng, p90, TASKS_PER_TICK)  # shared stream
            for name, timeout in fixed.items():
                _accumulate(totals[name], *_score_tick(exec_times, timeout), timeout)
            for name, model in adaptives:
                timeout = model.predict(last_snap[name])
                acked, requeued = _score_tick(exec_times, timeout)
                _accumulate(totals[name], acked, requeued, timeout)
                outcome = _outcome_snap(acked, requeued)
                model.update(outcome)  # close the loop: adapt on what it caused
                last_snap[name] = outcome

    return {n: _summary(totals[n]) for n in names}


def _accumulate(agg, acked, requeued, timeout):
    agg["acked"] += acked
    agg["requeued"] += requeued
    agg["timeout_sum"] += timeout
    agg["ticks"] += 1


def _neutral_snap():
    return QueueSnapshot(ts=0, pending=0, in_flight=0, dlq_depth=0,
                         acked_total=0, nacked_total=0)


def _outcome_snap(acked, requeued):
    # Encode the realized ack/requeue counts as rates (per 1s tick).
    return QueueSnapshot(ts=0, pending=0, in_flight=0, dlq_depth=0,
                         acked_total=0, nacked_total=0,
                         ack_rate=float(acked), nack_rate=float(requeued))


def _summary(agg):
    total = agg["acked"] + agg["requeued"]
    return {
        "requeue_rate": agg["requeued"] / total if total else 0.0,
        "avg_timeout": agg["timeout_sum"] / agg["ticks"] if agg["ticks"] else 0.0,
    }


def _load_sgd():
    """Load the trained SGD model. Lazy + guarded so the default replay run
    stays stdlib-only and works without scikit-learn installed."""
    try:
        from models.timeout_sgd import SGDTimeoutModel
    except ImportError as e:
        print(f"--with-sgd requested but scikit-learn is unavailable ({e}).\n"
              f"Install it: ml/.venv/bin/pip install -r ml/requirements-sgd.txt")
        return None
    model = SGDTimeoutModel.load_or_new()
    if not getattr(model, "_fitted", False):
        print("note: no trained SGD model found — run eval/train_sgd.py first "
              "(the SGD row would otherwise be cold-start).")
    return ("controller (SGD)", model)


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="recorded JSONL; only its length scales the run")
    ap.add_argument("--with-sgd", action="store_true",
                    help="add the trained scikit-learn SGD controller (needs the SGD extras)")
    args = ap.parse_args(argv)

    phases = DEFAULT_PHASES
    if args.data and os.path.exists(args.data):
        with open(args.data) as f:
            n = sum(1 for line in f if line.strip())
        if n > 0:
            # Spread recorded ticks across the three-phase shape.
            unit = max(1, n // 3)
            phases = [(unit, 8.0), (unit, 26.0), (n - 2 * unit, 8.0)]

    extra = _load_sgd() if args.with_sgd else None
    results = simulate(phases, extra_adaptive=extra)

    print(f"\nAblation — timeout tuning policies "
          f"({sum(t for t, _ in phases)} ticks, {TASKS_PER_TICK} tasks/tick)\n")
    print(f"{'policy':<28}{'requeue rate':>14}{'avg timeout':>14}")
    print("-" * 56)
    for name, r in results.items():
        print(f"{name:<28}{r['requeue_rate'] * 100:>13.1f}%{r['avg_timeout']:>12.1f}s")

    base = results["fixed-conservative (30s)"]
    for ctrl_name in [n for n in results if n.startswith("controller")]:
        ctrl = results[ctrl_name]
        dt = (ctrl["avg_timeout"] - base["avg_timeout"]) / base["avg_timeout"] * 100
        print(f"\n{ctrl_name} vs fixed-conservative: {dt:+.1f}% avg timeout "
              f"at {ctrl['requeue_rate'] * 100:.1f}% requeue "
              f"(vs {base['requeue_rate'] * 100:.1f}%)")
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
