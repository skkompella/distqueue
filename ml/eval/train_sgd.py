#!/usr/bin/env python3
"""Train the SGD timeout model offline on simulation ground truth.

In the simulation we know each tick's true p90 execution time, so we can
label data the live system never could. We show the model a range of
*symptoms* — the observable (ack/nack) state produced when a given
in-effect timeout meets a given load — and teach it the *cause*: the ideal
timeout `p90 * HEADROOM`. The model learns to read the right timeout off
the symptoms.

Train/test honesty: training uses a p90 grid that EXCLUDES the held-out
eval loads (8s, 26s) and seeds disjoint from the eval seed, so
`replay.py --with-sgd` is a genuine generalization test, not train-on-test.

Run (needs the SGD extras):
    ml/.venv/bin/python ml/eval/train_sgd.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import QueueSnapshot  # noqa: E402
from eval.replay import _exec_times, _score_tick  # noqa: E402
from models.timeout_sgd import HEADROOM, SGDTimeoutModel, features  # noqa: E402

# Disjoint from eval (eval uses p90 ∈ {8, 26}, SEED 1234).
TRAIN_P90S = [6, 10, 14, 18, 22, 30, 34, 38]
HOLDOUT_P90S = [8, 26]
TRAIN_SEEDS = range(1, 21)
# The timeout in effect while we observe — sweeping it exposes the model to
# tight, matched, and loose regimes for the same load.
INEFFECT_TIMEOUTS = [5, 8, 12, 18, 25, 35, 50]
TASKS = 200


def _sample(p90s, seeds):
    import random

    X, y = [], []
    for seed in seeds:
        rng = random.Random(seed)
        for p90 in p90s:
            for t in INEFFECT_TIMEOUTS:
                exec_times = _exec_times(rng, p90, TASKS)
                acked, requeued = _score_tick(exec_times, t)
                snap = QueueSnapshot(
                    ts=0, pending=0, in_flight=0, dlq_depth=0,
                    acked_total=0, nacked_total=0,
                    ack_rate=float(acked), nack_rate=float(requeued),
                )
                # The symptom was observed under in-effect timeout `t`.
                X.append(features(snap, t))
                y.append(p90 * HEADROOM)
    return X, y


def _mae(model, X, y):
    return sum(abs(float(model.reg.predict([x])[0]) - t) for x, t in zip(X, y)) / len(y)


def main():
    Xtr, ytr = _sample(TRAIN_P90S, TRAIN_SEEDS)
    Xho, yho = _sample(HOLDOUT_P90S, [9999])  # unseen load AND unseen seed

    model = SGDTimeoutModel().fit_offline(Xtr, ytr)
    model.save()

    print(f"[train] {len(ytr)} train samples, {len(yho)} held-out")
    print(f"[train] train MAE  = {_mae(model, Xtr, ytr):.2f}s")
    print(f"[train] holdout MAE = {_mae(model, Xho, yho):.2f}s "
          f"(ideal timeouts {min(yho):.1f}–{max(yho):.1f}s)")
    from models.base import STATE_DIR
    print(f"[train] saved → {STATE_DIR}/{model.name}.pkl")
    return 0


if __name__ == "__main__":
    sys.exit(main())
