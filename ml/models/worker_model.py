"""Recommend a worker count to drain the backlog within a target window.

Formula: workers = ceil(pending / (ack_rate_per_worker * target_drain_s)),
where ack_rate_per_worker is estimated from the observed total ack rate
divided by the worker count in effect. We don't know the true per-worker
rate, so we learn an effective service rate via EMA and size the pool to
clear `pending` in `target_drain_seconds`.

v1 note: this recommendation is advisory — the worker process doesn't yet
auto-resize. The value is still written to broker.conf and scored offline
by eval/replay.py.
"""

from __future__ import annotations

import math

from .base import OnlineModel

EMA_ALPHA = 0.30
MIN_WORKERS = 1
MAX_WORKERS = 64


class WorkerModel(OnlineModel):
    name = "worker"

    def __init__(self, target_drain_seconds: float = 30.0, assumed_workers: int = 4):
        self.target_drain_seconds = float(target_drain_seconds)
        # Effective per-worker service rate (tasks/sec), learned via EMA.
        self.per_worker_rate = 0.0
        self.assumed_workers = assumed_workers

    def update(self, snap) -> None:
        if snap.ack_rate <= 0:
            return
        # Attribute the observed ack rate across the workers we believe are
        # running, and smooth it.
        observed = snap.ack_rate / max(self.assumed_workers, 1)
        if self.per_worker_rate == 0.0:
            self.per_worker_rate = observed
        else:
            self.per_worker_rate += EMA_ALPHA * (observed - self.per_worker_rate)

    def predict(self, snap) -> float:
        if self.per_worker_rate <= 0:
            # No throughput signal yet: keep current assumption.
            return float(self.assumed_workers)
        drainable_per_worker = self.per_worker_rate * self.target_drain_seconds
        needed = math.ceil(snap.pending / max(drainable_per_worker, 1e-6))
        # Always keep at least enough workers to match arrival; floor at 1.
        return float(_clamp(needed, MIN_WORKERS, MAX_WORKERS))


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))
