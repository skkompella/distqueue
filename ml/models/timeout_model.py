"""Recommend the task timeout from observed nack pressure.

Signal: nack_pressure = nack_rate / ack_rate. A high ratio means workers
are being interrupted before they finish (timeout too tight); a sustained
near-zero ratio means we're leaving slack on the table and can tighten,
which makes genuinely stuck tasks redeliver sooner.

Model (the dependency-free default): an exponential moving average of a
target timeout that ratchets up fast on nack pressure and drifts down
slowly when the queue is healthy. EMA gives stability — one noisy scrape
can't yank the timeout around. The optional SGD variant (see module docs)
swaps this rule for a learned regressor behind the same API.
"""

from __future__ import annotations

from .base import OnlineModel

HIGH_PRESSURE = 0.10  # >10% nack:ack → too tight, raise
LOW_PRESSURE = 0.01  # <1% nack:ack → healthy, tighten gently
RAISE_FACTOR = 1.20
LOWER_FACTOR = 0.95
EMA_ALPHA = 0.30  # weight on the newest target; higher = more responsive
MIN_TIMEOUT = 5.0
MAX_TIMEOUT = 300.0


class TimeoutModel(OnlineModel):
    name = "timeout"

    def __init__(self, default_seconds: float = 30.0):
        self.value = float(default_seconds)

    def update(self, snap) -> None:
        target = self._target(snap, current=self.value)
        # EMA toward the target.
        self.value += EMA_ALPHA * (target - self.value)
        self.value = _clamp(self.value, MIN_TIMEOUT, MAX_TIMEOUT)

    def predict(self, snap) -> float:
        return _clamp(self.value, MIN_TIMEOUT, MAX_TIMEOUT)

    @staticmethod
    def _target(snap, current: float) -> float:
        pressure = snap.nack_pressure
        if pressure > HIGH_PRESSURE:
            return current * RAISE_FACTOR
        if pressure < LOW_PRESSURE:
            return current * LOWER_FACTOR
        return current


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))
