"""Recommend max_retries from the failure profile.

Intuition:
  - High nack:ack with DLQ filling → tasks are failing structurally, not
    transiently; more retries just waste work. Cut retries.
  - Moderate nack:ack, low DLQ → transient failures that retries fix.
    Keep a healthy retry budget.
  - Near-zero nack:ack → almost nothing fails; retries are nearly free
    insurance, keep the default.

Rule-based and interpretable. (The spec's logistic-regression upgrade can
replace `predict` once replay data justifies it.) An EMA on the chosen
level keeps the output from flapping between adjacent integers.
"""

from __future__ import annotations

from .base import OnlineModel

EMA_ALPHA = 0.30


class RetryModel(OnlineModel):
    name = "retry"

    def __init__(self, default: int = 3):
        self.value = float(default)

    def update(self, snap) -> None:
        self.value += EMA_ALPHA * (self._target(snap) - self.value)

    def predict(self, snap) -> float:
        return float(round(_clamp(self.value, 1, 5)))

    @staticmethod
    def _target(snap) -> float:
        pressure = snap.nack_pressure
        if pressure > 0.5:  # mostly failing → stop wasting retries
            return 1.0
        if pressure > 0.1:
            return 2.0
        return 3.0  # healthy → default budget


def _clamp(v, lo, hi):
    return max(lo, min(hi, v))
