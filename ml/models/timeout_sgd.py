"""scikit-learn online-SGD timeout model — the optional upgrade to the EMA
controller (the spec's Step 8).

This module imports numpy + scikit-learn and is therefore loaded ONLY on
the SGD path (via models.timeout_factory). The control-plane core never
imports it, so a bare-Python install keeps working.

What it learns: a mapping from the broker's OBSERVABLE state to a good
timeout. Offline (eval/train_sgd.py) it is trained on simulation data where
the ground-truth p90 execution time is known and the label is
`p90 * HEADROOM`. Live, where no per-task time exists, it continues with
`partial_fit` on the same self-supervised nack-pressure signal the EMA
model uses — so its edge, if any, comes from the learned offline prior.

Unlike the EMA model (whose predict ignores the snapshot and returns an
integrated value), this model's predict READS the snapshot features — it
can react to current observable state within a single tick.
"""

from __future__ import annotations

import math

import numpy as np
from sklearn.linear_model import SGDRegressor

from .base import OnlineModel
from .timeout_model import (
    HIGH_PRESSURE,
    LOW_PRESSURE,
    LOWER_FACTOR,
    MAX_TIMEOUT,
    MIN_TIMEOUT,
    RAISE_FACTOR,
)

HEADROOM = 1.2  # timeout target = p90 execution time * HEADROOM
N_FEATURES = 6

PRESSURE_CAP = 10.0  # nack_pressure saturates here (10× more requeues than acks)


def features(snap, in_effect_timeout: float) -> list[float]:
    """Observable feature vector, fixed-scaled (no fitted scaler to drift).

    The first feature is the timeout currently IN EFFECT — without it the
    observable state is under-determined: a given nack_pressure could be a
    light load under a tight timeout or a heavy load under a loose one, and
    those imply different ideal timeouts. The current setpoint resolves the
    ambiguity, and the controller always knows it (it's what it last set).

    nack_pressure is capped — it's unbounded when a too-tight timeout
    requeues everything (ack_rate→0), which would destabilize the regressor.
    """
    return [
        in_effect_timeout / 60.0,
        min(snap.nack_pressure, PRESSURE_CAP),
        math.log1p(max(0.0, snap.ack_rate)),
        math.log1p(max(0.0, snap.nack_rate)),
        math.log1p(max(0.0, snap.pending)),
        math.log1p(max(0.0, snap.in_flight)),
    ]


class SGDTimeoutModel(OnlineModel):
    name = "timeout_sgd"

    def __init__(self, default_seconds: float = 30.0):
        self.default = float(default_seconds)
        # The timeout currently in effect (the last value we recommended).
        # Used as a feature and updated on each predict, so the closed loop
        # observes outcomes labeled with the setpoint that produced them.
        self._last_pred = float(default_seconds)
        # Adaptive LR keeps learning until the loss plateaus; small alpha
        # regularizes the thin feature set.
        self.reg = SGDRegressor(
            loss="squared_error",
            learning_rate="adaptive",
            eta0=0.05,
            alpha=1e-4,
            random_state=0,
        )
        self._fitted = False

    # --- offline training (eval/train_sgd.py) ---

    def fit_offline(self, X, y) -> "SGDTimeoutModel":
        self.reg.fit(np.asarray(X, dtype=float), np.asarray(y, dtype=float))
        self._fitted = True
        return self

    # --- OnlineModel interface ---

    def _raw_predict(self, snap, in_effect: float) -> float:
        """Pure prediction (no state mutation) for a given in-effect timeout."""
        if not self._fitted:
            return _clamp(self.default, MIN_TIMEOUT, MAX_TIMEOUT)
        x = np.asarray([features(snap, in_effect)], dtype=float)
        return _clamp(float(self.reg.predict(x)[0]), MIN_TIMEOUT, MAX_TIMEOUT)

    def predict(self, snap) -> float:
        pred = self._raw_predict(snap, self._last_pred)
        self._last_pred = pred  # becomes the in-effect setpoint for next tick
        return pred

    def update(self, snap) -> None:
        # This snapshot was produced under the timeout last in effect
        # (self._last_pred). Label it with the same self-supervised
        # nack-pressure heuristic the EMA model uses; _last_pred is advanced
        # only by predict, so this stays consistent.
        target = self._online_target(snap)
        x = np.asarray([features(snap, self._last_pred)], dtype=float)
        self.reg.partial_fit(x, np.asarray([target], dtype=float))
        self._fitted = True

    def _online_target(self, snap) -> float:
        current = self._raw_predict(snap, self._last_pred)
        pressure = snap.nack_pressure
        if pressure > HIGH_PRESSURE:
            return _clamp(current * RAISE_FACTOR, MIN_TIMEOUT, MAX_TIMEOUT)
        if pressure < LOW_PRESSURE:
            return _clamp(current * LOWER_FACTOR, MIN_TIMEOUT, MAX_TIMEOUT)
        return current


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))
