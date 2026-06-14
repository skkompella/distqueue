"""The single seam that keeps scikit-learn out of the control plane's
default import path.

`make_timeout_model("ema")` (the default everywhere) imports nothing new.
`make_timeout_model("sgd")` lazily imports the sklearn-backed model; if
sklearn (or numpy) isn't installed, it logs and falls back to EMA, so the
controller never fails to start because of a missing optional dependency.
"""

from __future__ import annotations

from .timeout_model import TimeoutModel


def make_timeout_model(kind: str = "ema"):
    if kind == "ema":
        return TimeoutModel.load_or_new()
    if kind == "sgd":
        try:
            from .timeout_sgd import SGDTimeoutModel
        except ImportError as e:
            print(f"[timeout] sgd requested but scikit-learn is unavailable "
                  f"({e}); falling back to EMA")
            return TimeoutModel.load_or_new()
        return SGDTimeoutModel.load_or_new()
    raise ValueError(f"unknown timeout model {kind!r} (want 'ema' or 'sgd')")
