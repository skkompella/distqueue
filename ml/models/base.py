"""OnlineModel: the controller's contract for an incrementally-trained
recommender, plus stdlib pickle persistence to ml/.model_state/.

Each model consumes a QueueSnapshot's features one observation at a time
(update) and returns a recommended parameter value (predict). The default
implementations are EMA/heuristic — interpretable and dependency-free; an
optional scikit-learn SGD subclass can drop in later behind the same API.
"""

from __future__ import annotations

import pathlib
import pickle
from abc import ABC, abstractmethod

STATE_DIR = pathlib.Path(__file__).resolve().parent.parent / ".model_state"


class OnlineModel(ABC):
    name: str  # persistence filename stem; set by each subclass

    @abstractmethod
    def update(self, snap) -> None:
        """Incorporate one new observation (a QueueSnapshot)."""

    @abstractmethod
    def predict(self, snap) -> float:
        """Return the recommended parameter value for the current state."""

    def save(self) -> None:
        STATE_DIR.mkdir(exist_ok=True)
        with open(STATE_DIR / f"{self.name}.pkl", "wb") as f:
            pickle.dump(self, f)

    @classmethod
    def load_or_new(cls, *args, **kwargs) -> "OnlineModel":
        path = STATE_DIR / f"{cls.name}.pkl"
        if path.exists():
            try:
                with open(path, "rb") as f:
                    obj = pickle.load(f)
                if isinstance(obj, cls):
                    return obj
            except Exception as e:  # noqa: BLE001 — corrupt state → start fresh
                print(f"[{cls.name}] could not load state ({e}); starting fresh")
        return cls(*args, **kwargs)
