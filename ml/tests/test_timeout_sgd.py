import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import QueueSnapshot  # noqa: E402
from models.timeout_factory import make_timeout_model  # noqa: E402

try:
    import sklearn  # noqa: F401
    import numpy  # noqa: F401
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False


def snap(ack_rate=0.0, nack_rate=0.0, pending=0.0, in_flight=0.0):
    return QueueSnapshot(ts=0, pending=pending, in_flight=in_flight, dlq_depth=0,
                         acked_total=0, nacked_total=0,
                         ack_rate=ack_rate, nack_rate=nack_rate)


class TestFactoryFallback(unittest.TestCase):
    """Runs in ANY environment — the whole point is graceful degradation."""

    def test_ema_is_default(self):
        self.assertEqual(type(make_timeout_model("ema")).__name__, "TimeoutModel")

    def test_sgd_falls_back_to_ema_when_unavailable(self):
        # Force the lazy import to fail, regardless of whether sklearn is
        # actually installed.
        saved = sys.modules.get("models.timeout_sgd")
        sys.modules["models.timeout_sgd"] = None
        try:
            m = make_timeout_model("sgd")
            self.assertEqual(type(m).__name__, "TimeoutModel")
        finally:
            if saved is not None:
                sys.modules["models.timeout_sgd"] = saved
            else:
                sys.modules.pop("models.timeout_sgd", None)

    def test_unknown_kind_raises(self):
        with self.assertRaises(ValueError):
            make_timeout_model("nope")


@unittest.skipUnless(_HAS_SKLEARN, "scikit-learn not installed")
class TestSGDTimeoutModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from models.timeout_sgd import SGDTimeoutModel

        cls.SGDTimeoutModel = SGDTimeoutModel
        # Train the real model (same routine the system ships), so the test
        # validates the actual artifact rather than a toy. Fast (~1k samples).
        from eval.train_sgd import TRAIN_P90S, TRAIN_SEEDS, _sample

        X, y = _sample(TRAIN_P90S, TRAIN_SEEDS)
        cls.model = SGDTimeoutModel().fit_offline(X, y)

    def test_predict_within_bounds(self):
        from models.timeout_model import MAX_TIMEOUT, MIN_TIMEOUT

        for s in (snap(), snap(ack_rate=100, nack_rate=0), snap(ack_rate=1, nack_rate=100)):
            p = self.model.predict(s)
            self.assertGreaterEqual(p, MIN_TIMEOUT)
            self.assertLessEqual(p, MAX_TIMEOUT)

    def test_generalizes_to_held_out_load(self):
        # It learned a real mapping, not noise: held-out MAE (unseen p90 AND
        # unseen seed) is well under the target range's width.
        from eval.train_sgd import HOLDOUT_P90S, _mae, _sample

        Xho, yho = _sample(HOLDOUT_P90S, [9999])
        self.assertLess(_mae(self.model, Xho, yho), 10.0)

    def test_online_update_keeps_bounds(self):
        from models.timeout_model import MAX_TIMEOUT, MIN_TIMEOUT

        m = self.SGDTimeoutModel(default_seconds=30.0)
        s = snap(ack_rate=50, nack_rate=30)
        for _ in range(50):
            m.update(s)
            p = m.predict(s)
            self.assertGreaterEqual(p, MIN_TIMEOUT)
            self.assertLessEqual(p, MAX_TIMEOUT)

    def test_held_out_requeue_is_sane(self):
        # The SGD controller on held-out load must at least be far safer
        # than the naive-aggressive policy (the bar EMA also clears).
        from eval.replay import simulate

        r = simulate(extra_adaptive=("controller (SGD)", self.model))
        self.assertLess(r["controller (SGD)"]["requeue_rate"],
                        r["fixed-aggressive (10s)"]["requeue_rate"] / 3)


if __name__ == "__main__":
    unittest.main()
