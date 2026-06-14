import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from eval.replay import simulate  # noqa: E402


class TestReplayAblation(unittest.TestCase):
    """The ablation's headline claim, made deterministic (fixed seed)."""

    def setUp(self):
        self.r = simulate()
        self.cons = self.r["fixed-conservative (30s)"]
        self.aggr = self.r["fixed-aggressive (10s)"]
        self.ctrl = self.r["controller (EMA)"]

    def test_controller_far_below_aggressive_requeue(self):
        # The whole point: don't requeue legitimate slow tasks like a naive
        # tight timeout does.
        self.assertLess(self.ctrl["requeue_rate"], self.aggr["requeue_rate"] / 3)

    def test_controller_tightens_below_conservative(self):
        # ...while still detecting stuck tasks faster than the safe fixed
        # timeout.
        self.assertLess(self.ctrl["avg_timeout"], self.cons["avg_timeout"])

    def test_controller_requeue_stays_low(self):
        self.assertLess(self.ctrl["requeue_rate"], 0.10)


if __name__ == "__main__":
    unittest.main()
