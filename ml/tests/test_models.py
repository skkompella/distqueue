import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import QueueSnapshot  # noqa: E402
from models.retry_model import RetryModel  # noqa: E402
from models.timeout_model import MAX_TIMEOUT, MIN_TIMEOUT, TimeoutModel  # noqa: E402
from models.worker_model import WorkerModel  # noqa: E402


def snap(pending=0.0, ack_rate=0.0, nack_rate=0.0, in_flight=0.0):
    return QueueSnapshot(
        ts=0.0,
        pending=pending,
        in_flight=in_flight,
        dlq_depth=0.0,
        acked_total=0.0,
        nacked_total=0.0,
        ack_rate=ack_rate,
        nack_rate=nack_rate,
    )


class TestTimeoutModel(unittest.TestCase):
    def test_increases_under_nack_pressure(self):
        m = TimeoutModel(default_seconds=30.0)
        high = snap(pending=100, ack_rate=10.0, nack_rate=5.0)  # 50% pressure
        for _ in range(20):
            m.update(high)
        self.assertGreater(m.predict(high), 30.0)

    def test_tightens_when_healthy(self):
        m = TimeoutModel(default_seconds=30.0)
        healthy = snap(pending=5, ack_rate=100.0, nack_rate=0.0)  # ~0 pressure
        for _ in range(20):
            m.update(healthy)
        self.assertLess(m.predict(healthy), 30.0)

    def test_clamped_to_bounds(self):
        m = TimeoutModel(default_seconds=30.0)
        high = snap(pending=100, ack_rate=1.0, nack_rate=100.0)
        for _ in range(500):
            m.update(high)
        self.assertLessEqual(m.predict(high), MAX_TIMEOUT)
        self.assertGreaterEqual(m.predict(high), MIN_TIMEOUT)

    def test_stable_in_steady_state(self):
        m = TimeoutModel(default_seconds=30.0)
        neutral = snap(pending=10, ack_rate=100.0, nack_rate=5.0)  # 5% — between bands
        for _ in range(50):
            m.update(neutral)
        self.assertAlmostEqual(m.predict(neutral), 30.0, delta=0.5)


class TestWorkerModel(unittest.TestCase):
    def test_scales_with_backlog_and_clamps(self):
        m = WorkerModel(target_drain_seconds=10.0, assumed_workers=4)
        # 4 workers achieving 40 acks/s ⇒ 10/worker/s ⇒ 100/worker over 10s.
        warm = snap(pending=0, ack_rate=40.0)
        for _ in range(10):
            m.update(warm)
        big = snap(pending=5000, ack_rate=40.0)
        rec = m.predict(big)
        self.assertGreaterEqual(rec, 1)
        self.assertLessEqual(rec, 64)  # MAX_WORKERS
        # 5000 / (10/s * 10s) = 50 workers.
        self.assertAlmostEqual(rec, 50.0, delta=2.0)

    def test_no_throughput_signal_keeps_assumption(self):
        m = WorkerModel(assumed_workers=4)
        self.assertEqual(m.predict(snap(pending=100, ack_rate=0.0)), 4.0)


class TestRetryModel(unittest.TestCase):
    def test_cuts_retries_when_mostly_failing(self):
        m = RetryModel(default=3)
        failing = snap(ack_rate=1.0, nack_rate=10.0)  # pressure ~10
        for _ in range(20):
            m.update(failing)
        self.assertEqual(m.predict(failing), 1)

    def test_keeps_default_when_healthy(self):
        m = RetryModel(default=3)
        healthy = snap(ack_rate=100.0, nack_rate=0.0)
        for _ in range(20):
            m.update(healthy)
        self.assertEqual(m.predict(healthy), 3)

    def test_output_is_clamped_integer(self):
        m = RetryModel(default=3)
        r = m.predict(snap(ack_rate=100.0, nack_rate=1.0))
        self.assertEqual(r, float(int(r)))
        self.assertGreaterEqual(r, 1)
        self.assertLessEqual(r, 5)


if __name__ == "__main__":
    unittest.main()
