import os
import sys
import tomllib
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_writer import BrokerConfig, ConfigWriter  # noqa: E402


class TestConfigWriter(unittest.TestCase):
    def setUp(self):
        import tempfile

        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "broker.conf")
        self.addCleanup(self.tmp.cleanup)

    def test_write_produces_valid_toml(self):
        w = ConfigWriter(self.path)
        w.write_if_changed(BrokerConfig(task_timeout_seconds=15.0, worker_count=2, max_retries=2))
        with open(self.path, "rb") as f:
            result = tomllib.load(f)
        self.assertEqual(result["tuning"]["task_timeout_seconds"], 15.0)
        self.assertEqual(result["tuning"]["worker_count"], 2)
        self.assertEqual(result["tuning"]["max_retries"], 2)

    def test_no_write_if_unchanged(self):
        w = ConfigWriter(self.path)
        cfg = BrokerConfig()
        self.assertTrue(w.write_if_changed(cfg))  # first write
        self.assertFalse(w.write_if_changed(BrokerConfig()))  # identical → skip

    def test_sub_decisecond_jitter_does_not_rewrite(self):
        w = ConfigWriter(self.path)
        self.assertTrue(w.write_if_changed(BrokerConfig(task_timeout_seconds=12.00)))
        # 12.03 rounds to 12.0 → no rewrite.
        self.assertFalse(w.write_if_changed(BrokerConfig(task_timeout_seconds=12.03)))
        # 12.2 is a real change.
        self.assertTrue(w.write_if_changed(BrokerConfig(task_timeout_seconds=12.2)))

    def test_clamp(self):
        cfg = BrokerConfig(task_timeout_seconds=999, worker_count=100, max_retries=99)
        cfg.clamp(5.0, 300.0, 1, 32)
        self.assertEqual(cfg.task_timeout_seconds, 300.0)
        self.assertEqual(cfg.worker_count, 32)
        self.assertEqual(cfg.max_retries, 10)

    def test_atomic_write_leaves_no_tmp(self):
        w = ConfigWriter(self.path)
        w.write_if_changed(BrokerConfig(task_timeout_seconds=20.0))
        leftovers = [f for f in os.listdir(self.tmp.name) if f.endswith(".tmp")]
        self.assertEqual(leftovers, [])

    def test_per_type_timeouts_emitted(self):
        w = ConfigWriter(self.path)
        w.write_if_changed(BrokerConfig(timeouts={"email": 8.0, "video": 120.0}))
        with open(self.path, "rb") as f:
            result = tomllib.load(f)
        self.assertEqual(result["timeouts"]["email"], 8.0)
        self.assertEqual(result["timeouts"]["video"], 120.0)


if __name__ == "__main__":
    unittest.main()
