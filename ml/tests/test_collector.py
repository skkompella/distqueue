import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import Collector  # noqa: E402


class TestCollectorParse(unittest.TestCase):
    def test_parse_prometheus_text(self):
        fixture = """
# HELP queue_pending tasks awaiting delivery
# TYPE queue_pending gauge
queue_pending{node="single"} 42
queue_in_flight{node="single"} 7
queue_dlq_depth{node="single"} 2
queue_acked_total{node="single"} 1234
queue_nacked_total{node="single"} 56
"""
        c = Collector("http://unused")
        snap = c._parse(fixture, now=1000.0)
        self.assertEqual(snap.pending, 42)
        self.assertEqual(snap.in_flight, 7)
        self.assertEqual(snap.dlq_depth, 2)
        self.assertEqual(snap.acked_total, 1234)
        self.assertEqual(snap.nacked_total, 56)
        # Single-node: no raft metrics present.
        self.assertIsNone(snap.is_leader)
        # First scrape → rates are zero.
        self.assertEqual(snap.ack_rate, 0.0)
        self.assertEqual(snap.nack_rate, 0.0)

    def test_parses_raft_fields_when_present(self):
        fixture = """
queue_acked_total{node="node1"} 10
raft_is_leader{node="node1"} 1
raft_term{node="node1"} 4
raft_elections_started_total{node="node1"} 3
"""
        snap = Collector("http://unused")._parse(fixture, now=1.0)
        self.assertEqual(snap.is_leader, 1)
        self.assertEqual(snap.term, 4)
        self.assertEqual(snap.elections_total, 3)

    def test_ack_rate_computation(self):
        c = Collector("http://unused")
        # Two scrapes 10s apart, ack_total 1000 → 1500 ⇒ 50/s; nack 100→110 ⇒ 1/s.
        c._parse("queue_acked_total 1000\nqueue_nacked_total 100", now=1000.0)
        snap = c._parse("queue_acked_total 1500\nqueue_nacked_total 110", now=1010.0)
        self.assertAlmostEqual(snap.ack_rate, 50.0)
        self.assertAlmostEqual(snap.nack_rate, 1.0)
        self.assertAlmostEqual(snap.nack_pressure, 1.0 / (50.0 + 1e-6))

    def test_counter_reset_does_not_go_negative(self):
        c = Collector("http://unused")
        c._parse("queue_acked_total 5000", now=1000.0)
        # Broker restarted: counter dropped. Rate clamps to 0, not negative.
        snap = c._parse("queue_acked_total 10", now=1010.0)
        self.assertEqual(snap.ack_rate, 0.0)

    def test_ignores_unknown_metrics_and_comments(self):
        snap = Collector("http://unused")._parse(
            "# a comment\ngo_goroutines 12\nqueue_pending 3\n", now=1.0
        )
        self.assertEqual(snap.pending, 3)


if __name__ == "__main__":
    unittest.main()
