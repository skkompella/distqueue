"""Scrape the broker's Prometheus endpoint and turn it into a typed feature
vector. No ML logic lives here — just I/O and parsing.

Stdlib only (urllib): the control plane must run on a bare Python with no
pip installs, possibly offline.
"""

from __future__ import annotations

import time
import urllib.request
from dataclasses import dataclass
from typing import Optional


@dataclass
class QueueSnapshot:
    ts: float  # unix time of this scrape

    pending: float  # queue_pending
    in_flight: float  # queue_in_flight
    dlq_depth: float  # queue_dlq_depth
    acked_total: float  # queue_acked_total (raw counter)
    nacked_total: float  # queue_nacked_total (raw counter)

    # Derived rates (per second), computed across the gap to the previous
    # scrape. Zero on the first scrape.
    ack_rate: float = 0.0
    nack_rate: float = 0.0

    # Raft fields — None in single-node mode (metrics absent).
    is_leader: Optional[float] = None
    term: Optional[float] = None
    elections_total: Optional[float] = None

    @property
    def nack_pressure(self) -> float:
        """nack_rate / ack_rate — the central 'timeout too tight' signal.
        Guarded against divide-by-zero."""
        return self.nack_rate / (self.ack_rate + 1e-6)


# Metric name → QueueSnapshot field. Counters and gauges are read the same
# way (last sample wins); rates are derived afterward.
_GAUGE_FIELDS = {
    "queue_pending": "pending",
    "queue_in_flight": "in_flight",
    "queue_dlq_depth": "dlq_depth",
    "queue_acked_total": "acked_total",
    "queue_nacked_total": "nacked_total",
    "raft_is_leader": "is_leader",
    "raft_term": "term",
    "raft_elections_started_total": "elections_total",
}


class Collector:
    def __init__(self, url: str, timeout: float = 5.0):
        self.url = url
        self.timeout = timeout
        self._prev: Optional[QueueSnapshot] = None

    def scrape(self) -> Optional[QueueSnapshot]:
        """Fetch and parse once. Returns None on any network/HTTP error so
        the controller can simply skip the tick and retry."""
        try:
            with urllib.request.urlopen(self.url, timeout=self.timeout) as resp:
                text = resp.read().decode("utf-8")
        except Exception as e:  # noqa: BLE001 — any failure → skip this tick
            print(f"[collector] scrape failed: {e}")
            return None
        return self._parse(text, now=time.time())

    def _parse(self, text: str, now: float) -> QueueSnapshot:
        values: dict[str, float] = {}
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            # Prometheus text: `name{labels} value [timestamp]`. We only
            # need the metric base name and the value.
            metric, _, rest = line.partition(" ")
            if not rest:
                continue
            base = metric.split("{", 1)[0]
            field = _GAUGE_FIELDS.get(base)
            if field is None:
                continue
            try:
                values[field] = float(rest.split()[0])
            except ValueError:
                continue

        snap = QueueSnapshot(
            ts=now,
            pending=values.get("pending", 0.0),
            in_flight=values.get("in_flight", 0.0),
            dlq_depth=values.get("dlq_depth", 0.0),
            acked_total=values.get("acked_total", 0.0),
            nacked_total=values.get("nacked_total", 0.0),
            is_leader=values.get("is_leader"),
            term=values.get("term"),
            elections_total=values.get("elections_total"),
        )
        self._fill_rates(snap)
        self._prev = snap
        return snap

    def _fill_rates(self, snap: QueueSnapshot) -> None:
        prev = self._prev
        if prev is None:
            return
        dt = snap.ts - prev.ts
        if dt <= 0:
            return
        # Counters only grow; a decrease means the broker restarted, so we
        # treat the rate as 0 for that window rather than reporting a
        # negative spike.
        snap.ack_rate = max(0.0, (snap.acked_total - prev.acked_total) / dt)
        snap.nack_rate = max(0.0, (snap.nacked_total - prev.nacked_total) / dt)
