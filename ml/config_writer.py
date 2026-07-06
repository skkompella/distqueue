"""Write the broker's tuning file atomically and signal a hot-reload.

The broker reads `broker.conf` on SIGHUP, so two things matter:
  1. The write must be atomic (temp file + rename) — a half-written file
     would feed the broker malformed TOML.
  2. Only push (and signal) when the config actually changed, to avoid
     pointless SIGHUP churn.

Stdlib only: a tiny hand-rolled TOML writer covers the one `[tuning]`
table, so there's no dependency on tomli-w.
"""

from __future__ import annotations

import os
import pathlib
import signal
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class BrokerConfig:
    task_timeout_seconds: float = 30.0
    worker_count: int = 4  # relayed by the broker via Stats; workers auto-resize
    max_retries: int = 3
    timeouts: Dict[str, float] = field(default_factory=dict)  # per-type (future)

    def clamp(
        self,
        timeout_min: float,
        timeout_max: float,
        workers_min: int,
        workers_max: int,
        retries_min: int = 1,
        retries_max: int = 10,
    ) -> "BrokerConfig":
        self.task_timeout_seconds = _clamp(self.task_timeout_seconds, timeout_min, timeout_max)
        self.worker_count = int(_clamp(self.worker_count, workers_min, workers_max))
        self.max_retries = int(_clamp(self.max_retries, retries_min, retries_max))
        return self

    def rounded(self) -> "BrokerConfig":
        """A comparison-friendly copy: timeout rounded to 0.1s so sub-decisecond
        jitter doesn't trigger a rewrite every tick."""
        return BrokerConfig(
            task_timeout_seconds=round(self.task_timeout_seconds, 1),
            worker_count=self.worker_count,
            max_retries=self.max_retries,
            timeouts={k: round(v, 1) for k, v in self.timeouts.items()},
        )


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _dump_toml(cfg: BrokerConfig) -> str:
    lines = [
        "# Written by the distqueue adaptive control plane. Hot-reloaded by",
        "# the broker on SIGHUP. Do not edit by hand while the controller runs.",
        "",
        "[tuning]",
        f"task_timeout_seconds = {cfg.task_timeout_seconds:.1f}",
        f"worker_count = {cfg.worker_count}",
        f"max_retries = {cfg.max_retries}",
    ]
    if cfg.timeouts:
        lines.append("")
        lines.append("[timeouts]")
        for task_type, secs in sorted(cfg.timeouts.items()):
            lines.append(f"{task_type} = {secs:.1f}")
    return "\n".join(lines) + "\n"


class ConfigWriter:
    def __init__(self, config_path: str, broker_pid_file: Optional[str] = None):
        self.path = pathlib.Path(config_path)
        self.pid_file = broker_pid_file
        self._last_written: Optional[BrokerConfig] = None

    def write_if_changed(self, cfg: BrokerConfig) -> bool:
        """Write + signal only if the (rounded) config differs from the last
        one written. Returns True iff a write happened."""
        rounded = cfg.rounded()
        if self._last_written == rounded:
            return False
        self._atomic_write(_dump_toml(rounded))
        self._last_written = rounded
        self._send_sighup()
        return True

    def _atomic_write(self, content: str) -> None:
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(content)
        os.replace(tmp, self.path)  # atomic on POSIX

    def _send_sighup(self) -> None:
        if not self.pid_file:
            return
        try:
            pid = int(pathlib.Path(self.pid_file).read_text().strip())
            os.kill(pid, signal.SIGHUP)
        except (OSError, ValueError) as e:
            print(f"[config_writer] SIGHUP failed: {e}")
