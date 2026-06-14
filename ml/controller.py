#!/usr/bin/env python3
"""The adaptive control plane's main loop.

Every `--interval` seconds: scrape the broker's metrics, update the three
models with the new observation, and — once warmed up — write the
recommended tuning to broker.conf (which the broker hot-reloads on SIGHUP).

Single-node target by design (scrapes one /metrics, signals one broker).
Run alongside a broker started with `--config broker.conf --pid-file
broker.pid --metrics-port 7000`.
"""

from __future__ import annotations

import argparse
import signal
import sys
import time

from collector import Collector
from config_writer import BrokerConfig, ConfigWriter
from models.retry_model import RetryModel
from models.timeout_factory import make_timeout_model
from models.worker_model import WorkerModel


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="distqueue adaptive control plane")
    ap.add_argument("--metrics", default="http://localhost:7000/metrics")
    ap.add_argument("--config", default="broker.conf")
    ap.add_argument("--pid-file", default="broker.pid")
    ap.add_argument("--interval", type=float, default=10.0)
    ap.add_argument("--min-samples", type=int, default=5,
                    help="scrapes to observe before pushing any config")
    ap.add_argument("--persist-every", type=int, default=20,
                    help="save model state every N samples")
    ap.add_argument("--timeout-model", choices=("ema", "sgd"), default="ema",
                    help="timeout controller: ema (stdlib, default) or sgd "
                         "(needs scikit-learn; falls back to ema if absent)")
    ap.add_argument("--timeout-min", type=float, default=5.0)
    ap.add_argument("--timeout-max", type=float, default=300.0)
    ap.add_argument("--workers-min", type=int, default=1)
    ap.add_argument("--workers-max", type=int, default=32)
    ap.add_argument("--once", action="store_true",
                    help="run a single iteration and exit (for smoke tests)")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    collector = Collector(args.metrics)
    writer = ConfigWriter(args.config, args.pid_file)
    t_model = make_timeout_model(args.timeout_model)
    w_model = WorkerModel.load_or_new()
    r_model = RetryModel.load_or_new()
    models = (t_model, w_model, r_model)

    def persist_and_exit(*_):
        for m in models:
            m.save()
        print("[controller] models persisted; exiting")
        sys.exit(0)

    signal.signal(signal.SIGTERM, persist_and_exit)
    signal.signal(signal.SIGINT, persist_and_exit)

    print(f"[controller] start metrics={args.metrics} interval={args.interval}s "
          f"warmup={args.min_samples} timeout_model={type(t_model).__name__}")

    samples = 0
    while True:
        snap = collector.scrape()
        if snap is None:
            _sleep(args.interval, args.once)
            if args.once:
                return 1
            continue

        for m in models:
            m.update(snap)
        samples += 1

        if samples < args.min_samples:
            print(f"[controller] warming up ({samples}/{args.min_samples}) "
                  f"pending={snap.pending:.0f} ack/s={snap.ack_rate:.1f} "
                  f"nack/s={snap.nack_rate:.2f}")
        else:
            cfg = recommend(snap, t_model, w_model, r_model, args)
            if writer.write_if_changed(cfg):
                print(f"[controller] pushed: timeout={cfg.task_timeout_seconds:.1f}s "
                      f"workers={cfg.worker_count} retries={cfg.max_retries} "
                      f"(nack_pressure={snap.nack_pressure:.3f})")

        if samples % args.persist_every == 0:
            for m in models:
                m.save()

        if args.once:
            return 0
        _sleep(args.interval, args.once)


def recommend(snap, t_model, w_model, r_model, args) -> BrokerConfig:
    return BrokerConfig(
        task_timeout_seconds=t_model.predict(snap),
        worker_count=int(round(w_model.predict(snap))),
        max_retries=int(round(r_model.predict(snap))),
    ).clamp(args.timeout_min, args.timeout_max, args.workers_min, args.workers_max)


def _sleep(interval, once):
    if not once:
        time.sleep(interval)


if __name__ == "__main__":
    sys.exit(main())
