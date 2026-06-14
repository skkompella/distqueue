#!/usr/bin/env python3
"""Record live broker snapshots to JSONL for offline replay.

Run alongside a broker under load:

    python3 eval/record.py --metrics http://localhost:7000/metrics \
            --out eval/data/run_001.jsonl --duration 300 --interval 2

Each line is one QueueSnapshot (including derived ack/nack rates).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collector import Collector  # noqa: E402


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", default="http://localhost:7000/metrics")
    ap.add_argument("--out", required=True)
    ap.add_argument("--duration", type=float, default=300.0)
    ap.add_argument("--interval", type=float, default=2.0)
    args = ap.parse_args(argv)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    collector = Collector(args.metrics)

    n = 0
    deadline = time.time() + args.duration
    with open(args.out, "w") as f:
        while time.time() < deadline:
            snap = collector.scrape()
            if snap is not None:
                f.write(json.dumps(dataclasses.asdict(snap)) + "\n")
                f.flush()
                n += 1
            time.sleep(args.interval)
    print(f"[record] wrote {n} snapshots to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
