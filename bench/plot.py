#!/usr/bin/env python3
"""Render the README benchmark figures as hand-built SVG — standard library
only, no matplotlib/numpy. Vector output renders crisply on GitHub.

Two modes:
    python3 bench/plot.py --parse docs/img/bench_raw.txt   # raw → bench_data.json
    python3 bench/plot.py                                  # bench_data.json → *.svg

`bench/run.sh` produces the raw file; this script never needs Go or the
network to re-render once bench_data.json exists.
"""

from __future__ import annotations

import argparse
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
IMG = os.path.join(os.path.dirname(HERE), "docs", "img")
DATA = os.path.join(IMG, "bench_data.json")

# --- palette ---
INK = "#1f2933"
MUTED = "#7b8794"
GRID = "#e4e7eb"
BLUE = "#2563eb"      # ext4 / primary
CYAN = "#06b6d4"      # tmpfs / secondary
GREEN = "#16a34a"     # good / EMA
AMBER = "#d97706"     # caution
RED = "#dc2626"       # bad / aggressive
SLATE = "#64748b"     # SGD / neutral
FONT = ('font-family="-apple-system,BlinkMacSystemFont,Segoe UI,Helvetica,'
        'Arial,sans-serif"')


# ====================================================================
# raw → structured data
# ====================================================================

def _section(raw, name):
    m = re.search(rf"=== SECTION {name} ===\n(.*?)(?=\n=== SECTION |\Z)", raw, re.S)
    return m.group(1) if m else ""


def _bench_rates(block):
    """name → tasks/sec from `go test -bench` ReportMetric output."""
    out = {}
    for m in re.finditer(r"^(Benchmark\w+).*?([\d.]+)\s+tasks/sec", block, re.M):
        out[m.group(1)] = float(m.group(2))
    return out


def _latency(block):
    m = re.search(r"P50=(\S+)\s+P95=(\S+)\s+P99=(\S+)", block)
    if not m:
        return None
    return {"p50": _dur_us(m.group(1)), "p95": _dur_us(m.group(2)), "p99": _dur_us(m.group(3))}


def _dur_us(s):
    """Go duration string → microseconds."""
    if s.endswith("ms"):
        return float(s[:-2]) * 1000
    if s.endswith("µs") or s.endswith("us"):
        return float(s[:-2])
    if s.endswith("ns"):
        return float(s[:-2]) / 1000
    if s.endswith("s"):
        return float(s[:-1]) * 1_000_000
    return float(s)


def _ablation(block):
    rows = {}
    for m in re.finditer(r"^(fixed-\S+ \(\d+s\)|controller \(\w+\))\s+([\d.]+)%\s+([\d.]+)s",
                         block, re.M):
        rows[m.group(1)] = {"requeue": float(m.group(2)), "timeout": float(m.group(3))}
    return rows


def parse(raw_path):
    with open(raw_path) as f:
        raw = f.read()

    data = {}
    ext4 = _bench_rates(_section(raw, "ext4_bench"))
    tmpfs = _bench_rates(_section(raw, "tmpfs_bench"))
    data["throughput"] = {"ext4": ext4, "tmpfs": tmpfs}

    scaling = [(int(m.group(1)), float(m.group(2)))
               for m in re.finditer(r"scaling producers=(\d+) tasks_per_sec=([\d.]+)",
                                    _section(raw, "scaling"))]
    data["scaling"] = sorted(scaling)

    data["latency"] = {
        "ext4": _latency(_section(raw, "latency_ext4")),
        "tmpfs": _latency(_section(raw, "latency_tmpfs")),
    }

    abl = _ablation(_section(raw, "ablation"))
    abl.update(_ablation(_section(raw, "ablation_sgd")))  # adds SGD row if present
    data["ablation"] = abl

    cl = _section(raw, "cluster")
    cluster = {}
    for key, label in (("seq", "seq_tasks_per_sec"), ("conc", "conc_tasks_per_sec")):
        m = re.search(rf"cluster {label}=([\d.]+)", cl)
        if m:
            cluster[key] = float(m.group(1))
    data["cluster"] = cluster

    os.makedirs(IMG, exist_ok=True)
    with open(DATA, "w") as f:
        json.dump(data, f, indent=2)
    print(f"[plot] parsed → {DATA}")
    return data


# ====================================================================
# minimal SVG toolkit
# ====================================================================

def esc(s):
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class SVG:
    def __init__(self, w, h):
        self.w, self.h = w, h
        self.parts = []

    def rect(self, x, y, w, h, fill, rx=0, opacity=1.0):
        self.parts.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" '
            f'rx="{rx}" fill="{fill}" opacity="{opacity}"/>')

    def line(self, x1, y1, x2, y2, stroke=GRID, width=1, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ""
        self.parts.append(
            f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
            f'stroke="{stroke}" stroke-width="{width}"{d}/>')

    def polyline(self, pts, stroke, width=2.5, fill="none"):
        p = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        self.parts.append(
            f'<polyline points="{p}" fill="{fill}" stroke="{stroke}" '
            f'stroke-width="{width}" stroke-linejoin="round"/>')

    def circle(self, x, y, r, fill):
        self.parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r}" fill="{fill}"/>')

    def text(self, x, y, s, size=13, fill=INK, anchor="start", weight="normal", rotate=None):
        tr = f' transform="rotate({rotate} {x:.1f} {y:.1f})"' if rotate is not None else ""
        self.parts.append(
            f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" fill="{fill}" '
            f'text-anchor="{anchor}" font-weight="{weight}"{tr} {FONT}>{esc(s)}</text>')

    def polygon(self, pts, fill, opacity=1.0):
        p = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        self.parts.append(f'<polygon points="{p}" fill="{fill}" opacity="{opacity}"/>')

    def save(self, name):
        body = "\n".join(self.parts)
        doc = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" '
               f'height="{self.h}" viewBox="0 0 {self.w} {self.h}" {FONT}>\n'
               f'<rect width="{self.w}" height="{self.h}" fill="white"/>\n'
               f'{body}\n</svg>\n')
        path = os.path.join(IMG, name)
        with open(path, "w") as f:
            f.write(doc)
        print(f"[plot] wrote {path}")


def _fmt_rate(v):
    if v >= 10000:
        return f"{v / 1000:.0f}k"
    if v >= 1000:
        return f"{v / 1000:.1f}k"
    return f"{v:.0f}"


# ====================================================================
# figures
# ====================================================================

W, H = 760, 380
PAD_L, PAD_R, PAD_T, PAD_B = 70, 24, 60, 64


def fig_throughput(data):
    ext4 = data["throughput"]["ext4"]
    tmpfs = data["throughput"]["tmpfs"]
    order = [("BenchmarkEnqueue", "enqueue\n1 producer"),
             ("BenchmarkEnqueueParallel", "enqueue\nconcurrent"),
             ("BenchmarkDequeue", "dequeue"),
             ("BenchmarkEndToEnd", "end-to-end\nround trip")]
    s = SVG(W, H)
    s.text(PAD_L, 30, "Single-node throughput", size=18, weight="700")
    s.text(PAD_L, 48, "tasks/sec — log scale; ext4 (real fsync) vs tmpfs (CPU ceiling)",
           size=12, fill=MUTED)

    import math
    vals = [v for d in (ext4, tmpfs) for v in d.values() if v > 0]
    vmax = max(vals)
    lo = 100  # log floor below the smallest bar (ext4 end-to-end ~856/s)
    plot_h = H - PAD_T - PAD_B
    base_y = H - PAD_B

    def y_of(v):
        v = max(v, lo)
        return base_y - plot_h * (math.log10(v) - math.log10(lo)) / (math.log10(vmax * 1.3) - math.log10(lo))

    for p in range(2, 7):  # 100,1k,10k,100k,1M gridlines
        gv = 10 ** p
        if gv > vmax * 1.3:
            break
        y = y_of(gv)
        s.line(PAD_L, y, W - PAD_R, y, GRID, 1)
        s.text(PAD_L - 8, y + 4, _fmt_rate(gv), size=11, fill=MUTED, anchor="end")

    n = len(order)
    slot = (W - PAD_L - PAD_R) / n
    bw = slot * 0.3
    for i, (key, label) in enumerate(order):
        cx = PAD_L + slot * (i + 0.5)
        for j, (d, color, name) in enumerate(((ext4, BLUE, "ext4"), (tmpfs, CYAN, "tmpfs"))):
            v = d.get(key, 0)
            if v <= 0:
                continue
            x = cx + (j - 0.5) * (bw + 4) - bw / 2
            y = y_of(v)
            s.rect(x, y, bw, base_y - y, color, rx=2)
            s.text(x + bw / 2, y - 6, _fmt_rate(v), size=10.5, fill=INK, anchor="middle", weight="600")
        for li, ln in enumerate(label.split("\n")):
            s.text(cx, base_y + 18 + li * 13, ln, size=11, fill=INK, anchor="middle")
    # legend
    lx = W - PAD_R - 150
    for j, (color, name) in enumerate(((BLUE, "ext4 (NVMe)"), (CYAN, "tmpfs"))):
        s.rect(lx + j * 80, 36, 11, 11, color, rx=2)
        s.text(lx + j * 80 + 16, 46, name, size=11, fill=INK)
    s.save("throughput.svg")


def fig_group_commit(data):
    pts = data["scaling"]
    s = SVG(W, H)
    s.text(PAD_L, 30, "Group commit: throughput scales with concurrency", size=18, weight="700")
    s.text(PAD_L, 48, "ext4 — concurrent producers share one fsync, so N writers cost ~one sync",
           size=12, fill=MUTED)

    xs = [p for p, _ in pts]
    ys = [r for _, r in pts]
    import math
    base_y = H - PAD_B
    plot_h = H - PAD_T - PAD_B
    plot_w = W - PAD_L - PAD_R
    xmin, xmax = math.log2(min(xs)), math.log2(max(xs))
    ymax = max(ys) * 1.12

    def X(p):
        return PAD_L + plot_w * (math.log2(p) - xmin) / (xmax - xmin)

    def Y(r):
        return base_y - plot_h * r / ymax

    for frac in range(0, 6):
        gv = ymax * frac / 5
        y = base_y - plot_h * frac / 5
        s.line(PAD_L, y, W - PAD_R, y, GRID, 1)
        s.text(PAD_L - 8, y + 4, _fmt_rate(gv), size=11, fill=MUTED, anchor="end")
    for p in xs:
        s.text(X(p), base_y + 18, str(p), size=11, fill=INK, anchor="middle")
    s.text((PAD_L + W - PAD_R) / 2, H - 12, "producers (log scale)", size=12, fill=MUTED, anchor="middle")

    coords = [(X(p), Y(r)) for p, r in pts]
    # area under curve
    area = coords + [(coords[-1][0], base_y), (coords[0][0], base_y)]
    s.parts.append('<polygon points="' + " ".join(f"{x:.1f},{y:.1f}" for x, y in area)
                   + f'" fill="{BLUE}" opacity="0.10"/>')
    s.polyline(coords, BLUE, 3)
    for (x, y), (p, r) in zip(coords, pts):
        s.circle(x, y, 4, BLUE)
    # annotate endpoints
    s.text(coords[0][0] + 6, coords[0][1] - 8, f"{_fmt_rate(ys[0])}/s", size=11, fill=INK, weight="600")
    s.text(coords[-1][0] - 4, coords[-1][1] - 12, f"{_fmt_rate(ys[-1])}/s", size=12,
           fill=BLUE, anchor="end", weight="700")
    s.save("group_commit.svg")


def fig_latency(data):
    ext4 = data["latency"]["ext4"]
    tmpfs = data["latency"]["tmpfs"]
    s = SVG(W, H)
    s.text(PAD_L, 30, "Round-trip latency (enqueue + dequeue + ack)", size=18, weight="700")
    s.text(PAD_L, 48, "log scale — ext4 pays ~2 fsyncs per cycle; tmpfs is the CPU floor",
           size=12, fill=MUTED)

    pcts = [("p50", "P50"), ("p95", "P95"), ("p99", "P99")]
    import math
    base_y = H - PAD_B
    plot_h = H - PAD_T - PAD_B
    # Log scale: ext4 is ~1ms, tmpfs ~5–18µs (100× apart) — linear would
    # erase the tmpfs bars.
    lo = 1.0  # 1µs floor
    vmax = max(ext4["p99"], tmpfs["p99"])

    def y_of(v):
        v = max(v, lo)
        return base_y - plot_h * (math.log10(v) - math.log10(lo)) / (math.log10(vmax * 1.6) - math.log10(lo))

    for p in range(0, 4):  # 1µs,10µs,100µs,1ms gridlines
        gv = 10 ** p
        if gv > vmax * 1.6:
            break
        y = y_of(gv)
        s.line(PAD_L, y, W - PAD_R, y, GRID, 1)
        lbl = f"{gv / 1000:.0f}ms" if gv >= 1000 else f"{gv:.0f}µs"
        s.text(PAD_L - 8, y + 4, lbl, size=11, fill=MUTED, anchor="end")

    n = len(pcts)
    slot = (W - PAD_L - PAD_R) / n
    bw = slot * 0.28
    for i, (key, label) in enumerate(pcts):
        cx = PAD_L + slot * (i + 0.5)
        for j, (d, color) in enumerate(((ext4, BLUE), (tmpfs, CYAN))):
            v = d[key]
            x = cx + (j - 0.5) * (bw + 4) - bw / 2
            y = y_of(v)
            s.rect(x, y, bw, base_y - y, color, rx=2)
            lbl = f"{v / 1000:.2f}ms" if v >= 1000 else f"{v:.1f}µs"
            s.text(x + bw / 2, y - 6, lbl, size=10, fill=INK, anchor="middle", weight="600")
        s.text(cx, base_y + 18, label, size=12, fill=INK, anchor="middle")
    lx = W - PAD_R - 150
    for j, (color, name) in enumerate(((BLUE, "ext4 (NVMe)"), (CYAN, "tmpfs"))):
        s.rect(lx + j * 80, 36, 11, 11, color, rx=2)
        s.text(lx + j * 80 + 16, 46, name, size=11, fill=INK)
    s.save("latency.svg")


def fig_ablation(data):
    abl = data["ablation"]
    # canonical order + colors
    spec = [("fixed-conservative (30s)", "fixed-conservative", AMBER),
            ("fixed-aggressive (10s)", "fixed-aggressive", RED),
            ("controller (EMA)", "controller (EMA)", GREEN),
            ("controller (SGD)", "controller (SGD)", SLATE)]
    rows = [(lbl, abl[k], c) for k, lbl, c in spec if k in abl]

    s = SVG(W, H)
    s.text(PAD_L, 30, "Control plane: requeue rate vs average timeout", size=18, weight="700")
    s.text(PAD_L, 48, "lower-left is better — cut timeout latency without requeuing legitimately-slow tasks",
           size=12, fill=MUTED)

    base_y = H - PAD_B
    plot_h = H - PAD_T - PAD_B
    plot_w = W - PAD_L - PAD_R
    xmax = 35  # avg timeout seconds
    ymax = 20  # requeue %

    def X(t):
        return PAD_L + plot_w * t / xmax

    def Y(r):
        return base_y - plot_h * r / ymax

    for frac in range(0, 6):
        ry = ymax * frac / 5
        y = base_y - plot_h * frac / 5
        s.line(PAD_L, y, W - PAD_R, y, GRID, 1)
        s.text(PAD_L - 8, y + 4, f"{ry:.0f}%", size=11, fill=MUTED, anchor="end")
    for tx in range(0, xmax + 1, 5):
        x = X(tx)
        s.line(x, PAD_T, x, base_y, GRID, 1)
        s.text(x, base_y + 18, f"{tx}s", size=11, fill=MUTED, anchor="middle")
    s.text((PAD_L + W - PAD_R) / 2, H - 12, "average timeout (s) — lower = faster stuck-task detection",
           size=12, fill=MUTED, anchor="middle")
    s.text(22, (PAD_T + base_y) / 2, "requeue rate", size=12, fill=MUTED, anchor="middle", rotate=-90)

    # "better" arrow toward the lower-left corner (drawn, no glyph).
    ax, ay = X(4.5), Y(2.2)
    s.line(ax + 26, ay - 22, ax, ay, GREEN, 2)
    s.polygon([(ax, ay), (ax + 11, ay - 3), (ax + 4, ay - 11)], GREEN)
    s.text(ax + 30, ay - 24, "better", size=12, fill=GREEN, weight="700")

    # Points (no inline labels — named in the legend to avoid collisions).
    for lbl, v, c in rows:
        s.circle(X(v["timeout"]), Y(v["requeue"]), 8, c)

    # Legend box in the empty upper area.
    lx, ly = X(13), Y(19)
    s.text(lx, ly - 8, "policy", size=11, fill=MUTED, weight="700")
    s.text(lx + 168, ly - 8, "requeue", size=11, fill=MUTED, weight="700", anchor="end")
    s.text(lx + 230, ly - 8, "timeout", size=11, fill=MUTED, weight="700", anchor="end")
    for i, (lbl, v, c) in enumerate(rows):
        ry = ly + i * 20
        s.circle(lx + 6, ry - 4, 6, c)
        s.text(lx + 18, ry, lbl, size=12, fill=INK, weight="600")
        s.text(lx + 168, ry, f"{v['requeue']:.1f}%", size=12, fill=INK, anchor="end")
        s.text(lx + 230, ry, f"{v['timeout']:.0f}s", size=12, fill=INK, anchor="end")
    s.save("ablation.svg")


def render():
    with open(DATA) as f:
        data = json.load(f)
    fig_throughput(data)
    fig_group_commit(data)
    fig_latency(data)
    fig_ablation(data)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parse", metavar="RAW", help="parse raw bench output → bench_data.json")
    args = ap.parse_args()
    if args.parse:
        parse(args.parse)
    render()


if __name__ == "__main__":
    main()
