#!/usr/bin/env python3
"""Plots throughput and p99 latency vs clients from bench/results/*.json.

    python3 scripts/plot_bench.py            # needs: pip install matplotlib

Writes bench/plots/throughput_vs_clients.png and bench/plots/p99_vs_clients.png,
one line per fsync mode. If a run was repeated, the newest file wins.
"""
import glob
import json
import os
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

runs = defaultdict(dict)  # fsync_mode -> {clients: result}
for path in sorted(glob.glob("bench/results/*.json")):
    with open(path) as f:
        r = json.load(f)
    runs[r["fsync_mode"]][r["workload"]["clients"]] = r

os.makedirs("bench/plots", exist_ok=True)
for metric, label, fname in [
    (lambda r: r["throughput_ops_s"], "throughput (ops/s)", "throughput_vs_clients.png"),
    (lambda r: r["latency_ms"]["p99"], "p99 latency (ms)", "p99_vs_clients.png"),
]:
    plt.figure(figsize=(6, 4))
    for mode, by_clients in sorted(runs.items()):
        xs = sorted(by_clients)
        plt.plot(xs, [metric(by_clients[x]) for x in xs], marker="o", label=f"fsync={mode}")
    plt.xlabel("concurrent clients")
    plt.ylabel(label)
    plt.xscale("log", base=2)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.title(f"raft-kv, 3 nodes: {label} vs clients")
    plt.tight_layout()
    plt.savefig(os.path.join("bench/plots", fname), dpi=120)
    print("wrote bench/plots/" + fname)
