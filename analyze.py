
"""Aggregate results/*.json -> results/summary.csv, markdown table and PNG charts."""
import csv
import glob
import json
import os
import re
from collections import defaultdict

RES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
COLS = ["completed", "makespan_s", "throughput_tps", "lat_mean_s", "lat_p95_s", "lat_max_s",
        "jain_fairness", "retried_tasks"]


def main():
    groups = defaultdict(list)
    for f in sorted(glob.glob(os.path.join(RES, "*.json"))):
        m = re.match(r"(.+)_(baseline|proposed)_(\d+)\.json$", os.path.basename(f))
        if m and "attacker" not in f:
            with open(f) as fh:
                groups[(m.group(1), m.group(2))].append(json.load(fh))
    rows = []
    for (cond, mode), runs in sorted(groups.items()):
        row = {"condition": cond, "mode": mode, "runs": len(runs)}
        for c in COLS:
            row[c] = round(sum(r["metrics"][c] for r in runs) / len(runs), 3)
        st = [r["scheduler"]["stats"] or {} for r in runs]
        for k in ("redistributed", "timeouts", "worker_failures", "blocked"):
            row[k] = round(sum(s.get(k, 0) for s in st) / len(st), 2)
        rows.append(row)
    if not rows:
        print("no results found in", RES)
        return
    keys = list(rows[0].keys())
    with open(os.path.join(RES, "summary.csv"), "w", newline="") as f:
        w = csv.DictWriter(f, keys)
        w.writeheader()
        w.writerows(rows)
    print("| " + " | ".join(keys) + " |\n|" + "---|" * len(keys))
    for r in rows:
        print("| " + " | ".join(str(r[k]) for k in keys) + " |")
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib missing: charts skipped")
        return
    conds = sorted({r["condition"] for r in rows})
    for metric, title in (("lat_p95_s", "95th percentile latency (s)"),
                          ("throughput_tps", "Throughput (tasks/s)"),
                          ("makespan_s", "Makespan (s)")):
        fig, ax = plt.subplots(figsize=(8, 4))
        for i, mode in enumerate(("baseline", "proposed")):
            vals = [next((r[metric] for r in rows if r["condition"] == c and r["mode"] == mode), 0)
                    for c in conds]
            ax.bar([x + i * 0.38 for x in range(len(conds))], vals, 0.38, label=mode)
        ax.set_xticks([x + 0.19 for x in range(len(conds))])
        ax.set_xticklabels(conds, rotation=15)
        ax.set_title(title)
        ax.legend()
        fig.tight_layout()
        fig.savefig(os.path.join(RES, metric + ".png"), dpi=130)
        plt.close(fig)
    print("charts written to", RES)


if __name__ == "__main__":
    main()
