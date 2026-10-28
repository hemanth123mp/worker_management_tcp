#!/usr/bin/env python3
"""Task-submitting client / load generator. Writes a JSON report with metrics."""
import argparse
import json
import random
import socket
import threading
import time

from common import DEFAULT_PORT, Channel, jain_index, percentile


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="10.0.0.1")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--tasks", type=int, default=100)
    ap.add_argument("--rate", type=float, default=20.0, help="submissions per second")
    ap.add_argument("--mean-duration", type=float, default=0.25)
    ap.add_argument("--payload-kb", type=int, default=24)
    ap.add_argument("--deadline", type=float, default=120.0)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--label", default="run")
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    random.seed(a.seed)

    sock = socket.create_connection((a.server, a.port), timeout=10)
    sock.settimeout(None)
    ch = Channel(sock)
    ch.send({"type": "CLIENT_HELLO"})
    r = ch.recv()
    assert r and r.get("type") == "CLIENT_READY", r

    results, send_ts, stats_box = {}, {}, {}
    lock = threading.Lock()
    all_done, stats_evt = threading.Event(), threading.Event()

    def receiver():
        while True:
            try:
                m = ch.recv()
            except (OSError, ValueError):
                break
            if m is None:
                break
            t = m.get("type")
            if t in ("DONE", "ERROR"):
                with lock:
                    results[m["task_id"]] = dict(m, recv_ts=time.time())
                    if len(results) >= a.tasks:
                        all_done.set()
            elif t == "STATS_REPLY":
                stats_box.update(m)
                stats_evt.set()

    threading.Thread(target=receiver, daemon=True).start()
    data = "x" * (a.payload_kb * 1024)
    t0 = time.time()
    for i in range(a.tasks):
        delay = t0 + i / a.rate - time.time()
        if delay > 0:
            time.sleep(delay)
        send_ts[i] = time.time()
        ch.send({"type": "SUBMIT", "task_id": i, "data": data,
                 "duration": round(random.uniform(0.5, 1.5) * a.mean_duration, 4)})
    all_done.wait(max(1.0, a.deadline - (time.time() - t0)))
    t_end = time.time()
    ch.send({"type": "STATS"})
    stats_evt.wait(5)

    done = {k: v for k, v in results.items() if v["type"] == "DONE"}
    lats = [v["recv_ts"] - send_ts[k] for k, v in done.items()]
    makespan = max([v["recv_ts"] for v in results.values()] or [t_end]) - t0
    per_worker = {}
    for v in done.values():
        per_worker[v["worker"]] = per_worker.get(v["worker"], 0) + 1
    norm = [per_worker.get(w["id"], 0) / (w["capacity"] * w["speed"])
            for w in stats_box.get("workers", [])]
    report = {
        "label": a.label, "policy": stats_box.get("policy"),
        "params": vars(a),
        "metrics": {
            "submitted": a.tasks, "completed": len(done),
            "failed": a.tasks - len(done), "makespan_s": round(makespan, 3),
            "throughput_tps": round(len(done) / makespan, 3) if makespan else 0,
            "lat_mean_s": round(sum(lats) / len(lats), 3) if lats else 0,
            "lat_p50_s": round(percentile(lats, 50), 3),
            "lat_p95_s": round(percentile(lats, 95), 3),
            "lat_p99_s": round(percentile(lats, 99), 3),
            "lat_max_s": round(max(lats), 3) if lats else 0,
            "jain_fairness": round(jain_index(norm), 4),
            "retried_tasks": sum(1 for v in done.values() if v.get("attempts", 1) > 1),
        },
        "per_worker_completed": per_worker,
        "scheduler": {k: stats_box.get(k) for k in ("stats", "workers")},
        "completion_times": sorted(round(v["recv_ts"] - t0, 3) for v in done.values()),
    }
    ch.close()
    txt = json.dumps(report, indent=2)
    if a.out:
        with open(a.out, "w") as f:
            f.write(txt)
    print(json.dumps(report["metrics"], indent=2))


if __name__ == "__main__":
    main()
