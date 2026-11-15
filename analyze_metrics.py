"""Summarise metrics.csv: completion time, makespan, requeues, per-worker task counts."""

import argparse
import csv
from collections import Counter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", nargs="?", default="metrics.csv")
    parser.add_argument("--scheduler", default="")
    parser.add_argument("--scenario", default="")
    parser.add_argument("--csv", help="append one summary row to this CSV file")
    args = parser.parse_args()

    queued, done, per_worker = {}, {}, Counter()
    requeued = failures = timeouts = 0
    with open(args.filename, newline="") as source:
        for row in csv.DictReader(source):
            event, task_id = row["event"], row["task_id"]
            if event == "queued":
                queued[task_id] = float(row["timestamp"])
            elif event == "done":
                done[task_id] = float(row["timestamp"])
                per_worker[row["worker"]] += 1
            elif event == "requeued":
                requeued += 1
            elif event == "worker_failed":
                failures += 1
            elif event == "task_timeout":
                timeouts += 1

    times = sorted(done[t] - queued[t] for t in done if t in queued)
    avg = sum(times) / len(times) if times else 0.0
    p95 = times[min(len(times) - 1, int(0.95 * len(times)))] if times else 0.0
    makespan = (max(done.values()) - min(queued.values())) if done and queued else 0.0
    unfinished = len(queued) - len(done)
    spread = " ".join(f"{w}:{n}" for w, n in sorted(per_worker.items()))

    print(f"submitted={len(queued)} completed={len(done)} unfinished={unfinished}")
    print(f"requeued={requeued} worker_failures={failures} task_timeouts={timeouts}")
    print(f"avg completion={avg:.2f}s  p95={p95:.2f}s  makespan={makespan:.2f}s")
    print(f"tasks per worker: {spread}")

    if args.csv:
        try:
            new = open(args.csv).read() == ""
        except FileNotFoundError:
            new = True
        with open(args.csv, "a", newline="") as out:
            writer = csv.writer(out, lineterminator="\n")
            if new:
                writer.writerow(["scenario", "scheduler", "completed", "unfinished", "requeued",
                                 "avg_s", "p95_s", "makespan_s", "tasks_per_worker"])
            writer.writerow([args.scenario, args.scheduler, len(done), unfinished, requeued,
                             f"{avg:.2f}", f"{p95:.2f}", f"{makespan:.2f}", spread])


if __name__ == "__main__":
    main()
