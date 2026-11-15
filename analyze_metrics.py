# reads metrics.csv (written by the server) and prints completed, requeued, avg time, makespan, per-worker count

import argparse
import csv
from collections import Counter


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", nargs="?", default="metrics.csv")
    parser.add_argument("--scheduler", default="")
    parser.add_argument("--scenario", default="")
    parser.add_argument("--csv", help="also append one summary row to this csv")
    args = parser.parse_args()

    queued, done, per_w = {}, {}, Counter()    # task id -> timestamps, and finished tasks per worker
    requeued = 0
    with open(args.filename, newline="") as f:
        for row in csv.DictReader(f):
            ev, tid = row["event"], row["task_id"]
            if ev == "queued":
                queued[tid] = float(row["timestamp"])
            elif ev == "done":
                done[tid] = float(row["timestamp"])
                per_w[row["worker"]] += 1
            elif ev == "requeued":
                requeued += 1

    # completion time = finished minus submitted, so it includes waiting in the queue
    times = [done[t] - queued[t] for t in done if t in queued]
    avg = sum(times) / len(times) if times else 0.0
    makespan = (max(done.values()) - min(queued.values())) if done and queued else 0.0
    spread = " ".join(f"{w}:{n}" for w, n in sorted(per_w.items()))

    print(f"completed={len(done)}/{len(queued)}  requeued={requeued}")
    print(f"avg completion={avg:.2f}s  makespan={makespan:.2f}s")
    print(f"tasks per worker: {spread}")

    if args.csv:
        try:
            new = open(args.csv).read() == ""      # write the header only for a new/empty file
        except FileNotFoundError:
            new = True
        with open(args.csv, "a", newline="") as out:
            w = csv.writer(out, lineterminator="\n")
            if new:
                w.writerow(["scenario", "scheduler", "completed", "requeued",
                            "avg_s", "makespan_s", "tasks_per_worker"])
            w.writerow([args.scenario, args.scheduler, len(done), requeued,
                        f"{avg:.2f}", f"{makespan:.2f}", spread])


if __name__ == "__main__":
    main()
