"""Print the three metrics needed for the small scheduler comparison."""

import csv
import sys


def main():
    filename = sys.argv[1] if len(sys.argv) == 2 else "metrics.csv"
    queued, done, requeued = {}, {}, 0
    with open(filename, newline="") as source:
        for row in csv.DictReader(source):
            event, task_id = row["event"], row["task_id"]
            if event == "queued":
                queued[task_id] = float(row["timestamp"])
            elif event == "done":
                done[task_id] = float(row["timestamp"])
            elif event == "requeued":
                requeued += 1
    completion_times = [done[task_id] - queued[task_id] for task_id in done if task_id in queued]
    print(f"completed tasks: {len(completion_times)}")
    print(f"requeued tasks: {requeued}")
    if completion_times:
        print(f"average completion time: {sum(completion_times) / len(completion_times):.2f} seconds")


if __name__ == "__main__":
    main()
