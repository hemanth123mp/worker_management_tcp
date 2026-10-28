"""Install a simple OpenFlow drop rule after a worker failure."""

import json
import subprocess
import time
from pathlib import Path

EVENT_FILE = Path("sdn_events.jsonl")
position = 0
blocked = set()


def block_worker(worker_ip):
    if worker_ip in blocked:
        return
    rules = [f"priority=100,ip,nw_src={worker_ip},actions=drop",
             f"priority=100,ip,nw_dst={worker_ip},actions=drop"]
    for rule in rules:
        subprocess.run(["ovs-ofctl", "-O", "OpenFlow13", "add-flow", "s1", rule], check=True)
    blocked.add(worker_ip)
    print(f"SDN rule installed: blocked failed worker {worker_ip}", flush=True)


def main():
    global position
    print("SDN manager waiting for failed-worker events", flush=True)
    while True:
        try:
            if EVENT_FILE.exists() and EVENT_FILE.stat().st_size < position:
                position = 0
            with EVENT_FILE.open() as source:
                source.seek(position)
                for line in source:
                    event = json.loads(line)
                    if event.get("event") == "block_failed_worker":
                        block_worker(event["worker_ip"])
                position = source.tell()
        except FileNotFoundError:
            pass
        time.sleep(0.5)


if __name__ == "__main__":
    main()
