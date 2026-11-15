"""SDN manager: receives TCP events from server.py and programs the OpenFlow switch (OVS).

Events (newline-delimited JSON over TCP port 9999):
  worker_failed      -> quarantine: drop all IP traffic to/from that worker (auto-expires)
  worker_registered  -> remove the drop rules for that worker
  worker_busy/idle   -> workload tracking (logged)
Also samples switch port counters every 2 s (monitoring).
"""

import argparse
import json
import socket
import subprocess
import threading
import time

args = None
blocked = {}                # worker ip -> time the quarantine ends
load = {}                   # worker ip -> "busy" / "idle"
lock = threading.Lock()


def ofctl(*command):
    full = ["ovs-ofctl", "-O", "OpenFlow13", *command]
    if args.dry_run:
        print("[dry-run] " + " ".join(full), flush=True)
        return ""
    return subprocess.run(full, check=True, capture_output=True, text=True).stdout


def block(ip):
    with lock:
        for field in ("nw_src", "nw_dst"):
            ofctl("add-flow", args.bridge,
                  f"priority=100,ip,{field}={ip},hard_timeout={args.quarantine},actions=drop")
        blocked[ip] = time.time() + args.quarantine
    print(f"BLOCK {ip} for {args.quarantine}s", flush=True)


def unblock(ip):
    with lock:
        if ip not in blocked:
            return
        for field in ("nw_src", "nw_dst"):
            ofctl("--strict", "del-flows", args.bridge, f"priority=100,ip,{field}={ip}")
        del blocked[ip]
    print(f"UNBLOCK {ip}", flush=True)


def serve(connection):
    with connection, connection.makefile("r") as reader:
        for line in reader:
            try:
                event = json.loads(line)
                ip, kind = event["worker_ip"], event["event"]
                if kind == "worker_failed":
                    block(ip)
                elif kind == "worker_registered":
                    unblock(ip)
                elif kind in ("worker_busy", "worker_idle"):
                    state = kind.split("_")[1]
                    if load.get(ip) != state:
                        load[ip] = state
                        print(f"LOAD {ip} {state}", flush=True)
            except (ValueError, KeyError, subprocess.CalledProcessError) as error:
                print(f"bad event {line.strip()!r}: {error}", flush=True)


def monitor():
    while True:
        time.sleep(2)
        with lock:
            for ip in [ip for ip, end in blocked.items() if end < time.time()]:
                del blocked[ip]
                print(f"QUARANTINE ENDED {ip}", flush=True)
        if not args.dry_run:
            stats = ofctl("dump-ports", args.bridge)
            with open("port_stats.log", "a") as log:
                log.write(f"--- {time.time():.1f}\n{stats}")


def main():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--bridge", default="s1")
    parser.add_argument("--quarantine", type=int, default=8, help="seconds a failed worker stays blocked")
    parser.add_argument("--dry-run", action="store_true", help="print ovs-ofctl commands instead of running them")
    args = parser.parse_args()
    threading.Thread(target=monitor, daemon=True).start()
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.bind, args.port))
        server.listen()
        print(f"SDN manager listening on TCP {args.port}", flush=True)
        while True:
            connection, _ = server.accept()
            threading.Thread(target=serve, args=(connection,), daemon=True).start()


if __name__ == "__main__":
    main()
