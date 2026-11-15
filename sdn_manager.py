# sdn manager: takes tcp events from server.py and turns them into openflow rules on s1 (ovs-ofctl)
#   worker_failed     -> drop that worker's ip traffic for a while (quarantine)
#   worker_registered -> remove the drop rules
#   worker_busy/idle  -> just tracked and logged
# not a real openflow controller, it only drives ovs through its command line tool

import argparse
import json
import socket
import subprocess
import threading
import time

args = None
blocked = {}        # ip -> when its quarantine ends
load = {}           # ip -> "busy" / "idle"
lock = threading.Lock()


def ofctl(*cmd):
    full = ["ovs-ofctl", "-O", "OpenFlow13", *cmd]
    if args.dry_run:                         # used by local_test.py, no switch there
        print("[dry-run] " + " ".join(full), flush=True)
        return ""
    return subprocess.run(full, check=True, capture_output=True, text=True).stdout


def block(ip):
    with lock:
        for field in ("nw_src", "nw_dst"):   # one rule from the worker, one to it
            # priority 100 beats the priority 0 NORMAL rule. hard_timeout = switch removes it by
            # itself, otherwise a blocked worker could never reconnect
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


def serve(conn):
    with conn, conn.makefile("r") as reader:
        for line in reader:
            try:
                ev = json.loads(line)
                ip, kind = ev["worker_ip"], ev["event"]
                if kind == "worker_failed":
                    block(ip)
                elif kind == "worker_registered":
                    unblock(ip)
                elif kind in ("worker_busy", "worker_idle"):
                    state = kind.split("_")[1]
                    if load.get(ip) != state:        # only log changes
                        load[ip] = state
                        print(f"LOAD {ip} {state}", flush=True)
            except (ValueError, KeyError, subprocess.CalledProcessError) as err:
                print(f"bad event {line.strip()!r}: {err}", flush=True)


def monitor():
    while True:
        time.sleep(2)
        with lock:
            for ip in [ip for ip, end in blocked.items() if end < time.time()]:
                del blocked[ip]                      # list first so we can delete while looping
                print(f"QUARANTINE ENDED {ip}", flush=True)
        if not args.dry_run:
            stats = ofctl("dump-ports", args.bridge)     # port counters = monitoring
            with open("port_stats.log", "a") as f:
                f.write(f"--- {time.time():.1f}\n{stats}")


def main():
    global args
    parser = argparse.ArgumentParser()
    parser.add_argument("--bind", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9999)
    parser.add_argument("--bridge", default="s1")
    parser.add_argument("--quarantine", type=int, default=8, help="seconds a failed worker stays blocked")
    parser.add_argument("--dry-run", action="store_true", help="print the ovs-ofctl commands, don't run them")
    args = parser.parse_args()
    threading.Thread(target=monitor, daemon=True).start()
    with socket.socket() as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((args.bind, args.port))
        srv.listen()
        print(f"SDN manager listening on TCP {args.port}", flush=True)
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=serve, args=(conn,), daemon=True).start()


if __name__ == "__main__":
    main()
