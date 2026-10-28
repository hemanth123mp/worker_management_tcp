#!/usr/bin/env python3
"""Unauthorized host: repeatedly tries to register with a wrong token, then checks
whether it is still able to reach the scheduler (i.e. whether the SDN blocked it)."""
import argparse
import json
import socket
import time

from common import DEFAULT_PORT, Channel


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--server", default="10.0.0.1")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--attempts", type=int, default=8)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    res = []
    for i in range(a.attempts):
        t0 = time.time()
        try:
            s = socket.create_connection((a.server, a.port), timeout=2)
            ch = Channel(s)
            ch.send({"type": "REGISTER", "worker_id": "evil%d" % i, "auth": "wrong"})
            s.settimeout(2)
            r = ch.recv()
            res.append({"i": i, "t": round(t0, 3), "connected": True,
                        "reply": r.get("type") if r else None})
            ch.close()
        except OSError as e:
            res.append({"i": i, "t": round(t0, 3), "connected": False, "error": type(e).__name__})
        time.sleep(0.3)
    out = {"attempts": res, "blocked_at_end": all(not r["connected"] for r in res[-3:])}
    if a.out:
        with open(a.out, "w") as f:
            json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
