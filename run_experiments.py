#!/usr/bin/env python3
"""Automated experiments: static baseline vs SDN-aware scheduling, in Mininet.

Terminal 1:  ryu-manager sdn_controller.py          (from project root)
Terminal 2:  sudo python3 run_experiments.py [--repeats 2] [--only heavy_bg,failure_bg]
Then:        python3 analyze.py
Modes
  baseline : round-robin scheduler + plain L2 learning switch (no QoS / blocking / flush)
  proposed : least-loaded scheduler + SDN-aware controller (QoS queue, flush, blocking)
"""
import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import time

from mininet.log import info, setLogLevel

import topology

ROOT = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
WORKERS = [("w1", 2, 2.0), ("w2", 2, 1.5), ("w3", 2, 1.0), ("w4", 2, 0.5)]   # id, capacity, speed
MODES = {"baseline": dict(policy="round_robin", sdn_aware=False),
         "proposed": dict(policy="least_loaded", sdn_aware=True)}
CONDITIONS = [
    dict(name="light_nobg", tasks=150, rate=30, bg=False),
    dict(name="heavy_nobg", tasks=300, rate=60, bg=False),
    dict(name="heavy_bg", tasks=300, rate=60, bg=True),
    dict(name="failure_bg", tasks=300, rate=40, bg=True, inject="hang_w3", at=4.0),
    dict(name="attack", tasks=200, rate=40, bg=False, inject="attack", at=3.0),
]


def ctrl_event(**ev):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.sendto(json.dumps(ev).encode(), ("127.0.0.1", 9999))


def popen(host, argv, log=None):
    f = open(log, "w") if log else subprocess.DEVNULL
    return host.popen([str(a) for a in argv], stdout=f, stderr=subprocess.STDOUT, cwd=ROOT)


def run_trial(net, cond, mode, rep, outdir):
    cfg, name = MODES[mode], "%s_%s_%d" % (cond["name"], mode, rep)
    info("\n=== %s ===\n" % name)
    ctrl_event(event="CONFIG", sdn_aware=cfg["sdn_aware"])
    time.sleep(1.5)
    procs, workers = [], {}
    sched = net.get("sched")
    procs.append(popen(sched, [PY, "scheduler.py", "--policy", cfg["policy"], "--ctrl-host",
                               topology.NOTIFY_IP, "--hb-timeout", 3, "--task-timeout", 20],
                       "%s/%s_scheduler.log" % (outdir, name)))
    time.sleep(1.0)
    for wid, cap, speed in WORKERS:
        p = popen(net.get(wid), [PY, "worker.py", "--id", wid, "--server", "10.0.0.1",
                                 "--capacity", cap, "--speed", speed],
                  "%s/%s_%s.log" % (outdir, name, wid))
        workers[wid] = p
        procs.append(p)
    time.sleep(2.0)
    if cond.get("bg"):
        procs.append(popen(net.get("bgs"), ["iperf", "-s", "-p", 5001]))
        time.sleep(0.5)
        procs.append(popen(net.get("bgc"), ["iperf", "-c", "10.0.0.21", "-p", 5001, "-t", 300]))
        time.sleep(2.0)
    out = "%s/%s.json" % (outdir, name)
    client = popen(net.get("client"), [PY, "client.py", "--server", "10.0.0.1", "--tasks", cond["tasks"],
                                       "--rate", cond["rate"], "--label", name, "--out", out],
                   "%s/%s_client.log" % (outdir, name))
    inj = cond.get("inject")
    if inj:
        time.sleep(cond["at"])
        if inj == "hang_w3":
            info("*** injecting failure: w3 hangs (SIGSTOP)\n")
            os.kill(workers["w3"].pid, signal.SIGSTOP)
        elif inj == "attack":
            info("*** injecting attack from host att\n")
            procs.append(popen(net.get("att"), [PY, "attacker.py", "--server", "10.0.0.1", "--out",
                                                "%s/%s_attacker.json" % (outdir, name)],
                               "%s/%s_attacker.log" % (outdir, name)))
    client.wait()
    for p in procs:
        try:
            p.kill()
        except OSError:
            pass
        p.wait()
    time.sleep(1.0)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repeats", type=int, default=2)
    ap.add_argument("--only", default="", help="comma separated condition names")
    ap.add_argument("--outdir", default=os.path.join(ROOT, "results"))
    a = ap.parse_args()
    os.makedirs(a.outdir, exist_ok=True)
    setLogLevel("info")
    net = topology.start()
    try:
        info("*** connectivity check\n")
        if net.pingAll() > 0:
            info("!!! pingAll lost packets - is ryu-manager running?\n")
        info(net.get("sched").cmd("ping -c1 -W1 %s" % topology.NOTIFY_IP))
        sel = [c for c in CONDITIONS if not a.only or c["name"] in a.only.split(",")]
        for cond in sel:
            for rep in range(a.repeats):
                for mode in MODES:
                    run_trial(net, cond, mode, rep, a.outdir)
    finally:
        net.stop()


if __name__ == "__main__":
    main()
