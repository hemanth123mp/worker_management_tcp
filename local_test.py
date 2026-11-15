"""Local tests WITHOUT Mininet (127.0.0.1, SDN manager in --dry-run mode).

Runs the functional tests (T1-T6) and the same scenarios as topology.py, and writes
results/summary_local.csv.   Usage: python3 local_test.py [--quick]
Keep SCENARIOS / WORKERS in sync with topology.py.
"""

import csv
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable
# scenario -> (seconds between a client's tasks, start offset between clients, crash worker1?)
SCENARIOS = {"burst": (3, 0, False), "light": (6, 1.5, False), "failure": (6, 1.5, True)}
TASKS_PER_CLIENT, TASK_DELAY = 5, 2
WORKERS = [("worker1", 2.0), ("worker2", 1.0), ("worker3", 0.5)]
CLIENTS = ["client1", "client2", "client3", "client4"]
passed, failed = [], []


class Rig:
    def __init__(self, name, scheduler="adaptive"):
        self.dir = Path(tempfile.mkdtemp(prefix=f"wm_{name}_"))
        self.scheduler, self.procs = scheduler, []

    def start(self, script, *args, log):
        proc = subprocess.Popen([PY, str(ROOT / script), *args], cwd=self.dir,
                                stdout=open(self.dir / log, "w"), stderr=subprocess.STDOUT)
        self.procs.append(proc)
        return proc

    def boot(self):
        self.start("sdn_manager.py", "--dry-run", "--quarantine", "6", log="sdn_manager.log")
        time.sleep(0.5)
        self.start("server.py", "--scheduler", self.scheduler, "--sdn-host", "127.0.0.1", log="server.log")
        time.sleep(0.7)

    def worker(self, name, *args):
        return self.start("worker.py", "127.0.0.1", name, *args, log=f"{name}.log")

    def client(self, name, delay, *args):
        return self.start("client.py", "127.0.0.1", name, str(delay), *args, log=f"{name}.log")

    def text(self, log):
        try:
            return (self.dir / log).read_text()
        except FileNotFoundError:
            return ""

    def count(self, event):
        return self.text("metrics.csv").count(f",{event},")

    def wait_done(self, total, timeout):
        end = time.time() + timeout
        while self.count("done") < total and time.time() < end:
            time.sleep(0.5)
        time.sleep(0.5)

    def close(self):
        for proc in self.procs:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except Exception:
                pass


def check(name, condition, detail=""):
    (passed if condition else failed).append(name)
    print(f"  [{'PASS' if condition else 'FAIL'}] {name} {detail}", flush=True)


def test_late_register():
    print("T1: task queued BEFORE the worker registers", flush=True)
    rig = Rig("late"); rig.boot()
    rig.client("c1", 2).wait(); rig.worker("workerA"); rig.wait_done(1, 10)
    check("task completes", rig.count("done") == 1)
    check("worker not wrongly failed", rig.count("worker_failed") == 0)
    rig.close()


def test_task_timeout():
    print("T2: task timeout -> requeued, slow worker NOT blocked", flush=True)
    rig = Rig("timeout"); rig.boot()
    rig.worker("slow", "--slowdown", "5", "--speed", "2"); time.sleep(0.5)   # only worker at first
    rig.client("c1", 4, "--tasks", "1"); time.sleep(1); rig.worker("fast", "--speed", "1")
    rig.wait_done(1, 25)
    check("task_timeout logged", rig.count("task_timeout") >= 1)
    check("task finished by another worker", rig.count("done") == 1)
    check("slow worker not failed / not blocked", rig.count("worker_failed") == 0 and "BLOCK" not in rig.text("sdn_manager.log"))
    rig.close()


def test_crash_and_restart():
    print("T3: worker crashes -> requeue + SDN block; restarted worker re-registers", flush=True)
    rig = Rig("crash"); rig.boot()
    rig.worker("worker1", "--speed", "1", "--fail-after", "3", "--restart-after", "3")
    rig.worker("worker2", "--speed", "1"); time.sleep(0.5)
    rig.client("c1", 5, "--tasks", "1")
    rig.wait_done(1, 25); time.sleep(9)
    check("worker_failed + requeued", rig.count("worker_failed") >= 1 and rig.count("requeued") >= 1)
    check("SDN manager got worker_failed over TCP (BLOCK)", "BLOCK" in rig.text("sdn_manager.log"))
    check("task completed once", rig.count("done") == 1)
    check("worker1 registered again after restart", rig.count("registered") >= 3)
    check("quarantine removed (UNBLOCK or expiry)", "UNBLOCK" in rig.text("sdn_manager.log") or "QUARANTINE ENDED" in rig.text("sdn_manager.log"))
    rig.close()


def test_hang():
    print("T4: worker hangs (SIGSTOP) -> heartbeat timeout, recovers after SIGCONT", flush=True)
    rig = Rig("hang"); rig.boot()
    w1 = rig.worker("worker1", "--speed", "1"); rig.worker("worker2", "--speed", "1"); time.sleep(0.5)
    rig.client("c1", 8, "--tasks", "1"); time.sleep(2)
    os.kill(w1.pid, signal.SIGSTOP); time.sleep(5)
    rig.wait_done(1, 25)
    os.kill(w1.pid, signal.SIGCONT); time.sleep(3)
    check("heartbeat loss detected", "heartbeat lost" in rig.text("metrics.csv"))
    check("task completed by the other worker", rig.count("done") == 1)
    check("hung worker recovered after SIGCONT", rig.count("worker_recovered") >= 1)
    rig.close()


def scenario(name, scheduler):
    interval, stagger, crash = SCENARIOS[name]
    rig = Rig(f"{name}_{scheduler}", scheduler); rig.boot()
    for number, (worker, speed) in enumerate(WORKERS):
        extra = ["--fail-on-task", "3", "--restart-after", "5"] if (crash and number == 0) else []
        rig.worker(worker, "--speed", str(speed), *extra)
    time.sleep(2)
    for index, client in enumerate(CLIENTS):
        rig.client(client, TASK_DELAY, "--tasks", str(TASKS_PER_CLIENT), "--interval", str(interval),
                   "--start-delay", str(index * stagger))
    total = len(CLIENTS) * TASKS_PER_CLIENT
    rig.wait_done(total, 90)
    out = subprocess.run([PY, str(ROOT / "analyze_metrics.py"), "metrics.csv", "--scenario", name,
                          "--scheduler", scheduler, "--csv", str(ROOT / "results" / "summary_local.csv")],
                         cwd=rig.dir, capture_output=True, text=True).stdout
    print(f"  {name}/{scheduler}\n    " + out.replace("\n", "\n    "), flush=True)
    done, requeued, sdn = rig.count("done"), rig.count("requeued"), rig.text("sdn_manager.log")
    check(f"{name}/{scheduler}: all {total} tasks completed", done == total)
    if crash:
        check(f"{name}/{scheduler}: failure handled (requeue + BLOCK)", requeued >= 1 and "BLOCK" in sdn)
    rig.close()


if __name__ == "__main__":
    (ROOT / "results").mkdir(exist_ok=True)
    if "--quick" not in sys.argv:
        (ROOT / "results" / "summary_local.csv").unlink(missing_ok=True)
    test_late_register(); test_task_timeout(); test_crash_and_restart(); test_hang()
    if "--quick" not in sys.argv:
        for sc in SCENARIOS:
            for sched in ("adaptive", "round_robin"):
                scenario(sc, sched)
    print(f"\n{len(passed)} passed, {len(failed)} failed", flush=True)
    sys.exit(1 if failed else 0)
