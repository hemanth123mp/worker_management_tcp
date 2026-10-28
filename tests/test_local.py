"""Functional tests of the TCP application on localhost (no Mininet needed).
Run:  python3 -m unittest tests.test_local -v
"""
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from common import Channel  # noqa: E402

_port = [15000]


def spawn(*args):
    return subprocess.Popen([sys.executable] + list(args), cwd=ROOT,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def get_stats(port):
    s = socket.create_connection(("127.0.0.1", port), timeout=5)
    ch = Channel(s)
    ch.send({"type": "CLIENT_HELLO"})
    ch.recv()
    ch.send({"type": "STATS"})
    r = ch.recv()
    ch.close()
    return r


class Cluster:
    def __init__(self, policy="least_loaded", workers=(("w1", 2, 2.0), ("w2", 2, 1.0), ("w3", 2, 0.5)),
                 hb_timeout=1.5, task_timeout=20):
        _port[0] += 1
        self.port = _port[0]
        self.procs, self.workers = [], {}
        self.sched = spawn("scheduler.py", "--port", str(self.port), "--policy", policy,
                           "--hb-timeout", str(hb_timeout), "--task-timeout", str(task_timeout))
        self.procs.append(self.sched)
        for _ in range(50):
            try:
                socket.create_connection(("127.0.0.1", self.port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.1)
        for wid, cap, speed in workers:
            p = spawn("worker.py", "--id", wid, "--server", "127.0.0.1", "--port", str(self.port),
                      "--capacity", str(cap), "--speed", str(speed), "--hb-interval", "0.5")
            self.workers[wid] = p
            self.procs.append(p)
        for _ in range(50):
            if len(get_stats(self.port)["workers"]) == len(workers):
                break
            time.sleep(0.1)

    def client(self, tasks=40, rate=40, dur=0.1, kb=1, deadline=60):
        out = tempfile.mktemp(suffix=".json")
        subprocess.run([sys.executable, "client.py", "--server", "127.0.0.1", "--port", str(self.port),
                        "--tasks", str(tasks), "--rate", str(rate), "--mean-duration", str(dur),
                        "--payload-kb", str(kb), "--deadline", str(deadline), "--out", out],
                       cwd=ROOT, stdout=subprocess.DEVNULL, check=True)
        with open(out) as f:
            return json.load(f)

    def close(self):
        for p in self.procs:
            try:
                p.kill()
            except OSError:
                pass
            p.wait()


class TestTaskSystem(unittest.TestCase):
    def test_registration_and_completion(self):
        c = Cluster()
        try:
            self.assertEqual(len(get_stats(c.port)["workers"]), 3)
            r = c.client(tasks=40)
            self.assertEqual(r["metrics"]["completed"], 40)
        finally:
            c.close()

    def test_least_loaded_prefers_fast_workers(self):
        c = Cluster(policy="least_loaded")
        try:
            r = c.client(tasks=120, rate=80, dur=0.15)
            pw = r["per_worker_completed"]
            self.assertEqual(r["metrics"]["completed"], 120)
            self.assertGreater(pw["w1"], pw["w3"])
        finally:
            c.close()

    def test_round_robin_is_load_oblivious(self):
        c = Cluster(policy="round_robin")
        try:
            r = c.client(tasks=60, rate=80, dur=0.1)
            pw = r["per_worker_completed"]
            self.assertEqual(r["metrics"]["completed"], 60)
            self.assertLessEqual(max(pw.values()) - min(pw.values()), 2)
        finally:
            c.close()

    def test_worker_crash_redistribution(self):
        c = Cluster()
        try:
            threading.Timer(0.8, lambda: c.workers["w2"].kill()).start()
            r = c.client(tasks=80, rate=20, dur=0.3)
            st = r["scheduler"]["stats"]
            self.assertEqual(r["metrics"]["completed"], 80)
            self.assertGreaterEqual(st["worker_failures"], 1)
            self.assertGreaterEqual(st["redistributed"], 1)
        finally:
            c.close()

    def test_hung_worker_detected_by_heartbeat(self):
        c = Cluster(hb_timeout=1.5)
        try:
            threading.Timer(0.8, lambda: os.kill(c.workers["w1"].pid, signal.SIGSTOP)).start()
            r = c.client(tasks=60, rate=20, dur=0.3)
            st = r["scheduler"]["stats"]
            self.assertEqual(r["metrics"]["completed"], 60)
            self.assertTrue(st["detect_delay_s"])
            os.kill(c.workers["w1"].pid, signal.SIGCONT)
        finally:
            c.close()

    def test_task_timeout_reassigns(self):
        c = Cluster(workers=(("w1", 1, 1.0), ("w2", 1, 1.0)), hb_timeout=30, task_timeout=1.0)
        try:
            threading.Timer(0.3, lambda: os.kill(c.workers["w1"].pid, signal.SIGSTOP)).start()
            r = c.client(tasks=10, rate=10, dur=0.1)
            self.assertEqual(r["metrics"]["completed"], 10)
            self.assertGreaterEqual(r["scheduler"]["stats"]["timeouts"], 1)
        finally:
            os.kill(c.workers["w1"].pid, signal.SIGCONT)
            c.close()

    def test_bad_token_rejected(self):
        c = Cluster()
        try:
            s = socket.create_connection(("127.0.0.1", c.port), timeout=3)
            ch = Channel(s)
            ch.send({"type": "REGISTER", "worker_id": "evil", "auth": "nope"})
            self.assertEqual(ch.recv()["type"], "REJECT")
            ch.close()
            st = get_stats(c.port)
            self.assertEqual(len(st["workers"]), 3)
            self.assertGreaterEqual(st["stats"]["rejected"], 1)
        finally:
            c.close()


if __name__ == "__main__":
    unittest.main()
