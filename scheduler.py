#!/usr/bin/env python3
"""Task scheduler / notification server (TCP, multi-threaded).

* Workers register (token-authenticated), send heartbeats and results.
* Clients submit tasks and are notified (DONE/ERROR) when they finish.
* Policies: round_robin (baseline, load-oblivious) | least_loaded (workload-aware).
* Failure handling: heartbeat timeout, connection loss, per-task timeout; tasks are
  re-queued and redistributed to other workers (max-retries limit).
* Socket <-> SDN integration: lifecycle events (WORKER_UP/DOWN, LOAD, BLOCK) are sent
  as UDP datagrams to the SDN controller's northbound notification port.
"""
import argparse
import itertools
import json
import logging
import socket
import threading
import time
from collections import deque

from common import AUTH_TOKEN, DEFAULT_PORT, Channel

log = logging.getLogger("scheduler")


class Task:
    def __init__(self, tid, cid, duration, data, client):
        self.id, self.cid, self.duration, self.data, self.client = tid, cid, duration, data, client
        self.submit_ts = time.time()
        self.assigned_at = 0.0
        self.attempt = 0
        self.retries = 0
        self.worker = None
        self.excluded = set()
        self.done = False


class Worker:
    def __init__(self, wid, ip, ch, capacity, speed):
        self.id, self.ip, self.ch = wid, ip, ch
        self.capacity, self.speed = capacity, speed
        self.active = {}            # task id -> Task currently assigned
        self.completed = 0
        self.assigned_total = 0
        self.last_hb = time.time()
        self.reported_load = 0
        self.alive = True


class Scheduler:
    def __init__(self, a):
        self.host, self.port, self.policy = a.host, a.port, a.policy
        self.task_timeout, self.hb_timeout = a.task_timeout, a.hb_timeout
        self.max_retries, self.token = a.max_retries, a.token
        self.block_strikes = a.block_strikes
        self.ctrl = (a.ctrl_host, a.ctrl_port) if a.ctrl_host else None
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.cv = threading.Condition(threading.RLock())
        self.workers, self.tasks, self.pending = {}, {}, deque()
        self.strikes, self.blocked = {}, set()
        self.rr = 0
        self.ids = itertools.count(1)
        self.stats = dict(submitted=0, completed=0, failed=0, timeouts=0, redistributed=0,
                          worker_failures=0, duplicates=0, blocked=0, rejected=0,
                          detect_delay_s=[])

    # ---------------- SDN northbound notifications (UDP) ----------------
    def notify(self, event, **kw):
        if not self.ctrl:
            return
        try:
            self.udp.sendto(json.dumps(dict(event=event, ts=time.time(), **kw)).encode(), self.ctrl)
        except OSError:
            pass

    # ---------------- server ----------------
    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(128)
        threading.Thread(target=self._dispatch_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()
        log.info("scheduler listening on %s:%d policy=%s", self.host, self.port, self.policy)
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self._handle, args=(conn, addr[0]), daemon=True).start()

    def _strike(self, ip, reason):
        with self.cv:
            self.strikes[ip] = self.strikes.get(ip, 0) + 1
            n = self.strikes[ip]
            self.stats["rejected"] += 1
            block = n >= self.block_strikes and ip not in self.blocked
            if block:
                self.blocked.add(ip)
                self.stats["blocked"] += 1
        log.warning("unauthorized access from %s (%s) strike %d", ip, reason, n)
        if block:
            log.warning("requesting SDN block of %s", ip)
            self.notify("BLOCK", ip=ip, reason=reason)

    def _handle(self, conn, ip):
        ch = Channel(conn)
        try:
            conn.settimeout(5)
            first = ch.recv()
            conn.settimeout(None)
        except (OSError, ValueError):
            self._strike(ip, "bad handshake")
            ch.close()
            return
        typ = first.get("type") if isinstance(first, dict) else None
        if typ == "REGISTER":
            if first.get("auth") != self.token or not first.get("worker_id"):
                self._strike(ip, "bad auth token")
                try:
                    ch.send({"type": "REJECT"})
                except OSError:
                    pass
                ch.close()
                return
            self._worker_session(ch, ip, first)
        elif typ == "CLIENT_HELLO":
            self._client_session(ch, ip)
        else:
            self._strike(ip, "unknown first message")
            ch.close()

    # ---------------- worker side ----------------
    def _worker_session(self, ch, ip, reg):
        w = Worker(str(reg["worker_id"]), ip, ch, max(1, int(reg.get("capacity", 1))),
                   max(0.01, float(reg.get("speed", 1.0))))
        try:
            ch.send({"type": "REGISTERED"})
        except OSError:
            ch.close()
            return
        with self.cv:
            old = self.workers.get(w.id)
        if old and old.alive:
            self._worker_failed(old, "replaced by new registration")
        with self.cv:
            self.workers[w.id] = w
            self.cv.notify_all()
        log.info("worker %s registered from %s (cap=%d speed=%.2f)", w.id, ip, w.capacity, w.speed)
        self.notify("WORKER_UP", worker_id=w.id, ip=ip)
        try:
            while True:
                m = ch.recv()
                if m is None:
                    break
                t = m.get("type")
                if t == "HEARTBEAT":
                    w.last_hb = time.time()
                    w.reported_load = m.get("load", 0)
                elif t == "RESULT":
                    w.last_hb = time.time()
                    self._on_result(w, m)
        except (OSError, ValueError):
            pass
        finally:
            self._worker_failed(w, "connection closed")

    def _on_result(self, w, m):
        with self.cv:
            t = self.tasks.get(m.get("task_id"))
            if t is None or t.done or t.worker is not w or t.attempt != m.get("attempt"):
                self.stats["duplicates"] += 1       # late result of a re-assigned task
                self.cv.notify_all()
                return
            t.done = True
            w.active.pop(t.id, None)
            w.completed += 1
            self.stats["completed"] += 1
            reply = {"type": "DONE", "task_id": t.cid, "worker": w.id, "attempts": t.attempt,
                     "latency": round(time.time() - t.submit_ts, 4),
                     "exec_time": m.get("exec_time"), "digest": m.get("digest")}
            self.cv.notify_all()
        self._send_client(t, reply)

    def _send_client(self, t, msg):
        try:
            t.client.send(msg)
        except OSError:
            pass

    def _worker_failed(self, w, reason):
        with self.cv:
            if not w.alive:
                return
            w.alive = False
            n = len(w.active)
            for t in list(w.active.values()):
                t.worker = None
                self.pending.appendleft(t)
            w.active.clear()
            self.stats["worker_failures"] += 1
            self.stats["redistributed"] += n
            if reason == "heartbeat timeout":
                self.stats["detect_delay_s"].append(round(time.time() - w.last_hb, 2))
            self.cv.notify_all()
        log.warning("worker %s FAILED (%s); %d task(s) re-queued", w.id, reason, n)
        w.ch.close()
        self.notify("WORKER_DOWN", worker_id=w.id, ip=w.ip, reason=reason, requeued=n)

    # ---------------- client side ----------------
    def _client_session(self, ch, ip):
        try:
            ch.send({"type": "CLIENT_READY"})
            while True:
                m = ch.recv()
                if m is None:
                    break
                t = m.get("type")
                if t == "SUBMIT":
                    tid = "t%d" % next(self.ids)
                    task = Task(tid, m.get("task_id"), float(m.get("duration", 0.1)),
                                m.get("data", ""), ch)
                    with self.cv:
                        self.tasks[tid] = task
                        self.pending.append(task)
                        self.stats["submitted"] += 1
                        self.cv.notify_all()
                elif t == "STATS":
                    ch.send(dict(self.snapshot(), type="STATS_REPLY"))
        except (OSError, ValueError):
            pass
        finally:
            ch.close()

    def snapshot(self):
        with self.cv:
            return {"policy": self.policy, "stats": dict(self.stats),
                    "workers": [{"id": w.id, "ip": w.ip, "alive": w.alive, "capacity": w.capacity,
                                 "speed": w.speed, "completed": w.completed,
                                 "assigned_total": w.assigned_total}
                                for w in self.workers.values()]}

    # ---------------- scheduling ----------------
    def _pick_locked(self):
        if not self.pending:
            return None
        alive = [w for w in self.workers.values() if w.alive]
        if not alive:
            return None
        task = self.pending[0]
        cands = [w for w in alive if w.id not in task.excluded]
        if not cands:                       # every worker already failed this task once
            task.excluded.clear()
            cands = alive
        if self.policy == "round_robin":
            cands.sort(key=lambda w: w.id)
            w = cands[self.rr % len(cands)]
            self.rr += 1
        else:                               # least_loaded: smallest estimated completion time
            free = [w for w in cands if len(w.active) < w.capacity]
            if not free:
                return None
            w = min(free, key=lambda x: ((len(x.active) + 1) / (x.capacity * x.speed),
                                         x.assigned_total))
        return self.pending.popleft(), w

    def _dispatch_loop(self):
        while True:
            with self.cv:
                pick = self._pick_locked()
                if pick is None:
                    self.cv.wait(0.2)
                    continue
                task, w = pick
                task.worker, task.assigned_at = w, time.time()
                task.attempt += 1
                w.active[task.id] = task
                w.assigned_total += 1
            try:
                w.ch.send({"type": "TASK", "task_id": task.id, "attempt": task.attempt,
                           "duration": task.duration, "data": task.data})
            except OSError:
                self._worker_failed(w, "send failed")

    def _monitor_loop(self):
        last_load = 0.0
        while True:
            time.sleep(0.25)
            now = time.time()
            with self.cv:
                dead = [w for w in self.workers.values()
                        if w.alive and now - w.last_hb > self.hb_timeout]
            for w in dead:
                self._worker_failed(w, "heartbeat timeout")
            failed_tasks = []
            with self.cv:
                for w in self.workers.values():
                    if not w.alive:
                        continue
                    for t in list(w.active.values()):
                        if now - t.assigned_at > self.task_timeout:
                            del w.active[t.id]
                            t.worker = None
                            t.excluded.add(w.id)
                            t.retries += 1
                            self.stats["timeouts"] += 1
                            if t.retries > self.max_retries:
                                t.done = True
                                self.stats["failed"] += 1
                                failed_tasks.append(t)
                            else:
                                self.stats["redistributed"] += 1
                                self.pending.appendleft(t)
                            log.warning("task %s timed out on %s (retry %d)", t.id, w.id, t.retries)
                if failed_tasks or self.pending:
                    self.cv.notify_all()
            for t in failed_tasks:
                self._send_client(t, {"type": "ERROR", "task_id": t.cid, "error": "max retries"})
            if now - last_load >= 1.0:
                last_load = now
                with self.cv:
                    load = {w.ip: {"id": w.id, "active": len(w.active), "capacity": w.capacity}
                            for w in self.workers.values() if w.alive}
                if load:
                    self.notify("LOAD", workers=load, pending=len(self.pending))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--policy", choices=["least_loaded", "round_robin"], default="least_loaded")
    ap.add_argument("--task-timeout", type=float, default=20.0)
    ap.add_argument("--hb-timeout", type=float, default=3.0)
    ap.add_argument("--max-retries", type=int, default=3)
    ap.add_argument("--token", default=AUTH_TOKEN)
    ap.add_argument("--block-strikes", type=int, default=3)
    ap.add_argument("--ctrl-host", default=None, help="SDN controller notification address")
    ap.add_argument("--ctrl-port", type=int, default=9999)
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    Scheduler(a).serve()


if __name__ == "__main__":
    main()
