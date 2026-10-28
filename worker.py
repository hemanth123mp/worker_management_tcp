#!/usr/bin/env python3
"""Worker node: registers with the scheduler over TCP, executes tasks, returns results.

A task is simulated as work lasting duration/speed seconds. `capacity` is the
number of tasks the worker executes concurrently; extra tasks wait in a local queue.
"""
import argparse
import hashlib
import logging
import queue
import socket
import threading
import time

from common import AUTH_TOKEN, DEFAULT_PORT, Channel

log = logging.getLogger("worker")


def serve(a):
    sock = socket.create_connection((a.server, a.port), timeout=5)
    sock.settimeout(None)
    ch = Channel(sock)
    ch.send({"type": "REGISTER", "worker_id": a.id, "capacity": a.capacity,
             "speed": a.speed, "auth": a.token})
    reply = ch.recv()
    if not reply or reply.get("type") != "REGISTERED":
        raise ConnectionError("registration rejected: %s" % reply)
    log.info("registered as %s (capacity=%d speed=%.2f)", a.id, a.capacity, a.speed)

    q = queue.Queue()
    stop = threading.Event()
    busy = [0]
    blk = threading.Lock()

    def executor():
        while not stop.is_set():
            try:
                t = q.get(timeout=0.5)
            except queue.Empty:
                continue
            with blk:
                busy[0] += 1
            t0 = time.time()
            time.sleep(float(t.get("duration", 0.1)) / a.speed)
            digest = hashlib.sha1(t.get("data", "").encode()).hexdigest()[:12]
            try:
                ch.send({"type": "RESULT", "task_id": t["task_id"], "attempt": t["attempt"],
                         "worker_id": a.id, "exec_time": round(time.time() - t0, 4),
                         "digest": digest})
            except OSError:
                stop.set()
                ch.close()
            finally:
                with blk:
                    busy[0] -= 1

    def heartbeat():
        while not stop.is_set():
            try:
                ch.send({"type": "HEARTBEAT", "worker_id": a.id, "load": q.qsize() + busy[0]})
            except OSError:
                stop.set()
                ch.close()
                return
            stop.wait(a.hb_interval)

    for _ in range(a.capacity):
        threading.Thread(target=executor, daemon=True).start()
    threading.Thread(target=heartbeat, daemon=True).start()
    try:
        while not stop.is_set():
            m = ch.recv()
            if m is None:
                break
            if m.get("type") == "TASK":
                q.put(m)
            elif m.get("type") == "SHUTDOWN":
                break
    finally:
        stop.set()
        ch.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--server", default="10.0.0.1")
    ap.add_argument("--port", type=int, default=DEFAULT_PORT)
    ap.add_argument("--capacity", type=int, default=2)
    ap.add_argument("--speed", type=float, default=1.0)
    ap.add_argument("--hb-interval", type=float, default=1.0)
    ap.add_argument("--token", default=AUTH_TOKEN)
    ap.add_argument("--reconnect", action="store_true", help="retry connection after failure")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    while True:
        try:
            serve(a)
        except (OSError, ConnectionError, ValueError) as e:
            log.warning("connection problem: %s", e)
        if not a.reconnect:
            break
        time.sleep(1.0)


if __name__ == "__main__":
    main()
