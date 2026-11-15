# task server: queue, scheduler, failure handling, tells the sdn manager. all tcp.

import argparse
import csv
import json
import queue
import socket
import threading
import time
from pathlib import Path

PORT = 5000              # clients and workers connect here
SDN_PORT = 9999          # sdn manager listens here
HEARTBEAT_TIMEOUT = 3    # silent for this long = worker is dead
GRACE_SECONDS = 3        # slack added to the expected task time

tasks = queue.Queue()    # waiting for a worker
workers = {}             # name -> worker dict
book = {}                # task_id -> task dict, every task ever submitted
lock = threading.RLock() # rlock: handle() holds it and metric() takes it again
rr_idx = 0               # where the round robin scan starts
sched = "adaptive"
sdn_host = ""
sdn_q = queue.Queue()    # events waiting to go to the sdn manager
mfile = None
mwriter = None


def send_json(conn, msg, slock=None):
    data = (json.dumps(msg) + "\n").encode()   # one json per line, the newline marks the end
    if slock:
        with slock:                            # 2 threads can write to the same worker socket
            conn.sendall(data)
    else:
        conn.sendall(data)


def metric(event, task_id="", worker="", note=""):
    with lock:
        mwriter.writerow([round(time.time(), 3), event, task_id, worker, note])
        mfile.flush()
    print(f"{event}: task={task_id} worker={worker} {note}", flush=True)


def notify_sdn(ev, ip, log=True):
    if not sdn_host:
        return
    sdn_q.put({"event": ev, "worker_ip": ip})  # just queue it, never wait on the network here
    if log:
        metric("sdn_notification", worker=ip, note=ev)


def sdn_sender():
    # background thread, keeps one tcp connection to the manager
    sock = None
    while True:
        line = (json.dumps(sdn_q.get()) + "\n").encode()
        for _ in range(2):             # 2nd try = reconnect
            try:
                if sock is None:
                    sock = socket.create_connection((sdn_host, SDN_PORT), timeout=2)
                sock.sendall(line)
                break
            except OSError:
                if sock:
                    sock.close()
                sock = None            # manager down -> event is dropped after 2 tries


def pick_worker():
    global rr_idx
    if sched == "round_robin":         # baseline: go around all workers in turn
        names = sorted(workers)
        for step in range(len(names)):
            w = workers[names[(rr_idx + step) % len(names)]]
            if w["state"] == "idle":
                rr_idx = (rr_idx + step + 1) % len(names)
                return w
        return None
    idle = sorted((w for w in workers.values() if w["state"] == "idle"), key=lambda w: w["name"])
    if not idle:
        return None
    # adaptive: fastest idle worker, tie -> fewer completed (minus makes "fewer" bigger)
    return max(idle, key=lambda w: (w["speed"], -w["completed"]))


def requeue(w, why):
    # put the worker's running task back in the queue, returns its id (or None)
    tid = w.get("current_task")
    t = book.get(tid)
    if not t or t["status"] != "running":
        return None
    t["status"] = "queued"
    t["worker"] = ""
    t["attempt"] += 1          # a late result from the old attempt gets ignored
    w["current_task"] = None
    tasks.put(t)
    metric("requeued", tid, w["name"], why)
    return tid


def fail_worker(w, why, block=True):
    if w["state"] == "offline":    # more than one check can catch the same failure, count it once
        return
    w["state"] = "offline"
    w["stale_task"] = requeue(w, why)
    metric("worker_failed", worker=w["name"], note=why)
    if block and w["ip"]:
        notify_sdn("worker_failed", w["ip"])


def task_timeout(w):
    # too slow but still alive: hand the task to someone else, park the worker. no sdn block.
    tid = w["current_task"]
    requeue(w, "task timeout")
    w["state"] = "slow"
    w["stale_task"] = tid
    metric("task_timeout", tid, w["name"])


def dispatch():
    with lock:
        while not tasks.empty():
            w = pick_worker()
            if w is None:
                return                 # nobody free, rest stays queued
            t = tasks.get_nowait()
            t["status"] = "running"
            t["worker"] = w["name"]
            t["started_at"] = time.time()
            w["state"] = "busy"
            w["current_task"] = t["task_id"]
            w["assigned"] += 1
            try:
                send_json(w["connection"], {"type": "task", "task": t}, w["send_lock"])
                metric("assigned", t["task_id"], w["name"], f"attempt={t['attempt']}")
                notify_sdn("worker_busy", w["ip"], log=False)
            except OSError:
                fail_worker(w, "send failed")   # this also requeues the task we just gave it


def watchdog():
    while True:
        time.sleep(1)
        with lock:
            now = time.time()
            for w in list(workers.values()):    # copy, the dict can change under us
                if w["state"] == "offline":
                    continue
                if now - w["last_seen"] > HEARTBEAT_TIMEOUT:
                    fail_worker(w, "heartbeat lost")
                    continue
                t = book.get(w.get("current_task"))
                if w["state"] == "busy" and t:
                    allowed = t["delay"] / w["speed"] + GRACE_SECONDS   # slower worker = longer
                    if now - t["started_at"] > allowed:
                        task_timeout(w)
        dispatch()


def handle(conn, addr, msg):
    kind = msg.get("type")

    if kind == "register":
        name = msg.get("worker")
        if not name:
            return {"type": "error", "message": "worker name required"}
        speed = float(msg.get("speed", 1.0))
        if speed <= 0:
            return {"type": "error", "message": "speed must be positive"}
        old = workers.get(name)
        if old and old["connection"] is not conn:        # same worker came back on a new socket
            fail_worker(old, "replaced by reconnect", block=False)
        workers[name] = {"name": name, "ip": addr[0], "connection": conn,
                         "send_lock": threading.Lock(), "state": "idle", "current_task": None,
                         "stale_task": None, "speed": speed, "last_seen": time.time(),
                         "assigned": old["assigned"] if old else 0,
                         "completed": old["completed"] if old else 0}
        metric("registered", worker=name, note=f"{addr[0]} speed={speed}")
        notify_sdn("worker_registered", addr[0])
        return {"type": "registered", "worker": name}

    if kind == "heartbeat":
        w = workers.get(msg.get("worker"))
        if w and w["connection"] is conn:                # ignore heartbeats from an old socket
            w["last_seen"] = time.time()
            if w["state"] == "offline":                  # hung worker woke up on the same socket
                w["state"] = "slow" if w["stale_task"] else "idle"
                metric("worker_recovered", worker=w["name"], note="heartbeat resumed")
                notify_sdn("worker_registered", w["ip"])
        return None

    if kind == "done":
        w = workers.get(msg.get("worker"))
        t = book.get(msg.get("task_id"))
        if (w and w["connection"] is conn and w["state"] == "slow"
                and msg.get("task_id") == w["stale_task"]):
            w["state"] = "idle"                          # it finished the old task, result is ignored
            w["stale_task"] = None
            metric("worker_recovered", worker=w["name"], note="finished stale task")
            return None
        # accept only if everything matches, incl. the attempt number
        ok = (w and t and w["connection"] is conn and w["state"] == "busy"
              and w["current_task"] == t["task_id"] and t["status"] == "running"
              and t["worker"] == w["name"] and msg.get("attempt") == t["attempt"])
        if ok:
            t["status"] = "done"
            t["finished_at"] = time.time()
            w["state"] = "idle"
            w["current_task"] = None
            w["completed"] += 1
            metric("done", t["task_id"], w["name"], f"attempt={t['attempt']}")
            notify_sdn("worker_idle", w["ip"], log=False)
        return None

    if kind == "submit":
        delay = float(msg.get("delay", 0))
        tid = msg.get("task_id")
        if delay <= 0 or not tid or tid in book:
            return {"type": "error", "message": "unique task id and positive delay required"}
        t = {"task_id": tid, "delay": delay, "status": "queued", "worker": "", "attempt": 1}
        book[tid] = t
        tasks.put(t)
        metric("queued", tid, note=f"delay={delay}")
        return {"type": "queued", "task_id": tid}

    if kind == "status":
        keys = ("name", "state", "speed", "assigned", "completed")
        return {"type": "status", "workers": [{k: w[k] for k in keys} for w in workers.values()]}

    return {"type": "error", "message": "unknown message"}


def conn_thread(conn, addr):
    wname = None               # only workers send a "worker" field
    try:
        with conn, conn.makefile("r") as reader:
            for line in reader:            # blocks until a full line, ends when peer disconnects
                try:
                    msg = json.loads(line)
                    with lock:
                        resp = handle(conn, addr, msg)
                        wname = msg.get("worker", wname)
                except (ValueError, TypeError, AttributeError) as err:
                    resp = {"type": "error", "message": f"bad message: {err}"}
                if resp:
                    send_json(conn, resp)
                dispatch()
    except OSError:
        pass
    finally:
        # connection ended = worker gone (if it didn't already reconnect on another socket)
        with lock:
            w = workers.get(wname)
            if w and w["connection"] is conn:
                fail_worker(w, "connection closed")
        dispatch()


def main():
    global sched, sdn_host, mfile, mwriter
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", choices=["adaptive", "round_robin"], default="adaptive")
    parser.add_argument("--sdn-host", default="10.0.0.254", help="sdn manager ip, '' turns sdn events off")
    parser.add_argument("--metrics", default="metrics.csv")
    args = parser.parse_args()
    sched, sdn_host = args.scheduler, args.sdn_host
    mfile = Path(args.metrics).open("w", newline="")
    mwriter = csv.writer(mfile)
    mwriter.writerow(["timestamp", "event", "task_id", "worker", "note"])

    threading.Thread(target=watchdog, daemon=True).start()
    if sdn_host:
        threading.Thread(target=sdn_sender, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)   # restart without waiting
        srv.bind(("0.0.0.0", PORT))
        srv.listen()
        print(f"Server listening on port {PORT}; scheduler={sched}", flush=True)
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=conn_thread, args=(conn, addr), daemon=True).start()


if __name__ == "__main__":
    main()
