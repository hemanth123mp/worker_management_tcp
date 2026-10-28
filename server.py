"""TCP task server: queue, scheduler, worker failure recovery, SDN notification (all TCP)."""

import argparse
import csv
import json
import queue
import socket
import threading
import time
from pathlib import Path

PORT = 5000              # clients and workers talk to the server here (TCP)
SDN_PORT = 9999          # the SDN manager listens here (TCP)
HEARTBEAT_TIMEOUT = 3    # seconds of silence before a worker is declared failed
GRACE_SECONDS = 3        # extra time allowed on top of the expected task time

tasks = queue.Queue()
workers = {}
task_book = {}
lock = threading.RLock()
round_robin_index = 0
scheduler = "adaptive"
sdn_host = ""
sdn_events = queue.Queue()
metrics_file = None
metrics_writer = None


def send_json(connection, message, send_lock=None):
    data = (json.dumps(message) + "\n").encode()
    if send_lock:
        with send_lock:
            connection.sendall(data)
    else:
        connection.sendall(data)


def metric(event, task_id="", worker="", note=""):
    with lock:
        metrics_writer.writerow([round(time.time(), 3), event, task_id, worker, note])
        metrics_file.flush()
    print(f"{event}: task={task_id} worker={worker} {note}", flush=True)


# ---------------------------------------------------------------- SDN (TCP)
def notify_sdn(event, worker_ip, log=True):
    """Queue an event for the SDN manager. Never blocks the scheduler."""
    if not sdn_host:
        return
    sdn_events.put({"event": event, "worker_ip": worker_ip})
    if log:
        metric("sdn_notification", worker=worker_ip, note=event)


def sdn_sender():
    """Keep one TCP connection to the SDN manager and send queued events."""
    sock = None
    while True:
        line = (json.dumps(sdn_events.get()) + "\n").encode()
        for _ in range(2):                       # second try = reconnect
            try:
                if sock is None:
                    sock = socket.create_connection((sdn_host, SDN_PORT), timeout=2)
                sock.sendall(line)
                break
            except OSError:
                if sock:
                    sock.close()
                sock = None


# ---------------------------------------------------------------- scheduling
def choose_idle_worker():
    global round_robin_index
    if scheduler == "round_robin":               # baseline: rotate over all workers
        names = sorted(workers)
        for step in range(len(names)):
            worker = workers[names[(round_robin_index + step) % len(names)]]
            if worker["state"] == "idle":
                round_robin_index = (round_robin_index + step + 1) % len(names)
                return worker
        return None
    idle = sorted((w for w in workers.values() if w["state"] == "idle"), key=lambda w: w["name"])
    if not idle:
        return None
    # adaptive: fastest idle worker, tie -> fewer completed tasks
    return max(idle, key=lambda w: (w["speed"], -w["completed"]))


def requeue_task(worker, reason):
    """Return the worker's running task to the queue. Returns the task id (or None)."""
    task_id = worker.get("current_task")
    task = task_book.get(task_id)
    if not task or task["status"] != "running":
        return None
    task["status"] = "queued"
    task["worker"] = ""
    task["attempt"] += 1
    worker["current_task"] = None
    tasks.put(task)
    metric("requeued", task_id, worker["name"], reason)
    return task_id


def fail_worker(worker, reason, block=True):
    """Mark a worker offline once, requeue its task, tell the SDN manager."""
    if worker["state"] == "offline":
        return
    worker["state"] = "offline"
    worker["stale_task"] = requeue_task(worker, reason)
    metric("worker_failed", worker=worker["name"], note=reason)
    if block and worker["ip"]:
        notify_sdn("worker_failed", worker["ip"])


def task_timeout(worker):
    """Task ran too long: requeue it elsewhere; the worker is 'slow' until it reports back."""
    task_id = worker["current_task"]
    requeue_task(worker, "task timeout")
    worker["state"] = "slow"
    worker["stale_task"] = task_id
    metric("task_timeout", task_id, worker["name"])


def dispatch():
    with lock:
        while not tasks.empty():
            worker = choose_idle_worker()
            if worker is None:
                return
            task = tasks.get_nowait()
            task["status"] = "running"
            task["worker"] = worker["name"]
            task["started_at"] = time.time()
            worker["state"] = "busy"
            worker["current_task"] = task["task_id"]
            worker["assigned"] += 1
            try:
                send_json(worker["connection"], {"type": "task", "task": task}, worker["send_lock"])
                metric("assigned", task["task_id"], worker["name"], f"attempt={task['attempt']}")
                notify_sdn("worker_busy", worker["ip"], log=False)
            except OSError:
                fail_worker(worker, "send failed")


def monitor_workers():
    while True:
        time.sleep(1)
        with lock:
            now = time.time()
            for worker in list(workers.values()):
                if worker["state"] == "offline":
                    continue
                if now - worker["last_seen"] > HEARTBEAT_TIMEOUT:
                    fail_worker(worker, "heartbeat lost")
                    continue
                task = task_book.get(worker.get("current_task"))
                if worker["state"] == "busy" and task:
                    allowed = task["delay"] / worker["speed"] + GRACE_SECONDS
                    if now - task["started_at"] > allowed:
                        task_timeout(worker)
        dispatch()


# ---------------------------------------------------------------- messages
def handle_message(connection, address, message):
    kind = message.get("type")
    if kind == "register":
        name = message.get("worker")
        if not name:
            return {"type": "error", "message": "worker name required"}
        speed = float(message.get("speed", 1.0))
        if speed <= 0:
            return {"type": "error", "message": "speed must be positive"}
        old = workers.get(name)
        if old and old["connection"] is not connection:
            fail_worker(old, "replaced by reconnect", block=False)
        workers[name] = {"name": name, "ip": address[0], "connection": connection,
                         "send_lock": threading.Lock(), "state": "idle", "current_task": None,
                         "stale_task": None, "speed": speed, "last_seen": time.time(),
                         "assigned": old["assigned"] if old else 0,
                         "completed": old["completed"] if old else 0}
        metric("registered", worker=name, note=f"{address[0]} speed={speed}")
        notify_sdn("worker_registered", address[0])
        return {"type": "registered", "worker": name}
    if kind == "heartbeat":
        worker = workers.get(message.get("worker"))
        if worker and worker["connection"] is connection:
            worker["last_seen"] = time.time()
            if worker["state"] == "offline":     # a hung worker woke up on the same connection
                worker["state"] = "slow" if worker["stale_task"] else "idle"
                metric("worker_recovered", worker=worker["name"], note="heartbeat resumed")
                notify_sdn("worker_registered", worker["ip"])
        return None
    if kind == "done":
        worker = workers.get(message.get("worker"))
        task = task_book.get(message.get("task_id"))
        if (worker and worker["connection"] is connection and worker["state"] == "slow"
                and message.get("task_id") == worker["stale_task"]):
            worker["state"] = "idle"             # slow worker finished its old task; result ignored
            worker["stale_task"] = None
            metric("worker_recovered", worker=worker["name"], note="finished stale task")
            return None
        valid = (worker and task and worker["connection"] is connection and worker["state"] == "busy"
                 and worker["current_task"] == task["task_id"] and task["status"] == "running"
                 and task["worker"] == worker["name"] and message.get("attempt") == task["attempt"])
        if valid:
            task["status"] = "done"
            task["finished_at"] = time.time()
            worker["state"] = "idle"
            worker["current_task"] = None
            worker["completed"] += 1
            metric("done", task["task_id"], worker["name"], f"attempt={task['attempt']}")
            notify_sdn("worker_idle", worker["ip"], log=False)
        return None
    if kind == "submit":
        delay = float(message.get("delay", 0))
        task_id = message.get("task_id")
        if delay <= 0 or not task_id or task_id in task_book:
            return {"type": "error", "message": "unique task id and positive delay required"}
        task = {"task_id": task_id, "delay": delay, "status": "queued", "worker": "", "attempt": 1}
        task_book[task_id] = task
        tasks.put(task)
        metric("queued", task_id, note=f"delay={delay}")
        return {"type": "queued", "task_id": task_id}
    if kind == "status":
        keys = ("name", "state", "speed", "assigned", "completed")
        return {"type": "status", "workers": [{key: w[key] for key in keys} for w in workers.values()]}
    return {"type": "error", "message": "unknown message"}


def client_thread(connection, address):
    worker_name = None
    try:
        with connection, connection.makefile("r") as reader:
            for line in reader:
                try:
                    message = json.loads(line)
                    with lock:
                        response = handle_message(connection, address, message)
                        worker_name = message.get("worker", worker_name)
                except (ValueError, TypeError, AttributeError) as error:
                    response = {"type": "error", "message": f"bad message: {error}"}
                if response:
                    send_json(connection, response)
                dispatch()
    except OSError:
        pass
    finally:
        with lock:
            worker = workers.get(worker_name)
            if worker and worker["connection"] is connection:
                fail_worker(worker, "connection closed")
        dispatch()


def main():
    global scheduler, sdn_host, metrics_file, metrics_writer
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", choices=["adaptive", "round_robin"], default="adaptive")
    parser.add_argument("--sdn-host", default="10.0.0.254", help="SDN manager IP ('' disables SDN events)")
    parser.add_argument("--metrics", default="metrics.csv")
    args = parser.parse_args()
    scheduler, sdn_host = args.scheduler, args.sdn_host
    metrics_file = Path(args.metrics).open("w", newline="")
    metrics_writer = csv.writer(metrics_file)
    metrics_writer.writerow(["timestamp", "event", "task_id", "worker", "note"])

    threading.Thread(target=monitor_workers, daemon=True).start()
    if sdn_host:
        threading.Thread(target=sdn_sender, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", PORT))
        server.listen()
        print(f"Server listening on port {PORT}; scheduler={scheduler}", flush=True)
        while True:
            connection, address = server.accept()
            threading.Thread(target=client_thread, args=(connection, address), daemon=True).start()


if __name__ == "__main__":
    main()
