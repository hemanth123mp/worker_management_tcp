"""Small TCP task queue with worker failure recovery."""

import argparse
import csv
import json
import queue
import socket
import threading
import time
from pathlib import Path

PORT = 5000
HEARTBEAT_TIMEOUT = 3
GRACE_SECONDS = 3

tasks = queue.Queue()
workers = {}
task_book = {}
lock = threading.RLock()
round_robin_index = 0
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


def notify_sdn(worker_ip):
    """The SDN manager reads this small file and blocks the failed worker IP."""
    with Path("sdn_events.jsonl").open("a") as event_file:
        event_file.write(json.dumps({"event": "block_failed_worker", "worker_ip": worker_ip}) + "\n")
    metric("sdn_notification", worker=worker_ip, note="failed worker blocked")


def choose_idle_worker():
    global round_robin_index
    idle = [worker for worker in workers.values() if worker["state"] == "idle"]
    if not idle:
        return None
    idle.sort(key=lambda worker: worker["name"])
    if scheduler == "round_robin":
        selected = idle[round_robin_index % len(idle)]
        round_robin_index += 1
        return selected
    return min(idle, key=lambda worker: (worker["completed"], worker["assigned"], worker["name"]))


def requeue_task(worker, reason):
    task_id = worker.get("current_task")
    if not task_id:
        return
    task = task_book.get(task_id)
    if not task or task["status"] != "running":
        return
    task["status"] = "queued"
    task["worker"] = ""
    task["attempt"] += 1
    worker["current_task"] = None
    tasks.put(task)
    metric("requeued", task_id, worker["name"], reason)


def fail_worker(worker, reason):
    """Mark a worker offline once and return its unfinished task to the queue."""
    if worker["state"] == "offline":
        return
    worker["state"] = "offline"
    requeue_task(worker, reason)
    metric("worker_failed", worker=worker["name"], note=reason)
    if worker["ip"]:
        notify_sdn(worker["ip"])


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
                task_id = worker.get("current_task")
                task = task_book.get(task_id)
                missed_heartbeat = now - worker["last_seen"] > HEARTBEAT_TIMEOUT
                task_timeout = task and now - task["started_at"] > task["delay"] + GRACE_SECONDS
                if missed_heartbeat or task_timeout:
                    fail_worker(worker, "heartbeat lost" if missed_heartbeat else "task timeout")
        dispatch()


def handle_message(connection, address, message):
    kind = message.get("type")
    if kind == "register":
        name = message.get("worker")
        if not name:
            return {"type": "error", "message": "worker name required"}
        old = workers.get(name)
        if old and old["connection"] is not connection:
            fail_worker(old, "replaced by reconnect")
        workers[name] = {"name": name, "ip": address[0], "connection": connection,
                         "send_lock": threading.Lock(), "state": "idle", "current_task": None,
                         "last_seen": time.time(), "assigned": 0, "completed": 0}
        metric("registered", worker=name, note=address[0])
        return {"type": "registered", "worker": name}
    if kind == "heartbeat":
        worker = workers.get(message.get("worker"))
        if worker and worker["connection"] is connection:
            worker["last_seen"] = time.time()
        return None
    if kind == "done":
        worker = workers.get(message.get("worker"))
        task = task_book.get(message.get("task_id"))
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
        return {"type": "status", "workers": [{key: worker[key] for key in ("name", "state", "assigned", "completed")}
                                                   for worker in workers.values()]}
    return {"type": "error", "message": "unknown message"}


def client_thread(connection, address):
    worker_name = None
    try:
        with connection, connection.makefile("r") as reader:
            for line in reader:
                message = json.loads(line)
                with lock:
                    response = handle_message(connection, address, message)
                    worker_name = message.get("worker", worker_name)
                if response:
                    send_json(connection, response)
                dispatch()
    except (OSError, json.JSONDecodeError):
        pass
    finally:
        with lock:
            worker = workers.get(worker_name)
            if worker and worker["connection"] is connection:
                fail_worker(worker, "connection closed")
        dispatch()


def main():
    global scheduler, metrics_file, metrics_writer
    parser = argparse.ArgumentParser()
    parser.add_argument("--scheduler", choices=["adaptive", "round_robin"], default="adaptive")
    args = parser.parse_args()
    scheduler = args.scheduler
    metrics_file = Path("metrics.csv").open("w", newline="")
    metrics_writer = csv.writer(metrics_file)
    metrics_writer.writerow(["timestamp", "event", "task_id", "worker", "note"])
    Path("sdn_events.jsonl").write_text("")

    threading.Thread(target=monitor_workers, daemon=True).start()
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
