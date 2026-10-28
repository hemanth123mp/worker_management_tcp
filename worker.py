"""TCP worker: register, heartbeat, receive one task, and report completion."""

import argparse
import json
import socket
import threading
import time

PORT = 5000


def send_json(connection, message, send_lock):
    with send_lock:
        connection.sendall((json.dumps(message) + "\n").encode())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("server_ip")
    parser.add_argument("worker_name")
    parser.add_argument("--fail-after", type=float, help="stop this worker after N seconds for the demo")
    args = parser.parse_args()

    with socket.create_connection((args.server_ip, PORT), timeout=5) as connection:
        send_lock = threading.Lock()
        my_ip = connection.getsockname()[0]
        send_json(connection, {"type": "register", "worker": args.worker_name, "ip": my_ip}, send_lock)
        print(json.loads(connection.makefile("r").readline()), flush=True)
        started = time.time()
        alive = True

        def heartbeat():
            nonlocal alive
            while alive:
                if args.fail_after and time.time() - started >= args.fail_after:
                    print(f"{args.worker_name}: simulated failure", flush=True)
                    alive = False
                    connection.close()
                    return
                try:
                    send_json(connection, {"type": "heartbeat", "worker": args.worker_name}, send_lock)
                except OSError:
                    return
                time.sleep(1)

        threading.Thread(target=heartbeat, daemon=True).start()
        with connection.makefile("r") as reader:
            for line in reader:
                message = json.loads(line)
                if message.get("type") != "task":
                    continue
                task = message["task"]
                print(f"{args.worker_name}: running {task['task_id']} for {task['delay']} seconds", flush=True)
                time.sleep(task["delay"])
                if alive:
                    send_json(connection, {"type": "done", "worker": args.worker_name,
                                           "task_id": task["task_id"], "attempt": task["attempt"]}, send_lock)
                    print(f"{args.worker_name}: done {task['task_id']}", flush=True)


if __name__ == "__main__":
    main()
