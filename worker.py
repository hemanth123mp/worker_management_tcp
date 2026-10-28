"""TCP worker: register, heartbeat, run tasks one at a time, report completion."""

import argparse
import json
import socket
import threading
import time

PORT = 5000


def send_json(connection, message, send_lock):
    with send_lock:
        connection.sendall((json.dumps(message) + "\n").encode())


def session(args, simulate_failure):
    """One TCP session with the server. Returns True if it ended in a simulated crash."""
    with socket.create_connection((args.server_ip, PORT), timeout=2) as connection:
        connection.settimeout(None)               # timeout was only for connecting
        reader = connection.makefile("r")         # ONE reader for the whole session
        send_lock = threading.Lock()
        stop, crashed = threading.Event(), threading.Event()
        received = 0

        def crash():
            print(f"{args.worker_name}: simulated crash", flush=True)
            crashed.set()
            try:
                connection.shutdown(socket.SHUT_RDWR)       # server sees the connection close
            except OSError:
                pass

        send_json(connection, {"type": "register", "worker": args.worker_name, "speed": args.speed}, send_lock)
        print(json.loads(reader.readline()), flush=True)
        started = time.time()

        def heartbeat():
            while not stop.is_set():
                if simulate_failure and args.fail_after and time.time() - started >= args.fail_after:
                    crash()
                    return
                try:
                    send_json(connection, {"type": "heartbeat", "worker": args.worker_name}, send_lock)
                except OSError:
                    return
                stop.wait(1)

        threading.Thread(target=heartbeat, daemon=True).start()
        try:
            for line in reader:
                message = json.loads(line)
                if message.get("type") != "task":
                    continue
                task = message["task"]
                work_time = task["delay"] / args.speed * args.slowdown
                print(f"{args.worker_name}: running {task['task_id']} for {work_time:.1f}s", flush=True)
                received += 1
                if simulate_failure and args.fail_on_task == received:
                    crashed.wait(work_time / 2)                  # crash half-way through this task
                    crash()
                    break
                if crashed.wait(work_time):                      # a crash interrupts the task
                    break
                send_json(connection, {"type": "done", "worker": args.worker_name,
                                       "task_id": task["task_id"], "attempt": task["attempt"]}, send_lock)
                print(f"{args.worker_name}: done {task['task_id']}", flush=True)
        except (OSError, ValueError):
            pass
        finally:
            stop.set()
        return crashed.is_set()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("server_ip")
    parser.add_argument("worker_name")
    parser.add_argument("--speed", type=float, default=1.0, help="task time = delay / speed")
    parser.add_argument("--fail-after", type=float, help="simulate a crash after N seconds")
    parser.add_argument("--fail-on-task", type=int, help="simulate a crash half-way through the Nth task")
    parser.add_argument("--restart-after", type=float, help="after the crash, reconnect N seconds later")
    parser.add_argument("--slowdown", type=float, default=1.0, help="test only: run tasks N times slower than reported")
    args = parser.parse_args()

    crashed_before = False
    while True:
        try:
            crashed = session(args, simulate_failure=not crashed_before)
        except OSError as error:
            if not crashed_before:
                print(f"{args.worker_name}: cannot connect: {error}", flush=True)
                return
            time.sleep(1)                         # restarting worker: keep trying (SDN quarantine)
            continue
        if not crashed or args.restart_after is None:
            return
        crashed_before = True
        print(f"{args.worker_name}: restarting in {args.restart_after}s", flush=True)
        time.sleep(args.restart_after)


if __name__ == "__main__":
    main()
