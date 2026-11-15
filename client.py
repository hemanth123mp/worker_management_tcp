# sends delayed tasks to the server over one tcp connection, then exits

import argparse
import json
import socket
import time

PORT = 5000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("server_ip")
    parser.add_argument("client_name")
    parser.add_argument("delay", type=float, help="task length in seconds")
    parser.add_argument("--tasks", type=int, default=1, help="how many tasks to send")
    parser.add_argument("--interval", type=float, default=0.0, help="seconds between tasks")
    parser.add_argument("--start-delay", type=float, default=0.0, help="wait before the first task")
    args = parser.parse_args()

    time.sleep(args.start_delay)     # used to stagger the clients
    with socket.create_connection((args.server_ip, PORT), timeout=5) as conn:
        reader = conn.makefile("r")
        for n in range(1, args.tasks + 1):
            # id has to be unique, the server rejects repeats
            msg = {"type": "submit", "task_id": f"{args.client_name}-t{n}", "delay": args.delay}
            conn.sendall((json.dumps(msg) + "\n").encode())
            print(reader.readline().strip(), flush=True)
            if n < args.tasks:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
