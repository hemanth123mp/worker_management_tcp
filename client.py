"""Submit delayed tasks to the TCP server over one connection."""

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
    parser.add_argument("--tasks", type=int, default=1, help="how many tasks to submit")
    parser.add_argument("--interval", type=float, default=0.0, help="seconds between submissions")
    parser.add_argument("--start-delay", type=float, default=0.0, help="wait before the first submission")
    args = parser.parse_args()

    time.sleep(args.start_delay)
    with socket.create_connection((args.server_ip, PORT), timeout=5) as connection:
        reader = connection.makefile("r")
        for number in range(1, args.tasks + 1):
            message = {"type": "submit", "task_id": f"{args.client_name}-t{number}", "delay": args.delay}
            connection.sendall((json.dumps(message) + "\n").encode())
            print(reader.readline().strip(), flush=True)
            if number < args.tasks:
                time.sleep(args.interval)


if __name__ == "__main__":
    main()
