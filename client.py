"""Submit one simple delayed task to the TCP server."""

import json
import socket
import sys

PORT = 5000


def main():
    if len(sys.argv) != 4:
        print("usage: python3 client.py SERVER_IP CLIENT_NAME DELAY")
        return
    server_ip, client_name, delay = sys.argv[1], sys.argv[2], float(sys.argv[3])
    message = {"type": "submit", "task_id": f"{client_name}-task", "delay": delay}
    with socket.create_connection((server_ip, PORT), timeout=5) as connection:
        connection.sendall((json.dumps(message) + "\n").encode())
        print(connection.makefile("r").readline().strip())


if __name__ == "__main__":
    main()
