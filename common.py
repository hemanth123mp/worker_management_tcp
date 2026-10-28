"""Shared helpers: newline-delimited JSON framing over raw TCP sockets."""
import json
import socket
import threading

DEFAULT_PORT = 5000
AUTH_TOKEN = "jackfruit-secret"
MAX_LINE = 8 * 1024 * 1024


class Channel:
    """Thin wrapper around a connected TCP socket.

    Protocol: one JSON object per line ("\n" terminated). send() is thread-safe;
    recv() must be used by a single reader thread.
    """

    def __init__(self, sock):
        self.sock = sock
        self.buf = b""
        self.wlock = threading.Lock()
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError:
            pass

    def send(self, obj):
        data = (json.dumps(obj, separators=(",", ":")) + "\n").encode()
        with self.wlock:
            self.sock.sendall(data)

    def recv(self):
        """Return next message (dict) or None on EOF. Raises OSError/ValueError."""
        while b"\n" not in self.buf:
            if len(self.buf) > MAX_LINE:
                raise ValueError("message too large")
            chunk = self.sock.recv(65536)
            if not chunk:
                return None
            self.buf += chunk
        line, self.buf = self.buf.split(b"\n", 1)
        return json.loads(line.decode())

    def close(self):
        for fn in (lambda: self.sock.shutdown(socket.SHUT_RDWR), self.sock.close):
            try:
                fn()
            except OSError:
                pass


def percentile(values, p):
    if not values:
        return 0.0
    v = sorted(values)
    k = (len(v) - 1) * p / 100.0
    lo, hi = int(k), min(int(k) + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (k - lo)


def jain_index(xs):
    xs = [x for x in xs if x is not None]
    if not xs or sum(x * x for x in xs) == 0:
        return 0.0
    return (sum(xs) ** 2) / (len(xs) * sum(x * x for x in xs))
