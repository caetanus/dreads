#!/usr/bin/env python3
# Deterministic single-process pubsub check with a PROPER buffered RESP reader
# (previous sloppy framing produced garbage). Subscribe N prefix patterns on one
# connection; on a second connection query PUBSUB DBGSTATS [numpat,ins,rem,drop]
# and publish matching channels, reading each reply exactly.
#   pubsub_check.py <port> <N>
import socket, sys

port, N = int(sys.argv[1]), int(sys.argv[2])

def enc(*a):
    b = ("*%d\r\n" % len(a)).encode()
    for x in a:
        xb = x.encode()
        b += ("$%d\r\n" % len(xb)).encode() + xb + b"\r\n"
    return b

class Client:
    def __init__(self):
        self.s = socket.create_connection(("127.0.0.1", port))
        self.s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = b""
    def _line(self):
        while b"\r\n" not in self.buf:
            self.buf += self.s.recv(1 << 16)
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line
    def read(self):
        t = self._line()
        k = t[:1]
        if k in (b":", b"+"):
            return int(t[1:]) if k == b":" else t[1:].decode()
        if k == b"-":
            return Exception(t[1:].decode())
        if k == b"$":
            n = int(t[1:])
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.s.recv(1 << 16)
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data
        if k == b"*":
            n = int(t[1:])
            return [self.read() for _ in range(n)]
        raise ValueError(f"bad reply {t!r}")
    def cmd(self, *a, nreplies=1):
        self.s.sendall(enc(*a))
        return [self.read() for _ in range(nreplies)][-1]
    def send(self, *a):
        self.s.sendall(enc(*a))
    def drain(self, timeout=2.0):
        self.s.settimeout(timeout)
        try:
            while True:
                if not self.s.recv(1 << 16):
                    break
        except socket.timeout:
            pass
        self.s.settimeout(None)
        self.buf = b""

sub = Client()
sub.send("PSUBSCRIBE", *[f"zzz:{i}:*" for i in range(N)])
sub.drain()  # let the whole PSUBSCRIBE be processed; then ignore its confirmations

pub = Client()
print(f"  DBGSTATS [numpat,ins,rem,drop] = {pub.cmd('PUBSUB', 'DBGSTATS')}")
for i in ([0] if N == 1 else [0, N // 2, N - 1]):
    print(f"  zzz:{i}:x -> {pub.cmd('PUBLISH', f'zzz:{i}:x', 'x')} receivers")
