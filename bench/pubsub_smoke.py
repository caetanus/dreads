#!/usr/bin/env python3
# 10 smoke tests: on ONE long-lived connection subscribe pattern smoke<i>:* then
# from a second connection publish the matching channel smoke<i>:hit and check
# the PUBLISH receiver count (must be 1). Proves subscribe+publish end to end.
import socket, sys

port = int(sys.argv[1]) if len(sys.argv) > 1 else 7001

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
        t = self._line(); k = t[:1]
        if k == b":": return int(t[1:])
        if k == b"+": return t[1:].decode()
        if k == b"-": return Exception(t[1:].decode())
        if k == b"$":
            n = int(t[1:])
            if n < 0: return None
            while len(self.buf) < n + 2: self.buf += self.s.recv(1 << 16)
            d, self.buf = self.buf[:n], self.buf[n+2:]; return d
        if k == b"*": return [self.read() for _ in range(int(t[1:]))]
        raise ValueError(t)
    def cmd(self, *a):
        self.s.sendall(enc(*a)); return self.read()
    def send(self, *a):
        self.s.sendall(enc(*a))
    def drain(self, timeout=0.3):
        self.s.settimeout(timeout)
        try:
            while True:
                if not self.s.recv(1 << 16): break
        except socket.timeout: pass
        self.s.settimeout(None); self.buf = b""

sub = Client()
pub = Client()
ok = 0
for i in range(10):
    p = f"smoke{i}:*"
    sub.send("PSUBSCRIBE", p)
    sub.drain(0.3)  # consume the subscribe confirmation
    r = pub.cmd("PUBLISH", f"smoke{i}:hit", "x")
    status = "OK" if r == 1 else "FAIL"
    print(f"  [{status}] subscribe {p} -> publish smoke{i}:hit = {r} receivers")
    ok += (r == 1)
print(f"RESULT {ok}/10 passed")
