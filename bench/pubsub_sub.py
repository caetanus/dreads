#!/usr/bin/env python3
# Holds one connection subscribed to P non-matching patterns, then idles and
# drains. Registers pattern-match load on the server without any delivery, so a
# PUBLISH to a non-matching channel isolates the matching cost.
#   pubsub_sub.py <host> <port> <npat> <mode>
# mode picks the pattern SHAPE, which decides how dreads' header index behaves:
#   prefix      zzz:{i}:*     distinct headers -> dreads probes O(len), flat  (dreads wins)
#   suffix      *:tag{i}      leading star     -> dreads FALLBACK O(P)         (tie w/ Valkey)
#   sameheader  shared:*:t{i} one shared header -> all P are candidates, O(P)  (neither good)
import socket, sys

host, port, npat, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
s = socket.create_connection((host, port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

def pat(i):
    if mode == "prefix":     return f"zzz:{i}:*"
    if mode == "suffix":     return f"*:tag{i}"
    if mode == "sameheader": return f"shared:*:t{i}"
    raise SystemExit("bad mode")

if npat > 0:
    args = ["PSUBSCRIBE"] + [pat(i) for i in range(npat)]
    buf = bytearray(f"*{len(args)}\r\n".encode())
    for a in args:
        ab = a.encode()
        buf += f"${len(ab)}\r\n".encode() + ab + b"\r\n"
    s.sendall(buf)

# Drain until idle (registration is done once the server stops replying). Do not
# wait for exactly npat confirmations: dreads may drop them past its queue cap.
s.settimeout(1.0)
try:
    while True:
        if not s.recv(1 << 16):
            break
except socket.timeout:
    pass

print("READY", flush=True)

# Idle: keep the connection (and its P patterns) alive, discarding anything.
s.settimeout(None)
try:
    while True:
        if not s.recv(1 << 16):
            break
except Exception:
    pass
