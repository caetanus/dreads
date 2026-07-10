#!/usr/bin/env python3
# Synchronous-replication throughput for Valkey/Redis via WAIT, which
# redis-benchmark cannot drive. Each connection pipelines PIPE SETs followed by
# one `WAIT <nrep> <timeout>` (block until nrep replicas ack), mirroring
# redis-benchmark -c CONNS -P PIPE but with every write durable on nrep replicas
# before it counts — the apples-to-apples for dreads' raft majority commit.
#
# Usage: wait_bench.py <port> <nrep> [conns] [pipe] [duration_s]
import socket, sys, threading, time

PORT = int(sys.argv[1])
NREP = int(sys.argv[2])
CONNS = int(sys.argv[3]) if len(sys.argv) > 3 else 50
PIPE = int(sys.argv[4]) if len(sys.argv) > 4 else 16
DUR = float(sys.argv[5]) if len(sys.argv) > 5 else 5.0


def resp(*a):
    return ("*%d\r\n" % len(a)) + "".join("$%d\r\n%s\r\n" % (len(str(x)), x) for x in a)


total = [0] * CONNS


def worker(idx):
    s = socket.create_connection(("127.0.0.1", PORT))
    s.settimeout(5)
    unit = ("".join(resp("SET", "wk%d_%d" % (idx, i), "v") for i in range(PIPE))
            + resp("WAIT", str(NREP), "1000")).encode()
    need = PIPE + 1  # PIPE +OK replies plus one WAIT integer reply
    t0 = time.time()
    n = 0
    while time.time() - t0 < DUR:
        s.sendall(unit)
        got = 0
        buf = b""
        while got < need:
            c = s.recv(65536)
            if not c:
                break
            buf += c
            got = buf.count(b"\r\n")
        n += PIPE
    total[idx] = n
    s.close()


ts = [threading.Thread(target=worker, args=(i,)) for i in range(CONNS)]
t0 = time.time()
for t in ts:
    t.start()
for t in ts:
    t.join()
print("%.0f" % (sum(total) / (time.time() - t0)))
