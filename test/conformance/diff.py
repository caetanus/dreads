#!/usr/bin/env python3
# Differential (inverted-blackbox) conformance test: replay command sequences
# over RESP against BOTH dreads and a reference Valkey, and diff the raw replies.
# Valkey is the oracle — any mismatch that isn't a documented drift (DRIFT.md)
# is a real bug. Each group runs on a freshly FLUSHALL'd db on both servers.
#
#   diff.py <dreads_port> <valkey_port>
import socket, sys

DREADS, VALKEY = int(sys.argv[1]), int(sys.argv[2])


def conn(port):
    s = socket.create_connection(("127.0.0.1", port))
    s.settimeout(5)
    return s


def send(s, args):
    req = "*%d\r\n" % len(args) + "".join(
        "$%d\r\n%s\r\n" % (len(str(a)), a) for a in args)
    s.sendall(req.encode())


class Reader:
    def __init__(self, s):
        self.s = s
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            self.buf += self.s.recv(65536)
        i = self.buf.index(b"\r\n")
        line, self.buf = self.buf[:i], self.buf[i + 2:]
        return line

    def _take(self, n):
        while len(self.buf) < n:
            self.buf += self.s.recv(65536)
        d, self.buf = self.buf[:n], self.buf[n:]
        return d

    # returns a normalized python repr of exactly one reply
    def reply(self):
        t = self._line()
        k = t[:1]
        if k in (b"+", b"-", b":"):
            return t.decode(errors="replace")
        if k == b"$":
            n = int(t[1:])
            if n < 0:
                return None
            d = self._take(n)
            self._take(2)
            return d.decode(errors="replace")
        if k in (b"*", b"~", b">"):
            n = int(t[1:])
            if n < 0:
                return None
            return [self.reply() for _ in range(n)]
        return t.decode(errors="replace")  # RESP3 types we don't special-case


# --- command groups: each is a list of commands run in sequence on a clean db ---
GROUPS = [
    # strings
    ["SET k v", "GET k", "APPEND k xyz", "STRLEN k", "GETRANGE k 0 2",
     "SETRANGE k 1 ZZ", "GET k", "GETDEL k", "EXISTS k"],
    ["SET n 10", "INCR n", "INCRBY n 5", "DECR n", "DECRBY n 3", "INCRBYFLOAT n 1.5"],
    ["SET k v EX 100", "TTL k", "PERSIST k", "TTL k", "EXPIRE k 50", "TTL k", "TYPE k"],
    ["MSET a 1 b 2 c 3", "MGET a b c nope", "DEL a b", "EXISTS a b c"],
    ["SET k 5", "SETNX k 9", "GET k", "SETNX k2 9", "GET k2", "SETEX k3 100 hi", "GET k3"],
    ["SET k hello", "SET k world GET", "GET k", "SET k x KEEPTTL"],
    # bit ops
    ["SETBIT b 7 1", "GET b", "GETBIT b 7", "BITCOUNT b", "BITPOS b 1"],
    # hashes
    ["HSET h f1 v1 f2 v2", "HGET h f1", "HMGET h f1 f2 f3", "HGETALL h", "HLEN h",
     "HDEL h f1", "HEXISTS h f1", "HEXISTS h f2", "HKEYS h", "HVALS h", "HSTRLEN h f2"],
    ["HSET h n 5", "HINCRBY h n 3", "HINCRBYFLOAT h n 1.5", "HSETNX h n 9", "HGET h n"],
    # lists
    ["RPUSH l a b c", "LPUSH l z", "LRANGE l 0 -1", "LLEN l", "LINDEX l 0",
     "LPOP l", "RPOP l", "LRANGE l 0 -1", "LSET l 0 Q", "LRANGE l 0 -1"],
    ["RPUSH l a b c a b", "LREM l 1 a", "LRANGE l 0 -1", "LINSERT l BEFORE b X", "LRANGE l 0 -1"],
    ["RPUSH l 1 2 3 4 5", "LTRIM l 1 3", "LRANGE l 0 -1", "LPOS l 3"],
    # sets
    ["SADD s a b c", "SCARD s", "SISMEMBER s a", "SISMEMBER s z", "SMEMBERS s",
     "SREM s a", "SCARD s", "SMISMEMBER s b z"],
    ["SADD s1 a b c d", "SADD s2 c d e", "SINTER s1 s2", "SUNION s1 s2",
     "SDIFF s1 s2", "SINTERCARD 2 s1 s2"],
    # sorted sets
    ["ZADD z 1 a 2 b 3 c", "ZCARD z", "ZSCORE z b", "ZRANK z c", "ZREVRANK z c",
     "ZRANGE z 0 -1", "ZRANGE z 0 -1 WITHSCORES", "ZRANGEBYSCORE z 1 2",
     "ZINCRBY z 5 a", "ZSCORE z a", "ZCOUNT z 1 3", "ZREM z b", "ZCARD z"],
    ["ZADD z 1 a 2 b 3 c", "ZPOPMIN z", "ZPOPMAX z", "ZRANGEBYLEX z - +", "ZMSCORE z c a"],
    # keys / generic
    ["SET a 1", "SET b 2", "RENAME a c", "GET c", "EXISTS a", "COPY c d", "GET d",
     "TYPE c", "OBJECT ENCODING c"],
    ["SET a 1", "EXPIRE a 100", "PERSIST a", "TTL a", "PTTL a", "EXPIRETIME a"],
    # errors / edge
    ["SET k v", "LPUSH k x", "INCR k", "GET k"],
    ["INCR nope", "GET nope", "STRLEN nope", "TYPE nope", "TTL nope", "LLEN nope"],
    ["HSET h f v", "GET h", "SADD h x"],
    # scan family (cursor semantics differ — expect drift; here just shape)
    ["MSET x1 1 x2 2 x3 3", "DBSIZE", "KEYS *"],
]

KNOWN_DRIFT_HINTS = (  # commands/areas DRIFT.md already flags as intentionally different
    "OBJECT ENCODING", "KEYS", "SCAN", "INFO",
)


def run_group(reader, sock, cmds):
    out = []
    for c in cmds:
        send(sock, c.split(" "))
        out.append(reader.reply())
    return out


def main():
    dr, vk = conn(DREADS), conn(VALKEY)
    rdr, rvk = Reader(dr), Reader(vk)
    total = matches = drifts = 0
    for cmds in GROUPS:
        send(dr, ["FLUSHALL"]); rdr.reply()
        send(vk, ["FLUSHALL"]); rvk.reply()
        od = run_group(rdr, dr, cmds)
        ov = run_group(rvk, vk, cmds)
        for c, a, b in zip(cmds, od, ov):
            total += 1
            if a == b:
                matches += 1
            else:
                drifts += 1
                tag = "  (known?)" if any(h in c for h in KNOWN_DRIFT_HINTS) else "  <-- CHECK"
                print("DRIFT: %-28s dreads=%r  valkey=%r%s" % (c, a, b, tag))
    print("\n%d commands: %d match, %d differ" % (total, matches, drifts))
    dr.close(); vk.close()
    sys.exit(1 if drifts else 0)


main()
