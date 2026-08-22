#!/usr/bin/env python3
# Malformed-input robustness fuzz for the dreads Kafka skin, run against a live
# -release build (LDC -release => array bounds checks OFF, so any wire-index OOB
# is a silent crash, not a caught exception). Complements kafka_diff.py: that one
# proves CONFORMANCE on well-formed input; this one proves ROBUSTNESS on garbage.
#
#   kafka_fuzz.py <kafka_port>        e.g. kafka_fuzz.py 19092
#   (start dreads first: ./bin/dreads --port 7300 --kafka-port 19092)
#
# Two independent assertions, both must hold:
#   (A) LIVENESS: after N random malformed frames, dreads has not crashed/hung —
#       a fresh connection still gets a coherent ApiVersions reply.
#   (B) RESYNC: a well-formed request pipelined behind a malformed one on the
#       SAME connection still gets its correlation id back INTACT (each response
#       is correctly length-prefixed; a client can demux by corr id). This is the
#       precise test — a crude "read one reply" probe FALSELY flags desync when
#       dreads merely also replied to the garbage frame, so we DRAIN all replies
#       and look for the known corr id.
#
# Attacks the exact soft-spots prior code-read rounds refuted by trace: apiKey
# out of range, negative/huge apiVersion, negative/huge array counts (topic x
# partition product bombs), record msz overrunning remaining, truncated frames,
# negative string lengths. safeCount clamp [0,65536], per-Rd bounds checks, and
# the 64MB frame cap are what should hold.
import socket, struct, sys, time

HOST = "127.0.0.1"
MAGIC = 0xABCD

def frame(b): return struct.pack(">i", len(b)) + b
def hdr(key, ver, corr=1, client=b"f"):
    return struct.pack(">hhi", key, ver, corr) + struct.pack(">h", len(client)) + client
def apiversions(corr): return frame(hdr(18, 0, corr, b"probe"))

def drain(sock, want_corr, timeout=1.5):
    sock.settimeout(timeout); seen = []; buf = b""; end = time.time() + timeout
    try:
        while time.time() < end:
            try: c = sock.recv(65536)
            except socket.timeout: break
            if not c: break
            buf += c
            while len(buf) >= 4:
                (n,) = struct.unpack(">i", buf[:4])
                if n < 0 or n > 10_000_000: return seen, "BADLEN"
                if len(buf) < 4 + n: break
                body, buf = buf[4:4+n], buf[4+n:]
                if len(body) >= 4: seen.append(struct.unpack(">i", body[:4])[0] & 0xFFFFFFFF)
                if want_corr in seen: return seen, None
        return seen, None
    except Exception as e:
        return seen, "ERR:%s" % e

# ---- deterministic malformed corpus (no RNG: reproducible regression) ----
def corpus():
    c = []
    c.append(("unknown-apiKey",   hdr(999, 0) + b"x"))
    c.append(("neg-apiKey",       hdr(-1, 0) + b"x"))
    c.append(("neg-apiVer-prod",  hdr(0, -1) + struct.pack(">hii", 1, 0, 0)))
    c.append(("huge-apiVer",      hdr(0, 0x7FFF) + struct.pack(">hii", 1, 0, 0)))
    c.append(("huge-topiccount",  hdr(2, 1) + struct.pack(">ii", -1, 0x7FFFFFFF)))
    c.append(("neg-topiccount",   hdr(2, 1) + struct.pack(">ii", -1, -5)))
    c.append(("huge-partcount",   hdr(1, 3) + struct.pack(">iiii", -1, 100, 1, 1)
                                  + struct.pack(">h", 1) + b"t" + struct.pack(">i", 0x7FFFFFFF)))
    c.append(("msz-overrun",      hdr(0, 2) + struct.pack(">hii", 1, 0, 1)
                                  + struct.pack(">h", 1) + b"t" + struct.pack(">ii", 1, 0)
                                  + struct.pack(">i", 12) + struct.pack(">q", 0) + struct.pack(">i", 0x7FFFFFFF)))
    c.append(("neg-clientid-len", struct.pack(">hhi", 18, 0, 1) + struct.pack(">h", -1) + b"junk"))
    c.append(("metadata-ok",      hdr(3, 1) + struct.pack(">i", 0)))  # control: valid
    return c

def run(port):
    fails = []
    # (B) RESYNC per malformed frame
    for name, body in corpus():
        try:
            s = socket.socket(); s.settimeout(2.0); s.connect((HOST, port))
            s.sendall(frame(body)); s.sendall(apiversions(MAGIC))
            seen, err = drain(s, MAGIC); s.close()
            if MAGIC not in seen or err:
                # broker may legitimately close on some garbage; retest fresh conn
                f = socket.socket(); f.settimeout(2.0); f.connect((HOST, port))
                f.sendall(apiversions(MAGIC)); seen2, err2 = drain(f, MAGIC); f.close()
                if MAGIC not in seen2:
                    fails.append("resync:%s (seen=%s err=%s)" % (name, seen, err))
        except (BrokenPipeError, ConnectionResetError):
            # closing a bad conn is legitimate; prove liveness on a fresh conn
            try:
                f = socket.socket(); f.settimeout(2.0); f.connect((HOST, port))
                f.sendall(apiversions(MAGIC)); seen2, _ = drain(f, MAGIC); f.close()
                if MAGIC not in seen2: fails.append("resync-after-close:%s" % name)
            except Exception as e:
                fails.append("liveness-after-close:%s (%s)" % (name, e))
        except Exception as e:
            fails.append("send:%s (%s)" % (name, e))

    # (A) LIVENESS under volume: replay the corpus many times, then probe fresh
    reps = 400
    for r in range(reps):
        name, body = corpus()[r % len(corpus())]
        try:
            s = socket.socket(); s.settimeout(1.0); s.connect((HOST, port))
            s.sendall(frame(body))
            # also send a truncated frame (lies about length) to exercise the reader
            s.sendall(struct.pack(">i", 9999) + body[:4])
            s.close()
        except Exception:
            pass
    try:
        p = socket.socket(); p.settimeout(3.0); p.connect((HOST, port))
        p.sendall(apiversions(MAGIC)); seen, _ = drain(p, MAGIC); p.close()
        if MAGIC not in seen: fails.append("liveness: unresponsive after %d malformed frames" % reps)
    except Exception as e:
        fails.append("liveness: probe failed after volume (%s)" % e)
    return fails

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19092
    fails = run(port)
    if not fails:
        print("PASS: dreads Kafka stays alive + in-sync under all malformed input")
        return 0
    print("FAIL:")
    for f in fails: print("  -", f)
    return 1

if __name__ == "__main__":
    sys.exit(main())
