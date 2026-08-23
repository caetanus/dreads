#!/usr/bin/env python3
# Empirical malformed-RecordBatch-v2 fuzz for the fresh decodeV2Batch path,
# against a live -release build (bounds checks OFF). Sends Produce v3 requests
# whose record set is a deliberately-broken v2 batch; dreads must stay ALIVE and
# in-sync (a valid ApiVersions after each still gets a coherent reply), and must
# reject the garbage (no crash/hang/OOB).
import socket, struct, sys

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 19092
HOST = "127.0.0.1"

# CRC-32C (Castagnoli), reflected poly 0x82F63B78 — what a v2 batch carries.
_TBL = []
for i in range(256):
    c = i
    for _ in range(8):
        c = (0x82F63B78 ^ (c >> 1)) if (c & 1) else (c >> 1)
    _TBL.append(c & 0xFFFFFFFF)
def crc32c(data):
    c = 0xFFFFFFFF
    for b in data:
        c = _TBL[(c ^ b) & 0xFF] ^ (c >> 8)
    return c ^ 0xFFFFFFFF

def uvarint(v):
    out = b""
    while v >= 0x80:
        out += bytes([(v & 0x7F) | 0x80]); v >>= 7
    return out + bytes([v & 0x7F])
def svar(v):  # zigzag-encoded signed varint (Python's arithmetic >> handles negatives)
    u = (v << 1) ^ (v >> 63)
    return uvarint(u & ((1 << 64) - 1))

def rec(ts_delta, off_delta, key, val, headers=b"\x00"):
    body = b"\x00"  # attributes
    body += svar(ts_delta) + svar(off_delta)
    body += (svar(-1) if key is None else svar(len(key)) + key)
    body += (svar(-1) if val is None else svar(len(val)) + val)
    body += headers
    return svar(len(body)) + body

def batch(records_bytes, count, attrs=0, corrupt_crc=False, magic=2, force_batchlen=None):
    after_crc = struct.pack(">h", attrs) + struct.pack(">i", max(count - 1, 0))  # attrs, lastOffsetDelta
    after_crc += struct.pack(">q", 0) + struct.pack(">q", 0)     # first/max ts
    after_crc += struct.pack(">q", -1) + struct.pack(">h", -1) + struct.pack(">i", -1)  # pid, pepoch, baseseq
    after_crc += struct.pack(">i", count) + records_bytes
    crc = crc32c(after_crc)
    if corrupt_crc:
        crc ^= 0xFFFFFFFF
    hdr = struct.pack(">q", 0)  # baseOffset
    tail = struct.pack(">i", 0) + bytes([magic & 0xFF]) + struct.pack(">I", crc & 0xFFFFFFFF) + after_crc
    blen = force_batchlen if force_batchlen is not None else len(tail)
    return hdr + struct.pack(">i", blen) + tail

def produce_v3(topic, part, recordset, corr=1):
    b = struct.pack(">hhi", 0, 3, corr) + struct.pack(">h", 1) + b"f"  # hdr
    b += struct.pack(">h", -1)                    # transactional_id = null
    b += struct.pack(">hi", 1, 1000)              # acks=1, timeout
    b += struct.pack(">i", 1)                     # 1 topic
    tb = topic.encode(); b += struct.pack(">h", len(tb)) + tb
    b += struct.pack(">i", 1)                     # 1 partition
    b += struct.pack(">i", part) + struct.pack(">i", len(recordset)) + recordset
    return struct.pack(">i", len(b)) + b

def apiversions(corr=0xABCD):
    b = struct.pack(">hhi", 18, 0, corr) + struct.pack(">h", 5) + b"probe"
    return struct.pack(">i", len(b)) + b

def read_reply(s, t=2.0):
    s.settimeout(t)
    try:
        ln = s.recv(4)
        if len(ln) < 4: return None
        (n,) = struct.unpack(">i", ln)
        if n < 0 or n > 10_000_000: return b"BADLEN"
        buf = b""
        while len(buf) < n:
            c = s.recv(n - len(buf))
            if not c: break
            buf += c
        return buf
    except socket.timeout:
        return None

def cases():
    good = rec(0, 0, b"k", b"v")
    out = []
    out.append(("valid-1rec", batch(good, 1)))
    out.append(("valid-3rec", batch(rec(0,0,b"a",b"1")+rec(1,1,b"b",b"2")+rec(2,2,b"c",b"3"), 3)))
    out.append(("bad-crc", batch(good, 1, corrupt_crc=True)))
    out.append(("compressed-attr", batch(good, 1, attrs=0x02)))
    out.append(("count-huge", batch(good, 0x7FFFFFFF)))
    out.append(("count-negative", batch(good, -1)))
    out.append(("reclen-huge", batch(svar(0x7FFFFFFF) + b"\x00", 1)))
    out.append(("key-len-huge", batch(svar(20) + b"\x00" + svar(0) + svar(0x7FFFFFFF) + b"xx" + svar(-1) + b"\x00", 1)))
    out.append(("header-count-huge", batch(rec(0,0,b"k",b"v", headers=svar(0x7FFFFFFF)), 1)))
    out.append(("truncated-batch", batch(good, 1)[:30]))
    out.append(("batchlen-lies", batch(good, 1, force_batchlen=0x7FFFFFFF)))
    out.append(("too-short", struct.pack(">q", 0) + struct.pack(">i", 5) + b"\x00\x00\x00"))
    out.append(("magic-99", batch(good, 1, magic=99)))
    out.append(("empty-recordset", b""))
    return out

def main():
    alive_fails = 0; rejected = 0; accepted = 0
    for name, rs in cases():
        try:
            s = socket.socket(); s.settimeout(2.5); s.connect((HOST, PORT))
            s.sendall(produce_v3("v2fuzz_%s" % name.replace("-", ""), 0, rs))
            r = read_reply(s)
            # decode produce v3 response error code for partition 0 (best-effort)
            errc = None
            if r and len(r) >= 4:
                # [corr][throttle? no—v3 has topics then throttle at end]
                # response: [corr i32][topics: cnt][topic][parts:cnt][part][err i16][baseoff i64][logtime i64]...
                try:
                    p = 4; (tc,) = struct.unpack(">i", r[p:p+4]); p += 4
                    (tl,) = struct.unpack(">h", r[p:p+2]); p += 2 + tl
                    (pc,) = struct.unpack(">i", r[p:p+4]); p += 4
                    p += 4  # partition
                    (errc,) = struct.unpack(">h", r[p:p+2])
                except Exception:
                    errc = "?"
            if errc not in (0, None):
                rejected += 1
            elif errc == 0:
                accepted += 1
            s.close()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception:
            pass
        # liveness on a fresh conn
        try:
            p = socket.socket(); p.settimeout(2.5); p.connect((HOST, PORT))
            p.sendall(apiversions()); rr = read_reply(p); p.close()
            if not (rr and len(rr) >= 8 and rr != b"BADLEN"):
                alive_fails += 1
                print("  *** LIVENESS FAIL after case: %s" % name)
        except Exception as e:
            alive_fails += 1
            print("  *** LIVENESS PROBE ERR after %s: %s" % (name, e))
        print("  %-20s err=%s" % (name, errc))
    print("=" * 50)
    print("cases=%d rejected(err!=0)=%d accepted(err=0)=%d liveness_fails=%d" %
          (len(cases()), rejected, accepted, alive_fails))
    print("RESULT:", "PASS (alive through all malformed v2 batches)" if alive_fails == 0 else "*** INVESTIGATE ***")

if __name__ == "__main__":
    main()
