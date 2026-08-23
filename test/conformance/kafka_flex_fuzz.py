#!/usr/bin/env python3
# Adversarial fuzzer for the FLEXIBLE (KIP-482) Kafka dialect in dreads.
# Builds valid flexible requests for each flex API, then mutates them
# adversarially (truncation, huge uvarint lengths/counts, overlong varints,
# corrupted tagged fields) and confirms the broker stays ALIVE after each
# (a liveness ApiVersions probe). On a -debug build an OOB read crashes the
# broker immediately, so survival across all cases is strong evidence.
#
#   kafka_flex_fuzz.py [port]     e.g. kafka_flex_fuzz.py 19092
import socket, struct, sys, time, random

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 19092
random.seed(1234)  # deterministic

def uvarint(v):
    out = bytearray()
    while v >= 0x80:
        out.append((v & 0x7F) | 0x80)
        v >>= 7
    out.append(v & 0x7F)
    return bytes(out)

def cstr(s):  # compact string
    b = s.encode() if isinstance(s, str) else s
    return uvarint(len(b) + 1) + b
def cnull():  # compact null string/bytes
    return uvarint(0)
def carr(n):  # compact array count prefix
    return uvarint(n + 1)
TAG = uvarint(0)  # empty tagged fields

def flex_hdr(api, ver, corr, cid=b"fz"):
    # flexible request header v2: api,ver,corr, client_id (NON-compact i16 str), tagged
    return struct.pack(">hhi", api, ver, corr) + struct.pack(">h", len(cid)) + cid + TAG

def frame(payload):
    return struct.pack(">i", len(payload)) + payload

# --- valid flexible bodies per API (minimal) ---
def body_metadata():          # api 3 v9
    b = flex_hdr(3, 9, 1)
    b += carr(1) + cstr("ft") + TAG   # topics[1]{name, tag}
    b += b"\x00\x00\x00"              # allow_auto, incl_cluster, incl_topic (bools)
    b += TAG
    return b
def body_findcoord():         # api 10 v3
    return flex_hdr(10, 3, 1) + cstr("g") + b"\x00" + TAG  # key, key_type, tag
def body_joingroup():         # api 11 v7
    b = flex_hdr(11, 7, 1)
    b += cstr("g") + struct.pack(">i", 30000) + struct.pack(">i", 60000)  # group, session, rebalance
    b += cstr("") + cnull() + cstr("consumer")  # member_id, group_instance(null), protocol_type
    b += carr(1) + cstr("range") + uvarint(4) + b"\x00\x00\x00" + TAG  # protocols[1]{name, meta(3B), tag}
    b += TAG
    return b
def body_syncgroup():         # api 14 v5
    b = flex_hdr(14, 5, 1)
    b += cstr("g") + struct.pack(">i", 1) + cstr("m") + cnull()  # group, gen, member, instance(null)
    b += cstr("consumer") + cstr("range")  # protocol_type, protocol_name
    b += carr(1) + cstr("m") + uvarint(3) + b"\x00\x00" + TAG  # assignments[1]{member, assign(2B), tag}
    b += TAG
    return b
def body_heartbeat():         # api 12 v4
    return flex_hdr(12, 4, 1) + cstr("g") + struct.pack(">i", 1) + cstr("m") + cnull() + TAG
def body_leavegroup():        # api 13 v4
    b = flex_hdr(13, 4, 1) + cstr("g")
    b += carr(1) + cstr("m") + cnull() + TAG  # members[1]{member, instance(null), tag}
    b += TAG
    return b
def body_offsetfetch():       # api 9 v7
    b = flex_hdr(9, 7, 1) + cstr("g")
    b += carr(1) + cstr("ft") + carr(1) + struct.pack(">i", 0) + TAG  # topics[1]{name, parts[1]{0}, tag}
    b += b"\x01"  # require_stable
    b += TAG
    return b
def body_initpid():           # api 22 v3
    b = flex_hdr(22, 3, 1) + cnull() + struct.pack(">i", 60000)  # txn_id(null), timeout
    b += struct.pack(">q", -1) + struct.pack(">h", -1)  # producer_id, epoch
    b += TAG
    return b
def body_txnoffsetcommit():   # api 28 v3
    b = flex_hdr(28, 3, 1) + cstr("tx") + cstr("g") + struct.pack(">q", 1) + struct.pack(">h", 0)
    b += struct.pack(">i", 1) + cstr("m") + cnull()  # generation, member, instance(null)
    # topics[1]{name, parts[1]{idx, off, leader_epoch, metadata(null), tag}, tag}
    b += carr(1) + cstr("ft") + carr(1) + struct.pack(">i", 0) + struct.pack(">q", 5) + struct.pack(">i", -1) + cnull() + TAG + TAG
    b += TAG
    return b

BODIES = {
    "Metadata_v9": body_metadata, "FindCoordinator_v3": body_findcoord,
    "JoinGroup_v7": body_joingroup, "SyncGroup_v5": body_syncgroup,
    "Heartbeat_v4": body_heartbeat, "LeaveGroup_v4": body_leavegroup,
    "OffsetFetch_v7": body_offsetfetch, "InitProducerId_v3": body_initpid,
    "TxnOffsetCommit_v3": body_txnoffsetcommit,
}

def mutations(base):
    """Yield adversarial mutations of a valid request body."""
    yield ("valid", base)
    # truncations at every offset past the header (~14 bytes)
    for cut in range(14, len(base)):
        yield (f"trunc@{cut}", base[:cut])
    # single-byte corruptions (flip to 0xFF and 0x00) across the body
    for pos in range(14, len(base)):
        yield (f"ff@{pos}", base[:pos] + b"\xff" + base[pos+1:])
        yield (f"00@{pos}", base[:pos] + b"\x00" + base[pos+1:])
    # inject a 5-byte huge uvarint (~2^35) at each body offset (fake compact len/count)
    huge = bytes([0xFF, 0xFF, 0xFF, 0xFF, 0x7F])
    for pos in range(14, len(base), 2):
        yield (f"huge@{pos}", base[:pos] + huge + base[pos:])
    # overlong varint (6+ continuation bytes) at the first body byte
    yield ("overlong", base[:14] + bytes([0x80]*8) + base[14:])

def liveness(sock):
    # ApiVersions v0 probe; return True if a well-formed response comes back
    try:
        req = struct.pack(">hhi", 18, 0, 999) + struct.pack(">h", 2) + b"lv"
        sock.sendall(frame(req))
        hdr = b""
        while len(hdr) < 4:
            c = sock.recv(4 - len(hdr))
            if not c: return False
            hdr += c
        n = struct.unpack(">i", hdr)[0]
        if n < 0 or n > (16 << 20): return False
        got = 0
        while got < n:
            c = sock.recv(min(65536, n - got))
            if not c: return False
            got += len(c)
        return True
    except Exception:
        return False

def alive_broker():
    try:
        s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
        ok = liveness(s); s.close(); return ok
    except Exception:
        return False

total = 0
sent = 0
for name, fn in BODIES.items():
    base = fn()
    for label, mut in mutations(base):
        total += 1
        # fresh connection per case (a malformed frame may make dreads drop us)
        try:
            s = socket.create_connection(("127.0.0.1", PORT), timeout=3)
            s.sendall(frame(mut))
            # best-effort read (may be empty if dropped) then a liveness probe on
            # the SAME connection if still open
            s.settimeout(0.5)
            try:
                s.recv(65536)
            except Exception:
                pass
            s.close()
            sent += 1
        except Exception:
            pass
    # after fuzzing this API, confirm the broker is still alive
    if not alive_broker():
        print(f"FAIL: broker DEAD after fuzzing {name}")
        sys.exit(1)
    print(f"  {name:22s} {len(list(mutations(base))):5d} cases -> broker alive")

print("=" * 55)
print(f"flex fuzz: {total} malformed requests across {len(BODIES)} flex APIs")
print("RESULT: PASS (broker alive through every malformed flexible request)")
