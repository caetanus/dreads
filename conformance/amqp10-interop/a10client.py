"""Minimal raw AMQP 1.0 client for the dreads interop matrix (no deps)."""
import socket, struct

def frame(ftype, ch, body): return struct.pack(">IBBH", 8 + len(body), 2, ftype, ch) + body
def perf_list(code, fields):
    payload = b"".join(fields)
    return b"\x00\x53" + bytes([code]) + b"\xd0" + struct.pack(">II", 4 + len(payload), len(fields)) + payload
def str8(x):
    b = x.encode() if isinstance(x, str) else x
    return b"\xa1" + bytes([len(b)]) + b
def sym8(x):
    b = x.encode() if isinstance(x, str) else x
    return b"\xa3" + bytes([len(b)]) + b
def vbin8(b): return b"\xa0" + bytes([len(b)]) + b
def u32(v): return b"\x70" + struct.pack(">I", v)
def described(code, inner): return b"\x00\x53" + bytes([code]) + inner
def dlist(fields):
    payload = b"".join(fields)
    return b"\xd0" + struct.pack(">II", 4 + len(payload), len(fields)) + payload
def dmap(pairs):
    payload = b"".join(pairs)
    return b"\xd1" + struct.pack(">II", 4 + len(payload), len(pairs)) + payload

class A10:
    def __init__(self, host="127.0.0.1", port=5672, user="guest", pw="guest"):
        self.s = socket.create_connection((host, port))
        self.next_handle = 0
        self.next_did = 0
        self.s.sendall(b"AMQP\x03\x01\x00\x00"); self.rd(8); self.rd_frame()
        self.s.sendall(frame(1, 0, perf_list(0x41, [sym8("PLAIN"), vbin8(b"\x00" + user.encode() + b"\x00" + pw.encode())])))
        self.rd_frame()
        self.s.sendall(b"AMQP\x00\x01\x00\x00"); self.rd(8)
        self.s.sendall(frame(0, 0, perf_list(0x10, [str8("interop"), b"\x40", u32(1 << 20), b"\x60\x03\xff", u32(0)])))
        self.rd_frame()
        self.s.sendall(frame(0, 0, perf_list(0x11, [b"\x40", u32(0), u32(2048), u32(2048)])))
        self.rd_frame()

    def rd(self, n):
        b = b""
        while len(b) < n:
            c = self.s.recv(n - len(b))
            if not c: raise EOFError
            b += c
        return b

    def rd_frame(self, timeout=None):
        self.s.settimeout(timeout)
        h = self.rd(8)
        size, doff, ftype, ch = struct.unpack(">IBBH", h)
        body = self.rd(size - 8) if size > 8 else b""
        return ftype, ch, body[(doff * 4) - 8:]

    def attach_sender(self, addr):
        h = self.next_handle; self.next_handle += 1
        target = described(0x29, dlist([str8(addr)]))
        self.s.sendall(frame(0, 0, perf_list(0x12, [str8("s%d" % h), u32(h), b"\x42", b"\x40", b"\x40", b"\x40", target])))
        self.rd_frame()  # attach echo
        self.rd_frame()  # flow credit
        return h

    def attach_receiver(self, addr, credit=10):
        h = self.next_handle; self.next_handle += 1
        source = described(0x28, dlist([str8(addr)]))
        self.s.sendall(frame(0, 0, perf_list(0x12, [str8("r%d" % h), u32(h), b"\x41", b"\x40", b"\x40", source, b"\x40"])))
        self.rd_frame()  # attach echo
        fl = perf_list(0x13, [u32(0), u32(2048), u32(0), u32(2048), u32(h), u32(0), u32(credit)])
        self.s.sendall(frame(0, 0, fl))
        return h

    def send(self, handle, sections, settled=True):
        did = self.next_did; self.next_did += 1
        tr = perf_list(0x14, [u32(handle), u32(did), vbin8(b"t%d" % did), u32(0),
                              b"\x41" if settled else b"\x42", b"\x42"])
        self.s.sendall(frame(0, 0, tr + sections))
        if not settled:
            ft, ch, b = self.rd_frame(5)
            assert b[2] == 0x15, "expected disposition, got 0x%02x" % b[2]
            return b  # raw disposition body
        return None

    def recv_message(self, timeout=5):
        while True:
            ft, ch, b = self.rd_frame(timeout)
            if len(b) == 0:
                continue  # keepalive
            if b[2] == 0x14:
                # transfer: capture the delivery-id (field 1), then the
                # message after the performative list
                assert b[3] == 0xD0
                lsz = struct.unpack(">I", b[4:8])[0]
                fields = b[12:8 + lsz]
                i = _skip_value(fields, 0)  # handle
                # delivery-id constructor at i
                c0 = fields[i]
                if c0 == 0x43:
                    self.last_did = 0
                elif c0 == 0x52:
                    self.last_did = fields[i + 1]
                elif c0 == 0x70:
                    self.last_did = struct.unpack(">I", fields[i+1:i+5])[0]
                return b[8 + lsz:]
            # ignore flow etc.

def parse_sections(msg):
    """Crude section splitter: returns dict code -> raw value bytes."""
    out = {}
    i = 0
    while i < len(msg):
        assert msg[i] == 0x00 and msg[i+1] == 0x53
        code = msg[i+2]
        j = i + 3
        out[code] = (msg, j)
        # skip one value
        j = _skip_value(msg, j)
        out[code] = msg[i+3:j]
        i = j
    return out

def _skip_value(m, i):
    c = m[i]; i += 1
    if c in (0x40, 0x41, 0x42, 0x43, 0x44, 0x45): return i
    if c in (0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56): return i + 1
    if c in (0x60, 0x61): return i + 2
    if c in (0x70, 0x71, 0x72): return i + 4
    if c in (0x80, 0x81, 0x82, 0x83): return i + 8
    if c == 0x98: return i + 16
    if c in (0xA0, 0xA1, 0xA3): return i + 1 + m[i]
    if c in (0xB0, 0xB1, 0xB3): return i + 4 + struct.unpack(">I", m[i:i+4])[0]
    if c in (0xC0, 0xC1, 0xE0): return i + 1 + m[i]
    if c in (0xD0, 0xD1, 0xF0): return i + 4 + struct.unpack(">I", m[i:i+4])[0]
    if c == 0x00:  # described
        i = _skip_value(m, i)
        return _skip_value(m, i)
    raise AssertionError("ctor 0x%02x" % c)
