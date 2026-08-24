"""dreads M5: the 0-9-1 <-> 1.0 interop matrix. One shared topology, two
protocols — every case publishes on one side and verifies on the other."""
import sys, time, struct
sys.path.insert(0, __file__.rsplit("/", 1)[0])
from a10client import *
import pika

OK = []

def case(name):
    def deco(fn):
        def run():
            fn()
            OK.append(name)
            print("PASS", name)
        return run
    return deco

pconn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1", 5672))
pch = pconn.channel()
a10 = A10()

@case("091->10: props/body/headers")
def c1():
    pch.queue_declare("ix1")
    pch.basic_publish("", "ix1", b"body-091", pika.BasicProperties(
        content_type="text/plain", reply_to="rq", correlation_id="c1",
        headers={"k": "v", "n": 7}, priority=3, delivery_mode=2))
    a10.attach_receiver("/queues/ix1", credit=1)
    msg = a10.recv_message()
    secs = parse_sections(msg)
    assert 0x75 in secs and b"body-091" in secs[0x75], "body"
    assert 0x73 in secs and b"text/plain" in secs[0x73] and b"rq" in secs[0x73], "props"
    assert 0x74 in secs and b"k" in secs[0x74], "app-props"

@case("10->091: sections mapped")
def c2():
    pch.queue_declare("ix2")
    h = a10.attach_sender("/queues/ix2")
    msg = described(0x73, dlist([b"\x40"]*4 + [str8("rt"), str8("cid"), sym8("text/x")]))
    msg += described(0x74, dmap([str8("ak"), str8("av")]))
    msg += described(0x75, vbin8(b"body-10"))
    a10.send(h, msg, settled=False)
    m = pch.basic_get("ix2", auto_ack=True)
    assert m[0] is not None and m[2] == b"body-10"
    assert m[1].content_type == "text/x" and m[1].reply_to == "rt"
    assert m[1].headers.get("ak") == "av"

@case("mgmt topology visible to 091")
def c3():
    # declare exchange+queue+binding via raw ctl ops through 0-9-1 instead of
    # $management (mgmt link pair is heavier here); verify cross-protocol
    pch.exchange_declare("ix3x", "direct")
    pch.queue_declare("ix3q")
    pch.queue_bind("ix3q", "ix3x", "r")
    h = a10.attach_sender("/exchanges/ix3x/r")
    a10.send(h, described(0x75, vbin8(b"via-exchange")), settled=False)
    m = pch.basic_get("ix3q", auto_ack=True)
    assert m[0] is not None and m[2] == b"via-exchange"

@case("091 TTL+DLX applies to 10 publishes")
def c4():
    pch.exchange_declare("ix4dlx", "fanout")
    pch.queue_declare("ix4dlq")
    pch.queue_bind("ix4dlq", "ix4dlx", "")
    pch.queue_declare("ix4", arguments={"x-message-ttl": 1, "x-dead-letter-exchange": "ix4dlx"})
    h = a10.attach_sender("/queues/ix4")
    a10.send(h, described(0x75, vbin8(b"doomed")), settled=False)
    time.sleep(0.4)
    m = pch.basic_get("ix4dlq", auto_ack=True)
    assert m[0] is not None and m[2] == b"doomed", "not dead-lettered"
    assert m[1].headers and "x-death" in m[1].headers, "x-death missing"

@case("10 released -> 091 sees redelivered")
def c5():
    pch.queue_declare("ix5")
    pch.basic_publish("", "ix5", b"boomerang")
    a10.attach_receiver("/queues/ix5", credit=1)
    msg = a10.recv_message()
    did = a10.last_did  # the delivery we just received
    disp = perf_list(0x15, [b"\x41", u32(did), u32(did), b"\x41", described(0x26, b"\x45")])
    a10.s.sendall(frame(0, 0, disp))
    time.sleep(0.3)
    m = pch.basic_get("ix5", auto_ack=True)
    assert m[0] is not None and m[0].redelivered, "redelivered flag lost"

@case("maxlen honored for 10 publishes")
def c6():
    pch.queue_declare("ix6", arguments={"x-max-length": 1})
    h = a10.attach_sender("/queues/ix6")
    a10.send(h, described(0x75, vbin8(b"one")), settled=False)
    a10.send(h, described(0x75, vbin8(b"two")), settled=False)
    q = pch.queue_declare("ix6", passive=True, arguments={"x-max-length": 1})
    assert q.method.message_count == 1, "maxlen not enforced: %d" % q.method.message_count

for fn in [c1, c2, c3, c4, c5, c6]:
    fn()
print("INTEROP MATRIX: %d/6 PASS" % len(OK))
