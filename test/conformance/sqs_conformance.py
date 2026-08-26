#!/usr/bin/env python3
# Broker-observable Amazon SQS conformance suite for the dreads SQS skin.
# Speaks the raw SQS JSON protocol (X-Amz-Target: AmazonSQS.<Op>,
# application/x-amz-json-1.0) over urllib — NO boto3 dependency — so it runs in
# CI. Drives the observable contract: queue lifecycle, the send/receive/delete
# round-trip, MD5 sums, visibility timeout, batches, PurgeQueue, FIFO ordering +
# 5-minute dedup + per-group lock, and DelaySeconds.
#
#   sqs_conformance.py <sqs_port>    e.g. ... 9324
#   (start dreads first: ./bin/dreads --port 7300 --sqs-port 9324)
#
# This suite exists because a bare NUM_DBS off-by-one once left EVERY SQS op
# crashing the broker with nothing in CI to catch it — the send/receive round
# trip below fails loudly the instant that regresses.
import sys, json, time, hashlib, urllib.request, urllib.error

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9324
BASE = "http://127.0.0.1:%d/" % PORT

passed = 0
failed = 0
fails = []


def call(op, body):
    """POST one SQS JSON op; return (status, parsed-json). AWS returns errors as
    4xx with a JSON {__type, message} body, so we parse both paths."""
    req = urllib.request.Request(
        BASE, data=json.dumps(body).encode(),
        headers={"X-Amz-Target": "AmazonSQS." + op,
                 "Content-Type": "application/x-amz-json-1.0"})
    try:
        with urllib.request.urlopen(req, timeout=8) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except Exception:
            return e.code, {"raw": raw}


def check(name, cond, detail=""):
    global passed, failed
    if cond:
        print("  PASS  %-52s %s" % (name, detail)); passed += 1
    else:
        print("  FAIL  %-52s %s" % (name, detail)); failed += 1; fails.append(name)


def md5hex(s):
    return hashlib.md5(s.encode()).hexdigest()


# --- [LIFECYCLE] -----------------------------------------------------------
print("\n[LIFECYCLE]")
st, r = call("CreateQueue", {"QueueName": "conf_q"})
url = r.get("QueueUrl", "")
check("CreateQueue returns a QueueUrl", st == 200 and url.endswith("conf_q"),
      "url=%s" % url)
st, r = call("GetQueueUrl", {"QueueName": "conf_q"})
check("GetQueueUrl round-trips the name", r.get("QueueUrl", "").endswith("conf_q"))
st, r = call("ListQueues", {})
check("ListQueues includes the queue",
      any(u.endswith("conf_q") for u in r.get("QueueUrls", [])))

# --- [ROUND-TRIP] the op that a NUM_DBS regression crashes on ---------------
print("\n[ROUND-TRIP]")
body = "hello sqs"
st, r = call("SendMessage", {"QueueUrl": url, "MessageBody": body})
mid = r.get("MessageId", "")
check("SendMessage returns MessageId + correct MD5",
      st == 200 and mid != "" and r.get("MD5OfMessageBody") == md5hex(body),
      "md5=%s" % r.get("MD5OfMessageBody"))
st, r = call("ReceiveMessage", {"QueueUrl": url})
msgs = r.get("Messages", [])
got = msgs[0] if msgs else {}
check("ReceiveMessage returns the body + a receipt handle",
      got.get("Body") == body and got.get("ReceiptHandle", "") != "")
rh = got.get("ReceiptHandle", "")
st, r = call("DeleteMessage", {"QueueUrl": url, "ReceiptHandle": rh})
check("DeleteMessage succeeds", st == 200)
st, r = call("ReceiveMessage", {"QueueUrl": url})
check("queue empty after delete", not r.get("Messages"))

# --- [VISIBILITY] ----------------------------------------------------------
print("\n[VISIBILITY]")
call("SendMessage", {"QueueUrl": url, "MessageBody": "vis"})
st, r = call("ReceiveMessage", {"QueueUrl": url, "VisibilityTimeout": 2})
inflight = r.get("Messages", [{}])[0].get("ReceiptHandle", "")
check("message delivered with a visibility timeout", inflight != "")
st, r = call("ReceiveMessage", {"QueueUrl": url})
check("in-flight message is invisible to a second receive", not r.get("Messages"))
time.sleep(3)  # timeout (2s) + the ~1s sweep
st, r = call("ReceiveMessage", {"QueueUrl": url})
check("message reappears after the visibility timeout", bool(r.get("Messages")))
# clean up the reappeared one
for m in r.get("Messages", []):
    call("DeleteMessage", {"QueueUrl": url, "ReceiptHandle": m["ReceiptHandle"]})

# --- [BATCH] ---------------------------------------------------------------
print("\n[BATCH]")
st, r = call("SendMessageBatch", {"QueueUrl": url, "Entries": [
    {"Id": "a", "MessageBody": "b1"},
    {"Id": "b", "MessageBody": "b2"},
    {"Id": "c", "MessageBody": "b3"}]})
check("SendMessageBatch accepts 3 entries", len(r.get("Successful", [])) == 3)
seen = set()
for _ in range(6):
    st, r = call("ReceiveMessage", {"QueueUrl": url, "MaxNumberOfMessages": 10})
    for m in r.get("Messages", []):
        seen.add(m["Body"])
        call("DeleteMessage", {"QueueUrl": url, "ReceiptHandle": m["ReceiptHandle"]})
    if len(seen) >= 3:
        break
check("all 3 batch messages received", seen == {"b1", "b2", "b3"}, "got=%s" % sorted(seen))

# --- [PURGE] ---------------------------------------------------------------
print("\n[PURGE]")
call("SendMessage", {"QueueUrl": url, "MessageBody": "gone"})
call("PurgeQueue", {"QueueUrl": url})
st, r = call("ReceiveMessage", {"QueueUrl": url})
check("PurgeQueue empties the queue", not r.get("Messages"))

# --- [DELAY] ---------------------------------------------------------------
print("\n[DELAY]")
call("SendMessage", {"QueueUrl": url, "MessageBody": "later", "DelaySeconds": 2})
st, r = call("ReceiveMessage", {"QueueUrl": url})
check("DelaySeconds hides the message initially", not r.get("Messages"))
time.sleep(3)
st, r = call("ReceiveMessage", {"QueueUrl": url})
check("delayed message appears after its delay", bool(r.get("Messages")))
for m in r.get("Messages", []):
    call("DeleteMessage", {"QueueUrl": url, "ReceiptHandle": m["ReceiptHandle"]})

# --- [FIFO] ----------------------------------------------------------------
print("\n[FIFO]")
st, r = call("CreateQueue", {"QueueName": "conf.fifo",
                             "Attributes": {"FifoQueue": "true"}})
furl = r.get("QueueUrl", "")
check("FIFO queue created", furl.endswith("conf.fifo"))
# ordered delivery within one group
for i in range(3):
    call("SendMessage", {"QueueUrl": furl, "MessageBody": "m%d" % i,
                         "MessageGroupId": "g1",
                         "MessageDeduplicationId": "d%d" % i})
order = []
for _ in range(6):
    st, r = call("ReceiveMessage", {"QueueUrl": furl, "MaxNumberOfMessages": 10})
    for m in r.get("Messages", []):
        order.append(m["Body"])
        call("DeleteMessage", {"QueueUrl": furl, "ReceiptHandle": m["ReceiptHandle"]})
    if len(order) >= 3:
        break
check("FIFO delivers a group in send order", order == ["m0", "m1", "m2"],
      "order=%s" % order)
# dedup: same MessageDeduplicationId is dropped
call("SendMessage", {"QueueUrl": furl, "MessageBody": "x",
                     "MessageGroupId": "g2", "MessageDeduplicationId": "dup"})
call("SendMessage", {"QueueUrl": furl, "MessageBody": "x",
                     "MessageGroupId": "g2", "MessageDeduplicationId": "dup"})
cnt = 0
for _ in range(4):
    st, r = call("ReceiveMessage", {"QueueUrl": furl, "MaxNumberOfMessages": 10})
    for m in r.get("Messages", []):
        cnt += 1
        call("DeleteMessage", {"QueueUrl": furl, "ReceiptHandle": m["ReceiptHandle"]})
check("FIFO dedup drops the duplicate MessageDeduplicationId", cnt == 1, "delivered=%d" % cnt)

# --- [TEARDOWN] ------------------------------------------------------------
print("\n[TEARDOWN]")
call("DeleteQueue", {"QueueUrl": url})
call("DeleteQueue", {"QueueUrl": furl})
st, r = call("ListQueues", {})
check("DeleteQueue removes the queues",
      not any(u.endswith(("conf_q", "conf.fifo")) for u in r.get("QueueUrls", [])))

print("\n" + "=" * 60)
print("SQS conformance: %d passed, %d failed" % (passed, failed))
if failed:
    print("FAILED:", ", ".join(fails))
    sys.exit(1)
print("PASS: dreads satisfies the observable SQS contract")
