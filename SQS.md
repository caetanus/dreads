# Amazon SQS

dreads speaks the **Amazon SQS** JSON API as a native face — a hand-rolled
HTTP/1.1 server that answers the `X-Amz-Target: AmazonSQS.<Op>` /
`application/x-amz-json-1.0` protocol that **boto3** and the **AWS CLI** use. Point
them at dreads with a custom endpoint URL and they talk SQS.

The one-ring bet, again:

> An SQS queue **is** the Redis list at `sqs.q.<name>`. **SendMessage is an
> `RPUSH`**; **ReceiveMessage** pops the front and parks a copy in the in-flight
> (visibility) hash `sqs.if.<name>`; **DeleteMessage** removes that in-flight copy
> by receipt handle. Every op routes through the sharded data plane, so it is
> correct under `--shards` and durable via the per-shard AOF — no SQS-specific
> persistence code.

> Stated honestly. This page tells you what works and where it differs from real
> AWS SQS.

## Enabling it

| directive | flag | default | meaning |
|---|---|---|---|
| `sqs-port N` | `--sqs-port=N` | `0` (off) | SQS HTTP listener, e.g. `9324` |
| `sqs-bind ADDR` | `--sqs-bind=ADDR` | all | bind address for the SQS listener |
| `sqs-db N` | `--sqs-db=N` | `19` | logical DB for queue + in-flight state |
| `appendonly yes` | `--appendonly=yes` | `no` | persist queues to the AOF |

```sh
dreads --port=6379 --sqs-port=9324 --appendonly=yes --dir=/var/lib/dreads
```

```python
import boto3
sqs = boto3.client("sqs", endpoint_url="http://localhost:9324",
                   region_name="us-east-1",
                   aws_access_key_id="x", aws_secret_access_key="x")
url = sqs.create_queue(QueueName="orders")["QueueUrl"]
sqs.send_message(QueueUrl=url, MessageBody="hello")
msg = sqs.receive_message(QueueUrl=url, VisibilityTimeout=30)["Messages"][0]
sqs.delete_message(QueueUrl=url, ReceiptHandle=msg["ReceiptHandle"])
```

## What is supported

**Queue management** — `CreateQueue`, `GetQueueUrl`, `ListQueues`, `DeleteQueue`,
`GetQueueAttributes`, `SetQueueAttributes`, `PurgeQueue`.

**Messaging** — `SendMessage`, `SendMessageBatch`, `ReceiveMessage`,
`DeleteMessage`, `DeleteMessageBatch`. A received message carries a **receipt
handle**; the message is invisible for its **visibility timeout** (per-receive,
default 30s) and becomes visible again if not deleted within it — a ~1-second
sweep promotes expired in-flight messages back to the queue.

**Delay** — `DelaySeconds` on send holds a message invisible until its delay
elapses (the same sweep promotes it).

**Standard and FIFO queues** — a `.fifo`-suffixed queue is FIFO:

- **`MessageGroupId`** is required; a group is processed in order, and an
  in-flight message locks its group so the next receive skips to another group
  (SQS's per-group ordering).
- **Deduplication** — `MessageDeduplicationId` (or content-based) dedups within
  AWS's **5-minute** window.

Messages are MD5-summed like AWS (`MD5OfMessageBody`).

## Where it differs from AWS SQS

The JSON protocol — targets, request/response shapes, receipt handles, MD5 sums,
FIFO group/dedup semantics — follows AWS, with **boto3** / **aws-cli** as the
yardstick. dreads is a **local, self-hosted** SQS, so the differences are what you
would expect from swapping a managed cloud service for one process.

### Divergent by design — these will stay

- **No AWS authentication.** SigV4 signatures on the request are **not verified**
  — any credentials are accepted. dreads is meant to run inside your trust
  boundary (or behind your own gateway), not exposed as a public AWS endpoint.
- **Durability is the AOF (everysec), not AWS's managed redundancy.** A confirmed
  send is in memory + written to the AOF, fsync within 1s / on clean shutdown —
  not the cloud service's cross-AZ 11-nines. Redundancy, if you want it, is
  dreads' Raft, not AWS.
- **Visibility is swept at ~1s granularity.** A message reappears within about a
  second of its visibility timeout expiring, not to the millisecond.

### Not done yet — gaps we will close

- **No `ChangeMessageVisibility` / `...Batch`.** A consumer cannot extend or
  shorten an in-flight message's visibility after receiving it; the timeout is
  fixed at receive time.
- **No dead-letter queues.** `RedrivePolicy` / `maxReceiveCount` are not honored —
  a message received but never deleted simply reappears; it is not moved to a DLQ
  after N receives (and `ApproximateReceiveCount` is not tracked).

## See also

- [AMQP.md](AMQP.md) — the other queue-shaped skin over the same list model.
- [SHARDING.md](SHARDING.md) — how a queue's list is placed on a shard.
- `source/dreads/sqs.d` — the implementation; `SQS-PLAN.md` — the design notes.
