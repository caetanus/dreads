# golib Kafka conformance against dreads

Runs the **real** conformance suite from
[`faustbrian/golib` `pkg/kafka/kafkatest`](https://github.com/faustbrian/golib/blob/main/pkg/kafka/docs/conformance.md)
directly against a running dreads, instead of a hand-derived mirror. golib's own
`TestPublicConformance` obtains a broker via testcontainers (a real Kafka
container) and then drives `kafkatest.Run*Conformance(t, harness)` where the
harness is just a `[]string` of bootstrap addresses. `dreads_conformance_test.go`
builds the same `kafkatest.BrokerHarness` pointed at dreads — no container — and
calls each conformance seam. This is a true blackbox run: the tests are the
library author's, unmodified.

## Run

```sh
# 1. build + start dreads on a free Kafka port (Apache's container binds
#    127.0.0.1:9092 AND :9093 — use a port it does not, e.g. 19092)
dub build --build=release --compiler=ldc2
./bin/dreads --port 7300 --kafka-port 19092 &

# 2. run a seam (needs Go >= 1.26; go test fetches franz-go/kadm/golib on demand)
cd test/conformance/golib
go test -count=1 -timeout 100s -run TestDreadsReplayConformance   -v .
go test -count=1 -timeout 100s -run TestDreadsProducerConformance -v .
# … Inspector / Consumer / Transaction likewise
```

`go.mod` pins golib at commit `a31bcbe` and franz-go v1.21.6 / kadm v1.18.0.

## Results (2026-08-22, dreads @ 8420777, pre-flexible Kafka dialect)

| Seam | Result | Detail |
|------|--------|--------|
| **Replay** | ✅ PASS | Planned ranges resume + report exact independent progress. dreads' direct-partition fetch/offset semantics conform fully. |
| **Producer** | ✅ 4/4 | All pass since RecordBatch v2 / record-headers support (Produce v3/Fetch v4) landed. The `synchronous binary-safe owned delivery` case round-trips a header + explicit timestamp, which now works. |
| **Inspector** | ❌ FAIL | `RunOnce()` drives a consumer-group poll (`consumer poll permanent failure`). |
| **Consumer** | ❌ FAIL | at-least-once settlement is defined over consumer-group commits. |
| **Transaction** | ❌ FAIL | requires transactions (transactional producer + consume-transform-produce). |

### What the remaining failures mean (none is a memory-safety / correctness bug)

- **Producer sync-delivery** (now PASSES): the test round-trips a record header +
  explicit timestamp. This was the driver for adding **RecordBatch v2 / magic-2**
  support (Produce v3 / Fetch v4): dreads now decodes v2 batches (carrying
  headers), stores headers in an internal blob, and re-encodes a v2 batch on
  Fetch v4+ (down-converting to v1 for old clients). Adding v2 also resolved the
  earlier "compressed batch" symptom — franz-go on Produce v3 sends **uncompressed
  v2** by default (the snappy wrapper only appeared when franz-go was forced down
  to magic-1). Compression of v2 batches remains a separate unbuilt gap.
- **Inspector / Consumer / Transaction**: all require **consumer groups**
  (FindCoordinator/JoinGroup/SyncGroup/OffsetCommit/OffsetFetch) and/or
  **transactions** — deliberately-unbuilt feature milestones. They are reported,
  not implemented, per the project's standing rule.

### Robustness note

dreads stayed **alive with zero stray processes** through every seam, including
the consumer-group and transaction seams that hammer it with unimplemented
APIs — it returns errors (surfaced by franz-go as retryable/permanent delivery
failures), never crashes or hangs the broker.

## Takeaway

The seams that exercise dreads' **implemented** surface — **Replay** (fetch) and
the bulk of **Producer** (produce + delivery metadata + ordering + fencing) —
**conform to the real golib suite**. Every failure is an unbuilt milestone
(record headers, consumer groups, transactions), not a defect in existing code.
