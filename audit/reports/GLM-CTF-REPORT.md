# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

# GLM-CTF-REPORT — `source/dreads/sqs.d`

Line numbers are approximate (function-anchored) since the listing was unlabeled.

---

## FLAG 1 — Cross-client message-body corruption via TLS static buffer held across a parking hop

**Where:** `jsonStr()` (`static char[65536] ub;`) consumed in `opSendMessage()` — `msgBody` is used *after* `dedupSeen()`'s `exec(hget)` which parks the fiber on a cross-shard hop.
**Severity: HIGH** (cross-client data corruption; the exact bug class that already shipped once)

**Attack trace:**
1. Client A sends `X-Amz-Target: AmazonSQS.SendMessage` on a FIFO queue with `MessageDeduplicationId` set, body `"AAAA..."`:
   `auto msgBody = jsonStr(b, "MessageBody");` → slice into TLS `ub`.
2. `dedupSeen(name, dedup, mid, md5, msgBody)` executes `hget sqs.dd.<name> <dedup>`. Under sharding this key may be owned by another shard → `amqpDataExec` hops → `kafkaGroupHopImpl`-style park on `pnd.done.wait`. **Fiber A yields while holding a slice into `ub`.**
3. Client B's connection fiber (same thread, SO_REUSEPORT router) runs its own `opSendMessage` → its `jsonStr` writes body `"BBBB..."` into the *same* `ub`.
4. A resumes: `md5Hex(msgBody)` and `sendOne(name, msgBody, ...)` now read **B's bytes**. A's queue receives B's body; A's dedup store records B's md5; the HTTP response reports B's MD5 as A's `MD5OfMessageBody`.

Internal state: `ub` is thread-local and shared by every connection fiber on that router thread; nothing revalidates the slice after the park.

**Root cause:** `jsonStr` returns a slice of one static TLS buffer; callers hold it across `exec()` calls that can park the fiber (sharded hop). Note the codebase already knows this pattern — `kafkaGroupHopImpl` re-clears `reply` "AFTER the park" for exactly this reason, and the sweep copies `names` into `namesCopy` for the same reason — but `jsonStr` results were missed.

**Fix:** unescape into a caller-supplied `ref ByteBuffer` (or `appendJsonStr`-style copy into a per-op buffer) immediately after extraction, before any `exec`. Minimum viable fix: in `opSendMessage`, copy `msgBody` into a local `ByteBuffer` right after `jsonStr`, before `dedupSeen`.

---

## FLAG 2 — Unbounded memory read/alloc from one connection (`Content-Length` loop)

**Where:** `onConn()` — the `while (whole.length < hend + clen)` refill loop.
**Severity: HIGH** (remote DoS, unbounded memory from a single connection)

**Attack trace:**
```
POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 99999999999\r\n\r\n<bytes...>
```
- `toSize()` has no cap; `clen` is attacker-controlled and unvalidated.
- The loop `whole.append(tmp[0 .. r])` grows a heap `ByteBuffer` without limit while the attacker keeps streaming (each 10 s `waitForData` window resets as long as data flows; even a slow trickle of one byte per read keeps `r > 0`).
- ~N GB of `Content-Length` → ~N GB RSS → OOM-kill of the whole broker (all skins, all shards share the process).

**Root cause:** no maximum body size; trust of `Content-Length` before any bound check.

**Fix:** enforce a config cap (e.g. 1–4 MB) on `clen`; reject with 413 and close if `hend + clen` exceeds it.

---

## FLAG 3 — FIFO receive is not atomic: duplicate delivery + group-lock race

**Where:** `opReceiveMessage()` — `lrange` snapshot → `sismember` → `lrem` → `sadd` as four separate `exec` calls.
**Severity: HIGH** (breaks FIFO exactly-once-ish contract; duplicates across consumers)

**Attack trace (two connections, sharded so each `exec` parks the fiber):**
1. Queue `q.fifo` holds records R1(group=g1), R2(group=g2).
2. Conn A: `lrange` snapshot → sees R1. `exec(lrem ... R1)` hops → A **parks**.
3. Conn B (same or another router): `lrange` → still sees R1 (A's LREM not applied yet, or in flight) → B also `lrem`s R1 (count 1 — second LREM is a no-op if A's landed, but B already holds R1 in *its* snapshot) → both A and B `hset sqs.if.q.fifo <handleA/B> = ...R1` and both return R1 to their clients.
4. Same race on `sismember grpkey`: two receivers can both pass the "group not locked" check for the same group before either `sadd` lands → two in-flight messages of one group, destroying FIFO ordering.
5. Also corrupts the delete path: `opDeleteMessage` "unconditionally" srems the group because "at most one message per group is ever in-flight" — the invariant this race breaks, so a delete can unlock a group that still has a live in-flight message.

**Root cause:** the snapshot/lrem/lock sequence is 3–4 independent routed commands with fiber yields (hops) between them; no MULTI/EXEC or Lua atomicity.

**Fix:** perform the FIFO pick + LREM + SADD + HSET as one atomic unit on the owning shard (a script or a dedicated core op), or serialize receives per queue with a local lock spanning the whole receive.

---

## FLAG 4 — `DeleteMessageBatch` never releases the FIFO group lock → permanent queue stall

**Where:** `opDeleteMessageBatch()` — only `hdel`s the in-flight handle; no `srem sqs.grp.<name> <group>` (unlike `opDeleteMessage`).
**Severity: HIGH** (remote, trivially triggerable permanent DoS of a FIFO queue's group)

**Attack trace:**
1. Create `q.fifo`; send message with `MessageGroupId=g1`; receive it (locks `g1` via `sadd sqs.grp.q.fifo g1`).
2. Delete via **batch**:
   `X-Amz-Target: AmazonSQS.DeleteMessageBatch`, body `{"Entries":[{"Id":"a","ReceiptHandle":"<handle>"}]}`.
3. Handler runs only `hdel sqs.if.q.fifo <handle>`. `sqs.grp.q.fifo` still contains `g1`.
4. Every subsequent `ReceiveMessage` skips all `g1` records forever (`sismember == 1 → continue`). The visibility sweep only srems the group when a record **expires** — the record was deleted, so it never re-appears. Group `g1` is dead until queue deletion. Also note `sqsVisibilitySweep`'s group-unlock only fires on expiry, so even the non-batch delete-then-nothing path relies on it.

**Root cause:** batch delete omitted the FIFO unlock logic implemented in single delete.

**Fix:** factor the "hget → split → srem grp" unlock out of `opDeleteMessage` and call it per entry in the batch op (or drop the unconditional-srem design and reference-count group locks).

---

## FLAG 5 — Total absence of authentication (SigV4-less SQS)

**Where:** `onConn()` / `dispatch()` — no `Authorization` header, signature, or credential check of any kind before executing every operation.
**Severity: HIGH** (any TCP peer gets full read/write/delete on every queue, incl. `PurgeQueue` and `DeleteQueue`)

**Attack trace:** any host that can reach `sqsPort` sends:
```
POST / HTTP/1.1\r\nX-Amz-Target: AmazonSQS.ReceiveMessage\r\nContent-Length: N\r\n\r\n
{"QueueUrl":"http://x/000000000000/victim-queue"}
```
and receives the victim's messages; `DeleteQueue`/`PurgeQueue` destroy them. Cross-tenant disclosure is limited only by queue-name knowledge (`ListQueues` enumerates **all** queue names globally — there is no per-owner namespace).

**Root cause:** no auth layer installed for the skin.

**Fix:** gate `dispatch` behind SigV4 verification (or at minimum a shared token / bind-to-loopback default); namespace `Q_REGISTRY` per credential.

---

## FLAG 6 — `VisibilityTimeout` integer overflow → immediate-redelivery churn

**Where:** `opReceiveMessage()` — `immutable deadline = nowMs() + visTimeout * 1000;` with only a `< 0` clamp (no upper bound; AWS cap is 43200).
**Severity: MED**

**Attack trace:** `ReceiveMessage` with `"VisibilityTimeout": 9223372036854` → `visTimeout * 1000` overflows `long` → negative deadline → stored in `sqs.if.<q>`; the 1 s sweep sees `dl <= now` and immediately LPUSHes the record back while the client still holds it → continuous duplicate delivery + a record that ping-pongs between queue and in-flight hash every second, burning CPU on every sweep pass (multiplied per message).

**Root cause:** unclamped client integer used in ms arithmetic.

**Fix:** clamp `visTimeout` to `[0, 43200]` like `DelaySeconds` is clamped.

---

## FLAG 7 — Silent body truncation at 64 KB

**Where:** `jsonStr()` — `if (o < ub.length) ub[o++] = ...` drops bytes past 65536 instead of erroring.
**Severity: MED** (silent data corruption; also wrong `MD5OfMessageBody`)

**Attack trace:** send `"MessageBody"` of 100 KB (valid SQS allows 256 KB). `jsonStr` returns the first 65536 unescaped bytes; `sendOne` stores and MD5s the truncation; the client gets a success response whose `MD5OfMessageBody` matches only the truncated payload. Receiver gets corrupted data with no error anywhere.

**Fix:** return failure (400 `InvalidMessageContents`) when the value exceeds the buffer, and size the buffer to the SQS max (256 KB).

---

## SUSPECT (unproven from this file alone)

- **`header()` scans into the body** (no stop at the blank line): a JSON body containing a raw `\r\nContent-Length: <n>\r\n` line can inject a fake length when the real header is absent — impact limited to feeding flag 2's loop; needs `amqpDataExec` behavior to confirm nothing worse.
- **`opDeleteMessage` without queue-existence check + cross-queue handle probing**: handles are 48-hex random, so brute-force is impractical; but if `randHex`'s entropy source is weak (see `dreads.tls`, not shown), an attacker could predict handles of *other* queues' in-flight messages and delete/redrive them. Confirm `randHex` uses a CSPRNG.
- **Sweep ordering (LPUSH front) for standard queues**: a visibility-timeout expiry re-inserts at the head, reordering a standard queue's delivery — probably acceptable for SQS semantics, but confirm nothing depends on it.
- **`jsonEachEntry` brace matcher `if (c=='\\') i++;`** can skip past a closing quote's partner and swallow a `}` — parser confusion only within the attacker's own request; no cross-request effect found.

---

## Ranked summary

1. HIGH sqs.d `opSendMessage`/`jsonStr` — TLS static `ub` slice held across parking hop → cross-client body substitution.
2. HIGH sqs.d `onConn` — unbounded `Content-Length` read loop → single-connection OOM DoS.
3. HIGH sqs.d `opReceiveMessage` — non-atomic FIFO lrange/lrem/sadd → duplicate delivery + group-order break.
4. HIGH sqs.d `opDeleteMessageBatch` — missing FIFO group unlock → permanent group stall.
5. HIGH sqs.d `onConn`/`dispatch` — zero authentication; full queue access + global `ListQueues` enumeration.
6. MED sqs.d `opReceiveMessage` — `VisibilityTimeout` overflow → immediate-redelivery churn.
7. MED sqs.d `jsonStr` — 64 KB silent body truncation → corrupted stored message + wrong MD5.

---

# CTF flags — `kafkagroup.d`

# GLM CTF Report — `source/dreads/kafkagroup.d`

I read the full FSM and cross-checked the transport (`kafkaGroupHopImpl` in server.d hands raw client-built op bytes to `kgroupApply` in the owner drain; the decoder (`KgRd`) itself is bounds-safe — `str16`/`bytes32`/`i32` all validate before slicing, and every stored string is `.idup`'d out of the ring slice, so the known "slice into reused ring/TLS buffer" class is *not* present here). What survives scrutiny are logic/abuse flags, all client-reachable by speaking the Kafka protocol's JoinGroup/SyncGroup/LeaveGroup/Txn paths:

---

## FLAG 1 — HIGH — `KGOP_LEAVE` has no generation or requester check: any client can evict arbitrary members of any group (cross-tenant disruption / persistent rebalance-storm DoS)

**Where:** `kgroupApply`, `case KGOP_LEAVE` (~line 560 region of kafkagroup.d).

**Attack trace:** attacker connects to the Kafka port, discovers (or guesses — ids are `dreads-<ctr>-<tick>`, and `KGOP_DESCRIBE` hands them out, see FLAG 4) member ids of victim group `g`, then sends a LeaveGroup op:

```
[u8 5 (KGOP_LEAVE)][u16 1 "g"][i32 1]
  [u16 len mid  "dreads-17-3421"]
```

`kgroupApply` does, per id: `g.members.remove(mid)` — **no `gen` field is even parsed, no membership proof, no epoch check** (compare `KGOP_HEARTBEAT`/`KGOP_SYNC`/`KGOP_COMMIT_CHECK`, all of which fence on `gen != g.generation`). Then `enterPreparing(g, now)` forces every survivor into a rebalance. Repeating this in a loop (the member id pattern is enumerable, and DescribeGroups refreshes it) keeps the group permanently in `ST_PREPARING` — consumers never get a stable assignment, a sustained liveness DoS from one connection, and cross-tenant: group names are a flat namespace routed by `keyToSlot("kafka.cg."+group)`.

**Root cause:** the op trusts the member-id list verbatim; only the Kafka skin's outer ACL (if any) could stop it, and the FSM is documented as the coordinator authority.

**Fix:** parse `[str mid][i32 gen]` per member like Heartbeat does; reject `mid` not owned by the requesting connection (the request should carry the requester's connection identity, or at minimum require a matching generation and, for foreign ids, return UNKNOWN_MEMBER unless the requester is that member).

---

## FLAG 2 — MED — Transactional-id table `tTxns` is unbounded and never swept: remote memory exhaustion

**Where:** `case KGOP_TXN_INIT` (`tTxns[groupName.idup] = t;`) and the absence of any txn cleanup in `kgroupSweep()`.

**Attack trace:** one connection sends N `InitProducerId` requests with distinct transactional.ids (`txn-0` … `txn-N`), each:

```
[u8 10 (KGOP_TXN_INIT)][u16 len tid][i32 900000]
```

Each creates a `new KgTxn` (with its `.idup`'d key, plus `tps`/`offs` arrays if the attacker then calls TXN_ADD/TXN_OFFSETS, each entry holding `.idup`'d topic strings and metadata — KBs per id) and is **never freed**: `kgroupSweep` only walks `tGroups` (which is capped at `KG_MAX_GROUPS = 4096`), nothing ever removes from `tTxns`. Every entry also pins the whole `KgTxnOff[] offs` it buffered. Result: unbounded heap growth on the owner shard from a single small frame repeated — a trivial remote OOM that takes down the whole shard process (shared with all tenants on that shard).

**Root cause:** the `KG_MAX_GROUPS` sanity cap was applied to groups but the newer txn table (KAFKA-TXN-PLAN T2) got neither a cap nor a sweep.

**Fix:** cap `tTxns.length` (return error 51/79-equivalent when exceeded) and add idle-txn eviction to `kgroupSweep` (e.g. drop txns with no AddPartitions within `transaction.timeout`), mirroring Kafka's transaction log reaping.

---

## FLAG 3 — MED — `KGOP_TXN_ADD` silently truncates topic names to 280 bytes: distinct topics collide into one txn partition entry (marker/offset corruption)

**Where:** `case KGOP_TXN_ADD`, the `char[300] tb` block:

```d
size_t tl = topic.length <= 280 ? topic.length : 280;
tb[0 .. tl] = topic[0 .. tl];
tb[tl] = '\x1f';
... snprintf(..., "%d", part)
auto tps = tb[0 .. tl + 1 + pl].idup;
```

**Attack trace:** client A registers partitions for two different long topics sharing a 280-byte prefix (topic length is attacker-chosen, up to the skin's frame limit; `str16` caps at 65535), e.g. `T = "x"*280`:
- AddPartitionsToTxn: `T+"-real"` partition 3, then `T+"-decoy"` partition 3 → both collapse to the same `tps` entry `"x"*280 + \x1f + "3"`.
- On EndTxn (COMMIT), the coordinator returns that single entry to the caller (kafka.d writes commit markers / applies buffered `KgTxnOff`s), so **transactional markers are written for the wrong topic's partition** and a consumer reading `T+"-decoy"` sees aborted records as committed (or vice versa) — a transactional-isolation break, not just noise. The `offs` entries in `KGOP_TXN_OFFSETS` are *not* truncated, so the partition list and the offset list can disagree for the same txn.

**Root cause:** fixed 300-byte scratch with silent truncation instead of rejecting over-length topics (real Kafka caps topic at 249 and errors).

**Fix:** reject `topic.length > 249` with INVALID_TOPIC_EXCEPTION (error 36) instead of truncating.

---

## FLAG 4 — MED (info disclosure, severity conditional) — `KGOP_DESCRIBE` returns every member's metadata, client ids, and assignments with no membership/auth check

**Where:** `case KGOP_DESCRIBE`.

**Attack trace:** `[u8 6][u16 len "g"]` from any connection returns `[state][generation][protocol][members…]{mid, gii, clientId, subscription-metadata bytes32, assignment bytes32}` for the whole group. The subscription metadata blob is the raw consumer-protocol subscription the leader uploaded — including topic names and (in some client stacks) userdata that can carry internal host/selector strings. Whether this is a flag or spec-compliant depends on kafka.d enforcing an ACL before routing to `KGOP_DESCRIBE` — the FSM itself enforces nothing. Also directly feeds FLAG 1 (member-id enumeration).

**Verdict:** FLAG if kafka.d routes DescribeGroups pre-auth/unauthenticated (cannot be confirmed from this file); otherwise SUSPECT. Suggested fix regardless: gate Describe on the skin's read ACL.

---

## SUSPECT (not proven from this file)

- **`KGOP_COMMIT_CHECK` generation-fencing bypass after protocol-mismatch reset:** `closeBarrier` returning false sets `state = ST_EMPTY` **with members still present**; `COMMIT_CHECK` then answers `KG_NONE` on the "no live group" branch without checking `gen` or membership — a stale member could commit offsets under an old generation. Needs a trace through kafka.d's OffsetCommit path to confirm reachability and impact.
- **`KGOP_SYNC` leader re-entry:** while `ST_COMPLETING`, the leader may call SYNC repeatedly with different assignments, overwriting members' assignments after some followers already fetched theirs (split-brain assignment within one generation). Likely benign-ish but violates the single-assignment-per-generation contract; would need a two-follower interleaving to demonstrate.
- **TXN offsets buffer aliasing across txns:** `KGOP_TXN_OFFSETS` overwrites `t.offGroup`/`t.offs` per call (no append), and `TXN_END` clears them — no UAF, but two interleaved TxnOffsetCommit calls silently drop the first. Correctness nit, needs a skin-level PoC.

## Ranked summary

1. HIGH — kafkagroup.d KGOP_LEAVE: no gen/auth on member removal → remote cross-tenant group-kick / rebalance-storm DoS
2. MED — kafkagroup.d KGOP_TXN_INIT/tgroupSweep: unbounded, never-swept `tTxns` → single-connection OOM of the shard
3. MED — kafkagroup.d KGOP_TXN_ADD: 280-byte topic truncation → txn partition collision, commit markers for wrong topic (tx-isolation break)
4. MED* — kafkagroup.d KGOP_DESCRIBE: unauthenticated full group metadata/assignment disclosure (*pending kafka.d ACL check)
5. SUSPECT — COMMIT_CHECK gen-bypass after closeBarrier(false); SYNC leader re-assignment; TxnOffsetCommit overwrite

---

# CTF flags — `mqtt.d`

# GLM-CTF-REPORT.md — MQTT skin (`source/dreads/mqtt.d`)

## FLAG 1 — Last Will is dead code: `fireWill` can never run on real teardown (MED-HIGH, protocol violation / broken security contract)

**Where:** `mqttTeardown()` — first block:

```d
if (c.connected)
{
    atomicOp!"-="(gMqttClientsConnected, 1);
    c.connected = false;          // <-- cleared here
}
...
if (c.connected)                  // <-- ALWAYS false at this point
    fireWill(c);
```

**Attack trace (provable from control flow):**
1. Client W connects: `CONNECT` with `CleanSession=1` (or v5 session-expiry 0), a Will (`willTopic="alarm/door"`, payload `"forced"`), will-delay = 0.
2. Client S subscribes `alarm/#`.
3. W's TCP is killed (RST / cable pull — any abnormal drop).
4. Serve fiber: `waitForData` fails → `mqttParkOrEnd(c, false)`. Eligibility requires `queueHold` (needs sessionExpiry>0 **and** filters) or `willDelayHold` (needs wdEff>0). With expiry 0 and delay 0, `eligible == false` → park returns false → `mqttTeardown`.
5. Teardown sets `c.connected = false` in its *first* statement, so the later `if (c.connected) fireWill(c)` is unreachable. **No will is ever published.**

The only paths that ever fire a will are the *delayed* will in `mqttParkOrEnd`'s timed loop. Every immediate-will abnormal disconnect (the common case: clean session + LWT, or any protocol-error close of a session with a will) silently drops the Will — a dead-man's-switch bypass from the client's perspective.

**Root cause:** the will-fire guard tests a flag that was just cleared three statements earlier.

**Fix:** capture `immutable bool wasConnected = c.connected;` before clearing, and gate `fireWill(c)` on that (willTopic is already the "not yet fired / not cleared" flag — `if (c.willTopic.length) fireWill(c)` alone is sufficient).

---

## FLAG 2 — Unauthenticated per-connection 16 MB RAM pin, ×N connections (MED, remote DoS)

**Where:** `MQTT_MAX_PACKET = 16 << 20` (top constants) + `serveMqttClient` read path: `auto space = inb.freeSpace(avail); c.tcp.read(space); inb.grow(avail);` with `readDeadline = MQTT_CONNECT_TIMEOUT` (30 s).

**Attack trace:** a pre-CONNECT client sends a fixed header whose remaining-length varint decodes to ~16 MB (e.g. `0x30 0xFF 0xFF 0xFF 0x7F`), then trickles body bytes. The serve loop buffers up to 16 MB **before any CONNECT/auth** and holds it for 30 s. No per-IP or global pre-auth buffer budget exists in this file; a few hundred sockets pin gigabytes of heap on every shard thread (each SO_REUSEPORT listener accepts its share). Slow but steady: each connection can also renew the window by holding the read open (keepalive deadline applies only after CONNECT; the pre-CONNECT deadline is a fixed 30 s per read wait — re-arming per partial read extends the hold).

**Root cause:** the max-packet cap is per-packet but there is no aggregate pre-authentication memory admission.

**Fix:** cap the input buffer at a few KB until `CONNECT` completes (a legitimate CONNECT is tiny); only raise the cap to `MQTT_MAX_PACKET` post-auth, and add a global pre-auth buffered-bytes watermark.

---

## SUSPECT (unproven from the code shown — what's needed to confirm)

1. **TLS `fwdProps` aliasing across a fan-out yield (potential cross-client corruption).** `handlePacket` PT_PUBLISH: `static ByteBuffer fwdProps;` is TLS; `props = fwdProps.data` is then passed to `mqttDeliverLocal` **and** `gMqttFanout` (= `shardMqttFanout` → `shardEnqueue`, which **yields** when the SPSC lane is full — see shard.d). If `shardMqttFanout` copies `props` into a fiber-local frame buffer *before* enqueueing (as `shardMqttConnBcast` appears to), the alias window is yield-free and this is safe. If instead it passes `props` as a `shardEnqueue2` segment held across the backpressure yield, a second PUBLISH on the same thread rewrites `fwdProps` mid-yield and client B's properties get spliced into client A's replicated publish. **Confirm by reading the full body of `shardMqttFanout` in server.d.**

2. **TLS `kb`/`rb` in `mqttSessionPut/Exists/Del` across `gMqttExec` hop yields.** `gMqttExec` = `amqpDataExec`, which hops cross-shard and parks on a `ShardPending` (the `kafkaGroupHopImpl` pattern). If `amqpDataExec` builds its hop payload (copying `args`) before its first yield, safe; if it retains the `args` slices (which point into the TLS `kb` static) across the park, a concurrent CONNECT on the same thread rewrites the key mid-hop. Needs `amqpDataExec`'s body.

3. **Cross-shard freeze race → use-after-free of `parked.obox`.** `mqttResumeXShard` waits up to 500 ms for `frozen`, then adopts; the owner's redirect window is only 1000 ms from freeze, after which its teardown runs `c.obox.release()` (malloc-plane free) on the owner thread while the adopter may still be walking `parked.obox.data` cross-thread. With a near-deadline freeze and a large (~64 MB) offline queue, adopt can outlive the window. Confirm by timing instrumented repro: freeze at t≈499 ms, obox ≈64 MB.

4. **Conformance/availability nit (LOW):** `if ((flags & 0x40) && !(flags & 0x80)) return false;` closes v5 connections carrying a password without a username — legal in MQTT 5.0 ([MQTT-3.1.2-22] was removed). Rejects valid v5 clients; no security impact.

---

## Ranked summary

1. **MED-HIGH** mqtt.d `mqttTeardown` — Will never fires on non-park teardown (`c.connected` cleared before the `fireWill` guard); dead-man's-switch contract broken. PROVEN.
2. **MED** mqtt.d read path — unauthenticated 16 MB/conn pre-CONNECT buffering, 30 s hold, unbounded aggregate. PROVEN.
3. **SUSPECT** TLS `fwdProps` cross-yield aliasing in PUBLISH fan-out (needs `shardMqttFanout` body).
4. **SUSPECT** TLS `kb`/`rb` aliasing across `gMqttExec` hop park (needs `amqpDataExec` body).
5. **SUSPECT** cross-shard freeze 1 s window vs slow adopt → UAF on `parked.obox`.
6. **LOW** v5 password-without-username wrongly rejected (conformance).

---

# CTF flags — `amqp10.d`

# GLM-CTF-REPORT.md — AMQP 1.0 skin (`source/dreads/amqp10.d`)

Analysis pass restricted to `amqp10.d` with `shard.d`/`server.d` used for hop reasoning. Flags are proven from the code shown; unproven leads in SUSPECT.

---

## FLAG 1 — SASL is optional client-side: bare protocol header = full auth bypass

- **File:line:** `amqp10Serve` (`amqp10.d`, entry point; the `else a10Send(c, AMQP10_HDR_BARE[])` branch), dispatched from `dreads.amqp` on the 8-byte header.
- **Severity:** HIGH (auth bypass)

**Attack trace (PoC):**
```python
s = socket(); s.connect(("host", amqp_port))
s.send(b"AMQP\x00\x01\x00\x00")     # protocol-id 0 = BARE, not 3 (SASL)
# server immediately echoes the bare header, no SASL exchange happens:
#   -> `else a10Send(c, AMQP10_HDR_BARE[])` and straight into the AMQP layer
s.send(open_frame)                  # PERF_OPEN  -> opened=true
s.send(begin_frame); s.send(attach_sender_to_any_queue)
s.send(transfer_with_secret_payload)  # full publish privileges, zero credentials
```
Internal state: `saslLayer == false` ⇒ the entire SASL block (`a10SaslCheck`, `aclUser`, `aclCheckPassword`) is skipped. Even with `aclUserCount() > 1` (ACL configured and required on every other skin), the AMQP 1.0 port grants full data-plane access. All subsequent management ops (`a10HandleMgmt` PUT/DELETE on `/queues/*`, `/exchanges/*`, `/bindings`) are also unauthenticated — an attacker can delete or purge another tenant's queues.

**Root cause:** the header dispatch treats protocol-id 0 as a legal "skip SASL" mode. In AMQP 1.0 the SASL layer is mandatory before open when the broker advertises it; RabbitMQ refuses a bare header when authentication is required.

**Fix:** when the ACL store is populated (`aclUserCount() > 1`), respond to a bare header with the SASL header + `SASL_MECHANISMS` (force the client into SASL), and close if the client opens without an authenticated SASL outcome. Track an `authed` bit on `A10Conn` and refuse `PERF_OPEN` when it is unset.

---

## FLAG 2 — Disposition range `first=0, last=ulong.max-1` wedges the shard's event loop forever

- **File:line:** `a10HandleDisposition`, `foreach (id; first .. last + 1)`.
- **Severity:** HIGH (remote DoS, single small frame, thread-per-core shard wedge)

**Attack trace:**
```
POST-auth (or via FLAG 1, no auth):
begin(ch=1); attach(receiver link, h=0)   # any client-receiver link
DISPOSITION frame, ch=1:
  role      = true (0x41)
  first     = uint0 (0x43)          -> first = 0
  last      = ulong 0xFFFFFFFFFFFFFFFE
  settled   = true
  state     = accepted (or omitted)
```
Internal state: `role=true` passes the gate; `first=0`, `last=0xFFFF…FE` ⇒ `foreach (id; 0UL .. 0xFFFF…FF)` — up to 2⁶⁴ iterations of `id in ps.unsettled` (an AA lookup that never yields). The connection fiber runs on the shard's event loop with no `yield()` in the loop ⇒ the entire shard stops serving every other connection on it. Note `last = ulong.max` itself wraps to an empty range, but `ulong.max - 1` (or any huge value) is a full wedge; a 30-byte frame kills a core.

**Root cause:** client-controlled `last` is not validated against `first` or bounded by the number of unsettled deliveries.

**Fix:** reject/clamp when `last < first` or `last - first` exceeds the session's unsettled count (`ps.unsettled.length`), or iterate over the AA's keys filtered by range instead of the numeric range.

---

## FLAG 3 — String properties longer than 255 bytes corrupt the stored 0-9-1 record framing

- **File:line:** `a10MapMessage`, tail: `props.appendByte(cast(char) contentType.length); props.append(contentType);` — same for `correlationId` and `replyTo`.
- **Severity:** MED (persistent data corruption in queue records; no memory unsafety because the 0-9-1 readers shown are bounds-checked)

**Attack trace:**
```
attach sender to queue q; TRANSFER with message sections:
  properties(0x73): field 6 content-type = str32 (0xB1) len=0x000003E8 "A"*1000
  data(0x75): "x"
```
Internal state: `contentType.length == 1000`; `cast(char)1000` truncates to `0xE8` (232). The stored props blob is `[flags][0xE8]"A"*1000...` — a consumer parsing with the reader in `a10BuildMessage`/0-9-1 props walk reads length 232 then continues mid-string: every subsequent field (headers table length, delivery-mode, correlation-id) is read from attacker-shifted offsets. The 16-bit `flags` still claims the fields present, so the record is persistently self-inconsistent, and the headers-table u32 read can consume bytes from the message *body*, mis-attributing payload bytes as table length (bounded reads, so disclosure is limited to record-local misalignment, but every consumer of the record misparses). `correlationId`/`replyTo` have the same single-byte length encoding.

**Root cause:** AMQP 1.0 `str32` fields can be up to frame size (1 MiB advertised); the 0-9-1 short-string property encoding uses one length byte with no check.

**Fix:** clamp/refuse content-type/correlation-id/replyTo longer than 255 on the 1.0→0-9-1 mapping (return `amqp:link:message-size-exceeded`-style link error or truncate explicitly), or switch the props encoding to long strings (u32 length) and update readers symmetrically.

---

## FLAG 4 — Unbounded per-connection state: links, sessions and 16 MiB fragment buffers

- **File:line:** `a10HandleAttach` (`ps.links[lk.handle] = lk` — handle is any client u32, no cap), `A10Session` insert on `PERF_BEGIN` (channel-max 1024 advertised but `c.sessions[fchan]` accepts any ushort and duplicates overwrite), `a10HandleTransfer` fragment cap `16 * 1024 * 1024` **per link**.
- **Severity:** MED (remote DoS via memory exhaustion)

**Attack trace:**
```
open; begin(ch=1)
repeat 100 000 times:
   attach(ch=1, handle=i, role=receiver, source="/queues/victim")  # no queue-exists cost — bare-name queues auto-declare
```
Each receiver attach: (a) inserts an `A10Link` (strings, filter arrays), (b) calls `a10ConsumerInc`/`a10PrioAdd`, and (c) **spawns a delivery fiber** (`a10StartDelivery`) that lives until detach, sleeping 1 ms per loop — 100 k parked fibers + 100 k AA entries from a few MB of wire traffic. On sender links, start N transfers with `more=true` and stop: each link holds up to 16 MiB in `plk.pending` ⇒ N×16 MiB retained with no global per-connection cap and no idle reclaim (fragments are only freed on the next `more=false` or detach).

**Root cause:** per-link/per-session limits exist (16 MiB per link, 2048 session window) but there is no per-connection link-count cap, no per-connection aggregate fragment-memory budget, and `A10Link.pending` is never aged out.

**Fix:** cap links per session/connection (e.g. 256), cap aggregate `pending` bytes per connection (e.g. 64 MiB, detaching the largest link when exceeded), and bound `c.sessions` to the advertised channel-max.

---

## SUSPECT (unproven from the code shown — needs `amqp.d` / `amqpDataExec` body to confirm)

- **S1 — TLS-static aliasing across the cross-shard hop yield (possible record corruption):** `a10HandleTransfer` builds the publish payload in `static ByteBuffer props / bodyBuf` (TLS) and passes `props.data` into `a10Publish` → `gAmqpPush` → `amqpDataExec`. `kafkaGroupHopImpl` (server.d) documents that a hop *parks the fiber* and explicitly re-clears a caller-shared TLS static afterward — evidence that `amqpDataExec` can yield with caller slices live. If `amqpDataExec`'s cross-shard path copies `args` only after acquiring the pending (or re-reads them after the park), a second AMQP 1.0 connection served by the same thread can clobber `props` during the park, and the first publish hops with/returns corrupted bytes. Same pattern for `static ByteBuffer sb` in the stream-offset timestamp scan and for `gAmqpPeekAt`'s internal `static ByteBuffer rbk2` (two delivery fibers peeking concurrently on one thread). **To confirm:** read `amqpDataExec`'s argument-copy point relative to `acquireShardPending`/`pnd.done.wait`, and whether `gAmqpPeekAt` clears `rbk2` after the park like `kafkaGroupHopImpl` does.
- **S2 — `a10SaslCheck` "default" fallback:** a PLAIN response with fewer than two NULs falls back to user `"default"` with an empty password; if an ACL defines `default` with an empty/weak password this is a trivial login. Confirm `aclCheckPassword` rejects empty passwords.

---

## Ranked summary

```
1  HIGH  amqp10Serve (header dispatch)        Auth bypass: bare "AMQP\x00\x01\x00\x00" header skips SASL entirely; full publish/mgmt access with zero credentials
2  HIGH  a10HandleDisposition                  DoS: disposition first=0 last=ulong.max-1 loops 2^64 AA lookups without yielding — wedges the whole shard event loop
3  MED   a10MapMessage (props tail)            Corruption: >255-byte content-type/correlation-id/replyTo truncate the 0-9-1 length byte — persistently mis-framed queue records
4  MED   a10HandleAttach / a10HandleTransfer   DoS: no link-count cap (each receiver attach spawns a fiber) and 16 MiB fragment buffer PER LINK — unbounded memory per connection
S1 SUSPECT a10HandleTransfer static props/bodyBuf + amqpDataExec hop  TLS-static payload aliasing across the hop-park yield (needs amqp.d to confirm)
S2 SUSPECT a10SaslCheck                        "default"/empty-password fallback on malformed PLAIN response
```

---

# CTF flags — `amqp.d`

# GLM-CTF-REPORT.md — dreads AMQP 0-9-1 skin (`source/dreads/amqp.d`)

Two PROVEN flags (one CRITICAL auth bypass, one frame-length integer overflow) plus substantiated SUSPECTs on the TLS-buffer-aliasing class. Line numbers refer to the target file as provided.

---

## FLAG 1 — CRITICAL: Full data-plane access with zero authentication (handshake state never enforced)

**Where:** `serveAmqpClient` / `handleFrame` — there is no "authenticated" / "tuned" / "opened" state machine. `c.aclAuth` is only *set* at start-ok (case 10/mth 11) and only *consulted* at connection.open (vhost grant). No `basic.*`, `queue.*`, or `exchange.*` path checks handshake progress or ACL user.

**Attack trace (exact client bytes):**

```
1. "AMQP\x00\x00\x09\x01"                      # protocol header; server sends Connection.Start, we ignore it
2. frame: type=1 chan=1 {cls=20 mth=10}         # channel.open on ch 1  -> open-ok (case 20/10: no auth gate)
3. frame: type=1 chan=1 {cls=50 mth=10, queue="victim", flags=0}   # queue.declare -> declare-ok
4. frame: type=1 chan=1 {cls=60 mth=40, ex="", rk="victim"}        # basic.publish
5. content header + body                                          # message lands in amq.q.victim
6. frame: type=1 chan=1 {cls=60 mth=20, queue="victim", noack=1}  # basic.consume -> receive everything
```

The client never sends start-ok. Internal state: `c.aclAuth == null` (never assigned). Every gate that exists keys on `aclAuth !is null`:

- connection.open vhost check: `if (aup !is null && ...)` — null passes (and we skipped open anyway);
- `aclOff = aclUserCount() <= 1` legacy accept-any is never even reached.

Result: with a fully configured ACL (multiple users, passwords, vhost grants), an unauthenticated TCP peer gets publish/consume/declare/delete on every queue — full cross-tenant read and write. This defeats the entire `dreads.acl` layer for the AMQP skin.

**Root cause:** `handleFrame` dispatches class 40/50/60/85/90 on any channel with no per-connection lifecycle flag; channel.open (cls 20) is likewise ungated.

**Fix:** add `enum phase { hdr, start, tune, open, ready }` to `AmqpConn`; reject (connectionClose 501/503) any frame not on the allowed phase; set `ready` only after connection.open, and make open mandatory after a successful start-ok/tune-ok. Additionally gate data methods on `phase == ready`.

---

## FLAG 2 — HIGH: u32 integer overflow in the frame-size check bypasses frame-max (unbounded per-connection buffering)

**Where:** serve loop:

```d
immutable fsize = (cast(uint) d[pos+3] << 24) | ... | d[pos+6];
if (fsize + 8 > c.frameMax)   // fsize is uint; 8 is int -> uint arithmetic, WRAPS
```

`fsize + 8` is evaluated in 32-bit unsigned. For `fsize ≥ 0xFFFFFFF8` the sum wraps to ≤ 7, so the `> c.frameMax` test passes for a nominally ~4 GiB frame. The next guard (`d.length - pos < 7 + cast(size_t) fsize + 1`) is size_t-safe, so the loop just keeps waiting for data — and the read path appends every byte the client sends into `inb` with no cap.

**Attack trace:** send the 8-byte header, complete tune-ok normally (frameMax negotiated to 4096 or default), then send one frame header `01 00 00 FF FF FF FF F9` followed by an endless body stream. The broker buffers everything (multi-GB) in `inb` despite the advertised 131072-byte cap; when 4 GiB−7 bytes finally arrive, the frame is parsed as one method payload. N connections × N GB = OOM kill of the broker. The comment "refuse instead of buffering an attacker-chosen u32 of bytes" is defeated by the wrap.

**Root cause:** mixing `uint fsize` with the small-int constant before widening.

**Fix:** `if (cast(ulong) fsize + 8 > c.frameMax)` — or check `fsize > c.frameMax - 8` after clamping `c.frameMax ≥ 4096` (already guaranteed).

---

## FLAG 3 — HIGH (provable aliasing of *record* bytes is guarded; the *key* bytes are not) — cross-queue record injection via `deadLetter`'s TLS `kb5` across the `gAmqpPush` reply-wait

**Where:** `deadLetter` sink:

```d
routeTo(meta.dlx, rk, ..., (q) nothrow {
    static ByteBuffer kb5;              // TLS, shared by ALL fibers on this shard thread
    queueKey(q, kb5);
    if (gAmqpPush !is null)
        gAmqpPush(kb5.data.asChars, blobc);   // gAmqpPush -> amqpDataExec -> HOPS and PARKS for the reply
});
```

`gAmqpPush` (server.d) is the *ack-waiting* variant (`amqpDataExec` with a pending + `while (!pnd.ready) wait`). The park inside it is a yield point at which **any other fiber on the same shard thread** (a consumer fiber's deadLetter, another connection's `settleNegative`, the TTL reaper, a second `deadLetter` in the same fan-out to a second DLX-bound queue) executes `queueKey(q', kb5)` and **rewrites the very buffer whose slice `gAmqpPush` is still using as the hop's key argument** — `args[1]` slices TLS memory, exactly the class the file itself documents (`kafkaGroupHopImpl` re-clears its *reply* after the park "may be a caller-shared TLS static that another fiber wrote to while we were parked", and every `queue.delete`/`purge`/`get` path was stack-copied for this reason). Note the record half (`blobc` → `dlrecStatic`) IS guarded with `dlrecBusy`, but the **key** `kb5` has no such guard, and neither do `kb4` (settleNegative), `kb6` (requeueAllUnacked), `kbT` (commitTx), `kb10` (a10Publish).

**Attack trace (shards ≥ 2):** victim client A rejects (nack, requeue=false) a message on queue `Q` whose DLX routes cross-shard; concurrently attacker client B (same shard thread as A, trivially arranged by connection count) hammers `queue.declare`/nack-requeues on queue `R`. Fiber interleaving: A's sink runs `queueKey("R2"…)`? — precisely: A's sink fills `kb5="amq.q.dlq"` and parks inside `gAmqpPush`; B's `settleNegative` runs `queueKey(u.queue, kb4)`… `kb4` is distinct, but B's own `deadLetter`/`gAmqpPushFront` paths that share `kb5` (any deadLetter on B's side) rewrite `kb5="amq.q.attacker"`; A resumes/continues its fan-out to the *next* DLX destination using the clobbered key → **A's dead-lettered message is RPUSHed into the attacker's queue** (cross-client disclosure), or a benign queue's record is injected into another tenant's queue.

Confirmation needs only the invariant "amqpDataExec may yield while `args` still reference the caller's buffer" — which the file's own comments assert as a live hazard class. I rank it HIGH and provable-by-construction given that invariant; if `amqpDataExec` provably deep-copies `args` before its first yield it downgrades to defense-in-depth.

**Fix:** stack-copy the key before `gAmqpPush`/`gAmqpPushFront` at every site (the `delKeyStore` pattern already used in delete/purge/get), or make `gAmqpPush*` take an owned copy.

---

## SUSPECT (unproven from the code shown)

- **`requeueAllUnacked` / `settleNegative` record buffers (`rq6`, `rq4`) alias across the `gAmqpPushFront` park** — same class as Flag 3 but for the *payload* half; `markRedelivered(rq6, u.blob)` then `gAmqpPushFront(kb6, rq6)` parks, and a second dying connection on the same thread rewrites `rq6`. Needs the same amqpDataExec copy-semantics confirmation as Flag 3.
- **`gAmqpPush`'s own `static ByteBuffer rb` reply TLS (server.d)** shared by concurrent same-thread callers — replies could interleave, but each caller reads `rb` only after its own wake; a woken caller may read the *other* caller's reply if both park on different pendings and the second completes first. Needs amqpDataExec body to confirm per-call isolation.
- **`basic.get` returns `sleep(2.msecs)` per call whenever the queue has a live consumer** — a cheap per-request latency/stall amplifier (DoS-lite), not a deadlock.
- **`amqpRegEnsure`'s `while (gAmqpRegMutex is null) {}`** — if `new Mutex` ever throws, every subsequent AMQP connection spins that thread forever. OOM-only trigger.

---

## Ranked summary

1. CRITICAL — amqp.d handleFrame: no handshake/auth state machine — pre-start-ok client gets full queue/publish/consume access, defeating configured ACLs.
2. HIGH — amqp.d serve loop: `fsize + 8` uint wrap bypasses frame-max → unbounded per-connection input buffering (OOM DoS).
3. HIGH — amqp.d deadLetter (kb5) + settleNegative/requeueAllUnacked/commitTx/a10Publish: unguarded TLS key buffers alias across the gAmqpPush hop park → cross-queue record injection / disclosure.
4. SUSPECT — rq4/rq6 payload TLS aliasing across gAmqpPushFront parks; gAmqpPush shared reply buffer rb; per-get 2ms sleep amplification; amqpRegEnsure OOM spin.

---

# CTF flags — `kafka.d`

# GLM-CTF-REPORT.md — `dreads` Kafka skin (source/dreads/kafka.d)

Scope: `kafka.d` (+ routing context from `server.d` / `shard.d`). Three PROVEN flags, one strongly-evidenced flag, suspects listed at the end.

---

## FLAG 1 — SASL/PLAIN "legacy accept-any" becomes superuser / ACL privilege escalation

**File:line:** `kafka.d`, `kafkaPlainCheck()` (in the SASL block, ~`private bool kafkaPlainCheck(...)`); interacts with `isSuperUser()` and `authorize()`.

**Severity:** CRITICAL (auth bypass / privilege escalation when Kafka ACLs are in use)

**Attack trace:**

Preconditions (defaults): `gKafkaRequireSasl = false`, one seeded ACL user (the default install), `kafka-super-users = "User:admin"`, and ACL enforcement active (`gKafkaAclActive > 0`, set by `kafkaAclPrime()` or any prior CreateAcls).

1. Attacker connects to port 9092 and sends SASL Handshake (apiKey 17) with mechanism `PLAIN`.
2. Attacker sends SASL Authenticate (apiKey 36) with token `\0admin\0anypasswd` (authcid = `admin`, any password).
3. `kafkaPlainCheck` hits:
   ```d
   if (aclUserCount() <= 1)
   {
       // legacy accept-any — still RECORD the claimed principal for ACLs
       ctx.principalBuf[0 .. auser.length] = auser;  // "admin"
       ...
       ctx.authed = true;
       return true;
   }
   ```
   The password is **never checked**; the claimed principal is recorded as authoritative.
4. Every subsequent `authorize(ctx, ...)` builds `princForm = "User:admin"` and `isSuperUser("admin")` matches the `kafka-super-users` entry → **all checks bypassed**.

Even without a super-user configured, the attacker can claim *any* principal that holds an ALLOW binding, defeating the entire SimpleAclAuthorizer model (`authorize()` matches on `b.principal == princForm`, and the principal is attacker-chosen, unauthenticated).

**Root cause:** the legacy accept-any branch was written before ACL enforcement keyed off `ctx.principal`; the two features compose into identity spoofing.

**Fix:** when `gKafkaAclActive > 0` (or whenever `kafka-super-users` is non-empty), never take the accept-any branch — require a real password match; or in the accept-any branch force `principalLen = 0` so the principal is `ANONYMOUS` for enforcement purposes.

---

## FLAG 2 — Admin / group APIs bypass Kafka ACL enforcement entirely

**File:line:** `handleRequest` dispatch; `authorize()` is called **only** in `handleProduce` (KOP_WRITE), `handleFetch` (KOP_READ), and `handleJoinGroup` (KRES_GROUP/KOP_READ). No `authorize()` in: `handleCreateTopics`, `handleDeleteTopics`, `handleCreatePartitions`, `handleDeleteRecords`, `handleIncrementalAlterConfigs`, `handleAlterConfigs`, `handleCreateAcls`, `handleDeleteAcls`, `handleDescribeConfigs`, `handleOffsetCommit`, `handleOffsetDelete`, `handleDeleteGroups`, `handleEndTxn`/txn family.

**Severity:** HIGH (ACL bypass; destructive admin ops and self-grant)

**Attack trace:**

Preconditions: ACLs active (`gKafkaAclActive = 1`), `gKafkaRequireSasl` default **false** (so no authentication is even needed — the anonymous context reaches the dispatcher), and a DENY-everything or restricted binding set for anonymous.

1. Attacker (no SASL at all — the SASL gate in `handleRequest` is skipped because `gKafkaRequireSasl == false`) sends **DeleteTopics (apiKey 20, v1)** naming victim topics `t1..t64`.
   - `handleDeleteTopics` → no `authorize` → `registeredTopicPartitions` succeeds → per partition `["del", "kafka.t.<t>.<p>"]` via `gKafkaExec` — **all victim data destroyed**, error `E_NONE` returned.
2. Alternatively, send **CreateAcls (apiKey 30, v1)** with binding `{rtype=2(TOPIC), pattern=LITERAL, name="*", principal="User:*", op=ALL(1), perm=ALLOW(3)}`.
   - `handleCreateAcls` → no `authorize` → HSETs the wildcard-ALLOW binding into `kafka.acls`. From then on `authorize()` allows everyone everything — **permanent, AOF-persisted** self-grant.
3. Alternatively, **DeleteAcls (31)** removes a victim's DENY bindings, or **OffsetCommit (8)** forges/destroys any group's committed offsets (`handleOffsetCommit` writes `kafka.cg.<group>` with no KRES_GROUP check even though JoinGroup has one), or **DeleteGroups (42)** wipes another tenant's offsets.

**Root cause:** enforcement was added only to the data-plane hot paths; the admin/coordinator APIs were never gated.

**Fix:** add `authorize(tKafkaCtx, ...)` checks (KOP_CREATE/DELETE/ALTER/ALTER_CONFIGS on KRES_TOPIC/CLUSTER; KRES_GROUP ops in OffsetCommit/OffsetDelete/DeleteGroups) at the top of each handler, and require the create/delete-ACL ops to be super-user only.

---

## FLAG 3 — Cross-client data disclosure in OffsetFetch: TLS metadata buffer consumed *after* a cross-shard hop

**File:line:** `kafka.d`, `handleOffsetFetch` (classic, ~line of the `static ByteBuffer mb;` local) and `handleOffsetFetchFlex` (`static ByteBuffer mbf;`); the clobbering hop is `fetchGroupEpoch()` → `gKafkaExec` → `amqpDataExec` → cross-shard pending wait (`kafkaGroupHopImpl`-style park in `server.d`).

**Severity:** HIGH (cross-tenant disclosure of arbitrary attacker-stored bytes)

**Attack trace** (two connections A and B on the *same shard thread*, sharded mode so the group's `kafka.cg.*` key lives on another shard):

1. Client A commits metadata it wants to exfiltrate-verify, e.g. OffsetCommit (apiKey 8) for group `gA`, topic `t`, partition 0, metadata = `SECRET-A` (arbitrary bytes, up to request-frame size).
2. Client B sends OffsetFetch (apiKey 9) for group `gB`, topic `t`, partition 0:
   ```d
   static ByteBuffer mb;                                   // TLS, SHARED
   immutable hasMeta = off >= 0 && fetchGroupMeta(group, topic, part, mb);   // fills TLS mb
   putI32(o, part);
   putI64(o, off);
   if (ver >= 5)
       putI32(o, off >= 0 ? fetchGroupEpoch(group, topic, part) : -1);       // <-- HOPS, PARKS fiber
   ...
   if (hasMeta)
       putStr(o, cast(const(char)[]) mb.data);            // <-- reads TLS mb AFTER the park
   ```
3. While B is parked inside `fetchGroupEpoch`'s cross-shard hop, client **A**'s OffsetFetch for `gA` runs on the same thread and its `fetchGroupMeta` **overwrites the same TLS `mb`** with `SECRET-A`.
4. B resumes and emits `SECRET-A` as *its own* committed_metadata — B receives another client's data. The identical pattern exists in the flex path (`mbf` + `fetchGroupEpoch` between fill and `putCStr(o, mbf.data)`).

The codebase's own convention ("TLS: consumed before the next hop" — see `partBase`, `kafkaGroupHopImpl`, `handleEndTxn`'s stack copies) is violated exactly here.

**Root cause:** a TLS static `ByteBuffer` held live across a fiber-switching data-plane hop.

**Fix:** copy the metadata to a stack buffer (bounded, e.g. `char[256]`) immediately after `fetchGroupMeta`, or reorder so `fetchGroupEpoch` is called before `fetchGroupMeta`.

---

## FLAG 4 — `handleMetadataFlex`: TLS `allBuf` topic slices used across `registerTopic` hops → registry corruption with another connection's topic names

**File:line:** `kafka.d`, `handleMetadataFlex` — `static ByteBuffer allBuf;` (HKEYS reply) → `topics[nt++] = m` (slices into `allBuf`) → later `foreach (t; topics[0 .. nt]) if (validTopic(t)) registerTopic(t);` where `registerTopic` → `gKafkaExec` → cross-shard hop that **parks the fiber**.

**Severity:** MED-HIGH (cross-tenant state corruption of `kafka.topics` / wrong data-plane keys; memory-safe)

**Attack trace:**

1. Client A sends Metadata v9+ with `topics = null` (all-topics). `allBuf` receives the HKEYS reply; `topics[]` is filled with slices into `allBuf`.
2. The auto-create loop begins `registerTopic(topics[k])`; each call hops and parks.
3. While A is parked, client B (same thread) sends an all-topics Metadata; its `handleMetadataFlex` **clears and refills the same TLS `allBuf`** with B's (attacker-chosen) topic names.
4. A resumes and continues the loop: `topics[k+1..]` now alias B's buffer contents — the broker **registers B's topic names (or byte-garbage from the reused buffer) into `kafka.topics`** on A's behalf, and can validate `validTopic` against clobbered bytes. Result: phantom/cross-tenant topics in the registry, wrong partition-count attributions, AOF-persisted.

Note the classic `handleMetadata` was explicitly fixed to a *stack* window for exactly this hazard ("STACK-local (not TLS static)… would be clobbered by another connection during the park") — the flex path reintroduced it via `allBuf`.

**Fix:** copy the HKEYS member strings into a per-call stack arena (bounded, e.g. 512×~260B, or `idup` into a fiber-local buffer) before the register loop, as the classic path does.

---

## SUSPECT (unproven from shown code)

- **`pushRecords` argv/blobArena TLS under ring backpressure** (`handleProduce` staging + `pushRecords`'s `static const(char)[][] argv`): if `amqpDataExec` ever parks *before* copying `argv`/`blobArena` contents into the SPSC ring (e.g. ring-full `yield()` in `shardEnqueue` before a successful `push2`), a sibling produce fiber on the same thread clobbers `blobArena` and the stored records corrupt. Need `amqpDataExec`'s exact copy-before-first-yield ordering to confirm; if it copies args into the request encoding before any park, this is safe.
- **`handleAddPartitionsToTxn` unbounded `req` staging**: up to 65536×65536 topic/partition entries appended into the TLS `req` buffer (~16+ MB per request from a 12-byte prefix) — memory-amplification DoS candidate; check `ByteBuffer` OOM policy (drops client vs aborts).
- **`joinLoop`/`syncLoop` 70 s hold**: a client opening many parallel JoinGroup connections pins serve fibers for 70 s each — connection/fiber exhaustion DoS; measure fiber stack/RSS budget before flagging.
- **acks/`gAmqpAofFlush` cross-shard durability**: the serve loop flushes *this* shard's AOF before replying, but a hopped RPUSH's owning-shard AOF flush relies on the owner's drain ordering; verify the owner flushes before `ShardMsg.reply` on every path (server.d comments claim it does).

---

## Ranked summary

1. CRITICAL — kafka.d `kafkaPlainCheck`: PLAIN accept-any records attacker-chosen principal → super-user/ACL escalation when `aclUserCount()<=1`.
2. HIGH — kafka.d admin/group handlers: no `authorize()` on CreateTopics/DeleteTopics/CreateAcls/DeleteAcls/DeleteRecords/AlterConfigs/OffsetCommit/DeleteGroups → full ACL bypass + destructive ops unauthenticated.
3. HIGH — kafka.d `handleOffsetFetch`/`handleOffsetFetchFlex`: TLS `mb`/`mbf` read after `fetchGroupEpoch` hop → cross-client committed-metadata disclosure.
4. MED-HIGH — kafka.d `handleMetadataFlex`: TLS `allBuf` slices across `registerTopic` hops → cross-tenant topic-registry corruption.
5. SUSPECT — pushRecords/blobArena TLS clobber under ring backpressure; AddPartitionsToTxn req amplification; 70 s join fiber pinning; cross-shard ack-before-AOF ordering.
