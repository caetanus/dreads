# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

# sqs.d — Round 5 review

## Flags

**None.** No new provable memory-safety / disclosure / corruption / authz defect found in `source/dreads/sqs.d`; the round-4 fixes verified airtight:

- **Receive path** (`opReceiveMessage`): `qkey/ifkey/grpkey` are per-call `ByteBuffer`s, the FIFO snapshot is copied into per-call `snapCopy` *before* any parking `exec` (sismember/lrem/sadd/hset), `recs[]`/`batchGroups[]`/`g2` all point into `snapCopy` (per-call), and the delivered `rec` is copied to per-call `recCopy` before the `hset` reuses `rb`. No slice into a shared TLS survives a park.
- **`jsonStr`'s TLS `ub`**: every body is copied out per-call (`bodyBuf` in `opSendMessage`, `ebodyBuf` in `opSendMessageBatch`) before `sendOne` parks; raw slices (`name`, `dedup`, `group`, `handle`, `id`) used as later `exec` *arguments* are serialized into the hop payload before the park (args-passed-to-hop pattern — refuted class).
- **`sqsVisibilitySweep` statics** (`rb/ifkey/qkey/namesCopy/expHandles/expRecs/dueIds/dueRecs/expDedup/gk/dlkey/ddkey`): each is written/read only by the sweep's own fiber; the collect-then-act structure keeps the packed records (`expRecs`/`dueRecs`) untouched while the act-loop's `exec`s park (replies land in `val`/`dlall`, different statics), and each queue's `hgetall` clobbers `rb` only after the previous queue's iteration fully completed. The `\x1f` length-prefix packing bounds are checked (`pi+4`, `pi+rl`).
- **256 KiB body cap** enforced on both single-send and batch entries *before* `jsonStr`'s 256 KiB `ub` decode, with Failed-array reporting for batch.
- `splitRecord` cannot OOB on short records (`f` elements default to null; `nf < 3` cap).
- `findKey` is depth-1/structure-aware — a `"QueueUrl"` substring inside a body value or nested attributes can no longer spoof a field.
- `queueFromUrl` yields attacker-controlled queue names, but every downstream use is length-delimited RESP args against a prefixed key — no RESP injection, no key aliasing (names can't contain `/`-free collisions with other prefixes since prefixes are fixed literals).

## SUSPECT (unproven, not flags)

- `respEachBulk` (`sqs.d` ~line 1075): after `i++` past `$`, `d[i] == '-'` can read one byte past the end if a reply ends immediately after `$`. Every caller feeds broker-generated, well-formed RESP from `amqpDataExec`, so no attacker-controlled truncation path exists — would need a way to make the data plane emit a bare `$` at buffer end. DoS-only at worst, currently unreachable.

## Backlog (non-flag, one line each)

- `opReceiveMessage` FIFO scan caps at 1024 snapshot records (`recs[32]→recs[1024]`) and silently drops the tail — very deep FIFO queues under-deliver per call (correctness/DoS-hardening, not memory safety).
- `onConn` serves exactly one request per connection (no keep-alive) and re-reads headers with a stale `hend` in the body loop's exit condition — cosmetic.

**stdout summary:**
```
[R5] sqs.d: no new provable defect; round-4 fixes verified airtight (per-call keys/snapshot, body cap, sweep statics scoped, splitRecord bounded)
SUSPECT: respEachBulk 1-byte OOB past '$' at reply end — broker-generated RESP only, unreachable
backlog: FIFO snapshot truncation at 1024 recs (under-delivery, DoS-hardening)
```

---

# CTF flags — `kafkagroup.d`

## Round-5 review: `source/dreads/kafkagroup.d`

### Verdict: **no new provable memory-safety / disclosure / corruption / authz defect** in this file.

What I checked and why each candidate is closed:

1. **Decoder bounds (KgRd)** — `u8/i32/str16/bytes32` all bounds-check against `p.length` and set `ok=false` on truncation; every consuming path either checks `r.ok` or breaks before use. `bytes32`'s `i + n > p.length` on size_t cannot overflow for u32 n. No OOB read.

2. **TLS-across-hop aliasing** — `kgroupApply` is invoked either directly (same-shard, synchronous) or in the owner's drain on a live ring slice; it contains **no yields/parks** (all `.idup` copies for retained strings: `pnames/pmetas/gii/clientId/assignment/mid/grp/e.topic/e.meta`). Nothing retains a slice of `req` past return. This is the airtight version of the pattern the charter warns about — no flag.

3. **TXN_ADD stack buffer** (`char[300] tb`): `topic.length ≤ 249` and `0 ≤ part ≤ 1_000_000` are both enforced *before* `tb[tl] = '\x1f'` and `snprintf(tb.ptr+tl+1, 300-tl-1, ...)` (≥50 bytes for a ≤7-digit int). No overflow. TXN_END's re-parse of `topic\x1fpart` splits safely with `sep = x.length` default.

4. **`emitJoinOk` / `KGOP_DESCRIBE` null-deref (`auto m = id in g.members; m.gii`)** — could crash only if `g.order` held an id absent from `g.members`. I traced every mutation site: JOIN-new, KIP-394 registration, LEAVE (removes from both), `closeBarrier` (rebuilds order from members), `evictStale` (removes from both) — the order⊆members invariant holds everywhere. Both emit sites are additionally guarded by the post-`closeBarrier` `(useMid in g.members) is null` check. No flag.

5. **Allocation caps** — members 512, groups 4096, txn tps 8192, offs 16384, meta budget 8 MiB, topic-count 64/4096 clamps, idle-txn eviction at `TXN_MAX_TIMEOUT_MS`. The O(N²) tps dedup was already replaced by the hash set. `TXN_OFFSETS` stops buffering at the meta budget.

6. **Auth/confusion** — txn ops check `pid+epoch`; pid is CSPRNG (round-4 fix). TXN_INIT epoch-bump by anyone knowing the transactional.id mirrors real Kafka's transactional.id-as-credential model and is not a cross-tenant bypass given the pid fence. Static-membership gii reclaim and client-supplied `member.id` takeover match Kafka semantics.

**Backlog (non-flag, one line):** KGOP_TXN_INIT lets any client that knows a victim's transactional.id fence-DoS them via epoch bump (parity with real Kafka; consider an ACL on txn ids if multi-tenant).

**Summary:** kafkagroup.d — no new provable defect; round-1..4 fixes verified as holding.

---

# CTF flags — `mqtt.d`

## Round-5 findings — `source/dreads/mqtt.d`

### FLAG 1 (HIGH) — Cross-connection shared TLS read buffer on the MQTT-over-WebSocket path: one client's wire bytes decoded on another client's connection (identity/ACL confusion + stream corruption)

**File:line:** `source/dreads/mqtt.d`, inside `serveMqttClient`'s read loop, WS branch:
```d
static ubyte[65536] wsread = void;   // <-- TLS static, ONE instance per THREAD
...
try {
    rn = c.tcp.read(wsread[0 .. (avail < wsread.length ? ... )], IOMode.once);
}
...
if (!c.wsCodec.feed(wsread[0 .. cast(size_t) rn]))
```

**Root cause.** `wsread` is a function-local `static`, i.e. thread-local — but a *single* buffer shared by **every WebSocket MQTT connection served on that shard thread*. It is handed to `c.tcp.read(..., IOMode.once)`, which **parks the fiber until socket data arrives**. While fiber A is parked inside its read with `wsread`'s address as the destination, fiber B (a different client, same thread) enters the same branch and issues its own `tcp.read` on the *same buffer*. The event loop completes each read by copying into the buffer pointer each caller supplied — both point at `wsread`. Whichever completion lands last clobbers the other's bytes; each fiber then resumes with its own `rn` but the *shared* buffer contents, and feeds `wsread[0..rn]` into **its own** `wsCodec`.

Note this is precisely the defect class commit c694d96 fixed for the WS *handshake* (`reqbuf`/`respbuf` made per-connection) — the post-handshake read buffer was left static. It is NOT the refuted args-passed-to-a-hop pattern: here the buffer is *used by a blocking read across the yield*, and a sibling connection concurrently uses the same memory — the clobber happens while both fibers are parked.

**Attack trace / PoC.**
1. Shard thread T serves MQTT-over-WS (port `mqttWsPort`). Client A connects, completes the WS handshake, sends a partial WebSocket frame and stalls → A's fiber parks inside `tcp.read(wsread, once)`.
2. Client B connects to the same port (SO_REUSEPORT may hash it to T; with a single shard it always does), handshakes, and sends its MQTT `CONNECT` carrying **B's username/password** as a WS binary frame.
3. B's read loop calls `tcp.read(wsread, once)` — same TLS buffer. The driver writes B's CONNECT bytes into `wsread`; B's fiber resumes, `rn_B` correct → fine for B.
4. A's stalled bytes arrive; A's pending read completes, writing over `wsread` (or completes *before* B's resume, in which case B's completion clobbers A's). One of the two fibers resumes with the other client's bytes under its own `rn`.
5. Outcome A (identity confusion): A's `handlePacket` parses **B's CONNECT on A's connection** → A's `MqttConn` is authenticated **as B's ACL user**; A subsequently publishes/subscribes with B's privileges (`c.aclUser = B's user`). A will-send/B's subscriptions are also installed on A's conn.
   Outcome B (disclosure/corruption): B's decoder consumes A's frames → A's SUBSCRIBE executed on B's session, A's credentials parsed on B's connection (credential confusion), streams desynchronize → protocol-error DoS.

Both outcomes are reachable from wire bytes only; no privileges needed (this is pre-auth for a fresh A).

**Severity:** HIGH — cross-client disclosure / authorization confusion on a sharded listener; also a stream-corruption DoS.

**RCE assessment:** DoS-ONLY / identity-confusion, not memory corruption — the injected bytes still pass through the bounded MQTT/WS decoders; there is no write primitive beyond the shared buffer itself (two kernel/loop writes into the same 64 KiB region, both within bounds).

**Suggested fix.** Make the read scratch per-connection, exactly as the handshake buffers were: either a stack `ubyte[65536]` local (non-`static`) in the readloop — it is only live across the single read — or a `ubyte[]` member of `MqttConn` (grow-once). The sibling `static ByteBuffer plain` in the same block is safe as-is (cleared, filled and consumed with no suspension in between), but should be made per-conn too for uniformity.

---

### Verified-clean notes (checked, not flags)

- `static ByteBuffer fwdProps` (PT_PUBLISH) and the `pkt/pktV5/q1/sidBuf` statics in `mqttDeliverLocal`: no suspension point between fill and last use; the fan-out hop copies into the SPSC ring before returning. Refuted-class; still clean.
- SUBSCRIBE's `static granted/filters/retainOk[64]`: no yield in the parse/retained-replay window.
- `mqttSessionExists/Put/Del` TLS statics `kb/rb`: the hop's reply is (re)cleared+appended by the caller's own fiber *after* the park with no yield before the read, so cross-fiber aliasing does not corrupt the value actually consumed; worst case is none. No defect.
- `msgExpiryFromProps`, `expiryValueOffsetInPacket`, `stripExpiryProp`, all v5 property parsers: bounds-checked on every read; CONNECT keepalive truncation check present (prior fix verified).
- `takeoverLocal` per-call `db`, WS `reqbuf/respbuf` per-connection: round-4 fixes verified in place.
- Known-deferred items (xshard-adopt-lifetime UAF, double-session, FIFO-atomic) not re-raised.

### DoS-hardening backlog (one line, not flags)
- `mqttLingerClose`'s 500 ms drain window and the ~1 s redirect window in `mqttParkOrEnd` are per-conn, but a flood of protocol-error v5 closes pins fibers+512 B each — bounded by conn count; backlog only.

---

# CTF flags — `amqp10.d`

# Round-5 CTF findings — `source/dreads/amqp10.d`

## FLAG 1 — HIGH — `a10HandleDisposition`: modified-state annotation bytes are a slice of the shared TLS read buffer, re-read **after** data-plane hops park → cross-client disclosure / message corruption

**File:line:** `source/dreads/amqp10.d`, `a10HandleDisposition` — the slice is taken at
`modAnnBytes = ann.bytes;` (inside the `nf >= 5` / `state == 0x27` parse), and the
post-hop re-reads are the two loop bodies
`auto md2 = A10Dec(modAnnBytes); foreach (mi; 0 .. modAnnCount / 2) …`
executed on iterations **2..N** of `foreach (id; c.dispScratch)`.

### Root cause
`a10ReadFrame` decodes every inbound frame into ONE thread-local scratch buffer:

```d
static ubyte[] buf; // TLS scratch (consumed before return; no yield holds it)
...
body_ = buf[skip .. rest];
```

That comment's invariant ("no yield holds it") is violated on the disposition path.
`modAnnBytes` (and `modAnnCount`) point into `buf` via `fields.p` → `stv.bytes` →
`ann.bytes`. The disposition loop then walks `c.dispScratch` and, for each unsettled
delivery in `[first,last]`, calls `a10RequeueAnn` / `a10Requeue` / `a10Reject` — all of
which reach the data plane (`amqpDataExec` / `gAmqpPushFront`) and **park on a
cross-shard hop when the queue's key is owned by another shard**. During that park,
any sibling AMQP 1.0 connection served by the same thread has its read fiber call
`a10ReadFrame`, which does `buf.length`/refill on the *same* TLS `buf` — clobbering
`modAnnBytes`' contents. On the next loop iteration the clobbered bytes are decoded as
the annotation map and spliced into a *different* message's headers.

Note round-4 already fixed this exact class for the mgmt body (`mgmtCopy` per-call in
`a10HandleTransfer`) — the disposition `modified` path was missed.

### Concrete attack trace (sharded, ≥2 shards, one thread serving ≥2 AMQP 1.0 conns)
1. Attacker **A** attaches a receiver to queue `qA` whose key hashes to a remote
   shard, is delivered ≥2 unsettled messages (delivery-ids d, d+1).
2. Attacker **B** (any second AMQP 1.0 connection on the same shard thread) stands by.
3. A sends one `disposition(role=receiver, first=d, last=d+1, state=modified{
   delivery-failed=true, message-annotations={ "x-evil": <str> }})` with a non-empty
   annotations map (so the `modAnnBytes` branch is taken).
4. Iteration 1: `annTbl` built from the real map; `a10RequeueAnn(qA, blob, annTbl)`
   hops to the remote owner and **parks**.
5. While parked, B sends any AMQP frame (e.g. a 64-byte `transfer` containing
   B-controlled payload). B's read fiber runs `a10ReadFrame` on this thread →
   `buf[0..64]` now holds B's bytes. (If B's frame is *shorter* than the original,
   the tail of A's map bytes survives — either way A decodes attacker-influenced
   bytes; if longer, `buf` is reallocated and A's slice points into freed/reused
   GC memory → stale read.)
6. Iteration 2 (delivery d+1): `A10Dec(modAnnBytes)` decodes B's frame bytes as a
   map; string values become `'S'` headers, everything else is appended **raw** as
   `'x'` byte-array headers, and the record is requeued to `qA`.
7. A consumes from `qA` → receives a message whose message-annotations /
   application-properties contain verbatim bytes of **B's wire frame** (which for B
   is an unrelated client can include B's SASL response fragment, payload of B's
   publishes, B's addresses) — cross-client disclosure. Conversely B can *aim* this
   at corrupting A's redelivered messages (data corruption of the stored record).

Primitive classification: **read-only aliasing into GC-reused memory**; the leaked
bytes are re-emitted to the attacker inside a message header. Not a write primitive →
DoS-ONLY-to-DISCLOSURE, no RCE path (all decoded output is length-framed into
`annTbl` and appended, never used as an index/pointer). CONFIRMED cross-client
disclosure under sharding; in single-shard mode no park occurs and it is latent.

### Fix
Copy the annotations out of the read buffer before the loop, mirroring the round-4
`mgmtCopy` fix: `auto annCopy = modAnnBytes.idup;` (and snapshot `modAnnCount`) right
after parsing field 5, and decode `annTbl` from `annCopy` per iteration. Same
treatment for any other `fields`-derived slice that survives a call to
`a10Requeue/a10Reject/a10RequeueAnn`.

---

## Backlog (not flags)

- `a10StartDelivery` stream filter `continue` paths advance `streamPos` with no
  sleep — a fully-filtered large stream spins the fiber's peek loop hot (CPU burn).
- Same stream-offset-timestamp scan uses TLS `static ByteBuffer sb` shared across
  connections; two concurrent timestamp-offset attaches on one thread can interleave
  peeks — copy per attach or key per link.
- Delivery-fiber `pl5.outCredit--` can underflow to `uint.max` if a `flow credit=0`
  arrives while the fiber is parked inside `a10Pop` (credit accounting race, bounded
  by the 8192-unsettled cap).
- Re-`attach` with an already-live handle overwrites `ps.links[handle]` while the old
  delivery fiber still references it (duplicate deliveries, consumer-count leak) —
  should force `detached=true` and wait/replace cleanly.

---

## Per-file verdict
- **amqp10.d**: one NEW provable defect (FLAG 1, disposition TLS-aliasing,
  HIGH/cross-client disclosure). Round-4 fixes (`mgmtCopy`, decoder depth budget,
  `a10ClampN`, per-conn fragment budget) verified sound on re-read; the
  `a10SendDetachError` `snprintf` slice is safe because `entity.length ≤ 512 < 600`.

## Ranked summary
1. HIGH amqp10.d a10HandleDisposition — modified-annotations slice of shared TLS `a10ReadFrame` buffer re-decoded after cross-shard requeue parks → cross-client frame-byte disclosure into requeued messages; fix = per-call `idup` of annotation bytes before the loop.
2. Backlog: stream-filter no-sleep spin; timestamp-offset TLS `sb` sharing; outCredit underflow race; handle-re-attach fiber aliasing.

---

# CTF flags — `amqp.d`

# GLM-CTF-REPORT — Round 5, target `source/dreads/amqp.d`

## FLAG 1 — `a10Publish`: unguarded TLS record buffer aliases across reentrant publishes → cross-client message swap / corruption (HIGH)

**File:line:** `source/dreads/amqp.d`, `a10Publish` (the `static ByteBuffer rec; // TLS: consumed by the push sink in-walk` block, ~the 1.0-shim section, after `a10EnsureQueue`).

**Class:** cross-client disclosure + data corruption (in-bounds buffer reuse — not memory-unsafe, so not RCE; corruption/disclosure only).

**Root cause:** The 0-9-1 twin of this exact code path (`finishPublish`) guards its shared staging record with the `recBusy` reentrancy flag precisely because the routing sink YIELDS (cross-shard `gAmqpPush` hops and parks on a `ShardPending`), and a concurrent same-thread publish would rewrite the buffer the parked fan-out still reads. `a10Publish` uses a bare `static ByteBuffer rec` with **no busy flag**, and `payload = rec.data.asChars` is re-read by **every** sink invocation of the `foreach`-equivalent fan-out — i.e. after each yield.

**Concrete attack trace:**

Preconditions: `--shards 2`, AMQP port open. Client A and client B are both **AMQP 1.0** connections whose accepts hashed to the **same shard thread** (SO_REUSEPORT spreads, but same-shard co-tenancy is the common case at low N and trivially arrangeable by opening many conns).

1. Client A attaches a sender to a **fanout** exchange `F` bound to queues `q1` (local shard) and `q2` (remote shard, owner = shard 1) and transfers message `M_A`.
2. `a10Publish("F", rk, props, M_A)` builds `rec` = framed `M_A`; `routeTo` matches `q1`, `q2`.
3. Sink for `q1`: `gAmqpPush(key_q1, payload)` — remote hop, parks on a `ShardPending`. **Yield.**
4. While A's serve fiber is parked, client B's 1.0 fiber on the same thread transfers `M_B` to any queue. Its `a10Publish` executes `rec.clear(); buildRecord(...M_B...)` — **the very buffer A's `payload` slice points into is rewritten with M_B**.
5. A's reply arrives; A wakes, iterates to the sink for `q2`: `gAmqpPush(key_q2, payload)` — `payload` still slices `rec`, whose contents are now `M_B` (or a torn mix of `M_B` and stale `M_A` bytes, since `clear()` + shorter/longer appends leave A's fixed-length slice reading past B's logical length into stale bytes — all in-bounds, but arbitrary wrong content).
6. `q2` (and any later fan-out destination) durably stores **client B's message body under client A's publish**, and/or a corrupted record. Consumers of `q2` receive B's payload; B's own fan-out destinations correspondingly receive whichever bytes the interleaving left.

Result: provable cross-client message-body disclosure and durable queue corruption, driven entirely by two normal publishes racing. Note the file itself documents this exact hazard class two functions away ("the reentrant caller takes a fresh local so the parked fan-out keeps reading its own bytes") — the guard was simply never added to the 1.0 shim.

**Fix:** mirror `finishPublish`:

```d
static ByteBuffer recStatic; static bool recBusy;
ByteBuffer recLocal; ByteBuffer* rec = &recLocal;
if (!recBusy) { recBusy = true; rec = &recStatic; }
scope (exit) if (rec is &recStatic) recBusy = false;
```
(or make `rec` a plain per-call local; the alloc-avoidance rationale is identical to the 0-9-1 path, which accepted the guard cost).

**RCE assessment:** DoS/corruption-only. The aliasing is a same-object `ByteBuffer` reuse with in-bounds lengths — no OOB write primitive, no attacker-controlled pointer; it cannot reach a return address or vtable.

---

## Verified-clean (no new provable defect)

- **`finishPublish`** (`recStatic`/`recBusy`, `sp`/`drp` per-call, `seenArena` stack copies, `kb3` consumed into the hop payload pre-park): the round-4 hardening holds; I traced every post-yield read and each either slices a guarded or per-call buffer or is copied into the hop before any park (the refuted args-pattern).
- **`deadLetter`** (`dlrec` guarded; `xbuf`/`paug`/`xoth` consumed by `buildRecord` before `routeTo`'s yields; sink `kb5` re-filled per call then args-copied pre-park): clean.
- **`commitTx`** (`kbT` args-copied pre-park; `stamped` is a per-record `dup`): clean.
- **`routeTo`** (`destStatic` guarded by `destBusy` held across the sink yields; `visited`/`visitedDepth` never read across a yield — the reentrant refill happens only after the outer `collect` walk completes): clean.
- **`amqpTtlSweep`, `basic.get`, `settleNegative`, `requeueAllUnacked`, `a10Pop`/`a10Requeue`/`a10RequeueAnn`**: all cross-yield operands are stack-copied or consumed pre-yield; `popped`/`head`/`pay`/`rq*` TLS statics are re-filled synchronously post-wake and read before any further yield.
- **Codecs** (`Rd`, `tableWalk`, `propsHeaders`/`propsReplyTo`/`propsExpiration`, `splitRecord`, `mergeXDeath`, `appendHeadersExcept`, `drParse`): all length-checked; no OOB found.
- **AuthZ**: per-op ACL present on publish/get/consume/bind/unbind/purge/delete/exchange-delete; CC/BCC default-exchange keys ACL-checked; direct-reply tokens CSPRNG-keyed. No bypass found in this file. (Whether the AMQP 1.0 skin enforces the same per-op ACL around `a10Publish` must be checked in `amqp10.d` — listed under SUSPECT.)

## SUSPECT (unproven)

- **`a10Publish`/`a10Bind`/`a10DeclareQueue` carry no ACL check inside this module** — if `dreads.amqp10` doesn't gate transfers/attach with `aclCanAccessKey`, a restricted 1.0 user bypasses the 0-9-1 per-op ACL. Need `amqp10.d` source to confirm; the 0-9-1 paths are gated.

## DoS-hardening backlog (not flags)

- `basic.cancel`/`channel.close` unknown-tag paths burn a fixed 200×1ms spin per bogus tag — bounded, per-connection, backlog only.

## Ranked summary

1. HIGH — amqp.d `a10Publish`: unguarded TLS `rec` staging buffer; reentrant 1.0 publish during a cross-shard fan-out hop rewrites the record a parked fan-out still reads → later fan-out queues durably store another client's message body (cross-client disclosure + queue corruption). Fix: recBusy guard / per-call buffer. Corruption-only, not RCE.

---

# CTF flags — `kafka.d`

# GLM-CTF-REPORT — Round 5 — `source/dreads/kafka.d`

## FLAG 1 (HIGH — authorization bypass / cross-tenant): `tKafkaCtx` is a TLS global clobbered by any sibling fiber's yield, so `authorize()` runs with ANOTHER connection's principal

**File:line:** `kafka.d` — declaration `private KafkaConnCtx* tKafkaCtx;` (ACL-enforcement section), set at the top of `handleRequest` (`tKafkaCtx = ctx;`), consumed by `authorize(tKafkaCtx, ...)` throughout.

**Root cause.** `tKafkaCtx` is thread-local, not fiber-local. `handleRequest` installs it once per request, but the Kafka handlers **yield** on every cross-shard data-plane hop (`gKafkaExec` → `amqpDataExec` → shard pending wait, and `kgOp` → group-coordinator hop). The shard thread runs many connections as fibers; when fiber B's `handleRequest` runs during fiber A's park, it overwrites `tKafkaCtx = &ctxB`. When A resumes and later calls `authorize(tKafkaCtx, ...)`, the principal is **B's**. The in-code comment ("authorize() copies the principal to the stack before its own cross-shard hop, so a fiber switch cannot clobber it") defends the wrong window: the clobber happens to the *pointer* before `authorize` is even entered, not to the principal slice inside it.

**Concrete attack trace (Produce write on a DENY'd topic; ACLs active, `gKafkaAclActive > 0`):**

Setup: ACL store has `TOPIC / secret / User:* / WRITE / DENY` (plus some ALLOW so `gKafkaAclActive=1`); principal `admin` is in `kafka-super-users`. Attacker connects **without SASL** (or as any low-priv user) on the same shard thread that serves an `admin` connection (single port, SO_REUSEPORT — trivially arranged by opening many admin connections).

1. Attacker sends **Produce v2** with **two partitions** for topic `secret` (or one partition plus anything that hops first, e.g. a compacted-topic config probe).
2. Partition-1 processing executes `authorize(tKafkaCtx, KRES_TOPIC, "secret", KOP_WRITE)` → denied, `err = E_TOPIC_AUTH_FAILED`... but before partition 2 is parsed, the handler for partition 1 runs hops that park the fiber. Even on a denied partition the code reaches `topicCompacted(topic)`? No — `compacted` is gated on `err == E_NONE`; but `partBase()` and the `records` staging still run, and on the *first allowed* partition (e.g. produce to an allowed topic `pub` in the same request, partitions of `pub` first) `topicCompacted`, `pidCheck`, and `pushRecords` each hop and yield.
3. During the yield, the admin connection's request runs on this thread → `tKafkaCtx = &adminCtx`.
4. Attacker's fiber resumes; for topic `secret` the call `authorize(tKafkaCtx, KRES_TOPIC, "secret", KOP_WRITE)` reads `adminCtx.principal()` = `"admin"` → `isSuperUser("admin")` → **true** → the DENY is bypassed and the record is stored.

Same defect reachable more cheaply via **Fetch** (`handleFetch` calls `authorize(... KOP_READ)` *per partition*, after `partBase`/`partLen` hops — cross-shard `partLen` yields on every non-owner partition in sharded mode), via **Metadata-flex** per-topic `authorize` after `registerTopic` hops, **OffsetCommit/OffsetFetch** topic loops after `storeGroupOffset`/`fetchGroupOffset` hops, **DeleteGroups** per-group `authorize` after prior groups' `kgOp` hops, **DescribeGroups** likewise, and the group ops (`JoinGroup`/`Heartbeat`/`LeaveGroup`/`SyncGroup`) whose `authorize` follows earlier hops in the same request (e.g. multi-group batching in DeleteGroups, or OffsetCommit's `KGOP_COMMIT_CHECK` hop before per-partition stores on the next request… each handler that authorizes *after* its first yield is exploitable).

**Impact:** full read/write ACL bypass on topics, groups, and txn ids — cross-tenant disclosure (Fetch of a DENY'd topic) and cross-tenant injection (Produce). Not memory corruption; authorization class. Confirmed by control-flow reading; primitive is "borrow the concurrently-served principal".

**Fix.** Stop carrying the auth context in TLS. Thread `KafkaConnCtx* ctx` as a function parameter from `handleRequest` into every handler and into `authorize(ctx, ...)` (the handlers already take `(ref Rd r, short ver, ref ByteBuffer o)` — add the ctx parameter; ~30 mechanical call sites). Alternatively copy the principal (≤64 bytes) into a fiber-local stack buffer at `handleRequest` entry and pass that slice to `authorize`.

---

## SUSPECT (unproven / needs confirmation)

- **`gKafkaRequireSasl` is a no-op when no ACL users exist** (`kafkaPlainCheck`: `aclUserCount() <= 1` → legacy accept-any, `authed = true`). An operator setting `kafka-require-sasl` with an unseeded user store gets an open broker while believing auth is enforced. Not a wire-exploitable bypass beyond that misconfiguration; classify as config-foot-gun. Fix: when `gKafkaRequireSasl` is true, never take the legacy accept-any branch.

## DoS-hardening backlog (not flags)

- `joinLoop`/`syncLoop` hold a connection fiber up to 70 s per JoinGroup/SyncGroup (spec-faithful, but a botnet of joins parks fibers; consider a per-shard concurrent-join cap).
- Per-request decompression budget is 512 MB (`KAFKA_DECOMP_REQ_MAX`) — CPU/alloc spike per request; consider lowering or rate-limiting.

## Verified-no-new-defect notes

- `handleFetch` fallback `fblobs` TLS slices: consumed (encoded) with no hop between `rangeRecords` return and `encodeV2BatchFromInternal` — safe.
- `parseGroupOffsets`/`emitAllGroupOffsets`: parse→emit with no intervening hop — safe.
- `handleAddPartitionsToTxn` / `handleEndTxn` / `handleDeleteAcls` / `handleCreateTopics` / `handleDescribeConfigs` / `handleMetadata`: all post-hop consumers read stack copies or request-buffer slices — verified.
- `decodeV2Batch`, `parseStoredRec`, SCRAM buffer math (`am[704]`, `sf[192]`, `nonceBuf[128]`), snappy/lz4/zstd/gzip bomb guards, `patchCArrLen` fixed-width varint, OOM-guarded size backpatch in `handleRequest` epilogue — all bounds-checked correctly.

---

## Ranked summary

1. **HIGH — kafka.d `tKafkaCtx` TLS clobber → authorize() uses another connection's principal (ACL/super-user bypass, cross-tenant read/write).** Fix: pass `KafkaConnCtx*` per-call instead of TLS.
2. SUSPECT — `kafka-require-sasl` inert with zero ACL users (config foot-gun).
3. Backlog — 70 s join/sync fiber hold; 512 MB/request decompression budget.
