# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

# GLM-CTF-REPORT — `source/dreads/sqs.d` (post-hardening pass)

The b2dc859 fixes in this file (record copy-out via `recCopy`/`bodyBuf`/`ebodyBuf`, sweep-length masking, `fifoUnlock` arg-serialization) check out at their sites — but the FIFO receive path still holds **live slices into a shared TLS static across `exec()` parks**, which is exactly the aliasing class the hardening was supposed to kill. Details below, plus two correctness/DoS flags.

---

## FLAG 1 — FIFO receive: stale `snap`/`recs`/`batchGroups` TLS slices across `exec()` parks → cross-queue message disclosure (CRITICAL)

**Where:** `opReceiveMessage`, the `static ByteBuffer snap;` + `static const(char)[][1024] recs;` + `const(char)[][32] batchGroups` block and the FIFO scan loop.

**Root cause:** `snap`, `recs[]`, and every `g2` stored in `batchGroups` are slices into the **function-static TLS** `snap` ByteBuffer. Inside the FIFO selection loop the code calls `exec()` (`sismember`, `lrem`, `sadd`) — and `exec` is `gSqsExec = amqpDataExec`, which under `shards>1` **parks the fiber on a cross-shard hop** (the group key / queue key owner is any shard by hash). While parked, a sibling fiber on the same thread runs another SQS request, whose `opReceiveMessage` re-enters and refills the *same* `snap` static. The parked fiber's `recs[]` then point at the sibling's queue contents. The hardening commit added `recCopy` — but only **after** the record is selected; the scan loop (`recs[scanPos]`, `splitRecord(recs[scanPos], m2,d2,g2,b2)`) re-reads the static **after** every park, and the `lrem` arg `rec` is a raw slice taken pre-`exec` too.

**Attack trace (shards≥2):**
1. Attacker opens two connections that land on the **same router thread** (SO_REUSEPORT spread makes this trivially repeatable; retry until same shard).
2. Conn A: `ReceiveMessage` on `queueA.fifo` with `MaxNumberOfMessages:10`. LRANGE fills `snap` with A's records; `recs[0..n]` set. Iteration 0 calls `exec(sismember sqs.grp.queueA.fifo g)`; that key hashes to a foreign shard → fiber A parks inside the hop.
3. Victim conn B (same thread): `ReceiveMessage` on `queueB.fifo`. Its LRANGE executes `snap.clear(); append(...)` — the **same static buffer** — now holding queueB's records (msgid/md5/group/**body**).
4. Fiber A resumes: `rec = recs[scanPos]` now aliases queueB's record bytes. A `LREM`s that record out of `sqs.q.queueB.fifo`'s key? — no: it LREMs from *its own* `qkey` (queueA), which fails silently (record not there), and then A's response emits `"Body"` = **queueB's message body** (splitRecord over the aliased slice), with queueB's MessageId and a fresh receipt handle inserted into `sqs.if.queueA.fifo`.

Result: **cross-tenant message disclosure** (flag class 2) plus queueB silently losing a delivered-to-nobody record is *not* lost (LREM targeted A's key) — but B's `snap`-backed state is now also inconsistent for its own in-flight iteration. Memory-safety-wise the old slices stay GC-valid (ByteBuffer growth reallocates, old array survives), so this is a **data-confusion primitive, not corruption** — classification: HIGH-impact confidentiality flag, not RCE.

**Fix:** copy the LRANGE result out of `snap` into a per-call `ByteBuffer` immediately after the single `exec(lr)`, and build `recs[]`/`batchGroups` as owned copies (or offsets into the local copy) before any further `exec`. Same discipline already applied to `rec`/`msgBody`.

---

## FLAG 2 — `PurgeQueue` leaves `sqs.grp.*` group locks → permanent FIFO group deadlock (MED)

**Where:** `opPurgeQueue` deletes only `sqs.q.<name>` and `sqs.if.<name>`.

**Root cause / trace:** Client sends `PurgeQueue` on a FIFO queue that has an in-flight message (group locked via `sadd sqs.grp.<name> <g>`). Purge deletes the queue list and the in-flight hash but **not** the `sqs.grp.<name>` set. Every later `ReceiveMessage` skips all records whose group is `sismember` of that set (`exec(sm...)` == 1 → `continue`), and nothing ever removes the group (the sweep only srems groups of *expired in-flight* records it finds in `sqs.if.*`, which Purge just deleted). All future sends to that group are **undeliverable forever** — a persistent, one-request, unauthenticated availability kill on the queue.

Note `opDeleteQueue` *does* clear `GRP_PREFIX`/`DD_PREFIX`/`DL_PREFIX`; Purge is the missed sibling.

**Fix:** in `opPurgeQueue`, also `del` `sqs.grp.<name>` (and `sqs.dd.<name>` for hygiene; `sqs.dl.<name>` per AWS semantics deletes pending delayed messages too).

---

## FLAG 3 — FIFO receive: 1024-record snapshot cap + full-queue LRANGE per receive (MED)

**Where:** `static const(char)[][1024] recs;` and the `lrange 0 -1` in `opReceiveMessage`.

**Trace:** (a) With >1024 queued records, `nrecs` saturates at 1024; records past index 1023 are invisible to the scan. If any of the first 1024 records belongs to a locked group, `scanPos` stalls below 1024 *and* can never reach record 1024+ → head-of-line starvation, messages never deliverable (persistent, no recovery until the locked group's blocker is deleted — and combined with Flag 2, potentially never). (b) Every FIFO receive executes `LRANGE 0 -1` and copies up to 1024 records — a queue with 10M messages makes each Receive an O(queue) server-side walk: a client looping `MaxNumberOfMessages=1` receives is a cheap CPU/memory amplifier (records can be ~256 KiB each → each receive materializes up to ~256 MiB of TLS copies).

**Fix:** page the snapshot (`lrange scanPos scanPos+N`), and/or make the group-lock check owner-side atomic (overlaps TODO(FIFO-atomic), but the starvation and amplification are independent of atomicity).

---

## SUSPECT (unproven / lower confidence)

- **`dedupSeen` TOCTOU (sqs.d, dedupSeen/dedupStore):** check-then-send spans a park; two concurrent sends with the same `MessageDeduplicationId` can both miss and both enqueue → duplicate delivery (exactly-once contract broken). Also `dedupSeen` returns the stored mid/md5 without comparing `body_` — a repeat dedup id with a *different* body silently returns the old MessageId. Confirm with a concurrent-send trace; fix by making dedup+send one owner-side op (same family as TODO(FIFO-atomic)).
- **`onConn` header truncation:** the request must fully fit headers+partial body logic; a header block >64 KiB with `hend==0` and non-zero `clen` yields `reqBody=null` (silently misparsed) — correctness only, no OOB found (`hend <= req.length` is guarded).
- **`toSize` overflow on Content-Length:** digit accumulation wraps `size_t`; could under-read the body. Parsed-body confusion only, `respBulk`/`jsonStr` bounds all held up under review.

---

## Ranked summary

1. CRITICAL — sqs.d opReceiveMessage: FIFO `snap`/`recs` TLS aliasing across exec parks → cross-queue body disclosure (sharded).
2. MED — sqs.d opPurgeQueue: leaves `sqs.grp.*` locks → permanent FIFO group undeliverability.
3. MED — sqs.d opReceiveMessage: 1024-record cap starves tail records; per-receive full LRANGE amplification.
4. SUSPECT — dedupSeen TOCTOU + no-body-compare dedup collision.
5. SUSPECT — Content-Length overflow/header-size truncation (correctness only).

---

# CTF flags — `kafkagroup.d`

# GLM-CTF pass — `source/dreads/kafkagroup.d`

I read the whole FSM plus the hop plumbing in `shard.d`/`server.d`. No memory-corruption primitive found in this file — every `KgRd` accessor bounds-checks before slicing, `bytes32` rejects a negative i32 as a ~4 GiB length (no size_t wrap on 64-bit), the `char[300] tb` TXN_ADD buffer is correctly guarded by the new `topic.length > 249 → continue` (249 + 1 + max 11-digit int = 261 < 300, no truncation-collision), and `tTxns` is capped at 4096 with idle eviction. The hardening items touching this file check out. What remains are resource-exhaustion and protocol-contract flags:

---

## FLAG 1 (MED — remote unbounded memory growth / DoS): txn state never bounded across requests
**File:** `kafkagroup.d`, `KGOP_TXN_ADD` (~line with `t.tps ~= tps`) and `KGOP_TXN_OFFSETS` (`t.offs ~= e`).

**Root cause:** The new caps (`n2 > 1024`, `n2 > 4096`) bound only *one request*. Nothing bounds the accumulated `t.tps` / `t.offs` arrays, and they are cleared only on `TXN_END` or `TXN_INIT`. A client never has to end the transaction.

**Attack trace:** SASL-authenticated (or open, if `--kafka-require-sasl` off) producer:
1. `InitProducerId(txn.id="evil", timeout=600000)` → pid/epoch 0.
2. Loop forever: `AddPartitionsToTxn(pid, epoch, 1024 fresh "topic-<i>-p<j>")` — dedup never hits, `t.tps` grows 1024 strings/request.
3. Interleave `TxnOffsetCommit` requests: each appends up to 4096 `KgTxnOff` entries whose `meta` is a `str16` up to 64 KB (`.idup`'d — heap-retained).

With ~64 KB metas, a single modest request train grows TLS heap at hundreds of MB per round trip on the owning shard; there is no per-txn ceiling and no idle eviction while the attacker keeps `lastMs` fresh (any op refreshes it). One connection, one txn id, OOM the shard thread.

**Fix:** cap totals (e.g. `t.tps.length + n ≤ 8192`, `t.offs.length + n ≤ 16384`, plus a byte budget for `meta`); on overflow return error 47/51 and reset the txn.

---

## FLAG 2 (MED — protocol contract / malformed wire output): `wStr16` silently truncates
**File:** `kafkagroup.d`, `wStr16` (`n = s.length > 0xFFFF ? 0xFFFF : s.length`).

**Root cause:** Member ids, assignments (`wBytes32` is fine — i32), `protoName`, and especially `emitJoinOk`/`KGOP_DESCRIBE` member lists use `wStr16`. A group pushed to many members with long client-ids/giis, or a `memberMeta` blob near 64 KB, gets its string *body* chopped while the declared count/structure says otherwise — the consumer desyncs its parser on the JoinGroup reply.

**Attack trace:** Join with `group.instance.id` of length 70 000 (str16 read caps at 65 535 on the *request* side — good), then a second joiner whose clientId pushes the leader's member-list encoding past per-string limits; the leader's `emitJoinOk` emits a truncated gii string inside an otherwise count-correct array → the real client library misparses, or a hostile peer uses the desync to read adjacent reply bytes the broker appended after the truncation point (bounded — same ByteBuffer — so this is a correctness/DoS flag, not disclosure).

**Fix:** reject >0xFFFF strings at decode time (error 23/25) instead of truncating at encode.

---

## FLAG 3 (LOW — remote DoS/liveness): unbounded `sessMs` makes a member immortal
**File:** `KGOP_JOIN` (`nm.sessMs = sessMs > 0 ? sessMs : 45_000`), `evictStale`.

**Trace:** One JoinGroup with `session.timeout.ms = 2147483647`, then disconnect. `evictStale` (`now - lastMs >= sessMs`) never fires; `KGOP_DROP` (DeleteGroups) returns 68 NON_EMPTY_GROUP forever; the group also blocks its slot in the 4096-group TLS table. Real Kafka caps session.timeout.ms; this broker doesn't.

**Fix:** clamp `sessMs` to e.g. 1 800 000 (Kafka's max) and `rebMs` similarly (rebMs is already capped via `REBALANCE_CAP_MS`).

---

## SUSPECT (not provable from this file)

- **`KGOP_TXN_OFFSETS` writes offsets to an arbitrary `grp`** (`t.offGroup = grp.idup`): if the Kafka skin's ACL model separates consumer groups per tenant, a transactional producer can commit offsets into a victim group ( TxnOffsetCommit for a group it isn't a member of — `KGOP_COMMIT_CHECK` is only used on the plain path). Need `kafka.d`'s OffsetCommit routing + ACL checks to confirm cross-tenant impact; if groups are ACL-checked at TxnOffsetCommit apply time, downgrade to none.
- **`closeBarrier` failure leaves `state = ST_EMPTY` with live members** (`KGOP_JOIN`/`JOIN_POLL` failure path): heartbeats then return UNKNOWN_MEMBER while members linger until session eviction — liveness wart, not security.

---

## Ranked summary
1. MED kafkagroup.d — unbounded `t.tps`/`t.offs` growth across repeated AddPartitionsToTxn/TxnOffsetCommit → one-connection OOM (FLAG 1)
2. MED kafkagroup.d `wStr16` — silent 64 KB truncation emits structurally malformed JoinGroup/Describe replies (FLAG 2)
3. LOW kafkagroup.d `KGOP_JOIN` — uncapped `sessMs` pins a dead member/group forever (FLAG 3)
4. SUSPECT — TxnOffsetCommit targets arbitrary consumer group (`offGroup`), possible cross-tenant offset overwrite pending kafka.d ACL check
5. SUSPECT — closeBarrier failure leaves ST_EMPTY with resident members (liveness only)

No memory-corruption / RCE-grade primitive found in this file; all decoder reads are bounds-checked and the TXN_ADD stack buffer is now correctly sized.

---

# CTF flags — `mqtt.d`

# GLM-CTF-REPORT — `source/dreads/mqtt.d` (post-hardening pass)

## Verification of the landed fixes in this file

- **Pre-connect cap**: `MQTT_PRECONNECT_MAX` (256 KB) is applied at packet-parse time (`pktCap = c.connected ? MAX : PRECONNECT`) before the body is ever handed to `handlePacket` — correct, and the 30 s `MQTT_CONNECT_TIMEOUT` bounds the pre-CONNECT slowloris. ✔
- **v5 password-without-username**: gated on `c.protoVer < 5` — correct per MQTT 5. ✔
- **Will-fire-once**: `fireWill` nulls `c.willTopic` first thing; the park loop, teardown, and `mqttMigrateParked` all clear or single-shot it; the freeze path nulls the will before publishing `frozen`. ✔
- CONNECT keepalive-truncation OOB (`i + 2 > p.length` before reading `ka`) — bounded. ✔
- All v5 property walkers (`mqttParseConnectProps`, `mqttParsePubProps`, `mqttParseWillProps`, `mqttDisconnectSEI`, `stripExpiryProp`, `expiryValueOffsetInPacket`) bound every read by `end ≤ p.length`. I found no OOB in them. ✔

One of the fixes, however, left a sibling hole — see Flag 1.

---

## FLAG 1 — Will message bypasses the channel ACL (HIGH, PROVEN)

**Where**: `handlePacket` `case PT_CONNECT` (will parsing block, ~"`if (flags & 0x04)`") vs `case PT_PUBLISH`'s `clientReserved` check; `fireWill`.

**Root cause**: A normal PUBLISH is dropped (drop-but-ack) when `c.aclUser !is null && !aclCanAccessChannel(c.aclUser, topic)`. The CONNECT handler validates the **will topic** only with `mqttValidTopicName(wt)` and the `$`-prefix check — it never runs `aclCanAccessChannel`. On abnormal disconnect, `fireWill` → `mqttDeliverLocal` + `gMqttFanout` publishes it like any other PUBLISH.

**Attack trace** (unprivileged but authenticated MQTT user `u`, denied channel `secret/#`):
1. `CONNECT` proto v4, clean=1, will-flag=1, will-QoS 0, will-retain 0, will topic `secret/leak`, will payload `pwn`, username `u`, password ok.
2. Kill the TCP connection (no DISCONNECT).
3. Serve fiber → `mqttParkOrEnd(c, false)` → non-eligible or park-expiry → `mqttTeardown` → `wasConnected` → `fireWill(c)` → `mqttDeliverLocal("secret/leak", "pwn", …)` and `gMqttFanout(...)` — the payload is delivered to every subscriber of `secret/#` on every shard, and **stored as retained if will-retain is set**, persisting the injection.

Result: persistent injection into an ACL-denied channel that the same user could never `PUBLISH` to directly. This is an ACL bypass class flag (cross-tenant data injection), not just correctness.

**Fix**: at CONNECT, after `mqttValidTopicName(wt)`, reject the CONNECT (or drop the will) when `c.aclUser !is null && !aclCanAccessChannel(c.aclUser, wt)`; defensively re-check in `fireWill` (users can be disabled between CONNECT and disconnect).

---

## FLAG 2 — v5 forwardable props ride a TLS static across a yielding fan-out (MED, SUSPECT→likely)

**Where**: `case PT_PUBLISH`: `static ByteBuffer fwdProps; … props = fwdProps.data;` then `gMqttFanout(topic, payload, retain, rseq, qos, props)`.

**Root cause**: `props` is a live slice of a **thread-local static** buffer. `gMqttFanout` → `shardMqttFanout` → `shardEnqueue`, which *yields* under ring backpressure and retries the push from the same slice. The codebase itself documents this exact hazard (`kafkaGroupHopImpl`: *"hb is STACK-local: shardEnqueue yields under ring backpressure and a TLS static would be clobbered by another fiber's hop during that yield"*) — this site violates that rule. If `shardMqttFanout` does not serialize `props` into a fiber-local buffer before the enqueue, then during the yield another PUBLISH fiber on the same shard rewrites `fwdProps`, and the retried push copies the **wrong client's v5 properties** (correlation-data, content-type, user-property, response-topic) into the cross-shard `mqttPub` frame — cross-client property confusion delivered to subscribers.

**Why SUSPECT not flag**: the receiving drain's `mqttPub` format (`[topic][propsLen u32][props][payload]`) implies the producer serializes props into one contiguous buffer; if that buffer is fiber-local (built before `shardEnqueue`), the aliasing window is closed inside `gMqttFanout` and only `shard.d`'s own copy discipline matters. Confirmation requires `shardMqttFanout`'s body (not in scope here).

**Fix either way**: copy `props` into a stack/fiber-local `ByteBuffer` (or `.idup`) before calling `gMqttFanout` — cheap and removes the class.

---

## FLAG 3 — `mqtt.sess.*` session-present oracle/spoof via the shared keyspace (LOW-MED)

**Where**: `mqttSessionExists` / `mqttSessionPut` / `mqttSessionDel` — key `mqtt.sess.<clientId>` in `gConfig.mqttDb` (17) via `gMqttExec` (the generic RESP data plane).

**Root cause**: the "does a persistent session exist" signal is a plain key in a normally-addressable DB. Any client that can reach RESP `SELECT 17` (or a Kafka/SQS/AMQP skin routed at db 17 — the skins share `amqpDataExec`) can:
- `SET mqtt.sess.victim 1` (no TTL) → the victim's next clean_start=0 CONNECT gets a **false CONNACK session-present=1** (protocol-contract violation, client-side state confusion), or
- `DEL`/`EXPIRE` it → false session-absent.

No message disclosure (the session *contents* are thread-local), so this is a contract/state flag, not data theft. If db 17 is not ACL-restricted per-user, this is reachable today.

**Fix**: namespace-guard the key at the `amqpDataExec` boundary (reject `mqtt.sess.*` from non-internal skins), or store session-present under a db no skin exposes.

---

## SUSPECT section (unproven / inspected-and-cleared)

- **`fwdProps` aliasing** — see Flag 2; need `shardMqttFanout` source to promote/demote.
- **Cross-shard adopt (`mqttResumeXShard`) reading another shard's `MqttConn`**: the owner publishes `frozen` only after setting `redirect` (deliverTo drops) from inside its park loop with no intervening yield, and the adopting thread only reads after observing `frozen`. Looks sound; the residual risk is the GC not scanning the adopting thread's temporary `parked` reference — but `parked` is a fiber-stack local (scanned) and the owner still holds it in `gLocalClients` during the redirect window. No flag.
- **`static granted[64]/filters[64]/retainOk[64]` in SUBSCRIBE**: no yield between fill and use (retained replay appends only; `mqttFlushDirty` is emit-only). Cleared.
- **Dead path**: `mqttParkOrEnd` never actually returns `true` (all branches return `false`), so the `continue readloop` rebinding paths in the serve loop are dead code. Cosmetic — but the comments claim a rebind that the implementation abandoned; worth a cleanup before someone "fixes" against the comment.
- **Empty `readDeadline` keepalive=0 → `Duration.max`**: spec-legal idle; TCP death still detected. Not a flag.

---

## Ranked summary

1. FLAG 1 (HIGH, ACL-bypass): MQTT Will topic never ACL-checked at CONNECT — will publish on disconnect reaches channels the user is denied; `mqtt.d` CONNECT will block / `fireWill`.
2. FLAG 2 (MED, SUSPECT): v5 PUBLISH `props` is a TLS-static slice passed into the yielding `gMqttFanout`/`shardEnqueue` path — cross-client property confusion under ring backpressure if the fan-out doesn't pre-serialize.
3. FLAG 3 (LOW-MED): `mqtt.sess.<id>` keyspace spoofing via any db-17-capable client → false session-present/absent in CONNACK.

---

# CTF flags — `amqp10.d`

# AMQP 1.0 skin (amqp10.d) — post-hardening review

Verification of the landed fixes first, then new flags.

## Fix verification (commit b2dc859, this file)

- **SASL gate** — present and correct on *both* paths: `aclUserCount() > 1` refuses a bare header (offers SASL header, hangs up) and `a10SaslCheck` refuses `ANONYMOUS` under ACL. ✔ No sibling bypass found: there is no other entry into the AMQP layer (`amqp10Serve` is the only entry, dispatched after the 8-byte header).
- **Disposition snapshot** (`dispIds` filtered to `[first,last]`) — the 2^64 spin is gone. ✔ (but see Flag 3's TLS note).
- **Props clamp** — contentType/correlationId/replyTo all clamped to 255 with matching length byte. ✔ `expiration` is self-generated digits (≤20), safe.
- **Frame-size** — `size > A10_MAX_FRAME`, `size < 8`, `doff < 2`, `doff*4 > size` all checked before `buf[skip .. rest]`; `skip ≤ rest` guaranteed. Decoder (`take`/`be`/`u8`) is uniformly bounds-checked. ✔ No OOB read/write found in the codec.
- **Link cap** `A10_MAX_LINKS_PER_CONN = 4096` — present, counted across sessions. ✔ but see Flag 2: it does not bound *bytes*.

---

## FLAG 1 — TLS frame scratch `buf` aliases frame bodies across connections → cross-client disclosure / record corruption (HIGH)

**Where:** `a10ReadFrame` (`static ubyte[] buf; // TLS scratch`), consumed by `amqp10Serve` → `a10HandleTransfer` / `a10HandleMgmt`; same class: `static ByteBuffer props, bodyBuf` (a10HandleTransfer), `static ByteBuffer hdrTbl/sb/annTbl/dcTbl/bArgs/dispIds`.

**Root cause:** every connection served on this shard thread shares ONE `buf`. The comment claims "consumed before return; no yield holds it", but the mgmt and transfer handlers *do* yield while holding slices into `buf`: `a10QueueExists`, `a10DeclareQueue`, `a10QueueLen`, `a10Pop`, `a10PeekAt`, `a10Publish` all cross into the amqp.d data plane (`amqpDataExec`/`gAmqpLen`/…), which under `sharded()` **hops and parks** (server.d `kafkaGroupHopImpl` explicitly documents that park). Even unsharded, `a10Send` yields on `wlock`/`tcp.write` in paths that use body slices afterwards.

**Attack trace (shards ≥ 2, two conns A and B accepted by the same shard thread):**
1. A sends `PUT /queues/q` with an `arguments` map containing a secret marker (e.g. `x-max-length` = 0x41414141).
2. A's read fiber enters `a10HandleMgmt`; `bodyMapBytes`/`corrRaw` point into `buf`. It calls `a10QueueExists(qn)` → data-plane hop → **parks**.
3. B's read fiber (same thread) reads any frame → `a10ReadFrame` overwrites `buf` with B's frame body.
4. A resumes: `a10Fnv(args.bytes)` hashes B's bytes; crucially `gA10ArgsRaw[q] = args9.bytes.idup` **copies B's frame body into the queue's stored metadata**, and the "201/200" response's `corrRaw` (raw correlation-id re-emit) now contains B's bytes.
5. Any client (or B itself) does `GET /queues/q`: `a10QueueInfoMap` replays `gA10ArgsRaw` verbatim → **B's private frame payload is disclosed to A** (and to every future reader).
   Same mechanism on the publish path: `props`/`bodyBuf`/`hdrTbl` TLS statics passed by slice into `a10Publish`; if A parks inside the hop and B's transfer fiber runs, A publishes a record built from B's headers/body — silent cross-tenant message corruption.

**RCE assessment:** DoS-ONLY / data corruption. All slices remain in-bounds (ByteBuffer manages its own memory); there is no controlled *write* primitive, only content substitution. No path to control-flow hijack.

**Fix:** make the frame body connection-scoped (member `ubyte[] buf` on `A10Conn`, or a per-fiber stack `ByteBuffer`), and replace every `static ByteBuffer` in the transfer/mgmt/disposition paths with stack-local buffers (the codebase already knows this rule — `kafkaGroupHopImpl` re-clears after park for exactly this reason; here the data itself must not be shared, not just re-cleared).

---

## FLAG 2 — Unbounded fragment memory: 4096 links × 16 MiB pending ≈ 64 GiB per connection (MED, remote DoS)

**Where:** `a10HandleTransfer` fragment accumulation (`plk.pending`, cap `16 * 1024 * 1024` per link) + `A10_MAX_LINKS_PER_CONN = 4096`.

**Trace:** open one connection, `begin`, then 4096 `attach`s (distinct handles, sender role, distinct queue names — or even the same; the cap counts *links*, not queues). On each link send one TRANSFER with `more=true` carrying a 1 MiB body, repeat 16× (≤16 MiB each). Never send `more=false`. Every link now holds a 16 MiB `pending` array the broker will never free until detach. Single connection, ~64 GiB resident → OOM kill. Fragments aren't even bounded per-*connection*, only per-link. Sibling: the delivery fiber honors client-granted `outCredit` up to `uint.max`, `idup`-ing the whole queue into `ps.unsettled` before TCP backpressure stops it.

**Fix:** per-connection (and global) pending-bytes budget; refuse/kill the link when exceeded; cap `outCredit` to something sane before entering the delivery loop.

---

## FLAG 3 — `dispIds` TLS shared across connections' read fibers (MED-LOW, correctness/DoS)

**Where:** `a10HandleDisposition` — `static ulong[] dispIds; // TLS: drain fiber only`. The comment is wrong: dispositions arrive on each connection's **read fiber**, and multiple connections share the thread. The handler fills `dispIds`, then calls `a10Requeue`/`a10Reject`/`a10RequeueAnn` (data-plane hops → park). Conn B's disposition resets `dispIds.length = 0` (which can also reallocate) while conn A iterates it. D's `foreach` over the array captures the old slice, so A iterates stale content — ids looked up in A's *own* `ps.unsettled`, so it degrades to missed/settled-by-accident dispositions when ids collide numerically (delivery-ids are small and start at 0 — collisions are the norm). Result: a `released`/`rejected` from one client can settle-and-drop (or requeue) another client's delivery of the *same numeric id* on the same thread. Requires the hop-park interleave; still a real cross-client delivery-state confusion. Fix: stack-local `ulong[]` (or small fixed buffer); it's bounded by in-flight count.

---

## FLAG 4 — DETACH with no handle field silently kills link 0 (LOW)

**Where:** PERF_DETACH handler: `uint handle;` default-init 0 when `nf < 1`, then `ps.links.remove(0)` and requeue of handle 0's unsettled deliveries. A malformed/minimal detach frame (`attach` fields omitted) tears down the client's handle-0 link and requeues its in-flight deliveries — a protocol-contract violation, at worst self-inflicted. Fix: `handle = uint.max` sentinel, skip if not supplied.

---

## SUSPECT (unproven from this file alone)

- **No per-user authorization on the `$management` node.** The connection-level SASL gate is fixed, but unlike the Kafka `authorize()` fix, nothing here checks the *authenticated user* against queue/exchange ownership: any credentialed user can `DELETE /queues/<any>`, declare/delete exchanges, rebind bindings. If 0-9-1 has per-vhost/user ACLs, this is a gap. Need amqp.d's ACL surface to confirm.
- **`a10Publish` yield behavior** — if it parks with the caller's `props.data`/`bodyBuf.data` slices (likely, it funnels into `gAmqpPush`/`amqpDataExec` which park), Flag 1's publish-path corruption fires at shards=1 too, not only sharded. One read of amqp.d `a10Publish` confirms.
- **`gA10ArgsHash`/`gA10QueueType`/`gA10ArgsRaw` are TLS** — redeclare-equivalence (409) and queue-type are only enforced on the shard that saw the declare; a client hashed to another shard redeclares the same queue with different `x-max-length`/type and gets 201. Correctness/divergence, not disclosure.
- **Transfer ignores link credit and link role** — a client can TRANSFER on a *receiver* link's handle (message gets RPUSH'd to that queue) and exceed granted credit; bounded by Flag 2's fix, but worth enforcing role/credit for contract compliance.

---

## Ranked summary

1. HIGH — amqp10.d `a10ReadFrame`/transfer/mgmt TLS statics alias frame bodies across connections on hops → cross-client disclosure & record corruption (DoS-ONLY, no RCE primitive).
2. MED — 4096 links × 16 MiB fragment buffers = ~64 GiB per connection, unauthenticated-before-auth DoS.
3. MED-LOW — `dispIds` TLS cross-conn reuse: cross-client settle/requeue confusion on colliding delivery-ids.
4. LOW — handle-less DETACH destroys link 0.
5. SUSPECT — no per-user authorize on $management; a10Publish park promotes Flag 1 to shards=1; TLS gA10Args* shard-local 409 bypass.

---

# CTF flags — `amqp.d`

# GLM-CTF-REPORT — `source/dreads/amqp.d` (post-b2dc859 verification pass)

## Verification of the landed hardening in this file

The handshake gate (`cls != 10 && !c.opened` with `aclUserCount()>1`), the SASL PLAIN/AMQPLAIN validation, the vhost-access 530, the stack-copy pattern around every `queueKey` TLS buffer that survives a hop (`basic.get`, `queue.delete`, `queue.purge`, `enforceMaxLen`, `amqpTtlSweep`, `a10Pop`, `a10PeekAt`), the busy-guarded TLS statics (`recStatic`, `dlrecStatic`, `destStatic`, `cbStatic` in `ctlBroadcast`), the topic-match word cap, and the exchange/queue cardinality caps all check out — I could not re-derive a memory-safety break from any of them. The two frame-length checks (`fsize+8 > frameMax` before tune, size_t arithmetic on `7+fsize+1`) are overflow-free. `splitRecord` v1–v4, `tableWalk`, `propsHeaders`, `mergeXDeath`, `appendHeadersExcept` are all bounds-checked on every client-controlled length.

What remains:

---

## FLAG 1 (HIGH) — No per-operation ACL authorization: any authenticated user reaches every queue/exchange

**File:** `amqp.d`, `handleFrame` — `c.aclAuth` is stored at start-ok (~line of `c.aclAuth = au;`) and consulted exactly once afterwards: the vhost-grant check in `connection.open`. It is **never checked again** on `queue.declare/bind/purge/delete`, `exchange.*`, `basic.publish/get/consume`, or `tx.commit`.

**Root cause:** The b2dc859 hardening added `authorize()` gates to the Kafka admin handlers and the AMQP *handshake*, but the AMQP 0-9-1 data plane got only the handshake gate. `AclUser.root.keyPats` (key grants) are never applied to AMQP queue/exchange names, unlike the Kafka skin.

**Attack trace (ACL configured, `aclUserCount()>1`):**
1. Connect, `start-ok` PLAIN as restricted user `bob` (granted only `keyPats: ["bob.*"]`, plus one chanPat so the vhost-530 check passes).
2. `connection.open "/"` → open-ok (bob has *a* grant, so the coarse vhost check passes).
3. `queue.declare vip.queue` ; `basic.get vip.queue` → **bob reads victim's queue** (records are plaintext `amq.q.vip.queue` in DB 16).
4. Or `basic.publish exchange=amq.direct rk=victim.queue` → writes into any victim queue.

Contrast: the same `bob` on the Kafka skin is stopped by the new `authorize()` calls. This is a cross-skin authorization asymmetry, i.e. the "sibling site" the hardening commit missed.

**Severity:** HIGH (cross-tenant read/write with any valid low-priv credential; not exploitable with zero creds).

**Fix:** mirror the Kafka pattern: on every class 40/50/60 method, resolve the target name(s) (`ex`/`q`/resolved routing destinations) and call the shared ACL check against `c.aclAuth`, answering 403 ACCESS_REFUSED on denial.

---

## FLAG 2 (MED) — Per-connection RAM DoS: 2047 channels × 128 MB staged publish bodies

**File:** `amqp.d` — `AMQP_MAX_BODY = 128MB`, `AMQP_MAX_CHANNELS = 2047`, `PendingPub.payload` per channel; `handleFrame` FRAME_BODY appends `p` into `ch.pub.payload` with only the per-message `bodySize ≤ AMQP_MAX_BODY` bound.

**Root cause:** The byte caps that exist (`AMQP_MAX_TX_BYTES`, `AMQP_MAX_UNACKED_BYTES`) cover tx buffers and the unacked window — nothing bounds the sum of *in-progress content assemblies* across channels. Each of the 2047 channels can hold one incomplete publish of 128 MB → ~256 GB pinned per connection, and the bytes only need to *arrive once* (they stay pinned indefinitely: just never send the final body frame, so `finishPublish` never fires and the buffer never drains).

**Attack trace:** open 2047 channels; on each, `basic.publish` + content-header with `bodySize = 128MB`; dribble body frames (or send all — either way the payload persists until the header's bodySize is satisfied); stop. One connection pins ~256 GB (OOM) or, with N smaller connections, exhausts memory proportionally. Broker-side there is no eviction: the pending pubs live until the channel/conn closes.

**Severity:** MED (remote DoS, bandwidth-bound to pin but *keeps* the memory after; a 10 Gbps peer pins 128 MB in ~0.1 s per channel).

**Fix:** a per-connection `pendingBytes` accumulator like `unackedBytes`, capped (e.g. 64 MB); exceeding it → connection 501 FRAME_ERROR close. Cheap: add in FRAME_BODY, subtract in `finishPublish`/channel teardown.

---

## FLAG 3 (LOW/MED, correctness) — `putShortStr` length-byte truncation desyncs consumer frame streams for >255-byte routing keys

**File:** `amqp.d` — `putShortStr` does `o.appendByte(cast(char) s.length)` with no clamp; reachable with a long rk via `basic.deliver`/`basic.get-ok` (`putShortStr(b, drk)`) and `basic.return` (`putShortStr(b, rkn)`).

**Attack trace (cross-protocol, requires RESP access to DB 16 — cross-protocol ingest is an advertised feature):**
1. RESP: `SELECT 16; RPUSH amq.q.victim <crafted v4 record>` where the record's routing-key field is 300 bytes (splitRecord accepts up to 0xFFFF, `buildRecord` clamps at 0xFFFF — but a hand-built record has no clamp).
2. AMQP consumer on `victim`: `basic.get` → `recordRoutingKey` returns the 300-byte rk → `putShortStr(b, grk)` emits length byte `0x2C` (300 & 0xFF) followed by **300 bytes**. Every spec-strict consumer (pika/java) now parses 44 bytes of rk and then misinterprets the remaining 256 rk bytes + the following content-header frame as method fields → stream desync, client-side crashes/parser confusion. The frame's declared size still matches, so this is a *persistent* desync poisoning the victim's consumer, triggerable by a third party with RESP access.

**Severity:** LOW-MED (client-side parser confusion / DoS of the victim's consumer; no broker memory unsafety — `append` copies the real slice, no OOB).

**Fix:** clamp in `putShortStr` (`s[0 .. min(s.length,255)]`) or truncate rk at the record boundary; same for `emitContent`'s verbatim `props` (a crafted u32 propLen also lets the content-**header** frame exceed the negotiated `frameMax`, violating the tune contract — clamp/split like body frames).

---

## SUSPECT (unproven from this file alone)

1. **`settleNegative` / `requeueAllUnacked` TLS key buffers across `gAmqpPushFront`'s hop:** `kb4`/`rq4`/`kb6`/`rq6` are TLS statics passed by slice into `gAmqpPushFront` → `amqpDataExec`, which (per `kafkaGroupHopImpl`'s comment) can yield *while the hop payload is being staged from the caller's buffer*. If `amqpDataExec` does not copy `args` into its hop buffer before its first yield, a concurrent fiber's `queueKey` clobbers `kb4` mid-hop → LPUSH to the wrong queue key (message misdelivery). Every other site in this file stack-copies first; these do not. Need `amqpDataExec`'s body to confirm; if it copies args before yielding, downgrade to style.
2. **`channel.open` replacing a live channel number** (`c.chans[chan] = Channel(true)` without requeue): unacked records survive in `c.unacked` (keyed by tag, channel-filtered by number), so no loss — but a tx buffered on the old channel struct is silently dropped. Correctness nit, not a flag.
3. **`gQueuePrios` never cleaned on queue delete (op 9)** — unbounded only up to the declare cap; leak-only.

## Ranked summary

1. FLAG 1 HIGH — amqp.d handleFrame: no per-op ACL check after handshake; restricted user reads/writes any queue/exchange (authz bypass, sibling-miss of b2dc859).
2. FLAG 2 MED — amqp.d FRAME_BODY: no per-conn cap on staged publish bodies; 2047×128MB ≈ 256 GB pinned per connection (remote DoS).
3. FLAG 3 LOW/MED — amqp.d putShortStr/emitContent: >255-byte rk (crafted cross-protocol record) truncates the length byte → persistent consumer frame-stream desync; header frame can also exceed negotiated frameMax.
4. SUSPECT — settleNegative/requeueAllUnacked TLS kb4/rq4/kb6/rq6 across gAmqpPushFront yield (needs amqpDataExec source).

---

# CTF flags — `kafka.d`

# GLM-CTF-REPORT — kafka.d pass (post-hardening verification)

The landed fixes in this file check out: Produce/Fetch topic `authorize()` are present and placed before any TLS staging; the OffsetFetch/MetadataFlex TLS-vs-fiber-local splits are correct (`allBuf` is non-static, fetch key copies to stack before `partLen`'s hop, meta is filled last with no hop between fill and read); `kafkaPlainCheck` no longer records the claimed principal in legacy mode; `kafkaAclPrime` is deferred via `runTask`. Good. Three sibling sites were missed, though — one serious.

---

## FLAG 1 (HIGH, cross-tenant write corruption — PLAUSIBLE, precondition named) — TLS-static aliasing across the produce-store hop under SPSC backpressure

**Sites:** `handleProduce` — `static ByteBuffer kb` (part key), `static ByteBuffer blobArena`, `static const(char)[][] slices`, `static size_t[] offs`; `pushRecords` — `static ByteBuffer rb` reply and `static const(char)[][] argv` (all in kafka.d, `pushRecords`/`handleProduce` region). Same class: `handleAlterConfigs`'s `static ByteBuffer ckey` passed live into `gKafkaExec`.

**Root cause:** `pushRecords(kb.data.asChars, slices[0..nrec])` hands *slices into three TLS statics* to `gKafkaExec` (→ `amqpDataExec` → `shardEnqueue`). `shardEnqueue` **yields on a full ring** (`lane.push` fails → `shardWake` + `yield` → retry) while the payload pointer still aliases the TLS buffers. During that yield, another connection's fiber **on the same shard thread** runs `handleProduce`, which does `partKey(topic2, part2, kb)` and `blobArena.clear()` — rewriting the exact bytes the first fiber is about to push. `kafkaGroupHopImpl` in server.d fixed this exact hazard for its own hop ("hb is STACK-local: shardEnqueue yields under ring backpressure and a TLS static would be clobbered…") and `amqpPushStage` ditto — but the Kafka produce store path was never converted.

**Attack trace (sharded mode, shards≥2):**
1. Attacker opens two Kafka connections that the SO_REUSEPORT router lands on the **same** shard thread, both producing to topics whose keys hash to shard 1 (owner).
2. Stall shard 1's drain: its drain fiber is synchronous, so a single 64 MB Produce containing ~1M tiny records (`KAFKA_MAX_RECORDS = 1<<20` crc32 validations, explicitly "no yield") blocks the consumer for seconds while its 16384-slot inbound ring (`INBOUND_CAP`) fills with fire-and-forget traffic the attacker also generates (AMQP `basic.publish` with `gAmqpPush` is fire-and-forget — no reply needed to fill the ring).
3. Conn A sends Produce(topicA) → its fiber reaches `shardEnqueue`, ring full, `yield()`.
4. Conn B's Produce(topicB) runs, rewrites `kb`, `blobArena`, `argv`, `slices`.
5. Conn A resumes: `lane.push` retries and copies **topicB's key with topicA's fiber's argv/slices** (or any interleaving) into the ring.

**Impact:** records appended to the wrong topic's list with the wrong base-offset acked to the producer — cross-tenant data misdirection + durable offset corruption (AOF-persisted). Memory-safe, so **not RCE**; it is a data-integrity/cross-tenant flag, the broker's worst class after memory safety.

**Precondition (named honestly):** the SPSC lane must actually be full when the retry happens. Under an attacker-driven load pattern as above this is reachable; it is not reachable on an idle broker. I rank it HIGH because the codebase itself treats this exact pattern as a must-fix class (two sibling commits), and the precondition is attacker-controllable.

**Fix:** make `kb`, `blobArena`, `slices`, `argv` fiber-local (stack `ByteBuffer`s as `kafkaGroupHopImpl` does), or copy the flattened `rpush key rec1..recN` wire bytes into a stack buffer before the single `gKafkaExec` call. Same for `handleAlterConfigs`'s `ckey`.

---

## FLAG 2 (MED, ACL bypass / cross-tenant disclosure) — no `authorize()` on the read/admin-describe APIs

**Sites:** `handleDescribeAcls` (no gate — contrast `handleCreateAcls`/`handleDeleteAcls`, which were gated in the hardening commit), `handleDescribeConfigs`, `handleListGroups`, `handleDescribeGroups`, `handleOffsetFetch`/`handleOffsetFetchFlex`, `handleMetadata`/`handleMetadataFlex`, `handleHeartbeat`.

**Root cause:** the hardening pass added `authorize()` to the *write* admin APIs and Produce/Fetch, but every DESCRIBE-class API answers unconditionally. Once `gKafkaAclActive > 0` (one CreateAcls from an admin), the model promises enforcement — an anonymous principal can still enumerate everything.

**Attack trace (one connection, no SASL, `kafka-require-sasl` off — the default):
- `OffsetFetch` v2, `group="victim-group"`, topics = null (`rawN < 0`) → `emitAllGroupOffsets` HGETALLs `kafka.cg.victim-group` and dumps **every committed offset and its metadata** for a group the attacker never joined. Committed metadata is client-chosen app data → direct cross-tenant disclosure.
- `DescribeAcls` v0 with an all-wildcard filter → dumps the full ACL store including **every principal name and host** (`kafka.acls` HGETALL, no gate at all).
- All-topics `Metadata` v1 (`topics = -1`) → lists every topic name in `kafka.topics`; `ListGroups` lists every group.

**Severity:** MED — information disclosure + ACL-model bypass; no memory-safety impact.

**Fix:** gate each with `authorize(tKafkaCtx, KRES_GROUP/KRES_CLUSTER/KRES_TOPIC, name, KOP_DESCRIBE/KOP_READ)`; at minimum gate `DescribeAcls` with `KOP_DESCRIBE` on CLUSTER and `OffsetFetch` with `KOP_READ` on the group.

---

## FLAG 3 (LOW–MED, correctness / cross-group aliasing) — group-id truncation in fixed stack copies of `kafka.cg.<group>` keys

**Sites:** `handleDeleteGroups` (`char[9+256] gst`), `handleOffsetDelete` (same), `handleEndTxn` (`char[256] gbuf`, `char[256][64] otb`).

**Root cause:** the *store* path (`storeGroupOffset`/`storeGroupMeta`/`fetchGroupOffset`) uses the unbounded `keyb.data` ("kafka.cg." + full group, group is an i16 string up to 32767 bytes), but the *delete/txn-apply* paths clamp the same key into a 265- or 256-byte stack buffer. For groups longer than ~246–256 chars the key silently truncates.

**Trace:** client A commits offsets for group `"G"*300 + "A"`, client B for `"G"*300 + "B"` (distinct full keys). `DeleteGroups("G"*300+"A")` → `DEL kafka.cg.<first 256 chars>` — hits a key nobody wrote (silent no-op), and if any group's *full* name happens to equal a truncated prefix (e.g. a group literally named `"G"*256`), deletes/EndTxn-offset-applies land on **that** group's hash — cross-group offset corruption. Also `EndTxn`'s `gbuf`/`otb` truncate topic (256) and metadata (128) similarly for TxnOffsetCommit echo.

**Fix:** validate group length (≤249, like topics) at every entry point, or reject groups whose composed key exceeds the copy buffer instead of clamping.

---

## SUSPECT (unproven from this file alone)

- **`kafkaScramStep` client-first:** `char[32] sb; b64enc(au.scramSalt, sb)` — if the stored SCRAM salt is >24 bytes, base64 needs >32 chars. Verify `scramSalt` length and `b64enc`'s bound handling in `dreads.tls`/`dreads.acl`; if unbounded it is a stack overflow (would be CRITICAL, RCE-grade). Needs the acl.d definition to confirm.
- **`authorize()` ignores the binding `host` field** entirely — KIP-140 host-scoped ALLOW/DENY is stored and echoed but never matched. Real but low-impact (flat "no host checking" posture); fold into Flag 2's fix.
- **`handleDeleteTopics`** performs up to 1024 `DEL` hops per topic × 64 topics with no per-request hop budget (unlike Metadata/Fetch's `tMetaProbes`) — bounded but slow; low-grade CPU/latency DoS.
- **Truncated request header** (`sz < 8`): `handleRequest` returns with zero response bytes → that connection hangs until timeout. Per-connection DoS only; arguably spec-tolerable.

---

## Ranked summary

1. HIGH — kafka.d `handleProduce`/`pushRecords` (also `handleAlterConfigs` ckey): TLS statics (`kb`/`blobArena`/`slices`/`argv`) live across `shardEnqueue`'s full-ring yield → cross-topic record misdirection/offset corruption; precondition (full SPSC lane) attacker-forgeable. DoS-no, corruption-yes.
2. MED — missing `authorize()` on `OffsetFetch` (committed offsets+metadata dump), `DescribeAcls` (principal/host dump), `DescribeConfigs`, `List/DescribeGroups`, `Metadata`, `Heartbeat` — ACL-model bypass + cross-tenant disclosure once any ACL exists.
3. LOW–MED — `kafka.cg.<group>` key truncation (265/256-byte stack copies in DeleteGroups/OffsetDelete/EndTxn) → silent delete failure / cross-group aliasing for long group ids.
4. SUSPECT — `b64enc(scramSalt → char[32])` possible stack overflow (needs acl.d salt size); authorize() host field unenforced; DeleteTopics hop storm.
