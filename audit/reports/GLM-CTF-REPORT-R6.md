# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

# sqs.d — Round 6 Report

## Verdict

**No new provable memory-safety, cross-client-disclosure, data-corruption, or authorization defect.** The round-4/5 fixes (per-call `qkey`/`ifkey`/`grpkey`, `snapCopy` ownership, `recCopy` before the parking `hset`, per-call `bodyBuf`/`ebodyBuf` out of `jsonStr`'s TLS `ub`, `SqsErr` as a per-call `ref`) all verify as holding under a re-read:

- `opReceiveMessage`: `recs[]`/`batchGroups[]` slice only into the per-call `snapCopy`; `rec` is copied into a per-call `recCopy` before the first parking `exec` that would refill the TLS `rb`; `val` holds only per-call data.
- `opSendMessage`/`opSendMessageBatch`: bodies are copied out of `jsonStr`'s TLS `ub` before `sendOne`/`dedupSeen`/`dedupStore` can park; oversize bodies are rejected against `SQS_MAX_BODY` using the raw-length upper bound before decode.
- `fifoUnlock`: the `group2` slice taken from `rb.data` is passed as an exec ARG (serialized into the hop payload before `rb` can be refilled by a sibling fiber) — the refuted-safe pattern.
- `sqsVisibilitySweep`: `namesCopy` decouples the outer iteration from `names`; `expHandles`/`expRecs`/`dueIds`/`dueRecs`/`expDedup` TLS statics are reset per queue and are not read across an `exec` that refills them (`expHandles[i]` slices point into `rb.data`, which is not reused until the next queue's `hgetall`); the packed-record length arithmetic (`pi + 4 > packed.length`, `pi + rl > packed.length`) is bounds-checked before every slice.
- Decoders (`respBulk`, `respEachBulk`, `splitRecord`, `jsonStr`, `jsonStrRaw`, `findKey`, `jsonEachEntry`, `header`, `toSize`): bounds-checked; `splitRecord`'s `f` is default-initialized (null slices, no uninitialized read); `findKey` is depth-aware so a value string can't spoof a top-level field.

## SUSPECT (unproven from the code shown — needs one fact confirmed)

**S-1. Visibility/delay/dedup sweep may only run on shard 0's event loop → visibility-timeout redelivery, delayed-message promotion, and dedup reaping never happen for queues owned by shards ≥ 1.**
- Where: `server.d` (the single `setTimer(1.seconds, () nothrow { sqsVisibilitySweep(); }, true)` in the skin setup) vs `sqs.d sqsVisibilitySweep`'s own guard `if (sharded() && shardOfSlot(keyToSlot(ifkey.data)) != tShard) return;`.
- Trace: with `--shards 2`, create queue `q` whose `sqs.if.q` slot maps to shard 1 → `SendMessage` → `ReceiveMessage` with `VisibilityTimeout=0` (deadline = now). The sweep fiber lives on the main thread's event loop (tShard = 0); the guard returns for shard-1 keys, so the expired record is never `LPUSH`ed back and the handle never `HDEL`ed — the message is permanently invisible (and `sqs.dl.`/`sqs.dd.` cleanup likewise never runs, so the dedup hash grows unbounded). Same class for delayed sends: `DelaySeconds=10` messages on non-zero shards never promote.
- Why not proven: the sweep's own comment says "Called by a per-shard timer," and `startShards` (not in the provided extract) may install per-shard timers on the worker event loops. If it does, the guard is correct and this is a non-issue. To confirm: check whether `startShards` registers `sqsVisibilitySweep` on each worker loop; if not, the fix is to register the timer per shard (or drop the guard and let the single timer hop — `exec` already hops correctly from any thread).

## Ranked summary

1. SUSPECT S-1 — cross-shard visibility-sweep coverage: possible permanent message invisibility / unbounded dedup growth on shards ≥ 1 (MED if `startShards` lacks the per-shard timer; non-issue otherwise).

No new provable defect; round-4/5 fixes verified holding. DoS-hardening backlog: `recs[1024]` cap in `opReceiveMessage` silently truncates FIFO snapshots of deeper queues (delivery stalls until next receive) — behavioral, one line, not a flag.

---

# CTF flags — `kafkagroup.d`

# kafkagroup.d — Round 6

## Verdict: no new provable defect; fixes verified

I re-read the full file against the round-5 baseline. Traces I actively tried and why each fails to be a flag:

- **KgRd bounds** (`str16`/`bytes32`/`i32`/`u8`): every reader checks `i + n > p.length` before slicing and flips `ok`; all call sites bail on `!r.ok`. No over-read.
- **TXN_ADD scratch** (`char[300] tb`): topic capped at 249, `part` capped at 1,000,000 (≤7 digits), so `tl + 1 + pl ≤ 257 < 300`; `snprintf` is bounded by the remaining buffer. No overflow; the `.idup` also escapes the stack buffer correctly.
- **TXN_END emit**: the round-5 wStr16/overflow fix holds — all `wStr16` inputs originate from `str16`-parsed (≤65535) or `.idup`'d stored strings; nothing exceeds the u16 length cap, and `wI32` counts are capped by `KG_TXN_MAX_TPS`/`KG_TXN_MAX_OFFS` on the buffering side.
- **TXN_INIT pid**: CSPRNG-minted (sequential fallback), epoch fencing on ADD/OFFSETS/END is checked before any state mutation. No cross-producer takeover without knowing pid+epoch.
- **Static-member reclaim / KIP-394 79 path**: `useMid` is either a stored key, `.idup`'d, or built from a stack `buf` + `.idup`; nothing aliases the ring payload after return. `closeBarrier`-removal guards before `emitJoinOk` are present in both JOIN and JOIN_POLL.
- **Ring-slice lifetime**: `mid`, `gii`, `clientId`, `grp`, `meta` etc. are only compared or copied (`wStr16`/`wBytes32` append) within the synchronous drain call — no slice is stored past the call, and the drain never yields.
- **Unbounded-growth check**: groups capped (KG_MAX_GROUPS), members capped (KG_MAX_MEMBERS), txn partitions/offsets/meta-bytes capped, txns idle-evicted in `kgroupSweep`. `KGOP_LEAVE` count clamped to KG_MAX_MEMBERS.
- **DoS backlog (one line)**: `evictStale`'s dead-list × order rescan is O(n²) at ≤512 members on the 50 ms tick — negligible, backlog-only, not a flag.

Converged: kafkagroup.d is clean this round.

---

# CTF flags — `mqtt.d`

# MQTT (mqtt.d) — Round 6 Audit

## Result

**No new provable memory-safety, cross-client-disclosure, data-corruption, or authorization defect; fixes verified.**

### Verification notes (what I re-checked and why it holds)

- **TLS-static scratch across yields**: every remaining static in a yielding path was either already converted per-call (`takeoverLocal`'s `db`, WS `reqbuf`/`respbuf`, `c.wsOut`) or provably yield-free in its fill→consume window:
  - SUBSCRIBE's `granted[64]`/`filters[64]`/`retainOk[64]`, `pb`, `sidBuf`, `q1`, `pkt`/`pktV5`, `filtered` (expiry sweep), `wpBuf`, `fwdProps` (fanout copies into the SPSC ring before return — the refuted args-passed-to-hop pattern): all filled and fully copied out (`.idup`, `obox.append`, `o.append`) with no suspension point in between.
  - `static ubyte[65536] wsread` and wss `static ByteBuffer plain`: filled and synchronously fed to `wsCodec.feed` (which copies) with no yield.
- **Decoder bounds**: `rdStr`, `decodeVarint` (≤4 bytes), `mqttParseConnectProps` / `ParseSubProps` / `ParsePubProps` / `ParseWillProps` / `DisconnectSEI` / `msgExpiryFromProps` / `expiryValueOffsetInPacket` / `stripExpiryProp` all bound every length-prefixed skip by an `end ≤ p.length` gate; the CONNECT keepalive truncation check (`i + 2 > p.length`) holds.
- **AuthZ**: PUBLISH topic ACL and `$`-topic drop are applied on the *resolved* (post-alias) topic; will-topic ACL is enforced at CONNECT and re-checked in `fireWill`; SUBSCRIBE filters are ACL-gated with correct wildcard/literal match modes; takeover uses the monotonic `connGen` so a stale broadcast cannot evict a newer session.
- **State/hop safety**: `ShardMsg.mqttPub` fan-in slices into the ring are consumed without yield inside `mqttDeliverLocal` (all escapes are `.idup`/copies); `mqttFlushDirty` is yield-free; `kafkaGroupHopImpl`-style pending handling has no MQTT analog that parks on a shared static; the xshard-adopt UAF and double-delivery window remain the two known-deferred TODOs and were not re-raised.
- `mqttParkOrEnd`'s "returns true" rebind path is currently dead code (it always returns false) — a behavioral note only, not a defect.

### DoS-hardening backlog (one line, not a flag)

- `inflightMsg` (per-conn persistent-session redelivery cache) is count-bounded by the window (≤1024) but not byte-bounded; with `session-expiry > 0`, one slow-acking subscriber can pin up to ~1024 × 16 MB ≈ 16 GB of idup'd PUBLISH packets — consider a byte cap like `heldBytes`.

## Ranked summary

(none — mqtt.d: no new provable defect; fixes verified)

---

# CTF flags — `amqp10.d`

# GLM-CTF-REPORT — Round 6, `source/dreads/amqp10.d`

After a full read against the round-5 state (mgmtCopy, modAnnCopy, dispScratch, per-call scratch, fragment budgets, tKafkaCtx-class guards all verified present and holding), I found **one provable new defect** (a durability/data-loss race) plus one minor protocol-contract bug on the same fiber. No memory-corruption, cross-client disclosure, or authz bypass remains reachable from this file's decoder or handlers.

---

## FLAG 1 (MED) — Popped message stranded in `unsettled` after `a10TeardownRequeue` already ran → permanent message loss on disconnect

**File:** `amqp10.d` — `a10StartDelivery` delivery-fiber loop (the `ps5.unsettled[did] = A10Out(...)` insertion) racing `a10TeardownRequeue` / the `scope (exit)` in `amqp10Serve`.

**Severity:** MED (data loss of an at-least-once delivery; requires sharded/hopped `a10Pop` timing).

**Trace:**
1. Client opens an AMQP 1.0 connection, `begin`, attaches a **receiver** link to queue `q` and grants credit via flow.
2. The delivery fiber for the link runs its loop: `got = a10Pop(pl5.rkey, pay)` — under sharding this parks in the data-plane hop (LPOP routed to the owning shard).
3. While it is parked, the client **closes the TCP connection** (or sends `close`). The read fiber exits its loop and runs `amqp10Serve`'s `scope (exit)`:
   - `a10TeardownRequeue(c)` → requeues every entry currently in `ps.unsettled` (the deliveries already sent unsettled);
   - `closeQuiet(tcp)`.
4. The `a10Pop` reply now lands; the delivery fiber **resumes after the pop** — the message `M` is already removed from `q`'s list. Nothing re-checks `cc.closing` between the pop and the bookkeeping, so it executes:
   `ps5.unsettled[did] = A10Out(pl5.rkey, pay.data.idup, h5, pl5.stream);` and attempts the transfer (send fails on the closed socket).
5. `M` now lives only in `ps.unsettled`. The teardown that requeues unsettled deliveries has **already run**; the link/conn are dead. `M` is never requeued and never delivered → **silent loss of an acked-durably-stored message**, violating the unsettled-redelivery contract (AMQP 1.0 §2.6.12 / our at-least-once guarantee). With durable messages + AOF, `M` exists in the AOF under `q` but was popped before the loss — same result: gone from `q`.

Note the DETACH/END paths are *not* affected the same way (their `a10RequeueUnsettled` is followed by connection teardown which re-sweeps all unsettled) — only the final teardown race strands the entry, because nothing runs after it.

**Root cause:** the delivery fiber inserts into `ps.unsettled` after a parking operation (`a10Pop`), with no revalidation that the teardown sweep hasn't already passed. The fiber-local check `while (!cc.closing)` only guards the *top* of the loop.

**Fix:** after `a10Pop`/`a10PeekAt` returns, re-check `cc.closing` (and `pl5.detached`); if set, `a10Requeue(pl5.rkey, pay.data)` the just-popped blob (or, for streams, simply drop the position increment) and return *before* inserting into `unsettled` or sending. Alternatively, have the teardown set a `swept` flag on the conn and have the delivery fiber treat `swept == true` as "requeue locally instead of tracking".

---

## FLAG 2 (LOW/MED) — Delivery fiber uses stale link pointer across the `a10Pop` park: transfer sent on a detached handle, credit/delivery-count mutated on a removed link

**File:** `amqp10.d` — same loop: `auto pl5 = h5 in ps5.links;` fetched once, then `a10Pop` parks; the read fiber's `PERF_DETACH` handler runs `ps.links.remove(handle)` during the park (it does set `pl4.detached = true` *before* removal, but on the **old** node — the fiber never re-reads it mid-iteration).

**Trace:** receiver link with credit → fiber parks in `a10Pop` → client sends `detach(handle)` → read fiber requeues unsettled for that handle and removes the link, echoing detach(closed) → fiber resumes, `pl5.deliveryCount++; pl5.outCredit--;` writes into the removed node, sends a TRANSFER for a handle the client has already detached (protocol violation; proton-j treats an uncorrelated handle as a connection error) and adds a fresh `unsettled[did]` that only the later teardown recovers.

Memory-safety note: the node is GC memory and the fiber stack roots it, so this is **not** a UAF — it is a contract violation plus mutation of dead state, hence LOW/MED, not HIGH. (I explicitly checked for an RCE angle: no primitive — no free, no allocator reuse path from `AA.remove` here.)

**Fix:** same as Flag 1 — after the pop returns, re-fetch `pl5 = h5 in ps5.links` and bail if null/detached (requeue the popped blob first).

---

## SUSPECT (not flagged)

- `static ByteBuffer sb` in the `a10HandleAttach` stream-timestamp offset scan (offKind 5/6): it is shared TLS across connections and the `a10PeekAt` inside parks — but the read of `sb.data` (in `splitRecord`) happens in the same fiber quantum after the callee returns, with no yield between fill and read, and each caller's own `a10PeekAt` overwrites `sb` after its park resolves. No read-after-hop of a clobbered slice could be constructed. Left as SUSPECT only for the (out-of-file) `rbk2`/`amqpDataExec` post-park clear discipline in server.d — if `amqpDataExec` does not clear its reply buffer after the park, that would be a server.d finding, not this file's.
- Unbounded `c.sessions` growth (65536 channels × A10Session) — DoS-hardening backlog at most: memory bounded by ~tens of MB per connection; one line under the backlog heading, consistent with prior rounds.

## Verified clean (this file)

Decoder bounds (`u8/be/take`, list/map size-vs-count, `MAX_DEPTH`), `a10ClampN`, shortstr 255 clamps in `a10MapMessage`, fragment budget accounting (add-after-success, single reclaim per path), `mgmtCopy`/`modAnnCopy`/`dispScratch` per-call copies, `a10Publish` recStatic guard interplay with `props`/`bodyBuf`/`hdrTbl` (filled and consumed with no cross-fiber read after return), `a10UriDecode` bounds, mgmt-link/SASL gates (bare header refused when ACL configured).

---

## Ranked summary

1. **MED — amqp10.d a10StartDelivery/amqp10Serve teardown race:** message popped via `a10Pop` inserted into `unsettled` after `a10TeardownRequeue` has swept → permanent silent loss of an unsettled delivery on client disconnect; re-check `cc.closing` post-pop and requeue locally.
2. **LOW/MED — amqp10.d stale `pl5` across `a10Pop` park:** transfer + credit/count mutation on a DETACH-removed link (GC keeps it non-UAF); re-fetch the link pointer after the park.
3. DoS backlog: unbounded per-conn `sessions` (65536 × A10Session) — cap as links already are.

---

# CTF flags — `amqp.d`

# GLM-CTF-REPORT — Round 6 — target: `source/dreads/amqp.d`

## FLAG 1 (HIGH — cross-client disclosure + queue-data corruption): `basic.get` emits / requeues a TLS-static payload buffer READ AFTER a cross-shard hop

**File:** `source/dreads/amqp.d`, `handleFrame` `case 70 (basic.get)`. The declaration `static ByteBuffer pay; // TLS` (~line of the get handler), the yield point `immutable remaining = gAmqpLen !is null ? gAmqpLen(getKey) : 0;`, and the post-hop reads `Unacked(q.idup, pay.data.idup, …)`, `recordRoutingKey(pay.data)`, `emitContent(o, chan, pay.data, c.frameMax)`.

**Guard check (per charter):** `pay` has **no** recBusy/recStatic-style guard (unlike `finishPublish`'s `rec`/`sp`/`drp`, `deadLetter`'s `dlrec`, `a10Publish`'s `recp`, and `ctlBroadcast`'s `cbBusy`). The sibling hazards in this same handler (the queue *key*) were fixed with stack copies (`getKeyStore`), but the popped *payload* was left on the shared static.

**Root cause / trace:**

1. Broker runs `--shards 2` (any N>1). Clients A and B both land on shard thread 1 (SO_REUSEPORT).
2. A: `basic.get` queue `QA`, `no-ack=false`, where `amq.q.QA` hashes to shard 2.
3. `gAmqpPop(getKey, pay)` hops to shard 2, fills TLS `pay` with A's message `M_A`, returns `getHit=true`.
4. Next statement `gAmqpLen(getKey)` ("llen") also hops to shard 2 → A's serve fiber **parks** on `pnd.done.wait` (the `kafkaGroupHopImpl`/`amqpDataExec` reply-park).
5. While A is parked, B's serve fiber on the SAME thread runs `basic.get` on queue `QB` (also owned by shard 2, holding `M_B`): `pay.clear(); gAmqpPop(getKey, pay)` overwrites the shared TLS buffer with `M_B`.
6. A's LLEN reply arrives; A resumes and **reads `pay` after the hop**: `c.unacked[gtag] = Unacked(q.idup, pay.data.idup, …)` then `emitContent(o, chan, pay.data, …)` → A's `basic.get-ok` carries **B's message body/headers/routing-key**.

**Impact:** cross-tenant message disclosure (B's payload delivered to A), plus corruption: the wrong blob is recorded in `c.unacked`, so A's later `basic.reject requeue=true` LPUSHes B's message into queue `QA`, and a `nack requeue=false` dead-letters B's message through A's DLX — persistent cross-queue data injection. Classification: not a memory-safety primitive (ByteBuffer bounds are respected); **information-disclosure / data-corruption HIGH**, wire-reachable by any unauthenticated-default client on a sharded deployment.

**Fix:** make `pay` a per-call local (`ByteBuffer pay;` — the consumer fiber already does exactly this), or stack/fresh-copy the popped blob before calling `gAmqpLen`. One-line change; the `gAmqpLen` hop can stay.

## Everything else

- `finishPublish` (rec/sp/drp/seenArena), `deadLetter` (dlrec/xbuf/paug), `a10Publish` (recStatic/recBusy), `ctlBroadcast` (cbBusy), the stack key-copies in purge/delete/get/enforceMaxLen/a10Pop/amqpTtlSweep: guards verified holding; no post-hop read of an unguarded TLS slice found.
- `settleNegative`/`requeueAllUnacked`/`a10Requeue*`: TLS buffers are consumed by `gAmqpPush(Front)` before its yield and the only post-yield use (`enforceMaxLen`) re-derives everything from idup'd strings — clean.
- Decoders (`Rd`, `tableWalk`, `propsHeaders`/`propsReplyTo`/`propsExpiration`/`replaceReplyTo`/`mergeXDeath`/`splitRecord`): all length checks re-verified; no OOB.
- DoS backlog (one line): per-conn `cancelledTags` spin-waits (200 ms) per bogus `basic.cancel` remain a mild CPU-amplification item only.

**Ranked summary:**

1. HIGH — amqp.d basic.get: TLS-static `pay` read after `gAmqpLen` cross-shard hop → cross-client message disclosure + wrong-blob requeue/dead-letter (no reentrancy guard on `pay`; fix = per-call buffer).

---

# CTF flags — `kafka.d`

# GLM-CTF-REPORT.md — kafka.d, Round 6

## Flag 1 (only new provable defect) — Unguarded raw 4-byte size patch in `handleFetch` records-size backpatch (OOM sibling of the fixed epilogue bug)

**File:line:** `source/dreads/kafka.d`, `handleFetch`, the block:

```d
// patch records size
auto d3 = cast(ubyte[]) o.data;
immutable rsz = o.length - recAt - 4;
d3[recAt]     = cast(ubyte)(rsz >> 24);
d3[recAt + 1] = cast(ubyte)(rsz >> 16);
d3[recAt + 2] = cast(ubyte)(rsz >> 8);
d3[recAt + 3] = cast(ubyte)(rsz & 0xFF);
```

**Severity:** HIGH (4-byte heap OOB write; corrupting-adjacent-class — same defect family as the round-fixed `handleRequest` epilogue patch, which got an explicit `o.length >= sizeAt + 4` guard; this inner patch did not).

**Root cause:** Earlier in the same loop iteration, `immutable recAt = o.length; putI32(o, 0);` reserves the records-length field. If that `putI32` (or a prior append in the same iteration) hits the allocator OOM path (`tByteBufferOom`), `ByteBuffer` appends become no-ops and `o.length` can stay at exactly `recAt` (or below `recAt + 4`). Every other backpatch site was hardened against this: `patchI32()` itself checks `off + 4 > d.length → return`, and the `handleRequest` epilogue checks `o.length >= sizeAt + 4` before its raw patch — with a comment explicitly naming the "4-byte OOB write past the allocation under -release (heap corruption → broker crash)" hazard. This raw patch has **no guard**. On the OOM path it writes 4 bytes at `o.data[recAt .. recAt+4]` past the live allocation.

**Concrete attack trace:**
1. Open a Kafka connection and produce enough records to topic `T` partition 0 that a single Fetch v4 response body approaches the response builder's memory ceiling (the broker's allocation-failure path — the same mechanism `tByteBufferOom` exists for).
2. Send `Fetch v4` for `T/0` with `partition_max_bytes = 0x7FFFFFFF` (clamped to `KAFKA_MAX_RESP`, i.e. a 128 MB budget) repeatedly on several connections until one reply build's `o.append` fails inside `encodeV2BatchFromInternal`/`emitV1Record` — note `overCap` is computed *before* the emission block, so a partition already past the ceiling still reaches the patch code (`overCap` only gates the *reading*, never the patch).
3. In the failing iteration, `putI32(o, 0)` at `recAt` no-ops (buffer at capacity, `tByteBufferOom = true`). Control falls straight to the patch: `o.length == recAt`, so `rsz = 0 - 4` underflows to `0xFFFF_FFFF_FFFF_FFFC`, and `d3[recAt..recAt+4]` writes `FF FF FF FF` four bytes **past the end of the malloc'd ByteBuffer allocation** — heap corruption under `-release` (no bounds check; `o.data` is `ubyte[]` into manual malloc'd memory, invisible to GC).
4. The corrupted word lands in whatever follows the buffer in the shard allocator — adjacent allocation header / freelist metadata / a neighboring client's reply buffer. `serveKafkaClient` then observes `tByteBufferOom` and drops only *this* client, but the write already happened.

**Exploitation assessment:** Primitive is a 4-byte relative heap OOB write of a fixed value (`0xFFFFFFFF`) at a groomable offset (the end of a reply allocation whose size the attacker drives via `max_bytes`/record counts). Not directly RCE: no info-leak primitive here and the value is constant. **Classification: DoS-ONLY to PLAUSIBLE heap-metadata corruption** — if the shard allocator's freelist/size words live after the buffer, `0xFFFFFFFF` there causes a subsequent free/alloc misbehavior (crash or further corruption). A full chain would need a second, value-controlled write; none is present.

**Fix:** Mirror the epilogue guard:

```d
if (o.length >= recAt + 4)
{
    auto d3 = cast(ubyte[]) o.data;
    immutable rsz = o.length - recAt - 4;
    ... // existing four byte stores
}
```

(Or replace the raw patch with the already-bounds-checked `patchI32(o, recAt, cast(int)(o.length - recAt - 4))` guarded on `o.length >= recAt + 4`.)

---

## Everything else — no new provable defect; fixes verified

Verified holding, per the round-5 charter:

- **tKafkaCtx stack-capture**: all 33 `authorize()` sites now capture `auto ctx = tKafkaCtx;` at handler top (ListGroups, DeleteGroups, OffsetDelete, Join/JoinFlex, OffsetCommit/OffsetFetch(±Flex), Heartbeat, Sync(±Flex), Leave, DescribeConfigs, DescribeGroups, InitProducerId, AddPartitions/Offsets, EndTxn, TxnOffsetCommit(±Flex), Metadata/MetadataFlex, CreateTopics, AlterConfigs, IncrementalAlterConfigs, CreatePartitions, DeleteRecords, DeleteTopics, Create/Describe/DeleteAcls, Produce, Fetch). `authorize()` itself copies the principal into a stack `pb` before its `aclLoadAll` hop.
- **Post-hop slice reads**: Fetch/ListOffsets partition keys go through the stack `keyStore`/`k3store` copies; `partBase` copies into `bst`/`pst` stacks before the GET hop; EndTxn copies topics/offset-group/metadata to stack arrays before the marker RPUSH hops; DescribeConfigs/CreateTopics/DeleteGroups/IncrementalAlterConfigs stage on stack arrays or per-op stack key copies; DeleteAcls copies matched fields into the fiber-local `delArena` before HDEL hops; `handleMetadataFlex` uses the fiber-local `allBuf` for the all-topics window. All correct.
- **Produce**: `kb`/`blobArena`/`slices`/`offs` staging, CRC validation, truncation rejection, `KAFKA_MAX_RECORDS`/`KAFKA_MAX_RESP`/decompression budgets (`KAFKA_DECOMP_MAX`, `KAFKA_DECOMP_REQ_MAX`) all verified bounded; `patchI32`/`patchCArrLen` are internally bounds-checked.
- **SCRAM**: `am[704]` overflow check (`ao + cfwp.length > am.length`), nonce/cfb bounds, `gbuf[256]` with the `<= 249` guard — all present.
- **Backlog (one line)**: `joinLoop`/`syncLoop` hold the connection up to the 70 s deadline per JoinGroup — protocol-faithful, DoS-hardening-backlog only.

**Ranked summary:**

| # | Flag | File:line | Severity | Class |
|---|------|-----------|----------|-------|
| 1 | Unguarded 4-byte raw records-size patch in `handleFetch` → heap OOB write `FF FF FF FF` on the reply-build OOM path | kafka.d (`handleFetch`, "// patch records size" block) | HIGH | DoS-ONLY / PLAUSIBLE heap corruption |
