# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

**sqs.d (round 7): no new provable defect; fixes verified.**

Verification notes on the leads I chased to ground before concluding:

1. **`opReceiveMessage` park-safety** — re-checked the round-6 hardening: `qkey`/`ifkey`/`grpkey` are per-call, the FIFO snapshot is owned per-call via `snapCopy` (the `recs[]` slices and `batchGroups` entries all point into it, not into a shared static), and the popped record is copied to per-call `recCopy` before the parking `hset`. The non-FIFO `rec = respBulk(rb.data)` (shared TLS `rb`) is copied out before any subsequent park. Clean.

2. **`jsonStr` TLS `ub[262144]` aliasing** — `opSendMessage` copies to per-call `bodyBuf` before `dedupSeen`/`sendOne` can park; `opSendMessageBatch` copies to per-entry `ebodyBuf` before `sendOne`. The `o < ub.length` bound in the unescaper also can't overflow (every branch is guarded). Clean.

3. **Shared-static-across-park in `opDeleteMessage`/`opDeleteMessageBatch` (`ifkey`)** — a sibling batch fiber *can* refill `ifkey` with another queue name during a park inside `fifoUnlock`/a prior entry, so the subsequent `hdel` can be issued against `sqs.if.<otherQueue>` with *this* request's handle. But receipt handles are 48 random hex generated per receive; the mismatched (key, handle) pair matches no field, so the worst outcome is a self-inflicted no-op (failed delete → later re-delivery of one's own message). No wrong-principal deletion, no cross-client disclosure, no OOB — this is the same class already considered and discounted in earlier rounds, not a flag.

4. **`sqsVisibilitySweep` statics (`rb`, `expHandles`, `expRecs`, `dueRecs`, `qkey`, `gk`)** — each `static` is *function-local*, so client-op fibers (which use their own per-function statics) cannot clobber them; the sweep runs as a single timer fiber per loop, and all packed-record buffers (`expRecs`/`dueRecs`) are copies taken before the parking execs. The `expHandles[i]` slices into `rb` are only consumed before the next `hgetall` refill of `rb`. Clean.

5. **`splitRecord` with <3 separators** — uninitialized-fields concern is void: `f` fields past `nf` remain `null` (empty slices), all downstream uses (`appendJsonStr`, `srem` args) length-check. No OOB read/write.

6. **JSON parsers** — `findKey` is depth-aware (nested `MessageAttributes` can't spoof top-level fields), `jsonEachEntry` brace-matches with escape handling, `jsonStrRaw` bounds-checks the escape skip (`i + 1 < j.length`). `splitRecord`/`respBulk`/`respEachBulk`/`respEachPair` all bound-check (`i + n > d.length` guards). The literal-`\x1f`-in-JSON name injection only creates oddly-named self-owned keys, not cross-tenant access.

7. **FIFO TOCTOU** (`sismember→lrem→sadd`, `dedupSeen→dedupStore`) — explicitly tagged TODO in-source; known-deferred, not re-raised.

DoS-hardening note (non-flag, one line): a FIFO queue holding >1024 records makes `opReceiveMessage` silently scan only the first 1024 snapshot entries per call — bounded work, no memory growth.

**sqs.d: no new provable defect; fixes verified.**

---

# CTF flags — `kafkagroup.d`

# kafkagroup.d — Round 7 audit

## Verification performed

I walked every opcode handler (`JOIN`, `JOIN_POLL`, `SYNC`, `HEARTBEAT`, `LEAVE`, `DESCRIBE`, `DROP`, `SUBSCRIBED`, `TXN_INIT/ADD/OFFSETS/END`, `COMMIT_CHECK`) against the memory-safety and cross-client criteria:

**Decoder bounds (`KgRd`)**: every read (`u8`, `i32`, `str16`, `bytes32`) pre-checks `i + n > p.length` and latches `ok=false`; `bytes32`'s negative-length case becomes a ~4 GiB `size_t` that fails the bounds check without wrap. The 8-byte pid/offset loops go through the checked `u8()`. No unchecked post-hop read.

**TXN_ADD stack buffer**: `char[300] tb` with topic ≤ 249 (rejected above), `\x1f` at index 249, `snprintf` into `tb.ptr+tl+1` with remaining ≥ 50 bytes for a part ≤ 1,000,000 (≤ 7 digits + NUL). No overflow path.

**Ring-slice lifetimes**: all strings stored into TLS state (`tGroups`, `tTxns`, member fields, `protoNames`/`protoMetas`, `assignment`, `KgTxnOff`) are `.idup`'d out of the ring payload; the map key is `groupName.idup`. The round-6-style "TLS static aliases the hop buffer during a park" class is not present — this file is the *drain-side* consumer, and `kgroupApply` never parks (the caller in server.d owns the pending wait; `kafkaGroupHopImpl` uses a stack `ByteBuffer` and re-clears `reply` after the park, verified in the context extract).

**Post-closeBarrier null-deref guards**: both `KGOP_JOIN` and `KGOP_JOIN_POLL` re-check `g.state == ST_EMPTY || (mid in g.members) is null` before `emitJoinOk`, so the removed-member case can't reach `emitJoinOk`'s `id in g.members` deref. Inside `emitJoinOk` the `order ⊆ members` invariant holds (every insert appends to both; `LEAVE`/`evictStale`/`closeBarrier` remove from both). `closeBarrier`'s `lead` deref is guarded by the `leaderAlive` re-election and the early-return on the no-common-protocol path.

**Grooming / OOM budgets**: `KG_MAX_GROUPS`, `KG_MAX_MEMBERS`, `KG_TXN_MAX_TPS/OFFS`, `KG_TXN_META_BUDGET`, topic-length ≤ 249, part ≤ 1e6 all cap attacker-driven growth; the O(N²) tps dedup was already replaced by the `tpsSeen` set. The txn `epoch++` short-wrap after 32 768 re-inits is an exact-match fencing value throughout — no bypass, DoS-grade at most.

**Authz / cross-client**: pid minting from CSPRNG prevents the TXN_ADD forging noted in the comment; group ops carry no principal here and no wrong-principal check is provable from this file alone.

## Conclusion

**no new provable defect; fixes verified.**

---

# CTF flags — `mqtt.d`

# GLM-CTF-REPORT.md — Round 7 (target: `source/dreads/mqtt.d`)

## FLAG 1 — HIGH — TLS-static read buffer shared by concurrent MQTT-over-WebSocket reads → cross-client packet injection / framing corruption

**File:line:** `source/dreads/mqtt.d`, `serveMqttClient` WS branch:

```d
static ubyte[65536] wsread = void;          // ← TLS STATIC, shared by EVERY WS conn on this thread
...
rn = c.tcp.read(wsread[0 .. ...], IOMode.once);   // parks the fiber with wsread as the read target
...
if (!c.wsCodec.feed(wsread[0 .. cast(size_t) rn]))
```

and the same defect pre-auth in the WS handshake loop:

```d
ubyte[8192] hb = void;   // OK — this one is fiber-local
```
(the TLS-leg path's `static ByteBuffer plain` is safe: filled and consumed with no suspension in between; only the raw-TCP WS read paths alias).

**Class:** cross-client data injection / data corruption (and memory-safety-adjacent framing confusion). Every previously fixed TLS-after-hop site was about *write* buffers; this is a *read* destination aliasing across concurrent fibers.

**Root cause:** `c.tcp.read(buf, IOMode.once)` parks the calling fiber and registers `buf` as the recv destination. `wsread` is a `static` (thread-local) 64 KB array shared by **all WebSocket MQTT connections served on that shard thread**. Two WS connections whose serve fibers are simultaneously parked in `read` therefore have **two overlapping recv destinations in the same memory**.

**Concrete attack trace** (shards ≥ 1; both connections must land on the same shard thread — SO_REUSEPORT hashing or repeated reconnects until same-thread, trivially achieved with shards=1):

1. Client A and client B each open `ws://broker:<mqttWsPort>` and complete the RFC 6455 upgrade. Both serve fibers reach the WS read loop and call `c.tcp.read(wsread[0..65536], IOMode.once)`; both park with recv registered on the **same** `wsread`.
2. B sends a masked WS binary frame containing `PUBLISH t/secret <payload>` (or `CONNECT` with B's credentials). The event loop runs B's recv first: B's plaintext lands in `wsread[0..nB]`, B's fiber is made runnable.
3. Before B's fiber is dispatched, A's socket also becomes readable; A's recv fires and **overwrites `wsread[0..nA]`** (the buffers alias). Whichever recv wrote last, both fibers now `feed()` bytes out of the same memory:
   - If A's fiber runs after B's overwrite: A's `wsCodec` is fed **B's MQTT bytes**. B's PUBLISH is decoded, authenticated, and delivered **as if it came from A's session** — a client with a denied ACL topic can get its publish executed under a victim session, or B's CONNECT/publish payload bytes are parsed inside A's protocol state.
   - Symmetrically, B receives A's bytes — direct cross-client content disclosure at the MQTT layer.
   - If the interleaved bytes split a WS frame boundary, `wsCodec.feed` decodes a hybrid frame → spurious close or corrupt MQTT packet parsed under the wrong conn.
4. Reliability of the trigger: the attacker fully controls both endpoints (A and B are both the attacker's sockets). Sending on B, waiting ~0, then sending on A deterministically makes the later recv clobber the earlier fiber's not-yet-consumed bytes, because the woken fiber only runs when the event loop yields to it — the second recv can be dispatched in between. Flooding both sockets makes the window trivially hit.

**Post-aliasing read line (proof of cross-client flow):** `c.wsCodec.feed(wsread[0 .. cast(size_t) rn])` — `rn` belongs to this fiber, but the bytes at `wsread[0..rn]` were last written by whichever sibling fiber's recv ran later. The length is self-consistent, the *contents* are another client's.

**Why this is not the already-refuted `fwdProps` class:** `fwdProps` was a write-side static whose slice was consumed synchronously before any hop; here the suspension point is **inside** the read itself — the buffer is live across the park by construction, no backpressure or OOM boundary needed, just two concurrent WS connections on one thread.

**Suggested fix:** make the read destination fiber-owned: `ubyte[65536] wsread = void;` as a plain local (stack; 64 KB is fine for a vibe fiber — or size it down to a chunked read like the 8192-byte handshake buffer), or stash a per-connection read scratch on `MqttConn` (it already owns `wsOut` for exactly this reason). Audit for any other `static` used as an `IOMode`-read destination in the skins — the pattern is "buffer live across a park", not "buffer crossing a hop".

**Severity rationale:** not RCE — the primitive is byte-level stream substitution between two attacker-reachable sessions, not a controlled write. But it *is* a genuine cross-client injection (ACL identity confusion on the MQTT channel) and disclosure channel between arbitrary tenants on a shared shard. HIGH.

---

## Files with no new finding

- `sqs.d`, `kafkagroup.d`: no new provable defect; fixes verified (per round-6 context).
- `amqp.d`, `amqp10.d`, `kafka.d`: not re-audited line-by-line this round beyond the fixed list; nothing new raised here.
- Remaining DoS-hardening backlog (obox caps, spin budget) — no new item.

## Ranked summary

1. **HIGH — mqtt.d serveMqttClient (WS read loop): `static ubyte[65536] wsread` shared as the `IOMode.once` read target by all WS connections on a thread → concurrent recv destinations alias → one client's MQTT bytes fed into another client's `wsCodec` (cross-client injection/disclosure, ACL identity confusion, framing corruption). PoC: two WS conns same shard, both parked in read, staggered sends. Fix: fiber-local or per-conn read buffer.**

---

# CTF flags — `amqp10.d`

# GLM-CTF-REPORT — Round 7, target `source/dreads/amqp10.d`

## Verdict

**no new provable defect; fixes verified**

## What I verified (and why it holds)

**Round-6 fixes confirmed present and correctly scoped:**
- `a10HandleTransfer` mgmt path: per-call `mgmtCopy` before `a10HandleMgmt` — corrRaw/to/subject/bodyMapBytes all slice the connection-owned copy, live across every `a10*Declare/Delete/Bind` park. ✔
- `a10HandleDisposition`: `modAnnBytes` copied to a stack `modAnnCopy` before the parking settle loop; `c.dispScratch` is connection-scoped and rebuilt per disposition. ✔
- Delivery fiber: `linkRkey`/`linkStream` captured before the fetch park; the `ps5r/pl5r` re-validation (with `rkey != linkRkey` mismatch → requeue-or-drop) sits before any post-park `ps5`/`pl5` use on the **non-stream** path. ✔

**Other candidate classes re-audited and refuted:**
- `a10ReadFrame` TLS `buf`: `size ≥ 8`, `doff ≥ 2`, `doff*4 ≤ size`, `size ≤ 1 MiB` all checked; `skip ≤ rest` follows from `doff*4 ≤ size`. All handlers either copy out (`idup`, stack buffers, `mgmtCopy`) or consume the slice synchronously before any yield (`props`/`bodyBuf`/`hdrTbl` → `a10Publish`; `a10MapMessage`'s `msgTo` is copied into stack `exB/rkB` by `a10ResolveAddress` before the `a10Publish` park).
- `nextOutgoingId` shared between the mgmt responder (read fiber) and delivery fibers: both run on the same event-loop thread and the `++` is never separated from its use by a yield, so ids stay unique; the presettled mgmt transfer inserts no `unsettled` entry, so no disposition/requeue confusion.
- Decoder core (`A10Dec`): `take/be/u8` all bounds-check; recursion bounded (`MAX_DEPTH 32`); list8/32, map8/32, array8/32 size-vs-payload validation (`sz < 1/4` reject, `take(sz-…)` re-checks) is sound.
- `a10UriDecode`, `a10ClampN`, shortstr clamps in `a10MapMessage`, and `a10PatchU32`'s bounds-checked patch — all correct.
- Memory-growth knobs (`A10_MAX_LINKS_PER_CONN`, `A10_MAX_PENDING_BYTES_PER_CONN`, `A10_MAX_UNSETTLED_PER_SESSION`, 16 MiB per-link fragment cap) are enforced on the paths that can blow them (attach cap, transfer fragment accumulation, delivery-fiber backpressure). — DoS-hardening-backlog: none new.

## One scope note on the KNOWN-DEFERRED item (not a new flag)

The deferred "amqp10 stream-branch `streamPos++` after `a10PeekAt`" follow-up is **broader than the one line suggests**: in `a10StartDelivery`, the entire bloom chunk-scan (`a10PeekAt` per chunk member, `pl5.bloomChunk`/`pl5.bloomPass` writes, `a10BloomHit(..., pl5)`) and the expression-filter block (`a10BuildMessage` + `a10ExprMatch(..., pl5)`) all dereference `pl5` **after** parks and **before** the `ps5r/pl5r` re-validation — the same stale-AA-pointer exposure, several more sites. When that TODO is fixed (re-validate inside the chunk loop, or capture by value), all of these sites must move under the guard. This is the same defect, same branch — reported here only so the fix covers the full extent.

## Summary

```
[none] amqp10.d round 7: no new provable defect; fixes verified (scope note: deferred stream-branch stale-pl5 extends to bloom-scan/expr-filter sites)
```

---

# CTF flags — `amqp.d`

## Round 7 — amqp.d

**no new provable defect; fixes verified.**

What I re-audited and why it holds:

1. **basic.get (round-6 fix verified)** — the popped record is copied to the per-call `recCopy` before the `gAmqpLen(getKey)` hop; `getKey` itself is a stack copy over the `kb2` TLS hazard; all post-hop reads (`recordRedelivered/RoutingKey/Exchange`, the `method()` delegate, `emitContent`) consume `recCopy`, never `pay`. The expired-head drain loop's `deadLetter` yields happen while `pay` is either cleared or about to be refilled — no live alias survives.
2. **finishPublish** — `rec`/`recBusy` reentrancy guard, per-call `sp`/`drp` prop buffers, stack `seenArena` dedup copies, `ccKeys` slice stable sources (`ch.pub.props` written only by this same serve fiber), memo (`tPubMemoQBuf[256]`) length-checked before fill, `gAmqpPushStage` copies into the SPSC ring before returning so the confirm promise is sound.
3. **deadLetter** — `dlrec`/`dlrecBusy` guard, `xbuf/xoth/paug` TLS consumed by `buildRecord` before the `routeTo` yield; pure-automatic cycle drop intact.
4. **Decoder surfaces** — `Rd.*`, `tableWalk`, `appendHeadersExcept`, `mergeXDeath`, `splitRecord` (all four record versions), `propsHeaders/ReplyTo/Expiration`, `amqpTopicMatches` (MAXSEG-capped, linear backtracker): every length/offset is checked before the slice; no `i > t.length` escapes; `isExpired` uses the overflow-safe `pm <= now - ttl` form.
5. **Frame loop** — `fsize + 8 > c.frameMax` with size_t arithmetic (no u32 wrap), frame-end byte checked, per-conn `pendingBytes` cap decremented on both the finish and mid-assembly channel-drop paths.
6. **Auth/authz** — handshake gate, vhost grant, per-op `aclCanAccessKey` on publish/get/consume/bind/unbind/purge/delete/exchange-delete (read vs write polarity correct), CC-key write checks on the default exchange, direct-reply token `drParse` CSPRNG-keyed sig — no wrong-principal check found.
7. **Hop fabric interactions** (shard.d/server.d) — every queue-key TLS buffer that crosses a yielding hop (`purge`, `delete`, `get`, `enforceMaxLen`, TTL sweep, `a10Pop`/`a10PeekAt`) has the stack-copy pattern; `ctlBroadcast` has the `cbBusy` reentrancy guard; `routeTo` has both the `dests` guard and the depth-memo visited set.

Backlog (DoS-hardening, one line, not a flag): channels killed by 404/406/tx-confirm conflicts are removed from `c.chans` before `releaseChannels` runs, leaking that channel's malloc'd `pub.payload`/`pub.props` block until process exit (slow per-channel RAM leak under adversarial churn).

**Ranked summary:** (none) — amqp.d is clean this round; all six prior rounds' fixes verified in place.

---

# CTF flags — `kafka.d`

**kafka.d: no new provable defect; fixes verified.**

Audit summary of what I re-checked this round (all previously fixed or inherently safe):

1. **All TLS statics vs hop-yield clobber** — every site now either copies to stack before the hop (`handleFetch`'s `keyStore`, `handleListOffsets`'s `k3store`, `handleEndTxn`'s `tbuf/gbuf/otb`, `handleDeleteTopics`' `kst/cst`, `handleIncrementalAlterConfigs`' `kstore`, `partBase`'s `bst/pst`), uses fiber-local buffers (`handleMetadataFlex`'s `allBuf`, `handleCreateTopics`' stack arrays), or consumes the TLS buffer synchronously before any yield (`kb`/`blobArena` in `handleProduce` are staged only *after* `topicCompacted`/`partBase`/`pidCheck` — the hops that yield all run pre-staging, exactly as documented).
2. **Size backpatches** — the `handleFetch` records-size and the epilogue frame-size patches both route through `patchI32` with the `o.length >= sizeAt+4` OOM guard; `patchCArrLen` is likewise bounds-checked. The round-6 fix holds.
3. **Wire readers** — `Rd` bounds-checks every read; `decodeV2Batch` validates record bounds against `recEnd`, CRC32C, header sections via `validHeaderSection`, record counts against `KAFKA_MAX_RECORDS`, and all four decompressors are bomb-capped per-batch and per-request (`tKafkaDecompUsed`).
4. **Authz** — every handler stack-captures `tKafkaCtx` before its first hop and authorizes the correct resource/principal; the legacy PLAIN accept-any path correctly pins principal to ANONYMOUS (`principalLen = 0`), so it cannot masquerade as a super-user once ACLs activate.
5. **The `handleFetch` budget clamp** (128 MB per partition while `o` can already hold ~128 MB from earlier partitions, so a single request can transiently reach ~256 MB of reply buffer before the next partition-top ceiling check) is bounded, allocator-guarded, and per-connection — DoS-hardening-backlog territory at most, not a memory-safety flag.

Known-deferred items (DeleteRecords GET..SET window, etc.) were not re-raised.

One-line stdout summary:
```
kafka.d — no new provable defect; fixes verified (round 7 clean)
```
