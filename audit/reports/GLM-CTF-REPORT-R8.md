# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

**no new provable defect; d479f8b fixes verified**

## Verification notes (what I checked and why it's clean)

**d479f8b items confirmed present and correct in this file:**
- `opReceiveMessage` / `opDeleteMessage` / `opDeleteMessageBatch`: `ifkey` (and `qkey`, `grpkey`) are now per-call `ByteBuffer`s, re-read to build the HDEL/HSET/LREM args after every park — the wrong-queue HDEL is closed.
- `opReceiveMessage`: `snapCopy` / `recs` are per-call; FIFO snapshot slices held across the sismember/lrem/sadd parks point into call-owned memory. `rec` is copied via `recCopy` before the HSET.
- `writeErr`: per-call `body_` (no post-yield static clobber / CL mismatch).
- `jsonStrRaw`/`findKey`: raw slices point into the per-connection request buffer (`buf`/`whole` on the fiber frame), which no sibling fiber touches; `findKey` is depth-aware so a key-looking string inside a value can't spoof a field.
- `toSize` saturates on overflow → `MAX_BODY` 413; `jsonStr` `ub` writes are bounds-guarded by `o < ub.length` and the raw-length > 256 KiB pre-checks in both `opSendMessage` and `opSendMessageBatch`.
- `jsonEachEntry` clamps the trailing slice; `splitRecord`'s `f[1]`/`f[2]` may be null on a malformed record but that only yields empty JSON fields (which cannot occur for broker-written records), no memory unsafety.

**Remaining static TLS buffers audited for the aliasing class** — `rb`, `key`, `rec`, `dval`, `val`, `grpkey`, `namesCopy` (in `sendOne`, `fifoUnlock`, `opReceiveMessage`, `sqsVisibilitySweep`): each is used either (a) fully written and passed as an exec *argument*, which `amqpDataExec` encodes/copies into the hop before its first park (the pattern verified in `kafkaGroupHopImpl` — only the *reply* needs post-park re-copy, and every post-exec read of `rb.data` here is synchronous with no yield between `exec` return and parse), or (b) copied into call-owned memory immediately after the exec returns (`snapCopy.append(snap.data)` runs before any further park). No post-hop read of a shared static remains.

**Visibility sweep**: `expHandles`/`expRecs`/`dueIds`/`dueRecs` statics are packed/unpacked within one non-parking callback pass; the record slices in `expRecs` are length-prefixed copies, and the per-message `exec` args are re-read from `ifkey`/`qkey`/`gk` statics that are rebuilt before each use and copied at exec entry. The FIFO/non-FIFO interleaving there is data-plane only; FIFO TOCTOU remains in the KNOWN-DEFERRED bucket.

**HTTP layer**: single-request-per-connection (`Connection: close`), `hend == 0` → null body → default 400; header scan and `bodyStart` are bounds-safe; no auth surface in this skin beyond the documented SigV4-less design.

sqs.d is clean at this pass.

---

# CTF flags — `kafkagroup.d`

**no new provable defect; d479f8b fixes verified** (as far as this file is concerned — the d479f8b changes were in kafka.d/amqp10.d/sqs.d; kafkagroup.d was re-audited in full).

Basis for the clean call, on the specific classes I hunted:

**Decoder bounds (KgRd).** `u8/i32/str16/bytes32` all bound-check before slicing; on truncation they set `ok=false` and return null/0. Every loop (`np ≤ 64`, `nass ≤ KG_MAX_MEMBERS`, `nm ≤ 512`, `n2 ≤ 1024/4096`, `nt ≤ 4096`) clamps negative/oversized counts to 0, and each iteration re-checks `r.ok` after reads. `bytes32` casts a wire i32 to `uint` then checks `i + n > p.length` — a length ≥ 2³¹ becomes a huge size_t on 64-bit and is rejected, not under-read. `KGOP_SUBSCRIBED`'s hand-rolled consumer-metadata parse re-checks `i2 + 2`/`i2 + tl` against `meta.length` before every slice.

**TXN_ADD stack buffer.** topic clamped to ≤249 before the `char[300] tb` copy; `tb[tl]='\x1f'` at ≤249, `snprintf` gets ≥50 bytes and its int return `pl` is ≥0 for `%d` — `tb[0 .. tl+1+pl]` is in-bounds.

**Ring-slice lifetime.** `req` is a zero-copy slice into the SPSC ring valid only until `kgroupApply` returns; every value that outlives the call (group key, member ids, gii, clientId, ptype, protoNames/protoMetas, assignments in SYNC, txn tps/offs/meta) goes through `.idup` before storage. Transient lookups (`mid in g.members`, `tmid`) never persist a raw slice. `useMid` from the wire is `mid.idup`; the snprintf'd broker id is `buf[...].idup` before any yield-capable path (and the drain never yields anyway).

**Cross-client/authz.** No reply carries data derived from another requester — replies are built only from the group named in the request, which is keyed to the caller's own group. Member/generation fencing (`KGOP_SYNC/HEARTBEAT/COMMIT_CHECK` gen check, `TXN_*` pid+epoch check with CSPRNG-minted pids) is consistently enforced; TXN ops compare pid AND epoch before any state read.

**Null-deref candidates checked.** `KGOP_DESCRIBE`'s unchecked `auto m = id in g.members; wU8(o, m.gii...)` is safe only because `g.order` and `g.members` are updated in lockstep at every removal site (JOIN, JOIN-79, LEAVE, closeBarrier, evictStale) — I traced each; no path desynchronizes them. `closeBarrier`'s `lead.protoType` deref is guarded by the `leaderAlive`/`order[0]` election. `emitJoinOk` callers all guard `useMid/mid in g.members` after `closeBarrier` (the drop case is handled in both JOIN and JOIN_POLL).

**Sweep reentrancy.** `kgroupSweep` mutates members inside `foreach (name, g; tGroups)` but never removes from `tGroups` mid-iteration (drops are collected then applied), and `tTxns` eviction likewise — no AA mutation during iteration.

Remaining rough edges (state-machine races like a JOIN-79 registered member occupying the member cap, or `KG_MAX_GROUPS` exhaustion returning a misleading REBALANCE code) are availability/correctness-of-error-code issues at most, not memory-safety, disclosure, or authz flags — and several overlap the known-deferred resource-limit backlog.

---

# CTF flags — `mqtt.d`

**Target file `source/dreads/mqtt.d` — pass 9 (confirmation over d479f8b):**

**no new provable defect; d479f8b fixes verified.**

Supporting notes (what I checked and why it holds):

1. **TLS static buffers vs. hops** — the remaining TLS statics in this file (`fwdProps` in PT_PUBLISH, `wpBuf` in CONNECT, `kb`/`rb` in the session helpers, `pkt`/`pktV5`/`q1`/`sidBuf` in `mqttDeliverLocal`/`mqttReleaseHeld`) are each either (a) consumed fully synchronously with no suspension point before their data is copied out (idup'd, appended into a per-conn `obox`, or copied into a per-call buffer by `shardMqttFanout`/`amqpDataExec` before the only yield inside `shardEnqueue`'s ring-full retry), or (b) written and read within a single no-yield window (SUBSCRIBE's `granted`/`filters`/`retainOk` scratch). None exhibits a post-hop READ on shared static state.

2. **The WS/TLS static-buffer aliasing class** — `wsOut`, `wsread`, `plain`, the WS handshake `reqbuf`/`respbuf`, and `takeoverLocal`'s `db` are all per-connection or per-call, and no fiber yields while holding a live slice of another connection's frame buffer. `sendTo`'s wlock + per-conn `wsOut` preserve ordering.

3. **Decoders** — every property-block walker (`mqttParseConnectProps`, `mqttParsePubProps`, `mqttParseWillProps`, `mqttParseSubProps`, `mqttDisconnectSEI`, `msgExpiryFromProps`, `expiryValueOffsetInPacket`, `stripExpiryProp`) bounds every read by an `end ≤ p.length` derived from a validated varint + length prefix; the CONNECT keepalive truncation check is present; `decodeVarint` caps at 4 bytes; packet caps (`MQTT_PRECONNECT_MAX`, `MQTT_MAX_PACKET`) are enforced before body slicing. No reachable OOB.

4. **Authz** — CONNECT password check gates `c.aclUser`; PUBLISH topic, SUBSCRIBE filter, and will topic all pass `aclCanAccessChannel` (including the `fireWill` defensive recheck and the drop-but-ack `$`-topic guard). No data-plane path precedes auth.

5. **Cross-client** — takeover is gen-gated (newest wins); `mqttAdoptState`/`mqttMigrateParked`/`mqttResumeXShard` operate only on the frozen/parked session claimed for the same clientId; the ShardPending fabric entries in server.d fill the reply into the pending carried by tag, never a wrong requester. The only lifetime hole (xshard adopt >1s deschedule) is the documented KNOWN-DEFERRED `xshard-adopt-lifetime` TODO, not re-raised.

6. **State-machine edge cases I probed and found safe**: `subInfo`/`filters` index alignment across subscribe-replace, failed append, and UNSUBSCRIBE swap-remove; QoS2 receive-max enforcement and dedup; shared-subscription RR fall-through; retained tombstone seq/TTL caps; `nextDeliveryPid` skipping pids in either handshake map; `mqttExpireOfflineQueue`'s in-place obox rebuild bounded by decodeVarint-validated packet ends.

No flag to report for this file. No report file written (no findings).

---

# CTF flags — `amqp10.d`

## Confirmation pass over d479f8b — target: `source/dreads/amqp10.d`

### Verification of the d479f8b fixes in this file

1. **`A10Conn.frameBuf` (a10ReadFrame)** — verified correct. The buffer is per-connection, `a10ReadExact` parks with `c.frameBuf[0 .. rest]` as the live destination, and no sibling connection can refill it. Bounds are safe: `size` is validated (`8 ≤ size ≤ A10_MAX_FRAME`, `doff*4 ≤ size`), so `skip = doff*4 - 8 ≤ rest` and `c.frameBuf[skip .. rest]` is in range. The `frameBuf.length < rest` grow-only reuse does not shrink, so no stale-slice aliasing.
2. **Stream-branch link re-validation after each `a10PeekAt` park** — verified present and correct in all three park sites inside the delivery fiber (the initial peek, the per-chunk peek inside the bloom scan — re-validated *before* the `!okc` break's downstream uses, which was the sharp edge — and the post-pop fetch revalidation with the requeue-then-stop path for a non-stream popped message).
3. **mgmt copy (`mgmtCopy`) and `modAnnCopy` / `c.dispScratch`** — verified. `a10HandleMgmt` receives a copy of `msg`; `corrRaw`/`to`/`subject`/`bodyMapBytes` are all slices into that connection-local copy or the stack `nb`, safe across the `a10DeclareQueue`/`a10Requeue` hops. The disposition path copies `modAnnBytes` out of the ring frame before the parking settle loop, and `dispScratch` is connection-scoped as documented.

### New-defect sweep

I traced the remaining candidates:

- **`msgOff` with a null `fields.p`** (a `list0` 0x45 performative gives `lst.bytes = null` with `ok == true`): `fields.p.ptr - body_.ptr` with a null ptr yields a huge `size_t`, and the `if (msgOff > body_.length) return;` guard rejects it. Not exploitable.
- **`a10MapMessage` expiration writer**: max 20 digits for a `u64` into `char[24]` — no underflow.
- **`a10UriDecode`** bounds (`i2 + 2 < src.length`, `o2 < buf.length`) — correct.
- **`a10SendDetachError`'s `a10Str(*o, eb[0 .. en])` uses the *unclamped* snprintf return** (every sibling site — `a10HandleMgmt`'s two `eb`/`eb2` and the bindings-location string — uses `a10ClampN`; this one does not). If `entity.length` could exceed ~578, `eb[0 .. en]` would read past the 600-byte stack buffer. However, every reachable `entity` (`lk.rkey`, `plk.rkey`, `lk.exchange`) is bounded to ≤512 by the `addrBuf`/`exBuf`/`rkBuf` `char[512]` staging, so `en ≤ ~534 < 600` on all current paths. **SUSPECT (latent, not currently reachable)** — recommend adding `a10ClampN` for consistency so a future caller with a longer entity name cannot turn it into an OOB read.

**Result: no new provable defect; d479f8b fixes verified.**

No flags to rank; one SUSPECT (unclamped snprintf slice in `a10SendDetachError`, unreachable at ≤512-byte entity names, one-line clamp fix recommended).

---

# CTF flags — `amqp.d`

**amqp.d: no new provable defect; d479f8b fixes verified.**

Notes from the sweep (why the plausible candidates do not rise to flags):

- **Yield-across-TLS-key hazards**: every site that crosses a data-plane hop (queue.purge, queue.delete, basic.get, enforceMaxLen, amqpTtlSweep, a10PeekAt, a10Pop) stack-copies the `amq.q.<name>` key before the yielding call, exactly the d479f8b pattern. The remaining TLS `queueKey` buffers (`kb3/kb5/kb6/kb4`, `rq4/rq6`) are either consumed synchronously by `gAmqpPush/PushFront` (which copies into its own hop buffer before the park — same assumption the fixed sites rely on and that seven prior passes validated) or sit behind busy-guards (`recStatic`, `dlrecStatic`, `cbStatic`, `destStatic`). The `unacked` blob is `idup`'d at delivery/get time, so no post-hop read of a reused buffer.
- **Decoders**: `Rd`, `tableWalk`, `appendHeadersExcept`, `propsHeaders/propsReplyTo/propsExpiration`, `splitRecord`, `xDeathOthers`, `mergeXDeath`, `drParse` all bounds-check every client-controlled length before slicing; malformed input returns null/early, never an OOB.
- **Frame loop**: size checked against negotiated `frameMax` (≥4096) before the `7+fsize+1` slice, size_t arithmetic (no u32 wrap); `pendingBytes`, tx, unacked, channels, consumers all capped.
- **Authz**: finishPublish/CC/BCC write-ACL, per-op queue read/write ACLs, exclusive 405, vhost gate, handshake gate, internal-exchange publish 403 — all check the correct principal (`c.aclAuth`) and the correct target; reply-token forgery is sig-gated by a CSPRNG-seeded secret.
- **Correctness**: ack/nack/reject/recover are channel-scoped with tag-validity 406; mandatory returns confirm; requeue ordering sorted; TTL overflow compared in the non-overflowing direction; topic matcher linear with segment caps.

No RCE primitive, no cross-client disclosure path, no authz bypass provable from this code.

---

# CTF flags — `kafka.d`

## GLM-CTF-REPORT — kafka.d confirmation pass (commit d479f8b)

### Verification of d479f8b fixes in this file

- `tKafkaClientId`: captured to a local in `handleJoinGroup` / `handleJoinGroupFlex` **before** `registerGroupName` parks — verified correct.
- `tMetaAllowAuto` / `metaProbes`: now handler-locals in both `handleMetadata` and `handleMetadataFlex` — verified.
- `hopProbes`: handler-local in `handleFetch` and `handleListOffsets` — verified.
- JoinGroup `req` is fiber-local (`ByteBuffer req;` on the frame), `joinLoop`/`syncLoop` rebuild `req` after each sleep — verified.
- `tKafkaCtx`: captured at the top of every handler that authorizes, including all txn/ACL/admin handlers — verified.
- `tKafkaAdvPort`: captured in `handleMetadataFlex` (`auto advPort = tKafkaAdvPort;` before the hops) — verified there. **But the sibling site in classic `handleMetadata` was missed — see FLAG 1.**

---

### FLAG 1 — MED: classic Metadata v0-v8 reads `tKafkaAdvPort` AFTER cross-shard hops (missed sibling of the d479f8b `tKafkaAdvPort` fix)

**File:** `source/dreads/kafka.d`, `handleMetadata` (classic path, non-flex) — the `putI32(o, tKafkaAdvPort);` in the brokers section, which executes after `registeredTopicPartitions(t)` / `topicPartitionCount(t, metaProbes)` have run for each topic. Those call `gKafkaExec` (`hget` / `llen`), which under sharding hops and **parks** the fiber.

**Trace (requires shards>1 and `--kafka-tls-port` set):**
1. Conn A (plaintext) and conn B (TLS) are accepted by the same shard thread (both listeners live on that thread; fibers interleave).
2. Conn A sends classic Metadata v1 naming a topic whose registry hash / partition keys hash to a foreign shard.
3. `serveKafkaClient` sets `tKafkaAdvPort = gKafkaPort` for A's request, enters `handleMetadata`.
4. The handler writes the brokers entry fields; `registeredTopicPartitions(t)` → `hget` hop → fiber parks. (Even if the hget returns without parking, `topicPartitionCount` → `partLen` cross-shard LLEN hop parks for a non-owned key.)
5. While parked, conn B's frame loop runs `tKafkaAdvPort = gKafkaTlsPort` (TLS conn with `gKafkaTlsPort != 0`).
6. A resumes and emits `putI32(o, tKafkaAdvPort)` → advertises the **TLS** port to a plaintext client.

**Impact:** the plaintext client's next bootstrap/FindCoordinator reconnect goes to the TLS listener with a plaintext handshake → perpetual reconnect loop (client-visible DoS for that client). The mirror case (TLS client briefly advertised the plaintext port) is transient since per-request set/restore is per fiber... actually no: the value is read *post-park*, so whichever sibling set it last wins — both directions possible. No disclosure/corruption; wrong-port advertisement only. Note `handleFindCoordinator` reads `tKafkaAdvPort` with **no** hop before the read, so it is safe — this is specifically the classic-Metadata site.

**Root cause:** the d479f8b capture (`auto advPort = tKafkaAdvPort;`) was applied to `handleMetadataFlex` only; the classic `handleMetadata` performs the identical hops and then reads the TLS static.

**Fix:** at the top of classic `handleMetadata`, add `auto advPort = tKafkaAdvPort;` and emit `putI32(o, advPort);` — one-line mirror of the flex fix.

---

### Everything else

No other new provable defect. Notable re-checks that came up clean:
- `handleEndTxn` stack-copies (`tbuf`/`gbuf`/`otb`) are consumed only after the `kgOp` reply parse; topic truncation at 256 is rejected downstream by `validTopic` (≤249) before key construction.
- `parseGroupOffsets`' TLS `tGo*` arrays are filled and fully consumed by the emitters before any further hop (both emitters perform no `gKafkaExec` between parse and emit).
- `aclLoadAll` slices point into the TLS `rb` but every consumer (`authorize`, Describe/DeleteAcls emit loop, DeleteAcls `delArena` copies) reads them before the next hop; DeleteAcls explicitly copies to a fiber-local arena before the HDEL hops.
- `partField`/`snprintf` stack buffers are length-clamped; `Rd` is bounds-checked throughout; produce v1/v2 decoders (CRC, `KAFKA_MAX_RECORDS`, decompression bombs, per-request decomp budget) hold.

**no new provable defect beyond FLAG 1; d479f8b fixes otherwise verified.**

---

Ranked summary:
1. MED — kafka.d `handleMetadata` (classic): `tKafkaAdvPort` read after cross-shard `hget`/`llen` hops → TLS/plain sibling fiber swaps the advertised port (missed sibling of the d479f8b advPort capture; fix = capture to local like the flex path).
