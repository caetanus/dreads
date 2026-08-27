# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `config.d`

# GLM-CTF-REPORT — pass 9, target: `source/dreads/config.d` (+ the new DREADS_* env layer)

## Flag 1 — `sqs-db` accepts 19–255; any value ≥ NUM_DBS indexes past the per-shard keyspace array

**Where:** `config.d`, `applyDirective` case `"sqs-db"` (the `immutable n = value.to!uint; if (n >= 256) return false; cfg.sqsDb = n;` block). Contrast the sibling cases `"amqp-db"/"mqtt-db"/"kafka-db"` one screen up, which correctly reject `n > 18`.

**Severity:** HIGH (memory-safety, OOB struct index) — but operator/env-reachable only, not client-reachable. Classification: **DoS-ONLY / corruption-at-boot**, not RCE (no client control of the index; the primitive is a fixed misplaced `Keyspace*`, not a steerable write).

**Concrete trace:**

```
# docker-compose / container env
DREADS_SQS_DB=100        # envToDirective -> "sqs-db", applyDirective accepts (100 < 256)
```

Boot path: env → `envToDirective` → `applyDirective("sqs-db", "100", cfg)` → `cfg.sqsDb = 100`.

Later, in `server.d` (both shown extracts, ~line 693):
```d
gSqsExec = (args, reply) { amqpDataExec(args, reply, cast(int) gConfig.sqsDb); };
```
`amqpDataExec` resolves the keyspace via `myKeyspace(db)` (shard.d) = `&gShardKs[tShard * NUM_DBS + db]` with `NUM_DBS == 20`, or `gDbs[db]` unsharded. With `db = 100`:

- sharded: `gShardKs` has length `gShardCount * 20`; index `tShard*20 + 100` reads/writes a `Keyspace` struct **outside the array** for every shard except one — for shard 0 it is `gShardKs[100]`, 80 `Keyspace`s past the end (`(100/20)=5` shards' worth). With `gShardCount=1` it's 80 structs OOB.
- unsharded: `gDbs[100]` against a 20-element array.

Every SQS request then does `ks.lookupTyped(...)` / writes through that pointer — a wild-pointer read/write on foreign heap memory adjacent to the array. It is UB at minimum; depending on layout it can be an OOB *write* (queue creation inserts into the bogus keyspace), so classify as potential heap corruption, DoS in practice, RCE not reachable (value fixed at boot, no info-leak primitive to aim it).

Note the asymmetry proves intent: the three other skin DBs were given the bound `n > 18` (fits 0..19 = NUM_DBS-1); `sqs-db` kept a stale `>= 256` bound — default `sqsDb = 19` shows 19 must stay legal, and nothing above it is.

**Root cause:** copy/paste-bound mismatch — `sqs-db` validated against an old 256-db layout instead of NUM_DBS.

**Fix:** `if (n > 19) return false;` in the `"sqs-db"` case (or a shared `parseSkinDb` used by all four cases with a single `n >= NUM_DBS` rejection). Optionally assert/defensively clamp in `myKeyspace` itself so no future directive path can repeat this.

---

## Minor (correctness, not memory-safety) — uppercase `amqp-db`/`mqtt-db` silently configures `kafka-db`

`applyDirective` computes `lname = name.toLower` for the switch, but inside the shared case the branch is:
```d
if (name == "amqp-db") cfg.amqpDb = n;
else if (name == "mqtt-db") cfg.mqttDb = n;
else cfg.kafkaDb = n;
```
A config file line `AMQP-DB 3` (or any casing variant) matches the case via `lname` but fails both string compares and falls into the `else`, writing `kafkaDb = 3`. Wrong-keyspace routing for the Kafka skin (data goes to db 3), silently. The env path lowercases in `envToDirective`, so it's immune — this is file-config-only. Severity LOW/MED (data-placement corruption, no disclosure since db 3 is still RESP-visible/private per config). **Fix:** branch on `lname`, not `name`.

---

## Suspect / backlog (one-liners, not flags)

- `proto-max-bulk-len` / `client-query-buffer-limit` via `parseMemory` accept 0 and tiny values with no floor — degenerate config, DoS-hardening only.
- `parseMemory` `num.to!ulong * mult` can overflow silently for absurd inputs (e.g. `"18446744073709551615gb"`) — wraps to a small limit; operator-only, cosmetic.
- `envToDirective` allocates per env var scanned (`outp ~= ...`) at boot only — no issue.

## Sweeps of the six skins

Given no source changes since round 8, and with the prior-pass refutations standing (broker-before-hop for handleMetadata; stack-captured hop buffers confirmed again in `amqpPushStage`/`kafkaGroupHopImpl` here — both use stack-local `ByteBuffer hb`, copied by `shardEnqueue` before any `yield`), plus the mqttPub fan-in bounds checks re-verified against `12 + tl + 4 <= p.length` / `po + pl <= p.length` in the server.d extract:

- amqp.d — no new provable defect; fixes verified
- amqp10.d — no new provable defect; fixes verified
- mqtt.d — no new provable defect; fixes verified
- kafka.d — no new provable defect; fixes verified
- kafkagroup.d — no new provable defect; fixes verified
- sqs.d — no new provable defect; fixes verified

## Ranked summary

1. HIGH `config.d applyDirective "sqs-db"` — accepts 19–255, `DREADS_SQS_DB=100` → `myKeyspace(100)`/`gDbs[100]` OOB Keyspace index on every SQS op; operator/env-reachable heap corruption at boot. DoS-only, no RCE path. Fix: bound to `n > 19`.
2. LOW/MED `config.d` skin-db case branches on `name` not `lname` — `AMQP-DB 3` in a config file silently sets `kafkaDb`. Fix: compare `lname`.

---

# CTF flags — `sqs.d`

**no new provable defect; fixes verified.**

Sweep notes for the record (all examined, none flag-grade):

- **jsonStr shared TLS `ub` (256 KiB)** — every caller (`opSendMessage`, `opSendMessageBatch`) copies the returned slice into a per-call `ByteBuffer`/`bodyBuf` **before** any parking `exec()`, and the `SQS_MAX_BODY` check on the raw (escaped) length is a strict upper bound on the decoded size, so `o < ub.length` never truncates a legal body. The refuted-aliasing class does not recur.
- **opReceiveMessage FIFO path** — `snapCopy`, `recs`, `batchGroups`, `qkey/ifkey/grpkey`, `recCopy` are all per-call; slices held across `exec()` parks point into per-call storage. The guarded-site list holds.
- **fifoUnlock / opDeleteMessage(Batch)** — `group2` and `handle` are consumed as exec args (serialized before the park); `ifkey` is per-call. Verified.
- **sqsVisibilitySweep statics** — `rb`, `expRecs`, `expHandles`, `dlall`, `ddall` are function-local statics used only by this single timer fiber; `expHandles[i]` slices into `rb.data` remain valid because the per-message re-pushes write into `val`, never `rb`. Packed `[u32 len][bytes]` decode is bounds-checked (`pi + 4 > packed.length` / `pi + rl > packed.length`).
- **HTTP layer** — `toSize` saturates on overflow, `MAX_BODY = 4 MiB` caps `clen`, `hend + clen` cannot overflow size_t; `bodyStart`, `jsonEachEntry` (trailing-backslash clamp), `findKey` (depth-aware) all bounds-safe.
- **respEachBulk tail `d[i]` after `i++`** — a reply ending in a bare `$` could read one byte past `d.length`; however `d` is an internally-generated RESP reply (ByteBuffer capacity > length, well-formed), not attacker-crafted bytes, so not reachable. Not a flag; a `i < d.length` guard before the `nil` check would be cosmetic hardening only.

Backlog (DoS/availability, one line each):
- FIFO queue with >1024 backlogged messages silently drops records beyond `recs[1024]` in a receive snapshot (messages stuck until queue shrinks).
- Sweep caps (256 expired/due/dedup entries per tick) bound drain rate on deep queues — self-healing over ticks.
- Known-deferred FIFO/dedup TOCTOU remains as documented in the code's TODO.

---

# CTF flags — `kafkagroup.d`

## kafkagroup.d — final sweep result

I re-read the whole file against the charter (decoder bounds, TLS-state aliasing across yields, capacity math, authz/fencing, wire-format truncation). Candidate-by-candidate:

- **KgRd (u8/i32/str16/bytes32)** — every accessor checks `i + n > p.length` on 64-bit `size_t` before slicing; `bytes32`'s u32 count cannot overflow `size_t` addition on the target. No OOB.
- **KGOP_TXN_ADD `char[300] tb`** — the `topic.length > 249` guard precedes the copy; `snprintf(tb.ptr + tl + 1, tb.length - tl - 1, ...)` is in bounds for tl ≤ 249. No overflow.
- **KGOP_SUBSCRIBED meta walk** — every index advance (`i2+2`, `i2+tl`) re-checked against `meta.length` before read; `meta.length < 6` guarded. No OOB.
- **group/member consistency** (`order` vs `members`) — every add/remove path (JOIN-79 registration, fresh JOIN, LEAVE, closeBarrier, evictStale) updates both arrays together; the `id in g.members` derefs in emitJoinOk/DESCRIBE cannot see a null from a desynced order entry.
- **Close/drop-after-closeBarrier guards** in JOIN and JOIN_POLL (the `g.state == ST_EMPTY || (mid in g.members) is null` checks) prevent emitting for a removed member.
- **Caps** — KG_MAX_GROUPS/MEMBERS, 64 protocols, TXN TPS/OFFS/META budgets, 4096/1024 count clamps all negative-safe (`n < 0 ? 0 : n` patterns). Pid minted from CSPRNG with a comment justifying the fallback. Epoch fencing (pid + epoch equality) is evaluated against the correct principal in all four txn ops.
- **Alias-across-yield** — nothing in this file yields; it runs entirely in the owner's drain; all `idup`s happen before any state commit.
- **short epoch wrap** (TXN_INIT after ~32k re-inits) — correctness nit at most, not exploitable (equality check, no stale-epoch resurrect since epoch -1 requires the attacker to also know the pid).

**kafkagroup.d: no new provable defect; fixes verified.**

### Scope note on the DREADS_<NAME> env layer (config.d / app.d)

The tasking asked for a full charter review of the new environment-variable directive layer in `source/dreads/config.d` and `source/app.d`, but **those files were not included in the provided context** — only shard.d and server.d extracts were, and server.d shows just one pre-existing env read (`DREADS_KAFKA_AUTOCREATE`, a boolean getenv/strcmp — benign). I cannot substantiate a trace for code I was not shown, and the charter forbids unsourced claims. I need the actual text of the env-parsing/mapping code (the `DREADS_<NAME>` → directive translation table and any length/port/path parsing it feeds) to audit it. If the mapping only matches fixed directive names and passes values through the same `getenv` string → existing directive parser used for command lines (which prior rounds verified), the layer adds no new parse surface; but that must be confirmed against the source, not assumed.

**Backlog (non-flags, one line each):**
- LEAVE reply may emit fewer per-member error codes than the header count when the request truncates mid-loop (caller-side re-encode should tolerate; worth a defensive recount).
- TXN epoch `short` wraps after ~32k FindCoordinator re-inits on one transactional.id (bounded client-driven churn; no safety impact).
- KG_MAX_GROUPS rejection reuses KG_REBALANCE_IN_PROGRESS (27) where a dedicated "coordinator capacity" code would be more accurate.

---

# CTF flags — `mqtt.d`

# GLM-CTF-REPORT — Round 9, target `source/dreads/mqtt.d` (fresh sweep + no new code in this file)

Everything on the do-not-raise list was skipped. Two items survived scrutiny; only the first meets this round's bar.

## FLAG 1 — Shard-drain fiber parks on a victim's `wlock` during cross-shard takeover → whole-shard cross-shard traffic stalls (remote DoS, sharded mode)

**File:line:** `mqtt.d`, `takeoverLocal()` (the `victim.connected && victim.protoVer == 5 && !victim.offline` branch calling `sendTo(victim, db.data)`), reached from the shard drain via `mqttTakeover` (server.d drain handler, `ShardMsg.mqttConnect`).

**Severity:** HIGH (remote DoS — one shard's SPSC fabric stops draining; DoS-only, no write primitive, not RCE. Exploitation stops at availability: the parked fiber holds no corrupted pointer, only a mutex wait.)

**Attack trace (sharded deployment, ≥2 shards):**

1. Attacker opens MQTT conn **A** on shard 0's port: v5 `CONNECT` with `clientId=X`, keepalive 60, then `SUBSCRIBE "t" qos0`.
2. Attacker opens conn **P** (any shard) and publishes a sustained stream to `t` — enough that A's writer fiber (`mqttWriter`) is inside `sendTo(A, wbox)` blocked in `tcp.write` (A stops reading / advertises a zero TCP window). `sendTo` holds `c.wlock` (`c.wlock.lock(); scope(exit) unlock;` around the write) for the entire stall.
3. Attacker opens conn **B** on **shard 1's** port and sends one `CONNECT` with the same `clientId=X` (any credentials that pass — same ACL user as A).
4. Shard 1's CONNECT path runs `gMqttConnBcast(X, gen)` → `ShardMsg.mqttConnect` hop → shard 0's **drain fiber** runs `mqttTakeover(X, gen)` → `takeoverLocal` → victim A is `connected && protoVer==5 && !offline` → `sendTo(victim, db.data)` → `victim.wlock.lock()`.
5. The lock is held by A's writer, which is parked in a stalled `tcp.write`. A vibe `TaskMutex` **parks the calling fiber** — the calling fiber here is the shard-0 **drain fiber**.
6. Shard 0 stops draining `gInbound[0]`. Every lane into shard 0 fills (cap 16384); every producer (`shardEnqueue` from any fiber routing a shard-0-owned key) spins in its `yield()` retry loop forever. All cross-shard operations touching shard-0-owned keys hang process-wide. The stall persists as long as the attacker keeps A's window closed (TCP write timeout is minutes, and A never errors out).

One slow reader + one 10-byte CONNECT = indefinite, shard-wide, cross-tenant denial of service.

**Root cause:** `takeoverLocal` performs a *socket write* from the drain fiber's context. Every other drain-side path in this skin was deliberately made append-only (`deliverTo` → obox, `mqttFlushDirty` → emit-only, "the drain fiber can never be parked on a subscriber's socket" — that exact invariant is documented above `mqttFlushDirty`). The v5 takeover DISCONNECT send is the one violation of it.

**Suggested fix:** Never `sendTo` from the drain. In `takeoverLocal`, for the v5-reason case, stage the DISCONNECT bytes onto the victim itself (e.g. `victim.takeoverDisc = 0x8E` flag / prepend to `victim.obox` under the already-same-thread obox discipline) and let the victim's **writer fiber** emit it when it next wakes (`victim.closed = true; victim.flushEvt.emit()` already happen — the writer drains obox before exiting). Alternatively `tryLock` and skip the reason send if the writer holds the lock. Also audit `mqttResumeSignal`/future drain-side senders for the same pattern.

**Classification:** CONFIRMED DoS (shard stall). Not RCE: no memory corruption anywhere in the trace.

---

## SUSPECT (not raised as a flag this round — same-client correctness, below the stated bar)

- **Cross-shard `cleanStart=1` does not end a parked session on the owner shard.** `mqtt.d` CONNECT path: `parked is null && c.cleanStart` runs neither `mqttResumeXShard` nor any owner-side discard; the takeover broadcast reaches `takeoverLocal`, which for an *offline* victim sets `closed`/closes the (already dead) socket but **never emits `reconnectEvt`**, so the parked fiber sleeps until session-expiry (possibly `uint.max`). Impact: `gParkedPool` still holds the entry; a *later* `cleanStart=0` reconnect from any shard claims and resumes it via `mqttResumeXShard` — redelivering the pre-clean-start `inflightMsg` queue the client asked to discard ([MQTT-3.1.2-6] violation), plus retained subs/`gMqttSubTotal` pinned until expiry. Same client only — no cross-client disclosure. Fix: in `takeoverLocal`, when the victim is offline, set `victim.discard = true` and emit `reconnectEvt` (ending the parked fiber and its pool entry). Related to, but distinct from, the deferred xshard-adopt TODO/NOTE.

## DoS-hardening backlog

- `takeoverLocal` called synchronously from a same-shard CONNECT also parks the *new* client's serve fiber on the victim's `wlock` (attacker stalls only their own fiber — subsumed by Flag 1's fix).
- `mqttLingerClose` 500 ms per-conn drain window is attacker-extendable per connection (bounded, fiber-local only).

## Files checked, no other findings

- `mqtt.d` decoders (`decodeVarint`, `rdStr`, all `mqttParse*Props`, `stripExpiryProp`, `expiryValueOffsetInPacket`, `mqttDisconnectSEI`): every read bounded by `end`/`p.length` — no OOB.
- SUBSCRIBE static TLS scratch (`granted`/`filters`/`retainOk`) and the `wpBuf`/`fwdProps`/`pb`/`q1`/`filtered` TLS buffers: filled and consumed with no suspension point in between (the one cross-yield candidate, `gMqttFanout` yielding inside `shardEnqueue`, occurs *after* the last read of `fwdProps`) — no aliasing.
- Per-conn `wsOut`/`reqbuf`/`respbuf`, writer swap discipline, `tDirty` emit-only flush, flow-control windows, retained caps, alias byte caps, `$`-topic guards, ACL checks on CONNECT/SUBSCRIBE/PUBLISH/will: verified against the wrong-principal and disclosure classes — no defect found.

**Ranked summary:**

1. HIGH — mqtt.d `takeoverLocal` via `mqttTakeover` (drain): drain fiber parks on victim `wlock` held by a stalled `tcp.write` → shard-0 inbound ring stops draining, all cross-shard ops on that shard hang — remote DoS, one slow reader + one CONNECT. Fix: move the v5 takeover DISCONNECT onto the victim's writer fiber.

---

# CTF flags — `amqp10.d`

**no new provable defect; fixes verified**

Detail of this pass on `source/dreads/amqp10.d`:

- **Decoder bounds re-walked end-to-end.** `A10Dec.u8/be/take` all fail-closed (`ok=false`) before any OOB slice; `a10Performative` rejects on `!d.ok`; list8/list32/array8/array32 all validate `sz >= header` and take exactly `sz - n` — a wire size larger than the frame just fails `take`. Depth budget (32) bounds nested described recursion. No integer-overflow path: all length fields are ≤ u32 and checked against `p.length` before slicing.
- **`a10ReadFrame`** re-checked: `size ∈ [8, 1MiB]`, `doff ≥ 2`, `doff*4 ≤ size` → `skip ≤ rest`, so `frameBuf[skip .. rest]` is always in-bounds; the buffer is per-connection (`c.frameBuf`), the known cross-client class is covered.
- **`a10SendDetachError` snprintf slice** (`eb[0 .. en]`, no `a10ClampN` here): verified the only `entity` arguments (`plk.rkey`, `lk.exchange`, `lk.rkey` from refuse paths) originate from the ≤512-byte `addrBuf`/decoded path, so `en ≤ ~540 < 600` — no OOB read. (Style inconsistency with `a10ClampN` sites, not a flag.)
- **`a10MapMessage` TTL digit write** into `char[24]`: max 20 digits for u64, fits.
- **`a10UriDecode`** `%XX` bounds (`i2 + 2 < src.length`) correct; output bounded by `buf.length`.
- All previously patched sites re-verified present: `mgmtCopy` (transfer mgmt hop), `modAnnCopy` + connection-scoped `dispScratch` (disposition), per-connection `frameBuf`, per-conn link/pending/unsettled caps, post-park link re-validation in the delivery fiber (including inside the bloom chunk scan), the pre-16MiB and 128MiB pending budgets with correct reclaim on all three exit paths.
- Stream offset spec parsing (case 5/6) peeks bounded by queue length — behavior, not memory safety; nothing exploitable.

**On the new env-directive layer (`config.d` / `app.d`)**: those files were **not provided** in this run, so the DREADS_<NAME> mapping cannot be substantively reviewed here. From what is visible in the server.d extract, the one env read shown (`DREADS_KAFKA_AUTOCREATE`) is a pure strcmp→bool with no length/index propagation — safe as written. Recommend the env layer get its own dedicated pass with the actual sources in scope.

No report file written (no flags to rank; writing an empty `GLM-CTF-REPORT.md` would just churn the repo). Backlog (non-flags, one line each):
- DoS-hardening: unbounded per-connection session map growth (channels 0..1023 × sessions keyed by any ushort — bounded by frame count only).
- DoS-hardening: mgmt GET on a huge stream queue does a full linear peek scan per request.

---

# CTF flags — `amqp.d`

## Pass 9 — amqp.d final sweep

**Result: no new provable defect; fixes verified.**

What I checked with fresh eyes, and why each candidate fails to be a flag:

1. **Decoder bounds (Rd, tableWalk, propsHeaders/propsReplyTo/propsExpiration/replaceReplyTo/mergeXDeath/appendHeadersExcept/splitRecord, all record versions v1–v4)** — every length-prefix advance is followed by an explicit `i + n > length` check before any slice; unknown field-table type tags abort rather than guess a size. `splitRecord` bounds-checks every variant before slicing rk/props/body. No reachable OOB.

2. **putShortStr truncation** — clamps `n = min(s.length, 255)` and slices `s[0..n]` consistently; the length byte always matches the emitted bytes. No frame desync.

3. **Stack/TLS buffer discipline around yields** — all the previously-fixed sites are intact and correctly guarded: `finishPublish` recStatic/recBusy + per-call `sp`/`drp`; `deadLetter` dlrecStatic/dlrecBusy; purge/delete/get/maxlen/TTL-sweep queue keys stack-copied across the `gAmqpLen`/`gAmqpPop` hops; `ctlBroadcast` cbBusy; `routeTo` destBusy + visited-memo with the desync-length guard on allocation failure; `a10Publish` recStatic/recBusy; consumer-fiber tag/queue idup'd before the first yield. I traced each for a post-hop READ of a shared static — none found.

4. **Client-controlled lengths into fixed buffers** — `exbuf[256]`/`qbuf[256]` (shortstr max 255, CRLF strip only shrinks), `tagbuf[128]`, `drtb[64]`, `drtok[160]`, `keyStore[268]` (`"amq.q."`+255 = 261 max, and every copy site clamps with `min(len, cap)`). All fits.

5. **Authz** — publish/get/consume/bind/unbind/purge/delete/exchange-delete all gate on `c.aclAuth` with the correct read/write axis; the handshake gate (`cls != 10 && !c.opened`) holds when ACL users exist; CC/BCC keys on the default exchange re-check write ACL; direct-reply tokens verify `drSig` against the CSPRNG-seeded secret. `exclusiveDenied` is evaluated against `c.connId` (the right principal) at every queue op.

6. **Cross-client disclosure** — `basic.get` copies the record out of TLS `pay` into per-call `recCopy` before the `gAmqpLen` park; delivery tags are conn-monotonic and every multiple-settle filters `u.chan == chan`; the direct-reply path validates `(connId, chan, gen, sig)` before touching `gConnsById`.

7. **DoS caps** — pendingBytes 256MB, tx count+bytes, unacked count+bytes, consumers, channels, bindings, exchanges, cancelledTags, TTL sweep bounds, consumer burst 64 — all present and enforced on the wire paths; nothing here rises above the backlog threshold.

### SUSPECT (not a flag)
- `finishPublish` dedup arena (`char[2048]`, 64 entries): a publish fanning out to >64 distinct queue names or >2KB of names silently skips dedup for the overflow — worst case a queue bound via two matching topic patterns receives the message twice. Correctness nit, dedup-loss-only, no memory or disclosure impact. Not worth a flag; note for the backlog at most.

### Backlog (one line each, per rules)
- Dedup arena overflow (above) can double-deliver on pathological binding fan-out.
- 0-9-1 close path keeps processing already-pipelined frames after a connection.close (documented RabbitMQ divergence).

No flag entries to add to GLM-CTF-REPORT.md this round; the file is clean against the charter.

---

# CTF flags — `kafka.d`

## Result

**kafka.d: no new provable defect; fixes verified.**

## Verification notes (what I re-checked this pass)

**Decoder bounds.** `Rd` (i8/i16/i32/i64/str/bytesI32/uvarint/cstr/cbytes/carrlen/skipTaggedFields) bounds-checks every field against `p.length` and clears `ok`; `getUVarint` caps at 10 bytes, `Rd.uvarint` at 5. `decodeV2Batch` validates the 61-byte fixed header, CRC-32C over the covered region, `nrec ≤ KAFKA_MAX_RECORDS`, per-record `rlen` against `recEnd`, and `validHeaderSection` before any verbatim header re-emit. The v1 produce walk rejects truncated tails, wrong magic, compression bits, and CRC mismatches before storing. All count fields route through `safeCount` (`≤ KAFKA_MAX_ARRAY`), responses through `KAFKA_MAX_RESP`, decompression through per-batch + per-request budgets with pre-allocation bomb checks (snappy/zstd content-size, gzip/lz4 incremental). The OOM-guarded `patchI32`/epilogue size backpatch is present and correct.

**Hop/TLS discipline.** Every handler stack-captures `tKafkaCtx` (and `tKafkaClientId`/`tKafkaAdvPort`) before any park. Staging sites I traced — `handleJoinGroup`/`Flex` (fiber-local `req`), `syncLoop`/`joinLoop` (mid copied to `midBuf`, `req` rebuilt after each sleep), `handleEndTxn` (tbuf/gbuf/otb/ometab stack copies, `ogrp ≤ 249` alias guard), `handleDeleteAcls` (delArena copies before HDEL hops), `handleDescribeConfigs`/`handleCreateTopics`/`handleIncrementalAlterConfigs` (stack arrays, key copied to `kstore`), `handleFetch`/`handleListOffsets` (key copied to `keyStore`/`k3store`, handler-local probe budgets) — all consume TLS statics synchronously or copy to stack/heap before the first yield. `handleMetadataFlex` all-topics window is fiber-local `allBuf`. `handleOffsetFetch` fills metadata TLS buffer last, no hop between fill and read. Produce `kb/blobArena/slices` and `DescribeGroups rep` are guarded sites per instructions and re-verified as such.

**Authz.** Every data/group/admin API gates against the stack-captured principal; the all-topics Metadata bootstrap is intentionally ungated (documented); `kafkaPlainCheck` correctly refuses to record a client-claimed principal under the legacy accept-any path; ACL admin (Create/Delete/DescribeAcls) requires cluster ALTER/DESCRIBE so an anonymous client cannot self-grant; explicit-DENY-wins matching order is correct.

**SCRAM.** `nonceBuf[128]` (cnonce ≤96 + 24), `cfbBuf[256]` (bare length checked), `sfBuf[192]` (2+128+3+32+3+10 = 178 max), and the `am[704]` AuthMessage assembly (guarded `ao + cfwp.length > am.length`) are all bounded — no overflow from a hostile token.

**Cross-client.** Reply routing (`ShardMsg.reply` batch walker in server.d) is null-guarded and length-checked; per-connection `outb`/`inb` are fiber-owned; no post-hop read of a shared static found that isn't already in the guarded list.

**Deferred scope note:** the newly merged `DREADS_<NAME>` environment-directive layer lives in `source/dreads/config.d` / `source/app.d`, which were not included in this pass's provided sources — I could not apply the charter to it here and it still needs a dedicated review (specifically: whether any `DREADS_*` value reaches a port/length/count parse without validation).

## DoS-hardening backlog (not flags)

- `joinLoop`/`syncLoop` can each hold a connection fiber up to 70 s per JoinGroup/SyncGroup (spec-legal, but N connections × 70 s of parked fibers is unbounded fiber count).
- `handleAddPartitionsToTxn` stages up to 65536 topic/partition pairs into a TLS `req` before the single coordinator op (bounded by frame size, but multi-MB TLS growth per request).
