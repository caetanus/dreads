# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

# GLM-CTF-REPORT.md — `source/dreads/sqs.d`

## FLAG 1 — Uninitialized stack disclosure in `dedupSeen` (garbage `mid` echoed to client)

**File:** `sqs.d`, `dedupSeen()` (~line 300) + `opSendMessage()` (`char[32] mid = void, md5 = void;`)

**Severity:** HIGH (cross-tenant-adjacent info leak of 32 bytes of stack memory per call; needs one write to the internal SQS keyspace via any db-19 client)

**Root cause:** `opSendMessage` declares `char[32] mid = void` (uninitialized). `dedupSeen` returns `true` after parsing the stored dedup value but fills `mid` only `if (fmid.length == 32)`. Any stored `sqs.dd.<name>` field whose value does not decode to a 32-char message id leaves `mid` as raw stack garbage, which is then serialized verbatim into the JSON response: `o.append(mid[])`.

The dedup hash is not opaque to other clients: it is an ordinary keyspace hash in db 19 (`gConfig.sqsDb`), writable by any RESP client (or another skin routed to that DB).

**Attack trace:**
1. RESP client: `SELECT 19` then
   `HSET "sqs.dd.victim.fifo" "poison" "9999999999999\x1fXX\x1fYY"` (fexp = a far-future ms timestamp so `nowMs() > exp` is false; `fmid.length == 2 ≠ 32`).
2. Attacker (or the victim queue user) on the SQS port:
   ```
   POST / HTTP/1.1
   X-Amz-Target: AmazonSQS.SendMessage
   {"QueueUrl":".../victim.fifo","MessageBody":"x","MessageGroupId":"g","MessageDeduplicationId":"poison"}
   ```
3. Internal: `queueExists` → true; `isFifo` → true; `dedupSeen` HGETs the poisoned value, `exp` parses to a huge number (window not passed), returns **true**; `fmid.length != 32` → `mid[0..32]` never written.
4. Response: `{"MessageId":"<32 bytes of uninitialized stack>","MD5OfMessageBody":"..."}` — `md5` is safe (`md5Hex` fallback), `mid` is **not**.

Each response leaks 32 bytes of whatever was last on that fiber's stack (pointers, fragments of other requests' bodies/keys). Repeatable at will, one leak per request. Not a write primitive, so this is **DoS/info-leak grade, not RCE**; but a leaked pointer defeats ASLR for a companion memory-safety bug elsewhere.

**Fix:** initialize `mid = char[32].init` (or `mid[] = '0'`) at declaration in `opSendMessage`, and have `dedupSeen` return `false` on any malformed stored value (`fmid.length != 32 || fmd5.length != 32`) instead of partially succeeding.

---

## FLAG 2 — Unbounded request-body buffering from `Content-Length` (remote memory-exhaustion DoS)

**File:** `sqs.d`, `onConn()` — the `while (whole.length < hend + clen)` loop

**Severity:** HIGH (trivially remote, unbounded per-connection allocation, N connections multiply it)

**Root cause:** `clen = header(req, "content-length").toSize` is fully attacker-controlled and uncapped; the read loop keeps appending to the heap `ByteBuffer whole` until it has `hend + clen` bytes, only giving up after a 10-second idle timeout. AWS caps SQS payloads at 256 KiB; this server has no cap.

**Attack trace:** open N connections, each sending
```
POST / HTTP/1.1\r\nHost:x\r\nContent-Length: 2000000000\r\nX-Amz-Target: AmazonSQS.SendMessage\r\n\r\n{
```
then drip one byte every few seconds (well under the 10 s `waitForData` timeout). Each connection grows `whole` toward 2 GB; ~a dozen such connections OOM the broker while it keeps servicing (or crashes on failed allocation). `toSize`'s `size_t` wrap needs >20 digits, so overflow is not even needed — a plain honest big number suffices.

**Fix:** reject/`413` any `clen > 300_000` (AWS max 256 KiB + slack) before entering the loop.

---

## FLAG 3 — `findKey` matches keys inside string values (field spoofing inside `MessageBody`)

**File:** `sqs.d`, `findKey()` / `jsonInt` / `jsonStrRaw`

**Severity:** MED (protocol-contract confusion; combined with Flag 1's precondition it is another way to steer parsing, and it lets a *body* override request parameters)

**Root cause:** `findKey` scans raw bytes for `"key"` with no string/nesting context — it does not skip the backslash of an escaped `\"` inside a JSON string value.

**Attack trace:** `SendMessage` with body
```json
{"QueueUrl":".../q","MessageBody":"a\"VisibilityTimeout\":\"0\",\"MaxNumberOfMessages\":\"10\" b","DelaySeconds":60}
```
The escaped `\"VisibilityTimeout\"` inside the body matches `findKey` first (it appears earlier than any real top-level field), so `jsonInt(b,"DelaySeconds",0)` — and in `ReceiveMessage`, `VisibilityTimeout`/`MaxNumberOfMessages` — read values the attacker planted in the *body*, not the actual request fields. For a conformant client that sets a real top-level `DelaySeconds: 0` after a body containing `"DelaySeconds":"900"`, the injected value wins (findKey returns the first match). Mostly self-inflicted, but it breaks the contract for any body that legitimately quotes JSON field names, and it feeds Flag 1's parsing path.

**Fix:** make `findKey` skip over string contents (track in-string + backslash state) instead of scanning byte-wise.

---

## FLAG 4 — `\x1f` (SEP) in message body truncates the delivered message

**File:** `sqs.d`, `splitRecord` / `sendOne` record format

**Severity:** LOW (silent data corruption, no memory-safety impact)

**Root cause:** the record uses `\x1f` as a field separator, but the body passes through `jsonStr` unescaping, which happily emits `\u001f` (any `<0x20` handled; and the raw byte survives too via `default: dec = c`). `splitRecord` stops at the first 3 SEPs, so a body containing `\x1f` is truncated at receive; bytes after it are dropped. Worse, in `sqsVisibilitySweep` the re-push path reparses the record — a SEP inside the *body* can shift nothing (first 3 splits consumed) but a SEP in the *group* can't occur either; still, delivery returns a body that differs byte-for-byte from what was sent, violating SQS's exactly-the-body contract, and `MD5OfBody` no longer matches what the client computes → client-side integrity failures.

**Attack trace:** send `MessageBody: "A\u001fSECRET"` → ReceiveMessage returns `"A"` with the original md5.

**Fix:** escape/strip `\x1f` in `sendOne` (or length-prefix the body field).

---

## (a) Review of commit 297afc8's sqs-relevant changes

- **FIFO LRANGE snapshot per-call (`snapCopy`)** — correct and complete: `recs[]` and `batchGroups[]` now slice into the per-call `snapCopy`, and `rec` is re-copied into per-call `recCopy` before the parking `hset`. The `static ByteBuffer snap` is still written first but never held across a park — fine.
- **PurgeQueue clearing `DD/GRP/DL`** — now mirrors `opDeleteQueue`; correct.
- No behavior change for conformant clients observed in these two fixes beyond Flag 2/3/4 above, which predate/postdate the commit scope.

## (b) SUSPECT (unproven from this file alone)

- **`sendOne` / `dedupStore` TLS `rec`/`val`/`key` statics as exec args under ring backpressure:** if `amqpDataExec`'s remote hop can `yield()` inside `shardEnqueue` *before* copying args into the ring, a sibling fiber on the same shard refills `rec` mid-push (the exact class the commit fixed for `snap`). Needs `amqpDataExec`'s copy-before-yield order confirmed; if it copies args before any yield, this is safe.
- **`opCreateQueue` non-idempotent attribute handling / no `queueExists` re-check** — cosmetic.

---

## Ranked summary

1. HIGH — sqs.d `dedupSeen`/`opSendMessage`: uninitialized `char[32] mid` echoed → 32-byte stack disclosure via poisoned `sqs.dd.*` hash (db 19).
2. HIGH — sqs.d `onConn`: uncapped `Content-Length` body buffering → remote unbounded-memory DoS.
3. MED — sqs.d `findKey`: naive key match inside escaped body strings → request-field spoofing.
4. LOW — sqs.d `splitRecord`/`sendOne`: `\x1f` in body truncates delivered messages / breaks MD5 contract.

---

# CTF flags — `kafkagroup.d`

# GLM-CTF-REPORT — `source/dreads/kafkagroup.d`

Verification of the 297afc8 fixes first, then the flags.

## 297afc8 verification (this file)

- **sessMs clamp** (`KG_MAX_SESSION_MS`, sessMs>0?…:45_000): present in both the KIP-394 registration and the full-join path. ✅
- **Per-txn accumulation caps**: `KG_TXN_MAX_TPS` (8192, checked as `t.tps.length + addN >` — no overflow, addN pre-clamped to 1024), `KG_TXN_MAX_OFFS` (16384, same pattern), `KG_TXN_META_BUDGET` (8MB, checked *before* buffering). ✅
- **No TLS-static aliasing in this file**: every string stored into TLS state (`pnames`, `useMid`, `gii`, `grp`, `e.topic`, `e.meta`, `tps`) is `.idup`'d out of the ring slice before the hop returns. ✅
- **`char[300] tb` staging buffer**: topic ≤249 (rejected above 249, so no truncate+collide), `tb[tl]='\x1f'`, snprintf bounded by `tb.length - tl - 1` — max 249+1+11+NUL = 262 ≤ 300. No overflow. ✅
- **KgRd bounds**: all readers check `i + n > p.length`; `bytes32`'s `cast(uint)` of a negative i32 becomes a huge size_t that the length check rejects. No OOB read found in the decoder. ✅
- **Missed sibling of the caps**: none — `KG_MAX_MEMBERS`/`KG_MAX_GROUPS` were already in place.

**New defects found below.**

---

## FLAG 1 — HIGH (DoS): O(N²) dedup scan in KGOP_TXN_ADD stalls the never-yielding shard drain

**File:line:** `kafkagroup.d`, `KGOP_TXN_ADD` case (the `foreach (x; t.tps) if (x == tps)` scan), interacting with the drain contract documented at the top of the file ("the owner's drain executes them back-to-back WITHOUT yielding") and `shardDrainOnce` in shard.d (which calls `fn`/pops with no yield between messages).

**Root cause:** each partition added to an open transaction is deduplicated by a **linear scan of all previously added `topic\x1fpart` strings**, and `kgroupApply` runs in the owner shard's drain, which never yields. Filling a txn to the 8192-partition cap requires ~8192 inserts, each scanning all prior entries: ~33.5M string comparisons of ~260-byte keys ≈ **~9 GB of memcmp executed with zero yields** on the owner shard's core. The cap bounds *memory*, not *work*.

**Attack trace** (one connection, Kafka skin, any client — no auth needed beyond the skin's connect):
1. `InitProducerId(transactional.id="attacker")` → pid P, epoch 0.
2. `AddPartitionsToTxn` request with 1024 unique entries `topic="aatt…"(249 same-length bytes)<different suffix>`, parts 0..1023. Reply 0.
3. Repeat step 2 with fresh suffixes ~7 more times. Each request's last insert scans ~k·1024 existing entries. The 8th request alone does 1024 inserts × up to 8192 existing × ~260-byte compares ≈ 2.2 GB touched — a **sub-second to multi-second full stall of the shard's drain**: every hopped command (all skins on that shard), every AOF flush tick, and every cross-shard reply for that shard freezes behind it. Repeat forever; with 4096 txn ids per shard, a single attacker can hold every shard's drain stalled in rotation.
4. Cheap multiplier: also available via 8192 distinct entries reached through many small requests (work is identical — it is the total N that is quadratic).

Secondary amplifier (same class, cheaper to reach): `closeBarrier`'s protocol matching is members × leader-protocols × member-protocols (512 × 64 × 64 ≈ 2M string compares per barrier close), and `emitJoinOk` iterates all 512 members — also all in the never-yielding drain.

**Severity:** HIGH remote DoS (dreaded "one connection stalls a whole shard core"; other tenants on that shard are starved).

**Fix:** replace the linear `tps` dedup with a hash set (`size_t[string]` or a `HashSet`), or a sorted array + binary insert; bound total work per op (e.g. reject when `t.tps.length > 1024` outright); and/or yield budget in the drain between inbound messages.

---

## FLAG 2 — MED/HIGH (unauthenticated takeover): transactional.id and group.instance.id have no ownership binding

**File:line:** `KGOP_TXN_INIT` (epoch++, clears `tps/offs/offGroup`), `KGOP_TXN_ADD/OFFSETS/END` (only pid+epoch checked), and `KGOP_JOIN`'s static-membership reclaim (`if (m.gii == gii) useMid = id`).

**Root cause:** coordinator state is keyed solely by the client-supplied `transactional.id` / `group.instance.id` string. Any client that can reach the Kafka port can name another producer's transactional id (or a static member's instance id) — nothing in *this file* binds either to an authenticated principal.

**Attack trace (txn fence + offset poisoning):**
1. Victim V uses `transactional.id="orders-v1"` and holds pid P/epoch 0 with an OPEN txn.
2. Attacker sends `InitProducerId(transactional.id="orders-v1")` → same `KgTxn` is found, **`t.epoch++` (now 1), buffered tps/offs wiped**, attacker receives pid P, epoch 1.
3. All of V's subsequent AddPartitions/EndTxn get error 47 (fenced) — permanent DoS of V's producer; V's open txn is silently aborted (buffered offsets discarded).
4. Worse: attacker sends `TxnOffsetCommit(group="orders-group", offsets = attacker-chosen)` then `EndTxn(commit)` — the group's committed offsets are moved to attacker-chosen values, silently skipping/replaying V's consumers (data-integrity corruption of the offset store).

**Attack trace (static member steal):** `JoinGroup(group=G, group.instance.id="<victim's static id>")` → the reclaim loop hands the attacker the victim's member id; the attacker now sits in the group, receives the leader's assignment blob in the next rebalance, and both connections race `lastMs` heartbeats on the same member id (last-writer-wins flapping).

**Severity:** MED if kafka.d enforces an ACL/SASL principal binding on FindCoordinator/txn APIs (not visible here); HIGH if txn ids are reachable pre-auth. In either case the *within-skin* collision (two anonymous clients) is real.

**Fix:** record the authenticated principal (SASL user / TLS CN) with each `KgTxn` and each static `gii` at first INIT/JOIN; on re-INIT with a different principal return `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` / `GROUP_AUTHORIZATION_FAILED`.

---

## FLAG 3 — LOW (correctness): negative partition survives TXN_ADD but is mangled to positive on TXN_END

**File:line:** `KGOP_TXN_ADD` (`immutable part = r.i32();` — signed, no range check) vs `KGOP_TXN_END` (digit-only parse `if (c >= '0' && c <= '9')` drops the `-`).

**Trace:** AddPartitionsToTxn with part = -1 stores `"topic\x1f-1"`; EndTxn replies topic + **part = 1**. The caller then writes commit/abort markers on partition 1 instead of rejecting -1 — control-record corruption of the wrong partition.

**Fix:** reject `part < 0` (and > 2^31-1) in TXN_ADD with error 47/51.

---

## SUSPECT (not proven)

- **`g.protoType = lead.protoType` unguarded** in `closeBarrier` (a few lines below the guarded `if (lead !is null)` block): a null `lead` here would crash the drain. I could not construct an `order`/`members` desync (LEAVE, evictStale, closeBarrier all maintain both consistently), so this is defense-in-depth, not a flag — but add `if (lead !is null)` or an assert; a future desync turns it into a remote crash.
- **KGOP_TXN_INIT ignores `r.ok`** after reading `txnTimeout`: truncated payload silently means timeout=0 (accepted). Harmless today only because the value isn't used beyond the max check — a future use would be a defect. Suggest an `if (!r.ok) { wI16(o,50); return; }`.
- **Unbounded `order.length`-proportional `emitJoinOk` per poll**: every JOIN_POLL of the leader re-serializes ≤512 members × ≤64KB metadata ≈ 32MB into the reply ByteBuffer per poll tick, per member, for the whole rebalance window — a secondary drain-stall/allocator amplifier next to Flag 1. Bounding member metadata (e.g. 4KB/64KB per member) would close it.

---

## Ranked summary

1. HIGH — kafkagroup.d KGOP_TXN_ADD: O(N²) ~9GB memcmp dedup in never-yielding drain = one-connection shard stall (DoS).
2. MED/HIGH — KGOP_TXN_INIT/JOIN: no principal binding on transactional.id / group.instance.id → epoch-fence DoS + committed-offset poisoning + static-member theft.
3. LOW — KGOP_TXN_ADD/END: negative partition round-trips as its absolute value (wrong-partition markers).
4. SUSPECT — unguarded `lead.protoType` in closeBarrier; TXN_INIT ignores truncation; unbounded leader re-serialization per poll.

---

# CTF flags — `mqtt.d`

# GLM-CTF-REPORT — mqtt.d pass (on top of 297afc8)

## FLAG 1 — Static-TLS WebSocket write scratch `wb` in `sendTo` aliases across concurrent socket writes → cross-client byte corruption / disclosure

**File:** `source/dreads/mqtt.d`, `sendTo()` (the `if (c.wsCodec !is null)` branch, `static ByteBuffer wb; // TLS scratch`)

**Severity:** HIGH (cross-client data disclosure + protocol desync; DoS-grade at minimum)

**Primitive:** two fibers on the same shard thread both copy their outbound frames into ONE thread-local static buffer, then *yield* inside `c.tcp.write(wb.data)` while vibe keeps issuing partial writes from that same slice. No memory-safety violation (slices stay in bounds) — the corruption is *content*: fiber B's `wb.clear(); wb.append(...)` rewrites the bytes fiber A's in-flight write is still draining from.

**Attack trace (provable):**
1. Two MQTT-over-WebSocket clients A and B are accepted by the same shard thread (SO_REUSEPORT makes this the normal case; an attacker just opens both conns until they land on one thread — with 1 shard it's guaranteed).
2. A subscribes to `slow/feed` and stops reading its socket. Someone publishes a large payload to `slow/feed` (or A simply sets a v5 `maximum-packet-size` near the frame size so the write fragments). A's **writer fiber** enters `sendTo` → `wb.clear()` → `wsEncodeBinary(wb, A-frame)` → `legSend`/`c.tcp.write(wb.data)` — the kernel socket buffer for A is full, the write parks the fiber **holding a live slice of `wb`**.
3. B's writer fiber (or B's PONG flush, or B's CONNACK) runs `sendTo` on the same thread: `wb.clear()` destroys A's pending frame and `wsEncodeBinary(wb, B-frame)` installs B's bytes — including B's *subscribed payload content* — at the same address.
4. A's write resumes; vibe continues writing from the slice: **the tail of A's socket stream is B's frame bytes.** A receives a copy of a message delivered to B (cross-client disclosure), and both A's and B's WS/MQTT framing desyncs (A sees a truncated frame + B's frame → parser garbage → spurious `Packet Too Large` closes / crashy clients).

Note the sibling: `takeoverLocal()` uses `static ByteBuffer db` and passes `db.data` to `sendTo`; on a non-WS victim `sendTo` calls `c.tcp.write(bytes)` directly on that slice — the same aliasing across two concurrent takeovers on one thread. Same fix site class.

**Root cause:** "TLS scratch, consumed synchronously by sendTo" comment is false — `sendTo` *blocks on the socket*, i.e. yields, so the scratch is not consumed synchronously. This is exactly the static-TLS-aliasing class the SQS delete path already shipped once.

**Fix:** make the WS encode scratch per-`MqttConn` (e.g. `c.wsOut` released by the writer/teardown), or copy into `wbox` before writing. Same for `takeoverLocal`'s `db` (stack/per-conn buffer).

**Classification:** DoS + disclosure only; no OOB write, no pointer corruption → not RCE.

---

## FLAG 2 — Cross-shard session adopt races the owner's 1s redirect window → use-after-free on `parked.obox` / state torn mid-read

**File:** `mqttResumeXShard()` + `mqttAdoptState()` vs `mqttParkOrEnd()` freeze branch (redirect window `rdl = now + 1000.msecs`)

**Severity:** MED-HIGH (SUSPECT-race; UAF on a malloc'd ByteBuffer, thread-crossing)

**Trace:** reconnecting shard claims `parked` from `gParkedPool`, sends `mqttResume`, spin-waits up to **500 ms** for `frozen`, then `mqttAdoptState(parked, newc)` reads `parked.filters / obox / heldQ / inflightMsg` — all memory owned by the **other shard's thread**. The owner, after setting `frozen`, holds `redirect` for only **1000 ms**, then returns from `mqttParkOrEnd` → `mqttTeardown` runs on the owner thread: `trieUnsubscribe`, `c.filters = null`, and critically **`c.obox.release()`** (frees the malloc-plane buffer) — with *zero synchronization* against the adopter. If the reconnecting shard's fiber is descheduled >1 s between observing `frozen` and finishing the adopt (scheduler pressure, a slow `gMqttExec`-style hop on the same fiber is not in the adopt path, but GC pause / oversubscribed cores suffice), it reads a **freed obox** (`parked.obox.append`/`.data` in `mqttAdoptState`) and copies freed bytes into the new session — UAF read, garbage disclosure, or a crash in `release`'d-memory read. The handshake is time-based, not ownership-based: the comment "~1s covers the adopt" is an assumption, not a guarantee.

**Fix:** the owner must not tear down until the adopter *acknowledges* (e.g. owner stays parked on `reconnectEvt` until an explicit `mqttResumeDone` hop arrives, with the 1s window only as a leak-bailout that still can't free `obox` — hand obox ownership to the adopter and `release` nothing on the owner side).

**Classification:** PLAUSIBLE-RCE-adjacent UAF (freed malloc buffer read/written cross-thread) but read-only copy — honest label: UAF read, disclosure/crash; a groomable write is not apparent. SUSPECT until the >1s deschedule is demonstrated.

---

## FLAG 3 — Failed cross-shard freeze leaves a double session: CONNACK session-present=1 while the old shard's parked session stays subscribed

**File:** `mqttResumeXShard` (the `if (!ok) return false;`) + CONNECT flow (`sessPresent = mqttSessionExists(...)`)

**Severity:** MED (correctness / duplicate delivery; mild cross-tenant weirdness)

**Trace:** client id `X` parks persistently on shard 1 (in `gParkedPool`, in shard 1's trie, `mqtt.sess.X` in the keyspace). Reconnect lands on shard 0, `clean_start=0`. `gMqttResume(1, "X")` is sent but shard 1's drain doesn't freeze within 500 ms (ring backpressure — `shardEnqueue` yields under a full lane, which an attacker can *cause* by flooding shard 1's inbound ring with cross-shard traffic). `mqttResumeXShard` returns false → **fresh session**, and — crucially — the pool entry was already removed and shard 1's parked session was never told anything. `mqttSessionExists` reads the still-live keyspace record → **CONNACK session-present=1**. Now the *old* parked session is still in shard 1's trie with `offline=true` (its obox keeps queueing) *and* the new session is live on shard 0: every publish to the old filters is delivered twice (once live, once queued into a session nobody will ever resume), and at old-session expiry a stale will may fire.

**Fix:** on freeze failure, re-`parkedPoolPut(parked)` or send a `discard` hop so the owner ends the parked session; don't report session-present from the keyspace record while a possibly-live parked session exists.

---

## FLAG 4 — Unbounded TLS associative-array growth keyed by attacker-controlled `$share` group names (`groupIdx`, `gShareRR`)

**File:** `mqttDeliverLocal` — `static size_t[][string] groupIdx; // TLS, reused` and `private size_t[string] gShareRR;`

**Severity:** MED (remote memory-exhaustion DoS, unauthenticated pre-ACL-escape)

**Trace:** v5 CONNECT (no username → `default` ACL user or legacy-allow), then pipelined SUBSCRIBE packets each carrying 64 filters of the form `$share/<random-32-char>/a`. Each *distinct* group string inserts a permanent key into `groupIdx` (via `require`) and `gShareRR` (`gShareRR[g] = rr+1`). The per-message reset only does `lst.length = 0` — **keys and bucket arrays are never freed**. With MQTT_MAX_PACKET = 16 MB and ~32-byte group names, one connection inserts ≈ 500k keys per 16 MB sent; a handful of reconnects pins hundreds of MB of TLS AA per shard thread, outside every per-conn budget (the conn can then DISCONNECT — the keys persist for thread life). MQTT_MAX_SUBS caps entries per conn, not distinct group names.

**Fix:** bound `groupIdx`/`gShareRR` by key count (drop/overwrite past N, like the retained caps), or clear both per publish when empty.

---

## Correctness confirmation of the 297afc8 items touching this file

- **Will ACL at CONNECT**: present (drop-but-no-fire) and re-checked defensively in `fireWill` — correct; the v5 empty-clientId assigned-id path stores no will-ACL bypass.
- **`sessionId`/keepalive truncated-CONNECT OOB**: the `i + 2 > p.length` guard before reading keepalive is present; all property parsers (`mqttParseConnectProps/PubProps/WillProps/SubProps`, `mqttDisconnectSEI`, `stripExpiryProp`, `msgExpiryFromProps`) bound every read by `end` — I found no remaining unchecked skip.
- No behavior change for conformant clients introduced by those guards.

## SUSPECT (not flags)

- `mqttPublishSys` statics (`nb`, `ub2`) — single timer fiber, synchronous; safe.
- SUBSCRIBE's `static granted[64]/filters[64]/retainOk[64]` — genuinely no yield inside the window; safe as written, but fragile (any future hop added there becomes Flag-1-class).
- `mqttLingerClose` 500 ms read-drain per protocol-error close — bounded slowloris-ish (one fiber, 512B scratch); acceptable.

## Ranked summary

1. HIGH — mqtt.d `sendTo` WS branch: static TLS `wb` rewritten while another fiber's `tcp.write(wb.data)` is parked → cross-client frame/payload disclosure + desync (DoS/disclosure, not RCE).
2. MED-HIGH — `mqttResumeXShard`/`mqttAdoptState` vs owner teardown: time-based (1 s) handshake, no ownership transfer → UAF read of `parked.obox` under scheduler delay (SUSPECT race, path shown).
3. MED — failed cross-shard freeze leaves double-subscribed stale session + wrong session-present=1.
4. MED — `$share` group names grow TLS AAs `groupIdx`/`gShareRR` without bound → unauthenticated per-thread memory DoS.

---

# CTF flags — `amqp10.d`

# CTF findings — `source/dreads/amqp10.d`

Scope note: the decoder core (`A10Dec.u8/be/take`) is sound — every client length is bounds-checked against `p.length` on 64-bit with no overflow path. The flags below are lifetime/aliasing and resource-exhaustion bugs, not raw OOB.

---

## FLAG 1 — Unbounded `unsettled` map: remote memory-exhaustion DoS (HIGH)

**Where:** `A10Session.unsettled` (struct decl), written in `a10StartDelivery`'s fiber:
```d
ps5.unsettled[did] = A10Out(pl5.rkey, (cast(const(ubyte)[]) pay.data).idup, h5, pl5.stream);
```
and the credit source, `PERF_FLOW` handling: `plf.outCredit = credit;` where `credit = cast(uint) cr.u` (a client u64 truncated to uint — up to 4 Gi-credit).

**Root cause:** commit 297afc8 added caps for the *ingress* fragment path (`A10_MAX_PENDING_BYTES_PER_CONN`, per-link 16 MiB) but the *egress* unsettled-delivery map has **no cap at all**. An entry (with an `idup`'d copy of the full message blob) is created per delivery and is removed **only** on a client disposition (`a10HandleDisposition`), session end, or teardown.

**Attack trace (any conformant AMQP 1.0 client):**
1. CONNECT/open/begin; `attach` a receiver link to a queue that has (or will have) a large backlog — e.g. a second connection pumping 1 MiB messages.
2. Send `flow {handle, link-credit: 0xFFFFFFFF}` — `outCredit = 4294967295`.
3. **Read the TCP socket continuously** (so `a10Send` never blocks and the delivery fiber never stalls), but **never send a disposition**.

The delivery fiber pops → `unsettled[did] = blob.idup` → sends → loops. Every delivered megabyte is retained forever in `c.sessions[ch].unsettled`. With a co-operating publisher the attacker grows broker memory without bound until the OOM killer / GC death. Classification: **DoS-ONLY** (no write primitive; the retained data is heap-managed).

**Fix:** cap unsettled count/bytes per link (mirror `A10_MAX_PENDING_BYTES_PER_CONN`); stop decrementing credit / pause the delivery fiber when the cap is hit; also reject `credit` values that overflow `uint` rather than truncating.

---

## FLAG 2 — TLS-static buffers & ring-slice used across a parking hop: cross-client corruption of requeued message headers (HIGH, sharded builds)

**Where:** `a10HandleDisposition`, the `0x27` (modified) branch:
```d
static ByteBuffer annTbl; // TLS: consumed synchronously   <-- comment is wrong
...
a10RequeueAnn(po.queue, po.blob, cast(const(ubyte)[]) annTbl.data, !modUndeliverable);
```
`static ByteBuffer dcTbl; // TLS` same branch; and `modAnnBytes` / `modAnnCount` are slices **into the frame body**, i.e. into `a10ReadFrame`'s `static ubyte[] buf` (TLS, shared by every connection on the shard thread).

**Root cause:** the disposition fix moved the *id snapshot* to `c.dispScratch` precisely because `a10Requeue/a10RequeueAnn/a10Reject` take the data-plane hop and **park**. But the annotation *payload* still lives in thread-shared statics (`annTbl`, `dcTbl`) or in the shared read scratch (`modAnnBytes → static buf`). While fiber A is parked in `a10RequeueAnn`, connection B on the same thread can (a) run its own disposition (`annTbl.clear()` + rewrite) or (b) read a frame (`buf` rewritten) — and A resumes requeueing with B's bytes.

**Attack trace:**
- Conn A (shard thread T): deliver a message, then send `disposition {role:receiver, state: modified{delivery-failed:true, message-annotations:{k:"v"}}}` on a **cross-shard-owned queue** (forces the hop → park).
- Conn B on thread T, during A's park: send a transfer whose body contains attacker-chosen bytes (rewrites `buf`), and/or a modified-disposition with different annotations (rewrites `annTbl`).
- A resumes: `a10RequeueAnn(queue, blob, annTbl.data …)` — the requeued/redelivered message now carries **B's frame bytes spliced into its 0-9-1 headers table**, visible to whichever consumer later receives it. That is cross-tenant data injection into another client's message (B's annotation content is itself attacker data, so the direct disclosure is limited to B-controlled bytes — but for two *victim* connections A and C racing, C's private frame content ends up inside A's redelivered message headers: genuine cross-client disclosure).

The same class sits in `a10HandleAttach`'s stream timestamp-offset scan (`static ByteBuffer sb;` refilled by `a10PeekAt` → `gAmqpPeekAt` → `amqpDataExec` hop) and `a10HandleMgmt`'s `static ByteBuffer bArgs` passed to `a10Bind` (fan-out can yield under ring backpressure). `server.d`'s own `kafkaGroupHopImpl` comment ("`reply` may be a caller-shared TLS static that another fiber wrote to while we were parked") documents that this aliasing is real on this codebase's hop fabric.

**Severity note:** with the GC, the old `buf` array stays alive (the slice is scanned), so this is *content* corruption / cross-client data mixing, not UAF — **DoS/correctness + limited disclosure, not RCE**. Still: a slice into one TLS buffer aliasing two live values — the exact class that shipped the SQS bug.

**Fix:** make `annTbl`/`dcTbl`/`sb`/`bArgs` per-connection (or stack `ByteBuffer` locals like the rest of the file's encode paths), and `modAnnBytes = ….idup` before the park-inducing loop.

---

## SUSPECT (unproven from this file alone)

- **`/management` node has no per-user ACL gate.** `a10HandleAttach` marks `lk.isMgmt` for address `/management` with no ACL check, and `a10HandleMgmt` exposes DELETE queue/exchange, purge, bind/unbind. If the recent per-operation AMQP ACL covered only the data plane (per the commit description), a SASL-authenticated low-privilege user can delete another tenant's topology. Needs `dreads.acl`/amqp.d to confirm whether `a10DeleteQueue`/`a10Bind` are ACL-checked internally.
- **`gAmqpPeekAt`/`gAmqpPop` reply aliasing (server.d TLS `rbk`/`rb2`)**: two concurrent receivers on the same shard thread peeking cross-shard queues can each receive the *other's* record after the hop park — full cross-client message disclosure. The aliasing primitive is provable from `server.d` (shared `static ByteBuffer rbk` + park inside `amqpDataExec`); confirming the wrong-record handoff needs `amqpDataExec`'s body, which wasn't provided. Same fix family as Flag 2.
- **Truncated-value dangling key in `a10MapMessage` SEC_APP_PROPERTIES**: if the *value* read fails (`!md.ok`) after the key was appended, `hdrTbl` keeps a key with no value → malformed 0-9-1 table handed to `a10Publish`/consumers (misframed header table downstream). Low severity; fix by checking `md.ok` before appending the key.

---

## Ranked summary

1. HIGH — amqp10.d `a10StartDelivery`/`PERF_FLOW`: unbounded `unsettled` map per receiver link (credit=0xFFFFFFFF + read-and-never-disposition) → remote memory-exhaustion DoS. DoS-only.
2. HIGH — amqp10.d `a10HandleDisposition` (annTbl/dcTbl/modAnnBytes), `a10HandleAttach` (sb), `a10HandleMgmt` (bArgs): TLS statics / frame-scratch slices live across parking data-plane hops, aliased across same-thread connections → cross-client header/annotation corruption & mixing (sharded builds).
3. SUSPECT — `/management` topology ops (delete/purge/bind) not per-user ACL-gated; needs acl cross-check.
4. SUSPECT — server.d `gAmqpPeekAt`/`gAmqpPop` shared TLS reply buffers across parked fibers → possible cross-client message disclosure; needs amqpDataExec body.
5. LOW — `a10MapMessage` app-props: dangling table key on truncated value → malformed downstream header table.

---

# CTF flags — `amqp.d`

# GLM-CTF-REPORT — `source/dreads/amqp.d` (AMQP 0-9-1 skin)

Scope note: I read the full target file plus the shard/server routing extracts. Flags below are substantiated from the code shown; unproven leads go to SUSPECT. No memory-corruption primitive (controlled write / OOB index) was found in this file's decoders — every length-prefixed reader (`Rd`, `tableWalk`, `propsHeaders`, `propsReplyTo`, `propsExpiration`, `splitRecord`, `mergeXDeath`, `appendHeadersExcept`, `xDeathOthers`, `buildXDeathEntry`) bounds-checks every client-controlled length before slicing. The flags are an ACL bypass, a CPU-exhaustion DoS, and two lower-severity items.

---

## FLAG 1 — HIGH: ACL write-bypass via `CC`/`BCC` headers on the default exchange

**Where:** `finishPublish` — ACL gate (~"Per-operation ACL" block, `target = ch.pub.exchange.length ? ch.pub.exchange : ch.pub.rkey`) vs. the CC/BCC collection block and `routeTo(ch.pub.exchange, ch.pub.rkey, hdrs, pushSink, ccKeys[0 .. nCc])`.

**Severity:** HIGH (auth/ACL bypass → cross-tenant queue write).

**Root cause:** The per-operation publish ACL authorizes exactly ONE target: the exchange name, or — for the default exchange (`""`) — the routing key. But the sender-selected-routing feature harvests up to 32 extra routing keys from the `CC`/`BCC` message headers and passes them as `altKeys` to `routeTo`. In `routeTo`'s default-exchange branch, **every `altKey` is sunk as a raw queue name with no ACL check**:

```d
if (ex.length == 0) {
    sink(rkey);
    foreach (ak; altKeys)
        sink(ak);   // <- no aclCanAccessKey(au, ak, ...)
```

and `pushSink` only checks queue *existence*, never authorization.

**Attack trace (exact frames):**
1. Operator configures ACL users (`aclUserCount() > 1`). Attacker `u_low` is granted write access to exactly one queue, `allowed`, and nothing else.
2. `u_low` connects, authenticates (start-ok PLAIN), opens a channel, declares queue `allowed` (declare is not ACL-gated per-key beyond the exchange/rkey publish gate; or `allowed` pre-exists).
3. `u_low` sends `basic.publish(exchange="", routing_key="allowed", mandatory=0)`, then a content header whose basic-properties headers table contains `CC` = field-array of one longstr `"secret"`, then the body frame.
4. `finishPublish`: ACL check runs on `target = "allowed"` → **passes**. CC parsing (`ty != 'A'` check passes; elements are `'S'`) collects `ccKeys[0] = "secret"`, `nCc = 1`. BCC is accepted and stripped the same way.
5. `routeTo("", "allowed", …, pushSink, ["secret"])` → `sink("allowed")` then `sink("secret")` → `gAmqpPushStage("amq.q.secret", record)` — the message is **written into the victim tenant's queue `secret`**, which `u_low` has no grant on. A victim consumer on `secret` receives the attacker's payload; combined with any read grant asymmetry this is cross-tenant injection.

Same-frame shape for a 0-9-1 client (pika-style): publish frame `<60 40><00 00><07 'allowed'><00>`; header frame with property-flags 0x2000 and a headers table `{"CC": array ["secret"]}`.

Note the CC/BCC keys also ride into the **direct default-exchange path only**; for named exchanges the altKeys are matched against that exchange's bindings, which the exchange-level ACL already gates — so the default-exchange case is the actual hole.

**Fix:** in `finishPublish`, when `ch.pub.exchange.length == 0` and an ACL is configured, run `aclCanAccessKey(au, ak, false, true)` for every `ccKeys[i]` (drop or 403 on first denial) before calling `routeTo`. Cheap: ≤32 checks once per publish.

---

## FLAG 2 — HIGH: exponential-backtracking CPU burn in `amqpTopicMatches` → single-frame shard freeze

**Where:** `amqpTopicMatches` (mid-`#` backtracking loop) reached from `routeTo`'s `ExType.topic` branch (twice per binding: once for `rkey`, once per `altKey`), and from the alternate-exchange cascade.

**Severity:** HIGH (remote DoS; yield-free, monopolizes the shard thread — every AMQP/SQS/Kafka/MQTT/RESP client pinned to that shard stalls).

**Root cause:** The 128-word guard bounds the KEY's word count, but not the search space. A mid-pattern `#` tries **every** remaining suffix position, each recursing into `rest` which may itself contain `#`:

```d
size_t k2 = ki;
for (;;) {
    if (amqpTopicMatches(rest, key[k2 .. $])) return true;
    while (k2 < key.length && key[k2] != '.') k2++;
    ...
    k2++;
}
```

With `p` `#`-groups and `w` key words, the work is the number of compositions of `w` into `p` parts ≈ C(w, p) — exponential. There is no memoization, no length/feasibility pruning (e.g. "rest needs ≥ N literal words, key has M — prune"), and no yield: one call can burn effectively unbounded CPU inside a single `handleFrame`, and the serve loop cannot even answer a heartbeat.

**Attack trace:**
1. Connect, declare topic exchange `e` (`exchange.declare(type="topic")`).
2. `queue.bind(queue="q", exchange="e", routing_key = "#.x.#.x.#.x. … .x")` — pattern shortstr ≤ 255 bytes ⇒ ~63 `"#.x."` groups.
3. `basic.publish(exchange="e", routing_key = "b.b.b. … .b")` — routing key shortstr ≤ 255 bytes ⇒ 128 words (passes the `words > 128` guard: 255 bytes = 127 dots + 1 = 128 words).
4. `routeTo` → `amqpTopicMatches(pattern, key)`: each of the 63 `#`s tries up to ~128 split points, each recursing — the match can never succeed (no `x` in the key), so the full C(128, 63) ≈ 10³⁷ search is explored. The shard thread is wedged in one frame parse; the process stays up but that core serves nothing. Repeat per shard (bind the same pattern, publish per shard via key-hash steering) to freeze the whole broker.

**Fix (pick one or combine):**
- Prune by literal-word count: precompute per pattern the minimum number of non-`#`, non-`*`-consuming words needed and bail when the key has fewer remaining words; and cap total `#`-expansion steps with a per-call step budget (e.g. 10 000) returning false when exceeded.
- Memoize (pattern-suffix, key-suffix) fails in a small TLS scratch table.
- Reject binding patterns with more than, say, 8 `#` segments at `queue.bind` time (406), and same for routing keys (real topic keys have a handful of words; the 128-word key cap alone provably does not bound the product).

---

## FLAG 3 — MED: outbound content-header frame can exceed the peer's negotiated frame-max

**Where:** `emitContent` — the content-header frame is emitted in one piece (`frameStart … putU64(body) … o.append(props)`), with only the **body** chunked to `frameMax - 8`. Frame-max is clamped only ≥ 4096 (`tune-ok` handler: `if (c.frameMax < 4096) c.frameMax = 4096`).

**Root cause / trace:** A consumer tunes `frame-max = 4096` (`connection.tune-ok`). A publisher on another connection publishes a message whose basic-properties block (headers table with CC, x-death chains, etc.) is up to ~128 KB (allowed: the publisher's own frame-max is 131072). The message is delivered to the consumer; `emitContent(o, chan, pay.data, c.frameMax /* 4096 */)` emits a **header frame of ~128 KB**, far above the consumer's negotiated 4096. A spec-strict client (the java client is) treats an over-size frame as a fatal framing error and drops the connection — a third party can remotely disconnect any consumer of a shared queue by publishing large-header messages. Not memory-unsafe for us; a contract/robustness flag.

**Fix:** cap the stored props at publish time to `frameMax - 24` per connection, or split is not possible for headers per spec — better: reject/truncate at ingest when `props.length > AMQP_FRAME_MAX - 32` (406), since a conformant 0-9-1 peer can never have produced a larger property block anyway.

---

## FLAG 4 — MED/SUSPECT: direct-reply token secret is guessable (`gDrSecret`)

**Where:** serve-loop init `if (gDrSecret == 0) gDrSecret = monoMs() * 0x9E37… + cast(ulong) cast(void*) c;` and `drSig`/`drParse`.

`monoMs()` at first-connect is wall-clock ms — an attacker who knows roughly when the broker booted has maybe ~2^25–2^35 candidate secrets; the pointer term adds some ASLR entropy but heap bases are often predictable. Offline brute force of ONE observed token (`amq.rabbitmq.reply-to.<cid>-<chan>-<gen>-<sig>` — the attacker sees their own token by publishing with `reply-to=amq.rabbitmq.reply-to` against their own reply consumer) yields the secret, after which the attacker can forge tokens for **any connection id / channel** and have `finishPublish`'s `drDirect` path inject a message straight into another client's live direct-reply consumer (`sendTo(tc, …)` with attacker-chosen body). That is cross-client data injection. Also a benign init race (two shards writing concurrently). **Not confirmed exploitable** without measuring the actual entropy of `monoMs ^ ptr` on the target build — hence SUSPECT — but the fix is trivial and should be done regardless: seed from `secureRandom`/`getrandom(8)` once at boot under a CAS.

---

## SUSPECT (unconfirmed, needs one fact from `amqpDataExec`)

**TLS queue-key/record buffers held across a yielding `gAmqpPush`/`gAmqpPushFront` hop** — `settleNegative` (`kb4`, `rq4`), `requeueAllUnacked` (`kb6`, `rq6`), `commitTx` txSink (`kbT`), `deadLetter` sink (`kb5`), `a10Requeue` (`kbr`, `rqr`), `a10RequeueAnn` (`kra`, `rqa`). This file's own comments establish the class ("the sink's cross-shard RPUSH YIELDS", and the purge/delete/get/enforceMaxLen/TTL-sweep sites were all converted to stack copies), and `server.d`'s `kafkaGroupHopImpl` proves `amqpDataExec` can park while a caller's TLS static is live. If `amqpDataExec` copies its `args` into the hop payload **before** its first yield (including the full-ring retry inside `shardEnqueue`), these sites are safe; if any path reads `args` after a park (or the ring-full retry re-reads them), two interleaved fibers on one shard push a record into the **wrong queue's list** (cross-tenant corruption). To confirm: check whether `amqpDataExec`'s encode step is yield-free up to the `shardEnqueue` copy. If in doubt, apply the same `char[8+256+4]` stack-copy pattern used by the already-fixed siblings — it is mechanical and cheap.

Also noted, non-flags: `deadLetter`'s sink lacks the `queueExists` gate the live publish path has (ghost `amq.q.*` lists creatable via DLX to undeclared names — cosmetic/robustness only); `queue.purge`/`basic.get` with an empty queue name don't apply the spec's "current queue" default that `queue.bind` does (interop nit); `amqpRegEnsure`'s loser spin hangs forever if the winner's `new Mutex` throws (OOM-only).

---

## Ranked summary

1. HIGH — amqp.d `finishPublish`/`routeTo`: CC/BCC altKeys on the default exchange skip the per-op write ACL → publish with `CC:["secret"]` writes into an ungranted victim queue.
2. HIGH — amqp.d `amqpTopicMatches`: exponential `#`-backtracking; one 255-byte routing key vs a 255-byte `#.x.`-pattern binding wedges a shard core yield-free.
3. MED — amqp.d `emitContent`: oversized content-header frame exceeds a 4 KB-negotiated peer's frame-max → remote third-party consumer disconnect.
4. MED/SUSPECT — amqp.d `gDrSecret`: low-entropy direct-reply token secret → offline brute force enables forged reply tokens / cross-client injection.
5. SUSPECT — amqp.d `settleNegative`/`requeueAllUnacked`/`commitTx`/`deadLetter`/`a10Requeue*`: TLS key buffers (`kb4/kb6/kbT/kb5/kbr/kra`) cross a yielding hop without the stack-copy guard used by sibling sites; wrong-queue push if `amqpDataExec` reads args post-park.

---

# CTF flags — `kafka.d`

# GLM-CTF-REPORT.md — kafka.d pass (commit 297afc8)

Focus file: `source/dreads/kafka.d`. Cross-file reasoning uses `server.d` (routing extracts) and `shard.d`. Severity ranked; every flag below has a concrete trace substantiated by the code shown. Items I could not fully prove (missing `amqpDataExec` body) are in SUSPECT with the exact missing fact named.

---

## FLAG 1 (HIGH) — `handleDescribeGroups`: member/protocol data read from a shared TLS reply buffer *after* a yielding hop → cross-group disclosure

**Where:** `handleDescribeGroups` (`static ByteBuffer req, rep; // TLS` + the `groupExists(groups[i])` call between the parse and the emission), kafka.d.

**Root cause:** `kgOp(...)` fills the **thread-shared TLS** buffer `rep`. The `Rd rr` slices (`proto`, `ptype`, and every member field `mid/gii/cid/meta/assign` read inside the member loop) point into `rep`. Between the parse and the emission, the handler calls `groupExists(groups[i])`, which issues `gKafkaExec(["hlen", ...])` — a data-plane call that **parks the fiber** whenever the group's key is owned by another shard (or the ring is under backpressure). During that park, any *other* connection served by the same thread that executes another `handleDescribeGroups` reuses `rep` via the same `static` declaration.

**Attack trace (sharded mode, shards≥2, two connections on the same shard thread):**
1. Conn A (attacker) sends `DescribeGroups(v0)` naming `g_victim` (a group with live members, whose `kafka.cg.g_victim` hashes to a *remote* shard — attacker picks the name so the coordinator hash routes off-thread).
2. Conn B (colluding or simply concurrent) sends `DescribeGroups` naming `g_attacker`.
3. A's `kgOp` returns; `rr` now slices `rep` holding g_victim's members, client-ids, metadata, assignments.
4. A calls `groupExists(g_victim)` → HLEN hops → A parks.
5. B's `kgOp` overwrites `rep` with g_attacker's FSM state.
6. A resumes and emits, for `group_id = g_victim`, the member_id / group_instance_id / **client_id / member_metadata / member_assignment** bytes now sitting in `rep` — i.e. another group's data.

**Primitive / impact:** disclosure of another principal's consumer-group assignment and subscription metadata; also silent corruption of the victim's DescribeGroups reply (wrong member counts possible if the two `rep` layouts differ, `nmemb` was snapshotted from the victim but the per-member reads come from the attacker's buffer → `Rd` runs off the end of the shorter reply; `Rd` is bounds-checked, so it degrades to nulls, no memory unsafety). DoS-ONLY + information disclosure, no RCE path (no write primitive).

**Fix:** copy the FSM reply to a fiber-local `ByteBuffer` (or copy `proto/ptype/members` to stack) before `groupExists`, exactly the pattern already used in `handleEndTxn` (`tbuf`/`otb` stack copies) and `handleDeleteTopics`. Alternatively hoist `groupExists` to *before* `kgOp`.

---

## FLAG 2 (HIGH, conditional — see note) — `handleProduce`: record blobs and the partition key live in TLS statics across the `pushRecords` data-plane hop

**Where:** `handleProduce`: `static ByteBuffer kb; // TLS`, `static ByteBuffer blobArena; // TLS`, `static const(char)[][] slices; static size_t[] offs;` then `pushRecords(kb.data.asChars, slices[0 .. nrec])` → `gKafkaExec(argv[0 .. nrec+2], rb)` with `argv`/`rb` also TLS inside `pushRecords`.

**Root cause:** `kafkaGroupHopImpl` in server.d documents the invariant explicitly: *"hb is STACK-local: shardEnqueue yields under ring backpressure and a TLS static would be clobbered by another fiber's hop during that yield."* The produce path violates it: when the partition key (`kafka.t.<topic>.<p>`) is owned by another shard, `pushRecords` hops with `argv[1]` pointing into `kb` (TLS) and `argv[2..]` pointing into `blobArena`/`slices` (TLS). Two clobber windows exist:
- the normal cross-shard park inside the hop, and
- ring-backpressure yields inside `shardEnqueue` (payload must stay valid across the retry loop).

During either, a sibling produce fiber on the same thread runs `blobArena.clear()` + re-append and rewrites `kb` via its own `partKey`.

**Attack trace:** two connections on one shard thread, sharded mode. Conn A produces to topic `T` partition 3 (remote owner) a large batch (records ≥ a few KB so the RESP synthesis + ring push is slow / or flood the ring to force backpressure yields). Conn B interleaves produces to topic `U` partition 9. A's `pushRecords` parks; B's `partKey` rewrites `kb` from `kafka.t.T.3` to `kafka.t.U.9` and `blobArena` now holds B's records. When A's enqueue finally copies the payload, the RPUSH lands on **B's key with a mix of A/B records, or A's records land in B's partition** — cross-tenant record misdelivery and log corruption at wrong offsets.

**Conditional:** this is a CONFIRMED design violation only if `amqpDataExec` does not deep-copy its `args` before the first yield. The file's own repeated defensive stack-copies (`handleFetch`'s `keyStore`, `handleListOffsets`'s `k3store`, `handleEndTxn`'s `tbuf`) strongly indicate it does not. Verify by reading `amqpDataExec`; if it copies args synchronously pre-hop, downgrade to non-issue — but then all those stack copies elsewhere are dead code, which is itself evidence.

**Fix:** copy `kb` to a stack array (same `char[8+KAFKA_MAX_TOPIC+16]` pattern as Fetch) and make `blobArena`/`slices`/`offs` fiber-local `ByteBuffer`s (the EndTxn comment shows the allocator churn concern was already solved once with stack arrays for the bounded case; for the unbounded produce case a per-connection arena owned by `KafkaConnCtx` is the clean fix).

---

## FLAG 3 (MED-HIGH) — `topicPartitionCount`: TLS key buffer used across the `partLen` hop (missed sibling of the Fetch/ListOffsets stack-copy fix)

**Where:** `topicPartitionCount`: `static ByteBuffer kb; partKey(topic, p, kb); auto key = kb.data.asChars; ... len = partLen(key); // not the owner shard: data-plane LLEN (hops)`.

Same aliasing class as Flag 2 but metadata-only: during the LLEN park, a sibling `handleMetadata`/`handleProduce` on the same thread rewrites `kb`, so the retried ring push can carry the LLEN for a *different* topic's partition. Result: wrong advertised partition counts (a producer writes to a partition index that doesn't exist, or a topic reports another topic's populated run). No memory unsafety (Rd/RESP parsers are bounds-checked), but it silently corrupts the routing contract and is trivially triggerable: flood `Metadata` for topics `aaaa…` and `bbbb…` alternately from two connections on one thread. Fix: stack-copy the key before `partLen` exactly as `handleFetch` does.

---

## FLAG 4 (MED) — Kafka ACL enforcement gaps: InitProducerId / AddPartitionsToTxn / TxnOffsetCommit / Heartbeat / SyncGroup / LeaveGroup have no `authorize()` on the transactional id or group

**Where:** `handleInitProducerId` (only `EndTxn` checks `KRES_TXNID`/`KOP_WRITE`), `handleAddPartitionsToTxn`, `handleTxnOffsetCommit(Flex)`, `handleHeartbeat`, `handleSyncGroup(Flex)`, `handleLeaveGroup`.

**Attack trace (ACLs active, `gKafkaAclActive == 1`, attacker is an authenticated non-super principal with no TXNID grants):**
1. Victim (a transactional producer) holds `tid = "victim-txn"` at epoch *n*.
2. Attacker sends `InitProducerId(v0, transactional_id="victim-txn")` — no `authorize(KRES_TXNID, …)` → `txnInit` runs the coordinator's `KGOP_TXN_INIT`, **bumping the epoch**.
3. Victim's next transactional produce is answered `INVALID_PRODUCER_EPOCH (47)` — repeatable at will → targeted producer fencing DoS.
4. Similarly, `Heartbeat(member_id of victim, group)` without `KRES_GROUP` READ lets an unauthorized principal drive a victim consumer group into rebalance loops (Heartbeat answers `REBALANCE_IN_PROGRESS` / member eviction via the FSM), and `AddPartitionsToTxn`/`TxnOffsetCommit` let an unauthorized client register partitions/offsets under a victim tid.

Root cause: the 297afc8 authorization sweep covered the describe/enumerate APIs but not the txn-coordinator handshake trio or the group keep-alive ops. Fix: add `authorize(tKafkaCtx, KRES_TXNID, tid, KOP_WRITE)` in `handleInitProducerId`/`handleAddPartitionsToTxn`/`handleTxnOffsetCommit*`, and `KRES_GROUP` READ in Heartbeat/Sync/Leave (matching JoinGroup's existing gate).

---

## SUSPECT (unproven, named gap)

- **`serveKafkaClient` SASL raw-mode frame accounting:** in `saslRawMode`, a bare token frame is consumed with `pos += 4 + sz` and the reply is buffered into `outb`, but the flush happens only after the frame loop; a client that pipelines handshake + token + a data request in one TCP segment gets the token reply deferred — conformance wart at most. Needs a client trace to call.
- **`emitJoinErr` v6 shape:** at flexible v6, `putCStr(o, "")` for protocol_name — spec-correct per KIP-543? Needs a librdkafka v6 join to confirm; flagged only as a conformance check.
- **Flag 2/3 dependence on `amqpDataExec` arg lifetime** — the single fact needed to confirm or clear both: does `amqpDataExec` copy `args` into its hop payload before any `yield()`? If yes, Flags 2–3 collapse to style; every other defensive copy in this file says no.

## Ranked summary (stdout)

```
HIGH  kafka.d handleDescribeGroups  — TLS `rep` slices read after groupExists() hop park → cross-group member/assignment disclosure (2-conn same-thread trace)
HIGH  kafka.d handleProduce         — key/blobArena/slices TLS statics live across pushRecords hop; sibling produce clobbers → records RPUSHed to wrong partition (conditional on amqpDataExec arg lifetime)
MED-H kafka.d topicPartitionCount   — TLS kb across partLen hop (missed sibling of Fetch keyStore fix) → wrong advertised partition counts
MED   kafka.d InitProducerId/AddPartitionsToTxn/TxnOffsetCommit/Heartbeat/Sync/Leave — missing authorize(): unauthorized txn-id epoch bump fences victim producer (INVALID_PRODUCER_EPOCH DoS)
SUSP  amqpDataExec arg-copy semantics — confirms/clears flags 2-3
```
