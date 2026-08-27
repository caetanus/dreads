# GLM-5.3 Security-Hardening CTF — dreads skins

_Adversarial pass by GLM-5.3 (z.ai) driven file-by-file. Each flag needs independent verification before any fix._

---

# CTF flags — `sqs.d`

# GLM-CTF-REPORT — `source/dreads/sqs.d` (round 4)

## FLAG 1 — HIGH: shared TLS key buffers (`qkey`/`ifkey`/`grpkey`) used across `exec()` parks → in-flight record written to the WRONG queue → cross-client message disclosure

**File:line:** `opReceiveMessage`, `sqs.d` (~line where `static ByteBuffer rb, qkey, ifkey, val, grpkey;` is declared, and the `hset` near the end of the receive loop). Also the FIFO `sismember`/`lrem`/`sadd` calls that read `grpkey.data`/`qkey.data` after earlier parks.

**Root cause.** Round-3 fixed the *record* aliasing (`snap`→`snapCopy`, `rec`→`recCopy`), but the **key** buffers are still `static ByteBuffer` (TLS), i.e. shared by every concurrent SQS connection fiber on this thread. `opReceiveMessage` builds `qkey`/`ifkey`/`grpkey` once at the top, then performs multiple `exec()` calls. Under sharding, any key whose owner shard ≠ `tShard` makes `amqpDataExec` park the fiber on the ShardPending. While parked, a sibling `opReceiveMessage` (another HTTP connection handled by the same shard thread — normal with the SO_REUSEPORT multi-router, and trivially arrangeable with two connections) runs `ifkey.clear(); ifkey.append(IF_PREFIX); ifkey.append(name)` with **its own** queue name. When fiber 1 resumes, its subsequent commands read the *rebuilding* fiber's key:

```d
const(char)[][4] hset = ["hset", cast(const(char)[]) ifkey.data,   // <-- now "sqs.if.B"
    cast(const(char)[]) handle[], cast(const(char)[]) val.data];
exec(hset[], rb);
```

So the in-flight record of **queue A** (client A's message: msgid, md5, group, **body**) is stored under **queue B**'s in-flight hash.

**Concrete attack trace (proof of disclosure):**
1. Sharded deployment (`--shards N`, N≥2; pick queue names whose slots hash to a foreign shard so the pop parks).
2. Attacker controls queue `B` (or simply listens on it). Victim sends to queue `A`.
3. Conn 1: `ReceiveMessage` on `A` with `VisibilityTimeout: 0`. Conn 2 (same shard thread): `ReceiveMessage` on `B`, issued so fiber 2 rebuilds `ifkey` while fiber 1 is parked inside the `lpop` exec. Fiber 1's `hset` now targets `sqs.if.B` with A's record and fiber 1's `deadline = nowMs() + 0`.
4. `sqsVisibilitySweep` (1 s timer) sweeps `sqs.if.B` on B's owner shard, finds the already-expired entry, and **LPUSHes A's record into `sqs.q.B`**.
5. Attacker's next `ReceiveMessage` on `B` returns the victim's full message body from queue A. Cross-tenant disclosure, no auth needed.

Secondary corruption from the same bug: the receipt handle issued for A is now unknown to `sqs.if.A` (DeleteMessage fails, message re-delivered from A's list — duplication), and in the FIFO branch `sismember grpkey.data` / `lrem qkey.data` read sibling-rebuilt keys, so group locks are checked/enforced against the wrong queue's lock set (FIFO ordering bypass across groups/queues).

**Fix.** Make `qkey`/`ifkey`/`grpkey` per-call `ByteBuffer` locals (like `snapCopy`/`recCopy`), or snapshot `key.data` into a per-call buffer before each `exec` that follows a park. Same audit for every `static ByteBuffer key` in this file that is built once and read after an intervening `exec` (`opDeleteQueue` and `opPurgeQueue` rebuild `key` before every use — they are fine; `opReceiveMessage` is the offender).

---

## FLAG 2 — MED: `jsonStr` silently truncates message bodies at 256 KiB while the HTTP layer admits 4 MiB → stored message corrupted with a self-consistent (wrong) MD5

**File:line:** `jsonStr` (`static char[262144] ub;` with `if (o < ub.length) ub[o++] = …`), vs `onConn`'s `MAX_BODY = 4 * 1024 * 1024`.

**Trace:** send `SendMessage` with a 300 KiB `MessageBody`. `Content-Length` passes the 4 MiB cap, the body parses, but `jsonStr` drops every byte past 262 144 with no error. `sendOne` computes the MD5 **of the truncated body**, RPUSHes it, and returns `MD5OfMessageBody` of the truncated text. The client (boto3 verifies MD5) sees a mismatch — but the broker has *durably stored and will re-deliver* a message that is not what was sent, and every SQS size limit check is bypassed (a "256 KiB max" queue silently stores 256 KiB, but callers believing the ack have delivered truncated payloads). Severity MED: data corruption, not disclosure.

**Fix:** reject bodies whose unescaped length exceeds 262 144 with `AWS.SimpleQueueService.MessageTooLong` (413/400), instead of truncating.

---

## FLAG 3 — MED (design-confirm): the SQS skin performs no authentication at all

Every operation (`CreateQueue`…`PurgeQueue`) dispatches with zero SigV4 / token / ACL checks, and queue URLs are guessable (`queueFromUrl` takes the last path segment). Any peer that can reach `sqsPort` can read, purge, or delete every tenant's queues. The charter lists "SigV4-less SQS" as an auth surface; if this is intentional pre-release scope, it must at minimum be documented and the port treated as trust-boundary-internal. Combined with Flag 1 there is a full unauthenticated cross-tenant read primitive. **Fix/decision:** enforce SigV4 or a shared-secret header gate before GA.

---

## SUSPECT / DoS-hardening-backlog (not CTF flags)

- **`onConn` single `read(IOMode.once)` + `bodyStart()==0` on split headers** → request rejected as 400; a slow-drip client just gets errors. Backlog: loop until headers complete.
- **`splitRecord` on malformed records** yields null fields (D slice `.init`), no OOB — safe.
- **`respEachBulk`/`respBulk`/`findKey`/`jsonEachEntry` bounds** — re-checked, all length-guarded; the round-3 structure-aware `findKey` and `Content-Length` clamp hold.
- **`sqsVisibilitySweep` statics (`expHandles`, `expRecs`, `dueIds`, …)** are shared TLS, but the sweep is a single timer fiber per shard with no concurrent sibling — safe today; would break if the sweep ever became per-queue parallel.

## Ranked summary

1. **HIGH — sqs.d opReceiveMessage: shared TLS `ifkey`/`qkey`/`grpkey` rebuilt by a sibling fiber during an exec park → victim's in-flight record hset under attacker's queue → visibility sweep re-pushes it into the attacker's queue (cross-tenant message body disclosure).**
2. **MED — jsonStr 256 KiB silent truncation vs 4 MiB body cap: durably stored truncated message with self-consistent MD5 (data corruption / contract violation).**
3. **MED — zero authentication on the entire SQS skin (no SigV4): unauthenticated read/purge/delete of all queues.**
4. *Backlog — partial-header 400 handling; single-fiber-only assumption in the visibility sweep.*

---

# CTF flags — `kafkagroup.d`

# CTF pass — `source/dreads/kafkagroup.d`

## Verdict on the round-3 fixes visible here

- **TXN_ADD hash-set dedup + negative-partition drop + topic-length-249 guard**: verified correct. `char[300] tb`, `tl ≤ 249`, `pl ≤ 11` digits → max `tl+1+pl = 261 < 300`, no overflow; `r.ok` checked before each read; the `t.tpsSeen` set is cleared exactly wherever `tps` is cleared (TXN_INIT and TXN_END). ✔
- **TXN_OFFS / TXN_END caps** (`KG_TXN_MAX_OFFS`, `KG_TXN_META_BUDGET`): checked before append, budget accumulates only accepted entries. ✔
- `KgRd` is fully bounds-checked (str16/bytes32/i32/u8 all test `i + n > p.length` before slicing; `n` is uint promoted to 64-bit `i + n` so no size_t wrap). ✔ **No out-of-bounds primitive found in this file.**

---

## FLAG 1 — HIGH: Producer-id/epoch entropy is ~trivially brute-forceable → cross-client transaction hijack (abort/fence/steal offsets)

**Where:** `kafkagroup.d`, `gKgPidCtr` declaration (`private shared long gKgPid_ctr = 5000;`), `KGOP_TXN_INIT` (`t.pid = atomicOp!"+="(gKgPidCtr, 1);`), and the `KGOP_TXN_ADD` / `KGOP_TXN_OFFSETS` / `KGOP_TXN_END` guards (`if (tp is null || (*tp).pid != pid) …; if (epoch != t.epoch) …`).

**Root cause:** The *only* credential on TXN_ADD/OFFSETS/END is `(pid, epoch)`. Pids are a **shared global counter starting at 5000, incremented once per transactional id ever created** — a tiny, dense, enumerable space (and an attacker can *inflate* the counter deterministically by creating its own txn ids, learning the exact current range). Epochs start at 0 and increment on re-init — also tiny. There is no per-transactional.id secret; fencing is knowledge of a ~12-bit number.

**Attack trace** (Kafka skin, classic protocol, from any client that can reach the txn-coordinator path):
1. Attacker opens a Kafka connection, sends InitProducerId for its own ids `attacker-1..50` → observes the pid values handed back (e.g. 5001…5050) → now knows the live pid window for all clients on this broker.
2. For victim pid candidates p in that window and epochs 0..3, attacker sends a TXN_ADD payload `[op=11][txnLen][victimTxnId][pid=p][epoch=e][1 partition]` via the group-coordinator hop. Error 51/47 = miss; `err=0` = **hit — attacker is now inside the victim's open transaction**.
3. Attacker sends **TXN_END with committed-flag = 0 (abort)**:
   - Reply `[i16 0][i32 nTps]{topic,part}*[offGroup][nOffs]{topic,part,off,meta}*` — a **cross-client disclosure of the victim's buffered TxnOffsetCommit data** (group name, topics, partitions, offsets, and attacker-readable `meta` strings the victim committed).
   - Side effect: `t.tps/offs/offGroup` are cleared and the real producer's subsequent EndTxn/Adds get 51 or return empty — **silent data corruption of the victim's transactional offset pipeline** (the victim's consumer group will consume from wrong offsets after its aborted-txn offsets vanish).
4. Cheaper variant needing no pid at all: send **TXN_INIT for the victim's transactional.id** (usually a guessable string like `my-producer-1`). That bumps `t.epoch` and wipes `t.tps/offs` unconditionally — permanent fencing + buffered-offset loss for the victim. *(This variant is only a flag if kafka.d's round-3 `authorize()` does NOT ACL-gate InitProducerId per txn-id; if it does, the pid-guessing path in steps 1–3 remains, since ADD/OFFSETS/END gate on pid+epoch only.)*

**Severity:** HIGH (cross-client disclosure + data corruption). Not memory corruption → **DoS/corruption grade, not RCE**.

**Fix:** allocate pids from a CSPRNG over the full 63-bit range (mirroring the AMQP direct-reply-secret fix in the same round), and/or bind a random 128-bit secret to the txn id at INIT, required on every ADD/OFFSETS/END.

---

## FLAG 2 — MED: TXN_END partition re-parse integer overflow → wrong partition in aborted/committed-txn marker list

**Where:** `kafkagroup.d`, `KGOP_TXN_END`, the reconstruction loop:
```d
int part = 0;
if (sep < x.length)
    foreach (c; x[sep + 1 .. $])
        if (c >= '0' && c <= '9')
            part = part * 10 + (c - '0');
```

**Trace:** TXN_ADD already rejects `part < 0` and caps topic length, but a `part` of e.g. `2147483648` (or any value > INT_MAX) passes the `part < 0` check as a *positive* i32 read (values up to 2^31−1 pass; values ≥ 2^31 arrive negative and are dropped — but `2000000000` passes) and is stringified into `tb`. On TXN_END, the digit loop `part*10 + …` overflows signed int (wraps to negative/garbage), so the caller writes control markers / txn-offsets for a **wrong or negative partition** — data corruption of the offsets hash (wrong key written on commit) reachable from a single well-formed client frame. Bounded corruption, no memory unsafety: **DoS/corruption-only, not RCE**.

**Fix:** parse with a `long` accumulator and reject/emit error 51 on ADD when `part > 999999` (real Kafka partition counts are small), or clamp on END with the stored numeric part rather than a re-parse (store `int part` alongside the string instead of re-parsing).

---

## Correctness note (not a flag)

`KGOP_JOIN` → `closeBarrier` returning `true` with `g.order.length == 0` (all members missed the round, but this joiner is in `members`): the code calls `emitJoinOk` for a member that was just removed, emitting a join-ok with empty protoName/protoType and a stale leader id; the member's subsequent SYNC correctly fails with 25. Compare with `KGOP_JOIN_POLL`, which *does* check `(mid in g.members) is null` after the deadline closeBarrier. Add the same check after the JOIN-path closeBarrier for protocol cleanliness.

## DoS-hardening-backlog (not CTF flags)

- `KgMember` per-member protocol metadata (`bytes32`, up to 64 entries) and `gii/clientId` are uncapped in bytes; `KG_MAX_MEMBERS × frame-size` bytes per group, `KG_MAX_GROUPS` per shard. Backlog: cap aggregate metadata bytes per member.
- `KGOP_TXN_ADD` up to 1024 tps per request × string idup per entry — fine under the caps, but the per-entry `idup` churn is GC pressure an attacker can amplify. Backlog only.

## Ranked summary

1. HIGH — kafkagroup.d `gKgPidCtr`/TXN guards: sequential 5000-based pids + tiny epochs ⇒ brute-forceable (pid,epoch) lets any client abort/steal another producer's open transaction and read its buffered offsets (TXN_END reply). Fix: CSPRNG pid/secret.
2. MED — kafkagroup.d KGOP_TXN_END part re-parse: signed-int overflow writes txn markers/offsets to a wrong partition (client-frame-driven corruption). Fix: numeric part stored, not re-parsed.

---

# CTF flags — `mqtt.d`

# GLM-CTF-REPORT — mqtt.d (round 4)

Ranked findings. I verified each trace against the code shown; the two WS-handshake and takeover items are the strongest proven defects.

---

## FLAG 1 — CRITICAL: WebSocket handshake uses **shared TLS static** `reqbuf`/`respbuf` across a 30-second yield → cross-client handshake contamination and identity injection

**File:line:** `source/dreads/mqtt.d`, `serveMqttClient`, WS branch (`static ByteBuffer reqbuf, respbuf;` immediately after `if (ws)`), and the `tail = wsBodyAfterHandshake(...)` line.

**Root cause:** `reqbuf` and `respbuf` are function-scope `static` (TLS) — one buffer per shard thread, shared by *every concurrently-handshaking* WS client. The handshake loop contains an explicit suspension point: `if (!tcp.waitForData(30.seconds)) break;` — a fiber parks here for up to 30 s with its client's partial HTTP request sitting in the shared `reqbuf`. Any other WS client accepted by the same shard thread (SO_REUSEPORT means every shard accepts) then **appends its own request bytes into the same buffer**.

**Attack trace (concrete):**
1. Client A (attacker) opens TCP to the MQTT-over-WS port, sends a partial HTTP upgrade request with no terminating `\r\n\r\n`, then stalls. Fiber A parks in `waitForData(30s)` with A's bytes in `reqbuf`.
2. Client B (victim) connects to the same port (kernel SO_REUSEPORT may hash it to the same shard — attacker improves odds by spraying many A-connections). B sends its full upgrade request **followed by a pipelined MQTT CONNECT** (legal per the code's own `wsBodyAfterHandshake` support). Fiber B appends B's bytes onto A's in `reqbuf`, sees `wsHeadersComplete` → true, computes the 101 for the **concatenated A+B request**, sends it to B, then does `tail = wsBodyAfterHandshake(reqbuf.data)` and feeds the leftover — **B's CONNECT — into B's codec** so far so good; the damage is on A's side:
3. Fiber A resumes with more of A's bytes appended after B's already-consumed region; `wsHeadersComplete` may now see completion from the mixed stream; A's `wsHandshakeResponse` is computed over a request that includes **B's headers** (wrong `Sec-WebSocket-Key` → B's Accept value / B's pipelined bytes), and critically `wsBodyAfterHandshake` on the shared `reqbuf` can return **B's pipelined MQTT CONNECT bytes**, which are fed into **A's** `c.wsCodec` (`cast(void) c.wsCodec.feed(tail)`).

**Consequence:** A's connection is established and authenticated from **B's CONNECT payload** (username/password/clientId). A inherits B's ACL user, B's session (session takeover of B's clientId, offline-queue adoption via `mqttMigrateParked`), and every subsequent frame A sends executes under B's identity — a cross-client **authorization bypass + disclosure**. Second-order: `respbuf` is also shared; a slow `legSend`/`tcp.write` of the 101 for one client can be mid-flight while the next handshake rewrites it.

**Fix:** make `reqbuf`/`respbuf` per-connection (stack locals or `MqttConn` members, like `c.wsOut` — the exact lesson already applied for WS frame writes). The `static ubyte[65536] wsread` in the same loop is borderline-safe only because each fiber feeds its codec before its next yield; convert it to a per-conn buffer too while you're there.

---

## FLAG 2 — MEDIUM/HIGH: `takeoverLocal` holds a TLS static buffer **across a yielding `sendTo`**

**File:line:** `takeoverLocal` — `static ByteBuffer db; db.clear(); mqttServerDisconnect(db, 0x8E); sendTo(victim, db.data);`

**Root cause:** `sendTo` takes `victim.wlock` and performs `legSend`/`tcp.write(wb.data)`/WS framing — all of which can **yield on write backpressure** while holding the slice `db.data` into the thread-local static. Meanwhile another fiber on the same shard processing a CONNECT with the same clientId calls `takeoverLocal` again → `db.clear()` + append (which may **realloc and free the old block**) while the first write is still in flight.

**Trace:** victim V1 has a full socket buffer; CONNECT(clientId=X, gen2) triggers `takeoverLocal` → `sendTo(V1, db.data)` parks in `tcp.write`. CONNECT(clientId=X, gen3) on a *new* connection (same shard) runs `db.clear()` → old 4-byte DISCONNECT block may be freed by realloc → V1's resumed write reads freed heap memory and transmits it (heap-disclosure primitive, small) or a rewritten packet. This is the precise class round-3 fixed for WS frames (`c.wsOut` per-conn) — `db` was missed.

**Severity cap:** the packet body is a fixed 4-byte DISCONNECT, so disclosure potential is a few bytes of freed-heap; primary impact is protocol corruption / UAF-read. **PLAUSIBLE-RCE: no** — no write primitive, no length control. DoS/disclosure-class.

**Fix:** stack-local `ByteBuffer db` (it's small and copied by `sendTo`'s WS path into `c.wsOut` anyway), or append into `victim.wsOut`-style per-conn storage.

---

## FLAG 3 — MEDIUM (data corruption): `fwdProps` TLS static re-read by `shardEnqueue` **after a yield** under ring backpressure

**File:line:** `PT_PUBLISH` handler — `static ByteBuffer fwdProps; ... props = fwdProps.data;` then `gMqttFanout(topic, payload, retain, rseq, qos, props)` → `shardMqttFanout` → `shardEnqueue(dst, hdr~payload, …)` in shard.d.

**Root cause:** `shardEnqueue` on a **full ring** does `shardWake(dst); yield();` and then **re-pushes from the same source slices** on retry. `topic`/`payload` slice the fiber-local `inb` (safe), but `props` slices the **TLS static `fwdProps`**. During the yield, a second publisher fiber's PUBLISH on the same thread does `fwdProps.clear()` + rebuild → the retried push serializes a *different or mixed* property block to the other shards. Downstream, `mqttDeliverLocal` on the peer parses that mixed block (`msgExpiryFromProps`) and forwards the bytes to subscribers — cross-client property corruption (content-type/user-property/correlation-data of client B delivered attached to client A's payload).

**Trace:** fill the cross-shard ring (16384 slots — e.g. a burst of large cross-shard MQTT publishes) so a publisher's fan-out blocks in `shardEnqueue`'s yield, while a co-located second connection publishes with v5 properties.

**Severity:** data corruption only under backpressure; no memory unsafety (lengths are re-bounded by the ring push). The sibling hops (`amqpPushStage`, `kafkaGroupHopImpl`) were fixed to stack-local buffers; the MQTT fan-out path was not.

**Fix:** in the PUBLISH handler, `idup` (or copy into a stack `ByteBuffer`) the forwardable props before `gMqttFanout`, mirroring `kafkaGroupHopImpl`'s "stack-local hb" discipline.

---

## Contract / backlog (not flags)

- **Retained-on-SUBSCRIBE replay ignores the client's v5 `maxPktSize`** (`PT_SUBSCRIBE` retained loop appends into `o` with only an `MQTT_OBOX_CAP` bound). A v5 client advertising a small maximum-packet-size can be sent a PUBLISH larger than it accepts — protocol-contract break, LOW. One `if (c.maxPktSize && o.length+pkt > c.maxPktSize) break;` fixes it.
- **DoS-hardening-backlog:** SUBSCRIBE-time retained replay scans all of `gRetained` (≤65536 topics) per SUBSCRIBE — O(N·filters) per packet, bounded but amplifying; `wsread` shared TLS buffer (analyzed above — safe only by the resume-then-feed-without-yield scheduling invariant; harden it with FLAG 1).
- Skipped per instructions: xshard-adopt-lifetime UAF, failed-freeze double session, TLS-across-hop-as-ARGS class.

---

## Ranked summary

1. **CRITICAL** — mqtt.d `serveMqttClient` WS branch: shared TLS `reqbuf`/`respbuf` across `waitForData(30s)` yield → cross-client handshake mixing; attacker can get victim's pipelined CONNECT fed into own codec → auth/session takeover of the victim.
2. **MED-HIGH** — mqtt.d `takeoverLocal`: `static ByteBuffer db` held across yielding `sendTo` → mid-write rewrite / freed-block read (4-byte disclosure ceiling). DoS-only-grade, no RCE path.
3. **MED** — mqtt.d `PT_PUBLISH` `fwdProps` TLS static re-read by `shardEnqueue` after backpressure yield → cross-client v5 property corruption in cross-shard fan-out.
4. **LOW** — retained-on-subscribe replay ignores v5 maximum-packet-size (contract break).

---

# CTF flags — `amqp10.d`

# AMQP 1.0 skin (amqp10.d) — hardening round-4 findings

I verified the round-3 fixes present here (unsettled-map cap, pendingBytes aggregate budget, per-conn link cap, dispScratch moved to connection state, msgOff fragment clamp, shortstr 255 clamps) — they look correct. Three **new** defects follow, ranked.

---

## FLAG 1 — Stack out-of-bounds READ via `snprintf` return value in bindings listing (memory disclosure)

**Where:** `a10HandleMgmt`, the `/bindings?src=...&dstq=...&key=...` GET branch — the `a10ListBindings` callback (`char[900] loc` block).

**Severity: HIGH** (CONFIRMED memory disclosure of ~N bytes past a stack buffer to the wire; read-only primitive, no corruption — an info-leak that defeats ASLR, useful as the missing primitive for any future write primitive).

**Trace:**
```d
char[900] loc = void;
immutable lnn = snprintf(loc.ptr, loc.length,
        "/bindings/src=%.*s;%s=%.*s;key=%.*s;args=",
        cast(int) src.length, src.ptr, ...);
a10Str(bodyOut, loc[0 .. lnn]);   // <-- OOB when lnn > 900
```
`snprintf` returns the *would-have-written* length, not the truncated length. `src`/`dst`/`key` are raw (undecoded) slices of the management message's `properties.to` string, whose only bound is the 1 MiB frame cap. A client sends a management request:

- attach a sender link to `/management`, transfer a message with `properties{to="/bindings?src=<400 bytes>&dstq=<400 bytes>&key=<400 bytes>", subject="GET"}`.

The formatted string needs ≈1270 bytes > 900, so `lnn ≈ 1270` and `loc[0 .. lnn]` slices **370 bytes past the 900-byte stack array**; those bytes are appended verbatim into the 200 response body returned to the client. The leaked bytes are adjacent stack contents of the serving fiber (pointers, other locals, possibly cached record slices) — deterministic, attacker-repeatable, tunable length by padding the query string.

**Root cause:** using `snprintf`'s return as a slice length without clamping to the buffer (and to what was actually written).

**Fix:** `if (lnn < 0) lnn = 0; if (lnn > loc.length) lnn = loc.length;` — or better, cap the echoed `src`/`dst`/`key` to a sane length before formatting. Audit the other `snprintf` sites: the `eb[600]`/`bb2[300]` ones are currently safe only because their `%.*s` inputs happen to be bounded (addrBuf≤512, key≤127) — clamp them too so a future caller can't reintroduce this.

---

## FLAG 2 — `a10ReadFrame`'s TLS static `buf` escapes into handlers that hop; slices are READ AFTER the hop (cross-client disclosure / topology corruption)

**Where:** `a10ReadFrame` (`static ubyte[] buf; // TLS scratch`) → the `body_` slice it returns → `a10HandleMgmt` (and the bindings-listing callback) using `corrRaw`, `bodyMapBytes`, `src`/`dst`/`key` **after** data-plane hops.

**Severity: HIGH** (CONFIRMED cross-client disclosure, sharded mode (`--shards >1`). Not memory-unsafe — `buf` only grows, and D GC reallocation leaves the old block alive, so the stale slices stay in-bounds — but they alias *another client's frame bytes*.)

**Trace (concrete):**
1. Client A connects to shard 0, attaches a sender link to `/management`, and sends `PUT /queues/victim` with `properties{message-id = "SECRET-A"}` and an `arguments` map.
2. The read fiber calls `a10ReadFrame` → `body_ = buf[skip..rest]` (slice into the **thread-shared** TLS `buf`), then `a10HandleMgmt` sets `corrRaw = val.bytes[at0..pd.i]` — a slice into `buf` — and `bodyMapBytes = val.bytes` — also into `buf`.
3. `a10DeclareQueue(qn, ...)` (and, on the redeclare path, `a10QueueExists`/`a10ExclusiveOwner`/`a10QueueMetaGet` first) route through `amqpDataExec` → cross-shard hop → the fiber **parks** on the `ShardPending`.
4. While A is parked, client **B**'s read fiber on the *same thread* reads a frame: `a10ReadFrame` reuses the same TLS `buf` (e.g. `buf[0..rest] = B's entire AMQP frame`).
5. A's fiber resumes. Post-park reads of the stale slices:
   - `a10MgmtRespond(..., corrRaw, ...)` — appends `corrRaw` raw into the response transfer. `corrRaw` now points at client **B's frame bytes** → **B's data is delivered to A** (post-hop read line: `if (corrRaw.length) o.append(cast(const(char)[]) corrRaw);`).
   - PUT path: `auto args9 = a10MapGet(bodyMapBytes, ...)` executed *after* the existence/meta hops, then `gA10ArgsRaw[qk9] = args9.bytes.idup` — **persistently stores another client's frame bytes** as the queue's declared-arguments metadata, echoed back to every future `GET /queues/<name>` by any client.
   - `/bindings` GET: `src`/`dst`/`key` (slices into `buf`) are re-read inside the `a10ListBindings` callback, which itself hops per binding → the `location` string leaks B's bytes.

This is exactly the "read after the hop returns" class the charter asks for — unlike the refuted args-passed-to-hop cases, these slices are re-dereferenced *after* `a10DeclareQueue`/`a10ListBindings` return.

**Root cause:** the frame body was never copied out of the single per-thread scratch buffer before entering a path that can yield; the comment "consumed before return; no yield holds it" is true only for the *non*-management handlers (transfer's `msg` is consumed by `a10MapMessage` before `a10Publish`), not for `a10HandleMgmt`.

**Fix:** in `a10HandleMgmt`, immediately `idup` the pieces that outlive any call that can hop: `corrRaw`, `bodyMapBytes` (or `msg.idup` wholesale at function entry), and decode `src`/`dst`/`key` into stack buffers before the listing callback. Alternatively make `a10ReadFrame` hand back an owned copy for the mgmt path. (Also make the comment on `buf` state the real invariant.)

---

## FLAG 3 — Unbounded recursion in the type decoder → stack overflow (remote crash)

**Where:** `A10Dec.readValue`, `case 0x00` (described constructor) — `auto d = readValue();` recurses with no depth limit. `a10Performative` and `skipValue` inherit it.

**Severity: MED — DoS-ONLY.** Classification per charter: no controlled write; the crash is a guard-page SIGSEGV with a fully attacker-chosen *depth*, not a steerable overwrite, so no RCE path.

**Trace:** send an AMQP frame whose body is `0x00` repeated N times (each byte: described-constructor whose descriptor is the next described value). Body size up to `A10_MAX_FRAME`−8 ≈ 1 MiB → recursion depth ≈ 1,048,576 frames of `readValue` (~100–200 B/frame) ⇒ ~100–200 MB of stack → guaranteed stack-guard SIGSEGV, killing the whole shard thread (every client on it). Reachable pre-auth if the SASL layer's `initial-response`... (no — that's parsed as raw bytes, not A10 values; but the **SASL mech is decoded with `readValue`**: `a10Performative(body_, ...)` on the SASL frame, then `fields.readValue()` for mech — one `0x00`-run in the sasl-init list reaches the same recursion). So this crashes the broker *before authentication*.

**Fix:** add a depth counter to `A10Dec` (e.g. `ubyte depth; if (depth++ > 64) { ok = false; return; }` in the described case and in `skipValue`'s two-read path), or an explicit iteration budget on total reads.

---

## DoS-hardening-backlog (not flags)

- `gA10ArgsHash` / `gA10QueueType` / `gA10ArgsRaw` / `gA10ArgsCount` TLS maps grow with every declared queue name and are never pruned on DELETE — unbounded per-shard memory (backlog item).
- Stream links: when the per-session unsettled cap trips, `streamPos` has already advanced past skipped records — a credit-starved/replay consumer silently loses offsets (correctness wrinkle, stream-only).
- `a10StartDelivery` polls at 1 ms per idle link × 4096 links/conn — CPU burn, bounded.

## Ranked summary

1. **HIGH** amqp10.d `a10HandleMgmt` /bindings listing — `snprintf` return used as slice length → OOB stack read (≈len−900 bytes) returned to client; info-leak/ASLR-defeat primitive.
2. **HIGH** amqp10.d `a10ReadFrame` TLS `buf` vs `a10HandleMgmt` — `corrRaw`/`bodyMapBytes`/`src,dst,key` re-read after data-plane hops park → cross-client frame-byte disclosure + persistent topology corruption via `gA10ArgsRaw`.
3. **MED (DoS-only, pre-auth)** amqp10.d `readValue` case 0x00 — unbounded recursion, `0x00`-fill frame → stack overflow, shard-thread crash.

---

# CTF flags — `amqp.d`

# GLM-CTF-REPORT — `source/dreads/amqp.d` (round 4)

I read the full file plus the shard-fabric extracts. The round-3 fixes (CC/BCC ACL, linear topic matcher, CSPRNG reply-to secret, frame-max clamp, per-conn byte caps) check out as landed and correct. Two new proven defects below; everything else I chased (the `kb4/kb5/kbT/kb3` TLS buffers, `rec`/`dlrec`/`dests`/`ctlBroadcast` statics, `amqpTtlSweep`'s stack-key copies, splitRecord/appendHeadersExcept/mergeXDeath/replaceReplyTo bounds walks, `tableWalk` length math, `drParse` overflow, `settleTagUnknown` channel scoping) is either busy-guarded, stack-copied before the yield, or bounds-checked — no flag.

---

## FLAG 1 — HIGH — Per-op ACL is enforced on publish/get/consume but NOT on `queue.purge` / `queue.delete` (and bind/unbind): any authenticated user can destroy or inject into queues they have no grant on

**Where:** `handleFrame`, `case 50` (`queue`), methods 30 (purge) and 40 (delete) — the handlers go straight from `exclusiveDenied()` to `gAmqpDelKey(delKey)` with no `aclCanAccessKey` call. Methods 20 (bind) / 50 (unbind) are likewise ungated. Contrast `case 60` mth 70/20 and `finishPublish`, which all do the `aclUserCount() > 1 → aclCanAccessKey(...)` check.

**Attack trace (destruction / availability of another tenant's data):**
1. Operator configures ACL users: `victim` owns queues `victim.orders` (grant `victim.*`), `attacker` has grant `attacker.*` only (any single key pattern passes the `connection.open` vhost gate at `case 10 mth 40` — a user with one key pattern is not the "no grants at all" 530 case).
2. `attacker` connects (PLAIN ok), opens a channel.
3. Sends `queue.purge("victim.orders")` → `exclusiveDenied` passes (not exclusive) → `gAmqpDelKey("amq.q.victim.orders")` → **victim's entire queue contents destroyed**. No read grant was ever needed; the attacker also learns the exact message count from `purge-ok.message_count` (depth info-leak on a queue it cannot read).
4. Same for `queue.delete("victim.orders")`: backing list DEL'd, existence tombstone broadcast — plus queue-delete cascades the binding tombstones, so victim's topology is destroyed too.

**Injection variant via bind (cross-tenant message injection):** `queue.bind(queue="victim.orders", exchange="attacker-ex", rk="x")` — the handler requires only that both *exist*; no ACL. `attacker` then `basic.publish` to `attacker-ex` (its own exchange, write-granted) → routed **into `victim.orders`**, a queue attacker has no write grant on. Combined with e2e binds this also lets an attacker splice another user's exchange graph.

**Root cause:** the per-op ACL gate was added only to the basic-class data ops (get/consume/publish) and the CC/BCC keys — the queue-management class was never gated.

**Fix:** in `case 50`, before any data-plane effect, run the same `aclUserCount() > 1 → aclCanAccessKey(au, q, /*read=*/false, /*write=*/true)` check for purge/delete/bind/unbind (and for `queue.declare` passive's count disclosure, consider gating or stripping `message_count` for non-granted users). Use the channel's `lastQueue`-resolved name, not the raw field.

**Severity justification:** requires ACLs to be configured (unconfigured brokers are accept-any by design), but when they are, this is a clean authorization bypass → cross-tenant data destruction + message injection. Not memory-unsafe; DoS-impact class.

---

## FLAG 2 — MEDIUM-HIGH — `finishPublish`: the TLS statics `sp` (BCC-strip) and `drp` (reply-to rewrite) are READ AFTER a yield — concurrent same-shard publish can swap another client's property block into the stored record

**Where:** `finishPublish`, `static ByteBuffer sp` and `static ByteBuffer drp` (neither has the `busy` guard used for `rec`/`recStatic`).

**Trace:**
1. Fiber F1 (conn A, shard thread T) publishes with a `BCC` header: `effProps = cast(const(ubyte[]) sp.data)` after the BCC-splice (`sp` is TLS, unguarded).
2. F1's message has `reply-to = amq.rabbitmq.reply-to`, so it enters the `drDirect` block and — when the token names a *different* live conn — executes `sendTo(tc, dout.data)`. `sendTo` takes `tc.wlock` (`TaskMutex.lock()` **yields** when the target conn's consumer fiber holds it) and `tcp.write` yields.
3. During that park, fiber F2 (conn B, same shard thread T) runs its own `finishPublish` with a BCC header → `sp.clear(); sp.append(...)` **clobbers the shared static**.
4. F1 resumes and reads `effProps` (which points into `sp`/`drp`) at:
   - `auto hdrs = propsHeaders(effProps);`
   - `buildRecord(*rec, cast(long) nowMs(), 0, ch.pub.rkey, effProps, ch.pub.payload.data, ch.pub.exchange);`

   → the record **stored into conn B's intended queue path / delivered to conn A's consumer carries conn B's property block** (headers, correlation-id, expiration, reply-to token) fused with conn A's body.

**Impact:** cross-client data corruption — one client's message is persisted and delivered bearing another client's headers (a leaked direct-reply token in `reply-to` is one concrete disclosure: the victim's RPC reply-consumer token ends up in a record the attacker can consume, since the record body is the attacker's own publish routed wherever the attacker's fan-out went). Not a controlled-write primitive — the clobbered value is another client's property bytes, not attacker-chosen offsets — so this is corruption/disclosure, not RCE.

**Root cause:** the comments on `sp`/`drp` claim "consumed by buildRecord before any yield", but the `drDirect` delivery block sits *between* the assignment and the consumption and contains a genuine yield (`sendTo` → mutex lock + socket write).

**Fix:** either (a) hoist the `buildRecord` of `rec` to before the `drDirect` block (the direct-reply delivery builds its own `drec` anyway), or (b) copy `effProps` into a stack/fresh buffer (or add `spBusy`/`drpBusy` guards like `recStatic`) before entering `drDirect`.

---

## SUSPECT (unproven, needs one more fact)

- **`queue.declare` (incl. passive) leaks `message_count`/`consumer_count` of queues the caller has no grant on** — same ungated class as Flag 1; I folded it there rather than count it separately. Confirm whether the test harness treats counts as sensitive.
- **`channel.open` permitted before `connection.open` under ACL** (`cls != 10 && !c.opened` only fires for classes other than 10 *and* 20 — no: the gate is `cls != 10`, so class 20 IS gated... re-checked: `if (cls != 10 && !c.opened)` blocks 20 too). Not a bug — withdrawn.

## DoS-hardening backlog (not CTF flags)

- `cancelledTags`/`gQueuePrios`/`gExchangeSeq` tombstones are capped (65536 / 4096) — fine; `gQueueLease` entries are removed only on delete — bounded by declare cap, OK.
- The 200×1ms spin-waits in `basic.cancel`/`channel.close` are per-event bounded — OK.

## Ranked summary

1. **HIGH — amqp.d `handleFrame` case 50 (purge mth 30 / delete mth 40 / bind 20 / unbind 50): no per-op ACL** — any granted user purges/deletes/binds arbitrary queues → cross-tenant destruction + message injection + depth leak. Fix: add the `aclCanAccessKey` gate.
2. **MED-HIGH — amqp.d `finishPublish` `sp`/`drp` TLS statics read after `sendTo` yield in the `drDirect` block → concurrent publish swaps another client's property block into the stored record** (`buildRecord(*rec, ..., effProps, ...)` / `propsHeaders(effProps)` are the post-yield reads). Fix: guard or copy `effProps` before the direct-reply send.

---

# CTF flags — `kafka.d`

# GLM-CTF-REPORT — kafka.d pass (round 4)

## Flag 1 — HIGH — Cross-client disclosure: `handleDescribeGroups` reads TLS-static `rep` slices AFTER a yield-producing hop

**File:** `source/dreads/kafka.d`, `handleDescribeGroups` (member-emission path).

**Root cause:** The handler parses the coordinator reply from the shared TLS static `static ByteBuffer req, rep;` into an `Rd rr` whose string/bytes results (`mid`, `gii`, `cid`, `meta`, `assign`, and `proto`/`ptype`) are *slices into `rep`*. It then calls `groupExists(groups[i])` — an `HLEN` through `gKafkaExec`/`amqpDataExec`, which **yields** when the group's hash key (`kafka.cg.<group>`) is owned by another shard. During that park, any other Kafka connection served by the same shard thread that executes `DescribeGroups`/`Heartbeat`/`OffsetCommit` (all of which share this same `static ByteBuffer rep`) overwrites the buffer. On resume, the member loop emits the *other request's* bytes:

```d
immutable dead = nmemb == 0 && st == 0 && !groupExists(groups[i]); // <-- hop, park
...
putStr(o, mid);            // slice into rep — read AFTER the park
putStr(o, cid.length ? cid : mid);
putBytesI32(o, meta, false);   // another group's member metadata/assignment
putBytesI32(o, assign, false);
```

Scalar fields (`st`, `gen`, `nmemb`) are read before the park (value-copied), so the loop count is safe — but every emitted *member field* is post-hop-read TLS data. This is exactly the already-fixed OffsetFetch/MetadataFlex class (both of which got stack copies); DescribeGroups was missed.

**PoC (sharded mode, ≥2 shards):**
1. Conn A (shard 0 thread) sends `DescribeGroups` for group `gA` whose `kafka.cg.gA` hashes to shard 1 (so `kgOp` and the subsequent `groupExists` HLEN both hop).
2. Conn B on the **same shard-0 thread** floods `DescribeGroups` for group `gB` (also foreign-owned).
3. Interleave: A's `kgOp` reply for `gA` is parsed into `rep`; A then parks in `groupExists`'s HLEN hop; B's handler runs, its `kgOp` reply for `gB` lands in the *same* `rep`; A resumes and emits `gB`'s member ids, client ids, subscription metadata and **binpack assignments** to client A.

Result: consumer-group membership and partition assignments of a group the attacker never named are disclosed byte-for-byte. (Not memory-unsafe — bounded slices — but a proven cross-tenant disclosure; if `rep` is retrimmed smaller by B, `Rd` slices are bounded by `ok` so it degrades to truncation/garbage, still wrong-client data.)

**Fix:** copy the member fields to stack/arena buffers before calling `groupExists` (the EndTxn pattern: `char[256][64]` copies), or move the `groupExists` probe *before* the `kgOp` hop.

---

## Flag 2 — MED — ACL bypass: Metadata performs no topic DESCRIBE authorization

**File:** `handleMetadata` / `handleMetadataFlex`.

**Root cause:** With `gKafkaAclActive > 0`, every data API (Fetch/Produce) and admin API checks `authorize(...)`, but Metadata never calls `authorize(tKafkaCtx, KRES_TOPIC, t, KOP_DESCRIBE)`. Kafka's SimpleAclAuthorizer gates Metadata with DESCRIBE and returns `TOPIC_AUTHORIZATION_FAILED` (29) per denied topic.

**PoC:** Configure one ACL binding (any `CreateAcls` turns enforcement on, e.g. a single ALLOW for `User:alice` on topic `secret-*`). An unauthenticated (`ANONYMOUS`) connection sends `Metadata v1` with `topics = null`: the handler runs `HKEYS kafka.topics` and returns **every topic name in the cluster** plus its partition count, and named queries for `secret-x` return full partition metadata instead of error 29. This defeats topic-confidentiality ACLs (`secret-*` prefixed DENY) — the attacker learns exactly which secret topics exist and their partition layout.

**Fix:** in both metadata handlers, `authorize(tKafkaCtx, KRES_TOPIC, t, KOP_DESCRIBE)`; on denial emit the topic entry with `E_TOPIC_AUTH_FAILED` and zero partitions (and skip the topic in the all-topics enumeration).

---

## Flag 3 — MED — Data corruption: DeleteRecords' LTRIM + SET of the base offset is not atomic

**File:** `handleDeleteRecords`.

**Root cause:** truncation is two separate data-plane ops — `LTRIM key drop -1` then `SET kafka.tb.<t>.<p> want` — with `hw`/`base` read **before** both. Two concurrent DeleteRecords on the same partition interleave:

1. A and B both read `base=0, llen=100` (`hw=100`).
2. A: target 30 → LTRIM drops 30, SET base=30. ✓
3. B (stale `base=0`, target 10): `want=10 ≥ base(0)` passes validation, LTRIM drops **another** 10 (list now holds 60), SET base=**10** — a *regression* below the true start.

Afterwards `hw = base + llen = 10 + 60 = 70` while only 60 records exist; `partBase` cache on other shards holds the larger epoch-30 value until invalidated, then flips to 10. Consequences: Fetch/ListOffsets report offsets 10–29 as in-range although those records are deleted; the per-shard `tBaseVal` caches on shards that already cached 30 now disagree with the owner (`epoch` bumps only once, but two caches under different epochs can hold different values after the second bump races a reader). Producers' `baseOffset` replies from `pbase + newLen - nrec` become wrong. This is silent offset/label corruption of the log contract, not just a wrong reply.

**Fix:** make the truncation a single owner-shard-atomic op (e.g. a small `ShardMsg`/script that does LLEN+LTRIM+SET back-to-back without yielding), or serialize per-partition via a coordinator FSM op like the group path, and validate `want >= currentBase` *on the owner at apply time*, not on the requester.

---

## SUSPECT (unproven)

- **`kafkaScramStep` server-first build into `char[192] sf`** (`sf[o .. o + saltB.length]`): bounded only if `au.scramSalt`'s b64 ≤ ~44 chars. If the ACL layer ever stores a longer salt, this is a stack overflow. Confirm `AclUser.scramSalt`'s fixed size; if ≤ 32 bytes it's safe.
- **Unbounded group id length** in Join/OffsetCommit/Fetch (`kafka.cg.<group>` with group up to 32 767 bytes): only DeleteGroups/OffsetDelete enforce ≤249. Key-length DoS / registry pollution — **DoS-hardening-backlog**, not a flag.
- **Metadata hop storm** on all-topics with many foreign-owned partitions: budgeted (`KAFKA_META_PROBE_BUDGET`) — fine; noting only.
- `handleEndTxn` ~41 KB of stack arrays per call; fine on default fiber stacks but worth watching if stack sizes shrink.

## Ranked summary

1. HIGH kafka.d `handleDescribeGroups` — TLS `rep` member slices read after `groupExists` hop → cross-client group-membership/assignment disclosure (stack-copy fix).
2. MED kafka.d `handleMetadata`/`handleMetadataFlex` — no KRES_TOPIC DESCRIBE ACL → anonymous full topic enumeration under active ACLs.
3. MED kafka.d `handleDeleteRecords` — non-atomic LTRIM+SET base → log-start-offset regression and offset mislabeling under concurrent truncation.
4. SUSPECT kafkaScramStep `sf[192]` sizing vs. stored salt length (needs `AclUser.scramSalt` size to confirm).
