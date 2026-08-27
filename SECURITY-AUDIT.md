# Security & correctness audit — triage matrix

Operational source of truth for the adversarial review campaign run on the skin
modules (2026-08-25/27). The raw reports are kept as evidence and are **not**
the tracking record — this file is. Each finding has a stable ID, a status, and,
where it was fixed, the commit and the test that would catch a regression.

Reports: [CODEX-REVIEW.md](audit/reports/CODEX-REVIEW.md), [CODEX-REVIEW-FINAL.md](audit/reports/CODEX-REVIEW-FINAL.md),
[CODEX-REVIEW-R9.md](audit/reports/CODEX-REVIEW-R9.md), [GLM-CTF-REPORT.md](audit/reports/GLM-CTF-REPORT.md)
and `GLM-CTF-REPORT-R2..R9.md`. Product-level critique: [PROJECT-CRITIQUE.md](audit/reports/PROJECT-CRITIQUE.md).

## Method

Two independent adversarial reviewers over nine passes: **codex `gpt-5.6-sol`**
(rounds 1, cross-check, 9) and **GLM-5.3** (rounds 1–9). Every finding was
re-verified against the source before any fix — **roughly half were refuted**,
several of them repeatedly. A refutation is recorded here with the reason,
because "we looked and it does not hold" is a result worth keeping: it stops the
same claim from being re-litigated every pass.

Standing rules applied throughout: **extend-only** (no existing RESP, keyspace or
sharding semantics changed) and **no RESP throughput regression** (interleaved A/B
on the first round, in-band sanity every round after).

Statuses: **fixed** · **refuted** (proven not to hold) · **by-design** ·
**deferred** (real, needs a design change) · **backlog** (resource limit, not a
correctness defect).

## Severity legend

`CRITICAL` memory corruption or authentication bypass · `HIGH` cross-client
disclosure, data corruption, authorization bypass, or shard-wide denial of
service · `MED` protocol/contract defect or bounded denial of service · `LOW`
cosmetic or self-inflicted only.

---

## Fixed

| ID | Origin | Sev | Finding | Commit | Regression test |
|---|---|---|---|---|---|
| BOOT-001 | own | HIGH | `kafkaAclPrime` did a blocking data-plane hop during boot, before the shard workers and the event loop existed — deadlocked every skin under sharding | `5287979` | `test/run-conformance.sh` (boots all five skins together) |
| DB-001 | own | HIGH | `NUM_DBS=19` did not cover the four skin DBs; SQS (db 19) crashed on **every** operation | earlier on branch | `test/conformance/sqs_conformance.py` |
| R1-AMQP-01 | codex+GLM | CRITICAL | No handshake gate: a client skipping start-ok reached `queue.declare`/`basic.publish`/`consume` with an ACL configured | `cb17c8d` | pika suite |
| R1-AMQP-02 | codex | HIGH | `fsize + 8 > frameMax` evaluated in 32-bit — wraps, defeating frame-max → unbounded buffering | `cb17c8d` | — |
| R1-KAFKA-01 | GLM | CRITICAL | PLAIN accept-any recorded the *client-claimed* principal → claim `admin`, become super-user | `cb17c8d` | — |
| R1-KAFKA-02 | GLM | HIGH | 12 admin/group handlers had no `authorize()` (DeleteTopics, CreateAcls self-grant, OffsetCommit…) | `cb17c8d` | golib suite (no-ACL no-op path) |
| R1-KAFKA-03 | codex | HIGH | OffsetFetch (classic+flex) read a TLS metadata buffer **after** the epoch hop → other client's committed metadata | `cb17c8d` | — |
| R1-KAFKA-04 | codex | HIGH | `handleMetadataFlex` sliced a TLS `allBuf` across `registerTopic` parks → registry corruption | `cb17c8d` | — |
| R1-AMQP10-01 | GLM | HIGH | Bare `AMQP\x00\x01\x00\x00` header skipped SASL entirely with an ACL configured | `cb17c8d` | — |
| R1-AMQP10-02 | GLM | HIGH | `DISPOSITION first=0,last=ulong.max-1` iterated the numeric range → single frame wedged the shard | `cb17c8d` | — |
| R1-MQTT-01 | codex+GLM | HIGH | Will never fired on non-park teardown: the guard tested a flag cleared three statements earlier — dead-man's-switch was dead | `cb17c8d` | Paho suite |
| R1-SQS-01..07 | codex+GLM | HIGH | Seven defects incl. stale record echoed to the client, body copied across a park, batch-delete never released the FIFO group lock, `VisibilityTimeout*1000` overflow, 64 KiB silent truncation, u16 sweep length | `cb17c8d` | `sqs_conformance.py` |
| R2-AMQP10-01 | own | HIGH | **Regression we introduced in R1**: the disposition fix used a thread-shared `static ulong[] dispIds` iterated across a hop | `ae27b22` | — |
| R2-SQS-01 | GLM | CRITICAL | R1 copied only the selected record; the FIFO scan still re-read `snap`/`recs` after every park → cross-queue disclosure | `ae27b22` | `sqs_conformance.py` |
| R2-KAFKA-01 | GLM | MED | Group id >249 truncated into fixed stack copies → silent no-op or cross-group aliasing | `ae27b22` | — |
| R2-MQTT-01 | GLM | HIGH | Will topic never ACL-checked at CONNECT → publish into a denied channel on disconnect (retained, if set) | `ae27b22` | — |
| R2-AMQP-01 | own | HIGH | Per-operation ACL on the data plane (publish/get/consume), authorizing exchange+routing-key behind an `aclUserCount()<=1` fast path | `ae27b22` | pika suite |
| R3-AMQP-01 | GLM | HIGH | CC/BCC header routing keys bypassed the R2 per-op ACL on the default exchange | `6c44e58` | — |
| R3-AMQP-02 | GLM | HIGH | `amqpTopicMatches` backtracked exponentially on `#.#.#…` — one publish froze the shard | `6c44e58` | the 11 topic-match unit tests |
| R3-AMQP-03 | GLM | MED | `gDrSecret` seeded from `monoMs ^ pointer` → forgeable direct-reply tokens. Now CSPRNG under a CAS | `6c44e58` | — |
| R3-MQTT-01 | GLM | HIGH | WebSocket `sendTo` used a thread-shared `static wb` held across a write yield → cross-client byte disclosure | `6c44e58` | — |
| R3-KGRP-01 | GLM | HIGH | `KGOP_TXN_ADD` dedup was O(N) per insert → O(N²) per request, stalling the never-yielding drain | `6c44e58` | — |
| R3-SQS-01 | GLM | MED | `dedupSeen` echoed an **uninitialized stack** `mid` as MessageId | `6c44e58` | — |
| R3-SQS-02 | GLM | MED | `findKey` was a naive substring scan → field spoofing from body/attribute values | `6c44e58` | `sqs_conformance.py` |
| R4-MQTT-01 | GLM | CRITICAL | WebSocket **handshake** used thread-shared `reqbuf`/`respbuf` across the 30 s pre-CONNECT yields → A's handshake computed over B's headers, B's pipelined CONNECT fed into A's codec | `cddb1ce` | — |
| R4-AMQP10-01 | GLM | HIGH | Described-type decoder recursed with no bound → stack overflow from one frame (open since R1) | `cddb1ce` | — |
| R4-AMQP10-02 | GLM | MED | `snprintf` return used as a slice length (can exceed the buffer) → OOB read | `cddb1ce` | — |
| R4-AMQP-01 | GLM | HIGH | `queue.bind/unbind/purge/delete` and `exchange.delete` had no per-op ACL | `cddb1ce` | — |
| R4-AMQP-02 | GLM | HIGH | `finishPublish` read TLS `sp`/`drp` after the direct-reply `sendTo` yield → property block swapped between clients | `cddb1ce` | — |
| R4-KGRP-01 | GLM | MED | Producer id was a sequential counter → guessable, forge another producer's txn. Now CSPRNG | `cddb1ce` | — |
| R4-KAFKA-01 | GLM | MED | `DeleteRecords` LTRIM+SET was not atomic → offset/highwater corruption. Forward-only base guard | `cddb1ce` | — |
| R5-KAFKA-01 | GLM | HIGH | **Cross-tenant authorization TOCTOU**: all 33 `authorize()` sites read the thread-local `tKafkaCtx`, which a sibling request reassigns during a park → the resumed handler authorized against another principal | `288ec15` | golib suite |
| R5-AMQP10-01 | GLM | HIGH | Disposition modified-state annotations sliced the shared frame buffer and were re-parsed across the settle loop's hops | `288ec15` | — |
| R5-AMQP-01 | GLM | HIGH | `a10Publish` re-read an unguarded `static` record slice **per routed queue** across the ack-waiting park → fan-out delivered another client's body | `288ec15` | — |
| R6-KAFKA-01 | GLM | CRITICAL | **Heap OOB write**: the Fetch records-size backpatch was a raw 4-byte write that skipped `patchI32`'s bounds check; on the allocation-failure path it wrote past the buffer | `bca2c00` | — |
| R6-AMQP-01 | GLM | HIGH | `basic.get` read the popped record from TLS `pay` **after** the `gAmqpLen` hop → wrong client's body and wrong unacked entry | `bca2c00` | pika suite |
| R6-AMQP10-01 | GLM | MED | Delivery fiber recorded the popped message into `unsettled` only after the park (teardown lost it) and dereferenced a stale `A10Link*` after a concurrent detach → UAF | `bca2c00` | — |
| CX-AMQP10-01 | codex | HIGH | `a10ReadFrame` assembled frames larger than one TCP segment **across parks** into a thread-shared `static buf` → cross-client frame injection on any fragmented frame | `b0678e0` | — |
| CX-KAFKA-01 | codex | HIGH | JoinGroup staged into a function-`static req` across `registerGroupName`'s park → this connection submitted a **sibling's** join payload | `b0678e0` | golib suite |
| CX-KAFKA-02 | codex | MED | Five more per-request thread-locals read after a park (client-id, allow-auto, adv-port, two probe budgets) | `b0678e0` | — |
| CX-SQS-01 | codex | HIGH | `ifkey` was function-`static` and re-read after `fifoUnlock`'s park → DeleteMessage HDEL'd the **wrong queue**; the acknowledged delete silently no-op'd | `b0678e0` | `sqs_conformance.py` |
| CX-SQS-02 | codex | MED | `jsonEachEntry` sliced past the request end on a trailing backslash → reachable `RangeError` (remote crash on a malformed body) | `b0678e0` | — |
| CX-SQS-03 | codex | MED | `Content-Length` parsed with no overflow check → a wrapped value slipped under the 4 MiB cap | `b0678e0` | — |
| R9-CFG-01 | codex+GLM | HIGH | `sqs-db` accepted 0..255 while its three siblings reject >18; the value indexes `gShardKs[tShard*NUM_DBS + db]` on every SQS op | `a7b8152` | `config.d` unittest |
| R9-CFG-02 | GLM | MED | `applyDirective` switched on the lowercased name but compared the original → `--AMQP-DB=5` silently configured `kafkaDb` | `a7b8152` | `config.d` unittest |
| R9-CFG-03 | codex | MED | `parseMemory` multiplied without an overflow check → `18014398509481984kb` wrapped to 0, reporting success | `a7b8152` | `config.d` unittest |
| R9-CFG-04 | **own** | MED | `parseMemory` took `out ulong` — zeroed on entry, so a **rejected** value wiped the field: the log said "ignoring invalid option" while `proto-max-bulk-len` (512 MB) became 0. Found by writing the regression test for R9-CFG-03; neither reviewer saw it | `a7b8152` | `config.d` unittest |
| R9-KAFKA-01 | codex | MED | `tKafkaDecompUsed` read after a park → a sibling Produce reset or inflated this request's 512 MiB decompression budget | `a7b8152` | — |
| R9-KAFKA-02 | codex | MED | Compact uvarint accepted a wrapping 5th byte (`<<28` in `uint`) — malformed frame became a valid length | `a7b8152` | varint round-trip tests |
| R9-KAFKA-03 | codex | MED | Record uvarint accepted a wrapping 10th byte (`<<63` in `ulong`) | `a7b8152` | varint round-trip tests |
| R9-MQTT-01 | GLM | HIGH | `takeoverLocal` wrote to a socket **from the drain fiber**; with the victim's writer parked in a stalled `tcp.write` holding `wlock`, the shard stopped draining and all cross-shard traffic hung | `a7b8152` | — |

## Deferred — real, needs a design change

| ID | Origin | Sev | Finding | Why not patched | Marker |
|---|---|---|---|---|---|
| D-MQTT-01 | codex+GLM | HIGH | Cross-shard session adopt has a **fixed-time** 1 s redirect window with no ownership handshake; an adopter descheduled past it reads a released `obox` (UAF read) | Needs an ownership/refcount handshake. A longer timeout narrows the window without closing it, and a rushed lock here risks a worse deadlock | `TODO(xshard-adopt-lifetime)` |
| D-SQS-01 | GLM | HIGH | FIFO receive is a non-atomic check-then-act across hops (snapshot → group check → remove → lock), so two receivers can take the same message; `dedupSeen`/`dedupStore` is the same family | Needs an owner-side atomic primitive. A lock held across a fiber park would stall the shard — the exact failure R9-MQTT-01 turned out to be | `TODO(FIFO-atomic)` |
| D-KAFKA-01 | GLM | MED | `DeleteRecords` still has a narrow same-window cross-shard race after the forward-only guard | Needs an owner-side atomic-max; the list key and base key are different families and share no slot, so MULTI/EXEC cannot cover it | noted in `cddb1ce` |
| D-KAFKA-02 | measured | MED | Admin API response encoding: `CreateTopics` returns a result librdkafka parses as NULL; `CreateAcls` v1 fails to parse. **Not on the produce/consume path** | Bounded work, not research — tracked with a dated measurement | `conformance/kafka-librdkafka/result-2026-08-26.txt`, README roadmap |
| D-AMQP10-01 | codex | LOW | Stream branch `streamPos++` after the `a10PeekAt` park (the peek removes nothing, so no message loss) | Same family as the fixed delivery-fiber re-validation; low impact | noted in `b0678e0` |

## Refuted — checked, does not hold

Kept so the same claim is not re-litigated. Each was traced to the line.

| Claim | Raised by | Why it does not hold |
|---|---|---|
| TLS buffer passed **as an argument** to a cross-shard hop is clobbered during the park | both, many rounds | `amqpDataExec`/`gKafkaExec`/`gAmqpPush` copy all args into `raw` and a **stack-local** hop frame *before* any yield; the park happens after the enqueue. Only a **post-hop read** of a shared static is a defect |
| kafka `handleProduce` `kb`/`blobArena`/`slices` | GLM (3×) | The above; the code even comments it (`kb // consumed into raw before any hop yield`) |
| kafka `handleDescribeGroups` `rep` | GLM (2×) | The only in-body hop is `groupExists`, reached **only** when `nmemb==0` (short-circuit); in that branch the reads emit literals, not `rep` slices |
| kafka `topicPartitionCount` TLS key | codex-round | The key is never read after the `partLen` hop |
| mqtt `fwdProps` clobbered by `shardEnqueue`'s backpressure yield | GLM (2×) | `shardMqttFanout` copies props into its staging buffer *before* the enqueue, and `shardEnqueue` re-reads `buf.data` (the copy), never `fwdProps`; `mbBusy` routes a reentrant fanout to a stack buffer |
| mqtt `wsread` aliases between two parked WS reads | GLM | `feed()` is `@nogc nothrow` (cannot yield), and vibe's `read(dst, IOMode.once)` fills `dst` **synchronously on the resumed turn** — the park is inside `waitForData` on the connection's *own* internal buffer. Two parked reads have disjoint async destinations |
| amqp `settleNegative`/`requeueAllUnacked` `kb4`/`rq4` | GLM | Args-copied-before-yield |
| amqp10 `a10Requeue` TLS | GLM | Args-copied-before-yield |
| sqs `\x1f` in the body truncates the record | GLM | `splitRecord` bounds the split to the first three separators; the body is the remainder, embedded `\x1f` included |
| sqs visibility-sweep statics | GLM | Function-local `static`s are per-function symbols and one timer per shard — no concurrent sweep |
| kafkagroup `wStr16` truncates at 64 KiB | GLM | `str16` already bounds every decoded string to ≤0xFFFF; the branch is unreachable |
| kafkagroup `TXN_END` partition reparse overflows | GLM | The value round-trips its own `%d`; it came from `r.i32()` |
| kafka classic `handleMetadata` adv-port read after a park | GLM | The broker block is emitted **before** any hop (the per-topic hops are later in the loop). Defensive capture applied anyway |
| SCRAM `b64enc(scramSalt, char[32])` stack overflow (the only RCE lead) | GLM | `scramSalt` is `ubyte[16]` → 24 base64 chars, fits |

## By design

| Claim | Why it stands |
|---|---|
| AMQP publisher confirms are sent before the owner shard applies/persists | Deliberate. A confirm means "the bytes are copied into the owner's ring", and the owner drains FIFO in-process; the loss window is microseconds and lies **inside** the everysec-AOF window the confirm already accepted. Removing it reinstates the ack round-trip that caused the retrograde scaling. Graceful shutdown drains the rings and flushes, so a clean stop honours every confirm |
| SQS has no SigV4 / accepts any credentials | Documented v1 scope, like local emulators |
| Legacy accept-any auth while only the seeded `default` ACL user exists | Intentional. Every gate added is conditioned on an ACL actually being configured, so the default install is unchanged |
| Kafka `LeaveGroup` does not fence on a generation | The wire request carries no `generationId`; requiring one would change the protocol. A membership check is present |
| Kafka transactional-id "takeover" | KIP-98 producer epochs already fence it: re-init bumps the epoch and every txn op rejects a stale one with code 47 |

## Backlog — resource limits, not correctness defects

Both reviewers listed these separately; none is a memory-safety or disclosure
defect. Unbounded input for an authenticated or operator-reachable path.

- `amqp10`: management argument/type maps are never pruned on queue delete.
- `kafka`: group id length is bounded only in DeleteGroups/OffsetDelete, so
  `kafka.cg.<up to 32 KiB>` keys are reachable elsewhere; per-member protocol
  metadata has no aggregate byte budget; all-topics Metadata materializes the
  whole registry reply before keeping 512 names.
- `sqs`: no queue-registry cardinality cap; ListQueues and the sweep materialize
  the full registry; FIFO receive does `LRANGE 0 -1` and keeps 1024 records, so
  deeper queues under-deliver (behavioural) and the walk is unbounded.
- `config`: `loadConfig` reads the config file with no size ceiling.
- `mqtt`: protocol-error close paths pin a fiber and ~512 B each, bounded by
  connection count.

## What this campaign did not establish

Stated plainly, because the matrix above can read as more assurance than it is:

- **No external audit.** Every finding here was raised and fixed in-house
  (with AI reviewers); a buyer should discount self-administered assurance.
- **No crash-consistency automation.** `kill -9` recovery has been exercised by
  hand, not continuously, and no test asserts that an acknowledged write always
  survives. This is the weakest link in the durability story and is named as
  such in the README roadmap.
- **Not every fix has a regression test.** The table says so per row. The
  conformance suites cover the protocol-visible ones; the concurrency fixes
  (post-hop reads, fiber interleavings) largely do not have targeted tests,
  because reproducing them needs a deterministic fiber scheduler.
