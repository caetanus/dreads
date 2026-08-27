# Correctness-hardening review

Scope: `amqp.d`, `amqp10.d`, `mqtt.d`, `kafka.d`, `kafkagroup.d`, `sqs.d`, and the relevant paths in `server.d`, `shard.d`, `raftq.d`, and `replicator.d`. Findings are ordered by severity. Every item below has a concrete failure trace; no unproved suspicions are included.

### 1. `source/dreads/replicator.d:178-181,272-298,370-389`; `source/dreads/raftq.d:33-42,53-64`

**Claim:** Raft `Pending` objects can be collected while their only references are in malloc/calloc memory, causing a use-after-free.

**Failure trace:** A sharded write calls `proposeForClient`, allocates `new Pending`, and puts its address into `propQ` as a `void*` tag. `CrossQueue` stores that tag in a `Slot` array allocated by C `calloc`, which the D GC does not scan. There is a pre-consumption interval in which no guaranteed GC root retains the object. After completion, `releaseSlot` puts the pointer into `Vec!(Pending*) freeSlots`; that vector uses `Mallocator`, so an idle pooled object again has no GC-scanned owner. A collection can reclaim the `Pending` and its `LocalManualEvent`. The next `acquireSlot` pops the dangling pointer and writes `ready`, `reply`, and `reqBuf`, or the Raft thread dereferences it at proposal/commit time, producing heap corruption, a crash, or a reply through a freed event. The shard pending pool explicitly solves the same problem with the GC-rooting `tPendAll` array at `shard.d:288-317`; the Raft pool has no equivalent.

**Severity:** CRITICAL

### 2. `source/dreads/raftq.d:12-17,101-115`; `source/dreads/replicator.d:404-426`; `source/dreads/server.d:8085-8115`

**Claim:** Every shard can produce expiry writes into a queue implemented for exactly one producer, so concurrent expiry corrupts or loses Raft proposals.

**Failure trace:** With `--shards 2` and Raft enabled, put an already-expired key on each shard and cause both shard threads to reap it concurrently. Both enter `expireReap` and call the single global `gReplicator.proposeServerWrite`. Both calls execute `propQ.tryPut`, although `RingCore.push` is explicitly SPSC: each producer raw-loads the same tail `T`, clears/appends the same slot buffer, and release-stores `T+1` without a CAS. One DEL overwrites the other (or the two concurrent `ByteBuffer` mutations corrupt its allocation), while both callers can receive `true` and delete their local key. Only one DEL is then replicated, leaving a follower with the other key. `lastClock` at `replicator.d:412-413` is also concurrently read and written without synchronization.

**Severity:** CRITICAL

### 3. `source/dreads/sqs.d:250-281,292-319,392-492,510-547,949-994`; `source/dreads/server.d:2851-2950`

**Claim:** SQS retains slices into reused TLS buffers across yielding cross-shard operations, so concurrent fibers overwrite live request bodies, keys, snapshots, and receipt state.

**Failure trace:** One concrete SendMessage interleaving is: request A parses FIFO body `"alpha"`; `jsonStr` returns a slice into the sole `static char[65536] ub`; A calls `dedupSeen`, whose HGET routes to another shard and parks in `amqpDataExec`; request B on the same thread parses body `"beta"` and overwrites `ub`; A resumes after a dedup miss and `sendOne` hashes and stores `"beta"` under A's request while replying success. A deletion trace is similar: FIFO DeleteMessage A builds the static `ifkey`, parks in its HGET, DeleteMessage B rewrites the same `ifkey`, and A resumes to issue its final HDEL with B's key, returning `{}` while A's message remains in flight. ReceiveMessage additionally retains static `qkey`, `ifkey`, `grpkey`, `snap`, and `recs` across several yielding calls; an interleaved receive can make A parse one queue's snapshot but LREM/HSET another queue, corrupting queue and visibility state.

**Severity:** CRITICAL

### 4. `source/dreads/config.d:333-340`; `source/dreads/obj.d:140-143`; `source/dreads/shard.d:73-76`; `source/dreads/server.d:2851-2889`

**Claim:** `sqs-db` accepts indices 20 through 255 even though only 20 databases exist, leading to bounds failure or cross-shard keyspace corruption.

**Failure trace:** Start with `sqs-db 20`. Configuration accepts it because it checks only `n < 256`, while valid indices are `0..19`. In standalone mode, the first SQS data operation reaches `&gDbs[20]`, which either trips a bounds check or indexes beyond the array in release code. With two shards, shard 0's unchecked `myKeyspace(20)` computes `gShardKs[20]`, which is actually shard 1 DB 0. Shard 0 then mutates shard 1's RESP keyspace directly and concurrently, violating share-nothing ownership; sufficiently large values or the last shard go out of the allocation entirely.

**Severity:** CRITICAL

### 5. `source/dreads/amqp10.d:118-131,310-321,630-631,803-821,4143-4164`

**Claim:** An unauthenticated AMQP 1.0 frame can force unbounded decoder recursion and exhaust the process stack.

**Failure trace:** Connect with the bare AMQP 1.0 header, then send a valid frame header whose body contains tens of thousands of `0x00` descriptor constructors followed by any terminal value. `a10Performative` calls `readValue`; every `0x00` recursively calls `readValue` with no depth bound. The accepted frame maximum is 1 MiB, enough for far more calls than a normal thread stack supports. Stack exhaustion occurs before the malformed performative can be rejected, crashing the serving thread/process.

**Severity:** CRITICAL

### 6. `source/dreads/obj.d:110-117`; `source/dreads/replicator.d:558-577`; `source/dreads/server.d:2455-2481,3614-3681,8085-8112`

**Claim:** `__gshared bool gApplying` is a racy process-wide nesting flag used by concurrent shard applies, so expiry can be replicated with the wrong semantics.

**Failure trace:** A committed command is fire-and-forget routed to owner shard B, where the handler sets `gApplying=true` and begins dispatch. Concurrently, shard 0 or another owner applies a second committed command, sets the same flag, completes first, and writes `false` while B is still inside dispatch. If B's command looks up a key that is expired at the injected clock, `expireReap` now sees false and proposes a fresh DEL instead of reaping deterministically inside the current log entry. For `INCR` on an expired key, B can store `1`, acknowledge it, and later apply that extra DEL ordered after the INCR, deleting the newly written value. The unsynchronized reads/writes are themselves a D data race; a single Boolean cannot represent overlapping apply scopes.

**Severity:** CRITICAL

### 7. `source/dreads/server.d:2292-2303,2614-2630,3716-3744,3753-3782`

**Claim:** Cross-shard command and reply codecs dereference misaligned `uint*` and `ulong*` pointers, invoking undefined behavior on every 64-bit hop header and on variably aligned later sections.

**Failure trace:** `appendHopCmd` reserves byte storage and stores an eight-byte pending pointer at `space.ptr + 4`; even when the allocation base is aligned, that address is only four-byte aligned. The owner reads it through `cast(const(ulong)*) sec.ptr`. Reply batching repeats the same layout at `server.d:2299-2300` and reads it at `2620`. A preceding variable-sized section can also leave the next `uint` section header unaligned. On an architecture enforcing alignment, the first cross-shard command/reply traps; on other targets the typed dereference violates the compiler's alignment assumptions and is undefined behavior.

**Severity:** HIGH

### 8. `source/dreads/amqp.d:4464-4473,4531-4539,2300-2328`; `source/dreads/server.d:3036-3072,2521-2533,3268`

**Claim:** AMQP publisher confirms are emitted before a remote queue owner applies or logs the publish, so a confirmed message can be lost on process failure.

**Failure trace:** A publisher on shard A publishes to a queue whose list key belongs to shard B with confirms enabled. `amqpPushStage` copies the RPUSH payload into B's volatile SPSC ring and returns without a pending/ack. `finishPublish` immediately appends `basic.ack`; the flush before socket send calls `myAof().flush()` on publisher shard A only. Kill the process after the client receives the ack but before B drains `ShardMsg.amqpPush`: B never mutates the list and never appends the command to its AOF. On restart the broker has lost a message it positively confirmed. Ring backpressure prevents overflow, but it does not make the copied ring slot durable or applied.

**Severity:** HIGH

### 9. `source/dreads/mqtt.d:2189-2209,2319-2327,2380-2385`

**Claim:** Immediate MQTT Wills never fire for non-persistent/non-parked sessions because teardown clears `connected` before testing it.

**Failure trace:** A clean-session MQTT 3.1.1 client CONNECTs with a Will topic and payload, then its TCP connection resets. With session expiry zero and Will Delay zero, `mqttParkOrEnd` returns false and comments that teardown will fire the Will. `mqttTeardown` first observes `connected`, decrements the metric, and sets it false. It later executes `if (c.connected) fireWill(c)`, which can no longer be true. Subscribers never receive the Will. MQTT 5 DISCONNECT reason `0x04` (Disconnect with Will Message) follows the same non-parked path and is also suppressed.

**Severity:** HIGH

### 10. `source/dreads/mqtt.d:825-860,2031-2070,3532-3549`

**Claim:** MQTT persistence stores only an existence marker but reports the full session as resumed after restart, losing subscriptions and QoS state while setting Session Present.

**Failure trace:** A persistent client subscribes at QoS 1, receives an unacknowledged publish, and disconnects. The keyspace/AOF persists only `mqtt.sess.<id> = "1"`; subscriptions, offline outbox, packet IDs, and QoS 1/2 maps live solely in the parked `MqttConn`. Restart destroys that object and the topic trie but reloads the marker. A `clean_start=0` reconnect finds no parked object, `mqttSessionExists` returns true, and CONNACK sets Session Present. `mqttAdoptState` never runs, so the old subscription and unacknowledged delivery are absent. The client is told its session survived while its required state and messages were lost.

**Severity:** HIGH

### 11. `source/dreads/sqs.d:404-466,480-492`

**Claim:** FIFO receive performs eligibility, removal, group locking, and in-flight insertion as separate commands and ignores a failed LREM, allowing duplicate concurrent delivery.

**Failure trace:** Put one FIFO message M in group G. Receivers R1 and R2 on the same shard both snapshot M with LRANGE, then interleave at the yielding `SISMEMBER`; both observe G unlocked. R1 executes LREM and removes M. R2 executes LREM and gets zero, but the result is ignored. Both then SADD G, create different receipt handles, HSET M into the in-flight hash, and return M successfully. One FIFO message is simultaneously delivered twice despite the per-group single-in-flight guarantee.

**Severity:** HIGH

### 12. `source/dreads/sqs.d:510-579,675-735`

**Claim:** `DeleteMessageBatch` deletes FIFO in-flight entries without releasing their group locks, permanently blocking later messages in those groups.

**Failure trace:** Queue contains M1 then M2 in group G. Receive M1, which adds G to `sqs.grp.<queue>` and stores M1 in the in-flight hash. Delete M1 through `DeleteMessageBatch`: the batch path only HDELs the receipt handle, unlike single DeleteMessage which HGETs the record and SREMs G. Future receives skip M2 because G remains locked. The visibility sweep cannot repair it because the HDEL removed the only record from which the sweep could recover G. M2 is blocked indefinitely.

**Severity:** HIGH

### 13. `source/dreads/sqs.d:602-621,675-724,738-785,949-994`

**Claim:** SQS visibility/delay sweeps encode record lengths in 16 bits even though accepted records exceed 65,535 bytes, truncating messages during promotion/redelivery.

**Failure trace:** Send a 65,536-byte body. `jsonStr` returns all 65,536 bytes, and the internal record adds 67 bytes for message id, MD5, separators, and an empty group, for length 65,603 (`0x10043`). When visibility expires, the sweep writes only the low two length bytes (`0x0043`) before the full record. On unpack it LPUSHes 67 bytes—metadata plus an empty body—and HDELs the original in-flight value. The body is irreversibly lost. The delayed-message sweep uses the same packing and corrupts the same input when delay expires.

**Severity:** HIGH

### 14. `source/dreads/sqs.d:250-287,949-994`

**Claim:** SQS silently truncates every decoded JSON string, including MessageBody, at 65,536 bytes and still returns success.

**Failure trace:** SendMessage supplies a valid 70 KiB JSON body. `jsonStr` continues scanning the JSON after its static buffer fills but stops copying at byte 65,536, then returns that prefix without an error flag. `sendOne` hashes and stores the prefix and the handler returns a successful MessageId/MD5 response. ReceiveMessage returns only the prefix, causing acknowledged data loss and an MD5 that does not describe the submitted body.

**Severity:** HIGH

### 15. `source/dreads/sqs.d:582-598,622-645,738-785`

**Claim:** PurgeQueue omits delayed-message and FIFO-group keys, so purged messages can reappear and purged group locks can wedge future traffic.

**Failure trace:** Send a standard-queue message with `DelaySeconds=60`, then call PurgeQueue. Purge deletes only the ready list and in-flight hash, leaving `sqs.dl.<queue>`. At its deadline the sweep reads that hash, RPUSHes the record into the supposedly purged queue, and the message is delivered. Separately, purge a FIFO queue while group G is in flight: the in-flight hash disappears but `sqs.grp.<queue>` remains, so a later message in G is skipped forever and no remaining record can unlock it.

**Severity:** HIGH

### 16. `source/dreads/kafkagroup.d:794-821`; `source/dreads/kafka.d:2493-2502,5300-5368,5400-5405,5625-5638`

**Claim:** Kafka's producer epoch fencing is coordinator-local and is not enforced by Produce or AddOffsetsToTxn, allowing a fenced zombie producer to keep writing.

**Failure trace:** Producer P1 calls InitProducerId for transactional id T and gets producer id X, epoch 0. P2 reinitializes T and gets X, epoch 1, which is intended to fence P1. P1 then sends a transactional Produce with X/epoch 0 to a partition it has not written before. Produce discards `transactional_id`; `pidCheck` finds no per-partition hash field and returns success, so the record is appended and acknowledged. Even on an old partition, its stored epoch remains 0 until epoch 1 writes there. P1 can also call AddOffsetsToTxn, whose implementation discards transactional id, producer id, and epoch and unconditionally returns `E_NONE`.

**Severity:** HIGH

### 17. `source/dreads/kafkagroup.d:96-107,794-817,1038-1056`; `source/dreads/kafka.d:6805-6878`

**Claim:** Kafka accepts transaction timeouts but never records or sweeps them, so an abandoned transaction can pin a partition's Last Stable Offset forever.

**Failure trace:** Initialize T with a 100 ms timeout, add partition `p`, produce a transactional record at offset 10, then terminate the producer without EndTxn. `KgTxn` has no start/deadline/timeout field; InitProducerId only checks the maximum and discards the value. `kgroupSweep` visits only consumer groups and even returns early when `tGroups` is empty. The produce path persists `txn:<pid> = 10`, and `computeLso` treats every such field as an open transaction without age. Therefore read-committed fetches remain capped at offset 10 indefinitely, hiding all later committed data. After restart the TLS `tTxns` coordinator state is gone while the persisted open marker remains, so the old transaction cannot be ended through its coordinator.

**Severity:** HIGH

### 18. `source/dreads/amqp.d:251-315,364,874-911,2708-2714,2950-3005`; `source/dreads/server.d:3317-3328`

**Claim:** AMQP durable exchanges, queues, and bindings are only TLS control-plane objects and are not persisted, so restart destroys durable topology.

**Failure trace:** Declare durable exchange X and durable queue Q, bind Q to X, publish a persistent message, and enable AOF. Declaration/binding mutates TLS associative arrays and fans bytecode to live shards, but no topology command or object is written to the keyspace/AOF; the durable bits are merely fields in those arrays. Restart reloads queue list records but all `gExchanges`, `gQueues`, metadata, and `gBindings` instances start empty. Passive declaration of X or Q returns not-found, routing via X no longer reaches Q, and the durable topology promised by declare-ok has disappeared.

**Severity:** HIGH

### 19. `source/dreads/amqp.d:874-911,2708-2714,2950-3005`; `source/dreads/server.d:3317-3328`

**Claim:** AMQP declare-ok/bind-ok is returned before other shards apply the topology update, exposing acknowledged topology as temporarily nonexistent.

**Failure trace:** With two shards, client A declares exchange X on shard 0. `ctlBroadcast` applies locally and copies a control record to shard 1's ring without a pending reply, then the handler immediately emits declare-ok. Before shard 1's drain fiber processes that ring, an already-connected client B scheduled on shard 1 publishes to X. B's TLS `gExchanges` has no X and closes the channel with NOT_FOUND (or, for a delayed binding update, returns an otherwise routable mandatory message as NO_ROUTE). The operation acknowledged to A is therefore not yet globally usable.

**Severity:** HIGH

### 20. `source/dreads/sqs.d:250-281,343-373`

**Claim:** SendMessageBatch bypasses FIFO group and deduplication validation that the single-message path enforces.

**Failure trace:** Create `orders.fifo`, then issue SendMessageBatch with an entry lacking MessageGroupId. Single SendMessage rejects this at `sqs.d:258-259`; the batch path accepts it, stores group `""`, and reports the entry in `Successful`. Send two batch entries (or repeat the request) with the same MessageDeduplicationId: the batch path never extracts that field and never calls `dedupSeen`/`dedupStore`, so both copies are enqueued. FIFO required grouping and five-minute deduplication are both bypassed.

**Severity:** MED

### 21. `source/dreads/amqp.d:2367-2390`

**Claim:** AMQP 0-9-1 accepts body-frame overshoot and stores bytes beyond the declared content body size.

**Failure trace:** Send basic.publish and a content header declaring `bodySize=1`, followed by one body frame containing two bytes `AB`. The handler appends the entire frame and completes when accumulated length is `>= bodySize`; it never rejects overshoot or slices to the declared size. The broker routes, stores, delivers, and may confirm a two-byte message even though the protocol header declared one byte, desynchronizing semantic content from its header.

**Severity:** MED

### 22. `source/dreads/mqtt.d:2046-2058,4047-4076`

**Claim:** MQTT shared unsubscribe removes the wrong persistence record when the same filter exists in multiple share groups.

**Failure trace:** A persistent MQTT 5 client subscribes normally to `sensors/#`, then subscribes to `$share/g/sensors/#`. Both `filters` entries contain the same actual filter; their aligned `subInfo` entries distinguish share group `""` from `"g"`. Unsubscribe `$share/g/sensors/#`: `trieUnsubscribe` correctly removes group g, but the bookkeeping loop matches only the filter and removes the first entry, the normal subscription. On park/reconnect, `mqttAdoptState` rebuilds subscriptions from the remaining arrays, restoring the unsubscribed shared subscription and losing the still-valid normal one.

**Severity:** MED

### 23. `source/dreads/mqtt.d:3347-3360`

**Claim:** MQTT 5 CONNECT accepts Receive Maximum zero as unlimited instead of rejecting the protocol error.

**Failure trace:** Send a syntactically valid MQTT 5 CONNECT containing property `0x21` (Receive Maximum) with value zero. The parser returns `recvMax=0`; the handler deliberately skips assigning `sendMax` and continues to a successful CONNACK. MQTT requires the server to treat zero as a protocol error and close/reject the connection, so a malformed peer instead obtains an unlimited outbound in-flight window.

**Severity:** MED

### 24. `source/dreads/sqs.d:52-86,1230-1239`

**Claim:** SQS trusts an unbounded client Content-Length and grows a full-request buffer until that size, enabling memory-exhaustion denial of service.

**Failure trace:** An unauthenticated connection sends an HTTP header with `Content-Length: 1073741824` and streams a 1 GiB body. `toSize` accepts the length without a cap or overflow detection; `onConn` repeatedly grows `ByteBuffer whole` until `hend + clen` bytes arrive. A few concurrent connections force multi-gigabyte allocation in the broker process and can terminate it before request parsing or authentication. On integer wrap, `hend + clen` can additionally make the completeness test incorrect.

**Severity:** MED

## Ranked summary

| Rank | Severity | Defect |
|---:|:---:|---|
| 1 | CRITICAL | Raft `Pending` pointers are not GC-rooted while stored in malloc/calloc containers |
| 2 | CRITICAL | Per-shard expiry turns the Raft proposal SPSC queue into an unsafe MPSC queue |
| 3 | CRITICAL | SQS live slices alias reused TLS buffers across yielding shard hops |
| 4 | CRITICAL | Out-of-range `sqs-db` indexes or aliases another shard's RESP keyspace |
| 5 | CRITICAL | AMQP 1.0 described-value recursion permits remote stack exhaustion |
| 6 | CRITICAL | Global `gApplying` races across concurrent committed applies |
| 7 | HIGH | Cross-shard codecs perform misaligned typed dereferences |
| 8 | HIGH | AMQP confirms precede remote apply and AOF logging |
| 9 | HIGH | MQTT immediate Wills are suppressed during teardown |
| 10 | HIGH | MQTT advertises resumed sessions after losing their actual state |
| 11 | HIGH | Non-atomic FIFO receive can deliver one SQS message twice |
| 12 | HIGH | SQS batch delete permanently retains FIFO group locks |
| 13 | HIGH | SQS sweep's 16-bit record length corrupts 64 KiB bodies |
| 14 | HIGH | SQS silently truncates bodies above 64 KiB |
| 15 | HIGH | SQS purge leaves delayed messages and FIFO locks behind |
| 16 | HIGH | Kafka Produce/AddOffsets do not enforce coordinator fencing |
| 17 | HIGH | Kafka transaction timeout is discarded, pinning LSO indefinitely |
| 18 | HIGH | AMQP durable topology is not persisted |
| 19 | HIGH | AMQP topology acknowledgements precede cross-shard application |
| 20 | MED | SQS batch send bypasses FIFO group/dedup requirements |
| 21 | MED | AMQP body-frame overshoot is stored and delivered |
| 22 | MED | MQTT shared unsubscribe removes the wrong session record |
| 23 | MED | MQTT accepts Receive Maximum zero |
| 24 | MED | SQS Content-Length drives unbounded allocation |

Validation: the existing `./bin/dreads_tests` binary completed 596 tests with 0 failures. These defects are concurrency, crash-window, restart, architecture-alignment, or malformed-input cases not exercised by that suite.
