# Independent correctness review of the protocol adapters

## Verdict

The copy-before-park implementation in `server.d`/`shard.d` is sound, and most of the refactor is correct. The six adapters are not completely clean, however: concrete remaining bugs exist in `amqp10.d`, `kafka.d`, `mqtt.d`, and `sqs.d`. `amqp.d` and `kafkagroup.d` are clean on all three requested properties.

This review treats a helper argument as safe once `shardEnqueue`/`RingCore.push` has accepted it, because the ring owns a byte copy. It still requires the caller's staging memory to remain stable if `shardEnqueue` yields on a full ring before that successful copy. The audited helper paths meet that requirement: `amqpDataExec`, `kafkaGroupHopImpl`, and the AMQP-push path use stack-local hop frames; the ring copies the payload before publishing the slot; `ShardPending` owns the reply; and the synchronous helpers clear and copy the caller's reply buffer again after the wait.

## Correctness bugs

### `source/dreads/amqp10.d`

- `source/dreads/amqp10.d:4054` — `streamOffThis = pl5.streamPos;` is the first dereference of `pl5` after `a10PeekAt` can park; a concurrent DETACH/END can remove or rehash `ps5.links`, making `pl5` stale. The write at line 4055 and the filter uses through line 4115 have the same defect; each inner `a10PeekAt` at line 4080 creates another stale-pointer window before line 4089 or the next loop iteration.
- `source/dreads/amqp10.d:4336` — `body_ = buf[skip .. rest];` reads the module-static/TLS frame buffer after `a10ReadExact` has parked with that same buffer as its read destination; another AMQP 1.0 connection can refill or grow `buf` meanwhile, so this connection can parse the sibling's frame bytes.

The AMQP 1.0 numeric decoder itself is otherwise sound: `A10Dec` bounds every read/slice, 32-bit container sizes cannot wrap `size_t` on this target, nested descriptors are capped at depth 32, and `a10ReadFrame` validates `size`, `doff`, the frame cap, and `skip <= rest`. The bounds-checked size backpatch is also correct.

### `source/dreads/kafka.d`

- `source/dreads/kafka.d:1920` — `putStr(req, tKafkaClientId is null ? "" : tKafkaClientId);` reads the module-global request client-id after `authorize` may have parked; a sibling request can replace `tKafkaClientId`, recording the wrong connection's client-id in the group member.
- `source/dreads/kafka.d:1972` and `source/dreads/kafka.d:2030` — both calls pass a function-static `req` to `joinLoop` after `registerGroupName` has called the parking executor; a sibling JoinGroup of the same dialect can rebuild `req`, so the first connection submits the sibling's join payload. The flexible and classic functions have separate statics, but each is reentrant with its own dialect.
- `source/dreads/kafka.d:5296` — `tMetaAllowAuto` is read after `registeredTopicPartitions` can park; another Metadata request can overwrite this per-request flag, causing the wrong request's `allow_auto_topic_creation` policy to be applied.
- `source/dreads/kafka.d:5426` — flexible Metadata emits `tKafkaAdvPort` after authorization/registry hops can park; a sibling plaintext or TLS Kafka connection can replace the module-global port, so the response can advertise the wrong listener.
- `source/dreads/kafka.d:5270` — the per-request `tMetaProbes` counter is read after earlier partition probes can park; sibling Metadata requests can reset or advance it, causing this request either to bypass its probe budget or stop early and advertise too few partitions.
- `source/dreads/kafka.d:6045` and `source/dreads/kafka.d:6305` — Fetch and ListOffsets share `tHopProbes`; after a prior `partLen` hop, a sibling request can reset or consume the counter, so the current response can exceed the intended hop budget or truncate its topic/partition results for another connection's activity.

The Kafka frame, request, record-batch, legacy message-set, compact-varint, tagged-field, and stored-record bounds checks are otherwise sound. Wire lengths are checked before slicing, request frames are capped at 64 MiB, record counts are capped, decompression has a per-batch cap, and `patchI32` checks its reserved slot before writing.

### `source/dreads/mqtt.d`

- `source/dreads/mqtt.d:2117` — `newc.obox.append(parked.obox.data);` can read released session storage: after publishing `frozen`, the owner keeps `parked` alive only for a fixed one-second redirect window, with no ownership acknowledgement from the adopter. If adoption is delayed, owner teardown reaches `c.obox.release()` before this read. The surrounding reads of `filters`, in-flight maps, and held state are exposed to the same lifetime race.

MQTT's shared WebSocket read scratch is safe under the actual ordering: `waitForData` completes first, the code reads no more than `leastSize` (so that read does not suspend), and `feed` consumes it before another yield. MQTT packet-length decoding is also sound: the remaining-length varint is limited to four bytes, packet sizes are capped before completion arithmetic/slicing, and individual string/property reads check their packet-local end.

### `source/dreads/sqs.d`

- `source/dreads/sqs.d:640` — `cast(const(char)[]) ifkey.data` is read after `fifoUnlock` has parked; another `opDeleteMessage` fiber can rebuild the function-static `ifkey`, so the HDEL targets the wrong queue and the acknowledged delete becomes a no-op.
- `source/dreads/sqs.d:664` — the batch variant re-reads its function-static `ifkey` after every parking `fifoUnlock` (and after earlier entries' hops), so a sibling batch can redirect an entry's HDEL to another queue and leave the message undeleted.
- `source/dreads/sqs.d:1241` — `dg(j[start .. i]);` slices past the request end when an unterminated entry string ends in `\`: line 1222 advances `i` once for the escape and line 1239 advances it again, leaving `i == j.length + 1`. This is a reachable `BoundsError` on malformed JSON.
- `source/dreads/sqs.d:1390` — `v = v * 10 + (c - '0');` parses an arbitrarily long `Content-Length` without an overflow check; `size_t` wrap can turn a huge declaration into a value below the 4 MiB cap and make the handler dispatch an incomplete body.
- `source/dreads/sqs.d:1453` — `conn.write(body_.data);` re-reads the function-static error body after the preceding header write can park; another connection can rebuild `body_`, producing a cross-connection error body or a header/body length mismatch.

The other SQS key/snapshot/record copies are correctly made per call before subsequent hops, and the remaining request/record slices are bounds-checked. Per-connection request buffers and normal success-response buffers are isolated.

## Modules clean on all three requested properties

- `source/dreads/amqp.d` — clean: no shared-static fill/park/re-read remains in the publish, routing, control-fanout, get, requeue, expiry, or dead-letter paths; frame/table/property/record lengths are bounded before indexing; channel/connection state is not recovered through a clobberable global after a yield.
- `source/dreads/kafkagroup.d` — clean: `KgRd` checks every read and slice, opcode counts are capped, retained ring-payload slices are duplicated before storage, and `kgroupApply` is owner-side and yield-free, so it has no cross-connection scratch or request-context exposure.

## Resource-limit notes (not correctness bugs above)

- `source/dreads/amqp10.d:2915-2921` — the management argument/type maps are populated for queue PUTs but not pruned on DELETE (and can be populated even when declaration does not create a queue), allowing unbounded retained metadata across attacker-chosen names.
- `source/dreads/kafka.d:1928` — flexible JoinGroup does not enforce Kafka's 249-byte group-id limit before building registry/routing keys and a 16-bit internal `putStr`; the outer 64 MiB frame cap is the only practical bound.
- `source/dreads/kafka.d:1952`, `source/dreads/kafka.d:2011`, and `source/dreads/kafkagroup.d:393` — protocol metadata is duplicated and retained per member without an aggregate per-group byte budget; the request-frame limit bounds one join but not the retained total across members/groups.
- `source/dreads/kafka.d:6963` — the intended 512 MiB cumulative decompression ceiling uses the TLS-global `tKafkaDecompUsed`; sibling Produce requests can reset it while this request is parked between partitions, so the aggregate per-request limit is not hard (the per-batch cap still holds).
- `source/dreads/kafka.d:5325` — all-topics Metadata materializes the complete `HKEYS kafka.topics` reply before keeping only 512 names, so registry cardinality can make one request allocate/do work without a matching response-side cap.
- `source/dreads/sqs.d:181` and `source/dreads/sqs.d:205` — queue creation has no registry-cardinality limit and ListQueues materializes the complete registry.
- `source/dreads/sqs.d:487` — FIFO ReceiveMessage executes `LRANGE 0 -1`, materializing the whole queue even though only the first 1,024 parsed records are considered.
- `source/dreads/sqs.d:781`, `source/dreads/sqs.d:853`, and `source/dreads/sqs.d:909` — the visibility sweep uses full `HGETALL` replies for in-flight, delayed, and dedup hashes before capping local processing to 256 entries.

## Short per-module summary

- `amqp.d`: clean on buffer lifetime, decoder bounds, and connection-state isolation.
- `amqp10.d`: two bugs — shared parked frame scratch and stale per-link state after stream fetches; decoder arithmetic itself is clean.
- `kafka.d`: decoder bounds are clean, but JoinGroup staging and several per-request TLS globals remain reentrancy-unsafe.
- `kafkagroup.d`: clean on all three properties.
- `mqtt.d`: packet bounds and WebSocket scratch lifetime are clean; cross-shard session adoption still lacks a lifetime handshake.
- `sqs.d`: two stale delete-key sites, one malformed-JSON OOB slice, one `Content-Length` overflow, and one shared error-response body.
