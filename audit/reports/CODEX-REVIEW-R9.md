# Protocol-adapter correctness review — R9

Reviewed the current working tree based on `3685277` against the requested three properties, including the `DREADS_*` directive layer. This includes the uncommitted `sqs-db`/case-insensitive skin-DB validation changes present in `config.d`. This is a source review; no fixes were made.

## Result

I found five findings: one MQTT cross-shard handoff lifetime race, one Kafka per-request TLS isolation bug, two malformed-terminal-byte overflows in Kafka varint readers, and unchecked multiplication in the memory directive parser. The current `sqs-db` validation rejects values above 19, so the previously possible keyspace OOB is not present in this tree. The cross-shard message helpers themselves obey the supplied lifetime ground truth: they build/copy the hop payload before parking and copy synchronous replies after the wait.

## Real bugs

### R9-1 — MQTT cross-shard adoption can read a released offline outbox (high)

- `source/dreads/mqtt.d:2117` — **exact stale access:** `newc.obox.append(parked.obox.data)` can race the owner teardown's `c.obox.release()` at line 2470.

One line: the adopter gets only a `frozen` flag, while the owner tears the session down after the fixed deadline at line 2325; there is no adopted/acknowledged handshake, so a descheduled adopter can read the released buffer (or silently lose the session state).

### R9-2 — Kafka decompression accounting is shared between parked requests (medium)

- `source/dreads/kafka.d:6971` — **exact stale TLS read:** `tKafkaDecompUsed` can hold a sibling connection's request value after the current Produce fiber parked in `gKafkaExec`.

One line: `handleProduce` resets the thread-local counter at line 5739, but a partition append can park before a later partition calls `decompressRecords`, allowing another Produce fiber to reset/increment the same counter and either bypass the 512 MiB request budget or reject the wrong request.

### R9-3 — Kafka compact-field uvarint accepts a wrapping fifth byte (medium)

- `source/dreads/kafka.d:405` — **exact wrapping length arithmetic:** `(b & 0x7f) << 28` is evaluated as `uint`, but the fifth byte is not restricted to `b <= 0x0f`.

One line: a malformed terminal byte such as `0x10` wraps `2^32` to zero and is accepted as a compact length/count instead of marking the frame malformed.

### R9-4 — Kafka record uvarint accepts a wrapping tenth byte (medium)

- `source/dreads/kafka.d:6474` — **exact wrapping length arithmetic:** `(b & 0x7f) << 63` is evaluated as `ulong`, but the tenth byte is not restricted to `b <= 0x01`.

One line: malformed RecordBatch varints with high bits in byte ten wrap/truncate into an apparently valid record length/delta instead of failing decoding.

### R9-5 — memory-size directives multiply without an overflow check (medium, operator/config reachable)

- `source/dreads/config.d:195` — **exact arithmetic wrap:** `bytes = num.to!ulong * mult` silently wraps for a syntactically valid value with a suffix.

One line: values such as `18014398509481984kb` wrap to zero, so `maxmemory`, `lua-memory-limit`, `stream-node-max-bytes`, `proto-max-bulk-len`, or `client-query-buffer-limit` can acquire a materially different value while the directive reports success.

## Per-module summary

| Module | Result |
|---|---|
| `source/dreads/amqp.d` | **Clean on all three properties.** Wire lengths are checked before slices; static fan-out/record scratch is guarded or copied into call-local storage before any hop; connection/channel state is revalidated where a hop can run another fiber. |
| `source/dreads/amqp10.d` | **Clean on all three properties.** The value decoder's depth limit and sub-decoder bounds are effective, frame assembly is per connection, and link/session pointers are re-looked-up after parking delivery operations. |
| `source/dreads/kafka.d` | **Not clean:** R9-2 violates per-request state isolation; R9-3 and R9-4 violate decoder arithmetic bounds. Other hop buffers, request context captures, frame sizes, and reply backpatches checked out. |
| `source/dreads/kafkagroup.d` | **Clean on all three properties.** `KgRd` bounds every length-derived read, retained state is owner-local/idup'd, and the FSM does not park while holding request slices or map pointers. |
| `source/dreads/mqtt.d` | **Not clean:** R9-1 violates cross-shard session lifetime/isolation. MQTT packet/property decoding and ordinary hop/fan-out buffer lifetimes checked out, including the per-connection WebSocket handshake/output buffers. |
| `source/dreads/sqs.d` | **Clean on all three properties.** Request bodies are capped, `Content-Length` saturates on overflow, JSON/RESP slices are checked, and request data held across `exec` parks is copied per call. |
| `source/dreads/config.d` | **Not clean:** R9-5 accepts wrapped size values. Skin database indices, including `sqs-db`, are correctly bounded in the current tree. It runs at startup, so hop lifetime and per-connection isolation do not otherwise apply. |
| `source/app.d` | **Clean on all three properties.** Environment iteration occurs before connection handling, the mapped directive/name strings have stable GC ownership, and ordinary values flow through `applyDirective` (apart from the intentional append-only filename convenience). |
| `source/dreads/server.d` (`amqpDataExec`, `gKafkaExec`, `gAmqpPush`) | **Clean for the scoped helpers on all three properties.** Arguments are serialized into a stack-local hop buffer before `shardEnqueue`; replies are cleared and recopied after the wait. |
| `source/dreads/shard.d` (`shardEnqueue`, `ShardPending`) | **Clean for the scoped helpers on all three properties.** `RingCore.push` copies payload bytes before publishing, pending slots remain rooted until release, and the retry yield happens before a successful copy. |

## Resource-limit backlog (not correctness findings above)

- `amqp10.d:2918-2925`: management metadata maps retain create/delete queue names and argument blobs without pruning or a lifetime-wide distinct-name cap.
- `kafka.d:5330`: all-topics Metadata materializes the entire remotely growable topic registry reply before retaining only its 512-topic output window.
- `sqs.d:181,204,775`: queue creation has no registry-count cap, while list and sweep paths materialize the full registry.
- `sqs.d:487,789,861,917`: FIFO receive and maintenance use full `LRANGE`/`HGETALL` replies, so queue/in-flight/delay/dedup growth has no per-operation memory/CPU ceiling.
- `config.d:804`: `loadConfig` reads the complete configuration file with no file-size ceiling; raw string directives also have no application-level length/schema cap (environment values remain OS-bounded).

## Verification note

The review was performed against the current source and exact line numbers were rechecked after writing this file. `dub test --config=unittest-no-dashboard` could not reach compilation in the managed workspace because DUB attempted to create build-cache entries under the read-only home cache; retrying with a writable top-level cache still hit read-only dependency build directories. This is a test-environment limitation, not a test failure.
