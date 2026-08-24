# Laundered upstream conformance suites

Adapted ("laundered") copies of upstream broker test suites so they run against
dreads' protocol skins. Each keeps the upstream tests byte-identical wherever
possible; only the broker-management plumbing (node boot, rabbitmqctl, TLS
certs from the CT harness) is replaced or dropped, with every exclusion
justified in-file. Companion harnesses already converged: pika
blocking_adapter_test (82/82) for AMQP, Paho v5/v3 (27/27, 10/10 — needs the
`&!test/nosubscribe` ACL) for MQTT, golib (5/5) for Kafka.

## rmq-erlang-ct — RabbitMQ server's own Erlang suite (amqp_client system_SUITE)

The upstream `deps/amqp_client/test/system_SUITE.erl` from rabbitmq-server
v3.13.x, network group only (the direct group runs inside the broker's Erlang
VM — impossible against a non-Erlang broker). Laundering: static connection
config (127.0.0.1:5672 guest/guest) instead of CT node boot; ssl /
resource-alarm / remote-socket-rpc cases dropped (justified in-file); a 60s
timetrap so a hang costs one minute, not CT's 30-minute default.

Run (no local Erlang needed):

    ./bin/dreads --port=16399 --amqp-port=5672 &
    docker run --rm --network host -v $PWD/conformance/rmq-erlang-ct:/w -w /w \
      erlang:26 rebar3 ct --suite test/dreads_system_SUITE

**CONVERGED 2026-08-24: all 344 tests pass.** The auth cases need the ACL
configured BEFORE the run (auth stays legacy accept-any while only the seeded
`default` user exists):

    redis-cli -p 16399 ACL SETUSER default on nopass '~*' '&*' +@all
    redis-cli -p 16399 ACL SETUSER guest on '>guest' '~*' '&*' +@all
    redis-cli -p 16399 ACL SETUSER test_user_no_perms on '>test_user_no_perms'

Earlier baseline for reference: 41 unique cases pass / 11 fail. 200 of the 209 counted
failures are just TWO cases repeated 100× by the upstream loop groups
(`bogus_rpc`, `hard_error` — dreads does not yet answer bogus/hard-error
methods with the exact close codes). Remaining: the auth cluster
(non_existent_user / invalid_password / no_permission / no_vhost /
non_existent_vhost — dreads accepts any PLAIN + vhost), consume_notification,
subscribe_nowait, basic_qos.

## rmq-java — rabbitmq-java-client FunctionalTestSuite

`DreadsFunctionalTestSuite.java` is the official FunctionalTestSuite minus the
five classes that drive the broker through rabbitmqctl/Host (management plane):
Policies, ConnectionRecovery, TopologyRecoveryFiltering, TopologyRecoveryRetry,
UserIDHeader. Drop the file into a rabbitmq-java-client checkout (v5.x branch)
at `src/test/java/com/rabbitmq/client/test/functional/` and run:

    ./bin/dreads --port=16399 --amqp-port=5672 &
    # the SaslMechanisms auth tests need the ACL configured (auth stays legacy
    # accept-any while only the seeded `default` user exists):
    redis-cli -p 16399 ACL SETUSER default on nopass '~*' '&*' +@all
    redis-cli -p 16399 ACL SETUSER guest on '>guest' '~*' '&*' +@all
    ./mvnw verify -Dit.test=DreadsFunctionalTestSuite -Drabbitmqctl.bin=/bin/false \
      -Dtest=zzz -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.check.skip=true

**CONVERGED 2026-08-24: 330 tests, 325 pass / 0 fail / 5 error / 7 skip.**
The 5 remaining errors are all tests that drive the broker through
`rabbitmqctl close_all_connections` (mapped to /bin/false in this harness):
checkAcksWithAutomaticRecovery, checkListenersWithAutoRecoveryConnection,
topologyRecoveryBindingFailure, topologyRecoveryConsumerFailure,
topologyRecoveryRetry — the same management-plane dependency that excluded
five whole classes from the suite. The 7 skips are justified in-file
(restartingExpiry x3 laundering + upstream version gates).

Original baseline for reference: 298 tests, 135 pass / 117 fail / 46 error / 6 skip.
Biggest fail clusters (all real conformance targets): DeadLetterExchange,
QueueExclusivity, Per*TTL, CcRoutes, QueueSizeLimit (x-max-length),
UnexpectedFrames, QosTests, QueueLease (x-expires), PerConsumerPrefetch,
ExchangeDeclare equivalence (406 on redeclare mismatch).

Note: both suites needed dreads to advertise `version` in the Connection.Start
server-properties — its absence NPE'd every java BrokerTestCase setUp.

## kafka-librdkafka — librdkafka's own test suite

librdkafka's tests/ (177 numbered C tests, confluentinc/librdkafka master) run
natively against an external broker — near-zero laundering: `test.conf` points
bootstrap.servers at dreads' Kafka skin, and `driver.sh` runs each test in an
ISOLATED runner process (the stock runner aborts the batch on one timeout),
5-way parallel, 90s cap per test, `-Q` quick mode.

    docker stop kafka                     # BENCH TRAP: the Apache container owns 9092
    ./bin/dreads --port=16399 --kafka-port=9092 &
    git clone --depth 1 https://github.com/confluentinc/librdkafka && cd librdkafka
    ./configure --disable-gssapi --disable-ssl && make -j && (cd tests && make build -j)
    cp ../conformance/kafka-librdkafka/test.conf tests/ && bash ../conformance/kafka-librdkafka/driver.sh

Baseline 2026-08-24 (dreads flexible/Kafka-2.5 dialect): 99 pass / 66 fail /
1 skip; 127/38 after the non-gated bug tail; 156/10 with real consumer
groups. **CONVERGED 2026-08-24 with idempotence + transactions (KIP-98, see
KAFKA-TXN-PLAN.md): 160 pass / 6 fail.** Every remaining failure is
structural: 0052/0077 need kafka-topics.sh, 0109/0115/0119 need kafka-acls.sh
+ broker-side ACL enforcement, 0064 needs an SSL-enabled librdkafka build.
Consumer groups are a real coordinator (dreads.kafkagroup,
KAFKA-GROUPS-PLAN.md); idempotent producers dedup on (pid, epoch, seq);
transactions have control markers, aborted-range tracking, LSO-capped
read_committed fetches and commit-buffered TxnOffsetCommit.
The admin surface grew along 0081's ladder: ACLs (29/30/31), DeleteRecords 21
(real log-start-offset, epoch-gated so hot paths pay zero until the first
truncation), ListGroups 16, DeleteGroups 42, OffsetDelete 47, AlterConfigs 33,
IncrementalAlterConfigs 44, CreateTopics/DeleteTopics/CreatePartitions
validation, authorized-operations masks, committed leader epochs.

The real-bug tail fixed during convergence (all extend-only in kafka.d):
Metadata cluster_id (was null; 0063), CreatePartitions API 37 (0044, 0112),
ListOffsets by timestamp KIP-79 (0054), OffsetCommit metadata persisted in a
sibling `<topic>/<part>#m` hash field + returned by OffsetFetch (0130, 0099),
IncrementalAlterConfigs API 44 (SET/DELETE/APPEND/SUBTRACT on
`kafka.tcfg.<topic>`) + DeleteTopics API 20 + compacted-topic produce gate
(keyless record → INVALID_RECORD 87 with v8 record_errors; 0011), zstd frames
without header content-size decompressed via bounded ZSTD_decompressStream
(librdkafka's streaming writer; 0017), all-topics Metadata window 64→512
topics (a full-suite run registers hundreds; 0114). Harness triage: 7 tests
(0075 0088 0104 0121 0123 0131 0149) have built-in wall-clock phases that the
speed multiplier 0.5 in test.conf falsely times out — driver.sh now runs that
SLOW list serially at the end under multiplier 2; all of them pass at
real-time pacing.

Laundering patch: `rmq-java/restarting-expiry-laundering.patch` — restartingExpiry
needs a management-plane broker restart; without it the test used to leave its
durable queue behind and poison the whole TTL class with legitimate 405/406s.
The patch cleans up over a fresh connection and skips the test. Apply with
`git apply` inside the rabbitmq-java-client checkout.
