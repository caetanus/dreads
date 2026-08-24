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

Baseline 2026-08-24: 41 unique cases pass / 11 fail. 200 of the 209 counted
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
    ./mvnw verify -Dit.test=DreadsFunctionalTestSuite -Drabbitmqctl.bin=/bin/false \
      -Dtest=zzz -Dsurefire.failIfNoSpecifiedTests=false -Dspotless.check.skip=true

Baseline 2026-08-24: 298 tests, 135 pass / 117 fail / 46 error / 6 skip.
Biggest fail clusters (all real conformance targets): DeadLetterExchange,
QueueExclusivity, Per*TTL, CcRoutes, QueueSizeLimit (x-max-length),
UnexpectedFrames, QosTests, QueueLease (x-expires), PerConsumerPrefetch,
ExchangeDeclare equivalence (406 on redeclare mismatch).

Note: both suites needed dreads to advertise `version` in the Connection.Start
server-properties — its absence NPE'd every java BrokerTestCase setUp.
