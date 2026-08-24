# AMQP 1.0 <-> 0-9-1 interop matrix (M5)

One shared topology, two protocols: `matrix.py` (raw-socket 1.0 client in
`a10client.py` + pika for 0-9-1) proves the cross-protocol contract:

1. 0-9-1 publish -> 1.0 consume: body + properties + application-properties.
2. 1.0 publish -> 0-9-1 get: sections mapped onto the fixed props + headers.
3. Exchange topology shared: 1.0 publish through an 0-9-1-declared binding.
4. 0-9-1 TTL+DLX applies to 1.0 publishes (x-death intact on the DLQ).
5. 1.0 released disposition -> 0-9-1 sees the redelivered flag.
6. x-max-length enforced on 1.0 publishes.

    ./bin/dreads --port=16399 --amqp-port=5672 &
    python3 conformance/amqp10-interop/matrix.py   # "INTEROP MATRIX: 6/6 PASS"

Companion suites: conformance/amqp10-rhea (real-client roundtrip) and the
rabbitmq-amqp-java-client integration classes (AmqpTest 52/54,
ConsumerOutcomeTest 7/7, AddressFormatTest 6/6, ManagementTest 5/7 — the
gaps are OAuth, rabbitmqctl-driven recovery, and stream filter semantics).
