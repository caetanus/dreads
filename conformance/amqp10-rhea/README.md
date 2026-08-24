# AMQP 1.0 smoke — rhea (node)

Round-trip against the dreads AMQP 1.0 skin with a real client: SASL PLAIN,
sender link (5 transfers -> accepted dispositions), receiver link with
credit_window flow control, message properties + application-properties
mapped through the shared 0-9-1 record.

    ./bin/dreads --port=16399 --amqp-port=5672 &
    npm install rhea && node roundtrip.js   # prints "M4 RHEA ROUNDTRIP OK"

Wire lesson encoded in the codec: skipping a DESCRIBED field takes two reads
(descriptor + value) — rhea's attach carries source/target as described lists
and elides default fields (role=false arrives as null).
