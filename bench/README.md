# Benchmarks

Reusable scripts to compare dreads against Valkey/Redis (and MQTT/Mosquitto).
Results live in [valkey-comparison.md](valkey-comparison.md).

## Ground rules

- **One server at a time.** Never run dreads and valkey together — a co-resident
  idle server perturbs the numbers (and process priority/niceness can skew a
  both-up run). Measure each alone.
- **Pin everything.** Server on its own core; client on separate cores. Both
  servers single-threaded, jemalloc, persistence off (unless testing AOF).
- Report **min / median / max** over several runs (median is the honest figure);
  peak is a separate best-of-N row.

## RESP servers (dreads / valkey)

```sh
# dreads (default = active-expire off):     printf 'port 7200\n' > d.conf
taskset -c 1 ./bin/dreads d.conf &
RUNS=5 bench/redis-bench.sh 7200 "DREADS"

# valkey, matched config (single core, no persistence):
taskset -c 1 valkey-server --port 7201 --save '' --appendonly no --io-threads 1 &
RUNS=5 bench/redis-bench.sh 7201 "VALKEY"
```

`redis-bench.sh` covers: data ops (min/med/max), `SET EX`, pattern pub/sub
scaling (`pN:*`, `aa*bb`, `aa*c?e*bb` — publish to a non-matching channel while N
patterns are subscribed, isolating the matcher cost), and fan-out (0/10/50 subs).
Knobs: `P N R RUNS CLIENT_CORES SUB_CORES`.

Active expiry on/off (dreads only) — toggle and re-run `SET EX`:
```sh
redis-cli -p 7200 config set active-expire yes   # or 'no'
```

## AOF (persistence write path)

Restart each server with append-only persistence, then re-run the write ops.
`appendfsync everysec` is the fast/durable default; `always` fsyncs per write.

```sh
# dreads
taskset -c 1 ./bin/dreads 7200 --appendonly &
# valkey
taskset -c 1 valkey-server --port 7201 --save '' \
  --appendonly yes --appendfsync everysec --io-threads 1 &

P=16 RUNS=5 bench/redis-bench.sh 7200 "DREADS (AOF)"   # SET/LPUSH/... rows are the AOF cost
```

## MQTT (Mosquitto) — cross-protocol pub/sub reference

```sh
mkdir -p /tmp/mosq
printf 'listener 1883\nallow_anonymous true\nmax_queued_messages 0\n' > /tmp/mosq/mosquitto.conf
docker run -d --name mosq --cpuset-cpus 1 -p 1883:1883 \
  -v /tmp/mosq/mosquitto.conf:/mosquitto/config/mosquitto.conf eclipse-mosquitto

bench/mqtt-bench.sh 1000000 50     # fan-out (50 subs) + wildcard pN/+ scaling
```

Cross-protocol caveats: different wire protocol and client; Mosquitto QoS 0 may
drop under slow subscribers. Treat the MQTT column as a rough reference.
