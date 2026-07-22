#!/usr/bin/env bash
# PGO build recipe: instrument -> train on the representative mix -> rebuild
# with the profile. Machine-specific: rerun on each box (the .profdata is not
# committed). Measured on the 3950X: SET −5.7% ins/op, INCR −7% cyc (+6% rps),
# shards=4 +5%, shards=8 +7% — and it stabilizes the serve loop's inlining,
# which otherwise sits on an inliner cliff (±50 ins/op of layout luck per edit).
#
# Usage: bench/pgo.sh [port]   (needs redis-benchmark/redis-cli; ~2 min)
set -euo pipefail
PORT="${1:-7791}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PGODIR="$ROOT/bin/pgo"
RT="$(ls /usr/lib/llvm*/lib/clang/*/lib/linux/libclang_rt.profile-x86_64.a 2>/dev/null | tail -1)"
[ -n "$RT" ] || { echo "clang profile runtime not found (install clang/compiler-rt)"; exit 1; }

echo "== 1/3 instrumented build =="
# -L-u -L__llvm_profile_runtime: force-link the runtime member that registers
# the atexit profile writer (two separate linker args — NOT '-u,sym').
DFLAGS="-fprofile-instr-generate -L-u -L__llvm_profile_runtime -L$RT" \
    dub build --compiler=ldc2 --build=release --force

echo "== 2/3 training (clean SHUTDOWN writes the profile) =="
mkdir -p "$PGODIR"; rm -f "$PGODIR"/*.profraw
pkill -9 -x dreads 2>/dev/null || true; sleep 0.4
LLVM_PROFILE_FILE="$PGODIR/single-%p.profraw" taskset -c 0 "$ROOT/bin/dreads" "$PORT" >/dev/null 2>&1 &
for i in $(seq 50); do redis-cli -p "$PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
for op in set get incr; do
  taskset -c 8 redis-benchmark -p "$PORT" -t "$op" -P 64 -c 25 -n 600000 -r 200000 -q >/dev/null 2>&1
done
redis-cli -p "$PORT" shutdown nosave 2>/dev/null || true
sleep 1
LLVM_PROFILE_FILE="$PGODIR/shard4-%p.profraw" taskset -c 0-3 "$ROOT/bin/dreads" "$PORT" --shards 4 >/dev/null 2>&1 &
for i in $(seq 50); do redis-cli -p "$PORT" ping >/dev/null 2>&1 && break; sleep 0.2; done
pids=()
for i in 0 1 2 3; do
  taskset -c $((8 + i)) redis-benchmark -p "$PORT" -t set -P 64 -c 25 -n 400000 -r 200000 -q >/dev/null 2>&1 &
  pids+=("$!")
done
wait "${pids[@]}"
taskset -c 8 redis-benchmark -p "$PORT" -t get -P 64 -c 25 -n 400000 -r 200000 -q >/dev/null 2>&1
redis-cli -p "$PORT" shutdown nosave 2>/dev/null || true
sleep 1

echo "== 3/3 optimized build =="
ldc-profdata merge -output="$PGODIR/dreads.profdata" "$PGODIR"/*.profraw
DFLAGS="-fprofile-instr-use=$PGODIR/dreads.profdata" \
    dub build --compiler=ldc2 --build=release --force
echo "PGO build ready: $ROOT/bin/dreads (profile: $PGODIR/dreads.profdata)"
