#!/usr/bin/env bash
# Build a static libsnappy.a from UPSTREAM snappy source — same pattern as
# vendor/lz4. Kafka producers use snappy for compressed RecordBatches; dreads
# decompresses on ingest. Snappy is C++, so the final link also needs libstdc++
# (added in dub.json lflags). Downloads the pristine upstream release tarball
# (cached + sha256-verified) and cmake-builds only the static library. The repo
# does NOT vendor the source tree, and the binary has NO runtime dependency on
# the system libsnappy.so. Idempotent: re-runs are no-ops once the lib is built.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VER=1.2.1
SHA=736aeb64d86566d2236ddffa2865ee5d7a82d26c9016b36218fcc27ea4f09f86
URL="https://github.com/google/snappy/archive/refs/tags/${VER}.tar.gz"

CACHE="${DIR}/cache"
BUILD="${DIR}/build"
TARBALL="${CACHE}/snappy-${VER}.tar.gz"
SRC="${BUILD}/snappy-${VER}"
LIB="${BUILD}/libsnappy.a"

# up to date? (lib newer than this script)
if [[ -f "${LIB}" && "${LIB}" -nt "${BASH_SOURCE[0]}" ]]; then
  exit 0
fi

mkdir -p "${CACHE}" "${BUILD}"

if [[ ! -f "${TARBALL}" ]]; then
  curl -fsSL "${URL}" -o "${TARBALL}.tmp"
  mv "${TARBALL}.tmp" "${TARBALL}"
fi
echo "${SHA}  ${TARBALL}" | sha256sum -c - >/dev/null

rm -rf "${SRC}"
tar xzf "${TARBALL}" -C "${BUILD}"

# static lib only: no tests, no benchmarks (they need submodules we don't fetch)
cmake -S "${SRC}" -B "${SRC}/out" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
  -DSNAPPY_BUILD_TESTS=OFF \
  -DSNAPPY_BUILD_BENCHMARKS=OFF \
  -DBUILD_SHARED_LIBS=OFF \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON >/dev/null
cmake --build "${SRC}/out" --target snappy >/dev/null
cp "${SRC}/out/libsnappy.a" "${LIB}"
echo "built ${LIB}"
