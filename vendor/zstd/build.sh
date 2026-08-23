#!/usr/bin/env bash
# Build a static libzstd.a from UPSTREAM zstd source — same pattern as
# vendor/lz4. Kafka producers use zstd for compressed RecordBatches; dreads
# decompresses on ingest. zstd is C (no libstdc++ needed). Downloads the
# pristine upstream release tarball (cached + sha256-verified) and builds only
# the static library. The binary has NO runtime dependency on the system
# libzstd.so. Idempotent: re-runs are no-ops once the lib is built.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VER=1.5.6
SHA=8c29e06cf42aacc1eafc4077ae2ec6c6fcb96a626157e0593d5e82a34fd403c1
URL="https://github.com/facebook/zstd/releases/download/v${VER}/zstd-${VER}.tar.gz"

CACHE="${DIR}/cache"
BUILD="${DIR}/build"
TARBALL="${CACHE}/zstd-${VER}.tar.gz"
SRC="${BUILD}/zstd-${VER}"
LIB="${BUILD}/libzstd.a"

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
# static lib only (-fPIC so it links into a PIE); no CLI, no shared lib.
make -C "${SRC}/lib" libzstd.a CC="${CC:-cc}" CFLAGS="-O3 -fPIC" \
  ZSTD_LIB_MINIFY=0 >/dev/null 2>&1
cp "${SRC}/lib/libzstd.a" "${LIB}"
echo "built ${LIB}"
