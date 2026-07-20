#!/usr/bin/env bash
# Build a static liblz4.a from UPSTREAM lz4 source — same pattern as vendor/lua.
#
# The repo does NOT vendor lz4's source tree; this script downloads the pristine
# upstream release tarball (cached + sha256-verified) and builds only the static
# library. dreads links it via dub.json `lflags` so the binary has NO runtime
# dependency on the system liblz4.so (which also fixes the Docker images, whose
# runtime layer never shipped liblz4). Idempotent: re-runs are no-ops once the
# lib is up to date.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VER=1.10.0
SHA=537512904744b35e232912055ccf8ec66d768639ff3abe5788d90d792ec5f48b
URL="https://github.com/lz4/lz4/releases/download/v${VER}/lz4-${VER}.tar.gz"

CACHE="${DIR}/cache"
BUILD="${DIR}/build"
TARBALL="${CACHE}/lz4-${VER}.tar.gz"
SRC="${BUILD}/lz4-${VER}"
LIB="${BUILD}/liblz4.a"

# up to date? (lib newer than this script)
if [[ -f "${LIB}" && "${LIB}" -nt "${BASH_SOURCE[0]}" ]]; then
  exit 0
fi

mkdir -p "${CACHE}" "${BUILD}"

# download once, verify every time
if [[ ! -f "${TARBALL}" ]]; then
  curl -fsSL "${URL}" -o "${TARBALL}.tmp"
  mv "${TARBALL}.tmp" "${TARBALL}"
fi
echo "${SHA}  ${TARBALL}" | sha256sum -c - >/dev/null

# fresh extract + build just the static library (-fPIC so it links into a PIE)
rm -rf "${SRC}"
tar xzf "${TARBALL}" -C "${BUILD}"
make -C "${SRC}/lib" liblz4.a CC="${CC:-cc}" CFLAGS="-O3 -fPIC" >/dev/null
cp "${SRC}/lib/liblz4.a" "${LIB}"
echo "built ${LIB} (lz4 ${VER})"
