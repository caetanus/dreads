#!/usr/bin/env bash
# Build a static liblua.a from UPSTREAM Lua source + dreads' read-only patch.
#
# The repo does NOT vendor Lua's source tree; it keeps only our patch
# (dreads-readonly-5.4.8.patch). This script downloads the pristine upstream
# tarball (cached + sha256-verified), applies the patch, and builds liblua.a.
# Idempotent: re-runs are no-ops once the lib is up to date.
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VER=5.4.8
SHA=4f18ddae154e793e46eeab727c59ef1c0c0c2b744e7b94219710d76f530629ae
URL="https://www.lua.org/ftp/lua-${VER}.tar.gz"
PATCH="${DIR}/dreads-readonly-${VER}.patch"

CACHE="${DIR}/cache"
BUILD="${DIR}/build"
TARBALL="${CACHE}/lua-${VER}.tar.gz"
SRC="${BUILD}/lua-${VER}"
LIB="${BUILD}/liblua.a"

# up to date? (lib newer than both the patch and this script)
if [[ -f "${LIB}" && "${LIB}" -nt "${PATCH}" && "${LIB}" -nt "${BASH_SOURCE[0]}" ]]; then
  exit 0
fi

mkdir -p "${CACHE}" "${BUILD}"

# download once, verify every time
if [[ ! -f "${TARBALL}" ]]; then
  curl -fsSL "${URL}" -o "${TARBALL}.tmp"
  mv "${TARBALL}.tmp" "${TARBALL}"
fi
echo "${SHA}  ${TARBALL}" | sha256sum -c - >/dev/null

# fresh extract + patch (never patch an already-patched tree)
rm -rf "${SRC}"
tar xzf "${TARBALL}" -C "${BUILD}"
patch -p1 -d "${SRC}/src" < "${PATCH}"

# build just the static library (no interpreter, no readline dependency).
# The platform macro follows the OS (mirrors build.ps1): Darwin -> MACOSX.
case "$(uname -s)" in
  Darwin) LUA_PLAT="-DLUA_USE_MACOSX" ;;
  *)      LUA_PLAT="-DLUA_USE_LINUX"  ;;
esac
make -C "${SRC}/src" liblua.a CC="${CC:-cc}" \
  MYCFLAGS="-O2 -fPIC ${LUA_PLAT}" MYLIBS="" >/dev/null
cp "${SRC}/src/liblua.a" "${LIB}"
echo "built ${LIB} (Lua ${VER} + dreads read-only patch)"
