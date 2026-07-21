#!/usr/bin/env bash
# Produce the single self-contained dist/index.html.gz that dreads embeds at compile
# time (stringImportPaths).
#
# THE COMMITTED BUNDLE IS AUTHORITATIVE. dist/index.html.gz is tracked in git, so
# CI, Docker, and a plain `dub build` NEVER run Vite/npm — they embed the committed
# bundle as-is. Building dreads therefore never requires Node, and survives the
# npm/vite toolchain breaking.
#
# When you edit the frontend (src/*.jsx, style.css), rebuild it explicitly and commit
# the result:
#     DREADS_REBUILD_UI=1 bash vendor/dashboard/build.sh
#     git add vendor/dashboard/dist/index.html.gz
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$DIR/dist/index.html.gz"

# default path: use the committed bundle, no Node, no Vite (CI / Docker / fresh clone)
if [[ "${DREADS_REBUILD_UI:-0}" != "1" ]]; then
  if [[ -f "$OUT" ]]; then
    exit 0
  fi
  echo "dashboard: no committed dist/index.html.gz — rebuild it with DREADS_REBUILD_UI=1 (needs Node), or build dreads with --config=no-dashboard" >&2
  exit 1
fi

# explicit rebuild (developer changed the frontend) — needs a Node toolchain
if ! command -v npm >/dev/null 2>&1; then
  echo "dashboard: DREADS_REBUILD_UI=1 but npm not found — install Node to rebuild the UI" >&2
  exit 1
fi
cd "$DIR"
if [[ ! -d node_modules ]]; then
  npm ci --no-audit --no-fund 2>/dev/null || npm install --no-audit --no-fund
fi
npm run build >/dev/null
# gzip the single-file bundle: smaller embed + served with Content-Encoding: gzip.
gzip -9 -f -n -c dist/index.html > "$OUT"
echo "built $OUT ($(wc -c < dist/index.html) -> $(wc -c < "$OUT") bytes gzipped)"
