#!/usr/bin/env bash
# Build the Preact + uPlot dashboard into a single self-contained dist/index.html.gz,
# which dreads embeds at compile time (stringImportPaths).
#
# The built bundle is COMMITTED to the repo (dist/index.html.gz is tracked), so
# BUILDING DREADS NEVER REQUIRES NODE. We only (re)build the UI when its sources
# actually changed AND a Node toolchain is present; otherwise the committed bundle
# is used as-is. This also means the build survives if the npm/vite toolchain ever
# breaks — and `--config=no-dashboard` skips the dashboard (and this) entirely.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$DIR/dist/index.html.gz" # dreads embeds the gzipped bundle (served as-is)

newest_src=$(find "$DIR/src" "$DIR/index.html" "$DIR/vite.config.js" "$DIR/package.json" \
  -type f -newer "$OUT" 2>/dev/null | head -1 || true)

# committed/prebuilt bundle is current -> nothing to do, no Node needed
if [[ -f "$OUT" && -z "$newest_src" ]]; then
  exit 0
fi

# no Node? fall back to the committed bundle instead of failing the whole dreads build
if ! command -v npm >/dev/null 2>&1; then
  if [[ -f "$OUT" ]]; then
    echo "dashboard: npm not found — using the committed dist/index.html.gz (sources changed but can't rebuild)" >&2
    exit 0
  fi
  echo "dashboard: npm not found AND no committed dist/index.html.gz — install Node, or build with --config=no-dashboard" >&2
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
