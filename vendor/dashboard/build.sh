#!/usr/bin/env bash
# Build the Preact + uPlot dashboard into a single self-contained dist/index.html,
# which dreads embeds at compile time (stringImportPaths). Idempotent: a no-op when
# the built file is newer than the sources. Mirrors the vendor/lua|lz4 build wiring.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="$DIR/dist/index.html.gz" # dreads embeds the gzipped bundle (served as-is)

newest_src=$(find "$DIR/src" "$DIR/index.html" "$DIR/vite.config.js" "$DIR/package.json" \
  -type f -newer "$OUT" 2>/dev/null | head -1 || true)
if [[ -f "$OUT" && -z "$newest_src" ]]; then
  exit 0
fi

cd "$DIR"
if [[ ! -d node_modules ]]; then
  npm ci --no-audit --no-fund 2>/dev/null || npm install --no-audit --no-fund
fi
npm run build >/dev/null
# gzip the single-file bundle: smaller embed + served with Content-Encoding: gzip.
gzip -9 -f -n -c dist/index.html > "$OUT"
echo "built $OUT ($(wc -c < dist/index.html) -> $(wc -c < "$OUT") bytes gzipped)"
