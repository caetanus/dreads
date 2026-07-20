#!/bin/sh
# dreads container entrypoint — redis/valkey-style config, kubernetes-env-native.
#
# The dreads binary itself speaks the redis-server CLI (a config file + any
# `--<directive> <value>` flags). This script adds the one thing the k8s world
# expects — ENV VARS — by translating them to those flags, so the image works
# out of the box whether you configure it by env (ConfigMap/Secret), a mounted/
# COPYed config file, or plain command-line args.
#
# Layers, LATER OVERRIDES EARLIER:
#   1. config file    $DREADS_CONFIG_FILE (default /etc/dreads/dreads.conf)
#   2. env vars       DREADS_<DIRECTIVE>=value  ->  --<directive> value
#   3. container args whatever you pass after the image (redis-server style)
set -eu

BIN="${DREADS_BIN:-/usr/local/bin/dreads}"
CONF="${DREADS_CONFIG_FILE:-/etc/dreads/dreads.conf}"

# env (k8s) -> redis-style flags. DREADS_MAXMEMORY_POLICY=lru -> --maxmemory-policy lru
# (underscores become dashes). Values are space-free — for anything exotic, use a
# config file. DREADS_CONFIG_FILE selects the base file and is not itself a flag.
FLAGS=""
for kv in $(env | grep '^DREADS_' || true); do
    key=${kv%%=*}
    # DREADS_CONFIG_FILE selects the base file; DREADS_BIN selects the binary —
    # both are entrypoint controls, not dreads directives, so never flag them.
    case "$key" in DREADS_CONFIG_FILE|DREADS_BIN) continue ;; esac
    d=$(printf '%s' "${key#DREADS_}" | tr 'A-Z_' 'a-z-')
    FLAGS="$FLAGS --$d ${kv#*=}"
done

# order on the command line = precedence (dreads applies left-to-right):
#   config file, then env-derived flags, then the container's own args.
if [ -f "$CONF" ]; then
    set -- "$CONF" $FLAGS "$@"
else
    set -- $FLAGS "$@"
fi
exec "$BIN" "$@"
