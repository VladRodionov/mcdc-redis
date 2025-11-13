#!/usr/bin/env bash
set -euo pipefail
REDIS_SERVER=${REDIS_SERVER:-redis-server}
REDIS_CLI=${REDIS_CLI:-redis-cli}
MOD=$(pwd)/build/mcdc.so

if [ ! -f "$MOD" ]; then
  make
fi

$REDIS_SERVER --loadmodule "$MOD" --daemonize yes
sleep 0.5
$REDIS_CLI ping
$REDIS_CLI mcdc.ping
