#!/usr/bin/env bash
# Run ONE instance of the Kafka Streams app in the foreground.
#   ./run-instance.sh 2            # local broker (config/local.properties)
#   ./run-instance.sh 2 --cloud    # the Confluent Cloud cluster in kstreams/.env
# `uv run kstreams start` runs this for the deployed demo, passing CONFIG and the keys in the environment.
# All instances share application.id=river-air-ops. INSTANCE_ID picks the state dir and the
# query port (7070 + id - 1). Stop one with: pkill -f 'river.air.instance=2'
set -euo pipefail
cd "$(dirname "$0")"

INSTANCE_ID="${1:-1}"
CONFIG="${CONFIG:-config/local.properties}"
if [[ "${2:-}" == "--cloud" ]]; then
  CONFIG=config/cloud.properties
  set -a; source .env; set +a
fi
APP_BIN=build/install/river-air-ops-kstreams/bin/river-air-ops-kstreams
[[ -x "$APP_BIN" ]] || ./gradlew installDist -q

export INSTANCE_ID
export NUM_STREAM_THREADS="${NUM_STREAM_THREADS:-2}"
export JAVA_OPTS="-Driver.air.instance=${INSTANCE_ID} ${JAVA_OPTS:-}"
exec "$APP_BIN" "$CONFIG"
