#!/usr/bin/env bash
# Start INSTANCES instances (default 3) in the background, staggered, logging to logs/instance-N.log.
#   ./run-all.sh            # local broker
#   ./run-all.sh --cloud    # Confluent Cloud
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p logs
./gradlew installDist -q
for id in $(seq 1 "${INSTANCES:-3}"); do
  nohup ./run-instance.sh "$id" "${1:-}" > "logs/instance-$id.log" 2>&1 &
  echo "instance $id: pid $!, queries on http://localhost:$((7069 + id))/api, log logs/instance-$id.log"
  sleep 3
done
