#!/usr/bin/env bash
# Rehearse a failover during RA417's delay ramp: stream at 10x, stop instance 2 at +50s,
# restart it at +90s, and watch produce-to-queryable lag the whole time (logs/watch.log).
# Expects run-all.sh to be running. Pass KILL=-9 to crash instance 2 instead of stopping it.
set -euo pipefail
cd "$(dirname "$0")/../.."
LOG=kstreams/logs/watch.log
FLIGHT="RA417-$(date -u +%Y%m%d)"

pkill -f "kstreams/local/verify.py watch" || true   # a watcher left over from an earlier run
uv run python -u kstreams/local/verify.py watch "$FLIGHT" --timeout 60 > "$LOG" 2>&1 &
until grep -q "^Watching" "$LOG"; do sleep 1; done
uv run python -u kstreams/datagen.py --skip-history --speed 10 --minutes 30 > kstreams/logs/datagen.log 2>&1 &
T0=$(date +%s)
at() { until [ $(( $(date +%s) - T0 )) -ge "$1" ]; do sleep 1; done; }

at 50; echo "$(date +%T) stopping instance 2 (signal ${KILL:--TERM})"; pkill "${KILL:--TERM}" -f 'river.air.instance=2'
at 90; echo "$(date +%T) restarting instance 2"; (cd kstreams && nohup ./run-instance.sh 2 > logs/instance-2.log 2>&1 &)   # outlives this script
at 200; pkill -INT -f "kstreams/local/verify.py watch"   # uv doesn't forward signals, so signal python directly
until ! pgrep -f "kstreams/local/verify.py watch" >/dev/null; do sleep 1; done
grep -vE "rdkafka|authlib|httpx2|^Watching" "$LOG"
