#!/usr/bin/env bash
# Stop every instance; each closes Kafka Streams within 30 seconds.
pkill -f 'river.air.instance=' && echo "stopping" || echo "no instances running"
