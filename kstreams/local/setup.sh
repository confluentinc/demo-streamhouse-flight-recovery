#!/usr/bin/env bash
# Start local Kafka + Schema Registry, then create the four source topics and register their
# schemas. Compacted like terraform/airline-demo, but 6 partitions (PARTITIONS) so the work spreads over instances.
set -euo pipefail
cd "$(dirname "$0")"

docker compose up -d --wait

for topic in flight_status passenger_itineraries hotel_inventory passenger_recommendations; do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" --partitions "${PARTITIONS:-6}" --config cleanup.policy=compact >/dev/null
  python3 -c 'import json,sys; print(json.dumps({"schema": open(sys.argv[1]).read()}))' "schemas/$topic.avsc" \
    | curl -sf --retry 15 --retry-all-errors --retry-delay 2 -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" --data @- \
        "http://localhost:8081/subjects/$topic-value/versions" >/dev/null
  echo "ready: $topic"
done
