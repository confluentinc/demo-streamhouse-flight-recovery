# River Air operations view on Kafka Streams

The operations dashboard, powered by Kafka Streams: stream processing inside your own service, alongside the Flink and Lightning Tables version. The app reads the same source topics Flink reads (`flight_status`, `passenger_itineraries`, `hotel_inventory`, `passenger_recommendations`), applies the same rules as Flink's [`passenger_state`](../terraform/airline-demo/sql/24-serving-passenger-state.sql) and [`flight_impact`](../terraform/airline-demo/sql/25-serving-flight-impact.sql), and keeps every view the dashboard needs as a state store it queries directly. Both arrive at the same numbers. Flink's results reach Lightning at each checkpoint, about once a minute; Kafka Streams commits every 100 ms and the dashboard reads the app's local state, so a change shows up in under a second.

You run it: three instances of an ordinary Java service, two stream threads each, with the KIP-1071 `streams` rebalance protocol. Built on Apache Kafka Streams **4.4.0 RC4**, from the Apache staging repository until 4.4.0 is released.

## On the deployed demo

`uv run deploy` starts the app on the demo's cluster with the app-manager keys from Terraform state, then offers both dashboards: Flink at http://127.0.0.1:8000, Kafka Streams at http://127.0.0.1:8001, and both side by side at http://127.0.0.1:8000/compare. `uv run airport-datagen` feeds both.

```bash
uv run kstreams start                          # 3 instances; logs in kstreams/logs/
uv run airport-app --engine kstreams --port 8001
uv run kstreams stop
```

`uv run destroy` stops the app and deletes its local state before it destroys the cluster.

## Topology

Four sub-topologies. The only internal topics between them belong to the two foreign-key joins and the at-risk count.

```
flight_status ─────────── drop unchanged ─▶ flights ─┬─▶ FK joins (subscriptions from passengers) ──┐
                                                     └─ left join at-risk count ─▶ flight-impact ─▶ index: flights by scheduled time
passenger_itineraries ─── drop unchanged ─▶ itineraries ─ FK join inbound ─ FK left join connecting ─▶ passenger-state
                                                     ▲                                                 ├─▶ index: passengers by inbound / connecting flight
                                                     └──────────────── responses ◀─────────────────────┘└─▶ count HIGH per inbound flight (repartition)
hotel_inventory ───────── drop unchanged ─▶ hotels
passenger_recommendations ─ drop unchanged ─▶ offers ─▶ index: booked offers by time
```

Sub-topologies 0 and 1 are the same logic as Flink's `sql/24` and `sql/25`. The rest is what Lightning Tables does for the Flink side: a state store answers only by key or key range, so the app keeps an index for each way the dashboard looks things up. Each index lives in the same task as the rows it indexes (`LocalIndex`), so it costs no repartition topic and no extra hop; range reads ask every instance anyway. Offers need no index: a passenger's are always `{passenger}-O1` and `-O2`.

Every active flight republishes its row once a minute, usually unchanged, and restarting the scene republishes every itinerary. `DropUnchanged` skips those repeats before they fan out through the joins, which cut the catch-up after a scene restart on Confluent Cloud from about a minute to under 20 seconds.

On the deployed demo the topics have one partition, because Flink's tables have one bucket, so the app has 4 tasks. On the local broker they have 6 partitions: 24 tasks, 8 per instance.

`topology.txt` and `topology.json` hold this topology as `Topology#describe()` prints it, for the Kafka Streams UI or a visualizer. `./gradlew describeTopology` (in `kstreams/`) regenerates them from the code, offline.

## Changing the topology

With the KIP-1071 `streams` protocol the broker keeps the group's topology, and through 4.4.0 it rejects a changed one: every instance fails with `INVALID_REQUEST: Topology updates are not supported yet`. After a change to `OpsTopology`, stop the app and delete the group and its internal topics, then start again; it rebuilds its state from the source topics.

```bash
uv run kstreams stop
kafka-streams-groups --bootstrap-server <bootstrap> --command-config <client.properties> \
  --delete --group river-air-ops --delete-all-internal-topics
```

On the local broker, run the tool inside the container: `docker compose -f kstreams/local/docker-compose.yml exec kafka /opt/kafka/bin/kafka-streams-groups.sh --bootstrap-server localhost:9092 ...`. A fresh `uv run deploy` has nothing to delete.

## HTTP API

Every instance answers every request. A key lookup goes to the instance that owns the key; a range read asks every instance and merges the answers, and returns 503 rather than a partial answer while a task is moving between instances.

| Endpoint | Returns |
|---|---|
| `GET /api/flights?from=&to=` | Flights scheduled in a window (epoch millis), with delay and at-risk count |
| `GET /api/flights/{flight_id}` | One flight and its passengers, HIGH first |
| `GET /api/flights/{flight_id}/passengers?leg=inbound\|connecting` | The passengers on one leg |
| `GET /api/passengers/{passenger_id}` and `.../offers` | One passenger's risk, and their offers |
| `GET /api/hotels`, `GET /api/offers/booked?since=` | Hotel rooms and rates; booked offers since a time |
| `GET /api/health` | This instance's state and active tasks |

Every response includes `servedBy` (the instances that answered), `queryMicros`, and `servedAt`. Instance 1 listens on port 7070, instance 2 on 7071, and instance 3 on 7072. `scripts/kstreams_backend.py` maps the dashboard's Lightning queries onto these endpoints, so the dashboard code is the same for both engines.

`query.api` picks the read API. `v1` (`KafkaStreams#store`) is the default because IQv2 (`KafkaStreams#query`) can deadlock the stream thread through 4.4.0: a query locks the store's `Position` and then the store, while a write takes them in the opposite order. This is [KAFKA-19629](https://issues.apache.org/jira/browse/KAFKA-19629), fixed on trunk (4.5) and not backported.

## Run it locally

Needs Java 17, Docker, and `uv`.

```bash
kstreams/local/setup.sh                                       # Kafka 4.4.0 RC4 + Schema Registry; 6-partition topics; schemas
uv run kstreams start --cluster local
uv run python kstreams/datagen.py --skip-history --speed 10   # the demo's generator, spread over all partitions
uv run airport-app --engine kstreams --cluster local
uv run kstreams stop
```

Check the app against the rules, and measure it:

```bash
uv run python kstreams/local/verify.py check                          # every flight, passenger, and day vs. a Python copy of sql/24-25
uv run python kstreams/local/verify.py watch RA417-$(date -u +%Y%m%d)   # produce-to-queryable lag per RA417 update
uv run python kstreams/local/verify.py lag                            # the app's backlog until it catches up
kstreams/local/failover-test.sh                                       # stop and restart an instance mid-ramp
```

Run `check` while the stream is quiet; during a stream the reference and the app read a moving target. Add `--cluster deployed` or `--cluster env` to point any of these at Confluent Cloud.

## Another Confluent Cloud cluster

To run against a cluster other than the deployed demo, put its keys in `kstreams/.env` (gitignored): `BOOTSTRAP_SERVERS`, `CLUSTER_API_KEY`, `CLUSTER_API_SECRET`, `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET`.

```bash
uv run python kstreams/cloud.py setup         # the 4 source topics (6 partitions) and their schemas
uv run kstreams start --cluster env
uv run python kstreams/datagen.py --cluster env --skip-history
uv run python kstreams/cloud.py teardown      # every topic and schema setup and the app created
```

The generator wrapper lets the producer choose partitions by key with murmur2, the Java client's partitioner, so `queryMetadataForKey` finds the right instance. The stock generator pins partition 0 because Flink's tables have one bucket.
