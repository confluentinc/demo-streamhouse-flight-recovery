"""Create or remove the Kafka Streams demo's topics and schemas on a separate Confluent Cloud cluster.

Only for the cluster in kstreams/.env. The deployed Streamhouse demo needs none of this: Terraform
creates its topics, and `uv run destroy` removes them with the cluster.

    uv run python kstreams/cloud.py setup      # 4 source topics (6 partitions, compacted) + their value schemas
    uv run python kstreams/cloud.py teardown   # those, plus every river-air-ops-* internal topic

Setup is idempotent. Stop the app (kstreams/stop-all.sh) before teardown.
"""

import json
import sys
from pathlib import Path

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from scripts.kstreams_backend import connection_config

SOURCES = ("flight_status", "passenger_itineraries", "hotel_inventory", "passenger_recommendations")
APPLICATION_ID = "river-air-ops"
PARTITIONS = 6
SCHEMAS = Path(__file__).parent / "local" / "schemas"


def setup(admin: AdminClient, registry: SchemaRegistryClient) -> None:
    existing = set(admin.list_topics(timeout=15).topics)
    new = [NewTopic(t, PARTITIONS, config={"cleanup.policy": "compact"}) for t in SOURCES if t not in existing]
    for topic, future in admin.create_topics(new).items() if new else []:
        future.result()
        print(f"created topic {topic}")
    for topic in SOURCES:
        schema = Schema(json.dumps(json.loads((SCHEMAS / f"{topic}.avsc").read_text())), "AVRO")
        schema_id = registry.register_schema(f"{topic}-value", schema)
        print(f"schema {topic}-value: id {schema_id}")


def teardown(admin: AdminClient, registry: SchemaRegistryClient) -> None:
    topics = [t for t in admin.list_topics(timeout=15).topics
              if t in SOURCES or t.startswith(APPLICATION_ID + "-")]
    for topic, future in admin.delete_topics(topics).items() if topics else []:
        future.result()
        print(f"deleted topic {topic}")
    for subject in registry.get_subjects():
        if subject.removesuffix("-value").removesuffix("-key") in SOURCES or subject.startswith(APPLICATION_ID + "-"):
            registry.delete_subject(subject)
            print(f"deleted subject {subject}")


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in ("setup", "teardown"):
        sys.exit(__doc__)
    kafka, registry_config = connection_config("env")  # never the deployed demo: Terraform owns its topics
    {"setup": setup, "teardown": teardown}[sys.argv[1]](AdminClient(kafka), SchemaRegistryClient(registry_config))


if __name__ == "__main__":
    main()
