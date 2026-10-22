"""Run the demo's generator (scripts/airport_datagen.py) for the Kafka Streams app.

    uv run python kstreams/datagen.py --skip-history --speed 10                 # local broker
    uv run python kstreams/datagen.py --cluster env --skip-history --speed 10   # Confluent Cloud (kstreams/.env)

The deployed demo doesn't need this: its topics have one partition, so `uv run airport-datagen`
feeds Flink and Kafka Streams alike.

The generator pins partition 0 because Flink's tables have one bucket. For the Kafka Streams app,
records spread over every partition by key, with the Java client's partitioner (murmur2), so
KafkaStreams#queryMetadataForKey finds the instance that owns each key.
All other arguments pass through to the generator.
"""

import sys

from scripts import airport_datagen, kstreams_backend


def main(argv):
    cluster = "local"
    if "--cluster" in argv:
        i = argv.index("--cluster")
        cluster, argv = argv[i + 1], argv[:i] + argv[i + 2:]
    airport_datagen.Publisher = kstreams_backend.publisher_class(cluster)
    airport_datagen.extract_kafka_credentials = lambda *args, **kwargs: {}
    airport_datagen.main(["--offers", "generator", *argv])


if __name__ == "__main__":
    main(sys.argv[1:])
