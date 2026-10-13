"""Run the Kafka Streams app (kstreams/) and serve the airport app from it instead of Lightning Tables.

    uv run kstreams start            # 3 instances on the deployed demo's cluster (keys from Terraform state)
    uv run kstreams stop
    uv run airport-app --engine kstreams --port 8001   # the same dashboard, powered by Kafka Streams

Clusters: "deployed" is the cluster `uv run deploy` created, so Kafka Streams reads the same topics
as Flink. "local" is the broker from kstreams/local/. "env" is another Confluent Cloud cluster whose
keys are in kstreams/.env.

KafkaStreamsDeployment answers the same lightning_query/lightning_scan calls the app already
makes, so the app's logic and screens don't change; only where the rows come from does. Writes
(select and book) go to Kafka through a publisher that partitions by key the way the Java client
does, which is what the Kafka Streams app needs to find each key's owner.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from dotenv import dotenv_values
from fastapi import HTTPException

from scripts import airport_datagen

INSTANCES = tuple(f"http://127.0.0.1:{port}/api" for port in (7070, 7071, 7072))
RETRY_FOR_SECONDS = 5  # long enough to ride out tasks moving between instances
LOCAL_KAFKA = {"bootstrap.servers": "localhost:9092"}
LOCAL_SCHEMA_REGISTRY = {"url": "http://localhost:8081"}
ROOT = Path(__file__).resolve().parent.parent
KSTREAMS = ROOT / "kstreams"
ENV_FILE = KSTREAMS / ".env"
CLUSTERS = ("deployed", "local", "env")

FLIGHT_FIELDS = {"flightId": "key", "scheduledTime": "scheduled_time", "estimatedTime": "estimated_time",
                 "delayMinutes": "delay_minutes", "affectedPassengers": "affected_passengers"}
PASSENGER_FIELDS = {"passengerId": "key", "inboundFlightId": "inbound_flight_id", "inboundSeat": "inbound_seat",
                    "connectingFlightId": "connecting_flight_id", "connectingSeat": "connecting_seat",
                    "finalDestination": "final_destination", "connectionMinutes": "connection_minutes"}
OFFER_FIELDS = {"offerId": "key", "passengerId": "passenger_id", "recommendedFlightId": "recommended_flight_id",
                "hotelName": "hotel_name", "hotelCost": "hotel_cost", "impactedAt": "impacted_at",
                "recommendedAt": "recommended_at"}
HOTEL_FIELDS = {"hotelName": "key", "availableRooms": "available_rooms", "nightlyRate": "nightly_rate"}


TIME_FIELDS = {"scheduled_time", "estimated_time", "impacted_at", "recommended_at"}


def _rename(row: dict, fields: dict) -> dict:
    """Kafka Streams JSON (camelCase, epoch millis) to Lightning's row shape (snake_case, id in "key", UTC strings)."""
    out = {}
    for name, value in row.items():
        if name == "computedAt":
            continue
        name = fields.get(name, name)
        if name in TIME_FIELDS and isinstance(value, int):
            value = datetime.fromtimestamp(value / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        out[name] = value
    return out


def _millis(value: datetime) -> int:
    return int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)


class KafkaStreamsDeployment:
    """Drop-in for airport_app.Deployment that reads the Kafka Streams app's state stores."""

    def __init__(self, instances=INSTANCES) -> None:
        self.instances = list(instances)
        self.http = requests.Session()

    def _get(self, path: str, **params) -> dict:
        deadline = time.monotonic() + RETRY_FOR_SECONDS
        while True:
            error = "no instance answered"
            for base in self.instances:
                try:
                    resp = self.http.get(base + path, params=params, timeout=3)
                except requests.RequestException as exc:
                    error = f"{base}: {exc}"
                    continue  # that instance is down; any other can answer
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 404:
                    return {}
                error = f"{base}: HTTP {resp.status_code} {resp.text[:200]}"
                if resp.status_code != 503:
                    raise HTTPException(status_code=502, detail=f"Kafka Streams query failed: {error}")
            if time.monotonic() > deadline:
                raise HTTPException(status_code=503, detail=f"Kafka Streams not ready: {error}")
            time.sleep(0.2)  # 503: a task is moving between instances

    def lightning_query(self, topic: str, key: str | None = None, limit: int = 200,
                        where=(), order_by: tuple[str, str] | None = None) -> list[dict]:
        conditions = {(column, op): value for column, op, value in where}
        if topic in ("flight_status", "flight_impact"):
            if key is not None:
                flight = self._get(f"/flights/{key}").get("flight")
                return [_rename(flight, FLIGHT_FIELDS)] if flight else []
            body = self._get("/flights", **{"from": _millis(conditions[("scheduled_time", ">=")]),
                                            "to": _millis(conditions[("scheduled_time", "<")])})
            return [_rename(row, FLIGHT_FIELDS) for row in body["flights"]]
        if topic == "passenger_state" and key is not None:
            body = self._get(f"/passengers/{key}")
            return [_rename({"passengerId": key, **body["passenger"]}, PASSENGER_FIELDS)] if body else []
        if topic == airport_datagen.OFFERS and ("passenger_id", "=") in conditions:
            body = self._get(f"/passengers/{conditions[('passenger_id', '=')]}/offers")
            return [_rename(row, OFFER_FIELDS) for row in body.get("offers", [])]
        if topic == airport_datagen.HOTELS:
            return [_rename(row, HOTEL_FIELDS) for row in self._get("/hotels")["hotels"]]
        raise NotImplementedError(f"No Kafka Streams view for {topic} {key} {list(conditions)}")

    def lightning_scan(self, topic: str, where=()) -> list[dict]:
        conditions = {(column, op): value for column, op, value in where}
        if topic == "passenger_state":
            for leg in ("inbound", "connecting"):
                if (f"{leg}_flight_id", "=") in conditions:
                    flight_id = conditions[(f"{leg}_flight_id", "=")]
                    body = self._get(f"/flights/{flight_id}/passengers", leg=leg)
                    return [_rename(row, PASSENGER_FIELDS) for row in body.get("passengers", [])]
        if topic == airport_datagen.OFFERS and conditions.get(("status", "=")) == "BOOKED":
            body = self._get("/offers/booked", since=_millis(conditions[("recommended_at", ">=")]))
            return [_rename(row, OFFER_FIELDS) for row in body["offers"]]
        raise NotImplementedError(f"No Kafka Streams scan for {topic} {list(conditions)}")


def default_cluster() -> str:
    """The deployed demo when there is one, else the local broker."""
    return "deployed" if (ROOT / "terraform" / "core" / "terraform.tfstate").exists() else "local"


def cluster_env(cluster: str) -> dict[str, str]:
    """Connection settings as the environment variables config/cloud.properties reads."""
    if cluster == "deployed":
        from scripts.terraform import extract_kafka_credentials

        c = extract_kafka_credentials("aws", ROOT)
        return {"BOOTSTRAP_SERVERS": c["bootstrap_servers"], "CLUSTER_API_KEY": c["kafka_api_key"],
                "CLUSTER_API_SECRET": c["kafka_api_secret"], "SCHEMA_REGISTRY_URL": c["schema_registry_url"],
                "SCHEMA_REGISTRY_API_KEY": c["schema_registry_api_key"],
                "SCHEMA_REGISTRY_API_SECRET": c["schema_registry_api_secret"]}
    if cluster == "env":
        return {k: v for k, v in dotenv_values(ENV_FILE).items() if v}
    raise ValueError(f"cluster {cluster!r} has no credentials")


def connection_config(cluster: str) -> tuple[dict, dict]:
    """Kafka client and Schema Registry settings for a cluster."""
    if cluster == "local":
        return LOCAL_KAFKA, LOCAL_SCHEMA_REGISTRY
    env = cluster_env(cluster)
    kafka = {"bootstrap.servers": env["BOOTSTRAP_SERVERS"], "security.protocol": "SASL_SSL",
             "sasl.mechanism": "PLAIN", "sasl.username": env["CLUSTER_API_KEY"],
             "sasl.password": env["CLUSTER_API_SECRET"]}
    registry = {"url": env["SCHEMA_REGISTRY_URL"],
                "basic.auth.user.info": f"{env['SCHEMA_REGISTRY_API_KEY']}:{env['SCHEMA_REGISTRY_API_SECRET']}"}
    return kafka, registry


class KeyPartitionedProducer:
    """A Producer that ignores the generator's partition=0 and partitions by key with murmur2, like Java."""

    def __init__(self, config: dict) -> None:
        self._producer = Producer({**config, "linger.ms": 20, "partitioner": "murmur2_random"})

    def produce(self, topic, key=None, value=None, partition=None, **kwargs):
        self._producer.produce(topic, key=key, value=value, **kwargs)

    def __getattr__(self, name):
        return getattr(self._producer, name)


def publisher_class(cluster: str) -> type[airport_datagen.Publisher]:
    """The generator's Publisher, pointed at a 6-partition test cluster (local or env) and partitioned by key.

    Not for the deployed demo: its topics have one partition and the stock Publisher writes to it, so every
    writer (the generator and both dashboards) uses that one instead and they all partition alike.
    """
    kafka, registry = connection_config(cluster)

    class StreamsPublisher(airport_datagen.Publisher):
        def __init__(self, credentials=None, dry_run: bool = False):
            self.dry_run = dry_run
            self.serializers = {}
            self.string = StringSerializer("utf_8")
            if dry_run:
                return
            self.schema_registry = SchemaRegistryClient(registry)
            self.producer = KeyPartitionedProducer(kafka)

    return StreamsPublisher


def start(cluster: str, instances: int = len(INSTANCES)) -> None:
    """Build the app and start its instances in the background (logs in kstreams/logs/)."""
    running = []
    for base in INSTANCES[:instances]:
        try:
            requests.get(base + "/health", timeout=1)
            running.append(base)
        except requests.RequestException:
            pass
    if running:  # the ports are taken, so new instances would exit at once
        raise SystemExit(f"Kafka Streams instances are already running on {', '.join(running)}; "
                         "stop them first with `uv run kstreams stop`")
    subprocess.run(["./gradlew", "installDist", "-q"], cwd=KSTREAMS, check=True)
    config = "config/local.properties" if cluster == "local" else "config/cloud.properties"
    env = {**os.environ, "CONFIG": os.environ.get("CONFIG", config),  # CONFIG overrides, for experiments
           "STATE_DIR": f"build/state/{cluster}"}  # state stores never cross clusters
    if cluster != "local":
        env.update(cluster_env(cluster))
    (KSTREAMS / "logs").mkdir(exist_ok=True)
    for instance in range(1, instances + 1):
        with open(KSTREAMS / "logs" / f"instance-{instance}.log", "w") as log:
            subprocess.Popen(["./run-instance.sh", str(instance)], cwd=KSTREAMS, env=env, stdin=subprocess.DEVNULL,
                             stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        print(f"  instance {instance}: queries on {INSTANCES[instance - 1]}, log kstreams/logs/instance-{instance}.log")
    deadline = time.monotonic() + 120
    ready = set()
    while len(ready) < instances and time.monotonic() < deadline:
        for base in INSTANCES[:instances]:
            try:
                if requests.get(base + "/health", timeout=2).json().get("state") == "RUNNING":
                    ready.add(base)
            except (requests.RequestException, ValueError):
                pass
        time.sleep(2)
    print(f"✓ Kafka Streams: {len(ready)} of {instances} instances running" if ready
          else "⚠ Kafka Streams did not reach RUNNING in 2 minutes; see kstreams/logs/")


def stop(clear_state: bool = False) -> None:
    """Stop the instances and the Kafka Streams dashboard; optionally delete their local state stores."""
    subprocess.run([str(KSTREAMS / "stop-all.sh")], check=False)
    subprocess.run(["pkill", "-f", "airport-app --engine kstreams"], check=False)
    if clear_state:
        deadline = time.monotonic() + 40  # each instance closes within 30 seconds
        while time.monotonic() < deadline and subprocess.run(
                ["pgrep", "-f", "river.air.instance="], capture_output=True).returncode == 0:
            time.sleep(1)
        shutil.rmtree(KSTREAMS / "build" / "state", ignore_errors=True)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run kstreams", description="Run the Kafka Streams app")
    parser.add_argument("command", choices=("start", "stop"))
    parser.add_argument("--cluster", choices=CLUSTERS, default=None,
                        help="deployed (default when the demo is deployed), local, or env (kstreams/.env)")
    args = parser.parse_args(argv)
    if args.command == "stop":
        stop()
    else:
        start(args.cluster or default_cluster())


if __name__ == "__main__":
    main()
