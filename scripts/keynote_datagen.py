"""Small, deterministic keynote fixture publisher using the deployed schemas."""

import argparse
import json
import logging
import random
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from .logging_utils import setup_logging
from .terraform import extract_kafka_credentials, get_project_root

FLIGHTS = "flight_status"
ITINERARIES = "passenger_connections"
HOTELS = "hotel_inventory"
OFFERS = "passenger_recommendations"
INBOUND = "JA417"
ONWARD = ("JA890", "JA891", "JA892")
HOTEL_IDS = ("Harbor Hotel", "Park Hotel")
PASSENGER_COUNT = 200
HISTORY_DAYS = 30
HISTORY_CONNECTION_BUFFER_MINUTES = 90
HISTORY_DELAY_CHOICES_MINUTES = (10, 20, 35, 50, 70, 90)


def _clock(value: str | None) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if value else datetime.now(timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)


def scenario(now: datetime, seed: int, phase: str):
    """Yield (topic, key, value) tuples for one finite scene phase."""
    if phase == "seed":
        flights = [
            (INBOUND, "ORD", "SFO", 20),
            (ONWARD[0], "SFO", "LAX", 90),
            (ONWARD[1], "SFO", "LAX", 180),
            (ONWARD[2], "SFO", "LAX", 240),
        ]
        for flight_id, origin, destination, minutes in flights:
            at = now + timedelta(minutes=minutes)
            yield FLIGHTS, flight_id, dict(origin=origin, destination=destination,
                                           scheduled_time=at, estimated_time=at, status="ON_TIME")
        for n in range(1, PASSENGER_COUNT + 1):
            yield ITINERARIES, f"P-{n:04d}", dict(
                inbound_flight_id=INBOUND, connecting_flight_id=ONWARD[0])
        for hotel_id, rate in zip(HOTEL_IDS, (Decimal("189.00"), Decimal("219.00")), strict=True):
            yield HOTELS, hotel_id, dict(available_rooms=200, nightly_rate=rate)
    elif phase == "delay":
        yield FLIGHTS, INBOUND, dict(origin="ORD", destination="SFO",
                                    scheduled_time=now + timedelta(minutes=20),
                                    estimated_time=now + timedelta(minutes=65), status="DELAYED")
    elif phase == "offers":
        at = now + timedelta(minutes=66)
        for n in range(1, PASSENGER_COUNT + 1):
            passenger_id = f"P-{n:04d}"
            for choice in (1, 2):
                yield OFFERS, f"{passenger_id}-O{choice}", dict(
                    passenger_id=passenger_id,
                    recommended_flight_id=ONWARD[choice],
                    hotel_id=HOTEL_IDS[choice - 1], status="OFFERED",
                    hotel_cost=Decimal("189.00" if choice == 1 else "219.00"),
                    recommended_at=at, recovered_at=None)
    elif phase == "sellout":
        yield HOTELS, HOTEL_IDS[0], dict(available_rooms=0, nightly_rate=Decimal("189.00"))
    elif phase == "history":
        yield from _history(now, seed)
    else:
        raise ValueError(f"Unknown phase: {phase}")


def _history(now: datetime, seed: int):
    """Backdated flights, connections, and completed offers for the Athena/Quick analytics scene.

    Keys are prefixed HIST-/H- so they never collide with the live scenario's flights
    (INBOUND, ONWARD) or passengers (P-nnnn); the app filters the live dashboard to those
    known live IDs so this backdated volume never shows up there.
    """
    rng = random.Random(seed)
    for day in range(1, HISTORY_DAYS + 1):
        scheduled = (now - timedelta(days=day)).replace(hour=9, minute=0)
        delay_minutes = rng.choice(HISTORY_DELAY_CHOICES_MINUTES)
        estimated = scheduled + timedelta(minutes=delay_minutes)
        inbound_id = f"HIST-{day:03d}-IN"
        onward_id = f"HIST-{day:03d}-ON"
        yield FLIGHTS, inbound_id, dict(
            origin="ORD", destination="SFO", scheduled_time=scheduled, estimated_time=estimated,
            status="DELAYED" if delay_minutes >= 20 else "LANDED")
        onward_scheduled = scheduled + timedelta(minutes=HISTORY_CONNECTION_BUFFER_MINUTES)
        yield FLIGHTS, onward_id, dict(
            origin="SFO", destination="LAX", scheduled_time=onward_scheduled,
            estimated_time=onward_scheduled, status="LANDED")
        connection_minutes = int((onward_scheduled - estimated).total_seconds() // 60)
        for n in range(1, rng.randint(8, 24) + 1):
            passenger_id = f"H-{day:03d}-{n:03d}"
            yield ITINERARIES, passenger_id, dict(
                inbound_flight_id=inbound_id, connecting_flight_id=onward_id)
            if connection_minutes >= 45:
                continue
            recommended_at = estimated + timedelta(minutes=1)
            recovered_at = recommended_at + timedelta(minutes=rng.randint(5, 45))
            booked_index = n % 2
            for choice, index in enumerate((booked_index, 1 - booked_index), start=1):
                yield OFFERS, f"{passenger_id}-O{choice}", dict(
                    passenger_id=passenger_id, recommended_flight_id=onward_id,
                    hotel_id=HOTEL_IDS[index],
                    hotel_cost=Decimal("189.00" if index == 0 else "219.00"),
                    status="BOOKED" if index == booked_index else "CLOSED",
                    recommended_at=recommended_at,
                    recovered_at=recovered_at if index == booked_index else None)


class Publisher:
    def __init__(self, credentials: dict | None, dry_run: bool):
        self.dry_run = dry_run
        self.serializers = {}
        self.string = StringSerializer("utf_8")
        if dry_run:
            return
        credentials = credentials or {}
        self.schema_registry = SchemaRegistryClient({
            "url": credentials["schema_registry_url"],
            "basic.auth.user.info": (
                f"{credentials['schema_registry_api_key']}:{credentials['schema_registry_api_secret']}")})
        self.producer = Producer({
            "bootstrap.servers": credentials["bootstrap_servers"],
            "security.protocol": "SASL_SSL", "sasl.mechanism": "PLAIN",
            "sasl.username": credentials["kafka_api_key"],
            "sasl.password": credentials["kafka_api_secret"],
        })

    def publish(self, topic: str, key: str, value: dict | None) -> None:
        if self.dry_run:
            print(json.dumps({"topic": topic, "key": key, "value": value}, default=str))
            return
        if topic not in self.serializers:
            registered = self.schema_registry.get_latest_version(f"{topic}-value")
            schema = json.loads(registered.schema.schema_str)
            self.serializers[topic] = (
                AvroSerializer(self.schema_registry, registered.schema.schema_str,
                               conf={"auto.register.schemas": False, "use.latest.version": True}),
                {field["name"]: field["type"] for field in schema["fields"]},
            )
        serializer, fields = self.serializers[topic]
        if value is not None:
            value = dict(value)
            for name, field_type in fields.items():
                if name not in value or not isinstance(value[name], datetime):
                    continue
                variants = field_type if isinstance(field_type, list) else [field_type]
                if any(isinstance(t, dict) and t.get("logicalType") == "timestamp-millis" for t in variants):
                    value[name] = value[name].replace(tzinfo=timezone.utc)
        context = SerializationContext(topic, MessageField.VALUE)
        self.producer.produce(topic, key=self.string(key, SerializationContext(topic, MessageField.KEY)),
                              value=serializer(value, context) if value is not None else None, partition=0)
        self.producer.poll(0)

    def flush(self) -> None:
        if not self.dry_run:
            pending = self.producer.flush(30)
            if pending:
                raise RuntimeError(f"{pending} records were not delivered")


def run(now: datetime, seed: int, phases: list[str], dry_run: bool, pause: float) -> None:
    credentials = None if dry_run else extract_kafka_credentials("aws", get_project_root())
    publisher = Publisher(credentials, dry_run)
    for index, phase in enumerate(phases):
        if index and pause:
            time.sleep(pause)
        count = 0
        for topic, key, value in scenario(now, seed, phase):
            publisher.publish(topic, key, value)
            count += 1
        publisher.flush()
        logging.info("%s: published %d records", phase, count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Finite keynote flight recovery fixture")
    parser.add_argument(
        "--phase", choices=["seed", "delay", "offers", "sellout", "history", "all"], default="all")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--now", help="ISO-8601 UTC scene clock")
    parser.add_argument("--pause", type=float, default=15.0, help="Seconds between phases")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reset", action="store_true", help="Write tombstones for the fixture keys")
    args = parser.parse_args()
    setup_logging()
    now = _clock(args.now)
    if args.reset:
        credentials = None if args.dry_run else extract_kafka_credentials("aws", get_project_root())
        publisher = Publisher(credentials, args.dry_run)
        for topic in (FLIGHTS, ITINERARIES, HOTELS, OFFERS):
            keys = {key for phase in ("seed", "delay", "offers", "sellout", "history")
                    for row_topic, key, _ in scenario(now, args.seed, phase) if row_topic == topic}
            for key in sorted(keys):
                publisher.publish(topic, key, None)
        publisher.flush()
        return
    phases = ["seed", "delay", "offers", "sellout"] if args.phase == "all" else [args.phase]
    run(now, args.seed, phases, args.dry_run, args.pause)


if __name__ == "__main__":
    main()
