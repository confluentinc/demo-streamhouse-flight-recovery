#!/usr/bin/env python3
"""
Airport-disruption datagen — the JA417 connection-cascade scenario.

Feeds the four source topics the airport Flink pipeline reads:
  flight_updates, passenger_itineraries, rebooking_inventory, gate_crew_status

The story is one inbound flight (JA417) whose arrival slips, cascading into the
onward connections its passengers are booked on:

  Phase "seed"  — steady state. JA417 arrives early enough that every passenger
                  makes their connection (passenger_journey.risk = OK).
  Phase "slip"  — JA417's flight_updates row is re-produced with a later
                  estimated_arrival. Because the source topics are upsert-keyed by
                  natural id, this UPDATES the flight in place; Flink recomputes
                  passenger_journey and the at-risk passengers flip to MISS / TIGHT,
                  which is what fires the recovery streaming agent.
  Phase "pivot" — JA512's departure moves later before approval. Maya changes
                  from MISS to TIGHT; the changed passenger_journey row retriggers
                  the agent, which drops the overnight plan and expedites her.

`uv run airport-datagen`            seed, wait --slip-delay, then slip (full demo run)
`uv run airport-datagen --phase seed`   just the steady state
`uv run airport-datagen --phase slip`   just the slip (JA417 re-produced later)
`uv run airport-datagen --phase pivot`  move JA512 later (plot twist)
`uv run airport-datagen --reset`        delete-records the 4 source topics, then exit
`uv run airport-datagen --dry-run`      print what would be produced, connect to nothing

Deterministic + clock-controllable per the repo's datagen contract:
  --seed   seeds any jitter (default 42); the scenario itself is fully scripted.
  --now    ISO-8601 base "now" all timestamps rebase to (default: real now).

Schema handling: the Flink CREATE TABLE statements register each topic's Avro value
schema in Schema Registry at deploy time, so this datagen FETCHES the registered
`<topic>-value` schema and serializes against it (auto.register.schemas=False). That
keeps the generator correct no matter how Flink chose to lay out the value record
(e.g. whether the primary-key column is included), and it adapts to the timestamp
logical type Flink picked. Keys are plain strings (the tables pin key.format=raw).
"""

import argparse
import json
import logging
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import Any

try:
    from confluent_kafka import Producer
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

    CONFLUENT_KAFKA_AVAILABLE = True
except ImportError:
    CONFLUENT_KAFKA_AVAILABLE = False

from .logging_utils import setup_logging
from .terraform import extract_kafka_credentials, get_project_root

# Source topics this datagen owns. Serving tables (passenger_journey, etc.) are
# maintained by Flink and are never written here.
SOURCE_TOPICS = [
    "flight_updates",
    "passenger_itineraries",
    "rebooking_inventory",
    "gate_crew_status",
]

# ---------------------------------------------------------------------------
# The JA417 scenario, expressed as minute-offsets from the base clock ("now").
# The connection rule Flink applies (see 08-insert-passenger-journey.sql):
#   make_connection = inbound.arrival + 30min <= connecting.departure
#   risk = MISS  if inbound.arrival + 30min >  connecting.departure
#          TIGHT if (departure - arrival) < 45min
#          OK    otherwise
# ---------------------------------------------------------------------------

INBOUND_FLIGHT = "JA417"
INBOUND_GATE = "A12"
INBOUND_ROUTE = "ORD-SFO"

# JA417 inbound arrival, minutes from "now": early at seed, slipped later at slip.
INBOUND_ARRIVAL_SEED_MIN = 20
DEFAULT_SLIP_MINUTES = 35  # slipped arrival = 20 + 35 = 55 min from now

# Onward departures out of the SFO hub. Chosen so every passenger is OK at seed
# (min headroom 46 min) and the 35-min slip pushes three to MISS and one to TIGHT.
#   flight   dest  departure(min)  gate   crew_margin  alt_gate  bags   post-slip
CONNECTING_FLIGHTS = [
    # flight, destination, dep_min, gate, crew_margin_min, alt_gate, bag_team, next_flight, next_dep_min, hotel
    ("JA890", "LAX", 80, "B7",  20, None,  True,  "JA905", 200, "Marriott LAX"),        # -> HOLD  (MISS)
    ("JA631", "SEA", 70, "B9",   5, "B10", True,  "JA655", 175, "Grand Hyatt SEA"),     # -> REGATE(MISS)
    ("JA512", "PDX", 66, "C3",   5, None,  False, "JA540", 190, "Kimpton RiverPlace"),  # -> PRIORITY_BAGS (MISS)
    ("JA742", "SAN", 95, "C5",  25, None,  True,  "JA760", 240, "Pendry San Diego"),    # -> HOLD  (TIGHT)
]

# Passengers, all inbound on JA417. (id, name, connecting_flight, checked_bags, international)
# `id` (P-10xx) is the unique key everything joins on; `name` is the friendly display
# name every consumer sees (ops app, passenger card, RTCE agent, Iceberg). Synthetic —
# no real PII. Spread across the four onward flights with a mix of bag/international
# status so bag_status and needs_recheck vary in passenger_journey.
PASSENGERS = [
    ("P-1001", "James Okoro",    "JA890", 1, False),
    ("P-1002", "Sofia Ramirez",  "JA890", 0, False),
    ("P-1003", "Aditya Nair",    "JA890", 2, True),
    ("P-1004", "Grace Kim",      "JA631", 0, False),
    ("P-1005", "Daniel Weber",   "JA631", 1, False),
    ("P-1006", "Amara Diallo",   "JA631", 1, True),
    ("P-1007", "Liam Murphy",    "JA512", 0, False),
    ("P-1008", "Priya Menon",    "JA512", 2, False),
    ("P-1009", "Maya Chen",      "JA512", 1, False),
    ("P-1010", "Noah Andersen",  "JA742", 0, False),
    ("P-1011", "Chloe Dubois",   "JA742", 1, False),
    ("P-1012", "Omar Haddad",    "JA742", 3, True),
]

# Plot twist: before Maya's overnight recovery can be approved, a ground-flow
# delay moves her original JA512 departure from +66 to +91 minutes. Her connection
# window grows from 11 to 36 minutes, so Flink changes her risk from MISS to TIGHT
# and the streaming agent replaces REBOOK with EXPEDITE.
PIVOT_FLIGHT = "JA512"
PIVOT_DESTINATION = "PDX"
PIVOT_DEPARTURE_MIN = 91
PIVOT_GATE = "C3"


def _dest_of(connecting_flight: str) -> str:
    return next(c[1] for c in CONNECTING_FLIGHTS if c[0] == connecting_flight)


class AirportDatagen:
    """Produces the JA417 scenario to the four source topics."""

    def __init__(self, creds: dict, base_now: datetime, dry_run: bool = False):
        self.log = logging.getLogger(__name__)
        self.base_now = base_now
        self.dry_run = dry_run
        self.creds = creds
        self.bootstrap = creds.get("bootstrap_servers", "")
        self.string_serializer = StringSerializer("utf_8") if CONFLUENT_KAFKA_AVAILABLE else None
        self._sr = None
        self._serializers: dict[str, AvroSerializer] = {}
        self._producer = None

    # --- clock ------------------------------------------------------------
    def at(self, minutes_from_now: int) -> datetime:
        """A wall-clock timestamp `minutes_from_now` after the base clock (naive UTC)."""
        return self.base_now + timedelta(minutes=minutes_from_now)

    # --- kafka / schema registry -----------------------------------------
    def _connect(self) -> None:
        if self.dry_run or self._producer is not None:
            return
        sr_auth = f"{self.creds['schema_registry_api_key']}:{self.creds['schema_registry_api_secret']}"
        self._sr = SchemaRegistryClient(
            {
                "url": self.creds["schema_registry_url"],
                "basic.auth.user.info": sr_auth,
            }
        )
        # Plain Producer: keys and values are serialized here (each topic has its
        # own registered value schema, so a single value.serializer wouldn't fit).
        self._producer = Producer(
            {
                "bootstrap.servers": self.bootstrap,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.creds["kafka_api_key"],
                "sasl.password": self.creds["kafka_api_secret"],
            }
        )

    def _serializer_for(self, topic: str) -> AvroSerializer:
        """Fetch the Flink-registered <topic>-value schema and build a serializer for it."""
        if topic in self._serializers:
            return self._serializers[topic]
        subject = f"{topic}-value"
        registered = self._sr.get_latest_version(subject)
        schema_str = registered.schema.schema_str
        ser = AvroSerializer(
            self._sr,
            schema_str,
            conf={"auto.register.schemas": False, "use.latest.version": True},
        )
        # remember which fields are timestamp-logical so we coerce datetimes right
        self._coercers = getattr(self, "_coercers", {})
        self._coercers[topic] = _timestamp_coercers(schema_str)
        self._serializers[topic] = ser
        self.log.info("Loaded registered schema for %s (id=%s)", subject, registered.schema_id)
        return ser

    def _produce(self, topic: str, key: str, value: dict) -> None:
        """Coerce datetimes to the schema's logical type, then produce one record."""
        if self.dry_run:
            printable = {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in value.items()}
            print(f"  [{topic}] {key} -> {json.dumps(printable, default=str)}")
            return
        ser = self._serializer_for(topic)
        coerced = dict(value)
        for field, coerce in self._coercers.get(topic, {}).items():
            if field in coerced and isinstance(coerced[field], datetime):
                coerced[field] = coerce(coerced[field])
        value_bytes = ser(coerced, SerializationContext(topic, MessageField.VALUE))
        key_bytes = self.string_serializer(key, SerializationContext(topic, MessageField.KEY))
        self._producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            partition=0,  # every source topic is 1 bucket / 1 partition
        )
        self._producer.poll(0)

    def _flush(self) -> None:
        if not self.dry_run and self._producer is not None:
            self._producer.flush(30)

    # --- scenario phases --------------------------------------------------
    def seed(self, slip_minutes: int) -> None:
        """Steady state: JA417 early, all connections OK; plus static reference data."""
        self._connect()
        arrival = self.at(INBOUND_ARRIVAL_SEED_MIN)
        self.log.info("Seeding: JA417 arrival %s (on time), %d passengers", arrival.isoformat(), len(PASSENGERS))

        # flight_updates: the inbound flight (early) + every onward departure
        self._produce("flight_updates", INBOUND_FLIGHT, {
            "route": INBOUND_ROUTE,
            "estimated_arrival": arrival,
            "estimated_departure": None,
            "gate": INBOUND_GATE,
        })
        for flight, dest, dep_min, gate, *_ in CONNECTING_FLIGHTS:
            self._produce("flight_updates", flight, {
                "route": f"SFO-{dest}",
                "estimated_arrival": None,
                "estimated_departure": self.at(dep_min),
                "gate": gate,
            })

        # passenger_itineraries: everyone inbound on JA417
        for pid, name, connecting, bags, intl in PASSENGERS:
            self._produce("passenger_itineraries", pid, {
                "passenger_name": name,
                "inbound_flight": INBOUND_FLIGHT,
                "connecting_flight": connecting,
                "destination": _dest_of(connecting),
                "checked_bags": bags,
                "international": intl,
            })

        # rebooking_inventory: keyed by destination (concierge agent joins this)
        for c in CONNECTING_FLIGHTS:
            dest, next_flight, next_dep_min, hotel = c[1], c[7], c[8], c[9]
            self._produce("rebooking_inventory", dest, {
                "next_flight": next_flight,
                "next_departure": self.at(next_dep_min),
                "hotel": hotel,
            })

        # gate_crew_status: keyed by onward flight_id (ops spoke joins this)
        for flight, _dest, _dep, _gate, crew_margin, alt_gate, bag_team, *_ in CONNECTING_FLIGHTS:
            self._produce("gate_crew_status", flight, {
                "crew_duty_margin_min": crew_margin,
                "alt_gate": alt_gate,
                "bag_team_available": bag_team,
            })

        self._flush()
        self.log.info("Seed complete.")

    def slip(self, slip_minutes: int) -> None:
        """Re-produce JA417 with a later arrival — the cascade trigger."""
        self._connect()
        slipped = self.at(INBOUND_ARRIVAL_SEED_MIN + slip_minutes)
        self.log.info(
            "SLIP: JA417 arrival now %s (+%d min) — connections will flip to MISS/TIGHT",
            slipped.isoformat(), slip_minutes,
        )
        self._produce("flight_updates", INBOUND_FLIGHT, {
            "route": INBOUND_ROUTE,
            "estimated_arrival": slipped,
            "estimated_departure": None,
            "gate": INBOUND_GATE,
        })
        self._flush()
        self.log.info("Slip produced. Watch passenger_journey.risk and passenger_recovery.")

    def pivot(self) -> None:
        """Move JA512 later so the agent must revise Maya's recovery."""
        self._connect()
        self.log.info(
            "PIVOT: %s departure moved later — Maya changes from MISS to TIGHT",
            PIVOT_FLIGHT,
        )
        self._produce(
            "flight_updates",
            PIVOT_FLIGHT,
            {
                "route": f"SFO-{PIVOT_DESTINATION}",
                "estimated_arrival": None,
                "estimated_departure": self.at(PIVOT_DEPARTURE_MIN),
                "gate": PIVOT_GATE,
            },
        )
        self._flush()
        self.log.info(
            "Pivot produced. Watch Maya's passenger_journey change to TIGHT and recovery to EXPEDITE."
        )

    # --- reset ------------------------------------------------------------
    def reset(self) -> None:
        """delete-records on the four source topics for an idempotent restart."""
        if self.dry_run:
            for t in SOURCE_TOPICS:
                print(f"  [reset] would delete-records on {t}")
            return
        admin = AdminClient(
            {
                "bootstrap.servers": self.bootstrap,
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.creds["kafka_api_key"],
                "sasl.password": self.creds["kafka_api_secret"],
            }
        )
        _delete_all_records(admin, SOURCE_TOPICS, self.log)


def _timestamp_coercers(schema_str: str) -> dict[str, Callable[[datetime], datetime]]:
    """Map each timestamp field -> a fn coercing a naive-UTC datetime to its logical type.

    Flink emits TIMESTAMP(3) as `local-timestamp-millis` (naive wall clock) and
    TIMESTAMP_LTZ as `timestamp-millis` (tz-aware). fastavro needs the matching
    Python datetime flavour, so we read the logical type straight from the schema.
    """
    coercers: dict[str, Callable[[datetime], datetime]] = {}
    schema = json.loads(schema_str)
    for field in schema.get("fields", []):
        logical = _logical_type_of(field.get("type"))
        if logical == "timestamp-millis":
            coercers[field["name"]] = lambda dt: dt.replace(tzinfo=timezone.utc)
        elif logical == "local-timestamp-millis":
            coercers[field["name"]] = lambda dt: dt.replace(tzinfo=None)
    return coercers


def _logical_type_of(field_type: Any) -> str | None:
    """Pull a timestamp logicalType out of a field type that may be a union/list."""
    if isinstance(field_type, dict):
        return field_type.get("logicalType")
    if isinstance(field_type, list):  # union, e.g. ["null", {...timestamp...}]
        for member in field_type:
            lt = _logical_type_of(member)
            if lt:
                return lt
    return None


def _delete_all_records(admin, topics: list, log) -> None:
    """Truncate each topic by asking the broker to delete up to the high watermark."""
    from confluent_kafka import TopicPartition
    from confluent_kafka.admin import OFFSET_END

    # Confirm the topics exist before trying to truncate.
    md = admin.list_topics(timeout=15)
    present = [t for t in topics if t in md.topics]
    missing = [t for t in topics if t not in md.topics]
    for t in missing:
        log.warning("Topic %s not found (never created?) — skipping reset for it", t)

    partitions = [TopicPartition(t, 0, OFFSET_END) for t in present]
    if not partitions:
        return
    futures = admin.delete_records(partitions)
    for tp, fut in futures.items():
        try:
            fut.result(timeout=30)
            log.info("Reset %s (records deleted up to end)", tp.topic)
        except Exception as e:
            log.warning("delete-records failed for %s: %s", tp.topic, e)


def _parse_now(value: str | None) -> datetime:
    """Base clock: --now ISO-8601 (naive UTC) or real now truncated to the minute."""
    if value:
        dt = datetime.fromisoformat(value)
        return dt.replace(tzinfo=None, second=0, microsecond=0)
    return datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Airport-disruption (JA417) datagen")
    parser.add_argument(
        "--phase",
        choices=["seed", "slip", "pivot", "all"],
        default="all",
        help=(
            "seed = steady state; slip = re-produce JA417 late; "
            "pivot = move JA512 later; all = seed, wait, slip (default)"
        ),
    )
    parser.add_argument(
        "--reset", action="store_true", help="delete-records the 4 source topics, then exit"
    )
    parser.add_argument(
        "--slip-minutes", type=int, default=DEFAULT_SLIP_MINUTES, help="how far JA417 slips (default 35)"
    )
    parser.add_argument(
        "--slip-delay", type=int, default=45, help="seconds between seed and slip in --phase all (default 45)"
    )
    parser.add_argument(
        "--now", help="ISO-8601 base clock all timestamps rebase to (default: real now)"
    )
    parser.add_argument(
        "--seed", type=int, default=42, help="RNG seed for reproducibility (default 42)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="print records instead of producing; no cluster"
    )
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    args = parser.parse_args()

    setup_logging(verbose=args.verbose)
    log = logging.getLogger(__name__)

    if not CONFLUENT_KAFKA_AVAILABLE and not args.dry_run:
        log.error("confluent-kafka not installed. Run `uv sync`, or use --dry-run.")
        sys.exit(1)

    import random

    random.seed(args.seed)

    creds: dict = {}
    if not args.dry_run:
        try:
            creds = extract_kafka_credentials("aws", get_project_root())
        except Exception as e:
            log.error("Could not read Kafka credentials from terraform state: %s", e)
            log.error("Deploy first (`uv run deploy`), or use --dry-run.")
            sys.exit(1)

    gen = AirportDatagen(creds, _parse_now(args.now), dry_run=args.dry_run)

    if args.reset:
        log.info("Resetting source topics...")
        gen.reset()
        log.info("Reset complete.")
        return

    if args.phase in ("seed", "all"):
        gen.seed(args.slip_minutes)

    if args.phase == "all":
        log.info("Waiting %ds before the slip (let passenger_journey settle to OK)...", args.slip_delay)
        if not args.dry_run:
            time.sleep(args.slip_delay)

    if args.phase in ("slip", "all"):
        gen.slip(args.slip_minutes)

    if args.phase == "pivot":
        gen.pivot()


if __name__ == "__main__":
    main()
