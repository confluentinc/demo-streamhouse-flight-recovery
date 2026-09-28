"""River Air flight recovery data: 30 days of history plus one live day at the SFO hub.

Each service day has 180 flights: 90 arrivals into SFO and 90 departures from it.
The live day is shifted so flight RA417 from Chicago is scheduled 50 minutes after the
stream starts. Its delay is announced in three growing steps, and 200 of its 260
passengers miss their connection. Every active flight publishes its status once a
minute. Announced delays only grow (a severe delay may shrink by at most three minutes
near landing), so a flight never goes from hours late back to on time.

The generator writes two rebooking offers a few seconds after a connection becomes HIGH
risk. Past bookings are completed the way customers would complete them; RA417's offers
stay open for the presenter. When the recovery agent runs in Flink (sql/26-30), it writes
the live day's offers and the generator writes only history offers.

    uv run airport-datagen                  # history + live day, then stream 90 minutes
    uv run airport-datagen --offers generator  # write today's offers even if the agent runs
    uv run airport-datagen --minutes 0      # publish the current state and exit
    uv run airport-datagen --dry-run --skip-history --minutes 5
    uv run airport-datagen --reset          # tombstone every generated key
"""

import argparse
import json
import logging
import os
import random
import signal
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from .logging_utils import setup_logging
from .terraform import extract_kafka_credentials, get_project_root

FLIGHTS = "flight_status"
ITINERARIES = "passenger_itineraries"
HOTELS = "hotel_inventory"
OFFERS = "passenger_recommendations"

AIRLINE = "RA"
HUB = "SFO"
SPOKES = ("ORD", "SEA", "LAX", "DEN", "PHX", "DFW", "ATL", "JFK", "BOS", "MSP", "LAS", "SAN", "PDX", "SLC", "IAH")

# Daily template, in minutes after midnight. 90 arrivals from 06:00 to 23:00; 75
# departures from 06:30 to 22:00 plus a last bank of 15, one to each destination.
ARRIVAL_MINUTES = tuple(360 + round(i * 1020 / 89) for i in range(90))
DEPARTURE_MINUTES = (
    *(390 + round(i * 930 / 74) for i in range(75)),
    1330, 1335, 1340, 1345, 1350, 1356, 1362, 1368, 1374, 1380, 1386, 1392, 1398, 1404, 1410)
LAST_BANK = 75

HERO_NUMBER = 417
HERO_ORIGIN = "ORD"
HERO_MINUTE = 21 * 60 + 40
HERO_LEAD = timedelta(minutes=50)
# (minutes relative to the scheduled arrival, announced delay in minutes)
HERO_REVEALS = ((-45, 30), (-35, 75), (-25, 115))
HERO_CONNECTING = 200
HERO_LOCAL = 60
HERO_BUFFER = (50, 110)

CONNECTION_MINUTES = 45  # Flink's HIGH threshold in sql/24-serving-passenger-state.sql
CONNECTING_SHARE = 0.3
CONNECTION_BUFFER = (60, 180)
PASSENGERS_PER_FLIGHT = (120, 180)
SEVERE_PER_DAY = 10
SEVERE_DELAY = (90, 240)
MODERATE_SHARE = 0.25
MODERATE_DELAY = (15, 60)
DELAYED_AT = 15
ACTIVE_BEFORE = timedelta(minutes=180)
BOARDING = timedelta(minutes=30)

HOTEL_RATES = {"Harbor Hotel": Decimal("189.00"), "Park Hotel": Decimal("219.00")}
HARBOR_ROOMS = 40
PARK_ROOMS = 150
SELLOUT = timedelta(minutes=40)
OVERNIGHT = timedelta(hours=6)
AGENT_LATENCY_SECONDS = (5, 40)
BOOKING_SHARE = 0.85
DECISION_SECONDS = (3 * 60, 90 * 60)
OFFER_EXPIRY = timedelta(minutes=90)

HISTORY_DAYS = 30
STREAM_MINUTES = 90
SEED = 42


@dataclass
class Flight:
    key: str
    number: int
    origin: str
    destination: str
    scheduled: datetime
    offset: int = 0  # seconds after each minute this flight publishes
    reveals: list[tuple[datetime, int]] = field(default_factory=list)  # (published at, delay minutes)

    @property
    def arrival(self) -> bool:
        return self.destination == HUB

    def delay_at(self, at: datetime) -> int:
        delay = 0
        for when, minutes in self.reveals:
            if when <= at:
                delay = minutes
        return delay

    def estimated_at(self, at: datetime = datetime.max) -> datetime:
        return self.scheduled + timedelta(minutes=self.delay_at(at))

    def status_at(self, at: datetime) -> str:
        estimated = self.estimated_at(at)
        if at >= estimated:
            return "LANDED" if self.arrival else "DEPARTED"
        if not self.arrival and at >= estimated - BOARDING:
            return "BOARDING"
        return "DELAYED" if self.delay_at(at) >= DELAYED_AT else "ON_TIME"

    def row(self, at: datetime) -> dict:
        return dict(origin=self.origin, destination=self.destination, scheduled_time=self.scheduled,
                    estimated_time=self.estimated_at(at), status=self.status_at(at))

    def ticks(self, after: datetime, until: datetime):
        """Once-a-minute update times in (after, until], from three hours out to completion."""
        tick = self.scheduled - ACTIVE_BEFORE + timedelta(seconds=self.offset)
        last = self.estimated_at() + timedelta(seconds=self.offset)
        if tick <= after:
            tick += timedelta(minutes=(after - tick) // timedelta(minutes=1) + 1)
        while tick <= min(last, until):
            yield tick
            tick += timedelta(minutes=1)


@dataclass
class Passenger:
    key: str
    inbound: Flight
    connecting: Flight | None
    impacted_at: datetime | None = None


@dataclass
class Recovery:
    """The agent's two offers for one HIGH-risk passenger and what the passenger did with them."""

    passenger: Passenger
    options: list[Flight]
    hotels: list[str | None]
    recommended_at: datetime
    resolved_at: datetime
    booked: int | None  # index of the booked option; None if the passenger never chose
    completes: bool = True  # False leaves the offers open for the presenter

    def rows(self, at: datetime):
        if at < self.recommended_at:
            return
        final = self.completes and at >= self.resolved_at
        for index, (flight, hotel) in enumerate(zip(self.options, self.hotels, strict=True)):
            status = "OFFERED"
            if final:
                status = "BOOKED" if index == self.booked else "CLOSED"
            yield f"{self.passenger.key}-O{index + 1}", dict(
                passenger_id=self.passenger.key, recommended_flight_id=flight.key, hotel_name=hotel,
                status=status, hotel_cost=HOTEL_RATES[hotel] if hotel else None,
                impacted_at=self.passenger.impacted_at, recommended_at=self.recommended_at)


@dataclass
class Day:
    service_date: date
    live: bool
    arrivals: list[Flight]
    departures: list[Flight]
    passengers: list[Passenger]

    @property
    def flights(self) -> list[Flight]:
        return self.arrivals + self.departures

    @property
    def hero(self) -> Flight:
        return self.arrivals[ARRIVAL_MINUTES.index(HERO_MINUTE)]


@dataclass
class Plan:
    start: datetime
    days: list[Day]  # oldest history first, live day last
    tomorrow: Day
    recoveries: list[Recovery]

    @property
    def live(self) -> Day:
        return self.days[-1]


def _clock(value: str | None) -> datetime:
    """Stream start as naive UTC, trimmed to the minute."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")) if value else datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)


def _delays(flight: Flight, steps) -> None:
    """Announce delays at (minutes from schedule, delay) steps, on this flight's update tick."""
    flight.reveals = [(flight.scheduled + timedelta(minutes=rel, seconds=flight.offset), delay)
                      for rel, delay in steps]


def _assign_delays(day: Day, rng: random.Random) -> None:
    hero = day.hero if day.live else None
    forced = {f.key for f in day.departures[LAST_BANK:]} if day.live else set()
    pool = [f for f in day.flights if f is not hero and f.key not in forced]
    severe = {f.key for f in rng.sample(pool, SEVERE_PER_DAY - (1 if hero else 0))}
    for flight in day.flights:
        if flight is hero:
            _delays(flight, HERO_REVEALS)
        elif flight.key in forced:
            continue
        elif flight.key in severe:
            delay = rng.randint(*SEVERE_DELAY)
            steps = [(-120, round(delay * 0.3)), (-80, round(delay * 0.65)), (-40, delay)]
            makeup = rng.randint(0, 3) if flight.arrival else 0
            if makeup:
                steps.append((delay - 20, delay - makeup))
            _delays(flight, steps)
        elif rng.random() < MODERATE_SHARE:
            delay = rng.randint(*MODERATE_DELAY)
            _delays(flight, [(-90, delay // 2), (-45, delay)])
        else:
            delay = rng.randint(-8 if flight.arrival else 0, 12)
            if delay:
                _delays(flight, [(-30 if flight.arrival else -40, delay)])


def _risk_flip(inbound: Flight, onward: Flight) -> tuple[bool, datetime | None]:
    """Whether the connection's risk changes at most once (OK -> HIGH), and when it does."""
    def at_risk(at):
        return onward.estimated_at(at) - inbound.estimated_at(at) < timedelta(minutes=CONNECTION_MINUTES)

    if at_risk(datetime.min):
        return False, None
    flip = None
    for when in sorted({w for w, _ in inbound.reveals} | {w for w, _ in onward.reveals}):
        risky = at_risk(when)
        if flip is None and risky:
            flip = when
        elif flip is not None and not risky:
            return False, None
    return True, flip


def _passengers(day: Day, rng: random.Random) -> list[Passenger]:
    passengers = []
    for flight in day.arrivals:
        hero = day.live and flight is day.hero
        if hero:
            plan = [True] * HERO_CONNECTING + [False] * HERO_LOCAL
            rng.shuffle(plan)
            low, high = HERO_BUFFER
        else:
            plan = [rng.random() < CONNECTING_SHARE for _ in range(rng.randint(*PASSENGERS_PER_FLIGHT))]
            low, high = CONNECTION_BUFFER
        candidates = [d for d in day.departures if d.destination != flight.origin
                      and timedelta(minutes=low) <= d.scheduled - flight.scheduled <= timedelta(minutes=high)]
        for seq, connects in enumerate(plan, start=1):
            passenger = Passenger(f"P-{day.service_date:%m%d}-{flight.number:03d}-{seq:03d}", flight, None)
            if connects and candidates:
                for onward in rng.sample(candidates, min(3, len(candidates))):
                    valid, flip = _risk_flip(flight, onward)
                    if valid:
                        passenger.connecting, passenger.impacted_at = onward, flip
                        break
            passengers.append(passenger)
    return passengers


def build_day(seed: int, service_date: date, midnight: datetime, live: bool = False,
              schedule_only: bool = False) -> Day:
    """One service day. The same seed, date, and variant always produce the same keys and values."""
    variant = "live" if live else "schedule" if schedule_only else "history"
    rng = random.Random(f"{seed}:{service_date.isoformat()}:{variant}")
    suffix = f"{service_date:%Y%m%d}"
    numbers = iter(range(101, 101 + len(ARRIVAL_MINUTES)))
    arrivals = []
    for i, minute in enumerate(ARRIVAL_MINUTES):
        hero = minute == HERO_MINUTE
        number = HERO_NUMBER if hero else next(numbers)
        arrivals.append(Flight(f"{AIRLINE}{number}-{suffix}", number, HERO_ORIGIN if hero else SPOKES[i % 15],
                               HUB, midnight + timedelta(minutes=minute), 0 if hero else rng.randrange(60)))
    departures = [Flight(f"{AIRLINE}{601 + i}-{suffix}", 601 + i, HUB, SPOKES[i % 15],
                         midnight + timedelta(minutes=minute), rng.randrange(60))
                  for i, minute in enumerate(DEPARTURE_MINUTES)]
    day = Day(service_date, live, arrivals, departures, [])
    if not schedule_only:
        _assign_delays(day, rng)
        day.passengers = _passengers(day, rng)
    return day


def _recoveries(day: Day, next_day: Day, seed: int, sellout_at: datetime | None) -> list[Recovery]:
    rng = random.Random(f"{seed}:{day.service_date.isoformat()}:{'live' if day.live else 'history'}:recovery")
    by_destination: dict[str, list[Flight]] = {}
    for flight in day.departures + next_day.departures:
        by_destination.setdefault(flight.destination, []).append(flight)

    def harbor_open(at):
        return sellout_at is None or at < sellout_at

    recoveries = []
    for passenger in day.passengers:
        if passenger.impacted_at is None:
            continue
        landed = passenger.inbound.estimated_at()
        ready = landed + timedelta(minutes=CONNECTION_MINUTES)
        options = [f for f in by_destination[passenger.connecting.destination] if f.scheduled >= ready][:2]
        if not options:
            continue
        recommended_at = passenger.impacted_at + timedelta(seconds=rng.randint(*AGENT_LATENCY_SECONDS))
        hotels = []
        for index, flight in enumerate(options):
            if flight.scheduled - landed < OVERNIGHT:
                hotels.append(None)
            else:
                hotels.append("Harbor Hotel" if index == 0 and harbor_open(recommended_at) else "Park Hotel")
        books = rng.random() < BOOKING_SHARE
        decided_at = recommended_at + timedelta(seconds=rng.randint(*DECISION_SECONDS))
        choice = 0 if rng.random() < 0.7 or len(options) == 1 else 1
        if books and hotels[choice] == "Harbor Hotel" and not harbor_open(decided_at) and len(options) > 1:
            choice = 1 - choice
        recoveries.append(Recovery(
            passenger, options, hotels, recommended_at,
            resolved_at=decided_at if books else recommended_at + OFFER_EXPIRY,
            booked=choice if books else None,
            completes=not (day.live and passenger.inbound is day.hero)))
    return recoveries


def build_plan(start: datetime, seed: int = SEED, history: bool = True) -> Plan:
    """History days, the live day starting at `start`, and tomorrow's schedule for overnight rebookings."""
    midnight = start + HERO_LEAD - timedelta(minutes=HERO_MINUTE)
    today = start.date()
    days = [build_day(seed, today - timedelta(days=back), midnight - timedelta(days=back))
            for back in range(HISTORY_DAYS if history else 0, 0, -1)]
    days.append(build_day(seed, today, midnight, live=True))
    tomorrow = build_day(seed, today + timedelta(days=1), midnight + timedelta(days=1), schedule_only=True)
    recoveries = []
    for day, next_day in zip(days, [*days[1:], tomorrow], strict=True):
        recoveries += _recoveries(day, next_day, seed, start + SELLOUT if day.live else None)
    return Plan(start, days, tomorrow, recoveries)


def _harbor(rooms: int) -> dict:
    return dict(available_rooms=rooms, nightly_rate=HOTEL_RATES["Harbor Hotel"])


def initial_records(plan: Plan):
    """Every row as of the stream start: history final state, the live day so far, tomorrow's schedule."""
    at = plan.start
    yield HOTELS, "Harbor Hotel", _harbor(HARBOR_ROOMS)
    yield HOTELS, "Park Hotel", dict(available_rooms=PARK_ROOMS, nightly_rate=HOTEL_RATES["Park Hotel"])
    for day in [*plan.days, plan.tomorrow]:
        for flight in day.flights:
            yield FLIGHTS, flight.key, flight.row(at)
    for day in plan.days:
        for passenger in day.passengers:
            # History keeps only connecting passengers; nobody else can be impacted.
            if day.live or passenger.connecting:
                yield ITINERARIES, passenger.key, dict(
                    inbound_flight_id=passenger.inbound.key,
                    connecting_flight_id=passenger.connecting.key if passenger.connecting else None)
    for recovery in plan.recoveries:
        for key, value in recovery.rows(at):
            yield OFFERS, key, value


def stream_events(plan: Plan, minutes: int) -> list[tuple[datetime, str, str, dict]]:
    """Timed live updates after the stream start: flights, the hotel sellout, and offers."""
    start, end = plan.start, plan.start + timedelta(minutes=minutes)
    events = []
    for flight in plan.live.flights:
        events += [(tick, FLIGHTS, flight.key, flight.row(tick)) for tick in flight.ticks(start, end)]
    for minute in range(1, int(SELLOUT / timedelta(minutes=1)) + 1):
        at = start + timedelta(minutes=minute)
        if at <= end:
            events.append((at, HOTELS, "Harbor Hotel",
                           _harbor(HARBOR_ROOMS - HARBOR_ROOMS * minute // int(SELLOUT.total_seconds() // 60))))
    for recovery in plan.recoveries:
        if recovery.passenger.inbound not in plan.live.arrivals:
            continue
        moments = [recovery.recommended_at] + ([recovery.resolved_at] if recovery.completes else [])
        for at in moments:
            if start < at <= end:
                events += [(at, OFFERS, key, value) for key, value in recovery.rows(at)]
    events.sort(key=lambda event: event[0])
    return events


def reset_keys(start: datetime, seed: int = SEED) -> dict[str, set[str]]:
    """Every key a run near `start` could have written, with a few days of margin either side."""
    keys = {FLIGHTS: set(), ITINERARIES: set(), HOTELS: set(HOTEL_RATES), OFFERS: set()}
    for back in range(-3, HISTORY_DAYS + 4):
        service_date = start.date() - timedelta(days=back)
        for live in (True, False):
            day = build_day(seed, service_date, start, live=live)
            keys[FLIGHTS].update(f.key for f in day.flights)
            for passenger in day.passengers:
                keys[ITINERARIES].add(passenger.key)
                if passenger.impacted_at:
                    keys[OFFERS].update({f"{passenger.key}-O1", f"{passenger.key}-O2"})
    return keys


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
            "linger.ms": 20,
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
        payload = serializer(value, context) if value is not None else None
        encoded_key = self.string(key, SerializationContext(topic, MessageField.KEY))
        while True:
            try:
                self.producer.produce(topic, key=encoded_key, value=payload, partition=0)
                break
            except BufferError:
                self.producer.poll(1)
        self.producer.poll(0)

    def flush(self) -> None:
        if not self.dry_run:
            pending = self.producer.flush(60)
            if pending:
                raise RuntimeError(f"{pending} records were not delivered")


def _pid_file():
    return get_project_root() / "tmp" / "datagen.pid"


def stop_previous_stream() -> None:
    """Stop a stream left running by an earlier deploy or run, so two streams never interleave."""
    path = _pid_file()
    try:
        pid = int(path.read_text().strip())
    except (OSError, ValueError):
        return
    if pid != os.getpid():
        try:
            command = subprocess.run(["ps", "-p", str(pid), "-o", "command="],
                                     capture_output=True, text=True, check=False).stdout
        except OSError:
            command = ""
        if "airport-datagen" in command or "airport_datagen" in command:
            os.kill(pid, signal.SIGTERM)
            logging.info("Stopped the previous data stream (pid %d)", pid)
    path.unlink(missing_ok=True)


def agent_enabled() -> bool:
    """True when terraform/airline-demo runs the recovery agent (its recovery_agent_enabled output)."""
    state = get_project_root() / "terraform" / "airline-demo" / "terraform.tfstate"
    try:
        return bool(json.loads(state.read_text())["outputs"]["recovery_agent_enabled"]["value"])
    except (OSError, KeyError, ValueError):
        return False


def run(start: datetime, seed: int = SEED, history: bool = True, minutes: int = STREAM_MINUTES,
        dry_run: bool = False, stream_only: bool = False, agent_offers: bool = False) -> None:
    plan = build_plan(start, seed, history=history and not stream_only)
    if agent_offers:
        live = {passenger.key for passenger in plan.live.passengers}
        plan.recoveries = [r for r in plan.recoveries if r.passenger.key not in live]
        logging.info("The recovery agent writes today's offers; the generator writes history only")
    publisher = Publisher(None if dry_run else extract_kafka_credentials("aws", get_project_root()), dry_run)
    if not dry_run:
        stop_previous_stream()
    if not stream_only:
        count = 0
        for topic, key, value in initial_records(plan):
            publisher.publish(topic, key, value)
            count += 1
        publisher.flush()
        logging.info("Published %d records: %d history days, today, and tomorrow's schedule",
                     count, len(plan.days) - 1)
    events = stream_events(plan, minutes)
    if not events:
        return
    pid_file = _pid_file()
    if not dry_run:
        pid_file.parent.mkdir(exist_ok=True)
        pid_file.write_text(str(os.getpid()))
    hero = plan.live.hero
    logging.info("Streaming %d updates for %d minutes; %s is scheduled at %s UTC",
                 len(events), minutes, hero.key, f"{hero.scheduled:%H:%M}")
    try:
        minute, sent = None, 0
        for at, topic, key, value in events:
            if not dry_run:
                wait = (at - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()
                if wait > 0:
                    time.sleep(wait)
            publisher.publish(topic, key, value)
            sent += 1
            if minute != at.replace(second=0):
                if minute is not None:
                    logging.info("%s UTC: %d updates", f"{minute:%H:%M}", sent)
                minute, sent = at.replace(second=0), 0
        publisher.flush()
        logging.info("Stream finished")
    finally:
        if not dry_run and pid_file.exists() and pid_file.read_text().strip() == str(os.getpid()):
            pid_file.unlink()


def reset(start: datetime, seed: int, dry_run: bool) -> None:
    publisher = Publisher(None if dry_run else extract_kafka_credentials("aws", get_project_root()), dry_run)
    if not dry_run:
        stop_previous_stream()
    for topic, keys in reset_keys(start, seed).items():
        for key in sorted(keys):
            publisher.publish(topic, key, None)
        logging.info("%s: wrote %d tombstones", topic, len(keys))
    publisher.flush()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run airport-datagen", description="River Air flight recovery data")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--now", help="ISO-8601 UTC stream start (default: now)")
    parser.add_argument("--minutes", type=int, default=STREAM_MINUTES,
                        help=f"minutes of live updates to stream (default: {STREAM_MINUTES}; 0 exits after publishing)")
    parser.add_argument("--skip-history", action="store_true", help=f"skip the {HISTORY_DAYS} days of history")
    parser.add_argument("--stream-only", action="store_true",
                        help="stream the live updates for an earlier --now without republishing")
    parser.add_argument("--dry-run", action="store_true", help="print records as JSON; no Kafka writes, no waiting")
    parser.add_argument("--reset", action="store_true", help="write tombstones for every generated key")
    parser.add_argument("--offers", choices=("auto", "agent", "generator"), default="auto",
                        help="who writes today's offers (default auto: the agent when Terraform runs it)")
    args = parser.parse_args(argv)
    setup_logging()
    start = _clock(args.now)
    try:
        if args.reset:
            reset(start, args.seed, args.dry_run)
        else:
            agent = args.offers == "agent" or (args.offers == "auto" and agent_enabled())
            run(start, args.seed, history=not args.skip_history, minutes=args.minutes,
                dry_run=args.dry_run, stream_only=args.stream_only, agent_offers=agent)
    except KeyboardInterrupt:
        logging.info("Stopped")


if __name__ == "__main__":
    main()
