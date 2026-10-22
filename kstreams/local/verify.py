"""Check the Kafka Streams app against an independent copy of Flink's rules, and measure freshness.

    uv run python kstreams/local/verify.py [--cluster deployed] check   # every flight and passenger, once
    uv run python kstreams/local/verify.py watch RA417-20261005  # lag from each flight update to IQv2

`check` reads flight_status and passenger_itineraries from the beginning, applies the rules in
sql/24 and sql/25, and compares every flight's delay and at-risk count and every passenger's risk
with what the app serves. Run it when the stream is quiet (or stopped).

`watch` follows one flight. For each new flight_status record it computes what the app should show
(delay and at-risk count), then polls the app until it does, and reports produce-to-queryable lag.
"""

import argparse
import itertools
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from datetime import timezone

import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from scripts.kstreams_backend import connection_config

# --cluster local (default), deployed (the demo's cluster, from Terraform state), or env (kstreams/.env)
CLUSTER = sys.argv[sys.argv.index("--cluster") + 1] if "--cluster" in sys.argv else "local"
KAFKA, SCHEMA_REGISTRY = connection_config(CLUSTER)
# Queries rotate over every instance, so routing between instances is exercised too.
PORTS = os.environ.get("APP_PORTS", "7070,7071,7072").split(",")
APPS = itertools.cycle(f"http://127.0.0.1:{port}/api" for port in PORTS)
HTTP = requests.Session()


def app():
    return next(APPS)
HIGH_BELOW = 45


def millis(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    return int((value if value.tzinfo else value.replace(tzinfo=timezone.utc)).timestamp() * 1000)


def minutes_between(a, b):
    if a is None or b is None:
        return None
    return int((millis(b) - millis(a)) / 60_000)  # truncate toward zero, like TIMESTAMPDIFF


class Reference:
    """Flink's passenger_state and flight_impact, recomputed in Python."""

    def __init__(self):
        self.flights = {}
        self.itineraries = {}

    def apply(self, topic, key, value):
        table = self.flights if topic == "flight_status" else self.itineraries
        if value is None:
            table.pop(key, None)
        else:
            table[key] = value

    def passenger(self, passenger_id):
        it = self.itineraries.get(passenger_id)
        inbound = it and self.flights.get(it["inbound_flight_id"])
        if not inbound:
            return None
        onward = self.flights.get(it["connecting_flight_id"]) if it["connecting_flight_id"] else None
        minutes = minutes_between(inbound["estimated_time"], onward["estimated_time"]) if onward else None
        if it["connecting_flight_id"] is None:
            risk = "NO_CONNECTION"
        elif minutes is not None and minutes < HIGH_BELOW:
            risk = "HIGH"
        else:
            risk = "OK"
        return {"risk": risk, "connectionMinutes": minutes}

    def at_risk(self):
        counts = Counter()
        for pid, it in self.itineraries.items():
            p = self.passenger(pid)
            if p and p["risk"] == "HIGH":
                counts[it["inbound_flight_id"]] += 1
        return counts

    def at_risk_on(self, flight_id):
        return sum(1 for pid, it in self.itineraries.items()
                   if it["inbound_flight_id"] == flight_id and (self.passenger(pid) or {}).get("risk") == "HIGH")

    def delay(self, flight_id):
        f = self.flights[flight_id]
        return minutes_between(f["scheduled_time"], f["estimated_time"])


def consumer(group):
    return Consumer({**KAFKA, "group.id": group, "enable.auto.commit": False,
                     "auto.offset.reset": "earliest", "isolation.level": "read_uncommitted"})


def read_to_end(ref, deserialize):
    c = consumer(f"verify-{time.time()}")
    parts = partitions(c)
    ends = {(tp.topic, tp.partition): c.get_watermark_offsets(tp)[1] for tp in parts}
    c.assign([TopicPartition(tp.topic, tp.partition, 0) for tp in parts])
    seen = defaultdict(int)
    while any(seen[tp] < end for tp, end in ends.items()):
        msg = c.poll(1.0)
        if msg is None or msg.error():
            continue
        seen[(msg.topic(), msg.partition())] = msg.offset() + 1
        ref.apply(msg.topic(), msg.key().decode(), deserialize(msg))
    c.close()
    return sum(ends.values())


def partitions(c):
    meta = c.list_topics(timeout=10).topics
    return [TopicPartition(t, p) for t in ("flight_status", "passenger_itineraries") for p in meta[t].partitions]


def check(ref, deserialize):
    records = read_to_end(ref, deserialize)
    at_risk = ref.at_risk()
    problems = []
    days = defaultdict(list)
    for fid in ref.flights:
        days[fid.rsplit("-", 1)[1]].append(fid)

    # One window read covers every flight; then each flight's delay and at-risk count must match.
    times = [millis(f["scheduled_time"]) for f in ref.flights.values() if f["scheduled_time"] is not None]
    served = {f["flightId"]: f for f in HTTP.get(f"{app()}/flights", timeout=30,
                                                 params={"from": min(times), "to": max(times) + 1}).json()["flights"]}
    if set(served) != set(ref.flights):
        problems.append(f"app has {len(served)} flights, reference has {len(ref.flights)}")
    for fid in ref.flights:
        got = served.get(fid)
        want = (ref.delay(fid), at_risk.get(fid, 0))
        if got and (got["delayMinutes"], got["affectedPassengers"]) != want:
            problems.append(f"{fid}: app delay/at-risk {got['delayMinutes']}/{got['affectedPassengers']}, "
                            f"reference {want[0]}/{want[1]}")

    mismatched = 0
    for pid in ref.itineraries:
        want = ref.passenger(pid)
        resp = HTTP.get(f"{app()}/passengers/{pid}", timeout=10)
        got = resp.json().get("passenger") if resp.ok else None
        if want is None and got is None:
            continue
        if not got or not want or (got["risk"], got["connectionMinutes"]) != (want["risk"], want["connectionMinutes"]):
            mismatched += 1
            if mismatched <= 5:
                problems.append(f"{pid}: app {got and (got['risk'], got['connectionMinutes'])}, reference {want}")
    if mismatched:
        problems.append(f"{mismatched} passengers differ")

    print(f"Read {records} records: {len(ref.flights)} flights over {len(days)} days, "
          f"{len(ref.itineraries)} passengers, {sum(at_risk.values())} at HIGH risk")
    for p in problems:
        print("MISMATCH", p)
    print("OK: the app matches the reference" if not problems else f"{len(problems)} problems")
    return not problems


def watch(ref, deserialize, flight_id, timeout):
    read_to_end(ref, deserialize)
    c = consumer(f"watch-{time.time()}")
    c.assign([TopicPartition(tp.topic, tp.partition, c.get_watermark_offsets(tp)[1]) for tp in partitions(c)])
    lags = []
    pending = []  # (produced_ms, want) for each flight update the app hasn't shown yet, oldest first
    unavailable_since = None
    http = requests.Session()
    print(f"Watching {flight_id}; Ctrl-C to stop")
    try:
        while True:
            # Keep consuming while we wait on the app, so the reference never falls behind.
            while (msg := c.poll(0)) is not None:
                if msg.error():
                    continue
                key = msg.key().decode()
                ref.apply(msg.topic(), key, deserialize(msg))
                if msg.topic() == "flight_status" and key == flight_id:
                    pending.append((msg.timestamp()[1], (ref.delay(flight_id), ref.at_risk_on(flight_id))))
            if not pending:
                time.sleep(0.01)
                continue
            instance = app()
            try:
                resp = http.get(f"{instance}/flights/{flight_id}", timeout=5)
                resp.raise_for_status()
                flight = resp.json()["flight"]
                got = (flight["delayMinutes"], flight["affectedPassengers"])
            except (requests.RequestException, KeyError, ValueError) as e:
                if unavailable_since is None:
                    unavailable_since = time.time()
                    print(f"{time.strftime('%X')} {instance} not answering: {str(e)[:80]}")
                time.sleep(0.05)
                continue
            now_ms = time.time() * 1000
            if unavailable_since is not None:
                print(f"{time.strftime('%X')} answering again after {now_ms - unavailable_since * 1000:.0f} ms")
                unavailable_since = None
            # The app may already show a later update; everything up to the newest match is delivered.
            matched = max((i for i, (_, want) in enumerate(pending) if want == got), default=None)
            if matched is not None:
                produced_ms, want = pending[matched]
                lag = now_ms - produced_ms
                lags.append(lag)
                skipped = f" ({matched} earlier superseded)" if matched else ""
                print(f"{time.strftime('%X')} delay {want[0]:>4} min  at risk {want[1]:>3}  "
                      f"queryable {lag:7.0f} ms after produce{skipped}")
                del pending[:matched + 1]
            while pending and now_ms - pending[0][0] > timeout * 1000:
                print(f"TIMEOUT after {timeout}s: want {pending.pop(0)[1]}, app shows {got}")
            time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        if lags:
            lags.sort()
            print(f"\n{len(lags)} updates: median {statistics.median(lags):.0f} ms, "
                  f"p95 {lags[int(0.95 * (len(lags) - 1))]:.0f} ms, max {lags[-1]:.0f} ms")


def lag(threshold, timeout, group="river-air-ops"):
    """Print the app's input backlog every 2 seconds; stop once it stays under threshold and say how long it took."""
    from confluent_kafka import ConsumerGroupTopicPartitions
    from confluent_kafka.admin import AdminClient, OffsetSpec

    admin = AdminClient(KAFKA)
    started, peak, quiet = time.time(), 0, 0
    while time.time() - started < timeout:
        try:
            committed = admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group)])[group].result().topic_partitions
            latest = admin.list_offsets({TopicPartition(tp.topic, tp.partition): OffsetSpec.latest()
                                         for tp in committed if tp.offset >= 0}, request_timeout=15)
            ends = {tp: latest[TopicPartition(tp.topic, tp.partition)].result().offset
                    for tp in committed if tp.offset >= 0}
        except Exception as e:  # a slow Confluent Cloud call; try again next round
            print(f"{time.strftime('%X')} probe failed: {str(e)[:80]}")
            time.sleep(2)
            continue
        total = sum(max(0, end - tp.offset) for tp, end in ends.items())
        peak = max(peak, total)
        by_topic = defaultdict(int)
        for tp, end in ends.items():
            by_topic[tp.topic.removeprefix(group + "-")] += max(0, end - tp.offset)
        busiest = ", ".join(f"{t} {n}" for t, n in sorted(by_topic.items(), key=lambda kv: -kv[1])[:3] if n)
        print(f"{time.strftime('%X')} +{time.time() - started:5.0f}s backlog {total:>7}  {busiest}")
        quiet = quiet + 1 if total < threshold and peak >= threshold else 0
        if quiet >= 3:
            print(f"drained: peak {peak} records, under {threshold} after {time.time() - started:.0f}s")
            return
        time.sleep(2)
    print(f"still {total} behind after {timeout}s (peak {peak})")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", choices=("local", "deployed", "env"), default="local")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    w = sub.add_parser("watch")
    w.add_argument("flight_id")
    w.add_argument("--timeout", type=float, default=30)
    lg = sub.add_parser("lag")
    lg.add_argument("--threshold", type=int, default=500, help="backlog that counts as caught up")
    lg.add_argument("--timeout", type=float, default=900)
    lg.add_argument("--group", default="river-air-ops", help="the app's application.id")
    args = parser.parse_args()
    if args.command == "lag":
        lag(args.threshold, args.timeout, args.group)
        return

    registry = SchemaRegistryClient(SCHEMA_REGISTRY)
    avro = AvroDeserializer(registry)

    def deserialize(msg):
        if msg.value() is None:
            return None
        return avro(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

    ref = Reference()
    if args.command == "check":
        sys.exit(0 if check(ref, deserialize) else 1)
    watch(ref, deserialize, args.flight_id, args.timeout)


if __name__ == "__main__":
    main()
