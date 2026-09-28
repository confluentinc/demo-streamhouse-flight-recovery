import json
from collections import Counter
from datetime import datetime, timedelta
from itertools import pairwise

import pytest

from scripts import airport_datagen as g

START = datetime(2026, 9, 28, 17, 0)
OFFER_FIELDS = {"passenger_id", "recommended_flight_id", "hotel_name", "status", "hotel_cost",
                "impacted_at", "recommended_at"}


@pytest.fixture(scope="module")
def plan():
    return g.build_plan(START)


@pytest.fixture(scope="module")
def records(plan):
    return list(g.initial_records(plan))


def test_each_day_has_180_river_air_flights_through_sfo(plan):
    for day in [*plan.days, plan.tomorrow]:
        assert len(day.flights) == 180
        assert len(day.arrivals) == len(day.departures) == 90
        assert all(f.destination == g.HUB for f in day.arrivals)
        assert all(f.origin == g.HUB for f in day.departures)
        assert len({f.key for f in day.flights}) == 180
        assert set(Counter(f.destination for f in day.departures).values()) == {6}
    assert len(plan.days) == g.HISTORY_DAYS + 1


def test_hero_flight_strands_200_of_260_passengers(plan):
    hero = plan.live.hero
    assert hero.key == "RA417-20260928"
    assert hero.origin == "ORD"
    assert hero.scheduled == START + timedelta(minutes=50)
    passengers = [p for p in plan.live.passengers if p.inbound is hero]
    assert len(passengers) == 260
    assert sum(p.connecting is None for p in passengers) == 60
    impacted = [p for p in passengers if p.impacted_at]
    assert len(impacted) == 200
    assert {p.impacted_at for p in impacted} <= {START + timedelta(minutes=5), START + timedelta(minutes=15)}
    assert [delay for _, delay in hero.reveals] == [30, 75, 115]


def test_about_ten_severe_delays_a_day(plan):
    for day in plan.days:
        severe = [f for f in day.flights if max((d for _, d in f.reveals), default=0) >= 90]
        assert len(severe) == g.SEVERE_PER_DAY


def test_delays_only_grow_and_statuses_only_move_forward(plan):
    order = {"ON_TIME": 0, "DELAYED": 1, "BOARDING": 2, "DEPARTED": 3, "LANDED": 3}
    for day in plan.days:
        for flight in day.flights:
            delays = [d for _, d in flight.reveals]
            assert all(later >= earlier - 3 for earlier, later in pairwise(delays))
            assert max(delays, default=0) - (delays[-1] if delays else 0) <= 3
            statuses = [order[flight.status_at(t)] for t in flight.ticks(datetime.min, datetime.max)]
            assert statuses == sorted(statuses)
            assert flight.status_at(datetime.max) in {"LANDED", "DEPARTED"}


def test_connection_risk_changes_at_most_once(plan):
    limit = timedelta(minutes=g.CONNECTION_MINUTES)
    for passenger in plan.live.passengers:
        if not passenger.connecting:
            continue
        inbound, onward = passenger.inbound, passenger.connecting
        ticks = sorted({*inbound.ticks(datetime.min, datetime.max), *onward.ticks(datetime.min, datetime.max)})
        risk = [onward.estimated_at(t) - inbound.estimated_at(t) < limit for t in ticks]
        assert sum(a != b for a, b in pairwise(risk)) <= 1
        assert not risk[0]
        assert (passenger.impacted_at is not None) == risk[-1]


def test_some_passengers_have_no_connection(plan):
    passengers = plan.live.passengers
    share = sum(p.connecting is None for p in passengers) / len(passengers)
    assert 0.6 < share < 0.8


def test_offers_are_feasible_and_recovery_time_excludes_decisions(plan):
    sellout = START + g.SELLOUT
    for recovery in plan.recoveries:
        passenger = recovery.passenger
        assert timedelta(seconds=5) <= recovery.recommended_at - passenger.impacted_at <= timedelta(seconds=40)
        landed = passenger.inbound.estimated_at()
        for flight, hotel in zip(recovery.options, recovery.hotels, strict=True):
            assert flight.destination == passenger.connecting.destination
            assert flight.scheduled >= landed + timedelta(minutes=g.CONNECTION_MINUTES)
            assert (hotel is not None) == (flight.scheduled - landed >= g.OVERNIGHT)
        rows = dict(recovery.rows(datetime.max))
        if not recovery.completes:
            assert {row["status"] for row in rows.values()} == {"OFFERED"}
            continue
        booked = [row for row in rows.values() if row["status"] == "BOOKED"]
        assert len(booked) == (recovery.booked is not None)
        for row in rows.values():
            assert set(row) == OFFER_FIELDS
            assert (row["hotel_cost"] is None) == (row["hotel_name"] is None)
            if row["status"] == "BOOKED" and passenger.inbound in plan.live.arrivals:
                assert not (row["hotel_name"] == "Harbor Hotel" and recovery.resolved_at >= sellout)


def test_hero_offers_stay_open_for_the_presenter(plan):
    hero = [r for r in plan.recoveries if r.passenger.inbound is plan.live.hero]
    assert len(hero) == 200
    assert all(not r.completes for r in hero)
    assert all(r.hotels[0] == "Harbor Hotel" for r in hero)  # offered before the sellout


def test_initial_records_stay_within_lightning_row_cap(plan, records):
    window = [f for d in [*plan.days, plan.tomorrow] for f in d.flights
              if START - timedelta(hours=18) <= f.scheduled < START + timedelta(hours=6)]
    assert len(window) == 180
    written = {key for topic, key, _ in records if topic == g.ITINERARIES}
    expected = {p.key for d in plan.days for p in d.passengers if d.live or p.connecting}
    assert written == expected
    assert all(value["scheduled_time"] <= START + timedelta(days=2)
               for topic, _, value in records if topic == g.FLIGHTS)


def test_stream_updates_each_active_flight_once_a_minute(plan):
    events = g.stream_events(plan, g.STREAM_MINUTES)
    assert all(START < at <= START + timedelta(minutes=g.STREAM_MINUTES) for at, *_ in events)
    assert [at for at, *_ in events] == sorted(at for at, *_ in events)
    per_flight = Counter((key, at.replace(second=0)) for at, topic, key, _ in events if topic == g.FLIGHTS)
    assert set(per_flight.values()) == {1}
    hero = plan.live.hero
    hero_updates = [value for _, topic, key, value in events if topic == g.FLIGHTS and key == hero.key]
    assert len(hero_updates) == g.STREAM_MINUTES
    assert hero_updates[-1]["estimated_time"] == hero.scheduled + timedelta(minutes=115)
    harbor = [value["available_rooms"] for _, topic, key, value in events if topic == g.HOTELS]
    assert harbor[-1] == 0
    assert harbor == sorted(harbor, reverse=True)
    offers = [value for _, topic, _, value in events if topic == g.OFFERS]
    assert sum(value["passenger_id"].startswith("P-0928-417-") for value in offers) == 400


def test_same_seed_same_data(records):
    again = list(g.initial_records(g.build_plan(START)))
    assert again == records
    other = list(g.initial_records(g.build_plan(START, seed=7)))
    assert other != records


def test_reset_covers_every_written_key(records):
    keys = g.reset_keys(START)
    for topic, key, _ in records:
        assert key in keys[topic]


def test_dry_run_prints_json(capsys):
    g.main(["--now", "2026-09-28T17:00:00Z", "--dry-run", "--skip-history", "--minutes", "1"])
    lines = capsys.readouterr().out.splitlines()
    assert any('"key": "RA417-20260928"' in line for line in lines)


def _offer_keys(capsys, **kwargs):
    g.run(START, history=False, minutes=30, dry_run=True, **kwargs)
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    return [row["key"] for row in rows if row["topic"] == g.OFFERS]


def test_agent_writes_ra417_offers_instead_of_the_generator(capsys):
    hero = f"P-0928-{g.HERO_NUMBER}-"
    assert any(key.startswith(hero) for key in _offer_keys(capsys))
    offers = _offer_keys(capsys, agent_offers=True)
    assert offers and not any(key.startswith(hero) for key in offers)
