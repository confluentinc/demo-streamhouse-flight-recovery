from datetime import datetime
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from scripts import airport_app
from scripts.airport_app import Deployment
from scripts.setup_rtce import _build_lightning_query

NOW = datetime(2026, 9, 28, 12, 0, 0)


# --- query builder -----------------------------------------------------------

def test_builder_escapes_quotes_and_renders_key():
    assert _build_lightning_query("passenger_state", key="P-1'; DROP", limit=1) == (
        "SELECT * FROM `passenger_state` WHERE \"KEY\" = 'P-1''; DROP' LIMIT 1")


def test_builder_renders_timestamp_in_numbers_and_order():
    query = _build_lightning_query(
        "flight_impact", limit=200,
        where=[("scheduled_time", ">=", datetime(2026, 9, 1, 0, 0, 0, 999)),
               ("delay_minutes", ">", 15), ("hotel_cost", "<=", Decimal("189.50")),
               ("status", "IN", ["DELAYED", "BOARDING"]), ("key", "LIKE", "RA4%")],
        order_by=("scheduled_time", "desc"))
    assert query == (
        "SELECT * FROM `flight_impact` WHERE scheduled_time >= TIMESTAMP '2026-09-01 00:00:00'"
        " AND delay_minutes > 15 AND hotel_cost <= 189.50"
        " AND status IN ('DELAYED', 'BOARDING') AND \"KEY\" LIKE 'RA4%'"
        " ORDER BY scheduled_time DESC LIMIT 200")


def test_builder_default_scan():
    assert _build_lightning_query("hotel_inventory") == "SELECT * FROM `hotel_inventory` LIMIT 10"


@pytest.mark.parametrize("kwargs", [
    {"topic": "flight_impact; DROP"},
    {"topic": "Flight_Impact"},
    {"topic": "flight_impact", "where": [("status OR 1=1", "=", "x")]},
    {"topic": "flight_impact", "where": [("status", "!=", "x")]},
    {"topic": "flight_impact", "where": [("status", "=", True)]},
    {"topic": "flight_impact", "where": [("status", "=", None)]},
    {"topic": "flight_impact", "where": [("status", "IN", "DELAYED")]},
    {"topic": "flight_impact", "where": [("status", "IN", [])]},
    {"topic": "flight_impact", "where": [("status", "=")]},
    {"topic": "flight_impact", "order_by": ("key", "SIDEWAYS")},
    {"topic": "flight_impact", "order_by": ("bad-column", "ASC")},
    {"topic": "flight_impact", "limit": 0},
    {"topic": "flight_impact", "limit": True},
])
def test_builder_rejects_invalid_input(kwargs):
    with pytest.raises(ValueError):
        _build_lightning_query(**kwargs)


# --- fake Lightning ----------------------------------------------------------

def _compare(row_value, op, value):
    if isinstance(value, datetime):
        row_value = airport_app._timestamp(row_value)
    elif isinstance(value, (int, float, Decimal)):
        row_value = Decimal(str(row_value))
    if row_value is None:
        return False
    return {"=": row_value == value, ">": row_value > value, ">=": row_value >= value,
            "<": row_value < value, "<=": row_value <= value}[op]


class FakeLightning:
    """In-memory tables that honour the builder's arguments and the 200-row cap."""

    def __init__(self, tables):
        self.tables = tables
        self.calls = []

    def query(self, topic, key=None, limit=200, where=(), order_by=None):
        _build_lightning_query(topic, key=key, limit=limit, where=where, order_by=order_by)
        self.calls.append({"topic": topic, "key": key, "where": list(where), "order_by": order_by})
        conditions = [("key", "=", key)] if key is not None else []
        conditions += list(where)
        rows = [dict(row) for row in self.tables.get(topic, [])
                if all(_compare(row.get(c), op, v) for c, op, v in conditions)]
        if order_by:
            rows.sort(key=lambda row: row[order_by[0]], reverse=order_by[1] == "DESC")
        return rows[:min(limit, 200)]


@pytest.fixture
def fake(monkeypatch):
    lightning = FakeLightning({})
    monkeypatch.setattr(Deployment, "lightning_query", lambda _self, *args, **kwargs: lightning.query(*args, **kwargs))
    monkeypatch.setattr(airport_app, "_now", lambda: NOW)
    writes = []
    monkeypatch.setattr(airport_app, "_write_offer", lambda offer: writes.append(dict(offer)))
    lightning.writes = writes
    return lightning


def test_lightning_scan_pages_until_short_page(fake):
    fake.tables["passenger_state"] = [
        {"key": f"P-0928-417-{n:03d}", "inbound_flight_id": "RA417-20260928"} for n in range(1, 451)]
    rows = airport_app.deployment.lightning_scan(
        "passenger_state", where=[("inbound_flight_id", "=", "RA417-20260928")])
    assert len(rows) == 450
    assert len({row["key"] for row in rows}) == 450
    assert len(fake.calls) == 3
    assert fake.calls[0]["where"] == [("inbound_flight_id", "=", "RA417-20260928")]
    assert fake.calls[1]["where"][-1] == ("key", ">", "P-0928-417-200")
    assert fake.calls[2]["where"][-1] == ("key", ">", "P-0928-417-400")
    assert all(call["order_by"] == ("key", "ASC") for call in fake.calls)


def _offer(key, hotel_name, cost, status="OFFERED"):
    return {"key": key, "passenger_id": "P-0928-417-001", "recommended_flight_id": "RA88-20260929",
            "hotel_name": hotel_name, "status": status, "hotel_cost": cost,
            "impacted_at": "2026-09-28 10:00:00.000000", "recommended_at": "2026-09-28 10:01:00.000000"}


HOTELS = [
    {"key": "Harbor Hotel", "available_rooms": "0", "nightly_rate": "189.00"},
    {"key": "Park Hotel", "available_rooms": "4", "nightly_rate": "219.00"},
]


def test_select_substitutes_available_hotel_then_book_closes_other(fake):
    fake.tables["hotel_inventory"] = HOTELS
    fake.tables["passenger_recommendations"] = [
        _offer("P-0928-417-001-O1", "Harbor Hotel", "189.00"),
        _offer("P-0928-417-001-O2", "Park Hotel", "219.00"),
    ]
    chosen = airport_app.select_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert chosen["status"] == "SELECTED"
    assert chosen["hotel_name"] == "Park Hotel"
    assert chosen["hotel_cost"] == "219.00"

    fake.tables["passenger_recommendations"][0] = chosen
    booked = airport_app.book_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert booked["status"] == "BOOKED"
    assert [(w["key"], w["status"]) for w in fake.writes] == [
        ("P-0928-417-001-O1", "SELECTED"),
        ("P-0928-417-001-O1", "BOOKED"),
        ("P-0928-417-001-O2", "CLOSED"),
    ]


def test_book_rejects_sold_out_hotel(fake):
    fake.tables["hotel_inventory"] = HOTELS
    fake.tables["passenger_recommendations"] = [
        _offer("P-0928-417-001-O1", "Harbor Hotel", "189.00", status="SELECTED")]
    with pytest.raises(airport_app.HTTPException) as error:
        airport_app.book_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert error.value.status_code == 409


def test_select_and_book_offer_without_hotel(fake):
    fake.tables["hotel_inventory"] = [{**row, "available_rooms": "0"} for row in HOTELS]
    fake.tables["passenger_recommendations"] = [_offer("P-0928-417-001-O1", None, None)]
    chosen = airport_app.select_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert chosen["status"] == "SELECTED"
    assert chosen["hotel_name"] is None and chosen["hotel_cost"] is None
    fake.tables["passenger_recommendations"][0] = chosen
    booked =airport_app.book_offer("P-0928-417-001", "P-0928-417-001-O1")
    assert booked["status"] == "BOOKED"


def test_write_offer_passes_nulls_and_parses_timestamps(monkeypatch):
    published = []

    class FakePublisher:
        def publish(self, topic, key, value):
            published.append((topic, key, value))

        def flush(self):
            pass

    monkeypatch.setattr(airport_app, "_publisher", FakePublisher())
    offer = _offer("P-0928-417-001-O1", None, None)
    airport_app._write_offer(offer)
    offer.update(hotel_name="Park Hotel", hotel_cost="219.00", recommended_at=1790000000000)
    airport_app._write_offer(offer)
    (topic, key, first), (_, _, second) = published
    assert (topic, key) == ("passenger_recommendations", "P-0928-417-001-O1")
    assert first["hotel_name"] is None and first["hotel_cost"] is None
    assert first["impacted_at"] == datetime(2026, 9, 28, 10, 0)
    assert first["recommended_at"] == datetime(2026, 9, 28, 10, 1)
    assert set(first) == {"passenger_id", "recommended_flight_id", "hotel_name", "status", "hotel_cost",
                          "impacted_at", "recommended_at"}
    assert second["hotel_cost"] == Decimal("219.00")
    assert second["recommended_at"] == datetime(2026, 9, 21, 14, 13, 20)


def _flight(n, scheduled, delay, affected, status="ON_TIME"):
    return {"key": f"RA{n}-20260928", "origin": "SFO", "destination": "SEA",
            "scheduled_time": scheduled, "status": status,
            "delay_minutes": str(delay), "affected_passengers": str(affected)}


def test_state_counts_and_delays(fake):
    fake.tables["flight_impact"] = [
        _flight(100, "2026-09-27 17:00:00.000000", 90, 50),  # before the 18h window
        _flight(417, "2026-09-28 11:00:00.000000", 95, 260, "DELAYED"),
        _flight(418, "2026-09-28 13:00:00.000000", 15, 3, "DELAYED"),
        _flight(419, "2026-09-28 09:00:00.000000", 14, 0),
        _flight(420, "2026-09-28 18:00:00.000000", 5, 0),  # at now+6h, excluded
    ] + [_flight(500 + n, f"2026-09-28 {n % 10:02d}:30:00.000000", 0, 1) for n in range(12)]
    fake.tables["passenger_recommendations"] = [
        {"key": f"P-{n}-O1", "status": "BOOKED", "recommended_at": "2026-09-28 08:00:00.000000"}
        for n in range(205)
    ] + [
        {"key": "P-old-O1", "status": "BOOKED", "recommended_at": "2026-09-26 08:00:00.000000"},
        {"key": "P-open-O1", "status": "OFFERED", "recommended_at": "2026-09-28 08:00:00.000000"},
    ]
    body = TestClient(airport_app.app).get("/api/state").json()
    keys = [row["key"] for row in body["flights"]]
    assert "RA100-20260928" not in keys and "RA420-20260928" not in keys
    assert body["counts"] == {"affected": 275, "booked": 205, "flights": 15, "delayed": 2}
    assert body["flights"][0]["delay_minutes"] == 0 and isinstance(body["flights"][0]["affected_passengers"], int)
    assert [row["key"] for row in body["delays"][:3]] == ["RA417-20260928", "RA418-20260928", "RA419-20260928"]
    assert len(body["delays"]) == 10
    impact_call = next(call for call in fake.calls if call["topic"] == "flight_impact")
    assert impact_call["order_by"] == ("scheduled_time", "ASC")


def test_flight_passengers_high_risk_first(fake):
    fake.tables["flight_impact"] = [_flight(417, "2026-09-28 11:00:00.000000", 95, 2, "DELAYED")]
    fake.tables["passenger_state"] = [
        {"key": "P-0928-417-001", "inbound_flight_id": "RA417-20260928", "risk": "OK"},
        {"key": "P-0928-417-002", "inbound_flight_id": "RA417-20260928", "risk": "HIGH"},
        {"key": "P-0928-417-003", "inbound_flight_id": "RA417-20260928", "risk": "NO_CONNECTION"},
        {"key": "P-0928-417-004", "inbound_flight_id": "RA417-20260928", "risk": "HIGH"},
        {"key": "P-0928-418-001", "inbound_flight_id": "RA418-20260928", "risk": "HIGH"},
    ]
    client = TestClient(airport_app.app)
    body = client.get("/api/flight/RA417-20260928").json()
    assert body["flight"]["delay_minutes"] == 95
    assert body["flight"]["estimated_time"] == "2026-09-28T12:35:00"
    assert [row["key"] for row in body["passengers"]] == [
        "P-0928-417-002", "P-0928-417-004", "P-0928-417-001", "P-0928-417-003"]
    assert client.get("/api/flight/RA999-20260928").status_code == 404
    assert client.get("/api/passenger/P-missing").status_code == 404
