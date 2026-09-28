from datetime import datetime
from decimal import Decimal

from scripts import keynote_app, keynote_datagen
from scripts.setup_rtce import _build_lightning_query


def test_fixture_has_200_passengers_and_two_offers_each():
    now = datetime(2026, 9, 24, 21, 0)
    seed = list(keynote_datagen.scenario(now, 42, "seed"))
    offers = list(keynote_datagen.scenario(now, 42, "offers"))
    delay = list(keynote_datagen.scenario(now, 42, "delay"))
    assert len(seed) == 206
    assert sum(topic == keynote_datagen.ITINERARIES for topic, _, _ in seed) == 200
    assert len(offers) == 400
    assert len({key for _, key, _ in offers}) == 400
    assert delay[0][2]["estimated_time"] > seed[0][2]["estimated_time"]
    assert all(len(value) == 2 for topic, _, value in seed if topic == keynote_datagen.ITINERARIES)


def test_history_fixture_is_backdated_and_disjoint_from_live_ids():
    now = datetime(2026, 9, 24, 21, 0)
    live_flight_ids = {keynote_datagen.INBOUND, *keynote_datagen.ONWARD}
    live_passenger_ids = {f"P-{n:04d}" for n in range(1, keynote_datagen.PASSENGER_COUNT + 1)}
    history = list(keynote_datagen.scenario(now, 42, "history"))

    flight_keys = {key for topic, key, _ in history if topic == keynote_datagen.FLIGHTS}
    passenger_keys = {key for topic, key, _ in history if topic == keynote_datagen.ITINERARIES}
    assert len(flight_keys) == keynote_datagen.HISTORY_DAYS * 2
    assert flight_keys.isdisjoint(live_flight_ids)
    assert passenger_keys.isdisjoint(live_passenger_ids)
    assert all(key.startswith("HIST-") for key in flight_keys)
    assert all(key.startswith("H-") for key in passenger_keys)

    for topic, _, value in history:
        if topic == keynote_datagen.FLIGHTS:
            assert value["scheduled_time"] < now
        if topic == keynote_datagen.OFFERS:
            assert value["status"] in {"BOOKED", "CLOSED"}
            assert (value["recovered_at"] is not None) == (value["status"] == "BOOKED")

    booked = [value for topic, _, value in history
              if topic == keynote_datagen.OFFERS and value["status"] == "BOOKED"]
    assert booked, "expects at least one completed historical recovery to query in Athena"

    again = list(keynote_datagen.scenario(now, 42, "history"))
    assert history == again


def test_selection_uses_available_hotel_then_books(monkeypatch):
    offers = [
        {"key": "P-0001-O1", "passenger_id": "P-0001", "recommended_flight_id": "JA891",
         "hotel_id": "Harbor Hotel", "status": "OFFERED", "hotel_cost": "189.00",
         "recommended_at": "2026-09-24T22:06:00", "recovered_at": None},
        {"key": "P-0001-O2", "passenger_id": "P-0001", "recommended_flight_id": "JA892",
         "hotel_id": "Park Hotel", "status": "OFFERED", "hotel_cost": "219.00",
         "recommended_at": "2026-09-24T22:06:00", "recovered_at": None},
    ]
    hotels = [
        {"key": "Harbor Hotel", "available_rooms": 0, "nightly_rate": "189.00"},
        {"key": "Park Hotel", "available_rooms": 200, "nightly_rate": "219.00"},
    ]
    writes = []
    monkeypatch.setattr(keynote_app, "_passenger_offers", lambda _: [dict(row) for row in offers])
    monkeypatch.setattr(keynote_app, "_rows", lambda topic: hotels if topic == "hotel_inventory" else [])
    monkeypatch.setattr(keynote_app, "_write_offer", lambda row: writes.append(dict(row)))

    selected = keynote_app.select_offer("P-0001", "P-0001-O1")
    assert selected["hotel_id"] == "Park Hotel"
    assert Decimal(str(selected["hotel_cost"])) == Decimal("219.00")
    assert selected["status"] == "SELECTED"
    offers[0] = selected
    booked = keynote_app.book_offer("P-0001", "P-0001-O1")
    assert booked["status"] == "BOOKED"
    assert writes[-1]["status"] == "CLOSED"
    assert writes[-1]["key"] == "P-0001-O2"


def test_passenger_offers_use_lightning_filter(monkeypatch):
    calls = []

    def rows(topic, **kwargs):
        calls.append((topic, kwargs))
        return [
            {"key": "P-0200-O2", "passenger_id": "P-0200"},
            {"key": "P-0200-O1", "passenger_id": "P-0200"},
        ]

    monkeypatch.setattr(keynote_app, "_rows", rows)
    offers = keynote_app._passenger_offers("P-0200")
    assert [offer["key"] for offer in offers] == ["P-0200-O1", "P-0200-O2"]
    assert calls == [("passenger_recommendations", {
        "limit": 3, "filter_column": "passenger_id", "filter_value": "P-0200",
    })]
    assert _build_lightning_query(
        "passenger_recommendations", None, 3, "passenger_id", "P-'0200"
    ) == "SELECT * FROM `passenger_recommendations` WHERE passenger_id = 'P-''0200' LIMIT 3"
