"""Small Lightning-backed keynote operations and passenger app."""

import argparse
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .airport_app import Deployment
from .keynote_datagen import INBOUND, OFFERS, ONWARD, Publisher
from .terraform import extract_kafka_credentials, get_project_root

STATIC = Path(__file__).parent / "static"
deployment = Deployment()
LIVE_FLIGHT_IDS = {INBOUND, *ONWARD}
app = FastAPI(title="Keynote flight recovery", docs_url=None, redoc_url=None)


def _rows(
    topic: str, key: str | None = None, limit: int = 1000,
    filter_column: str | None = None, filter_value: str | None = None,
) -> list[dict]:
    return [{name.lower(): value for name, value in row.items()}
            for row in deployment.lightning_query(
                topic, key=key, limit=limit,
                filter_column=filter_column, filter_value=filter_value,
            )]


def _timestamp(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000, timezone.utc).replace(tzinfo=None)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


def _write_offer(offer: dict) -> None:
    publisher = Publisher(extract_kafka_credentials("aws", get_project_root()), dry_run=False)
    publisher.publish(OFFERS, offer["key"], {
        "passenger_id": offer["passenger_id"],
        "recommended_flight_id": offer["recommended_flight_id"],
        "hotel_id": offer["hotel_id"],
        "status": offer["status"],
        "hotel_cost": Decimal(str(offer["hotel_cost"])),
        "recommended_at": _timestamp(offer["recommended_at"]),
        "recovered_at": _timestamp(offer.get("recovered_at")),
    })
    publisher.flush()


def _passenger_offers(passenger_id: str) -> list[dict]:
    return sorted(_rows(OFFERS, limit=3, filter_column="passenger_id", filter_value=passenger_id),
                  key=lambda row: row["key"])


@app.get("/api/state")
def state():
    # The history fixture backdates rows into these same compacted topics for the Athena/Quick
    # analytics scene; scope the live ops dashboard to the current scenario's flights only.
    flights = [row for row in _rows("flight_status") if row.get("key") in LIVE_FLIGHT_IDS]
    risk = [row for row in _rows("passenger_risk") if row.get("inbound_flight_id") in LIVE_FLIGHT_IDS]
    impact = [row for row in _rows("flight_impact") if row.get("key") in LIVE_FLIGHT_IDS]
    booked = [row for row in _rows(OFFERS, limit=1000, filter_column="status", filter_value="BOOKED")
              if row.get("passenger_id", "").startswith("P-")]
    return {
        "flights": flights,
        "risk": risk,
        "impact": impact,
        "counts": {
            "affected": sum(1 for row in risk if row.get("risk") == "HIGH"),
            "booked": len(booked),
        },
    }


@app.get("/api/passenger/{passenger_id}")
def passenger(passenger_id: str):
    risk = _rows("passenger_risk", key=passenger_id, limit=1)
    if not risk:
        raise HTTPException(status_code=404, detail="Passenger not found")
    return {"risk": risk[0], "offers": _passenger_offers(passenger_id)}


@app.post("/api/passenger/{passenger_id}/select/{offer_id}")
def select_offer(passenger_id: str, offer_id: str):
    offers = _passenger_offers(passenger_id)
    chosen = next((row for row in offers if row["key"] == offer_id), None)
    if chosen is None or chosen.get("status") not in {"OFFERED", "SELECTED"}:
        raise HTTPException(status_code=409, detail="Offer is unavailable")
    hotels = {row["key"]: row for row in _rows("hotel_inventory")}
    hotel = hotels.get(chosen["hotel_id"])
    if not hotel or int(hotel.get("available_rooms") or 0) < 1:
        available = sorted((row for row in hotels.values() if int(row.get("available_rooms") or 0) > 0),
                           key=lambda row: Decimal(str(row["nightly_rate"])))
        if not available:
            raise HTTPException(status_code=409, detail="No hotel rooms available")
        hotel = available[0]
        chosen["hotel_id"] = hotel["key"]
        chosen["hotel_cost"] = hotel["nightly_rate"]
    chosen["status"] = "SELECTED"
    _write_offer(chosen)
    return chosen


@app.post("/api/passenger/{passenger_id}/book/{offer_id}")
def book_offer(passenger_id: str, offer_id: str):
    offers = _passenger_offers(passenger_id)
    chosen = next((row for row in offers if row["key"] == offer_id), None)
    if chosen is None or chosen.get("status") != "SELECTED":
        raise HTTPException(status_code=409, detail="Select this offer first")
    hotels = {row["key"]: row for row in _rows("hotel_inventory")}
    if int(hotels.get(chosen["hotel_id"], {}).get("available_rooms") or 0) < 1:
        raise HTTPException(status_code=409, detail="Hotel sold out; select again for an alternative")
    chosen["status"] = "BOOKED"
    chosen["recovered_at"] = datetime.now(timezone.utc).isoformat()
    _write_offer(chosen)
    for offer in offers:
        if offer["key"] != offer_id and offer.get("status") in {"OFFERED", "SELECTED"}:
            offer["status"] = "CLOSED"
            _write_offer(offer)
    return chosen


@app.get("/")
def index():
    return FileResponse(STATIC / "keynote.html")


app.mount("/static", StaticFiles(directory=STATIC), name="static")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="uv run airport-app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)
    import uvicorn

    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
