"""
FastAPI app — the airport-disruption demo's visible consumer.

Two views over ONE maintained business entity (passenger_journey):

  * Ops dashboard  — every at-risk passenger + the recovery agent's recommended
    action, with an Approve control that executes it.
  * Passenger card — a single passenger's live status + concierge recovery message.

Data paths, mapped to the demo pillars:

  * READ  = Lightning Queries (the Lightning Tables REST serving API). Pillar 4:
    "serve current state to an app." The Global API key stays server-side; the
    browser only ever talks to this backend.
  * ACT   = the Approve button produces an EXECUTED recovery record back through
    Kafka (upsert on passenger_recovery). Pillar 6: "act while the moment is
    still recoverable." The change then shows up on the next Lightning read —
    a full closed loop through the platform.

Run:
    uv run airport-app                 # http://127.0.0.1:8000
    uv run airport-app --port 9000
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Reuse the existing credential/infra loaders rather than re-deriving them.
from scripts import setup_rtce as rtce
from scripts.terraform import extract_kafka_credentials, get_project_root

_STATIC = Path(__file__).parent / "static"

# The maintained entity, the agent's recommendations, and the ops rollup.
_JOURNEY_TOPIC = "passenger_journey"
_RECOVERY_TOPIC = "passenger_recovery"


class Deployment:
    """Lazily-loaded connection details for the live Confluent Cloud deployment.

    Everything comes from terraform state + credentials.env, exactly like the
    setup-rtce and datagen tooling, so the app targets whatever was deployed with
    no extra configuration.
    """

    def __init__(self) -> None:
        self._infra: dict[str, str] | None = None
        self._rtce_key: str | None = None
        self._rtce_secret: str | None = None
        self._producer = None  # lazy confluent_kafka.Producer
        self._recovery_serializer = None
        self._recovery_fields: list[str] | None = None
        self._string_serializer = None

    # --- Lightning (read) --------------------------------------------------
    def _load_rtce(self) -> None:
        if self._infra is not None:
            return
        creds_file = rtce._find_credentials_file()
        creds = rtce._load_env_file(creds_file)
        self._infra = rtce._get_infra(creds_file)
        self._rtce_key = creds.get(rtce._CRED_KEY, "")
        self._rtce_secret = creds.get(rtce._CRED_SECRET, "")
        if not self._rtce_key or not self._rtce_secret:
            raise HTTPException(
                status_code=503,
                detail=(
                    f"No RTCE/Lightning Global API key in credentials.env "
                    f"({rtce._CRED_KEY}). Run `uv run setup-rtce` first."
                ),
            )

    @property
    def lightning_url(self) -> str:
        self._load_rtce()
        region = self._infra["region"]
        cloud = self._infra.get("cloud", "aws").lower()
        return f"https://sql.{region}.{cloud}.confluent.cloud/query/v1alpha1"

    def lightning_query(
        self, topic: str, key: str | None = None, limit: int = 200,
        filter_column: str | None = None, filter_value: str | None = None,
    ) -> list[dict]:
        """Run a Lightning Query and return rows as a list of column->value dicts."""
        self._load_rtce()
        query = rtce._build_lightning_query(topic, key, limit, filter_column, filter_value)
        try:
            resp = requests.post(
                self.lightning_url,
                auth=(self._rtce_key, self._rtce_secret),
                json={
                    "catalog_name": self._infra["env_id"],
                    "database_name": self._infra["cluster_id"],
                    "query": query,
                },
                timeout=15,
            )
        except requests.RequestException as exc:
            raise HTTPException(
                status_code=502, detail=f"Lightning request failed: {exc}"
            ) from exc
        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Lightning query returned HTTP {resp.status_code}: {resp.text[:300]}",
            )
        body = resp.json().get("result", {})
        columns = [c["name"] for c in body.get("schema", {}).get("columns", [])]
        return [dict(zip(columns, row, strict=False)) for row in body.get("data", [])]

    # --- Kafka (act / write-back) -----------------------------------------
    def _load_producer(self):
        """Build a schema-aware producer for passenger_recovery on first use."""
        if self._producer is not None:
            return
        try:
            from confluent_kafka import Producer
            from confluent_kafka.schema_registry import SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroSerializer
            from confluent_kafka.serialization import StringSerializer
        except ImportError as exc:  # pragma: no cover - dependency guard
            raise HTTPException(
                status_code=503,
                detail=f"confluent-kafka not available for the Approve action: {exc}",
            ) from exc

        try:
            creds = extract_kafka_credentials("aws", get_project_root())
        except Exception as exc:  # broad: terraform/state errors surface as 503
            raise HTTPException(
                status_code=503,
                detail=f"Could not read Kafka credentials from terraform state: {exc}",
            ) from exc

        sr = SchemaRegistryClient(
            {
                "url": creds["schema_registry_url"],
                "basic.auth.user.info": (
                    f"{creds['schema_registry_api_key']}:{creds['schema_registry_api_secret']}"
                ),
            }
        )
        # Fetch the Flink-registered value schema so we serialize with the exact
        # field names the table expects (no hand-written Avro).
        registered = sr.get_latest_version(f"{_RECOVERY_TOPIC}-value")
        schema_str = registered.schema.schema_str
        self._recovery_serializer = AvroSerializer(sr, schema_str)
        self._recovery_fields = _avro_field_names(schema_str)
        self._string_serializer = StringSerializer("utf_8")
        self._producer = Producer(
            {
                "bootstrap.servers": creds["bootstrap_servers"],
                "security.protocol": "SASL_SSL",
                "sasl.mechanisms": "PLAIN",
                "sasl.username": creds["kafka_api_key"],
                "sasl.password": creds["kafka_api_secret"],
            }
        )

    def execute_recovery(self, key: str, recovery_type: str, action: str) -> None:
        """Produce an EXECUTED recovery record (upsert) for one passenger."""
        from confluent_kafka.serialization import MessageField, SerializationContext

        self._load_producer()
        # Map our three logical fields onto the schema's actual field names,
        # case-insensitively, so this survives casing differences.
        wanted = {"RECOVERY_TYPE": recovery_type, "ACTION": action, "STATUS": "EXECUTED"}
        value = {}
        for field in self._recovery_fields or []:
            value[field] = wanted.get(field.upper())
        ctx = SerializationContext(_RECOVERY_TOPIC, MessageField.VALUE)
        self._producer.produce(
            topic=_RECOVERY_TOPIC,
            key=self._string_serializer(
                key, SerializationContext(_RECOVERY_TOPIC, MessageField.KEY)
            ),
            value=self._recovery_serializer(value, ctx),
        )
        self._producer.flush(10)


def _avro_field_names(schema_str: str) -> list[str]:
    import json

    parsed = json.loads(schema_str)
    return [f["name"] for f in parsed.get("fields", [])]


def _merge_state(journeys: list[dict], recoveries: list[dict]) -> list[dict]:
    """Join the maintained entity with the agent's recommendation, by passenger KEY."""
    by_key = {r.get("KEY"): r for r in recoveries}
    merged = []
    for j in journeys:
        key = j.get("KEY")
        rec = by_key.get(key, {})
        merged.append(
            {
                "key": key,
                "passenger_name": j.get("PASSENGER_NAME"),
                "connecting_flight": j.get("CONNECTING_FLIGHT"),
                "destination": j.get("DESTINATION"),
                "gate": j.get("GATE"),
                "make_connection": _as_bool(j.get("MAKE_CONNECTION")),
                "minutes_to_departure": _as_int(j.get("MINUTES_TO_DEPARTURE")),
                "bag_status": j.get("BAG_STATUS"),
                "needs_recheck": _as_bool(j.get("NEEDS_RECHECK")),
                "risk": j.get("RISK"),
                "recovery_type": rec.get("RECOVERY_TYPE"),
                "action": rec.get("ACTION"),
                "status": rec.get("STATUS"),
            }
        )
    # Highest urgency first: MISS, then TIGHT, then the rest; then soonest departure.
    risk_rank = {"MISS": 0, "TIGHT": 1}
    merged.sort(
        key=lambda p: (
            risk_rank.get(p["risk"], 2),
            p["minutes_to_departure"] if p["minutes_to_departure"] is not None else 9999,
        )
    )
    return merged


def _as_bool(value):
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().upper() == "TRUE"
    return None


def _as_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


deployment = Deployment()
app = FastAPI(title="Airport Disruption Recovery", docs_url=None, redoc_url=None)


@app.get("/api/state")
def get_state():
    journeys = deployment.lightning_query(_JOURNEY_TOPIC)
    recoveries = deployment.lightning_query(_RECOVERY_TOPIC)
    passengers = _merge_state(journeys, recoveries)
    return {
        "passengers": passengers,
        "counts": {
            "total": len(passengers),
            "miss": sum(1 for p in passengers if p["risk"] == "MISS"),
            "tight": sum(1 for p in passengers if p["risk"] == "TIGHT"),
            "executed": sum(1 for p in passengers if p["status"] == "EXECUTED"),
        },
    }


@app.get("/api/passenger/{key}")
def get_passenger(key: str):
    journeys = deployment.lightning_query(_JOURNEY_TOPIC, key=key, limit=1)
    if not journeys:
        raise HTTPException(status_code=404, detail=f"No passenger {key}")
    recoveries = deployment.lightning_query(_RECOVERY_TOPIC, key=key, limit=1)
    return _merge_state(journeys, recoveries)[0]


@app.post("/api/passenger/{key}/approve")
def approve(key: str):
    passenger = get_passenger(key)
    if not passenger.get("recovery_type") or not passenger.get("action"):
        raise HTTPException(
            status_code=409,
            detail=f"No recommended recovery to execute for {key}",
        )
    deployment.execute_recovery(key, passenger["recovery_type"], passenger["action"])
    return {"key": key, "status": "EXECUTED"}


@app.get("/")
def index():
    return FileResponse(_STATIC / "index.html")


app.mount("/static", StaticFiles(directory=_STATIC), name="static")


def main() -> None:
    parser = argparse.ArgumentParser(prog="uv run airport-app")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        sys.exit("uvicorn is required: `uv sync` to install app dependencies.")

    print(f"Airport demo app on http://{args.host}:{args.port}")
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
