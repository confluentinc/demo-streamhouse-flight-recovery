from datetime import datetime

from scripts.airport_datagen import (
    DEFAULT_SLIP_MINUTES,
    INBOUND_ARRIVAL_SEED_MIN,
    PASSENGERS,
    PIVOT_DEPARTURE_MIN,
    AirportDatagen,
)


def test_pivot_moves_ja512_later(capsys):
    generator = AirportDatagen(
        creds={},
        base_now=datetime(2026, 11, 4, 16, 0),
        dry_run=True,
    )

    generator.pivot()

    output = capsys.readouterr().out
    assert "[flight_updates] JA512" in output
    assert '"route": "SFO-PDX"' in output
    assert '"estimated_departure": "2026-11-04T17:31:00"' in output
    assert '"gate": "C3"' in output


def test_pivot_changes_maya_from_miss_to_tight():
    slipped_arrival = INBOUND_ARRIVAL_SEED_MIN + DEFAULT_SLIP_MINUTES
    connection_minutes = PIVOT_DEPARTURE_MIN - slipped_arrival
    maya = next(passenger for passenger in PASSENGERS if passenger[0] == "P-1009")

    assert connection_minutes == 36
    assert 30 <= connection_minutes < 45
    assert maya[4] is False  # domestic; no international bag recheck during a tight transfer
