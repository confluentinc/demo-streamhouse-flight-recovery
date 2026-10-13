import pytest

from scripts import kstreams_backend


@pytest.fixture(autouse=True)
def no_real_kafka_streams(monkeypatch):
    """Tests never start or stop the Kafka Streams app actually running on this machine."""
    monkeypatch.setattr(kstreams_backend, "start", lambda *args, **kwargs: None)
    monkeypatch.setattr(kstreams_backend, "stop", lambda *args, **kwargs: None)
