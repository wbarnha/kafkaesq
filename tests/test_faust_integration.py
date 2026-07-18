"""Integration test constructing a real Faust app from converted settings.

faust.App silently ignores unknown keyword arguments, so construction alone
proves nothing — the test asserts every converted setting is reflected in
``app.conf``, which fails if kafkaesq emits a wrong setting name or value.
Skips unless faust (faust-streaming) is installed.
"""

from __future__ import annotations

import pytest

from kafkaesq import confluent_to_faust


def test_faust_app_accepts_converted_settings() -> None:
    faust = pytest.importorskip("faust")

    settings = confluent_to_faust(
        {
            "bootstrap.servers": "localhost:9092,localhost:9093",
            "group.id": "kafkaesq-it",
            "client.id": "kafkaesq-client",
            "session.timeout.ms": 45000,
            "heartbeat.interval.ms": 3000,
            "max.poll.interval.ms": 300000,
            "auto.commit.interval.ms": 2800,
            "auto.offset.reset": "earliest",
            "check.crcs": True,
            "group.instance.id": "kafkaesq-instance",
            "max.partition.fetch.bytes": 1048576,
            "acks": "all",
            "compression.type": "gzip",
            "message.max.bytes": 1000000,
        }
    )

    app = faust.App(settings.pop("id"), **settings)
    try:
        assert [str(url) for url in app.conf.broker] == [
            "kafka://localhost:9092",
            "kafka://localhost:9093",
        ]
        assert app.conf.broker_session_timeout == 45
        assert app.conf.broker_heartbeat_interval == 3
        assert app.conf.broker_max_poll_interval == 300
        assert app.conf.broker_commit_interval == 2.8
        assert app.conf.consumer_auto_offset_reset == "earliest"
        assert app.conf.broker_check_crcs is True
        assert app.conf.consumer_group_instance_id == "kafkaesq-instance"
        assert app.conf.consumer_max_fetch_size == 1048576
        assert app.conf.producer_acks == -1
        assert app.conf.producer_compression_type == "gzip"
        assert app.conf.producer_max_request_size == 1000000
    finally:
        # The app never starts; just release its resources.
        pass

