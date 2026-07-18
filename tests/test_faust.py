from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from kafkaesq import (
    KafkaesqWarning,
    confluent_to_aiokafka,
    confluent_to_faust,
    faust_to_confluent,
)
from kafkaesq.cli import main


class TestFaustToConfluent:
    def test_basic_settings(self) -> None:
        result = faust_to_confluent(
            {
                "id": "billing",
                "broker": "kafka://localhost:9092",
                "broker_client_id": "worker-1",
                "consumer_auto_offset_reset": "earliest",
            }
        )
        assert result == {
            "group.id": "billing",
            "bootstrap.servers": "localhost:9092",
            "client.id": "worker-1",
            "auto.offset.reset": "earliest",
        }

    def test_broker_url_forms(self) -> None:
        assert faust_to_confluent({"broker": "kafka://a:9092;kafka://b:9092"}) == {
            "bootstrap.servers": "a:9092,b:9092"
        }
        assert faust_to_confluent(
            {"broker": ["kafka://a:9092", "kafka://b:9092"]}
        ) == {"bootstrap.servers": "a:9092,b:9092"}
        assert faust_to_confluent({"broker": "localhost:9092"}) == {
            "bootstrap.servers": "localhost:9092"
        }

    def test_second_based_settings_become_milliseconds(self) -> None:
        result = faust_to_confluent(
            {
                "broker_session_timeout": 45,
                "broker_heartbeat_interval": 3.0,
                "broker_request_timeout": 90.0,
                "broker_max_poll_interval": 1000.0,
                "broker_commit_interval": 2.8,
            }
        )
        assert result == {
            "session.timeout.ms": 45000,
            "heartbeat.interval.ms": 3000,
            "request.timeout.ms": 90000,
            "max.poll.interval.ms": 1000000,
            "auto.commit.interval.ms": 2800,
        }

    def test_producer_settings(self) -> None:
        result = faust_to_confluent(
            {
                "producer_acks": -1,
                "producer_compression_type": None,
                "producer_linger": 0.005,
                "producer_max_request_size": 1048576,
            }
        )
        assert result == {
            "acks": "all",
            "compression.type": "none",
            "linger.ms": 5,
            "message.max.bytes": 1048576,
        }

    def test_deprecated_producer_linger_ms_alias(self) -> None:
        assert faust_to_confluent({"producer_linger_ms": 5}) == {"linger.ms": 5}

    def test_unmapped_faust_settings_warn(self) -> None:
        with pytest.warns(KafkaesqWarning, match="topic_partitions"):
            result = faust_to_confluent(
                {"id": "app", "topic_partitions": 8}
            )
        assert result == {"group.id": "app"}

    def test_chains_into_aiokafka(self) -> None:
        kwargs = confluent_to_aiokafka(
            faust_to_confluent(
                {"id": "billing", "broker": "kafka://localhost:9092"}
            )
        )
        assert kwargs == {
            "group_id": "billing",
            "bootstrap_servers": "localhost:9092",
        }


class TestConfluentToFaust:
    def test_basic_config(self) -> None:
        result = confluent_to_faust(
            {
                "bootstrap.servers": "a:9092,b:9092",
                "group.id": "billing",
                "session.timeout.ms": 45000,
                "auto.offset.reset": "latest",
            }
        )
        assert result == {
            "broker": "kafka://a:9092;kafka://b:9092",
            "id": "billing",
            "broker_session_timeout": 45,
            "consumer_auto_offset_reset": "latest",
        }

    def test_fractional_seconds_are_preserved(self) -> None:
        result = confluent_to_faust({"auto.commit.interval.ms": 2800})
        assert result == {"broker_commit_interval": 2.8}

    def test_acks_all_becomes_minus_one(self) -> None:
        assert confluent_to_faust({"acks": "all"}) == {"producer_acks": -1}
        assert confluent_to_faust({"acks": 1}) == {"producer_acks": 1}

    def test_confluent_aliases_are_canonicalized(self) -> None:
        result = confluent_to_faust({"metadata.broker.list": "localhost:9092"})
        assert result == {"broker": "kafka://localhost:9092"}

    def test_security_keys_are_unmapped(self) -> None:
        with pytest.warns(KafkaesqWarning, match="sasl.username"):
            result = confluent_to_faust(
                {
                    "bootstrap.servers": "localhost:9092",
                    "security.protocol": "SASL_SSL",
                    "sasl.username": "user",
                    "sasl.password": "secret",
                }
            )
        assert result == {"broker": "kafka://localhost:9092"}

    def test_round_trip(self) -> None:
        original = {
            "id": "billing",
            "broker": "kafka://a:9092;kafka://b:9092",
            "broker_session_timeout": 45,
            "broker_heartbeat_interval": 3,
            "consumer_auto_offset_reset": "earliest",
            "producer_acks": -1,
            "producer_max_request_size": 1048576,
        }
        assert confluent_to_faust(faust_to_confluent(original)) == original


class TestFaustCli:
    def test_faust_yaml_to_confluent_properties(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        pytest.importorskip("yaml")
        config_file = tmp_path / "faust.yaml"
        config_file.write_text(
            "id: billing\n"
            "broker: kafka://localhost:9092\n"
            "broker_session_timeout: 45\n"
            "consumer_auto_offset_reset: earliest\n"
        )
        code = main(
            [str(config_file), "--to", "confluent", "--format", "properties"]
        )
        captured = capsys.readouterr()
        assert code == 0
        assert captured.out == (
            "auto.offset.reset=earliest\n"
            "bootstrap.servers=localhost:9092\n"
            "group.id=billing\n"
            "session.timeout.ms=45000\n"
        )

    def test_faust_source_is_auto_detected(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "faust.json"
        config_file.write_text(
            json.dumps({"broker": "kafka://localhost:9092", "id": "app"})
        )
        code = main([str(config_file), "--to", "aiokafka"])
        captured = capsys.readouterr()
        assert code == 0
        assert json.loads(captured.out) == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "app",
        }

    def test_confluent_properties_to_faust_yaml_with_credentials_note(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        yaml = pytest.importorskip("yaml")
        config_file = tmp_path / "client.properties"
        config_file.write_text(
            "bootstrap.servers=pkc-00000.confluent.cloud:9092\n"
            "security.protocol=SASL_SSL\n"
            "sasl.mechanisms=PLAIN\n"
            "sasl.username=KEY\n"
            "sasl.password=SECRET\n"
            "session.timeout.ms=45000\n"
        )
        code = main(
            [
                str(config_file),
                "--to",
                "faust",
                "--format",
                "yaml",
                "--on-unmapped",
                "ignore",
            ]
        )
        captured = capsys.readouterr()
        assert code == 0
        assert yaml.safe_load(captured.out) == {
            "broker": "kafka://pkc-00000.confluent.cloud:9092",
            "broker_session_timeout": 45,
        }
        assert "broker_credentials" in captured.err

    def test_aiokafka_kwargs_to_faust(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "kwargs.json"
        config_file.write_text(
            json.dumps(
                {
                    "bootstrap_servers": "localhost:9092",
                    "group_id": "billing",
                    "session_timeout_ms": 45000,
                }
            )
        )
        code = main(
            [str(config_file), "--from", "aiokafka", "--to", "faust"]
        )
        captured = capsys.readouterr()
        assert code == 0
        assert json.loads(captured.out) == {
            "broker": "kafka://localhost:9092",
            "id": "billing",
            "broker_session_timeout": 45,
        }

    def test_faust_round_trip_through_cli(self, tmp_path: Path) -> None:
        yaml = pytest.importorskip("yaml")
        faust_file = tmp_path / "faust.yaml"
        faust_file.write_text(
            "id: billing\nbroker: kafka://localhost:9092\n"
            "broker_session_timeout: 45\n"
        )
        conf_file = tmp_path / "client.yaml"

        subprocess.run(
            [
                sys.executable, "-m", "kafkaesq",
                str(faust_file), "--to", "confluent",
                "--format", "yaml", "-o", str(conf_file),
            ],
            capture_output=True, text=True, check=True,
        )
        proc = subprocess.run(
            [
                sys.executable, "-m", "kafkaesq",
                str(conf_file), "--to", "faust", "--format", "yaml",
            ],
            capture_output=True, text=True, check=True,
        )
        assert yaml.safe_load(proc.stdout) == {
            "id": "billing",
            "broker": "kafka://localhost:9092",
            "broker_session_timeout": 45,
        }
