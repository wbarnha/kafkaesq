from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from kafkaesq.cli import main

JAVA_CONSUMER_PROPERTIES = """\
bootstrap.servers=broker1:9092,broker2:9092
group.id=billing
key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
max.poll.records=500
session.timeout.ms=45000
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="svc-user" password="s3cret";
ssl.truststore.location=/etc/kafka/client.truststore.jks
ssl.truststore.password=changeit
"""


def run_cli(
    args: list[str], capsys: pytest.CaptureFixture[str]
) -> tuple[int, str, str]:
    code = main(args)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


class TestValidateJavaConfig:
    def test_java_config_is_ok_with_guidance(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "consumer.properties"
        config_file.write_text(JAVA_CONSUMER_PROPERTIES)

        code, out, _ = run_cli(["validate", str(config_file)], capsys)

        assert code == 0
        assert "source: confluent" in out
        assert "java-only (6):" in out
        assert "ssl.truststore.location" in out
        assert "ssl.ca.location" in out  # guidance points at the PEM key
        assert "result: OK (6 warning(s))" in out

    def test_jaas_username_extracted_password_masked(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "consumer.properties"
        config_file.write_text(JAVA_CONSUMER_PROPERTIES)

        code, out, _ = run_cli(["validate", str(config_file)], capsys)

        assert code == 0
        assert 'found username="svc-user"' in out
        assert "s3cret" not in out
        assert "sasl.username" in out

    def test_strict_mode_fails_on_java_only_keys(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "consumer.properties"
        config_file.write_text(JAVA_CONSUMER_PROPERTIES)
        code, _, _ = run_cli(["validate", str(config_file), "--strict"], capsys)
        assert code == 1

    def test_dot_class_suffix_heuristic(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "bootstrap.servers=b:9092\n"
            "sasl.client.callback.handler.class=com.example.Handler\n"
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 0
        assert "sasl.client.callback.handler.class" in out
        assert "Java class-based setting" in out


class TestValidateValuesAndPortability:
    def test_bad_value_is_invalid_and_fails(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "bootstrap.servers=b:9092\nsession.timeout.ms=45s\n"
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 1
        assert "invalid (1):" in out
        assert "session.timeout.ms" in out
        assert "result: INVALID" in out

    def test_clean_config_is_ok(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "bootstrap.servers=b:9092\ngroup.id=g\nsession.timeout.ms=45000\n"
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 0
        assert "result: OK" in out
        assert "aiokafka 3/3" in out
        assert "kafka-python 3/3" in out
        # session.timeout.ms maps to faust; bootstrap/group.id too
        assert "faust 3/3" in out

    def test_librdkafka_extras_warn_but_pass(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Valid librdkafka keys outside kafkaesq's cross-library table must
        # not fail validation by default (they may be fine for the source).
        config_file = tmp_path / "extras.json"
        config_file.write_text(
            json.dumps(
                {
                    "bootstrap.servers": "b:9092",
                    "statistics.interval.ms": 1000,
                    "internal.termination.signal": 29,
                }
            )
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 0
        assert "unrecognized (2)" in out
        code, _, _ = run_cli(["validate", str(config_file), "--strict"], capsys)
        assert code == 1


class TestValidateGoConfigs:
    GO_CONSUMER = {
        "bootstrap.servers": "b:9092",
        "group.id": "orders",
        "auto.offset.reset": "earliest",
        "go.events.channel.enable": True,
        "go.application.rebalance.enable": True,
    }

    def test_go_binding_keys_get_guidance(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "go-consumer.json"
        config_file.write_text(json.dumps(self.GO_CONSUMER))
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 0
        assert "go-binding (2):" in out
        assert "go.events.channel.enable" in out
        assert ".Poll()" in out
        assert "result: OK (2 warning(s))" in out

    def test_unknown_go_prefixed_key_uses_generic_guidance(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "go.json"
        config_file.write_text(
            json.dumps({"bootstrap.servers": "b:9092", "go.future.option": 1})
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 0
        assert "go-binding (1):" in out
        assert "configured in Go code only" in out

    def test_strict_fails_on_go_binding_keys(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "go-consumer.json"
        config_file.write_text(json.dumps(self.GO_CONSUMER))
        code, _, _ = run_cli(["validate", str(config_file), "--strict"], capsys)
        assert code == 1

    def test_json_report_has_go_binding_section(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "go-consumer.json"
        config_file.write_text(json.dumps(self.GO_CONSUMER))
        code, out, _ = run_cli(
            ["validate", str(config_file), "--format", "json"], capsys
        )
        assert code == 0
        report = json.loads(out)
        assert sorted(report["go_binding"]) == [
            "go.application.rebalance.enable",
            "go.events.channel.enable",
        ]
        assert report["valid"] is True
        # The librdkafka keys still convert for Python targets.
        assert report["portability"]["aiokafka"]["portable"] == 3

    def test_ssl_file_keys_portability(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "ssl.properties"
        config_file.write_text(
            "ssl.ca.location=/ca.pem\nssl.crl.location=/crl.pem\n"
        )
        code, out, _ = run_cli(
            ["validate", str(config_file), "--format", "json"], capsys
        )
        assert code == 0
        report = json.loads(out)
        # ca.location works for both; crl.location only for kafka-python.
        assert report["portability"]["aiokafka"]["portable"] == 1
        assert report["portability"]["kafka-python"]["portable"] == 2


class TestValidateSourcesAndFormats:
    def test_snake_source_auto_detected(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "kwargs.json"
        config_file.write_text(
            json.dumps({"bootstrap_servers": "b:9092", "ssl_cafile": "/ca.pem"})
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 0
        assert "source: kafka-python" in out
        assert "result: OK" in out

    def test_faust_source(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "faust.json"
        config_file.write_text(
            json.dumps(
                {
                    "id": "app",
                    "broker": "kafka://b:9092",
                    "broker_session_timeout": "not-a-number",
                }
            )
        )
        code, out, _ = run_cli(["validate", str(config_file)], capsys)
        assert code == 1
        assert "source: faust" in out
        assert "broker_session_timeout" in out

    def test_json_report(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "consumer.properties"
        config_file.write_text(JAVA_CONSUMER_PROPERTIES)
        code, out, _ = run_cli(
            ["validate", str(config_file), "--format", "json"], capsys
        )
        assert code == 0
        report = json.loads(out)
        assert report["valid"] is True
        assert report["source"] == "confluent"
        assert "ssl.truststore.location" in report["java_only"]
        assert report["portability"]["aiokafka"]["portable"] == 5
        assert sorted(report["invalid"]) == []

    def test_stdin(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(sys, "stdin", io.StringIO("bootstrap.servers=b:9092\n"))
        code, out, _ = run_cli(["validate"], capsys)
        assert code == 0
        assert "result: OK" in out

    def test_missing_file(self, capsys: pytest.CaptureFixture[str]) -> None:
        code, _, err = run_cli(["validate", "/no/such/file"], capsys)
        assert code == 1
        assert "cannot read" in err

    def test_subprocess_entry_point(self, tmp_path: Path) -> None:
        config_file = tmp_path / "consumer.properties"
        config_file.write_text(JAVA_CONSUMER_PROPERTIES)
        proc = subprocess.run(
            [sys.executable, "-m", "kafkaesq", "validate", str(config_file)],
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0
        assert "java-only" in proc.stdout

    def test_convert_mode_still_works(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text("bootstrap.servers=b:9092\n")
        code, out, _ = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 0
        assert json.loads(out) == {"bootstrap_servers": "b:9092"}
