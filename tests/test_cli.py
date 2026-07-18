from __future__ import annotations

import io
import json
import subprocess
import sys
from pathlib import Path

import pytest

from kafkaesq.cli import main

CC_PROPERTIES = """\
# Confluent Cloud client template
bootstrap.servers=pkc-00000.us-west-2.aws.confluent.cloud:9092
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=CLUSTER_API_KEY
sasl.password=CLUSTER_API_SECRET
session.timeout.ms=45000
"""


def run_cli(
    args: list[str], capsys: pytest.CaptureFixture[str]
) -> tuple[int, str, str]:
    code = main(args)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


class TestPropertiesToKwargs:
    def test_confluent_properties_to_kafka_python_json(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "client.properties"
        config_file.write_text(CC_PROPERTIES)

        code, out, err = run_cli([str(config_file), "--to", "kafka-python"], capsys)

        assert code == 0
        assert json.loads(out) == {
            "bootstrap_servers": "pkc-00000.us-west-2.aws.confluent.cloud:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "CLUSTER_API_KEY",
            "sasl_plain_password": "CLUSTER_API_SECRET",
            "session_timeout_ms": 45000,
        }
        assert err == ""

    def test_confluent_properties_to_aiokafka_drops_ssl_context_with_note(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "client.properties"
        config_file.write_text(CC_PROPERTIES)

        code, out, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)

        assert code == 0
        result = json.loads(out)
        assert "ssl_context" not in result
        assert result["security_protocol"] == "SASL_SSL"
        assert result["session_timeout_ms"] == 45000
        assert "create_ssl_context" in err

    def test_ssl_file_options_appear_in_note(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "ssl.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9093\n"
            "security.protocol=SSL\n"
            "ssl.ca.location=/etc/ssl/ca.pem\n"
            "ssl.certificate.location=/etc/ssl/client.pem\n"
            "ssl.key.location=/etc/ssl/client.key\n"
        )

        code, out, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)

        assert code == 0
        assert "ssl_context" not in json.loads(out)
        assert "cafile='/etc/ssl/ca.pem'" in err
        assert "certfile='/etc/ssl/client.pem'" in err
        assert "keyfile='/etc/ssl/client.key'" in err

    def test_values_are_normalized(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "client.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9092\n"
            "group.id=billing\n"
            "enable.auto.commit=false\n"
            "auto.offset.reset=smallest\n"
            "compression.type=none\n"
            "acks=-1\n"
        )

        code, out, _ = run_cli([str(config_file), "--to", "kafka-python"], capsys)

        assert code == 0
        assert json.loads(out) == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "billing",
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "compression_type": None,
            "acks": "all",
        }


class TestKwargsToProperties:
    def test_json_kwargs_to_confluent_properties(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "kwargs.json"
        config_file.write_text(
            json.dumps(
                {
                    "bootstrap_servers": ["a:9092", "b:9092"],
                    "group_id": "billing",
                    "enable_auto_commit": False,
                    "ssl_cafile": "/etc/ssl/ca.pem",
                }
            )
        )

        code, out, err = run_cli(
            [str(config_file), "--to", "confluent", "--format", "properties"], capsys
        )

        assert code == 0
        assert out == (
            "bootstrap.servers=a:9092,b:9092\n"
            "enable.auto.commit=false\n"
            "group.id=billing\n"
            "ssl.ca.location=/etc/ssl/ca.pem\n"
        )
        assert err == ""

    def test_output_file(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "kwargs.json"
        config_file.write_text(json.dumps({"bootstrap_servers": "localhost:9092"}))
        out_file = tmp_path / "client.properties"

        code, out, _ = run_cli(
            [
                str(config_file),
                "--to",
                "confluent",
                "--format",
                "properties",
                "-o",
                str(out_file),
            ],
            capsys,
        )

        assert code == 0
        assert out == ""
        assert out_file.read_text() == "bootstrap.servers=localhost:9092\n"

    def test_properties_format_rejected_for_snake_targets(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        code, _, err = run_cli(
            ["--to", "aiokafka", "--format", "properties"], capsys
        )
        assert code == 2
        assert "only valid with --to confluent" in err


class TestSourceDetection:
    def test_dotted_keys_detected_as_confluent(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.json"
        config_file.write_text(json.dumps({"bootstrap.servers": "localhost:9092"}))
        code, out, _ = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 0
        assert json.loads(out) == {"bootstrap_servers": "localhost:9092"}

    def test_ssl_file_kwargs_detected_as_kafka_python(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "kp.json"
        config_file.write_text(
            json.dumps(
                {"bootstrap_servers": "localhost:9093", "ssl_cafile": "/ca.pem"}
            )
        )
        code, out, _ = run_cli([str(config_file), "--to", "confluent"], capsys)
        assert code == 0
        assert json.loads(out) == {
            "bootstrap.servers": "localhost:9093",
            "ssl.ca.location": "/ca.pem",
        }

    def test_kafka_python_to_aiokafka_via_bridge(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "kp.json"
        config_file.write_text(
            json.dumps(
                {
                    "bootstrap_servers": "localhost:9093",
                    "group_id": "g",
                    "security_protocol": "SSL",
                    "ssl_cafile": "/ca.pem",
                }
            )
        )
        code, out, err = run_cli(
            [str(config_file), "--from", "kafka-python", "--to", "aiokafka"], capsys
        )
        assert code == 0
        result = json.loads(out)
        # ssl file kwargs become an ssl_context, which is dropped with a note.
        assert result == {
            "bootstrap_servers": "localhost:9093",
            "group_id": "g",
            "security_protocol": "SSL",
        }
        assert "cafile='/ca.pem'" in err

    def test_same_source_and_target_errors(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.json"
        config_file.write_text(json.dumps({"bootstrap.servers": "localhost:9092"}))
        code, _, err = run_cli([str(config_file), "--to", "confluent"], capsys)
        assert code == 1
        assert "nothing to convert" in err


class TestStdinAndErrors:
    def test_stdin_input(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            sys, "stdin", io.StringIO("bootstrap.servers=localhost:9092\n")
        )
        code, out, _ = run_cli(["--to", "kafka-python"], capsys)
        assert code == 0
        assert json.loads(out) == {"bootstrap_servers": "localhost:9092"}

    def test_unmapped_keys_warn_on_stderr(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9092\nstatistics.interval.ms=1000\n"
        )
        code, out, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 0
        assert json.loads(out) == {"bootstrap_servers": "localhost:9092"}
        assert "warning:" in err
        assert "statistics.interval.ms" in err

    def test_on_unmapped_raise_exits_nonzero(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9092\nstatistics.interval.ms=1000\n"
        )
        code, _, err = run_cli(
            [str(config_file), "--to", "aiokafka", "--on-unmapped", "raise"], capsys
        )
        assert code == 1
        assert "statistics.interval.ms" in err

    def test_on_unmapped_ignore_is_silent(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9092\nstatistics.interval.ms=1000\n"
        )
        code, _, err = run_cli(
            [str(config_file), "--to", "aiokafka", "--on-unmapped", "ignore"], capsys
        )
        assert code == 0
        assert err == ""

    def test_malformed_properties_line(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text("bootstrap.servers localhost:9092\n")
        code, _, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 1
        assert "expected key=value" in err

    def test_invalid_json(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.json"
        config_file.write_text("{not json")
        code, _, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 1
        assert "invalid JSON" in err

    def test_missing_input_file(self, capsys: pytest.CaptureFixture[str]) -> None:
        code, _, err = run_cli(["/no/such/file.properties", "--to", "aiokafka"], capsys)
        assert code == 1
        assert "cannot read" in err

    def test_comments_and_blank_lines_are_skipped(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "c.properties"
        config_file.write_text(
            "# a comment\n\n! another comment\nbootstrap.servers=localhost:9092\n"
        )
        code, out, _ = run_cli([str(config_file), "--to", "kafka-python"], capsys)
        assert code == 0
        assert json.loads(out) == {"bootstrap_servers": "localhost:9092"}


class TestEntryPoints:
    def test_python_dash_m_invocation(self, tmp_path: Path) -> None:
        config_file = tmp_path / "client.properties"
        config_file.write_text(CC_PROPERTIES)

        proc = subprocess.run(
            [sys.executable, "-m", "kafkaesq", str(config_file), "--to", "kafka-python"],
            capture_output=True,
            text=True,
            check=True,
        )
        result = json.loads(proc.stdout)
        assert result["sasl_mechanism"] == "PLAIN"
        assert result["session_timeout_ms"] == 45000

    def test_round_trip_through_cli(self, tmp_path: Path) -> None:
        config_file = tmp_path / "client.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9092\n"
            "group.id=billing\n"
            "enable.auto.commit=false\n"
        )
        kwargs_file = tmp_path / "kwargs.json"
        back_file = tmp_path / "back.properties"

        subprocess.run(
            [
                sys.executable, "-m", "kafkaesq",
                str(config_file), "--to", "kafka-python", "-o", str(kwargs_file),
            ],
            capture_output=True, text=True, check=True,
        )
        subprocess.run(
            [
                sys.executable, "-m", "kafkaesq",
                str(kwargs_file), "--to", "confluent",
                "--format", "properties", "-o", str(back_file),
            ],
            capture_output=True, text=True, check=True,
        )

        assert back_file.read_text() == (
            "bootstrap.servers=localhost:9092\n"
            "enable.auto.commit=false\n"
            "group.id=billing\n"
        )


class TestYaml:
    def test_yaml_input_by_extension(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "client.yaml"
        config_file.write_text(
            "bootstrap.servers: localhost:9092\n"
            "group.id: billing\n"
            "enable.auto.commit: false\n"
            "session.timeout.ms: 45000\n"
        )
        code, out, _ = run_cli([str(config_file), "--to", "kafka-python"], capsys)
        assert code == 0
        assert json.loads(out) == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "billing",
            "enable_auto_commit": False,
            "session_timeout_ms": 45000,
        }

    def test_yaml_input_detected_from_stdin_content(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(
            sys, "stdin", io.StringIO("bootstrap_servers: localhost:9092\ngroup_id: g\n")
        )
        code, out, _ = run_cli(["--to", "confluent"], capsys)
        assert code == 0
        assert json.loads(out) == {
            "bootstrap.servers": "localhost:9092",
            "group.id": "g",
        }

    def test_yaml_output(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        yaml = pytest.importorskip("yaml")
        config_file = tmp_path / "client.properties"
        config_file.write_text(
            "bootstrap.servers=localhost:9092\ngroup.id=billing\n"
        )
        code, out, _ = run_cli(
            [str(config_file), "--to", "kafka-python", "--format", "yaml"], capsys
        )
        assert code == 0
        assert yaml.safe_load(out) == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "billing",
        }

    def test_yaml_round_trip_through_cli(self, tmp_path: Path) -> None:
        yaml = pytest.importorskip("yaml")
        config_file = tmp_path / "client.yml"
        config_file.write_text(
            "bootstrap.servers: localhost:9092\nenable.auto.commit: false\n"
        )
        kwargs_file = tmp_path / "kwargs.yaml"

        subprocess.run(
            [
                sys.executable, "-m", "kafkaesq",
                str(config_file), "--to", "kafka-python",
                "--format", "yaml", "-o", str(kwargs_file),
            ],
            capture_output=True, text=True, check=True,
        )
        proc = subprocess.run(
            [
                sys.executable, "-m", "kafkaesq",
                str(kwargs_file), "--to", "confluent", "--format", "yaml",
            ],
            capture_output=True, text=True, check=True,
        )
        assert yaml.safe_load(proc.stdout) == {
            "bootstrap.servers": "localhost:9092",
            "enable.auto.commit": False,
        }

    def test_explicit_input_format_overrides_detection(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # A .txt file whose content is YAML.
        config_file = tmp_path / "config.txt"
        config_file.write_text("bootstrap.servers: localhost:9092\n")
        code, out, _ = run_cli(
            [str(config_file), "--to", "aiokafka", "--input-format", "yaml"], capsys
        )
        assert code == 0
        assert json.loads(out) == {"bootstrap_servers": "localhost:9092"}

    def test_yaml_input_must_be_a_mapping(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        config_file = tmp_path / "bad.yaml"
        config_file.write_text("- just\n- a\n- list\n")
        code, _, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 1
        assert "must be a mapping" in err

    def test_missing_pyyaml_gives_install_hint(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import kafkaesq.cli as cli_module

        monkeypatch.setattr(cli_module, "yaml", None)
        config_file = tmp_path / "client.yaml"
        config_file.write_text("bootstrap.servers: localhost:9092\n")
        code, _, err = run_cli([str(config_file), "--to", "aiokafka"], capsys)
        assert code == 1
        assert "kafkaesq[yaml]" in err
