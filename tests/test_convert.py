from __future__ import annotations

import ssl

import pytest

from kafkaesq import (
    KafkaesqWarning,
    UnmappedConfigError,
    aiokafka_to_confluent,
    confluent_to_aiokafka,
)


class TestConfluentToAiokafka:
    def test_basic_consumer_config(self) -> None:
        result = confluent_to_aiokafka(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": "billing",
                "client.id": "worker-1",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": "false",
                "session.timeout.ms": "45000",
            }
        )
        assert result == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "billing",
            "client_id": "worker-1",
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "session_timeout_ms": 45000,
        }

    def test_basic_producer_config(self) -> None:
        result = confluent_to_aiokafka(
            {
                "bootstrap.servers": "a:9092,b:9092",
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "gzip",
                "linger.ms": 5,
                "transactional.id": "tx-1",
            }
        )
        assert result == {
            "bootstrap_servers": "a:9092,b:9092",
            "acks": "all",
            "enable_idempotence": True,
            "compression_type": "gzip",
            "linger_ms": 5,
            "transactional_id": "tx-1",
        }

    def test_confluent_aliases(self) -> None:
        result = confluent_to_aiokafka(
            {
                "metadata.broker.list": "localhost:9092",
                "sasl.mechanisms": "plain",
                "queue.buffering.max.ms": "10",
            }
        )
        assert result == {
            "bootstrap_servers": "localhost:9092",
            "sasl_mechanism": "PLAIN",
            "linger_ms": 10,
        }

    def test_security_values_are_normalized(self) -> None:
        result = confluent_to_aiokafka(
            {
                "security.protocol": "sasl_ssl",
                "sasl.mechanism": "scram-sha-256",
                "sasl.username": "user",
                "sasl.password": "secret",
            }
        )
        assert result == {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-256",
            "sasl_plain_username": "user",
            "sasl_plain_password": "secret",
        }

    @pytest.mark.parametrize(
        ("librdkafka_value", "aiokafka_value"),
        [
            ("smallest", "earliest"),
            ("beginning", "earliest"),
            ("largest", "latest"),
            ("end", "latest"),
            ("error", "none"),
            ("latest", "latest"),
        ],
    )
    def test_offset_reset_aliases(
        self, librdkafka_value: str, aiokafka_value: str
    ) -> None:
        result = confluent_to_aiokafka({"auto.offset.reset": librdkafka_value})
        assert result == {"auto_offset_reset": aiokafka_value}

    @pytest.mark.parametrize(
        ("acks_in", "acks_out"), [("all", "all"), (-1, "all"), ("-1", "all"), (1, 1), ("0", 0)]
    )
    def test_acks_values(self, acks_in: object, acks_out: object) -> None:
        assert confluent_to_aiokafka({"acks": acks_in}) == {"acks": acks_out}

    def test_compression_none_becomes_python_none(self) -> None:
        result = confluent_to_aiokafka({"compression.type": "none"})
        assert result == {"compression_type": None}

    def test_ssl_options_fold_into_ssl_context(self) -> None:
        result = confluent_to_aiokafka(
            {
                "security.protocol": "SSL",
                "enable.ssl.certificate.verification": "false",
                "ssl.endpoint.identification.algorithm": "none",
            }
        )
        context = result["ssl_context"]
        assert isinstance(context, ssl.SSLContext)
        assert context.check_hostname is False
        assert context.verify_mode == ssl.CERT_NONE
        assert result["security_protocol"] == "SSL"

    def test_unmapped_warns_and_drops_by_default(self) -> None:
        with pytest.warns(KafkaesqWarning, match="statistics.interval.ms"):
            result = confluent_to_aiokafka(
                {"bootstrap.servers": "localhost:9092", "statistics.interval.ms": 1000}
            )
        assert result == {"bootstrap_servers": "localhost:9092"}

    def test_unmapped_raise(self) -> None:
        with pytest.raises(UnmappedConfigError, match="error_cb"):
            confluent_to_aiokafka({"error_cb": print}, on_unmapped="raise")

    def test_unmapped_ignore(self) -> None:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = confluent_to_aiokafka(
                {"group.id": "g", "error_cb": print}, on_unmapped="ignore"
            )
        assert result == {"group_id": "g"}

    def test_invalid_on_unmapped_value(self) -> None:
        with pytest.raises(ValueError, match="on_unmapped"):
            confluent_to_aiokafka({"bogus.key": 1}, on_unmapped="explode")


class TestAiokafkaToConfluent:
    def test_basic_consumer_kwargs(self) -> None:
        result = aiokafka_to_confluent(
            bootstrap_servers="localhost:9092",
            group_id="billing",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_interval_ms=300000,
        )
        assert result == {
            "bootstrap.servers": "localhost:9092",
            "group.id": "billing",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "max.poll.interval.ms": 300000,
        }

    def test_accepts_dict_and_kwargs(self) -> None:
        result = aiokafka_to_confluent(
            {"bootstrap_servers": "a:9092", "client_id": "x"}, group_id="g"
        )
        assert result == {
            "bootstrap.servers": "a:9092",
            "client.id": "x",
            "group.id": "g",
        }

    def test_bootstrap_servers_list_is_joined(self) -> None:
        result = aiokafka_to_confluent(bootstrap_servers=["a:9092", "b:9092"])
        assert result == {"bootstrap.servers": "a:9092,b:9092"}

    def test_producer_kwargs(self) -> None:
        result = aiokafka_to_confluent(
            acks="all",
            compression_type=None,
            max_request_size=1048576,
            transactional_id="tx-1",
        )
        assert result == {
            "acks": "all",
            "compression.type": "none",
            "message.max.bytes": 1048576,
            "transactional.id": "tx-1",
        }

    def test_ssl_context_is_unmapped(self) -> None:
        context = ssl.create_default_context()
        with pytest.warns(KafkaesqWarning, match="ssl_context"):
            result = aiokafka_to_confluent(
                bootstrap_servers="a:9092", ssl_context=context
            )
        assert result == {"bootstrap.servers": "a:9092"}

    def test_unmapped_raise(self) -> None:
        with pytest.raises(UnmappedConfigError, match="value_deserializer"):
            aiokafka_to_confluent(value_deserializer=str, on_unmapped="raise")


class TestRoundTrip:
    def test_confluent_round_trip(self) -> None:
        original = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "billing",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "secret",
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
            "acks": "all",
        }
        assert aiokafka_to_confluent(confluent_to_aiokafka(original)) == original

    def test_aiokafka_round_trip(self) -> None:
        original = {
            "bootstrap_servers": "localhost:9092",
            "group_id": "billing",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "SCRAM-SHA-512",
            "sasl_plain_username": "user",
            "sasl_plain_password": "secret",
            "enable_auto_commit": False,
            "fetch_max_wait_ms": 500,
            "isolation_level": "read_committed",
        }
        assert confluent_to_aiokafka(aiokafka_to_confluent(original)) == original
