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
        # SASL_SSL without ssl.* keys gets a default ssl_context.
        assert isinstance(result.pop("ssl_context"), ssl.SSLContext)
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
        kwargs = confluent_to_aiokafka(original)
        # SASL_SSL gets a default ssl_context, which cannot round-trip.
        assert isinstance(kwargs.pop("ssl_context"), ssl.SSLContext)
        assert aiokafka_to_confluent(kwargs) == original

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
        result = confluent_to_aiokafka(aiokafka_to_confluent(original))
        assert isinstance(result.pop("ssl_context"), ssl.SSLContext)
        assert result == original


class TestConfluentToKafkaPython:
    def test_shared_kwargs_match_aiokafka_names(self) -> None:
        from kafkaesq import confluent_to_kafka_python

        result = confluent_to_kafka_python(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": "billing",
                "enable.auto.commit": "false",
                "auto.offset.reset": "smallest",
                "acks": -1,
                "compression.type": "none",
            }
        )
        assert result == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "billing",
            "enable_auto_commit": False,
            "auto_offset_reset": "earliest",
            "acks": "all",
            "compression_type": None,
        }

    def test_ssl_files_map_directly(self) -> None:
        from kafkaesq import confluent_to_kafka_python

        result = confluent_to_kafka_python(
            {
                "security.protocol": "ssl",
                "ssl.ca.location": "/etc/ssl/ca.pem",
                "ssl.certificate.location": "/etc/ssl/client.pem",
                "ssl.key.location": "/etc/ssl/client.key",
                "ssl.key.password": "hunter2",
                "ssl.crl.location": "/etc/ssl/crl.pem",
                "ssl.cipher.suites": "ECDHE-RSA-AES256-GCM-SHA384",
                "ssl.endpoint.identification.algorithm": "none",
            }
        )
        assert result == {
            "security_protocol": "SSL",
            "ssl_cafile": "/etc/ssl/ca.pem",
            "ssl_certfile": "/etc/ssl/client.pem",
            "ssl_keyfile": "/etc/ssl/client.key",
            "ssl_password": "hunter2",
            "ssl_crlfile": "/etc/ssl/crl.pem",
            "ssl_ciphers": "ECDHE-RSA-AES256-GCM-SHA384",
            "ssl_check_hostname": False,
        }

    def test_no_ssl_context_is_built(self) -> None:
        from kafkaesq import confluent_to_kafka_python

        result = confluent_to_kafka_python({"ssl.ca.location": "/etc/ssl/ca.pem"})
        assert "ssl_context" not in result
        assert result == {"ssl_cafile": "/etc/ssl/ca.pem"}

    def test_cert_verification_toggle_is_unmapped(self) -> None:
        from kafkaesq import confluent_to_kafka_python

        with pytest.warns(KafkaesqWarning, match="enable.ssl.certificate.verification"):
            result = confluent_to_kafka_python(
                {"group.id": "g", "enable.ssl.certificate.verification": "false"}
            )
        assert result == {"group_id": "g"}

    def test_unmapped_raise(self) -> None:
        from kafkaesq import confluent_to_kafka_python

        with pytest.raises(UnmappedConfigError, match="stats_cb"):
            confluent_to_kafka_python({"stats_cb": print}, on_unmapped="raise")


class TestKafkaPythonToConfluent:
    def test_basic_kwargs(self) -> None:
        from kafkaesq import kafka_python_to_confluent

        result = kafka_python_to_confluent(
            bootstrap_servers=["a:9092", "b:9092"],
            group_id="billing",
            enable_auto_commit=False,
            session_timeout_ms=45000,
        )
        assert result == {
            "bootstrap.servers": "a:9092,b:9092",
            "group.id": "billing",
            "enable.auto.commit": False,
            "session.timeout.ms": 45000,
        }

    def test_ssl_files_map_back(self) -> None:
        from kafkaesq import kafka_python_to_confluent

        result = kafka_python_to_confluent(
            security_protocol="SSL",
            ssl_cafile="/etc/ssl/ca.pem",
            ssl_certfile="/etc/ssl/client.pem",
            ssl_keyfile="/etc/ssl/client.key",
            ssl_password="hunter2",
            ssl_check_hostname=False,
        )
        assert result == {
            "security.protocol": "SSL",
            "ssl.ca.location": "/etc/ssl/ca.pem",
            "ssl.certificate.location": "/etc/ssl/client.pem",
            "ssl.key.location": "/etc/ssl/client.key",
            "ssl.key.password": "hunter2",
            "ssl.endpoint.identification.algorithm": "none",
        }

    def test_ssl_context_is_unmapped(self) -> None:
        from kafkaesq import kafka_python_to_confluent

        context = ssl.create_default_context()
        with pytest.warns(KafkaesqWarning, match="ssl_context"):
            result = kafka_python_to_confluent(
                bootstrap_servers="a:9092", ssl_context=context
            )
        assert result == {"bootstrap.servers": "a:9092"}

    def test_aiokafka_reverse_does_not_accept_ssl_files(self) -> None:
        with pytest.warns(KafkaesqWarning, match="ssl_cafile"):
            result = aiokafka_to_confluent(ssl_cafile="/etc/ssl/ca.pem")
        assert result == {}

    def test_round_trip(self) -> None:
        from kafkaesq import confluent_to_kafka_python, kafka_python_to_confluent

        original = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "billing",
            "security.protocol": "SSL",
            "ssl.ca.location": "/etc/ssl/ca.pem",
            "ssl.endpoint.identification.algorithm": "https",
            "session.timeout.ms": 45000,
            "acks": "all",
        }
        assert kafka_python_to_confluent(confluent_to_kafka_python(original)) == original


class TestBuildSslContextFlag:
    def test_ssl_keys_consumed_silently_when_disabled(self) -> None:
        import warnings as warnings_module

        with warnings_module.catch_warnings():
            warnings_module.simplefilter("error")
            result = confluent_to_aiokafka(
                {
                    "bootstrap.servers": "localhost:9093",
                    "security.protocol": "SSL",
                    "ssl.ca.location": "/does/not/exist/ca.pem",
                },
                build_ssl_context=False,
            )
        assert result == {
            "bootstrap_servers": "localhost:9093",
            "security_protocol": "SSL",
        }

    def test_no_default_context_for_sasl_ssl_when_disabled(self) -> None:
        result = confluent_to_aiokafka(
            {"security.protocol": "SASL_SSL"}, build_ssl_context=False
        )
        assert result == {"security_protocol": "SASL_SSL"}
