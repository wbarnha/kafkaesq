"""Integration tests against the real client libraries.

Two layers of confirmation:

1. Real-world configs taken verbatim from examples published on GitHub
   (source URL noted on each fixture) convert to the expected output.
2. The converted output is fed to the *actual* client constructors.
   All three libraries validate option names at construction time
   (confluent-kafka raises ``KafkaException`` on unknown properties,
   aiokafka raises ``TypeError`` on unknown kwargs, kafka-python raises
   ``AssertionError``/``KafkaConfigurationError`` on unrecognized configs),
   so successful construction proves every emitted key is real. No broker
   is required — clients are constructed but never started or polled.

The client libraries are optional test dependencies; each class skips if
its library is not installed.
"""

from __future__ import annotations

import asyncio
import os
import ssl
import warnings
from typing import Any

import pytest

from kafkaesq import (
    aiokafka_to_confluent,
    confluent_to_aiokafka,
    confluent_to_kafka_python,
    kafka_python_to_confluent,
)

# https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/consumer.py
CONFLUENT_EXAMPLE_CONSUMER: dict[str, Any] = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "example_consumer",
    "session.timeout.ms": 6000,
    "auto.offset.reset": "earliest",
    "enable.auto.offset.store": False,
}

# Confluent Cloud client template, as used by
# https://github.com/confluentinc/examples/tree/master/clients/cloud/python
# and https://github.com/confluentinc/confluent-kafka-python/blob/master/examples/sasl_producer.py
CONFLUENT_CLOUD_CONFIG: dict[str, Any] = {
    "bootstrap.servers": "pkc-00000.us-west-2.aws.confluent.cloud:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": "CLUSTER_API_KEY",
    "sasl.password": "CLUSTER_API_SECRET",
    "session.timeout.ms": 45000,
}

# https://github.com/dpkp/kafka-python/blob/master/example.py
KAFKA_PYTHON_EXAMPLE_CONSUMER: dict[str, Any] = {
    "bootstrap_servers": "localhost:9092",
    "auto_offset_reset": "earliest",
    "consumer_timeout_ms": 1000,
}

# https://github.com/aio-libs/aiokafka/blob/master/examples/ssl_consume_produce.py
# (its ssl_context comes from create_ssl_context(cafile=..., certfile=...,
# keyfile=..., password=...))
AIOKAFKA_SSL_EXAMPLE: dict[str, Any] = {
    "bootstrap_servers": "localhost:9093",
    "security_protocol": "SSL",
}


class TestRealWorldConfigs:
    def test_confluent_example_consumer_to_aiokafka(self) -> None:
        # enable.auto.offset.store is a librdkafka-only knob with no
        # aiokafka equivalent.
        with pytest.warns(match="enable.auto.offset.store"):
            result = confluent_to_aiokafka(CONFLUENT_EXAMPLE_CONSUMER)
        assert result == {
            "bootstrap_servers": "localhost:9092",
            "group_id": "example_consumer",
            "session_timeout_ms": 6000,
            "auto_offset_reset": "earliest",
        }

    def test_confluent_cloud_config_to_aiokafka(self) -> None:
        result = confluent_to_aiokafka(CONFLUENT_CLOUD_CONFIG)
        # librdkafka uses the system CA store when no ssl.* keys are set;
        # aiokafka requires an explicit ssl_context, so one is supplied.
        assert isinstance(result.pop("ssl_context"), ssl.SSLContext)
        assert result == {
            "bootstrap_servers": "pkc-00000.us-west-2.aws.confluent.cloud:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "CLUSTER_API_KEY",
            "sasl_plain_password": "CLUSTER_API_SECRET",
            "session_timeout_ms": 45000,
        }

    def test_confluent_cloud_config_to_kafka_python(self) -> None:
        result = confluent_to_kafka_python(CONFLUENT_CLOUD_CONFIG)
        assert result == {
            "bootstrap_servers": "pkc-00000.us-west-2.aws.confluent.cloud:9092",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "CLUSTER_API_KEY",
            "sasl_plain_password": "CLUSTER_API_SECRET",
            "session_timeout_ms": 45000,
        }

    def test_kafka_python_example_consumer_to_confluent(self) -> None:
        # consumer_timeout_ms is client-side iteration behavior with no
        # librdkafka equivalent.
        with pytest.warns(match="consumer_timeout_ms"):
            result = kafka_python_to_confluent(KAFKA_PYTHON_EXAMPLE_CONSUMER)
        assert result == {
            "bootstrap.servers": "localhost:9092",
            "auto.offset.reset": "earliest",
        }

    def test_aiokafka_ssl_example_to_confluent(self) -> None:
        result = aiokafka_to_confluent(AIOKAFKA_SSL_EXAMPLE)
        assert result == {
            "bootstrap.servers": "localhost:9093",
            "security.protocol": "SSL",
        }


PRODUCER_KWARGS: dict[str, Any] = {
    "bootstrap_servers": ["localhost:9092", "localhost:9093"],
    "client_id": "kafkaesq-it",
    "request_timeout_ms": 30000,
    "metadata_max_age_ms": 300000,
    "connections_max_idle_ms": 540000,
    "retry_backoff_ms": 100,
    "acks": "all",
    "enable_idempotence": True,
    "compression_type": "gzip",
    "linger_ms": 5,
    "max_request_size": 1048576,
    "transactional_id": "kafkaesq-tx",
    "transaction_timeout_ms": 60000,
}

CONSUMER_KWARGS: dict[str, Any] = {
    "bootstrap_servers": "localhost:9092",
    "client_id": "kafkaesq-it",
    "group_id": "kafkaesq-group",
    "group_instance_id": "kafkaesq-instance-1",
    "auto_offset_reset": "earliest",
    "enable_auto_commit": False,
    "auto_commit_interval_ms": 5000,
    "fetch_min_bytes": 1,
    "fetch_max_bytes": 52428800,
    "fetch_max_wait_ms": 500,
    "max_partition_fetch_bytes": 1048576,
    "session_timeout_ms": 45000,
    "heartbeat_interval_ms": 3000,
    "max_poll_interval_ms": 300000,
    "isolation_level": "read_committed",
    "check_crcs": True,
}


class TestConfluentAcceptsConvertedConfigs:
    """confluent_kafka.Producer/Consumer raise KafkaException on any
    property librdkafka does not know, so construction validates every
    dotted key kafkaesq emits."""

    def test_construction_rejects_unknown_properties(self) -> None:
        confluent_kafka = pytest.importorskip("confluent_kafka")
        with pytest.raises(confluent_kafka.KafkaException):
            confluent_kafka.Producer({"definitely.not.a.property": "x"})

    def test_producer_accepts_every_mapped_producer_key(self) -> None:
        confluent_kafka = pytest.importorskip("confluent_kafka")
        conf = aiokafka_to_confluent(PRODUCER_KWARGS)
        producer = confluent_kafka.Producer(conf)
        assert producer is not None

    def test_consumer_accepts_every_mapped_consumer_key(self) -> None:
        confluent_kafka = pytest.importorskip("confluent_kafka")
        conf = aiokafka_to_confluent(CONSUMER_KWARGS)
        consumer = confluent_kafka.Consumer(conf)
        consumer.close()

    def test_consumer_accepts_kafka_python_ssl_conf(self) -> None:
        confluent_kafka = pytest.importorskip("confluent_kafka")
        # librdkafka loads the CA file eagerly at construction, so point at
        # the real system CA bundle.
        paths = ssl.get_default_verify_paths()
        cafile = paths.cafile or paths.openssl_cafile
        if cafile is None or not os.path.exists(cafile):
            pytest.skip("no system CA bundle available")
        conf = kafka_python_to_confluent(
            bootstrap_servers="localhost:9093",
            group_id="g",
            security_protocol="SSL",
            ssl_cafile=cafile,
            ssl_check_hostname=False,
        )
        consumer = confluent_kafka.Consumer(conf)
        consumer.close()


class TestAiokafkaAcceptsConvertedConfigs:
    """AIOKafkaProducer/AIOKafkaConsumer have explicit signatures, so any
    kwarg kafkaesq emits that aiokafka does not support raises TypeError at
    construction."""

    @staticmethod
    def _construct(client_cls: type, *args: Any, **kwargs: Any) -> None:
        async def build() -> None:
            client = client_cls(*args, **kwargs)
            assert client is not None

        asyncio.run(build())

    def test_construction_rejects_unknown_kwargs(self) -> None:
        aiokafka = pytest.importorskip("aiokafka")
        with pytest.raises(TypeError):
            self._construct(aiokafka.AIOKafkaConsumer, definitely_not_a_kwarg=1)

    def test_consumer_accepts_every_mapped_consumer_key(self) -> None:
        aiokafka = pytest.importorskip("aiokafka")
        kwargs = confluent_to_aiokafka(aiokafka_to_confluent(CONSUMER_KWARGS))
        self._construct(aiokafka.AIOKafkaConsumer, "some-topic", **kwargs)

    def test_producer_accepts_every_mapped_producer_key(self) -> None:
        aiokafka = pytest.importorskip("aiokafka")
        kwargs = confluent_to_aiokafka(aiokafka_to_confluent(PRODUCER_KWARGS))
        self._construct(aiokafka.AIOKafkaProducer, **kwargs)

    def test_consumer_accepts_converted_confluent_cloud_config(self) -> None:
        aiokafka = pytest.importorskip("aiokafka")
        kwargs = confluent_to_aiokafka(CONFLUENT_CLOUD_CONFIG)
        # Without the supplied default ssl_context this would raise
        # ValueError("Cannot use SASL_SSL security protocol without ssl_context").
        self._construct(aiokafka.AIOKafkaConsumer, "some-topic", **kwargs)

    def test_consumer_accepts_converted_example_config(self) -> None:
        aiokafka = pytest.importorskip("aiokafka")
        kwargs = confluent_to_aiokafka(
            CONFLUENT_EXAMPLE_CONSUMER, on_unmapped="ignore"
        )
        self._construct(aiokafka.AIOKafkaConsumer, "some-topic", **kwargs)


class TestKafkaPythonAcceptsConvertedConfigs:
    """KafkaProducer/KafkaConsumer reject unrecognized config keys at
    construction, validating every snake_case key kafkaesq emits.
    api_version is pinned so the constructor skips broker version probing
    and never needs a live broker."""

    def test_construction_rejects_unknown_kwargs(self) -> None:
        kafka = pytest.importorskip("kafka")
        with pytest.raises(Exception, match="[Uu]nrecognized"):
            kafka.KafkaConsumer(
                bootstrap_servers="localhost:9092",
                api_version=(2, 6, 0),
                definitely_not_a_kwarg=1,
            )

    def test_consumer_accepts_every_mapped_consumer_key(self) -> None:
        kafka = pytest.importorskip("kafka")
        kwargs = confluent_to_kafka_python(kafka_python_to_confluent(CONSUMER_KWARGS))
        consumer = kafka.KafkaConsumer(api_version=(2, 6, 0), **kwargs)
        consumer.close(autocommit=False)

    def test_producer_accepts_every_mapped_producer_key(self) -> None:
        kafka = pytest.importorskip("kafka")
        kwargs = confluent_to_kafka_python(kafka_python_to_confluent(PRODUCER_KWARGS))
        producer = kafka.KafkaProducer(api_version=(2, 6, 0), **kwargs)
        producer.close(timeout=1)

    def test_consumer_accepts_converted_confluent_cloud_config(self) -> None:
        kafka = pytest.importorskip("kafka")
        kwargs = confluent_to_kafka_python(CONFLUENT_CLOUD_CONFIG)
        consumer = kafka.KafkaConsumer(api_version=(2, 6, 0), **kwargs)
        consumer.close(autocommit=False)

    def test_consumer_accepts_converted_ssl_file_config(self) -> None:
        kafka = pytest.importorskip("kafka")
        kwargs = confluent_to_kafka_python(
            {
                "bootstrap.servers": "localhost:9093",
                "group.id": "g",
                "security.protocol": "SSL",
                "ssl.ca.location": "/etc/ssl/ca.pem",
                "ssl.certificate.location": "/etc/ssl/client.pem",
                "ssl.key.location": "/etc/ssl/client.key",
                "ssl.endpoint.identification.algorithm": "none",
            }
        )
        # kafka-python loads SSL files on connect, not at construction.
        consumer = kafka.KafkaConsumer(api_version=(2, 6, 0), **kwargs)
        consumer.close(autocommit=False)


class TestCrossLibraryEquivalence:
    """The same source config drives all three clients."""

    def test_confluent_cloud_config_constructs_all_three_clients(self) -> None:
        confluent_kafka = pytest.importorskip("confluent_kafka")
        aiokafka = pytest.importorskip("aiokafka")
        kafka = pytest.importorskip("kafka")

        with warnings.catch_warnings():
            warnings.simplefilter("error")  # every key must convert cleanly
            aio_kwargs = confluent_to_aiokafka(CONFLUENT_CLOUD_CONFIG)
            kp_kwargs = confluent_to_kafka_python(CONFLUENT_CLOUD_CONFIG)

        consumer = confluent_kafka.Consumer(
            {**CONFLUENT_CLOUD_CONFIG, "group.id": "g"}
        )
        consumer.close()

        async def build() -> None:
            aiokafka.AIOKafkaConsumer("t", group_id="g", **aio_kwargs)

        asyncio.run(build())

        kp_consumer = kafka.KafkaConsumer(
            group_id="g", api_version=(2, 6, 0), **kp_kwargs
        )
        kp_consumer.close(autocommit=False)
