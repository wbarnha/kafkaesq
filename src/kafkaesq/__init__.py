"""kafkaesq — convert Kafka client configs between Python Kafka libraries.

Take a config written for confluent-kafka and use it with aiokafka, and
vice versa::

    from kafkaesq import confluent_to_aiokafka, aiokafka_to_confluent

    kwargs = confluent_to_aiokafka({
        "bootstrap.servers": "localhost:9092",
        "group.id": "billing",
        "enable.auto.commit": "false",
    })
    # {'bootstrap_servers': 'localhost:9092', 'group_id': 'billing',
    #  'enable_auto_commit': False}

    conf = aiokafka_to_confluent(kwargs)
    # {'bootstrap.servers': 'localhost:9092', 'group.id': 'billing',
    #  'enable.auto.commit': False}
"""

from .convert import (
    KafkaesqWarning,
    UnmappedConfigError,
    aiokafka_to_confluent,
    confluent_to_aiokafka,
    confluent_to_faust,
    confluent_to_kafka_python,
    faust_to_confluent,
    kafka_python_to_confluent,
)

__all__ = [
    "KafkaesqWarning",
    "UnmappedConfigError",
    "aiokafka_to_confluent",
    "confluent_to_aiokafka",
    "confluent_to_faust",
    "confluent_to_kafka_python",
    "faust_to_confluent",
    "kafka_python_to_confluent",
]

try:
    # Written by setuptools_scm at build time from the git tag.
    from ._version import __version__
except ModuleNotFoundError:
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("kafkaesq")
    except PackageNotFoundError:
        __version__ = "0.0.0+unknown"
