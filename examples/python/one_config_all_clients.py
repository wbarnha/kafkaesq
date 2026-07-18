#!/usr/bin/env python3
"""Drive every supported library from a single confluent-style config file.

Reads a librdkafka-style ``key=value`` .properties file and prints the
equivalent aiokafka/kafka-python constructor kwargs and Faust app settings.
Needs only kafkaesq — none of the client libraries have to be installed.

Usage::

    python one_config_all_clients.py [path/to/client.properties]

Defaults to the local-consumer.properties example next to this script.
"""

from __future__ import annotations

import sys
from pathlib import Path
from pprint import pprint

from kafkaesq import (
    confluent_to_aiokafka,
    confluent_to_faust,
    confluent_to_kafka_python,
)

DEFAULT_CONFIG = Path(__file__).parent.parent / "configs" / "local-consumer.properties"


def read_properties(path: Path) -> dict[str, str]:
    config: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "!")):
            key, _, value = line.partition("=")
            config[key.strip()] = value.strip()
    return config


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_CONFIG
    conf = read_properties(path)
    print(f"# source: {path}")

    print("\n# aiokafka — AIOKafkaConsumer(**kwargs) / AIOKafkaProducer(**kwargs)")
    pprint(confluent_to_aiokafka(conf, on_unmapped="ignore"))

    print("\n# kafka-python — KafkaConsumer(**kwargs) / KafkaProducer(**kwargs)")
    pprint(confluent_to_kafka_python(conf, on_unmapped="ignore"))

    print("\n# faust — faust.App(settings.pop('id'), **settings)")
    pprint(confluent_to_faust(conf, on_unmapped="ignore"))


if __name__ == "__main__":
    main()
