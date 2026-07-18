#!/usr/bin/env python3
"""Build a real AIOKafkaConsumer from a confluent-style .properties file.

Shows the intended end-to-end pattern: parse the ops-provided config file,
convert it with kafkaesq, and hand the kwargs straight to aiokafka. Requires
aiokafka (``pip install aiokafka``). The consumer is constructed but only
started if a broker is actually reachable — pass ``--run`` to try.

Usage::

    python aiokafka_consumer_from_properties.py [client.properties] [--run]
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from aiokafka import AIOKafkaConsumer

from kafkaesq import confluent_to_aiokafka

DEFAULT_CONFIG = Path(__file__).parent.parent / "configs" / "local-consumer.properties"


def read_properties(path: Path) -> dict[str, str]:
    config: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith(("#", "!")):
            key, _, value = line.partition("=")
            config[key.strip()] = value.strip()
    return config


async def main() -> None:
    args = [arg for arg in sys.argv[1:] if arg != "--run"]
    run = "--run" in sys.argv[1:]
    path = Path(args[0]) if args else DEFAULT_CONFIG

    kwargs = confluent_to_aiokafka(read_properties(path))
    consumer = AIOKafkaConsumer("payments", **kwargs)
    print(f"constructed AIOKafkaConsumer from {path.name} with:")
    for key, value in sorted(kwargs.items()):
        print(f"  {key}={value!r}")

    try:
        if run:
            await consumer.start()
            async for message in consumer:
                print(message.topic, message.partition, message.offset)
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
