# kafkaesq

A library for parsing Kafka configurations for all Python libraries.

Kafka client libraries in Python don't agree on how a config should be
spelled. [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python)
takes librdkafka-style dicts with dotted keys and loosely typed values, while
[aiokafka](https://github.com/aio-libs/aiokafka) takes snake_case constructor
kwargs with native Python types. **kafkaesq** converts between the two, in both
directions, so one config can drive both clients.

```
pip install kafkaesq
```

No runtime dependencies — you don't need either Kafka library installed to
convert configs.

## Usage

### confluent-kafka → aiokafka

```python
from kafkaesq import confluent_to_aiokafka

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "billing",
    "security.protocol": "sasl_ssl",
    "sasl.mechanism": "scram-sha-256",
    "sasl.username": "user",
    "sasl.password": "secret",
    "enable.auto.commit": "false",
    "auto.offset.reset": "smallest",   # librdkafka alias
}

kwargs = confluent_to_aiokafka(conf)
# {'bootstrap_servers': 'localhost:9092', 'group_id': 'billing',
#  'security_protocol': 'SASL_SSL', 'sasl_mechanism': 'SCRAM-SHA-256',
#  'sasl_plain_username': 'user', 'sasl_plain_password': 'secret',
#  'enable_auto_commit': False, 'auto_offset_reset': 'earliest'}

from aiokafka import AIOKafkaConsumer
consumer = AIOKafkaConsumer("payments", **kwargs)
```

Values are normalized along the way: string booleans and integers become real
`bool`/`int`, librdkafka offset-reset aliases (`smallest`, `largest`, ...) are
translated, `acks=-1` becomes `"all"`, `compression.type="none"` becomes
`None`, and librdkafka `ssl.*` file options are folded into a ready-made
`ssl_context`.

### aiokafka → confluent-kafka

```python
from kafkaesq import aiokafka_to_confluent

conf = aiokafka_to_confluent(
    bootstrap_servers=["a:9092", "b:9092"],
    group_id="billing",
    enable_auto_commit=False,
    compression_type=None,
)
# {'bootstrap.servers': 'a:9092,b:9092', 'group.id': 'billing',
#  'enable.auto.commit': False, 'compression.type': 'none'}

from confluent_kafka import Consumer
consumer = Consumer(conf)
```

A plain dict of kwargs works too: `aiokafka_to_confluent(kwargs_dict)`.

### Keys that can't be converted

Some options only exist on one side (callbacks like `error_cb`, librdkafka
internals like `statistics.interval.ms`, aiokafka's `value_deserializer`,
...). By default these are dropped with a `KafkaesqWarning`; choose stricter
or quieter behavior with `on_unmapped`:

```python
confluent_to_aiokafka(conf, on_unmapped="raise")   # UnmappedConfigError
confluent_to_aiokafka(conf, on_unmapped="ignore")  # drop silently
```

## Development

```
pip install -e .[dev]
pytest
```
