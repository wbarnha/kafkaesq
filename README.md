# kafkaesq

[![CI](https://github.com/wbarnha/kafkaesq/actions/workflows/ci.yml/badge.svg)](https://github.com/wbarnha/kafkaesq/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/kafkaesq)](https://pypi.org/project/kafkaesq/)
[![Python versions](https://img.shields.io/pypi/pyversions/kafkaesq)](https://pypi.org/project/kafkaesq/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A library for parsing Kafka configurations for all Python libraries.

Kafka client libraries in Python don't agree on how a config should be
spelled. [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python)
takes librdkafka-style dicts with dotted keys and loosely typed values, while
[aiokafka](https://github.com/aio-libs/aiokafka) and
[kafka-python](https://github.com/dpkp/kafka-python) take snake_case
constructor kwargs with native Python types. **kafkaesq** converts between
them, in both directions, so one config can drive all of these clients.

```
pip install kafkaesq
```

No runtime dependencies — you don't need any Kafka library installed to
convert configs.

**Supported libraries:**
[confluent-kafka](https://github.com/confluentinc/confluent-kafka-python)
(and by extension every librdkafka-based client) ·
[aiokafka](https://github.com/aio-libs/aiokafka) ·
[kafka-python](https://github.com/dpkp/kafka-python) ·
[Faust](https://github.com/faust-streaming/faust)

**Contents:**
[Python API](#usage) ·
[Command line](#command-line) ·
[Validating configs](#validating-configs-including-from-non-python-projects) ·
[Unconvertible keys](#keys-that-cant-be-converted) ·
[Examples](#examples) ·
[Development](#development)

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
`ssl_context` (an SSL security protocol with no `ssl.*` keys gets a default
context, matching librdkafka's system-CA fallback — pass
`build_ssl_context=False` to skip contexts entirely, e.g. when the SSL files
don't exist on the converting machine).

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

`faust_to_confluent` / `confluent_to_faust` convert
[Faust](https://github.com/faust-streaming/faust) app settings the same way
(Faust configures authentication with a runtime `broker_credentials` object,
so security keys are reported as unmapped in that direction).

### confluent-kafka ↔ kafka-python

`confluent_to_kafka_python` and `kafka_python_to_confluent` work the same
way. kafka-python shares aiokafka's kwarg names for everything in the mapping
table, but takes SSL file paths directly, so librdkafka `ssl.*` options map
one-to-one instead of folding into an `ssl_context`:

```python
from kafkaesq import confluent_to_kafka_python

kwargs = confluent_to_kafka_python({
    "bootstrap.servers": "localhost:9092",
    "security.protocol": "SSL",
    "ssl.ca.location": "/etc/ssl/ca.pem",
    "ssl.certificate.location": "/etc/ssl/client.pem",
    "ssl.key.location": "/etc/ssl/client.key",
})
# {'bootstrap_servers': 'localhost:9092', 'security_protocol': 'SSL',
#  'ssl_cafile': '/etc/ssl/ca.pem', 'ssl_certfile': '/etc/ssl/client.pem',
#  'ssl_keyfile': '/etc/ssl/client.key'}

from kafka import KafkaConsumer
consumer = KafkaConsumer("payments", **kwargs)
```

### Command line

The `kafkaesq` command (also `python -m kafkaesq`) converts config *files*.
Input may be a librdkafka/Java-style `key=value` .properties file, JSON, or
YAML — auto-detected from the file extension and content, or forced with
`--input-format`. Output is JSON by default; `--format yaml` writes YAML and
`--format properties` writes a .properties file (confluent target only).

```console
$ kafkaesq client.properties --to aiokafka
{
  "bootstrap_servers": "pkc-00000.us-west-2.aws.confluent.cloud:9092",
  "sasl_mechanism": "PLAIN",
  ...
}

$ kafkaesq client.yaml --to kafka-python --format yaml
bootstrap_servers: localhost:9092
group_id: billing

$ kafkaesq kwargs.json --to confluent --format properties -o client.properties

$ cat client.properties | kafkaesq --to kafka-python
```

[Faust](https://github.com/faust-streaming/faust) app settings are supported
as a fourth library (`--to faust`, or as auto-detected input): the app `id`
maps to `group.id`, `kafka://` broker URLs map to `bootstrap.servers`, and
Faust's second-based timeouts convert to/from milliseconds:

```console
$ kafkaesq faust.yaml --to confluent --format properties
$ kafkaesq client.properties --to faust --format yaml
broker: kafka://localhost:9092
broker_session_timeout: 45
id: billing
```

The source library is auto-detected from key style (dotted keys → confluent,
`broker`/`broker_*`/`consumer_*`/`producer_*` → faust, snake_case →
aiokafka/kafka-python); override with `--from`. Keys that can't
be converted are reported as warnings on stderr (`--on-unmapped raise|warn|ignore`).
Because an `ssl_context` is a runtime object that can't be written to a config
file, converting an SSL config to aiokafka prints a note with the equivalent
`create_ssl_context(...)` call instead.

YAML support requires PyYAML: `pip install kafkaesq[yaml]`.

### Validating configs (including from non-Python projects)

`kafkaesq validate` checks a config file without converting it: are the keys
recognized, do the values parse, and which libraries is the config portable
to? Because librdkafka-based clients in every language (Go, .NET, C/C++,
Node) share confluent-kafka's dotted keys, and the Java client shares most of
them, this works on configs from non-Python projects too:

```console
$ kafkaesq validate consumer.properties
source: confluent (11 keys)
ok (5): bootstrap.servers, group.id, session.timeout.ms, security.protocol, sasl.mechanism
java-only (6):
  ssl.truststore.location: Java truststore (JKS/PKCS12); librdkafka-family and Python clients use ssl.ca.location with a PEM file (export with keytool/openssl)
  sasl.jaas.config: Java JAAS login string; librdkafka-family and Python clients use sasl.username / sasl.password instead (found username="svc"; password not shown)
  ...
portability: aiokafka 5/11 · kafka-python 5/11 · faust 3/11
result: OK (6 warning(s))
```

Java-only keys (JKS truststores, JAAS login strings, class-based
serializers, `max.poll.records`, ...) get targeted guidance instead of a
generic warning; credentials found in JAAS strings are never echoed.
confluent-kafka-go configs are supported the same way: the librdkafka keys
validate normally and the Go binding's `go.*` options (`go.events.channel.enable`,
`go.delivery.reports`, ...) are reported as Go-only with guidance. Pure-Go
clients (sarama, segmentio/kafka-go, franz-go) configure via Go structs and
option functions rather than a standard config file, so there is nothing
file-shaped to validate — use kafkaesq's confluent/JSON output as the
reference to translate from. Bad values (`session.timeout.ms=45s`) make
validation fail with exit code 1; unrecognized keys are warnings unless
`--strict` is passed. `--format json` emits the report as JSON for CI
pipelines.

### Keys that can't be converted

Some options only exist on one side (callbacks like `error_cb`, librdkafka
internals like `statistics.interval.ms`, aiokafka's `value_deserializer`,
...). By default these are dropped with a `KafkaesqWarning`; choose stricter
or quieter behavior with `on_unmapped`:

```python
confluent_to_aiokafka(conf, on_unmapped="raise")   # UnmappedConfigError
confluent_to_aiokafka(conf, on_unmapped="ignore")  # drop silently
```

## Examples

The [`examples/`](examples/) directory has working config files for every
supported library — a Confluent Cloud template, a Java client config with
Java-only keys, a confluent-kafka-go config with `go.*` binding keys, a Faust
YAML app config, aiokafka/kafka-python kwargs — plus a walkthrough README of
CLI commands to run against them and Python scripts showing the API end to
end.

## Development

```
pip install -e .[dev]
pytest
```

Versioning is automatic via [setuptools_scm](https://setuptools-scm.readthedocs.io/):
the version comes from the git tag, so releasing is just creating a GitHub
Release tagged `vX.Y.Z` — no version bump commit needed. Untagged builds get
PEP 440 dev versions (`0.2.1.dev3+g1a2b3c4`).

The suite includes integration tests that feed converted configs to the real
`confluent_kafka`, `aiokafka`, and `kafka-python` client constructors (no
broker needed — all three validate option names at construction time). They
skip automatically unless the clients are installed:

```
pip install -e .[dev,integration]
pytest
```

CI additionally verifies converted configs against non-Python clients:
a Go job constructs real confluent-kafka-go consumers/producers from
CLI-converted configs, a Java job checks every emitted `.properties` key
against the Java client's `ConsumerConfig` and constructs a `KafkaConsumer`,
and a Faust job asserts converted settings are reflected in a real
`faust.App` — see [`.github/workflows/ci.yml`](.github/workflows/ci.yml)
and [`ci/`](ci/).
