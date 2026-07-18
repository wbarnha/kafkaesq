# kafkaesq examples

Working config files for every supported library, plus Python scripts showing
the API end to end. All commands below are run from this directory and need
only `pip install kafkaesq[yaml]` (the Python scripts note their own extras).

## Config files

| File | What it is |
|---|---|
| `configs/local-consumer.properties` | Plain local consumer config (librdkafka dotted keys) |
| `configs/confluent-cloud.properties` | Confluent Cloud SASL_SSL template |
| `configs/java-consumer.properties` | Config written for the *Java* client, including Java-only keys |
| `configs/go-consumer.json` | confluent-kafka-go config, including `go.*` binding keys |
| `configs/faust-app.yaml` | Faust app settings (seconds-based timeouts, `kafka://` broker URL) |
| `configs/aiokafka-kwargs.json` | aiokafka constructor kwargs |
| `configs/kafka-python-ssl.json` | kafka-python kwargs with SSL file options |

## Converting

```console
# properties -> aiokafka kwargs (JSON on stdout)
$ kafkaesq configs/local-consumer.properties --to aiokafka

# Confluent Cloud template -> kafka-python kwargs as YAML
$ kafkaesq configs/confluent-cloud.properties --to kafka-python --format yaml

# aiokafka kwargs -> librdkafka .properties file
$ kafkaesq configs/aiokafka-kwargs.json --to confluent --format properties -o client.properties

# kafka-python SSL kwargs -> confluent (ssl_cafile -> ssl.ca.location, ...)
$ kafkaesq configs/kafka-python-ssl.json --to confluent

# Faust settings -> confluent properties (seconds -> ms, kafka:// URL -> host list)
$ kafkaesq configs/faust-app.yaml --to confluent --format properties

# ...and back: confluent -> Faust YAML
$ kafkaesq configs/local-consumer.properties --to faust --format yaml

# stdin/stdout piping works everywhere
$ cat configs/local-consumer.properties | kafkaesq --to kafka-python
```

Converting an SSL config to aiokafka prints a note instead of an
`ssl_context` (a runtime object that can't live in a config file):

```console
$ kafkaesq configs/confluent-cloud.properties --to aiokafka
note: aiokafka needs an ssl_context, ... aiokafka.helpers.create_ssl_context()
{ ... }
```

## Validating

`validate` checks keys and values without converting, and reports per-library
portability. Try it on the non-Python configs:

```console
# Java config: shared keys pass; Java-only keys get targeted guidance
$ kafkaesq validate configs/java-consumer.properties

# Go config: librdkafka keys pass; go.* binding keys are explained
$ kafkaesq validate configs/go-consumer.json

# strict mode fails (exit 1) on anything that is not fully portable
$ kafkaesq validate configs/java-consumer.properties --strict

# machine-readable report for CI
$ kafkaesq validate configs/confluent-cloud.properties --format json
```

## Python scripts

- `python/one_config_all_clients.py` — reads a `.properties` file and prints
  the equivalent aiokafka kwargs, kafka-python kwargs, and Faust settings.
  Needs only kafkaesq.

  ```console
  $ python python/one_config_all_clients.py configs/local-consumer.properties
  ```

- `python/aiokafka_consumer_from_properties.py` — the end-to-end pattern:
  parse the ops-provided config file, convert, and construct a real
  `AIOKafkaConsumer` (add `--run` to actually consume; needs a broker).
  Requires `pip install aiokafka`.

  ```console
  $ python python/aiokafka_consumer_from_properties.py configs/local-consumer.properties
  ```
