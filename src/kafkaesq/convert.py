"""Convert Kafka client configs between confluent-kafka, aiokafka and kafka-python."""

from __future__ import annotations

import ssl
import warnings
from typing import Any, Mapping as TypingMapping

from ._faust import BY_CONFLUENT_KEY_FOR_FAUST, BY_FAUST_KEY
from ._mappings import (
    BY_CONFLUENT_KEY,
    BY_CONFLUENT_SSL_KEY,
    BY_SNAKE_KEY,
    BY_SNAKE_SSL_KEY,
    SSL_CONFLUENT_KEYS,
)
from ._mappings import _to_bool  # noqa: PLC2701 - internal reuse

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

_ON_UNMAPPED_CHOICES = ("raise", "warn", "ignore")


class KafkaesqWarning(UserWarning):
    """Warning emitted for config keys that cannot be converted."""


class UnmappedConfigError(KeyError):
    """Raised when a config key cannot be converted and ``on_unmapped="raise"``."""

    def __init__(self, keys: list[str], target: str) -> None:
        self.keys = keys
        self.target = target
        super().__init__(
            f"cannot convert config key(s) {keys!r} to {target}; "
            'pass on_unmapped="warn" or "ignore" to skip them'
        )

    def __str__(self) -> str:
        return self.args[0]


def _handle_unmapped(keys: list[str], target: str, on_unmapped: str) -> None:
    if on_unmapped not in _ON_UNMAPPED_CHOICES:
        raise ValueError(
            f"on_unmapped must be one of {_ON_UNMAPPED_CHOICES}, got {on_unmapped!r}"
        )
    if not keys:
        return
    if on_unmapped == "raise":
        raise UnmappedConfigError(keys, target)
    if on_unmapped == "warn":
        warnings.warn(
            f"dropping config key(s) with no {target} equivalent: {keys!r}",
            KafkaesqWarning,
            stacklevel=4,
        )


def _build_ssl_context(config: TypingMapping[str, Any]) -> ssl.SSLContext:
    """Fold librdkafka ``ssl.*`` options into a Python :class:`ssl.SSLContext`.

    aiokafka has no per-file SSL kwargs; it takes a ready-made ``ssl_context``.
    """
    cafile = config.get("ssl.ca.location")
    context = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH, cafile=cafile
    )
    certfile = config.get("ssl.certificate.location")
    if certfile is not None:
        context.load_cert_chain(
            certfile,
            keyfile=config.get("ssl.key.location"),
            password=config.get("ssl.key.password"),
        )
    endpoint_algo = config.get("ssl.endpoint.identification.algorithm")
    if endpoint_algo is not None and str(endpoint_algo).lower() == "none":
        context.check_hostname = False
    verify = config.get("enable.ssl.certificate.verification")
    if verify is not None and not _to_bool(verify):
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def _confluent_to_snake(
    config: TypingMapping[str, Any],
    *,
    target: str,
    ssl_as_files: bool,
    on_unmapped: str,
    build_ssl_context: bool = True,
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    unmapped: list[str] = []
    ssl_keys_present = False

    for key, value in config.items():
        mapping = BY_CONFLUENT_KEY.get(key)
        if mapping is None and ssl_as_files:
            mapping = BY_CONFLUENT_SSL_KEY.get(key)
        if mapping is not None:
            converted = value if mapping.to_snake is None else mapping.to_snake(value)
            result[mapping.snake_key] = converted
        elif not ssl_as_files and key in SSL_CONFLUENT_KEYS:
            ssl_keys_present = True
        else:
            unmapped.append(key)

    if not ssl_as_files and build_ssl_context:
        if ssl_keys_present:
            result["ssl_context"] = _build_ssl_context(config)
        elif str(result.get("security_protocol", "")).upper() in ("SSL", "SASL_SSL"):
            # aiokafka refuses SSL/SASL_SSL without an ssl_context, while
            # librdkafka falls back to the system CA store (e.g. a Confluent
            # Cloud config has no ssl.* keys at all) — mirror that fallback.
            result["ssl_context"] = ssl.create_default_context()

    _handle_unmapped(unmapped, target, on_unmapped)
    return result


def _snake_to_confluent(
    config: TypingMapping[str, Any] | None,
    kwargs: dict[str, Any],
    *,
    ssl_as_files: bool,
    on_unmapped: str,
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(config or {})
    merged.update(kwargs)

    result: dict[str, Any] = {}
    unmapped: list[str] = []

    for key, value in merged.items():
        mapping = BY_SNAKE_KEY.get(key)
        if mapping is None and ssl_as_files:
            mapping = BY_SNAKE_SSL_KEY.get(key)
        if mapping is None:
            unmapped.append(key)
            continue
        if key == "bootstrap_servers" and isinstance(value, (list, tuple)):
            value = ",".join(value)
        elif mapping.to_confluent is not None:
            value = mapping.to_confluent(value)
        result[mapping.confluent_key] = value

    _handle_unmapped(unmapped, "confluent-kafka", on_unmapped)
    return result


def confluent_to_aiokafka(
    config: TypingMapping[str, Any],
    *,
    on_unmapped: str = "warn",
    build_ssl_context: bool = True,
) -> dict[str, Any]:
    """Convert a confluent-kafka config dict to aiokafka constructor kwargs.

    ``config`` uses librdkafka-style dotted keys, e.g.::

        {"bootstrap.servers": "localhost:9092", "group.id": "billing",
         "enable.auto.commit": "false"}

    Returns a dict of snake_case kwargs suitable for ``AIOKafkaProducer`` /
    ``AIOKafkaConsumer``. librdkafka ``ssl.*`` file options are folded into a
    single ``ssl_context`` entry. If ``security.protocol`` is SSL-based but no
    ``ssl.*`` options are set (librdkafka would use the system CA store, e.g.
    a Confluent Cloud config), a default ``ssl_context`` is supplied, since
    aiokafka requires one for SSL protocols.

    ``on_unmapped`` controls what happens to keys with no aiokafka equivalent
    (callbacks, librdkafka internals, ...): ``"raise"``, ``"warn"`` (default,
    key is dropped) or ``"ignore"`` (key is dropped silently).

    ``build_ssl_context=False`` skips creating the ``ssl_context`` entirely
    (``ssl.*`` keys are consumed silently). Useful when the result must be
    serializable or the SSL files are not present on this machine.
    """
    return _confluent_to_snake(
        config,
        target="aiokafka",
        ssl_as_files=False,
        on_unmapped=on_unmapped,
        build_ssl_context=build_ssl_context,
    )


def confluent_to_kafka_python(
    config: TypingMapping[str, Any],
    *,
    on_unmapped: str = "warn",
) -> dict[str, Any]:
    """Convert a confluent-kafka config dict to kafka-python constructor kwargs.

    Returns a dict of snake_case kwargs suitable for ``KafkaProducer`` /
    ``KafkaConsumer``. Unlike aiokafka, kafka-python accepts SSL file paths
    directly, so librdkafka ``ssl.*`` options map to ``ssl_cafile``,
    ``ssl_certfile``, ``ssl_keyfile``, ``ssl_password``, ``ssl_crlfile``,
    ``ssl_ciphers`` and ``ssl_check_hostname`` instead of an ``ssl_context``.

    ``on_unmapped`` behaves as in :func:`confluent_to_aiokafka`.
    """
    return _confluent_to_snake(
        config, target="kafka-python", ssl_as_files=True, on_unmapped=on_unmapped
    )


def aiokafka_to_confluent(
    config: TypingMapping[str, Any] | None = None,
    *,
    on_unmapped: str = "warn",
    **kwargs: Any,
) -> dict[str, Any]:
    """Convert aiokafka constructor kwargs to a confluent-kafka config dict.

    Accepts either a dict of kwargs, keyword arguments, or both::

        aiokafka_to_confluent(bootstrap_servers="localhost:9092", group_id="g")
        aiokafka_to_confluent({"bootstrap_servers": "localhost:9092"})

    Returns a dict with librdkafka-style dotted keys suitable for
    ``confluent_kafka.Producer`` / ``confluent_kafka.Consumer``. A
    ``bootstrap_servers`` list is joined into librdkafka's comma-separated
    string form. ``ssl_context`` cannot be decomposed back into file paths and
    is treated as unmapped.

    ``on_unmapped`` behaves as in :func:`confluent_to_aiokafka`.
    """
    return _snake_to_confluent(
        config, kwargs, ssl_as_files=False, on_unmapped=on_unmapped
    )


def faust_to_confluent(
    config: TypingMapping[str, Any],
    *,
    on_unmapped: str = "warn",
) -> dict[str, Any]:
    """Convert Faust app settings to a confluent-kafka config dict.

    ``config`` uses Faust (faust-streaming) app-setting names, e.g.::

        {"id": "billing", "broker": "kafka://localhost:9092",
         "broker_session_timeout": 45}

    The Faust app ``id`` maps to ``group.id``, ``kafka://`` broker URLs are
    flattened to a ``bootstrap.servers`` host list, and second-based
    timeouts/intervals are converted to milliseconds.

    ``on_unmapped`` behaves as in :func:`confluent_to_aiokafka`.
    """
    result: dict[str, Any] = {}
    unmapped: list[str] = []

    for key, value in config.items():
        entry = BY_FAUST_KEY.get(key)
        if entry is None:
            unmapped.append(key)
            continue
        mapping, converter = entry
        result[mapping.confluent_key] = value if converter is None else converter(value)

    _handle_unmapped(unmapped, "confluent-kafka", on_unmapped)
    return result


def confluent_to_faust(
    config: TypingMapping[str, Any],
    *,
    on_unmapped: str = "warn",
) -> dict[str, Any]:
    """Convert a confluent-kafka config dict to Faust app settings.

    ``group.id`` becomes the Faust app ``id``, ``bootstrap.servers`` becomes
    a ``;``-separated list of ``kafka://`` broker URLs, and millisecond
    timeouts/intervals are converted to Faust's second-based settings.

    Authentication cannot be expressed in Faust settings — Faust takes a
    runtime ``broker_credentials`` object — so ``security.protocol``,
    ``sasl.*`` and ``ssl.*`` keys are reported as unmapped.

    ``on_unmapped`` behaves as in :func:`confluent_to_aiokafka`.
    """
    result: dict[str, Any] = {}
    unmapped: list[str] = []

    for key, value in config.items():
        canonical = BY_CONFLUENT_KEY.get(key)
        mapping = BY_CONFLUENT_KEY_FOR_FAUST.get(
            canonical.confluent_key if canonical is not None else key
        )
        if mapping is None:
            unmapped.append(key)
            continue
        result[mapping.faust_key] = (
            value if mapping.to_faust is None else mapping.to_faust(value)
        )

    _handle_unmapped(unmapped, "faust", on_unmapped)
    return result


def kafka_python_to_confluent(
    config: TypingMapping[str, Any] | None = None,
    *,
    on_unmapped: str = "warn",
    **kwargs: Any,
) -> dict[str, Any]:
    """Convert kafka-python constructor kwargs to a confluent-kafka config dict.

    Accepts either a dict of kwargs, keyword arguments, or both, like
    :func:`aiokafka_to_confluent`. kafka-python's SSL file kwargs
    (``ssl_cafile``, ``ssl_certfile``, ...) map back to their librdkafka
    ``ssl.*`` counterparts; ``ssl_context`` cannot be decomposed into file
    paths and is treated as unmapped.

    ``on_unmapped`` behaves as in :func:`confluent_to_aiokafka`.
    """
    return _snake_to_confluent(
        config, kwargs, ssl_as_files=True, on_unmapped=on_unmapped
    )
