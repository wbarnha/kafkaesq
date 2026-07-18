"""Convert Kafka client configs between confluent-kafka and aiokafka."""

from __future__ import annotations

import ssl
import warnings
from typing import Any, Mapping as TypingMapping

from ._mappings import (
    BY_AIOKAFKA_KEY,
    BY_CONFLUENT_KEY,
    SSL_CONFLUENT_KEYS,
)
from ._mappings import _to_bool  # noqa: PLC2701 - internal reuse

__all__ = [
    "KafkaesqWarning",
    "UnmappedConfigError",
    "aiokafka_to_confluent",
    "confluent_to_aiokafka",
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
            stacklevel=3,
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


def confluent_to_aiokafka(
    config: TypingMapping[str, Any],
    *,
    on_unmapped: str = "warn",
) -> dict[str, Any]:
    """Convert a confluent-kafka config dict to aiokafka constructor kwargs.

    ``config`` uses librdkafka-style dotted keys, e.g.::

        {"bootstrap.servers": "localhost:9092", "group.id": "billing",
         "enable.auto.commit": "false"}

    Returns a dict of snake_case kwargs suitable for ``AIOKafkaProducer`` /
    ``AIOKafkaConsumer``. librdkafka ``ssl.*`` file options are folded into a
    single ``ssl_context`` entry.

    ``on_unmapped`` controls what happens to keys with no aiokafka equivalent
    (callbacks, librdkafka internals, ...): ``"raise"``, ``"warn"`` (default,
    key is dropped) or ``"ignore"`` (key is dropped silently).
    """
    result: dict[str, Any] = {}
    unmapped: list[str] = []
    ssl_keys_present = False

    for key, value in config.items():
        mapping = BY_CONFLUENT_KEY.get(key)
        if mapping is not None:
            converted = value if mapping.to_aiokafka is None else mapping.to_aiokafka(value)
            result[mapping.aiokafka_key] = converted
        elif key in SSL_CONFLUENT_KEYS:
            ssl_keys_present = True
        else:
            unmapped.append(key)

    if ssl_keys_present:
        result["ssl_context"] = _build_ssl_context(config)

    _handle_unmapped(unmapped, "aiokafka", on_unmapped)
    return result


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
    merged: dict[str, Any] = dict(config or {})
    merged.update(kwargs)

    result: dict[str, Any] = {}
    unmapped: list[str] = []

    for key, value in merged.items():
        mapping = BY_AIOKAFKA_KEY.get(key)
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
