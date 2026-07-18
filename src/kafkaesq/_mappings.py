"""Mapping table between confluent-kafka (librdkafka) and aiokafka configs.

confluent-kafka uses librdkafka-style dotted keys with loosely typed values
(``{"bootstrap.servers": "host:9092", "enable.auto.commit": "false"}``), while
aiokafka takes snake_case constructor kwargs with native Python types
(``bootstrap_servers="host:9092", enable_auto_commit=False``).

Each :class:`Mapping` entry describes one config option that exists on both
sides, plus optional value converters for the cases where the two libraries
spell the *values* differently as well.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in ("true", "1", "yes", "on"):
            return True
        if lowered in ("false", "0", "no", "off"):
            return False
        raise ValueError(f"cannot interpret {value!r} as a boolean")
    return bool(value)


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError(f"cannot interpret {value!r} as an integer")
    return int(value)


def _to_upper(value: Any) -> str:
    return str(value).upper()


def _acks_to_aiokafka(value: Any) -> Any:
    # librdkafka: -1, "all" (and any int); aiokafka: 0, 1, "all".
    if isinstance(value, str) and value.strip().lower() == "all":
        return "all"
    number = int(value)
    return "all" if number == -1 else number


def _acks_to_confluent(value: Any) -> Any:
    return "all" if value == "all" else int(value)


def _compression_to_aiokafka(value: Any) -> Any:
    # librdkafka: "none", "gzip", "snappy", "lz4", "zstd"; aiokafka uses None
    # instead of "none".
    if value is None:
        return None
    lowered = str(value).strip().lower()
    return None if lowered in ("none", "inherit") else lowered


def _compression_to_confluent(value: Any) -> str:
    return "none" if value is None else str(value)


_OFFSET_RESET_ALIASES = {
    # librdkafka accepts several aliases that aiokafka does not.
    "smallest": "earliest",
    "beginning": "earliest",
    "largest": "latest",
    "end": "latest",
    "error": "none",
}


def _offset_reset_to_aiokafka(value: Any) -> str:
    lowered = str(value).strip().lower()
    return _OFFSET_RESET_ALIASES.get(lowered, lowered)


@dataclass(frozen=True)
class Mapping:
    confluent_key: str
    aiokafka_key: str
    to_aiokafka: Callable[[Any], Any] | None = None
    to_confluent: Callable[[Any], Any] | None = None
    confluent_aliases: tuple[str, ...] = field(default=())


MAPPINGS: tuple[Mapping, ...] = (
    # --- common / connection ---
    Mapping("bootstrap.servers", "bootstrap_servers",
            confluent_aliases=("metadata.broker.list",)),
    Mapping("client.id", "client_id"),
    Mapping("request.timeout.ms", "request_timeout_ms", _to_int),
    Mapping("metadata.max.age.ms", "metadata_max_age_ms", _to_int,
            confluent_aliases=("topic.metadata.refresh.interval.ms",)),
    Mapping("connections.max.idle.ms", "connections_max_idle_ms", _to_int),
    Mapping("retry.backoff.ms", "retry_backoff_ms", _to_int),
    # --- security ---
    Mapping("security.protocol", "security_protocol", _to_upper, _to_upper),
    Mapping("sasl.mechanism", "sasl_mechanism", _to_upper, _to_upper,
            confluent_aliases=("sasl.mechanisms",)),
    Mapping("sasl.username", "sasl_plain_username"),
    Mapping("sasl.password", "sasl_plain_password"),
    Mapping("sasl.kerberos.service.name", "sasl_kerberos_service_name"),
    # --- producer ---
    Mapping("acks", "acks", _acks_to_aiokafka, _acks_to_confluent,
            confluent_aliases=("request.required.acks",)),
    Mapping("enable.idempotence", "enable_idempotence", _to_bool),
    Mapping("compression.type", "compression_type",
            _compression_to_aiokafka, _compression_to_confluent,
            confluent_aliases=("compression.codec",)),
    Mapping("linger.ms", "linger_ms", _to_int,
            confluent_aliases=("queue.buffering.max.ms",)),
    Mapping("message.max.bytes", "max_request_size", _to_int),
    Mapping("transactional.id", "transactional_id"),
    Mapping("transaction.timeout.ms", "transaction_timeout_ms", _to_int),
    # --- consumer ---
    Mapping("group.id", "group_id"),
    Mapping("group.instance.id", "group_instance_id"),
    Mapping("auto.offset.reset", "auto_offset_reset",
            _offset_reset_to_aiokafka),
    Mapping("enable.auto.commit", "enable_auto_commit", _to_bool),
    Mapping("auto.commit.interval.ms", "auto_commit_interval_ms", _to_int),
    Mapping("fetch.min.bytes", "fetch_min_bytes", _to_int),
    Mapping("fetch.max.bytes", "fetch_max_bytes", _to_int),
    Mapping("fetch.wait.max.ms", "fetch_max_wait_ms", _to_int),
    Mapping("max.partition.fetch.bytes", "max_partition_fetch_bytes", _to_int),
    Mapping("session.timeout.ms", "session_timeout_ms", _to_int),
    Mapping("heartbeat.interval.ms", "heartbeat_interval_ms", _to_int),
    Mapping("max.poll.interval.ms", "max_poll_interval_ms", _to_int),
    Mapping("isolation.level", "isolation_level",
            to_aiokafka=lambda v: str(v).strip().lower()),
    Mapping("check.crcs", "check_crcs", _to_bool),
)

# librdkafka SSL options have no aiokafka kwarg equivalents; they are folded
# into a single ``ssl_context`` by kafkaesq.convert.
SSL_CONFLUENT_KEYS: tuple[str, ...] = (
    "ssl.ca.location",
    "ssl.certificate.location",
    "ssl.key.location",
    "ssl.key.password",
    "ssl.endpoint.identification.algorithm",
    "enable.ssl.certificate.verification",
)

BY_CONFLUENT_KEY: dict[str, Mapping] = {}
for _m in MAPPINGS:
    BY_CONFLUENT_KEY[_m.confluent_key] = _m
    for _alias in _m.confluent_aliases:
        BY_CONFLUENT_KEY[_alias] = _m

BY_AIOKAFKA_KEY: dict[str, Mapping] = {_m.aiokafka_key: _m for _m in MAPPINGS}
