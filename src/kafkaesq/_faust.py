"""Mapping between Faust app settings and confluent-kafka (librdkafka) configs.

Faust (faust-streaming) configures its Kafka clients through app settings:
``broker`` is a ``kafka://`` URL (or ``;``-separated list of them), the app
``id`` doubles as the consumer group id, client options are prefixed
``broker_``/``consumer_``/``producer_``, and timeouts/intervals are expressed
in **seconds**, not milliseconds.

Authentication is configured with the ``broker_credentials`` app setting,
which is a runtime object (``faust.SASLCredentials`` / an ``ssl.SSLContext``)
and therefore cannot be represented in a config file; security-related keys
are reported as unmapped when converting to Faust.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from ._mappings import _to_bool, _to_int


def _seconds_to_ms(value: Any) -> int:
    return int(round(float(value) * 1000))


def _ms_to_seconds(value: Any) -> Any:
    seconds = _to_int(value) / 1000
    return int(seconds) if seconds.is_integer() else seconds


def _broker_to_servers(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        urls = [str(url) for url in value]
    else:
        # Faust separates multiple broker URLs with ";".
        urls = str(value).replace(";", ",").split(",")
    hosts = []
    for url in urls:
        url = url.strip()
        if "://" in url:
            url = url.split("://", 1)[1]
        if url:
            hosts.append(url)
    return ",".join(hosts)


def _servers_to_broker(value: Any) -> str:
    if isinstance(value, (list, tuple)):
        hosts = [str(host) for host in value]
    else:
        hosts = str(value).split(",")
    return ";".join(f"kafka://{host.strip()}" for host in hosts if host.strip())


def _acks_to_confluent(value: Any) -> Any:
    # Faust uses -1 for "all acks".
    if isinstance(value, str) and value.strip().lower() == "all":
        return "all"
    number = int(value)
    return "all" if number == -1 else number


def _acks_to_faust(value: Any) -> int:
    if isinstance(value, str) and value.strip().lower() == "all":
        return -1
    return int(value)


def _compression_to_confluent(value: Any) -> str:
    return "none" if value is None else str(value)


def _compression_to_faust(value: Any) -> Any:
    lowered = str(value).strip().lower()
    return None if value is None or lowered == "none" else lowered


def _linger_seconds_to_ms(value: Any) -> int:
    return int(round(float(value) * 1000))


@dataclass(frozen=True)
class FaustMapping:
    faust_key: str
    confluent_key: str
    to_confluent: Callable[[Any], Any] | None = None
    to_faust: Callable[[Any], Any] | None = None
    # Extra Faust spellings accepted on input; the primary key is used for
    # output. ``(alias, converter)`` pairs; converter may be None.
    faust_aliases: tuple[tuple[str, Callable[[Any], Any] | None], ...] = field(
        default=()
    )


FAUST_MAPPINGS: tuple[FaustMapping, ...] = (
    # The Faust app id doubles as the Kafka consumer group id.
    FaustMapping("id", "group.id"),
    FaustMapping("broker", "bootstrap.servers",
                 _broker_to_servers, _servers_to_broker),
    FaustMapping("broker_client_id", "client.id"),
    FaustMapping("broker_request_timeout", "request.timeout.ms",
                 _seconds_to_ms, _ms_to_seconds,
                 faust_aliases=(("producer_request_timeout", _seconds_to_ms),)),
    FaustMapping("broker_session_timeout", "session.timeout.ms",
                 _seconds_to_ms, _ms_to_seconds),
    FaustMapping("broker_heartbeat_interval", "heartbeat.interval.ms",
                 _seconds_to_ms, _ms_to_seconds),
    FaustMapping("broker_max_poll_interval", "max.poll.interval.ms",
                 _seconds_to_ms, _ms_to_seconds),
    FaustMapping("broker_commit_interval", "auto.commit.interval.ms",
                 _seconds_to_ms, _ms_to_seconds),
    FaustMapping("broker_check_crcs", "check.crcs", _to_bool),
    FaustMapping("consumer_auto_offset_reset", "auto.offset.reset"),
    FaustMapping("consumer_group_instance_id", "group.instance.id"),
    FaustMapping("consumer_max_fetch_size", "max.partition.fetch.bytes",
                 _to_int),
    FaustMapping("producer_acks", "acks", _acks_to_confluent, _acks_to_faust),
    FaustMapping("producer_compression_type", "compression.type",
                 _compression_to_confluent, _compression_to_faust),
    # Faust's producer_linger is in seconds; the deprecated producer_linger_ms
    # spelling is accepted on input.
    FaustMapping("producer_linger", "linger.ms",
                 _linger_seconds_to_ms, _ms_to_seconds,
                 faust_aliases=(("producer_linger_ms", _to_int),)),
    FaustMapping("producer_max_request_size", "message.max.bytes", _to_int),
)

BY_FAUST_KEY: dict[str, tuple[FaustMapping, Callable[[Any], Any] | None]] = {}
for _m in FAUST_MAPPINGS:
    BY_FAUST_KEY[_m.faust_key] = (_m, _m.to_confluent)
    for _alias, _converter in _m.faust_aliases:
        BY_FAUST_KEY[_alias] = (_m, _converter)

BY_CONFLUENT_KEY_FOR_FAUST: dict[str, FaustMapping] = {
    _m.confluent_key: _m for _m in FAUST_MAPPINGS
}


def looks_like_faust(config: dict[str, Any]) -> bool:
    return any(
        key == "broker" or key.startswith(("broker_", "consumer_", "producer_"))
        for key in config
    )
