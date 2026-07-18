"""Awareness of Java-client-only config keys, for `kafkaesq validate`.

The Apache Kafka Java client shares most of its dotted config namespace with
librdkafka, but a handful of keys exist only in the Java ecosystem (JKS
truststores, JAAS login strings, class-based (de)serializers, ...). When a
Java project's config file is validated, these deserve targeted guidance
rather than a generic "unrecognized key" warning.
"""

from __future__ import annotations

import re
from typing import Any

_JAAS_USERNAME = re.compile(r'username\s*=\s*"([^"]*)"')


def _jaas_guidance(value: Any) -> str:
    guidance = (
        "Java JAAS login string; librdkafka-family and Python clients use "
        "sasl.username / sasl.password instead"
    )
    match = _JAAS_USERNAME.search(str(value))
    if match:
        guidance += f' (found username="{match.group(1)}"; password not shown)'
    return guidance


_SERIALIZER_GUIDANCE = (
    "Java class-based (de)serializer; Python clients take serializer "
    "callables at construction time instead"
)

JAVA_ONLY_KEYS: dict[str, Any] = {
    "ssl.truststore.location": (
        "Java truststore (JKS/PKCS12); librdkafka-family and Python clients "
        "use ssl.ca.location with a PEM file (export with keytool/openssl)"
    ),
    "ssl.truststore.password": (
        "Java truststore password; PEM CA files (ssl.ca.location) need none"
    ),
    "ssl.truststore.type": (
        "Java truststore format; other clients read PEM via ssl.ca.location"
    ),
    "ssl.keystore.location": (
        "Java keystore; use ssl.certificate.location + ssl.key.location "
        "with PEM files"
    ),
    "ssl.keystore.password": "Java keystore password; use ssl.key.password",
    "ssl.keystore.type": (
        "Java keystore format; other clients read PEM certificate/key files"
    ),
    "sasl.jaas.config": _jaas_guidance,
    "sasl.login.callback.handler.class": (
        "Java login callback class; other clients configure OAuth/SASL "
        "callbacks in code"
    ),
    "key.serializer": _SERIALIZER_GUIDANCE,
    "value.serializer": _SERIALIZER_GUIDANCE,
    "key.deserializer": _SERIALIZER_GUIDANCE,
    "value.deserializer": _SERIALIZER_GUIDANCE,
    "max.poll.records": (
        "Java consumer batching; no librdkafka equivalent, but aiokafka and "
        "kafka-python accept max_poll_records at construction"
    ),
    "fetch.max.wait.ms": (
        "Java spelling; librdkafka calls this fetch.wait.max.ms"
    ),
    "buffer.memory": (
        "Java producer buffer pool; librdkafka uses "
        "queue.buffering.max.messages / queue.buffering.max.kbytes"
    ),
    "partition.assignment.strategy": (
        "shared name, different values: Java takes assignor class names, "
        "librdkafka takes range/roundrobin/cooperative-sticky"
    ),
}


def java_only_guidance(key: str, value: Any) -> str | None:
    """Guidance string if ``key`` is a known Java-only config key, else None."""
    entry = JAVA_ONLY_KEYS.get(key)
    if entry is not None:
        return entry(value) if callable(entry) else entry
    if key.endswith(".class"):
        return "Java class-based setting with no equivalent in other clients"
    return None
