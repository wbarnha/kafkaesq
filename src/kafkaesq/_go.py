"""Awareness of confluent-kafka-go binding keys, for `kafkaesq validate`.

confluent-kafka-go configs use librdkafka's dotted keys (the same namespace
as confluent-kafka-python) plus a small set of ``go.``-prefixed options that
configure the Go binding itself — channel-based APIs, delivery reports,
batching. Those are valid for the Go client but have no equivalent anywhere
else, so validation reports them with targeted guidance instead of a generic
"unrecognized key" warning.
"""

from __future__ import annotations

GO_BINDING_KEYS: dict[str, str] = {
    "go.events.channel.enable": (
        "confluent-kafka-go consumer option: read via the .Events() channel "
        "instead of .Poll(); other clients poll/iterate in code"
    ),
    "go.events.channel.size": (
        "confluent-kafka-go .Events() channel buffer size; no equivalent in "
        "other clients"
    ),
    "go.application.rebalance.enable": (
        "confluent-kafka-go consumer option: deliver rebalance events to the "
        "application; other clients use rebalance callbacks/listeners"
    ),
    "go.delivery.reports": (
        "confluent-kafka-go producer option: emit delivery reports on "
        ".Events(); other clients use delivery callbacks or awaitables"
    ),
    "go.delivery.report.fields": (
        "confluent-kafka-go producer option: which fields to include in "
        "delivery reports; no equivalent in other clients"
    ),
    "go.batch.producer": (
        "confluent-kafka-go producer batching mode; other clients batch via "
        "linger.ms/batch settings"
    ),
    "go.produce.channel.size": (
        "confluent-kafka-go .ProduceChannel() buffer size; no equivalent in "
        "other clients"
    ),
    "go.logs.channel.enable": (
        "confluent-kafka-go option: deliver client logs on a Go channel; "
        "other clients use loggers/callbacks"
    ),
}


def go_binding_guidance(key: str) -> str | None:
    """Guidance string if ``key`` is a confluent-kafka-go binding key."""
    guidance = GO_BINDING_KEYS.get(key)
    if guidance is not None:
        return guidance
    if key.startswith("go."):
        return (
            "confluent-kafka-go binding option; configured in Go code only, "
            "no equivalent in other clients"
        )
    return None
