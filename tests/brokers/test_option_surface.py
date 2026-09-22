import importlib
import inspect
from collections.abc import Callable
from typing import Any

import pytest

from tests.marks import (
    require_aiokafka,
    require_aiopika,
    require_confluent,
    require_mqtt,
    require_nats,
    require_redis,
)

# an option the broker offers is offered by every level that registers the same
# endpoint (#2871); each name here is a level that lost one, and the list only shrinks
LOST_OPTIONS = {
    "kafka": {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": ["codec", "persistent"],
        "Publisher": ["autoflush", "persistent"],
    },
    "confluent": {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": ["codec", "persistent"],
        "Publisher": ["autoflush", "persistent"],
    },
    "nats": {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": ["codec", "persistent"],
        "Publisher": ["persistent"],
    },
    "rabbit": {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": ["channel", "codec", "persistent"],
        "Publisher": ["persistent"],
    },
    "redis": {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": ["codec", "message_format", "persistent"],
        "Publisher": ["message_format", "persistent"],
    },
    "mqtt": {
        "Router.subscriber": [],
        "Router.publisher": [],
        "Route": ["codec"],
        "Publisher": [],
    },
}

BROKERS = (
    pytest.param("kafka", "Kafka", marks=(pytest.mark.kafka(), require_aiokafka)),
    pytest.param(
        "confluent",
        "Kafka",
        marks=(pytest.mark.confluent(), require_confluent),
    ),
    pytest.param("nats", "Nats", marks=(pytest.mark.nats(), require_nats)),
    pytest.param("rabbit", "Rabbit", marks=(pytest.mark.rabbit(), require_aiopika)),
    pytest.param("redis", "Redis", marks=(pytest.mark.redis(), require_redis)),
    pytest.param("mqtt", "MQTT", marks=(pytest.mark.mqtt(), require_mqtt)),
)


@pytest.mark.parametrize(("package", "prefix"), BROKERS)
def test_every_level_takes_the_options_the_broker_takes(
    package: str,
    prefix: str,
) -> None:
    module = importlib.import_module(f"faststream.{package}")
    broker = getattr(module, f"{prefix}Broker")
    router = getattr(module, f"{prefix}Router")

    # a router registers through the broker's own registrator, so its two entries
    # stay empty until someone gives the router a signature of its own
    assert {
        "Router.subscriber": _lost(broker.subscriber, router.subscriber),
        "Router.publisher": _lost(broker.publisher, router.publisher),
        "Route": _lost(broker.subscriber, getattr(module, f"{prefix}Route")),
        "Publisher": _lost(broker.publisher, getattr(module, f"{prefix}Publisher")),
    } == LOST_OPTIONS[package]


def _lost(offered: Callable[..., Any], accepted: Callable[..., Any]) -> list[str]:
    return sorted(_options(offered) - _options(accepted))


def _options(obj: Callable[..., Any]) -> set[str]:
    # `*args` carries the destination, which each level spells its own way
    return {
        parameter.name
        for parameter in inspect.signature(obj).parameters.values()
        if parameter.kind in {parameter.POSITIONAL_OR_KEYWORD, parameter.KEYWORD_ONLY}
        and parameter.name != "self"
    }
