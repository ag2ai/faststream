from typing import Any

import pytest

from faststream import Config, FastStream
from faststream.exceptions import SetupError
from faststream.kafka import KafkaBroker, KafkaRouter
from faststream.redis import RedisBroker

# Kafka is the reference broker for these rules; Redis appears once, to show that
# two brokers under one App keep their own values.
pytestmark = [pytest.mark.kafka(), pytest.mark.redis()]


class Settings:
    """A settings-style object: values are attributes, not items."""

    IN_TOPIC = "orders"
    GROUP = "workers"


def test_broker_level_value() -> None:
    broker = KafkaBroker(config_values={"IN_TOPIC": "orders"})

    assert broker.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_app_level_value() -> None:
    broker = KafkaBroker()
    FastStream(broker, config_values={"IN_TOPIC": "orders"})

    assert broker.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_broker_level_wins_over_app_level() -> None:
    broker = KafkaBroker(config_values={"IN_TOPIC": "broker-wins"})
    FastStream(broker, config_values={"IN_TOPIC": "app-loses"})

    assert broker.config.resolve_option(Config("IN_TOPIC")) == "broker-wins"


def test_app_level_fills_the_keys_the_broker_misses() -> None:
    broker = KafkaBroker(config_values={"IN_TOPIC": "broker-wins"})
    FastStream(broker, config_values={"IN_TOPIC": "app-loses", "GROUP": "workers"})

    assert broker.config.resolve_option(Config("GROUP")) == "workers"


def test_each_broker_resolves_the_same_key_to_its_own_value() -> None:
    kafka = KafkaBroker(config_values={"TOPIC": "kafka-orders"})
    redis = RedisBroker(config_values={"TOPIC": "redis-orders"})
    FastStream(kafka, redis, config_values={"TOPIC": "app-loses"})

    assert kafka.config.resolve_option(Config("TOPIC")) == "kafka-orders"
    assert redis.config.resolve_option(Config("TOPIC")) == "redis-orders"


def test_broker_without_an_app_resolves_against_its_own_values_only() -> None:
    broker = KafkaBroker(config_values={"IN_TOPIC": "orders"})
    FastStream(KafkaBroker(), config_values={"GROUP": "workers"})

    assert broker.config.resolve_option(Config("IN_TOPIC")) == "orders"

    with pytest.raises(SetupError, match="GROUP"):
        broker.config.resolve_option(Config("GROUP"))


def test_object_source_is_read_by_attribute() -> None:
    """This is what makes pydantic-settings work with no adapter."""
    broker = KafkaBroker(config_values=Settings())

    assert broker.config.resolve_option(Config("IN_TOPIC")) == "orders"
    assert broker.config.resolve_option(Config("GROUP")) == "workers"


def test_mapping_source_does_not_fall_back_to_its_own_attributes() -> None:
    """`Config("items")` is a missing key, not `dict.items`."""
    broker = KafkaBroker(config_values={"IN_TOPIC": "orders"})

    with pytest.raises(SetupError, match="items"):
        broker.config.resolve_option(Config("items"))


def test_object_source_missing_attribute_raises() -> None:
    broker = KafkaBroker(config_values=Settings())

    with pytest.raises(SetupError, match="OUT_TOPIC"):
        broker.config.resolve_option(Config("OUT_TOPIC"))


def test_value_may_be_a_prepared_object() -> None:
    """Resolution happens before validation, so any object arrives intact."""
    prepared = object()
    broker = KafkaBroker(config_values={"IN_TOPIC": prepared})

    assert broker.config.resolve_option(Config("IN_TOPIC")) is prepared


def test_default_is_used_when_the_key_is_absent() -> None:
    broker = KafkaBroker(config_values={})

    assert broker.config.resolve_option(Config("IN_TOPIC", default="orders")) == "orders"


def test_none_is_usable_as_a_default() -> None:
    broker = KafkaBroker(config_values={})

    assert broker.config.resolve_option(Config("STREAM", default=None)) is None


def test_a_supplied_value_wins_over_the_default() -> None:
    broker = KafkaBroker(config_values={"IN_TOPIC": "orders"})

    assert (
        broker.config.resolve_option(Config("IN_TOPIC", default="fallback")) == "orders"
    )


def test_missing_key_raises_an_error_naming_the_key() -> None:
    broker = KafkaBroker()

    with pytest.raises(SetupError, match="IN_TOPIC"):
        broker.config.resolve_option(Config("IN_TOPIC"))


@pytest.mark.parametrize("option", [pytest.param(x) for x in ("orders", None, 42, False)])
def test_a_plain_option_passes_through_unchanged(option: Any) -> None:
    broker = KafkaBroker(config_values={"IN_TOPIC": "orders"})

    assert broker.config.resolve_option(option) is option


def test_router_included_into_a_broker_reaches_broker_values() -> None:
    router = KafkaRouter()

    broker = KafkaBroker(config_values={"IN_TOPIC": "orders"})
    broker.include_router(router)

    assert router.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_router_passed_to_the_constructor_reaches_broker_values() -> None:
    router = KafkaRouter()
    KafkaBroker(routers=(router,), config_values={"IN_TOPIC": "orders"})

    assert router.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_nested_routers_reach_broker_values() -> None:
    inner = KafkaRouter(prefix="inner_")
    outer = KafkaRouter(prefix="outer_", routers=(inner,))
    KafkaBroker(routers=(outer,), config_values={"IN_TOPIC": "orders"})

    assert inner.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_router_reaches_app_level_values() -> None:
    router = KafkaRouter(prefix="test_")
    broker = KafkaBroker(routers=(router,))
    FastStream(broker, config_values={"IN_TOPIC": "orders"})

    assert router.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_broker_added_to_the_app_after_its_routers_were_included() -> None:
    router = KafkaRouter()
    broker = KafkaBroker()
    broker.include_router(router)

    app = FastStream(config_values={"IN_TOPIC": "orders"})
    app.add_broker(broker)

    assert router.config.resolve_option(Config("IN_TOPIC")) == "orders"


def test_router_is_not_a_config_level() -> None:
    """Two levels only — Broker and App.

    The runtime half; the type-level half is
    `tests/mypy/kafka.py::check_config_values_is_not_a_router_parameter`, which
    is where an ignore is actually verified — this file is outside mypy's
    `files`, so one written here would claim a check nothing performs.
    """
    with pytest.raises(TypeError):
        KafkaRouter(config_values={"IN_TOPIC": "orders"})
