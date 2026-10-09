from typing import Any

import pytest

from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
    RabbitRouter,
)
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema

EXCHANGE = RabbitExchange("logs-ex", type=ExchangeType.TOPIC)


def routing_keys(schema: Any) -> dict[str, str]:
    """RabbitMQ addresses by routing key, and the key lives on the operation."""
    return {
        name: (channel.get("publish") or channel["subscribe"])["bindings"]["amqp"]["cc"]
        for name, channel in schema["channels"].items()
    }


@pytest.mark.rabbit()
def test_every_address_is_named_as_declared() -> None:
    broker = RabbitBroker()

    @broker.subscriber(RabbitQueue("logs-q", routing_key="logs.{level}"), EXCHANGE)
    async def handle_logs(body: str) -> None: ...

    broker.publisher(routing_key="cache{{shard}}", exchange=EXCHANGE)

    schema = get_2_6_0_schema(broker)

    assert routing_keys(schema) == {
        "logs-q:logs-ex:HandleLogs": "logs.{level}",
        "cache{shard}:logs-ex:Publisher": "cache{shard}",
    }


@pytest.mark.rabbit()
def test_publisher_schema_keeps_the_router_prefix_literal() -> None:
    """Fixes https://github.com/ag2ai/faststream/pull/3109."""
    broker = RabbitBroker()
    router = RabbitRouter(prefix="logs.{{tenant}}.")
    router.publisher(
        RabbitQueue("events", routing_key="events{{version}}.{level}"),
        EXCHANGE,
    )
    broker.include_router(router)

    assert routing_keys(get_2_6_0_schema(broker)) == {
        "events{version}.{level}:logs-ex:Publisher": "logs.{{tenant}}.events{version}.{level}",
    }
