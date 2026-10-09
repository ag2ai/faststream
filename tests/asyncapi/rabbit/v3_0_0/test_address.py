import pytest

from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
    RabbitRouter,
)
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema

EXCHANGE = RabbitExchange("logs-ex", type=ExchangeType.TOPIC)


@pytest.mark.rabbit()
def test_every_address_is_named_as_declared() -> None:
    broker = RabbitBroker()

    @broker.subscriber(RabbitQueue("logs-q", routing_key="logs.{level}"), EXCHANGE)
    async def handle_logs(body: str) -> None: ...

    broker.publisher(routing_key="cache{{shard}}", exchange=EXCHANGE)

    schema = get_3_0_0_schema(broker)

    assert set(schema["channels"]) == {
        "logs-q:logs-ex:HandleLogs",
        "cache{shard}:logs-ex:Publisher",
    }

    # RabbitMQ addresses by routing key, and the key lives on the operation.
    assert {
        name: operation["bindings"]["amqp"]["cc"]
        for name, operation in schema["operations"].items()
    } == {
        "logs-q:logs-ex:HandleLogsSubscribe": ["logs.{level}"],
        "cache{shard}:logs-ex:Publisher": ["cache{shard}"],
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

    schema = get_3_0_0_schema(broker)
    name = "events{version}.{level}:logs-ex:Publisher"
    assert (
        schema["channels"][name]["address"],
        schema["operations"][name]["bindings"]["amqp"]["cc"],
    ) == (
        "logs.{{tenant}}.events{version}.{level}",
        ["logs.{{tenant}}.events{version}.{level}"],
    )
