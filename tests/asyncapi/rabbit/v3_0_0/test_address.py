import pytest

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue
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
