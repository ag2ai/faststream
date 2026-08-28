import pytest

from faststream.kafka import KafkaBroker
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


@pytest.mark.kafka()
def test_every_address_is_named_as_declared() -> None:
    broker = KafkaBroker()

    @broker.subscriber(pattern="logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_3_0_0_schema(broker)

    # A Kafka topic is never compiled, so `{{` is not syntax there and nothing
    # takes it off. Only `pattern=` meets the parameter parser at all.
    assert {
        name: channel["bindings"]["kafka"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "logs.{level}:HandleLogs": "logs.{level}",
        "cache{{shard}}:Publisher": "cache{{shard}}",
    }
