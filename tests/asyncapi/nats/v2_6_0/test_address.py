import pytest

from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


@pytest.mark.nats()
def test_every_address_is_named_as_declared() -> None:
    broker = NatsBroker()

    @broker.subscriber("logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_2_6_0_schema(broker)

    assert {
        name: channel["bindings"]["nats"]["subject"]
        for name, channel in schema["channels"].items()
    } == {
        "logs.{level}:HandleLogs": "logs.{level}",
        "cache{shard}:Publisher": "cache{shard}",
    }
