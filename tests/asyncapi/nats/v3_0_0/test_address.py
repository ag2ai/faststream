import pytest

from faststream.nats import NatsBroker, NatsRouter
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


@pytest.mark.nats()
def test_every_address_is_named_as_declared() -> None:
    broker = NatsBroker()

    @broker.subscriber("logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_3_0_0_schema(broker)

    assert {
        name: channel["bindings"]["nats"]["subject"]
        for name, channel in schema["channels"].items()
    } == {
        "logs.{level}:HandleLogs": "logs.{level}",
        "cache{shard}:Publisher": "cache{shard}",
    }


@pytest.mark.nats()
def test_a_router_prefix_is_named_as_declared() -> None:
    broker = NatsBroker()
    router = NatsRouter(prefix="app{{v1}}.")

    @router.subscriber("logs.{level}")
    async def handle_logs(body: str) -> None: ...

    router.publisher("cache")
    broker.include_router(router)

    schema = get_3_0_0_schema(broker)

    assert {
        name: channel["bindings"]["nats"]["subject"]
        for name, channel in schema["channels"].items()
    } == {
        "app{v1}.logs.{level}:HandleLogs": "app{v1}.logs.{level}",
        "app{v1}.cache:Publisher": "app{v1}.cache",
    }
