import pytest

from faststream.mqtt import MQTTBroker, MQTTRouter
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


@pytest.mark.mqtt()
def test_every_address_is_named_as_declared() -> None:
    broker = MQTTBroker()

    @broker.subscriber("logs/{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_2_6_0_schema(broker)

    assert {
        name: channel["bindings"]["mqtt"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "logs/{level}:HandleLogs": "logs/{level}",
        "cache{shard}:Publisher": "cache{shard}",
    }


@pytest.mark.mqtt()
def test_a_router_prefix_is_named_as_declared() -> None:
    broker = MQTTBroker()
    router = MQTTRouter(prefix="app{{v1}}/")

    @router.subscriber("logs/{level}")
    async def handle_logs(body: str) -> None: ...

    router.publisher("cache")
    broker.include_router(router)

    schema = get_2_6_0_schema(broker)

    assert {
        name: channel["bindings"]["mqtt"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "app{v1}/logs/{level}:HandleLogs": "app{v1}/logs/{level}",
        "app{v1}/cache:Publisher": "app{v1}/cache",
    }
