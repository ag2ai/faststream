import pytest

from faststream.mqtt import MQTTBroker, MQTTRouter
from tests.asyncapi.base.v3_0_0.basic import get_3_0_0_schema


@pytest.mark.mqtt()
def test_every_address_is_named_as_declared() -> None:
    broker = MQTTBroker()

    @broker.subscriber("logs/{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_3_0_0_schema(broker)

    # The 3.0 channel key spells the topic with `.` where 2.6 keeps MQTT's own
    # `/`; the binding, which is what a client reads, keeps the slash in both.
    assert {
        name: channel["bindings"]["mqtt"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "logs.{level}:HandleLogs": "logs/{level}",
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

    schema = get_3_0_0_schema(broker)

    assert {
        name: channel["bindings"]["mqtt"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "app{v1}.logs.{level}:HandleLogs": "app{v1}/logs/{level}",
        "app{v1}.cache:Publisher": "app{v1}/cache",
    }
