import pytest

from faststream.kafka import KafkaBroker, KafkaRouter
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


@pytest.mark.kafka()
def test_every_address_is_named_as_declared() -> None:
    broker = KafkaBroker()

    @broker.subscriber(pattern="logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache{{shard}}")

    schema = get_2_6_0_schema(broker)

    # A Kafka topic is never compiled, so `{{` is not syntax there and nothing
    # takes it off. Only `pattern=` meets the parameter parser at all.
    assert {
        name: channel["bindings"]["kafka"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "logs.{level}:HandleLogs": "logs.{level}",
        "cache{{shard}}:Publisher": "cache{{shard}}",
    }


@pytest.mark.kafka()
def test_a_router_prefix_is_named_as_declared() -> None:
    broker = KafkaBroker()
    router = KafkaRouter(prefix="app{{v1}}.")

    @router.subscriber(pattern="logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.include_router(router)

    schema = get_2_6_0_schema(broker)

    # `pattern=` is the one Kafka argument that meets the parser, so it is the
    # one a prefix's escape comes off.
    assert {
        name: channel["bindings"]["kafka"]["topic"]
        for name, channel in schema["channels"].items()
    } == {"app{v1}.logs.{level}:HandleLogs": "app{v1}.logs.{level}"}
