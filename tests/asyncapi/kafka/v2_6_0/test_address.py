import pytest

from faststream.kafka import KafkaBroker, KafkaRouter
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


@pytest.mark.kafka()
def test_every_address_is_named_as_declared() -> None:
    broker = KafkaBroker()

    @broker.subscriber(pattern="logs.{level}")
    async def handle_logs(body: str) -> None: ...

    broker.publisher("cache")

    schema = get_2_6_0_schema(broker)

    # `pattern=` is the one Kafka argument that meets the parameter parser. A
    # topic is a literal, and it can hold no brace to escape: Kafka admits ASCII
    # alphanumerics, '.', '_' and '-' in a topic name and nothing else.
    assert {
        name: channel["bindings"]["kafka"]["topic"]
        for name, channel in schema["channels"].items()
    } == {
        "logs.{level}:HandleLogs": "logs.{level}",
        "cache:Publisher": "cache",
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
