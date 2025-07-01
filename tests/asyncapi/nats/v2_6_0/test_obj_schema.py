from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.specification import AsyncAPI
from tests.asyncapi.base.v2_6_0.basic import get_2_6_0_schema


def test_obj_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", obj_watch=True)
    async def handle() -> None: ...

    schema = get_2_6_0_schema(broker)

    assert schema["channels"] == {}
