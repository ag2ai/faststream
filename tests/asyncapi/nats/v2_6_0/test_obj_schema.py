from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


def test_obj_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", obj_watch=True)
    async def handle() -> None: ...

    schema = get_2_6_0_schema(broker)

    assert schema["channels"] == {}
