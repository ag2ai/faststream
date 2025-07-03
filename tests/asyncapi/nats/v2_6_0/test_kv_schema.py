from faststream.nats import NatsBroker
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema


def test_kv_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", kv_watch="test")
    async def handle() -> None: ...

    schema = get_2_6_0_schema(broker)

    assert schema["channels"] == {}
