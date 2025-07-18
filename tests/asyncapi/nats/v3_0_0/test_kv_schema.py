import pytest

from faststream.nats import NatsBroker
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.nats()
def test_kv_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", kv_watch="test")
    async def handle() -> None: ...

    schema = get_3_0_0_schema(broker)

    assert schema["channels"] == {}
