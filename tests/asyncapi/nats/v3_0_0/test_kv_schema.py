from faststream.nats import NatsBroker
from faststream.specification.asyncapi import AsyncAPI


def test_kv_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", kv_watch="test")
    async def handle() -> None: ...

    schema = AsyncAPI(broker, schema_version="3.0.0").to_jsonable()

    assert schema["channels"] == {}
