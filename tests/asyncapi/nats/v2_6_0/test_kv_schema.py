from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.specification import AsyncAPI


def test_kv_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", kv_watch="test")
    async def handle() -> None: ...

    schema = (
        FastStream(broker, specification=AsyncAPI(schema_version="2.6.0"))
        .schema.to_specification()
        .to_jsonable()
    )

    assert schema["channels"] == {}
