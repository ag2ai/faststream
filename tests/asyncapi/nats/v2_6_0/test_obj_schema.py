from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.specification import AsyncAPI


def test_obj_schema() -> None:
    broker = NatsBroker()

    @broker.subscriber("test", obj_watch=True)
    async def handle() -> None: ...

    schema = (
        FastStream(broker, specification=AsyncAPI(schema_version="2.6.0"))
        .schema.to_specification()
        .to_jsonable()
    )

    assert schema["channels"] == {}
