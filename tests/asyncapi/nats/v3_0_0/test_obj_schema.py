import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.nats import NatsBroker
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema


@pytest.mark.nats()
def test_obj_schema(snapshot_json: SnapshotAssertion) -> None:
    broker = NatsBroker()

    @broker.subscriber("test", obj_watch=True)
    async def handle() -> None: ...

    schema = get_3_0_0_schema(broker)

    assert schema == snapshot_json
