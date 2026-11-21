import pytest

from faststream.nats import TestApp, TestNatsBroker


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_atomic_batch() -> None:
    from docs.docs_src.nats.js.atomic_batch import app, broker, handle

    async with TestNatsBroker(broker), TestApp(app):
        assert [call.args for call in handle.mock.call_args_list] == [
            ({"id": 1},),
            ({"id": 2},),
            ({"id": 3},),
        ]
