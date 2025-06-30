import pytest

from faststream import TestApp
from faststream.nats import TestNatsBroker


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_basic() -> None:
    from examples.nats.e07_object_storage import app, broker, handler

    async with TestNatsBroker(broker, with_real=True):
        await broker.start()

        os = await broker.object_storage("example-bucket")
        try:
            existed_files = await os.list()
        except Exception:
            existed_files = ()

        call = True
        for file in existed_files:
            if file.name == "file.txt":
                call = False

        if call:
            async with TestApp(app):
                pass

        await handler.wait_call(3.0)
        handler.mock.assert_called_once_with("file.txt")
