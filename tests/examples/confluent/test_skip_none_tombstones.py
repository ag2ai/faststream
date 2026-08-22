import pytest

from examples.confluent.skip_none_tombstones import (
    app,
    archive_user,
    broker,
    delete_user,
    handle_event,
)
from faststream.confluent import TestApp, TestKafkaBroker


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_example() -> None:
    async with TestKafkaBroker(broker), TestApp(app):
        await handle_event.wait_call(3)

        delete_user.mock.assert_called_once_with("1")
        archive_user.mock.assert_called_once_with("2")

        # only the tombstone reached the topic:
        # the `skip_none` publisher sent nothing
        handle_event.mock.assert_called_once_with(b"")
