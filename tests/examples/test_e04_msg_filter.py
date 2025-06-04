import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_example() -> None:
    from examples.e04_msg_filter import app, broker, handle_json, handle_other_messages
    from faststream.rabbit import TestApp, TestRabbitBroker

    async with TestRabbitBroker(broker), TestApp(app):
        await handle_json.wait_call(3)
        await handle_other_messages.wait_call(3)

        handle_json.mock.assert_called_with({"msg": "Hello!"})
        handle_other_messages.mock.assert_called_with("Hello!")
