from unittest.mock import patch

import pytest
from aio_pika import IncomingMessage

from faststream.rabbit import TestApp, TestRabbitBroker
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_ack_exc() -> None:
    from docs.docs_src.rabbit.ack.errors import app, broker, handle

    with patch.object(IncomingMessage, "ack", spy_decorator(IncomingMessage.ack)) as m:
        async with TestRabbitBroker(broker, with_real=True), TestApp(app):
            await handle.wait_call(3)

            m.mock.assert_called_once()
