from unittest.mock import patch

import pytest

from faststream import TestApp
from faststream.mq import TestMQBroker
from faststream.mq.message import MQMessage
from tests.tools import spy_decorator


@pytest.mark.mq()
@pytest.mark.asyncio()
async def test_manual_ack() -> None:
    from examples.mq.manual_ack import app, broker

    with patch.object(MQMessage, "ack", spy_decorator(MQMessage.ack)) as ack:
        async with TestMQBroker(broker), TestApp(app):
            ack.mock.assert_called_once()
