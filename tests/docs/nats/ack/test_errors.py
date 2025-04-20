from unittest.mock import patch

import pytest
from nats.aio.msg import Msg

from faststream.nats import TestApp, TestNatsBroker
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_ack_exc() -> None:
    from docs.docs_src.nats.ack.errors import app, broker, handle

    with patch.object(Msg, "ack", spy_decorator(Msg.ack)) as m:
        async with TestNatsBroker(broker, with_real=True), TestApp(app):
            await handle.wait_call(3)

            assert m.mock.call_count
