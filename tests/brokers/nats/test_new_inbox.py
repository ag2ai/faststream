from unittest.mock import patch

import pytest
from nats.aio.client import Client as NatsClient

from faststream.nats import NatsBroker
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.nats()
async def test_new_inbox() -> None:
    with patch.object(
        NatsClient,
        "new_inbox",
        spy_decorator(NatsClient.new_inbox),
    ) as m:
        broker = NatsBroker(inbox_prefix="_FOO_TEST_INBOX")
        await broker.connect()
        inbox_name = await broker.new_inbox()

        m.mock.assert_called_once()
        assert inbox_name.startswith("_FOO_TEST_INBOX.")
