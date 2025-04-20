from unittest.mock import patch

import pytest

from faststream.confluent import TestApp, TestKafkaBroker
from faststream.confluent.client import AsyncConfluentConsumer
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.confluent()
@pytest.mark.slow()
async def test_ack_exc() -> None:
    from docs.docs_src.confluent.ack.errors import app, broker, handle

    with patch.object(
        AsyncConfluentConsumer,
        "commit",
        spy_decorator(AsyncConfluentConsumer.commit),
    ) as m:
        async with TestKafkaBroker(broker, with_real=True), TestApp(app):
            await handle.wait_call(20)

            assert m.mock.call_count
