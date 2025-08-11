from unittest.mock import patch

import pytest
from aiokafka import AIOKafkaConsumer

from faststream.kafka import TestApp, TestKafkaBroker
from tests.tools import spy_decorator


@pytest.mark.asyncio()
@pytest.mark.kafka()
@pytest.mark.connected()
@pytest.mark.slow()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
async def test_ack_exc() -> None:
    from docs.docs_src.kafka.ack.errors import app, broker, handle

    with patch.object(
        AIOKafkaConsumer,
        "commit",
        spy_decorator(AIOKafkaConsumer.commit),
    ) as m:
        async with TestKafkaBroker(broker, with_real=True), TestApp(app):
            await handle.wait_call(10)

            assert m.mock.call_count
