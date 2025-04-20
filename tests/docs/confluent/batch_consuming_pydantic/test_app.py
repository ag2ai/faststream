import pytest

from docs.docs_src.confluent.batch_consuming_pydantic.app import (
    HelloWorld,
    broker,
    handle_batch,
)
from faststream.confluent import TestKafkaBroker


@pytest.mark.asyncio()
async def test_me() -> None:
    async with TestKafkaBroker(broker):
        await broker.publish_batch(
            HelloWorld(msg="First Hello"),
            HelloWorld(msg="Second Hello"),
            topic="test_batch",
        )
        handle_batch.mock.assert_called_with(
            [dict(HelloWorld(msg="First Hello")), dict(HelloWorld(msg="Second Hello"))],
        )
