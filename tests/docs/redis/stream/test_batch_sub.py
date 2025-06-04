import pytest

from faststream.redis import TestRedisBroker
from tests.marks import python39


@pytest.mark.asyncio()
@python39
async def test_stream_batch() -> None:
    from docs.docs_src.redis.stream.batch_sub import broker, handle

    async with TestRedisBroker(broker) as br:
        await br.publish("Hi!", stream="test-stream")
        handle.mock.assert_called_once_with(["Hi!"])
