import pytest

from faststream.redis import TestRedisBroker
from tests.marks import python39


@pytest.mark.asyncio()
@python39
async def test_batch() -> None:
    from docs.docs_src.redis.list.sub_batch import broker, handle

    async with TestRedisBroker(broker) as br:
        await br.publish_batch("Hi", "again", list="test-list")
        handle.mock.assert_called_once_with(["Hi", "again"])
