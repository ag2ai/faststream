from unittest.mock import call

import pytest

from examples.redis.skip_none_batch import app, broker, handle, handle_list
from faststream.redis import TestApp, TestRedisBroker


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_example() -> None:
    async with TestRedisBroker(broker), TestApp(app):
        await handle.wait_call(3)
        await handle_list.wait_call(3)

        handle.mock.assert_has_calls([call("FastStream"), call("empty")])

        # `None` batch items were excluded,
        # and the all-`None` batch was skipped entirely
        handle_list.mock.assert_called_once_with(["Hi!", "FastStream"])
