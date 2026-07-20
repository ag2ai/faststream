from unittest.mock import Mock

import pytest

from faststream.redis import StreamGroupNotFoundError


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_group_task_exception() -> None:
    from docs.docs_src.redis.stream.group_task_exception import app, subscriber

    async def task() -> None:
        pass

    app.exit = Mock()

    subscriber._handle_task_exception(
        StreamGroupNotFoundError("missing group"),
        task,
        (),
        {},
    )

    app.exit.assert_called_once_with()
