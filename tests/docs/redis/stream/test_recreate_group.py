from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from faststream.redis import StreamGroupNotFoundError


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_recreate_group() -> None:
    from docs.docs_src.redis.stream.recreate_group import (
        handle_task_exception,
        recreate_group,
        redis,
        subscriber,
    )

    consume_task = MagicMock()

    with (
        patch.object(redis, "xgroup_create", new_callable=AsyncMock) as create_group,
        patch.object(subscriber, "add_task") as add_task,
    ):
        await recreate_group(consume_task, ("read",), {"key": "value"})

    create_group.assert_awaited_once()
    add_task.assert_called_once_with(consume_task, ("read",), {"key": "value"})

    with patch.object(subscriber, "add_task") as add_task:
        handle_task_exception(
            StreamGroupNotFoundError("missing group"),
            consume_task,
            (),
            {},
        )

    add_task.assert_called_once_with(recreate_group, (consume_task, (), {}))
