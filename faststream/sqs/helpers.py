from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

import anyio
from botocore.exceptions import BotoCoreError, ClientError

T = TypeVar("T")


async def poll_with_backoff(
    receive: Callable[[], Awaitable[T]],
    *,
    is_running: Callable[[], bool],
    on_error: Callable[[Exception, float], None],
    max_backoff: float = 30.0,
) -> AsyncIterator[T]:
    """Yield SQS receive results in a loop, backing off exponentially on errors.

    Transient client errors (bad credentials, deleted queue, network issues)
    are reported via ``on_error`` and retried with a growing delay instead of
    busy-spinning against the (billable) SQS API.
    """
    backoff = 1.0
    while is_running():
        try:
            result = await receive()
        except (ClientError, BotoCoreError) as e:
            on_error(e, backoff)
            await anyio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            continue
        backoff = 1.0
        yield result
