from collections.abc import Callable, Coroutine
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError

from faststream import FastStream, Logger
from faststream.redis import RedisBroker, StreamGroupNotFoundError, StreamSub

redis_url = "redis://localhost:6379"
stream_name = "test-stream"
group_name = "test-group"

broker = RedisBroker(redis_url)
redis = Redis.from_url(redis_url)
app = FastStream(broker)

subscriber = broker.subscriber(
    stream=StreamSub(stream_name, group=group_name, consumer="1"),
)
default_task_exception_handler = subscriber.handle_task_exception


async def recreate_group(
    func: Callable[..., Coroutine[Any, Any, Any]],
    func_args: tuple[Any, ...],
    func_kwargs: dict[str, Any],
) -> None:
    try:
        await redis.xgroup_create(
            name=stream_name,
            groupname=group_name,
            id="$",
            mkstream=True,
        )
    except ResponseError as error:
        if "BUSYGROUP" not in str(error):
            raise

    subscriber.add_task(func, func_args, func_kwargs)


def handle_task_exception(
    error: BaseException,
    func: Callable[..., Coroutine[Any, Any, Any]],
    func_args: tuple[Any, ...],
    func_kwargs: dict[str, Any],
) -> None:
    if isinstance(error, StreamGroupNotFoundError):
        subscriber.add_task(recreate_group, (func, func_args, func_kwargs))
        return

    default_task_exception_handler(error, func, func_args, func_kwargs)


subscriber.handle_task_exception = handle_task_exception


@app.after_shutdown
async def close_redis() -> None:
    await redis.aclose()


@subscriber
async def handle(msg: str, logger: Logger) -> None:
    logger.info(msg)
