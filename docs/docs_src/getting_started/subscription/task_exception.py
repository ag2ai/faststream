from collections.abc import Awaitable, Callable
from types import MethodType
from typing import Any

from faststream import FastStream, Logger
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)

subscriber = broker.subscriber("critical-channel")
default_task_exception_handler = subscriber._handle_task_exception


def handle_task_exception(
    self: Any,
    exc: BaseException,
    func: Callable[..., Awaitable[Any]],
    func_args: tuple[Any, ...],
    func_kwargs: dict[str, Any],
) -> None:
    if isinstance(exc, RuntimeError):
        self.running = False
        return

    default_task_exception_handler(exc, func, func_args, func_kwargs)


subscriber._handle_task_exception = MethodType(handle_task_exception, subscriber)


@subscriber
async def handle(msg: str, logger: Logger) -> None:
    logger.info(msg)
