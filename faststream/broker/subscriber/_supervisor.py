from __future__ import annotations

from asyncio import CancelledError
from logging import getLogger
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Coroutine

if TYPE_CHECKING:
    from asyncio import Task

    from faststream.broker.subscriber.mixins import TasksMixin

# stores how many times each coroutine has been retried
_attempts_counter: dict[Callable[..., Coroutine[Any, Any, Any]], int] = {}


class TaskCallbackSupervisor:
    __ignored_exceptions: ClassVar = [CancelledError]

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: tuple[Any] | None,
        func_kwargs: dict[str, Any] | None,
        subscriber: TasksMixin,
        *,
        max_attempts: int = 3,
    ):
        self.subscriber = subscriber
        self.func = func
        self.args: tuple[Any] | tuple[()] = func_args or ()
        self.kwargs: dict[str, Any] = func_kwargs or {}
        self.max_attempts = max_attempts

    def _register_task(self) -> None:
        attempts: int = _attempts_counter.get(self.func, 1)
        if attempts < self.max_attempts:
            self.subscriber.add_task(self.func, self.args, self.kwargs)  # type: ignore
            _attempts_counter[self.func] = attempts + 1

    def __call__(self, task: Task[Any]) -> None:
        if task.cancelled():
            return

        if (exc := task.exception()) and exc not in self.__ignored_exceptions:
            logger = getattr(self.subscriber, "logger", getLogger(__name__))
            logger.error(
                f"{task.get_name()} raised an exception, retrying...", exc_info=exc
            )
            self._register_task()
