from __future__ import annotations

import os
from asyncio import CancelledError
from logging import getLogger
from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:
    from asyncio import Task
    from collections.abc import Callable, Coroutine

    from faststream._internal.endpoint.subscriber.mixins import TasksMixin

# stores how many times each coroutine has been retried
_attempts_counter: dict[Callable[..., Coroutine[Any, Any, Any]], int] = {}

# supervisor can affect some test cases, so it might be useful to have global killswitch
SUPERVISOR_DISABLING_ENV_NAME: Final[str] = "FASTSTREAM_SUPERVISOR_DISABLED"


class TaskCallbackSupervisor:
    """Supervisor for asyncio.Task spawned in TaskMixin implemented via task callback."""

    __ignored_exceptions: tuple[type[BaseException], ...] = (CancelledError,)

    __slots__ = (
        "args",
        "func",
        "kwargs",
        "max_attempts",
        "subscriber",
    )

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: tuple[Any] | None,
        func_kwargs: dict[str, Any] | None,
        subscriber: TasksMixin,
        *,
        max_attempts: int = 3,
    ) -> None:
        self.subscriber = subscriber
        self.func = func
        self.args: tuple[Any] | tuple[()] = func_args or ()
        self.kwargs: dict[str, Any] = func_kwargs or {}
        self.max_attempts = max_attempts

    def _register_task(self) -> None:
        attempts: int = _attempts_counter.get(self.func, 1)
        if attempts < self.max_attempts:
            self.subscriber.add_task(self.func, self.args, self.kwargs)
            _attempts_counter[self.func] = attempts + 1

    @property
    def _is_disabled(self) -> bool:
        """Checks if supervisor is disabled globally."""
        try:
            integer: int = int(os.getenv(SUPERVISOR_DISABLING_ENV_NAME, default="0"))
        except (ValueError, TypeError):
            return False

        return bool(integer)

    def __call__(self, task: Task[Any]) -> None:
        if task.cancelled() or self._is_disabled:
            return

        if (exc := task.exception()) and not isinstance(exc, self.__ignored_exceptions):
            logger = getattr(self.subscriber, "logger", getLogger(__name__))
            logger.error(
                f"{task.get_name()} raised an exception, retrying...", exc_info=exc
            )
            self._register_task()
