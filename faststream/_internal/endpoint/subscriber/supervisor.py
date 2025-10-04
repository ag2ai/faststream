from __future__ import annotations

import logging
import os
from asyncio import CancelledError, Task
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from faststream._internal.endpoint.subscriber.mixins import TasksMixin


class TaskCallbackSupervisor:
    """Supervisor for asyncio.Task spawned in TaskMixin implemented via task callback."""

    __slots__ = (
        "args",
        "func",
        "ignored_exceptions",
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
        ignored_exceptions: tuple[type[BaseException], ...] = (CancelledError,),
    ) -> None:
        self.subscriber = subscriber
        self.func = func
        self.args = func_args or ()
        self.kwargs = func_kwargs or {}
        self.ignored_exceptions = ignored_exceptions

    @property
    def is_disabled(self) -> bool:
        # supervisor can affect some test cases, so it might be useful to have global killswitch.
        return bool(int(os.getenv("FASTSTREAM_SUPERVISOR_DISABLED", "0")))

    def __call__(self, task: Task[Any]) -> None:
        logger = self.subscriber._outer_config.logger

        logger.log(
            f"callback for {task.get_name()} is being executed...",
            log_level=logging.INFO,
        )

        if task.cancelled() or self.is_disabled:
            return

        if (exc := task.exception()) and not isinstance(exc, self.ignored_exceptions):
            logger.log(
                f"{task.get_name()} raised an exception, retrying...\n"
                "If this behavior causes issues, you can disable it via setting the FASTSTREAM_SUPERVISOR_DISABLED env to 1. "
                "Also, please consider opening issue on the repository: https://github.com/ag2ai/faststream.",
                exc_info=exc,
                log_level=logging.ERROR,
            )

            self.subscriber.add_task(self.func, self.args, self.kwargs)
