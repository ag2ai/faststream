from __future__ import annotations

import logging
import os
import time
from asyncio import CancelledError, Task
from collections import UserDict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from faststream._internal.endpoint.subscriber.mixins import TasksMixin


class _SupervisorCache(UserDict[int, float]):
    @property
    def ttl(self) -> float:
        return float(os.getenv("FASTSTREAM_SUPERVISOR_CACHE_TTL", "3600"))

    def _cleanup(self) -> None:
        # no need to clean up empty storage
        if not len(self):
            return

        # dict preserves insertion order hence if the first element is not outdated there is no reason to check others
        _, timestamp = next(iter(self.items()))
        now = time.time()
        if now - timestamp < self.ttl:
            return

        now, to_delete = time.time(), []
        for k, v in self.items():
            if now - v >= self.ttl:
                to_delete.append(k)
            else:
                break

        for k in to_delete:
            del self[k]

    def add(self, item: int) -> None:
        self[item] = time.time()

    def __contains__(self, key: object) -> bool:
        self._cleanup()
        return super().__contains__(key)


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

    # stores hash identifier of exceptions which whose info was printed
    __cache: _SupervisorCache = _SupervisorCache()

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

    @staticmethod
    def _get_exception_identifier(
        exception: BaseException, *, message_size: int = 4096
    ) -> int:
        # NOTE: method accepts only raised exceptions (e.g. with __traceback__)
        line, message, klass = (
            getattr(exception.__traceback__, "tb_lineno", 0),
            str(exception)[:message_size],
            str(exception.__class__),
        )
        to_hash = f"{line}-{message}-{klass}"

        return hash(to_hash)

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
            # trace is printed only once, but task is still retried
            identifier = self._get_exception_identifier(exc)
            if identifier not in self.__cache:
                self.__cache.add(identifier)
                logger.log(
                    f"{task.get_name()} raised an exception, retrying...\n"
                    "If this behavior causes issues, you can disable it via setting the FASTSTREAM_SUPERVISOR_DISABLED env to 1. "
                    "Also, please consider opening issue on the repository: https://github.com/ag2ai/faststream.",
                    exc_info=exc,
                    log_level=logging.ERROR,
                )

            self.subscriber.add_task(self.func, self.args, self.kwargs)
