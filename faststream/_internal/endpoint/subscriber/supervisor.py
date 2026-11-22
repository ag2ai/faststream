from __future__ import annotations

import asyncio
import logging
import os
import time
from asyncio import CancelledError, Task
from collections import UserDict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from faststream._internal.configs import BrokerConfig


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
        "broker_config",
        "func",
        "ignored_exceptions",
        "kwargs",
        "restart_on_failure",
        "tasks_supervisor",
    )

    # stores hash identifier of exceptions which whose info was printed
    __cache: _SupervisorCache = _SupervisorCache()

    def __init__(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: tuple[Any] | None,
        func_kwargs: dict[str, Any] | None,
        tasks_supervisor: TasksSupervisor,
        broker_config: BrokerConfig,
        *,
        ignored_exceptions: tuple[type[BaseException], ...] = (CancelledError,),
        restart_on_failure: bool = True,
    ) -> None:
        self.tasks_supervisor = tasks_supervisor
        self.broker_config = broker_config
        self.func = func
        self.args = func_args or ()
        self.kwargs = func_kwargs or {}
        self.ignored_exceptions = ignored_exceptions
        self.restart_on_failure = restart_on_failure

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
        self.broker_config.logger.log(
            f"callback for {task.get_name()} is being executed...",
            log_level=logging.INFO,
        )

        if task.cancelled() or self.is_disabled:
            return

        # `stop()` forgot this task: a restart would outlive the subscriber
        if task not in self.tasks_supervisor.tasks:
            return

        if (exc := task.exception()) and not isinstance(exc, self.ignored_exceptions):
            # trace is printed only once, but task is still retried
            identifier = self._get_exception_identifier(exc)
            if identifier not in self.__cache:
                self.__cache.add(identifier)
                if self.restart_on_failure:
                    message = (
                        f"{task.get_name()} raised an exception, retrying...\n"
                        "If this behavior causes issues, you can disable it via setting "
                        "the FASTSTREAM_SUPERVISOR_DISABLED env to 1. Also, please "
                        "consider opening issue on the repository: "
                        "https://github.com/ag2ai/faststream."
                    )
                else:
                    message = (
                        f"{task.get_name()} raised an exception and won't be restarted."
                    )

                self.broker_config.logger.log(
                    message, exc_info=exc, log_level=logging.ERROR
                )

            if self.restart_on_failure:
                self.tasks_supervisor.add_task(self.func, self.args, self.kwargs)


class TasksSupervisor:
    __slots__ = ("broker_config", "tasks")

    def __init__(self, broker_config: BrokerConfig) -> None:
        self.tasks: list[asyncio.Task[Any]] = []
        self.broker_config = broker_config

    def add_task(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]],
        func_args: tuple[Any, ...] | None = None,
        func_kwargs: dict[str, Any] | None = None,
        *,
        restart_on_failure: bool = True,
    ) -> None:
        args = func_args or ()
        kwargs = func_kwargs or {}
        task = asyncio.create_task(func(*args, **kwargs))
        callback = TaskCallbackSupervisor(
            func,
            func_args,
            func_kwargs,
            self,
            self.broker_config,
            restart_on_failure=restart_on_failure,
        )
        task.add_done_callback(callback)
        self.tasks.append(task)

    async def stop(self) -> None:
        """Clean up handler subscription, cancel consume task in graceful mode."""
        for task in self.tasks:
            if not task.done():
                task.cancel()

        self.tasks.clear()
