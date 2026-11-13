"""Run a CLI page's snippet the way the page does, inside the test process."""

import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any

import pytest
from typer.testing import CliRunner

from faststream import FastStream, TestApp
from faststream._internal.cli.main import cli

SUPERVISORS = "faststream._internal.cli.supervisors"


def run_app(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    patched_broker: AbstractAsyncContextManager[Any],
    app_path: str,
    *options: str,
) -> str:
    """Run `faststream run <app_path> <options>` and return what the app printed.

    Args:
        runner: the CLI runner capturing the output.
        monkeypatch: the context the patched pieces live in.
        patched_broker: the in-memory broker the app runs against.
        app_path: the snippet to run, as the CLI spells it.
        options: the options the page passes, `--workers 2` among them.
    """

    async def run(
        self: FastStream,
        log_level: int = logging.INFO,
        run_extra_options: dict[str, Any] | None = None,
    ) -> None:
        # the real `run` blocks until a signal: start the app against an
        # in-memory broker, run its hooks with the options the CLI parsed, stop it
        async with patched_broker, TestApp(self, run_extra_options):
            pass

    with monkeypatch.context() as patched:
        patched.setattr(FastStream, "run", run)
        # `--workers N` still numbers its children, but starts them here
        patched.setattr(f"{SUPERVISORS}.basereload.get_subprocess", start_worker_here)
        patched.setattr(f"{SUPERVISORS}.basereload.set_exit", ignore_signals)
        patched.setattr(f"{SUPERVISORS}.utils.ExitEvent.wait", stop_supervising)

        result = runner.invoke(cli, ["run", app_path, *options])

    assert result.exit_code == 0, result.output
    return result.output


class StartedWorker:
    """What the supervisor gets back in place of a spawned process."""

    pid = 0

    def start(self) -> None: ...

    def is_alive(self) -> bool:
        return True

    def terminate(self) -> None: ...

    def join(self, timeout: float | None = None) -> None: ...

    def kill(self) -> None: ...


def start_worker_here(
    target: Callable[..., None],
    args: tuple[Any, ...],
) -> StartedWorker:
    target(*args)
    return StartedWorker()


def ignore_signals(*args: Any, **kwargs: Any) -> None:
    # the supervisor would take SIGINT and SIGTERM over from pytest
    ...


def stop_supervising(self: Any, timeout: float | None = None) -> bool:
    # the supervisor watches its children until a signal arrives; the children
    # this run started have already finished by the time it starts waiting
    return True
