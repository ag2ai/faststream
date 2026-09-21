import asyncio
import multiprocessing
import os
import signal
import sys
import time
from collections.abc import Callable
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal._compat import IS_WINDOWS

if TYPE_CHECKING:
    from multiprocessing.context import SpawnProcess
    from types import FrameType

    from faststream._internal.basic_types import DecoratedCallableNone

multiprocessing.allow_connection_pickling()
spawn = multiprocessing.get_context("spawn")


# Matches gunicorn's graceful timeout: long enough for a worker to drain, short
# enough that a stuck one cannot hold the supervisor hostage.
SHUTDOWN_TIMEOUT = 30.0

# How often a lock-free wait re-reads its flag.
EXIT_POLL_INTERVAL = 0.05

HANDLED_SIGNALS: tuple[int, ...] = (
    signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
    signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
)
if IS_WINDOWS:  # pragma: py-not-win32
    # Windows signal 21. Sent by Ctrl+Break.
    sigbreak: int | None = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        HANDLED_SIGNALS += (sigbreak,)


class ExitEvent:
    """A `threading.Event` a signal handler can set, because nothing here takes a lock.

    `threading.Event.set()` holds the condition's lock while it notifies waiters. A
    second signal arriving in that window re-enters the handler and deadlocks the
    process on a lock it already owns (see issue #3119).
    """

    def __init__(self) -> None:
        self._is_set = False

    def set(self) -> None:
        self._is_set = True

    def is_set(self) -> bool:
        return self._is_set

    def wait(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout

        while not self._is_set:
            if deadline is None:
                time.sleep(EXIT_POLL_INTERVAL)
                continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False

            time.sleep(min(EXIT_POLL_INTERVAL, remaining))

        return True


def set_exit(
    func: Callable[[int, Optional["FrameType"]], Any],
    *,
    sync: bool = False,
) -> None:
    """Set exit handler for signals.

    Args:
        func: A callable object that takes an integer and an optional frame type as arguments and returns any value.
        sync: set sync or async signal callback.
    """
    if not sync:
        with suppress(NotImplementedError):
            loop = asyncio.get_event_loop()

            for sig in HANDLED_SIGNALS:
                loop.add_signal_handler(sig, func, sig, None)

            return

    # Windows or sync mode
    for sig in HANDLED_SIGNALS:
        signal.signal(sig, func)


def get_subprocess(
    target: "DecoratedCallableNone", args: tuple[Any, ...]
) -> "SpawnProcess":
    """Spawn a subprocess."""
    stdin_fileno: int | None
    try:
        stdin_fileno = sys.stdin.fileno()
    except OSError:
        stdin_fileno = None

    return spawn.Process(
        target=subprocess_started,
        args=args,
        kwargs={"t": target, "stdin_fileno": stdin_fileno},
    )


def stop_process(process: "SpawnProcess", timeout: float = SHUTDOWN_TIMEOUT) -> None:
    """Terminate a worker, escalating to SIGKILL if it outlives the timeout."""
    process.terminate()
    process.join(timeout)

    if process.is_alive():
        # An unbounded join hangs the supervisor forever on a worker deaf to SIGTERM.
        process.kill()
        process.join()


def subprocess_started(
    *args: Any,
    t: "DecoratedCallableNone",
    stdin_fileno: int | None,
) -> None:
    """Start a subprocess."""
    if stdin_fileno is not None:  # pragma: no cover
        sys.stdin = os.fdopen(stdin_fileno)
    t(*args)
