import os
import threading
from multiprocessing.context import SpawnProcess
from typing import Any, Optional, Tuple

from faststream.cli.supervisors.utils import get_subprocess, set_exit
from faststream.log import logger
from faststream.types import DecoratedCallable


class BaseReload:
    """A base class for implementing a reloader process.

    Attributes:
        _process : The spawned process
        _target : The target callable function
        _args : The arguments to be passed to the target function
        reload_delay : The delay between reloads
        should_exit : A threading event to signal the reloader to exit
        pid : The process ID of the reloader
        reloader_name : The name of the reloader

    Methods:
        __init__(self, target: DecoratedCallable, args: Tuple[Any, ...], reload_delay: Optional[float] = 0.5) -> None:
            Initializes the BaseReload object.

        run(self) -> None:
            Runs the reloader process.

        startup(self) -> None:
            Performs startup operations for the reloader process.

        restart(self) -> None:
            Restarts the process.

        shutdown(self) -> None:
            Shuts down the reloader process.

        _stop_process(self) -> None:
            Stops the spawned process.

        _start_process(self) -> SpawnProcess:
            Starts the spawned process.

        should_restart(self) -> bool:
            Determines whether the process should be restarted.

    """

    _process: SpawnProcess
    _target: DecoratedCallable
    _args: Tuple[Any, ...]

    reload_delay: Optional[float]
    should_exit: threading.Event
    pid: int
    reloader_name: str = ""

    def __init__(
        self,
        target: DecoratedCallable,
        args: Tuple[Any, ...],
        reload_delay: Optional[float] = 0.5,
    ) -> None:
        """Initialize a class instance.

        Args:
            target: The target callable object
            args: Tuple of arguments to be passed to the target callable
            reload_delay: Optional delay in seconds before reloading the target callable (default is 0.5 seconds)

        Returns:
            None

        """
        self._target = target
        self._args = args

        self.should_exit = threading.Event()
        self.pid = os.getpid()
        self.reload_delay = reload_delay

        set_exit(lambda *_: self.should_exit.set())

    def run(self) -> None:
        self.startup()
        while not self.should_exit.wait(self.reload_delay):
            if self.should_restart():  # pragma: no branch
                self.restart()
        self.shutdown()

    def startup(self) -> None:
        logger.info(f"Started reloader process [{self.pid}] using {self.reloader_name}")
        self._process = self._start_process()

    def restart(self) -> None:
        self._stop_process()
        logger.info("Process successfully reloaded")
        self._process = self._start_process()

    def shutdown(self) -> None:
        self._stop_process()
        logger.info(f"Stopping reloader process [{self.pid}]")

    def _stop_process(self) -> None:
        self._process.terminate()
        self._process.join()

    def _start_process(self) -> SpawnProcess:
        process = get_subprocess(target=self._target, args=self._args)
        process.start()
        return process

    def should_restart(self) -> bool:
        raise NotImplementedError("Reload strategies should override should_restart()")
