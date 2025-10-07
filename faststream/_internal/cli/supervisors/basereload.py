import os
import threading
from multiprocessing.context import SpawnProcess
from typing import TYPE_CHECKING

from faststream._internal.cli.supervisors.utils import get_subprocess, set_exit
from faststream._internal.logger import logger

if TYPE_CHECKING:
    from faststream._internal.cli.dto import RunArgs, RunFunction


class BaseReload:
    """A base class for implementing a reloader process."""

    _process: SpawnProcess

    reload_delay: float | None
    should_exit: threading.Event
    pid: int
    reloader_name: str = ""

    def __init__(
        self,
        target: "RunFunction",
        args: "RunArgs",
        reload_delay: float | None = 0.5,
    ) -> None:
        self._target = target
        self._args = args

        self.should_exit = threading.Event()
        self.pid = os.getpid()
        self.reload_delay = reload_delay

        set_exit(lambda *_: self.should_exit.set(), sync=True)

    def run(self) -> None:
        self.startup()
        while not self.should_exit.wait(self.reload_delay):
            if self.should_restart():  # pragma: no branch
                self.restart()
        self.shutdown()

    def startup(self) -> None:
        logger.info(
            "Started reloader process [%s] using %s",
            self.pid,
            self.reloader_name,
        )
        self._process = self.start_process()

    def restart(self) -> None:
        self._stop_process()
        logger.info("Process successfully reloaded")
        self._process = self.start_process()

    def shutdown(self) -> None:
        self._stop_process()
        logger.info("Stopping reloader process [%s]", self.pid)

    def _stop_process(self) -> None:
        self._process.terminate()
        self._process.join()

    def start_process(self, worker_id: int | None = None) -> SpawnProcess:
        self._args.extra_options["worker_id"] = worker_id
        process = get_subprocess(target=self._target, args=(self._args,))
        process.start()
        return process

    def should_restart(self) -> bool:
        msg = "Reload strategies should override should_restart()"
        raise NotImplementedError(msg)
