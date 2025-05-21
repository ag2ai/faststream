import signal
from typing import TYPE_CHECKING, Any

from faststream._internal.cli.supervisors.basereload import BaseReload
from faststream._internal.log import logger

if TYPE_CHECKING:
    from multiprocessing.context import SpawnProcess

    from faststream._internal.basic_types import DecoratedCallable


class Multiprocess(BaseReload):
    """A class to represent a multiprocess."""

    def __init__(
        self,
        target: "DecoratedCallable",
        args: tuple[Any, ...],
        workers: int,
        reload_delay: float = 0.5,
    ) -> None:
        super().__init__(target, args, reload_delay)

        self.workers = workers
        self.processes: list[SpawnProcess] = []

    def startup(self) -> None:
        logger.info(f"Started parent process [{self.pid}]")

        for _ in range(self.workers):
            process = self._start_process()
            logger.info(f"Started child process [{process.pid}]")
            self.processes.append(process)

    def shutdown(self) -> None:
        for process in self.processes:
            process.terminate()
            logger.info(f"Stopping child process [{process.pid}]")
            process.join()

        logger.info(f"Stopping parent process [{self.pid}]")

    def restart(self) -> None:
        active_processes = []

        for process in self.processes:
            if process.is_alive():
                active_processes.append(process)
                continue

            pid = process.pid
            exitcode = process.exitcode

            log_msg = "Worker (pid:%s) exited with code %s."
            if exitcode and abs(exitcode) == signal.SIGKILL:
                log_msg += " Perhaps out of memory?"
            logger.error(log_msg, pid, exitcode)

            process.kill()

            new_process = self._start_process()
            logger.info(f"Started child process [{new_process.pid}]")
            active_processes.append(new_process)

        self.processes = active_processes

    def should_restart(self) -> bool:
        return not all(p.is_alive() for p in self.processes)
