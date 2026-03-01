import signal
from typing import TYPE_CHECKING

from faststream._internal.cli.supervisors.basereload import BaseReload
from faststream._internal.logger import logger

if TYPE_CHECKING:
    from multiprocessing.context import SpawnProcess

    from faststream._internal.cli.dto import RunArgs, RunFunction


class Multiprocess(BaseReload):
    """A class to represent a multiprocess."""

    def __init__(
        self,
        target: "RunFunction",
        args: "RunArgs",
        workers: int,
        reload_delay: float = 0.5,
    ) -> None:
        super().__init__(target, args, reload_delay)

        self.workers = workers
        self.processes: list[SpawnProcess] = []

    def startup(self) -> None:
        logger.info("Started parent process [%s]", self.pid)

        for worker_id in range(self.workers):
            process = self.start_process(worker_id=worker_id)
            logger.info("Started child process %s [%s]", worker_id, process.pid)
            self.processes.append(process)

    def shutdown(self) -> None:
        for worker_id, process in enumerate(self.processes):
            process.terminate()
            logger.info("Stopping child process %s [%s]", worker_id, process.pid)
            process.join()

        logger.info("Stopping parent process [%s]", self.pid)

    def restart(self) -> None:
        active_processes = []

        for worker_id, process in enumerate(self.processes):
            if process.is_alive():
                active_processes.append(process)
                continue

            log_msg = "Worker %s (pid:%s) exited with code %s."
            if process.exitcode and abs(process.exitcode) == signal.SIGKILL:
                log_msg += " Perhaps out of memory?"
            logger.error(log_msg, worker_id, process.pid, process.exitcode)

            process.kill()

            new_process = self.start_process(worker_id=worker_id)
            logger.info("Started child process [%s]", new_process.pid)
            active_processes.append(new_process)

        self.processes = active_processes

    def should_restart(self) -> bool:
        return not all(p.is_alive() for p in self.processes)
