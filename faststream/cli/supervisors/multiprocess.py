from multiprocessing.context import SpawnProcess
from typing import Any, List, Tuple

from faststream.cli.supervisors.basereload import BaseReload
from faststream.log import logger
from faststream.types import DecoratedCallable


class Multiprocess(BaseReload):
    """A class to represent a multiprocess.

    Attributes:
        target : the target function to be executed by each process
        args : arguments to be passed to the target function
        workers : number of worker processes

    Methods:
        startup : starts the parent process and creates worker processes
        shutdown : terminates and joins all worker processes, and stops the parent process

    """

    def __init__(
        self,
        target: DecoratedCallable,
        args: Tuple[Any, ...],
        workers: int,
    ) -> None:
        """Initialize a new instance of the class.

        Args:
            target: The target callable object to be executed.
            args: The arguments to be passed to the target callable.
            workers: The number of workers to be used.

        Returns:
            None.

        """
        super().__init__(target, args, None)

        self.workers = workers
        self.processes: List[SpawnProcess] = []

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
