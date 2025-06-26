import signal
from multiprocessing.context import SpawnProcess
from typing import Any

import pytest

from faststream._internal.cli.supervisors.basereload import BaseReload, get_subprocess


class PatchedBaseReload(BaseReload):
    def restart(self) -> None:
        super().restart()
        self.should_exit.set()

    def should_restart(self) -> bool:
        return True

    def start_process(self, worker_id: int | None = None) -> SpawnProcess:
        process = get_subprocess(target=self._target, args=self._args)
        process.start()
        return process


def empty(*args: Any, **kwargs: Any) -> None:
    pass


@pytest.mark.slow()
def test_base() -> None:
    processor = PatchedBaseReload(target=empty, args=())

    processor._args = (processor.pid,)
    processor.run()

    code = abs(processor._process.exitcode or 0)
    assert code in {signal.SIGTERM.value, 0}
