import os
import signal
import time
from multiprocessing.context import SpawnProcess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from faststream._internal.cli.supervisors.utils import get_subprocess
from faststream._internal.cli.supervisors.watchfiles import WatchReloader
from tests.cli import interfaces
from tests.marks import skip_windows

DIR = Path(__file__).resolve().parent


class PatchedWatchReloader(WatchReloader):
    def start_process(self, worker_id: int | None = None) -> SpawnProcess:
        process = get_subprocess(target=self._target, args=self._args)
        process.start()
        return process


@pytest.mark.slow()
@skip_windows
def test_base(generate_template: interfaces.GenerateTemplateFactory) -> None:
    with generate_template("") as file_path:
        processor = PatchedWatchReloader(
            target=exit,
            args=(),
            reload_dirs=[str(file_path.parent)],
        )

        processor._args = (processor.pid,)
        processor.run()

        code = abs(processor._process.exitcode or 0)

    assert code in {signal.SIGTERM.value, 0}, code


@pytest.mark.slow()
@skip_windows
def test_restart(
    mock: MagicMock, generate_template: interfaces.GenerateTemplateFactory
) -> None:
    with generate_template("") as file_path:
        processor = PatchedWatchReloader(
            target=touch_file,
            args=(file_path,),
            reload_dirs=[file_path.parent],
        )

        mock.side_effect = lambda: exit(processor.pid)

        with patch.object(processor, "restart", mock):
            processor.run()

    mock.assert_called_once()


def touch_file(file: Path) -> None:
    while True:
        time.sleep(0.1)
        file.write_text("hello", encoding="utf-8")


def exit(parent_id: int) -> None:
    os.kill(parent_id, signal.SIGINT)
