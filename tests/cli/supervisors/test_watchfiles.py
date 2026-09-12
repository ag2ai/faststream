import signal
import threading
import time
from multiprocessing.context import SpawnProcess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from faststream._internal.cli.dto import RunArgs
from faststream._internal.cli.supervisors.utils import get_subprocess
from faststream._internal.cli.supervisors.watchfiles import WatchReloader
from tests.cli import interfaces
from tests.marks import skip_windows

DIR = Path(__file__).resolve().parent


class PatchedWatchReloader(WatchReloader):
    def start_process(self, worker_id: int | None = None) -> SpawnProcess:
        process = get_subprocess(target=self._target, args=(self._args,))
        process.start()
        return process


@pytest.mark.slow()
@skip_windows
def test_base(generate_template: interfaces.GenerateTemplateFactory) -> None:
    with generate_template("") as file_path:
        processor = PatchedWatchReloader(
            target=sleep_forever,
            args=RunArgs(app=""),
            reload_dirs=[str(file_path.parent)],
        )
        threading.Timer(0.1, processor.should_exit.set).start()

        processor.run()

    assert processor._process.exitcode == -signal.SIGTERM


@pytest.mark.slow()
@skip_windows
def test_restart(
    mock: MagicMock, generate_template: interfaces.GenerateTemplateFactory
) -> None:
    with generate_template("") as file_path:
        processor = PatchedWatchReloader(
            target=touch_file,
            args=RunArgs(app=str(file_path)),
            reload_dirs=[file_path.parent],
        )
        # one reload is all this pins, so stop before the watcher reports another
        mock.side_effect = processor.should_exit.set

        with patch.object(processor, "restart", mock):
            processor.run()

    mock.assert_called_once()


def sleep_forever(args: RunArgs) -> None:  # pragma: no cover
    time.sleep(60)


def touch_file(args: RunArgs) -> None:  # pragma: no cover
    while True:
        time.sleep(0.1)
        Path(args.app).write_text("hello", encoding="utf-8")
