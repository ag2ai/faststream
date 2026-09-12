import os
import signal
import threading
import time
from multiprocessing.context import SpawnProcess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from faststream._internal.cli.dto import RunArgs
from faststream._internal.cli.supervisors.utils import get_subprocess
from faststream._internal.cli.supervisors.watchfiles import WatchReloader
from tests.cli import interfaces
from tests.marks import skip_windows

DIR = Path(__file__).resolve().parent


class PatchedWatchReloader(WatchReloader):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.watching = threading.Event()

    def start_process(self, worker_id: int | None = None) -> SpawnProcess:
        process = get_subprocess(target=self._target, args=(self._args,))
        process.start()
        return process

    def should_restart(self) -> bool:
        self.watching.set()
        return super().should_restart()


@pytest.mark.slow()
@skip_windows
def test_watcher_stops_on_exit_signal(
    generate_template: interfaces.GenerateTemplateFactory,
) -> None:
    with generate_template("") as file_path:
        processor = PatchedWatchReloader(
            target=sleep_forever,
            args=RunArgs(app=""),
            reload_dirs=[str(file_path.parent)],
        )
        sender = threading.Thread(target=signal_while_watching, args=(processor,))
        sender.start()

        processor.run()
        sender.join()

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


def signal_while_watching(processor: PatchedWatchReloader) -> None:
    # the watcher blocks the run loop, and watchfiles brings its own interrupt
    # handling — only `stop_event` gets the reloader out of that step cleanly
    assert processor.watching.wait(timeout=10)
    time.sleep(0.2)
    os.kill(processor.pid, signal.SIGINT)


def sleep_forever(args: RunArgs) -> None:  # pragma: no cover
    time.sleep(60)


def touch_file(args: RunArgs) -> None:  # pragma: no cover
    while True:
        time.sleep(0.1)
        Path(args.app).write_text("hello", encoding="utf-8")
