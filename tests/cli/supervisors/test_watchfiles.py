import os
import signal
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from faststream._internal.cli.supervisors.watchfiles import WatchReloader
from tests.marks import skip_windows

DIR = Path(__file__).resolve().parent


@pytest.mark.slow()
@skip_windows
def test_base() -> None:
    processor = WatchReloader(target=exit, args=(), reload_dirs=[DIR])

    processor._args = (processor.pid,)
    processor.run()

    code = abs(processor._process.exitcode or 0)
    assert code in {signal.SIGTERM.value, 0}, code


@pytest.mark.slow()
@skip_windows
def test_restart(mock: MagicMock) -> None:
    file = DIR / "file.py"

    processor = WatchReloader(target=touch_file, args=(file,), reload_dirs=[DIR])

    mock.side_effect = lambda: exit(processor.pid)

    with patch.object(processor, "restart", mock):
        processor.run()

    try:
        mock.assert_called_once()
    finally:
        file.unlink()


def touch_file(file: Path) -> None:
    while True:
        time.sleep(0.1)
        file.write_text("hello")


def exit(parent_id: int) -> None:
    os.kill(parent_id, signal.SIGINT)
