import os
import signal
import sys
import time
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from faststream._internal.cli.supervisors.watchfiles import WatchReloader

DIR = Path(__file__).resolve().parent


def exit(parent_id) -> None:  # pragma: no cover
    os.kill(parent_id, signal.SIGINT)


@pytest.mark.slow()
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_base() -> None:
    processor = WatchReloader(target=exit, args=(), reload_dirs=[DIR])

    processor._args = (processor.pid,)
    processor.run()

    code = abs(processor._process.exitcode)
    assert code in {signal.SIGTERM.value, 0}


def touch_file(file: Path) -> None:  # pragma: no cover
    while True:
        time.sleep(0.1)
        with file.open("a") as f:
            f.write("hello")


@pytest.mark.slow()
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_restart(mock: Mock) -> None:
    file = DIR / "file.py"

    processor = WatchReloader(target=touch_file, args=(file,), reload_dirs=[DIR])

    mock.side_effect = lambda: os.kill(processor.pid, signal.SIGINT)

    with patch.object(processor, "restart", mock):
        processor.run()

    mock.assert_called_once()
    file.unlink()
