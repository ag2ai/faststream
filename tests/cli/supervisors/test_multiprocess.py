import os
import signal
import threading
import time

import pytest

from faststream._internal.cli.dto import RunArgs
from faststream._internal.cli.supervisors.multiprocess import Multiprocess
from tests.marks import skip_windows


def sleep_forever(args: RunArgs) -> None:  # pragma: no cover
    time.sleep(60)


@skip_windows
@pytest.mark.parametrize("sig", (signal.SIGINT, signal.SIGTERM))
def test_workers_are_terminated_on_exit_signal(sig: signal.Signals) -> None:
    processor = Multiprocess(target=sleep_forever, args=RunArgs(app=""), workers=2)
    # sent from here, not from a worker, so it cannot outlive the test and land
    # on the handler the next one installs
    sender = threading.Timer(0.1, os.kill, args=(processor.pid, sig))
    sender.start()

    processor.run()
    sender.join()

    assert [p.exitcode for p in processor.processes] == [-signal.SIGTERM] * 2
