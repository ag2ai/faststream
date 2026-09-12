import signal
import threading
import time

from faststream._internal.cli.dto import RunArgs
from faststream._internal.cli.supervisors.multiprocess import Multiprocess
from tests.marks import skip_windows


def sleep_forever(args: RunArgs) -> None:  # pragma: no cover
    time.sleep(60)


@skip_windows
def test_workers_are_terminated_on_shutdown() -> None:
    processor = Multiprocess(target=sleep_forever, args=RunArgs(app=""), workers=2)
    threading.Timer(0.1, processor.should_exit.set).start()

    processor.run()

    assert [p.exitcode for p in processor.processes] == [-signal.SIGTERM] * 2
