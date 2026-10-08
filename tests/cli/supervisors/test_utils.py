import signal
import time
from multiprocessing.synchronize import Event

from faststream._internal.cli.supervisors.utils import (
    get_subprocess,
    spawn,
    stop_process,
)
from tests.marks import skip_windows


def deaf_to_sigterm(ready: Event) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    ready.set()
    time.sleep(60)


@skip_windows
def test_stop_process_kills_worker_deaf_to_sigterm() -> None:
    ready = spawn.Event()

    process = get_subprocess(target=deaf_to_sigterm, args=(ready,))
    process.start()
    assert ready.wait(timeout=10)

    stop_process(process, timeout=0.1)

    assert process.exitcode == -signal.SIGKILL
