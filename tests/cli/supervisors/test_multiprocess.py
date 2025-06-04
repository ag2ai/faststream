import os
import signal
import sys

import pytest

from faststream._internal.cli.supervisors.multiprocess import Multiprocess


def exit(parent_id) -> None:  # pragma: no cover
    os.kill(parent_id, signal.SIGINT)


@pytest.mark.slow()
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_base() -> None:
    processor = Multiprocess(target=exit, args=(), workers=5)
    processor._args = (processor.pid,)
    processor.run()

    for p in processor.processes:
        code = abs(p.exitcode)
        assert code in {signal.SIGTERM.value, 0}
