import os
import signal

import pytest

from faststream._internal.cli.dto import RunArgs
from faststream._internal.cli.supervisors.multiprocess import Multiprocess
from tests.marks import skip_windows


def exit(args: RunArgs) -> None:  # pragma: no cover
    os.kill(
        int(args.extra_options["parent_id"]),
        signal.SIGINT,
    )
    raise SyntaxError


@skip_windows
@pytest.mark.flaky(reruns=3, reruns_delay=1)
def test_base() -> None:
    args = RunArgs(app="")
    processor = Multiprocess(target=exit, args=args, workers=2)
    processor._args.extra_options = {"parent_id": processor.pid}
    processor.run()

    for p in processor.processes:
        assert p.exitcode
        code = abs(p.exitcode)
        assert code in {signal.SIGTERM.value, 0}
