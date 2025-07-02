import os
import signal
import typing

from faststream._internal.cli.supervisors.multiprocess import Multiprocess
from tests.marks import skip_windows


def exit(parent_id: int, *args: typing.Any) -> None:  # pragma: no cover
    os.kill(parent_id, signal.SIGINT)
    raise SyntaxError


@skip_windows
def test_base() -> None:
    processor = Multiprocess(target=exit, args=(), workers=2)
    processor._args = (processor.pid, {})
    processor.run()

    for p in processor.processes:
        assert p.exitcode
        code = abs(p.exitcode)
        assert code == signal.SIGTERM.value
