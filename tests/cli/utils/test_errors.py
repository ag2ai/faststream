import re

from faststream._internal.cli.utils.errors import draw_startup_errors
from faststream.exceptions import StartupValidationError

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def test_draw_startup_errors(capsys) -> None:
    draw_startup_errors(
        StartupValidationError(
            invalid_fields=["wrong"],
            missed_fields=["required"],
        ),
    )

    stderr = ANSI_ESCAPE_RE.sub("", capsys.readouterr().err)
    assert "--wrong" in stderr
    assert "--required" in stderr
