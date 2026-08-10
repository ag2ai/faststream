import re

import pytest

from faststream._internal.cli.utils.errors import draw_startup_errors
from faststream.exceptions import StartupValidationError

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def test_draw_startup_errors(capsys: pytest.CaptureFixture[str]) -> None:
    error = StartupValidationError(
        missed_fields=("host",),
        invalid_fields=("port",),
    )

    draw_startup_errors(error)

    output = ANSI_ESCAPE_RE.sub("", capsys.readouterr().err)
    assert "Invalid value for '--port':" in output
    assert "Missing option '--host'." in output
