import pytest

from faststream._internal.cli.utils.errors import draw_startup_errors
from faststream.exceptions import StartupValidationError


def test_draw_startup_errors(capsys: pytest.CaptureFixture[str]) -> None:
    error = StartupValidationError(
        missed_fields=("host",),
        invalid_fields=("port",),
    )

    draw_startup_errors(error)

    output = capsys.readouterr().err
    assert "Invalid value for '--port':" in output
    assert "Missing option '--host'." in output
