from faststream._internal.cli.utils.errors import draw_startup_errors
from faststream.exceptions import StartupValidationError


def test_draw_startup_errors(capsys) -> None:
    draw_startup_errors(
        StartupValidationError(
            invalid_fields=["wrong"],
            missed_fields=["required"],
        ),
    )

    stderr = capsys.readouterr().err
    assert "--wrong" in stderr
    assert "--required" in stderr
