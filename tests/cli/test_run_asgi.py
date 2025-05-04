import logging
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from faststream._internal.cli.main import cli as faststream_app
from faststream.asgi import AsgiFastStream

IMPORT_FUNCTION_MOCK_PATH = (
    "faststream._internal.cli.utils.imports._import_object_or_factory"
)


def test_run_as_asgi(runner: CliRunner) -> None:
    app = AsgiFastStream(AsyncMock())
    app.run = AsyncMock()

    with patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app)):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--workers",
                "1",
            ],
        )

        assert result.exit_code == 0

        app.run.assert_awaited_once_with(
            logging.INFO,
            {"host": "0.0.0.0", "port": "8000"},
        )


def test_run_as_asgi_with_workers(runner: CliRunner) -> None:
    app = AsgiFastStream(AsyncMock())
    app.run = AsyncMock()

    asgi_multiprocess = (
        "faststream._internal.cli.supervisors.asgi_multiprocess.ASGIMultiprocess"
    )

    with (
        patch(asgi_multiprocess) as asgi_runner,
        patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app)),
    ):
        workers = 2

        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "-w",
                str(workers),
            ],
        )

        assert result.exit_code == 0

        asgi_runner.assert_called_once_with(
            target="faststream:app",
            args=("faststream:app", {}, False, 0),
            workers=workers,
        )


def test_run_as_asgi_factory(runner: CliRunner) -> None:
    app = AsgiFastStream(AsyncMock())
    app.run = AsyncMock()
    app_factory = MagicMock(return_value=app)

    with patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app_factory)):
        result = runner.invoke(
            faststream_app,
            ["run", "-f", "faststream:app"],
        )

        assert result.exit_code == 0

        app_factory.assert_called_once()
        app.run.assert_awaited_once_with(logging.INFO, {})


def test_run_as_asgi_multiprocess_with_log_level(runner: CliRunner) -> None:
    app = AsgiFastStream(AsyncMock())
    app.run = AsyncMock()

    asgi_multiprocess = (
        "faststream._internal.cli.supervisors.asgi_multiprocess.ASGIMultiprocess"
    )

    with (
        patch(asgi_multiprocess) as asgi_runner,
        patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app)),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--workers",
                "2",
                "--log-level",
                "critical",
            ],
        )
        assert result.exit_code == 0

        asgi_runner.assert_called_once_with(
            target="faststream:app",
            args=(
                "faststream:app",
                {},
                False,
                logging.CRITICAL,
            ),
            workers=2,
        )
        asgi_runner().run.assert_called_once()
