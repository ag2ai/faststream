import logging
from unittest.mock import AsyncMock, MagicMock, patch

from dirty_equals import IsPartialDict
from typer.testing import CliRunner

from faststream import FastStream
from faststream._internal.cli.main import cli as faststream_app

IMPORT_FUNCTION_MOCK_PATH = (
    "faststream._internal.cli.utils.imports._import_object_or_factory"
)


def test_run(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()

    with patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app)):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--extra",
                "1",
            ],
        )

        assert result.exit_code == 0

        app.run.assert_awaited_once_with(
            logging.INFO,
            {"extra": "1"},
        )


def test_run_factory(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()
    app_factory = MagicMock(return_value=app)

    with patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app_factory)):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "-f",
            ],
        )

        assert result.exit_code == 0

        app_factory.assert_called_once()
        app.run.assert_awaited_once()


def test_run_workers(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()

    with (
        patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app)),
        patch(
            "faststream._internal.cli.supervisors.multiprocess.Multiprocess",
        ) as mock,
    ):
        app_str = "faststream:app"
        result = runner.invoke(
            faststream_app,
            ["run", app_str, "-w", "2"],
        )

        assert result.exit_code == 0

        assert mock.call_args.kwargs == IsPartialDict({
            "args": (app_str, {}, False, logging.NOTSET, logging.DEBUG),
            "workers": 2,
        })


def test_run_factory_with_workers(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()
    app_factory = MagicMock(return_value=app)

    with (
        patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app_factory)),
        patch(
            "faststream._internal.cli.supervisors.multiprocess.Multiprocess",
        ) as mock,
    ):
        app_str = "faststream:app"
        result = runner.invoke(
            faststream_app,
            ["run", app_str, "-f", "-w", "2"],
        )

        assert result.exit_code == 0

        assert mock.call_args.kwargs == IsPartialDict({
            "args": (app_str, {}, True, logging.NOTSET, logging.DEBUG),
            "workers": 2,
        })


def test_run_reloader(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()

    with (
        patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app)),
        patch(
            "faststream._internal.cli.supervisors.watchfiles.WatchReloader",
        ) as mock,
    ):
        app_str = "faststream:app"

        result = runner.invoke(
            faststream_app,
            [
                "run",
                app_str,
                "-r",
                "--app-dir",
                "test",
                "--extension",
                "yaml",
            ],
        )

        assert result.exit_code == 0

        assert mock.call_args.kwargs == IsPartialDict({
            "args": (app_str, {}, False, logging.NOTSET),
            "reload_dirs": ["test"],
            "extra_extensions": ["yaml"],
        })


def test_run_reloader_with_factory(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()
    app_factory = MagicMock(return_value=app)

    with (
        patch(IMPORT_FUNCTION_MOCK_PATH, return_value=(None, app_factory)),
        patch(
            "faststream._internal.cli.supervisors.watchfiles.WatchReloader",
        ) as mock,
    ):
        app_str = "faststream:app"

        result = runner.invoke(
            faststream_app,
            [
                "run",
                app_str,
                "-f",
                "-r",
                "--app-dir",
                "test",
                "--extension",
                "yaml",
            ],
        )

        assert result.exit_code == 0

        assert mock.call_args.kwargs == IsPartialDict({
            "args": (app_str, {}, True, logging.NOTSET),
            "reload_dirs": ["test"],
            "extra_extensions": ["yaml"],
        })
