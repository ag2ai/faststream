import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from faststream import FastStream
from faststream._internal.cli.main import cli as faststream_app
from faststream._internal.cli.utils.logs import get_log_level


@pytest.mark.parametrize(
    (
        "level",
        "expected_level",
    ),
    (
        pytest.param("critical", logging.CRITICAL),
        pytest.param("fatal", logging.FATAL),
        pytest.param("error", logging.ERROR),
        pytest.param("warning", logging.WARNING),
        pytest.param("warn", logging.WARNING),
        pytest.param("info", logging.INFO),
        pytest.param("debug", logging.DEBUG),
        pytest.param("notset", logging.NOTSET),
    ),
)
def test_get_level(level: str, expected_level: int) -> None:
    assert get_log_level(level) == expected_level


def test_run_with_log_level(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, app),
    ):
        result = runner.invoke(
            faststream_app,
            ["run", "-l", "warning", "faststream:app"],
        )

        assert result.exit_code == 0, result.output

        assert app.logger.level == logging.WARNING


def test_run_with_wrong_log_level(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()

    with patch(
        "faststream._internal.cli.utils.imports._import_object_or_factory",
        return_value=(None, app),
    ):
        result = runner.invoke(
            faststream_app,
            ["run", "-l", "30", "faststream:app"],
        )

        assert result.exit_code == 2, result.output


def test_run_with_log_config(runner: CliRunner) -> None:
    app = FastStream(MagicMock())
    app.run = AsyncMock()

    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"app": {"format": "%(message)s"}},
        "handlers": {
            "app": {
                "class": "logging.StreamHandler",
                "formatter": "app",
                "level": "INFO",
            }
        },
        "loggers": {"app": {"level": "INFO", "handlers": ["app"]}},
    }

    with patch(
        "faststream._internal.cli.utils.logs._get_log_config",
        return_value=logging_config,
    ):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--log-config",
                "log_config.json",
            ],
        )
        app.run.assert_not_called()
        assert result.exit_code != 0
