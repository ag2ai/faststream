import logging
from unittest.mock import AsyncMock, Mock, patch

import pytest
from typer.testing import CliRunner

from faststream._internal.application import Application
from faststream.app import FastStream
from faststream.asgi import AsgiFastStream
from faststream.cli.main import cli as faststream_app
from faststream.cli.utils.logs import get_log_level


@pytest.mark.parametrize(
    "app", [pytest.param(FastStream()), pytest.param(AsgiFastStream())]
)
def test_run(runner: CliRunner, app: Application):
    app.run = AsyncMock()

    with patch(
        "faststream.cli.utils.imports._import_obj_or_factory", return_value=(None, app)
    ):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
            ],
        )
        app.run.assert_awaited_once_with(
            logging.INFO, {"host": "0.0.0.0", "port": "8000"}
        )
        assert result.exit_code == 0


@pytest.mark.parametrize("app", [pytest.param(AsgiFastStream())])
def test_run_as_asgi_with_single_worker(runner: CliRunner, app: Application):
    app.run = AsyncMock()

    with patch(
        "faststream.cli.utils.imports._import_obj_or_factory", return_value=(None, app)
    ):
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
        app.run.assert_awaited_once_with(
            logging.INFO, {"host": "0.0.0.0", "port": "8000"}
        )
        assert result.exit_code == 0


@pytest.mark.parametrize("workers", [3, 5, 7])
@pytest.mark.parametrize("app", [pytest.param(AsgiFastStream())])
def test_run_as_asgi_with_many_workers(
    runner: CliRunner, workers: int, app: Application
):
    asgi_multiprocess = "faststream.cli.supervisors.asgi_multiprocess.ASGIMultiprocess"
    _import_obj_or_factory = "faststream.cli.utils.imports._import_obj_or_factory"

    with patch(asgi_multiprocess) as asgi_runner, patch(
        _import_obj_or_factory, return_value=(None, app)
    ):
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
                str(workers),
            ],
        )
        assert result.exit_code == 0

        asgi_runner.assert_called_once()
        asgi_runner.assert_called_once_with(
            target="faststream:app",
            args=("faststream:app", {"host": "0.0.0.0", "port": "8000"}, False, None, 0),
            workers=workers,
        )
        asgi_runner().run.assert_called_once()


@pytest.mark.parametrize(
    "log_level",
    ["critical", "fatal", "error", "warning", "warn", "info", "debug", "notset"],
)
@pytest.mark.parametrize("app", [pytest.param(AsgiFastStream())])
def test_run_as_asgi_mp_with_log_level(
    runner: CliRunner, app: Application, log_level: str
):
    asgi_multiprocess = "faststream.cli.supervisors.asgi_multiprocess.ASGIMultiprocess"
    _import_obj_or_factory = "faststream.cli.utils.imports._import_obj_or_factory"

    with patch(asgi_multiprocess) as asgi_runner, patch(
        _import_obj_or_factory, return_value=(None, app)
    ):
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
                "3",
                "--log-level",
                log_level,
            ],
        )
        assert result.exit_code == 0

        asgi_runner.assert_called_once()
        asgi_runner.assert_called_once_with(
            target="faststream:app",
            args=(
                "faststream:app",
                {"host": "0.0.0.0", "port": "8000"},
                False,
                get_log_level(log_level),
            ),
            workers=3,
        )
        asgi_runner().run.assert_called_once()


@pytest.mark.parametrize(
    "app", [pytest.param(FastStream()), pytest.param(AsgiFastStream())]
)
def test_run_as_factory(runner: CliRunner, app: Application):
    app.run = AsyncMock()

    app_factory = Mock(return_value=app)

    with patch(
        "faststream.cli.utils.imports._import_obj_or_factory",
        return_value=(None, app_factory),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--factory",
            ],
        )
        app_factory.assert_called()
        app.run.assert_awaited_once_with(
            logging.INFO, {"host": "0.0.0.0", "port": "8000"}
        )
        assert result.exit_code == 0


@pytest.mark.parametrize(
    "app", [pytest.param(FastStream()), pytest.param(AsgiFastStream())]
)
def test_run_app_like_factory_but_its_fake(runner: CliRunner, app: Application):
    app.run = AsyncMock()

    with patch(
        "faststream.cli.utils.imports._import_obj_or_factory",
        return_value=(None, app),
    ):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--factory",
            ],
        )
        app.run.assert_not_called()
        assert result.exit_code != 0


@pytest.mark.parametrize("app", [pytest.param(AsgiFastStream())])
def test_run_as_asgi_mp_with_log_config(
    runner: CliRunner, app: Application, log_level: str
):
    app.run = AsyncMock()
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"app": {"format": " JSON CONFIG: %(message)s"}},
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
        "faststream.cli.utils.logs.get_log_config",
        return_value=logging_config,
    ):
        result = runner.invoke(
            faststream_app,
            [
                "run",
                "faststream:app",
                "--host",
                "0.0.0.0",
                "--port",
                "8000",
                "--log_config config.json",
            ],
        )
        app.run.assert_not_called()
        assert result.exit_code != 0
