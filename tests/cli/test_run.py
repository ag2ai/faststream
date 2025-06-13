import logging
import os
import urllib.request
from unittest.mock import AsyncMock, Mock, patch

import psutil
import pytest
from typer.testing import CliRunner

from faststream._internal.application import Application
from faststream.app import FastStream
from faststream.asgi import AsgiFastStream
from faststream.cli.main import cli as faststream_app
from faststream.cli.utils.logs import get_log_level


@pytest.mark.slow
def test_run(generate_template, faststream_cli) -> None:
    app_code = """
    from faststream.asgi import AsgiFastStream, AsgiResponse, get
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)

    app = AsgiFastStream(broker, asgi_routes=[
        ("/liveness", liveness_ping),
    ])
    """
    with generate_template(app_code) as app_path:
        module_name = str(app_path).replace(".py", "")
        print(module_name)
        with faststream_cli(
            [
                "faststream",
                "run",
                f"{module_name}:app",
            ]
        ) as cli_thread:
            with urllib.request.urlopen(  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                "http://127.0.0.1:8000/liveness"
            ) as response:
                assert response.read().decode() == "hello world"
                assert response.getcode() == 200


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
def test_run_as_asgi_with_many_workers(
    generate_template,
    faststream_cli,
    workers: int,
):
    app_code = """
    from faststream.asgi import AsgiFastStream, AsgiResponse, get
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)

    app = AsgiFastStream(broker, asgi_routes=[
        ("/liveness", liveness_ping),
    ])
    """
    with generate_template(app_code) as app_path:
        with faststream_cli(
            [
                "faststream",
                "run",
                f"{app_path.stem}:app",
                "--workers",
                str(workers),
            ],
            extra_env={
                "PATH": f"{app_path.parent}:{os.environ['PATH']}",
                "PYTHONPATH": str(app_path.parent),
            },
        ) as cli_thread:
            with urllib.request.urlopen(  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                "http://127.0.0.1:8000/liveness"
            ) as response:
                assert response.read().decode() == "hello world"
                assert response.getcode() == 200
            process = psutil.Process(pid=cli_thread.process.pid)
            assert len(process.children()) == workers + 1


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
                None,
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


@pytest.mark.parametrize(
    "log_config",
    [
        pytest.param("config.json"),
        pytest.param("config.toml"),
        pytest.param("config.yaml"),
        pytest.param("config.yml"),
    ],
)
@pytest.mark.parametrize("app", [pytest.param(AsgiFastStream())])
def test_run_as_asgi_mp_with_log_config(
    runner: CliRunner,
    app: Application,
    log_config: str,
):
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
        "faststream.cli.utils.logs._get_log_config",
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
                f"--log_config {log_config}",
            ],
        )
        app.run.assert_not_called()
        assert result.exit_code != 0
