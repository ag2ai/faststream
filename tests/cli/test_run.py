import os
import urllib.request
from unittest.mock import AsyncMock, patch

import psutil
import pytest
from typer.testing import CliRunner

from faststream._compat import IS_WINDOWS
from faststream._internal.application import Application
from faststream.asgi import AsgiFastStream
from faststream.cli.main import cli as faststream_app
from tests.marks import python310

pytestmark = [
    pytest.mark.slow,
    python310,
    pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows"),
]


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
    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app",
        ],
        extra_env={
            "PATH": f"{app_path.parent}:{os.environ['PATH']}",
            "PYTHONPATH": str(app_path.parent),
        },
    ), urllib.request.urlopen("http://127.0.0.1:8000/liveness") as response:
        assert response.read().decode() == "hello world"
        assert response.getcode() == 200


def test_run_as_asgi_with_single_worker(
    generate_template,
    faststream_cli,
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
    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--workers",
            "1",
        ],
        extra_env={
            "PATH": f"{app_path.parent}:{os.environ['PATH']}",
            "PYTHONPATH": str(app_path.parent),
        },
    ), urllib.request.urlopen("http://127.0.0.1:8000/liveness") as response:
        assert response.read().decode() == "hello world"
        assert response.getcode() == 200


@pytest.mark.parametrize("workers", [3, 5, 7])
def test_run_as_asgi_with_many_workers(
    generate_template,
    faststream_cli,
    workers: int,
):
    app_code = """
    from faststream.asgi import AsgiFastStream
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    app = AsgiFastStream(broker)
    """

    with generate_template(app_code) as app_path, faststream_cli(
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
        process = psutil.Process(pid=cli_thread.process.pid)

        assert len(process.children()) == workers + 1


@pytest.mark.parametrize(
    ("log_level", "numeric_log_level"),
    [
        ("critical", 50),
        ("fatal", 50),
        ("error", 40),
        ("warning", 30),
        ("warn", 30),
        ("info", 20),
        ("debug", 10),
        ("notset", 0),
    ],
)
def test_run_as_asgi_mp_with_log_level(
    generate_template,
    faststream_cli,
    log_level: str,
    numeric_log_level: int,
):
    app_code = """
    import logging

    from faststream.asgi import AsgiFastStream
    from faststream.log.logging import logger
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    app = AsgiFastStream(broker)

    @app.on_startup
    def print_log_level():
        logger.critical(f"Current log level is {logging.getLogger('uvicorn.asgi').level}")
    """

    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--workers",
            "3",
            "--log-level",
            log_level,
        ],
        extra_env={
            "PATH": f"{app_path.parent}:{os.environ['PATH']}",
            "PYTHONPATH": str(app_path.parent),
        },
    ) as cli_thread:
        pass
    stderr = cli_thread.process.stderr.read()

    assert f"Current log level is {numeric_log_level}" in stderr


def test_run_as_factory(generate_template, faststream_cli):
    app_code = """
    from faststream.asgi import AsgiFastStream, AsgiResponse, get
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)

    def app_factory():
        return AsgiFastStream(broker, asgi_routes=[
            ("/liveness", liveness_ping),
        ])
    """

    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app_factory",
            "--factory",
        ],
        extra_env={
            "PATH": f"{app_path.parent}:{os.environ['PATH']}",
            "PYTHONPATH": str(app_path.parent),
        },
    ), urllib.request.urlopen("http://127.0.0.1:8000/liveness") as response:
        assert response.read().decode() == "hello world"
        assert response.getcode() == 200


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
