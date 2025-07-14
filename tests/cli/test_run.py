import json
import random
import urllib.request

import psutil
import pytest

from faststream._compat import IS_WINDOWS
from tests.cli.conftest import FastStreamCLIFactory, GenerateTemplateFactory


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
def test_run(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    from unittest.mock import AsyncMock

    from faststream import FastStream

    app = FastStream(AsyncMock())
    """
    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app",
        ],
    ) as cli_thread:
        pass
    assert cli_thread.process

    assert cli_thread.process.returncode == 0


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
def test_run_asgi(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    import json
    from contextlib import asynccontextmanager

    from faststream import FastStream
    from faststream.asgi import AsgiResponse, get
    from faststream.nats import NatsBroker, TestNatsBroker

    broker = NatsBroker(max_reconnect_attempts=1)


    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)


    CONTEXT = {}


    @get
    async def context(scope):
        return AsgiResponse(json.dumps(CONTEXT).encode(), status_code=200)


    # must use broker implementation to generate the docs
    # but cannot connect to it, hence we patch it
    test_broker = TestNatsBroker(broker)


    @asynccontextmanager
    async def lifespan():
        async with test_broker:
            yield


    app = FastStream(
        broker,
        lifespan=lifespan,
    ).as_asgi(
        asgi_routes=[("/liveness", liveness_ping), ("/context", context)],
        asyncapi_path="/docs",
    )


    @app.on_startup
    async def start(test: int, port: int):
        CONTEXT["test"] = test
        CONTEXT["port"] = port

    """
    with generate_template(app_code) as app_path:
        port = random.randrange(40000, 65535)
        extra_param = random.randrange(1, 100)

        with faststream_cli(
            [
                "faststream",
                "run",
                f"{app_path.stem}:app",
                "--port",
                f"{port}",
                "--test",
                f"{extra_param}",
            ],
        ):
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/liveness"
            ) as response:
                assert response.read().decode() == "hello world"
                assert response.getcode() == 200

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/docs") as response:
                content = response.read().decode()
                assert content.strip().startswith("<!DOCTYPE html>")
                assert len(content) > 1200

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/context") as response:
                data = json.loads(response.read().decode())
                assert data == {"test": extra_param, "port": port}
                assert response.getcode() == 200


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
def test_run_as_asgi_with_single_worker(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    from unittest.mock import AsyncMock

    from faststream.asgi import AsgiFastStream, AsgiResponse, get

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)

    app = AsgiFastStream(AsyncMock(), asgi_routes=[
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
    ), urllib.request.urlopen("http://127.0.0.1:8000/liveness") as response:
        assert response.read().decode() == "hello world"
        assert response.getcode() == 200


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
@pytest.mark.parametrize("workers", [3, 5, 7])
def test_run_as_asgi_with_many_workers(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
    workers: int,
) -> None:
    app_code = """
    from unittest.mock import AsyncMock

    from faststream.asgi import AsgiFastStream

    app = AsgiFastStream(AsyncMock())
    """

    with generate_template(app_code) as app_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--workers",
            str(workers),
        ],
    ) as cli_thread:
        assert cli_thread.process
        process = psutil.Process(pid=cli_thread.process.pid)

        assert len(process.children()) == workers + 1


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
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
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
    log_level: str,
    numeric_log_level: int,
) -> None:
    app_code = """
    import logging
    from unittest.mock import AsyncMock

    from faststream.asgi import AsgiFastStream
    from faststream.log.logging import logger

    app = AsgiFastStream(AsyncMock())

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
    ) as cli_thread:
        pass
    assert cli_thread.process
    assert cli_thread.process.stderr
    stderr = cli_thread.process.stderr.read()

    assert f"Current log level is {numeric_log_level}" in stderr


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
def test_run_as_factory(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    from unittest.mock import AsyncMock

    from faststream.asgi import AsgiFastStream, AsgiResponse, get

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)

    def app_factory():
        return AsgiFastStream(AsyncMock(), asgi_routes=[
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
    ), urllib.request.urlopen("http://127.0.0.1:8000/liveness") as response:
        assert response.read().decode() == "hello world"
        assert response.getcode() == 200


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
@pytest.mark.parametrize(
    ("log_config_file_name", "log_config"),
    [
        pytest.param(
            "config.json",
            """
            {
                "version": 1,
                "loggers": {
                    "unique_logger_name": {
                        "level": 42
                    }
                }
            }
            """,
            id="json config",
        ),
        pytest.param(
            "config.toml",
            """
            version = 1

            [loggers.unique_logger_name]
            level = 42
            """,
            id="toml config",
        ),
        pytest.param(
            "config.yaml",
            """
            version: 1
            loggers:
                unique_logger_name:
                    level: 42
            """,
            id="yaml config",
        ),
        pytest.param(
            "config.yml",
            """
            version: 1
            loggers:
                unique_logger_name:
                    level: 42
            """,
            id="yml config",
        ),
    ],
)
def test_run_as_asgi_with_log_config(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
    log_config_file_name: str,
    log_config: str,
) -> None:
    app_code = """
    import logging
    from unittest.mock import AsyncMock

    from faststream.asgi import AsgiFastStream
    from faststream.log.logging import logger

    app = AsgiFastStream(AsyncMock())

    @app.on_startup
    def print_log_level():
        logger.critical(f"Current log level is {logging.getLogger('unique_logger_name').level}")
    """

    with generate_template(app_code) as app_path, generate_template(
        log_config, filename=log_config_file_name
    ) as log_config_file_path, faststream_cli(
        [
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--log-config",
            str(log_config_file_path),
        ],
    ) as cli_thread:
        pass
    assert cli_thread.process
    assert cli_thread.process.stderr
    stderr = cli_thread.process.stderr.read()

    assert "Current log level is 42" in stderr
