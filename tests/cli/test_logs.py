import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from faststream import FastStream
from faststream._internal.cli.main import cli as faststream_app
from faststream._internal.cli.utils.logs import get_log_level
from tests.marks import skip_windows

from .conftest import FastStreamCLIFactory, GenerateTemplateFactory


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


@pytest.mark.slow()
@skip_windows
@pytest.mark.parametrize(
    ("log_config_file_name", "log_config"),
    (
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
    ),
)
def test_run_as_asgi_with_log_config(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
    log_config_file_name: str,
    log_config: str,
) -> None:
    app_code = """
    import logging

    from faststream.asgi import AsgiFastStream
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    app = AsgiFastStream(broker)

    logger = logging.getLogger("faststream")

    @app.on_startup
    def print_log_level() -> None:
        logger.critical(f"Current log level is {logging.getLogger('unique_logger_name').level}")
    """
    with (
        generate_template(app_code) as app_path,
        generate_template(
            log_config,
            filename=log_config_file_name,
        ) as log_config_file_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--log-config",
            str(log_config_file_path),
        ) as cli,
    ):
        assert cli.wait_for_stderr("Current log level is 42")


@pytest.mark.slow()
@skip_windows
@pytest.mark.flaky(reruns=3, reruns_delay=1)
def test_run_as_asgi_mp_with_log_level(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
) -> None:
    app_code = """
    import logging

    from faststream.asgi import AsgiFastStream
    from faststream.nats import NatsBroker

    app = AsgiFastStream(NatsBroker())

    logger = logging.getLogger("faststream")

    @app.on_startup
    def print_log_level() -> None:
        logger.critical(f"Current log level is {logging.getLogger('uvicorn.asgi').level}")
    """
    log_level, numeric_log_level = "warn", 30

    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--workers",
            "2",
            "--log-level",
            log_level,
        ) as cli,
    ):
        assert cli.wait_for_stderr(f"Current log level is {numeric_log_level}")


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
