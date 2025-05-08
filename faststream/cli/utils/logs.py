import json
import logging
import logging.config
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, DefaultDict, Dict, Optional, Union

import typer

from faststream.exceptions import INSTALL_TOML, INSTALL_YAML

if TYPE_CHECKING:
    from faststream._internal.application import Application
    from faststream.types import LoggerProto


class LogLevels(str, Enum):
    """A class to represent log levels.

    Attributes:
        critical : critical log level
        error : error log level
        warning : warning log level
        info : info log level
        debug : debug log level
    """

    critical = "critical"
    fatal = "fatal"
    error = "error"
    warning = "warning"
    warn = "warn"
    info = "info"
    debug = "debug"
    notset = "notset"


LOG_LEVELS: DefaultDict[str, int] = defaultdict(
    lambda: logging.INFO,
    **{
        "critical": logging.CRITICAL,
        "fatal": logging.FATAL,
        "error": logging.ERROR,
        "warning": logging.WARNING,
        "warn": logging.WARN,
        "info": logging.INFO,
        "debug": logging.DEBUG,
        "notset": logging.NOTSET,
    },
)


class LogFiles(str, Enum):
    """The class to represent supported log configuration files."""

    json = ".json"
    yaml = ".yaml"
    yml = ".yml"
    toml = ".toml"


def get_log_level(level: Union[LogLevels, str, int]) -> int:
    """Get the log level.

    Args:
        level: The log level to get. Can be an integer, a LogLevels enum value, or a string.

    Returns:
        The log level as an integer.

    """
    if isinstance(level, int):
        return level

    if isinstance(level, LogLevels):
        return LOG_LEVELS[level.value]

    if isinstance(level, str):  # pragma: no branch
        return LOG_LEVELS[level.lower()]


def set_log_level(level: int, app: "Application") -> None:
    """Sets the log level for an application."""
    if app.logger and getattr(app.logger, "setLevel", None):
        app.logger.setLevel(level)  # type: ignore[attr-defined]

    broker_logger: Optional[LoggerProto] = getattr(app.broker, "logger", None)
    if broker_logger is not None and getattr(broker_logger, "setLevel", None):
        broker_logger.setLevel(level)  # type: ignore[attr-defined]


def _get_json_config(file: Path) -> Union[Dict[str, Any], Any]:
    """Parse json config file to dict."""
    with file.open("r") as config_file:
        return json.load(config_file)


def _get_yaml_config(file: Path) -> Union[Dict[str, Any], Any]:
    """Parse yaml config file to dict."""
    try:
        import yaml
    except ImportError as e:
        typer.echo(INSTALL_YAML, err=True)
        raise typer.Exit(1) from e

    with file.open("r") as config_file:
        return yaml.safe_load(config_file)


def _get_toml_config(file: Path) -> Union[Dict[str, Any], Any]:
    """Parse toml config file to dict."""
    try:
        import tomllib
    except ImportError:
        try:
            import tomli as tomllib
        except ImportError as e:
            typer.echo(INSTALL_TOML, err=True)
            raise typer.Exit(1) from e

    with file.open("rb") as config_file:
        return tomllib.load(config_file)


def _get_log_config(file: Path) -> Union[Dict[str, Any], Any]:
    """Read dict config from file."""
    if not file.exists():
        raise ValueError(f"File {file} specified to --log-config not found")

    file_format = file.suffix

    if file_format == LogFiles.json:
        logging_config = _get_json_config(file)
    elif file_format == LogFiles.yaml or file_format == LogFiles.yml:
        logging_config = _get_yaml_config(file)
    elif file_format == LogFiles.toml:
        logging_config = _get_toml_config(file)
    else:
        raise ValueError(
            f"Format {file_format} specified to --log-config file is not supported"
        )

    return logging_config


def set_log_config(file: Path) -> None:
    """Set the logging config from file."""
    configuration = _get_log_config(file)
    logging.config.dictConfig(configuration)
