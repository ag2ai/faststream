import logging
import sys
import warnings
from contextlib import suppress
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import anyio
import typer
from click.exceptions import MissingParameter
from typer.core import TyperOption

from faststream import FastStream
from faststream.__about__ import __version__
from faststream._internal.application import Application
from faststream.asgi.app import AsgiFastStream
from faststream.cli.docs.app import docs_app
from faststream.cli.utils.imports import import_from_string
from faststream.cli.utils.logs import (
    LogFiles,
    LogLevels,
    get_log_level,
    set_log_config,
    set_log_level,
)
from faststream.cli.utils.parser import parse_cli_args
from faststream.exceptions import INSTALL_WATCHFILES, SetupError, ValidationError

if TYPE_CHECKING:
    from faststream.broker.core.usecase import BrokerUsecase
    from faststream.types import AnyDict, SettingField

cli = typer.Typer(pretty_exceptions_short=True)
cli.add_typer(docs_app, name="docs", help="AsyncAPI schema commands")


def version_callback(version: bool) -> None:
    """Callback function for displaying version information."""
    if version:
        import platform

        typer.echo(
            f"Running FastStream {__version__} with {platform.python_implementation()} "
            f"{platform.python_version()} on {platform.system()}"
        )

        raise typer.Exit()


@cli.callback()
def main(
    version: Optional[bool] = typer.Option(
        False,
        "-v",
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show current platform, python and FastStream version.",
    ),
) -> None:
    """Generate, run and manage FastStream apps to greater development experience."""


@cli.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def run(
    ctx: typer.Context,
    app: str = typer.Argument(
        ...,
        help="[python_module:FastStream] - path to your application.",
    ),
    workers: int = typer.Option(
        1,
        show_default=False,
        help="Run [workers] applications with process spawning.",
        envvar="FASTSTREAM_WORKERS",
    ),
    log_level: LogLevels = typer.Option(
        LogLevels.notset,
        case_sensitive=False,
        help="Set selected level for FastStream and brokers logger objects.",
        envvar="FASTSTREAM_LOG_LEVEL",
    ),
    log_config: Optional[Path] = typer.Option(
        None,
        help=f"Set file to configure logging. Supported {[x.value for x in LogFiles]}",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        is_flag=True,
        help="Restart app at directory files changes.",
    ),
    watch_extensions: List[str] = typer.Option(
        (),
        "--extension",
        "--ext",
        "--reload-extension",
        "--reload-ext",
        help="List of file extensions to watch by.",
    ),
    app_dir: str = typer.Option(
        ".",
        "--app-dir",
        help=(
            "Look for APP in the specified directory, by adding this to the PYTHONPATH."
            " Defaults to the current working directory."
        ),
        envvar="FASTSTREAM_APP_DIR",
    ),
    is_factory: bool = typer.Option(
        False,
        "--factory",
        is_flag=True,
        help="Treat APP as an application factory.",
    ),
) -> None:
    """Run [MODULE:APP] FastStream application."""
    if watch_extensions and not reload:
        typer.echo(
            "Extra reload extensions has no effect without `--reload` flag."
            "\nProbably, you forgot it?"
        )

    app, extra = parse_cli_args(app, *ctx.args)
    casted_log_level = get_log_level(log_level)

    if app_dir:  # pragma: no branch
        sys.path.insert(0, app_dir)

    # Should be imported after sys.path changes
    module_path, app_obj = import_from_string(app, is_factory=is_factory)

    args = (app, extra, is_factory, log_config, casted_log_level)

    if reload and workers > 1:
        raise SetupError("You can't use reload option with multiprocessing")

    if reload:
        try:
            from faststream.cli.supervisors.watchfiles import WatchReloader
        except ImportError:
            warnings.warn(INSTALL_WATCHFILES, category=ImportWarning, stacklevel=1)
            _run(*args)

        else:
            if app_dir != ".":
                reload_dirs = [str(module_path), app_dir]
            else:
                reload_dirs = [str(module_path)]

            WatchReloader(
                target=_run,
                args=args,
                reload_dirs=reload_dirs,
                extra_extensions=watch_extensions,
            ).run()

    elif workers > 1:
        if isinstance(app_obj, FastStream):
            from faststream.cli.supervisors.multiprocess import Multiprocess

            Multiprocess(
                target=_run,
                args=(*args, logging.DEBUG),
                workers=workers,
            ).run()
        elif isinstance(app_obj, AsgiFastStream):
            from faststream.cli.supervisors.asgi_multiprocess import ASGIMultiprocess

            ASGIMultiprocess(
                target=app,
                args=args,  # type: ignore[arg-type]
                workers=workers,
            ).run()
        else:
            raise typer.BadParameter(
                f"Unexpected app type, expected FastStream or AsgiFastStream, got: {type(app_obj)}."
            )

    else:
        _run_imported_app(
            app_obj,
            extra_options=extra,
            log_level=casted_log_level,
            log_config=log_config,
        )


def _run(
    # NOTE: we should pass `str` due FastStream is not picklable
    app: str,
    extra_options: Dict[str, "SettingField"],
    is_factory: bool,
    log_config: Optional[Path],
    log_level: int = logging.NOTSET,
    app_level: int = logging.INFO,  # option for reloader only
) -> None:
    """Runs the specified application."""
    _, app_obj = import_from_string(app, is_factory=is_factory)
    _run_imported_app(
        app_obj,
        extra_options=extra_options,
        log_level=log_level,
        app_level=app_level,
        log_config=log_config,
    )


def _run_imported_app(
    app_obj: "Application",
    extra_options: Dict[str, "SettingField"],
    log_config: Optional[Path],
    log_level: int = logging.NOTSET,
    app_level: int = logging.INFO,  # option for reloader only
) -> None:
    if not isinstance(app_obj, Application):
        raise typer.BadParameter(
            f'Imported object "{app_obj}" must be "Application" type.',
        )

    if log_level > 0:
        set_log_level(log_level, app_obj)

    if log_config is not None:
        set_log_config(log_config)

    if sys.platform not in ("win32", "cygwin", "cli"):  # pragma: no cover
        with suppress(ImportError):
            import uvloop

            uvloop.install()

    try:
        anyio.run(
            app_obj.run,
            app_level,
            extra_options,
        )

    except ValidationError as e:
        ex = MissingParameter(
            message=(
                "You registered extra options in your application "
                "`lifespan/on_startup` hook, but does not set in CLI."
            ),
            param=TyperOption(param_decls=[f"--{x}" for x in e.fields]),
        )

        try:
            from typer import rich_utils

            rich_utils.rich_format_error(ex)
        except ImportError:
            ex.show()

        sys.exit(1)


@cli.command(
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True}
)
def publish(
    ctx: typer.Context,
    app: str = typer.Argument(..., help="FastStream app instance, e.g., main:app."),
    message: str = typer.Argument(..., help="Message to be published."),
    rpc: bool = typer.Option(False, help="Enable RPC mode and system output."),
    is_factory: bool = typer.Option(
        False,
        "--factory",
        is_flag=True,
        help="Treat APP as an application factory.",
    ),
) -> None:
    """Publish a message using the specified broker in a FastStream application.

    This command publishes a message to a broker configured in a FastStream app instance.
    It supports various brokers and can handle extra arguments specific to each broker type.
    These are parsed and passed to the broker's publish method.
    """
    app, extra = parse_cli_args(app, *ctx.args)
    extra["message"] = message
    extra["rpc"] = rpc

    try:
        if not app:
            raise ValueError("App parameter is required.")
        if not message:
            raise ValueError("Message parameter is required.")

        _, app_obj = import_from_string(app, is_factory=is_factory)

        if not app_obj.broker:
            raise ValueError("Broker instance not found in the app.")

        result = anyio.run(publish_message, app_obj.broker, extra)

        if rpc:
            typer.echo(result)

    except Exception as e:
        typer.echo(f"Publish error: {e}")
        sys.exit(1)


async def publish_message(broker: "BrokerUsecase[Any, Any]", extra: "AnyDict") -> Any:
    try:
        async with broker:
            return await broker.publish(**extra)
    except Exception as e:
        typer.echo(f"Error when broker was publishing: {e}")
        sys.exit(1)
